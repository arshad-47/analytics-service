import logging
import os
import urllib.request
import urllib.parse
import urllib.error
import mimetypes
import asyncio
import concurrent.futures
import functools
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import get_submission_type_and_payload
from app.services.image_blur import anonymize_face
from app.services.storage import get_object_storage, AccessMode, resolve_url

logger = logging.getLogger("analytics_service.temporal.activities")

BASE_DIR = Path(__file__).resolve().parents[2]
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUTS_DIR = BASE_DIR / "outputs"

# Dedicated executor for image download/blur/upload — kept separate from the
# worker's general activity thread pool (sized to WORKER_MAX_CONCURRENT_ACTIVITIES
# in app/temporal/worker.py) so image-processing concurrency can be tuned
# independently, without this activity's fan-out stealing thread-pool capacity
# from LLM/embedding activities or vice versa. Size via IMAGE_EXECUTOR_MAX_WORKERS
# (settings) — downloads/uploads are I/O-bound and benefit from more concurrency
# than there are cores, but the face-blur step itself is CPU-bound and gains
# nothing past that; the default splits the difference rather than optimizing
# purely for either side.
_IMAGE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=settings.IMAGE_EXECUTOR_MAX_WORKERS,
    thread_name_prefix="image-blur",
)

_blur_semaphore: asyncio.Semaphore = None
_blur_semaphore_loop = None


def _get_blur_semaphore() -> asyncio.Semaphore:
    # Lazily (re)created against whichever event loop is currently running —
    # a module-level asyncio.Semaphore() constructed at import time would bind
    # to the wrong loop (mirrors the same pattern app/database/db.py already
    # uses for its own connect lock, for the same reason). Size via
    # BLUR_CONCURRENCY_LIMIT (settings) — caps how many face-blur subprocesses
    # run at once ACROSS THE WHOLE WORKER PROCESS, not per submission.
    # anonymize_face() shells out to the deface CLI, which spawns a fresh
    # process and loads its own ONNX model weights on every call: memory-heavy,
    # unlike the download/upload legs. Load-tested: letting this scale with
    # submission concurrency (instead of a small global cap) got a deface
    # subprocess OOM-killed (exit code -9) under concurrent load. This should
    # be sized against available memory headroom, NOT submission count, worker
    # concurrency, or CPU count.
    global _blur_semaphore, _blur_semaphore_loop
    current_loop = asyncio.get_running_loop()
    if _blur_semaphore is None or _blur_semaphore_loop is not current_loop:
        _blur_semaphore = asyncio.Semaphore(settings.BLUR_CONCURRENCY_LIMIT)
        _blur_semaphore_loop = current_loop
    return _blur_semaphore


async def _run_in_image_executor(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_IMAGE_EXECUTOR, functools.partial(func, *args, **kwargs))


def _is_allowed_media_host(resolved_url: str) -> bool:
    """
    Restricts image downloads to MEDIA_BASE_URL's own host. image_urls come
    from stored submission payloads — an absolute URL there was previously
    passed straight to urlopen() with no host check, so a malicious or
    compromised upstream could make the worker fetch internal endpoints
    (SSRF), and the concurrent per-image fan-out would only increase how many
    such requests could be issued at once.
    """
    allowed_host = urllib.parse.urlparse(settings.MEDIA_BASE_URL).netloc.lower()
    if not allowed_host:
        return False
    return urllib.parse.urlparse(resolved_url).netloc.lower() == allowed_host


def _download_file(url: str, filename: str) -> Path:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DOWNLOADS_DIR / filename
    logger.info(f"Downloading {url} to {local_path}")

    with urllib.request.urlopen(url, timeout=60) as response:
        with open(local_path, "wb") as f:
            f.write(response.read())
    return local_path


def _compute_dynamic_scale(image_path: Path, cap: Optional[str]) -> Optional[str]:
    """
    Returns the --scale argument for deface, computed per-image based on its
    actual pixel dimensions, or None to run at native resolution.

    DEFACE_SCALE acts as a ceiling on the image's LONG EDGE, not a literal
    WxH constraint. This makes it orientation-agnostic — portrait and landscape
    photos with the same pixel budget are treated identically:

      DEFACE_SCALE=1920x1080  →  long-edge cap = max(1920, 1080) = 1920

      640×480   → long edge  480 ≤ 1920 → native res  ✅
      1080×1912 → long edge 1912 ≤ 1920 → native res  ✅  (portrait phone photo)
      1920×1080 → long edge 1920 ≤ 1920 → native res  ✅
      4000×3000 → long edge 4000 > 1920 → 1920×1440   (proportional downscale)
      6000×4000 → long edge 6000 > 1920 → 1920×1280   (proportional downscale)

    This eliminates the fixed-scale problem where a 640x480 thumbnail and a
    4000x3000 phone photo both got forced to the same detection resolution,
    making faces in the thumbnail too tiny to detect.
    """
    if not cap:
        # No cap configured — always run at native resolution
        return None

    # Parse the WxH cap string (e.g. "1920x1080") and derive the long-edge limit.
    # max() makes this orientation-agnostic: portrait images whose height exceeds
    # the smaller cap dimension are not penalised unnecessarily.
    try:
        cap_long_edge = max(int(v) for v in cap.lower().split("x"))
    except (ValueError, AttributeError):
        logger.warning(f"Invalid DEFACE_SCALE value {cap!r} — falling back to native resolution")
        return None

    # Read actual image dimensions — PIL is a deface dependency so always available
    try:
        from PIL import Image
        with Image.open(image_path) as img:
            img_w, img_h = img.size
    except Exception as exc:
        logger.warning(f"Could not read dimensions of {image_path.name} ({exc}) — using native resolution")
        return None

    img_long_edge = max(img_w, img_h)

    # Long edge already within cap → native resolution, no downscaling needed
    if img_long_edge <= cap_long_edge:
        logger.debug(
            f"{image_path.name} is {img_w}x{img_h} (long edge {img_long_edge}px) — "
            f"within {cap_long_edge}px cap, running detection at native resolution"
        )
        return None

    # Proportionally downscale so the long edge meets the cap exactly
    scale_factor = cap_long_edge / img_long_edge
    target_w = max(1, int(img_w * scale_factor))
    target_h = max(1, int(img_h * scale_factor))
    dynamic_scale = f"{target_w}x{target_h}"
    logger.info(
        f"{image_path.name} is {img_w}x{img_h} (long edge {img_long_edge}px) — "
        f"downscaling detection to {dynamic_scale} (cap {cap_long_edge}px, factor {scale_factor:.2f})"
    )
    return dynamic_scale


async def _process_one_image(submission_id: str, tenant_code: str, sub_type: str, i: int, url: Any) -> Dict[str, Any]:
    """
    Downloads, blurs, and uploads a single image. Runs concurrently with the
    submission's other images (see deface_blur_activity), bounded by
    PER_SUBMISSION_IMAGE_CONCURRENCY setting, instead of the previous one-at-a-time loop.
    """
    url_str = str(url).strip()
    if not (url_str.startswith("http://") or url_str.startswith("https://")):
        base_url = settings.MEDIA_BASE_URL
        if not base_url:
            raise ValueError("Relative image URL encountered but MEDIA_BASE_URL is not configured.")
        resolved_url = urllib.parse.urljoin(base_url.rstrip("/") + "/", url_str.lstrip("/"))
        logger.info(f"Reconstructed absolute URL for download: {resolved_url} (from relative path: {url_str})")
    else:
        resolved_url = url_str


    # Split URL into components (scheme, netloc, path, query, fragment) so that
    # unsafe characters like '#', '?', spaces, etc. inside the path are safely
    # quoted without losing the path structure or corrupting query/fragment delimiters.
    parsed = urllib.parse.urlsplit(resolved_url)
    quoted_path = urllib.parse.quote(parsed.path, safe="/:-._~%!$&'()*+,;=@")
    quoted_query = urllib.parse.quote(parsed.query, safe="=&-._~%!$'()*+,;:@/?") if parsed.query else ""
    quoted_fragment = urllib.parse.quote(parsed.fragment, safe="-._~%!$&'()*+,;=@/?") if parsed.fragment else ""
    resolved_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, quoted_path, quoted_query, quoted_fragment))

    if not _is_allowed_media_host(resolved_url):
        raise ValueError(
            f"Refusing to download image from disallowed host: {resolved_url!r} "
            f"(only MEDIA_BASE_URL's host, {settings.MEDIA_BASE_URL!r}, is permitted)"
        )

    parsed_path = urllib.parse.urlparse(resolved_url).path
    # Unquote path so GCP blob key uses real characters (e.g. spaces)
    unquoted_path = urllib.parse.unquote(parsed_path)
    parts = [p for p in unquoted_path.split("/") if p]
    if len(parts) >= 2:
        actual_name = f"{parts[-2]}/{parts[-1]}"
    else:
        actual_name = parts[-1] if parts else f"{submission_id}_{i}.jpg"

    ext = os.path.splitext(unquoted_path)[1]
    if not ext:
        ext = ".jpg"
    filename = f"{submission_id}_{tenant_code}_{i}{ext}"

    local_path = DOWNLOADS_DIR / filename
    output_path = OUTPUTS_DIR / f"blurred_{filename}"

    try:
        # 1. Download file locally
        await _run_in_image_executor(_download_file, resolved_url, filename)

        # 2. Deface/Blur image — gated globally (see BLUR_CONCURRENCY_LIMIT setting),
        # unlike the download/upload legs, since this is the memory-heavy step.
        # Dynamic scale: DEFACE_SCALE is a *ceiling*, not a fixed override. Images
        # smaller than the cap run at native resolution (better accuracy); images
        # larger than the cap are proportionally downscaled to fit within it (saves
        # CPU). This prevents both over-downscaling small images (which makes faces
        # too tiny to detect) and under-downscaling huge phone photos (which wastes
        # CPU on pixels the model can't use anyway).
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        deface_scale = _compute_dynamic_scale(local_path, settings.DEFACE_SCALE or None)
        deface_threshold = settings.DEFACE_THRESHOLD if settings.DEFACE_THRESHOLD is not None else 0.2
        async with _get_blur_semaphore():
            await _run_in_image_executor(
                anonymize_face,
                input_path=str(local_path),
                output_path=str(output_path),
                scale=deface_scale,
                threshold=deface_threshold,
            )

        # 3. Upload to Object Storage
        if "story" in sub_type:
            blob_prefix = settings.STORAGE_STORY_PREFIX
        else:
            blob_prefix = settings.STORAGE_DISCUSSION_PREFIX

        blob_name = f"{blob_prefix}/{actual_name}"
        content_type = mimetypes.guess_type(blob_name)[0] or "image/jpeg"

        storage = get_object_storage()
        stored_obj = await _run_in_image_executor(
            storage.upload_file,
            local_file_path=str(output_path),
            object_key=blob_name,
            content_type=content_type,
            access_mode=AccessMode.PUBLIC
        )
        public_url = resolve_url(stored_obj, storage)

        return {"relative_url": parsed_path, "public_url": public_url}

    except urllib.error.URLError as e:
        logger.warning(f"Failed to download image {resolved_url} (ignoring): {e}")
        return None
    except Exception as e:
        logger.error(f"Failed face blurring for {resolved_url}: {e}")
        raise
    finally:
        # Clean up local temporary files under all conditions (prevent disk leakage)
        if local_path.exists():
            try:
                local_path.unlink()
            except Exception as clean_err:
                logger.warning(f"Failed to delete temp file {local_path}: {clean_err}")
        if output_path.exists():
            try:
                output_path.unlink()
            except Exception as clean_err:
                logger.warning(f"Failed to delete temp file {output_path}: {clean_err}")


@activity.defn
async def deface_blur_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that downloads and runs local OpenCV/ONNX face blurring on ingestion images,
    then uploads the result to GCP Storage. A submission's images process concurrently with each
    other (bounded by PER_SUBMISSION_IMAGE_CONCURRENCY setting) instead of one at a time — load-tested,
    the sequential version made image_blur ~66% of a submission's total processing time.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]

    # Only hold a DB connection for the brief read/write on either side of the
    # actual work — never across the download/blur/upload work below. That work
    # is network- and CPU-bound (can run for minutes); holding a pool
    # connection idle for that whole time is what let a handful of concurrent
    # submissions pin most of the pool, starving every other activity that
    # needs a quick connection.
    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)

    image_urls = payload.get("image_urls")
    if not image_urls:
        return {"status": "skipped", "reason": "no image urls available"}

    semaphore = asyncio.Semaphore(settings.PER_SUBMISSION_IMAGE_CONCURRENCY)

    async def _bounded(i: int, url: Any) -> Dict[str, Any]:
        async with semaphore:
            return await _process_one_image(submission_id, tenant_code, sub_type, i, url)

    # gather() preserves input order in its results regardless of completion
    # order, so blurred_local_paths/relative_original_urls below stay aligned
    # with the original image_urls order exactly as the old sequential loop did.
    results = await asyncio.gather(
        *[_bounded(i, url) for i, url in enumerate(image_urls)],
        return_exceptions=True,
    )
    for r in results:
        # BaseException, not Exception — return_exceptions=True also captures
        # asyncio.CancelledError, which derives from BaseException. Temporal
        # cancels activity coroutines on cancellation requests and heartbeat
        # timeouts, so this path is reachable; missing it here would let a
        # CancelledError reach the dict-indexing below and raise a confusing
        # TypeError instead of properly propagating the cancellation.
        if isinstance(r, BaseException):
            raise r

    valid_results = [r for r in results if r is not None]
    blurred_local_paths = [r["public_url"] for r in valid_results]
    relative_original_urls = [r["relative_url"] for r in valid_results]

    # Save output paths back to DB — acquire fresh here rather than reusing a
    # connection held since the top, since the work above may have taken
    # minutes and the earlier connection would have sat idle that whole time.
    if blurred_local_paths or relative_original_urls:
        async with db.pool.acquire() as conn:
            if sub_type == "story":
                await conn.execute(
                    "UPDATE story_submissions SET blur_image_urls = $3, image_urls = $4, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths, relative_original_urls
                )
            else:
                await conn.execute(
                    "UPDATE discussion_submissions SET blur_image_urls = $3, image_urls = $4, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths, relative_original_urls
                )

    return {"status": "success", "blur_paths": blurred_local_paths}