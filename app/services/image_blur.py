import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("analytics_service.services.image_blur")


class DefaceError(RuntimeError):
    """Raised when the deface CLI exits non-zero. Carries the exit code for
    logging: 1 means any uncaught exception inside deface (bad input or a
    detector/runtime failure — the code can't tell which), negative means the
    process was killed by a signal (e.g. -9 on OOM)."""

    def __init__(self, returncode: int, stderr: str):
        self.returncode = returncode
        super().__init__(f"deface failed with exit code {returncode}: {stderr}")


class ImageNotBlurrableError(ValueError):
    """Raised when an image fails the pre-blur check (corrupt, truncated, not a
    real image, unsupported format, or can't be converted to RGB). Retrying
    the same file can't fix it, so callers skip the image instead of failing."""


# Formats deface can read and write back. MPO is the multi-picture JPEG many
# phone cameras produce; Pillow reports it separately but it is a normal JPEG.
BLURRABLE_FORMATS = {"JPEG", "MPO", "PNG", "WEBP"}


def _is_system_error(e: BaseException) -> bool:
    """OS-level failures (disk full, I/O error, ...) carry an errno; Pillow's
    own decode/encode errors don't. These are transient, so callers re-raise
    them for a Temporal retry instead of skipping the image."""
    return isinstance(e, OSError) and e.errno is not None


def prepare_image_for_blur(image_path: str) -> None:
    """
    Checks that the image can be blurred and converts it to RGB if needed.
    Raises ImageNotBlurrableError with the reason if it can't.

    The pixel data is fully decoded here (img.load()), so a truncated download
    or a corrupt file is caught before deface runs, not halfway through it.
    """
    from PIL import Image, UnidentifiedImageError

    path = Path(image_path)
    if not path.exists() or path.stat().st_size == 0:
        raise ImageNotBlurrableError(f"{path.name}: file is missing or empty")

    try:
        with Image.open(path) as img:
            if img.format not in BLURRABLE_FORMATS:
                raise ImageNotBlurrableError(
                    f"{path.name}: unsupported image format {img.format!r}"
                )
            width, height = img.size
            if width < 1 or height < 1:
                raise ImageNotBlurrableError(f"{path.name}: invalid size {width}x{height}")
            img.load()
    except ImageNotBlurrableError:
        raise
    except UnidentifiedImageError as e:
        raise ImageNotBlurrableError(f"{path.name}: not a readable image ({e})") from e
    except MemoryError as e:
        raise ImageNotBlurrableError(f"{path.name}: too large to decode in memory") from e
    except (OSError, SyntaxError, ValueError, Image.DecompressionBombError) as e:
        if _is_system_error(e):
            raise
        raise ImageNotBlurrableError(f"{path.name}: corrupt or truncated image ({e})") from e

    try:
        normalize_image_for_blur(str(path))
    except Exception as e:
        # MemoryError is skipped like a bad image; disk/I-O errors retry.
        if _is_system_error(e):
            raise
        raise ImageNotBlurrableError(f"{path.name}: could not convert to upright RGB ({e})") from e


def normalize_image_for_blur(image_path: str) -> bool:
    """
    Rewrites the image in place as upright, plain RGB. Returns True if the
    file was rewritten; an image that is already upright RGB is left untouched.

    Rotation: phone cameras store portrait photos sideways plus an EXIF
    orientation tag saying how to rotate them for display. deface ignores the
    tag (so it looks for faces lying sideways, which it detects less reliably)
    and drops all metadata from its output (so the blurred image is published
    sideways). Rotating the pixels upright first fixes both.

    Colour mode: deface reads images via imageio as a raw pixel array and
    writes the output by file extension. A CMYK JPEG comes back as a 4-channel
    array, which imageio then treats as RGBA on save and Pillow refuses to
    write as JPEG ("cannot write mode RGBA as JPEG"). RGBA/LA/P images saved
    under a .jpg name fail the same way.

    Both fixes share one re-save, so a JPEG is re-encoded at most once.
    """
    from PIL import Image, ImageOps

    path = Path(image_path)
    with Image.open(path) as img:
        original_mode, original_format = img.mode, img.format
        try:
            orientation = img.getexif().get(0x0112, 1)
        except Exception as e:
            # Malformed EXIF shouldn't cost us the image; blur it as stored.
            logger.warning(f"Could not read EXIF orientation of {path.name}, not rotating: {e}")
            orientation = 1
        # Valid tags are 1-8; 1 means already upright, anything else is ignored.
        rotate = orientation in range(2, 9)

        fixed = img
        if rotate:
            # exif_transpose returns an upright copy with the orientation tag removed.
            # The rewritten file carries no EXIF, so it can't be rotated a second time.
            try:
                fixed = ImageOps.exif_transpose(img)
            except Exception as e:
                logger.warning(f"Could not rotate {path.name} upright, blurring as stored: {e}")
                rotate = False

        if not rotate and original_mode == "RGB":
            return False

        if fixed.mode != "RGB":
            fixed = fixed.convert("RGB")
        fixed.load()

    # MPO (multi-picture JPEG from phone cameras) is written back as a plain JPEG.
    save_format = "JPEG" if original_format == "MPO" else original_format
    save_kwargs = {"quality": 95} if save_format in ("JPEG", "WEBP") else {}
    fixed.save(path, format=save_format, **save_kwargs)

    changes = []
    if rotate:
        changes.append(f"rotated upright (EXIF orientation {orientation})")
    if original_mode != "RGB":
        changes.append(f"converted {original_mode} to RGB")
    logger.info(f"Prepared {path.name} for face blurring: {', '.join(changes)}")
    return True


def _deface_command() -> list:
    deface_bin = shutil.which("deface")
    if deface_bin:
        return [deface_bin]
    # Fallback to python execution module
    return [sys.executable, "-m", "deface.deface"]

def anonymize_face(
    input_path: str,
    output_path: str,
    threshold: Optional[float] = None,
    scale: Optional[str] = None,
    mask_scale: Optional[float] = None,
    backend: Optional[str] = None,
    boxes: bool = False,
    draw_scores: bool = False,
    keep_metadata: bool = False
) -> Dict[str, Any]:
    """
    Executes face anonymization on an image or video path using the deface CLI.
    Runs locally as a generic Python helper.
    """
    in_path = Path(input_path).expanduser().resolve()
    out_path = Path(output_path).expanduser().resolve()

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    command = _deface_command()
    command.extend([
        str(in_path),
        "--output",
        str(out_path),
        "--replacewith",
        "blur",
    ])

    if threshold is not None:
        command.extend(["--thresh", str(threshold)])
    if scale:
        command.extend(["--scale", str(scale)])
    if mask_scale is not None:
        command.extend(["--mask-scale", str(mask_scale)])
    if backend:
        command.extend(["--backend", str(backend)])
    if boxes:
        command.append("--boxes")
    if draw_scores:
        command.append("--draw-scores")
    if keep_metadata:
        command.append("--keep-metadata")

    logger.info(f"Running deface command: {' '.join(command)}")
    
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(in_path.parent)
    )

    if result.returncode != 0:
        logger.error(f"deface failed with code {result.returncode}. STDERR: {result.stderr}")
        raise DefaceError(result.returncode, result.stderr)

    if not out_path.exists():
        raise RuntimeError(f"deface finished but output file was not created: {out_path}")

    logger.info(f"Successfully blurred faces. Output saved to {out_path}")
    return {
        "input_path": str(in_path),
        "output_path": str(out_path),
        "file_size_bytes": os.path.getsize(out_path),
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip()
    }
