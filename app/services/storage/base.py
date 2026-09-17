from typing import Protocol
from .models import StoredObject, AccessMode


class ObjectStorage(Protocol):
    """
    Provider-neutral storage interface.

    Every operation that touches a bucket accepts an ``access_mode`` so the
    adapter can route the call to the correct bucket:

      AccessMode.PUBLIC  → public bucket  (blurred images; publicly readable)
      AccessMode.PRIVATE → private bucket (internal CSVs; not publicly readable)

    Call sites MUST always pass ``access_mode`` explicitly — never rely on the
    default — so the bucket routing is obvious during code review.
    """

    def upload_file(
        self,
        local_file_path: str,
        object_key:      str,
        content_type:    str | None = None,
        access_mode:     AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject: ...

    def upload_bytes(
        self,
        data:         bytes,
        object_key:   str,
        content_type: str | None = None,
        access_mode:  AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject: ...

    def download_bytes(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes: ...

    def delete_object(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> None: ...

    def generate_access_url(
        self,
        object_key:         str,
        expires_in_seconds: int,
        access_mode:        AccessMode = AccessMode.PRIVATE,
    ) -> str: ...


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def build_public_url(stored_object: StoredObject) -> str:
    """Return the canonical public HTTP URL for a public-bucket object."""
    if stored_object.provider == "aws":
        return f"https://{stored_object.bucket}.s3.amazonaws.com/{stored_object.key}"
    elif stored_object.provider == "gcp":
        return f"https://storage.googleapis.com/{stored_object.bucket}/{stored_object.key}"
    elif stored_object.provider == "azure":
        return f"https://azure.blob.core.windows.net/{stored_object.bucket}/{stored_object.key}"
    elif stored_object.provider == "oci":
        return f"https://oci.objectstorage/{stored_object.bucket}/{stored_object.key}"
    return f"https://{stored_object.bucket}/{stored_object.key}"


def resolve_url(
    stored_object:      StoredObject,
    storage:            ObjectStorage,
    expires_in_seconds: int = 3600,
) -> str:
    """
    Return the best URL for accessing ``stored_object``.

    PUBLIC  → direct public URL (no expiry, no auth required).
    PRIVATE → pre-signed / signed URL valid for ``expires_in_seconds`` seconds.
    """
    if stored_object.access_mode == AccessMode.PUBLIC:
        return build_public_url(stored_object)
    return storage.generate_access_url(
        stored_object.key,
        expires_in_seconds = expires_in_seconds,
        access_mode        = AccessMode.PRIVATE,
    )
