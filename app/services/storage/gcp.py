import logging
from datetime import timedelta
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError, NotFound, Forbidden, TooManyRequests, ServiceUnavailable

from app.config import settings
from .base import ObjectStorage
from .models import StoredObject, AccessMode
from .errors import StorageError, StorageNotFoundError, StoragePermissionError, StorageTransientError

logger = logging.getLogger("analytics_service.services.storage.gcp")


class GcpStorage(ObjectStorage):
    """
    GCS adapter with dual-bucket routing.

    Uploads are routed by AccessMode:
      - AccessMode.PUBLIC  → STORAGE_PUBLIC_BUCKET  (blurred images, publicly readable)
      - AccessMode.PRIVATE → STORAGE_PRIVATE_BUCKET (internal CSVs, no public access)

    Downloads always target the private bucket (CSVs are the only objects we
    download programmatically). If you need to download from the public bucket,
    pass access_mode=AccessMode.PUBLIC to download_bytes.
    """

    def __init__(self, public_bucket: str, private_bucket: str):
        self.public_bucket  = public_bucket
        self.private_bucket = private_bucket
        self.project_id     = settings.PROJECT_ID
        
        # Initialize credentials and client once to avoid recreating them on every operation
        cred_dict = {
            "type": settings.TYPE,
            "project_id": settings.PROJECT_ID,
            "private_key_id": settings.PRIVATE_KEY_ID,
            "private_key": settings.PRIVATE_KEY.replace('\\n', '\n'),
            "client_email": settings.CLIENT_EMAIL,
            "client_id": settings.CLIENT_ID,
            "auth_uri": settings.AUTH_URI,
            "token_uri": settings.TOKEN_URI,
            "auth_provider_x509_cert_url": settings.AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": settings.CLIENT_X509_CERT_URL,
            "universe_domain": settings.UNIVERSE_DOMAIN,
        }
        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        self._client = storage.Client(credentials=credentials, project=self.project_id)

    def _handle_error(self, e: Exception) -> None:
        if isinstance(e, NotFound):
            raise StorageNotFoundError(str(e)) from e
        elif isinstance(e, Forbidden):
            raise StoragePermissionError(str(e)) from e
        elif isinstance(e, (TooManyRequests, ServiceUnavailable)):
            raise StorageTransientError(str(e)) from e
        elif isinstance(e, GoogleAPIError):
            raise StorageError(str(e)) from e
        raise StorageError(str(e)) from e

    # Uploads
    def upload_file(
        self,
        local_file_path: str,
        object_key: str,
        content_type: str | None = None,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject:
        bucket_name = self._bucket_for(access_mode)
        try:
            blob   = self._client.bucket(bucket_name).blob(object_key)
            blob.upload_from_filename(local_file_path, content_type=content_type)

            logger.info(
                "Uploaded file provider=gcp bucket=%s key=%s access_mode=%s",
                bucket_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "gcp",
                bucket = bucket_name,
                key = object_key,
                access_mode = access_mode,
                content_type = content_type,
            )
        except Exception as e:
            self._handle_error(e)

    def upload_bytes(
        self,
        data: bytes,
        object_key: str,
        content_type: str | None = None,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject:
        bucket_name = self._bucket_for(access_mode)
        try:
            blob = self._client.bucket(bucket_name).blob(object_key)
            blob.upload_from_string(data, content_type=content_type)
            # Public access is managed via Uniform Bucket-Level Access on the bucket.

            logger.info(
                "Uploaded bytes provider=gcp bucket=%s key=%s access_mode=%s",
                bucket_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "gcp",
                bucket = bucket_name,
                key = object_key,
                access_mode = access_mode,
                content_type = content_type,
            )
        except Exception as e:
            self._handle_error(e)

    # Downloads
    def download_bytes(
        self,
        object_key: str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes:
        bucket_name = self._bucket_for(access_mode)
        try:
            return self._client.bucket(bucket_name).blob(object_key).download_as_bytes()
        except Exception as e:
            self._handle_error(e)

    # Delete / URL helpers — target the correct bucket from access_mode.
    def generate_public_url(self, object_key: str) -> str:
        """Return the object key for persistence; callers add the bucket when building URLs."""
        return f"/{object_key.lstrip('/')}"

    def delete_object(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> None:
        bucket_name = self._bucket_for(access_mode)
        try:
            self._client.bucket(bucket_name).blob(object_key).delete()
        except Exception as e:
            self._handle_error(e)

    def generate_access_url(
        self,
        object_key: str,
        expires_in_seconds: int,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> str:
        """Generate a V4 signed URL. Meaningful only for private-bucket objects."""
        bucket_name = self._bucket_for(access_mode)
        try:
            blob = self._client.bucket(bucket_name).blob(object_key)
            return blob.generate_signed_url(
                version = "v4",
                expiration = timedelta(seconds=expires_in_seconds),
                method = "GET",
            )
        except Exception as e:
            self._handle_error(e)
