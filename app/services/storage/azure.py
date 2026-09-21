import logging
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ServiceRequestError

from app.config import settings
from .base import ObjectStorage
from .models import StoredObject, AccessMode
from .errors import StorageError, StorageNotFoundError, StoragePermissionError, StorageTransientError

logger = logging.getLogger("analytics_service.services.storage.azure")

class AzureStorage(ObjectStorage):
    """
    Azure Blob Storage adapter with dual-container routing.

    Uploads are routed by AccessMode:
      - AccessMode.PUBLIC  → public_container (blurred images; container is publicly readable)
      - AccessMode.PRIVATE → private_container (internal CSVs; no public access)
    """

    def __init__(self, public_bucket: str, private_bucket: str, connection_string: str = "", account_name: str = ""):
        self.public_bucket = public_bucket
        self.private_bucket = private_bucket
        
        if connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.account_name = self.blob_service_client.account_name
            self.account_key = self.blob_service_client.credential.account_key
        else:
            # Assuming Managed Identity / DefaultAzureCredential if only account_name is provided
            from azure.identity import DefaultAzureCredential
            account_url = f"https://{account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
            self.account_name = account_name
            self.account_key = None # DefaultAzureCredential doesn't expose account key

    def _container_for(self, access_mode: AccessMode) -> str:
        return self.public_bucket if access_mode == AccessMode.PUBLIC else self.private_bucket

    def _handle_error(self, e: Exception) -> None:
        if isinstance(e, ResourceNotFoundError):
            raise StorageNotFoundError(str(e)) from e
        elif isinstance(e, HttpResponseError):
            if e.status_code in (403, 401):
                raise StoragePermissionError(str(e)) from e
            elif e.status_code in (429, 500, 502, 503, 504):
                raise StorageTransientError(str(e)) from e
            raise StorageError(str(e)) from e
        elif isinstance(e, ServiceRequestError):
            raise StorageTransientError(str(e)) from e
        raise StorageError(str(e)) from e

    # ------------------------------------------------------------------
    # Uploads
    # ------------------------------------------------------------------

    def upload_file(
        self,
        local_file_path: str,
        object_key:      str,
        content_type:    str | None = None,
        access_mode:     AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject:
        container_name = self._container_for(access_mode)
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=object_key)
            with open(local_file_path, "rb") as data:
                # ContentSettings is needed for Content-Type
                from azure.storage.blob import ContentSettings
                content_settings = ContentSettings(content_type=content_type) if content_type else None
                blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
            
            logger.info(
                "Uploaded file provider=azure bucket=%s key=%s access_mode=%s",
                container_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider=    "azure",
                bucket=      container_name,
                key=         object_key,
                access_mode= access_mode,
                content_type=content_type,
            )
        except Exception as e:
            self._handle_error(e)

    def upload_bytes(
        self,
        data:         bytes,
        object_key:   str,
        content_type: str | None = None,
        access_mode:  AccessMode = AccessMode.PRIVATE,
    ) -> StoredObject:
        container_name = self._container_for(access_mode)
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=object_key)
            from azure.storage.blob import ContentSettings
            content_settings = ContentSettings(content_type=content_type) if content_type else None
            blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
            
            logger.info(
                "Uploaded bytes provider=azure bucket=%s key=%s access_mode=%s",
                container_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider=    "azure",
                bucket=      container_name,
                key=         object_key,
                access_mode= access_mode,
                content_type=content_type,
            )
        except Exception as e:
            self._handle_error(e)

    # ------------------------------------------------------------------
    # Downloads
    # ------------------------------------------------------------------

    def download_bytes(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes:
        container_name = self._container_for(access_mode)
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=object_key)
            return blob_client.download_blob().readall()
        except Exception as e:
            self._handle_error(e)

    # ------------------------------------------------------------------
    # Delete / URL helpers
    # ------------------------------------------------------------------

    def delete_object(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> None:
        container_name = self._container_for(access_mode)
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=object_key)
            blob_client.delete_blob()
        except Exception as e:
            self._handle_error(e)

    def generate_access_url(
        self,
        object_key:         str,
        expires_in_seconds: int,
        access_mode:        AccessMode = AccessMode.PRIVATE,
    ) -> str:
        """Generate a SAS URL for private objects."""
        container_name = self._container_for(access_mode)
        try:
            if not self.account_key:
                # If using managed identity, we need to generate a User Delegation SAS, which is more complex.
                # Since connection string is the primary method in settings, we assume account key is present.
                # For robustness, we can fallback to just blob url without SAS if key is missing
                # (though this would fail for private containers if anonymous access is off).
                logger.warning("Account key not found, cannot generate SAS URL. Returning base blob URL.")
                blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=object_key)
                return blob_client.url

            sas_token = generate_blob_sas(
                account_name=self.account_name,
                container_name=container_name,
                blob_name=object_key,
                account_key=self.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)
            )
            return f"https://{self.account_name}.blob.core.windows.net/{container_name}/{object_key}?{sas_token}"
        except Exception as e:
            self._handle_error(e)
