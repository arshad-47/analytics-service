import logging
import oci
import uuid
from datetime import datetime, timedelta, timezone
from oci.exceptions import ServiceError, RequestException, ConnectTimeout

from app.config import settings
from .base import ObjectStorage
from .models import StoredObject, AccessMode
from .errors import StorageError, StorageNotFoundError, StoragePermissionError, StorageTransientError

logger = logging.getLogger("analytics_service.services.storage.oci")

class OciStorage(ObjectStorage):
    """
    OCI Object Storage adapter with dual-bucket routing.

    Uploads are routed by AccessMode:
      - AccessMode.PUBLIC  → public_bucket (blurred images; bucket is publicly readable)
      - AccessMode.PRIVATE → private_bucket (internal CSVs; no public access)
    """

    def __init__(
        self,
        public_bucket: str,
        private_bucket: str,
        namespace: str | None = None,
        config_file: str | None = None,
        profile: str | None = None,
        region: str | None = None,
    ):
        self.public_bucket = public_bucket
        self.private_bucket = private_bucket
        self.namespace = namespace or settings.OCI_NAMESPACE
        self._pars: dict[tuple[str, str, int], tuple[str, datetime]] = {}

        config_file = config_file or settings.OCI_CONFIG_FILE
        profile = profile or settings.OCI_CONFIG_PROFILE
        region = region or settings.OCI_REGION

        try:
            # We assume config file authentication is used.
            self.config = oci.config.from_file(config_file, profile)
            if region:
                self.config["region"] = region
            self.client = oci.object_storage.ObjectStorageClient(self.config)
        except Exception as e:
            logger.warning(f"Failed to load OCI config from file: {e}. Attempting Instance Principals...")
            try:
                signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
                self.config = {}
                if region:
                    self.config["region"] = region
                self.client = oci.object_storage.ObjectStorageClient(self.config, signer=signer)
            except Exception as e2:
                logger.error(f"Failed to initialize OCI ObjectStorageClient: {e2}")
                raise StorageError(f"OCI initialization failed: {e2}") from e2

        self.region = self.config.get("region") or region or "us-ashburn-1"

    def _handle_error(self, e: Exception) -> None:
        if isinstance(e, ServiceError):
            if e.status == 404:
                raise StorageNotFoundError(str(e)) from e
            elif e.status in (401, 403):
                raise StoragePermissionError(str(e)) from e
            elif e.status in (429, 500, 502, 503, 504):
                raise StorageTransientError(str(e)) from e
            raise StorageError(str(e)) from e
        elif isinstance(e, (RequestException, ConnectTimeout)):
            raise StorageTransientError(str(e)) from e
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
            with open(local_file_path, "rb") as data:
                self.client.put_object(
                    self.namespace,
                    bucket_name,
                    object_key,
                    data,
                    content_type=content_type or "application/octet-stream"
                )

            logger.info(
                "Uploaded file provider=oci bucket=%s key=%s access_mode=%s",
                bucket_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "oci",
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
            self.client.put_object(
                self.namespace,
                bucket_name,
                object_key,
                data,
                content_type=content_type or "application/octet-stream"
            )

            logger.info(
                "Uploaded bytes provider=oci bucket=%s key=%s access_mode=%s",
                bucket_name, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "oci",
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
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes:
        bucket_name = self._bucket_for(access_mode)
        try:
            response = self.client.get_object(self.namespace, bucket_name, object_key)
            return response.data.content
        except Exception as e:
            self._handle_error(e)

    # Delete / URL helpers
    def generate_public_url(self, object_key: str) -> str:
        """Return the object key for persistence; callers add the bucket when building URLs."""
        return f"/{object_key.lstrip('/')}"

    def delete_object(
        self,
        object_key: str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> None:
        bucket_name = self._bucket_for(access_mode)
        try:
            self.client.delete_object(self.namespace, bucket_name, object_key)
        except Exception as e:
            self._handle_error(e)

    def generate_access_url(
        self,
        object_key: str,
        expires_in_seconds: int,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> str:
        """Generate a pre-authenticated request (PAR) for private objects."""
        bucket_name = self._bucket_for(access_mode)
        cache_key = (bucket_name, object_key, expires_in_seconds)
        now = datetime.now(timezone.utc)
        cached_par = self._pars.get(cache_key)
        endpoint = f"https://objectstorage.{self.region}.oraclecloud.com"
        if cached_par and cached_par[1] > now:
            return f"{endpoint}{cached_par[0]}"

        try:
            par_details = oci.object_storage.models.CreatePreauthenticatedRequestDetails(
                name = f"par_{uuid.uuid4().hex[:16]}",
                object_name = object_key,
                access_type = "ObjectRead",
                time_expires = now + timedelta(seconds=expires_in_seconds)
            )
            
            response = self.client.create_preauthenticated_request(
                self.namespace,
                bucket_name,
                par_details
            )
            
            full_path = response.data.full_path
            self._pars[cache_key] = (full_path, par_details.time_expires)
            return f"{endpoint}{full_path}"
        except Exception as e:
            self._handle_error(e)
