import logging
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError
from botocore.client import Config

from .base import ObjectStorage
from .models import StoredObject, AccessMode
from .errors import StorageError, StorageNotFoundError, StoragePermissionError, StorageTransientError

logger = logging.getLogger("analytics_service.services.storage.aws_s3")


class AwsS3Storage(ObjectStorage):
    """
    AWS S3 adapter with dual-bucket routing.

    Uploads are routed by AccessMode:
      - AccessMode.PUBLIC  → public_bucket  (blurred images; bucket policy makes objects publicly readable)
      - AccessMode.PRIVATE → private_bucket (internal CSVs; no public bucket policy)

    NOTE: This adapter does NOT set object ACLs (no ACL param in put_object /
    upload_file). Public readability for the public bucket is enforced at the
    bucket-policy level by the infrastructure team, NOT per-object ACL.
    This is the AWS recommended approach and avoids "Block Public Access" conflicts.
    """

    def __init__(
        self,
        public_bucket:   str,
        private_bucket:  str,
        region:          str,
        endpoint_url:    str | None = None,
        connect_timeout: int = 10,
        read_timeout:    int = 60,
        max_retries:     int = 3,
    ):
        self.public_bucket  = public_bucket
        self.private_bucket = private_bucket

        config = Config(
            connect_timeout = connect_timeout,
            read_timeout    = read_timeout,
            retries         = {"max_attempts": max_retries},
            signature_version = "s3v4",
        )

        self.s3_client = boto3.client(
            "s3",
            region_name  = region,
            endpoint_url = endpoint_url,
            config       = config,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bucket_for(self, access_mode: AccessMode) -> str:
        return self.public_bucket if access_mode == AccessMode.PUBLIC else self.private_bucket

    def _handle_error(self, e: Exception) -> None:
        if isinstance(e, ClientError):
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("NoSuchKey", "404"):
                raise StorageNotFoundError(str(e)) from e
            elif error_code == "AccessDenied":
                raise StoragePermissionError(str(e)) from e
            elif error_code == "NoSuchBucket":
                raise StorageError(str(e)) from e
            elif error_code in ("429", "500", "502", "503", "504", "TooManyRequestsException"):
                raise StorageTransientError(str(e)) from e
            raise StorageError(str(e)) from e
        elif isinstance(e, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
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
        bucket = self._bucket_for(access_mode)
        try:
            extra_args = {"ContentType": content_type or "application/octet-stream"}
            self.s3_client.upload_file(
                local_file_path,
                bucket,
                object_key,
                ExtraArgs=extra_args,
            )
            logger.info(
                "Uploaded file provider=aws bucket=%s key=%s access_mode=%s",
                bucket, object_key, access_mode.value,
            )
            return StoredObject(
                provider=    "aws",
                bucket=      bucket,
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
        bucket = self._bucket_for(access_mode)
        try:
            self.s3_client.put_object(
                Bucket=      bucket,
                Key=         object_key,
                Body=        data,
                ContentType= content_type or "application/octet-stream",
            )
            logger.info(
                "Uploaded bytes provider=aws bucket=%s key=%s access_mode=%s",
                bucket, object_key, access_mode.value,
            )
            return StoredObject(
                provider=    "aws",
                bucket=      bucket,
                key=         object_key,
                access_mode= access_mode,
                content_type=content_type,
            )
        except Exception as e:
            self._handle_error(e)

    # ------------------------------------------------------------------
    # Downloads — default to private bucket (CSVs).
    # ------------------------------------------------------------------

    def download_bytes(
        self,
        object_key:  str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes:
        bucket = self._bucket_for(access_mode)
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=object_key)
            return response["Body"].read()
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
        bucket = self._bucket_for(access_mode)
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=object_key)
        except Exception as e:
            self._handle_error(e)

    def generate_access_url(
        self,
        object_key:         str,
        expires_in_seconds: int,
        access_mode:        AccessMode = AccessMode.PRIVATE,
    ) -> str:
        """Generate a pre-signed URL. Useful only for private-bucket objects."""
        bucket = self._bucket_for(access_mode)
        try:
            return self.s3_client.generate_presigned_url(
                "get_object",
                Params=    {"Bucket": bucket, "Key": object_key},
                ExpiresIn= expires_in_seconds,
            )
        except Exception as e:
            self._handle_error(e)
