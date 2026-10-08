import logging
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError
from botocore.client import Config

from app.config import settings

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
    """

    def __init__(
        self,
        public_bucket: str,
        private_bucket: str,
        region: str | None = None,
        connect_timeout: int | None = None,
        read_timeout: int | None = None,
        max_retries: int | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
    ):
        self.public_bucket  = public_bucket
        self.private_bucket = private_bucket

        region = region or settings.STORAGE_REGION
        connect_timeout = settings.STORAGE_CONNECT_TIMEOUT_SECONDS if connect_timeout is None else connect_timeout
        read_timeout = settings.STORAGE_READ_TIMEOUT_SECONDS if read_timeout is None else read_timeout
        max_retries = settings.STORAGE_MAX_RETRIES if max_retries is None else max_retries
        aws_access_key_id = settings.AWS_ACCESS_KEY_ID if aws_access_key_id is None else aws_access_key_id
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY if aws_secret_access_key is None else aws_secret_access_key
        aws_session_token = settings.AWS_SESSION_TOKEN if aws_session_token is None else aws_session_token

        config = Config(
            connect_timeout = connect_timeout,
            read_timeout = read_timeout,
            retries = {"max_attempts": max_retries},
            signature_version = "s3v4",
        )

        client_kwargs = {
            "region_name": region,
            "config": config,
        }
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            client_kwargs["aws_session_token"] = aws_session_token

        self.s3_client = boto3.client("s3", **client_kwargs)

    # Helpers

    def _handle_error(self, e: Exception) -> None:
        if isinstance(e, ClientError):
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("NoSuchKey", "404", "NoSuchBucket"):
                raise StorageNotFoundError(str(e)) from e
            elif error_code == "AccessDenied":
                raise StoragePermissionError(str(e)) from e
            elif error_code in ("429", "500", "502", "503", "504", "TooManyRequestsException"):
                raise StorageTransientError(str(e)) from e
            raise StorageError(str(e)) from e
        elif isinstance(e, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
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
        bucket = self._bucket_for(access_mode)
        try:
            extra_args = {"ContentType": content_type or "application/octet-stream"}
            self.s3_client.upload_file(
                local_file_path,
                bucket,
                object_key,
                ExtraArgs = extra_args,
            )
            logger.info(
                "Uploaded file provider = aws bucket=%s key=%s access_mode=%s",
                bucket, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "aws",
                bucket = bucket,
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
        bucket = self._bucket_for(access_mode)
        try:
            self.s3_client.put_object(
                Bucket = bucket,
                Key = object_key,
                Body = data,
                ContentType = content_type or "application/octet-stream",
            )
            logger.info(
                "Uploaded bytes provider = aws bucket=%s key=%s access_mode=%s",
                bucket, object_key, access_mode.value,
            )
            return StoredObject(
                provider = "aws",
                bucket = bucket,
                key = object_key,
                access_mode = access_mode,
                content_type = content_type,
            )
        except Exception as e:
            self._handle_error(e)

    # Downloads — default to private bucket (CSVs).
    def download_bytes(
        self,
        object_key: str,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> bytes:
        bucket = self._bucket_for(access_mode)
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=object_key)
            return response["Body"].read()
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
        bucket = self._bucket_for(access_mode)
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=object_key)
        except Exception as e:
            self._handle_error(e)

    def generate_access_url(
        self,
        object_key: str,
        expires_in_seconds: int,
        access_mode: AccessMode = AccessMode.PRIVATE,
    ) -> str:
        """Generate a pre-signed URL. Useful only for private-bucket objects."""
        bucket = self._bucket_for(access_mode)
        try:
            return self.s3_client.generate_presigned_url(
                "get_object",
                Params ={"Bucket": bucket, "Key": object_key},
                ExpiresIn = expires_in_seconds,
            )
        except Exception as e:
            self._handle_error(e)
