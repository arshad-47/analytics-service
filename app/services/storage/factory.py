from app.config import settings
from .base import ObjectStorage
from .gcp import GcpStorage
from .aws_s3 import AwsS3Storage
from .azure import AzureStorage
from .oci import OciStorage

def validate_storage_config(provider: str, settings_obj) -> None:
    # Both buckets must always be configured — the code routes objects to the
    # correct bucket based on AccessMode, so both must exist at startup.
    assert settings_obj.STORAGE_PUBLIC_BUCKET,  "STORAGE_PUBLIC_BUCKET required"
    assert settings_obj.STORAGE_PRIVATE_BUCKET, "STORAGE_PRIVATE_BUCKET required"

    if provider == "aws":
        assert settings_obj.STORAGE_REGION, "STORAGE_REGION required for AWS"
    elif provider == "gcp":
        pass  # credentials come from the GCP service-account env vars
    elif provider == "oci":
        assert settings_obj.OCI_NAMESPACE, "OCI_NAMESPACE required for OCI"
    elif provider == "azure":
        assert (
            settings_obj.AZURE_STORAGE_ACCOUNT_NAME
            or settings_obj.AZURE_STORAGE_CONNECTION_STRING
        ), "AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING required"
    elif provider == "s3-compatible":
        assert settings_obj.STORAGE_ENDPOINT_URL, "STORAGE_ENDPOINT_URL required for s3-compatible"


def get_object_storage() -> ObjectStorage:
    provider = settings.STORAGE_PROVIDER.lower()
    validate_storage_config(provider, settings)

    if provider == "gcp":
        return GcpStorage(
            public_bucket  = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket = settings.STORAGE_PRIVATE_BUCKET,
        )

    if provider == "aws":
        return AwsS3Storage(
            public_bucket   = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket  = settings.STORAGE_PRIVATE_BUCKET,
            region          = settings.STORAGE_REGION,
            endpoint_url    = settings.STORAGE_ENDPOINT_URL or None,
            connect_timeout = settings.STORAGE_CONNECT_TIMEOUT_SECONDS,
            read_timeout    = settings.STORAGE_READ_TIMEOUT_SECONDS,
            max_retries     = settings.STORAGE_MAX_RETRIES,
        )

    if provider == "azure":
        return AzureStorage(
            public_bucket     = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket    = settings.STORAGE_PRIVATE_BUCKET,
            connection_string = settings.AZURE_STORAGE_CONNECTION_STRING,
            account_name      = settings.AZURE_STORAGE_ACCOUNT_NAME,
        )

    if provider == "oci":
        return OciStorage(
            public_bucket  = settings.STORAGE_PUBLIC_BUCKET,
            private_bucket = settings.STORAGE_PRIVATE_BUCKET,
            namespace      = settings.OCI_NAMESPACE,
            config_file    = settings.OCI_CONFIG_FILE,
            profile        = settings.OCI_CONFIG_PROFILE,
            region         = settings.OCI_REGION,
        )

    # S3-compatible can be wired in here following the same pattern.
    raise ValueError(f"Unsupported storage provider: {provider}")
