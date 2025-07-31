import contextlib
import requests
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret, validator

from unstructured_ingest.data_types.file_data import (
    FileDataSourceMetadata,
)
from unstructured_ingest.errors_v2 import ProviderError, UserAuthError, UserError
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecDownloader,
    FsspecDownloaderConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    boto3 = None
    ClientError = None
    NoCredentialsError = None

CONNECTOR_TYPE = "s3"

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines-avoid-characters
CHARACTERS_TO_AVOID = ["\\", "{", "^", "}", "%", "`", "]", '"', ">", "[", "~", "<", "#", "|"]

if TYPE_CHECKING:
    from s3fs import S3FileSystem


class S3IndexerConfig(FsspecIndexerConfig):
    pass


class S3AccessConfig(FsspecAccessConfig):
    key: Optional[str] = Field(
        default=None,
        description="If not anonymous, use this access key ID, if specified. Takes precedence "
        "over `aws_access_key_id` in client_kwargs.",
    )
    secret: Optional[str] = Field(
        default=None, description="If not anonymous, use this secret access key, if specified."
    )
    token: Optional[str] = Field(
        default=None, description="If not anonymous, use this security token, if specified."
    )
    # New ambient credentials fields
    use_ambient_credentials: bool = Field(
        default=False, 
        description="Use ambient AWS credentials (environment variables, profiles, IAM roles)"
    )
    presigned_url: Optional[str] = Field(
        default=None, 
        description="Presigned URL with list permissions to validate bucket access (required when using ambient credentials)"
    )
    role_arn: Optional[str] = Field(
        default=None, 
        description="ARN of the IAM role to assume for S3 operations (required when using ambient credentials)"
    )
    
    @validator('presigned_url', always=True)
    def validate_presigned_url(cls, v, values):
        if values.get('use_ambient_credentials') and not v:
            raise ValueError("presigned_url is required when use_ambient_credentials is True")
        if v and not v.startswith(('http://', 'https://')):
            raise ValueError("presigned_url must be a valid HTTP/HTTPS URL")
        return v
    
    @validator('role_arn', always=True)
    def validate_role_arn(cls, v, values):
        if values.get('use_ambient_credentials') and not v:
            raise ValueError("role_arn is required when use_ambient_credentials is True")
        if v and not v.startswith('arn:aws:iam::'):
            raise ValueError("role_arn must be a valid AWS IAM role ARN")
        return v


class S3ConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["s3", "s3a"], init=False)
    access_config: Secret[S3AccessConfig] = Field(default=S3AccessConfig(), validate_default=True)
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Use this endpoint_url, if specified. Needed for "
        "connecting to non-AWS S3 buckets.",
    )
    anonymous: bool = Field(
        default=False, description="Connect to s3 without local AWS credentials."
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    
    def _validate_presigned_url(self, presigned_url: str) -> bool:
        """
        Validate that the presigned URL works by making a HEAD request.
        Returns True if the URL is valid and accessible.
        """
        try:
            response = requests.head(presigned_url, timeout=30)
            # Accept 200 (OK) or 403 (Forbidden but exists)
            # 403 can happen if the presigned URL allows list but not head operation
            return response.status_code in [200, 403]
        except requests.RequestException as e:
            logger.error(f"Failed to validate presigned URL: {e}")
            return False
    
    def _assume_role_and_get_credentials(self, role_arn: str) -> dict[str, Any]:
        """
        Use ambient AWS credentials to assume the specified role and return temporary credentials.
        """
        try:
            if not boto3:
                raise ImportError("boto3 is required for ambient credentials support")
            
            # Create STS client using ambient credentials
            sts_client = boto3.client('sts')
            
            # Assume the role
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"unstructured-ingest-{int(time())}",
                DurationSeconds=3600  # 1 hour
            )
            
            credentials = response['Credentials']
            return {
                'key': credentials['AccessKeyId'],
                'secret': credentials['SecretAccessKey'],
                'token': credentials['SessionToken']
            }
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Failed to assume role {role_arn}: {e}")
            raise UserAuthError(f"Failed to assume role: {e}")
        except Exception as e:
            logger.error(f"Unexpected error assuming role {role_arn}: {e}")
            raise ProviderError(f"Role assumption failed: {e}")

    def get_access_config(self) -> dict[str, Any]:
        access_config = self.access_config.get_secret_value()
        
        # Handle ambient credentials flow
        if access_config.use_ambient_credentials:
            # Validate presigned URL first
            if not self._validate_presigned_url(access_config.presigned_url):
                raise UserAuthError("Invalid or inaccessible presigned URL provided")
            
            # Assume role and get temporary credentials
            temp_credentials = self._assume_role_and_get_credentials(access_config.role_arn)
            
            access_configs: dict[str, Any] = {"anon": False}
            if self.endpoint_url:
                access_configs["endpoint_url"] = self.endpoint_url
            
            # Use temporary credentials from assumed role
            access_configs.update(temp_credentials)
            return access_configs
        
        # Handle traditional flow (explicit credentials or anonymous)
        access_configs: dict[str, Any] = {"anon": self.anonymous}
        if self.endpoint_url:
            access_configs["endpoint_url"] = self.endpoint_url

        # Avoid injecting None by filtering out k,v pairs where the value is None
        # Exclude the new ambient credential fields from being passed to s3fs
        excluded_fields = {'use_ambient_credentials', 'presigned_url', 'role_arn'}
        access_configs.update(
            {k: v for k, v in access_config.model_dump().items() 
             if v is not None and k not in excluded_fields}
        )
        return access_configs

    @requires_dependencies(["s3fs", "fsspec"], extras="s3")
    @contextmanager
    def get_client(self, protocol: str) -> Generator["S3FileSystem", None, None]:
        access_config = self.access_config.get_secret_value()
        
        # Additional dependency check for ambient credentials
        if access_config.use_ambient_credentials and not boto3:
            raise ImportError("boto3 is required for ambient credentials support. Install with 'pip install boto3'")
        
        with super().get_client(protocol=protocol) as client:
            yield client

    def wrap_error(self, e: Exception) -> Exception:
        # s3fs maps botocore errors into python ones using mapping here:
        # https://github.com/fsspec/s3fs/blob/main/s3fs/errors.py
        if isinstance(e, PermissionError):
            return UserAuthError(e)
        if isinstance(e, FileNotFoundError):
            return UserError(f"File not found: {e}")
        if cause := getattr(e, "__cause__", None):
            error_response = cause.response
            error_meta = error_response["ResponseMetadata"]
            http_code = error_meta["HTTPStatusCode"]
            message = error_response["Error"].get("Message", str(e))
            if 400 <= http_code < 500:
                return UserError(message)
            if http_code >= 500:
                return ProviderError(message)
        logger.error(
            "Unhandled exception from S3 (type: %s, endpoint: %s): %s",
            type(e).__name__,
            self.endpoint_url or "default",
            e,
            exc_info=True,
        )
        return e


@dataclass
class S3Indexer(FsspecIndexer):
    connection_config: S3ConnectionConfig
    index_config: S3IndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def wrap_error(self, e: Exception) -> Exception:
        return self.connection_config.wrap_error(e=e)

    def get_path(self, file_info: dict) -> str:
        return file_info["Key"]

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["Key"]

        self.log_debug("Getting metadata for S3 object", context={"file_path": path})
        self.log_file_operation("Getting metadata", file_path=path)

        date_created = None
        date_modified = None
        modified = file_info.get("LastModified")
        if modified:
            date_created = str(modified.timestamp())
            date_modified = str(modified.timestamp())

        file_size = file_info.get("size") if "size" in file_info else None
        file_size = file_size or file_info.get("Size")

        version = file_info.get("ETag").rstrip('"').lstrip('"') if "ETag" in file_info else None
        metadata: dict[str, str] = {}
        with (
            contextlib.suppress(AttributeError),
            self.connection_config.get_client(protocol=self.index_config.protocol) as client,
        ):
            metadata = client.metadata(path=path)
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
        }
        if metadata:
            record_locator["metadata"] = metadata
        issue_characters = [char for char in CHARACTERS_TO_AVOID if char in path]
        if issue_characters:
            self.log_warning(
                f"File path contains characters that can cause issues with S3: {issue_characters}",
                context={"path": path, "problematic_characters": issue_characters},
            )
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            version=version,
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )


class S3DownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class S3Downloader(FsspecDownloader):
    protocol: str = "s3"
    connection_config: S3ConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[S3DownloaderConfig] = field(default_factory=S3DownloaderConfig)


class S3UploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class S3Uploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: S3ConnectionConfig
    upload_config: S3UploaderConfig = field(default=None)


s3_source_entry = SourceRegistryEntry(
    indexer=S3Indexer,
    indexer_config=S3IndexerConfig,
    downloader=S3Downloader,
    downloader_config=S3DownloaderConfig,
    connection_config=S3ConnectionConfig,
)

s3_destination_entry = DestinationRegistryEntry(
    uploader=S3Uploader,
    uploader_config=S3UploaderConfig,
    connection_config=S3ConnectionConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
