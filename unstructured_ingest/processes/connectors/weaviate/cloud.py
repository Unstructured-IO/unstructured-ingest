from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.processes.connectors.weaviate.weaviate import (
    WeaviateAccessConfig,
    WeaviateConnectionConfig,
    WeaviateUploader,
    WeaviateUploaderConfig,
    WeaviateUploadStager,
    WeaviateUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from weaviate.auth import AuthCredentials
    from weaviate.client import WeaviateClient

CONNECTOR_TYPE = "weaviate-cloud"


class CloudWeaviateAccessConfig(WeaviateAccessConfig):
    access_token: Optional[str] = Field(
        default=None, description="Used to create the bearer token."
    )
    api_key: Optional[str] = None
    client_secret: Optional[str] = None
    password: Optional[str] = None


class CloudWeaviateConnectionConfig(WeaviateConnectionConfig):
    cluster_url: str = Field(
        description="The WCD cluster URL or hostname to connect to. "
        "Usually in the form: rAnD0mD1g1t5.something.weaviate.cloud"
    )
    username: Optional[str] = None
    anonymous: bool = Field(default=False, description="if set, all auth values will be ignored")
    refresh_token: Optional[str] = Field(
        default=None,
        description="Will tie this value to the bearer token. If not provided, "
        "the authentication will expire once the lifetime of the access token is up.",
    )
    access_config: Secret[CloudWeaviateAccessConfig]

    def model_post_init(self, __context: Any) -> None:
        if self.anonymous:
            return
        access_config = self.access_config.get_secret_value()
        auths = {
            "api_key": access_config.api_key is not None,
            "bearer_token": access_config.access_token is not None,
            "client_secret": access_config.client_secret is not None,
            "client_password": access_config.password is not None and self.username is not None,
        }
        existing_auths = [auth_method for auth_method, flag in auths.items() if flag]

        if len(existing_auths) == 0:
            raise ValueError("No auth values provided and anonymous is False")
        if len(existing_auths) > 1:
            raise ValueError(
                "Multiple auth values provided, only one approach can be used: {}".format(
                    ", ".join(existing_auths)
                )
            )

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_api_key_auth(self) -> Optional["AuthCredentials"]:
        from weaviate.classes.init import Auth

        if api_key := self.access_config.get_secret_value().api_key:
            return Auth.api_key(api_key=api_key)
        return None

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_bearer_token_auth(self) -> Optional["AuthCredentials"]:
        from weaviate.classes.init import Auth

        if access_token := self.access_config.get_secret_value().access_token:
            return Auth.bearer_token(access_token=access_token, refresh_token=self.refresh_token)
        return None

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_client_secret_auth(self) -> Optional["AuthCredentials"]:
        from weaviate.classes.init import Auth

        if client_secret := self.access_config.get_secret_value().client_secret:
            return Auth.client_credentials(client_secret=client_secret)
        return None

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_client_password_auth(self) -> Optional["AuthCredentials"]:
        from weaviate.classes.init import Auth

        if (username := self.username) and (
            password := self.access_config.get_secret_value().password
        ):
            return Auth.client_password(username=username, password=password)
        return None

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_auth(self) -> "AuthCredentials":
        auths = [
            self.get_api_key_auth(),
            self.get_client_secret_auth(),
            self.get_bearer_token_auth(),
            self.get_client_password_auth(),
        ]
        auths = [auth for auth in auths if auth]
        if len(auths) == 0:
            raise ValueError("No auth values provided and anonymous is False")
        if len(auths) > 1:
            raise ValueError("Multiple auth values provided, only one approach can be used")
        return auths[0]

    @contextmanager
    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_client(self) -> Generator["WeaviateClient", None, None]:
        from weaviate import connect_to_weaviate_cloud
        from weaviate.classes.init import AdditionalConfig

        auth_credentials = None if self.anonymous else self.get_auth()
        with connect_to_weaviate_cloud(
            cluster_url=self.cluster_url,
            auth_credentials=auth_credentials,
            additional_config=AdditionalConfig(timeout=self.get_timeout()),
        ) as weaviate_client:
            yield weaviate_client


class CloudWeaviateUploadStagerConfig(WeaviateUploadStagerConfig):
    pass


@dataclass
class CloudWeaviateUploadStager(WeaviateUploadStager):
    upload_stager_config: CloudWeaviateUploadStagerConfig = field(
        default_factory=lambda: WeaviateUploadStagerConfig()
    )


class CloudWeaviateUploaderConfig(WeaviateUploaderConfig):
    pass


@dataclass
class CloudWeaviateUploader(WeaviateUploader):
    connection_config: CloudWeaviateConnectionConfig = field(
        default_factory=lambda: CloudWeaviateConnectionConfig()
    )
    upload_config: CloudWeaviateUploaderConfig = field(
        default_factory=lambda: CloudWeaviateUploaderConfig()
    )
    connector_type: str = CONNECTOR_TYPE


weaviate_cloud_destination_entry = DestinationRegistryEntry(
    connection_config=CloudWeaviateConnectionConfig,
    uploader=CloudWeaviateUploader,
    uploader_config=CloudWeaviateUploaderConfig,
    upload_stager=CloudWeaviateUploadStager,
    upload_stager_config=CloudWeaviateUploadStagerConfig,
)
