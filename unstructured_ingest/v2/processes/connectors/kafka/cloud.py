import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, Secret, SecretStr

from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.kafka.kafka import (
    KafkaAccessConfig,
    KafkaConnectionConfig,
    KafkaDownloader,
    KafkaDownloaderConfig,
    KafkaIndexer,
    KafkaIndexerConfig,
    KafkaUploader,
    KafkaUploaderConfig,
)

if TYPE_CHECKING:
    pass

CONNECTOR_TYPE = "kafka-cloud"


class CloudKafkaAccessConfig(KafkaAccessConfig):
    kafka_api_key: SecretStr = Field(
        description="Kafka API key to connect at the server", default=None
    )
    secret: SecretStr = Field(description="", default=None)


class CloudKafkaConnectionConfig(KafkaConnectionConfig):
    access_config: Secret[CloudKafkaAccessConfig]

    def get_consumer_configuration(self) -> dict:
        bootstrap = self.bootstrap_server
        port = self.port
        access_config = self.access_config.get_secret_value()

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "client.id": socket.gethostname(),
            "group.id": self.group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
            "sasl.username": access_config.kafka_api_key.get_secret_value(),
            "sasl.password": access_config.secret.get_secret_value(),
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_SSL",
            "logger": logger,
        }

        return conf

    def get_producer_configuration(self) -> dict:
        bootstrap = self.bootstrap_server
        port = self.port
        access_config = self.access_config.get_secret_value()

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "sasl.username": access_config.kafka_api_key.get_secret_value(),
            "sasl.password": access_config.secret.get_secret_value(),
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_SSL",
            "logger": logger,
        }

        return conf


class CloudKafkaIndexerConfig(KafkaIndexerConfig):
    pass


@dataclass
class CloudKafkaIndexer(KafkaIndexer):
    connection_config: CloudKafkaConnectionConfig
    index_config: CloudKafkaIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class CloudKafkaDownloaderConfig(KafkaDownloaderConfig):
    pass


@dataclass
class CloudKafkaDownloader(KafkaDownloader):
    connection_config: CloudKafkaConnectionConfig
    download_config: CloudKafkaDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


class CloudKafkaUploaderConfig(KafkaUploaderConfig):
    pass


@dataclass
class CloudKafkaUploader(KafkaUploader):
    connection_config: CloudKafkaConnectionConfig
    upload_config: CloudKafkaUploaderConfig
    connector_type: str = CONNECTOR_TYPE


kafka_cloud_source_entry = SourceRegistryEntry(
    connection_config=CloudKafkaConnectionConfig,
    indexer=CloudKafkaIndexer,
    indexer_config=CloudKafkaIndexerConfig,
    downloader=CloudKafkaDownloader,
    downloader_config=CloudKafkaDownloaderConfig,
)

kafka_cloud_destination_entry = DestinationRegistryEntry(
    connection_config=CloudKafkaConnectionConfig,
    uploader=CloudKafkaUploader,
    uploader_config=CloudKafkaUploaderConfig,
)
