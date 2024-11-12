import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from pydantic import Field, Secret, SecretStr

from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.v2.processes.connectors.kafka.kafka import (
    KafkaAccessConfig,
    KafkaConnectionConfig,
    KafkaDownloader,
    KafkaDownloaderConfig,
    KafkaIndexer,
    KafkaIndexerConfig,
)

if TYPE_CHECKING:
    pass

CONNECTOR_TYPE = "kafka-cloud"


class CloudKafkaAccessConfig(KafkaAccessConfig):
    api_key: Optional[SecretStr] = Field(
        description="Kafka API key to connect at the server", alias="kafka_api_key", default=None
    )
    secret: Optional[SecretStr] = Field(description="", default=None)


class CloudKafkaConnectionConfig(KafkaConnectionConfig):
    access_config: Secret[CloudKafkaAccessConfig]

    def get_consumer_configuration(self) -> dict:
        bootstrap = self.bootstrap_server
        port = self.port
        access_config = self.access_config.get_secret_value()

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "client.id": socket.gethostname(),
            "group.id": "default_group_id",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
            "message.max.bytes": 10485760,
            "sasl.username": access_config.api_key,
            "sasl.password": access_config.secret,
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_SSL",
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


kafka_cloud_source_entry = SourceRegistryEntry(
    connection_config=CloudKafkaConnectionConfig,
    indexer=CloudKafkaIndexer,
    indexer_config=CloudKafkaIndexerConfig,
    downloader=CloudKafkaDownloader,
    downloader_config=CloudKafkaDownloaderConfig,
)
