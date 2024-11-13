import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import Field, Secret

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

CONNECTOR_TYPE = "kafka-local"


class LocalKafkaAccessConfig(KafkaAccessConfig):
    pass


class LocalKafkaConnectionConfig(KafkaConnectionConfig):
    access_config: Secret[LocalKafkaAccessConfig] = Field(
        default=LocalKafkaAccessConfig(), validate_default=True
    )

    def get_consumer_configuration(self) -> dict:
        bootstrap = self.bootstrap_server
        port = self.port

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "client.id": socket.gethostname(),
            "group.id": "default_group_id",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
            "message.max.bytes": 10485760,
        }
        return conf


class LocalKafkaIndexerConfig(KafkaIndexerConfig):
    pass


@dataclass
class LocalKafkaIndexer(KafkaIndexer):
    connection_config: LocalKafkaConnectionConfig
    index_config: LocalKafkaIndexerConfig
    connector_type: str = CONNECTOR_TYPE


class LocalKafkaDownloaderConfig(KafkaDownloaderConfig):
    pass


@dataclass
class LocalKafkaDownloader(KafkaDownloader):
    connection_config: LocalKafkaConnectionConfig
    download_config: LocalKafkaDownloaderConfig
    connector_type: str = CONNECTOR_TYPE


kafka_local_source_entry = SourceRegistryEntry(
    connection_config=LocalKafkaConnectionConfig,
    indexer=LocalKafkaIndexer,
    indexer_config=LocalKafkaIndexerConfig,
    downloader=LocalKafkaDownloader,
    downloader_config=LocalKafkaDownloaderConfig,
)
