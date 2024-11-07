from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, ContextManager, Generator, Optional

from pydantic import Secret

from unstructured_ingest.error import (
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

if TYPE_CHECKING:
    from confluent_kafka import Consumer

CONNECTOR_TYPE = "kafka"


class KafkaAccessConfig(AccessConfig, ABC):
    pass


class KafkaConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[KafkaAccessConfig]
    timeout: Optional[float] = 1.0
    bootstrap_server: str
    port: int

    @abstractmethod
    def get_consumer_configuration(self) -> dict:
        pass

    @contextmanager
    @requires_dependencies(["confluent_kafka"], extras="kafka")
    def get_consumer(self) -> ContextManager["Consumer"]:
        from confluent_kafka import Consumer

        consumer = Consumer(self.get_consumer_configuration())
        try:
            logger.debug("kafka consumer connected")
            yield consumer
        finally:
            consumer.close()


class KafkaIndexerConfig(IndexerConfig):
    topic: str
    num_messages_to_consume: Optional[int] = 100

    def update_consumer(self, consumer: "Consumer") -> None:
        consumer.subscribe([self.topic])


@dataclass
class KafkaIndexer(Indexer):
    connection_config: KafkaConnectionConfig
    index_config: KafkaIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    @contextmanager
    def get_consumer(self) -> ContextManager["Consumer"]:
        with self.connection_config.get_consumer() as consumer:
            self.index_config.update_consumer(consumer=consumer)
            yield consumer

    @requires_dependencies(["confluent_kafka"], extras="kafka")
    def generate_messages(self) -> Generator[Any, None, None]:
        from confluent_kafka import KafkaError, KafkaException

        messages_consumed = 0
        max_empty_polls = 10
        empty_polls = 0
        num_messages_to_consume = self.index_config.num_messages_to_consume
        with self.get_consumer() as consumer:
            while messages_consumed < num_messages_to_consume and empty_polls < max_empty_polls:
                msg = consumer.poll(timeout=self.connection_config.timeout)
                if msg is None:
                    logger.debug("No Kafka messages found")
                    empty_polls += 1
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(
                            "Reached end of partition for topic %s [%d] at offset %d"
                            % (msg.topic(), msg.partition(), msg.offset())
                        )
                        break
                    else:
                        raise KafkaException(msg.error())
                try:
                    empty_polls = 0
                    messages_consumed += 1
                    yield msg
                finally:
                    consumer.commit(asynchronous=False)

    def generate_file_data(self, msg) -> FileData:
        msg_content = msg.value().decode("utf8")
        identifier = f"{msg.topic()}_{msg.partition()}_{msg.offset()}"
        additional_metadata = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "content": msg_content,
        }
        filename = f"{identifier}.txt"
        return FileData(
            identifier=identifier,
            connector_type=self.connector_type,
            source_identifiers=SourceIdentifiers(
                filename=filename,
                fullpath=filename,
            ),
            metadata=FileDataSourceMetadata(
                date_processed=str(time()),
            ),
            additional_metadata=additional_metadata,
            display_name=filename,
        )

    def run(self) -> Generator[FileData, None, None]:
        for message in self.generate_messages():
            yield self.generate_file_data(message)

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError()

    def precheck(self):
        try:
            with self.get_consumer() as consumer:
                cluster_meta = consumer.list_topics(timeout=self.connection_config.timeout)
                current_topics = [
                    topic for topic in cluster_meta.topics if topic != "__consumer_offsets"
                ]
                logger.info(f"successfully checked available topics: {current_topics}")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")


class KafkaDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class KafkaDownloader(Downloader):
    connection_config: KafkaConnectionConfig
    download_config: KafkaDownloaderConfig = field(default_factory=KafkaDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE
    version: Optional[str] = None
    source_url: Optional[str] = None

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        source_identifiers = file_data.source_identifiers
        if source_identifiers is None:
            raise ValueError("FileData is missing source_identifiers")

        # Build the download path using source_identifiers
        download_path = Path(self.download_dir) / source_identifiers.relative_path
        download_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            content = file_data.additional_metadata["content"]
            with open(download_path, "w") as file:
                file.write(content)
        except Exception as e:
            logger.error(f"Failed to download file {file_data.identifier}: {e}")
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        return self.generate_download_response(file_data=file_data, download_path=download_path)


kafka_source_entry = SourceRegistryEntry(
    connection_config=KafkaConnectionConfig,
    indexer=KafkaIndexer,
    indexer_config=KafkaIndexerConfig,
    downloader=KafkaDownloader,
    downloader_config=KafkaDownloaderConfig,
)
