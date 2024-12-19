import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, ContextManager, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer


class KafkaAccessConfig(AccessConfig, ABC):
    pass


class KafkaConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[KafkaAccessConfig]
    bootstrap_server: str
    port: int
    group_id: str = Field(
        description="A consumer group is a way to allow a pool of consumers "
        "to divide the consumption of data over topics and partitions.",
        default="default_group_id",
    )

    @abstractmethod
    def get_consumer_configuration(self) -> dict:
        pass

    @abstractmethod
    def get_producer_configuration(self) -> dict:
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

    @requires_dependencies(["confluent_kafka"], extras="kafka")
    def get_producer(self) -> "Producer":
        from confluent_kafka import Producer

        producer = Producer(self.get_producer_configuration())
        return producer


class KafkaIndexerConfig(IndexerConfig):
    topic: str = Field(description="which topic to consume from")
    num_messages_to_consume: Optional[int] = 100
    timeout: Optional[float] = Field(default=3.0, description="polling timeout", ge=3.0)

    def update_consumer(self, consumer: "Consumer") -> None:
        consumer.subscribe([self.topic])


@dataclass
class KafkaIndexer(Indexer, ABC):
    connection_config: KafkaConnectionConfig
    index_config: KafkaIndexerConfig

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
                msg = consumer.poll(timeout=self.index_config.timeout)
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

    async def run_async(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        raise NotImplementedError()

    def precheck(self):
        try:
            with self.get_consumer() as consumer:
                # timeout needs at least 3 secs, more info:
                # https://forum.confluent.io/t/kafkacat-connect-failure-to-confcloud-ssl/2513
                cluster_meta = consumer.list_topics(timeout=5)
                current_topics = [
                    topic for topic in cluster_meta.topics if topic != "__consumer_offsets"
                ]
                if self.index_config.topic not in current_topics:
                    raise SourceConnectionError(
                        "expected topic '{}' not detected in cluster: '{}'".format(
                            self.index_config.topic, ", ".join(current_topics)
                        )
                    )
                logger.info(f"successfully checked available topics: {current_topics}")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")


class KafkaDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class KafkaDownloader(Downloader, ABC):
    connection_config: KafkaConnectionConfig
    download_config: KafkaDownloaderConfig = field(default_factory=KafkaDownloaderConfig)
    version: Optional[str] = None
    source_url: Optional[str] = None

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
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


class KafkaUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Batch size")
    topic: str = Field(description="which topic to write to")
    timeout: Optional[float] = Field(
        default=10.0, description="Timeout in seconds to flush batch of messages"
    )


@dataclass
class KafkaUploader(Uploader, ABC):
    connection_config: KafkaConnectionConfig
    upload_config: KafkaUploaderConfig

    def precheck(self):
        try:
            with self.connection_config.get_consumer() as consumer:
                cluster_meta = consumer.list_topics(timeout=self.upload_config.timeout)
                current_topics = [
                    topic for topic in cluster_meta.topics if topic != "__consumer_offsets"
                ]
                logger.info(f"successfully checked available topics: {current_topics}")
                if self.upload_config.topic not in current_topics:
                    raise DestinationConnectionError(
                        "expected topic '{}' not detected in cluster: '{}'".format(
                            self.upload_config.topic, ", ".join(current_topics)
                        )
                    )

        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def produce_batch(self, elements: list[dict]) -> None:
        from confluent_kafka.error import KafkaException

        producer = self.connection_config.get_producer()
        failed_producer = False

        def acked(err, msg):
            nonlocal failed_producer
            if err is not None:
                failed_producer = True
                logger.error("Failed to deliver kafka message: %s: %s" % (str(msg), str(err)))

        for element in elements:
            producer.produce(
                topic=self.upload_config.topic,
                value=json.dumps(element),
                callback=acked,
            )

        while producer_len := len(producer):
            logger.debug(f"another iteration of kafka producer flush. Queue length: {producer_len}")
            producer.flush(timeout=self.upload_config.timeout)
        if failed_producer:
            raise KafkaException("failed to produce all messages in batch")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        for element_batch in batch_generator(data, batch_size=self.upload_config.batch_size):
            self.produce_batch(elements=element_batch)
