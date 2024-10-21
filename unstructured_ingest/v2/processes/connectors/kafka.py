import base64
import json
import socket
import random
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional

from pydantic import Field, Secret, SecretStr

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)

# TODO DEPRECATE IT ONCE FINISH THE DESTINATION
from unstructured_ingest.interfaces import (
    AccessConfig,
    BaseDestinationConnector,
    BaseIngestDoc,
    IngestDocSessionHandleMixin,
    WriteConfig,
)
from unstructured_ingest.logger import logger
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
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer

CONNECTOR_TYPE = "kafka"


class KafkaAccessConfig(AccessConfig):
    kafka_api_key: Optional[SecretStr] = Field(description="Kafka API key to connect at the server", default = None)
    secret: Optional[SecretStr] = Field(description="", default=None)
    

class KafkaConnectionConfig(ConnectionConfig):
    access_config: Secret[KafkaAccessConfig]
    timeout: Optional[float] = 1.0
    confluent: Optional[bool] = False
    bootstrap_server: str
    port: str
    topic: str
    num_messages_to_consume: Optional[int] = 1

    @requires_dependencies(["confluent_kafka"], extras="kafka")
    def get_client(self) -> "Consumer":
        from confluent_kafka import Consumer

        is_confluent = self.confluent
        bootstrap = self.bootstrap_server
        port = self.port

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "client.id": socket.gethostname(),
            "group.id": "your_group_id",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
            "message.max.bytes": 10485760,
        }

        if is_confluent:
            kafka_api_key = self.access_config.kafka_api_key
            secret = self.access_config.secret
            conf["sasl.mechanism"] = "PLAIN"
            conf["security.protocol"] = "SASL_SSL"
            conf["sasl.username"] = kafka_api_key
            conf["sasl.password"] = secret

        consumer = Consumer(conf)
        logger.debug(f"kafka consumer connected to bootstrap: {bootstrap}")
        topic = self.topic
        logger.info(f"subscribing to topic: {topic}")
        consumer.subscribe([topic])
        return consumer


class KafkaIndexerConfig(IndexerConfig):
    pass

@dataclass
class KafkaIndexer(Indexer):
    connection_config: KafkaConnectionConfig
    index_config: KafkaIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _get_messages(self):
        from confluent_kafka import KafkaError

        consumer = self.connection_config.get_client()
        running = True

        collected = {}
        num_messages_to_consume = self.connection_config.num_messages_to_consume
        logger.info(f"config set for blocking on {num_messages_to_consume} messages")
        # Consume specified number of messages
        while running:
            msg = consumer.poll(timeout=self.connection_config.timeout)
            if msg is None:
                logger.debug("No Kafka messages found")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
            else:
                msg_content = json.loads(msg.value().decode("utf8"))
                collected[f"{msg.topic()}_{msg.partition()}_{msg.offset()}_{msg_content['filename']}"] = msg_content
                logger.debug(f"found {len(collected)} messages, stopping")
                consumer.commit(asynchronous=False)
                break

        return collected

    def run(self) -> Generator[FileData, None, None]:
        messages_consumed = self._get_messages()
        for key in messages_consumed.keys():
            yield FileData(
                identifier=key.split('_')[0],
                connector_type=self.connector_type,
                metadata=FileDataSourceMetadata(
                    date_processed=str(time()),
                ),
                additional_metadata={
                    "filename": messages_consumed[key]["filename"],
                    "content": messages_consumed[key]["content"],
                },
            )

    def precheck(self):
        try:
            _ = self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")


class KafkaDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class KafkaDownloader(Downloader):
    connection_config: KafkaConnectionConfig
    download_config: KafkaDownloaderConfig = field(default_factory=lambda: KafkaDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE
    version: Optional[str] = None
    source_url: Optional[str] = None

    def _tmp_download_file(self, filename: str):
        topic_file = self.connection_config.topic + "-" + filename
        return Path(self.download_dir) / topic_file

    def _create_full_tmp_dir_path(self, filename: str):
        self._tmp_download_file(filename).parent.mkdir(parents=True, exist_ok=True)       

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        filename = file_data.additional_metadata["filename"]
        self._create_full_tmp_dir_path(filename)
        download_path = self._tmp_download_file(filename)

        try:
            pdf_data = base64.b64decode(file_data.additional_metadata["content"])
            with open(download_path, "wb") as file:
                file.write(pdf_data)
        except Exception:
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        return self.generate_download_response(file_data = file_data, download_path= download_path)


# TODO address it in a separate PR -> destination
@dataclass
class KafkaWriteConfig(WriteConfig):
    batch_size: int = 4


@dataclass
class KafkaDestinationConnector(IngestDocSessionHandleMixin, BaseDestinationConnector):
    """Connector to write BaseIngestDoc types to Kafka
    Writes messages to Kafka in the format:
    "type"<type>
    "text":<the partitioned text>
    "filename":<name of the upstream file>
    """

    write_config: KafkaWriteConfig
    connector_config: KafkaConnectionConfig
    _producer: Optional["Producer"] = None

    @property
    def kafka_producer(self):
        if self._producer is None:
            self._producer = self.create_producer()
        return self._producer

    def initialize(self):
        pass

    @requires_dependencies(["confluent_kafka"], extras="kafka")
    def create_producer(self) -> "Producer":
        from confluent_kafka import Producer

        is_confluent = self.connector_config.access_config.confluent
        bootstrap = self.connector_config.bootstrap_server
        port = self.connector_config.port

        conf = {
            "bootstrap.servers": f"{bootstrap}:{port}",
            "client.id": socket.gethostname(),
        }

        if is_confluent:
            api_key = self.connector_config.access_config.kafka_api_key
            secret = self.connector_config.access_config.secret
            conf["sasl.mechanism"] = "PLAIN"
            conf["security.protocol"] = "SASL_SSL"
            conf["sasl.username"] = api_key
            conf["sasl.password"] = secret

        producer = Producer(conf)
        logger.debug(f"connected to bootstrap: {bootstrap}")
        return producer

    def check_connection(self):
        try:
            self.kafka_producer
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @DestinationConnectionError.wrap
    def upload_msg(self, batch) -> int:
        logger.debug(f"uploading batch: {batch}")
        topic = self.connector_config.topic
        producer = self.kafka_producer
        uploaded = 0
        for i in range(len(batch)):
            filename = f'{batch[i].pop("filename")}'
            producer.produce(topic, key=filename, value=str(batch[i]))
            uploaded += 1
        return uploaded

    @DestinationConnectionError.wrap
    def write_dict(self, *args, dict_list: List[Dict[str, Any]], **kwargs) -> None:
        logger.info(f"writing {len(dict_list)} documents to Kafka")
        num_uploaded = 0

        for chunk in batch_generator(dict_list, self.write_config.batch_size):
            num_uploaded += self.upload_msg(chunk)  # noqa: E203

        producer = self.kafka_producer
        producer.flush()
        logger.info(f"uploaded {num_uploaded} documents to Kafka")

    def write(self, docs: List[BaseIngestDoc]) -> None:
        content_list: List[Dict[str, Any]] = []
        for doc in docs:
            local_path = doc._output_filename
            with open(local_path) as json_file:
                dict_content = json.load(json_file)
                for content in dict_content:
                    content_list.append(
                        {
                            "type": content["type"],
                            "text": content["text"],
                            "filename": content["metadata"]["filename"],
                        }
                    )
        self.write_dict(dict_list=content_list)


kafka_source_entry = SourceRegistryEntry(
    connection_config=KafkaConnectionConfig,
    indexer=KafkaIndexer,
    indexer_config=KafkaIndexerConfig,
    downloader=KafkaDownloader,
    downloader_config=KafkaDownloaderConfig,
)
