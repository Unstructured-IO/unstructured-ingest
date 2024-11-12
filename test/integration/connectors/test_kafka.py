import socket
import tempfile
from pathlib import Path

import pytest
from confluent_kafka import Producer

from test.integration.connectors.utils.constants import (
    SOURCE_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.processes.connectors.kafka.local import (
    CONNECTOR_TYPE,
    LocalKafkaConnectionConfig,
    LocalKafkaDownloader,
    LocalKafkaDownloaderConfig,
    LocalKafkaIndexer,
    LocalKafkaIndexerConfig,
)

SEED_MESSAGES = 10
TOPIC = "fake-topic"


@pytest.fixture
def kafka_seed_topic() -> str:
    with docker_compose_context(docker_compose_path=env_setup_path / "kafka"):
        conf = {
            "bootstrap.servers": "localhost:29092",
            "client.id": socket.gethostname(),
            "message.max.bytes": 10485760,
        }
        producer = Producer(conf)
        for i in range(SEED_MESSAGES):
            message = f"This is some text for message {i}"
            producer.produce(topic=TOPIC, value=message)
        producer.flush(timeout=10)
        print(f"kafka topic {TOPIC} seeded with {SEED_MESSAGES} messages")
        yield TOPIC


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
async def test_kafka_source_local(kafka_seed_topic: str):
    connection_config = LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = LocalKafkaDownloaderConfig(download_dir=tempdir_path)
        indexer = LocalKafkaIndexer(
            connection_config=connection_config,
            index_config=LocalKafkaIndexerConfig(topic=kafka_seed_topic, num_messages_to_consume=5),
        )
        downloader = LocalKafkaDownloader(
            connection_config=connection_config, download_config=download_config
        )
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id="kafka", expected_num_files=5, validate_downloaded_files=True
            ),
        )
