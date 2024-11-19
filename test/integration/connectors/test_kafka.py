import json
import tempfile
import time
from pathlib import Path

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.kafka.local import (
    CONNECTOR_TYPE,
    LocalKafkaConnectionConfig,
    LocalKafkaDownloader,
    LocalKafkaDownloaderConfig,
    LocalKafkaIndexer,
    LocalKafkaIndexerConfig,
    LocalKafkaUploader,
    LocalKafkaUploaderConfig,
)

SEED_MESSAGES = 10
TOPIC = "fake-topic"


def get_admin_client() -> AdminClient:
    conf = {
        "bootstrap.servers": "localhost:29092",
    }
    return AdminClient(conf)


@pytest.fixture
def docker_compose_ctx():
    with docker_compose_context(docker_compose_path=env_setup_path / "kafka") as ctx:
        yield ctx


def wait_for_topic(topic: str, retries: int = 10, interval: int = 1):
    admin_client = get_admin_client()
    current_topics = admin_client.list_topics().topics
    attempts = 0
    while topic not in current_topics and attempts < retries:
        attempts += 1
        print(
            "Attempt {}: Waiting for topic {} to exist in {}".format(
                attempts, topic, ", ".join(current_topics)
            )
        )
        time.sleep(interval)
        current_topics = admin_client.list_topics().topics
    if topic not in current_topics:
        raise TimeoutError(f"Timeout out waiting for topic {topic} to exist")


@pytest.fixture
def kafka_seed_topic(docker_compose_ctx) -> str:
    conf = {
        "bootstrap.servers": "localhost:29092",
    }
    producer = Producer(conf)
    for i in range(SEED_MESSAGES):
        message = f"This is some text for message {i}"
        producer.produce(topic=TOPIC, value=message)
    producer.flush(timeout=10)
    print(f"kafka topic {TOPIC} seeded with {SEED_MESSAGES} messages")
    wait_for_topic(topic=TOPIC)
    return TOPIC


@pytest.fixture
def kafka_upload_topic(docker_compose_ctx) -> str:
    admin_client = get_admin_client()
    admin_client.create_topics([NewTopic(TOPIC, 1, 1)])
    return TOPIC


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
        indexer.precheck()
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id="kafka", expected_num_files=5, validate_downloaded_files=True
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_kafka_source_local_precheck_fail_no_cluster():
    connection_config = LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092)
    indexer = LocalKafkaIndexer(
        connection_config=connection_config,
        index_config=LocalKafkaIndexerConfig(topic=TOPIC, num_messages_to_consume=5),
    )
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
def test_kafka_source_local_precheck_fail_no_topic(kafka_seed_topic: str):
    connection_config = LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092)
    indexer = LocalKafkaIndexer(
        connection_config=connection_config,
        index_config=LocalKafkaIndexerConfig(topic="topic", num_messages_to_consume=5),
    )
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


def get_all_messages(topic: str, max_empty_messages: int = 5) -> list[dict]:
    conf = {
        "bootstrap.servers": "localhost:29092",
        "group.id": "default_group_id",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    messages = []
    try:
        empty_count = 0
        while empty_count < max_empty_messages:
            msg = consumer.poll(timeout=1)
            if msg is None:
                empty_count += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    raise KafkaException(msg.error())
            try:
                message = json.loads(msg.value().decode("utf8"))
                messages.append(message)
            finally:
                consumer.commit(asynchronous=False)
    finally:
        print("closing consumer")
        consumer.close()
    return messages


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
async def test_kafka_destination_local(upload_file: Path, kafka_upload_topic: str):
    uploader = LocalKafkaUploader(
        connection_config=LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092),
        upload_config=LocalKafkaUploaderConfig(topic=TOPIC, batch_size=10),
    )
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    uploader.precheck()
    if uploader.is_async():
        await uploader.run_async(path=upload_file, file_data=file_data)
    else:
        uploader.run(path=upload_file, file_data=file_data)
    all_messages = get_all_messages(topic=kafka_upload_topic)
    with upload_file.open("r") as upload_fs:
        content_to_upload = json.load(upload_fs)
    assert len(all_messages) == len(content_to_upload), (
        f"expected number of messages ({len(content_to_upload)}) doesn't "
        f"match how many messages read off of kakfa topic {kafka_upload_topic}: {len(all_messages)}"
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
def test_kafka_destination_local_precheck_fail_no_cluster():
    uploader = LocalKafkaUploader(
        connection_config=LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092),
        upload_config=LocalKafkaUploaderConfig(topic=TOPIC, batch_size=10),
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()
