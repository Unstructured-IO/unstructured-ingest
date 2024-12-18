import json
import os
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
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.kafka.cloud import (
    CloudKafkaAccessConfig,
    CloudKafkaConnectionConfig,
    CloudKafkaDownloader,
    CloudKafkaDownloaderConfig,
    CloudKafkaIndexer,
    CloudKafkaIndexerConfig,
)
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


def wait_for_topic(
    topic: str,
    retries: int = 10,
    interval: int = 1,
    exists: bool = True,
    admin_client=None,
):
    if admin_client is None:
        admin_client = get_admin_client()
    current_topics = admin_client.list_topics().topics
    attempts = 0
    while (topic not in current_topics) == exists and attempts < retries:
        attempts += 1
        print(
            "Attempt {}: Waiting for topic {} to {} exist. Current topics: [{}]".format(
                attempts, topic, "" if exists else "not", ", ".join(current_topics)
            )
        )
        time.sleep(interval)
        current_topics = admin_client.list_topics().topics
    if (topic not in current_topics) == exists:
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
            configs=SourceValidationConfigs(
                test_id="kafka-local", expected_num_files=5, validate_downloaded_files=True
            ),
        )


@pytest.fixture
def kafka_seed_topic_cloud(expected_messages: int = 5) -> int:
    conf = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_SECRET"],
        "sasl.mechanism": "PLAIN",
        "security.protocol": "SASL_SSL",
    }
    admin_client = AdminClient(conf)
    try:
        res = admin_client.delete_topics([TOPIC], operation_timeout=10)
        for topic, f in res.items():
            f.result()
            print(f"Topic {topic} removed")
            wait_for_topic(TOPIC, 5, 1, False, admin_client)
    except Exception:
        pass

    cluster_meta = admin_client.list_topics()
    current_topics = [topic for topic in cluster_meta.topics if topic != "__consumer_offsets"]

    assert TOPIC not in current_topics, f"Topic {TOPIC} shouldn't exist"

    # Kafka Cloud allows to use replication_factor=1 only for Dedicated clusters.
    topic_obj = NewTopic(TOPIC, num_partitions=1, replication_factor=3)

    res = admin_client.create_topics([topic_obj], operation_timeout=10, validate_only=False)
    for topic, f in res.items():
        f.result()

    producer = Producer(conf)
    for i in range(expected_messages):
        message = f"This is some text for message {i}"
        producer.produce(topic=TOPIC, value=message)
    producer.flush(timeout=10)
    return expected_messages


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env("KAFKA_API_KEY", "KAFKA_SECRET", "KAFKA_BOOTSTRAP_SERVER")
async def test_kafka_source_cloud(kafka_seed_topic_cloud: int):
    """
    In order to have this test succeed, you need to create cluster on Confluent Cloud,
    and create the API key with admin privileges. By default, user account keys have it.
    """

    expected_messages = kafka_seed_topic_cloud

    connection_config = CloudKafkaConnectionConfig(
        bootstrap_server=os.environ["KAFKA_BOOTSTRAP_SERVER"],
        port=9092,
        access_config=CloudKafkaAccessConfig(
            kafka_api_key=os.environ["KAFKA_API_KEY"],
            secret=os.environ["KAFKA_SECRET"],
        ),
    )

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = CloudKafkaDownloaderConfig(download_dir=tempdir_path)
        indexer = CloudKafkaIndexer(
            connection_config=connection_config,
            index_config=CloudKafkaIndexerConfig(
                topic=TOPIC,
                num_messages_to_consume=expected_messages,
            ),
        )
        downloader = CloudKafkaDownloader(
            connection_config=connection_config, download_config=download_config
        )
        indexer.precheck()
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=SourceValidationConfigs(
                test_id="kafka-cloud",
                exclude_fields_extend=["connector_type"],
                expected_num_files=expected_messages,
                validate_downloaded_files=True,
                validate_file_data=True,
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
