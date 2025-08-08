import json
import tempfile
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from test.integration.connectors.kafka.conftest import (
    TOPIC,
    get_admin_client,
    get_all_messages,
    wait_for_topic,
)
from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    UNCATEGORIZED_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.docker_compose import docker_compose_context
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
    source_filedata_display_name_set_check,
)
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connectors.kafka.local import (
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


@pytest.fixture
def docker_compose_ctx():
    with docker_compose_context(docker_compose_path=env_setup_path / "kafka") as ctx:
        yield ctx


@pytest.fixture
def kafka_seed_topic(docker_compose_ctx) -> str:
    conf = {
        "bootstrap.servers": "localhost:29092",
    }
    producer = Producer(conf)
    for i in range(SEED_MESSAGES):
        message = f"This is some text for message {i}"
        producer.produce(topic=TOPIC, value=message)
    while producer_len := len(producer):
        logger.info(f"another iteration of kafka producer flush. Queue length: {producer_len}")
        producer.flush(timeout=10)
    logger.info(f"kafka topic {TOPIC} seeded with {SEED_MESSAGES} messages")
    wait_for_topic(topic=TOPIC)
    return TOPIC


@pytest.fixture
def kafka_upload_topic(docker_compose_ctx) -> str:
    admin_client = get_admin_client()
    admin_client.create_topics([NewTopic(TOPIC, 1, 1)])
    wait_for_topic(topic=TOPIC)
    return TOPIC


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
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
                test_id="kafka-local",
                expected_num_files=5,
                validate_downloaded_files=True,
                predownload_file_data_check=source_filedata_display_name_set_check,
                postdownload_file_data_check=source_filedata_display_name_set_check,
            ),
        )


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
def test_kafka_source_local_precheck_fail_no_cluster():
    connection_config = LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092)
    indexer = LocalKafkaIndexer(
        connection_config=connection_config,
        index_config=LocalKafkaIndexerConfig(topic=TOPIC, num_messages_to_consume=5),
    )
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
def test_kafka_source_local_precheck_fail_no_topic(kafka_seed_topic: str):
    connection_config = LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092)
    indexer = LocalKafkaIndexer(
        connection_config=connection_config,
        index_config=LocalKafkaIndexerConfig(topic="topic", num_messages_to_consume=5),
    )
    with pytest.raises(SourceConnectionError):
        indexer.precheck()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, UNCATEGORIZED_TAG)
async def test_kafka_destination_local(
    kafka_upload_topic: str,
    upload_file: Path,
):
    """
    Creates empty topic in localhost instance, sends 1 partitioned file using Uploader.
    Downloader should download it.
    """

    uploader = LocalKafkaUploader(
        connection_config=LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092),
        upload_config=LocalKafkaUploaderConfig(topic=kafka_upload_topic, batch_size=10),
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
    conf = {
        "bootstrap.servers": "localhost:29092",
        "group.id": "default_group_id",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    }
    all_messages = get_all_messages(conf=conf, topic=kafka_upload_topic)
    with upload_file.open("r") as upload_fs:
        content_to_upload = json.load(upload_fs)
    assert len(all_messages) == len(content_to_upload), (
        f"expected number of messages ({len(content_to_upload)}) doesn't "
        f"match how many messages read off of kafka topic {kafka_upload_topic}: {len(all_messages)}"
    )
    assert all_messages == content_to_upload


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, UNCATEGORIZED_TAG)
def test_kafka_destination_local_precheck_fail_no_cluster():
    uploader = LocalKafkaUploader(
        connection_config=LocalKafkaConnectionConfig(bootstrap_server="localhost", port=29092),
        upload_config=LocalKafkaUploaderConfig(topic=TOPIC, batch_size=10),
    )
    with pytest.raises(DestinationConnectionError):
        uploader.precheck()
