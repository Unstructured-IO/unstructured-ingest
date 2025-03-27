import json
import os
import tempfile
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from test.integration.connectors.kafka.conftest import TOPIC, get_all_messages, wait_for_topic
from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SOURCE_TAG,
    UNCATEGORIZED_TAG,
)
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connectors.kafka.cloud import (
    CloudKafkaAccessConfig,
    CloudKafkaConnectionConfig,
    CloudKafkaDownloader,
    CloudKafkaDownloaderConfig,
    CloudKafkaIndexer,
    CloudKafkaIndexerConfig,
    CloudKafkaUploader,
    CloudKafkaUploaderConfig,
)
from unstructured_ingest.processes.connectors.kafka.local import CONNECTOR_TYPE
from unstructured_ingest.types.file_data import FileData, SourceIdentifiers


@pytest.fixture
def kafka_seed_topic_cloud(request) -> int:
    expected_messages: int = request.param
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
            logger.info(f"Topic {topic} removed")
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
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, UNCATEGORIZED_TAG)
@requires_env("KAFKA_API_KEY", "KAFKA_SECRET", "KAFKA_BOOTSTRAP_SERVER")
@pytest.mark.parametrize("kafka_seed_topic_cloud", [5], indirect=True)
async def test_kafka_source_cloud(kafka_seed_topic_cloud: int):
    """
    Creates topic in cloud, sends 5 simple messages. Downloader should download them.
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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, UNCATEGORIZED_TAG)
@requires_env("KAFKA_API_KEY", "KAFKA_SECRET", "KAFKA_BOOTSTRAP_SERVER")
@pytest.mark.parametrize(
    "kafka_seed_topic_cloud", [0], indirect=True
)  # make it just create topic, without messages
async def test_kafka_destination_cloud(upload_file: Path, kafka_seed_topic_cloud: int):
    """
    Creates empty topic in cloud, sends 1 partitioned file using Uploader.
    In order to have this test succeed, you need to create cluster on Confluent Cloud.
    """

    connection_config = CloudKafkaConnectionConfig(
        bootstrap_server=os.environ["KAFKA_BOOTSTRAP_SERVER"],
        port=9092,
        access_config=CloudKafkaAccessConfig(
            kafka_api_key=os.environ["KAFKA_API_KEY"],
            secret=os.environ["KAFKA_SECRET"],
        ),
    )

    uploader = CloudKafkaUploader(
        connection_config=connection_config,
        upload_config=CloudKafkaUploaderConfig(topic=TOPIC, batch_size=10),
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
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_SECRET"],
        "sasl.mechanism": "PLAIN",
        "security.protocol": "SASL_SSL",
        "group.id": "default_group_name",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    }

    all_messages = get_all_messages(conf=conf, topic=TOPIC)
    with upload_file.open("r") as upload_fs:
        content_to_upload = json.load(upload_fs)
    assert len(all_messages) == len(content_to_upload), (
        f"expected number of messages ({len(content_to_upload)}) doesn't "
        f"match how many messages read off of kafka topic {TOPIC}: {len(all_messages)}"
    )
    assert all_messages == content_to_upload
