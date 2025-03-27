import json
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from unstructured_ingest.logger import logger

TOPIC = "fake-topic"


def get_admin_client() -> AdminClient:
    conf = {
        "bootstrap.servers": "localhost:29092",
    }
    return AdminClient(conf)


def wait_for_topic(
    topic: str,
    retries: int = 10,
    interval: int = 2,
    exists: bool = True,
    admin_client=None,
):
    if admin_client is None:
        admin_client = get_admin_client()
    current_topics = admin_client.list_topics().topics
    attempts = 0
    while (topic not in current_topics) == exists and attempts < retries:
        attempts += 1
        logger.info(
            "Attempt {}: Waiting for topic {} to {} exist. Current topics: [{}]".format(
                attempts, topic, "" if exists else "not", ", ".join(current_topics)
            )
        )
        time.sleep(interval)
        current_topics = admin_client.list_topics().topics
    logger.info(
        "Attempt {} succeeded: Waiting for topic {} to {} exist. Current topics: [{}]".format(
            attempts, topic, "" if exists else "not", ", ".join(current_topics)
        )
    )

    if (topic not in current_topics) == exists:
        raise TimeoutError(f"Timeout out waiting for topic {topic} to exist")


def get_all_messages(conf: dict, topic: str, max_empty_messages: int = 3) -> list[dict]:
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    messages = []
    try:
        empty_count = 0
        while empty_count < max_empty_messages:
            msg = consumer.poll(timeout=5)
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
        consumer.close()
    return messages
