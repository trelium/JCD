"""
Thin wrappers around confluent-kafka that add:
  - structured logging on every produce/consume event
  - automatic retry with exponential back-off on transient errors
  - dead-letter topic routing on repeated failures
"""
from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DLQ_SUFFIX = ".dlq"

# ---------------------------------------------------------------------------
# Admin helpers
# ---------------------------------------------------------------------------

def ensure_topics(topics: list[str], num_partitions: int = 3, replication: int = 1) -> None:
    """Create topics if they don't exist (idempotent)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication)
        for t in topics
        if t not in existing
    ]
    if not to_create:
        return
    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("kafka.topic_created", topic=topic)
        except KafkaException as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

def get_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",                      # wait for all replicas
            "retries": 5,
            "retry.backoff.ms": 200,
            "enable.idempotence": True,
        }
    )


def publish(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    def ack(err: Any, msg: Any) -> None:
        if err:
            log.error("kafka.produce_error", topic=topic, key=key, error=str(err))
        else:
            log.debug("kafka.produced", topic=topic, key=key, offset=msg.offset())

    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(payload).encode(),
        callback=ack,
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

def get_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,        # manual commit after processing
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
        }
    )
    consumer.subscribe(topics)
    return consumer


def consume_loop(
    consumer: Consumer,
    producer: Producer,
    handler: Callable[[dict[str, Any]], None],
    max_retries: int = 3,
) -> None:
    """
    Blocking consume loop.
    - Deserialises JSON messages
    - Calls handler
    - On failure retries up to max_retries with exponential back-off
    - After max_retries routes message to <topic>.dlq
    - Commits offset only on success or DLQ routing
    """
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("kafka.consumer_error", error=str(msg.error()))
            continue

        topic = msg.topic()
        raw = msg.value()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.error("kafka.bad_json", topic=topic, error=str(exc))
            _send_to_dlq(producer, topic, raw)
            consumer.commit(message=msg)
            continue

        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                handler(payload)
                consumer.commit(message=msg)
                log.info("kafka.message_processed", topic=topic, attempt=attempt)
                break
            except Exception as exc:
                last_exc = exc
                wait = 0.2 * (2 ** (attempt - 1))
                log.warning(
                    "kafka.handler_error",
                    topic=topic,
                    attempt=attempt,
                    error=str(exc),
                    retry_in=wait,
                )
                time.sleep(wait)
        else:
            log.error("kafka.max_retries_exceeded", topic=topic, error=str(last_exc))
            _send_to_dlq(producer, topic, raw)
            consumer.commit(message=msg)


def _send_to_dlq(producer: Producer, origin_topic: str, raw: bytes) -> None:
    dlq_topic = origin_topic + DLQ_SUFFIX
    producer.produce(dlq_topic, value=raw)
    producer.poll(0)
    log.warning("kafka.sent_to_dlq", dlq_topic=dlq_topic)
