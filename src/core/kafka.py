"""
Kafka async producer and consumer factory.
Uses aiokafka for non-blocking I/O compatible with asyncio.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)

_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    """Return a started global Kafka producer (singleton)."""
    global _producer
    if _producer is None:
        settings = get_settings()
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            compression_type="gzip",
            enable_idempotence=True,
            max_batch_size=1_048_576,  # 1 MB
            linger_ms=10,
            acks="all",
        )
        await _producer.start()
        logger.info("kafka_producer_started", servers=settings.kafka_bootstrap_servers)
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None
        logger.info("kafka_producer_stopped")


async def publish(
    topic: str,
    value: Dict[str, Any],
    key: Optional[str] = None,
) -> None:
    """Publish a JSON message to a Kafka topic."""
    producer = await get_producer()
    await producer.send_and_wait(topic, value=value, key=key)
    logger.debug("kafka_message_published", topic=topic, key=key)


@asynccontextmanager
async def consumer_context(
    topic: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> AsyncIterator[AIOKafkaConsumer]:
    """Context manager that yields a started Kafka consumer."""
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,   # manual commit for exactly-once semantics
        max_poll_records=100,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )
    await consumer.start()
    logger.info("kafka_consumer_started", topic=topic, group_id=group_id)
    try:
        yield consumer
    finally:
        await consumer.stop()
        logger.info("kafka_consumer_stopped", topic=topic, group_id=group_id)


async def run_consumer_loop(
    topic: str,
    group_id: str,
    handler: Callable[[Dict[str, Any]], Any],
    dead_letter_topic: Optional[str] = None,
) -> None:
    """
    Infinite consumer loop. Calls handler(message) for each record.
    On failure, sends to dead-letter topic if configured.
    Commits offset only after successful processing.
    """
    settings = get_settings()
    async with consumer_context(topic, group_id) as consumer:
        async for msg in consumer:
            try:
                await handler(msg.value)
                await consumer.commit()
                logger.debug("kafka_message_processed", topic=topic, offset=msg.offset)
            except Exception as exc:
                logger.error(
                    "kafka_message_failed",
                    topic=topic,
                    offset=msg.offset,
                    error=str(exc),
                    exc_info=True,
                )
                if dead_letter_topic:
                    producer = await get_producer()
                    await producer.send_and_wait(
                        dead_letter_topic,
                        value={"original": msg.value, "error": str(exc)},
                    )
