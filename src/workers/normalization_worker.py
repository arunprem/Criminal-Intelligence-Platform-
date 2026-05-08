"""
Normalization Worker — Kafka consumer for person_raw topic.

Consumes raw person events → normalizes → indexes to normalized_person
→ publishes to person_normalized topic.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from src.core.config import get_settings
from src.core.elasticsearch import bulk_index, get_es_client
from src.core.kafka import publish, run_consumer_loop
from src.core.logging import configure_logging, get_logger
from src.normalization.pipeline import NormalizationPipeline

logger = get_logger(__name__)
settings = get_settings()

pipeline = NormalizationPipeline()


async def handle_raw_person(message: Dict[str, Any]) -> None:
    """
    Process a single person_raw message.
    Expected message shape:
        {
            "source_index": "accused",
            "source_id": "abc123",
            "raw_doc": { ... }
        }
    """
    source_index = message["source_index"]
    source_id = message["source_id"]
    raw_doc = message["raw_doc"]

    # Normalize
    normalized = pipeline.process(raw_doc, source_index, source_id)
    doc_id = normalized.pop("_id")

    # Index to normalized_person
    es = get_es_client()
    await es.index(
        index=settings.index_normalized_person,
        id=doc_id,
        document=normalized,
    )

    # Publish downstream
    await publish(
        settings.kafka_topic_person_normalized,
        {
            "normalized_id": doc_id,
            "source_index": source_index,
            "source_id": source_id,
            "blocking_keys": normalized.get("blocking_keys", []),
        },
        key=doc_id,
    )
    logger.debug(
        "raw_person_processed",
        normalized_id=doc_id,
        source=source_index,
    )


async def main() -> None:
    configure_logging()
    logger.info("normalization_worker_starting")
    await run_consumer_loop(
        topic=settings.kafka_topic_person_raw,
        group_id=settings.kafka_consumer_group_normalization,
        handler=handle_raw_person,
        dead_letter_topic="person_raw_dlq",
    )


if __name__ == "__main__":
    asyncio.run(main())
