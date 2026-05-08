"""
Resolution Worker — Kafka consumer for person_normalized topic.

Consumes normalized person IDs → runs entity resolution → updates
master_person → publishes to person_resolved topic.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from src.core.config import get_settings
from src.core.elasticsearch import get_es_client
from src.core.kafka import run_consumer_loop
from src.core.logging import configure_logging, get_logger
from src.entity_resolution.pipeline import EntityResolutionPipeline

logger = get_logger(__name__)
settings = get_settings()


async def handle_normalized_person(message: Dict[str, Any]) -> None:
    """
    Process a person_normalized event.
    Fetches normalized document, runs entity resolution.
    """
    normalized_id = message["normalized_id"]
    es = get_es_client()

    try:
        resp = await es.get(
            index=settings.index_normalized_person,
            id=normalized_id,
        )
        normalized_doc = resp["_source"]
        normalized_doc["normalized_id"] = normalized_id
    except Exception as exc:
        logger.error(
            "normalized_doc_fetch_failed",
            normalized_id=normalized_id,
            error=str(exc),
        )
        raise

    er_pipeline = EntityResolutionPipeline(es)
    result = await er_pipeline.resolve(normalized_doc)

    logger.info(
        "entity_resolution_complete",
        normalized_id=normalized_id,
        action=result["action"],
        master_id=result.get("master_person_id"),
        score=result.get("score", 0),
    )


async def main() -> None:
    configure_logging()
    logger.info("resolution_worker_starting")
    await run_consumer_loop(
        topic=settings.kafka_topic_person_normalized,
        group_id=settings.kafka_consumer_group_resolution,
        handler=handle_normalized_person,
        dead_letter_topic="person_normalized_dlq",
    )


if __name__ == "__main__":
    asyncio.run(main())
