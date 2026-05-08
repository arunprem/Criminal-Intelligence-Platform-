"""
Relationship Worker — Kafka consumer for person_resolved topic.

Consumes master person create/update events → generates relationships
→ publishes to relationships_generated topic.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from src.core.config import get_settings
from src.core.elasticsearch import get_es_client
from src.core.kafka import run_consumer_loop
from src.core.logging import configure_logging, get_logger
from src.relationships.generator import RelationshipGenerator

logger = get_logger(__name__)
settings = get_settings()


async def handle_resolved_person(message: Dict[str, Any]) -> None:
    """
    Process a person_resolved event.
    Generates FIR-based and shared-attribute relationships.
    """
    master_id = message.get("master_person_id")
    fir_numbers = message.get("fir_numbers", [])
    action = message.get("action", "unknown")

    if not master_id:
        logger.warning("resolved_event_missing_master_id", message=message)
        return

    es = get_es_client()
    generator = RelationshipGenerator(es)

    total_rels = 0

    # Generate FIR-based relationships for each FIR
    for fir_no in fir_numbers:
        count = await generator.generate_for_fir(fir_no)
        total_rels += count

    # Generate shared-attribute relationships
    attr_count = await generator.generate_shared_attributes(master_id)
    total_rels += attr_count

    logger.info(
        "relationship_generation_complete",
        master_id=master_id,
        action=action,
        total_relationships=total_rels,
    )


async def main() -> None:
    configure_logging()
    logger.info("relationship_worker_starting")
    await run_consumer_loop(
        topic=settings.kafka_topic_person_resolved,
        group_id=settings.kafka_consumer_group_relationship,
        handler=handle_resolved_person,
        dead_letter_topic="person_resolved_dlq",
    )


if __name__ == "__main__":
    asyncio.run(main())
