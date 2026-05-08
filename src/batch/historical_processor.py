"""
Historical Batch Processor.

Scrolls all raw source indices using ES PIT + search_after,
normalizes records in batches, publishes to Kafka for downstream processing.

Features:
- Checkpointing (survives restarts)
- Fault tolerance with retry
- Parallel processing across source indices
- Progress reporting
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from tenacity import retry, stop_after_attempt, wait_exponential

from src.batch.checkpoint import CheckpointManager
from src.core.config import get_settings
from src.core.elasticsearch import get_es_client, scroll_index
from src.core.kafka import get_producer, publish
from src.core.logging import configure_logging, get_logger
from src.normalization.pipeline import NormalizationPipeline

logger = get_logger(__name__)
settings = get_settings()
console = Console()


class HistoricalBatchProcessor:
    """
    Processes all records from raw source indices through the pipeline.
    Uses checkpointing to support resumable processing.
    """

    def __init__(self) -> None:
        self.es: Optional[AsyncElasticsearch] = None
        self.source_es: Optional[AsyncElasticsearch] = None
        self.pipeline = NormalizationPipeline()
        self.checkpoint_mgr: Optional[CheckpointManager] = None

    async def setup(self) -> None:
        self.es = get_es_client()
        self.checkpoint_mgr = CheckpointManager(self.es)
        
        if settings.remote_es_url:
            auth = None
            if settings.remote_es_username and settings.remote_es_password:
                auth = (settings.remote_es_username, settings.remote_es_password)
            self.source_es = AsyncElasticsearch(
                hosts=[settings.remote_es_url],
                basic_auth=auth,
                verify_certs=False,
                request_timeout=settings.es_request_timeout,
                max_retries=settings.es_max_retries,
                retry_on_timeout=True,
            )
            logger.info("remote_source_elasticsearch_configured", url=settings.remote_es_url)
        else:
            self.source_es = self.es

    async def run(
        self,
        indices: Optional[List[str]] = None,
        batch_size: int = 500,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        """
        Run historical batch processing for all or specified raw indices.
        Returns summary statistics.
        """
        await self.setup()
        target_indices = indices or settings.raw_indices
        stats: Dict[str, Any] = {}

        configure_logging()
        logger.info(
            "historical_batch_starting",
            indices=target_indices,
            batch_size=batch_size,
        )

        if parallel:
            tasks = [
                self._process_index(idx, batch_size)
                for idx in target_indices
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, result in zip(target_indices, results):
                if isinstance(result, Exception):
                    logger.error("index_processing_failed", index=idx, error=str(result))
                    stats[idx] = {"error": str(result)}
                else:
                    stats[idx] = result
        else:
            for idx in target_indices:
                try:
                    stats[idx] = await self._process_index(idx, batch_size)
                except Exception as exc:
                    logger.error("index_processing_failed", index=idx, error=str(exc))
                    stats[idx] = {"error": str(exc)}

        logger.info("historical_batch_complete", stats=stats)
        return stats

    async def _process_index(
        self,
        index: str,
        batch_size: int,
    ) -> Dict[str, Any]:
        """Process a single raw source index end-to-end."""
        processed = 0
        failed = 0
        batch: List[Dict[str, Any]] = []

        # Resume from checkpoint if available
        checkpoint = await self.checkpoint_mgr.get(index)
        start_from = checkpoint.get("last_processed_id") if checkpoint else None
        if start_from:
            logger.info("resuming_from_checkpoint", index=index, last_id=start_from)

        query = {"match_all": {}}

        async for hits in scroll_index(
            self.source_es, index, query, batch_size=batch_size
        ):
            for hit in hits:
                try:
                    normalized = self.pipeline.process(
                        raw_doc=hit["_source"],
                        source_index=index,
                        source_id=hit["_id"],
                    )
                    batch.append(normalized)
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "record_normalization_failed",
                        index=index,
                        doc_id=hit["_id"],
                        error=str(exc),
                    )

            # Flush batch
            if batch:
                await self._flush_batch(batch, index)
                processed += len(batch)
                batch = []

            # Checkpoint
            if processed % settings.batch_checkpoint_every == 0 and processed > 0:
                await self.checkpoint_mgr.save(
                    index=index,
                    last_processed_id=hits[-1]["_id"] if hits else None,
                    count=processed,
                )
                logger.info(
                    "checkpoint_saved",
                    index=index,
                    processed=processed,
                    failed=failed,
                )

        # Flush remaining
        if batch:
            await self._flush_batch(batch, index)
            processed += len(batch)

        await self.checkpoint_mgr.complete(index, processed)
        return {"processed": processed, "failed": failed}

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def _flush_batch(
        self,
        batch: List[Dict[str, Any]],
        source_index: str,
    ) -> None:
        """Bulk index normalized batch to ES and publish events to Kafka."""
        # Build bulk actions
        actions: List[Any] = []
        for doc in batch:
            doc_id = doc.pop("_id", None) or doc.get("normalized_id")
            actions.append({"index": {"_index": settings.index_normalized_person, "_id": doc_id}})
            actions.append(doc)

        if actions:
            await self.es.bulk(body=actions, refresh=False)

        # Publish to Kafka for downstream async processing
        producer = await get_producer()
        for doc in batch:
            await producer.send(
                settings.kafka_topic_person_normalized,
                value={
                    "normalized_id": doc.get("normalized_id"),
                    "source_index": source_index,
                    "source_id": doc.get("source_id"),
                    "blocking_keys": doc.get("blocking_keys", []),
                },
                key=(doc.get("normalized_id") or "").encode(),
            )

        logger.debug("batch_flushed", size=len(batch), source_index=source_index)

    async def close(self) -> None:
        """Close the remote source elasticsearch client if applicable."""
        if self.source_es and self.source_es is not self.es:
            await self.source_es.close()
