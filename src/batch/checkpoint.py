"""
Checkpoint Manager — Persists batch processing progress to Elasticsearch.
Allows resumption after crashes or restarts.

Checkpoint document structure:
  {
    "index": "accused",
    "last_processed_id": "abc123",
    "count": 50000,
    "status": "in_progress" | "complete",
    "started_at": "...",
    "updated_at": "...",
    "completed_at": "..."
  }
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class CheckpointManager:
    CHECKPOINT_INDEX = "pipeline_checkpoints"

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es

    async def get(self, index: str) -> Optional[Dict[str, Any]]:
        """Retrieve checkpoint for an index. Returns None if not found."""
        try:
            resp = await self.es.get(
                index=self.CHECKPOINT_INDEX, id=self._checkpoint_id(index)
            )
            return resp["_source"]
        except NotFoundError:
            return None
        except Exception as exc:
            logger.warning("checkpoint_get_failed", index=index, error=str(exc))
            return None

    async def save(
        self,
        index: str,
        last_processed_id: Optional[str],
        count: int,
    ) -> None:
        """Upsert a checkpoint document."""
        now = datetime.now(timezone.utc).isoformat()
        doc = {
            "index": index,
            "last_processed_id": last_processed_id,
            "count": count,
            "status": "in_progress",
            "updated_at": now,
        }
        await self.es.update(
            index=self.CHECKPOINT_INDEX,
            id=self._checkpoint_id(index),
            body={
                "doc": doc,
                "doc_as_upsert": True,
                "upsert": {**doc, "started_at": now},
            },
            retry_on_conflict=3,
        )
        logger.debug("checkpoint_saved", index=index, count=count)

    async def complete(self, index: str, total: int) -> None:
        """Mark checkpoint as complete."""
        now = datetime.now(timezone.utc).isoformat()
        await self.es.update(
            index=self.CHECKPOINT_INDEX,
            id=self._checkpoint_id(index),
            body={
                "doc": {
                    "status": "complete",
                    "count": total,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
            retry_on_conflict=3,
        )
        logger.info("checkpoint_completed", index=index, total=total)

    async def reset(self, index: str) -> None:
        """Delete checkpoint to force reprocessing from scratch."""
        try:
            await self.es.delete(
                index=self.CHECKPOINT_INDEX,
                id=self._checkpoint_id(index),
            )
            logger.info("checkpoint_reset", index=index)
        except NotFoundError:
            pass

    @staticmethod
    def _checkpoint_id(index: str) -> str:
        return f"batch_checkpoint_{index}"
