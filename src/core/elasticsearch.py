"""
Elasticsearch async client factory with connection pooling,
retry logic, and health-check utilities.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_es_client() -> AsyncElasticsearch:
    """Return a cached AsyncElasticsearch client."""
    settings = get_settings()

    http_auth: Optional[tuple] = None
    if settings.es_username:
        http_auth = (settings.es_username, settings.es_password)

    ca_certs: Optional[str] = settings.es_ca_cert or None

    client = AsyncElasticsearch(
        hosts=settings.es_hosts_list,
        http_auth=http_auth,
        ca_certs=ca_certs,
        verify_certs=bool(ca_certs),
        request_timeout=settings.es_request_timeout,
        max_retries=settings.es_max_retries,
        retry_on_timeout=True,
        retry_on_status=[429, 502, 503, 504],
        connections_per_node=10,
    )
    logger.info("elasticsearch_client_created", hosts=settings.es_hosts_list)
    return client


async def close_es_client() -> None:
    client = get_es_client()
    await client.close()
    logger.info("elasticsearch_client_closed")


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def bulk_index(
    client: AsyncElasticsearch,
    index: str,
    docs: List[Dict[str, Any]],
    id_field: str = "_id",
    pipeline: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Bulk index documents. Each doc may have an `_id` field.
    Returns summary with counts of success/failure.
    """
    actions: List[Dict[str, Any]] = []
    for doc in docs:
        doc_id = doc.pop(id_field, None)
        meta: Dict[str, Any] = {"_index": index}
        if doc_id:
            meta["_id"] = doc_id
        if pipeline:
            meta["pipeline"] = pipeline
        actions.append({"index": meta})
        actions.append(doc)

    resp = await client.bulk(body=actions, refresh="wait_for" if len(docs) < 100 else False)
    errors = [item for item in resp["items"] if "error" in item.get("index", {})]
    if errors:
        logger.warning(
            "bulk_index_partial_failure",
            index=index,
            total=len(docs),
            failed=len(errors),
            first_error=errors[0]["index"].get("error"),
        )
    return {
        "total": len(docs),
        "succeeded": len(docs) - len(errors),
        "failed": len(errors),
        "errors": errors[:5],  # return first 5 errors for diagnostics
    }


async def scroll_index(
    client: AsyncElasticsearch,
    index: str,
    query: Dict[str, Any],
    batch_size: int = 500,
    sort_field: str = "_id",
):
    """
    Efficient scroll using search_after + PIT (point-in-time).
    Yields batches of hits. Avoids deep pagination issues.
    """
    pit_resp = await client.open_point_in_time(index=index, keep_alive="5m")
    pit_id = pit_resp["id"]
    search_after: Optional[List] = None
    total_yielded = 0

    try:
        while True:
            body: Dict[str, Any] = {
                "size": batch_size,
                "query": query,
                "sort": [{sort_field: "asc"}, {"_shard_doc": "asc"}],
                "pit": {"id": pit_id, "keep_alive": "5m"},
            }
            if search_after:
                body["search_after"] = search_after

            resp = await client.search(body=body)
            hits = resp["hits"]["hits"]
            if not hits:
                break

            yield hits
            total_yielded += len(hits)
            search_after = hits[-1]["sort"]
            logger.debug("scroll_progress", index=index, yielded=total_yielded)

    finally:
        await client.close_point_in_time(body={"id": pit_id})
        logger.info("scroll_complete", index=index, total_yielded=total_yielded)


async def ensure_index(
    client: AsyncElasticsearch,
    index: str,
    mapping: Dict[str, Any],
) -> bool:
    """Create index if it does not exist. Returns True if created."""
    exists = await client.indices.exists(index=index)
    if exists:
        logger.info("index_already_exists", index=index)
        return False
    await client.indices.create(index=index, body=mapping)
    logger.info("index_created", index=index)
    return True
