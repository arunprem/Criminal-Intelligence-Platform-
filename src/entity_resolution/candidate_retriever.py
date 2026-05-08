"""
Candidate Retriever — Phase 2 of Entity Resolution.

Uses Elasticsearch multi-strategy search to retrieve top candidates
for a normalized person document. Avoids full dataset comparison by
leveraging blocking keys, phonetic indexes, and vector similarity.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class CandidateRetriever:
    """
    Retrieves top candidate normalized_person documents for entity resolution.

    Strategy (executed as a single bool query for efficiency):
    1. Exact phone match           (boost=10)
    2. Phonetic name match         (boost=5)
    3. ngram name match            (boost=3)
    4. Relative name phonetic      (boost=3)
    5. kNN vector similarity       (run separately if enabled)
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es
        self.index = settings.index_normalized_person

    async def retrieve(
        self,
        normalized_doc: Dict[str, Any],
        max_candidates: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve top candidates for a normalized person document.
        Returns a list of candidate hits with _score and _source.
        """
        candidates: Dict[str, Dict[str, Any]] = {}

        # ── Strategy 1: Blocking key match (fast pre-filter) ─────────────────
        blocking_candidates = await self._blocking_search(
            normalized_doc["blocking_keys"],
            exclude_id=normalized_doc.get("normalized_id"),
        )
        for hit in blocking_candidates:
            candidates[hit["_id"]] = hit

        # ── Strategy 2: Exact phone match (highest recall) ───────────────────
        if normalized_doc.get("normalized_phones"):
            phone_candidates = await self._phone_search(
                normalized_doc["normalized_phones"],
                exclude_id=normalized_doc.get("normalized_id"),
            )
            for hit in phone_candidates:
                cid = hit["_id"]
                if cid not in candidates or hit["_score"] > candidates[cid]["_score"]:
                    candidates[cid] = hit

        # ── Strategy 3: kNN vector search (if embedding enabled) ──────────────
        if settings.embedding_enabled and normalized_doc.get("name_vector"):
            vector_candidates = await self._vector_search(
                normalized_doc["name_vector"],
                district=normalized_doc.get("district"),
                exclude_id=normalized_doc.get("normalized_id"),
            )
            for hit in vector_candidates:
                cid = hit["_id"]
                if cid not in candidates:
                    candidates[cid] = hit

        # Sort by score descending, return top N
        sorted_candidates = sorted(
            candidates.values(),
            key=lambda h: h.get("_score", 0),
            reverse=True,
        )
        logger.debug(
            "candidates_retrieved",
            doc_id=normalized_doc.get("normalized_id"),
            total=len(sorted_candidates),
            capped=min(len(sorted_candidates), max_candidates),
        )
        return sorted_candidates[:max_candidates]

    async def _blocking_search(
        self,
        blocking_keys: List[str],
        exclude_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Match on shared blocking keys — lightweight first pass."""
        if not blocking_keys:
            return []

        must_not = [{"term": {"_id": exclude_id}}] if exclude_id else []
        query: Dict[str, Any] = {
            "bool": {
                "should": [
                    {"terms": {"blocking_keys": blocking_keys, "boost": 1.0}},
                ],
                "must_not": must_not,
                "minimum_should_match": 1,
            }
        }
        return await self._search(query, size=200)

    async def _phone_search(
        self,
        phones: List[str],
        exclude_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Exact match on normalized phone numbers — highest precision signal."""
        must_not = [{"term": {"_id": exclude_id}}] if exclude_id else []
        query: Dict[str, Any] = {
            "bool": {
                "should": [
                    {"terms": {"normalized_phones": phones, "boost": 10.0}},
                    {"terms": {"primary_phone": phones, "boost": 10.0}},
                ],
                "must_not": must_not,
                "minimum_should_match": 1,
            }
        }
        return await self._search(query, size=50)

    async def _vector_search(
        self,
        name_vector: List[float],
        district: Optional[str],
        exclude_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """kNN cosine similarity search on name embeddings."""
        knn_query: Dict[str, Any] = {
            "field": "name_vector",
            "query_vector": name_vector,
            "k": 50,
            "num_candidates": 200,
        }
        if district:
            knn_query["filter"] = {"term": {"district": district}}

        resp = await self.es.search(
            index=self.index,
            knn=knn_query,
            size=50,
            _source=True,
        )
        hits = resp["hits"]["hits"]
        return [h for h in hits if h["_id"] != exclude_id]

    async def _search(
        self,
        query: Dict[str, Any],
        size: int = 100,
    ) -> List[Dict[str, Any]]:
        try:
            resp = await self.es.search(
                index=self.index,
                query=query,
                size=size,
                _source=True,
                timeout="10s",
            )
            return resp["hits"]["hits"]
        except Exception as exc:
            logger.error("candidate_search_failed", error=str(exc))
            return []
