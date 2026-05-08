"""
Entity Resolution Orchestrator.

Combines CandidateRetriever + SimilarityScorer + MergeEngine
into a single async pipeline step.
"""
from __future__ import annotations

from typing import Any, Dict

from elasticsearch import AsyncElasticsearch

from src.core.logging import get_logger
from src.entity_resolution.candidate_retriever import CandidateRetriever
from src.entity_resolution.merger import MergeEngine
from src.entity_resolution.scorer import SimilarityScorer

logger = get_logger(__name__)


class EntityResolutionPipeline:
    """
    Async entity resolution pipeline.
    Input  : normalized_person document
    Output : master_person_id + action taken
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.retriever = CandidateRetriever(es)
        self.scorer = SimilarityScorer()
        self.merger = MergeEngine(es)

    async def resolve(
        self,
        normalized_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Full resolution pipeline for one normalized_person document.
        Returns {'action': ..., 'master_person_id': ..., 'score': ...}
        """
        # Phase 1: Candidate retrieval
        candidates = await self.retriever.retrieve(normalized_doc)

        if not candidates:
            # No candidates — create new master directly
            result = await self.merger.decide_and_merge(normalized_doc, [])
            return result

        # Phase 2: Score all candidates
        scored = self.scorer.score_all(normalized_doc, candidates)

        # Phase 3: Merge decision
        result = await self.merger.decide_and_merge(normalized_doc, scored)

        logger.info(
            "entity_resolved",
            normalized_id=normalized_doc.get("normalized_id"),
            action=result["action"],
            score=result.get("score", 0),
            master_id=result.get("master_person_id"),
        )
        return result
