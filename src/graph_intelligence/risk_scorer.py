"""
Risk Scorer — Computes criminal risk score for master persons.

Risk factors (with weights):
  - Accused FIR count (high)
  - District mobility (medium)
  - Network centrality (medium)
  - Co-accused with high-risk persons (high)
  - Recency of activity (medium)
  - Number of aliases (low)
  - Total role diversity (low)

Score range: 0.0 (no risk) — 10.0 (maximum risk)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import AsyncElasticsearch

from src.core.config import get_settings
from src.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Maximum expected values for normalization
MAX_ACCUSED_FIRS = 20
MAX_DISTRICTS = 14  # Kerala has 14 districts
MAX_ALIASES = 10
MAX_NETWORK_SIZE = 200


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


class RiskScorer:
    """
    Computes a 0–10 risk score for a master_person based on
    criminal profile signals.
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es
        self.master_index = settings.index_master_person
        self.rel_index = settings.index_relationships

    async def compute(self, master_id: str) -> Tuple[float, List[Dict[str, Any]]]:
        """
        Compute risk score for a master person.
        Returns (score_0_to_10, list_of_risk_factors).
        """
        person = await self._get_person(master_id)
        if not person:
            return 0.0, []

        factors: List[Dict[str, Any]] = []
        raw_score = 0.0

        # ── Factor 1: Accused FIR count (weight 2.5) ──────────────────────────
        accused_firs = await self._count_firs_as_role(master_id, "accused")
        accused_score = _clamp(accused_firs / MAX_ACCUSED_FIRS) * 2.5
        factors.append({"factor": "accused_fir_count", "weight": accused_score,
                         "detail": f"{accused_firs} FIRs as accused"})
        raw_score += accused_score

        # ── Factor 2: District mobility (weight 1.5) ──────────────────────────
        districts = person.get("districts") or []
        mobility_score = _clamp(len(districts) / MAX_DISTRICTS) * 1.5
        factors.append({"factor": "district_mobility", "weight": mobility_score,
                         "detail": f"active in {len(districts)} districts"})
        raw_score += mobility_score

        # ── Factor 3: Network centrality (weight 2.0) ─────────────────────────
        centrality = person.get("centrality_score") or 0.0
        centrality_score = _clamp(centrality) * 2.0
        factors.append({"factor": "network_centrality", "weight": centrality_score,
                         "detail": f"centrality={centrality:.4f}"})
        raw_score += centrality_score

        # ── Factor 4: Co-accused with high-risk persons (weight 2.0) ──────────
        high_risk_coaccused = await self._count_high_risk_connections(master_id)
        coaccused_score = _clamp(high_risk_coaccused / 10) * 2.0
        factors.append({"factor": "high_risk_connections", "weight": coaccused_score,
                         "detail": f"{high_risk_coaccused} high-risk co-accused"})
        raw_score += coaccused_score

        # ── Factor 5: Recency (weight 1.0) ────────────────────────────────────
        last_activity = person.get("last_activity_date")
        recency_score = 0.0
        if last_activity:
            try:
                last_dt = datetime.fromisoformat(last_activity.replace("Z", "+00:00"))
                days_ago = (datetime.now(timezone.utc) - last_dt).days
                recency_score = _clamp(1.0 - (days_ago / 730)) * 1.0  # decay over 2 years
            except Exception:
                pass
        factors.append({"factor": "recency", "weight": recency_score,
                         "detail": f"last_activity={last_activity}"})
        raw_score += recency_score

        # ── Factor 6: Alias count (weight 0.5) ────────────────────────────────
        aliases = person.get("aliases") or []
        alias_score = _clamp(len(aliases) / MAX_ALIASES) * 0.5
        factors.append({"factor": "alias_count", "weight": alias_score,
                         "detail": f"{len(aliases)} aliases"})
        raw_score += alias_score

        # ── Factor 7: Role diversity (weight 0.5) ─────────────────────────────
        roles = set(r.get("role") for r in person.get("person_roles") or [] if r.get("role"))
        role_score = _clamp(len(roles) / 4) * 0.5
        factors.append({"factor": "role_diversity", "weight": role_score,
                         "detail": f"roles: {list(roles)}"})
        raw_score += role_score

        final_score = round(min(raw_score, 10.0), 2)

        # Persist risk score back to master_person
        await self._update_risk_score(master_id, final_score, factors)

        return final_score, factors

    async def _count_firs_as_role(self, master_id: str, role: str) -> int:
        resp = await self.es.search(
            index=self.master_index,
            query={
                "bool": {
                    "must": [
                        {"term": {"master_person_id": master_id}},
                        {"nested": {
                            "path": "person_roles",
                            "query": {"term": {"person_roles.role": role}},
                        }},
                    ]
                }
            },
            aggs={
                "fir_count": {
                    "nested": {"path": "person_roles"},
                    "aggs": {
                        "by_role": {
                            "filter": {"term": {"person_roles.role": role}},
                            "aggs": {
                                "unique_firs": {"cardinality": {"field": "person_roles.fir_no"}}
                            },
                        }
                    },
                }
            },
            size=0,
        )
        try:
            return resp["aggregations"]["fir_count"]["by_role"]["unique_firs"]["value"]
        except KeyError:
            return 0

    async def _count_high_risk_connections(
        self,
        master_id: str,
        high_risk_threshold: float = 6.0,
    ) -> int:
        resp = await self.es.search(
            index=self.rel_index,
            query={
                "bool": {
                    "should": [
                        {"term": {"source_master_id": master_id}},
                        {"term": {"target_master_id": master_id}},
                    ],
                    "must": [
                        {"term": {"relationship_type": "CO_ACCUSED_WITH"}},
                        {"term": {"is_active": True}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            size=200,
            _source=["source_master_id", "target_master_id"],
        )
        neighbor_ids = []
        for hit in resp["hits"]["hits"]:
            src = hit["_source"]["source_master_id"]
            tgt = hit["_source"]["target_master_id"]
            neighbor_ids.append(tgt if src == master_id else src)

        if not neighbor_ids:
            return 0

        high_risk_resp = await self.es.count(
            index=self.master_index,
            query={
                "bool": {
                    "must": [
                        {"ids": {"values": neighbor_ids}},
                        {"range": {"risk_score": {"gte": high_risk_threshold}}},
                    ]
                }
            },
        )
        return high_risk_resp["count"]

    async def _get_person(self, master_id: str) -> Optional[Dict[str, Any]]:
        try:
            resp = await self.es.get(index=self.master_index, id=master_id)
            return resp["_source"]
        except Exception:
            return None

    async def _update_risk_score(
        self,
        master_id: str,
        score: float,
        factors: List[Dict[str, Any]],
    ) -> None:
        await self.es.update(
            index=self.master_index,
            id=master_id,
            body={"doc": {"risk_score": score, "risk_factors": factors}},
            retry_on_conflict=3,
        )
