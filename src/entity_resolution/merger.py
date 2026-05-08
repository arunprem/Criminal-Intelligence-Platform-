"""
Merge Decision Engine — Entity Resolution Phase 4.

Decides whether a normalized_person record should:
  a) Be merged into an existing master_person (score ≥ auto_merge threshold)
  b) Be queued for human review (review threshold ≤ score < auto_merge)
  c) Create a new master_person (score < review threshold)

Updates master_person atomically using ES scripted upsert.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch, ConflictError

from src.core.config import get_settings
from src.core.kafka import publish
from src.core.logging import get_logger
from src.entity_resolution.scorer import ScoredCandidate

logger = get_logger(__name__)
settings = get_settings()

_MERGE_DECISION_VERSION = "1.0.0"


class MergeEngine:
    """
    Creates or updates master_person documents based on entity resolution scores.
    Maintains full merge history for auditability.
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es
        self.master_index = settings.index_master_person
        self.normalized_index = settings.index_normalized_person

    async def decide_and_merge(
        self,
        probe_doc: Dict[str, Any],
        scored_candidates: List[ScoredCandidate],
    ) -> Dict[str, Any]:
        """
        Main entry point. Returns a result dict with:
          - action: 'merged' | 'new' | 'review'
          - master_person_id
          - score (if merged/review)
        """
        now = datetime.now(timezone.utc).isoformat()
        top = scored_candidates[0] if scored_candidates else None

        if top and top.score >= settings.er_auto_merge_threshold:
            # Auto merge into existing master
            master_id = await self._get_or_find_master(top.candidate_id)
            await self._merge_into_master(probe_doc, master_id, top, now)
            await self._update_normalized_ref(probe_doc["normalized_id"], master_id)
            await self._publish_resolved(probe_doc, master_id, "merged", top.score)
            logger.info(
                "entity_auto_merged",
                normalized_id=probe_doc["normalized_id"],
                master_id=master_id,
                score=top.score,
            )
            return {"action": "merged", "master_person_id": master_id, "score": top.score}

        elif top and top.score >= settings.er_review_threshold:
            # Queue for human review
            await publish(
                settings.kafka_topic_review_queue,
                {
                    "probe_normalized_id": probe_doc["normalized_id"],
                    "top_candidate_id": top.candidate_id,
                    "score": top.score,
                    "breakdown": top.breakdown,
                    "timestamp": now,
                },
                key=probe_doc.get("normalized_id"),
            )
            logger.info(
                "entity_queued_for_review",
                normalized_id=probe_doc["normalized_id"],
                score=top.score,
            )
            return {"action": "review", "master_person_id": None, "score": top.score}

        else:
            # Create new master person
            master_id = await self._create_master(probe_doc, now)
            await self._update_normalized_ref(probe_doc["normalized_id"], master_id)
            await self._publish_resolved(probe_doc, master_id, "new", 0.0)
            logger.info(
                "entity_new_master",
                normalized_id=probe_doc["normalized_id"],
                master_id=master_id,
            )
            return {"action": "new", "master_person_id": master_id, "score": 0.0}

    async def _get_or_find_master(self, normalized_id: str) -> str:
        """Retrieve the master_person_id linked to a normalized_person doc."""
        resp = await self.es.get(
            index=self.normalized_index, id=normalized_id, _source=["master_person_id"]
        )
        master_id = resp["_source"].get("master_person_id")
        if not master_id:
            # Candidate hasn't been resolved yet — create a new master for it
            resp_full = await self.es.get(index=self.normalized_index, id=normalized_id)
            master_id = await self._create_master(resp_full["_source"], datetime.now(timezone.utc).isoformat())
            await self._update_normalized_ref(normalized_id, master_id)
        return master_id

    async def _create_master(
        self,
        normalized_doc: Dict[str, Any],
        now: str,
    ) -> str:
        master_id = str(uuid.uuid4())
        doc: Dict[str, Any] = {
            "master_person_id": master_id,
            "status": "active",
            "primary_name": normalized_doc.get("normalized_name", ""),
            "name_variants": list({
                normalized_doc.get("normalized_name", ""),
                normalized_doc.get("transliterated_name", ""),
            } - {""}),
            "aliases": normalized_doc.get("aliases", []),
            "phonetic_primary_name": normalized_doc.get("phonetic_name", ""),
            "canonical_phone": normalized_doc.get("primary_phone"),
            "all_phones": normalized_doc.get("normalized_phones", []),
            "canonical_address": normalized_doc.get("normalized_address", ""),
            "all_addresses": [normalized_doc.get("normalized_address", "")] if normalized_doc.get("normalized_address") else [],
            "districts": [normalized_doc.get("district")] if normalized_doc.get("district") else [],
            "police_stations": [normalized_doc.get("police_station")] if normalized_doc.get("police_station") else [],
            "dob": normalized_doc.get("dob"),
            "age": normalized_doc.get("age"),
            "age_group": normalized_doc.get("age_group"),
            "gender": normalized_doc.get("gender"),
            "relative_names": normalized_doc.get("normalized_relative_names", []),
            "connected_firs": normalized_doc.get("normalized_fir_numbers", []),
            "person_roles": self._build_role_entry(normalized_doc),
            "source_documents": [{
                "index": normalized_doc.get("source_index", ""),
                "doc_id": normalized_doc.get("source_id", ""),
                "normalized_id": normalized_doc.get("normalized_id", ""),
            }],
            "risk_score": 0.0,
            "risk_factors": [],
            "gang_ids": [],
            "centrality_score": 0.0,
            "network_size": 0,
            "merge_history": [],
            "created_at": now,
            "last_updated": now,
            "last_activity_date": now,
        }
        await self.es.index(
            index=self.master_index,
            id=master_id,
            document=doc,
        )
        return master_id

    async def _merge_into_master(
        self,
        probe_doc: Dict[str, Any],
        master_id: str,
        scored: ScoredCandidate,
        now: str,
    ) -> None:
        """
        Scripted upsert to merge probe attributes into existing master_person.
        Uses painless script to append to lists without duplicates.
        """
        script = {
            "source": """
                // Merge phones
                for (phone in params.phones) {
                    if (!ctx._source.all_phones.contains(phone)) {
                        ctx._source.all_phones.add(phone);
                    }
                }
                // Merge FIRs
                for (fir in params.firs) {
                    if (!ctx._source.connected_firs.contains(fir)) {
                        ctx._source.connected_firs.add(fir);
                    }
                }
                // Merge name variants
                if (params.name != null && !ctx._source.name_variants.contains(params.name)) {
                    ctx._source.name_variants.add(params.name);
                }
                // Merge aliases
                for (alias in params.aliases) {
                    if (!ctx._source.aliases.contains(alias)) {
                        ctx._source.aliases.add(alias);
                    }
                }
                // Merge source docs
                ctx._source.source_documents.add(params.source_doc);
                // Append role
                ctx._source.person_roles.addAll(params.roles);
                // Merge districts and stations
                for (d in params.districts) {
                    if (!ctx._source.districts.contains(d)) ctx._source.districts.add(d);
                }
                for (ps in params.stations) {
                    if (!ctx._source.police_stations.contains(ps)) ctx._source.police_stations.add(ps);
                }
                // Update timestamps
                ctx._source.last_updated = params.now;
                // Append merge history entry
                ctx._source.merge_history.add(params.merge_entry);
            """,
            "lang": "painless",
            "params": {
                "phones": probe_doc.get("normalized_phones", []),
                "firs": probe_doc.get("normalized_fir_numbers", []),
                "name": probe_doc.get("normalized_name"),
                "aliases": probe_doc.get("aliases", []),
                "source_doc": {
                    "index": probe_doc.get("source_index", ""),
                    "doc_id": probe_doc.get("source_id", ""),
                    "normalized_id": probe_doc.get("normalized_id", ""),
                },
                "roles": self._build_role_entry(probe_doc),
                "districts": [probe_doc["district"]] if probe_doc.get("district") else [],
                "stations": [probe_doc["police_station"]] if probe_doc.get("police_station") else [],
                "now": now,
                "merge_entry": {
                    "merged_master_id": None,
                    "merged_at": now,
                    "merged_by": "auto_resolution",
                    "confidence_score": scored.score,
                    "reason": f"score={scored.score:.3f} breakdown={scored.breakdown}",
                },
            },
        }
        await self.es.update(
            index=self.master_index,
            id=master_id,
            body={"script": script},
            retry_on_conflict=3,
        )

    async def _update_normalized_ref(
        self,
        normalized_id: str,
        master_id: str,
    ) -> None:
        """Link normalized_person to its resolved master_person."""
        await self.es.update(
            index=self.normalized_index,
            id=normalized_id,
            body={"doc": {"master_person_id": master_id, "processing_status": "resolved"}},
            retry_on_conflict=3,
        )

    async def _publish_resolved(
        self,
        probe_doc: Dict[str, Any],
        master_id: str,
        action: str,
        score: float,
    ) -> None:
        await publish(
            settings.kafka_topic_person_resolved,
            {
                "master_person_id": master_id,
                "normalized_id": probe_doc.get("normalized_id"),
                "action": action,
                "score": score,
                "fir_numbers": probe_doc.get("normalized_fir_numbers", []),
                "source_index": probe_doc.get("source_index"),
            },
            key=master_id,
        )

    @staticmethod
    def _build_role_entry(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        role = doc.get("person_role")
        if not role:
            return []
        entries = []
        for fir in doc.get("normalized_fir_numbers") or [None]:
            entries.append({
                "role": role,
                "fir_no": fir,
                "district": doc.get("district"),
                "police_station": doc.get("police_station"),
                "date": doc.get("dob"),
            })
        return entries
