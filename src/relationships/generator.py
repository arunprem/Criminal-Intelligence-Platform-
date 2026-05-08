"""
Relationship Generator — Extracts and upserts relationships between master persons.

Relationship types generated:
  FIR-based:    ACCUSED_IN, VICTIM_IN, WITNESS_IN, COMPLAINANT_IN, CO_ACCUSED_WITH
  Shared attr:  SHARES_PHONE, SHARES_ADDRESS, RELATED_TO
  Inferred:     ASSOCIATED_WITH
"""
from __future__ import annotations

import hashlib
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import AsyncElasticsearch

from src.core.config import get_settings
from src.core.kafka import publish
from src.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

# Relationship types
class RelType:
    ACCUSED_IN = "ACCUSED_IN"
    VICTIM_IN = "VICTIM_IN"
    WITNESS_IN = "WITNESS_IN"
    COMPLAINANT_IN = "COMPLAINANT_IN"
    CO_ACCUSED_WITH = "CO_ACCUSED_WITH"
    SHARES_PHONE = "SHARES_PHONE"
    SHARES_ADDRESS = "SHARES_ADDRESS"
    RELATED_TO = "RELATED_TO"
    ASSOCIATED_WITH = "ASSOCIATED_WITH"

# Base strength scores by relationship type
STRENGTH_MAP = {
    RelType.CO_ACCUSED_WITH: 0.90,
    RelType.SHARES_PHONE: 0.85,
    RelType.RELATED_TO: 0.75,
    RelType.ACCUSED_IN: 0.60,
    RelType.VICTIM_IN: 0.50,
    RelType.SHARES_ADDRESS: 0.50,
    RelType.ASSOCIATED_WITH: 0.40,
    RelType.COMPLAINANT_IN: 0.35,
    RelType.WITNESS_IN: 0.30,
}


def _rel_id(src: str, tgt: str, rel_type: str) -> str:
    """Generate a stable, deterministic relationship_id."""
    # Always use sorted order for bidirectional dedup (except case roles)
    if rel_type in (RelType.CO_ACCUSED_WITH, RelType.SHARES_PHONE,
                    RelType.SHARES_ADDRESS, RelType.RELATED_TO,
                    RelType.ASSOCIATED_WITH):
        key = f"{min(src, tgt)}::{max(src, tgt)}::{rel_type}"
    else:
        key = f"{src}::{tgt}::{rel_type}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


class RelationshipGenerator:
    """
    Generates and incrementally upserts relationships into ES.
    Triggered when a master_person is created or updated.
    """

    def __init__(self, es: AsyncElasticsearch) -> None:
        self.es = es
        self.rel_index = settings.index_relationships
        self.event_index = settings.index_relationship_events
        self.master_index = settings.index_master_person

    async def generate_for_fir(
        self,
        fir_no: str,
        district: Optional[str] = None,
        police_station: Optional[str] = None,
    ) -> int:
        """
        Generate all relationships for a given FIR number.
        Queries master_person index for all persons linked to this FIR.
        Returns count of relationships created/updated.
        """
        now = datetime.now(timezone.utc).isoformat()

        # Fetch all master persons connected to this FIR
        persons = await self._fetch_persons_for_fir(fir_no)
        if not persons:
            logger.debug("no_persons_for_fir", fir_no=fir_no)
            return 0

        # Group by role
        role_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for person in persons:
            for role_entry in person.get("person_roles", []):
                if role_entry.get("fir_no") == fir_no:
                    role_groups[role_entry["role"]].append(person)
                    break
            else:
                # Person has this FIR but role unknown — check connected_firs
                if fir_no in (person.get("connected_firs") or []):
                    role_groups["unknown"].append(person)

        rels_created = 0

        # 1. Person → FIR role relationships (person IS accused/victim/etc IN fir)
        for role, role_persons in role_groups.items():
            for person in role_persons:
                rel_type = self._role_to_rel_type(role)
                if rel_type:
                    created = await self._upsert_relationship(
                        source_id=person["master_person_id"],
                        target_id=fir_no,          # FIR as target pseudo-entity
                        rel_type=rel_type,
                        fir_no=fir_no,
                        evidence_index=self.master_index,
                        evidence_doc_id=person["master_person_id"],
                        district=district,
                        police_station=police_station,
                        now=now,
                    )
                    rels_created += int(created)

        # 2. CO_ACCUSED_WITH relationships (accused × accused)
        accused_list = role_groups.get("accused", [])
        for i, acc_a in enumerate(accused_list):
            for acc_b in accused_list[i + 1:]:
                created = await self._upsert_relationship(
                    source_id=acc_a["master_person_id"],
                    target_id=acc_b["master_person_id"],
                    rel_type=RelType.CO_ACCUSED_WITH,
                    fir_no=fir_no,
                    evidence_index=self.master_index,
                    evidence_doc_id=acc_a["master_person_id"],
                    district=district,
                    police_station=police_station,
                    now=now,
                )
                rels_created += int(created)

        # 3. ASSOCIATED_WITH (accused × victim, accused × complainant)
        for acc in role_groups.get("accused", []):
            for vic in role_groups.get("victim", []) + role_groups.get("complainant", []):
                created = await self._upsert_relationship(
                    source_id=acc["master_person_id"],
                    target_id=vic["master_person_id"],
                    rel_type=RelType.ASSOCIATED_WITH,
                    fir_no=fir_no,
                    evidence_index=self.master_index,
                    evidence_doc_id=acc["master_person_id"],
                    district=district,
                    police_station=police_station,
                    now=now,
                )
                rels_created += int(created)

        logger.info(
            "fir_relationships_generated",
            fir_no=fir_no,
            persons=len(persons),
            relationships=rels_created,
        )
        return rels_created

    async def generate_shared_attributes(self, master_id: str) -> int:
        """
        Generate SHARES_PHONE, SHARES_ADDRESS, RELATED_TO for a master person
        by querying for others sharing the same attributes.
        """
        now = datetime.now(timezone.utc).isoformat()
        person = await self._get_master(master_id)
        if not person:
            return 0

        rels_created = 0

        # SHARES_PHONE
        for phone in person.get("all_phones") or []:
            sharers = await self._find_by_phone(phone, exclude_id=master_id)
            for sharer in sharers:
                created = await self._upsert_relationship(
                    source_id=master_id,
                    target_id=sharer["master_person_id"],
                    rel_type=RelType.SHARES_PHONE,
                    fir_no=None,
                    evidence_index=self.master_index,
                    evidence_doc_id=master_id,
                    evidence_field="all_phones",
                    evidence_value=phone,
                    now=now,
                )
                rels_created += int(created)

        # RELATED_TO (shared relative names as proxy for family relationship)
        for rel_name in person.get("relative_names") or []:
            if len(rel_name) < 4:
                continue
            related = await self._find_by_relative_name(rel_name, exclude_id=master_id)
            for r in related:
                created = await self._upsert_relationship(
                    source_id=master_id,
                    target_id=r["master_person_id"],
                    rel_type=RelType.RELATED_TO,
                    fir_no=None,
                    evidence_index=self.master_index,
                    evidence_doc_id=master_id,
                    evidence_field="relative_names",
                    evidence_value=rel_name,
                    now=now,
                )
                rels_created += int(created)

        return rels_created

    # ── Private helpers ────────────────────────────────────────────────────────

    async def _upsert_relationship(
        self,
        source_id: str,
        target_id: str,
        rel_type: str,
        fir_no: Optional[str],
        evidence_index: str,
        evidence_doc_id: str,
        evidence_field: Optional[str] = None,
        evidence_value: Optional[str] = None,
        district: Optional[str] = None,
        police_station: Optional[str] = None,
        now: str = "",
    ) -> bool:
        """
        Upsert relationship document. Increments occurrence_count and
        updates last_seen on existing. Creates new on first occurrence.
        Returns True if relationship was newly created.
        """
        rel_id = _rel_id(source_id, target_id, rel_type)
        base_strength = STRENGTH_MAP.get(rel_type, 0.3)

        evidence = {
            "index": evidence_index,
            "doc_id": evidence_doc_id,
            "field": evidence_field or "fir",
            "value": evidence_value or fir_no or "",
        }

        script = {
            "source": """
                ctx._source.occurrence_count += 1;
                ctx._source.last_seen = params.now;
                ctx._source.strength = Math.min(1.0, ctx._source.strength + 0.02);
                if (params.fir_no != null && !ctx._source.fir_numbers.contains(params.fir_no)) {
                    ctx._source.fir_numbers.add(params.fir_no);
                }
                ctx._source.evidence_sources.add(params.evidence);
                ctx._source.updated_at = params.now;
            """,
            "lang": "painless",
            "params": {
                "now": now,
                "fir_no": fir_no,
                "evidence": evidence,
            },
        }

        upsert_doc = {
            "relationship_id": rel_id,
            "source_master_id": source_id,
            "target_master_id": target_id,
            "relationship_type": rel_type,
            "strength": base_strength,
            "fir_numbers": [fir_no] if fir_no else [],
            "districts": [district] if district else [],
            "police_stations": [police_station] if police_station else [],
            "evidence_sources": [evidence],
            "occurrence_count": 1,
            "first_seen": now,
            "last_seen": now,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }

        resp = await self.es.update(
            index=self.rel_index,
            id=rel_id,
            body={"script": script, "upsert": upsert_doc},
            retry_on_conflict=3,
        )

        is_new = resp.get("result") == "created"

        # Log relationship event (immutable audit)
        await self._log_event(
            rel_id=rel_id,
            source_id=source_id,
            target_id=target_id,
            rel_type=rel_type,
            event_type="CREATED" if is_new else "STRENGTHENED",
            fir_no=fir_no,
            evidence=evidence,
            now=now,
        )

        # Publish to Kafka
        await publish(
            settings.kafka_topic_relationships_generated,
            {
                "relationship_id": rel_id,
                "source_master_id": source_id,
                "target_master_id": target_id,
                "relationship_type": rel_type,
                "is_new": is_new,
            },
            key=rel_id,
        )
        return is_new

    async def _log_event(
        self,
        rel_id: str,
        source_id: str,
        target_id: str,
        rel_type: str,
        event_type: str,
        fir_no: Optional[str],
        evidence: Dict[str, Any],
        now: str,
    ) -> None:
        event_doc = {
            "event_id": str(uuid.uuid4()),
            "relationship_id": rel_id,
            "source_master_id": source_id,
            "target_master_id": target_id,
            "relationship_type": rel_type,
            "event_type": event_type,
            "fir_no": fir_no,
            "source_doc_ref": {
                "index": evidence.get("index", ""),
                "doc_id": evidence.get("doc_id", ""),
            },
            "evidence_field": evidence.get("field"),
            "evidence_value": evidence.get("value"),
            "strength_delta": 0.02,
            "timestamp": now,
            "processed_by": "relationship_generator_v1",
        }
        await self.es.index(index=self.event_index, document=event_doc)

    async def _fetch_persons_for_fir(self, fir_no: str) -> List[Dict[str, Any]]:
        resp = await self.es.search(
            index=self.master_index,
            query={"term": {"connected_firs": fir_no}},
            size=500,
        )
        return [h["_source"] for h in resp["hits"]["hits"]]

    async def _get_master(self, master_id: str) -> Optional[Dict[str, Any]]:
        try:
            resp = await self.es.get(index=self.master_index, id=master_id)
            return resp["_source"]
        except Exception:
            return None

    async def _find_by_phone(
        self, phone: str, exclude_id: str
    ) -> List[Dict[str, Any]]:
        resp = await self.es.search(
            index=self.master_index,
            query={
                "bool": {
                    "must": [{"term": {"all_phones": phone}}],
                    "must_not": [{"term": {"master_person_id": exclude_id}}],
                }
            },
            size=50,
        )
        return [h["_source"] for h in resp["hits"]["hits"]]

    async def _find_by_relative_name(
        self, rel_name: str, exclude_id: str
    ) -> List[Dict[str, Any]]:
        resp = await self.es.search(
            index=self.master_index,
            query={
                "bool": {
                    "must": [{"match": {"relative_names": {"query": rel_name, "fuzziness": "AUTO"}}}],
                    "must_not": [{"term": {"master_person_id": exclude_id}}],
                }
            },
            size=20,
        )
        return [h["_source"] for h in resp["hits"]["hits"]]

    @staticmethod
    def _role_to_rel_type(role: str) -> Optional[str]:
        return {
            "accused": RelType.ACCUSED_IN,
            "victim": RelType.VICTIM_IN,
            "witness": RelType.WITNESS_IN,
            "complainant": RelType.COMPLAINANT_IN,
        }.get(role.lower())
