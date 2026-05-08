"""
Normalization Pipeline Orchestrator.

Transforms a raw document from any source index into a normalized_person record.
Handles field extraction with graceful fallbacks for varying raw document structures.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.core.config import get_settings
from src.core.logging import get_logger
from src.normalization.address_normalizer import normalize_address
from src.normalization.blocking import compute_age_group, generate_blocking_keys
from src.normalization.fir_normalizer import normalize_firs
from src.normalization.name_normalizer import (
    normalize_aliases,
    normalize_name,
    get_name_prefix,
)
from src.normalization.phone_normalizer import normalize_phones

logger = get_logger(__name__)
settings = get_settings()

NORMALIZATION_VERSION = "1.0.0"


class NormalizationPipeline:
    """
    Stateless pipeline that converts a raw police record into a
    normalized_person document ready for Elasticsearch indexing.
    """

    def process(
        self,
        raw_doc: Dict[str, Any],
        source_index: str,
        source_id: str,
    ) -> Dict[str, Any]:
        """
        Main entry point. Returns a normalized_person dict or raises ValueError.
        """
        try:
            return self._build_normalized(raw_doc, source_index, source_id)
        except Exception as exc:
            logger.error(
                "normalization_failed",
                source_index=source_index,
                source_id=source_id,
                error=str(exc),
                exc_info=True,
            )
            raise

    def _build_normalized(
        self,
        raw: Dict[str, Any],
        source_index: str,
        source_id: str,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()

        # ── 1. Extract raw fields ──────────────────────────────────────────────
        person_info = raw.get("personInformation", raw.get("person_information", {})) or {}
        case_info = raw.get("caseInfodetails", raw.get("case_info_details", {})) or {}

        raw_name = (
            person_info.get("name")
            or person_info.get("personName")
            or raw.get("name", "")
        )
        raw_aliases: List[str] = (
            person_info.get("aliases", [])
            or raw.get("aliases", [])
            or []
        )
        raw_phones: List[str] = self._extract_phones(raw, person_info)
        raw_addresses: List[str] = self._extract_addresses(raw, person_info)
        raw_district = (
            raw.get("district")
            or case_info.get("district")
            or person_info.get("district", "")
        )
        raw_station = (
            raw.get("policeStation")
            or raw.get("police_station")
            or case_info.get("policeStation", "")
        )
        raw_firs: List[str] = self._extract_firs(raw, case_info)
        raw_relative_names: List[str] = self._extract_relative_names(raw, person_info)
        raw_dob = person_info.get("dob") or person_info.get("dateOfBirth") or raw.get("dob")
        raw_age = person_info.get("age") or raw.get("age")
        gender = (person_info.get("gender") or raw.get("gender", "")).upper()
        person_role = self._infer_role(source_index, raw)

        # ── 2. Normalize fields ───────────────────────────────────────────────
        normalized_name, phonetic_name, transliterated_name = normalize_name(raw_name)
        normalized_aliases = normalize_aliases(raw_aliases)
        normalized_phones = normalize_phones(raw_phones)
        primary_phone = normalized_phones[0] if normalized_phones else None

        # Address: use primary address entry
        primary_raw_addr = raw_addresses[0] if raw_addresses else ""
        normalized_address, address_locality, address_district = normalize_address(
            primary_raw_addr, raw_district
        )
        district = address_district or _safe_str(raw_district)
        police_station = _safe_str(raw_station)

        normalized_fir_numbers = normalize_firs(raw_firs, police_station[:10] if police_station else None)
        normalized_relative_names, relative_phonetics = self._normalize_relatives(raw_relative_names)

        age = _parse_age(raw_age)
        age_group = compute_age_group(age)
        name_prefix = get_name_prefix(normalized_name)
        phone_prefix = primary_phone[:7] if primary_phone else None

        # ── 3. Generate blocking keys ─────────────────────────────────────────
        blocking_keys = generate_blocking_keys(
            normalized_name=normalized_name,
            phonetic_name=phonetic_name,
            normalized_phones=normalized_phones,
            district=district,
            police_station=police_station,
            address_locality=address_locality,
            age=age,
            age_group=age_group,
        )

        # ── 4. Generate stable normalized_id ──────────────────────────────────
        normalized_id = self._generate_id(source_index, source_id)

        return {
            "_id": normalized_id,
            "normalized_id": normalized_id,
            "source_index": source_index,
            "source_id": source_id,
            "person_role": person_role,
            "master_person_id": None,  # filled by entity resolution
            # Name
            "normalized_name": normalized_name,
            "phonetic_name": phonetic_name,
            "transliterated_name": transliterated_name,
            "name_prefix": name_prefix,
            # Phones
            "normalized_phones": normalized_phones,
            "primary_phone": primary_phone,
            "phone_prefix": phone_prefix,
            # Address
            "normalized_address": normalized_address,
            "address_locality": address_locality,
            "address_district": district,
            "address_state": "kerala",
            "address_pincode": None,
            # Relatives
            "normalized_relative_names": normalized_relative_names,
            "relative_name_phonetics": relative_phonetics,
            # FIRs
            "fir_numbers": raw_firs,
            "normalized_fir_numbers": normalized_fir_numbers,
            "police_station": police_station,
            "district": district,
            # Demographics
            "dob": _parse_dob(raw_dob),
            "age": age,
            "age_group": age_group,
            "gender": gender if gender in ("M", "F", "MALE", "FEMALE", "OTHER") else None,
            # Aliases
            "aliases": normalized_aliases,
            # Blocking
            "blocking_keys": blocking_keys,
            # Metadata
            "raw_doc_ref": {"index": source_index, "id": source_id},
            "processing_status": "normalized",
            "normalization_version": NORMALIZATION_VERSION,
            "created_at": now,
            "updated_at": now,
        }

    # ── Private helpers ────────────────────────────────────────────────────────

    def _extract_phones(
        self, raw: Dict[str, Any], person_info: Dict[str, Any]
    ) -> List[str]:
        phones: List[str] = []
        # Check mobileNumber field (single or list)
        mobile = raw.get("mobileNumber") or person_info.get("mobileNumber") or person_info.get("mobile")
        if mobile:
            if isinstance(mobile, list):
                phones.extend(str(m) for m in mobile if m)
            else:
                phones.append(str(mobile))
        # Check phones array
        phones_arr = raw.get("phones") or person_info.get("phones") or []
        if isinstance(phones_arr, list):
            phones.extend(str(p) for p in phones_arr if p)
        return phones

    def _extract_addresses(
        self, raw: Dict[str, Any], person_info: Dict[str, Any]
    ) -> List[str]:
        addresses: List[str] = []
        addr = raw.get("addresses") or person_info.get("addresses") or []
        if isinstance(addr, list):
            for a in addr:
                if isinstance(a, dict):
                    parts = [
                        a.get("houseNo", ""),
                        a.get("street", ""),
                        a.get("locality", ""),
                        a.get("ward", ""),
                        a.get("panchayat", ""),
                        a.get("municipality", ""),
                        a.get("district", ""),
                        a.get("state", ""),
                        a.get("pincode", ""),
                    ]
                    addresses.append(" ".join(p for p in parts if p))
                elif isinstance(a, str) and a.strip():
                    addresses.append(a)
        elif isinstance(addr, str) and addr.strip():
            addresses.append(addr)

        # Also check flat address field
        flat = raw.get("address") or person_info.get("address")
        if flat and isinstance(flat, str):
            addresses.append(flat)
        return addresses

    def _extract_firs(
        self, raw: Dict[str, Any], case_info: Dict[str, Any]
    ) -> List[str]:
        firs: List[str] = []
        fir = case_info.get("firNo") or case_info.get("fir_no") or raw.get("firNo") or raw.get("fir_no")
        if fir:
            firs.append(str(fir))
        fir_list = raw.get("fir_numbers") or case_info.get("fir_numbers") or []
        if isinstance(fir_list, list):
            firs.extend(str(f) for f in fir_list if f)
        return list(set(firs))

    def _extract_relative_names(
        self, raw: Dict[str, Any], person_info: Dict[str, Any]
    ) -> List[str]:
        names: List[str] = []
        # Father's name
        for field in ("fatherName", "father_name", "husbandName", "husband_name",
                      "spouseName", "spouse_name", "guardianName"):
            val = person_info.get(field) or raw.get(field)
            if val:
                names.append(str(val))
        # Hierarchy / relationship names
        hierarchy = raw.get("hierarchy") or raw.get("relationshipNames") or []
        if isinstance(hierarchy, list):
            names.extend(str(h) for h in hierarchy if h and isinstance(h, str))
        elif isinstance(hierarchy, dict):
            names.extend(str(v) for v in hierarchy.values() if v)
        return names

    def _normalize_relatives(
        self, relative_names: List[str]
    ) -> Tuple[List[str], List[str]]:
        from src.normalization.name_normalizer import normalize_name
        from metaphone import doublemetaphone

        normalized = []
        phonetics = []
        for name in relative_names:
            norm, phon, _ = normalize_name(name)
            if norm:
                normalized.append(norm)
                if phon:
                    phonetics.append(phon)
        return list(set(normalized)), list(set(phonetics))

    def _infer_role(self, source_index: str, raw: Dict[str, Any]) -> str:
        role_map = {
            "accused": "accused",
            "victim": "victim",
            "complainant": "complainant",
            "witness": "witness",
        }
        for key, role in role_map.items():
            if key in source_index.lower():
                return role
        return raw.get("personRole") or raw.get("role") or raw.get("type") or "unknown"

    @staticmethod
    def _generate_id(source_index: str, source_id: str) -> str:
        """Generate a stable normalized_id from source index + source id."""
        raw = f"{source_index}::{source_id}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ── Utility functions ──────────────────────────────────────────────────────────

def _safe_str(val: Any) -> str:
    return str(val).strip().lower() if val else ""


def _parse_age(val: Any) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _parse_dob(val: Any) -> Optional[str]:
    if not val:
        return None
    if isinstance(val, str):
        # Normalize common date formats
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                from datetime import datetime
                return datetime.strptime(val.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None
