"""
Blocking key generator.

Generates multiple blocking keys per normalized person record.
These keys dramatically reduce the candidate space for entity resolution
by ensuring similar records share at least one key (recall guarantee).

Key families:
1. Phone prefix    → 7-digit prefix of canonical phone
2. District+Name   → district + first 4 chars of name
3. Station+Age     → police station code + age group
4. Locality+Name   → locality + first 4 chars of name
5. Phonetic        → Double Metaphone code of name
6. Name prefix     → first 4 chars of normalized name (broad)
"""
from __future__ import annotations

import re
from typing import List, Optional

from src.normalization.phone_normalizer import get_phone_prefix

_SAFE = re.compile(r"[^a-z0-9]")


def _safe(s: str) -> str:
    """Make a string safe for use as a blocking key token."""
    return _SAFE.sub("", s.lower().strip())


def compute_age_group(age: Optional[int]) -> str:
    """Bucket age into 5-year groups for blocking."""
    if not age or age <= 0:
        return "unknown"
    bucket = (age // 5) * 5
    return f"{bucket}to{bucket + 4}"


def generate_blocking_keys(
    normalized_name: str,
    phonetic_name: str,
    normalized_phones: List[str],
    district: str,
    police_station: str,
    address_locality: str,
    age: Optional[int] = None,
    age_group: Optional[str] = None,
) -> List[str]:
    """
    Generate all blocking keys for a normalized person record.
    Returns a deduplicated list of blocking key strings.
    """
    keys: set[str] = set()

    name_prefix = _safe(normalized_name)[:4] if normalized_name else ""
    safe_district = _safe(district)
    safe_station = _safe(police_station)
    safe_locality = _safe(address_locality)
    safe_phonetic = _safe(phonetic_name) if phonetic_name else ""
    ag = age_group or compute_age_group(age)
    safe_ag = _safe(ag)

    # ── Family 1: Phone prefix blocks ────────────────────────────────────────
    for phone in normalized_phones:
        prefix = get_phone_prefix(phone, length=7)
        if prefix:
            keys.add(f"ph_{prefix}")
            # Phone + district (high specificity)
            if safe_district:
                keys.add(f"ph_{prefix}_dist_{safe_district}")

    # ── Family 2: District + Name prefix ─────────────────────────────────────
    if safe_district and name_prefix:
        keys.add(f"dist_{safe_district}_name_{name_prefix}")

    # ── Family 3: Station + Age group ────────────────────────────────────────
    if safe_station and safe_ag and safe_ag != "unknown":
        keys.add(f"ps_{safe_station}_age_{safe_ag}")

    # ── Family 4: Locality + Name prefix ─────────────────────────────────────
    if safe_locality and name_prefix:
        keys.add(f"loc_{safe_locality}_name_{name_prefix}")

    # ── Family 5: Phonetic code ───────────────────────────────────────────────
    if safe_phonetic:
        keys.add(f"phon_{safe_phonetic}")
        if safe_district:
            keys.add(f"phon_{safe_phonetic}_dist_{safe_district}")

    # ── Family 6: Broad name prefix (catch-all) ───────────────────────────────
    if name_prefix:
        keys.add(f"name_{name_prefix}")

    return sorted(keys)
