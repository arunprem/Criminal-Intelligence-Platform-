"""
Indian phone number normalization.

Rules:
- Strip country code +91 / 0091 / 0
- Remove spaces, dashes, dots, parentheses
- Validate 10-digit Indian mobile format
- Output canonical 12-digit form: 91XXXXXXXXXX
"""
from __future__ import annotations

import re
from typing import List, Optional

import phonenumbers

_STRIP_CHARS = re.compile(r"[\s\-\.\(\)\/]")
_COUNTRY_PREFIXES = re.compile(r"^(?:\+91|0091|91|0)(?=\d{10}$)")
_VALID_MOBILE = re.compile(r"^[6-9]\d{9}$")  # Indian mobile: starts 6-9


def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """
    Normalize a single phone number to canonical 91XXXXXXXXXX form.
    Returns None if invalid.
    """
    if not raw:
        return None

    cleaned = _STRIP_CHARS.sub("", str(raw).strip())
    cleaned = _COUNTRY_PREFIXES.sub("", cleaned)

    if not _VALID_MOBILE.match(cleaned):
        # Try phonenumbers library as fallback
        try:
            parsed = phonenumbers.parse(raw, "IN")
            if phonenumbers.is_valid_number(parsed):
                national = str(parsed.national_number)
                if _VALID_MOBILE.match(national):
                    return f"91{national}"
        except Exception:
            pass
        return None

    return f"91{cleaned}"


def normalize_phones(raw_phones: List[Optional[str]]) -> List[str]:
    """
    Normalize a list of raw phone numbers.
    Returns deduplicated list of valid canonical numbers.
    """
    result = set()
    for raw in raw_phones:
        normalized = normalize_phone(raw)
        if normalized:
            result.add(normalized)
    return sorted(result)


def get_phone_prefix(canonical_phone: Optional[str], length: int = 7) -> Optional[str]:
    """
    Return first N digits of a canonical phone number for blocking.
    E.g. '919876543210' → '9198765' (first 7 digits)
    """
    if not canonical_phone or len(canonical_phone) < length:
        return None
    return canonical_phone[:length]


def phones_match(phone_a: Optional[str], phone_b: Optional[str]) -> bool:
    """Exact match on canonical forms."""
    if not phone_a or not phone_b:
        return False
    return phone_a == phone_b


def phones_overlap(phones_a: List[str], phones_b: List[str]) -> bool:
    """Check if any phone number appears in both lists."""
    return bool(set(phones_a) & set(phones_b))
