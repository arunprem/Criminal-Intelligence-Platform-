"""
Indian address normalization for Kerala police records.

Handles:
- District name canonicalization (abbreviations → full name)
- Locality / ward / panchayat extraction
- State defaults (Kerala)
- Pincode validation
- Whitespace and punctuation cleanup
- Address fingerprint generation for blocking
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, Optional, Tuple

from unidecode import unidecode

_WHITESPACE = re.compile(r"\s+")
_NON_ALPHA_NUM = re.compile(r"[^a-zA-Z0-9\s,\-/]")
_PINCODE = re.compile(r"\b(6[0-9]{5})\b")  # Kerala pincodes start with 6

# Canonical district names for Kerala
KERALA_DISTRICTS: Dict[str, str] = {
    "tvm": "thiruvananthapuram",
    "trivandrum": "thiruvananthapuram",
    "tsr": "thrissur",
    "tcr": "thrissur",
    "ernakulam": "ernakulam",
    "ekm": "ernakulam",
    "kochi": "ernakulam",
    "cochin": "ernakulam",
    "malabar": "malappuram",
    "mlp": "malappuram",
    "kozhikode": "kozhikode",
    "calicut": "kozhikode",
    "kzd": "kozhikode",
    "kollam": "kollam",
    "qlm": "kollam",
    "quilon": "kollam",
    "pathanamthitta": "pathanamthitta",
    "pta": "pathanamthitta",
    "alappuzha": "alappuzha",
    "alleppey": "alappuzha",
    "apy": "alappuzha",
    "kottayam": "kottayam",
    "ktm": "kottayam",
    "idukki": "idukki",
    "idk": "idukki",
    "palakkad": "palakkad",
    "palghat": "palakkad",
    "pkd": "palakkad",
    "thrissur": "thrissur",
    "wayanad": "wayanad",
    "wyd": "wayanad",
    "kannur": "kannur",
    "cannanore": "kannur",
    "knr": "kannur",
    "kasaragod": "kasaragod",
    "ksd": "kasaragod",
    "kasargod": "kasaragod",
}

# Address stop words
_STOP_WORDS = {
    "house", "near", "opposite", "opp", "building", "bld",
    "plot", "flat", "room", "no", "number", "ph", "po",
    "post", "office", "village", "vill", "gram",
}


def normalize_district(raw_district: Optional[str]) -> str:
    """Map any abbreviation or variant to canonical Kerala district name."""
    if not raw_district:
        return ""
    cleaned = raw_district.strip().lower()
    return KERALA_DISTRICTS.get(cleaned, cleaned)


def extract_pincode(address_text: str) -> Optional[str]:
    """Extract Kerala pincode from address text."""
    match = _PINCODE.search(address_text)
    return match.group(1) if match else None


def extract_locality(address_text: str) -> str:
    """
    Heuristic locality extraction:
    - Take first meaningful token that is not a stop word
    - After removing numbers and punctuation
    """
    if not address_text:
        return ""
    tokens = address_text.lower().split(",")
    for token in tokens:
        token = token.strip()
        words = [w for w in token.split() if w not in _STOP_WORDS and len(w) > 3]
        if words:
            return words[0]
    return ""


def normalize_address(
    raw_address: Optional[str],
    raw_district: Optional[str] = None,
) -> Tuple[str, str, str]:
    """
    Normalize an address string.

    Returns:
        normalized_address : cleaned address string
        address_locality   : extracted locality for blocking
        address_district   : canonical district name
    """
    if not raw_address:
        normalized_district = normalize_district(raw_district)
        return "", "", normalized_district

    # Transliterate Malayalam characters if present
    try:
        text = unidecode(unicodedata.normalize("NFC", raw_address))
    except Exception:
        text = raw_address

    text = _NON_ALPHA_NUM.sub(" ", text)
    text = _WHITESPACE.sub(" ", text).strip().lower()

    # Remove stop words from address for fingerprinting
    tokens = [t for t in text.split() if t not in _STOP_WORDS and len(t) > 1]
    normalized = " ".join(tokens)

    locality = extract_locality(text)
    district = normalize_district(raw_district) or _infer_district_from_address(text)

    return normalized, locality, district


def _infer_district_from_address(text: str) -> str:
    """Try to infer district from address text by keyword lookup."""
    for key, canonical in KERALA_DISTRICTS.items():
        if re.search(r"\b" + re.escape(key) + r"\b", text):
            return canonical
    return ""


def address_fingerprint(normalized_address: str) -> str:
    """
    Generate a compact fingerprint for blocking:
    sorted first 3 meaningful tokens joined.
    """
    if not normalized_address:
        return ""
    tokens = sorted(
        [t for t in normalized_address.split() if len(t) > 3][:3]
    )
    return "_".join(tokens)
