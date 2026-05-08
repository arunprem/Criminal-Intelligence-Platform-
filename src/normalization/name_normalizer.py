"""
Malayalam and English name normalization.

Handles:
- Unicode NFC normalization
- Lowercase + whitespace cleanup
- Malayalam → Roman transliteration (ISO-15919 / ITRANS)
- Double Metaphone phonetic encoding
- Common nickname/alias expansion
"""
from __future__ import annotations

import re
import unicodedata
from typing import List, Optional, Tuple

import jellyfish
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate
from metaphone import doublemetaphone
from unidecode import unidecode

# Malayalam Unicode range U+0D00–U+0D7F
_MALAYALAM_PATTERN = re.compile(r"[\u0D00-\u0D7F]")
_WHITESPACE = re.compile(r"\s+")
_NON_ALPHA = re.compile(r"[^a-zA-Z\u0D00-\u0D7F\s]")

# Common title/prefix words to strip
_TITLES = {
    "mr", "mrs", "ms", "dr", "sri", "smt", "shri", "km",
    "adv", "prof", "rev", "er", "fr", "br", "sis",
}

# Common Malayalam name prefixes / suffix expansions
_MALAYALAM_NICKNAMES: dict[str, str] = {
    "ക്കൻ": "krishnan",
    "ഗണേഷ്": "ganesh",
    "മുഹമ്മദ്": "mohammed",
    "മോഹൻ": "mohan",
    "ജോൺ": "john",
    "ജോസ്": "jose",
    "തോമസ്": "thomas",
}


def _nfc(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def _has_malayalam(text: str) -> bool:
    return bool(_MALAYALAM_PATTERN.search(text))


def transliterate_malayalam(text: str) -> str:
    """
    Transliterate Malayalam script to Roman (ISO-15919).
    Falls back to unidecode if indic-transliteration fails.
    """
    if not _has_malayalam(text):
        return text
    try:
        roman = transliterate(text, sanscript.MALAYALAM, sanscript.ITRANS)
        # Post-process: remove non-alpha, lowercase
        roman = re.sub(r"[^a-zA-Z\s]", " ", roman)
        return _WHITESPACE.sub(" ", roman).strip().lower()
    except Exception:
        return unidecode(text).lower().strip()


def normalize_name(raw_name: Optional[str]) -> Tuple[str, str, str]:
    """
    Normalize a person name (Malayalam or English).

    Returns:
        normalized_name    : cleaned ASCII name
        phonetic_name      : Double Metaphone primary code
        transliterated_name: Roman transliteration (if Malayalam input)
    """
    if not raw_name or not raw_name.strip():
        return "", "", ""

    text = _nfc(raw_name.strip())

    # Step 1 – Transliterate if Malayalam
    transliterated = ""
    if _has_malayalam(text):
        transliterated = transliterate_malayalam(text)
        working = transliterated
    else:
        working = unidecode(text)  # handle diacritics in Roman script

    # Step 2 – Lowercase, strip non-alpha, collapse whitespace
    working = working.lower()
    working = _NON_ALPHA.sub(" ", working)
    working = _WHITESPACE.sub(" ", working).strip()

    # Step 3 – Remove title words
    tokens = [t for t in working.split() if t not in _TITLES]
    normalized = " ".join(tokens)

    # Step 4 – Phonetic encoding (Double Metaphone on first 2 tokens)
    name_tokens = normalized.split()[:2]
    phonetic_parts = []
    for token in name_tokens:
        primary, _secondary = doublemetaphone(token)
        if primary:
            phonetic_parts.append(primary)
    phonetic = " ".join(phonetic_parts)

    return normalized, phonetic, transliterated


def normalize_aliases(aliases: List[str]) -> List[str]:
    """Normalize a list of alias strings."""
    result = []
    for alias in aliases:
        norm, _, _ = normalize_name(alias)
        if norm:
            result.append(norm)
    return list(set(result))


def get_name_prefix(normalized_name: str, length: int = 4) -> str:
    """Extract first N chars of first token for blocking."""
    if not normalized_name:
        return ""
    first_token = normalized_name.split()[0]
    return first_token[:length]


def levenshtein_similarity(a: str, b: str) -> float:
    """Return Levenshtein similarity score in [0, 1]."""
    if not a or not b:
        return 0.0
    dist = jellyfish.levenshtein_distance(a, b)
    max_len = max(len(a), len(b))
    return 1.0 - (dist / max_len)


def jaro_winkler_similarity(a: str, b: str) -> float:
    """Return Jaro-Winkler similarity score in [0, 1]."""
    if not a or not b:
        return 0.0
    return jellyfish.jaro_winkler_similarity(a, b)


def name_similarity(name_a: str, name_b: str) -> float:
    """
    Combined name similarity: average of Jaro-Winkler and
    phonetic exact match bonus.
    """
    if not name_a or not name_b:
        return 0.0
    jw = jaro_winkler_similarity(name_a, name_b)
    # Phonetic bonus
    ph_a = doublemetaphone(name_a.split()[0])[0] if name_a else ""
    ph_b = doublemetaphone(name_b.split()[0])[0] if name_b else ""
    phonetic_match = 1.0 if ph_a and ph_a == ph_b else 0.0
    return round((jw * 0.6) + (phonetic_match * 0.4), 4)
