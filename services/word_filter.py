"""
Module 7 — Banned-word filter.

Scope: user submissions only.  Admin messages and cross-channel collected
content are never passed through this filter.

Algorithm:
  - Exact match: replace the word with equal-length '*' characters.
  - Fuzzy / root match: if a bad-word has fuzzy_match=1, any string that
    *contains* the word as a substring is also masked.

The word list is cached in memory and refreshed every call to reduce DB
round-trips.  Because bad words change rarely, a simple module-level cache
with a TTL is good enough.
"""
from __future__ import annotations

import re
import time
from typing import List, Dict

from database import db

_CACHE_TTL = 60  # seconds
_cache: List[Dict] = []
_cache_ts: float = 0.0


async def _get_words() -> List[Dict]:
    global _cache, _cache_ts
    if time.monotonic() - _cache_ts > _CACHE_TTL:
        _cache = await db.get_bad_words()
        _cache_ts = time.monotonic()
    return _cache


async def filter_text(text: str) -> str:
    """
    Replace all bad words in *text* with equal-length '*' strings.
    Returns the (possibly unchanged) string.
    """
    if not text:
        return text

    words = await _get_words()
    result = text

    for entry in words:
        word = entry["word"]
        if not word:
            continue

        if entry["fuzzy_match"]:
            # Substring / root match — find all occurrences
            pattern = re.compile(re.escape(word), re.IGNORECASE)
        else:
            # Exact word boundary match
            pattern = re.compile(
                r"(?<!\w)" + re.escape(word) + r"(?!\w)", re.IGNORECASE
            )

        def _mask(m: re.Match) -> str:
            return "*" * len(m.group())

        result = pattern.sub(_mask, result)

    return result


def invalidate_cache() -> None:
    """Force next call to reload from DB (call after adding/removing words)."""
    global _cache_ts
    _cache_ts = 0.0
