"""
Module 6 — Automatic content classification.

Uses jieba segmentation + keyword-weight matching to select a category.

Algorithm:
  1. Segment the text with jieba.
  2. For every category, sum the weights of keywords present in the token set.
  3. The category with the highest total score wins.
  4. Ties broken by category name alphabetically (deterministic).
  5. If no keyword matches at all, return the default category.
"""
from __future__ import annotations

import time
import jieba
from typing import List, Optional

from database import db

# Silence jieba init log
jieba.setLogLevel("WARN")

# ── Simple category cache (TTL = 30s) ─────────────────────────────────────────
# Avoids a DB round-trip on every message classification.
_CACHE_TTL = 30.0
_category_cache: list = []
_cache_ts: float = 0.0


async def _get_categories_cached() -> list:
    global _category_cache, _cache_ts
    now = time.monotonic()
    if now - _cache_ts > _CACHE_TTL:
        _category_cache = await db.get_categories()
        _cache_ts = now
    return _category_cache


def invalidate_category_cache() -> None:
    """Call this after adding/removing/updating categories so cache refreshes."""
    global _cache_ts
    _cache_ts = 0.0


async def classify_text(text: str) -> str:
    """
    Return the best-matching category name.
    Falls back to the default category name if nothing matches.
    """
    if not text or not text.strip():
        return await db.get_default_category_name()

    categories = await _get_categories_cached()
    if not categories:
        return await db.get_default_category_name()

    tokens = set(jieba.cut(text))

    best_name: str = ""
    best_score: int = 0

    for cat in categories:
        score = 0
        for kw in cat.get("keywords", []):
            word = kw.get("word", "")
            weight = kw.get("weight", 1)
            if word and word in tokens:
                score += weight
        if score > best_score or (score == best_score and score > 0 and cat["name"] < best_name):
            best_score = score
            best_name = cat["name"]

    if best_score == 0 or not best_name:
        return await db.get_default_category_name()

    return best_name


async def get_all_category_names() -> List[str]:
    cats = await _get_categories_cached()
    return [c["name"] for c in cats]
