"""
Module 4 — Ad-button service.

Builds InlineKeyboardMarkup rows for the currently-active ad package.
The ad rows are appended *before* the reaction row built by publisher.py.
"""
from __future__ import annotations

from collections import defaultdict
from typing import List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from database import db


async def build_ad_rows() -> List[List[InlineKeyboardButton]]:
    """
    Return a list of InlineKeyboardButton rows for the active ad package.
    Returns an empty list if no package is active or it has no buttons.
    """
    package = await db.get_current_ad_package()
    if not package:
        return []

    buttons = await db.get_package_buttons(package["id"])
    if not buttons:
        return []

    # Group buttons by row_index (preserving insertion order within each row)
    rows_map: dict[int, list] = defaultdict(list)
    for btn in sorted(buttons, key=lambda b: (b["row_index"], b["col_index"])):
        rows_map[btn["row_index"]].append(
            InlineKeyboardButton(text=btn["label"], url=btn["url"])
        )

    return [rows_map[ri] for ri in sorted(rows_map)]
