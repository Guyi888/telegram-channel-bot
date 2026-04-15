"""
Miscellaneous shared utilities.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from telegram import Message, User


def format_user(user: User) -> str:
    """Return '@username (ID)' or 'ID' when no username."""
    if user.username:
        return f"@{user.username} (ID: {user.id})"
    name = user.full_name or str(user.id)
    return f"{name} (ID: {user.id})"


def format_ts(ts: datetime | str | None) -> str:
    if ts is None:
        return "—"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return ts
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def content_type_label(msg: Message) -> str:
    """Return a human-readable Chinese content-type label for a Message."""
    if msg.photo:
        return "图片"
    if msg.video:
        return "视频"
    if msg.document:
        return "文件"
    if msg.audio:
        return "音频"
    if msg.voice:
        return "语音"
    if msg.sticker:
        return "贴纸"
    if msg.animation:
        return "动图"
    if msg.text:
        return "文字"
    return "未知"


def extract_text(msg: Message) -> str:
    """Return text or caption from a message (may be empty string)."""
    return msg.text or msg.caption or ""


def paginate(items: list, page: int, page_size: int = 10):
    """Return a slice of items for the given 1-based page."""
    start = (page - 1) * page_size
    return items[start: start + page_size], len(items)


def escape_html(text: str) -> str:
    """Minimal HTML escaping for safe use in HTML parse-mode messages."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )
