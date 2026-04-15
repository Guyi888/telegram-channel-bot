"""
Publisher service — the single place where messages are sent to the target
channel.

Every published message gets:
  1. Ad buttons (Module 4) — zero or more rows above
  2. Reaction buttons (Module 5) — one fixed row at the bottom
  3. Category tag (Module 6) — appended to caption/text as  #类别名

The returned message_id can be stored in the message_map table.
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional, Tuple

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from telegram.error import TelegramError, RetryAfter

from database import db
from services.ad_service import build_ad_rows
from services.classifier import classify_text

logger = logging.getLogger(__name__)


# ── Reaction keyboard row ─────────────────────────────────────────────────────

def _reaction_row(
    message_id: int,
    likes: int,
    dislikes: int,
    discussion_url: Optional[str],
) -> List[InlineKeyboardButton]:
    row = [
        InlineKeyboardButton(
            f"👍 {likes}" if likes else "👍",
            callback_data=f"react:like:{message_id}",
        ),
    ]
    if discussion_url:
        row.append(InlineKeyboardButton("💬 讨论一下", url=discussion_url))
    row.append(
        InlineKeyboardButton(
            f"👎 {dislikes}" if dislikes else "👎",
            callback_data=f"react:dislike:{message_id}",
        )
    )
    return row


async def build_reply_markup(
    message_id: int, likes: int = 0, dislikes: int = 0
) -> InlineKeyboardMarkup:
    """Build the full InlineKeyboardMarkup for a channel post."""
    discussion_url = await db.get_discussion_group()
    ad_rows = await build_ad_rows()
    reaction = _reaction_row(message_id, likes, dislikes, discussion_url)

    keyboard = ad_rows + [reaction]
    return InlineKeyboardMarkup(keyboard)


# ── Core publish helpers ──────────────────────────────────────────────────────

async def _send_with_retry(coro, retries: int = 3):
    """Wrap a send coroutine with RetryAfter / flood-wait handling."""
    for attempt in range(retries):
        try:
            return await coro
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning("Flood wait %ss — sleeping", wait)
            await asyncio.sleep(wait)
        except TelegramError as e:
            logger.error("Telegram error (attempt %s): %s", attempt + 1, e)
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2)


async def _append_category(text: str | None, category: str) -> str:
    tag = f"\n\n#{category}" if category else ""
    return (text or "") + tag


# ── Public API ────────────────────────────────────────────────────────────────

async def publish_text(
    bot: Bot,
    target_channel: str,
    text: str,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    """Publish a plain-text message. Returns the new message_id."""
    if not category:
        category = await classify_text(text)
    final_text = await _append_category(text, category)

    msg = await _send_with_retry(
        bot.send_message(
            chat_id=target_channel,
            text=final_text,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    await bot.edit_message_reply_markup(
        chat_id=target_channel,
        message_id=msg.message_id,
        reply_markup=markup,
    )
    return msg.message_id


async def publish_photo(
    bot: Bot,
    target_channel: str,
    file_id: str,
    caption: str | None = None,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    if not category:
        category = await classify_text(caption or "")
    final_caption = await _append_category(caption, category)

    msg = await _send_with_retry(
        bot.send_photo(
            chat_id=target_channel,
            photo=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    await bot.edit_message_reply_markup(
        chat_id=target_channel,
        message_id=msg.message_id,
        reply_markup=markup,
    )
    return msg.message_id


async def publish_video(
    bot: Bot,
    target_channel: str,
    file_id: str,
    caption: str | None = None,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    if not category:
        category = await classify_text(caption or "")
    final_caption = await _append_category(caption, category)

    msg = await _send_with_retry(
        bot.send_video(
            chat_id=target_channel,
            video=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    await bot.edit_message_reply_markup(
        chat_id=target_channel,
        message_id=msg.message_id,
        reply_markup=markup,
    )
    return msg.message_id


async def publish_document(
    bot: Bot,
    target_channel: str,
    file_id: str,
    caption: str | None = None,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    if not category:
        category = await classify_text(caption or "")
    final_caption = await _append_category(caption, category)

    msg = await _send_with_retry(
        bot.send_document(
            chat_id=target_channel,
            document=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    await bot.edit_message_reply_markup(
        chat_id=target_channel,
        message_id=msg.message_id,
        reply_markup=markup,
    )
    return msg.message_id


async def publish_album(
    bot: Bot,
    target_channel: str,
    media_items: List[dict],  # [{"type": "photo"|"video", "file_id": ..., "caption": ...}]
    caption: str | None = None,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    """
    Publish a MediaGroup album.  The category tag is appended to the caption
    of the first item.  Returns the message_id of the first sent message.
    """
    if not category:
        category = await classify_text(caption or "")
    final_caption = await _append_category(caption, category)

    media_group = []
    for i, item in enumerate(media_items):
        cap = final_caption if i == 0 else None
        if item["type"] == "photo":
            media_group.append(InputMediaPhoto(
                media=item["file_id"], caption=cap, parse_mode=parse_mode if cap else None))
        elif item["type"] == "video":
            media_group.append(InputMediaVideo(
                media=item["file_id"], caption=cap, parse_mode=parse_mode if cap else None))
        elif item["type"] == "document":
            media_group.append(InputMediaDocument(
                media=item["file_id"], caption=cap, parse_mode=parse_mode if cap else None))

    messages = await _send_with_retry(
        bot.send_media_group(chat_id=target_channel, media=media_group)
    )
    if not messages:
        return None

    first_msg_id = messages[0].message_id
    # For albums, attach reaction buttons via a follow-up text message (Telegram
    # doesn't support inline keyboards on media group messages)
    discussion_url = await db.get_discussion_group()
    ad_rows = await build_ad_rows()
    reaction = _reaction_row(first_msg_id, 0, 0, discussion_url)
    keyboard = InlineKeyboardMarkup(ad_rows + [reaction])

    await _send_with_retry(
        bot.send_message(
            chat_id=target_channel,
            text="↑ 互动",
            reply_markup=keyboard,
        )
    )
    return first_msg_id


async def publish_from_submission(
    bot: Bot,
    target_channel: str,
    submission: dict,
    category: str | None = None,
) -> Optional[int]:
    """
    High-level: publish a submission dict (as stored in DB) to the channel.
    Returns the new channel message_id.
    """
    data = submission["message_data"]
    ctype = submission["content_type"]
    sign = _build_signature(submission)
    text = data.get("text", "")
    caption_base = f"{text}\n\n{sign}" if sign else text

    if ctype == "text":
        return await publish_text(bot, target_channel, caption_base, category=category)
    elif ctype == "photo":
        return await publish_photo(
            bot, target_channel, data["file_id"], caption=caption_base, category=category)
    elif ctype == "video":
        return await publish_video(
            bot, target_channel, data["file_id"], caption=caption_base, category=category)
    elif ctype == "document":
        return await publish_document(
            bot, target_channel, data["file_id"], caption=caption_base, category=category)
    elif ctype == "album":
        return await publish_album(
            bot, target_channel, data["items"],
            caption=caption_base, category=category)
    else:
        logger.warning("Unknown content_type: %s", ctype)
        return None


def _build_signature(submission: dict) -> str:
    sign_type = submission["sign_type"]
    if sign_type == "anonymous":
        return ""
    elif sign_type == "username":
        username = submission.get("username", "")
        return f"— @{username}" if username else ""
    elif sign_type == "custom":
        name = submission.get("custom_name", "")
        return f"— {name}" if name else ""
    return ""


async def update_reaction_markup(
    bot: Bot,
    target_channel: str,
    message_id: int,
) -> None:
    """Re-fetch counts and update the inline keyboard on a channel post."""
    likes, dislikes = await db.get_reaction_counts(message_id)
    markup = await build_reply_markup(message_id, likes, dislikes)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel,
            message_id=message_id,
            reply_markup=markup,
        )
    except TelegramError as e:
        # Message not modified is fine — just log at DEBUG level
        logger.debug("edit_message_reply_markup: %s", e)
