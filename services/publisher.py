"""
Publisher service — the single place where messages are sent to the target channel.

Every published message gets:
  1. Ad buttons (Module 4)
  2. Reaction buttons (Module 5)
  3. Category tag (Module 6) — appended as #类别名
  4. Signature — inline in same message caption/text

Signature format (all inline, HTML):
  anonymous  → 「匿名投稿」
  username   → 「<a href="tg://user?id=ID">@username</a>」
  custom     → 「自定义名（<a href="tg://user?id=ID">@username</a>）」
"""
from __future__ import annotations

import asyncio
import html as html_module
import logging
from typing import List, Optional

from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
)
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
    return InlineKeyboardMarkup(ad_rows + [reaction])


# ── Core publish helpers ──────────────────────────────────────────────────────

async def _send_with_retry(make_coro, retries: int = 3):
    """
    make_coro: zero-argument callable returning a fresh coroutine each call.
    Provides RetryAfter / flood-wait handling.

    IMPORTANT: always pass a lambda (or other callable), never a bare coroutine
    object, because coroutines can only be awaited once.
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return await make_coro()
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning("Flood wait %ss — sleeping (attempt %s)", wait, attempt + 1)
            last_exc = e
            await asyncio.sleep(wait)
        except TelegramError as e:
            logger.error("Telegram error (attempt %s): %s", attempt + 1, e)
            last_exc = e
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2)
    if last_exc:
        raise last_exc
    return None


def _append_category_tag(text: str | None, category: str) -> str:
    # Strip decorative angle-brackets stored around category names in the DB,
    # e.g. "<激情乱伦>" → "#激情乱伦" (valid Telegram hashtag, valid HTML).
    clean = (category or "").strip("<>").strip()
    tag = f"\n\n#{clean}" if clean else ""
    return (text or "") + tag


# ── Signature builder ─────────────────────────────────────────────────────────

def _build_signature(submission: dict) -> str:
    """
    Return HTML-formatted inline signature string.

    anonymous → 「匿名投稿」
    username  → 「<a href="tg://user?id=ID">@username</a>」
    custom    → 「custom_name（<a href="tg://user?id=ID">@username</a>）」
    """
    sign_type = submission.get("sign_type", "anonymous")
    user_id = submission.get("user_id", 0)
    username = (submission.get("username") or "").strip()
    custom_name = (submission.get("custom_name") or "").strip()

    display = f"@{username}" if username else str(user_id)
    # html.escape the display name so special chars don't break parse_mode=HTML
    user_link = f'<a href="tg://user?id={user_id}">{html_module.escape(display)}</a>'

    if sign_type == "anonymous":
        return "「匿名投稿」"
    elif sign_type == "username":
        return f"「{user_link}」"
    elif sign_type == "custom":
        if custom_name:
            return f"「{html_module.escape(custom_name)}（{user_link}）」"
        return f"「{user_link}」"
    return "「匿名投稿」"


# ── Public API ────────────────────────────────────────────────────────────────

async def publish_text(
    bot: Bot,
    target_channel: str,
    text: str,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    if not category:
        category = await classify_text(text)
    final_text = _append_category_tag(text, category)

    msg = await _send_with_retry(
        lambda: bot.send_message(
            chat_id=target_channel,
            text=final_text,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel, message_id=msg.message_id, reply_markup=markup
        )
    except TelegramError as e:
        logger.debug("edit_message_reply_markup: %s", e)
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
    final_caption = _append_category_tag(caption, category)

    msg = await _send_with_retry(
        lambda: bot.send_photo(
            chat_id=target_channel,
            photo=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel, message_id=msg.message_id, reply_markup=markup
        )
    except TelegramError as e:
        logger.debug("edit_message_reply_markup: %s", e)
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
    final_caption = _append_category_tag(caption, category)

    msg = await _send_with_retry(
        lambda: bot.send_video(
            chat_id=target_channel,
            video=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel, message_id=msg.message_id, reply_markup=markup
        )
    except TelegramError as e:
        logger.debug("edit_message_reply_markup: %s", e)
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
    final_caption = _append_category_tag(caption, category)

    msg = await _send_with_retry(
        lambda: bot.send_document(
            chat_id=target_channel,
            document=file_id,
            caption=final_caption,
            parse_mode=parse_mode,
        )
    )
    if not msg:
        return None

    markup = await build_reply_markup(msg.message_id)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel, message_id=msg.message_id, reply_markup=markup
        )
    except TelegramError as e:
        logger.debug("edit_message_reply_markup: %s", e)
    return msg.message_id


async def publish_album(
    bot: Bot,
    target_channel: str,
    media_items: List[dict],
    caption: str | None = None,
    parse_mode: str = "HTML",
    category: str | None = None,
) -> Optional[int]:
    """
    Publish a MediaGroup album to the target channel.

    Sends all items in a single send_media_group call so they appear as one
    grouped post in the channel.  Reaction / ad buttons are attached via a
    follow-up message containing an invisible Braille-space character so it
    looks like a plain button row without visible text.

    Returns the message_id of the first album message.
    """
    if not category:
        category = await classify_text(caption or "")
    final_caption = _append_category_tag(caption, category)

    media_group = []
    for i, item in enumerate(media_items):
        cap = final_caption if i == 0 else None
        pm = parse_mode if cap else None
        if item["type"] == "photo":
            media_group.append(InputMediaPhoto(media=item["file_id"], caption=cap, parse_mode=pm))
        elif item["type"] == "video":
            media_group.append(InputMediaVideo(media=item["file_id"], caption=cap, parse_mode=pm))
        elif item["type"] == "document":
            media_group.append(InputMediaDocument(media=item["file_id"], caption=cap, parse_mode=pm))

    if not media_group:
        return None

    messages = await _send_with_retry(
        lambda: bot.send_media_group(chat_id=target_channel, media=media_group)
    )
    if not messages:
        return None

    first_msg_id = messages[0].message_id
    markup = await build_reply_markup(first_msg_id)
    # Follow-up message carries the reaction keyboard.
    # U+2800 (Braille blank) is a valid non-empty character that renders invisibly,
    # so the message appears as a clean button row with no caption text.
    try:
        await _send_with_retry(
            lambda m=markup: bot.send_message(
                chat_id=target_channel,
                text="\u2800",
                reply_markup=m,
            )
        )
    except TelegramError as e:
        logger.debug("Album reaction follow-up failed: %s", e)

    return first_msg_id


async def publish_from_submission(
    bot: Bot,
    target_channel: str,
    submission: dict,
    category: str | None = None,
) -> Optional[int]:
    """
    High-level: publish a submission dict to the channel.
    Signature is appended inline to the caption/text.
    Returns the new channel message_id.
    """
    data = submission["message_data"]
    ctype = submission["content_type"]
    sign = _build_signature(submission)
    # Escape user-provided text so angle brackets don't break HTML parse_mode
    text = html_module.escape(data.get("text", "") or "")

    # Build caption with signature inline
    if text and sign:
        caption_base = f"{text}\n\n{sign}"
    elif sign:
        caption_base = sign
    else:
        caption_base = text

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


async def update_reaction_markup(
    bot: Bot,
    target_channel: str,
    message_id: int,
) -> None:
    """Re-fetch reaction counts and update the inline keyboard."""
    likes, dislikes = await db.get_reaction_counts(message_id)
    markup = await build_reply_markup(message_id, likes, dislikes)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target_channel, message_id=message_id, reply_markup=markup
        )
    except TelegramError as e:
        logger.debug("update_reaction_markup: %s", e)
