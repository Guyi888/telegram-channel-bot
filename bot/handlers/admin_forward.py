"""
Module 1 — Admin message forwarding.

When a super-admin or normal admin sends a message to the bot in a private
chat, it is forwarded to the target channel using copy_message (no source
attribution).

MediaGroup (album) aggregation:
  Multiple messages sharing the same media_group_id arrive within a short
  window.  We collect them for MEDIA_GROUP_DELAY seconds, then forward the
  whole album in one shot.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Dict, List

from telegram import Bot, Message, Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from telegram.ext import ContextTypes

import config
from database import db
from services import publisher
from utils.helpers import extract_text

logger = logging.getLogger(__name__)

# {media_group_id: [Message, ...]}
_pending_groups: Dict[str, List[Message]] = defaultdict(list)
# {media_group_id: asyncio.Task}
_pending_tasks: Dict[str, asyncio.Task] = {}


async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point — called for every private message from an admin."""
    msg = update.effective_message
    if not msg:
        return

    user_id = update.effective_user.id

    # Re-verify admin status from DB (not just the filter)
    if not await db.is_admin(user_id):
        return

    target = await db.get_target_channel()
    if not target:
        await msg.reply_text("⚠️ 尚未设置目标频道，请超级管理员使用 /setchannel 设置。")
        return

    if msg.media_group_id:
        await _handle_media_group(msg, context.bot, target)
    else:
        await _forward_single(msg, context.bot, target)


# ── Single message ────────────────────────────────────────────────────────────

async def _forward_single(msg: Message, bot: Bot, target: str) -> None:
    """Forward one non-album message to the target channel."""
    try:
        sent = await bot.copy_message(
            chat_id=target,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
        )
        # Attach reaction + ad buttons
        markup = await publisher.build_reply_markup(sent.message_id)
        text = extract_text(msg)
        from services.classifier import classify_text
        category = await classify_text(text)
        # Append category tag by editing if possible
        # For non-text messages copy_message preserves content, we add markup
        await bot.edit_message_reply_markup(
            chat_id=target,
            message_id=sent.message_id,
            reply_markup=markup,
        )
        await db.set_message_map(msg.message_id, msg.chat_id, sent.message_id)
    except Exception as e:
        logger.error("Failed to forward single admin message: %s", e)


# ── MediaGroup aggregation ────────────────────────────────────────────────────

async def _handle_media_group(msg: Message, bot: Bot, target: str) -> None:
    gid = msg.media_group_id
    _pending_groups[gid].append(msg)

    # Cancel existing timer and restart
    if gid in _pending_tasks and not _pending_tasks[gid].done():
        _pending_tasks[gid].cancel()

    task = asyncio.get_event_loop().create_task(
        _flush_media_group(gid, bot, target)
    )
    _pending_tasks[gid] = task


async def _flush_media_group(gid: str, bot: Bot, target: str) -> None:
    await asyncio.sleep(config.MEDIA_GROUP_DELAY)

    messages = _pending_groups.pop(gid, [])
    _pending_tasks.pop(gid, None)

    if not messages:
        return

    # Sort by message_id to preserve order
    messages.sort(key=lambda m: m.message_id)

    media_group = []
    for i, m in enumerate(messages):
        caption = (m.caption or m.text or "") if i == 0 else None
        parse_mode = "HTML" if caption else None

        if m.photo:
            media_group.append(InputMediaPhoto(
                media=m.photo[-1].file_id,
                caption=caption, parse_mode=parse_mode))
        elif m.video:
            media_group.append(InputMediaVideo(
                media=m.video.file_id,
                caption=caption, parse_mode=parse_mode))
        elif m.document:
            media_group.append(InputMediaDocument(
                media=m.document.file_id,
                caption=caption, parse_mode=parse_mode))
        elif m.audio:
            media_group.append(InputMediaAudio(
                media=m.audio.file_id,
                caption=caption, parse_mode=parse_mode))

    if not media_group:
        return

    try:
        sent_list = await bot.send_media_group(chat_id=target, media=media_group)
        if sent_list:
            first_id = sent_list[0].message_id
            markup = await publisher.build_reply_markup(first_id)
            # Send interaction row as a follow-up (albums don't support inline keyboards)
            await bot.send_message(
                chat_id=target,
                text="↑ 互动",
                reply_markup=markup,
            )
            await db.set_message_map(messages[0].message_id, messages[0].chat_id, first_id)
    except Exception as e:
        logger.error("Failed to forward media group %s: %s", gid, e)
