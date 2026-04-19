"""
Module 1 — Admin message forwarding.

Admin post-to-channel flow:
  1. Admin sends content (text / photo / video / document / album) to bot
  2. Bot stores the message reference + original text/caption in user_data
     and shows a signature selection keyboard:
       [🕵️ 匿名发布]  [✍️ 署名发布]  [❌ 取消]
  3. Admin clicks a button
  4. Bot publishes to the target channel with signature INLINE in the caption
     (same message, bottom of caption — NOT a separate message)
  5. Reaction + ad buttons are attached
  6. Bot confirms success / failure to the admin

Signature format (HTML):
  anonymous → 「匿名发布」
  named     → 「✍️ <a href="tg://user?id=ID">@username</a>」
              (falls back to full_name when no username)

Commands (/panel, /status …) are handled by management.py first and never
reach this handler.

MediaGroup (album) aggregation:
  Multiple messages sharing the same media_group_id arrive within a short
  window.  We collect them for MEDIA_GROUP_DELAY seconds, then show the
  signature keyboard once for the whole album.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Dict, List, Optional

from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
    Message, Update,
)
from telegram.ext import ContextTypes

import config
from database import db
from services import publisher

logger = logging.getLogger(__name__)

# Pending media group buffers: {group_id: [Message, ...]}
_pending_groups: Dict[str, List[Message]] = defaultdict(list)
_pending_tasks: Dict[str, asyncio.Task] = {}

# Signature selection keyboard
_SIGN_KEYBOARD = InlineKeyboardMarkup([
    [InlineKeyboardButton("🕵️ 匿名发布", callback_data="admin_post:anonymous")],
    [InlineKeyboardButton("✍️ 署名发布", callback_data="admin_post:named")],
    [InlineKeyboardButton("❌ 取消",     callback_data="admin_post:cancel")],
])


def _build_admin_signature(action: str, user) -> str:
    """
    Build inline HTML signature for admin posts.

    anonymous → 「匿名发布」
    named     → 「✍️ <a href="tg://user?id=ID">@username</a>」
    """
    if action == "anonymous":
        return "「匿名发布」"
    # named
    uid = user.id
    display = f"@{user.username}" if user.username else (user.full_name or str(uid))
    return f'「✍️ <a href="tg://user?id={uid}">{display}</a>」'


# ── Entry point ───────────────────────────────────────────────────────────────

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Called for every private non-command message from an admin.
    Stores message info and shows the signature keyboard — does NOT publish yet.
    """
    msg = update.effective_message
    user = update.effective_user
    if not msg or not user:
        return

    if not await db.is_admin(user.id):
        return

    # Chinese text commands (e.g. "添加目标频道 @xxx") are handled by
    # management.py (group=10).  Return early here so the signature keyboard
    # is NOT shown when an admin types a management command.
    _CN_PREFIXES = (
        "添加目标频道", "删除目标频道", "添加来源频道", "删除来源频道",
        "添加管理员", "删除管理员", "添加分类", "添加违禁词",
        "封禁用户", "解封用户", "设置投稿上限", "打开面板", "管理面板", "控制台",
    )
    _text_content = (msg.text or "").strip()
    if _text_content and any(_text_content.startswith(p) for p in _CN_PREFIXES):
        return

    target = await db.get_target_channel()
    if not target:
        await msg.reply_text("⚠️ 尚未设置目标频道，请超级管理员使用 /setchannel 设置。")
        return

    if msg.media_group_id:
        await _buffer_admin_media_group(msg, context)
    else:
        # Single message — store content info and ask for signature mode.
        # Use text_html / caption_html so Telegram formatting entities (bold,
        # italic, links …) are preserved AND angle-brackets in plain text are
        # already HTML-escaped by PTB.
        context.user_data["admin_pending"] = {
            "chat_id":      msg.chat_id,
            "message_id":   msg.message_id,
            "media_group":  None,
            "caption":      msg.caption_html or msg.text_html or "",
            "is_text_only": bool(msg.text and not msg.photo and not msg.video
                                 and not msg.document and not msg.audio),
        }
        await msg.reply_text("📤 请选择发布署名方式：", reply_markup=_SIGN_KEYBOARD)


# ── MediaGroup aggregation ────────────────────────────────────────────────────

async def _buffer_admin_media_group(msg: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Collect album messages; once the stream settles, show the signature keyboard."""
    gid = msg.media_group_id
    _pending_groups[gid].append(msg)

    if gid in _pending_tasks and not _pending_tasks[gid].done():
        _pending_tasks[gid].cancel()

    async def _flush():
        await asyncio.sleep(config.MEDIA_GROUP_DELAY)
        messages = _pending_groups.pop(gid, [])
        _pending_tasks.pop(gid, None)
        if not messages:
            return
        messages.sort(key=lambda m: m.message_id)

        # Build album item list, recording the first caption
        # Build album items with file_ids so we can use send_media_group.
        # caption_html / text_html gives us HTML-safe text with entities intact.
        first_caption = messages[0].caption_html or messages[0].text_html or ""
        album_items = []
        for m in messages:
            item: dict = {"message_id": m.message_id}
            if m.photo:
                item["type"] = "photo"
                item["file_id"] = m.photo[-1].file_id
            elif m.video:
                item["type"] = "video"
                item["file_id"] = m.video.file_id
            elif m.document:
                item["type"] = "document"
                item["file_id"] = m.document.file_id
            elif m.audio:
                item["type"] = "audio"
                item["file_id"] = m.audio.file_id
            else:
                item["type"] = "text"
                item["file_id"] = None
            album_items.append(item)

        context.user_data["admin_pending"] = {
            "chat_id":      messages[0].chat_id,
            "message_id":   messages[0].message_id,
            "media_group":  album_items,
            "caption":      first_caption,
            "is_text_only": False,
        }
        await messages[0].reply_text("📤 请选择发布署名方式：", reply_markup=_SIGN_KEYBOARD)

    task = asyncio.get_running_loop().create_task(_flush())
    _pending_tasks[gid] = task


# ── Callback: signature selected ─────────────────────────────────────────────

async def handle_admin_post_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle admin_post:anonymous / admin_post:named / admin_post:cancel.

    Reads pending info from user_data, builds the signature inline, and
    publishes the content to the target channel.
    """
    query = update.callback_query
    user = update.effective_user
    await query.answer()

    if not await db.is_admin(user.id):
        return

    action = query.data.split(":")[1]  # anonymous | named | cancel

    if action == "cancel":
        context.user_data.pop("admin_pending", None)
        try:
            await query.edit_message_text("❌ 已取消发布。")
        except Exception:
            await query.message.reply_text("❌ 已取消发布。")
        return

    pending = context.user_data.pop("admin_pending", None)
    if not pending:
        try:
            await query.edit_message_text("⚠️ 没有待发布的内容，请重新发送消息。")
        except Exception:
            await query.message.reply_text("⚠️ 没有待发布的内容，请重新发送消息。")
        return

    target = await db.get_target_channel()
    if not target:
        await query.edit_message_text("⚠️ 尚未设置目标频道。")
        return

    bot = context.bot
    sig = _build_admin_signature(action, user)

    # Compose caption: original_text + blank line + signature
    original_caption = pending.get("caption", "")
    if original_caption:
        caption_with_sig = f"{original_caption}\n\n{sig}"
    else:
        caption_with_sig = sig

    try:
        if pending.get("media_group"):
            await _publish_album(
                bot, target,
                pending["chat_id"],
                pending["media_group"],
                caption_with_sig,
            )
        else:
            await _publish_single(
                bot, target,
                pending["chat_id"],
                pending["message_id"],
                caption_with_sig,
                pending.get("is_text_only", False),
            )

        try:
            await query.edit_message_text("✅ 已成功发布到频道！")
        except Exception:
            await query.message.reply_text("✅ 已成功发布到频道！")

    except Exception as e:
        logger.error("Admin post to channel failed: %s", e)
        err_text = f"❌ 发布失败：{e}"
        try:
            await query.edit_message_text(err_text)
        except Exception:
            await query.message.reply_text(err_text)


# ── Internal publish helpers ──────────────────────────────────────────────────

async def _publish_single(
    bot: Bot,
    target: str,
    src_chat: int,
    message_id: int,
    caption_with_sig: str,
    is_text_only: bool,
) -> None:
    """
    Publish one message to the target channel with the signature inline.

    For text-only messages: send_message (copy_message has no way to override text).
    For media messages: copy_message with caption override.
    """
    if is_text_only:
        sent = await bot.send_message(
            chat_id=target,
            text=caption_with_sig,
            parse_mode="HTML",
        )
        sent_msg_id = sent.message_id
    else:
        sent = await bot.copy_message(
            chat_id=target,
            from_chat_id=src_chat,
            message_id=message_id,
            caption=caption_with_sig,
            parse_mode="HTML",
        )
        sent_msg_id = sent.message_id

    # Attach reaction + ad buttons
    markup = await publisher.build_reply_markup(sent_msg_id)
    try:
        await bot.edit_message_reply_markup(
            chat_id=target, message_id=sent_msg_id, reply_markup=markup
        )
    except Exception as e:
        logger.debug("Could not attach reaction buttons: %s", e)


async def _publish_album(
    bot: Bot,
    target: str,
    src_chat: int,
    album_items: List[dict],
    caption_with_sig: str,
) -> None:
    """
    Publish admin album to the target channel as a single grouped post.

    Uses send_media_group with the file_ids stored in album_items so all
    media appear as one album.  A follow-up message with an invisible
    Braille-blank character carries the reaction keyboard (Telegram does not
    allow inline keyboards on media-group messages themselves).
    """
    media_list = []
    for i, item in enumerate(album_items):
        fid = item.get("file_id")
        t = item.get("type", "")
        if not fid or t == "text":
            continue
        cap = caption_with_sig if i == 0 else None
        pm = "HTML" if cap else None
        if t == "photo":
            media_list.append(InputMediaPhoto(media=fid, caption=cap, parse_mode=pm))
        elif t == "video":
            media_list.append(InputMediaVideo(media=fid, caption=cap, parse_mode=pm))
        elif t == "document":
            media_list.append(InputMediaDocument(media=fid, caption=cap, parse_mode=pm))
        elif t == "audio":
            media_list.append(InputMediaAudio(media=fid, caption=cap, parse_mode=pm))

    if not media_list:
        return

    sent_list = await bot.send_media_group(chat_id=target, media=media_list)
    if not sent_list:
        return

    markup = await publisher.build_reply_markup(sent_list[0].message_id)
    try:
        await bot.send_message(
            chat_id=target,
            text="\u2800",   # Braille blank — invisible text, shows button row only
            reply_markup=markup,
        )
    except Exception as e:
        logger.debug("Could not send album reaction row: %s", e)
