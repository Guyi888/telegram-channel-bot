"""
Module 2 — User submission system.

State machine (ConversationHandler):
  WAITING_CONTENT   → user sends content
  WAITING_SIGNATURE → user picks signature mode via inline button
  WAITING_CUSTOM_NAME → user types their custom display name
  CONFIRMING        → preview shown, user confirms / re-edits / cancels

After confirmation the submission is written to DB and admins are notified.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup, Message,
    Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument,
)
from telegram.ext import (
    CallbackQueryHandler, CommandHandler, ConversationHandler,
    ContextTypes, MessageHandler, filters,
)

import config
from database import db
from services.word_filter import filter_text
from services.classifier import classify_text
from utils.helpers import content_type_label, extract_text, format_user, format_ts

logger = logging.getLogger(__name__)

# ── States ────────────────────────────────────────────────────────────────────
WAITING_CONTENT = 1
WAITING_SIGNATURE = 2
WAITING_CUSTOM_NAME = 3
CONFIRMING = 4
WAITING_REJECT_REASON = 5  # Admin sub-state (handled in callbacks.py)

# ── MediaGroup collection (same pattern as admin_forward) ─────────────────────
_pending_groups: Dict[str, List[Message]] = defaultdict(list)
_pending_tasks: Dict[str, asyncio.Task] = {}
_group_resolved: Dict[str, asyncio.Event] = {}  # gid → resolved event


# ── Conversation entry ────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle /start — greet user and prompt for content."""
    user = update.effective_user

    if await db.is_admin(user.id):
        await update.message.reply_text(
            "👋 您好，管理员！直接发送消息即可转发至目标频道。\n"
            "使用 /status 查看运行状态。"
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 欢迎投稿！\n\n"
        "请直接发送您想投稿的内容（文字、图片、视频、文件或相册均可）。\n"
        "输入 /cancel 随时取消。"
    )
    return WAITING_CONTENT


async def receive_content(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Step 1 — receive user's content."""
    user = update.effective_user
    msg = update.effective_message

    # Blacklist check (silent drop)
    if await db.is_blacklisted(user.id):
        logger.info("Blacklisted user %s tried to submit", user.id)
        return ConversationHandler.END

    # Rate-limit check — cooldown
    if await db.check_user_cooldown(user.id):
        await msg.reply_text(
            "⏳ 您的投稿过于频繁，已进入 24 小时冷却期，请稍后再试。"
        )
        return ConversationHandler.END

    # Daily limit check
    daily_count = await db.count_user_submissions_today(user.id)
    limit = await db.get_submission_limit()
    if daily_count >= limit:
        await msg.reply_text(
            f"❌ 您今日的投稿次数已达上限（{limit} 条），请明天再试。"
        )
        return ConversationHandler.END

    # Rapid-fire check (3 submissions in 60 seconds → 24h cooldown)
    recent = await db.count_user_submissions_in_window(
        user.id, config.RATE_LIMIT_WINDOW_SECONDS)
    if recent >= config.RATE_LIMIT_COUNT:
        await db.set_user_cooldown(user.id, config.COOLDOWN_HOURS)
        await msg.reply_text(
            "⚠️ 您投稿太频繁，已触发冷却机制，24 小时内无法继续投稿。"
        )
        return ConversationHandler.END

    if msg.media_group_id:
        return await _collect_media_group(msg, user, context)
    else:
        await _store_single_message(msg, user, context)
        return await _show_signature_options(update, context)


async def _store_single_message(
    msg: Message, user, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Extract and store message info in user_data."""
    text = extract_text(msg)

    if msg.photo:
        ctype = "photo"
        file_id = msg.photo[-1].file_id
        data = {"file_id": file_id, "text": text}
    elif msg.video:
        ctype = "video"
        file_id = msg.video.file_id
        data = {"file_id": file_id, "text": text}
    elif msg.document:
        ctype = "document"
        file_id = msg.document.file_id
        data = {"file_id": file_id, "text": text}
    elif msg.audio:
        ctype = "audio"
        file_id = msg.audio.file_id
        data = {"file_id": file_id, "text": text}
    else:
        ctype = "text"
        data = {"text": text}

    context.user_data["submission"] = {
        "content_type": ctype,
        "message_data": data,
        "raw_text": text,
    }


async def _collect_media_group(
    msg: Message, user, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Aggregate MediaGroup then move to signature selection."""
    gid = msg.media_group_id
    _pending_groups[gid].append(msg)

    if gid not in _group_resolved:
        _group_resolved[gid] = asyncio.Event()

    if gid in _pending_tasks and not _pending_tasks[gid].done():
        _pending_tasks[gid].cancel()

    async def _flush():
        # Always set the event even if an exception occurs, to unblock the waiter
        try:
            await asyncio.sleep(config.MEDIA_GROUP_DELAY)
            messages = _pending_groups.pop(gid, [])
            messages.sort(key=lambda m: m.message_id)

            items = []
            caption = ""
            for i, m in enumerate(messages):
                if i == 0:
                    caption = extract_text(m)
                if m.photo:
                    items.append({"type": "photo", "file_id": m.photo[-1].file_id})
                elif m.video:
                    items.append({"type": "video", "file_id": m.video.file_id})
                elif m.document:
                    items.append({"type": "document", "file_id": m.document.file_id})

            context.user_data["submission"] = {
                "content_type": "album",
                "message_data": {"items": items, "text": caption},
                "raw_text": caption,
            }
        finally:
            # Always unblock the waiter — even on error
            event = _group_resolved.get(gid)
            if event:
                event.set()

    task = asyncio.get_event_loop().create_task(_flush())
    _pending_tasks[gid] = task

    # Wait for aggregation — hold a reference before the task can pop it
    event = _group_resolved.get(gid)
    if event:
        await event.wait()
    _group_resolved.pop(gid, None)
    return await _show_signature_options_raw(msg, context)


# ── Signature selection ───────────────────────────────────────────────────────

async def _show_signature_options(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🕵️ 匿名投稿", callback_data="sign:anonymous")],
        [InlineKeyboardButton("👤 展示用户名", callback_data="sign:username")],
        [InlineKeyboardButton("✏️ 自定义署名", callback_data="sign:custom")],
        [InlineKeyboardButton("❌ 取消", callback_data="sign:cancel")],
    ])
    await update.effective_message.reply_text(
        "📝 请选择您的署名方式：", reply_markup=keyboard
    )
    return WAITING_SIGNATURE


async def _show_signature_options_raw(msg: Message, context: ContextTypes.DEFAULT_TYPE) -> int:
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🕵️ 匿名投稿", callback_data="sign:anonymous")],
        [InlineKeyboardButton("👤 展示用户名", callback_data="sign:username")],
        [InlineKeyboardButton("✏️ 自定义署名", callback_data="sign:custom")],
        [InlineKeyboardButton("❌ 取消", callback_data="sign:cancel")],
    ])
    await msg.reply_text("📝 请选择您的署名方式：", reply_markup=keyboard)
    return WAITING_SIGNATURE


async def handle_signature_choice(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    await query.answer()
    choice = query.data.split(":")[1]
    user = update.effective_user

    if choice == "cancel":
        await query.edit_message_text("❌ 已取消投稿。")
        context.user_data.clear()
        return ConversationHandler.END

    if choice == "username":
        if not user.username:
            await query.edit_message_text(
                "⚠️ 您尚未设置 Telegram 用户名。\n"
                "请前往 Telegram 设置添加用户名后重新投稿，或选择其他署名方式。"
            )
            return WAITING_SIGNATURE

    context.user_data["sign_type"] = choice

    if choice == "custom":
        await query.edit_message_text(
            "✏️ 请输入您想展示的自定义署名（20 字以内）："
        )
        return WAITING_CUSTOM_NAME

    return await _show_preview(query, context, update)


async def handle_custom_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = (update.effective_message.text or "").strip()
    if not name:
        await update.effective_message.reply_text("⚠️ 署名不能为空，请重新输入：")
        return WAITING_CUSTOM_NAME
    if len(name) > 20:
        await update.effective_message.reply_text("⚠️ 署名最多 20 个字，请重新输入：")
        return WAITING_CUSTOM_NAME

    context.user_data["custom_name"] = name
    return await _show_preview(update.effective_message, context, update)


# ── Preview ───────────────────────────────────────────────────────────────────

async def _show_preview(target, context: ContextTypes.DEFAULT_TYPE, update) -> int:
    sub = context.user_data.get("submission", {})
    sign_type = context.user_data.get("sign_type", "anonymous")
    custom_name = context.user_data.get("custom_name", "")
    user = update.effective_user

    sign_labels = {
        "anonymous": "🕵️ 匿名投稿",
        "username": f"👤 @{user.username or '未设置'}",
        "custom": f"✏️ {custom_name}",
    }
    sign_label = sign_labels.get(sign_type, "匿名")

    content_preview = sub.get("message_data", {}).get("text", "") or "[媒体文件]"
    if len(content_preview) > 200:
        content_preview = content_preview[:200] + "…"

    ctype_label = {"text": "文字", "photo": "图片", "video": "视频",
                   "document": "文件", "album": "相册", "audio": "音频"}.get(
        sub.get("content_type", "text"), "内容"
    )

    text = (
        f"📋 <b>请确认您的投稿内容：</b>\n\n"
        f"<b>署名方式：</b>{sign_label}\n"
        f"<b>内容类型：</b>{ctype_label}\n"
        f"<b>内容预览：</b>\n{content_preview}"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ 确认提交", callback_data="submit:confirm"),
            InlineKeyboardButton("✏️ 重新编辑", callback_data="submit:reedit"),
            InlineKeyboardButton("❌ 取消", callback_data="submit:cancel"),
        ]
    ])

    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await target.reply_text(text, parse_mode="HTML", reply_markup=keyboard)

    return CONFIRMING


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action == "cancel":
        await query.edit_message_text("❌ 已取消投稿。")
        context.user_data.clear()
        return ConversationHandler.END

    if action == "reedit":
        sub = context.user_data.get("submission", {})
        ctype = sub.get("content_type", "text")
        ctype_label = {"text": "文字", "photo": "图片", "video": "视频",
                       "document": "文件", "album": "相册", "audio": "音频"}.get(ctype, "内容")
        original_text = sub.get("message_data", {}).get("text", "") or ""
        if original_text:
            preview = original_text[:300] + ("…" if len(original_text) > 300 else "")
            prompt = (
                f"✏️ 请重新发送您的投稿内容：\n\n"
                f"<b>原内容（{ctype_label}）：</b>\n{preview}"
            )
        else:
            prompt = f"✏️ 请重新发送您的投稿内容（原内容为 {ctype_label}，无文字）："
        await query.edit_message_text(prompt, parse_mode="HTML")
        context.user_data.pop("submission", None)
        return WAITING_CONTENT

    # Confirm — write to DB and notify admins
    user = update.effective_user
    sub = context.user_data.get("submission", {})
    sign_type = context.user_data.get("sign_type", "anonymous")
    custom_name = context.user_data.get("custom_name", "")

    raw_text = sub.get("raw_text", "")
    filtered_text = await filter_text(raw_text)
    # Apply filter back to message_data
    if "text" in sub.get("message_data", {}):
        sub["message_data"]["text"] = filtered_text

    submission_id = await db.create_submission(
        user_id=user.id,
        username=user.username or "",
        sign_type=sign_type,
        custom_name=custom_name or None,
        content_type=sub.get("content_type", "text"),
        message_data=sub.get("message_data", {}),
    )

    await query.edit_message_text(
        "✅ 投稿已提交，正在等待审核，结果将通过私信通知您。\n感谢您的投稿！"
    )

    # Notify admins
    await _notify_admins(context.bot, submission_id, user, sub, sign_type)

    context.user_data.clear()
    return ConversationHandler.END


# ── Admin notification ────────────────────────────────────────────────────────

async def _notify_admins(
    bot: Bot,
    submission_id: int,
    user,
    sub: dict,
    sign_type: str,
) -> None:
    sign_labels = {
        "anonymous": "🕵️ 匿名投稿",
        "username": f"👤 展示用户名 (@{user.username or '无'})",
        "custom": f"✏️ 自定义署名",
    }
    sign_label = sign_labels.get(sign_type, "匿名")
    ctype_label = {"text": "文字", "photo": "图片", "video": "视频",
                   "document": "文件", "album": "相册", "audio": "音频"}.get(
        sub.get("content_type", "text"), "内容"
    )
    content_preview = sub.get("message_data", {}).get("text", "") or "[媒体文件]"
    if len(content_preview) > 300:
        content_preview = content_preview[:300] + "…"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    text = (
        f"📥 <b>新投稿待审核</b>  (ID: <code>{submission_id}</code>)\n\n"
        f"👤 投稿者：@{user.username or '无'}（ID: <code>{user.id}</code>）\n"
        f"🏷 署名方式：{sign_label}\n"
        f"📎 内容类型：{ctype_label}\n"
        f"⏰ 投稿时间：{now}\n"
        f"─────────────────\n"
        f"{content_preview}\n"
        f"─────────────────"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ 通过发布", callback_data=f"review:approve:{submission_id}"),
            InlineKeyboardButton("❌ 拒绝", callback_data=f"review:reject:{submission_id}"),
            InlineKeyboardButton("✏️ 编辑后发布", callback_data=f"review:edit:{submission_id}"),
        ]
    ])

    # Determine where to send: review group or all admins' DMs
    import config as _cfg

    targets = []
    if _cfg.REVIEW_GROUP_ID:
        targets.append(_cfg.REVIEW_GROUP_ID)
    else:
        admins = await db.list_admins()
        targets = [a["user_id"] for a in admins]

    for chat_id in targets:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as e:
            logger.error("Failed to notify admin %s: %s", chat_id, e)


# ── Cancel / timeout fallback ─────────────────────────────────────────────────

async def cancel_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.effective_message.reply_text("❌ 已取消投稿。")
    return ConversationHandler.END


# ── ConversationHandler factory ───────────────────────────────────────────────

def build_submission_conversation() -> ConversationHandler:
    """Return the fully-configured ConversationHandler for user submissions."""
    return ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            # Non-admin private message triggers content submission
            MessageHandler(
                filters.ChatType.PRIVATE & ~filters.COMMAND,
                receive_content,
            ),
        ],
        states={
            WAITING_CONTENT: [
                MessageHandler(
                    filters.ChatType.PRIVATE & ~filters.COMMAND,
                    receive_content,
                )
            ],
            WAITING_SIGNATURE: [
                CallbackQueryHandler(handle_signature_choice, pattern=r"^sign:")
            ],
            WAITING_CUSTOM_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_name)
            ],
            CONFIRMING: [
                CallbackQueryHandler(handle_confirm, pattern=r"^submit:")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_submission)],
        per_user=True,
        per_chat=True,
        per_message=False,
        allow_reentry=True,
    )
