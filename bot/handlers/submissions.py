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

# ── MediaGroup collection ─────────────────────────────────────────────────────
_pending_groups: Dict[str, List[Message]] = defaultdict(list)
_pending_tasks: Dict[str, asyncio.Task] = {}
_group_resolved: Dict[str, asyncio.Event] = {}


# ── Conversation entry ────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle /start — show role-specific panel."""
    user = update.effective_user

    if await db.is_admin(user.id):
        from bot.handlers.management import show_admin_main_panel
        await show_admin_main_panel(update.message, context)
        return ConversationHandler.END

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 开始投稿", callback_data="user_action:submit")],
        [InlineKeyboardButton("❓ 投稿须知", callback_data="user_action:help")],
    ])
    await update.message.reply_text(
        "👋 欢迎使用投稿机器人！\n\n"
        "您可以直接发送内容（文字、图片、视频、文件或相册）开始投稿，\n"
        "也可以点击下方按钮。\n\n"
        "输入 /cancel 随时取消。",
        reply_markup=keyboard,
    )
    return WAITING_CONTENT


async def handle_user_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle user panel button callbacks."""
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action == "submit":
        await query.edit_message_text(
            "📝 请直接发送您想投稿的内容（文字、图片、视频、文件或相册）："
        )
        return WAITING_CONTENT
    elif action == "help":
        await query.edit_message_text(
            "📋 <b>投稿须知</b>\n\n"
            "• 支持文字、图片、视频、文件、相册\n"
            "• 每天投稿次数有上限（由管理员设定）\n"
            "• 内容需符合频道主题，含违规内容将被拒绝\n"
            "• 审核结果将通过私信通知您\n\n"
            "直接发送内容即可开始投稿！",
            parse_mode="HTML",
        )
        return WAITING_CONTENT
    return WAITING_CONTENT


async def receive_content(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Step 1 — receive user's content."""
    user = update.effective_user
    msg = update.effective_message

    if await db.is_blacklisted(user.id):
        logger.info("Blacklisted user %s tried to submit", user.id)
        return ConversationHandler.END

    if await db.check_user_cooldown(user.id):
        await msg.reply_text("⏳ 您的投稿过于频繁，已进入 24 小时冷却期，请稍后再试。")
        return ConversationHandler.END

    daily_count = await db.count_user_submissions_today(user.id)
    limit = await db.get_submission_limit()
    if daily_count >= limit:
        await msg.reply_text(f"❌ 您今日的投稿次数已达上限（{limit} 条），请明天再试。")
        return ConversationHandler.END

    recent = await db.count_user_submissions_in_window(user.id, config.RATE_LIMIT_WINDOW_SECONDS)
    if recent >= config.RATE_LIMIT_COUNT:
        await db.set_user_cooldown(user.id, config.COOLDOWN_HOURS)
        await msg.reply_text("⚠️ 您投稿太频繁，已触发冷却机制，24 小时内无法继续投稿。")
        return ConversationHandler.END

    if msg.media_group_id:
        return await _collect_media_group(msg, user, context)
    else:
        await _store_single_message(msg, user, context)
        return await _show_signature_options(update, context)


async def _store_single_message(msg: Message, user, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = extract_text(msg)
    if msg.photo:
        ctype, file_id = "photo", msg.photo[-1].file_id
        data = {"file_id": file_id, "text": text}
    elif msg.video:
        ctype, file_id = "video", msg.video.file_id
        data = {"file_id": file_id, "text": text}
    elif msg.document:
        ctype, file_id = "document", msg.document.file_id
        data = {"file_id": file_id, "text": text}
    elif msg.audio:
        ctype, file_id = "audio", msg.audio.file_id
        data = {"file_id": file_id, "text": text}
    else:
        ctype = "text"
        data = {"text": text}
    context.user_data["submission"] = {"content_type": ctype, "message_data": data, "raw_text": text}


async def _collect_media_group(msg: Message, user, context: ContextTypes.DEFAULT_TYPE) -> int:
    gid = msg.media_group_id
    _pending_groups[gid].append(msg)

    if gid not in _group_resolved:
        _group_resolved[gid] = asyncio.Event()

    if gid in _pending_tasks and not _pending_tasks[gid].done():
        _pending_tasks[gid].cancel()

    async def _flush():
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
            event = _group_resolved.get(gid)
            if event:
                event.set()

    task = asyncio.get_event_loop().create_task(_flush())
    _pending_tasks[gid] = task

    event = _group_resolved.get(gid)
    if event:
        await event.wait()
    _group_resolved.pop(gid, None)
    return await _show_signature_options_raw(msg, context)


# ── Signature selection ───────────────────────────────────────────────────────

def _signature_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🕵️ 匿名投稿", callback_data="sign:anonymous")],
        [InlineKeyboardButton("👤 展示用户名", callback_data="sign:username")],
        [InlineKeyboardButton("✏️ 自定义署名", callback_data="sign:custom")],
        [InlineKeyboardButton("❌ 取消", callback_data="sign:cancel")],
    ])


async def _show_signature_options(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text("📝 请选择您的署名方式：", reply_markup=_signature_keyboard())
    return WAITING_SIGNATURE


async def _show_signature_options_raw(msg: Message, context: ContextTypes.DEFAULT_TYPE) -> int:
    await msg.reply_text("📝 请选择您的署名方式：", reply_markup=_signature_keyboard())
    return WAITING_SIGNATURE


async def handle_signature_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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
                "请前往设置添加用户名后重新投稿，或选择其他署名方式。",
                reply_markup=_signature_keyboard(),
            )
            return WAITING_SIGNATURE

    context.user_data["sign_type"] = choice

    if choice == "custom":
        # Always set sign_type BEFORE the edit, so if edit fails state still transitions correctly
        try:
            await query.edit_message_text("✏️ 请输入您想展示的自定义署名（20 字以内）：")
        except Exception:
            await query.message.reply_text("✏️ 请输入您想展示的自定义署名（20 字以内）：")
        return WAITING_CUSTOM_NAME

    return await _show_preview(query, context, update)


async def _handle_text_in_signature_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Bug 1 fix: user typed text while in WAITING_SIGNATURE.
    If sign_type is 'custom' (edit message may have failed), treat text as custom name.
    Otherwise re-show the keyboard.
    """
    if context.user_data.get("sign_type") == "custom":
        return await handle_custom_name(update, context)
    await update.effective_message.reply_text(
        "👆 请点击上方按钮选择署名方式：", reply_markup=_signature_keyboard()
    )
    return WAITING_SIGNATURE


async def handle_custom_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = (update.effective_message.text or "").strip()
    if not name:
        await update.effective_message.reply_text("⚠️ 署名不能为空，请重新输入：")
        return WAITING_CUSTOM_NAME
    if len(name) > 20:
        await update.effective_message.reply_text("⚠️ 署名最多 20 个字，请重新输入：")
        return WAITING_CUSTOM_NAME

    context.user_data["custom_name"] = name
    context.user_data["sign_type"] = "custom"
    return await _show_preview(update.effective_message, context, update)


# ── Preview (sends actual media file) ────────────────────────────────────────

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

    ctype = sub.get("content_type", "text")
    data = sub.get("message_data", {})
    raw_text = data.get("text", "")
    ctype_label = {"text": "文字", "photo": "图片", "video": "视频",
                   "document": "文件", "album": "相册", "audio": "音频"}.get(ctype, "内容")

    caption = (
        f"📋 <b>请确认您的投稿内容：</b>\n\n"
        f"<b>署名方式：</b>{sign_label}\n"
        f"<b>内容类型：</b>{ctype_label}\n"
    )
    if raw_text:
        preview = raw_text[:200] + ("…" if len(raw_text) > 200 else "")
        caption += f"<b>文字内容：</b>\n{preview}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ 确认提交", callback_data="submit:confirm"),
            InlineKeyboardButton("✏️ 重新编辑", callback_data="submit:reedit"),
            InlineKeyboardButton("❌ 取消", callback_data="submit:cancel"),
        ]
    ])

    chat_id = update.effective_chat.id
    bot = context.bot

    try:
        if ctype == "photo" and data.get("file_id"):
            await bot.send_photo(chat_id=chat_id, photo=data["file_id"],
                                  caption=caption, parse_mode="HTML", reply_markup=keyboard)
        elif ctype == "video" and data.get("file_id"):
            await bot.send_video(chat_id=chat_id, video=data["file_id"],
                                  caption=caption, parse_mode="HTML", reply_markup=keyboard)
        elif ctype == "document" and data.get("file_id"):
            await bot.send_document(chat_id=chat_id, document=data["file_id"],
                                     caption=caption, parse_mode="HTML", reply_markup=keyboard)
        elif ctype == "audio" and data.get("file_id"):
            await bot.send_audio(chat_id=chat_id, audio=data["file_id"],
                                  caption=caption, parse_mode="HTML", reply_markup=keyboard)
        elif ctype == "album":
            items = data.get("items", [])
            album_caption = caption + f"\n共 {len(items)} 个媒体文件"
            await bot.send_message(chat_id=chat_id, text=album_caption,
                                    parse_mode="HTML", reply_markup=keyboard)
        else:
            await bot.send_message(chat_id=chat_id, text=caption,
                                    parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.warning("Preview send failed (%s), fallback to text: %s", ctype, e)
        await bot.send_message(chat_id=chat_id, text=caption,
                                parse_mode="HTML", reply_markup=keyboard)

    return CONFIRMING


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]

    if action == "cancel":
        try:
            await query.edit_message_text("❌ 已取消投稿。")
        except Exception:
            await query.message.reply_text("❌ 已取消投稿。")
        context.user_data.clear()
        return ConversationHandler.END

    if action == "reedit":
        context.user_data.pop("submission", None)
        try:
            await query.edit_message_text("✏️ 请重新发送您的投稿内容（文字、图片、视频、文件或相册）：")
        except Exception:
            await query.message.reply_text("✏️ 请重新发送您的投稿内容（文字、图片、视频、文件或相册）：")
        return WAITING_CONTENT

    # Confirm
    user = update.effective_user
    sub = context.user_data.get("submission", {})
    sign_type = context.user_data.get("sign_type", "anonymous")
    custom_name = context.user_data.get("custom_name", "")

    raw_text = sub.get("raw_text", "")
    filtered_text = await filter_text(raw_text)
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

    try:
        await query.edit_message_text(
            "✅ 投稿已提交，正在等待审核，结果将通过私信通知您。\n感谢您的投稿！"
        )
    except Exception:
        await query.message.reply_text(
            "✅ 投稿已提交，正在等待审核，结果将通过私信通知您。\n感谢您的投稿！"
        )

    await _notify_admins(context.bot, submission_id, user, sub, sign_type)
    context.user_data.clear()
    return ConversationHandler.END


# ── Admin notification (with actual media) ────────────────────────────────────

async def _notify_admins(bot: Bot, submission_id: int, user, sub: dict, sign_type: str) -> None:
    sign_labels = {
        "anonymous": "🕵️ 匿名投稿",
        "username": f"👤 展示用户名 (@{user.username or '无'})",
        "custom": "✏️ 自定义署名",
    }
    sign_label = sign_labels.get(sign_type, "匿名")
    ctype = sub.get("content_type", "text")
    ctype_label = {"text": "文字", "photo": "图片", "video": "视频",
                   "document": "文件", "album": "相册", "audio": "音频"}.get(ctype, "内容")
    data = sub.get("message_data", {})
    content_preview = data.get("text", "") or "[媒体文件]"
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

    import config as _cfg
    targets = []
    if _cfg.REVIEW_GROUP_ID:
        targets.append(_cfg.REVIEW_GROUP_ID)
    else:
        admins = await db.list_admins()
        targets = [a["user_id"] for a in admins]

    for chat_id in targets:
        try:
            # 1. Send review notification with action buttons
            await bot.send_message(chat_id=chat_id, text=text,
                                    parse_mode="HTML", reply_markup=keyboard)
            # 2. Send the actual media so admin can see the real content
            file_id = data.get("file_id")
            media_caption = data.get("text", "") or None
            if ctype == "photo" and file_id:
                await bot.send_photo(chat_id=chat_id, photo=file_id, caption=media_caption)
            elif ctype == "video" and file_id:
                await bot.send_video(chat_id=chat_id, video=file_id, caption=media_caption)
            elif ctype == "document" and file_id:
                await bot.send_document(chat_id=chat_id, document=file_id, caption=media_caption)
            elif ctype == "audio" and file_id:
                await bot.send_audio(chat_id=chat_id, audio=file_id, caption=media_caption)
            elif ctype == "album":
                items = data.get("items", [])
                if items:
                    media_group = []
                    for i, item in enumerate(items[:10]):
                        cap = media_caption if i == 0 else None
                        fid = item.get("file_id")
                        if not fid:
                            continue
                        if item["type"] == "photo":
                            media_group.append(InputMediaPhoto(media=fid, caption=cap))
                        elif item["type"] == "video":
                            media_group.append(InputMediaVideo(media=fid, caption=cap))
                        elif item["type"] == "document":
                            media_group.append(InputMediaDocument(media=fid, caption=cap))
                    if media_group:
                        await bot.send_media_group(chat_id=chat_id, media=media_group)
        except Exception as e:
            logger.error("Failed to notify admin %s: %s", chat_id, e)


# ── Cancel / timeout fallback ─────────────────────────────────────────────────

async def cancel_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.effective_message.reply_text("❌ 已取消投稿。")
    return ConversationHandler.END


# ── ConversationHandler factory ───────────────────────────────────────────────

def build_submission_conversation() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, receive_content),
        ],
        states={
            WAITING_CONTENT: [
                CallbackQueryHandler(handle_user_action, pattern=r"^user_action:"),
                MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, receive_content),
            ],
            WAITING_SIGNATURE: [
                CallbackQueryHandler(handle_signature_choice, pattern=r"^sign:"),
                # Bug 1 fix: handle text typed in this state instead of re-entering
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_text_in_signature_state),
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
