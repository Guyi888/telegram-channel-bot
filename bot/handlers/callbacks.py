"""
Review callbacks — admin approves / rejects / edits a submission.

Callback data patterns:
  review:approve:<submission_id>
  review:reject:<submission_id>
  review:edit:<submission_id>
  review:reason:<submission_id>:<reason_key>   (pre-defined reason)
  review:custom_reason:<submission_id>          (admin types reason)
  category:<submission_id>:<category_name>      (category override during edit)
"""
from __future__ import annotations

import logging
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler, CommandHandler, ConversationHandler,
    ContextTypes, MessageHandler, filters,
)

from database import db
from services import publisher
from services.classifier import classify_text, get_all_category_names

logger = logging.getLogger(__name__)

# Pre-defined rejection reasons
REJECT_REASONS = [
    ("内容与频道主题不符", "off_topic"),
    ("含有违规内容", "violation"),
    ("重复内容", "duplicate"),
    ("广告/垃圾信息", "spam"),
    ("其他原因", "other"),
]

# Conversation state for admin typing a custom reason
_TYPING_CUSTOM_REASON = 100


async def handle_review_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    admin = update.effective_user

    if not await db.is_admin(admin.id):
        await query.answer("⛔ 无权限", show_alert=True)
        return

    parts = query.data.split(":")
    action = parts[1]
    submission_id = int(parts[2])

    submission = await db.get_submission(submission_id)
    if not submission:
        await query.edit_message_text("⚠️ 投稿不存在或已被处理。")
        return

    if submission["status"] != "pending":
        await query.edit_message_text(
            f"⚠️ 该投稿已被处理（状态：{submission['status']}）。"
        )
        return

    if action == "approve":
        await _do_approve(query, admin, submission_id, submission, context)
    elif action == "reject":
        await _show_reject_reasons(query, submission_id)
    elif action == "edit":
        await _do_edit_flow(query, admin, submission_id, submission, context)


async def _do_approve(query, admin, submission_id: int, submission: dict, context) -> None:
    """Classify, publish, update DB, notify submitter."""
    target = await db.get_target_channel()
    if not target:
        await query.edit_message_text("⚠️ 未设置目标频道，无法发布。")
        return

    # Auto-classify
    raw_text = submission["message_data"].get("text", "")
    category = await classify_text(raw_text)

    # Let admin confirm/change category
    cat_names = await get_all_category_names()
    keyboard_rows = []
    row = []
    for name in cat_names:
        row.append(InlineKeyboardButton(
            f"{'✅ ' if name == category else ''}#{name}",
            callback_data=f"cat_approve:{submission_id}:{name}",
        ))
        if len(row) == 3:
            keyboard_rows.append(row)
            row = []
    if row:
        keyboard_rows.append(row)
    keyboard_rows.append([
        InlineKeyboardButton(
            f"✅ 确认使用 #{category}", callback_data=f"cat_approve:{submission_id}:{category}"
        )
    ])

    await query.edit_message_text(
        f"📋 系统自动识别分类：<b>#{category}</b>（点击修改或确认）",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )


async def handle_category_approve(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Admin selected a category — now publish."""
    query = update.callback_query
    await query.answer()
    admin = update.effective_user

    if not await db.is_admin(admin.id):
        return

    parts = query.data.split(":")
    submission_id = int(parts[1])
    category = parts[2]

    submission = await db.get_submission(submission_id)
    if not submission or submission["status"] != "pending":
        await query.edit_message_text("⚠️ 投稿已被处理。")
        return

    target = await db.get_target_channel()
    if not target:
        await query.edit_message_text("⚠️ 未设置目标频道。")
        return

    msg_id = await publisher.publish_from_submission(
        context.bot, target, submission, category=category
    )
    if msg_id:
        await db.update_submission_status(
            submission_id, "approved", reviewed_by=admin.id
        )
        await db.log_action(
            admin.id, "approve_submission",
            f"submission_id={submission_id}, category={category}"
        )
        await query.edit_message_text(
            f"✅ 投稿 #{submission_id} 已发布至频道，分类：#{category}。"
        )
        # Notify submitter
        await _notify_submitter_approved(context.bot, submission)
    else:
        await query.edit_message_text("❌ 发布失败，请检查目标频道配置。")


async def _show_reject_reasons(query, submission_id: int) -> None:
    keyboard = []
    for label, key in REJECT_REASONS:
        keyboard.append([InlineKeyboardButton(
            label,
            callback_data=f"review_reason:{submission_id}:{key}",
        )])
    keyboard.append([InlineKeyboardButton(
        "✏️ 输入自定义原因",
        callback_data=f"review_custom_reason:{submission_id}",
    )])
    keyboard.append([InlineKeyboardButton(
        "← 返回", callback_data=f"review:approve:{submission_id}",  # just go back
    )])
    await query.edit_message_text(
        "请选择拒绝原因：",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_reject_reason(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    admin = update.effective_user

    if not await db.is_admin(admin.id):
        return

    parts = query.data.split(":")
    submission_id = int(parts[1])
    reason_key = parts[2]

    reason_map = {key: label for label, key in REJECT_REASONS}
    reason_text = reason_map.get(reason_key, reason_key)

    await _finalize_rejection(context.bot, query, admin, submission_id, reason_text)


async def handle_custom_reason_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Ask admin to type a rejection reason."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    submission_id = int(parts[1])
    context.user_data["reject_submission_id"] = submission_id

    await query.edit_message_text(
        "✏️ 请输入拒绝理由（直接发送文字）："
    )
    return _TYPING_CUSTOM_REASON


async def handle_custom_reason_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    msg = update.effective_message
    admin = update.effective_user
    reason = msg.text.strip() if msg.text else ""

    if not reason:
        await msg.reply_text("⚠️ 理由不能为空，请重新输入：")
        return _TYPING_CUSTOM_REASON

    submission_id = context.user_data.pop("reject_submission_id", None)
    if not submission_id:
        await msg.reply_text("⚠️ 找不到对应的投稿。")
        return ConversationHandler.END

    await _finalize_rejection(context.bot, msg, admin, submission_id, reason)
    return ConversationHandler.END


async def _finalize_rejection(bot, target, admin, submission_id: int, reason: str) -> None:
    submission = await db.get_submission(submission_id)
    if not submission or submission["status"] != "pending":
        try:
            await target.reply_text("⚠️ 投稿已被处理。")
        except Exception:
            pass
        return

    await db.update_submission_status(
        submission_id, "rejected",
        reject_reason=reason,
        reviewed_by=admin.id,
    )
    await db.log_action(
        admin.id, "reject_submission",
        f"submission_id={submission_id}, reason={reason}",
    )

    edit_text = f"❌ 投稿 #{submission_id} 已拒绝。\n原因：{reason}"
    try:
        if hasattr(target, "edit_message_text"):
            await target.edit_message_text(edit_text)
        else:
            await target.reply_text(edit_text)
    except Exception:
        pass

    # Notify submitter
    await _notify_submitter_rejected(bot, submission, reason)


async def _do_edit_flow(query, admin, submission_id: int, submission: dict, context) -> None:
    """Admin wants to edit content before publishing — ask for new content."""
    context.user_data["edit_submission_id"] = submission_id
    context.user_data["edit_original"] = submission
    await query.edit_message_text(
        "✏️ 请发送修改后的投稿内容（文字 / 图片 / 视频 / 文件）：\n"
        "发送 /skip 保持原内容直接发布。"
    )


async def handle_admin_edit_content(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Receive the admin's replacement content and publish it."""
    msg = update.effective_message
    admin = update.effective_user

    if not await db.is_admin(admin.id):
        return

    submission_id = context.user_data.pop("edit_submission_id", None)
    original = context.user_data.pop("edit_original", None)
    if not submission_id or not original:
        return

    target = await db.get_target_channel()
    if not target:
        await msg.reply_text("⚠️ 未设置目标频道。")
        return

    if msg.text == "/skip":
        # Publish original
        category = await classify_text(original["message_data"].get("text", ""))
        msg_id = await publisher.publish_from_submission(
            context.bot, target, original, category=category
        )
    else:
        # Build replacement submission from admin's message
        from utils.helpers import extract_text as _et
        text = _et(msg)
        if msg.photo:
            data = {"file_id": msg.photo[-1].file_id, "text": text}
            ctype = "photo"
        elif msg.video:
            data = {"file_id": msg.video.file_id, "text": text}
            ctype = "video"
        elif msg.document:
            data = {"file_id": msg.document.file_id, "text": text}
            ctype = "document"
        else:
            data = {"text": text}
            ctype = "text"

        replacement = dict(original)
        replacement["content_type"] = ctype
        replacement["message_data"] = data

        category = await classify_text(text)
        msg_id = await publisher.publish_from_submission(
            context.bot, target, replacement, category=category
        )

    if msg_id:
        await db.update_submission_status(
            submission_id, "approved", reviewed_by=admin.id
        )
        await db.log_action(
            admin.id, "edit_and_approve",
            f"submission_id={submission_id}",
        )
        await msg.reply_text(f"✅ 投稿 #{submission_id} 已编辑并发布。")
        await _notify_submitter_approved(context.bot, original)
    else:
        await msg.reply_text("❌ 发布失败，请检查目标频道配置。")


# ── Submitter notifications ───────────────────────────────────────────────────

async def _notify_submitter_approved(bot, submission: dict) -> None:
    try:
        await bot.send_message(
            chat_id=submission["user_id"],
            text="✅ 您的投稿已通过审核并发布至频道！感谢您的投稿。",
        )
    except Exception as e:
        logger.debug("Cannot notify submitter %s: %s", submission["user_id"], e)


async def _notify_submitter_rejected(bot, submission: dict, reason: str) -> None:
    try:
        await bot.send_message(
            chat_id=submission["user_id"],
            text=(
                f"❌ 您的投稿未通过审核\n"
                f"原因：{reason}\n\n"
                f"欢迎重新投稿！"
            ),
        )
    except Exception as e:
        logger.debug("Cannot notify submitter %s: %s", submission["user_id"], e)
