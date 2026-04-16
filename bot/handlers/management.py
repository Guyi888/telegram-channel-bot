"""
Admin management command handlers + inline admin panel.

Bug fixes:
  Bug 4  — /status shows full channel name + ID for all targets
  Bug 6  — Admin button panel via /panel or /start
  Bug 8  — Chinese text command support
  Bug 9  — Unlimited target channels (/addtarget /deltarget)
  Bug 10 — Admin logs displayed in Chinese
"""
from __future__ import annotations

import logging
import re

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.ext import (
    CallbackQueryHandler, CommandHandler, ContextTypes,
    MessageHandler, filters,
)

import config
from database import db
from services.word_filter import invalidate_cache as invalidate_word_cache
from utils.helpers import escape_html, format_ts

logger = logging.getLogger(__name__)

# ── Chinese action label map (Bug 10) ────────────────────────────────────────

ACTION_LABELS = {
    "set_target_channel":   "设置目标频道",
    "add_target_channel":   "添加目标频道",
    "del_target_channel":   "删除目标频道",
    "add_source_channel":   "添加来源频道",
    "del_source_channel":   "删除来源频道",
    "add_admin":            "添加管理员",
    "del_admin":            "删除管理员",
    "set_discussion_group": "设置讨论群组",
    "toggle_ad_package":    "切换广告套餐状态",
    "delete_ad_package":    "删除广告套餐",
    "add_ad_package":       "添加广告套餐",
    "add_ad_button":        "添加广告按钮",
    "add_category":         "添加分类",
    "del_category":         "删除分类",
    "add_bad_word":         "添加违禁词",
    "del_bad_word":         "删除违禁词",
    "set_submission_limit": "设置投稿上限",
    "ban_user":             "封禁用户",
    "unban_user":           "解封用户",
    "approve_submission":   "通过投稿",
    "reject_submission":    "拒绝投稿",
    "edit_and_approve":     "编辑并发布投稿",
    "refresh_collector":    "刷新采集源",
}


def _cn_action(action: str) -> str:
    return ACTION_LABELS.get(action, action)


# ── Permission decorators ─────────────────────────────────────────────────────

def super_only(handler):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not await db.is_super_admin(uid):
            await update.effective_message.reply_text("⛔ 此命令仅超级管理员可用。")
            return
        return await handler(update, context)
    wrapper.__name__ = handler.__name__
    return wrapper


def admin_only(handler):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not await db.is_admin(uid):
            return
        return await handler(update, context)
    wrapper.__name__ = handler.__name__
    return wrapper


# ── Admin main panel (Bug 6/8) ────────────────────────────────────────────────

async def show_admin_main_panel(target, context) -> None:
    """Show the main admin control panel."""
    text = "🎛 <b>管理员控制台</b>\n\n请选择要管理的功能："
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 频道管理", callback_data="admpanel:channels"),
            InlineKeyboardButton("👥 管理员管理", callback_data="admpanel:admins"),
        ],
        [
            InlineKeyboardButton("📂 分类管理", callback_data="admpanel:categories"),
            InlineKeyboardButton("🚫 违禁词管理", callback_data="admpanel:badwords"),
        ],
        [
            InlineKeyboardButton("📦 广告套餐", callback_data="admpanel:ads"),
            InlineKeyboardButton("⚙️ 系统设置", callback_data="admpanel:settings"),
        ],
        [
            InlineKeyboardButton("📊 运行状态", callback_data="admpanel:status"),
            InlineKeyboardButton("📋 操作日志", callback_data="admpanel:logs"),
        ],
    ])
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await target.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def handle_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route admin panel callbacks."""
    query = update.callback_query
    await query.answer()
    if not await db.is_admin(update.effective_user.id):
        return

    parts = query.data.split(":")
    sub = parts[1] if len(parts) > 1 else ""

    if sub == "back":
        await show_admin_main_panel(query, context)

    elif sub == "channels":
        targets = await db.get_all_target_channels()
        sources = await db.get_source_channels()
        t_lines = [f"• {r.get('channel_name','未知')} (<code>{r['channel_id']}</code>)" for r in targets] or ["未设置"]
        s_lines = [f"• {r.get('channel_name','未知')} (<code>{r['channel_id']}</code>)" for r in sources] or ["未设置"]
        text = (
            "📢 <b>频道管理</b>\n\n"
            "<b>目标频道（发布内容到）：</b>\n" + "\n".join(t_lines) + "\n\n"
            "<b>来源频道（采集内容从）：</b>\n" + "\n".join(s_lines) + "\n\n"
            "发送 <code>添加目标频道 @xxx</code> 或 <code>/addtarget @xxx</code>\n"
            "发送 <code>删除目标频道 @xxx</code> 或 <code>/deltarget @xxx</code>\n"
            "发送 <code>添加来源频道 @xxx</code> 或 <code>/addsource @xxx</code>\n"
            "发送 <code>删除来源频道 @xxx</code> 或 <code>/delsource @xxx</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "admins":
        admins = await db.list_admins()
        lines = []
        for a in admins:
            level_str = "🔑 超级管理员" if a["level"] == 1 else "👤 管理员"
            uname = f"@{a['username']}" if a.get("username") else str(a["user_id"])
            lines.append(f"• {uname} ({level_str})")
        text = (
            "👥 <b>管理员列表</b>\n\n" + ("\n".join(lines) or "无管理员") + "\n\n"
            "发送 <code>添加管理员 @xxx</code> 或 <code>/addadmin @xxx</code>\n"
            "发送 <code>删除管理员 ID</code> 或 <code>/deladmin ID</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "categories":
        cats = await db.get_categories()
        if cats:
            lines = [
                f"• #{escape_html(c['name'])}{'（默认）' if c['is_default'] else ''} ID:{c['id']} 关键词:{len(c['keywords'])}"
                for c in cats
            ]
            text = "📂 <b>分类列表</b>\n\n" + "\n".join(lines) + "\n\n"
        else:
            text = "📂 <b>分类列表</b>\n\n暂无分类\n\n"
        text += (
            "发送 <code>添加分类 名称</code> 或 <code>/addcat 名称</code>\n"
            "发送 <code>/addkw 分类ID 关键词 权重</code>\n"
            "发送 <code>/delcat 分类ID</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "badwords":
        words = await db.get_bad_words()
        if words:
            lines = [
                f"{i}. {escape_html(w['word'])}{'（模糊）' if w['fuzzy_match'] else ''} ID:{w['id']}"
                for i, w in enumerate(words[:20], 1)
            ]
            text = "🚫 <b>违禁词列表</b>\n\n" + "\n".join(lines) + "\n\n"
        else:
            text = "🚫 <b>违禁词列表</b>\n\n暂无违禁词\n\n"
        text += (
            "发送 <code>添加违禁词 词语</code> 或 <code>/addbw 词语</code>\n"
            "发送 <code>/delbw ID</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "ads":
        await _show_ad_menu(query, context)

    elif sub == "settings":
        limit = await db.get_submission_limit()
        discussion = await db.get_discussion_group() or "未设置"
        text = (
            "⚙️ <b>系统设置</b>\n\n"
            f"📝 每日投稿上限：{limit} 条\n"
            f"💬 讨论群组：{escape_html(discussion)}\n\n"
            "发送 <code>设置投稿上限 数字</code> 或 <code>/submissionlimit 数字</code>\n"
            "发送 <code>/setgroup URL</code> — 设置讨论群组"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "status":
        targets = await db.get_all_target_channels()
        sources = await db.get_source_channels()
        admins = await db.list_admins()
        daily_limit = await db.get_submission_limit()
        stats = await db.get_daily_stats()
        t_str = ", ".join(r.get("channel_name", str(r["channel_id"])) for r in targets) or "未设置"
        text = (
            f"📊 <b>运行状态</b>\n\n"
            f"📢 目标频道：{escape_html(t_str)}\n"
            f"📡 来源频道：{len(sources)} 个\n"
            f"👥 管理员：{len(admins)} 人\n"
            f"📝 投稿上限：{daily_limit} 条\n\n"
            f"📈 今日统计（{stats['date']}）\n"
            f"  投稿：{stats['submissions']}  发布：{stats['approved']}  拒绝：{stats['rejected']}\n"
            f"  👍 {stats['likes']}  👎 {stats['dislikes']}"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]])
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)

    elif sub == "logs":
        page = int(parts[2]) if len(parts) > 2 else 1
        offset = (page - 1) * 10
        logs = await db.get_logs(limit=10, offset=offset)
        lines = [f"📋 <b>操作日志（第 {page} 页）</b>\n"]
        for log in logs:
            admin_name = f"@{log['admin_username']}" if log.get("admin_username") else str(log["admin_id"])
            action_cn = _cn_action(log["action"])
            lines.append(
                f"<code>{format_ts(log['created_at'])}</code>  {admin_name}\n"
                f"  → {escape_html(action_cn)}"
                + (f"  <i>{escape_html(str(log['detail'] or ''))}</i>" if log.get("detail") else "")
            )
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("← 上页", callback_data=f"admpanel:logs:{page-1}"))
        if len(logs) == 10:
            nav.append(InlineKeyboardButton("下页 →", callback_data=f"admpanel:logs:{page+1}"))
        rows = ([nav] if nav else []) + [[InlineKeyboardButton("← 返回主菜单", callback_data="admpanel:back")]]
        kb = InlineKeyboardMarkup(rows)
        await query.edit_message_text(
            "\n\n".join(lines) if logs else "暂无操作日志。",
            parse_mode="HTML", reply_markup=kb,
        )


# ── /panel ────────────────────────────────────────────────────────────────────

@admin_only
async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_admin_main_panel(update.message, context)


# ── /setchannel / /addtarget / /deltarget ────────────────────────────────────

@super_only
async def cmd_setchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/setchannel @频道  （替换所有目标频道）")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        await db.set_target_channel(chat.id, chat.title or raw)
        await db.log_action(update.effective_user.id, "set_target_channel", str(chat.id))
        await update.message.reply_text(
            f"✅ 目标频道已设置为：{escape_html(chat.title or raw)} (<code>{chat.id}</code>)",
            parse_mode="HTML",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ 设置失败：{e}\n请确保机器人已加入该频道且有发布权限。")


@super_only
async def cmd_addtarget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add an additional target channel without removing existing ones."""
    if not context.args:
        await update.message.reply_text("用法：/addtarget @频道  （追加，不替换现有目标频道）")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        await db.add_target_channel(chat.id, chat.title or raw)
        await db.log_action(update.effective_user.id, "add_target_channel", str(chat.id))
        await update.message.reply_text(
            f"✅ 已追加目标频道：{escape_html(chat.title or raw)} (<code>{chat.id}</code>)",
            parse_mode="HTML",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ 添加失败：{e}")


@super_only
async def cmd_deltarget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/deltarget @频道 或 /deltarget -100xxx")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        removed = await db.remove_target_channel(chat.id)
        if removed:
            await db.log_action(update.effective_user.id, "del_target_channel", str(chat.id))
            await update.message.reply_text(f"✅ 已移除目标频道：<code>{chat.id}</code>", parse_mode="HTML")
        else:
            await update.message.reply_text("⚠️ 该频道不在目标列表中。")
    except Exception as e:
        await update.message.reply_text(f"❌ 操作失败：{e}")


# ── /addsource / /delsource ───────────────────────────────────────────────────

@super_only
async def cmd_addsource(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/addsource @频道")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        await db.add_source_channel(chat.id, chat.title or raw)
        await db.log_action(update.effective_user.id, "add_source_channel", str(chat.id))
        # Refresh running collector + auto-join
        collector = context.bot_data.get("collector")
        if collector:
            sources = await db.get_source_channels()
            collector.refresh_sources_sync([s["channel_id"] for s in sources])
            try:
                await collector.join_channel(chat.id)
            except Exception as je:
                logger.warning("Auto-join %s failed: %s", chat.id, je)
        await update.message.reply_text(
            f"✅ 已添加来源频道：{escape_html(chat.title or raw)} (<code>{chat.id}</code>)",
            parse_mode="HTML",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ 添加失败：{e}")


@super_only
async def cmd_delsource(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/delsource @频道")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        removed = await db.remove_source_channel(chat.id)
        if removed:
            await db.log_action(update.effective_user.id, "del_source_channel", str(chat.id))
            collector = context.bot_data.get("collector")
            if collector:
                sources = await db.get_source_channels()
                collector.refresh_sources_sync([s["channel_id"] for s in sources])
            await update.message.reply_text(f"✅ 已移除来源频道：<code>{chat.id}</code>", parse_mode="HTML")
        else:
            await update.message.reply_text("⚠️ 该频道不在来源列表中。")
    except Exception as e:
        await update.message.reply_text(f"❌ 操作失败：{e}")


# ── /addadmin / /deladmin ─────────────────────────────────────────────────────

@super_only
async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/addadmin @用户 或 /addadmin <user_id>")
        return
    raw = context.args[0]
    try:
        if raw.startswith("@"):
            user = await context.bot.get_chat(raw)
            uid, uname = user.id, user.username or ""
        else:
            uid, uname = int(raw), ""
    except Exception as e:
        await update.message.reply_text(f"❌ 无法获取用户信息：{e}")
        return
    if uid in config.SUPER_ADMIN_IDS:
        await update.message.reply_text("⚠️ 该用户是超级管理员，无需添加。")
        return
    await db.add_admin(uid, uname, level=2, added_by=update.effective_user.id)
    await db.log_action(update.effective_user.id, "add_admin", f"uid={uid}")
    await update.message.reply_text(f"✅ 已将 <code>{uid}</code> 添加为管理员。", parse_mode="HTML")


@super_only
async def cmd_deladmin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/deladmin @用户 或 /deladmin <user_id>")
        return
    raw = context.args[0]
    try:
        uid = int(raw) if raw.lstrip("-").isdigit() else (await context.bot.get_chat(raw)).id
    except Exception as e:
        await update.message.reply_text(f"❌ 无法获取用户：{e}")
        return
    removed = await db.remove_admin(uid)
    if removed:
        await db.log_action(update.effective_user.id, "del_admin", f"uid={uid}")
        await update.message.reply_text(f"✅ 已移除管理员 <code>{uid}</code>。", parse_mode="HTML")
    else:
        await update.message.reply_text("⚠️ 未找到该普通管理员（超级管理员不可删除）。")


# ── /setgroup ─────────────────────────────────────────────────────────────────

@super_only
async def cmd_setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/setgroup https://t.me/your_group")
        return
    url = context.args[0]
    await db.set_discussion_group(url)
    await db.log_action(update.effective_user.id, "set_discussion_group", url)
    await update.message.reply_text(f"✅ 讨论群组已设置为：{url}")


# ── /submissionlimit ──────────────────────────────────────────────────────────

@super_only
async def cmd_submissionlimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        current = await db.get_submission_limit()
        await update.message.reply_text(f"当前每日投稿上限：{current} 条\n用法：/submissionlimit <数字>")
        return
    n = int(context.args[0])
    if n < 1 or n > 10000:
        await update.message.reply_text("⚠️ 请设置 1–10000 之间的数值。")
        return
    await db.set_submission_limit(n)
    await db.log_action(update.effective_user.id, "set_submission_limit", str(n))
    await update.message.reply_text(f"✅ 每日投稿上限已设为 {n} 条。")


# ── /ban / /unban ─────────────────────────────────────────────────────────────

@admin_only
async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/ban @用户 或 /ban <user_id>")
        return
    raw = context.args[0]
    try:
        uid = int(raw) if raw.lstrip("-").isdigit() else (await context.bot.get_chat(raw)).id
    except Exception as e:
        await update.message.reply_text(f"❌ 无法获取用户：{e}")
        return
    await db.add_to_blacklist(uid, update.effective_user.id)
    await db.log_action(update.effective_user.id, "ban_user", f"uid={uid}")
    await update.message.reply_text(f"✅ 用户 <code>{uid}</code> 已加入黑名单。", parse_mode="HTML")


@admin_only
async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/unban @用户 或 /unban <user_id>")
        return
    raw = context.args[0]
    try:
        uid = int(raw) if raw.lstrip("-").isdigit() else (await context.bot.get_chat(raw)).id
    except Exception as e:
        await update.message.reply_text(f"❌ 无法获取用户：{e}")
        return
    removed = await db.remove_from_blacklist(uid)
    if removed:
        await db.log_action(update.effective_user.id, "unban_user", f"uid={uid}")
        await update.message.reply_text(f"✅ 用户 <code>{uid}</code> 已从黑名单移除。", parse_mode="HTML")
    else:
        await update.message.reply_text("⚠️ 该用户不在黑名单中。")


# ── /logs ─────────────────────────────────────────────────────────────────────

@super_only
async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    offset = (page - 1) * 10
    logs = await db.get_logs(limit=10, offset=offset)
    if not logs:
        await update.message.reply_text("📋 暂无操作日志。")
        return
    lines = [f"📋 <b>操作日志（第 {page} 页）</b>\n"]
    for log in logs:
        admin_name = f"@{log['admin_username']}" if log.get("admin_username") else str(log["admin_id"])
        lines.append(
            f"<code>{format_ts(log['created_at'])}</code>  {admin_name}\n"
            f"  → {escape_html(_cn_action(log['action']))}"
            + (f"  <i>{escape_html(str(log['detail'] or ''))}</i>" if log.get("detail") else "")
        )
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("← 上一页", callback_data=f"logs:{page-1}"))
    if len(logs) == 10:
        nav.append(InlineKeyboardButton("下一页 →", callback_data=f"logs:{page+1}"))
    markup = InlineKeyboardMarkup([nav]) if nav else None
    await update.message.reply_text("\n\n".join(lines), parse_mode="HTML", reply_markup=markup)


async def handle_logs_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not await db.is_super_admin(update.effective_user.id):
        return
    page = int(query.data.split(":")[1])
    offset = (page - 1) * 10
    logs = await db.get_logs(limit=10, offset=offset)
    lines = [f"📋 <b>操作日志（第 {page} 页）</b>\n"]
    for log in logs:
        admin_name = f"@{log['admin_username']}" if log.get("admin_username") else str(log["admin_id"])
        lines.append(
            f"<code>{format_ts(log['created_at'])}</code>  {admin_name}\n"
            f"  → {escape_html(_cn_action(log['action']))}"
        )
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("← 上一页", callback_data=f"logs:{page-1}"))
    if len(logs) == 10:
        nav.append(InlineKeyboardButton("下一页 →", callback_data=f"logs:{page+1}"))
    markup = InlineKeyboardMarkup([nav]) if nav else None
    await query.edit_message_text(
        "\n\n".join(lines) if logs else "无更多日志。",
        parse_mode="HTML", reply_markup=markup,
    )


# ── /status ───────────────────────────────────────────────────────────────────

@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    targets = await db.get_all_target_channels()
    sources = await db.get_source_channels()
    admins = await db.list_admins()
    daily_limit = await db.get_submission_limit()
    discussion = await db.get_discussion_group() or "未设置"
    stats = await db.get_daily_stats()

    t_lines = "\n".join(
        f"  • {r.get('channel_name','未知')} (<code>{r['channel_id']}</code>)" for r in targets
    ) if targets else "  未设置"
    s_lines = "\n".join(
        f"  • {r.get('channel_name','未知')} (<code>{r['channel_id']}</code>)" for r in sources
    ) if sources else "  未设置"

    text = (
        f"🤖 <b>机器人运行状态</b>\n\n"
        f"📢 目标频道：\n{t_lines}\n\n"
        f"📡 来源频道（共 {len(sources)} 个）：\n{s_lines}\n\n"
        f"👥 管理员：{len(admins)} 人\n"
        f"💬 讨论群组：{escape_html(discussion)}\n"
        f"📝 每日投稿上限：{daily_limit} 条\n\n"
        f"📊 <b>今日统计（{stats['date']}）</b>\n"
        f"  投稿数：{stats['submissions']}\n"
        f"  发布数：{stats['approved']}\n"
        f"  拒绝数：{stats['rejected']}\n"
        f"  👍 点赞：{stats['likes']}  👎 点踩：{stats['dislikes']}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ── /buttons / Ad package management ─────────────────────────────────────────

@super_only
async def cmd_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _show_ad_menu(update.message, context)


async def _show_ad_menu(target, context) -> None:
    packages = await db.get_ad_packages()
    lines = ["📦 <b>广告套餐列表</b>\n"]
    keyboard = []
    for pkg in packages:
        status = "✅" if pkg["is_active"] else "❌"
        default_tag = "（默认）" if pkg["is_default"] else ""
        time_info = f"{pkg['start_time']}–{pkg['end_time']}" if pkg["start_time"] else "全天"
        lines.append(f"{status} <b>{escape_html(pkg['name'])}</b> {default_tag}  [{time_info}]  ID:{pkg['id']}")
        keyboard.append([
            InlineKeyboardButton(
                f"{'禁用' if pkg['is_active'] else '启用'} {pkg['name']}",
                callback_data=f"adpkg:toggle:{pkg['id']}:{0 if pkg['is_active'] else 1}",
            ),
            InlineKeyboardButton("🗑 删除", callback_data=f"adpkg:del:{pkg['id']}"),
        ])
    keyboard.append([InlineKeyboardButton("➕ 新建套餐", callback_data="adpkg:new")])
    msg_text = ("\n".join(lines) if packages else "暂无广告套餐。\n")
    msg_text += "\n\n使用 /addbtn <套餐ID> <文字> <URL> 添加按钮。"
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(msg_text, parse_mode="HTML",
                                        reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await target.reply_text(msg_text, parse_mode="HTML",
                                  reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_adpkg_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if not await db.is_super_admin(update.effective_user.id):
        return
    parts = query.data.split(":")
    action = parts[1]
    if action == "toggle":
        pkg_id, new_state = int(parts[2]), int(parts[3])
        await db.toggle_ad_package(pkg_id, bool(new_state))
        await db.log_action(update.effective_user.id, "toggle_ad_package",
                             f"id={pkg_id}, active={new_state}")
        await _show_ad_menu(query, context)
    elif action == "del":
        pkg_id = int(parts[2])
        await db.delete_ad_package(pkg_id)
        await db.log_action(update.effective_user.id, "delete_ad_package", f"id={pkg_id}")
        await _show_ad_menu(query, context)


@super_only
async def cmd_addbtn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    usage = "用法：/addbtn <套餐ID> <按钮文字> <URL> [行] [列]"
    if len(context.args) < 3:
        await update.message.reply_text(usage)
        return
    try:
        pkg_id = int(context.args[0])
        label = context.args[1]
        url = context.args[2]
        row = int(context.args[3]) if len(context.args) > 3 else 0
        col = int(context.args[4]) if len(context.args) > 4 else 0
    except ValueError:
        await update.message.reply_text(usage)
        return
    btn_id = await db.add_ad_button(pkg_id, label, url, row, col)
    await db.log_action(update.effective_user.id, "add_ad_button",
                         f"pkg={pkg_id}, label={label}")
    await update.message.reply_text(f"✅ 按钮已添加（ID: {btn_id}）。")


@super_only
async def cmd_addpkg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    usage = "用法：/addpkg <名称> [开始时间 HH:MM] [结束时间 HH:MM] [优先级]"
    if not context.args:
        await update.message.reply_text(usage)
        return
    name = context.args[0]
    start = context.args[1] if len(context.args) > 1 else None
    end = context.args[2] if len(context.args) > 2 else None
    priority = int(context.args[3]) if len(context.args) > 3 else 0
    is_default = (start is None)
    pkg_id = await db.add_ad_package(name, start, end, is_default=is_default, priority=priority)
    await db.log_action(update.effective_user.id, "add_ad_package", f"name={name}, id={pkg_id}")
    await update.message.reply_text(
        f"✅ 套餐「{name}」已创建（ID: {pkg_id}）。\n/addbtn {pkg_id} <文字> <URL> 添加按钮。"
    )


# ── /categories ───────────────────────────────────────────────────────────────

@super_only
async def cmd_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cats = await db.get_categories()
    if not cats:
        await update.message.reply_text(
            "暂无分类。\n/addcat <名称> — 添加\n/addkw <分类ID> <关键词> <权重> — 添加关键词"
        )
        return
    lines = ["📂 <b>分类列表</b>\n"]
    for c in cats:
        default_tag = "（默认）" if c["is_default"] else ""
        lines.append(
            f"• <b>#{escape_html(c['name'])}</b> {default_tag}  ID:{c['id']}  关键词:{len(c['keywords'])}"
        )
    lines.append("\n/addcat <名称> [default] — 新增分类")
    lines.append("/addkw <分类ID> <关键词> <权重> — 添加关键词")
    lines.append("/delcat <分类ID> — 删除分类")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@super_only
async def cmd_addcat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/addcat <分类名> [default]")
        return
    name = context.args[0].lstrip("#")
    is_default = len(context.args) > 1 and context.args[1].lower() == "default"
    cat_id = await db.add_category(name, [], is_default=is_default)
    await db.log_action(update.effective_user.id, "add_category", f"name={name}, id={cat_id}")
    await update.message.reply_text(f"✅ 分类 #{name} 已创建（ID: {cat_id}）。")


@super_only
async def cmd_addkw(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    usage = "用法：/addkw <分类ID> <关键词> <权重>"
    if len(context.args) < 3:
        await update.message.reply_text(usage)
        return
    try:
        cat_id = int(context.args[0])
        word = context.args[1]
        weight = int(context.args[2])
    except ValueError:
        await update.message.reply_text(usage)
        return
    cats = await db.get_categories()
    cat = next((c for c in cats if c["id"] == cat_id), None)
    if not cat:
        await update.message.reply_text("⚠️ 分类不存在。")
        return
    kws = [k for k in cat["keywords"] if k.get("word") != word]
    kws.append({"word": word, "weight": weight})
    await db.update_category(cat_id, keywords=kws)
    await update.message.reply_text(
        f"✅ 已为分类 #{cat['name']} 添加关键词「{word}」（权重:{weight}）。"
    )


@super_only
async def cmd_delcat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("用法：/delcat <分类ID>")
        return
    cat_id = int(context.args[0])
    ok = await db.delete_category(cat_id)
    if ok:
        await db.log_action(update.effective_user.id, "del_category", f"id={cat_id}")
        await update.message.reply_text(f"✅ 分类 ID:{cat_id} 已删除。")
    else:
        await update.message.reply_text("⚠️ 分类不存在。")


# ── /badwords ─────────────────────────────────────────────────────────────────

@super_only
async def cmd_badwords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    words = await db.get_bad_words()
    if not words:
        await update.message.reply_text(
            "暂无违禁词。\n/addbw <词语> [fuzzy] — 添加（fuzzy=模糊匹配）\n/delbw <ID> — 删除"
        )
        return
    lines = ["🚫 <b>违禁词列表</b>\n"]
    for i, w in enumerate(words, 1):
        fuzzy_tag = "（模糊）" if w["fuzzy_match"] else ""
        lines.append(f"{i}. <code>{escape_html(w['word'])}</code> {fuzzy_tag}  ID:{w['id']}")
    lines.append("\n/addbw <词语> [fuzzy] — 添加\n/delbw <ID> — 删除")
    await update.message.reply_text("\n".join(lines[:32]), parse_mode="HTML")


@super_only
async def cmd_addbw(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/addbw <词语> [fuzzy]")
        return
    word = context.args[0]
    fuzzy = len(context.args) > 1 and context.args[1].lower() == "fuzzy"
    ok = await db.add_bad_word(word, fuzzy, update.effective_user.id)
    if ok:
        invalidate_word_cache()
        await db.log_action(update.effective_user.id, "add_bad_word", f"word={word}, fuzzy={fuzzy}")
        mode = "（模糊匹配）" if fuzzy else ""
        await update.message.reply_text(f"✅ 已添加违禁词：{word} {mode}")
    else:
        await update.message.reply_text("⚠️ 添加失败（可能已存在）。")


@super_only
async def cmd_delbw(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("用法：/delbw <违禁词ID>")
        return
    wid = int(context.args[0])
    ok = await db.remove_bad_word(wid)
    if ok:
        invalidate_word_cache()
        await db.log_action(update.effective_user.id, "del_bad_word", f"id={wid}")
        await update.message.reply_text(f"✅ 违禁词 ID:{wid} 已删除。")
    else:
        await update.message.reply_text("⚠️ 未找到该违禁词。")


# ── Chinese text command handler (Bug 8) ──────────────────────────────────────

_CN_CMD_PATTERNS = [
    (re.compile(r"^添加目标频道\s+(\S+)"), "addtarget"),
    (re.compile(r"^删除目标频道\s+(\S+)"), "deltarget"),
    (re.compile(r"^添加来源频道\s+(\S+)"), "addsource"),
    (re.compile(r"^删除来源频道\s+(\S+)"), "delsource"),
    (re.compile(r"^添加管理员\s+(\S+)"), "addadmin"),
    (re.compile(r"^删除管理员\s+(\S+)"), "deladmin"),
    (re.compile(r"^添加分类\s+(\S+)"), "addcat"),
    (re.compile(r"^添加违禁词\s+(\S+)"), "addbw"),
    (re.compile(r"^封禁用户\s+(\S+)"), "ban"),
    (re.compile(r"^解封用户\s+(\S+)"), "unban"),
    (re.compile(r"^设置投稿上限\s+(\d+)"), "submissionlimit"),
    (re.compile(r"^(?:打开面板|管理面板|控制台)$"), "panel"),
]

_CN_CMD_HANDLERS: dict = {}


async def handle_cn_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detect Chinese text commands from admins and route to handlers."""
    if not await db.is_admin(update.effective_user.id):
        return
    text = (update.effective_message.text or "").strip()
    for pattern, cmd_name in _CN_CMD_PATTERNS:
        m = pattern.match(text)
        if m:
            handler_fn = _CN_CMD_HANDLERS.get(cmd_name)
            if handler_fn:
                context.args = list(m.groups()) if m.groups() else []
                await handler_fn(update, context)
            return


# ── Register all handlers ─────────────────────────────────────────────────────

def register_management_handlers(app) -> None:
    """Register all management command and callback handlers."""
    cmds = [
        ("setchannel",      cmd_setchannel),
        ("addtarget",       cmd_addtarget),
        ("deltarget",       cmd_deltarget),
        ("addsource",       cmd_addsource),
        ("delsource",       cmd_delsource),
        ("addadmin",        cmd_addadmin),
        ("deladmin",        cmd_deladmin),
        ("setgroup",        cmd_setgroup),
        ("buttons",         cmd_buttons),
        ("addpkg",          cmd_addpkg),
        ("addbtn",          cmd_addbtn),
        ("categories",      cmd_categories),
        ("addcat",          cmd_addcat),
        ("addkw",           cmd_addkw),
        ("delcat",          cmd_delcat),
        ("badwords",        cmd_badwords),
        ("addbw",           cmd_addbw),
        ("delbw",           cmd_delbw),
        ("submissionlimit", cmd_submissionlimit),
        ("ban",             cmd_ban),
        ("unban",           cmd_unban),
        ("logs",            cmd_logs),
        ("status",          cmd_status),
        ("panel",           cmd_panel),
    ]
    for cmd, handler in cmds:
        app.add_handler(CommandHandler(cmd, handler))

    # Populate Chinese command dispatch table
    _CN_CMD_HANDLERS.update({
        "addtarget":       cmd_addtarget,
        "deltarget":       cmd_deltarget,
        "addsource":       cmd_addsource,
        "delsource":       cmd_delsource,
        "addadmin":        cmd_addadmin,
        "deladmin":        cmd_deladmin,
        "addcat":          cmd_addcat,
        "addbw":           cmd_addbw,
        "ban":             cmd_ban,
        "unban":           cmd_unban,
        "submissionlimit": cmd_submissionlimit,
        "panel":           cmd_panel,
    })

    # Callback handlers
    app.add_handler(CallbackQueryHandler(handle_logs_page,      pattern=r"^logs:\d+$"))
    app.add_handler(CallbackQueryHandler(handle_adpkg_callback, pattern=r"^adpkg:"))
    app.add_handler(CallbackQueryHandler(handle_admin_panel,    pattern=r"^admpanel:"))

    # Chinese text commands — low priority so ConversationHandler runs first
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
            handle_cn_command,
        ),
        group=10,
    )
