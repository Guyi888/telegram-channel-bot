"""
Admin management command handlers.

Command         Level       Purpose
─────────────────────────────────────────────────────
/setchannel     super       Set target publish channel
/addsource      super       Add source collection channel
/delsource      super       Remove source collection channel
/addadmin       super       Add normal admin
/deladmin       super       Remove normal admin
/setgroup       super       Set discussion group URL
/buttons        super       Ad-button package management (interactive menu)
/categories     super       Category management (interactive menu)
/badwords       super       Bad-word management (interactive menu)
/submissionlimit super      Set daily submission limit
/ban            super/norm  Add user to blacklist
/unban          super/norm  Remove user from blacklist
/logs           super       View operation logs
/status         super/norm  Bot status overview
"""
from __future__ import annotations

import logging
from typing import Optional

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup, Update,
)
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes

import config
from database import db
from services.word_filter import invalidate_cache as invalidate_word_cache
from utils.helpers import escape_html, format_ts

logger = logging.getLogger(__name__)


# ── Permission decorators ────────────────────────────────────────────────────

def super_only(handler):
    """Decorator: only allow super admins."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not await db.is_super_admin(uid):
            await update.effective_message.reply_text("⛔ 此命令仅超级管理员可用。")
            return
        return await handler(update, context)
    wrapper.__name__ = handler.__name__
    return wrapper


def admin_only(handler):
    """Decorator: only allow any admin."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not await db.is_admin(uid):
            return  # silently ignore non-admins
        return await handler(update, context)
    wrapper.__name__ = handler.__name__
    return wrapper


# ── /setchannel ───────────────────────────────────────────────────────────────

@super_only
async def cmd_setchannel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/setchannel @频道 或 /setchannel -100xxx")
        return
    raw = context.args[0]
    try:
        # Try to get chat info
        chat = await context.bot.get_chat(raw)
        await db.set_target_channel(chat.id, chat.title or raw)
        await db.log_action(update.effective_user.id, "set_target_channel", str(chat.id))
        await update.message.reply_text(
            f"✅ 目标频道已设置为：{escape_html(chat.title or raw)} (<code>{chat.id}</code>)",
            parse_mode="HTML",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ 设置失败：{e}\n请确保机器人已加入该频道且有发布权限。")


# ── /addsource / /delsource ───────────────────────────────────────────────────

@super_only
async def cmd_addsource(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/addsource @频道 或 /addsource -100xxx")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        await db.add_source_channel(chat.id, chat.title or raw)
        await db.log_action(update.effective_user.id, "add_source_channel", str(chat.id))
        await update.message.reply_text(
            f"✅ 已添加来源频道：{escape_html(chat.title or raw)} (<code>{chat.id}</code>)",
            parse_mode="HTML",
        )
        # Notify Pyrogram collector to refresh its monitored set immediately
        collector = context.application.bot_data.get("collector")
        if collector:
            sources = await db.get_source_channels()
            collector.refresh_sources_sync([s["channel_id"] for s in sources])
    except Exception as e:
        await update.message.reply_text(f"❌ 添加失败：{e}")


@super_only
async def cmd_delsource(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("用法：/delsource @频道 或 /delsource -100xxx")
        return
    raw = context.args[0]
    try:
        chat = await context.bot.get_chat(raw)
        removed = await db.remove_source_channel(chat.id)
        if removed:
            await db.log_action(update.effective_user.id, "del_source_channel", str(chat.id))
            await update.message.reply_text(f"✅ 已移除来源频道：<code>{chat.id}</code>", parse_mode="HTML")
            # Notify Pyrogram collector to refresh its monitored set immediately
            collector = context.application.bot_data.get("collector")
            if collector:
                sources = await db.get_source_channels()
                collector.refresh_sources_sync([s["channel_id"] for s in sources])
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
            uid = user.id
            uname = user.username or ""
        else:
            uid = int(raw)
            uname = ""
    except Exception as e:
        await update.message.reply_text(f"❌ 无法获取用户信息：{e}")
        return

    if uid in config.SUPER_ADMIN_IDS:
        await update.message.reply_text("⚠️ 该用户是超级管理员，无需添加。")
        return

    await db.add_admin(uid, uname, level=2, added_by=update.effective_user.id)
    await db.log_action(update.effective_user.id, "add_admin", f"uid={uid}")
    await update.message.reply_text(
        f"✅ 已将 <code>{uid}</code>（@{uname}）添加为普通管理员。",
        parse_mode="HTML",
    )


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
    if n < 1 or n > 100:
        await update.message.reply_text("⚠️ 请设置 1–100 之间的数值。")
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
    await update.message.reply_text(f"✅ 用户 <code>{uid}</code> 已加入投稿黑名单。", parse_mode="HTML")


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
    page = 1
    if context.args and context.args[0].isdigit():
        page = int(context.args[0])

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
            f"  → {escape_html(log['action'])}"
            + (f"  <i>{escape_html(str(log['detail'] or ''))}</i>" if log.get("detail") else "")
        )

    keyboard = []
    if page > 1:
        keyboard.append(InlineKeyboardButton("← 上一页", callback_data=f"logs:{page-1}"))
    if len(logs) == 10:
        keyboard.append(InlineKeyboardButton("下一页 →", callback_data=f"logs:{page+1}"))

    markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="HTML", reply_markup=markup
    )


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
            f"  → {escape_html(log['action'])}"
        )

    keyboard = []
    if page > 1:
        keyboard.append(InlineKeyboardButton("← 上一页", callback_data=f"logs:{page-1}"))
    if len(logs) == 10:
        keyboard.append(InlineKeyboardButton("下一页 →", callback_data=f"logs:{page+1}"))

    markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    await query.edit_message_text(
        "\n\n".join(lines) if logs else "无更多日志。",
        parse_mode="HTML", reply_markup=markup
    )


# ── /status ───────────────────────────────────────────────────────────────────

@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target = await db.get_target_channel() or "未设置"
    sources = await db.get_source_channels()
    admins = await db.list_admins()
    daily_limit = await db.get_submission_limit()
    discussion = await db.get_discussion_group() or "未设置"
    stats = await db.get_daily_stats()

    text = (
        f"🤖 <b>机器人运行状态</b>\n\n"
        f"📢 目标频道：<code>{target}</code>\n"
        f"📡 来源频道：{len(sources)} 个\n"
        f"👥 管理员：{len(admins)} 人\n"
        f"💬 讨论群组：{escape_html(discussion)}\n"
        f"📝 每日投稿上限：{daily_limit} 条\n\n"
        f"📊 <b>今日统计（{stats['date']}）</b>\n"
        f"  投稿数：{stats['submissions']}\n"
        f"  发布数：{stats['approved']}\n"
        f"  拒绝数：{stats['rejected']}\n"
        f"  👍：{stats['likes']}  👎：{stats['dislikes']}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ── /buttons — Ad package management (interactive) ────────────────────────────

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
            InlineKeyboardButton(f"🗑 删除", callback_data=f"adpkg:del:{pkg['id']}"),
        ])

    keyboard.append([InlineKeyboardButton("➕ 新建套餐", callback_data="adpkg:new")])

    msg_text = "\n".join(lines) if packages else "暂无广告套餐。\n"
    msg_text += "\n\n使用 /addbtn <套餐ID> <文字> <URL> <行> <列> 添加按钮。"

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


# Quick command to add a button to an existing package
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
                         f"pkg={pkg_id}, label={label}, url={url}")
    await update.message.reply_text(f"✅ 按钮已添加（ID: {btn_id}）。")


# Quick command to create a package
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
        f"✅ 套餐「{name}」已创建（ID: {pkg_id}）。\n"
        f"使用 /addbtn {pkg_id} <文字> <URL> 添加按钮。"
    )


# ── /categories ───────────────────────────────────────────────────────────────

@super_only
async def cmd_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cats = await db.get_categories()
    if not cats:
        await update.message.reply_text(
            "暂无分类。使用 /addcat <名称> 添加分类。\n"
            "使用 /addkw <分类ID> <关键词> <权重> 添加关键词。"
        )
        return
    lines = ["📂 <b>分类列表</b>\n"]
    for c in cats:
        default_tag = "（默认）" if c["is_default"] else ""
        kw_count = len(c["keywords"])
        lines.append(f"• <b>#{escape_html(c['name'])}</b> {default_tag}  ID:{c['id']}  关键词:{kw_count}")
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

    kws = cat["keywords"]
    kws = [k for k in kws if k.get("word") != word]  # replace if exists
    kws.append({"word": word, "weight": weight})
    await db.update_category(cat_id, keywords=kws)
    await update.message.reply_text(f"✅ 已为分类 #{cat['name']} 添加关键词「{word}」（权重:{weight}）。")


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
    # Paginate at 30 words per page
    text = "\n".join(lines[:32])
    await update.message.reply_text(text, parse_mode="HTML")


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


# ── Register all handlers ─────────────────────────────────────────────────────

def register_management_handlers(app) -> None:
    """Register all management command handlers on the Application."""
    cmds = [
        ("setchannel", cmd_setchannel),
        ("addsource", cmd_addsource),
        ("delsource", cmd_delsource),
        ("addadmin", cmd_addadmin),
        ("deladmin", cmd_deladmin),
        ("setgroup", cmd_setgroup),
        ("buttons", cmd_buttons),
        ("addpkg", cmd_addpkg),
        ("addbtn", cmd_addbtn),
        ("categories", cmd_categories),
        ("addcat", cmd_addcat),
        ("addkw", cmd_addkw),
        ("delcat", cmd_delcat),
        ("badwords", cmd_badwords),
        ("addbw", cmd_addbw),
        ("delbw", cmd_delbw),
        ("submissionlimit", cmd_submissionlimit),
        ("ban", cmd_ban),
        ("unban", cmd_unban),
        ("logs", cmd_logs),
        ("status", cmd_status),
    ]
    for cmd, handler in cmds:
        app.add_handler(CommandHandler(cmd, handler))

    # Callback handlers
    app.add_handler(CallbackQueryHandler(handle_logs_page, pattern=r"^logs:\d+$"))
    app.add_handler(CallbackQueryHandler(handle_adpkg_callback, pattern=r"^adpkg:"))
