"""
Telegram Channel Management Bot — Main Entry Point

python-telegram-bot v20 manages its own event loop via run_polling() /
run_webhook().  All async initialisation (DB, collector tasks) must be
done inside post_init / post_shutdown callbacks, NOT in a separate
asyncio.run() wrapper.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from telegram import BotCommand, Update
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler,
    CommandHandler, MessageHandler, filters,
)

import config
from database import db
from bot.handlers.admin_forward import handle_admin_message
from bot.handlers.submissions import build_submission_conversation
from bot.handlers.management import register_management_handlers
from bot.handlers.reactions import handle_reaction
from bot.handlers.callbacks import (
    handle_review_callback,
    handle_category_approve,
    handle_reject_reason,
    handle_custom_reason_start,
    build_admin_edit_conversation,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── post_init: runs inside PTB's event loop ───────────────────────────────────

async def post_init(application: Application) -> None:
    """Called by PTB after the Application is ready but before polling starts."""

    # 1. Initialise database
    await db.init_db(config.DATABASE_PATH)
    logger.info("Database initialised at %s", config.DATABASE_PATH)

    # 2. Seed super-admins
    for uid in config.SUPER_ADMIN_IDS:
        if not await db.is_admin(uid):
            await db.add_admin(uid, "", level=1, added_by=0)
            logger.info("Seeded super-admin %s", uid)

    # 3. Set bot command menu
    await application.bot.set_my_commands([
        BotCommand("start",           "开始 / 投稿"),
        BotCommand("panel",           "管理员操作面板"),
        BotCommand("status",          "查看机器人状态"),
        BotCommand("addtarget",       "添加目标频道（超级管理员）"),
        BotCommand("deltarget",       "删除目标频道（超级管理员）"),
        BotCommand("setchannel",      "设置目标频道（超级管理员）"),
        BotCommand("addsource",       "添加来源频道（超级管理员）"),
        BotCommand("delsource",       "删除来源频道（超级管理员）"),
        BotCommand("addadmin",        "添加管理员（超级管理员）"),
        BotCommand("deladmin",        "删除管理员（超级管理员）"),
        BotCommand("setgroup",        "设置讨论群组（超级管理员）"),
        BotCommand("buttons",         "广告套餐管理（超级管理员）"),
        BotCommand("addpkg",          "新建广告套餐（超级管理员）"),
        BotCommand("addbtn",          "添加广告按钮（超级管理员）"),
        BotCommand("categories",      "分类管理（超级管理员）"),
        BotCommand("addcat",          "添加分类（超级管理员）"),
        BotCommand("badwords",        "违禁词管理（超级管理员）"),
        BotCommand("addbw",           "添加违禁词（超级管理员）"),
        BotCommand("submissionlimit", "设置每日投稿上限（超级管理员）"),
        BotCommand("ban",             "封禁用户投稿"),
        BotCommand("unban",           "解封用户投稿"),
        BotCommand("logs",            "查看操作日志（超级管理员）"),
        BotCommand("cancel",          "取消当前操作"),
    ])

    # 4. Start Pyrogram collector + queue consumer (background tasks)
    if config.API_ID and config.API_HASH and config.PHONE_NUMBER:
        try:
            from collector.channel_collector import ChannelCollector, run_queue_consumer
            collector = ChannelCollector(application.bot.token)
            # Store reference so management handlers can call refresh_sources_sync()
            application.bot_data["collector"] = collector
            asyncio.create_task(collector.start())
            asyncio.create_task(run_queue_consumer(application.bot))
            logger.info("Collector and queue consumer tasks started.")
        except Exception as e:
            logger.error("Failed to start collector: %s", e)
    else:
        logger.info("Pyrogram credentials not set — cross-channel collection disabled.")


# ── Daily stats scheduler ─────────────────────────────────────────────────────

async def _send_daily_stats(bot) -> None:
    stats = await db.get_daily_stats()
    text = (
        f"📊 <b>每日运营统计（{stats['date']}）</b>\n\n"
        f"📥 今日投稿：{stats['submissions']} 条\n"
        f"✅ 审核通过：{stats['approved']} 条\n"
        f"❌ 审核拒绝：{stats['rejected']} 条\n"
        f"👍 点赞数：{stats['likes']}\n"
        f"👎 点踩数：{stats['dislikes']}"
    )
    admins = await db.list_admins()
    super_ids = [a["user_id"] for a in admins if a["level"] == 1] or config.SUPER_ADMIN_IDS
    for uid in super_ids:
        try:
            await bot.send_message(chat_id=uid, text=text, parse_mode="HTML")
        except Exception as e:
            logger.warning("Failed to send daily stats to %s: %s", uid, e)


def _setup_scheduler(app: Application) -> None:
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        scheduler.add_job(
            _send_daily_stats,
            trigger="cron",
            hour=23,
            minute=55,
            args=[app.bot],
        )
        scheduler.start()
        logger.info("Daily stats scheduler started (fires at 23:55 CST).")
    except ImportError:
        logger.warning("apscheduler not installed — daily stats disabled.")


# ── Application builder ───────────────────────────────────────────────────────

async def _route_private_message(update: Update, context) -> None:
    """Route private messages: admins → forward; others → submission flow."""
    if not update.effective_user:
        return
    if await db.is_admin(update.effective_user.id):
        await handle_admin_message(update, context)


def build_application() -> Application:
    app = (
        ApplicationBuilder()
        .token(config.BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Management commands (highest priority)
    register_management_handlers(app)

    # Submission conversation for non-admin users
    app.add_handler(build_submission_conversation(), group=1)

    # Route private messages: admin → forward, non-admin → submission conv
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & ~filters.COMMAND,
            _route_private_message,
        ),
        group=2,
    )

    # Reaction callbacks
    app.add_handler(
        CallbackQueryHandler(handle_reaction, pattern=r"^react:(like|dislike):\d+$")
    )

    # Admin edit conversation (must be registered before plain review callbacks
    # so it intercepts review:edit:* before the catch-all handler sees it)
    app.add_handler(build_admin_edit_conversation())

    # Review callbacks (approve / reject only — edit is handled by the conversation above)
    app.add_handler(
        CallbackQueryHandler(handle_review_callback, pattern=r"^review:(approve|reject):\d+$")
    )
    app.add_handler(
        CallbackQueryHandler(handle_category_approve, pattern=r"^cat_approve:\d+:.+$")
    )
    app.add_handler(
        CallbackQueryHandler(handle_reject_reason, pattern=r"^review_reason:\d+:.+$")
    )
    app.add_handler(
        CallbackQueryHandler(handle_custom_reason_start, pattern=r"^review_custom_reason:\d+$")
    )

    return app


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    if not config.BOT_TOKEN:
        logger.critical("BOT_TOKEN is not set. Exiting.")
        sys.exit(1)

    import os
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    app = build_application()
    _setup_scheduler(app)

    if config.WEBHOOK_URL:
        logger.info("Starting in Webhook mode: %s", config.WEBHOOK_URL)
        app.run_webhook(
            listen="0.0.0.0",
            port=config.WEBHOOK_PORT,
            url_path=config.BOT_TOKEN,
            webhook_url=f"{config.WEBHOOK_URL}/{config.BOT_TOKEN}",
            secret_token=config.WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        logger.info("Starting in Polling mode.")
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )


if __name__ == "__main__":
    main()
