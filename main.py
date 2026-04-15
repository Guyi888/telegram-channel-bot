"""
Telegram Channel Management Bot — Main Entry Point

Starts:
  1. Database initialisation
  2. python-telegram-bot Application (polling or webhook)
  3. Pyrogram-based channel collector (if API credentials configured)
  4. APScheduler daily stats report

Handler registration order matters in python-telegram-bot:
  Handlers are matched in registration order, so more-specific handlers
  (commands, conversation entry-points) must come before catch-all handlers.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from telegram import Update
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
    handle_custom_reason_text,
    handle_admin_edit_content,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── Admin filter (used to route private messages to the correct handler) ──────

class _AdminFilter(filters.MessageFilter):
    """True when the sender is a registered admin."""
    def filter(self, message):
        # Synchronous check is not possible; we use an async workaround below
        return True  # Fallthrough — actual check done inside handler

    async def __call__(self, message):
        return await db.is_admin(message.from_user.id) if message.from_user else False


# ── Daily stats scheduler ─────────────────────────────────────────────────────

async def _send_daily_stats(bot) -> None:
    """Push daily statistics to all super admins."""
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


# ── Application setup ─────────────────────────────────────────────────────────

async def post_init(application: Application) -> None:
    """Seed super-admins into DB after bot starts."""
    for uid in config.SUPER_ADMIN_IDS:
        if not await db.is_admin(uid):
            await db.add_admin(uid, "", level=1, added_by=0)
            logger.info("Seeded super-admin %s", uid)

    # Set bot commands hint
    from telegram import BotCommand
    await application.bot.set_my_commands([
        BotCommand("start",           "开始 / 投稿"),
        BotCommand("status",          "查看机器人状态"),
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


def build_application() -> Application:
    app = (
        ApplicationBuilder()
        .token(config.BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # ── Management commands (must come before catch-all handlers) ─────────────
    register_management_handlers(app)

    # ── Submission conversation (for non-admin private users) ─────────────────
    submission_conv = build_submission_conversation()
    app.add_handler(submission_conv, group=1)

    # ── Admin message forwarding (private chat, any content) ─────────────────
    # We route inside the handler; only admins actually forward anything.
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & ~filters.COMMAND,
            _route_private_message,
        ),
        group=2,
    )

    # ── Reaction callbacks ────────────────────────────────────────────────────
    app.add_handler(
        CallbackQueryHandler(handle_reaction, pattern=r"^react:(like|dislike):\d+$")
    )

    # ── Review callbacks ──────────────────────────────────────────────────────
    app.add_handler(
        CallbackQueryHandler(handle_review_callback, pattern=r"^review:(approve|reject|edit):\d+$")
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


async def _route_private_message(update: Update, context) -> None:
    """Route a private message: admins → forward; others → submission flow."""
    if not update.effective_user:
        return
    if await db.is_admin(update.effective_user.id):
        await handle_admin_message(update, context)
    # Non-admins are handled by the ConversationHandler (group=1)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    if not config.BOT_TOKEN:
        logger.critical("BOT_TOKEN is not set. Exiting.")
        sys.exit(1)

    # Initialise database
    await db.init_db(config.DATABASE_PATH)
    logger.info("Database initialised at %s", config.DATABASE_PATH)

    app = build_application()
    _setup_scheduler(app)

    # Start Pyrogram collector (if credentials are configured)
    collector_task = None
    consumer_task = None
    if config.API_ID and config.API_HASH and config.PHONE_NUMBER:
        try:
            from collector.channel_collector import ChannelCollector, run_queue_consumer
            collector = ChannelCollector(config.BOT_TOKEN)

            async def _start_collector():
                await collector.start()

            collector_task = asyncio.create_task(_start_collector())
            consumer_task = asyncio.create_task(run_queue_consumer(app.bot))
            logger.info("Collector and queue consumer tasks started.")
        except Exception as e:
            logger.error("Failed to start collector: %s", e)
    else:
        logger.info("Pyrogram credentials not set — cross-channel collection disabled.")

    # Start the bot
    if config.WEBHOOK_URL:
        logger.info("Starting in Webhook mode: %s", config.WEBHOOK_URL)
        await app.run_webhook(
            listen="0.0.0.0",
            port=config.WEBHOOK_PORT,
            url_path=config.BOT_TOKEN,
            webhook_url=f"{config.WEBHOOK_URL}/{config.BOT_TOKEN}",
            secret_token=config.WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        logger.info("Starting in Polling mode.")
        await app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )

    # Cleanup
    if collector_task:
        collector_task.cancel()
    if consumer_task:
        consumer_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
