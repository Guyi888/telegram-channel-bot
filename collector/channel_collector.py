"""
Module 3 — Cross-channel content collection via Pyrogram (personal account API).

Architecture:
  Pyrogram client  →  SQLite message queue  →  Bot consumer task

The collector runs as a second asyncio task alongside the bot.  Messages from
monitored source channels are serialised to JSON and pushed into the queue.
A separate consumer task (started in main.py) polls the queue and uses the
Bot API to copy them to the target channel.

Risk notice (written per requirements):
  - This module uses the Telegram personal account (MTProto) API.
  - USE A DEDICATED SECONDARY ACCOUNT — do not risk your main account.
  - Automated data collection must comply with Telegram's Terms of Service.
  - The session is stored locally; protect it like a password.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import List, Optional, Set

import config
from database import db

logger = logging.getLogger(__name__)

# ── Ad / spam filter ──────────────────────────────────────────────────────────

# Regex patterns that strongly indicate advertisement content
_AD_URL_RE = re.compile(
    r'https?://\S+|t\.me/\S+|telegram\.me/\S+',
    re.IGNORECASE,
)
_AD_USERNAME_RE = re.compile(
    r'@[a-zA-Z][a-zA-Z0-9_]{3,}',  # @username with 5+ total chars
)


async def _is_ad_message(message) -> bool:
    """
    Return True if the message should be filtered out as an advertisement.

    Checks (in order):
      1. Filter is disabled in DB config → always pass through
      2. Pyrogram message entities: URL, TextLink, Mention → reject
      3. Regex fallback on text/caption → reject if URL or @username found
      4. Custom keyword list stored in DB config → reject if any match
    """
    # 1. Check if filter is enabled
    enabled = await db.get_config("ad_filter_enabled", "1")
    if enabled != "1":
        return False

    text = message.text or message.caption or ""

    # 2. Check Pyrogram entities (most reliable)
    entities = getattr(message, "entities", None) or \
               getattr(message, "caption_entities", None) or []

    if PYROGRAM_AVAILABLE:
        try:
            from pyrogram.enums import MessageEntityType
            for ent in entities:
                if ent.type in (
                    MessageEntityType.URL,
                    MessageEntityType.TEXT_LINK,
                    MessageEntityType.MENTION,
                ):
                    logger.info("Ad filter: entity %s found in msg %s", ent.type, message.id)
                    return True
        except Exception:
            pass  # fallback to regex below

    # 3. Regex fallback
    if text:
        if _AD_URL_RE.search(text):
            logger.info("Ad filter: URL pattern found in msg %s", message.id)
            return True
        if _AD_USERNAME_RE.search(text):
            logger.info("Ad filter: @username pattern found in msg %s", message.id)
            return True

    # 4. Custom keywords
    kw_raw = await db.get_config("ad_filter_keywords", "")
    if kw_raw and text:
        keywords = [k.strip() for k in kw_raw.split(",") if k.strip()]
        text_lower = text.lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                logger.info("Ad filter: keyword '%s' found in msg %s", kw, message.id)
                return True

    return False

# Pyrogram may not be installed in every deployment — import lazily so the bot
# still starts even when the collector is not configured.
try:
    from pyrogram import Client, filters
    from pyrogram.errors import SessionPasswordNeeded, FloodWait
    from pyrogram import types as ptypes
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    logger.warning("pyrogram not installed — cross-channel collection disabled.")


class ChannelCollector:
    """
    Manages the Pyrogram client and relays messages to the SQLite queue.

    Reconnect logic:
      On disconnect the client tries to reconnect up to RECONNECT_MAX_RETRIES
      times with RECONNECT_DELAY_SECONDS delay between attempts.
      After exhausting retries a super-admin alert is sent via the Bot API.
    """

    def __init__(self, bot_token: str) -> None:
        if not PYROGRAM_AVAILABLE:
            raise RuntimeError("pyrogram is not installed.")

        self._bot_token = bot_token
        self._client: Optional[Client] = None
        self._monitored: Set[int] = set()
        self._running = False
        self._retry_count = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Create Pyrogram client, register handlers BEFORE start(), then connect."""
        if not config.API_ID or not config.API_HASH or not config.PHONE_NUMBER:
            logger.warning("Pyrogram credentials not configured — collector not started.")
            return

        self._client = Client(
            name=config.SESSION_NAME,
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            phone_number=config.PHONE_NUMBER,
        )

        # ── Register handlers BEFORE start() — required by Pyrogram v2 ──────────
        # Use filters.channel to capture channel posts specifically.
        # Also capture edited channel posts so updates are not missed.
        @self._client.on_message(filters.channel)
        async def _on_channel_post(client, message):
            await self._handle_message(message)

        @self._client.on_edited_message(filters.channel)
        async def _on_edited_channel_post(client, message):
            # Treat edits same as new posts so content is not missed
            await self._handle_message(message)

        logger.info("Channel message handlers registered.")
        await self._start_with_retry()

    async def _start_with_retry(self) -> None:
        while self._retry_count <= config.RECONNECT_MAX_RETRIES:
            try:
                await self._client.start()
                self._running = True
                self._retry_count = 0
                logger.info("Pyrogram collector started successfully.")

                # Refresh monitored channel set from DB and auto-join channels
                await self._refresh_sources()

                # Keep running indefinitely
                await asyncio.Event().wait()

            except FloodWait as e:
                logger.warning("FloodWait %ss during collector start", e.value)
                await asyncio.sleep(e.value)
            except Exception as e:
                self._retry_count += 1
                logger.error(
                    "Collector error (attempt %s/%s): %s",
                    self._retry_count, config.RECONNECT_MAX_RETRIES, e
                )
                if self._retry_count > config.RECONNECT_MAX_RETRIES:
                    await self._alert_admins(
                        f"❗ 跨频道采集器连接失败，已重试 {config.RECONNECT_MAX_RETRIES} 次。\n错误：{e}"
                    )
                    break
                await asyncio.sleep(config.RECONNECT_DELAY_SECONDS)

    async def stop(self) -> None:
        self._running = False
        if self._client and self._client.is_connected:
            await self._client.stop()
            logger.info("Pyrogram collector stopped.")

    # ── Source channel management ─────────────────────────────────────────────

    async def _refresh_sources(self) -> None:
        sources = await db.get_source_channels()
        self._monitored = {s["channel_id"] for s in sources}
        logger.info("Monitoring %s source channels.", len(self._monitored))

        # Auto-join: prefer @username over numeric ID (more reliable in Pyrogram)
        for src in sources:
            cid = src["channel_id"]
            username = (src.get("username") or "").strip()
            peer = f"@{username}" if username else cid
            try:
                await self._client.join_chat(peer)
                logger.info("Joined source channel %s (%s)", cid, peer)
            except Exception as e:
                logger.warning("join_chat(%s) skipped: %s", peer, e)

    async def join_channel(self, channel_id: int, username: str = "") -> bool:
        """
        Join a channel so Pyrogram receives its messages.
        Called by management handler after a new source channel is added.
        Prefers @username over numeric ID.
        """
        if not self._client or not self._running:
            logger.warning("Collector not running — cannot join channel %s", channel_id)
            return False
        peer = f"@{username}" if username else channel_id
        try:
            await self._client.join_chat(peer)
            self._monitored.add(channel_id)
            logger.info("Joined and now monitoring channel %s (%s)", channel_id, peer)
            return True
        except Exception as e:
            logger.error("Failed to join channel %s (%s): %s", channel_id, peer, e)
            return False

    async def leave_channel(self, channel_id: int) -> None:
        """Leave a source channel when it is removed from monitoring."""
        self._monitored.discard(channel_id)
        if not self._client or not self._running:
            return
        try:
            await self._client.leave_chat(channel_id)
            logger.info("Left channel %s", channel_id)
        except Exception as e:
            logger.debug("leave_chat(%s) skipped: %s", channel_id, e)

    def refresh_sources_sync(self, channel_ids: List[int]) -> None:
        """Called by management handler when source list changes (sync version)."""
        self._monitored = set(channel_ids)

    # ── Message handler ───────────────────────────────────────────────────────

    async def _handle_message(self, message) -> None:
        """Called for every channel post the personal account receives."""
        chat_id = getattr(message.chat, "id", None)
        logger.info("Received channel message: chat_id=%s monitored=%s",
                    chat_id, chat_id in self._monitored)

        # Only process messages from monitored channels
        if chat_id not in self._monitored:
            return

        # Ad / spam filter
        if await _is_ad_message(message):
            logger.info("Dropped ad message %s from channel %s", message.id, chat_id)
            return

        # Serialise message to a dict for the queue
        data = await self._serialize_message(message)
        await db.enqueue_message(chat_id, data)
        logger.info("Queued message %s from channel %s (type=%s)",
                    message.id, chat_id, data.get("content_type"))

    async def _serialize_message(self, message) -> dict:
        """
        Store only the source channel ID and message ID.

        Pyrogram file_ids are MTProto-layer IDs that are NOT compatible with
        the Bot API.  Instead of trying to pass file_ids across the boundary,
        we store the original message coordinates and use Bot API
        copy_message() in the queue consumer to copy the content.
        """
        return {
            "source_channel_id": message.chat.id,
            "source_message_id": message.id,
            "media_group_id": message.media_group_id,
            "text": message.text or message.caption or "",
            "content_type": "channel_message",  # consumed by copy_message path
        }

    # ── Admin alerts ──────────────────────────────────────────────────────────

    async def _alert_admins(self, text: str) -> None:
        """Send a plain HTTP request to alert super admins via Bot API."""
        import aiohttp
        admins = await db.list_admins()
        super_admins = [a["user_id"] for a in admins if a["level"] == 1]
        super_admins = super_admins or config.SUPER_ADMIN_IDS

        url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as session:
            for uid in super_admins:
                try:
                    await session.post(url, json={"chat_id": uid, "text": text})
                except Exception as e:
                    logger.error("Failed to alert admin %s: %s", uid, e)


# ── Queue consumer ────────────────────────────────────────────────────────────

async def run_queue_consumer(bot) -> None:
    """
    Background task: continuously drain the message queue and publish
    collected messages to the target channel via the Bot API.

    Uses copy_message where the bot has already uploaded the file (works for
    messages the bot forwarded).  For messages collected by Pyrogram (file_id
    from a different DC context), we use send_* methods with the file_id.

    Flood-wait handling: catches RetryAfter and sleeps accordingly.
    """
    from telegram.error import RetryAfter, TelegramError
    from services.publisher import (
        publish_text, publish_photo, publish_video, publish_document
    )

    logger.info("Queue consumer started.")
    # MediaGroup aggregation buffer: {media_group_id: [item, ...]}
    group_buffer: dict = {}
    group_timer: dict = {}

    while True:
        try:
            items = await db.dequeue_messages(limit=config.QUEUE_BATCH_SIZE)
            target = await db.get_target_channel()

            for item in items:
                if not target:
                    await db.mark_queue_processed(item["id"])
                    continue

                data = item["message_data"]
                gid = data.get("media_group_id")
                ctype = data.get("content_type", "text")

                if gid:
                    # Buffer for album aggregation
                    if gid not in group_buffer:
                        group_buffer[gid] = []
                    group_buffer[gid].append((item["id"], data))
                    group_timer[gid] = asyncio.get_event_loop().time()
                else:
                    await _publish_queue_item(bot, target, item["id"], data,
                                               publish_text, publish_photo,
                                               publish_video, publish_document)

            # Flush aged media groups (older than MEDIA_GROUP_DELAY)
            now = asyncio.get_event_loop().time()
            for gid in list(group_buffer.keys()):
                if now - group_timer.get(gid, now) >= config.MEDIA_GROUP_DELAY:
                    await _flush_group(bot, target, gid, group_buffer.pop(gid, []))
                    group_timer.pop(gid, None)

        except Exception as e:
            logger.error("Queue consumer error: %s", e)

        await asyncio.sleep(config.QUEUE_POLL_INTERVAL)


async def _copy_and_react(bot, target: str, source_channel_id: int,
                          source_message_id: int) -> bool:
    """
    Copy a single message from a public source channel to the target channel
    using Bot API copy_message, then attach reaction buttons.
    Returns True on success.
    """
    from telegram.error import RetryAfter, TelegramError
    from services.publisher import build_reply_markup
    try:
        msg = await bot.copy_message(
            chat_id=target,
            from_chat_id=source_channel_id,
            message_id=source_message_id,
        )
        logger.info("Copied message %s from %s → %s (new id=%s)",
                    source_message_id, source_channel_id, target, msg.message_id)
        # Attach reaction / ad buttons
        try:
            markup = await build_reply_markup(msg.message_id)
            await bot.edit_message_reply_markup(
                chat_id=target,
                message_id=msg.message_id,
                reply_markup=markup,
            )
        except Exception as e:
            logger.debug("Could not add reaction buttons to %s: %s", msg.message_id, e)
        return True
    except RetryAfter as e:
        logger.warning("RetryAfter %ss copying msg %s", e.retry_after, source_message_id)
        await asyncio.sleep(e.retry_after + 1)
        return False
    except TelegramError as e:
        logger.error("copy_message failed (src=%s msg=%s): %s",
                     source_channel_id, source_message_id, e)
        return False


async def _publish_queue_item(bot, target, queue_id, data,
                               publish_text, publish_photo,
                               publish_video, publish_document) -> None:
    from telegram.error import RetryAfter
    ctype = data.get("content_type", "text")

    try:
        if ctype == "channel_message":
            # New path: use Bot API copy_message (avoids Pyrogram↔BotAPI file_id mismatch)
            await _copy_and_react(
                bot, target,
                data["source_channel_id"],
                data["source_message_id"],
            )
        elif ctype == "text":
            text = data.get("text", "")
            if text:
                await publish_text(bot, target, text)
        elif ctype == "photo":
            await publish_photo(bot, target, data["file_id"], caption=data.get("text", ""))
        elif ctype == "video":
            await publish_video(bot, target, data["file_id"], caption=data.get("text", ""))
        elif ctype == "document":
            await publish_document(bot, target, data["file_id"], caption=data.get("text", ""))
        else:
            text = data.get("text", "")
            if text:
                await publish_text(bot, target, text)
        await db.mark_queue_processed(queue_id)
    except RetryAfter as e:
        logger.warning("RetryAfter %ss for queue item %s", e.retry_after, queue_id)
        await asyncio.sleep(e.retry_after + 1)
    except Exception as e:
        logger.error("Failed to publish queue item %s: %s", queue_id, e)
        await db.mark_queue_processed(queue_id)  # Don't block the queue


async def _flush_group(bot, target, gid, items) -> None:
    """
    Publish a buffered MediaGroup.

    For channel_message items we use copy_message for each message individually.
    Telegram will group them visually in the target channel if they are sent
    in rapid succession and share the same media_group_id (when using forwardMessage);
    with copy_message each arrives as a separate message, which is acceptable.
    """
    if not items or not target:
        return

    for queue_id, data in items:
        if data.get("content_type") == "channel_message":
            await _copy_and_react(
                bot, target,
                data["source_channel_id"],
                data["source_message_id"],
            )
        else:
            # Legacy path for old-format queue items with file_id
            from services.publisher import publish_album
            file_id = data.get("file_id")
            if file_id:
                try:
                    ctype = data.get("content_type", "photo")
                    await publish_album(bot, target,
                                        [{"type": ctype, "file_id": file_id}],
                                        caption=data.get("text", ""))
                except Exception as e:
                    logger.error("Failed to flush legacy item in group %s: %s", gid, e)
        await db.mark_queue_processed(queue_id)
