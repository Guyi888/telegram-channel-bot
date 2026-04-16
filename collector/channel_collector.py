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
from typing import List, Optional, Set

import config
from database import db

logger = logging.getLogger(__name__)

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

        # Auto-join all monitored channels so Pyrogram receives their updates
        for src in sources:
            cid = src["channel_id"]
            try:
                await self._client.join_chat(cid)
                logger.info("Joined source channel %s", cid)
            except Exception as e:
                # Already a member, or cannot join (private, etc.) — non-fatal
                logger.warning("join_chat(%s) skipped: %s", cid, e)

    async def join_channel(self, channel_id: int) -> bool:
        """
        Join a channel so Pyrogram receives its messages.
        Called by management handler after a new source channel is added.
        Returns True on success.
        """
        if not self._client or not self._running:
            logger.warning("Collector not running — cannot join channel %s", channel_id)
            return False
        try:
            await self._client.join_chat(channel_id)
            self._monitored.add(channel_id)
            logger.info("Joined and now monitoring channel %s", channel_id)
            return True
        except Exception as e:
            logger.error("Failed to join channel %s: %s", channel_id, e)
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

        # Serialise message to a dict for the queue
        data = await self._serialize_message(message)
        await db.enqueue_message(chat_id, data)
        logger.info("Queued message %s from channel %s (type=%s)",
                    message.id, chat_id, data.get("content_type"))

    async def _serialize_message(self, message) -> dict:
        """Convert a Pyrogram Message to a JSON-serialisable dict."""
        data: dict = {
            "message_id": message.id,
            "source_channel_id": message.chat.id,
            "media_group_id": message.media_group_id,
            "text": message.text or message.caption or "",
            "content_type": "text",
        }

        if message.photo:
            data["content_type"] = "photo"
            data["file_id"] = message.photo.file_id
        elif message.video:
            data["content_type"] = "video"
            data["file_id"] = message.video.file_id
        elif message.document:
            data["content_type"] = "document"
            data["file_id"] = message.document.file_id
        elif message.audio:
            data["content_type"] = "audio"
            data["file_id"] = message.audio.file_id
        elif message.animation:
            data["content_type"] = "animation"
            data["file_id"] = message.animation.file_id

        return data

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


async def _publish_queue_item(bot, target, queue_id, data,
                               publish_text, publish_photo,
                               publish_video, publish_document) -> None:
    from telegram.error import RetryAfter
    ctype = data.get("content_type", "text")
    text = data.get("text", "")

    try:
        if ctype == "text":
            await publish_text(bot, target, text)
        elif ctype == "photo":
            await publish_photo(bot, target, data["file_id"], caption=text)
        elif ctype == "video":
            await publish_video(bot, target, data["file_id"], caption=text)
        elif ctype == "document":
            await publish_document(bot, target, data["file_id"], caption=text)
        else:
            # Fallback: forward as text if we don't know how to handle
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
    """Publish a buffered MediaGroup album."""
    from services.publisher import publish_album
    if not items or not target:
        return

    media_list = []
    caption = ""
    for i, (queue_id, data) in enumerate(items):
        if i == 0:
            caption = data.get("text", "")
        ctype = data.get("content_type", "photo")
        file_id = data.get("file_id")
        if file_id:
            media_list.append({"type": ctype, "file_id": file_id})

    if media_list:
        try:
            await publish_album(bot, target, media_list, caption=caption)
        except Exception as e:
            logger.error("Failed to flush media group %s: %s", gid, e)

    for queue_id, _ in items:
        await db.mark_queue_processed(queue_id)
