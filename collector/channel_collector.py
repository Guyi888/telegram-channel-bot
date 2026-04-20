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
from typing import Dict, List, Optional, Set

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
        # last processed message_id per channel — used for polling dedup
        self._last_msg_ids: Dict[int, int] = {}
        self._poll_task: Optional[asyncio.Task] = None

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
        # Use filters.channel | filters.group to capture both broadcast channels
        # and megagroup/supergroup-type channels (some "channels" in Telegram are
        # technically supergroups where only admins can post).
        _ch_filter = filters.channel | filters.group

        @self._client.on_message(_ch_filter)
        async def _on_channel_post(client, message):
            await self._handle_message(message)

        @self._client.on_edited_message(_ch_filter)
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

                # Start backup polling loop as a concurrent task
                self._poll_task = asyncio.get_running_loop().create_task(
                    self._poll_channels()
                )

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
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
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

        # Initialise _last_msg_ids.
        # Fetch the last 11 messages; use the 11th as the baseline so the
        # FIRST poll immediately picks up the 10 most recent messages already
        # in each channel.  This lets admins see results right away without
        # waiting for the source channels to post brand-new content.
        # If the channel has fewer than 11 messages, start from 0 (collect all).
        synced = 0
        for src in sources:
            cid = src["channel_id"]
            username = (src.get("username") or "").strip()
            peer = f"@{username}" if username else cid
            try:
                history: list = []
                async for msg in self._client.get_chat_history(peer, limit=11):
                    history.append(msg)
                if history:
                    # Set baseline to the 11th message (index 10); if fewer exist use 0
                    baseline = history[10].id if len(history) >= 11 else 0
                    self._last_msg_ids[cid] = baseline
                    logger.info(
                        "init channel %s (%s): latest_id=%s baseline=%s (will collect last %d msgs)",
                        cid, peer, history[0].id, baseline, len(history) - (1 if len(history) >= 11 else 0)
                    )
                synced += 1
            except Exception as e:
                logger.warning("init failed for %s: %s", peer, e)
        logger.info("Channel init completed for %d/%d source channels.", synced, len(sources))

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
            # Init last_msg_id: baseline at 11th-from-top so first poll collects last 10 msgs
            try:
                history: list = []
                async for msg in self._client.get_chat_history(peer, limit=11):
                    history.append(msg)
                if history:
                    baseline = history[10].id if len(history) >= 11 else 0
                    self._last_msg_ids[channel_id] = baseline
                    logger.info("init newly joined channel %s: baseline=%s", channel_id, baseline)
            except Exception as sync_e:
                logger.warning("init failed for newly joined %s: %s", channel_id, sync_e)
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
        msg_id = getattr(message, "id", 0)
        logger.info("Received channel message: chat_id=%s msg_id=%s monitored=%s",
                    chat_id, msg_id, chat_id in self._monitored)

        # Only process messages from monitored channels
        if chat_id not in self._monitored:
            return

        # Deduplication: skip if already handled by polling or a previous call
        last = self._last_msg_ids.get(chat_id, 0)
        if msg_id and msg_id <= last:
            logger.debug("Skipping already-processed msg %s from %s", msg_id, chat_id)
            return

        # Ad / spam filter
        if await _is_ad_message(message):
            logger.info("Dropped ad message %s from channel %s", msg_id, chat_id)
            if msg_id:
                self._last_msg_ids[chat_id] = max(last, msg_id)
            return

        # Serialise message to a dict for the queue
        data = await self._serialize_message(message)
        await db.enqueue_message(chat_id, data)
        logger.info("Queued message %s from channel %s (type=%s)",
                    msg_id, chat_id, data.get("content_type"))

        # Update last seen ID so polling won't re-process this
        if msg_id:
            self._last_msg_ids[chat_id] = max(last, msg_id)

    async def _poll_channels(self) -> None:
        """
        Backup polling loop — runs every 60 s and fetches the latest messages
        from every monitored channel.

        Why this exists:
          Pyrogram's real-time update handler only fires for channels whose pts
          is already tracked in the session (i.e. channels the account joined
          BEFORE the current session was created).  Newly joined channels often
          don't deliver real-time updates until the session is restarted because
          Telegram requires a GetChannelDifference handshake that Pyrogram
          doesn't perform automatically for freshly joined channels.

          This loop bridges that gap: even if real-time updates never arrive,
          new messages are caught within POLL_INTERVAL seconds.

        Deduplication:
          _last_msg_ids[channel_id] stores the highest message_id already
          processed (by this loop OR by the real-time handler).  We only
          enqueue messages with id > that value.
        """
        POLL_INTERVAL = getattr(config, "COLLECTOR_POLL_INTERVAL", 60)
        logger.info("Backup polling loop started (interval=%ss).", POLL_INTERVAL)
        poll_count = 0

        while self._running:
            await asyncio.sleep(POLL_INTERVAL)
            if not self._running or not self._client:
                break

            poll_count += 1
            logger.info("Poll #%d: checking %d source channels...", poll_count, len(self._monitored))

            for channel_id in list(self._monitored):
                try:
                    new_msgs: List = []
                    last_id = self._last_msg_ids.get(channel_id, 0)
                    channel_latest_id = None

                    async for msg in self._client.get_chat_history(channel_id, limit=20):
                        if channel_latest_id is None:
                            channel_latest_id = msg.id
                        if msg.id <= last_id:
                            break  # already processed; messages are newest-first
                        new_msgs.append(msg)

                    logger.info("Poll channel %s: stored_last=%s channel_latest=%s new_count=%d",
                                channel_id, last_id, channel_latest_id, len(new_msgs))

                    if not new_msgs:
                        continue

                    # Process in chronological order (oldest → newest)
                    new_msgs.reverse()
                    for msg in new_msgs:
                        if await _is_ad_message(msg):
                            logger.info("Poll: dropped ad msg %s from %s", msg.id, channel_id)
                            continue
                        data = await self._serialize_message(msg)
                        await db.enqueue_message(channel_id, data)
                        logger.info("Poll: queued msg %s from channel %s (type=%s)",
                                    msg.id, channel_id, data.get("content_type"))

                    # Advance the high-water mark
                    max_id = max(m.id for m in new_msgs)
                    self._last_msg_ids[channel_id] = max(
                        self._last_msg_ids.get(channel_id, 0), max_id
                    )

                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.warning("Poll error for channel %s: %s", channel_id, e)

        logger.info("Backup polling loop stopped.")

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
                    await session.post(url, json={"chat_id": uid, "text": text, "parse_mode": "HTML"})
                except Exception as e:
                    logger.error("Failed to alert admin %s: %s", uid, e)


async def _notify_channel_error(bot, html_text: str) -> None:
    """Send a channel/collection error alert to all super admins."""
    try:
        admins = await db.list_admins()
        super_ids = [a["user_id"] for a in admins if a["level"] == 1] or config.SUPER_ADMIN_IDS
        for uid in super_ids:
            try:
                await bot.send_message(chat_id=uid, text=html_text, parse_mode="HTML")
            except Exception as e:
                logger.debug("Failed to send error alert to %s: %s", uid, e)
    except Exception as e:
        logger.error("_notify_channel_error failed: %s", e)


# ── Queue consumer ────────────────────────────────────────────────────────────

async def run_queue_consumer(bot, collector=None) -> None:
    """
    Background task: drain the message queue and publish collected messages
    to the target channel.

    Album handling:
      Messages sharing the same media_group_id are buffered together.
      The timer for each group is set ONCE (on first encounter) and never
      reset — this prevents the "never-flushed" bug where re-fetching the
      same unprocessed rows would continuously reset the timer.

      Once the group timer expires (>= MEDIA_GROUP_DELAY seconds) the whole
      group is flushed.  Pyrogram is tried first to send a proper media-group
      so items appear as a single album in the target channel; Bot API
      individual copy_message is the fallback.

    Deduplication:
      buffered_ids tracks queue IDs already loaded into the buffer so the
      same row is never double-added when the consumer re-fetches it on the
      next poll cycle.
    """
    from services.publisher import (
        publish_text, publish_photo, publish_video, publish_document
    )

    logger.info("Queue consumer started.")

    # {gid: [(queue_id, data), ...]}
    group_buffer: dict = {}
    # {gid: first_seen_monotonic_time}  — set ONCE, never updated
    group_timer: dict = {}
    # queue IDs already loaded into group_buffer (prevents double-buffering)
    buffered_ids: set = set()

    while True:
        try:
            pyrogram_client = None
            if collector is not None:
                pyrogram_client = getattr(collector, "_client", None)

            target = await db.get_target_channel()
            items = await db.dequeue_messages(limit=config.QUEUE_BATCH_SIZE)

            for item in items:
                if not target:
                    await db.mark_queue_processed(item["id"])
                    continue

                data = item["message_data"]
                gid = data.get("media_group_id")

                if gid:
                    if item["id"] not in buffered_ids:
                        if gid not in group_buffer:
                            group_buffer[gid] = []
                            # Set timer ONCE — never reset so it naturally expires
                            group_timer[gid] = asyncio.get_event_loop().time()
                        group_buffer[gid].append((item["id"], data))
                        buffered_ids.add(item["id"])
                else:
                    await _publish_queue_item(
                        bot, target, item["id"], data,
                        publish_text, publish_photo, publish_video, publish_document,
                        pyrogram_client=pyrogram_client,
                    )

            # Flush groups whose timer has expired
            if target:
                now = asyncio.get_event_loop().time()
                for gid in list(group_buffer.keys()):
                    if now - group_timer.get(gid, 0) >= config.MEDIA_GROUP_DELAY:
                        group_items = group_buffer.pop(gid, [])
                        group_timer.pop(gid, None)
                        for qid, _ in group_items:
                            buffered_ids.discard(qid)
                        await _flush_group(
                            bot, target, gid, group_items,
                            pyrogram_client=pyrogram_client,
                        )

        except Exception as e:
            logger.error("Queue consumer error: %s", e)

        await asyncio.sleep(config.QUEUE_POLL_INTERVAL)


async def _copy_and_react(bot, target: str, source_channel_id: int,
                          source_message_id: int, pyrogram_client=None) -> bool:
    """
    Copy a single message from a source channel to the target channel, then
    attach reaction buttons.  Returns True on success.

    Copy strategy:
      1. Pyrogram client (if available and connected) — the personal account is
         a member of the source channel so copy_message always works.
      2. Bot API copy_message — fallback (requires bot to be in source channel).
    """
    from telegram.error import RetryAfter, TelegramError
    from services.publisher import build_reply_markup

    sent_msg_id: int | None = None

    # ── Strategy 1: Pyrogram copy ─────────────────────────────────────────────
    if pyrogram_client is not None:
        try:
            is_connected = getattr(pyrogram_client, "is_connected", False)
            if callable(is_connected):
                is_connected = is_connected()
        except Exception:
            is_connected = False

        if is_connected:
            try:
                result = await pyrogram_client.copy_message(
                    chat_id=target,
                    from_chat_id=source_channel_id,
                    message_id=source_message_id,
                )
                sent_msg_id = result.id
                logger.info(
                    "Pyrogram-copied msg %s from %s → %s (new id=%s)",
                    source_message_id, source_channel_id, target, sent_msg_id,
                )
            except Exception as e:
                logger.warning(
                    "Pyrogram copy failed (src=%s msg=%s), trying Bot API: %s",
                    source_channel_id, source_message_id, e,
                )

    # ── Strategy 2: Bot API copy_message (fallback) ───────────────────────────
    if sent_msg_id is None:
        try:
            msg = await bot.copy_message(
                chat_id=target,
                from_chat_id=source_channel_id,
                message_id=source_message_id,
            )
            sent_msg_id = msg.message_id
            logger.info(
                "Bot API-copied msg %s from %s → %s (new id=%s)",
                source_message_id, source_channel_id, target, sent_msg_id,
            )
        except RetryAfter as e:
            logger.warning("RetryAfter %ss copying msg %s", e.retry_after, source_message_id)
            await asyncio.sleep(e.retry_after + 1)
            return False
        except TelegramError as e:
            logger.error(
                "copy_message failed (src=%s msg=%s): %s",
                source_channel_id, source_message_id, e,
            )
            # Notify admins about persistent channel failure
            await _notify_channel_error(
                bot,
                f"❗ 来源频道 <code>{source_channel_id}</code> 消息转发失败\n"
                f"消息 ID：{source_message_id}\n"
                f"错误：{e}\n\n"
                f"可能原因：Bot 未加入该频道或消息已被删除。\n"
                f"建议：删除该频道后用 @用户名 重新添加。"
            )
            return False

    # ── Attach reaction / ad buttons ─────────────────────────────────────────
    if sent_msg_id is not None:
        try:
            markup = await build_reply_markup(sent_msg_id)
            await bot.edit_message_reply_markup(
                chat_id=target,
                message_id=sent_msg_id,
                reply_markup=markup,
            )
        except Exception as e:
            logger.debug("Could not add reaction buttons to %s: %s", sent_msg_id, e)

    return sent_msg_id is not None


async def _publish_queue_item(bot, target, queue_id, data,
                               publish_text, publish_photo,
                               publish_video, publish_document,
                               pyrogram_client=None) -> None:
    from telegram.error import RetryAfter
    ctype = data.get("content_type", "text")

    try:
        if ctype == "channel_message":
            # Use Pyrogram first (personal account is member of source channel),
            # fall back to Bot API copy_message.
            await _copy_and_react(
                bot, target,
                data["source_channel_id"],
                data["source_message_id"],
                pyrogram_client=pyrogram_client,
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


async def _flush_group(bot, target, gid, items, pyrogram_client=None) -> None:
    """
    Publish a buffered media group to the target channel.

    Strategy (in order):
      1. Pyrogram send_media_group — produces a proper grouped album in the
         target channel.  Requires the personal account to be an admin of the
         target channel.  Falls back to strategy 2 on any failure.
      2. Bot API copy_message per item — items arrive as separate messages;
         each gets its own reaction button row via _copy_and_react.
    """
    if not items or not target:
        return

    all_channel_msgs = all(
        d.get("content_type") == "channel_message" for _, d in items
    )

    # ── Strategy 1: Pyrogram send_media_group ────────────────────────────────
    if all_channel_msgs and pyrogram_client and PYROGRAM_AVAILABLE:
        sent_ok = await _try_pyrogram_album(pyrogram_client, bot, target, gid, items)
        if sent_ok:
            for queue_id, _ in items:
                await db.mark_queue_processed(queue_id)
            return

    # ── Strategy 2: individual Bot API copy_message ───────────────────────────
    for queue_id, data in items:
        if data.get("content_type") == "channel_message":
            await _copy_and_react(
                bot, target,
                data["source_channel_id"],
                data["source_message_id"],
                pyrogram_client=pyrogram_client,
            )
        else:
            from services.publisher import publish_album
            file_id = data.get("file_id")
            if file_id:
                try:
                    ctype = data.get("content_type", "photo")
                    await publish_album(bot, target,
                                        [{"type": ctype, "file_id": file_id}],
                                        caption=data.get("text", ""))
                except Exception as e:
                    logger.error("Legacy album item failed (group %s): %s", gid, e)
        await db.mark_queue_processed(queue_id)


async def _try_pyrogram_album(pyrogram_client, bot, target: str,
                               gid, items) -> bool:
    """
    Try to send a collected album as a grouped message using the Pyrogram
    personal-account client.

    Steps:
      1. Retrieve the original messages from the source channel.
      2. Build Pyrogram InputMedia* objects from their file references.
      3. Call client.send_media_group(target, media=[...]).
      4. Attach reaction buttons via a Bot API follow-up message.

    Returns True on success, False on any failure (caller falls back).

    NOTE: The personal account must be an admin of the target channel for
    step 3 to succeed.  If it is not, Pyrogram will raise ChatWriteForbidden
    and we fall back gracefully.
    """
    try:
        is_connected = getattr(pyrogram_client, "is_connected", False)
        if callable(is_connected):
            is_connected = is_connected()
        if not is_connected:
            return False

        from pyrogram.types import (
            InputMediaPhoto, InputMediaVideo,
            InputMediaDocument, InputMediaAudio,
        )

        source_channel_id = items[0][1]["source_channel_id"]
        message_ids = [d["source_message_id"] for _, d in items]
        first_text = next(
            (d["text"] for _, d in items if d.get("text")), ""
        )

        # Fetch the original messages from the source channel
        source_msgs = await pyrogram_client.get_messages(
            source_channel_id, message_ids
        )
        if not source_msgs:
            return False

        # get_messages may return a single Message instead of a list
        if not isinstance(source_msgs, (list, tuple)):
            source_msgs = [source_msgs]
        source_msgs = [m for m in source_msgs if m and getattr(m, "id", None)]
        source_msgs.sort(key=lambda m: m.id)

        media_list = []
        for i, msg in enumerate(source_msgs):
            cap = first_text if i == 0 and first_text else None
            if msg.photo:
                media_list.append(InputMediaPhoto(msg.photo.file_id, caption=cap))
            elif msg.video:
                media_list.append(InputMediaVideo(msg.video.file_id, caption=cap))
            elif msg.document:
                media_list.append(InputMediaDocument(msg.document.file_id, caption=cap))
            elif msg.audio:
                media_list.append(InputMediaAudio(msg.audio.file_id, caption=cap))

        if not media_list:
            return False

        # Resolve target for Pyrogram (accepts int or @username string)
        t = str(target).strip()
        target_peer = int(t) if t.lstrip("-").isdigit() else t

        sent = await pyrogram_client.send_media_group(target_peer, media=media_list)
        if not sent:
            return False

        if not isinstance(sent, (list, tuple)):
            sent = [sent]

        first_id = sent[0].id
        from services.publisher import build_reply_markup
        markup = await build_reply_markup(first_id)
        try:
            await bot.send_message(
                chat_id=target,
                text="\u2800",   # Braille blank — invisible follow-up for buttons
                reply_markup=markup,
            )
        except Exception as e:
            logger.debug("Album reaction follow-up failed (group %s): %s", gid, e)

        logger.info("Pyrogram album group %s → %s (%d items)", gid, target, len(sent))
        return True

    except Exception as e:
        logger.warning("_try_pyrogram_album failed (group %s): %s", gid, e)
        return False
