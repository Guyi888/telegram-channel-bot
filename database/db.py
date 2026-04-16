"""
Database layer — async SQLite via aiosqlite.

All public coroutines open their own connection so callers don't need to
manage connection lifetimes.  For high-throughput writes (reactions) a
short PRAGMA optimisation is applied.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

_DB_PATH: str = ""


# ============================================================
# Initialisation
# ============================================================

async def init_db(db_path: str) -> None:
    """Create all tables and seed super-admins from config."""
    global _DB_PATH
    _DB_PATH = db_path

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")

        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id   INTEGER PRIMARY KEY,
                username  TEXT,
                level     INTEGER NOT NULL,   -- 1=super, 2=normal
                added_by  INTEGER,
                added_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS channels (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                type         TEXT    NOT NULL,  -- 'target' | 'source'
                channel_id   INTEGER NOT NULL,
                channel_name TEXT,
                added_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(type, channel_id)
            );

            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS submissions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                username     TEXT,
                sign_type    TEXT    NOT NULL,  -- anonymous|username|custom
                custom_name  TEXT,
                content_type TEXT    NOT NULL,
                message_data TEXT    NOT NULL,  -- JSON
                status       TEXT    DEFAULT 'pending',  -- pending|approved|rejected
                reject_reason TEXT,
                submitted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reviewed_by   INTEGER,
                reviewed_at   TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS submission_times (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_cooldowns (
                user_id        INTEGER PRIMARY KEY,
                cooldown_until TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reactions (
                message_id INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                type       TEXT    NOT NULL,  -- like|dislike
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS categories (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL UNIQUE,
                keywords   TEXT    DEFAULT '[]',  -- JSON [{word,weight},...]
                is_default INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS bad_words (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                word        TEXT    NOT NULL UNIQUE,
                fuzzy_match INTEGER DEFAULT 0,
                added_by    INTEGER,
                added_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS ad_packages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL,
                start_time TEXT,    -- HH:MM  (NULL → all-day)
                end_time   TEXT,    -- HH:MM
                is_default INTEGER DEFAULT 0,
                is_active  INTEGER DEFAULT 1,
                priority   INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS ad_buttons (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                package_id INTEGER NOT NULL REFERENCES ad_packages(id) ON DELETE CASCADE,
                label      TEXT    NOT NULL,
                url        TEXT    NOT NULL,
                row_index  INTEGER DEFAULT 0,
                col_index  INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS admin_logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id   INTEGER NOT NULL,
                action     TEXT    NOT NULL,
                detail     TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS blacklist (
                user_id   INTEGER PRIMARY KEY,
                banned_by INTEGER,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS message_map (
                source_message_id  INTEGER NOT NULL,
                source_channel_id  INTEGER NOT NULL,
                target_message_id  INTEGER,
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_message_id, source_channel_id)
            );

            CREATE TABLE IF NOT EXISTS message_queue (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                source_channel_id INTEGER NOT NULL,
                message_data     TEXT    NOT NULL,  -- JSON
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed        INTEGER DEFAULT 0
            );

            -- Indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_submission_times_user
                ON submission_times(user_id, submitted_at);
            CREATE INDEX IF NOT EXISTS idx_reactions_msg
                ON reactions(message_id);
            CREATE INDEX IF NOT EXISTS idx_queue_unprocessed
                ON message_queue(processed, id);
        """)
        await conn.commit()


def _db() -> aiosqlite.Connection:
    """Return a new connection with Row factory set."""
    conn = aiosqlite.connect(_DB_PATH)
    return conn


# ============================================================
# Admin helpers
# ============================================================

async def is_admin(user_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT 1 FROM admins WHERE user_id=?", (user_id,))
        return await cur.fetchone() is not None


async def is_super_admin(user_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT 1 FROM admins WHERE user_id=? AND level=1", (user_id,))
        return await cur.fetchone() is not None


async def get_admin(user_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM admins WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_admin(user_id: int, username: str, level: int, added_by: int) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO admins(user_id,username,level,added_by) VALUES(?,?,?,?)",
            (user_id, username, level, added_by))
        await conn.commit()


async def remove_admin(user_id: int) -> bool:
    """Remove non-super admin. Returns True if a row was deleted."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM admins WHERE user_id=? AND level!=1", (user_id,))
        await conn.commit()
        return cur.rowcount > 0


async def list_admins() -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM admins ORDER BY level, added_at")
        return [dict(r) for r in await cur.fetchall()]


# ============================================================
# Channel helpers
# ============================================================

async def get_target_channel() -> Optional[str]:
    """Return primary target channel id (string), or None."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT channel_id FROM channels WHERE type='target' ORDER BY id ASC LIMIT 1")
        row = await cur.fetchone()
        return str(row[0]) if row else None


async def get_all_target_channels() -> List[Dict]:
    """Return all configured target channels."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM channels WHERE type='target' ORDER BY id ASC")
        return [dict(r) for r in await cur.fetchall()]


async def set_target_channel(channel_id: int, channel_name: str) -> None:
    """Legacy: replace single target channel (kept for compatibility)."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute("DELETE FROM channels WHERE type='target'")
        await conn.execute(
            "INSERT INTO channels(type,channel_id,channel_name) VALUES('target',?,?)",
            (channel_id, channel_name))
        await conn.commit()


async def add_target_channel(channel_id: int, channel_name: str) -> bool:
    """Add a target channel without removing existing ones."""
    try:
        async with aiosqlite.connect(_DB_PATH) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO channels(type,channel_id,channel_name) VALUES('target',?,?)",
                (channel_id, channel_name))
            await conn.commit()
        return True
    except Exception:
        return False


async def remove_target_channel(channel_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM channels WHERE type='target' AND channel_id=?", (channel_id,))
        await conn.commit()
        return cur.rowcount > 0


async def get_source_channels() -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM channels WHERE type='source'")
        return [dict(r) for r in await cur.fetchall()]


async def add_source_channel(channel_id: int, channel_name: str) -> bool:
    try:
        async with aiosqlite.connect(_DB_PATH) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO channels(type,channel_id,channel_name) VALUES('source',?,?)",
                (channel_id, channel_name))
            await conn.commit()
        return True
    except Exception:
        return False


async def remove_source_channel(channel_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM channels WHERE type='source' AND channel_id=?", (channel_id,))
        await conn.commit()
        return cur.rowcount > 0


# ============================================================
# Config key-value store
# ============================================================

async def get_config(key: str, default: str = "") -> str:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute("SELECT value FROM config WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else default


async def set_config(key: str, value: str) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (key, value))
        await conn.commit()


async def get_discussion_group() -> Optional[str]:
    v = await get_config("discussion_group_url")
    return v or None


async def set_discussion_group(url: str) -> None:
    await set_config("discussion_group_url", url)


async def get_submission_limit() -> int:
    return int(await get_config("daily_submission_limit", "5"))


async def set_submission_limit(n: int) -> None:
    await set_config("daily_submission_limit", str(n))


# ============================================================
# Submission helpers
# ============================================================

async def create_submission(
    user_id: int, username: str, sign_type: str,
    custom_name: str | None, content_type: str,
    message_data: dict,
) -> int:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            """INSERT INTO submissions
               (user_id,username,sign_type,custom_name,content_type,message_data)
               VALUES(?,?,?,?,?,?)""",
            (user_id, username, sign_type, custom_name,
             content_type, json.dumps(message_data)))
        await conn.execute(
            "INSERT INTO submission_times(user_id) VALUES(?)", (user_id,))
        await conn.commit()
        return cur.lastrowid


async def get_submission(submission_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM submissions WHERE id=?", (submission_id,))
        row = await cur.fetchone()
        if not row:
            return None
        d = dict(row)
        d["message_data"] = json.loads(d["message_data"])
        return d


async def update_submission_status(
    submission_id: int, status: str,
    reject_reason: str | None = None,
    reviewed_by: int | None = None,
) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            """UPDATE submissions
               SET status=?, reject_reason=?, reviewed_by=?, reviewed_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (status, reject_reason, reviewed_by, submission_id))
        await conn.commit()


async def count_user_submissions_today(user_id: int) -> int:
    today = datetime.now().strftime("%Y-%m-%d")
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM submission_times WHERE user_id=? AND DATE(submitted_at)=?",
            (user_id, today))
        row = await cur.fetchone()
        return row[0] if row else 0


async def count_user_submissions_in_window(user_id: int, seconds: int) -> int:
    since = (datetime.now() - timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) FROM submission_times WHERE user_id=? AND submitted_at>?",
            (user_id, since))
        row = await cur.fetchone()
        return row[0] if row else 0


async def set_user_cooldown(user_id: int, hours: int = 24) -> None:
    until = (datetime.now() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO user_cooldowns(user_id,cooldown_until) VALUES(?,?)",
            (user_id, until))
        await conn.commit()


async def check_user_cooldown(user_id: int) -> bool:
    """Return True if the user is currently in a submission cooldown."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT cooldown_until FROM user_cooldowns WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return False
        until = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
        return datetime.now() < until


# ============================================================
# Blacklist helpers
# ============================================================

async def is_blacklisted(user_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT 1 FROM blacklist WHERE user_id=?", (user_id,))
        return await cur.fetchone() is not None


async def add_to_blacklist(user_id: int, banned_by: int) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO blacklist(user_id,banned_by) VALUES(?,?)",
            (user_id, banned_by))
        await conn.commit()


async def remove_from_blacklist(user_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM blacklist WHERE user_id=?", (user_id,))
        await conn.commit()
        return cur.rowcount > 0


# ============================================================
# Reaction helpers
# ============================================================

async def get_reaction_counts(message_id: int) -> Tuple[int, int]:
    """Return (likes, dislikes)."""
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            """SELECT
                 SUM(CASE WHEN type='like'    THEN 1 ELSE 0 END),
                 SUM(CASE WHEN type='dislike' THEN 1 ELSE 0 END)
               FROM reactions WHERE message_id=?""",
            (message_id,))
        row = await cur.fetchone()
        return (row[0] or 0, row[1] or 0) if row else (0, 0)


async def get_user_reaction(message_id: int, user_id: int) -> Optional[str]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT type FROM reactions WHERE message_id=? AND user_id=?",
            (message_id, user_id))
        row = await cur.fetchone()
        return row[0] if row else None


async def toggle_reaction(
    message_id: int, user_id: int, reaction_type: str
) -> Tuple[int, int]:
    """
    Toggle a like/dislike reaction with mutual exclusion.
    - Clicking the same button again removes the reaction.
    - Switching from like to dislike (or vice versa) replaces it.
    Returns updated (likes, dislikes).
    """
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        cur = await conn.execute(
            "SELECT type FROM reactions WHERE message_id=? AND user_id=?",
            (message_id, user_id))
        existing_row = await cur.fetchone()
        existing = existing_row[0] if existing_row else None

        if existing == reaction_type:
            # Toggle off
            await conn.execute(
                "DELETE FROM reactions WHERE message_id=? AND user_id=?",
                (message_id, user_id))
        elif existing:
            # Switch reaction — also update created_at so daily stats are accurate
            await conn.execute(
                "UPDATE reactions SET type=?, created_at=CURRENT_TIMESTAMP WHERE message_id=? AND user_id=?",
                (reaction_type, message_id, user_id))
        else:
            # New reaction
            await conn.execute(
                "INSERT INTO reactions(message_id,user_id,type) VALUES(?,?,?)",
                (message_id, user_id, reaction_type))

        await conn.commit()

        cur = await conn.execute(
            """SELECT
                 SUM(CASE WHEN type='like'    THEN 1 ELSE 0 END),
                 SUM(CASE WHEN type='dislike' THEN 1 ELSE 0 END)
               FROM reactions WHERE message_id=?""",
            (message_id,))
        row = await cur.fetchone()
        return (row[0] or 0, row[1] or 0) if row else (0, 0)


# ============================================================
# Category helpers
# ============================================================

async def get_categories() -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM categories ORDER BY is_default DESC, name")
        rows = await cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["keywords"] = json.loads(d["keywords"])
            result.append(d)
        return result


async def add_category(name: str, keywords: list, is_default: bool = False) -> int:
    async with aiosqlite.connect(_DB_PATH) as conn:
        if is_default:
            await conn.execute("UPDATE categories SET is_default=0")
        cur = await conn.execute(
            "INSERT INTO categories(name,keywords,is_default) VALUES(?,?,?)",
            (name, json.dumps(keywords), 1 if is_default else 0))
        await conn.commit()
        return cur.lastrowid


async def update_category(
    cat_id: int,
    name: str | None = None,
    keywords: list | None = None,
    is_default: bool | None = None,
) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        # Wrap all updates in a single transaction so is_default stays consistent
        await conn.execute("BEGIN")
        try:
            if name is not None:
                await conn.execute(
                    "UPDATE categories SET name=? WHERE id=?", (name, cat_id))
            if keywords is not None:
                await conn.execute(
                    "UPDATE categories SET keywords=? WHERE id=?",
                    (json.dumps(keywords), cat_id))
            if is_default is True:
                await conn.execute("UPDATE categories SET is_default=0")
                await conn.execute(
                    "UPDATE categories SET is_default=1 WHERE id=?", (cat_id,))
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def delete_category(cat_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM categories WHERE id=?", (cat_id,))
        await conn.commit()
        return cur.rowcount > 0


async def get_default_category_name() -> str:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT name FROM categories WHERE is_default=1 LIMIT 1")
        row = await cur.fetchone()
        return row[0] if row else "综合"


# ============================================================
# Bad-word helpers
# ============================================================

async def get_bad_words() -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT * FROM bad_words ORDER BY id")
        return [dict(r) for r in await cur.fetchall()]


async def add_bad_word(word: str, fuzzy_match: bool, added_by: int) -> bool:
    try:
        async with aiosqlite.connect(_DB_PATH) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO bad_words(word,fuzzy_match,added_by) VALUES(?,?,?)",
                (word, 1 if fuzzy_match else 0, added_by))
            await conn.commit()
        return True
    except Exception:
        return False


async def remove_bad_word(word_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM bad_words WHERE id=?", (word_id,))
        await conn.commit()
        return cur.rowcount > 0


# ============================================================
# Ad-package helpers
# ============================================================

async def get_ad_packages() -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM ad_packages ORDER BY priority DESC, id")
        return [dict(r) for r in await cur.fetchall()]


async def get_current_ad_package() -> Optional[Dict]:
    """Return the active ad package for the current wall-clock time."""
    now_str = datetime.now().strftime("%H:%M")
    packages = await get_ad_packages()

    # Prefer time-windowed, non-default packages with highest priority
    for pkg in packages:
        if not pkg["is_active"] or pkg["is_default"]:
            continue
        s, e = pkg.get("start_time"), pkg.get("end_time")
        if s and e:
            if s <= e:
                if s <= now_str <= e:
                    return pkg
            else:  # wrap-around e.g. 22:00 → 02:00
                if now_str >= s or now_str <= e:
                    return pkg

    # Fall back to default package
    for pkg in packages:
        if pkg["is_active"] and pkg["is_default"]:
            return pkg
    return None


async def add_ad_package(
    name: str, start_time: str | None, end_time: str | None,
    is_default: bool = False, priority: int = 0,
) -> int:
    async with aiosqlite.connect(_DB_PATH) as conn:
        if is_default:
            await conn.execute("UPDATE ad_packages SET is_default=0")
        cur = await conn.execute(
            """INSERT INTO ad_packages(name,start_time,end_time,is_default,priority)
               VALUES(?,?,?,?,?)""",
            (name, start_time, end_time, 1 if is_default else 0, priority))
        await conn.commit()
        return cur.lastrowid


async def toggle_ad_package(package_id: int, is_active: bool) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "UPDATE ad_packages SET is_active=? WHERE id=?",
            (1 if is_active else 0, package_id))
        await conn.commit()


async def delete_ad_package(package_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM ad_packages WHERE id=?", (package_id,))
        await conn.commit()
        return cur.rowcount > 0


async def add_ad_button(
    package_id: int, label: str, url: str,
    row_index: int = 0, col_index: int = 0,
) -> int:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            """INSERT INTO ad_buttons(package_id,label,url,row_index,col_index)
               VALUES(?,?,?,?,?)""",
            (package_id, label, url, row_index, col_index))
        await conn.commit()
        return cur.lastrowid


async def get_package_buttons(package_id: int) -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM ad_buttons WHERE package_id=? ORDER BY row_index, col_index",
            (package_id,))
        return [dict(r) for r in await cur.fetchall()]


async def delete_ad_button(button_id: int) -> bool:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "DELETE FROM ad_buttons WHERE id=?", (button_id,))
        await conn.commit()
        return cur.rowcount > 0


# ============================================================
# Logging helpers
# ============================================================

async def log_action(admin_id: int, action: str, detail: str | None = None) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "INSERT INTO admin_logs(admin_id,action,detail) VALUES(?,?,?)",
            (admin_id, action, detail))
        await conn.commit()


async def get_logs(limit: int = 20, offset: int = 0) -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            """SELECT l.*, a.username AS admin_username
               FROM admin_logs l
               LEFT JOIN admins a ON l.admin_id=a.user_id
               ORDER BY l.created_at DESC LIMIT ? OFFSET ?""",
            (limit, offset))
        return [dict(r) for r in await cur.fetchall()]


# ============================================================
# Message-map helpers
# ============================================================

async def set_message_map(
    source_msg_id: int, source_channel_id: int, target_msg_id: int
) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            """INSERT OR REPLACE INTO message_map
               (source_message_id,source_channel_id,target_message_id)
               VALUES(?,?,?)""",
            (source_msg_id, source_channel_id, target_msg_id))
        await conn.commit()


async def get_target_message(
    source_msg_id: int, source_channel_id: int
) -> Optional[int]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            """SELECT target_message_id FROM message_map
               WHERE source_message_id=? AND source_channel_id=?""",
            (source_msg_id, source_channel_id))
        row = await cur.fetchone()
        return row[0] if row else None


# ============================================================
# Queue helpers (SQLite-backed)
# ============================================================

async def enqueue_message(source_channel_id: int, message_data: dict) -> int:
    async with aiosqlite.connect(_DB_PATH) as conn:
        cur = await conn.execute(
            "INSERT INTO message_queue(source_channel_id,message_data) VALUES(?,?)",
            (source_channel_id, json.dumps(message_data)))
        await conn.commit()
        return cur.lastrowid


async def dequeue_messages(limit: int = 5) -> List[Dict]:
    async with aiosqlite.connect(_DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM message_queue WHERE processed=0 ORDER BY id LIMIT ?",
            (limit,))
        rows = await cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["message_data"] = json.loads(d["message_data"])
            result.append(d)
        return result


async def mark_queue_processed(queue_id: int) -> None:
    async with aiosqlite.connect(_DB_PATH) as conn:
        await conn.execute(
            "UPDATE message_queue SET processed=1 WHERE id=?", (queue_id,))
        await conn.commit()


# ============================================================
# Daily stats
# ============================================================

async def get_daily_stats() -> Dict[str, Any]:
    today = datetime.now().strftime("%Y-%m-%d")
    async with aiosqlite.connect(_DB_PATH) as conn:
        async def scalar(sql, *args):
            cur = await conn.execute(sql, args)
            row = await cur.fetchone()
            return row[0] or 0 if row else 0

        return {
            "date": today,
            "submissions": await scalar(
                "SELECT COUNT(*) FROM submissions WHERE DATE(submitted_at)=?", today),
            "approved": await scalar(
                "SELECT COUNT(*) FROM submissions WHERE DATE(reviewed_at)=? AND status='approved'", today),
            "rejected": await scalar(
                "SELECT COUNT(*) FROM submissions WHERE DATE(reviewed_at)=? AND status='rejected'", today),
            "likes": await scalar(
                "SELECT COUNT(*) FROM reactions WHERE DATE(created_at)=? AND type='like'", today),
            "dislikes": await scalar(
                "SELECT COUNT(*) FROM reactions WHERE DATE(created_at)=? AND type='dislike'", today),
        }
