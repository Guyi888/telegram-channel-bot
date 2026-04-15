"""
Module 5 — Reaction callbacks (👍 / 👎).

Callback data format:  react:<type>:<message_id>
  type = 'like' | 'dislike'
  message_id = the channel message_id that was reacted to
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from database import db
from services.publisher import update_reaction_markup

logger = logging.getLogger(__name__)


async def handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        _, reaction_type, message_id_str = query.data.split(":")
        message_id = int(message_id_str)
    except (ValueError, AttributeError):
        logger.warning("Malformed reaction callback data: %s", query.data)
        return

    user_id = update.effective_user.id

    likes, dislikes = await db.toggle_reaction(message_id, user_id, reaction_type)

    target = await db.get_target_channel()
    if not target:
        return

    await update_reaction_markup(context.bot, target, message_id)
