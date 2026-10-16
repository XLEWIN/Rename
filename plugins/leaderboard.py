# =============================================================================
# plugins/leaderboard.py — /leaderboard command + callback filters
# =============================================================================
from __future__ import annotations

import logging
import html
import asyncio
import time
from functools import wraps
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, Tuple

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    InputMediaPhoto,
)
from pyrogram.enums import ParseMode, ChatType
from pyrogram.errors import MessageNotModified, FloodWait

from helper.database import DARKXSIDE78
from config import Config
from plugins.admin import safe_edit, safe_get_user, _on_cooldown, safe_delete_message

logger = logging.getLogger(__name__)

# PERIOD_CONFIG: (display_name, start_time_factory)

AUTO_DELETE_SECONDS: int = 60
LEADERBOARD_PIC = "https://i.ibb.co/zWdtYYk8/photo-2026-05-02-16-50-50-7635343626171580456.jpg"

# Period config: (display_name, start_time_factory)
_PERIOD_CONFIG: dict[str, tuple[str, Any]] = {
    "today": (
        "𝗧𝗢𝗗𝗔𝗬",
        lambda now: now.replace(hour=0, minute=0, second=0, microsecond=0),
    ),
    "week": (
        "𝗧𝗛𝗜𝗦 𝗪𝗘𝗘𝗞",
        lambda now: (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
    ),
    "month": (
        "𝗧𝗛𝗜𝗦 𝗠𝗢𝗡𝗧𝗛",
        lambda now: now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
    ),
    "year": (
        "This Year",
        lambda now: now.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        ),
    ),
    "lifetime": ("𝗔𝗹𝗹 𝗧𝗜𝗠𝗘", lambda _: None),
}


# ─────────────────────────────────────────────────────────────
# Service Layer
# ─────────────────────────────────────────────────────────────

class LeaderboardService:
    def __init__(self):
        self._cache: dict[str, tuple[float, list[dict]]] = {}
        self._ttl: float = 30.0
        self._lock = asyncio.Lock()

    async def get_top(self, filter_type: str) -> list[dict]:
        now = time.monotonic()
        async with self._lock:
            cached = self._cache.get(filter_type)
            if cached and (now - cached[0] < self._ttl):
                return cached[1]

        data = await self._fetch_leaderboard_data(filter_type)
        
        async with self._lock:
            self._cache[filter_type] = (time.monotonic(), data)
            
        return data

    async def _fetch_leaderboard_data(self, filter_type: str) -> list[dict]:
        now_dt = datetime.now(timezone.utc)
        _, start_fn = _PERIOD_CONFIG.get(filter_type, _PERIOD_CONFIG["lifetime"])
        start_time: Optional[datetime] = start_fn(now_dt)

        if start_time is not None:
            pipeline = [
                {"$unwind": "$history"},
                {"$match": {"history.timestamp": {"$gte": start_time}}},
                {
                    "$group": {
                        "_id": "$_id",
                        "rename_count": {"$sum": 1},
                    }
                },
                {"$sort": {"rename_count": -1}},
                {"$limit": 5},
            ]
        else:
            pipeline = [
                {
                    "$addFields": {
                        "rename_count": {"$size": {"$ifNull": ["$history", []]}}
                    }
                },
                {"$match": {"rename_count": {"$gt": 0}}},
                {"$sort": {"rename_count": -1}},
                {"$limit": 5},
            ]

        return await DARKXSIDE78.leaderboard_top(pipeline)

    async def get_user_rank(self, requester_id: int, filter_type: str) -> Tuple[Optional[int], int]:
        """Returns (rank, count)"""
        now_dt = datetime.now(timezone.utc)
        _, start_fn = _PERIOD_CONFIG.get(filter_type, _PERIOD_CONFIG["lifetime"])
        start_time: Optional[datetime] = start_fn(now_dt)

        user_count = 0
        user_rank = None

        if start_time is not None:
            user_pipe = [
                {"$match": {"_id": requester_id}},
                {"$unwind": "$history"},
                {"$match": {"history.timestamp": {"$gte": start_time}}},
                {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
            ]
            user_data = await DARKXSIDE78.leaderboard_user_rank(user_pipe)
            if user_data:
                user_count = user_data[0]["count"]

            if user_count > 0:
                rank_pipe = [
                    {"$unwind": "$history"},
                    {"$match": {"history.timestamp": {"$gte": start_time}}},
                    {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
                    {"$match": {"count": {"$gt": user_count}}},
                    {"$count": "higher"},
                ]
                rank_result = await DARKXSIDE78.leaderboard_user_rank(rank_pipe)
                user_rank = (rank_result[0]["higher"] + 1) if rank_result else 1
        else:
            user_doc = await DARKXSIDE78.leaderboard_user_doc(requester_id)
            history = user_doc.get("history", []) if user_doc else []
            user_count = len(history)
            if user_count > 0:
                higher = await DARKXSIDE78.leaderboard_rank_count(
                    {"$expr": {"$gt": [{"$size": {"$ifNull": ["$history", []]}}, user_count]}}
                )
                user_rank = higher + 1

        return user_rank, user_count

lb_svc = LeaderboardService()

async def _evict_leaderboard_cache():
    while True:
        try:
            await asyncio.sleep(600)  # Evict every 10 minutes
            now = time.monotonic()
            async with lb_svc._lock:
                expired = [k for k, v in lb_svc._cache.items() if now - v[0] > lb_svc._ttl]
                for k in expired:
                    lb_svc._cache.pop(k, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[leaderboard] Cache eviction fault: {e}")

def init(*args, **kwargs):
    loop = asyncio.get_running_loop()
    loop.create_task(_evict_leaderboard_cache())
    logger.info("[leaderboard] Background maintenance started.")

# ─────────────────────────────────────────────────────────────
# UI & Formatting Layers
# ─────────────────────────────────────────────────────────────

def _esc(text: Optional[str]) -> str:
    if not text:
        return "Unknown"
    return html.escape(str(text))

def build_lb_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Tᴏᴅᴀʏ", callback_data=f"lb_today:{user_id}"),
                InlineKeyboardButton("Wᴇᴇᴋ", callback_data=f"lb_week:{user_id}"),
                InlineKeyboardButton("Mᴏɴᴛʜ", callback_data=f"lb_month:{user_id}"),
            ],
            [
                InlineKeyboardButton("Yᴇᴀʀ", callback_data=f"lb_year:{user_id}"),
                InlineKeyboardButton("Lɪғᴇᴛɪᴍᴇ", callback_data=f"lb_lifetime:{user_id}"),
            ],
            [
                InlineKeyboardButton("Cʟᴏsᴇ", callback_data=f"lb_close:{user_id}")
            ]
        ]
    )

async def resolve_user_info(client: Client, uids: list[int]) -> dict[int, str]:
    """Batch fetches user info concurrently."""
    names = {}
    
    async def fetch_name(uid: int):
        try:
            tg_user = await safe_get_user(client, uid)
            if tg_user and hasattr(tg_user, "first_name"):
                return uid, _esc(tg_user.first_name)
        except Exception:
            pass
        
        # Fallback
        user_doc_name = await DARKXSIDE78.get_user_first_name(uid)
        if user_doc_name:
            return uid, _esc(user_doc_name)
        return uid, "Unknown"

    if uids:
        results = await asyncio.gather(*(fetch_name(uid) for uid in uids))
        for uid, name in results:
            names[uid] = name
        
    return names

def format_leaderboard_text(
    filter_type: str,
    top_users: list[dict],
    user_names: dict[int, str],
    user_rank: Optional[int] = None,
    user_count: int = 0,
    is_requester: bool = False
) -> str:

    period_name, _ = _PERIOD_CONFIG.get(
        filter_type,
        _PERIOD_CONFIG["lifetime"]
    )

    lines = [
        f"<blockquote><b>✦ {period_name} 𝗟𝗘𝗔𝗗𝗘𝗥𝗕𝗢𝗔𝗥𝗗</b></blockquote>\n"
        f"<blockquote><b>≡ Tᴏᴘ 5 ᴍᴏsᴛ ᴀᴄᴛɪᴠᴇ ᴜsᴇʀs</b></blockquote>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"➖➖➖➖➖➖➖➖➖➖➖\n"
    ]

    if not top_users:
        lines.append(
            "<blockquote>"
            "✗ Nᴏ ʀᴇɴᴀᴍᴇ ᴅᴀᴛᴀ ʏᴇᴛ ғᴏʀ ᴛʜɪs ᴘᴇʀɪᴏᴅ."
            "</blockquote>\n"
        )

    else:
        for idx, entry in enumerate(top_users, 1):

            uid = entry["_id"]
            count = entry.get("rename_count", 0)
            name = user_names.get(uid, "Unknown")

            rank_str = f"0{idx}" if idx < 10 else str(idx)

            lines.append(
                f"<b>{rank_str} • </b> "
                f"<a href=\"tg://user?id={uid}\">{name}</a>\n"
                f"└ <b>{count}</b> Rᴇɴᴀᴍᴇs\n"
            )

    lines.append(
        "➖➖➖➖➖➖➖➖➖➖➖\n"
        "━━━━━━━━━━━━━━━━━━"
    )

    if is_requester:

        if user_rank:
            lines.append(
                f"<blockquote>"
                f"<b>≡ Yᴏᴜʀ ʀᴀɴᴋ :</b> "
                f"#{user_rank}  »  {user_count} Rᴇɴᴀᴍᴇs"
                f"</blockquote>"
            )

        else:
            lines.append(
                "<blockquote>"
                "<b>≡ Yᴏᴜʀ ʀᴀɴᴋ :</b> Uɴʀᴀɴᴋᴇᴅ"
                "</blockquote>"
            )

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────

@Client.on_message((filters.private | filters.group) & filters.command("leaderboard"))
async def leaderboard_cmd(client: Client, message: Message):
    try:
        requester_id = message.from_user.id if message.from_user else 0
        if requester_id and _on_cooldown(requester_id, "leaderboard_cmd"):
            return await message.reply_text( "⧗ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ...", quote=True)

        filter_type = "today"
        top_users = await lb_svc.get_top(filter_type)
        
        user_rank, user_count = None, 0
        if requester_id:
            user_rank, user_count = await lb_svc.get_user_rank(requester_id, filter_type)
            
        uids_to_fetch = [u["_id"] for u in top_users]
        user_names = await resolve_user_info(client, uids_to_fetch)
        
        is_private = message.chat.type == ChatType.PRIVATE
        text = format_leaderboard_text(
            filter_type=filter_type,
            top_users=top_users,
            user_names=user_names,
            user_rank=user_rank,
            user_count=user_count,
            is_requester=bool(requester_id)
        )
        
        keyboard = build_lb_markup(requester_id)

        sent = await message.reply_photo(
            photo=LEADERBOARD_PIC,
            caption=text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
            quote=True,
        )

        if not is_private and sent:
            async def _cleanup():
                await asyncio.sleep(AUTO_DELETE_SECONDS)
                try:
                    await safe_delete_message(sent)
                    await safe_delete_message(message)
                except Exception as e:
                    logger.warning(f"[leaderboard] Cleanup failed: {e}")
            asyncio.create_task(_cleanup())

    except Exception as e:
        logger.error(f"leaderboard_cmd: {e}", exc_info=True)
        await message.reply_text( "<b>✗ ᴇʀʀᴏʀ ʟᴏᴀᴅɪɴɢ ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ.</b>", parse_mode=ParseMode.HTML)

@Client.on_callback_query(filters.regex(r"^lb_(today|week|month|year|lifetime):(\d+)$"))
async def leaderboard_filter_cb(client: Client, query: CallbackQuery):
    try:
        requester_id = query.from_user.id
        filter_type = query.matches[0].group(1)
        owner_id = int(query.matches[0].group(2))

        if owner_id != 0 and requester_id != owner_id:
            return await query.answer("ⓘ ᴛʜɪꜱ ɪꜱ ɴᴏᴛ ʏᴏᴜʀ ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ !", show_alert=True)

        if _on_cooldown(requester_id, "leaderboard_cb"):
            return await query.answer("⧗ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ...", show_alert=True)

        await query.answer(f"ʟᴏᴀᴅɪɴɢ {filter_type}...")

        top_users = await lb_svc.get_top(filter_type)
        user_rank, user_count = await lb_svc.get_user_rank(requester_id, filter_type)
        
        uids_to_fetch = [u["_id"] for u in top_users]
        user_names = await resolve_user_info(client, uids_to_fetch)
        
        text = format_leaderboard_text(
            filter_type=filter_type,
            top_users=top_users,
            user_names=user_names,
            user_rank=user_rank,
            user_count=user_count,
            is_requester=True
        )
        
        keyboard = build_lb_markup(requester_id)

        try:
            await query.message.edit_caption(
                caption=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
        except MessageNotModified:
            pass
        except Exception as edit_err:
            logger.warning(f"[leaderboard] Failed to edit_caption: {edit_err}")

    except Exception as e:
        logger.error(f"leaderboard_filter_cb: {e}", exc_info=True)
        await query.answer("✗ ᴇʀʀᴏʀ", show_alert=True)

# ─────────────────────────────────────────────────────────────
# Close button
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^lb_close:(\d+)$"))
async def leaderboard_close_cb(client: Client, query: CallbackQuery):
    try:
        requester_id = query.from_user.id
        owner_id = int(query.matches[0].group(1))

        if owner_id != 0 and requester_id != owner_id:
            return await query.answer("ⓘ ᴛʜɪꜱ ɪꜱ ɴᴏᴛ ʏᴏᴜʀ ʟᴇᴀᴅᴇʀʙᴏᴀʀᴅ !", show_alert=True)

        await safe_delete_message(query.message)
        if query.message.reply_to_message:
            await safe_delete_message(query.message.reply_to_message)
    except Exception:
        pass
    await query.answer()

__all__ = ["init"]
