

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — PREMIUM SYSTEM ====================

from __future__ import annotations

import html
import logging
import re
import time
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Tuple
from functools import wraps

import pytz
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, OperationFailure
from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import (
    FloodWait,
    MessageNotModified,
    MessageIdInvalid,
    ChatWriteForbidden,
    UserIsBlocked,
    PeerIdInvalid,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from helper.database import DARKXSIDE78
from plugins.admin import (
    _admin_or_sudo_filter,
    _sudo_filter,
    close_btn,
    safe_edit,
    get_loading_text,
    safe_get_users, # Updated from admin module refactor
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗛𝗘𝗟𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for Premium system API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[premium] FloodWait in {func.__name__}: {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

@retry_on_flood
async def safe_chat_action(client: Client, chat_id: int, action: ChatAction = ChatAction.TYPING):
    """Sends chat action with automatic FloodWait retry."""
    await client.send_chat_action(chat_id, action)

@retry_on_flood
async def safe_reply_photo(client: Client, chat_id: int, photo: str, caption: str, markup=None):
    """Sends photo with automatic FloodWait retry."""
    return await client.send_photo(chat_id, photo, caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚𝗦 |
# ═══════════════════════════════════════════════════════════

_PAGE_SIZE = 10
_CMD_LAST_USED: dict[tuple[int, str], float] = {}
IMG_PREMIUM_HEADER = "https://i.ibb.co/rL0w6Ry/photo-2026-05-02-19-25-34-7635377839881060380.jpg"

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class PremiumService:
    def __init__(self):
        self._total_cache: Optional[int] = None
        self._ts: float = 0.0
        self._ttl: float = 30.0

    async def get_total(self) -> int:
        now = time.monotonic()
        if self._total_cache is not None and (now - self._ts < self._ttl):
            return self._total_cache

        from helper.db_premium import db as _db
        total = await _db.count_premium_users()
        self._total_cache = total
        self._ts = now
        return total

    def invalidate(self):
        self._total_cache = None
        self._ts = 0.0

    async def add(self, uid: int, expiry: datetime):
        await DARKXSIDE78.add_premium_user(uid, expiry)
        self.invalidate()

    async def remove(self, uid: int):
        await DARKXSIDE78.remove_premium_user(uid)
        self.invalidate()

    async def get_page(self, page: int):
        from helper.db_premium import db as _db
        return await _db.list_premium_users_paginated(page * _PAGE_SIZE, _PAGE_SIZE)

    async def safe_resolve_user(self, client: Client, message: Message, args: list[str]) -> Tuple[Optional[dict], Optional[str]]:
        target_ref = None

        if message.reply_to_message and message.reply_to_message.from_user:
            target_ref = str(message.reply_to_message.from_user.id)
        elif len(args) > 1:
            target_ref = args[1].strip()

        if not target_ref:
            return None, "No target specified. Please reply to a user or provide an ID/Username."

        if target_ref.startswith("@"):
            if not re.match(r"^@[a-zA-Z0-9_]{4,32}$", target_ref):
                return None, "Malformed username signature."
            user_doc = await DARKXSIDE78.get_user_by_username(target_ref[1:])
            if not user_doc:
                return None, f"Username {target_ref} is not registered in the database."
        else:
            # Fix 3: Restrict to positive integers only
            if not re.match(r"^\d{5,15}$", target_ref):
                return None, "Malformed ID signature (positive numeric 5-15 digits only)."
            user_doc = await DARKXSIDE78.get_user(int(target_ref))
            if not user_doc:
                return None, f"User ID {target_ref} is not registered in the database."

        # Cache warmup using batch fetcher
        async def _prewarm(uid: int):
            try:
                await safe_get_users(client, uid)
            except:
                pass
        asyncio.create_task(_prewarm(user_doc["_id"]))

        return user_doc, None


prem_svc = PremiumService()

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗖𝗞𝗚𝗥𝗢𝗨𝗡𝗗 𝗧𝗔𝗦𝗞𝗦 |
# ═══════════════════════════════════════════════════════════

async def _evict_old_cooldowns():
    """Cleans up the command cooldown dictionary."""
    while True:
        try:
            await asyncio.sleep(600)
            now = time.monotonic()
            expired = [k for k, v in _CMD_LAST_USED.items() if now - v > 600]
            for k in expired:
                _CMD_LAST_USED.pop(k, None)
            if expired:
                logger.debug(f"[premium] Evicted {len(expired)} cooldown entries")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[premium] Cooldown eviction error: {e}")

def _on_cooldown(user_id: int, cmd: str) -> bool:
    """Tracks command cooldowns locally."""
    key = (user_id, cmd)
    now = time.monotonic()
    if now - _CMD_LAST_USED.get(key, 0.0) < 3.0: # Match _CMD_COOLDOWN pattern
        return True
    _CMD_LAST_USED[key] = now
    return False

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

@retry_on_flood
async def _do_edit_callback(cq: CallbackQuery, text: str, markup=None):
    """Internal helper to execute callback edit with FloodWait retries."""
    return await cq.edit_message_text(
        text, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True
    )

async def safe_edit_callback(cq: CallbackQuery, text: str, markup=None) -> None:
    """Fix 2: Hardened callback editor with automatic FloodWait retries."""
    try:
        await _do_edit_callback(cq, text, markup)
    except MessageNotModified:
        pass
    except (MessageIdInvalid, ChatWriteForbidden, UserIsBlocked, PeerIdInvalid) as e:
        logger.debug(f"[premium] Unrecoverable callback edit: {e}")
    except Exception as exc:
        logger.error(f"[premium] Callback Edit Fatal: {exc}")

async def respond_with_error(message: Message, error_msg: str) -> None:
    text = (
        f"<blockquote expandable>[ ✗ ] 𝗘𝗥𝗥𝗢𝗥 𝗘𝗡𝗖𝗢𝗨𝗡𝗧𝗘𝗥𝗘𝗗</blockquote>\n\n"
        f"<b>Rᴇᴀsᴏɴ:</b> <code>{_e(error_msg)}</code>\n\n"
        f"<i>ᴄᴏɴᴛᴀᴄᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ ɪғ ᴛʜɪs ᴘᴇʀsɪsᴛs.</i>"
    )
    await safe_edit(message, text, reply_markup=close_btn())

def _e(text: str) -> str: return html.escape(str(text))

def _user_link(doc: dict) -> str:
    uid = doc.get("_id", "?")
    name = _e(doc.get("first_name") or doc.get("username") or str(uid))
    return f'<a href="tg://user?id={uid}">{name}</a>'

def _fmt_dt(dt: Optional[datetime]) -> str:
    if not isinstance(dt, datetime): return "—"
    if dt.year == 9999: return "Lifetime ♾️"
    if dt.tzinfo is None: dt = pytz.utc.localize(dt)
    return dt.strftime("%Y-%m-%d %H:%M UTC")

def _parse_premium_duration(raw: str) -> Optional[datetime]:
    raw = raw.lower().strip()
    if raw == "lifetime":
        return datetime(9999, 12, 31, tzinfo=pytz.utc)
    m = re.match(r"(\d+)([a-z]+)", raw)
    if not m:
        raise ValueError("Duration format: [number][unit]. Examples: 2weeks, 1year.")
    num, unit = int(m.group(1)), m.group(2)
    if num > 1000:
        raise ValueError("Duration length exceeded maximum thresholds.")
    unit_map = {
        "m": "minutes", "min": "minutes", "minute": "minutes", "minutes": "minutes",
        "h": "hours", "hour": "hours", "hours": "hours",
        "d": "days", "day": "days", "days": "days",
        "w": "weeks", "week": "weeks", "weeks": "weeks",
        "month": "months", "months": "months",
        "y": "years", "year": "years", "years": "years",
    }
    mapped = unit_map.get(unit)
    if not mapped:
        raise ValueError(f"Unrecognized unit: {unit}. Use: d, w, month, y.")
    now = datetime.now(pytz.utc)
    if mapped == "months":
        yr, mo = divmod(now.month - 1 + num, 12)
        return now.replace(year=now.year + yr, month=mo + 1)
    if mapped == "years":
        return now.replace(year=now.year + num)
    return now + timedelta(**{mapped: num})

def _page_nav(prefix: str, current: int, total_pages: int) -> InlineKeyboardMarkup:
    row: list[InlineKeyboardButton] = []
    if current > 0:
        row.append(InlineKeyboardButton("◀ Pʀᴇᴠ", callback_data=f"{prefix}_p{current - 1}"))
    row.append(InlineKeyboardButton(f"{current + 1}/{total_pages}", callback_data="noop_prem"))
    if current < total_pages - 1:
        row.append(InlineKeyboardButton("Nᴇxᴛ ▶", callback_data=f"{prefix}_p{current + 1}"))
    return InlineKeyboardMarkup([
        row,
        [InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="close_msg")],
    ])

@Client.on_callback_query(filters.regex(r"^noop_prem$"))
async def cb_noop(client: Client, cq: CallbackQuery) -> None:
    await cq.answer()

# ═══════════════════════════════════════════════════════════
#  | 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗦𝗬𝗦𝗧𝗘𝗠 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("add_premium") & filters.private & _sudo_filter())
async def cmd_add_premium(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "add_premium"):
        return

    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=2)
        user, err = await prem_svc.safe_resolve_user(client, message, args)
        
        if err or (not message.reply_to_message and len(args) < 3) or (message.reply_to_message and len(args) < 2):
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/add_premium &lt;@username/userid&gt; &lt;duration&gt;</code>\n"
                "›› <b>Oʀ :</b> <code>Rᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴡɪᴛʜ /add_premium &lt;duration&gt;</code>\n\n"
                "<blockquote>◍ <b>Exᴀᴍᴘʟᴇs :</b>\n"
                "›› <code>1day</code>, <code>2weeks</code>, <code>3months</code>, <code>1year</code>, <code>lifetime</code></blockquote>"
                + (f"\n<blockquote>⚠️ {err}</blockquote>" if err else ""),
                reply_markup=close_btn()
            )

        uid = user["_id"]
        
        # Parse Duration from correct index
        raw_duration = args[1] if message.reply_to_message else args[2]
        expiry = _parse_premium_duration(raw_duration)

        await safe_edit(temp, "<b><i>Cᴏᴍᴍɪᴛᴛɪɴɢ ᴘʀᴇᴍɪᴜᴍ ʙʟᴏᴄᴋ...</i></b>")
        
        await prem_svc.add(uid, expiry)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗕𝗟𝗢𝗖𝗞 𝗪𝗥𝗜𝗧𝗧𝗘𝗡</blockquote>\n\n"
            f"›› <b>Usᴇʀ :</b> {_user_link(user)} (<code>{uid}</code>)\n"
            f"<blockquote>◍ <b>Exᴘɪʀᴇs :</b>\n<code>{_fmt_dt(expiry)}</code></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[premium] Added {uid} until {expiry} by {message.from_user.id}")

    except ValueError as e:
        await respond_with_error(temp, str(e))
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[premium] cmd_add_premium fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Unhandled Node Exception: {str(e)[:100]}")

@Client.on_message(filters.command("remove_premium") & filters.private & _sudo_filter())
async def cmd_remove_premium(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "remove_premium"):
        return

    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=1)
        user, err = await prem_svc.safe_resolve_user(client, message, args)
        
        if err:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/remove_premium &lt;@username/userid&gt;</code>\n"
                "›› <b>Oʀ :</b> <code>Rᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴡɪᴛʜ /remove_premium</code>\n\n"
                f"<blockquote>⚠️ {err}</blockquote>",
                reply_markup=close_btn()
            )

        uid = user["_id"]
        if not bool(user.get("is_premium")):
            return await respond_with_error(temp, f"State Violation: User {uid} has no active premium flags.")

        await safe_edit(temp, "<b><i>Rᴇᴠᴏᴋɪɴɢ ᴘʀᴇᴍɪᴜᴍ ᴘʀɪᴠɪʟᴇɢᴇs...</i></b>")

        await prem_svc.remove(uid)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗖𝗟𝗘𝗔𝗥𝗔𝗡𝗖𝗘 𝗥𝗘𝗩𝗢𝗞𝗘𝗗</blockquote>\n\n"
            f"›› <b>Usᴇʀ :</b> {_user_link(user)} (<code>{uid}</code>)\n\n"
            f"<blockquote><i>Privileges purged. Restored to base config.</i></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[premium] Wiped premium from {uid} by {message.from_user.id}")

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[premium] cmd_remove_premium fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Unhandled Node Exception: {str(e)[:100]}")

@Client.on_message(filters.command("premiumlist") & filters.private & _admin_or_sudo_filter())
async def cmd_premiumlist(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "premiumlist"):
        return

    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)
    try:
        await safe_edit(temp, "<b><i>Fᴇᴛᴄʜɪɴɢ...</i></b>")
        text, markup = await _premiumlist_page(0)
        
        await safe_reply_photo(client, message.chat.id, IMG_PREMIUM_HEADER, text, markup)
        await temp.delete()
        
    except Exception as e:
        logger.error(f"[premium] premiumlist fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Failed initializing pointer: {str(e)[:50]}")

async def _premiumlist_page(page: int) -> tuple[str, InlineKeyboardMarkup]:
    total = await prem_svc.get_total()

    if total == 0:
        return (
            "<blockquote expandable>[ ⓘ ] 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗟𝗜𝗦𝗧</blockquote>\n\n<i>No active premium users found.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="close_msg")]]),
        )

    docs = await prem_svc.get_page(page)
    skip = page * _PAGE_SIZE

    lines = [
        f"<blockquote><b>✦ 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗔𝗨𝗗𝗜𝗧</b></blockquote>\n"
        f"<blockquote>›› ᴛᴏᴛᴀʟ : <code>{total}</code> ᴘʀᴇᴍɪᴜᴍ ᴜsᴇʀ(s)</blockquote>\n"
        "────────────────────\n"
    ]
    for i, doc in enumerate(docs, start=skip + 1):
        uid = doc["_id"]
        name = _e(doc.get("first_name") or doc.get("username") or str(uid))
        expiry = _fmt_dt(doc.get("premium_expiry"))
        lines.append(
            f"<blockquote><b>{i}.</b> <a href=\"tg://user?id={uid}\">{name}</a> [<code>{uid}</code>]\n"
            f"↳ <i>Exᴘɪʀᴇs: {expiry}</i></blockquote>\n"
        )

    total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    return "\n".join(lines), _page_nav("premiumlist", page, total_pages)

@Client.on_callback_query(filters.regex(r"^premiumlist_p(\d+)$") & _admin_or_sudo_filter())
async def cb_premiumlist(client: Client, cq: CallbackQuery) -> None:
    try:
        page = int(cq.matches[0].group(1))
        text, markup = await _premiumlist_page(page)
        await safe_edit_callback(cq, text, markup=markup)
        await cq.answer()
    except Exception as e:
        logger.error(f"[premium] Pagination cb_premium fault: {e}")
        await cq.answer("Pointer transition fault.", show_alert=True)

async def init(*args, **kwargs):
    """Fix 4: Starts background maintenance tasks."""
    loop = asyncio.get_running_loop()
    loop.create_task(_evict_old_cooldowns())
    logger.info("[premium] Background maintenance tasks started.")

__all__ = ["prem_svc", "init"]

logger.info("[✓] Premium Engine Module Hardened & Loaded")

