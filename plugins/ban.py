# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — BAN SYSTEM ====================

from __future__ import annotations

import html
import logging
import re
import time
import asyncio
from typing import Optional, Tuple
from functools import wraps

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
    _on_cooldown,
    safe_get_users, # Updated from admin module refactor
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗛𝗘𝗟𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for Ban system API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[ban] FloodWait in {func.__name__}: {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

@retry_on_flood
async def safe_chat_action(client: Client, chat_id: int, action: ChatAction = ChatAction.TYPING):
    """Sends chat action with automatic FloodWait retry."""
    await client.send_chat_action(chat_id, action)

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class BanService:
    """Encapsulates DB operations and caches to shield handlers from Raw DB calls."""
    
    def __init__(self):
        self._total_banned_cache: Optional[int] = None
        self._total_banned_ts: float = 0.0
        self._total_ttl: float = 30.0  # seconds

    async def get_total_banned(self) -> int:
        now = time.monotonic()
        if self._total_banned_cache is not None and (now - self._total_banned_ts < self._total_ttl):
            return self._total_banned_cache
            
        total = await DARKXSIDE78.count_banned_users()
        self._total_banned_cache = total
        self._total_banned_ts = now
        return total

    def invalidate_total_cache(self) -> None:
        self._total_banned_cache = None
        self._total_banned_ts = 0.0

    async def ban_user(self, user_id: int, reason: str) -> None:
        await DARKXSIDE78.ban_user(user_id, reason)
        self.invalidate_total_cache()

    async def unban_user(self, user_id: int) -> None:
        await DARKXSIDE78.unban_user(user_id)
        self.invalidate_total_cache()

    async def is_banned(self, user_id: int) -> tuple[bool, Optional[str]]:
        return await DARKXSIDE78.is_user_banned(user_id)


    async def get_banlist_page(self, page: int) -> list[dict]:
        return await DARKXSIDE78.get_banlist_page(page, 10) # Fixed _PAGE_SIZE local usage

    async def safe_resolve_user(self, client: Client, message: Message, args: list[str]) -> Tuple[Optional[dict], Optional[str]]:
        """
        Resolves the target user safely using replies or args.
        Returns: (user_dict, error_string)
        """
        target_ref = None

        # Determine target from reply or args
        if message.reply_to_message and message.reply_to_message.from_user:
            target_ref = str(message.reply_to_message.from_user.id)
        elif len(args) > 1:
            target_ref = args[1].strip()

        if not target_ref:
            return None, "No target specified. Please reply to a user or provide an ID/Username."

        # Fetch from DB (try ID first, then username)
        if target_ref.startswith("@"):
            if not re.match(r"^@[a-zA-Z0-9_]{4,32}$", target_ref):
                return None, "Malformed username signature."
            user_doc = await DARKXSIDE78.get_user_by_username(target_ref[1:])
            if not user_doc:
                return None, f"Username {target_ref} is not registered in the database."
        else:
            # Fix 4: Restrict to positive integers only
            if not re.match(r"^\d{5,15}$", target_ref):
                return None, "Malformed ID signature (positive numeric 5-15 digits only)."
            user_doc = await DARKXSIDE78.get_user(int(target_ref))
            if not user_doc:
                return None, f"User ID {target_ref} is not registered in the database."

        # Fix 7: Safeguard cache pre-seed with proper task handling
        async def _prewarm_cache(uid: int):
            try:
                # Use batch fetcher from admin module which is already FloodWait-proofed
                await safe_get_users(client, uid)
            except Exception as e:
                logger.debug(f"[ban] Cache pre-seed failure for {uid}: {e}")

        asyncio.create_task(_prewarm_cache(user_doc["_id"]))

        return user_doc, None

ban_svc = BanService()

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
    """Fix 6: Hardened callback editor with automatic FloodWait retries."""
    try:
        await _do_edit_callback(cq, text, markup)
    except MessageNotModified:
        pass
    except (MessageIdInvalid, ChatWriteForbidden, UserIsBlocked, PeerIdInvalid) as e:
        logger.debug(f"[ban] Unrecoverable callback edit: {e}")
    except Exception as exc:
        logger.error(f"[ban] Callback Edit Fatal: {exc}")

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

def _page_nav(prefix: str, current: int, total_pages: int) -> InlineKeyboardMarkup:
    row: list[InlineKeyboardButton] = []
    if current > 0:
        row.append(InlineKeyboardButton("◀ Pʀᴇᴠ", callback_data=f"{prefix}_p{current - 1}"))
    row.append(InlineKeyboardButton(f"{current + 1}/{total_pages}", callback_data="noop_ban"))
    if current < total_pages - 1:
        row.append(InlineKeyboardButton("Nᴇxᴛ ▶", callback_data=f"{prefix}_p{current + 1}"))
    return InlineKeyboardMarkup([
        row,
        [InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="close_msg")],
    ])

@Client.on_callback_query(filters.regex(r"^noop_ban$"))
async def cb_noop(client: Client, cq: CallbackQuery) -> None:
    await cq.answer()

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗡 𝗦𝗬𝗦𝗧𝗘𝗠 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("ban") & filters.private & _admin_or_sudo_filter())
async def cmd_ban(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "ban"):
        return

    # Fix 1: Wrap chat action
    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=2)
        target_user, err = await ban_svc.safe_resolve_user(client, message, args)
        
        if err:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/ban &lt;@username/userid&gt; [reason]</code>\n"
                "›› <b>Oʀ :</b> <code>Rᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴡɪᴛʜ /ban [reason]</code>\n\n"
                f"<blockquote>⚠️ {err}</blockquote>",
                reply_markup=close_btn()
            )

        uid = target_user["_id"]
        ban_status = target_user.get("ban_status") or {}
        
        if ban_status.get("is_banned") is True:
            return await respond_with_error(temp, f"State Violation: User {uid} is already banned.")

        if await DARKXSIDE78.is_admin(uid) or uid == Config.OWNER_ID:
             return await respond_with_error(temp, f"Constraint Violation: Cannot ban an administrator.")

        # Determine reason
        if message.reply_to_message and len(args) > 1:
            reason = _e(" ".join(args[1:]))
        elif len(args) > 2:
            reason = _e(args[2])
        else:
            reason = "Violation of terms identified during audit."

        await safe_edit(temp, "<b><i>Cᴏᴍᴍɪᴛᴛɪɴɢ ʙᴀɴ ᴛᴏ ᴅᴀᴛᴀʙᴀsᴇ...</i></b>")
        
        await ban_svc.ban_user(uid, reason)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗨𝗦𝗘𝗥 𝗕𝗔𝗡 𝗥𝗘𝗚𝗜𝗦𝗧𝗘𝗥𝗘𝗗</blockquote>\n\n"
            f"›› <b>ɪᴅ :</b> <code>{uid}</code>\n"
            f"›› <b>Nᴀᴍᴇ :</b> {_user_link(target_user)}\n"
            f"<blockquote>◍ <b>Rᴇᴀsᴏɴ :</b>\n<i>{reason}</i></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[ban] Banned {uid} by {message.from_user.id}")

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[ban] cmd_ban fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Unhandled Node Exception: {str(e)[:100]}")

@Client.on_message(filters.command("unban") & filters.private & _admin_or_sudo_filter())
async def cmd_unban(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "unban"):
        return

    # Fix 1: Wrap chat action
    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=1)
        target_user, err = await ban_svc.safe_resolve_user(client, message, args)
        
        if err:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/unban &lt;@username/userid&gt;</code>\n"
                "›› <b>Oʀ :</b> <code>Rᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴡɪᴛʜ /unban</code>\n\n"
                f"<blockquote>⚠️ {err}</blockquote>",
                reply_markup=close_btn()
            )

        uid = target_user["_id"]
        ban_status = target_user.get("ban_status") or {}
        
        if ban_status.get("is_banned") is not True:
            return await respond_with_error(temp, f"State Violation: User {uid} has no active ban records.")

        await safe_edit(temp, "<b><i>Rᴇᴠᴏᴋɪɴɢ sᴀɴᴄᴛɪᴏɴs...</i></b>")

        await ban_svc.unban_user(uid)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗨𝗦𝗘𝗥 𝗦𝗔𝗡𝗖𝗧𝗜𝗢𝗡𝗦 𝗟𝗜𝗙𝗧𝗘𝗗</blockquote>\n\n"
            f"›› <b>ɪᴅ :</b> <code>{uid}</code>\n"
            f"›› <b>Nᴀᴍᴇ :</b> {_user_link(target_user)}\n\n"
            f"<blockquote><i>Database constraints resolved. Access fully restored.</i></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[ban] Unbanned {uid} by {message.from_user.id}")

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[ban] cmd_unban fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Unhandled Node Exception: {str(e)[:100]}")

@Client.on_message(filters.command("banlist") & filters.private & _admin_or_sudo_filter())
async def cmd_banlist(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "banlist"):
        return

    # Fix 1: Wrap chat action
    await safe_chat_action(client, message.chat.id)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)
    try:
        await safe_edit(temp, "<b><i>Fᴇᴛᴄʜɪɴɢ...</i></b>")
        text, markup = await _banlist_page(0)
        
        # Fix 3: Wrap photo reply with retry
        @retry_on_flood
        async def _safe_photo_reply():
            await message.reply_photo(
                photo=IMG_BAN_HEADER,
                caption=text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
            
        await _safe_photo_reply()
        await temp.delete()
        
    except Exception as e:
        logger.error(f"[ban] banlist fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Failed initializing pointer: {str(e)[:50]}")

async def _banlist_page(page: int) -> tuple[str, InlineKeyboardMarkup]:
    total = await ban_svc.get_total_banned()

    if total == 0:
        return (
            "<blockquote expandable>[ ⓘ ] 𝗕𝗔𝗡 𝗟𝗜𝗦𝗧</blockquote>\n\n<i>No banned users found.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="close_msg")]]),
        )

    docs = await ban_svc.get_banlist_page(page)
    skip = page * 10

    lines = [
        f"<blockquote><b>✦ 𝗕𝗔𝗡 𝗟𝗜𝗦𝗧 𝗔𝗨𝗗𝗜𝗧</b></blockquote>\n"
        f"<blockquote>›› ᴛᴏᴛᴀʟ : <code>{total}</code> ᴜsᴇʀ(s)</blockquote>\n"
        "────────────────────\n"
    ]
    for i, doc in enumerate(docs, start=skip + 1):
        uid = doc["_id"]
        name = _e(doc.get("first_name") or doc.get("username") or str(uid))
        reason = _e(doc.get("ban_status", {}).get("ban_reason") or "—")
        lines.append(
            f"<blockquote><b>{i}.</b> <a href=\"tg://user?id={uid}\">{name}</a> [<code>{uid}</code>]\n"
            f"↳ <i>{reason}</i></blockquote>\n"
        )

    total_pages = max(1, (total + 9) // 10)
    return "\n".join(lines), _page_nav("banlist", page, total_pages)

@Client.on_callback_query(filters.regex(r"^banlist_p(\d+)$") & _admin_or_sudo_filter())
async def cb_banlist(client: Client, cq: CallbackQuery) -> None:
    try:
        page = int(cq.matches[0].group(1))
        text, markup = await _banlist_page(page)
        await safe_edit_callback(cq, text, markup=markup)
        await cq.answer()
    except Exception as e:
        logger.error(f"[ban] Pagination cb_ban fault: {e}")
        await cq.answer("Pointer transition fault.", show_alert=True)

logger.info("[✓] Ban Engine Module Hardened & Loaded")
