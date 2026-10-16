# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — FORCE SUBSCRIPTION ====================

import os
import asyncio
import random
import time
import logging
import html
from datetime import datetime, timedelta
from typing import Optional, List, Union
from functools import wraps

from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import (
    ChatAdminRequired,
    ChannelPrivate,
    InputUserDeactivated,
    PeerIdInvalid,
    UserIsBlocked,
    UserNotParticipant,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ChatMemberUpdated,
)

from config import Config, env_int
from helper.database import DARKXSIDE78
from helper.fsub import Rexy

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚𝗦 & 𝗦𝗧𝗔𝗧𝗘 |
# ═══════════════════════════════════════════════════════════

FSUB_LINK_EXPIRY = env_int("FSUB_LINK_EXPIRY", 0)
IMAGE_URL = Config.FORCE_PIC

ON_TXT  = "Eɴᴀʙʟᴇᴅ"
OFF_TXT = "Dɪsᴀʙʟᴇᴅ"

RFSUB_CMD_TXT = (
    "<blockquote><b>✦ 𝗥𝗘𝗤𝗨𝗘𝗦𝗧 𝗙𝗦𝗨𝗕 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n"
    "<blockquote><b>›› ʀᴇǫᴜᴇsᴛ ғsᴜʙ ᴍᴏᴅᴇ :</b> {req_mode}</blockquote>\n"
    "────────────────────\n"
    "<blockquote>≡ ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ʙᴜᴛᴛᴏɴs ᴛᴏ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs</blockquote>"
)

FSUB_LIST_PHOTO = "https://i.ibb.co/yFdkz3B3/photo-2026-05-02-18-57-27-7635371560638873632.jpg"
REQFSUB_PHOTO   = "https://i.ibb.co/fYp8FNPX/photo-2026-05-02-19-01-34-7635371659423121432.jpg"
ITEMS_PER_PAGE  = 5

LOADING_ANIMATIONS: list[str] = [
    "<b><i>ᴘʀᴏᴄᴇssɪɴɢ…</i></b>",
    "<b><i>ʟᴏᴀᴅɪɴɢ…</i></b>",
    "<b><i>ᴡᴏʀᴋɪɴɢ…</i></b>",
    "<b><i>ғᴇᴛᴄʜɪɴɢ…</i></b>",
]

_CMD_COOLDOWN = 3.0
_cmd_last: dict[tuple[int, str], float] = {}

# Concurrency limit for bulk API operations
FETCH_SEMAPHORE = asyncio.Semaphore(5)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for F-Sub API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[fsub] FloodWait in {func.__name__}: sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class FsubService:
    def __init__(self):
        self._channels_cache: Optional[list[int]] = None
        self._channels_ts: float = 0.0
        self._ttl: float = 30.0

    async def list_channels(self) -> list[int]:
        now = time.monotonic()
        if self._channels_cache is not None and (now - self._channels_ts < self._ttl):
            return self._channels_cache
        
        channels = await Rexy.show_channels()
        self._channels_cache = channels
        self._channels_ts = now
        return channels

    def invalidate(self):
        self._channels_cache = None
        self._channels_ts = 0.0

    async def get_request_fsub(self) -> bool:
        return await Rexy.get_request_fsub()

    async def set_request_fsub(self, enabled: bool) -> None:
        await Rexy.set_request_fsub(enabled)
        channels = await self.list_channels()
        mode_str = "on" if enabled else "off"
        for cid in channels:
            await Rexy.set_channel_mode(cid, mode_str)

    async def add_hold_request(self, chat_id: int, user_id: int) -> None:
        await Rexy.req_user(chat_id, user_id)

    async def remove_hold_request(self, chat_id: int, user_id: int) -> None:
        await Rexy.del_req_user(chat_id, user_id)

    async def add_channel(self, cid: int) -> bool:
        if await Rexy.channel_exist(cid):
            return False
        await Rexy.add_channel(cid)
        glob_mode = await self.get_request_fsub()
        await Rexy.set_channel_mode(cid, "on" if glob_mode else "off")
        self.invalidate()
        return True

    async def remove_channel(self, cid: int) -> bool:
        if not await Rexy.channel_exist(cid):
            return False
        await Rexy.rem_channel(cid)
        self.invalidate()
        return True

    async def is_subscribed(self, client: Client, user_id: int) -> bool:
        channels = await self.list_channels()
        return await check_user_subscription(client, user_id, channels)

    async def render_not_joined(self, client: Client, message: Message) -> None:
        user_id = message.from_user.id
        temp = await message.reply(await get_loading_text(), parse_mode=ParseMode.HTML)
        try:
            channels = await self.list_channels()
            req_mode = await self.get_request_fsub()
            buttons  = await build_fsub_buttons(client, channels, req_mode)
            try:
                await temp.delete()
            except:
                pass
            await render_fsub_ui(client, message, buttons)
        except Exception as e:
            logger.error(f"[fsub] Critical render fault: {e}", exc_info=True)
            await respond_with_error(temp, str(e))

    async def validate_channel(self, client: Client, cid: int) -> Optional[str]:
        """Validates if the bot has proper rights in the channel."""
        try:
            chat = await safe_get_chat(client, cid, use_cache=False)
            if not chat:
                return f"{cid} (inaccessible)"
            if chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP):
                return f"{cid} (not a channel/supergroup)"

            bot_member = await safe_get_chat_member(client, chat.id, "me")
            if not bot_member or bot_member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return f"{cid} (bot lacks admin privileges)"
            return None
        except ChatAdminRequired:
            return f"{cid} (bot lacks admin rights)"
        except ChannelPrivate:
            return f"{cid} (private — bot has no access)"
        except PeerIdInvalid:
            return f"{cid} (ID doesn't exist or inaccessible)"
        except Exception as e:
            return f"{cid} (error: {str(e)[:60]})"

fsub_svc = FsubService()

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗛𝗔𝗧 𝗗𝗔𝗧𝗔 𝗖𝗔𝗖𝗛𝗘 |
# ═══════════════════════════════════════════════════════════

class ChatDataCache:
    def __init__(self, max_age: int = 3600) -> None:
        self._cache: dict[int, object]     = {}
        self._timestamps: dict[int, float] = {}
        self._max_age = max_age
        self._lock = asyncio.Lock()

    @retry_on_flood
    async def _fetch_chat(self, client: Client, chat_id: int):
        async with FETCH_SEMAPHORE:
            return await client.get_chat(chat_id)

    async def get(self, chat_id: int, client: Client) -> Optional[object]:
        now = time.monotonic()
        async with self._lock:
            entry = self._cache.get(chat_id)
            ts    = self._timestamps.get(chat_id, 0.0)
            if entry is not None and now - ts < self._max_age:
                return entry

        try:
            data = await self._fetch_chat(client, chat_id)
        except Exception as e:
            logger.warning(f"[fsub] ChatDataCache: fetch failed for {chat_id}: {e}")
            async with self._lock:
                return self._cache.get(chat_id)  

        async with self._lock:
            self._cache[chat_id]      = data
            self._timestamps[chat_id] = now
        return data

    async def invalidate(self, chat_id: int) -> None:
        async with self._lock:
            self._cache.pop(chat_id, None)
            self._timestamps.pop(chat_id, None)

    async def cleanup(self) -> None:
        async with self._lock:
            now = time.monotonic()
            expired = [cid for cid, ts in self._timestamps.items() if now - ts > self._max_age]
            for cid in expired:
                self._cache.pop(cid, None)
                self._timestamps.pop(cid, None)
            if expired:
                logger.debug(f"[fsub] ChatDataCache cleaned {len(expired)} entries")

chat_cache = ChatDataCache()

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗖𝗞𝗚𝗥𝗢𝗨𝗡𝗗 𝗧𝗔𝗦𝗞𝗦 |
# ═══════════════════════════════════════════════════════════

async def _evict_old_cooldowns():
    """Cleans up the command cooldown dictionary."""
    while True:
        try:
            await asyncio.sleep(600)
            now = time.monotonic()
            expired = [k for k, v in _cmd_last.items() if now - v > 600]
            for k in expired:
                _cmd_last.pop(k, None)
            if expired:
                logger.debug(f"[fsub] Evicted {len(expired)} cooldown entries")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[fsub] Cooldown eviction error: {e}")

async def _periodic_cache_cleanup():
    """Periodically cleans up the chat data cache."""
    while True:
        try:
            await asyncio.sleep(300)
            await chat_cache.cleanup()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[fsub] Cache cleanup error: {e}")

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗔𝗙𝗘 𝗔𝗣𝗜 𝗪𝗥𝗔𝗣𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

@retry_on_flood
async def safe_chat_action(client: Client, chat_id: int, action: ChatAction = ChatAction.TYPING):
    await client.send_chat_action(chat_id, action)

@retry_on_flood
async def safe_reply_photo(client: Client, chat_id: int, photo: str, caption: str, markup=None):
    return await client.send_photo(chat_id, photo, caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)

@retry_on_flood
async def safe_create_invite_link(client: Client, chat_id: int, **kwargs):
    async with FETCH_SEMAPHORE:
        return await client.create_chat_invite_link(chat_id, **kwargs)

@retry_on_flood
async def safe_get_chat_member(client: Client, chat_id: int, user_id: Union[int, str]):
    async with FETCH_SEMAPHORE:
        try:
            return await client.get_chat_member(chat_id, user_id)
        except PeerIdInvalid:
            try:
                await client.get_chat(chat_id)
                return await client.get_chat_member(chat_id, user_id)
            except Exception as e:
                logger.error(f"[fsub] Peer resolution failed for member check in {chat_id}: {e}")
                raise
        except Exception:
            raise

@retry_on_flood
async def safe_approve_join_request(client: Client, chat_id: int, user_id: int):
    return await client.approve_chat_join_request(chat_id, user_id)

@retry_on_flood
async def _do_edit(msg: Message, text: str, **kwargs):
    return await msg.edit_text(text, **kwargs)

async def safe_edit(msg: Message, text: str, **kwargs) -> bool:
    """Edits message with FloodWait retry and reply fallback."""
    kwargs.setdefault("parse_mode", ParseMode.HTML)
    if "reply_markup" not in kwargs or kwargs["reply_markup"] is None:
        kwargs["reply_markup"] = close_btn()
    
    try:
        await _do_edit(msg, text, **kwargs)
        return True
    except Exception as e:
        logger.warning(f"[fsub] safe_edit failed (non-flood): {e}. Falling back to reply.")
        try:
            await msg.reply(text, **kwargs)
            return True
        except Exception as e2:
            logger.error(f"[fsub] safe_edit fallback failed: {e2}")
            return False

async def safe_get_chat(client: Client, chat_id: int, use_cache: bool = True) -> Optional[object]:
    try:
        if use_cache:
            return await chat_cache.get(chat_id, client)
        
        @retry_on_flood
        async def _fetch():
            async with FETCH_SEMAPHORE:
                try:
                    return await client.get_chat(chat_id)
                except PeerIdInvalid:
                    # In this context, we are already trying to fetch it.
                    # If it's a channel ID, Pyrogram might need it as a string or handled differently.
                    raise
        
        data = await _fetch()
        await chat_cache.invalidate(chat_id)
        return data
    except PeerIdInvalid:
        # Final attempt with direct call (bypassing semaphore/cache if needed)
        try:
            return await client.get_chat(chat_id)
        except Exception:
            return None
    except Exception as e:
        logger.warning(f"[fsub] safe_get_chat({chat_id}) fault: {e}")
        return None

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

async def get_loading_text() -> str:
    return random.choice(LOADING_ANIMATIONS)

def _on_cooldown(user_id: int, cmd: str) -> bool:
    key = (user_id, cmd)
    now = time.monotonic()
    if now - _cmd_last.get(key, 0.0) < _CMD_COOLDOWN:
        return True
    _cmd_last[key] = now
    return False

async def respond_with_error(message: Message, error_msg: str) -> None:
    text = (
        f"<blockquote expandable>[ ✗ ] 𝗘𝗥𝗥𝗢𝗥 𝗘𝗡𝗖𝗢𝗨𝗡𝗧𝗘𝗥𝗘𝗗</blockquote>\n\n"
        f"<b>Rᴇᴀsᴏɴ:</b> <code>{html.escape(error_msg)}</code>\n\n"
    )
    await safe_edit(message, text)

def close_btn() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="fsub_close")]])

def fsub_page_keyboard(page: int, total: int) -> InlineKeyboardMarkup:
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("‹‹ Pʀᴇᴠ", callback_data=f"fsub_prev_{page}"))
    if page < total:
        row.append(InlineKeyboardButton("Nᴇxᴛ ››", callback_data=f"fsub_next_{page}"))

    rows = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="fsub_close")])
    return InlineKeyboardMarkup(rows)

def is_sudo():
    async def filter_func(_, __, m: Message):
        if not m.from_user: return False
        if m.from_user.id == Config.OWNER_ID: return True
        return await DARKXSIDE78.is_admin(m.from_user.id)
    return filters.create(filter_func)

async def _fresh_link(bot: Client, chat_id: int, chat_obj: object) -> Optional[str]:
    if getattr(chat_obj, "username", None):
        return f"https://t.me/{chat_obj.username}"
    try:
        invite = await safe_create_invite_link(bot, chat_id)
        return invite.invite_link
    except:
        return None

async def _build_channel_line(bot: Client, cid: int) -> str:
    try:
        chat = await safe_get_chat(bot, cid)
        if not chat:
            return f"<blockquote>›› <code>{cid}</code> — <i>Uɴᴀᴠᴀɪʟᴀʙʟᴇ</i></blockquote>\n"
        link  = await _fresh_link(bot, cid, chat)
        title = getattr(chat, "title", str(cid))
        if link:
            return f"<blockquote>›› <a href='{link}'>{title}</a> [<code>{cid}</code>]</blockquote>\n"
        return f"<blockquote>›› {title} [<code>{cid}</code>]</blockquote>\n"
    except (PeerIdInvalid, ChannelPrivate, ChatAdminRequired):
        return f"<blockquote>›› <code>{cid}</code> — <i>Iɴᴀᴄᴄᴇssɪʙʟᴇ/Lᴀᴄᴋs Rɪɢʜᴛs</i></blockquote>\n"
    except Exception:
        return f"<blockquote>›› <code>{cid}</code> — <i>Eʀʀᴏʀ</i></blockquote>\n"


# ═══════════════════════════════════════════════════════════
#  Join Request / Member Handlers
# ═══════════════════════════════════════════════════════════

@Client.on_chat_member_updated()
async def handle_chat_members(client: Client, chat_member_updated: ChatMemberUpdated):
    try:
        chat_id = chat_member_updated.chat.id
        if await Rexy.channel_exist(chat_id):
            old_member = chat_member_updated.old_chat_member
            if not old_member:
                return
            if old_member.status == ChatMemberStatus.MEMBER:
                user_id = old_member.user.id
                if await Rexy.req_user_exist(chat_id, user_id):
                    await fsub_svc.remove_hold_request(chat_id, user_id)
    except Exception as e:
        logger.error(f"[fsub] Chat member update fault: {e}")

@Client.on_chat_join_request()
async def handle_join_request(client: Client, cjr) -> None:
    chat_id = cjr.chat.id
    user_id = cjr.from_user.id
    try:
        req_mode: bool = await fsub_svc.get_request_fsub()

        if req_mode:
            await fsub_svc.add_hold_request(chat_id, user_id)
            return

        await safe_approve_join_request(client, chat_id, user_id)
        await fsub_svc.remove_hold_request(chat_id, user_id)

    except (ChatAdminRequired, UserIsBlocked, InputUserDeactivated) as e:
        logger.warning(f"[fsub] Join request constrained for {user_id} in {chat_id}: {e}")
    except Exception as e:
        logger.error(f"[fsub] Join request error {chat_id}/{user_id}: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════
#  /addfsub
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("addfsub") & is_sudo())
async def add_fsub_cmd(bot: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "addfsub"):
        return

    await safe_chat_action(bot, m.chat.id)
    temp = await m.reply(await get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        if len(m.command) < 2:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/addfsub -100xxxxxxxxxx</code>\n\n"
                "<blockquote>◍ <b>Exᴀᴍᴘʟᴇs :</b>\n"
                "›› <code>/addfsub -1001234567890</code>\n"
                "›› <code>/addfsub -1001234567890 -1009876543210</code></blockquote>",
            )

        raw = m.command[1:]
        added: list[int]       = []
        exists_list: list[int] = []
        invalid: list[str]     = []
        total = len(raw)

        for idx, r in enumerate(raw, 1):
            if idx % 3 == 0 or idx == total or idx == 1:
                await safe_edit(
                    temp,
                    f"<blockquote expandable>⏣ Vᴇʀɪғʏɪɴɢ Cʜᴀɴɴᴇʟs</blockquote>\n\n"
                    f"›› <b>Pʀᴏɢʀᴇss :</b> <code>{idx}/{total}</code>\n"
                    f"›› <b>Cʜᴇᴄᴋɪɴɢ :</b> <code>{r}</code>",
                )

            try:
                cid = int(r)
            except ValueError:
                invalid.append(f"{r} (not a number)")
                continue

            err = await fsub_svc.validate_channel(bot, cid)
            if err:
                invalid.append(err)
                continue

            ok = await fsub_svc.add_channel(cid)
            (added if ok else exists_list).append(cid)

        lines: list[str] = ["\n"]
        if added:
            lines.append("<b>[ ✓ ] Fᴏʀᴄᴇ-Sᴜʙ Cʜᴀɴɴᴇʟs Aᴅᴅᴇᴅ</b>\n" + "\n".join(f"<blockquote>›› <code>{x}</code></blockquote>" for x in added))
            logger.info(f"[fsub] Channels configured: {added} by {m.from_user.id}")
        if exists_list:
            lines.append("<b>[ ⓘ ] Aʟʀᴇᴀᴅʏ Exɪsᴛs</b>\n" + "\n".join(f"<blockquote>›› <code>{x}</code> — already exists</blockquote>" for x in exists_list))
        if invalid:
            lines.append("<b>[ ✗ ] Fᴀɪʟᴇᴅ/Iɴᴠᴀʟɪᴅ</b>\n" + "\n".join(f"<blockquote>›› {x}</blockquote>" for x in invalid))

        await safe_edit(temp, "".join(lines))

    except Exception as e:
        logger.error(f"[fsub] /addfsub fault: {e}", exc_info=True)
        await respond_with_error(temp, str(e))


# ═══════════════════════════════════════════════════════════
#  /remfsub (or delfsub)
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command(["remfsub", "delfsub"]) & is_sudo())
async def rem_fsub_cmd(bot: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "remfsub"):
        return

    await safe_chat_action(bot, m.chat.id)
    temp = await m.reply(await get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        if len(m.command) < 2:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/remfsub &lt;id | all&gt;</code>\n\n"
                "<blockquote>◍ <b>Exᴀᴍᴘʟᴇs :</b>\n"
                "›› <code>/remfsub -1001234567890</code>\n"
                "›› <code>/remfsub all</code></blockquote>",
            )

        if m.command[1].lower() == "all":
            channels = await fsub_svc.list_channels()
            if not channels:
                return await safe_edit(
                    temp,
                    "<blockquote expandable>[ ⓘ ] Nᴏ Cʜᴀɴɴᴇʟs</blockquote>\n"
                    "<b><blockquote>Nᴏ ғᴏʀᴄᴇ-sᴜʙ ᴄʜᴀɴɴᴇʟs ᴛᴏ ʀᴇᴍᴏᴠᴇ.</blockquote></b>",
                )

            total = len(channels)
            for idx, ch_id in enumerate(channels, 1):
                await fsub_svc.remove_channel(ch_id)
                if idx % 5 == 0 or idx == total:
                    filled = "█" * min(int((idx/total)*20), 20)
                    empty  = "░" * (20 - len(filled))
                    await safe_edit(
                        temp,
                        f"<blockquote expandable>⏣ Rᴇᴍᴏᴠɪɴɢ Cʜᴀɴɴᴇʟs</blockquote>\n"
                        f"<b>Pʀᴏɢʀᴇss :</b> <code>{idx}/{total}</code> {filled}{empty}",
                    )

            logger.info(f"[fsub] All channels wiped by {m.from_user.id}")
            return await safe_edit(
                temp,
                f"<blockquote expandable>[ ✓ ] Aʟʟ Cʜᴀɴɴᴇʟs Rᴇᴍᴏᴠᴇᴅ</blockquote>\n"
                f"<blockquote><b>Tᴏᴛᴀʟ Rᴇᴍᴏᴠᴇᴅ :</b> <code>{total}</code> ᴄʜᴀɴɴᴇʟ(s)</blockquote>",
            )

        raw = m.command[1:]
        removed: list[int]  = []
        notfound: list[int] = []
        invalid: list[str]  = []
        total = len(raw)

        for idx, r in enumerate(raw, 1):
            if idx % 3 == 0 or idx == total or idx == 1:
                await safe_edit(
                    temp,
                    f"<blockquote expandable>⏣ Rᴇᴍᴏᴠɪɴɢ Cʜᴀɴɴᴇʟs</blockquote>\n"
                    f"<blockquote><b>Pʀᴏɢʀᴇss :</b> <code>{idx}/{total}</code></blockquote>",
                )

            try:
                cid = int(r)
            except ValueError:
                invalid.append(r)
                continue

            ok = await fsub_svc.remove_channel(cid)
            (removed if ok else notfound).append(cid)

        lines: list[str] = ["\n"]
        if removed:
            lines.append("<b>[ ✓ ] Rᴇᴍᴏᴠᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ</b>\n" + "\n".join(f"<blockquote>›› <code>{x}</code></blockquote>" for x in removed))
            logger.info(f"[fsub] Channels removed: {removed} by {m.from_user.id}")
        if notfound:
            lines.append("<b>[ ✗ ] Nᴏᴛ Fᴏᴜɴᴅ ɪɴ Lɪsᴛ</b>\n" + "\n".join(f"<blockquote>›› <code>{x}</code> — not registered</blockquote>" for x in notfound))
        if invalid:
            lines.append("<b>[ ✗ ] Iɴᴠᴀʟɪᴅ ɪds</b>\n" + "\n".join(f"<blockquote>›› <code>{x}</code> — parsing failed</blockquote>" for x in invalid))

        await safe_edit(temp, "".join(lines))

    except Exception as e:
        logger.error(f"[fsub] /remfsub fault: {e}", exc_info=True)
        await respond_with_error(temp, str(e))


# ═══════════════════════════════════════════════════════════
#  /listfsub (or /channels)
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command(["listfsub", "channels"]) & is_sudo())
async def list_fsub_cmd(bot: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "listfsub"):
        return

    temp = await m.reply(await get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        channels = await fsub_svc.list_channels()

        if not channels:
            return await safe_edit(
                temp,
                "<blockquote expandable>[ ⓘ ] Nᴏ Cʜᴀɴɴᴇʟs</blockquote>\n"
                "<b><blockquote>Nᴏ ғᴏʀᴄᴇ-sᴜʙ ᴄʜᴀɴɴᴇʟs ᴄᴏɴғɪɢᴜʀᴇᴅ.</blockquote></b>\n\n"
                "<blockquote>◍ Usᴇ <code>/addfsub</code> ᴛᴏ ᴀᴅᴅ ᴄʜᴀɴɴᴇʟs.</blockquote>",
            )

        page       = 1
        per        = ITEMS_PER_PAGE
        total_page = (len(channels) + per - 1) // per
        slice_ids  = channels[:per]

        await safe_edit(
            temp,
            f"<blockquote expandable>⏣ Lᴏᴀᴅɪɴɢ Cʜᴀɴɴᴇʟs</blockquote>\n"
            f"<blockquote>Fᴇᴛᴄʜɪɴɢ <code>{len(slice_ids)}</code> ᴄʜᴀɴɴᴇʟ(s)…</blockquote>",
        )

        lines_list: list[str] = list(
            await asyncio.gather(*(_build_channel_line(bot, cid) for cid in slice_ids))
        )

        caption = (
            f"<blockquote><b>✦ 𝗙𝗢𝗥𝗖𝗘-𝗦𝗨𝗕 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 𝗟𝗜𝗦𝗧</b></blockquote>\n"
            f"<blockquote>›› ᴘᴀɢᴇ <code>{page}/{total_page}</code></blockquote>\n"
            "────────────────────\n"
            "<blockquote expandable>❐ Fᴏʀᴄᴇ-Sᴜʙ Cʜᴀɴɴᴇʟs</blockquote>\n"
            + "".join(lines_list)
        )

        await temp.delete()
        
        await safe_reply_photo(
            bot, m.chat.id, 
            FSUB_LIST_PHOTO, caption, 
            fsub_page_keyboard(page, total_page)
        )

    except Exception as e:
        logger.error(f"[fsub] listfsub fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Eʀʀᴏʀ ʟɪsᴛɪɴɢ: {e}")

@Client.on_callback_query(filters.regex(r"^fsub_(prev|next)_(\d+)$"))
async def fsub_page_handler(bot: Client, q: CallbackQuery) -> None:
    await q.answer()
    try:
        parts     = q.data.split("_")
        direction = parts[1]
        page      = int(parts[2])

        channels = await fsub_svc.list_channels()
        per        = ITEMS_PER_PAGE
        total_page = (len(channels) + per - 1) // per

        page = max(1, page - 1) if direction == "prev" else min(total_page, page + 1)
        start     = (page - 1) * per
        slice_ids = channels[start: start + per]

        lines_list: list[str] = list(
            await asyncio.gather(*(_build_channel_line(bot, cid) for cid in slice_ids))
        )

        caption = (
            f"<blockquote><b>✦ 𝗙𝗢𝗥𝗖𝗘-𝗦𝗨𝗕 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 𝗟𝗜𝗦𝗧</b></blockquote>\n"
            f"<blockquote>›› ᴘᴀɢᴇ <code>{page}/{total_page}</code></blockquote>\n"
            "────────────────────\n"
            "<blockquote expandable>❐ Fᴏʀᴄᴇ-Sᴜʙ Cʜᴀɴɴᴇʟs</blockquote>\n"
            + "".join(lines_list)
        )

        @retry_on_flood
        async def _do_edit_caption():
            await q.message.edit_caption(
                caption=caption,
                reply_markup=fsub_page_keyboard(page, total_page),
                parse_mode=ParseMode.HTML,
            )
        
        try:
            await _do_edit_caption()
        except Exception as e:
            logger.warning(f"[fsub] Pagination edit_caption failed: {e}")

    except Exception as e:
        logger.error(f"[fsub] Pagination callback error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════
#  Close Callback
# ═══════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex(r"^fsub_close$"))
async def close_fsub(_: Client, q: CallbackQuery) -> None:
    await q.answer()
    try:
        await q.message.delete()
    except Exception as e:
        logger.warning(f"[fsub] close_fsub delete failed: {e}")


# ═══════════════════════════════════════════════════════════
#  /reqfsub
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("reqfsub") & is_sudo())
async def handle_reqFsub(client: Client, message: Message) -> None:
    if _on_cooldown(message.from_user.id, "reqfsub"):
        return

    await safe_chat_action(client, message.chat.id)
    try:
        enabled: bool = await fsub_svc.get_request_fsub()
        texting = ON_TXT if enabled else OFF_TXT

        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"{'◍' if enabled else '○'} Oɴ", callback_data="reqfsub_on"),
                InlineKeyboardButton(f"{'◍' if not enabled else '○'} Oғғ", callback_data="reqfsub_off"),
            ]
        ])

        await safe_reply_photo(
            client, message.chat.id, 
            REQFSUB_PHOTO, 
            RFSUB_CMD_TXT.format(req_mode=texting), 
            buttons
        )

    except Exception as e:
        logger.error(f"[fsub] reqfsub UI fault: {e}", exc_info=True)

@Client.on_callback_query(filters.regex(r"^reqfsub_on$"))
async def reqfsub_on(_: Client, query: CallbackQuery) -> None:
    try:
        await fsub_svc.set_request_fsub(True)
        await query.answer("Rᴇǫᴜᴇsᴛ Fsᴜʙ Eɴᴀʙʟᴇᴅ", show_alert=True)
        
        @retry_on_flood
        async def _do_edit():
            await query.message.edit_caption(
                caption=RFSUB_CMD_TXT.format(req_mode=ON_TXT),
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("◍ Oɴ",  callback_data="reqfsub_on"),
                        InlineKeyboardButton("○ Oғғ", callback_data="reqfsub_off"),
                    ]
                ])
            )
        await _do_edit()
        logger.info(f"[fsub] REQ_FSUB Enabled by {query.from_user.id}")
    except Exception as e:
        logger.warning(f"[fsub] reqfsub_on callback error: {e}")

@Client.on_callback_query(filters.regex(r"^reqfsub_off$"))
async def reqfsub_off(_: Client, query: CallbackQuery) -> None:
    try:
        await fsub_svc.set_request_fsub(False)
        await query.answer("Rᴇǫᴜᴇsᴛ Fsᴜʙ Dɪsᴀʙʟᴇᴅ", show_alert=True)
        
        @retry_on_flood
        async def _do_edit():
            await query.message.edit_caption(
                caption=RFSUB_CMD_TXT.format(req_mode=OFF_TXT),
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("○ Oɴ",  callback_data="reqfsub_on"),
                        InlineKeyboardButton("◍ Oғғ", callback_data="reqfsub_off"),
                    ]
                ])
            )
        await _do_edit()
        logger.info(f"[fsub] REQ_FSUB Disabled by {query.from_user.id}")
    except Exception as e:
        logger.warning(f"[fsub] reqfsub_off callback error: {e}")


# ═══════════════════════════════════════════════════════════
#  Subscription Sub-routines
# ═══════════════════════════════════════════════════════════

async def is_supervised(client: Client, user_id: int, channel_id: int) -> bool:
    try:
        member = await safe_get_chat_member(client, channel_id, user_id)
        if not member: return False
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER}
    except UserNotParticipant:
        mode = await Rexy.get_channel_mode(channel_id)
        if mode == "on":
            return await Rexy.req_user_exist(channel_id, user_id)
        return False
    except Exception as e:
        logger.error(f"[fsub] is_supervised failure for {channel_id}: {e}")
        return False

is_sub = is_supervised

async def check_user_subscription(client: Client, user_id: int, channels: list[int]) -> bool:
    """Verifies if user has satisfied fsub requirements."""
    if not channels:
        return True
    if user_id == Config.OWNER_ID:
        return True

    for cid in channels:
        if not await is_sub(client, user_id, cid):
            mode = await Rexy.get_channel_mode(cid)
            if mode == "on":
                await asyncio.sleep(2)  # Wait for Telegram latency on req_fsub
                if await is_sub(client, user_id, cid):
                    continue
            return False
    return True

async def build_fsub_buttons(client: Client, channels: list[int], req_mode: bool) -> list[list[InlineKeyboardButton]]:
    """Builds invites and UI rows securely."""
    buttons = []
    for chat_id in channels:
        try:
            data = await safe_get_chat(client, chat_id)
            if not data:
                continue

            name = data.title
            link = None

            if req_mode and not getattr(data, "username", None):
                expire = datetime.utcnow() + timedelta(seconds=FSUB_LINK_EXPIRY) if FSUB_LINK_EXPIRY else None
                invite = await safe_create_invite_link(
                    client, chat_id=chat_id, creates_join_request=True, expire_date=expire
                )
                link = invite.invite_link
            else:
                if getattr(data, "username", None):
                    link = f"https://t.me/{data.username}"
                else:
                    expire = datetime.utcnow() + timedelta(seconds=FSUB_LINK_EXPIRY) if FSUB_LINK_EXPIRY else None
                    invite = await safe_create_invite_link(
                        client, chat_id=chat_id, expire_date=expire
                    )
                    link = invite.invite_link

            if link:
                buttons.append([InlineKeyboardButton(text=name, url=link)])

        except (ChatAdminRequired, ChannelPrivate):
            logger.warning(f"[fsub] Missing rights or private channel constraint for {chat_id}")
        except Exception as e:
            logger.warning(f"[fsub] Button gen fail {chat_id}: {e}")
            
    return buttons

async def render_fsub_ui(client: Client, message: Message, buttons: list[list[InlineKeyboardButton]]) -> None:
    """Renders the ultimate blocked screen."""
    force_message = (
        f"<b><blockquote>◈ Hᴇʏ </b>{message.from_user.mention} ×</b></blockquote>\n"
        f"<b><blockquote>›› ‼️ ʟᴏᴏᴋs ʟɪᴋᴇ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ sᴜʙsᴄʀɪʙᴇᴅ ᴛᴏ ᴏᴜʀ ᴄʜᴀɴɴᴇʟs ʏᴇᴛ, sᴜʙsᴄʀɪʙᴇ ɴᴏᴡ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ᴀᴄᴄᴇss</b></blockquote>"
    )
    markup = InlineKeyboardMarkup(buttons)
    try:
        await safe_reply_photo(
            client, message.from_user.id, 
            IMAGE_URL, force_message, markup
        )
    except Exception as e:
        logger.warning(f"[fsub] Fallback render to text mode: {e}")
        @retry_on_flood
        async def _do_reply():
            await client.send_message(message.from_user.id, force_message, reply_markup=markup, parse_mode=ParseMode.HTML)
        await _do_reply()

async def init(*args, **kwargs):
    """Fix 6: Starts background maintenance tasks."""
    loop = asyncio.get_event_loop()
    loop.create_task(_evict_old_cooldowns())
    loop.create_task(_periodic_cache_cleanup())
    logger.info("[fsub] Background maintenance tasks started.")

__all__ = ["chat_cache", "is_sub", "fsub_svc", "init"]

logger.info("[✓] Force Subscription Module Hardened & Loaded")
