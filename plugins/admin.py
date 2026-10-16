# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — ADMIN COMMANDS ====================

import html
import logging
import random
import time
import asyncio
from typing import Optional, List, Union
from functools import wraps

from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import FloodWait
from pyrogram.errors import MessageNotModified
from pyrogram.errors.exceptions.bad_request_400 import (
    InputUserDeactivated,
    PeerIdInvalid,
    UserIsBlocked,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    InputMedia,
)

from config import Config
from helper.database import DARKXSIDE78
from helper.utils import metrics

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚𝗦 & 𝗦𝗧𝗔𝗧𝗘 |
# ═══════════════════════════════════════════════════════════

ADMIN_LIST_PHOTO = "https://i.ibb.co/fd1jTGbh/photo-2026-05-02-19-22-34-7635377444744069144.jpg"

LOADING_ANIMATIONS: list[str] = [
    "<b><i>ᴘʀᴏᴄᴇssɪɴɢ…</i></b>",
    "<b><i>ʟᴏᴀᴅɪɴɢ…</i></b>",
    "<b><i>ᴡᴏʀᴋɪɴɢ…</i></b>",
    "<b><i>ғᴇᴛᴄʜɪɴɢ…</i></b>",
]

_CMD_COOLDOWN = 3.0
_cmd_last_used: dict[tuple[int, str], float] = {}

# Concurrency limit for bulk API operations
FETCH_SEMAPHORE = asyncio.Semaphore(5)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for Pyrogram API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[admin] FloodWait detected: sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class AdminService:
    def __init__(self):
        self._cache: Optional[list[int]] = None
        self._ts: float = 0.0
        self._ttl: float = 60.0

    async def get_admins(self) -> list[int]:
        now = time.monotonic()
        if self._cache is not None and (now - self._ts < self._ttl):
            return self._cache

        admins = await DARKXSIDE78.get_admin_ids()
        self._cache = admins
        self._ts = now
        return admins

    def invalidate(self):
        self._cache = None
        self._ts = 0.0

    async def add(self, uid: int, actor_id: int, username: str, first_name: str):
        await DARKXSIDE78.add_admin(uid, actor_id, username, first_name)
        self.invalidate()

    async def remove(self, uid: int):
        await DARKXSIDE78.remove_admin(uid)
        self.invalidate()

    async def is_admin(self, uid: int) -> bool:
        admins = await self.get_admins()
        return uid in admins

admin_svc = AdminService()

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗦𝗘𝗥 𝗗𝗔𝗧𝗔 𝗖𝗔𝗖𝗛𝗘 |
# ═══════════════════════════════════════════════════════════

class UserDataCache:
    def __init__(self, max_age: int = 3600) -> None:
        self._cache: dict[int, object]     = {}
        self._timestamps: dict[int, float] = {}
        self._max_age = max_age
        self._lock = asyncio.Lock()

    @retry_on_flood
    async def _fetch_user(self, client: Client, user_id: int):
        async with FETCH_SEMAPHORE:
            return await client.get_users(user_id)

    async def get(self, user_id: int, client: Client) -> Optional[object]:
        now = time.monotonic()

        async with self._lock:
            entry = self._cache.get(user_id)
            ts    = self._timestamps.get(user_id, 0.0)
            if entry is not None and now - ts < self._max_age:
                return entry

        try:
            data = await self._fetch_user(client, user_id)
        except Exception as e:
            logger.warning(f"[admin] UserDataCache fetch failed for {user_id}: {e}")
            async with self._lock:
                return self._cache.get(user_id)

        async with self._lock:
            self._cache[user_id]      = data
            self._timestamps[user_id] = time.monotonic()
        return data

    async def invalidate(self, user_id: int) -> None:
        async with self._lock:
            self._cache.pop(user_id, None)
            self._timestamps.pop(user_id, None)

    async def cleanup(self) -> None:
        async with self._lock:
            now     = time.monotonic()
            expired = [uid for uid, ts in self._timestamps.items() if now - ts > self._max_age]
            for uid in expired:
                self._cache.pop(uid, None)
                self._timestamps.pop(uid, None)
            if expired:
                logger.debug(f"[admin] UserDataCache cleaned {len(expired)} entries")

user_cache = UserDataCache()

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗖𝗞𝗚𝗥𝗢𝗨𝗡𝗗 𝗧𝗔𝗦𝗞𝗦 |
# ═══════════════════════════════════════════════════════════

async def _evict_old_cooldowns():
    """Cleans up the command cooldown dictionary to prevent unbounded memory growth."""
    while True:
        try:
            await asyncio.sleep(600) # Every 10 minutes
            now = time.monotonic()
            expired = [k for k, v in _cmd_last_used.items() if now - v > 600]
            for k in expired:
                _cmd_last_used.pop(k, None)
            if expired:
                logger.debug(f"[admin] Evicted {len(expired)} cooldown entries")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[admin] Cooldown eviction error: {e}")

async def _periodic_cache_cleanup():
    """Periodically cleans up the user data cache."""
    while True:
        try:
            await asyncio.sleep(300) # Every 5 minutes
            await user_cache.cleanup()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[admin] Cache cleanup error: {e}")

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

def get_loading_text() -> str:
    return random.choice(LOADING_ANIMATIONS)

@retry_on_flood
async def _do_edit(msg: Message, text: str, **kwargs):
    """Internal helper to execute edit_text/edit_caption with FloodWait retries."""
    if msg.photo or msg.video or msg.document or msg.animation:
        # If it's media, we edit the caption
        return await msg.edit_caption(caption=text, **kwargs)
    return await msg.edit_text(text, **kwargs)

async def safe_edit(msg: Message, text: str, **kwargs) -> bool:
    """Edits a message with automatic FloodWait retries and a reply fallback."""
    try:
        await _do_edit(msg, text, **kwargs)
        return True
    except Exception as e:
        logger.warning(f"[admin] safe_edit failed (non-flood): {e}. Falling back to reply.")
        try:
            # Fallback: reply to the trigger or just send in chat
            await msg.reply(text, **kwargs)
            return True
        except Exception as e2:
            logger.error(f"[admin] safe_edit fallback failed: {e2}")
            return False

async def respond_with_error(message: Message, error_msg: str) -> None:
    text = (
        f"<blockquote expandable>[ ✗ ] 𝗘𝗥𝗥𝗢𝗥 𝗘𝗡𝗖𝗢𝗨𝗡𝗧𝗘𝗥𝗘𝗗</blockquote>\n\n"
        f"<b>Rᴇᴀsᴏɴ:</b> <code>{html.escape(error_msg)}</code>\n\n"
        f"<i>ᴄᴏɴᴛᴀᴄᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ ɪғ ᴛʜɪs ᴘᴇʀsɪsᴛs.</i>"
    )
    await safe_edit(message, text, reply_markup=close_btn(), parse_mode=ParseMode.HTML)

@retry_on_flood
async def safe_get_users(client: Client, user_ids: Union[int, List[int]]) -> Union[Optional[object], List[object]]:
    """Fetches users in batch with FloodWait protection and concurrency limiting."""
    async with FETCH_SEMAPHORE:
        try:
            return await client.get_users(user_ids)
        except Exception as e:
            logger.warning(f"[admin] safe_get_users({user_ids}): {e}")
            return [] if isinstance(user_ids, list) else None

# Alias for singular usage compatibility
safe_get_user = safe_get_users

@retry_on_flood
async def safe_send_message(client: Client, chat_id: Union[int, str], text: str, **kwargs) -> Optional[Message]:
    """Sends a message with automatic FloodWait retries and peer resolution."""
    try:
        return await client.send_message(chat_id=chat_id, text=text, **kwargs)
    except PeerIdInvalid:
        # Force resolution for integers
        try:
            if isinstance(chat_id, int) or (isinstance(chat_id, str) and chat_id.startswith("-100")):
                await client.get_chat(chat_id)
                return await client.send_message(chat_id=chat_id, text=text, **kwargs)
        except Exception as e:
            logger.error(f"[admin] Failed to resolve chat {chat_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"[admin] safe_send_message to {chat_id} failed: {e}")
        return None

@retry_on_flood
async def safe_edit_media(client: Client, chat_id: Union[int, str], message_id: int, media: InputMedia, **kwargs) -> bool:
    """Edits a message media with automatic FloodWait retries."""
    try:
        await client.edit_message_media(chat_id=chat_id, message_id=message_id, media=media, **kwargs)
        return True
    except MessageNotModified:
        return True
    except Exception as e:
        logger.error(f"[admin] safe_edit_media failed: {e}")
        return False

async def safe_delete_message(message: Union[Message, CallbackQuery]) -> bool:
    """Safely delete a message without crashing on errors."""
    try:
        if isinstance(message, CallbackQuery):
            await message.message.delete()
        else:
            await message.delete()
        return True
    except Exception:
        return False

def close_btn() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖️", callback_data="close_msg")]])

def _on_cooldown(user_id: int, cmd: str) -> bool:
    key = (user_id, cmd)
    now = time.monotonic()
    last = _cmd_last_used.get(key, 0.0)
    if now - last < _CMD_COOLDOWN:
        return True
    _cmd_last_used[key] = now
    return False

def _sudo_filter():
    return filters.create(lambda _, __, m: bool(m.from_user) and m.from_user.id == Config.OWNER_ID)

def _admin_or_sudo_filter():
    async def filter_func(_, __, m: Message):
        if not m.from_user: return False
        if m.from_user.id == Config.OWNER_ID: return True
        return await admin_svc.is_admin(m.from_user.id)
    return filters.create(filter_func)

# ═══════════════════════════════════════════════════════════
#  /add_admin
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command(["add_admin", "addadmin"]) & _sudo_filter())
async def addadmin(client: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "add_admin"):
        return

    await m.reply_chat_action(ChatAction.TYPING)
    temp = await m.reply(get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        if len(m.command) < 2:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/add_admin &lt;id1&gt; [id2 …]</code>\n\n"
                "<blockquote>◍ <b>Exᴀᴍᴘʟᴇs :</b>\n"
                "›› <code>/add_admin 123456789</code>\n"
                "›› <code>/add_admin 123456789 987654321</code></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=close_btn(),
            )

        # Fix 7: Input validation and filtering
        raw_ids = []
        for x in m.command[1:]:
            if x.isdigit():
                uid = int(x)
                if uid > 0: # Fix 7: Validate > 0
                    raw_ids.append(uid)
        
        if not raw_ids:
            return await safe_edit(
                temp,
                "<blockquote expandable>[ ✗ ] Iɴᴠᴀʟɪᴅ Iɴᴘᴜᴛ</blockquote>\n"
                "<b><blockquote>Pʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴠᴀʟɪᴅ ᴘᴏsɪᴛɪᴠᴇ ɴᴜᴍᴇʀɪᴄ ᴜsᴇʀ IDs.</blockquote></b>",
                parse_mode=ParseMode.HTML,
                reply_markup=close_btn(),
            )

        # Fix 6: Filter duplicates, owner, and already admins
        unique_ids = list(set(raw_ids))
        to_fetch = []
        already = []
        
        for uid in unique_ids:
            if uid == Config.OWNER_ID or await admin_svc.is_admin(uid):
                already.append(uid)
            else:
                to_fetch.append(uid)

        added: list[int]   = []
        invalid: list[str] = []

        if to_fetch:
            # Fix 3: Batch fetch users
            await safe_edit(
                temp,
                f"<blockquote expandable>⏣ Vᴇʀɪғʏɪɴɢ Usᴇʀs</blockquote>\n\n"
                f"›› <b>Fᴇᴛᴄʜɪɴɢ :</b> <code>{len(to_fetch)}</code> ᴜsᴇʀs…",
                parse_mode=ParseMode.HTML,
            )
            
            users_list = await safe_get_users(client, to_fetch)
            if not isinstance(users_list, list): users_list = [users_list] if users_list else []
            
            found_ids = {u.id: u for u in users_list if u}
            
            for uid in to_fetch:
                user = found_ids.get(uid)
                if not user:
                    invalid.append(f"{uid} (not found/inaccessible)")
                    continue
                
                try:
                    username = getattr(user, "username", "") or ""
                    first_name = getattr(user, "first_name", "") or ""
                    
                    await admin_svc.add(uid, m.from_user.id, username, first_name)
                    added.append(uid)
                    logger.info(f"[admin] Admin Added: {uid} by {m.from_user.id}")
                except Exception as e:
                    invalid.append(f"{uid} (error: {str(e)[:40]})")

        # UI Generation
        lines: list[str] = ["\n"]
        if added:
            links = "\n".join(f"<blockquote>›› <a href='tg://openmessage?user_id={uid}'>{uid}</a></blockquote>" for uid in added)
            lines.append("<b>[ ✓ ] Aᴅᴍɪɴs Aᴅᴅᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ</b>\n" + links + "\n")
        if already:
            links = "\n".join(f"<blockquote>›› <code>{uid}</code> — already admin</blockquote>" for uid in already)
            lines.append("<b>[ ⓘ ] Aʟʀᴇᴀᴅʏ Aᴅᴍɪɴs</b>\n" + links + "\n")
        if invalid:
            links = "\n".join(f"<blockquote>›› <code>{x}</code></blockquote>" for x in invalid)
            lines.append("<b>[ ✗ ] Iɴᴠᴀʟɪᴅ / Sᴋɪᴘᴘᴇᴅ</b>\n" + links + "\n")

        await safe_edit(temp, "".join(lines), parse_mode=ParseMode.HTML, reply_markup=close_btn())
        if added: await m.reply_chat_action(ChatAction.CHOOSE_STICKER)

    except Exception as e:
        logger.error(f"[admin] /add_admin fault: {e}", exc_info=True)
        await respond_with_error(temp, f"Unexpected Fault: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════
#  /remove_admin
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command(["remove_admin", "remadmin"]) & _sudo_filter())
async def removeadmin(client: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "remove_admin"):
        return

    await m.reply_chat_action(ChatAction.TYPING)
    temp = await m.reply(get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        args = m.command[1:]

        if not args:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/remove_admin &lt;id | all&gt;</code>\n\n"
                "<blockquote expandable>◍ <b>Exᴀᴍᴘʟᴇs :</b>\n"
                "›› <code>/remove_admin 123456789</code>\n"
                "›› <code>/remove_admin all</code></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=close_btn(),
            )

        if args[0].lower() == "all":
            all_admins = await admin_svc.get_admins()
            if not all_admins:
                return await safe_edit(temp, "<b>[ ⓘ ] Nᴏ Aᴅᴍɪɴs ғᴏᴜɴᴅ ᴛᴏ ʀᴇᴍᴏᴠᴇ.</b>", parse_mode=ParseMode.HTML)

            total = len(all_admins)
            for idx, uid in enumerate(all_admins, 1):
                await admin_svc.remove(uid)
                if idx % 5 == 0 or idx == total:
                    await safe_edit(temp, f"<b>Rᴇᴍᴏᴠɪɴɢ Aᴅᴍɪɴs :</b> <code>{idx}/{total}</code>", parse_mode=ParseMode.HTML)
            
            return await safe_edit(
                temp,
                f"<b>[ ✓ ] Aʟʟ Aᴅᴍɪɴs Rᴇᴍᴏᴠᴇᴅ</b>\n<blockquote><b>Tᴏᴛᴀʟ :</b> <code>{total}</code></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=close_btn(),
            )

        # Single/Multiple removal
        removed = []
        notfound = []
        for x in args:
            if x.isdigit():
                uid = int(x)
                if await admin_svc.is_admin(uid):
                    await admin_svc.remove(uid)
                    removed.append(uid)
                else:
                    notfound.append(uid)

        lines = ["\n"]
        if removed:
            links = "\n".join(f"<blockquote>›› <code>{uid}</code></blockquote>" for uid in removed)
            lines.append("<b>[ ✓ ] Rᴇᴍᴏᴠᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ</b>\n" + links + "\n")
        if notfound:
            links = "\n".join(f"<blockquote>›› <code>{uid}</code></blockquote>" for uid in notfound)
            lines.append("<b>[ ✗ ] Nᴏᴛ Iɴ Lɪsᴛ</b>\n" + links + "\n")

        await safe_edit(temp, "".join(lines), parse_mode=ParseMode.HTML, reply_markup=close_btn())

    except Exception as e:
        logger.error(f"[admin] /remove_admin fault: {e}")
        await respond_with_error(temp, f"Unexpected Fault: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════
#  /metrics
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("check_channels") & _admin_or_sudo_filter())
async def check_channels_cmd(client: Client, m: Message) -> None:
    temp = await m.reply("<b>🔍 Cʜᴇᴄᴋɪɴɢ ᴄʜᴀɴɴᴇʟ ᴀᴄᴄᴇss…</b>")
    
    report = "<b>📊 Cʜᴀɴɴᴇʟ Dɪᴀɢɴᴏsᴛɪᴄs</b>\n\n"
    
    for label, cid in [("LOG", Config.LOG_CHANNEL), ("DUMP", Config.DUMP_CHANNEL)]:
        report += f"◍ <b>{label}:</b> <code>{cid}</code>\n"
        try:
            chat = await client.get_chat(cid)
            report += f"  ├ <b>Sᴛᴀᴛᴜs:</b> ✅ Aᴄᴄᴇssɪʙʟᴇ\n"
            report += f"  ├ <b>Tɪᴛʟᴇ:</b> {chat.title}\n"
            report += f"  └ <b>Tʏᴘᴇ:</b> {chat.type}\n\n"
        except PeerIdInvalid:
            report += f"  └ <b>Sᴛᴀᴛᴜs:</b> ❌ <code>PeerIdInvalid</code> (Try resolving manually or check ID)\n\n"
        except Exception as e:
            report += f"  └ <b>Sᴛᴀᴛᴜs:</b> ❌ ᴇʀʀᴏʀ: <code>{e}</code>\n\n"
            
    await temp.edit(report, parse_mode=ParseMode.HTML)

@Client.on_message(filters.command(["metrics"]) & _admin_or_sudo_filter())
async def show_metrics_cmd(_: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "metrics"):
        await m.reply_text("<b><blockquote>⏳ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ...</blockquote></b>", parse_mode=ParseMode.HTML)
        return
    report = metrics.get_report()
    await m.reply_text(report, parse_mode=ParseMode.HTML, reply_markup=close_btn())


# ═══════════════════════════════════════════════════════════
#  /admins_list
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command(["admins_list", "adminlist"]) & _admin_or_sudo_filter())
async def listadmin(client: Client, m: Message) -> None:
    if _on_cooldown(m.from_user.id, "admins_list"):
        return

    temp = await m.reply(get_loading_text(), parse_mode=ParseMode.HTML)

    try:
        admin_ids = await admin_svc.get_admins()
        if not admin_ids:
            return await safe_edit(temp, "<b>[ ⓘ ] Nᴏ ʙᴏᴛ ᴀᴅᴍɪɴs ᴄᴏɴғɪɢᴜʀᴇᴅ.</b>", parse_mode=ParseMode.HTML, reply_markup=close_btn())

        total = len(admin_ids)
        await safe_edit(temp, f"<b>Fᴇᴛᴄʜɪɴɢ <code>{total}</code> ᴀᴅᴍɪɴ ᴅᴇᴛᴀɪʟs…</b>", parse_mode=ParseMode.HTML)

        # Fix 3: Batch fetch admins
        users_list = await safe_get_users(client, admin_ids)
        if not isinstance(users_list, list): users_list = [users_list] if users_list else []
        found_map = {u.id: u for u in users_list if u}

        admin_list = ""
        for uid in admin_ids:
            user = found_map.get(uid)
            if user:
                first_name = html.escape(getattr(user, "first_name", "Nᴏ Nᴀᴍᴇ") or "Nᴏ Nᴀᴍᴇ")
                uname = getattr(user, "username", None)
                username = f"@{uname}" if uname else "Nᴏ Usᴇʀɴᴀᴍᴇ"
                admin_list += f"<blockquote><b>Nᴀᴍᴇ:</b> <a href='tg://openmessage?user_id={uid}'>{first_name}</a>\n<b>Usᴇʀ:</b> {username}\n<b>ɪᴅ:</b> <code>{uid}</code></blockquote>\n\n"
            else:
                admin_list += f"<blockquote><b>ɪᴅ:</b> <code>{uid}</code>\n<i>Uɴᴀᴠᴀɪʟᴀʙʟᴇ / Dᴇᴀᴄᴛɪᴠᴀᴛᴇᴅ</i></blockquote>\n\n"

        caption = (
            f"<blockquote><b>✦ 𝗕𝗢𝗧 𝗔𝗗𝗠𝗜𝗡𝗦 𝗟𝗜𝗦𝗧</b></blockquote>\n"
            f"<blockquote>›› ᴛᴏᴛᴀʟ : <code>{total}</code> ᴀᴅᴍɪɴ(s)</blockquote>\n"
            "────────────────────\n" + admin_list
        )

        try:
            await temp.delete()
        except:
            pass

        try:
            await m.reply_photo(photo=ADMIN_LIST_PHOTO, caption=caption, parse_mode=ParseMode.HTML, reply_markup=close_btn())
        except Exception:
            await m.reply(caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=close_btn())

    except Exception as e:
        logger.error(f"[admin] /admins_list fault: {e}")
        await respond_with_error(temp, f"Error Loading List: {str(e)[:200]}")


# ═══════════════════════════════════════════════════════════
#  Callbacks & Init
# ═══════════════════════════════════════════════════════════

@Client.on_callback_query(filters.regex("^close_msg$"))
async def close_msg_cb(_: Client, cb: CallbackQuery) -> None:
    await cb.answer()
    try:
        await cb.message.delete()
    except Exception:
        pass

async def init(*args, **kwargs):
    """Fix 11: Starts background cleanup tasks."""
    loop = asyncio.get_event_loop()
    loop.create_task(_evict_old_cooldowns())
    loop.create_task(_periodic_cache_cleanup())
    logger.info("[admin] Background maintenance tasks started.")

# ═══════════════════════════════════════════════════════════
#  Exports
# ═══════════════════════════════════════════════════════════
__all__ = [
    "admin_svc",
    "user_cache",
    "safe_get_users",
    "safe_get_user",
    "safe_send_message",
    "safe_edit_media",
    "safe_delete_message",
    "retry_on_flood",
    "get_loading_text",
    "safe_edit",
    "respond_with_error",
    "close_btn",
    "_on_cooldown",
    "_sudo_filter",
    "_admin_or_sudo_filter",
    "init"
]

logger.info("[✓] Admin Management Module Refactored & Hardened")
