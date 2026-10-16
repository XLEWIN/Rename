#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀᴛɪᴏɴ ᴅᴏɴᴇ                │      @NyxRexy
#│  (ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ)                   │      @ShadowedTomb
#└──────────────────────────────────────────┘

import os
import time
import html
import logging
import asyncio
import inspect
import contextlib
from typing import Optional, Callable, Awaitable, Dict, Any, Set
from collections import OrderedDict, defaultdict
from functools import wraps

from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatAction
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from pyrogram.errors import MessageNotModified, FloodWait, RPCError

from plugins.access_control import check_ban_status, check_fsub_and_ban
from helper.database import DARKXSIDE78
from plugins.admin import safe_edit, close_btn

# Setup logging
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  | 𝗦𝗧𝗥𝗨𝗖𝗧𝗨𝗥𝗘𝗗 𝗖𝗢𝗡𝗦𝗧𝗔𝗡𝗧𝗦 & 𝗨𝗜 𝗧𝗘𝗫𝗧𝗦 |
# ─────────────────────────────────────────────────────────────

class UI_Texts:
    # Base Errors & Limits
    RATE_LIMIT = (
        "<blockquote>⧗ 𝗥𝗔𝗧𝗘 𝗟𝗜𝗠𝗜𝗧 𝗘𝗫𝗖𝗘𝗘𝗗𝗘𝗗</blockquote>\n\n"
        "◍  Pʟᴇᴀꜱᴇ ꜱʟᴏᴡ ᴅᴏᴡɴ ᴀɴᴅ ᴡᴀɪᴛ ᴀ ᴍᴏᴍᴇɴᴛ ʙᴇꜰᴏʀᴇ ᴛʀʏɪɴɢ ᴀɢᴀɪɴ."
    )
    API_RATE_LIMIT = (
        "<blockquote>⧗ 𝗔𝗣𝗜 𝗥𝗔𝗧𝗘 𝗟𝗜𝗠𝗜𝗧</blockquote>\n\n"
        "◍  Tᴇʟᴇɢʀᴀᴍ ᴘᴀᴜꜱᴇᴅ ᴛʜᴇ ꜱʏꜱᴛᴇᴍ ᴛᴏ ᴘʀᴇᴠᴇɴᴛ ꜱᴘᴀᴍ.\n\n"
        "<blockquote>ⓘ Pʟᴇᴀꜱᴇ ᴡᴀɪᴛ <code>{time}</code>ꜱ</blockquote>"
    )
    DB_ERROR = (
        "<blockquote>✗ 𝗗𝗔𝗧𝗔𝗕𝗔𝗦𝗘 𝗘𝗥𝗥𝗢𝗥</blockquote>\n\n"
        "◍  Tʜᴇ ꜱʏꜱᴛᴇᴍ ᴇɴᴄᴏᴜɴᴛᴇʀᴇᴅ ᴀɴ ɪꜱꜱᴜᴇ ᴡʜɪʟᴇ ᴄᴏɴɴᴇᴄᴛɪɴɢ.\n\n"
        "<blockquote>ⓘ <code>{error}</code></blockquote>"
    )
    SYS_ERROR = (
        "<blockquote>✗ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗘𝗥𝗥𝗢𝗥</blockquote>\n\n"
        "◍  Aɴ ᴜɴᴇxᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴅᴜʀɪɴɢ ᴛʜᴇ ᴏᴘᴇʀᴀᴛɪᴏɴ.\n\n"
        "<blockquote>ⓘ <code>{error}</code></blockquote>"
    )
    API_ERROR = (
        "<blockquote>✗ 𝗔𝗣𝗜 𝗘𝗥𝗥𝗢𝗥</blockquote>\n\n"
        "◍  Tᴇʟᴇɢʀᴀᴍ API ʀᴇᴊᴇᴄᴛᴇᴅ ᴛʜᴇ ʀᴇQᴜᴇꜱᴛ.\n\n"
        "<blockquote>ⓘ <code>{error}</code></blockquote>"
    )
    TIMEOUT = (
        "<blockquote>⧗ 𝗢𝗣𝗘𝗥𝗔𝗧𝗜𝗢𝗡 𝗧𝗜𝗠𝗘𝗗 𝗢𝗨𝗧</blockquote>\n\n"
        "◍  Tʜᴇ ᴅᴀᴛᴀʙᴀꜱᴇ ʀᴇꜱᴘᴏɴꜱᴇ ᴛᴏᴏᴋ ᴛᴏᴏ ʟᴏɴɢ. Pʟᴇᴀꜱᴇ ᴛʀʏ ᴀɢᴀɪɴ."
    )
    INITIATING = "<blockquote>⧗ 𝗜𝗡𝗜𝗧𝗜𝗔𝗧𝗜𝗡𝗚 𝗣𝗥𝗢𝗖𝗘𝗦𝗦...</blockquote>"
    
    # Format Specific
    NO_FORMAT = (
        "<blockquote>ⓘ 𝗡𝗢 𝗙𝗢𝗥𝗠𝗔𝗧 𝗙𝗢𝗨𝗡𝗗</blockquote>\n\n"
        "◍  Yᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀɴʏ Aᴜᴛᴏ-Rᴇɴᴀᴍᴇ ᴛᴇᴍᴘʟᴀᴛᴇ ꜱᴇᴛ.\n\n"
        "<blockquote>➥ Pʟᴇᴀꜱᴇ ꜱᴇᴛ ᴏɴᴇ ᴜꜱɪɴɢ /format</blockquote>"
    )
    FORMAT_OVERWRITE_WARN = (
        "<blockquote>ⓘ 𝗙𝗢𝗥𝗠𝗔𝗧 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗦𝗘𝗧</blockquote>\n\n"
        "◍  A ꜰᴏʀᴍᴀᴛ ᴛᴇᴍᴘʟᴀᴛᴇ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ Pʟᴇᴀꜱᴇ ᴜꜱᴇ /delformat ᴛᴏ ʀᴇᴍᴏᴠᴇ ɪᴛ ʙᴇꜰᴏʀᴇ ꜱᴇᴛᴛɪɴɢ ᴀ ɴᴇᴡ ᴏɴᴇ.</blockquote>"
    )
    FORMAT_SAVE_SUCCESS = (
        "<blockquote>✓ 𝗙𝗢𝗥𝗠𝗔𝗧 𝗦𝗔𝗩𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴀᴜᴛᴏ-ʀᴇɴᴀᴍᴇ ᴛᴇᴍᴘʟᴀᴛᴇ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ.\n\n"
        "<blockquote>➥ <code>{format_template}</code></blockquote>"
    )
    FORMAT_DEL_SUCCESS = (
        "<blockquote>✓ 𝗙𝗢𝗥𝗠𝗔𝗧 𝗗𝗘𝗟𝗘𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴀᴜᴛᴏ-ʀᴇɴᴀᴍᴇ ᴛᴇᴍᴘʟᴀᴛᴇ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ."
    )
    VIEW_FORMAT = (
        "<blockquote>ⓘ 𝗖𝗨𝗥𝗥𝗘𝗡𝗧 𝗙𝗢𝗥𝗠𝗔𝗧</blockquote>\n\n"
        "◍  Tʜᴇ ꜰᴏʟʟᴏᴡɪɴɢ ꜰᴏʀᴍᴀᴛ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ <code>{format_template}</code></blockquote>"
    )

    # Preferences Specific
    SET_MEDIA_PROMPT = (
        "<blockquote>ⓘ 𝗠𝗘𝗗𝗜𝗔 𝗣𝗥𝗘𝗙𝗘𝗥𝗘𝗡𝗖𝗘</blockquote>\n\n"
        "◍  Cʜᴏᴏꜱᴇ ʜᴏᴡ ᴛʜᴇ ʙᴏᴛ ꜱʜᴏᴜʟᴅ ʜᴀɴᴅʟᴇ ʏᴏᴜʀ ᴜᴘʟᴏᴀᴅᴇᴅ ꜰɪʟᴇꜱ.\n\n"
        "<blockquote>➥ Cᴜʀʀᴇɴᴛ : <code>{current}</code></blockquote>"
    )
    MEDIA_UPDATED = (
        "<blockquote>✓ 𝗠𝗘𝗗𝗜𝗔 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ꜰɪʟᴇꜱ ᴡɪʟʟ ɴᴏᴡ ʙᴇ ᴘʀᴏᴄᴇꜱꜱᴇᴅ ᴀꜱ ʏᴏᴜ ꜱᴇʟᴇᴄᴛᴇᴅ.\n\n"
        "<blockquote>➥ Cᴜʀʀᴇɴᴛ : <code>{current}</code></blockquote>"
    )
    SET_SOURCE_PROMPT = (
        "<blockquote>ⓘ 𝗥𝗘𝗡𝗔𝗠𝗘 𝗦𝗢𝗨𝗥𝗖𝗘</blockquote>\n\n"
        "◍  Cʜᴏᴏꜱᴇ ᴡʜᴇʀᴇ ᴛʜᴇ ʙᴏᴛ ᴇxᴛʀᴀᴄᴛꜱ ᴠᴀʀɪᴀʙʟᴇꜱ ꜰʀᴏᴍ (Fɪʟᴇɴᴀᴍᴇ ᴏʀ Cᴀᴘᴛɪᴏɴ).\n\n"
        "<blockquote>➥ Cᴜʀʀᴇɴᴛ : <code>{current}</code></blockquote>"
    )
    SOURCE_UPDATED = (
        "<blockquote>✓ 𝗦𝗢𝗨𝗥𝗖𝗘 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Tʜᴇ ʙᴏᴛ ᴡɪʟʟ ɴᴏᴡ ᴇxᴛʀᴀᴄᴛ ᴠᴀʀɪᴀʙʟᴇꜱ ꜰʀᴏᴍ ʏᴏᴜʀ ꜱᴇʟᴇᴄᴛɪᴏɴ.\n\n"
        "<blockquote>➥ Cᴜʀʀᴇɴᴛ : <code>{current}</code></blockquote>"
    )

    # ─── CALLBACK ALERTS (Plain Text / No HTML) ───
    CB_RATE_LIMIT = "⧗ Sʟᴏᴡ ᴅᴏᴡɴ. Pʟᴇᴀꜱᴇ ᴡᴀɪᴛ."
    CB_DB_ERROR = "✗ Dᴀᴛᴀʙᴀꜱᴇ Eʀʀᴏʀ : {error}"
    CB_SYS_ERROR = "✗ Eʀʀᴏʀ Eɴᴄᴏᴜɴᴛᴇʀᴇᴅ"
    CB_ALREADY_SELECTED = "ⓘ Aʟʀᴇᴀᴅʏ ꜱᴇʟᴇᴄᴛᴇᴅ"
    CB_MEDIA_UPDATED = "✓ Mᴇᴅɪᴀ ᴘʀᴇꜰᴇʀᴇɴᴄᴇ ᴜᴘᴅᴀᴛᴇᴅ"
    CB_SOURCE_UPDATED = "✓ Sᴏᴜʀᴄᴇ ꜱᴇᴛ ᴛᴏ {source}"


# ─────────────────────────────────────────────────────────────
#  | 𝗘𝗫𝗖𝗘𝗣𝗧𝗜𝗢𝗡𝗦 & 𝗧𝗔𝗦𝗞 𝗥𝗘𝗚𝗜𝗦𝗧𝗥𝗬 |
# ─────────────────────────────────────────────────────────────

class DatabaseUnavailableError(Exception):
    """Raised to prevent ghost-states when the DB completely fails."""
    pass

# Global task registry to prevent orphaned animations during shutdown
_active_animations: Set[asyncio.Task] = set()

class RateLimiter:
    def __init__(self, max_size: int = 10000):
        self._last: OrderedDict[int, float] = OrderedDict()
        self._cooldown = 2.0  # seconds
        self._max_size = max_size

    def check(self, user_id: int) -> bool:
        now = time.monotonic()
        if user_id in self._last:
            if now - self._last[user_id] < self._cooldown:
                return False
        
        self._last[user_id] = now
        self._last.move_to_end(user_id)
        
        if len(self._last) > self._max_size:
            self._last.popitem(last=False)
            
        return True

rate_limiter = RateLimiter()


async def retry_db(func: Callable, *args, retries: int = 3, timeout: float = 5.0) -> Any:
    """DB wrapper with timeout, backoff, and sync/async safety."""
    for i in range(retries):
        try:
            res = func(*args)
            if inspect.isawaitable(res):
                return await asyncio.wait_for(res, timeout=timeout)
            else:
                return res
        except Exception as e:
            if i == retries - 1:
                logger.error(f"[DB_ERROR] Max retries reached: {e}")
                raise
            await asyncio.sleep(0.5 * (2 ** i))


class UserPreferencesService:
    def __init__(self, ttl: float = 60.0, max_size: int = 1000):
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._ts: Dict[str, float] = {}
        self._ttl = ttl
        self._max_size = max_size
        
        # Per-user atomic locks to prevent race conditions on rapid writes
        self._user_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        # Global lock for cache dict mutations to prevent thread-unsafe evictions
        self._cache_lock = asyncio.Lock()

    async def _get(self, user_id: int, key: str, fetcher: Callable) -> Optional[str]:
        now = time.monotonic()
        cache_key = f"{user_id}:{key}"

        async with self._cache_lock:
            if cache_key in self._cache:
                if now - self._ts.get(cache_key, 0) < self._ttl:
                    self._cache.move_to_end(cache_key)
                    return self._cache[cache_key]
                else:
                    # Lazy TTL cleanup: Evict emotionally damaged elephants
                    del self._cache[cache_key]
                    del self._ts[cache_key]

        try:
            val = await retry_db(fetcher, user_id)
        except Exception as e:
            logger.error(f"[Cache-Fetch-Error] uid:{user_id} key:{key} -> {e}")
            raise DatabaseUnavailableError("Database is temporarily unreachable.")
        
        async with self._cache_lock:
            self._cache[cache_key] = val
            self._ts[cache_key] = now
            self._cache.move_to_end(cache_key)
            
            if len(self._cache) > self._max_size:
                oldest_key, _ = self._cache.popitem(last=False)
                self._ts.pop(oldest_key, None)
                
        return val

    async def _invalidate(self, user_id: int, key: str) -> None:
        cache_key = f"{user_id}:{key}"
        async with self._cache_lock:
            self._cache.pop(cache_key, None)
            self._ts.pop(cache_key, None)

    # ─── PUBLIC TYPED API ───
    async def get_format(self, user_id: int) -> Optional[str]:
        return await self._get(user_id, "format", DARKXSIDE78.get_format_template)

    async def set_format(self, user_id: int, template: Optional[str]) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.set_format_template, user_id, template)
            await self._invalidate(user_id, "format")

    async def get_media(self, user_id: int) -> Optional[str]:
        return await self._get(user_id, "media", DARKXSIDE78.get_media_preference)

    async def set_media(self, user_id: int, media: Optional[str]) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.set_media_preference, user_id, media)
            await self._invalidate(user_id, "media")

    async def get_source(self, user_id: int) -> Optional[str]:
        return await self._get(user_id, "source", DARKXSIDE78.get_metadata_source)

    async def set_source(self, user_id: int, source: Optional[str]) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.set_metadata_source, user_id, source)
            await self._invalidate(user_id, "source")

    async def get_thumb_engine(self, user_id: int) -> str:
        return await self._get(user_id, "thumb_engine", DARKXSIDE78.get_thumb_engine) or "off"

    async def set_thumb_engine(self, user_id: int, engine: str) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.set_thumb_engine, user_id, engine)
            await self._invalidate(user_id, "thumb_engine")


user_pref_svc = UserPreferencesService()


# ─────────────────────────────────────────────────────────────
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 & 𝗔𝗡𝗜𝗠𝗔𝗧𝗜𝗢𝗡 |
# ─────────────────────────────────────────────────────────────

def sanitize_template(text: str) -> str:
    """Storage-safe sanitization. Length-limit only. Do NOT HTML escape here."""
    return str(text)[:1024]


def handle_errors(func: Callable):
    """Handle errors gracefully with user-friendly messages for standard calls"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait: {e.value}s in {func.__name__}")
            await asyncio.sleep(e.value)
            return await func(*args, **kwargs)
        except DatabaseUnavailableError as e:
            ctx = args[1]
            if hasattr(ctx, "reply_text"):
                await ctx.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)
            elif hasattr(ctx, "answer"): # Fallback for CallbackQueries
                await ctx.answer(UI_Texts.CB_DB_ERROR.format(error=str(e)[:50]), show_alert=True)
        except MessageNotModified:
            logger.debug("Message not modified")
        except Exception as e:
            user_id = None
            if len(args) > 1 and hasattr(args[1], "from_user"):
                user_id = args[1].from_user.id if args[1].from_user else None

            logger.error(f"[ERR] func={func.__name__} user={user_id} err={e}", exc_info=True)
            try:
                ctx = args[1]
                if hasattr(ctx, "reply_text"):
                    await ctx.reply_text(UI_Texts.SYS_ERROR.format(error=html.escape(str(e))[:200]), parse_mode=ParseMode.HTML)
                elif hasattr(ctx, "answer"): # Fallback for CallbackQueries
                    await ctx.answer(UI_Texts.CB_SYS_ERROR, show_alert=True)
            except:
                pass
    return wrapper


async def play_animation(message: Message, frames: list, stop_event: asyncio.Event, delay: float = 0.5):
    """Handles infinite async loop animation gracefully using an Event flag."""
    try:
        while not stop_event.is_set():
            for frame in frames:
                if stop_event.is_set():
                    break
                try:
                    await message.edit_text(frame, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(delay)
                except MessageNotModified:
                    await asyncio.sleep(delay)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except Exception as e:
                    logger.debug(f"[anim-loop] stopped for {message.chat.id}: {e}")
                    stop_event.set()
                    break
    except Exception as e:
        logger.debug(f"[anim-loop-fatal]: {e}")


async def safe_exec(action_func: Callable[[], Awaitable[None]], message: Message, success_text: str):
    """Executes a callable async action with race-condition safe loading animations."""
    frames = [
        "<blockquote>⧗ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚.</blockquote>",
        "<blockquote>⧗ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚..</blockquote>",
        "<blockquote>⧗ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚...</blockquote>",
        "<blockquote>⧗ 𝗣𝗥𝗢𝗖𝗘𝗦𝗦𝗜𝗡𝗚....</blockquote>"
    ]
    
    stop_event = asyncio.Event()
    anim_task = asyncio.create_task(play_animation(message, frames, stop_event))
    
    # Register task for graceful shutdown tracking
    _active_animations.add(anim_task)
    anim_task.add_done_callback(_active_animations.discard)

    async def cleanup_anim():
        stop_event.set()
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(anim_task, timeout=2.0)
        await asyncio.sleep(0.1)

    try:
        await action_func()
        await cleanup_anim()
        await safe_edit(message, success_text, reply_markup=close_btn())

    except FloodWait as e:
        await cleanup_anim()
        logger.warning(f"[Rate-Limit] Triggered FloodWait: {e.value}s")
        await safe_edit(message, UI_Texts.API_RATE_LIMIT.format(time=e.value), reply_markup=close_btn())

    except asyncio.TimeoutError:
        await cleanup_anim()
        logger.error(f"[user-cmd] DB Timeout (user={message.chat.id})")
        await safe_edit(message, UI_Texts.TIMEOUT, reply_markup=close_btn())
        
    except DatabaseUnavailableError as e:
        await cleanup_anim()
        logger.error(f"[user-cmd] DB Unavailable Fault: {e}")
        await safe_edit(message, UI_Texts.DB_ERROR.format(error=str(e)), reply_markup=close_btn())

    except RPCError as e:
        await cleanup_anim()
        logger.error(f"[user-cmd] API RPC Fault: {e}")
        err_msg = html.escape(str(e))
        await safe_edit(message, UI_Texts.API_ERROR.format(error=err_msg), reply_markup=close_btn())

    except Exception as e:
        await cleanup_anim()
        logger.error(f"[user-cmd] System Write Fault (user={message.chat.id}): {e}", exc_info=True)
        safe_err_text = html.escape(str(e))[:200]
        
        try:
            await safe_edit(message, UI_Texts.SYS_ERROR.format(error=safe_err_text), reply_markup=close_btn())
        except Exception as edit_err:
            logger.error(f"[safe_edit fallback] Could not edit message: {edit_err}")
            with contextlib.suppress(Exception):
                await message.edit_text(f"✗ Eʀʀᴏʀ: {safe_err_text}")


# ─────────────────────────────────────────────────────────────
#  | 𝗛𝗘𝗟𝗣𝗘𝗥 𝗙𝗨𝗡𝗖𝗧𝗜𝗢𝗡𝗦 |
# ─────────────────────────────────────────────────────────────

def build_media_keyboard(selected: str = None) -> InlineKeyboardMarkup:
    def mark(text: str, key: str) -> str:
        return f"✓ {text}" if key == selected else text

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(mark("Dᴏᴄᴜᴍᴇɴᴛ", "document"), callback_data="setmedia_document"),
            InlineKeyboardButton(mark("Vɪᴅᴇᴏ", "video"), callback_data="setmedia_video"),
        ],
        [
            InlineKeyboardButton(mark("Aᴜᴅɪᴏ", "audio"), callback_data="setmedia_audio"),
        ]
    ])


def build_source_keyboard(selected: str = "filename") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{'✓ ' if selected == 'filename' else ''}Fɪʟᴇɴᴀᴍᴇ", callback_data="setsource_filename"),
            InlineKeyboardButton(f"{'✓ ' if selected == 'caption' else ''}Cᴀᴘᴛɪᴏɴ", callback_data="setsource_caption"),
        ]
    ])


# ─────────────────────────────────────────────────────────────
#  | 𝗔𝗨𝗧𝗢 𝗥𝗘𝗡𝗔𝗠𝗘 𝗖𝗢𝗠𝗠𝗔𝗡𝗗 |
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("format"))
@check_ban_status
@handle_errors
async def auto_rename_command(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)
    
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    
    existing_format = await user_pref_svc.get_format(user_id)
    if existing_format:
        return await message.reply_text(UI_Texts.FORMAT_OVERWRITE_WARN, parse_mode=ParseMode.HTML)

    command_parts = message.text.split(maxsplit=1)
    
    if len(message.command) < 2 or not command_parts[1].strip():
        await message.reply_text(
            "<blockquote expandable><b>✦ Iᴍᴘᴏʀᴛᴀɴᴛ</b> : Bᴇғᴏʀᴇ ᴜsɪɴɢ /format ᴛᴏ ʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ғɪʟᴇs, ᴘʟᴇᴀsᴇ ᴜsᴇ ᴀ ᴄʟᴇᴀʀ ᴛᴇᴍᴘʟᴀᴛᴇ. Tʜᴇ ʙᴏᴛ ᴡɪʟʟ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ʀᴇɴᴀᴍᴇ ғɪʟᴇs ʙᴀsᴇᴅ ᴏɴ ᴛʜɪs ғᴏʀᴍᴀᴛ.\n\n"
            "<b>Fᴏʀᴍᴀᴛᴛɪɴɢ Oᴘᴛɪᴏɴs :</b>\n\n"
            "<b>◍ Cᴏʀᴇ :</b>\n"
            "- <code>{title}</code> : Cʟᴇᴀɴᴇᴅ ᴛɪᴛʟᴇ ᴀғᴛᴇʀ ᴍᴇᴛᴀᴅᴀᴛᴀ / ᴊᴜɴᴋ ʀᴇᴍᴏᴠᴀʟ.\n"
            "- <code>{year}</code> : Rᴇʟᴇᴀsᴇ ʏᴇᴀʀ (ᴇ.ɢ., 2024).\n"
            "- <code>{group}</code> : Rᴇʟᴇᴀsᴇ / Fᴀɴsᴜʙ ɢʀᴏᴜᴘ.\n"
            "- <code>{size}</code> : Fɪʟᴇ sɪᴢᴇ ᴡɪᴛʜ ᴜɴɪᴛ (ᴇ.ɢ., 1.2 GB).\n"
            "- <code>{mode}</code> : Cᴏɴᴛᴇɴᴛ ᴛʏᴘᴇ — <code>SERIES</code>, <code>MANGA</code>, ᴏʀ <code>MOVIE</code>.\n\n"
            "<b>◍ Sᴇʀɪᴇs :</b>\n"
            "- <code>{season}</code> : Sᴇᴀsᴏɴ ɴᴜᴍʙᴇʀ (ᴇ.ɢ., 01).\n"
            "- <code>{episode}</code> : Sɪɴɢʟᴇ ᴇᴘɪsᴏᴅᴇ ɴᴜᴍʙᴇʀ.\n"
            "- <code>{episode_start}</code> : Fɪʀsᴛ ᴇᴘɪsᴏᴅᴇ ɪɴ ᴀ ʙᴀᴛᴄʜ / ʀᴀɴɢᴇ.\n"
            "- <code>{episode_end}</code> : Lᴀsᴛ ᴇᴘɪsᴏᴅᴇ ɪɴ ᴀ ʙᴀᴛᴄʜ / ʀᴀɴɢᴇ.\n"
            "- <code>{season_episode}</code> : Cᴏᴍʙɪɴᴇᴅ (ᴇ.ɢ., S01E01 ᴏʀ S01E01-03).\n"
            "- <code>{episode_absolute}</code> : Aʙsᴏʟᴜᴛᴇ ᴇᴘɪsᴏᴅᴇ ɴᴜᴍʙᴇʀ.\n"
            "- <code>{part}</code> : Pᴀʀᴛ / Cᴏᴜʀ ɴᴜᴍʙᴇʀ (ᴇ.ɢ., 1).\n\n"
            "<b>◍ Mᴀɴɢᴀ :</b>\n"
            "- <code>{volume}</code> : Vᴏʟᴜᴍᴇ ɴᴜᴍʙᴇʀ.\n"
            "- <code>{chapter}</code> : Cʜᴀᴘᴛᴇʀ ɴᴜᴍʙᴇʀ (sᴜᴘᴘᴏʀᴛs ᴅᴇᴄɪᴍᴀʟ).\n\n"
            "<b>◍ Vɪᴅᴇᴏ / Rᴇʟᴇᴀsᴇ :</b>\n"
            "- <code>{quality}</code> : Dɪsᴘʟᴀʏ ǫᴜᴀʟɪᴛʏ ʟᴀʙᴇʟ (ᴇ.ɢ., 480ᴘ, 720ᴘ, 1080ᴘ, 2160ᴘ).\n"
            "- <code>{type}</code> : Dɪsᴘʟᴀʏ ᴛʏᴘᴇ (ᴇ.ɢ., OVA, Special, OP, ED).\n"
            "- <code>{episode_type}</code> : Rᴀᴡ ᴇᴘɪsᴏᴅᴇ ᴛʏᴘᴇ (ᴇ.ɢ., OVA, SP, ONA).\n"
            "- <code>{resolution}</code> : Tᴇᴄʜɴɪᴄᴀʟ ʀᴇsᴏʟᴜᴛɪᴏɴ ᴅᴇᴛᴇᴄᴛᴇᴅ ғʀᴏᴍ ᴍᴇᴅɪᴀ.\n"
            "- <code>{source}</code> : Sᴏᴜʀᴄᴇ ᴛᴀɢ (ᴇ.ɢ., WEB-DL, WEBRip, BluRay, HDTV, HDRip).\n"
            "- <code>{edition}</code> : Eᴅɪᴛɪᴏɴ ᴛᴀɢ (ᴇ.ɢ., PROPER, REPACK, EXTENDED).\n"
            "- <code>{release_flags}</code> : Rᴇʟᴇᴀsᴇ ғʟᴀɢs (ᴇ.ɢ., SUB, DUB, MULTI-SUB).\n"
            "- <code>{video_codec}</code> : Vɪᴅᴇᴏ ᴄᴏᴅᴇᴄ (x264, x265, HEVC).\n"
            "- <code>{bit_depth}</code> : Bɪᴛ ᴅᴇᴘᴛʜ (8bit, 10bit).\n\n"
            "<b>◍ Aᴜᴅɪᴏ :</b>\n"
            "- <code>{audio}</code> : Sʜᴏʀᴛ ᴅɪsᴘʟᴀʏ ᴛᴀɢ (ᴇ.ɢ., Sub, Subs, Dub, Dual, Tri, Multi).\n"
            "- <code>{audio_codec}</code> : Aᴜᴅɪᴏ ᴄᴏᴅᴇᴄ (AAC, DTS, AC3, DDP).\n"
            "- <code>{audio_layout}</code> : Aᴜᴅɪᴏ ʟᴀʏᴏᴜᴛ (Stereo, 5.1, 7.1).\n"
            "- <code>{audio_tracks}</code> : Tʀᴀᴄᴋ ᴅᴇsᴄʀɪᴘᴛɪᴏɴ (ᴇ.ɢ., Dual Audio, Multi Audio).\n\n"
            "<b>◍ Lᴀɴɢᴜᴀɢᴇ :</b>\n"
            "- <code>{language}</code> : Pʀɪᴍᴀʀʏ ʟᴀɴɢᴜᴀɢᴇ.\n"
            "- <code>{languages}</code> : Aʟʟ ʟᴀɴɢᴜᴀɢᴇs ᴊᴏɪɴᴇᴅ (ᴇ.ɢ., English + Hindi).</blockquote>\n\n"
            "<b>◍ Exᴀᴍᴘʟᴇs :</b>\n"
            "<b>➥ Mᴀɴɢᴀ :</b>\n"
            "<code>/format [C{chapter}-VOL{volume}] {title}</code>\n\n"
            "<b>➥ Aɴɪᴍᴇ :</b>\n"
            "<code>/format [S{season}-E{episode}] {title} [{quality}] [{audio}]</code>\n\n"
            "<b>➥ Mᴏᴠɪᴇ :</b>\n"
            "<code>/format {title} ({year}) [{quality}] [{source}]</code>\n\n"
            "<b>➥ Bᴀᴛᴄʜ Eᴘɪsᴏᴅᴇs :</b>\n"
            "<code>/format [S{season}-E{episode_start}-{episode_end}] {title}</code>\n\n"
            "<b><blockquote expandable>◍ Nᴏᴛᴇs :</b>\n"
            "- Mɪssɪɴɢ ᴘʟᴀᴄᴇʜᴏʟᴅᴇʀs ᴀʀᴇ sɪʟᴇɴᴛʟʏ ʀᴇᴍᴏᴠᴇᴅ.\n"
            "- Eᴍᴘᴛʏ ʙʀᴀᴄᴋᴇᴛs ᴀɴᴅ ᴇᴍᴘᴛʏ ᴘᴀʀᴇɴᴛʜᴇsᴇs ᴀʀᴇ ᴀᴜᴛᴏᴄʟᴇᴀɴᴇᴅ.\n"
            "- Sᴏᴍᴇ ɴᴜᴍᴇʀɪᴄ ᴘʟᴀᴄᴇʜᴏʟᴅᴇʀs ᴍᴀʏ ғᴀʟʟ ʙᴀᴄᴋ ᴛᴏ <code>01</code> ɪғ ɴᴏᴛ ғᴏᴜɴᴅ.\n"
            "- <code>{mode}</code> ɪs ᴜsᴇғᴜʟ ɪғ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ sᴇᴇ ᴡʜᴇᴛʜᴇʀ ᴛʜᴇ ʙᴏᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ ᴛʜᴇ ғɪʟᴇ ᴀs SERIES, MANGA, ᴏʀ MOVIE.</blockquote>",
            parse_mode=ParseMode.HTML,
            quote=True
        )
        return

    # Store raw, safe from infinite length but not corrupted by HTML escapes
    format_template = sanitize_template(command_parts[1].strip())
    
    mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
    logger.info(f"[SET_FORMAT] user={user_id}")

    # Display layer html.escape applied here
    safe_display_template = html.escape(format_template)
    success_msg = UI_Texts.FORMAT_SAVE_SUCCESS.format(format_template=safe_display_template)

    await safe_exec(
        lambda: user_pref_svc.set_format(user_id, format_template),
        mkn,
        success_msg
    )


@Client.on_message(filters.private & filters.command("delformat"))
@check_ban_status
@handle_errors
async def delete_auto_rename(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)
    
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    
    template = await user_pref_svc.get_format(user_id)
    if not template:
        return await message.reply_text(UI_Texts.NO_FORMAT, parse_mode=ParseMode.HTML)
    
    mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
    logger.info(f"[DEL_FORMAT] user={user_id}")
    
    await safe_exec(
        lambda: user_pref_svc.set_format(user_id, None),
        mkn,
        UI_Texts.FORMAT_DEL_SUCCESS
    )


@Client.on_message(filters.private & filters.command("viewformat"))
@check_ban_status
@handle_errors
async def show_auto_rename(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)
    
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    
    template = await user_pref_svc.get_format(user_id)
    if not template:
        return await message.reply_text(UI_Texts.NO_FORMAT, parse_mode=ParseMode.HTML)
    
    # Display layer html.escape applied here
    safe_display_template = html.escape(template)
    
    await message.reply_text(
        UI_Texts.VIEW_FORMAT.format(format_template=safe_display_template),
        parse_mode=ParseMode.HTML
    )


# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗘𝗗𝗜𝗔 𝗣𝗥𝗘𝗙𝗘𝗥𝗘𝗡𝗖𝗘 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦 |
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("setmedia"))
@check_ban_status
@handle_errors
async def set_media_command(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    current = await user_pref_svc.get_media(user_id)
    keyboard = build_media_keyboard(current)
    
    await message.reply_text(
        UI_Texts.SET_MEDIA_PROMPT.format(current=current.upper() if current else 'Nᴏᴛ Sᴇᴛ'),
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        quote=True
    )


@Client.on_callback_query(filters.regex(r"^setmedia_"))
@handle_errors
async def handle_media_selection(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if not rate_limiter.check(user_id):
        return await callback_query.answer(UI_Texts.CB_RATE_LIMIT, show_alert=True)

    media_type = callback_query.data.split("_", 1)[1].lower()

    try:
        await user_pref_svc.set_media(user_id, media_type)
        logger.info(f"[SET_MEDIA] user={user_id} media={media_type}")
    except DatabaseUnavailableError as e:
        return await callback_query.answer(UI_Texts.CB_DB_ERROR.format(error=str(e)[:50]), show_alert=True)

    keyboard = build_media_keyboard(media_type)
    text = UI_Texts.MEDIA_UPDATED.format(current=media_type.upper())

    try:
        await callback_query.message.edit_text(
            text=text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer(UI_Texts.CB_MEDIA_UPDATED)
    except MessageNotModified:
        await callback_query.answer(UI_Texts.CB_ALREADY_SELECTED, show_alert=False)


# ─────────────────────────────────────────────────────────────
#  | 𝗥𝗘𝗡𝗔𝗠𝗘 𝗦𝗢𝗨𝗥𝗖𝗘 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦 |
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.command(['set_rename_source', 'source', 'renamesource', 'set_rename', 'set_source', 'setsource']) 
    & filters.private
)
@check_ban_status
@handle_errors
async def set_rename_source(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    current_source = await user_pref_svc.get_source(user_id) or "filename"
    keyboard = build_source_keyboard(current_source)
    
    await message.reply_text(
        UI_Texts.SET_SOURCE_PROMPT.format(current=current_source.upper()),
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        quote=True
    )


@Client.on_callback_query(filters.regex(r"^setsource_"))
@handle_errors
async def handle_rename_source_selection(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if not rate_limiter.check(user_id):
        return await callback_query.answer(UI_Texts.CB_RATE_LIMIT, show_alert=True)

    new_source = callback_query.data.split("_", 1)[1].lower()

    try:
        await user_pref_svc.set_source(user_id, new_source)
        logger.info(f"[SET_SOURCE] user={user_id} source={new_source}")
    except DatabaseUnavailableError as e:
        return await callback_query.answer(UI_Texts.CB_DB_ERROR.format(error=str(e)[:50]), show_alert=True)

    keyboard = build_source_keyboard(new_source)
    text = UI_Texts.SOURCE_UPDATED.format(current=new_source.upper())

    try:
        await callback_query.message.edit_text(
            text=text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer(UI_Texts.CB_SOURCE_UPDATED.format(source=new_source.upper()), show_alert=False)
    except MessageNotModified:
        await callback_query.answer(UI_Texts.CB_ALREADY_SELECTED, show_alert=False)
