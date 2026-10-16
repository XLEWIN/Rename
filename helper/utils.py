
#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʀᴇsɪʟɪᴇɴᴛ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ═══════════════════════════════════════════════════════════
#  | 𝗜𝗠𝗣𝗢𝗥𝗧𝗦 |
# ═══════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import html
import logging
import os
import random
import re
import shutil
import threading
import time
import tempfile
import itertools
from collections import deque, defaultdict
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union, List, Set, Tuple
from contextlib import suppress

from pytz import timezone
from pyrogram.errors import (
    FloodWait,
    MessageDeleteForbidden,
    MessageNotModified,
    InputUserDeactivated,
    UserIsBlocked,
    PeerIdInvalid,
    UserDeactivated,
    RPCError,
)
from pyrogram.enums import ParseMode
from pyrogram.types import Message, CallbackQuery

try:
    from helper.database.database import DARKXSIDE78
except ImportError:
    DARKXSIDE78 = None

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚 𝗗𝗘𝗙𝗔𝗨𝗟𝗧𝗦 |
# ═══════════════════════════════════════════════════════════

try:
    from config import Config, Txt
except ImportError:
    class Config:  # type: ignore[no-redef]
        UPDATE_TIME = 1.0
        FLOODWAIT_RETRIES = 5
        LOG_CHANNEL = None
        SAFE_DELETE_ROOTS = []
        CLEANUP_INTERVAL = 600
        MAX_ACTIVE_TRACKERS = 3000
        MAX_TRACKERS_PER_USER = 25
        GLOBAL_API_CONCURRENCY = 50
        SHUTDOWN_TIMEOUT = 15
        ABANDON_FLOODWAIT_THRESHOLD = 300

    class Txt:  # type: ignore[no-redef]
        PROGRESS_BAR = "{}%\n{} / {}\nSpeed: {}/s\nETA: {}"

logger = logging.getLogger(__name__)


def _config_int(name: str, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        value = int(getattr(Config, name, default))
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _config_float(name: str, default: float, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    try:
        value = float(getattr(Config, name, default))
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


_GLOBAL_API_CONCURRENCY = _config_int("GLOBAL_API_CONCURRENCY", 50, minimum=1)
_EDIT_API_CONCURRENCY = _config_int("EDIT_API_CONCURRENCY", 4, minimum=1)
_SEND_API_CONCURRENCY = _config_int("SEND_API_CONCURRENCY", 12, minimum=1)
_EDIT_WORKER_COUNT = _config_int("EDIT_WORKER_COUNT", 8, minimum=1, maximum=32)
_MAX_RETRIES = _config_int("FLOODWAIT_RETRIES", 5, minimum=1, maximum=10)
_API_TIMEOUT = _config_int("API_TIMEOUT", 15, minimum=1)
_ABANDON_FW = _config_int("ABANDON_FLOODWAIT_THRESHOLD", 300, minimum=1)
_UPDATE_TIME = _config_float("UPDATE_TIME", 1.0, minimum=0.5)
_MAX_TRACKERS = _config_int("MAX_ACTIVE_TRACKERS", 3000, minimum=1)
_USER_QUOTA = _config_int("MAX_TRACKERS_PER_USER", 25, minimum=1)
_LOG_QUEUE_MAXSIZE = _config_int("LOG_QUEUE_MAXSIZE", 1000, minimum=10)
_RENDER_QUEUE_MAXSIZE = _config_int("RENDER_QUEUE_MAXSIZE", 5000, minimum=10)
_RENDER_QUEUE_WARN_SOFT = _config_int("RENDER_QUEUE_WARN_SOFT", 50, minimum=1)
_RENDER_QUEUE_WARN_HARD = _config_int("RENDER_QUEUE_WARN_HARD", 200, minimum=1)
_RENDER_STALE_SECONDS = _config_float("RENDER_STALE_SECONDS", 2.0, minimum=0.5)
_RENDER_LOOP_INTERVAL = _config_float("RENDER_LOOP_INTERVAL", 0.5, minimum=0.1)
_PROGRESS_SAMPLE_WINDOW = _config_float("PROGRESS_SAMPLE_WINDOW", 20.0, minimum=2.0)
_PROGRESS_SAMPLE_MIN_GAP = _config_float("PROGRESS_SAMPLE_MIN_GAP", 0.2, minimum=0.05)
_PROGRESS_MIN_WINDOW = _config_float("PROGRESS_MIN_WINDOW", 1.0, minimum=0.2)
_PROGRESS_MIN_SPEED_BPS = _config_float("PROGRESS_MIN_SPEED_BPS", 1024.0, minimum=1.0)
_PROGRESS_MIN_PERCENT_DELTA = _config_float("PROGRESS_MIN_PERCENT_DELTA", 1.0, minimum=0.0)

# ═══════════════════════════════════════════════════════════
#  | 𝗟𝗔𝗭𝗬-𝗜𝗡𝗜𝗧 𝗚𝗟𝗢𝗕𝗔𝗟 𝗦𝗬𝗡𝗖 𝗣𝗥𝗜𝗠𝗜𝗧𝗜𝗩𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

_SEM_API: Optional[asyncio.Semaphore] = None
_SEM_EDIT: Optional[asyncio.Semaphore] = None
_SEM_SEND: Optional[asyncio.Semaphore] = None
_STATES_LOCK: Optional[asyncio.Lock] = None
_INIT_LOCK = threading.Lock()
_LOG_QUEUE: Optional[asyncio.Queue] = None
_RENDER_QUEUE: Optional[asyncio.PriorityQueue] = None

def get_semaphore(name: str) -> asyncio.Semaphore:
    if _SEM_API is None: _init_primitives()
    if name == "api": return _SEM_API
    if name == "edit": return _SEM_EDIT
    return _SEM_SEND

def get_states_lock() -> asyncio.Lock:
    if _STATES_LOCK is None: _init_primitives()
    return _STATES_LOCK

def get_log_queue() -> asyncio.Queue:
    if _LOG_QUEUE is None: _init_primitives()
    return _LOG_QUEUE

def get_render_queue() -> asyncio.PriorityQueue:
    if _RENDER_QUEUE is None: _init_primitives()
    return _RENDER_QUEUE

_user_tracker_counts: Dict[int, int] = defaultdict(int)
_states: Dict[str, TransferState] = {}
_active_trackers: Set[TransferState] = set()

# ═══════════════════════════════════════════════════════════
#  | 𝗠𝗘𝗧𝗥𝗜𝗖𝗦 & 𝗧𝗘𝗟𝗘𝗠𝗘𝗧𝗥𝗬 |
# ═══════════════════════════════════════════════════════════

class UtilsMetrics:
    def __init__(self):
        self._counts = defaultdict(int)
        self.worker_restarts = 0
        self.api_latency_sum = 0.0
        self.api_calls_count = 0
        self.supervisor_backoffs = defaultdict(lambda: 30.0)

    def inc(self, key: str, val: int = 1):
        self._counts[key] += val

    def record_api(self, latency: float):
        self.api_calls_count += 1
        self.api_latency_sum += latency

    def __getattr__(self, name: str) -> int:
        if name in self._counts:
            return self._counts[name]
        raise AttributeError(f"UtilsMetrics has no attribute '{name}'")

    def get_report(self) -> str:
        avg_latency = (self.api_latency_sum / self.api_calls_count * 1000) if self.api_calls_count else 0
        return (
            "<b>Rᴜɴᴛɪᴍᴇ Kᴇʀɴᴇʟ Tᴇʟᴇᴍᴇᴛʀʏ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"<b>• Avɢ API Lᴀᴛᴇɴᴄʏ:</b> <code>{avg_latency:.1f}ms</code>\n"
            f"<b>• Wᴏʀᴋᴇʀ Rᴇsᴛᴀʀᴛs:</b> <code>{self.worker_restarts}</code>\n"
            f"<b>• FʟᴏᴏᴅWᴀɪᴛ Rᴇᴛʀɪᴇs:</b> <code>{self._counts['floodwait_retries']}</code>\n"
            f"<b>• Tʀᴀᴄᴋᴇʀs Aᴄᴛɪᴠᴇ:</b> <code>{len(_states)}</code>\n"
            f"<b>• Eᴅɪᴛ Cᴀᴄʜᴇ Hɪᴛs:</b> <code>{self._counts['edit_cache_hits']}</code>\n"
            f"<b>• API Tɪᴍᴇᴏᴜᴛs:</b> <code>{self._counts['api_timeouts']}</code>\n"
            f"<b>• Lᴏɢs Dʀᴏᴘᴘᴇᴅ:</b> <code>{self._counts['log_messages_dropped']}</code>\n"
            f"<b>• Fɪʟᴇ Oᴘ Eʀʀᴏʀs:</b> <code>{self._counts['safe_delete_failures'] + self._counts['safe_move_failures']}</code>"
        )

metrics = UtilsMetrics()

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗡𝗜𝗙𝗜𝗘𝗗 𝗔𝗣𝗜 𝗪𝗥𝗔𝗣𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

async def _safe_api_call(func: Callable, sem: Optional[asyncio.Semaphore] = None, *args, **kwargs) -> bool:
    """Hardened API wrapper with global throttling, jittered retries, and FW cutoffs."""
    actual_sem = sem or get_semaphore("api")
    for attempt in range(_MAX_RETRIES):
        try:
            async with actual_sem:
                start = time.perf_counter()
                if hasattr(asyncio, "timeout"):
                    async with asyncio.timeout(_API_TIMEOUT):
                        await func(*args, **kwargs)
                else:
                    await asyncio.wait_for(func(*args, **kwargs), timeout=_API_TIMEOUT)
                metrics.record_api(time.perf_counter() - start)
                return True
                
        except MessageNotModified:
            metrics.inc("edit_cache_hits")
            return True
            
        except (MessageDeleteForbidden, InputUserDeactivated, UserIsBlocked, PeerIdInvalid, UserDeactivated):
            return False
            
        except FloodWait as e:
            if e.value > _ABANDON_FW or attempt == _MAX_RETRIES - 1:
                logger.error(f"Abandoning API call: FloodWait {e.value}s exceeds threshold or retries exhausted")
                return False
                
            metrics.inc("floodwait_retries")
            wait = e.value + 1 + random.uniform(0.2, 1.2)
            logger.warning(f"FloodWait: sleeping {wait:.2f}s")
            await asyncio.sleep(wait)
            
        except (asyncio.TimeoutError, TimeoutError):
            metrics.inc("api_timeouts")
            if attempt == _MAX_RETRIES - 1: return False
            await asyncio.sleep(1 + random.uniform(0.1, 0.5))
            
        except RPCError as e:
            err_code = getattr(e, "CODE", 0)
            err_name = getattr(e, "NAME", str(e))
            retryable_codes = {400, 420, 500, 503}
            retryable_names = ("INTERNAL_SERVER_ERROR", "TIMEOUT", "RETRY_AFTER", "FLOOD_WAIT", "PEER_ID_INVALID_TEMP")
            
            if err_code not in retryable_codes and not any(r in err_name for r in retryable_names):
                logger.error(f"Non-retryable RPC Error: {err_name} (Code: {err_code})")
                return False
            if attempt == _MAX_RETRIES - 1: return False
            await asyncio.sleep(1 + random.uniform(0.1, 0.5))
            
        except Exception as e:
            logger.error(f"Local API Wrapper Error: {e}")
            return False
            
    return False

async def safe_edit_message(message: Optional[Message], text: str, **kwargs: Any) -> bool:
    if not message: return False
    return await _safe_api_call(message.edit_text, sem=get_semaphore("edit"), text=text, **kwargs)

async def safe_edit_caption(message: Optional[Message], caption: str, **kwargs: Any) -> bool:
    if not message: return False
    return await _safe_api_call(message.edit_caption, sem=get_semaphore("edit"), caption=caption, **kwargs)

def handle_floodwait() -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(_MAX_RETRIES):
                try:
                    if hasattr(asyncio, "timeout"):
                        async with asyncio.timeout(_API_TIMEOUT):
                            return await func(*args, **kwargs)
                    else:
                        return await asyncio.wait_for(func(*args, **kwargs), timeout=_API_TIMEOUT)
                except FloodWait as e:
                    if e.value > _ABANDON_FW or attempt == _MAX_RETRIES - 1: raise
                    wait = e.value + 1 + random.uniform(0.5, 1.5)
                    metrics.inc("floodwait_retries")
                    await asyncio.sleep(wait)
                except (asyncio.TimeoutError, TimeoutError):
                    if attempt == _MAX_RETRIES - 1: raise
                    await asyncio.sleep(1)
            raise RuntimeError(f"{func.__name__} exhausted retries")
        return wrapper
    return decorator

# ═══════════════════════════════════════════════════════════
#  | 𝗣𝗥𝗢𝗚𝗥𝗘𝗦𝗦 𝗘𝗡𝗚𝗜𝗡𝗘 |
# ═══════════════════════════════════════════════════════════


_PRECOMPUTED_BARS = ["".join("▰" if i < filled else "▱" for i in range(20)) for filled in range(21)]

class TransferState:
    def __init__(self, client: Any, message: Message, ud_type: str, cancel_button: Any = None):
        self.client = client
        self.chat_id = message.chat.id
        self.msg_id = message.id
        self.is_media = bool(message.photo or message.video or message.document or message.animation or message.audio or message.voice or message.sticker)
        self.ud_type = ud_type
        self.state_key = f"{self.msg_id}_{self.ud_type}"
        self.user_id = message.from_user.id if message.from_user else 0
        self.cancel_button = cancel_button
        
        self.start_time = time.monotonic()
        self.last_sample_time = self.start_time
        self.last_render_time = 0.0
        
        self.last_bytes = 0
        self.total = 0
        
        self._samples: deque = deque(maxlen=64)
        self._samples.append((self.start_time, 0))
        self.display_speed = 0.0
        self.last_stable_speed = 0.0
        
        self.is_finalizing = False
        self.final_rendered = False
        
        self.last_rendered_text = ""
        self.last_rendered_percentage = -1.0
        self.dirty = True  # FIX 20: Track if state has changed since last render

    def _explicit_cleanup(self):
        """Direct cleanup, entirely synchronous and dictionary pop."""
        if _states.get(self.state_key) is self:
            _states.pop(self.state_key, None)
            if self.user_id:
                _user_tracker_counts[self.user_id] = max(0, _user_tracker_counts[self.user_id] - 1)

_SEQ = itertools.count()

async def _render_dispatcher():
    q = get_render_queue()
    while True:
        try:
            await asyncio.sleep(_RENDER_LOOP_INTERVAL)
            now = time.monotonic()
            
            cadence = _UPDATE_TIME
            qs = q.qsize()
            if qs > _RENDER_QUEUE_WARN_SOFT: cadence = max(_UPDATE_TIME, 2.0)
            if qs > _RENDER_QUEUE_WARN_HARD: cadence = max(_UPDATE_TIME, 5.0)
            
            trackers = list(_active_trackers)
            for state in trackers:
                if state.final_rendered:
                    continue
                
                # FIX 20: Skip render if nothing changed
                if not state.dirty and not state.is_finalizing:
                    continue
                    
                time_since = now - state.last_render_time
                if time_since < cadence and not state.is_finalizing:
                    continue
                    
                cur = state.last_bytes
                tot = state.total
                spd = state.display_speed
                is_fin = state.is_finalizing
                
                pct = (cur * 100 / tot) if tot > 0 else 0.0
                # 0.05% threshold — was 0.2%, which caused near-zero visible
                # updates on large files transferring slowly.
                if not is_fin and abs(pct - state.last_rendered_percentage) < _PROGRESS_MIN_PERCENT_DELTA:
                    continue
                    
                eta_str = TimeFormatter(int(((tot - cur) / spd) * 1000)) if spd > 0 else "..."
                bar_idx = 20 if is_fin else min(20, int((pct / 100) * 20))
                bar = _PRECOMPUTED_BARS[bar_idx]
                
                text = (
                    f"<b>{state.ud_type}</b>\n<code>{bar}</code>"
                    f"{Txt.PROGRESS_BAR.format(round(pct, 2), humanbytes(cur), humanbytes(tot), humanbytes(spd), eta_str)}"
                )

                if text == state.last_rendered_text and not is_fin:
                    continue
                    
                state.last_render_time = now
                state.last_rendered_text = text
                state.last_rendered_percentage = pct
                state.dirty = False # Reset dirty flag after queueing
                
                priority = 0 if is_fin else 1
                try:
                    q.put_nowait((priority, now, next(_SEQ), state, text))
                except asyncio.QueueFull:
                    pass
                    
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"Render dispatcher error: {e}")
            await asyncio.sleep(2)

async def _edit_worker():
    q = get_render_queue()
    while True:
        try:
            priority, queued_time, _, state, text = await q.get()
            now = time.monotonic()
            
            if priority != 0 and (now - queued_time) > _RENDER_STALE_SECONDS:
                q.task_done()
                continue
                
            if not getattr(state, "client", None):
                q.task_done()
                continue

            if state.is_media:
                func = state.client.edit_message_caption
                kwargs = {"chat_id": state.chat_id, "message_id": state.msg_id, "caption": text, "parse_mode": ParseMode.HTML, "reply_markup": state.cancel_button}
            else:
                func = state.client.edit_message_text
                kwargs = {"chat_id": state.chat_id, "message_id": state.msg_id, "text": text, "parse_mode": ParseMode.HTML, "reply_markup": state.cancel_button}
            
            # Pass _SEM_EDIT into _safe_api_call so it is only held during
            # the actual Telegram request — not across the full retry+FloodWait
            # loop, which previously could starve all 8 edit slots during a flood.
            res = await _safe_api_call(func, sem=get_semaphore("edit"), **kwargs)
                
            if priority == 0:
                state.final_rendered = True
                async with get_states_lock():
                    _active_trackers.discard(state)
                    state._explicit_cleanup()
                    
            q.task_done()
            
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"Edit worker error: {e}")
            await asyncio.sleep(1)

async def progress_for_pyrogram(
    current: int, total: int, ud_type: str, message: Message, start: float, client: Any = None, cancel_button: Any = None
) -> None:
    msg_id = message.id
    user_id = message.from_user.id if message.from_user else 0
    state_key = f"{msg_id}_{ud_type}"
    
    state = _states.get(state_key)
    
    if not state:
        actual_client = client or getattr(message, "_client", None)
        new_state = TransferState(actual_client, message, ud_type, cancel_button)
        async with get_states_lock():
            if state_key not in _states:
                if len(_states) >= _MAX_TRACKERS or (user_id and _user_tracker_counts[user_id] >= _USER_QUOTA):
                    metrics.inc("progress_edits_skipped")
                    return
                _states[state_key] = new_state
                _active_trackers.add(new_state)
                if user_id: _user_tracker_counts[user_id] += 1
                state = new_state
            else:
                state = _states[state_key]

    now = time.monotonic()
    
    if state.is_finalizing: return
    
    prev_bytes = state.last_bytes
    time_delta = now - state.last_sample_time
    if time_delta > 0:
        if time_delta > _PROGRESS_SAMPLE_MIN_GAP:
            state._samples.append((now, current))
            
        state.last_sample_time = now
        state.last_bytes = current
        state.total = total
        state.dirty = True # Mark for next render pass
        
        while len(state._samples) > 1 and now - state._samples[0][0] > _PROGRESS_SAMPLE_WINDOW:
            state._samples.popleft()
            
        window_time = now - state._samples[0][0]
        if len(state._samples) > 1 and window_time >= _PROGRESS_MIN_WINDOW:
            window_bytes = current - state._samples[0][1]
            spd = window_bytes / window_time
            if spd < _PROGRESS_MIN_SPEED_BPS:
                if current > prev_bytes and state.last_stable_speed > 0:
                    spd = state.last_stable_speed
                elif state.display_speed > 0:
                    spd = state.display_speed
            else:
                state.last_stable_speed = spd
                
            state.display_speed = spd
        elif current > prev_bytes and state.last_stable_speed > 0:
            state.display_speed = state.last_stable_speed
            
    if current >= total:
        state.is_finalizing = True

async def cleanup_tracker_memory(max_age: int = 3600) -> None:
    now = time.monotonic()
    stale_keys = []
    
    async with get_states_lock():
        for key, state in list(_states.items()):
            if (now - state.last_sample_time > max_age) and not state.is_finalizing:
                stale_keys.append(key)

    for key in stale_keys:
        async with get_states_lock():
            state = _states.get(key)
            if state and not state.is_finalizing:
                _states.pop(key, None)
                _active_trackers.discard(state)
                if state.user_id:
                    _user_tracker_counts[state.user_id] = max(0, _user_tracker_counts[state.user_id] - 1)

# ═══════════════════════════════════════════════════════════
#  | 𝗘𝗥𝗥𝗢𝗥 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

def robust_error_handler(func):
    @wraps(func)
    async def wrapper(client, message: Union[Message, CallbackQuery], *args, **kwargs):
        is_cb = isinstance(message, CallbackQuery)
        try:
            return await func(client, message, *args, **kwargs)
        except MessageNotModified:
            metrics.inc("edit_cache_hits")
            if is_cb:
                with suppress(Exception): await message.answer("Synced")
        except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid, UserDeactivated):
            text = "<b>✗ Access Error</b>\n<blockquote>Bot blocked or user unreachable.</blockquote>"
            if is_cb:
                with suppress(Exception): await message.answer("User unavailable", show_alert=True)
            else:
                with suppress(Exception): await message.reply_text(text, parse_mode=ParseMode.HTML)
        except FloodWait: raise
        except Exception as e:
            logger.error(f"Handler {func.__name__} error: {e}", exc_info=True)
            text = "<b>✗ Internal Error</b>\n<blockquote>An unexpected system error occurred. Please try again later.</blockquote>"
            with suppress(Exception):
                if is_cb:
                    await message.message.edit_text(text, parse_mode=ParseMode.HTML)
                else:
                    await message.reply_text(text, parse_mode=ParseMode.HTML)
    return wrapper

# ═══════════════════════════════════════════════════════════
#  | 𝗟𝗢𝗚𝗚𝗘𝗥 𝗘𝗡𝗚𝗜𝗡𝗘 |
# ═══════════════════════════════════════════════════════════

async def send_log(bot: Any, user: Any) -> bool:
    lc = getattr(Config, "LOG_CHANNEL", None)
    if not lc: return False
    try:
        ist = timezone("Asia/Kolkata")
        um = getattr(user, "mention", f"User {user.id}")
        un = html.escape(f"@{user.username}") if getattr(user, "username", None) else "N/A"
        bn = getattr(bot, "mention", None) or html.escape(getattr(bot, "first_name", "Bot"))
        text = (f"<b>❐ Nᴇᴡ Usᴇʀ Aᴄᴛɪᴠɪᴛʏ</b>\n◍ <b>Usᴇʀ :</b> {um}\n◍ <b>ID :</b> <code>{user.id}</code>\n"
                f"◍ <b>Usᴇʀɴᴀᴍᴇ :</b> <code>{un}</code>\n◍ <b>Bᴏᴛ :</b> {bn}\n◍ <b>Tɪᴍᴇ :</b> <code>{datetime.now(ist).strftime('%I:%M:%S %p')} IST</code>")
        get_log_queue().put_nowait((bot, lc, text))
        metrics.inc("log_messages_queued")
        return True
    except asyncio.QueueFull:
        metrics.inc("log_messages_dropped")
        return False
    except Exception: return False

async def _log_worker():
    q = get_log_queue()
    while True:
        try:
            bot, lc, text = await q.get()
            success = await _safe_api_call(bot.send_message, sem=get_semaphore("send"), chat_id=lc, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            if not success:
                metrics.inc("log_messages_dropped")
            q.task_done()
            if q.qsize() < 5: await asyncio.sleep(_RENDER_LOOP_INTERVAL)
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"Log worker error: {e}")
            await asyncio.sleep(5)

# ═══════════════════════════════════════════════════════════
#  | 𝗟𝗜𝗙𝗘𝗖𝗬𝗖𝗟𝗘 & 𝗦𝗨𝗣𝗘𝗥𝗩𝗜𝗦𝗜𝗢𝗡 |
# ═══════════════════════════════════════════════════════════

_supervisors: Dict[str, asyncio.Task] = {}

async def _supervise_task(w_name: str, w_func: Callable):
    logger.info(f"Supervisor for {w_name} active.")
    while True:
        worker = None
        try:
            worker = asyncio.create_task(w_func(), name=f"utils-{w_name}")
            await worker
            
            # If we reach here, it exited cleanly without exception
            metrics.worker_restarts += 1
            metrics.supervisor_backoffs[w_name] = 30.0
            logger.warning(f"Supervisor: {w_name} task exited cleanly. Backoff reset to 30.0s")
            await asyncio.sleep(30.0)
            
        except asyncio.CancelledError:
            if worker:
                worker.cancel()
                with suppress(asyncio.CancelledError):
                    await worker
            break
        except Exception as e:
            metrics.worker_restarts += 1
            metrics.supervisor_backoffs[w_name] = min(metrics.supervisor_backoffs[w_name] * 1.5, 300.0)
            logger.error(f"Supervisor for {w_name} failed: {e}. Backoff: {metrics.supervisor_backoffs[w_name]:.1f}s")
            await asyncio.sleep(metrics.supervisor_backoffs[w_name])

def _init_primitives():
    global _SEM_API, _SEM_EDIT, _SEM_SEND, _STATES_LOCK, _LOG_QUEUE, _RENDER_QUEUE
    with _INIT_LOCK:
        if _SEM_API is None:
            _SEM_API = asyncio.Semaphore(_GLOBAL_API_CONCURRENCY)
            _SEM_EDIT = asyncio.Semaphore(_EDIT_API_CONCURRENCY)
            _SEM_SEND = asyncio.Semaphore(_SEND_API_CONCURRENCY)
            _STATES_LOCK = asyncio.Lock()
            _LOG_QUEUE = asyncio.Queue(maxsize=_LOG_QUEUE_MAXSIZE)
            _RENDER_QUEUE = asyncio.PriorityQueue(maxsize=_RENDER_QUEUE_MAXSIZE)

def start_background_tasks():
    _init_primitives()
    for w_name, w_func in [("maintenance", _maintenance_worker), ("logger", _log_worker), ("dispatcher", _render_dispatcher)]:
        if w_name not in _supervisors or _supervisors[w_name].done():
            _supervisors[w_name] = asyncio.create_task(_supervise_task(w_name, w_func), name=f"utils-sup-{w_name}")
            
    for i in range(_EDIT_WORKER_COUNT):
        w_name = f"edit_worker_{i}"
        if w_name not in _supervisors or _supervisors[w_name].done():
            _supervisors[w_name] = asyncio.create_task(_supervise_task(w_name, _edit_worker), name=f"utils-sup-{w_name}")
            
    logger.info("[utils] Infrastructure supervision initialized.")
    
async def stop_background_tasks():
    logger.info("[utils] Stopping infrastructure...")
    timeout = getattr(Config, "SHUTDOWN_TIMEOUT", 15)
    
    for t in list(_supervisors.values()): t.cancel()
            
    all_tasks = list(_supervisors.values())
    if all_tasks:
        try:
            if hasattr(asyncio, "timeout"):
                async with asyncio.timeout(timeout):
                    await asyncio.gather(*all_tasks, return_exceptions=True)
            else:
                await asyncio.wait_for(asyncio.gather(*all_tasks, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Shutdown timeout ({timeout}s) exceeded.")
            
    _supervisors.clear()
    logger.info("[utils] Utility kernel shutdown complete.")

async def _maintenance_worker():
    while True:
        try:
            await asyncio.sleep(600)
            await cleanup_tracker_memory()
            metrics.supervisor_backoffs["maintenance"] = max(30.0, metrics.supervisor_backoffs["maintenance"] * 0.9)
        except asyncio.CancelledError: break
        except Exception: await asyncio.sleep(60)

# ═══════════════════════════════════════════════════════════
#  | 𝗙𝗜𝗟𝗘 & 𝗦𝗧𝗥𝗜𝗡𝗚 𝗛𝗘𝗟𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def humanbytes(size: Union[int, float]) -> str:
    try:
        s = float(size)
    except Exception:
        return "0 B"
        
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if s < 1024.0: return f"{s:.2f} {u}"
        s /= 1024.0
    return f"{s:.2f} PB"

def TimeFormatter(milliseconds: int) -> str:
    s, _ = divmod(int(milliseconds or 0), 1000)
    m, s = divmod(s, 60); h, m = divmod(m, 60); d, h = divmod(h, 24)
    p = []
    if d: p.append(f"{d}d")
    if h: p.append(f"{h}h")
    if m: p.append(f"{m}m")
    if s or not p: p.append(f"{s}s")
    return " ".join(p[:3])

def convert(seconds: int) -> str:
    try:
        total_seconds = int(seconds or 0)
    except Exception:
        return "00:00:00"
        
    if total_seconds < 0: return "00:00:00"
    days = total_seconds // 86400
    total_seconds %= 86400
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    if days > 0:
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def add_prefix_suffix(input_string: str, prefix: str = "", suffix: str = "") -> str:
    if not input_string: return ""
    name, ext = os.path.splitext(str(input_string).strip())
    return re.sub(r"\s+", " ", f"{prefix}{name}{suffix}{ext}").strip()

_ALLOWED_ROOTS = [os.path.abspath(os.getcwd()), os.path.realpath(tempfile.gettempdir())]
if hasattr(Config, "SAFE_DELETE_ROOTS") and Config.SAFE_DELETE_ROOTS:
    _ALLOWED_ROOTS.extend([os.path.abspath(r) for r in Config.SAFE_DELETE_ROOTS if r])
_DANGEROUS = ("/etc", "/sys", "/proc", "/dev", "/root", "/boot", "/bin", "/sbin")


def _is_path_within_allowed_roots(path: str) -> bool:
    for root in _ALLOWED_ROOTS:
        with suppress(ValueError):
            if os.path.commonpath([path, root]) == root:
                return True
    return False


def _is_safe_delete_target(abs_path: str) -> bool:
    if not abs_path or abs_path == "/":
        return False
    if any(abs_path == prefix or abs_path.startswith(prefix + "/") for prefix in _DANGEROUS):
        return False
    return _is_path_within_allowed_roots(abs_path)

async def safe_delete(*paths: Any) -> Dict[str, Any]:
    stats = {"succeeded": 0, "failed": 0, "errors": []}
    for path in paths:
        if not path: continue
        try:
            abs_path = os.path.realpath(str(path).strip())
            if not _is_safe_delete_target(abs_path):
                metrics.inc("safe_delete_failures")
                stats["failed"] += 1
                continue

            def _do_delete():
                if os.path.isdir(abs_path):
                    shutil.rmtree(abs_path)
                elif os.path.lexists(abs_path):
                    os.remove(abs_path)

            await asyncio.to_thread(_do_delete)
            stats["succeeded"] += 1
        except Exception as e:
            metrics.inc("safe_delete_failures")
            stats["failed"] += 1
            stats["errors"].append(str(e))
    return stats

async def safe_move(src: str, dst: str) -> bool:
    try:
        s, d = os.path.realpath(src), os.path.realpath(dst)

        if not (_is_path_within_allowed_roots(s) and _is_path_within_allowed_roots(d)):
            logger.debug(f"safe_move failed: invalid paths: {d}")
            metrics.inc("safe_move_failures")
            return False
            
        target = os.path.join(d, os.path.basename(s)) if os.path.isdir(d) else d
        if os.path.exists(target):
            logger.debug(f"safe_move failed: destination file exists: {target}")
            metrics.inc("safe_move_failures")
            return False
            
        await asyncio.to_thread(shutil.move, s, d); return True
    except Exception as e:
        logger.debug(f"safe_move error: {e}")
        metrics.inc("safe_move_failures")
        return False

__all__ = ["robust_error_handler", "progress_for_pyrogram", "humanbytes", "convert", "TimeFormatter", "add_prefix_suffix", "safe_delete", "safe_move", "send_log", "safe_edit_message", "safe_edit_caption", "handle_floodwait", "cleanup_tracker_memory", "start_background_tasks", "stop_background_tasks", "metrics"]
