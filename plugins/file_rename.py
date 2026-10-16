#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import itertools
import json
import logging
import mimetypes
import concurrent.futures
import os
import random
import re
import shutil
import time
import uuid
from collections import deque
from contextlib import suppress
from datetime import datetime
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple, Union

import aiofiles
import aiofiles.os
import aiohttp
import pytz
from PIL import Image, ImageOps
from PyPDF2 import PdfReader, PdfWriter
from pymongo import ReturnDocument

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import *
from helper import convert  # noqa: F401
from helper.database import DARKXSIDE78
from helper.db_premium import db
from helper.job_ledger import (
    job_ledger,
    STATUS_QUEUED, STATUS_DOWNLOADING, STATUS_PROCESSING,
    STATUS_UPLOADING, STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED,
)
from helper.utils import (
    humanbytes,
    progress_for_pyrogram,
    handle_floodwait,
    safe_edit_message,
    safe_edit_caption,
    _safe_api_call,
    get_semaphore,
)
from plugins import admin_controls
from plugins.access_control import check_ban_status, check_fsub_and_ban, check_fsub_and_ban_media
from plugins.antinsfw import check_anti_nsfw  # noqa: F401
from plugins.Features.anilist_thumb import (
    extract_anime_title,
    search_media_info,
)
from plugins.regex import (
    PeakTokenEngineV2_REXY,
    normalize_lang_code,
)
from plugins.intro_ui import (
    generate_ass_line,
    ass_to_web_hex,
    inject_into_ass,
    inject_into_srt,
    inject_into_vtt,
    inject_into_ttml,
    build_fallback_intro_ass,
)
from plugins.pdftool import pdf_svc
from plugins.metadata import MetadataService
try:
    from plugins.shortener import create_token_for_user, send_token_message   # correct spelling
except (ModuleNotFoundError, ImportError, Exception):
    try:
        from plugins.shortner import create_token_for_user, send_token_message    # legacy typo on server
    except (ModuleNotFoundError, ImportError, Exception):
        # Ultimate fallback: stubs so file_rename.py always loads
        async def create_token_for_user(user_id):
            return None, None, None, None
        async def send_token_message(client, user_id, message_to_reply=None, query=None):
            pass
# =============================================================================
# FIX 14: Structured logger factory
# =============================================================================
_base_logger = logging.getLogger(__name__)


def _log(level: str, msg: str, **ctx: Any) -> None:
    """Emit a structured log line with consistent context fields."""
    if ctx:
        parts = " ".join(f"{k}={v!r}" for k, v in ctx.items())
        line = f"{msg} | {parts}"
    else:
        line = msg
    getattr(_base_logger, level)(line)


# =============================================================================
# Constants
# =============================================================================
CON_LIMIT_ADMIN: int = Config.ADMIN_OR_PREMIUM_TASK_LIMIT
CON_LIMIT_NORMAL: int = Config.NORMAL_TASK_LIMIT
ADMINS: frozenset[int] = frozenset(Config.ADMIN)
GLOBAL_CONCURRENT_LIMIT: int = Config.GLOBAL_CONCURRENT_LIMIT
CANCEL_CODE_TTL: float = 60.0
CANCEL_ID_TTL: float = 1800.0
CANCEL_ID_SWEEP_INTERVAL: float = 900.0
VERIFY_CODE_TTL: float = 60.0       # same as CANCEL_CODE_TTL, explicit name
PROC_KILL_WAIT_TIMEOUT: float = 5.0  # FIX 8: max seconds to wait after kill
STATUS_DELETE_TIMEOUT: float = 5.0    # Max seconds for status message cleanup

# -- Error classification -------------------------------------------------
# Imported lazily to avoid circular import at module load
def _is_retryable(exc: Exception) -> bool:
    """
    False  → fatal; stop retrying immediately (FileReferenceExpired etc.)
    True   → transient; safe to retry with backoff
    """
    from pyrogram.errors import FileReferenceExpired, Forbidden  # local import
    _FATAL = (FileNotFoundError, FileReferenceExpired, Forbidden, PermissionError)
    _RETRY = (FloodWait, ConnectionError, TimeoutError, asyncio.TimeoutError, OSError)
    if isinstance(exc, _FATAL):
        return False
    return isinstance(exc, _RETRY)

# -- Per-task hard timeout ------------------------------------------------
TASK_HARD_TIMEOUT: float = 3600.0  # 1 hour absolute ceiling per task

# -- GLOBAL FFmpeg THRESHOLD (Fix 20: CPU Hardening) -----------------------
# Caps total simultaneous ffmpeg/ffprobe processes to prevent CPU starvation.
_FFMPEG_SEM: Optional[asyncio.Semaphore] = None

# -- Download timeout helper ----------------------------------------------
def _dl_timeout(file_size: int, min_speed_bps: int = 500_000) -> float:
    """Dynamic timeout from file size. Floor 120 s, cap 3600 s."""
    return max(120.0, min(3600.0, file_size / min_speed_bps))


def _ul_timeout(file_size: int, min_speed_bps: int = 350_000) -> float:
    """Dynamic upload timeout. Slightly looser than download to tolerate Telegram variance."""
    return max(180.0, min(5400.0, file_size / min_speed_bps))


def _get_ffmpeg_sem() -> asyncio.Semaphore:
    """Lazy-init the FFmpeg semaphore after the event loop is available."""
    global _FFMPEG_SEM
    if _FFMPEG_SEM is None:
        _FFMPEG_SEM = asyncio.Semaphore(10)
    return _FFMPEG_SEM

_FFPROBE_SEM: Optional[asyncio.Semaphore] = None

def _get_ffprobe_sem() -> asyncio.Semaphore:
    """Lazy-init the FFprobe semaphore."""
    global _FFPROBE_SEM
    if _FFPROBE_SEM is None:
        _FFPROBE_SEM = asyncio.Semaphore(20)
    return _FFPROBE_SEM

# -- Disk space guard -----------------------------------------------------
async def _has_disk_space(required: int, path: str = ".") -> bool:
    """Return True if free space >= 2× required bytes (headroom for output)."""
    try:
        usage = await asyncio.to_thread(shutil.disk_usage, path)
        return usage.free >= required * 2
    except OSError as exc:
        _log("warning", "_has_disk_space check failed, assuming space available",
             path=path, exc=str(exc))
        return True


# -- Stale working-file cleanup ------------------------------------------
# Each completed job deletes its own files in `finally`.  This periodic sweep
# handles only files stranded by a process crash, forced restart, or host
# shutdown.  It intentionally never scans outside these temporary folders.
_WORKING_STORAGE_DIRS: Tuple[str, ...] = ("downloads", "metadata", "processed")


def _cleanup_stale_working_files_sync(max_age: int) -> Tuple[int, int]:
    """Remove old files from the bot's temporary storage, returning count/bytes."""
    cutoff = time.time() - max_age
    removed_count = 0
    removed_bytes = 0
    workspace = os.path.abspath(os.getcwd())

    for folder in _WORKING_STORAGE_DIRS:
        root = os.path.abspath(os.path.join(workspace, folder))
        # Defence in depth: only operate on direct children of the workspace.
        if os.path.commonpath((workspace, root)) != workspace or not os.path.isdir(root):
            continue

        for current_root, _dirs, files in os.walk(root, followlinks=False):
            for filename in files:
                path = os.path.abspath(os.path.join(current_root, filename))
                if os.path.commonpath((root, path)) != root:
                    continue
                try:
                    stat = os.stat(path, follow_symlinks=False)
                    if stat.st_mtime >= cutoff:
                        continue
                    size = stat.st_size
                    os.remove(path)
                    removed_count += 1
                    removed_bytes += size
                except FileNotFoundError:
                    pass  # A normal job cleaned it first.
                except OSError as exc:
                    _log("debug", "stale storage cleanup skipped file", path=path, exc=str(exc))

        for current_root, dirs, _files in os.walk(root, topdown=False, followlinks=False):
            for dirname in dirs:
                path = os.path.abspath(os.path.join(current_root, dirname))
                if os.path.commonpath((root, path)) != root:
                    continue
                try:
                    os.rmdir(path)
                except OSError:
                    pass

    return removed_count, removed_bytes


async def cleanup_stale_working_files() -> Tuple[int, int]:
    """Safely clear abandoned downloads/metadata/processed files."""
    if not Config.STORAGE_CLEANUP_ENABLED:
        return 0, 0
    # A job may run for up to one hour; keep a safety buffer, but do not let
    # stranded files sit around for multiple hours when the host is low on disk.
    max_age = max(int(Config.STORAGE_CLEANUP_MAX_AGE), int(TASK_HARD_TIMEOUT + 900))
    removed_count, removed_bytes = await asyncio.to_thread(
        _cleanup_stale_working_files_sync, max_age
    )
    if removed_count:
        _log("info", "Removed stale working files", count=removed_count,
             bytes=removed_bytes, max_age_seconds=max_age)
    return removed_count, removed_bytes


async def _working_storage_sweeper() -> None:
    """Run a startup sweep, then continue cleaning stranded files safely."""
    interval = max(int(Config.STORAGE_CLEANUP_INTERVAL), 60)
    while True:
        try:
            await cleanup_stale_working_files()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Keep the maintenance worker alive if a transient filesystem
            # error occurs; the next interval gets another chance.
            _log("warning", "working storage sweep failed", exc=str(exc))
        await asyncio.sleep(interval)


# =============================================================================
# FIX 15: BotState — encapsulates all module-level mutable state
# =============================================================================
@dataclasses.dataclass
class _BotState:
    """
    Single owner of all mutable module-level state.
    Locks guard each dict independently so contention is minimised.
    """
    # user_id → {code, created_at}
    _verify: Dict[int, Dict[str, Any]] = dataclasses.field(default_factory=dict)
    _lock: Optional[asyncio.Lock] = None

    @property
    def _verify_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # FIX 10: evict stale verify entries on every access
    async def get_verify(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._verify_lock:
            self._evict_verify_unlocked()
            return self._verify.get(user_id)

    async def set_verify(self, user_id: int, data: Dict[str, Any]) -> None:
        async with self._verify_lock:
            self._verify[user_id] = data

    async def pop_verify(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._verify_lock:
            return self._verify.pop(user_id, None)

    def _evict_verify_unlocked(self) -> None:
        now = time.time()
        stale = [
            uid for uid, d in self._verify.items()
            if now - d.get("created_at", 0) > VERIFY_CODE_TTL
        ]
        for uid in stale:
            del self._verify[uid]
            _log("debug", "Evicted stale verify code", user_id=uid)

# Module-level singleton — created once at import time.
_state = _BotState()

# Atomic cancel-button ID generator.
_cancel_id_gen: itertools.count[int] = itertools.count(1)

# Media albums may dispatch one handler call per item. This cache makes only one
# handler invocation own the album and enqueue every supported file in it.
_media_group_seen: Dict[str, float] = {}
_MEDIA_GROUP_TTL: float = 30.0


def _evict_media_group_cache() -> None:
    now = time.monotonic()
    stale = [k for k, ts in _media_group_seen.items() if now - ts > _MEDIA_GROUP_TTL]
    for k in stale:
        del _media_group_seen[k]

async def _media_group_cache_evictor():
    while True:
        try:
            await asyncio.sleep(600)
            _evict_media_group_cache()
        except asyncio.CancelledError: break
        except Exception as e: _log("error", "Media group cache evictor failed", exc=str(e))

async def _verify_cache_evictor():
    while True:
        try:
            await asyncio.sleep(120)
            async with _state._verify_lock:
                now = time.time()
                stale = [uid for uid, d in _state._verify.items()
                         if now - d.get("created_at", 0) > VERIFY_CODE_TTL]
                for uid in stale:
                    del _state._verify[uid]
                    _log("debug", "Evicted stale verify code", user_id=uid)
        except asyncio.CancelledError: break
        except Exception as e: _log("error", "Verify cache evictor failed", exc=str(e))

# =============================================================================
# Persistent HTTP session — reuse TCP connections for AniList thumbnails etc.
# =============================================================================
_http_session: Optional[aiohttp.ClientSession] = None


async def _get_http_session() -> aiohttp.ClientSession:
    """Lazy-init a persistent aiohttp session with bounded connector pool."""
    global _http_session
    if _http_session is None or _http_session.closed:
        # Bounded connector pool to prevent socket exhaustion
        connector = aiohttp.TCPConnector(
            limit=20,          
            limit_per_host=5,  
            force_close=False, # Reuse connections
            enable_cleanup_closed=True
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=20, connect=5),
        )
    return _http_session

def _clean_meta_string(val: Any) -> str:
    """Sanitize metadata strings for ffmpeg, removing problematic characters."""
    if not val: return ""
    # Remove control characters and ensure UTF-8 safety
    s = str(val).replace('"', "'").replace("\\", "/")
    return "".join(ch for ch in s if ch.isprintable())

# Explicitly register common media types to harden mimetype detection
mimetypes.add_type("video/x-matroska", ".mkv")
mimetypes.add_type("video/webm", ".webm")
mimetypes.add_type("video/quicktime", ".mov")
mimetypes.add_type("audio/x-m4a", ".m4a")
mimetypes.add_type("audio/ogg", ".ogg")


async def close_http_session() -> None:
    """Gracefully close the shared HTTP session. Call from bot shutdown."""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None
        _log("info", "HTTP session closed")


# =============================================================================
# Safe message edit/delete — catch all Telegram API errors
# =============================================================================
async def _safe_edit(
    msg: Optional[Message],
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: ParseMode = ParseMode.HTML,
) -> bool:
    """
    Edit a message, swallowing ALL non-fatal Telegram errors.

    Returns True if the edit succeeded, False otherwise.
    Never raises — the rename pipeline must not crash because a status
    message edit failed (message deleted by user, FloodWait, etc.).
    """
    if msg is None:
        return False
    kwargs: Dict[str, Any] = {"reply_markup": reply_markup} if reply_markup is not None else {}
    if getattr(msg, "media", None) is not None or getattr(msg, "caption", None) is not None:
        if await safe_edit_caption(msg, text, parse_mode=parse_mode, **kwargs):
            return True
    return await safe_edit_message(msg, text, parse_mode=parse_mode, **kwargs)


async def _safe_edit_by_id(
    client: Client,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: ParseMode = ParseMode.HTML,
) -> bool:
    """Edit a message by ID, swallowing all errors."""
    try:
        kwargs: Dict[str, Any] = {"parse_mode": parse_mode}
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        return await _safe_api_call(
            client.edit_message_text,
            sem=get_semaphore("edit"),
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            **kwargs,
        )
    except Exception as exc:
        _log("debug", "_safe_edit_by_id suppressed", exc=type(exc).__name__)
        return False


async def _safe_delete(msg: Optional[Message], delay: int = 0) -> bool:
    """Delete a message, swallowing all errors. Returns True on success."""
    if msg is None:
        return False
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await msg.delete()
        return True
    except Exception as exc:
        _log("debug", "_safe_delete suppressed", exc=type(exc).__name__)
        return False


async def _safe_reply(
    message: Optional[Message],
    text: str,
    parse_mode: ParseMode = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> Optional[Message]:
    """Reply to a message, swallowing all errors. Returns the sent Message or None."""
    if message is None:
        return None
    try:
        kwargs: Dict[str, Any] = {"parse_mode": parse_mode}
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        async with get_semaphore("send"):
            return await message.reply_text(text, **kwargs)
    except FloodWait as e:
        try:
            await asyncio.sleep(min(e.value + 1, 10))
            async with get_semaphore("send"):
                return await message.reply_text(text, **kwargs)
        except Exception:
            return None
    except Exception as exc:
        _log("debug", "_safe_reply suppressed", exc=type(exc).__name__)
        return None


# =============================================================================
# Per-user stats
# =============================================================================
class _UserStats:
    __slots__ = (
        "total_tasks", "completed_tasks", "cancelled_tasks",
        "successful_tasks", "start_time", "completion_sent",
        "cancelled_globally", "last_activity",
    )

    def __init__(self) -> None:
        self.total_tasks: int = 0
        self.completed_tasks: int = 0
        self.cancelled_tasks: int = 0
        self.successful_tasks: int = 0
        self.start_time: Optional[float] = None
        self.completion_sent: bool = False
        self.cancelled_globally: bool = False
        self.last_activity: float = time.monotonic()

    def reset(self) -> None:
        self.total_tasks = 0
        self.completed_tasks = 0
        self.cancelled_tasks = 0
        self.successful_tasks = 0
        self.start_time = None
        self.completion_sent = False
        self.cancelled_globally = False
        self.last_activity = time.monotonic()

    def reset_keep_cancelled(self) -> None:
        self.total_tasks = 0
        self.completed_tasks = 0
        self.cancelled_tasks = 0
        self.successful_tasks = 0
        self.start_time = None
        self.completion_sent = False
        self.cancelled_globally = True
        self.last_activity = time.monotonic()

# =============================================================================
# FIX 6/9: Cancellation-aware, throttled, non-raising progress wrapper
# =============================================================================
# Debounce dict: task_id → timestamp of last successful progress edit.
# Prevents FloodWait storms at high concurrent_transmissions values.
_progress_last_sample: Dict[str, float] = {}
# 2.0s gap: Telegram ratelimits edits to ~1/s anyway, and 0.25s was causing
# 4x unnecessary edits per second, each with a ~200-500ms API round-trip that
# serialised Pyrogram's concurrent chunk pipeline and dropped upload speed.
PROGRESS_CALLBACK_MIN_GAP: float = 2.0

def make_cancel_aware_progress(
    task_id: str,
    task_queue_ref: "TaskQueue",
    inner: Callable[..., Any],
    throttle: float = PROGRESS_CALLBACK_MIN_GAP,
) -> Callable[..., Any]:
    """
    Wraps a Pyrogram progress callback.

    On cancellation:
      - Does NOT raise CancelledError (FIX 9: prevents post-cancel spam)
      - Silently returns — Pyrogram will call it at most once more, then stop
        when the underlying download/upload task is cancelled externally.

    The actual cancellation is driven by the asyncio.Task.cancel() call in
    TaskQueue.cancel_file(), not by this callback.

    NOTE: Must remain async def — this Pyrogram/Pyrofork build awaits the
    progress callback. A sync callable would return None and `await None`
    raises TypeError, silently crashing downloads inside the retry loop.
    """
    async def _progress(current: int, total: int, *args: Any, **kwargs: Any) -> None:
        if task_queue_ref.is_cancelled(task_id):
            _log("debug", "Progress callback suppressed after cancel", task_id=task_id)
            return

        now = time.monotonic()
        is_final = total > 0 and current >= total
        if not is_final and now - _progress_last_sample.get(task_id, 0) < throttle:
            return
        _progress_last_sample[task_id] = now

        try:
            await inner(current, total)
        except Exception as exc:
            _log("debug", "Progress callback inner error suppressed", task_id=task_id, exc=str(exc))
    return _progress


# =============================================================================
# TaskQueue
# =============================================================================
class TaskQueue:
    """
    Production-grade per-user task queue.

    Lock hierarchy (always acquired in this order to prevent deadlock):
      1. _locks_meta          — only to create/retrieve per-user locks
      2. _locks[user_id]      — all per-user state mutations
      3. _cancel_id_map_lock  — cancel_id_map only; never held with #2

    FIX 4: _file_states has its own dedicated lock (_fs_lock).
            is_cancelled() acquires _fs_lock for reads (no more advisory reads).
    """

    QueueItem = Union[Tuple[str, str, Any, Any], Tuple[str, None, None, None]]
    _MAX_RETRIES: int = 3
    _RETRY_BASE_DELAY: float = 2.0
    _STOP_SENTINEL: Tuple[str, None, None, None] = ("__STOP__", None, None, None)

    def __init__(self) -> None:
        self._queues: Dict[int, Deque[TaskQueue.QueueItem]] = {}
        self._processing: Dict[int, Set[str]] = {}
        self._tasks: Dict[str, asyncio.Task[None]] = {}
        self._status_messages: Dict[str, Tuple[int, int]] = {}

        # FIX 4: _file_states now has its own lock
        self._file_states: Dict[str, Dict[str, Any]] = {}
        self._fs_lock: asyncio.Lock = asyncio.Lock()

        # FIX 11: two module-level locks (meta + cancel_id_map)
        self._locks_meta: asyncio.Lock = asyncio.Lock()
        self._locks: Dict[int, asyncio.Lock] = {}

        self._cancel_id_map_lock: asyncio.Lock = asyncio.Lock()
        self._cancel_id_map: Dict[str, Tuple[str, float]] = {}

        self._active_processor_count: Dict[int, int] = {}
        self._semaphores: Dict[int, asyncio.Semaphore] = {}
        self._concurrency_limits: Dict[int, int] = {}
        self._stats: Dict[int, _UserStats] = {}
        self._to_delete_map: Dict[int, List[int]] = {}

        # Global semaphore — caps total simultaneous tasks across ALL users.
        # Prevents resource exhaustion on Heroku 512MB dynos when 200 users
        # are active.  Per-user semaphores still apply independently.
        self._global_sem: asyncio.Semaphore = asyncio.Semaphore(
            GLOBAL_CONCURRENT_LIMIT
        )

    # -------------------------------------------------------------------------
    # Lock helpers
    # -------------------------------------------------------------------------
    async def _get_lock(self, user_id: int) -> asyncio.Lock:
        async with self._locks_meta:
            if user_id not in self._locks:
                self._locks[user_id] = asyncio.Lock()
            return self._locks[user_id]

    async def add_to_delete(self, user_id: int, message_ids: List[int]) -> None:
        lock = await self._get_lock(user_id)
        async with lock:
            if user_id not in self._to_delete_map:
                self._to_delete_map[user_id] = []
            self._to_delete_map[user_id].extend(message_ids)

    async def pop_to_delete(self, user_id: int) -> List[int]:
        lock = await self._get_lock(user_id)
        async with lock:
            return self._to_delete_map.pop(user_id, [])

    # -------------------------------------------------------------------------
    # cancel_id_map
    # -------------------------------------------------------------------------
    async def register_cancel_id(self, short_id: str, task_id: str) -> None:
        async with self._cancel_id_map_lock:
            self._cancel_id_map[short_id] = (task_id, time.monotonic())

    async def _sweep_cancel_id_map(self) -> None:
        async with self._cancel_id_map_lock:
            now = time.monotonic()
            stale = [k for k, (_, ts) in self._cancel_id_map.items()
                     if now - ts >= CANCEL_ID_TTL]
            for k in stale:
                del self._cancel_id_map[k]

    async def resolve_cancel_id(self, short_id: str) -> Optional[str]:
        async with self._cancel_id_map_lock:
            entry = self._cancel_id_map.get(short_id)
            if not entry:
                return None
            task_id, created = entry
            if time.monotonic() - created >= CANCEL_ID_TTL:
                del self._cancel_id_map[short_id]
                return None
            return task_id

    async def remove_cancel_id(self, short_id: str) -> None:
        async with self._cancel_id_map_lock:
            self._cancel_id_map.pop(short_id, None)

    async def purge_cancel_ids_for_task(self, task_id: str) -> None:
        async with self._cancel_id_map_lock:
            stale = [k for k, (tid, _) in self._cancel_id_map.items() if tid == task_id]
            for k in stale:
                del self._cancel_id_map[k]

    # -------------------------------------------------------------------------
    # FIX 4: File state — reads now under _fs_lock
    # -------------------------------------------------------------------------
    def init_file_state(self, task_id: str, filename: str) -> None:
        # Called synchronously before add_task — task_id is unique and new,
        # so no racing writer exists yet.  No lock needed here.
        self._file_states[task_id] = {
            "cancelled": False, 
            "filename": filename,
            "created_at": time.time()
        }

    async def _set_file_cancelled(self, task_id: str) -> None:
        async with self._fs_lock:
            state = self._file_states.get(task_id)
            if state:
                state["cancelled"] = True

    def is_cancelled(self, task_id: str) -> bool:
        """
        Check cancellation flag without awaiting (called from sync-ish contexts).

        CPython dict reads are GIL-safe for simple value reads, but a concurrent
        .pop() during iteration can raise RuntimeError.  We wrap in try/except
        to handle that rare edge case without crashing the progress callback.
        """
        try:
            state = self._file_states.get(task_id)
            if state and state.get("cancelled"):
                return True
            uid = int(task_id.split(":")[0])
            stats = self._stats.get(uid)
            return bool(stats and stats.cancelled_globally)
        except (ValueError, IndexError, RuntimeError):
            return False

    async def is_cancelled_async(self, task_id: str) -> bool:
        async with self._fs_lock:
            state = self._file_states.get(task_id)
            if state and state.get("cancelled"):
                return True
        try:
            uid = int(task_id.split(":")[0])
        except (ValueError, IndexError):
            return False
        stats = self._stats.get(uid)
        return bool(stats and stats.cancelled_globally)

    def get_filename(self, task_id: str) -> str:
        return self._file_states.get(task_id, {}).get("filename", "Unknown")

    async def _pop_file_state(self, task_id: str) -> None:
        async with self._fs_lock:
            self._file_states.pop(task_id, None)

    async def _sweep_file_states(self, max_age: float = 7200.0) -> None:
        """Prune zombie states and status messages for tasks not in any queue."""
        now = time.time()
        async with self._fs_lock:
            # Gather all currently 'live' task IDs across all users
            live_tasks: Set[str] = set()
            for q in self._queues.values():
                for item in q:
                    if item != self._STOP_SENTINEL:
                        live_tasks.add(item[0])
            for p in self._processing.values():
                live_tasks.update(p)

            stale_states = [
                tid for tid, state in self._file_states.items()
                if tid not in live_tasks and now - state.get("created_at", 0) > max_age
            ]
            for tid in stale_states:
                self._file_states.pop(tid, None)
                self._status_messages.pop(tid, None)
                _log("debug", "Swept zombie file state", task_id=tid)

    async def _sweep_user_stats(self, max_age: float = 7200.0) -> None:
        """Prune user stats for users who have been idle/completed for a long time."""
        now = time.monotonic()
        to_evict: List[int] = []
        
        async with self._locks_meta:
            for uid, stats in list(self._stats.items()):
                # Only evict if they have no tasks and have been silent for max_age
                if not self._queues.get(uid) and not self._processing.get(uid):
                    if now - stats.last_activity > max_age:
                        to_evict.append(uid)
            
            for uid in to_evict:
                self._stats.pop(uid, None)
                self._locks.pop(uid, None)
                self._semaphores.pop(uid, None)
                self._concurrency_limits.pop(uid, None)
                self._queues.pop(uid, None)
                self._processing.pop(uid, None)
                self._active_processor_count.pop(uid, None)
                self._to_delete_map.pop(uid, None)
        
        if to_evict:
            _log("debug", f"Evicted state for {len(to_evict)} idle users")

    # -------------------------------------------------------------------------
    # Status messages
    # -------------------------------------------------------------------------
    def set_status_message(self, task_id: str, chat_id: int, message_id: int) -> None:
        self._status_messages[task_id] = (chat_id, message_id)

    async def _delete_status_message(
        self,
        client: Optional[Client],
        task_id: str,
        entry: Optional[Tuple[int, int]],
    ) -> None:
        if not client or not entry:
            return
        try:
            await asyncio.wait_for(
                client.delete_messages(*entry),
                timeout=STATUS_DELETE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            _log("warning", "delete status msg timed out", task_id=task_id)
        except Exception as exc:
            _log("debug", "delete status msg failed", task_id=task_id, exc=str(exc))

    # -------------------------------------------------------------------------
    # Cancellation
    # -------------------------------------------------------------------------
    async def cancel_file(self, task_id: str, client: Client) -> bool:
        # Atomic read+write under _fs_lock
        async with self._fs_lock:
            state = self._file_states.get(task_id)
            if not state or state.get("cancelled"):
                return False
            state["cancelled"] = True

        # Step 1: Soft-flag FIRST (crash-safe)

        # Step 2: Hard cancel — awaited so Mongo is committed before the user
        # sees the "Cancelled" confirmation in the button callback.
        await job_ledger.mark_cancelled(task_id)

        try:
            uid = int(task_id.split(":")[0])
        except (ValueError, IndexError):
            uid = None

        if uid is not None:
            lock = await self._get_lock(uid)
            async with lock:
                stats = self._stats.get(uid)
                if stats:
                    stats.cancelled_tasks += 1
                t = self._tasks.pop(task_id, None)
            if t and not t.done():
                t.cancel()

        entry = self._status_messages.pop(task_id, None)
        await self._delete_status_message(client, task_id, entry)

        return True

    async def cancel_all(self, user_id: int, client: Optional[Client] = None) -> int:
        status_entries: list[Tuple[str, Tuple[int, int]]] = []
        task_ids_to_cancel: list[str] = []
        lock = await self._get_lock(user_id)
        async with lock:
            stats = self._stats.get(user_id)
            if not stats:
                return 0

            stats.reset_keep_cancelled()

            cancelled = 0

            if user_id in self._queues:
                for item in self._queues[user_id]:
                    if item != self._STOP_SENTINEL:
                        task_id = item[0]
                        task_ids_to_cancel.append(task_id)
                        # Fix 1: Do NOT call mark_cancelled inside the lock.
                        # We collect task_ids and batch-cancel after the lock.
                        if client:
                            entry = self._status_messages.pop(task_id, None)
                            if entry:
                                status_entries.append((task_id, entry))

                real_queued = sum(1 for i in self._queues[user_id] if i != self._STOP_SENTINEL)
                cancelled += real_queued
                self._queues[user_id].clear()
                active = self._active_processor_count.get(user_id, 0)
                for _ in range(active):
                    self._queues[user_id].append(self._STOP_SENTINEL)

            for task_id in list(self._processing.get(user_id, set())):
                task_ids_to_cancel.append(task_id)
                t = self._tasks.pop(task_id, None)
                if t and not t.done():
                    t.cancel()
                    cancelled += 1
                if client:
                    entry = self._status_messages.pop(task_id, None)
                    if entry:
                        status_entries.append((task_id, entry))

        # Mark file_states cancelled outside user lock (FIX 4)
        async with self._fs_lock:
            for task_id in task_ids_to_cancel:
                state = self._file_states.get(task_id)
                if state:
                    state["cancelled"] = True

        # Fix 4: Soft-flag in Mongo FIRST

        for task_id, entry in status_entries:
            await self._delete_status_message(client, task_id, entry)

        # Fix 1: Await all mark_cancelled calls in a gathered batch so Mongo
        # is fully committed before we return the count to the caller.
        if task_ids_to_cancel:
            await asyncio.gather(
                *[job_ledger.mark_cancelled(tid) for tid in task_ids_to_cancel],
                return_exceptions=True,
            )

        _log("info", "cancel_all complete", user_id=user_id, cancelled=cancelled)
        return cancelled

    # -------------------------------------------------------------------------
    # Task submission
    # -------------------------------------------------------------------------
    async def add_task(
        self,
        user_id: int,
        task_id: str,
        file_id: str,
        message: Message,
        coro_func: Callable[[], Any],
    ) -> int:
        if await admin_controls.system_svc.get_admin_mode() and user_id not in ADMINS:
            await message.reply_text(
                "<b><blockquote>ⓘ Aᴅᴍɪɴ ᴍᴏᴅᴇ ᴀᴄᴛɪᴠᴇ - Oɴʟʏ ᴀᴅᴍɪɴs ᴄᴀɴ ǫᴜᴇᴜᴇ ғɪʟᴇs !</b></blockquote>"
            )
            return -1

        lock = await self._get_lock(user_id)
        async with lock:
            if user_id not in self._queues:
                self._queues[user_id] = deque()
                self._processing[user_id] = set()
                self._active_processor_count[user_id] = 0

            if user_id not in self._semaphores:
                limit = CON_LIMIT_ADMIN if user_id in ADMINS else CON_LIMIT_NORMAL
                self._semaphores[user_id] = asyncio.Semaphore(limit)
                self._concurrency_limits[user_id] = limit

            stats = self._stats.get(user_id)
            if stats is None or (
                stats.completion_sent
                and not self._queues[user_id]
                and not self._processing[user_id]
            ):
                self._stats[user_id] = _UserStats()
                stats = self._stats[user_id]

            if stats.start_time is None:
                stats.start_time = time.monotonic()

            stats.cancelled_globally = False
            stats.total_tasks += 1
            stats.last_activity = time.monotonic()
            
            # Bug 3 Fixed: exclude sentinels from position count
            real_queued = sum(1 for i in self._queues[user_id] if i != self._STOP_SENTINEL)
            position = len(self._processing[user_id]) + real_queued + 1
            self._queues[user_id].append((task_id, file_id, message, coro_func))

            active = self._active_processor_count[user_id]
            limit = self._concurrency_limits[user_id]
            queue_len = len(self._queues[user_id])

            # Spawn enough processors to cover every queued item up to the limit.
            to_spawn = max(0, min(queue_len, limit - active))
            
            # INDUSTRIAL PATCH: If we have items but NO active processors,
            # force-spawn one to prevent deadlock from timing races.
            if queue_len > 0 and self._active_processor_count[user_id] <= 0:
                to_spawn = max(1, to_spawn)

            for _ in range(to_spawn):
                self._active_processor_count[user_id] += 1
                asyncio.create_task(
                    self._process_user_queue(user_id),
                    name=f"queue-processor-{user_id}",
                )

        return position

    # -------------------------------------------------------------------------
    # Processor — exits only when queue is empty (atomic exit) or on sentinel
    # -------------------------------------------------------------------------
    async def _process_user_queue(self, user_id: int) -> None:
        _count_decremented = False   # guard against double-decrement in finally
        try:
            while True:
                lock = await self._get_lock(user_id)
                async with lock:
                    queue = self._queues.get(user_id)
                    if not queue:
                        # ── Atomic exit ──────────────────────────────────────
                        # Decrement active count while STILL holding the lock.
                        # This closes the race where add_task() reads active=1,
                        # skips spawning, and the new item sits with no processor:
                        #
                        #   Old:  release lock → (race window) → finally decrements
                        #   New:  decrement THEN release lock → add_task sees 0,
                        #         spawns correctly if there is new work.
                        cnt = self._active_processor_count.get(user_id, 1)
                        self._active_processor_count[user_id] = max(0, cnt - 1)
                        _count_decremented = True
                        _log("debug", "Processor exiting (queue empty)", user_id=user_id)
                        break
                    item = queue.popleft()

                if item == self._STOP_SENTINEL:
                    _log("debug", "Processor received stop sentinel", user_id=user_id)
                    break

                task_id, file_id, message, coro_func = item
                completed_ok = False

                # FIX 17: Dynamic UX - update user if server is full
                status_msg_id = self._status_messages.get(task_id)
                if status_msg_id:
                    chat_id, mid = status_msg_id
                    # If we are blocked by global limit, let the user know
                    if self._global_sem.locked():
                        await _safe_edit_by_id(message._client, chat_id, mid, 
                                               "<b><blockquote>⌛ Sᴇʀᴠᴇʀ ʙᴜsʏ, ᴡᴀɪᴛɪɴɢ ғᴏʀ ᴀ sʟᴏᴛ...</blockquote></b>")

                lock = await self._get_lock(user_id)
                async with lock:
                    self._processing[user_id].add(task_id)

                # Acquire global slot (module-wide limit)
                # Acquisition is wrapped in a try/finally to ensure the item is always 
                # accounted for in _processing and eventually discarded.
                try:
                    # FIX 17: Dynamic UX - update user if server is full
                    status_msg_id = self._status_messages.get(task_id)
                    if status_msg_id:
                        chat_id, mid = status_msg_id
                        if self._global_sem.locked():
                            await _safe_edit_by_id(message._client, chat_id, mid, 
                                                   "<b><blockquote>⌛ Sᴇʀᴠᴇʀ ʙᴜsʏ, ᴡᴀɪᴛɪɴɢ ғᴏʀ ᴀ sʟᴏᴛ...</blockquote></b>")

                    # Acquisition order: Global first, then Per-User.
                    # This prevents one user from hogging per-user slots while waiting for global slots.
                    # Persist context across retries so process_file_core can skip prepare phase
                    attempt_context = {}
                    async with self._global_sem:
                        if status_msg_id:
                            chat_id, mid = status_msg_id
                            await _safe_edit_by_id(message._client, chat_id, mid, 
                                                   "<b><blockquote>📡 Sᴛᴀʙɪʟɪᴢɪɴɢ ᴄᴏɴɴᴇᴄᴛɪᴏɴ...</blockquote></b>")
                        
                        await asyncio.sleep(random.uniform(0.05, 0.20))

                        semaphore = self._semaphores[user_id]
                        async with semaphore:
                            for attempt in range(self._MAX_RETRIES):
                                if self.is_cancelled(task_id):
                                    raise asyncio.CancelledError()
                                try:
                                    attempt_context["attempt"] = attempt
                                    attempt_context["max_retries"] = self._MAX_RETRIES
                                    coro = coro_func(attempt_context)
                                    t: asyncio.Task[None] = asyncio.create_task(coro)
                                    lock = await self._get_lock(user_id)
                                    async with lock:
                                        self._tasks[task_id] = t
                                    # Hard timeout: 1h ceiling regardless of file size
                                    try:
                                        await asyncio.wait_for(t, timeout=TASK_HARD_TIMEOUT)
                                    except asyncio.TimeoutError:
                                        t.cancel()
                                        with suppress(asyncio.CancelledError):
                                            await t
                                        raise
                                    
                                    completed_ok = True
                                    break

                                except asyncio.TimeoutError:
                                    # Hard timeout hit — cancel the shielded task
                                    _log(
                                        "error",
                                        "Task hard timeout",
                                        task_id=task_id,
                                        limit_h=TASK_HARD_TIMEOUT / 3600,
                                    )
                                    await self._handle_failure(
                                        message, file_id,
                                        TimeoutError("Task exceeded 1h hard limit"),
                                        attempt=attempt + 1
                                    )
                                    break

                                except FloodWait as e:
                                    # FIX 9 from v2: jitter prevents thundering herd
                                    jitter = random.uniform(1.0, 3.0)
                                    delay = e.value + jitter
                                    _log(
                                        "warning",
                                        "FloodWait",
                                        task_id=task_id,
                                        wait=e.value,
                                        jitter=f"{jitter:.1f}",
                                        attempt=attempt + 1,
                                    )
                                    await asyncio.sleep(delay)

                                except asyncio.CancelledError:
                                    raise

                                except Exception as exc:
                                    _log(
                                        "error",
                                        "Task attempt failed",
                                        task_id=task_id,
                                        attempt=attempt + 1,
                                        exc=str(exc),
                                    )
                                    # Fatal errors: no point retrying
                                    if not _is_retryable(exc):
                                        await self._handle_failure(message, file_id, exc, attempt=attempt + 1)
                                        break
                                    if attempt == self._MAX_RETRIES - 1:
                                        await self._handle_failure(message, file_id, exc, attempt=attempt + 1)
                                    else:
                                        # Exponential backoff, capped at 30 s
                                        backoff = min(
                                            self._RETRY_BASE_DELAY * (2 ** attempt),
                                            30.0,
                                        )
                                        _log(
                                            "info",
                                            "Retrying after backoff",
                                            task_id=task_id,
                                            backoff=f"{backoff:.1f}s",
                                        )
                                        await asyncio.sleep(backoff)

                except asyncio.CancelledError:
                    _log("info", "Task cancelled", task_id=task_id)
                    completed_ok = False

                finally:
                    lock = await self._get_lock(user_id)
                    async with lock:
                        self._processing[user_id].discard(task_id)
                        self._tasks.pop(task_id, None)
                    await self._pop_file_state(task_id)
                    self._status_messages.pop(task_id, None)

                if completed_ok:
                    await self._on_task_completed(user_id, message)

        except Exception as exc:
            _log("error", "Processor crashed", user_id=user_id, exc=str(exc), exc_info=True)
        finally:
            # Only decrement here if the atomic exit path didn't already do it.
            # Covers crashes and CancelledError paths where the loop didn't exit cleanly.
            if not _count_decremented:
                lock = await self._get_lock(user_id)
                async with lock:
                    cnt = self._active_processor_count.get(user_id, 1)
                    self._active_processor_count[user_id] = max(0, cnt - 1)
            
            # WATCHDOG: If the processor is exiting (due to crash or clean exit)
            # but there are STILL items in the queue and NO other processors
            # are alive, respawn a recovery processor.
            try:
                lock = await self._get_lock(user_id)
                async with lock:
                    queue_has_items = bool(self._queues.get(user_id))
                    active_now = self._active_processor_count.get(user_id, 0)
                    if queue_has_items and active_now <= 0:
                        self._active_processor_count[user_id] = 1
                        asyncio.create_task(
                            self._process_user_queue(user_id),
                            name=f"queue-recovery-{user_id}",
                        )
                        _log("warning", "Recovered dead processor", user_id=user_id)
            except Exception as watchdog_exc:
                _log("error", "Queue watchdog failed", user_id=user_id, exc=str(watchdog_exc))

            _log("debug", "Processor exited", user_id=user_id)

    # -------------------------------------------------------------------------
    # FIX 2: Completion tracking — cancel vs completion race fully closed
    # -------------------------------------------------------------------------
    async def _on_task_completed(self, user_id: int, message: Message) -> None:
        lock = await self._get_lock(user_id)
        async with lock:
            stats = self._stats.get(user_id)
            if stats is None:
                return

            stats.successful_tasks += 1
            stats.completed_tasks += 1
            stats.last_activity = time.monotonic()

            all_done = (
                not self._queues.get(user_id)
                and not self._processing.get(user_id)
            )
            if not all_done:
                return
            if stats.cancelled_globally:
                stats.reset()
                return
            if stats.successful_tasks == 0 or stats.completion_sent:
                return

            # Claim send slot atomically.
            stats.completion_sent = True
            total = stats.successful_tasks
            elapsed = int(time.monotonic() - stats.start_time) if stats.start_time else 0

            # FIX 2: Re-check cancel flag AFTER claiming the slot.
            # Scenario: cancel_all() sets cancelled_globally between the
            # "all_done" check above and the slot claim.  Without this re-check
            # the completion message would still fire.
            if stats.cancelled_globally:
                stats.reset()
                _log("info", "Completion suppressed — cancel arrived during claim", user_id=user_id)
                return

        # Network I/O intentionally outside lock.
        await self._send_completion_message(user_id, message, total, elapsed)

    async def _send_completion_message(
        self, user_id: int, message: Message, total: int, elapsed_seconds: int
    ) -> None:
        """
        Final summary message sent when a user's entire queue is completed.
        Handles token status, auto-delete scheduling, and completion stickers.
        """
        sticker_msg = None
        try:
            # Bug 4 Fixed: Floor to 1s so "0s" never appears
            elapsed_seconds = max(elapsed_seconds, 1)

            # 1. Calculate time metrics
            h, rem = divmod(elapsed_seconds, 3600)
            m, s = divmod(rem, 60)
            time_taken = (
                f"{h}h {m}m {s}s" if h else
                f"{m}m {s}s" if m else
                f"{s}s"
            )
            # Bug 3 Fixed: For single task, avg == total; for multiple, divide
            avg_s = (elapsed_seconds // total) if total > 0 else elapsed_seconds
            avg_time = f"{avg_s // 60}m {avg_s % 60:02d}s"

            # 2. Fire sticker AND all DB fetches in a single gather to minimize latency
            try:
                _gathered = await asyncio.gather(
                    message._client.send_sticker(
                        message.chat.id,
                        "CAACAgUAAxkBAAEQZpFpf8GpRaMZyHBgl9c5ki8eRjxBTgACww4AAhvZ6VcmysBaPL8rEjgE",
                    ),
                    DARKXSIDE78.get_user_prefs_batch(user_id),
                    DARKXSIDE78.get_variable("short_enabled", False),
                    DARKXSIDE78.get_variable("short_mode", "link"),
                    DARKXSIDE78.get_auto_delete(),
                    DARKXSIDE78.get_del_timer(),
                    return_exceptions=True,
                )
                sticker_msg   = _gathered[0] if not isinstance(_gathered[0], Exception) else None
                user_data     = _gathered[1] if not isinstance(_gathered[1], Exception) else {}
                short_enabled = _gathered[2] if not isinstance(_gathered[2], Exception) else False
                short_mode    = _gathered[3] if not isinstance(_gathered[3], Exception) else "link"
                auto_del      = _gathered[4] if not isinstance(_gathered[4], Exception) else False
                del_timer     = _gathered[5] if not isinstance(_gathered[5], Exception) else 600
            except Exception as gather_exc:
                _log("error", "Completion gather failure", user_id=user_id, exc=str(gather_exc))
                sticker_msg, user_data, short_enabled, short_mode, auto_del, del_timer = None, {}, False, "link", False, 600

            # 3. Token and Premium status
            used_tokens = 0
            remaining_tokens = "∞"
            is_premium = bool(user_data.get("is_premium", False))
            
            if not is_premium:
                if short_enabled:
                    if short_mode == "link":
                        used_tokens = total
                        current_balance = await DARKXSIDE78.get_token(user_id)
                        remaining_tokens = str(current_balance)
                    else:
                        used_tokens = 0
                        verify_expiry = user_data.get("verify_expiry")
                        if isinstance(verify_expiry, str):
                            try:
                                verify_expiry = datetime.fromisoformat(verify_expiry)
                            except ValueError:
                                verify_expiry = None
                        
                        if verify_expiry:
                            if verify_expiry.tzinfo is None:
                                verify_expiry = verify_expiry.replace(tzinfo=pytz.utc)
                        
                        if verify_expiry and datetime.now(pytz.utc) <= verify_expiry:
                            remaining_tokens = "Vᴇʀɪғɪᴇᴅ ✓"
                        else:
                            remaining_tokens = "Uɴᴠᴇʀɪғɪᴇᴅ ✗"

            # 4. Auto-delete notice
            autodel_notice = ""
            if auto_del:
                dm, ds = divmod(del_timer, 60)
                dh, dm = divmod(dm, 60)
                time_str = f"{dh}h {dm}m {ds}s" if dh else f"{dm}m {ds}s" if dm else f"{ds}s"
                autodel_notice = f"\n\n<b><blockquote><i>(Aᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ: Fɪʟᴇs ᴡɪʟʟ ʙᴇ ʀᴇᴍᴏᴠᴇᴅ ɪɴ {time_str})</i></blockquote></b>"

            # 5. Send main summary message
            summary_text = (
                f"<b><blockquote>≡ Aʟʟ Tᴀsᴋs Rᴇɴᴀᴍᴇ Cᴏᴍᴘʟᴇᴛᴇᴅ...</blockquote></b>\n\n"
                f"<b>◍ ᴛᴏᴛᴀʟ ᴛᴀsᴋs : « {total} »</b>\n"
                f"<b>◍ ᴛᴏᴛᴀʟ ᴛɪᴍᴇ ᴛᴀᴋᴇɴ : « {time_taken} »</b>\n"
                f"<b>◍ ᴀᴠᴇʀᴀɢᴇ ᴛɪᴍᴇ ᴘᴇʀ ᴛᴀsᴋ : « {avg_time} »</b>\n\n"
                f"<b>◍ ᴛᴏᴛᴀʟ ᴛᴏᴋᴇɴs ᴜsᴇᴅ : « {used_tokens} »</b>\n"
                f"<b>◍ ᴛᴏᴋᴇɴs ʀᴇᴍᴀɪɴɪɴɢ : « {remaining_tokens} »</b>"
                f"{autodel_notice}"
            )
            
            # Bug 2 Fixed: use _safe_reply (from helper.utils), guard comp_msg before .id access
            comp_msg = await _safe_reply(message, summary_text)
            _log("info", "Completion message sent", user_id=user_id, total=total)

            # 6. Schedule batch deletion
            if auto_del:
                try:
                    message_ids = await self.pop_to_delete(user_id)
                    if sticker_msg:
                        message_ids.append(sticker_msg.id)
                    if comp_msg:
                        message_ids.append(comp_msg.id)
                    
                    if message_ids:

                        await DARKXSIDE78.schedule_auto_delete(
                            chat_id=message.chat.id,
                            message_ids=message_ids,
                            delay=del_timer
                        )
                        _log("info", f"Scheduled {len(message_ids)} messages for auto-delete", user_id=user_id)
                except Exception as del_exc:
                    _log("warning", "auto_delete schedule failed", user_id=user_id, exc=str(del_exc))

        except Exception as exc:
            _log("error", "_send_completion_message critical failure", user_id=user_id, exc=str(exc))

    async def _handle_failure(

        self, message: Message, file_id: str, error: Exception, attempt: int = 0
    ) -> None:
        error_msg = f"❌ <b>Fᴀɪʟᴇᴅ Aғᴛᴇʀ {attempt} Aᴛᴛᴇᴍᴘᴛs</b>\n\n<b>Fɪʟᴇ ID:</b> <code>{file_id}</code>\n<b>Eʀʀᴏʀ:</b> <code>{str(error)[:200]}</code>"
        await _safe_reply(message, error_msg)

    async def get_queue_status(self, user_id: int) -> Dict[str, int]:
        lock = await self._get_lock(user_id)
        async with lock:
            # Bug 1 Fixed: Exclude sentinels from queued count
            q = sum(1 for i in self._queues.get(user_id, deque()) if i != self._STOP_SENTINEL)
            p = len(self._processing.get(user_id, set()))
            limit = self._concurrency_limits.get(user_id, CON_LIMIT_NORMAL)
            return {"queued": q, "processing": p, "total": q + p, "limit": limit}


    def get_concurrency_limit(self, user_id: int) -> int:
        return self._concurrency_limits.get(user_id, CON_LIMIT_NORMAL)

    async def get_accurate_position(self, user_id: int, task_id: str) -> int:
        """
        Return the 1-based queue position for a specific task.
        If the file is currently processing, returns 0 (i.e. "now processing").
        If not found, returns -1.
        """
        lock = await self._get_lock(user_id)
        async with lock:
            # Check if currently processing
            if task_id in self._processing.get(user_id, set()):
                return 0
            # Find position in queue
            queue = self._queues.get(user_id, deque())
            for idx, (tid, _, _, _) in enumerate(queue, 1):
                if tid == task_id:
                    return idx
        return -1

    async def get_global_stats(self) -> Dict[str, int]:
        """
        Return aggregate stats across ALL users.
        No await points inside the loop — safe under asyncio's single-thread model.
        Per-user errors are swallowed so one bad entry doesn't kill the whole reply.
        """
        total_queued = 0
        total_processing = 0
        total_users = 0
        for uid in list(self._queues.keys()):
            try:
                lock = await self._get_lock(uid)
                async with lock:
                    q = len(self._queues.get(uid, deque()))
                    p = len(self._processing.get(uid, set()))
                if q + p > 0:
                    total_users += 1
                total_queued += q
                total_processing += p
            except Exception:
                pass  # stale uid — skip silently
        return {
            "queued": total_queued,
            "processing": total_processing,
            "total": total_queued + total_processing,
            "active_users": total_users,
            "global_limit": GLOBAL_CONCURRENT_LIMIT,
        }

    # Removed duplicate Maintenance / Memory Management sweeper definitions



# =============================================================================
# Singleton
# =============================================================================
class TaskQueueProxy:
    def __init__(self):
        self._instance = None
    
    def __getattr__(self, name):
        if self._instance is None:
            try:
                self._instance = TaskQueue()
            except Exception as exc:
                raise RuntimeError(f"TaskQueue failed to initialise: {exc}") from exc

        return getattr(self._instance, name)

    def __repr__(self):
        return f"<TaskQueueProxy instance={self._instance}>"

task_queue = TaskQueueProxy()


# =============================================================================
# Command handlers
# =============================================================================

@Client.on_message((filters.group | filters.private) & filters.command("queue"))
@check_ban_status
async def queue_status(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    try:
        status = await task_queue.get_queue_status(user_id)
        gstats = await task_queue.get_global_stats()
        # Use the limit from status directly to avoid redundant lock acquisition
        limit = status.get("limit", task_queue.get_concurrency_limit(user_id))
        await _safe_reply(
            message,
            (
                f"<blockquote>✦ 𝗬𝗢𝗨𝗥 𝗤𝗨𝗘𝗨𝗘</blockquote>\n"
                f"<b><blockquote>┌ Pʀᴏᴄᴇssɪɴɢ :</b> <code>{status['processing']}/{limit}</code> ғɪʟᴇs\n"
                f"<b>├ Wᴀɪᴛɪɴɢ :</b> <code>{status['queued']}</code> ғɪʟᴇs\n"
                f"<b>└ Tᴏᴛᴀʟ :</b> <code>{status['total']}</code> ғɪʟᴇs</blockquote>\n"
                f"<blockquote>✦ 𝗦𝗘𝗥𝗩𝗘𝗥 𝗟𝗢𝗔𝗗</blockquote>\n"
                f"<b><blockquote>┌ Aᴄᴛɪᴠᴇ Tᴀsᴋs :</b> <code>{gstats['processing']}/{gstats['global_limit']}</code>\n"
                f"<b>├ Qᴜᴇᴜᴇᴅ :</b> <code>{gstats['queued']}</code>\n"
                f"<b>└ Aᴄᴛɪᴠᴇ Usᴇʀs :</b> <code>{gstats['active_users']}</code></blockquote>\n"
                f"<b><blockquote>ⓘ Usᴇ <code>/cancel</code> ᴛᴏ ᴄᴀɴᴄᴇʟ ᴀʟʟ ᴛᴀsᴋs</b></blockquote>"
            ),
        )
    except Exception as exc:
        _log("error", "queue_status handler failed", user_id=user_id, exc=str(exc))
        await _safe_reply(message, "<blockquote>✗ <b>Fᴀɪʟᴇᴅ ᴛᴏ ꜰᴇᴛᴄʜ ǫᴜᴇᴜᴇ sᴛᴀᴛᴜs.</b></blockquote>")



@Client.on_message((filters.group | filters.private) & filters.command("cancel"))
@check_ban_status
async def cancel_queue(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    code = random.randint(100_000, 999_999)
    await _state.set_verify(user_id, {"code": code, "created_at": time.time()})
    await message.reply_text(
        f"<blockquote>ⓘ 𝗖𝗢𝗡𝗙𝗜𝗥𝗠 𝗖𝗔𝗡𝗖𝗘𝗟𝗟𝗔𝗧𝗜𝗢𝗡</blockquote>\n\n"
        f"◍  Aʀᴇ ʏᴏᴜ ᴀʙsᴏʟᴜᴛᴇʟʏ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ᴄᴀɴᴄᴇʟ ᴀɴᴅ ʀᴇᴍᴏᴠᴇ <b>ᴀʟʟ</b> ᴀᴄᴛɪᴠᴇ ᴛᴀꜱᴋꜱ ꜰʀᴏᴍ ᴛʜᴇ ꜱʏꜱᴛᴇᴍ ?\n\n"
        f"<blockquote>➥ Pʟᴇᴀsᴇ ᴛʏᴘᴇ ᴛʜɪs ᴄᴏᴅᴇ : <code>{code}</code></blockquote>\n"
        f"<blockquote>⧗ Exᴘɪʀᴇs ɪɴ {int(CANCEL_CODE_TTL)} sᴇᴄᴏɴᴅs</blockquote>",
        parse_mode=ParseMode.HTML,
    )


# LOW FIX: add filters.private so group messages never trigger a pending
# cancel-code lookup for an unrelated user in the same chat.
@Client.on_message(
    filters.private & filters.text & ~filters.regex(r"^/")
)
async def verify_cancel_code(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    if message.text.startswith("/"):
        return
    user_id = message.from_user.id

    # FIX 10: get_verify evicts stale entries on every call
    data = await _state.get_verify(user_id)
    if not data:
        return

    if time.time() - data["created_at"] > VERIFY_CODE_TTL:
        await _state.pop_verify(user_id)
        await message.reply_text(
            "<blockquote>⧗ <b>Vᴇʀɪғɪᴄᴀᴛɪᴏɴ ᴇxᴘɪʀᴇᴅ.</b></blockquote>\n"
            "<blockquote>◍ Pʟᴇᴀsᴇ ᴜsᴇ <code>/cancel</code> ᴀɢᴀɪɴ.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    if not message.text.isdigit() or int(message.text) != data["code"]:
        await _state.pop_verify(user_id)
        await message.reply_text(
            "<blockquote>✗ <b>Iɴᴠᴀʟɪᴅ ᴄᴏᴅᴇ.</b></blockquote>\n<blockquote>◍ Cᴀɴᴄᴇʟʟᴀᴛɪᴏɴ ᴀʙᴏʀᴛᴇᴅ.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    await _state.pop_verify(user_id)
    cancelled = await task_queue.cancel_all(user_id, client)
    await message.reply_text(
        f"<blockquote>✓ <b>Cᴀɴᴄᴇʟʟᴇᴅ {cancelled} ᴛᴀsᴋs !</b></blockquote>"
        if cancelled > 0
        else "<blockquote>ⓘ <b>Nᴏ ᴛᴀsᴋs ᴛᴏ ᴄᴀɴᴄᴇʟ.</b></blockquote>",
        parse_mode=ParseMode.HTML,
    )


# =============================================================================
# Media utilities
# =============================================================================

QUALITY_ORDER = {
    "2160p": 1, "1440p": 2, "1080p": 3, "800p": 4, 
    "720p": 5, "480p": 6, "360p": 7, "240p": 8, "144p": 9
}

def detect_quality(file_name: str) -> int:
    try:
        m = re.search(
            r"(144p|240p|360p|480p|720p|800p|1080p|1440p|2160p)",
            file_name, re.IGNORECASE,
        )
        return QUALITY_ORDER.get(m.group(1).lower(), 8) if m else 9
    except Exception:
        return 9


async def _run_ffmpeg(
    cmd: List[str],
    timeout: float = 300.0,
    is_audio_only: bool = False,
) -> Tuple[int, str]:
    """
    Hardened FFmpeg execution with global concurrency cap, timeout, and
    strict process cleanup.

    is_audio_only: when True, uses a shorter semaphore wait (10s) and bypasses
    the semaphore entirely rather than failing — audio-only encodes are lightweight
    and should never queue behind heavy video tasks.
    """
    p: Optional[asyncio.subprocess.Process] = None
    _sem_acquired = False
    sem = _get_ffmpeg_sem()
    try:
        # We queue for a slot rather than bypassing to prevent CPU oversubscription.
        _sem_wait = min(timeout, 60.0)
        try:
            await asyncio.wait_for(sem.acquire(), timeout=_sem_wait)
            _sem_acquired = True
        except asyncio.TimeoutError:
            return -1, "TIMED_OUT_WAITING_FOR_SEM"
        try:
            t0 = time.monotonic()
            p = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr_b = await asyncio.wait_for(p.communicate(), timeout=timeout)
            duration = time.monotonic() - t0
            _log("info", f"ffmpeg command took {duration:.1f}s", cmd=" ".join(cmd[:3]))
            return p.returncode, stderr_b.decode(errors="ignore").strip()
        except asyncio.TimeoutError:
            await _kill_proc(p)
            return -1, "TIMED_OUT"
        except asyncio.CancelledError:
            await _kill_proc(p)
            raise
        except Exception as exc:
            _log("error", "_run_ffmpeg failed", exc=str(exc))
            return -1, str(exc)
        finally:
            if _sem_acquired:
                sem.release()
    except asyncio.CancelledError:
        await _kill_proc(p)
        raise


async def _extract_audio_to_pcm(
    ffmpeg_bin: str,
    src_path: str,
    output_dir: str,
    track_idx: int,
    tolerant: bool,
) -> Optional[str]:
    """
    Extract one audio track from src_path to a raw PCM WAV file.

    PCM extraction is a pure decode (no encoding) so it runs at 50-200x realtime
    even on throttled CPU. Subsequent encoding from WAV is faster because:
      - No container demuxer overhead on every encode pass
      - File is seekable and pre-normalized to 48kHz PCM
    """
    pcm_path = os.path.join(output_dir, f"_pcm_{uuid.uuid4().hex[:8]}_t{track_idx}.wav")
    cmd = [
        ffmpeg_bin, "-hide_banner", "-nostdin", "-threads", "1",
        "-analyzeduration", "100000",       # 100ms max probe (was unlimited)
        "-probesize", "500000",             # 500KB max probe (was 5MB)
        *_ffmpeg_input_tolerance_flags(tolerant),
        "-i", src_path,
        "-map", f"0:a:{track_idx}",
        "-vn", "-sn", "-dn",
        "-acodec", "pcm_s16le",            # raw PCM — pure decode, zero encode cost
        "-ar", "48000",                    # normalize sample rate now, not during encode
        "-y", pcm_path,
    ]
    ret, err = await _run_ffmpeg(cmd, timeout=300, is_audio_only=True)
    if ret != 0 or not os.path.exists(pcm_path):
        _log("warning", "_extract_audio_to_pcm failed", track=track_idx, err=err[:200])
        with suppress(Exception):
            await aiofiles.os.remove(pcm_path)
        return None
    return pcm_path


async def _extract_audio_stream_copy(
    ffmpeg_bin: str,
    src_path: str,
    output_dir: str,
    track_idx: int,
    tolerant: bool,
) -> Optional[str]:
    """
    Stream-copy extract one audio track from src_path to a tiny .mka file.

    Near-instant (moves packets, zero decode/encode). The output file is
    an audio-only container — no video demux overhead in subsequent encode.

    Replaces _extract_audio_to_pcm entirely.
    """
    out_path = os.path.join(
        output_dir,
        f"_aud_raw_{uuid.uuid4().hex[:8]}_t{track_idx}.mka",
    )
    cmd = [
        ffmpeg_bin, "-hide_banner", "-nostdin",
        "-threads", "1",                      # no threadpool overhead for copy
        "-analyzeduration", "100000",
        "-probesize", "500000",
        *_ffmpeg_input_tolerance_flags(tolerant),
        "-i", src_path,
        "-map", f"0:a:{track_idx}",
        "-vn", "-sn", "-dn",
        "-c:a", "copy",                       # pure stream copy — instant
        "-y", out_path,
    ]
    ret, err = await _run_ffmpeg(cmd, timeout=120, is_audio_only=True)
    if ret != 0 or not os.path.exists(out_path):
        _log("warning", "_extract_audio_stream_copy failed",
             track=track_idx, err=err[:200])
        with suppress(Exception):
            await aiofiles.os.remove(out_path)
        return None
    size = os.path.getsize(out_path)
    if size == 0:
        with suppress(Exception):
            await aiofiles.os.remove(out_path)
        return None
    _log("debug", "_extract_audio_stream_copy ok",
         track=track_idx, bytes=size)
    return out_path


async def _run_ffprobe(args: list[str], timeout: float = 15.0) -> Optional[bytes]:
    ffprobe = _find_ffprobe()
    if not ffprobe:
        _log("error", "ffprobe binary not found")
        return None
    proc: Optional[asyncio.subprocess.Process] = None
    try:
        async with _get_ffprobe_sem():
            proc = await asyncio.create_subprocess_exec(
                ffprobe, *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            if proc.returncode != 0:
                return None
            return stdout
    except asyncio.TimeoutError:
        # FIX 8: guarded kill + wait
        await _kill_proc(proc)
        return None
    except asyncio.CancelledError:
        await _kill_proc(proc)
        raise
    except Exception as exc:
        _log("error", "_run_ffprobe failed", exc=str(exc))
        return None


# FIX 8: Safe subprocess termination — never zombies
async def _kill_proc(proc: Optional[asyncio.subprocess.Process]) -> None:
    if proc is None:
        return
    try:
        proc.kill()
    except ProcessLookupError:
        return
    except Exception as exc:
        _log("warning", "proc.kill() failed", exc=str(exc))
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=PROC_KILL_WAIT_TIMEOUT)
    except asyncio.TimeoutError:
        _log("error", "proc.wait() timed out after kill — process may be zombie")
    except Exception as exc:
        _log("warning", "proc.wait() error after kill", exc=str(exc))


async def _safe_move(src: str, dst: str) -> bool:
    """Hardened atomic move with cross-device fallback."""
    if not src or not dst:
        return False
    try:
        # 1. Try atomic rename (fastest, intra-filesystem)
        await aiofiles.os.rename(src, dst)
        return True
    except (OSError, Exception):
        try:
            # 2. Fallback to shutil.move (cross-filesystem copy+delete)
            await asyncio.to_thread(shutil.move, src, dst)
            return True
        except Exception as exc:
            _log("error", "_safe_move failed", src=src, dst=dst, exc=str(exc))
            return False



_MEDIA_INFO_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}

_PROBEABLE_EXTS = {
    ".mp4", ".mkv", ".webm", ".mov", ".avi", ".flv", ".ts", ".m4v",
    ".mp3", ".m4a", ".aac", ".flac", ".ogg", ".opus", ".wav",
}

async def extract_media_info_cached(file_path: str, file_id: str, file_size: int) -> Dict[str, Any]:
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _PROBEABLE_EXTS:
        return {
            "video_codec": "", "audio_codec": "", "audio_layout": "",
            "audio_languages": [], "subtitle_languages": [], "all_languages": [],
            "resolution": "", "audio_tracks_count": 0, "subtitle_tracks_count": 0,
            "has_errors": False, "streams": [], "duration_seconds": 0,
            "video_width": 0, "video_height": 0,
        }
    
    cache_key = f"{file_id}:{file_size}"
    now = time.monotonic()
    
    if cache_key in _MEDIA_INFO_CACHE:
        expire_time, info = _MEDIA_INFO_CACHE[cache_key]
        if now < expire_time:
            return info
        else:
            del _MEDIA_INFO_CACHE[cache_key]

    info = await extract_media_info(file_path)
    if not info.get("has_errors"):
        _MEDIA_INFO_CACHE[cache_key] = (now + 3600, info)
    return info


async def extract_media_info(file_path: str) -> Dict[str, Any]:
    """
    Perform a single ffprobe call to extract structured media metadata.
    Returns a dict with resolution, codecs, layouts, and languages.
    """
    out = await _run_ffprobe(
        ["-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", file_path]
    )
    res = {
        "video_codec": "", "audio_codec": "", "audio_layout": "",
        "audio_languages": [], "subtitle_languages": [], "all_languages": [],
        "resolution": "", "audio_tracks_count": 0, "subtitle_tracks_count": 0,
        "has_errors": False, "streams": [], "duration_seconds": 0,
        "video_width": 0, "video_height": 0,
    }
    if not out:
        res["has_errors"] = True
        return res

    try:
        info = json.loads(out.decode(errors="ignore"))
        streams = info.get("streams", [])
        res["streams"] = streams

        
        # 1. Video
        v_streams = [s for s in streams if s.get("codec_type") == "video"]
        if v_streams:
            v0 = v_streams[0]
            res["video_codec"] = v0.get("codec_name", "").upper().replace("H264", "H264").replace("H265", "H265")
            res["video_width"] = int(v0.get("width", 0) or 0)
            res["video_height"] = int(v0.get("height", 0) or 0)
            h = v0.get("height", 0)
            if isinstance(h, int) and h > 0:
                for threshold, label in ((2160, "2160p"), (1440, "1440p"), (1080, "1080p"), (720, "720p"), (480, "480p")):
                    if h >= threshold:
                        res["resolution"] = label
                        break
                if not res["resolution"]: res["resolution"] = f"{h}p"

        # 2. Audio
        a_streams = [s for s in streams if s.get("codec_type") == "audio"]
        res["audio_tracks_count"] = len(a_streams)
        if a_streams:
            a0 = a_streams[0]
            res["audio_codec"] = a0.get("codec_name", "").upper()
            channels = a0.get("channels", 0)
            if channels == 2: res["audio_layout"] = "2.0"
            elif channels == 6: res["audio_layout"] = "5.1"
            elif channels == 8: res["audio_layout"] = "7.1"
            
            for s in a_streams:
                _tags = s.get("tags", {})
                lang = _tags.get("language") or _tags.get("LANGUAGE")
                if lang:
                    norm = normalize_lang_code(lang)
                    if norm and norm not in res["audio_languages"]:
                        res["audio_languages"].append(norm)

        # 3. Subtitles
        s_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
        res["subtitle_tracks_count"] = len(s_streams)
        for s in s_streams:
            _tags = s.get("tags", {})
            lang = _tags.get("language") or _tags.get("LANGUAGE")
            if lang:
                norm = normalize_lang_code(lang)
                if norm and norm not in res["subtitle_languages"]:
                    res["subtitle_languages"].append(norm)

        # 4. Merged languages
        res["all_languages"] = res["audio_languages"][:]
        for sl in res["subtitle_languages"]:
            if sl not in res["all_languages"]:
                res["all_languages"].append(sl)

        try:
            duration_raw = info.get("format", {}).get("duration")
            if duration_raw is not None:
                res["duration_seconds"] = max(0, int(float(duration_raw)))
        except Exception:
            pass

    except Exception as exc:
        _log("error", "extract_media_info parsing failed", exc=str(exc))
        res["has_errors"] = True
    return res


def get_audio_label_from_info(info: Dict[str, Any], release_flags: str = "") -> str:
    audio_count = info.get("audio_tracks_count", 0)
    sub_count = info.get("subtitle_tracks_count", 0)
    
    if audio_count >= 4: return "Multi"
    if audio_count == 3: return "Tri"
    if audio_count == 2: return "Dual"
    
    # Fallback to parser flags if single audio
    if release_flags in {"Sub", "Dub", "Multi-Sub"}:
        return "Sub" if release_flags == "Multi-Sub" else release_flags
        
    return info.get("audio_codec", "Sub")


# =============================================================================
# File utilities
# =============================================================================

async def process_thumbnail(
    thumb_path: str,
    media_type: str = "document",
    timeout: int = 30,
) -> Optional[str]:
    if not thumb_path or not isinstance(thumb_path, str):
        return None
    if not await aiofiles.os.path.exists(thumb_path):
        return None
    if media_type not in ("video", "document"):
        media_type = "document"

    def _sync() -> Optional[str]:
        try:
            with Image.open(thumb_path) as img:
                # Better transparency handling: paste onto white background
                if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                    background = Image.new("RGB", img.size, (255, 255, 255))
                    if img.mode == "P":
                        img = img.convert("RGBA")
                    background.paste(img, mask=img.split()[-1])
                    img = background
                elif img.mode != "RGB":
                    img = img.convert("RGB")

                # Telegram allows HD thumbnails (e.g. 1280px or 1024px) as long as 
                # the file size is strictly under 200 KB (200,000 bytes).
                # We iterate through a priority list of combinations to find 
                # the absolute highest resolution and quality that fits.
                tmp_save = thumb_path + ".tmp.jpg"
                
                configs = [
                    ((1280, 1280), 95, 0), # Pristine HD
                    ((1280, 1280), 85, 0), # Good HD
                    ((1024, 1024), 90, 0), # Very High Q HD
                    ((1024, 1024), 80, 1), # Standard HD
                    ((800, 800), 85, 0),   # High Res
                    ((800, 800), 75, 1),   # Standard Res
                    ((720, 720), 80, 0),   # 720p
                    ((480, 480), 85, 0),   # DVD Res
                    ((320, 320), 90, 0),   # Telegram default size
                ]
                
                for size, q, sub in configs:
                    curr_img = img.copy()
                    if curr_img.width > size[0] or curr_img.height > size[1]:
                        curr_img.thumbnail(size, Image.Resampling.LANCZOS)
                        
                    curr_img.save(
                        tmp_save,
                        "JPEG",
                        quality=q,
                        optimize=True,
                        progressive=True,
                        subsampling=sub
                    )
                    
                    # Telegram strictly enforces 200KB (204,800 bytes)
                    if os.path.getsize(tmp_save) <= 200_000:
                        break
                
            if os.path.exists(tmp_save):
                if os.path.exists(thumb_path):
                    os.remove(thumb_path)
                os.rename(tmp_save, thumb_path)
                return thumb_path
            return None
        except Exception as exc:
            _log("error", "process_thumbnail sync failure", exc=str(exc))
            return None

    try:
        result = await asyncio.wait_for(asyncio.to_thread(_sync), timeout=timeout)
        if not result:
            await cleanup_files(thumb_path)
            return None
        return result
    except asyncio.TimeoutError:
        await cleanup_files(thumb_path)
        return None
    except Exception as exc:
        _log("error", "process_thumbnail", exc=str(exc))
        await cleanup_files(thumb_path)
        return None


async def _prepare_thumb_path(task_id: str, prefix: str) -> str:
    # Use abspath so the directory is always resolved relative to the
    # process CWD, not whatever the caller's working directory happens to be.
    thumb_dir = os.path.abspath(os.path.join("thumbs", str(task_id)))
    await aiofiles.os.makedirs(thumb_dir, exist_ok=True)
    return os.path.join(thumb_dir, f"{prefix}_{uuid.uuid4().hex}.jpg")


async def _download_thumb_file(
    client: Client,
    source: Any,
    target_path: str,
    *,
    user_id: int,
    label: str,
) -> Optional[str]:
    try:
        downloaded = await client.download_media(source, file_name=target_path)
        if not downloaded:
            return None
        if not os.path.exists(downloaded):
            _log("warning", "thumbnail download missing on disk", user_id=user_id, label=label, path=downloaded)
            return None
        size = os.path.getsize(downloaded)
        if size <= 0:
            _log("warning", "thumbnail download empty", user_id=user_id, label=label, path=downloaded)
            await cleanup_files(downloaded)
            return None
        return downloaded
    except Exception as exc:
        _log("warning", "thumbnail download failed", user_id=user_id, label=label, exc=str(exc))
        await cleanup_files(target_path)
        return None


async def cleanup_files(*paths: Optional[str]) -> None:
    """Delete files in parallel for maximum I/O throughput."""
    valid_paths = [p for p in paths if p and isinstance(p, str)]
    if not valid_paths:
        return

    async def _safe_remove(p: str):
        try:
            if await aiofiles.os.path.exists(p):
                await aiofiles.os.remove(p)
        except (OSError, Exception) as exc:
            _log("debug", "cleanup_files suppressed", path=p, exc=type(exc).__name__)

    # Run all deletions concurrently
    await asyncio.gather(*[_safe_remove(p) for p in valid_paths], return_exceptions=True)

logger = _base_logger


_FFMPEG_FALLBACK_PATHS = {
    "ffmpeg": [
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "/opt/ffmpeg/bin/ffmpeg",
        "/opt/homebrew/bin/ffmpeg",
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
    ],
    "ffprobe": [
        "/usr/bin/ffprobe",
        "/usr/local/bin/ffprobe",
        "/opt/ffmpeg/bin/ffprobe",
        "/opt/homebrew/bin/ffprobe",
        r"C:\ffmpeg\bin\ffprobe.exe",
        r"C:\Program Files\ffmpeg\bin\ffprobe.exe",
        r"C:\Program Files (x86)\ffmpeg\bin\ffprobe.exe",
    ],
}

def _is_executable_file(path: Optional[str]) -> bool:
    return bool(path) and os.path.isfile(path) and os.access(path, os.X_OK)

def _candidate_binary_names(binary_name: str) -> List[str]:
    if os.name == "nt":
        return [f"{binary_name}.exe", binary_name]
    return [binary_name]

def _resolve_binary_from_dir(directory: str, binary_name: str) -> Optional[str]:
    if not directory:
        return None
    for candidate in _candidate_binary_names(binary_name):
        path = os.path.join(directory, candidate)
        if _is_executable_file(path):
            return path
    return None

def _find_ff_binary(binary_name: str) -> Optional[str]:
    """Resolve ffmpeg/ffprobe using env overrides, PATH, sibling binaries, then fallbacks."""
    env_keys = {
        "ffmpeg": ("FFMPEG_PATH", "FFMPEG"),
        "ffprobe": ("FFPROBE_PATH", "FFPROBE"),
    }

    for env_key in env_keys.get(binary_name, ()):
        env_path = os.getenv(env_key)
        if _is_executable_file(env_path):
            return env_path
        sibling = _resolve_binary_from_dir(env_path or "", binary_name)
        if sibling:
            return sibling

    found = shutil.which(binary_name)
    if found:
        return found

    counterpart = "ffprobe" if binary_name == "ffmpeg" else "ffmpeg"
    for env_key in env_keys.get(counterpart, ()):
        counterpart_path = os.getenv(env_key)
        if counterpart_path:
            sibling = _resolve_binary_from_dir(os.path.dirname(counterpart_path), binary_name)
            if sibling:
                return sibling

    for path in _FFMPEG_FALLBACK_PATHS.get(binary_name, []):
        if _is_executable_file(path):
            return path
    return None

def _find_ffmpeg() -> Optional[str]:
    """Locate ffmpeg across env overrides, PATH, and common install locations."""
    return _find_ff_binary("ffmpeg")

def _find_ffprobe() -> Optional[str]:
    """Locate ffprobe across env overrides, PATH, and common install locations."""
    return _find_ff_binary("ffprobe")

_CORRUPT_MEDIA_PATTERNS = (
    "corrupt",
    "invalid data found",
    "error while decoding",
    "packet too small",
    "truncated",
    "moov atom not found",
    "non monotonically increasing dts",
    "missing picture in access unit",
    "concealing",
    "malformed",
)

def _looks_like_corrupt_media(stderr_text: str) -> bool:
    text = (stderr_text or "").lower()
    return any(pattern in text for pattern in _CORRUPT_MEDIA_PATTERNS)

def _ffmpeg_input_tolerance_flags(tolerant: bool) -> List[str]:
    if not tolerant:
        return []
    return ["-err_detect", "ignore_err", "-fflags", "+discardcorrupt+genpts"]

def _size_ratio_floor(has_errors: bool, tolerant_input: bool, default_floor: float) -> float:
    """
    Corrupt/truncated sources can legitimately shrink after recovery because
    ffmpeg may discard unreadable packets, broken tails, or bad attachments.
    """
    if has_errors or tolerant_input:
        return min(default_floor, 0.35)
    return default_floor

# These codecs can be extracted as text and remuxed safely.  Keep this list in
# sync with the IntroSub extraction branch below; omitting one there makes the
# intro path take a different stream map than the normal (lossless) path.
_TEXT_SUBTITLE_CODECS = {
    "ass", "ssa", "subrip", "srt", "webvtt", "mov_text", "text", "ttml", "dfxp",
}

async def _probe_streams(file_path: str) -> List[Dict[str, Any]]:
    out = await _run_ffprobe(
        ["-v", "quiet", "-print_format", "json", "-show_streams", file_path]
    )
    if not out:
        return []
    try:
        return json.loads(out.decode(errors="ignore")).get("streams", [])
    except Exception as exc:
        _log("warning", "_probe_streams parse failed", path=file_path, exc=str(exc))
        return []

def _subtitle_stream_count(streams: List[Dict[str, Any]]) -> int:
    """Count subtitle streams for a loss-prevention check after remuxing."""
    return sum(1 for stream in streams if stream.get("codec_type") == "subtitle")


def _clean_meta_string(value: str) -> str:
    """Sanitize a metadata value for ffmpeg: strip control chars, limit length."""
    if not value:
        return ""
    # Remove ASCII control characters (0x00-0x1F except tab/newline) and null bytes
    cleaned = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", str(value))
    # Limit to 500 chars to avoid ffmpeg metadata overflow
    return cleaned[:500].strip()

async def add_metadata(
    input_path: str,
    output_path: str,
    user_id: int,
    timeout: int = 300,
    user_data: Optional[Dict[str, Any]] = None,
    has_errors: bool = False,
    probed_streams: Optional[List[Dict[str, Any]]] = None,
    transcode_res: Optional[Dict[str, Any]] = None,
) -> str:
    _audio_pre_encoded: Optional[str] = None
    ffmpeg = _find_ffmpeg()
    if not ffmpeg:
        raise RuntimeError("ffmpeg not in PATH")
    if not await aiofiles.os.path.exists(input_path):
        raise FileNotFoundError(f"Input not found: {input_path}")

    output_dir = os.path.dirname(output_path)
    if output_dir:
        await aiofiles.os.makedirs(output_dir, exist_ok=True)

    has_intro = False
    injected_subs = []
    try:

        try:
            premium_requested = bool(
                user_data and any(
                    (
                        user_data.get("metadata_purge"),
                        user_data.get("intro_sub"),
                        user_data.get("track_renaming"),
                    )
                )
            )

            # Optimisation: use user_data from batch query if available
            if user_data:
                md = {
                    "title":      user_data.get("title") or "",
                    "video":      user_data.get("video") or "",
                    "audio":      user_data.get("audio") or "",
                    "subtitle":   user_data.get("subtitle") or "",
                    "artist":     user_data.get("artist") or "",
                    "author":     user_data.get("author") or "",
                    "encoded_by": user_data.get("encoded_by") or "",
                    "custom_tag": user_data.get("custom_tag") or "",
                    "commentz":   user_data.get("commentz") or "",
                    "album":      user_data.get("album") or "",
                    "genre":      user_data.get("genre") or "",
                    "composer":   user_data.get("composer") or "",
                    "publisher":  user_data.get("publisher") or "",
                    "copyright":  user_data.get("copyright") or "",
                }
            else:
                (
                    title,
                    video,
                    audio,
                    subtitle,
                    artist,
                    author,
                    encoded_by,
                    custom_tag,
                    commentz,
                ) = await asyncio.gather(
                    DARKXSIDE78.get_title(user_id),
                    DARKXSIDE78.get_video(user_id),
                    DARKXSIDE78.get_audio(user_id),
                    DARKXSIDE78.get_subtitle(user_id),
                    DARKXSIDE78.get_artist(user_id),
                    DARKXSIDE78.get_author(user_id),
                    DARKXSIDE78.get_encoded_by(user_id),
                    DARKXSIDE78.get_custom_tag(user_id),
                    DARKXSIDE78.get_commentz(user_id),
                )
                md = {
                    "title":      title or "",
                    "video":      video or "",
                    "audio":      audio or "",
                    "subtitle":   subtitle or "",
                    "artist":     artist or "",
                    "author":     author or "",
                    "encoded_by": encoded_by or "",
                    "custom_tag": custom_tag or "",
                    "commentz":   commentz or "",
                    "album":      "",
                    "genre":      "",
                    "composer":   "",
                    "publisher":  "",
                    "copyright":  "",
                }

            premium_placeholders_present = any(
                md_value and any(token in md_value for token in ("{filename}", "{basename}", "{ext}", "{user_id}"))
                for md_value in md.values()
                if isinstance(md_value, str)
            )
            needs_premium_lookup = premium_requested or premium_placeholders_present
            # CRITICAL FIX: Trust user_data batch result as the primary source of truth.
            # Only hit the DB if the batch didn't already confirm premium status, and only
            # when a lookup is actually required (premium features / placeholders present).
            _ud_premium = bool((user_data or {}).get("is_premium") or (user_data or {}).get("premium"))
            is_premium = _ud_premium if not needs_premium_lookup else (
                _ud_premium or await db.is_premium(user_id)
            )

            _log("debug", "premium check", 
                 ud_premium_raw=repr((user_data or {}).get("is_premium")),
                 ud_premium_bool=_ud_premium,
                 needs_lookup=needs_premium_lookup,
                 is_premium=is_premium)

            if is_premium:
                v_map = {
                    "{filename}": os.path.basename(input_path),
                    "{basename}": os.path.splitext(os.path.basename(input_path))[0],
                    "{ext}": os.path.splitext(input_path)[1],
                    "{user_id}": str(user_id)
                }
                for k in md:
                    for var, val in v_map.items():
                        if md[k] and var in md[k]:
                            md[k] = md[k].replace(var, val)

        except Exception as exc:
            raise RuntimeError(f"DB metadata fetch: {exc}") from exc

        requested_global_tags = (
            "title", "author", "artist", "album", "genre",
            "composer", "publisher", "copyright", "encoded_by", "commentz",
        )
        has_global_metadata = any(md.get(tag) for tag in requested_global_tags)
        has_stream_metadata = any(md.get(tag) for tag in ("video", "audio", "subtitle"))
        has_custom_metadata = any("=" in pair for pair in md.get("custom_tag", "").split(",") if pair.strip())
        has_metadata_work = has_global_metadata or has_stream_metadata or has_custom_metadata

        if not (has_metadata_work or (is_premium and premium_requested)):
            _log("info", "add_metadata skipped: no effective metadata configured", user_id=user_id)
            return input_path

        # Capture input size BEFORE conversion for integrity check
        try:
            input_stat = await aiofiles.os.stat(input_path)
            input_size = input_stat.st_size
        except Exception as exc:
            _log("warning", "add_metadata: failed to stat input", path=input_path, exc=str(exc))
            input_size = 0

        # Subtitle codec mapping for MP4 compatibility
        sub_codec = "copy"
        if output_path.lower().endswith(".mp4"):
            sub_codec = "mov_text"

        if probed_streams is None:
            probed_streams = await _probe_streams(input_path)
        input_subtitle_count = _subtitle_stream_count(probed_streams)
        text_subtitle_indices = [
            s.get("index")
            for s in probed_streams
            if s.get("codec_type") == "subtitle" and s.get("codec_name", "").lower() in _TEXT_SUBTITLE_CODECS
        ]
        text_subtitle_indices = [idx for idx in text_subtitle_indices if isinstance(idx, int)]
        dropped_subtitle_indices = [
            s.get("index")
            for s in probed_streams
            if s.get("codec_type") == "subtitle" and s.get("codec_name", "").lower() not in _TEXT_SUBTITLE_CODECS
        ]
        dropped_subtitle_indices = [idx for idx in dropped_subtitle_indices if isinstance(idx, int)]
        corrupt_fast_path = bool(has_errors)

        # -- Purge Mode (Clean Slate) (PREMIUM ONLY) -----------------------------
        purge_flags = []
        track_meta_flags = []

        if is_premium:
            if user_data and user_data.get("metadata_purge"):
                # -map_metadata -1 clears global tags
                # -map_chapters -1 clears internal chapters (often contain ads)
                # -map_metadata:s... clears individual stream tags
                # -fflags +bitexact removes encoder timestamps/branding
                # -map -0:d? / -map -0:t? aggressively drops hidden data/attachment streams
                # Explicit handler and encoder wipes guarantee no residual signatures survive
                purge_flags = [
                    "-map_metadata", "-1",
                    "-map_chapters", "-1",
                    "-map_metadata:s:v", "-1",
                    "-map_metadata:s:a", "-1",
                    "-map_metadata:s:s", "-1",
                    "-map_metadata:p", "-1", # Strip Program-level metadata
                    "-map", "-0:d?",         # Drop all data streams (binary blobs)
                    "-map", "-0:t?",         # Drop all attachments (fonts/covers)
                    "-fflags", "+bitexact",
                    "-flags:v", "+bitexact",
                    "-flags:a", "+bitexact",
                    "-flags:s", "+bitexact",
                    # ── NUCLEAR OVERWRITES ──
                    "-metadata", "creation_time=",
                    "-metadata", "encoder=",
                    "-metadata", "title=",
                    "-metadata", "artist=",
                    "-metadata", "album=",
                    "-metadata", "comment=",
                    "-metadata:s:v", "title=",
                    "-metadata:s:a", "title=",
                    "-metadata:s:s", "title=",
                    "-metadata:s:v", "handler_name=",
                    "-metadata:s:a", "handler_name=",
                    "-metadata:s:s", "handler_name=",
                    "-metadata:s:a", "encoded_library_date=",
                    "-metadata:s:a", "writing_library=",
                ]

            # -- Seamless Subtitle Injection (PREMIUM ONLY) -----------------------
            # We inject branding into EVERY existing subtitle track.
            # A separate track is ONLY created if NO original subtitles exist.

            if is_premium and user_data and user_data.get("intro_sub") and not corrupt_fast_path:
                try:
                    itxt = _clean_meta_string(user_data.get("intro_text", "── SHADOWEDTOMB ──"))
                    itimer = int(user_data.get("intro_timer", "5"))

                    # Branding Styles (Shared with Global Overrides)
                    gcolor = user_data.get("sub_color", "Default")
                    ipos = user_data.get("sub_pos", "2")
                    ifsize = user_data.get("sub_font_size", "40")
                    if ifsize == "0": ifsize = "40"
                    ishad = user_data.get("sub_shadow", "0")

                    # Border settings
                    b_status = user_data.get("sub_bord_status", False)
                    b_color = user_data.get("sub_bord_color", "&H000000&")
                    b_width = user_data.get("sub_bord_width", "2")

                    # Determine intro color (use global if set, else default to White)
                    icol = gcolor if gcolor != "Default" else "&HFFFFFF&"
                    temp_dir = os.path.dirname(output_path)

                    # Respect b_status: if border is Off, force border width to "0"
                    _fb_bw = b_width if b_status else "0"

                    # Reuse the already-probed stream list from the full input file.
                    sub_streams = [s for s in probed_streams if s.get("codec_type") == "subtitle"]

                    is_mkv = output_path.lower().endswith(".mkv")

                    if not sub_streams:
                        # FALLBACK: Create a standalone ASS track only if zero subs exist in the original video
                        intro_sub_path = os.path.join(temp_dir, f"intro_{uuid.uuid4()}.ass")
                        ass_content = build_fallback_intro_ass(
                            text=itxt,
                            color=icol,
                            pos=ipos,
                            duration=itimer,
                            b_width=_fb_bw,
                            b_color=b_color,
                            font_size=ifsize,
                            shadow=ishad
                        )
                        async with aiofiles.open(intro_sub_path, "w", encoding="utf-8") as f:
                            await f.write(ass_content)
                        injected_subs.append((intro_sub_path, -1, "ass", "Intro Sub", "und"))
                        has_intro = True
                    else:
                        # PRIMARY: Extract and modify all existing subtitle tracks
                        text_sub_streams = [
                            s for s in sub_streams
                            if s.get("codec_name", "srt").lower() in _TEXT_SUBTITLE_CODECS
                        ]
                        
                        batch_extracted = False
                        if text_sub_streams:
                            try:
                                extract_cmd = [ffmpeg, "-hide_banner", "-i", input_path]
                                output_paths = []
                                for s in text_sub_streams:
                                    idx = s["index"]
                                    codec = s.get("codec_name", "srt").lower()
                                    ext = (
                                        "ass" if codec in ("ass", "ssa")
                                        else "vtt" if codec == "webvtt"
                                        else "ttml" if codec in ("ttml", "dfxp")
                                        else "srt"
                                    )
                                    raw_sub = os.path.join(temp_dir, f"raw_{idx}_{uuid.uuid4()}.{ext}")
                                    extract_cmd.extend(["-map", f"0:{idx}", "-y", raw_sub])
                                    output_paths.append((s, raw_sub, ext))
                                
                                ret_batch, stderr_batch = await _run_ffmpeg(extract_cmd, timeout=120)
                                if ret_batch == 0:
                                    batch_extracted = True
                                    for s, raw_sub, ext in output_paths:
                                        idx = s["index"]
                                        lang = s.get("tags", {}).get("language", "und")
                                        title = s.get("tags", {}).get("title", f"{normalize_lang_code(lang)}")
                                        try:
                                            if await aiofiles.os.path.exists(raw_sub):
                                                async with aiofiles.open(raw_sub, "r", encoding="utf-8", errors="ignore") as f:
                                                    content = await f.read()

                                                _has_content = False
                                                if ext == "ass":
                                                    _has_content = any(
                                                        l.startswith("Dialogue:") for l in content.splitlines()
                                                    )
                                                elif ext in ("srt", "vtt"):
                                                    _has_content = "-->" in content
                                                elif ext == "ttml":
                                                    _has_content = "<p" in content.lower()

                                                if not _has_content:
                                                    _log("warning", f"sub stream {idx} has zero duration / empty content, skipping injection")
                                                    try:
                                                        await aiofiles.os.remove(raw_sub)
                                                    except Exception:
                                                        pass
                                                    continue

                                                if ext == "ass":
                                                    playresy = 720
                                                    for lx in content.splitlines()[:100]:
                                                        if lx.strip().startswith("PlayResY:"):
                                                            try:
                                                                playresy = int(lx.split(":")[1].strip())
                                                            except:
                                                                pass
                                                    scale = playresy / 720.0 if playresy > 0 else 1.0
                                                    content = inject_into_ass(
                                                        content=content,
                                                        text=itxt,
                                                        color=icol,
                                                        pos=ipos,
                                                        duration=itimer,
                                                        b_width=_fb_bw,
                                                        b_color=b_color,
                                                        font_size=ifsize,
                                                        shadow=ishad,
                                                        scale=scale
                                                    )
                                                elif ext == "srt":
                                                    content = inject_into_srt(
                                                        content=content,
                                                        text=itxt,
                                                        color=icol,
                                                        pos=ipos,
                                                        duration=itimer,
                                                        b_width=_fb_bw,
                                                        b_color=b_color,
                                                        font_size=ifsize,
                                                        shadow=ishad
                                                    )
                                                elif ext == "vtt":
                                                    content = inject_into_vtt(
                                                        content=content,
                                                        text=itxt,
                                                        color=icol,
                                                        pos=ipos,
                                                        duration=itimer,
                                                        b_width=_fb_bw,
                                                        b_color=b_color,
                                                        font_size=ifsize,
                                                        shadow=ishad
                                                    )
                                                elif ext == "ttml":
                                                    content = inject_into_ttml(
                                                        content=content,
                                                        text=itxt,
                                                        color=icol,
                                                        pos=ipos,
                                                        duration=itimer,
                                                        b_width=_fb_bw,
                                                        b_color=b_color,
                                                        font_size=ifsize,
                                                        shadow=ishad,
                                                    )

                                                async with aiofiles.open(raw_sub, "w", encoding="utf-8") as f:
                                                    await f.write(content)

                                                injected_subs.append((raw_sub, idx, ext, title, lang))
                                                has_intro = True
                                        except Exception as ex:
                                            _log("warning", f"failed to inject watermark in stream {idx}", exc=str(ex))
                                            try:
                                                await aiofiles.os.remove(raw_sub)
                                            except Exception:
                                                pass
                                else:
                                    _log("warning", "batch subtitle extraction failed, attempting fallback to individual stream extraction", stderr=stderr_batch[:300])
                            except Exception as ex:
                                _log("warning", "error during batch subtitle extraction, attempting fallback", exc=str(ex))

                        # Safe Fallback to individual stream extraction
                        if not batch_extracted:
                            for s in sub_streams:
                                idx = s["index"]
                                codec = s.get("codec_name", "srt").lower()
                                lang = s.get("tags", {}).get("language", "und")
                                title = s.get("tags", {}).get("title", f"{normalize_lang_code(lang)}")

                                if codec in _TEXT_SUBTITLE_CODECS:
                                    try:
                                        ext = "srt"
                                        if codec in ("ass", "ssa"):
                                            ext = "ass"
                                        elif codec == "webvtt":
                                            ext = "vtt"
                                        elif codec in ("ttml", "dfxp"):
                                            ext = "ttml"

                                        raw_sub = os.path.join(temp_dir, f"raw_{idx}_{uuid.uuid4()}.{ext}")

                                        if (await _run_ffmpeg([ffmpeg, "-i", input_path, "-map", f"0:{idx}", "-y", raw_sub]))[0] == 0:
                                            async with aiofiles.open(raw_sub, "r", encoding="utf-8", errors="ignore") as f:
                                                content = await f.read()

                                            _has_content = False
                                            if ext == "ass":
                                                _has_content = any(
                                                    l.startswith("Dialogue:") for l in content.splitlines()
                                                )
                                            elif ext == "srt":
                                                _has_content = "-->" in content
                                            elif ext == "vtt":
                                                _has_content = "-->" in content
                                            elif ext == "ttml":
                                                _has_content = "<p" in content.lower()

                                            if not _has_content:
                                                _log("warning", f"sub stream {idx} has zero duration / empty content, skipping injection")
                                                try:
                                                    await aiofiles.os.remove(raw_sub)
                                                except Exception:
                                                    pass
                                                continue

                                            if ext == "ass":
                                                playresy = 720
                                                for lx in content.splitlines()[:100]:
                                                    if lx.strip().startswith("PlayResY:"):
                                                        try:
                                                            playresy = int(lx.split(":")[1].strip())
                                                        except:
                                                            pass
                                                scale = playresy / 720.0 if playresy > 0 else 1.0
                                                content = inject_into_ass(
                                                    content=content,
                                                    text=itxt,
                                                    color=icol,
                                                    pos=ipos,
                                                    duration=itimer,
                                                    b_width=_fb_bw,
                                                    b_color=b_color,
                                                    font_size=ifsize,
                                                    shadow=ishad,
                                                    scale=scale
                                                )
                                            elif ext == "srt":
                                                content = inject_into_srt(
                                                    content=content,
                                                    text=itxt,
                                                    color=icol,
                                                    pos=ipos,
                                                    duration=itimer,
                                                    b_width=_fb_bw,
                                                    b_color=b_color,
                                                    font_size=ifsize,
                                                    shadow=ishad
                                                )
                                            elif ext == "vtt":
                                                content = inject_into_vtt(
                                                    content=content,
                                                    text=itxt,
                                                    color=icol,
                                                    pos=ipos,
                                                    duration=itimer,
                                                    b_width=_fb_bw,
                                                    b_color=b_color,
                                                    font_size=ifsize,
                                                    shadow=ishad
                                                )
                                            elif ext == "ttml":
                                                content = inject_into_ttml(
                                                    content=content,
                                                    text=itxt,
                                                    color=icol,
                                                    pos=ipos,
                                                    duration=itimer,
                                                    b_width=_fb_bw,
                                                    b_color=b_color,
                                                    font_size=ifsize,
                                                    shadow=ishad,
                                                )

                                            async with aiofiles.open(raw_sub, "w", encoding="utf-8") as f:
                                                await f.write(content)

                                            injected_subs.append((raw_sub, idx, ext, title, lang))
                                            has_intro = True
                                        else:
                                            try:
                                                await aiofiles.os.remove(raw_sub)
                                            except Exception:
                                                pass
                                    except Exception as ex:
                                        _log("warning", f"failed to inject watermark in stream {idx}", exc=str(ex))
                                        try:
                                            await aiofiles.os.remove(raw_sub)
                                        except Exception:
                                            pass

                        # ── All-empty fallback ────────────────────────────────────────────────
                        # If every existing sub stream was zero-duration / empty and we couldn't
                        # inject into any of them, fall back to a standalone intro ASS track.
                        if not has_intro:
                            _log("info", "all sub streams were empty; creating standalone intro ASS track")
                            intro_sub_path = os.path.join(temp_dir, f"intro_{uuid.uuid4()}.ass")
                            ass_content = build_fallback_intro_ass(
                                text=itxt,
                                color=icol,
                                pos=ipos,
                                duration=itimer,
                                b_width=_fb_bw,
                                b_color=b_color,
                                font_size=ifsize,
                                shadow=ishad
                            )
                            async with aiofiles.open(intro_sub_path, "w", encoding="utf-8") as f:
                                await f.write(ass_content)
                            injected_subs.append((intro_sub_path, -1, "ass", "Intro Sub", "und"))
                            has_intro = True
                        # ── End all-empty fallback ────────────────────────────────────────────

                except Exception as e:
                    _log("warning", "sub_injection failure", exc=str(e))


            # -- Track Renaming (PREMIUM ONLY) ------------------------------------
            if user_data and user_data.get("track_renaming") and not corrupt_fast_path:
                try:
                    a_idx, s_idx = 0, 0
                    for s in probed_streams:
                        stype = s.get("codec_type")
                        if stype == "audio":
                            _tags = s.get("tags", {})
                            lang = _tags.get("language") or _tags.get("LANGUAGE") or "und"
                            codec, channels = s.get("codec_name", "").upper(), s.get("channels")
                            
                            if user_data and user_data.get("audio_conv_enabled", False):
                                codec = user_data.get("audio_format", "aac").upper()

                            norm = normalize_lang_code(lang)
                            layout = {1: "Mono", 2: "Stereo", 6: "5.1", 8: "7.1"}.get(channels, "")
                            auto_name = f"{norm} ({codec}{' ' + layout if layout else ''})".strip()
                            # CRITICAL FIX: md.get("audio") can return "" (empty string).
                            # "" is falsy so `or` correctly falls back to the {track} template,
                            # preventing audio track titles from being blanked entirely.
                            _a_tmpl = md.get("audio") or "{track}"
                            t_name = _a_tmpl.replace("{track}", auto_name)
                            track_meta_flags.extend([f"-metadata:s:a:{a_idx}", f"title={t_name}"])
                            a_idx += 1
                        elif stype == "subtitle":
                            _tags = s.get("tags", {})
                            lang = _tags.get("language") or _tags.get("LANGUAGE") or "und"
                            forced = s.get("disposition", {}).get("forced")
                            norm = normalize_lang_code(lang)
                            auto_name = f"{norm}{' [Forced]' if forced == 1 else ''}".strip()
                            # CRITICAL FIX: same empty-string guard as the audio branch above.
                            _s_tmpl = md.get("subtitle") or "{track}"
                            t_name = _s_tmpl.replace("{track}", auto_name)
                            track_meta_flags.extend([f"-metadata:s:s:{s_idx}", f"title={t_name}"])
                            s_idx += 1
                except Exception as e: _log("warning", "track_rename failure", exc=str(e))

        if corrupt_fast_path:
            _log(
                "info",
                "add_metadata corrupt fast path enabled",
                user_id=user_id,
                skipped_intro_sub=bool(user_data and user_data.get("intro_sub")),
                skipped_track_renaming=bool(user_data and user_data.get("track_renaming")),
            )

        tolerant_input = has_errors
        safe_metadata_mode = tolerant_input and not has_intro

        # MAX SUPPORT: Injecting all industry-standard global metadata tags
        global_meta = {
            "title": md.get("title"),
            "author": md.get("author"),
            "artist": md.get("artist"),
            "album": md.get("album"),
            "genre": md.get("genre"),
            "composer": md.get("composer"),
            "publisher": md.get("publisher"),
            "copyright": md.get("copyright"),
            "encoded_by": md.get("encoded_by"),
            "comment": md.get("commentz"),
        }

        def _append_metadata_flags(cmd: List[str]) -> None:
            cmd.extend(purge_flags + track_meta_flags)
            for tag, val in global_meta.items():
                if val:
                    cmd.extend(["-metadata", f"{tag}={_clean_meta_string(val)}"])
            if md.get("video"):
                cmd.extend(["-metadata:s:v", f"title={_clean_meta_string(md.get('video'))}"])
            if not track_meta_flags:
                if md.get("audio"):
                    cmd.extend(["-metadata:s:a", f"title={_clean_meta_string(md.get('audio'))}"])
                if md.get("subtitle"):
                    cmd.extend(["-metadata:s:s", f"title={_clean_meta_string(md.get('subtitle'))}"])
            if md.get("custom_tag"):
                for pair in md["custom_tag"].split(","):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        cmd.extend(["-metadata", f"{k.strip()}={_clean_meta_string(v.strip())}"])
            if not purge_flags:
                cmd.extend([
                    "-metadata", "ST_PRO_ID=SHADOWEDTOMB_V2",
                    "-metadata:s:v", "ENCODER_SIGNATURE=ST_CORE_V14",
                    "-metadata:s:a", "ST_ENGINE=ST_IRONCLAD_X",
                ])
            cmd.extend(["-loglevel", "error", "-y", output_path])

        # ── FAST PATH: parallel per-track pre-encode, then stream-copy mux ──────
        _t_fast_path_start = time.monotonic()
        if (
            transcode_res
            and not transcode_res.get("skip")
            and transcode_res.get("ffmpeg_args")
            # Note: has_intro is supported — subtitle input indices are offset correctly below
        ):
            # Always use .mka container — supports any codec + multiple audio streams
            _audio_pre_encoded = os.path.join(
                os.path.dirname(output_path),
                f"_aud_{uuid.uuid4().hex[:12]}.mka"
            )
            # Split original ffmpeg_args into:
            #   _encoder_flags — codec/bitrate/quality flags (used in pre-encode subprocess only)
            #   _metadata_tags — BPS/tag flags template for ONE track (expanded per-track below)
            _raw_audio_flags = transcode_res.get("ffmpeg_args", [])
            _encoder_flags: list[str] = []
            _metadata_tags_tmpl: list[tuple[str, str]] = []  # list of (flag, value) pairs
            _skip_next = False
            _last_flag = ""
            for _tok in _raw_audio_flags:
                if _skip_next:
                    _skip_next = False
                    _metadata_tags_tmpl.append((_last_flag, _tok))
                    continue
                if _tok.startswith("-metadata"):
                    _skip_next = True
                    _last_flag = _tok
                    continue
                _encoder_flags.append(_tok)

            # ── Count audio streams from probed_streams ───────────────────────────
            _audio_stream_count = max(
                sum(1 for s in (probed_streams or []) if s.get("codec_type") == "audio"),
                1,
            )
            _global_sem = _get_ffmpeg_sem()
            _available = max(1, _global_sem._value)
            _TRACK_PARALLEL_LIMIT = min(
                _audio_stream_count,
                Config.AUDIO_ENCODE_PARALLEL_LIMIT,
                _available
            )
            _track_par_sem = asyncio.Semaphore(_TRACK_PARALLEL_LIMIT)
            _target_fmt = transcode_res.get("target_fmt") or (
                (user_data or {}).get("audio_format", "aac") or "aac"
            )
            _target_br = transcode_res.get("target_br") or (
                (user_data or {}).get("audio_bitrate", "128k") or "128k"
            )

            _log(
                "info",
                "audio fast-path: starting parallel per-track encode",
                tracks=_audio_stream_count,
                parallel_limit=_TRACK_PARALLEL_LIMIT,
                encoder_flags=_encoder_flags,
            )

            async def _encode_one_track(track_idx: int) -> Optional[Tuple[str, int]]:
                async with _track_par_sem:
                    _t_total = time.monotonic()
                    _out_dir = os.path.dirname(output_path)
                    _t_path = os.path.join(
                        _out_dir,
                        f"_aud_{uuid.uuid4().hex[:8]}_t{track_idx}.mka",
                    )
                    
                    from plugins.audio_ui import _BITRATE_BPS
                    _target_bps_int = _BITRATE_BPS.get(_target_br, 0)

                    # Phase 1: stream-copy extract (near-instant)
                    raw_path = await _extract_audio_stream_copy(
                        ffmpeg, input_path, _out_dir, track_idx, tolerant_input,
                    )

                    if not raw_path:
                        # Fallback: direct encode from source
                        _t_cmd = [
                            ffmpeg, "-hide_banner", "-nostdin", "-threads", "1",
                            "-analyzeduration", "100000", "-probesize", "500000",
                            *_ffmpeg_input_tolerance_flags(tolerant_input),
                            "-i", input_path,
                            "-map", f"0:a:{track_idx}",
                            "-vn", "-sn", "-dn",
                            *_encoder_flags,
                            "-y", _t_path,
                        ]
                        _ret, _err = await _run_ffmpeg(_t_cmd, timeout=1800, is_audio_only=True)
                        if _ret != 0 or not os.path.exists(_t_path):
                            return None
                        return (_t_path, _target_bps_int)  # ← encoded at target

                    # Phase 1b: resolve source bitrate for passthrough decision
                    from plugins.audio_ui import (
                        can_passthrough_audio_track,
                        get_audio_stream_at,
                        _probe_audio_stream,
                    )
                    _src_codec: Optional[str] = None
                    _src_bps = 0
                    _stream = get_audio_stream_at(probed_streams or [], track_idx)
                    if _stream:
                        _src_codec = _stream.get("codec_name")
                        _src_bps = int(_stream.get("bit_rate") or 0)

                    if _src_bps == 0:
                        # Try estimating from file size + duration
                        dur = 0
                        for s in (probed_streams or []):
                            if s.get("duration"):
                                try:
                                    dur = float(s["duration"])
                                    if dur > 0:
                                        break
                                except Exception:
                                    pass
                        if dur > 0:
                            try:
                                _src_bps = int(os.path.getsize(raw_path) * 8 / dur)
                            except OSError:
                                pass
                        if _src_bps == 0:
                            # Last resort: probe the extracted audio file
                            _probe = await _probe_audio_stream(raw_path)
                            if _probe:
                                _src_codec, _src_bps = _probe

                    if can_passthrough_audio_track(_src_codec, _src_bps, _target_fmt, _target_br):
                        # Stream copy is valid: source is confirmed at or below target quality
                        try:
                            os.replace(raw_path, _t_path)
                        except OSError:
                            shutil.copy2(raw_path, _t_path)
                            with suppress(Exception):
                                await aiofiles.os.remove(raw_path)
                        _log("info", f"audio track {track_idx} stream-copy passthrough",
                             codec=_src_codec, src_bps=_src_bps, target=f"{_target_fmt}@{_target_br}")
                        return (_t_path, _src_bps)  # ← passthrough: use ACTUAL source bitrate for tags

                    # Phase 2: encode from extracted audio-only source
                    _enc_cmd = [
                        ffmpeg, "-hide_banner", "-nostdin", "-threads", "1",
                        "-analyzeduration", "100000", "-probesize", "500000",
                        "-i", raw_path,
                        *_encoder_flags,
                        "-y", _t_path,
                    ]
                    _ret, _err = await _run_ffmpeg(_enc_cmd, timeout=1800, is_audio_only=True)

                    with suppress(Exception):
                        await aiofiles.os.remove(raw_path)

                    if _ret != 0 or not os.path.exists(_t_path):
                        return None
                    return (_t_path, _target_bps_int)  # ← encoded at target

            # Fire all track encodes simultaneously
            _t_encode_start = time.monotonic()
            _track_results: list[Optional[Tuple[str, int]]] = list(
                await asyncio.gather(
                    *[_encode_one_track(i) for i in range(_audio_stream_count)],
                    return_exceptions=False,
                )
            )
            _log("info", "all track encodes finished",
                 elapsed=f"{time.monotonic() - _t_encode_start:.1f}s")

            _valid_tracks = [(p, bps) for res in _track_results if res for p, bps in [res] if p and os.path.exists(p)]

            _pre_ok = False
            if len(_valid_tracks) == _audio_stream_count:
                _valid_paths = [p for p, _ in _valid_tracks]
                _valid_bps   = [bps for _, bps in _valid_tracks]

                if _audio_stream_count == 1:
                    # Single track — skip merge, rename in-place
                    try:
                        os.replace(_valid_paths[0], _audio_pre_encoded)
                        _pre_ok = True
                    except Exception as _e:
                        _log("warning", "audio single-track rename failed", exc=str(_e))
                        with suppress(Exception):
                            await aiofiles.os.remove(_valid_paths[0])
                else:
                    # Multiple tracks — merge all per-track .mka via stream copy (fast, CPU-free)
                    _merge_cmd: list[str] = [ffmpeg, "-hide_banner"]
                    for _tp in _valid_paths:
                        _merge_cmd.extend(["-i", _tp])
                    for _mi in range(len(_valid_paths)):
                        _merge_cmd.extend(["-map", f"{_mi}:a"])
                    _merge_cmd.extend(["-c:a", "copy", "-y", _audio_pre_encoded])
                    _t_merge = time.monotonic()
                    _mret, _merr = await _run_ffmpeg(_merge_cmd, timeout=120)
                    _log("info" if _mret == 0 else "warning",
                         "audio track merge finished",
                         returncode=_mret,
                         elapsed=f"{time.monotonic() - _t_merge:.1f}s")
                    for _tp in _valid_paths:
                        with suppress(Exception):
                            await aiofiles.os.remove(_tp)
                    if _mret == 0 and os.path.exists(_audio_pre_encoded):
                        _pre_ok = True
                    else:
                        _log("warning", "audio merge failed, falling back to single-pass",
                             stderr=_merr[:200])
                        with suppress(Exception):
                            await aiofiles.os.remove(_audio_pre_encoded)
            else:
                _log("warning",
                     f"parallel encode partial failure "
                     f"({len(_valid_tracks)}/{_audio_stream_count}), falling back to single-pass")
                for _tp, _ in _valid_tracks:
                    with suppress(Exception):
                        await aiofiles.os.remove(_tp)

            if _pre_ok:
                try:
                    pre_size = (await aiofiles.os.stat(_audio_pre_encoded)).st_size
                except Exception:
                    pre_size = 0

                _all_bps_tags: list[str] = []
                for _aidx, _actual_bps in enumerate(_valid_bps):
                    if _actual_bps and _actual_bps > 0:
                        _all_bps_tags.extend([
                            f"-metadata:s:a:{_aidx}", f"BPS={_actual_bps}",
                            f"-metadata:s:a:{_aidx}", f"BPS-eng={_actual_bps}",
                        ])

                _log(
                    "info",
                    "audio pre-encode complete, switching to stream-copy mux",
                    tracks=_audio_stream_count,
                    parallel=_TRACK_PARALLEL_LIMIT,
                    size_bytes=pre_size,
                    total_elapsed=f"{time.monotonic() - _t_fast_path_start:.1f}s",
                    bps_tags_count=len(_all_bps_tags) // 2,
                )
                transcode_res = {
                    **transcode_res,
                    "ffmpeg_args": ["-c:a", "copy"] + _all_bps_tags,
                    "skip": True,
                }
            else:
                _audio_pre_encoded = None
                _log("warning", "audio fast-path failed entirely, using single-pass fallback",
                     elapsed=f"{time.monotonic() - _t_fast_path_start:.1f}s")
        else:
            _log(
                "info",
                "audio fast-path skipped",
                transcode_res_skip=transcode_res.get("skip") if transcode_res else None,
                has_intro=has_intro,
                has_ffmpeg_args=bool(transcode_res and transcode_res.get("ffmpeg_args")),
            )

        def _build_metadata_cmd(tolerant: bool, safe_streams: bool) -> List[str]:
            # ── Audio Conversion: use pre-resolved flags from UI module ──────────
            if transcode_res:
                _audio_flags: List[str] = transcode_res.get("ffmpeg_args", ["-c:a", "copy"])
            else:
                _audio_conv_enabled = bool((user_data or {}).get("audio_conv_enabled", False))
                if _audio_conv_enabled:
                    _aud_fmt     = (user_data or {}).get("audio_format", "aac") or "aac"
                    _aud_bitrate = (user_data or {}).get("audio_bitrate", "128k") or "128k"
                    from plugins.audio_ui import _build_ffmpeg_audio_args
                    _audio_flags = _build_ffmpeg_audio_args(_aud_fmt, _aud_bitrate)
                else:
                    _audio_flags = ["-c:a", "copy"]

            cmd: List[str] = [ffmpeg, "-hide_banner", "-threads", "0", *_ffmpeg_input_tolerance_flags(tolerant), "-i", input_path]
            
            # ── inject pre-encoded audio as second input ──────────────────────────
            # _audio_flags already contains ['-c:a', 'copy'] + BPS metadata tags from transcode_res
            _aud_input_idx = 0  # 0:a = original audio
            if _audio_pre_encoded and os.path.exists(_audio_pre_encoded):
                cmd.extend(["-i", _audio_pre_encoded])
                _aud_input_idx = 1  # 1:a = pre-encoded audio
                # DO NOT override _audio_flags here — transcode_res.ffmpeg_args already
                # has ['-c:a','copy'] + BPS tags set when pre-encode succeeded.

            if has_intro and injected_subs:
                for sub_path, _, _, _, _ in injected_subs:
                    cmd.extend(["-i", sub_path])
                cmd.extend(["-map", "0:v?", "-map", f"{_aud_input_idx}:a?", "-ignore_unknown"])
                if tolerant or safe_streams:
                    cmd.extend(["-map", "-0:t?", "-map", "-0:d?"])
                s_count = 0
                try:
                    for stream in probed_streams:
                        if stream.get("codec_type") != "subtitle":
                            continue
                        idx = stream.get("index")
                        if not isinstance(idx, int):
                            continue
                        codec_name = stream.get("codec_name", "").lower()
                        if safe_streams and codec_name not in _TEXT_SUBTITLE_CODECS:
                            continue
                        c_codec = "mov_text" if output_path.lower().endswith(".mp4") else "copy"
                        cmd.extend(["-map", f"0:{idx}", f"-c:s:{s_count}", c_codec])
                        s_count += 1
                except Exception as exc:
                    _log("warning", "failed to preserve original subtitle maps", exc=str(exc))

                for i, (_, _, codec, title, lang) in enumerate(injected_subs):
                    target_codec = "mov_text" if output_path.lower().endswith(".mp4") else ("ass" if codec == "ass" else "srt")
                    cmd.extend([
                        "-map", f"{_aud_input_idx + 1 + i}:0",  # offset by _aud_input_idx: when pre-encoded audio is input 1, subs start at 2
                        f"-metadata:s:s:{s_count}", f"title={title}",
                        f"-metadata:s:s:{s_count}", f"language={lang}",
                        f"-c:s:{s_count}", target_codec,
                    ])
                    s_count += 1
                cmd.extend(["-c:v", "copy", "-dn", *_audio_flags])
            elif safe_streams:
                cmd.extend(["-map", "0:v?", "-map", f"{_aud_input_idx}:a?"])
                for s_idx, abs_idx in enumerate(text_subtitle_indices):
                    cmd.extend(["-map", f"0:{abs_idx}", f"-c:s:{s_idx}", sub_codec])
                cmd.extend(["-map", "-0:t?", "-map", "-0:d?", "-ignore_unknown", "-c:v", "copy", "-dn", *_audio_flags])
            elif tolerant:
                cmd.extend([
                    "-map", "0:v?", "-map", f"{_aud_input_idx}:a?", "-map", "0:s?",
                    "-map", "-0:t?", "-map", "-0:d?",
                    "-ignore_unknown",
                    "-c:v", "copy", "-dn", *_audio_flags, "-c:s", sub_codec,
                ])
            else:
                if _aud_input_idx == 1:
                    cmd.extend(["-map", "0:v?", "-map", f"{_aud_input_idx}:a?", "-map", "0:s?", "-map", "-0:t?", "-map", "-0:d?", "-ignore_unknown", "-c:v", "copy", "-dn", *_audio_flags, "-c:s", sub_codec])
                else:
                    cmd.extend(["-map", "0", "-ignore_unknown", "-c:v", "copy", "-dn", *_audio_flags, "-c:s", sub_codec])
            _append_metadata_flags(cmd)
            return cmd


        if safe_metadata_mode:
            _log(
                "info",
                "add_metadata using safe stream map",
                user_id=user_id,
                kept_text_subtitles=len(text_subtitle_indices),
                dropped_nontext_subtitles=len(dropped_subtitle_indices),
            )

        # ── Fix: Audio conversion is CPU intensive. Give it up to 30 mins to avoid timeout kills ──
        if (user_data or {}).get("audio_conv_enabled", False):
            timeout = max(timeout, 1800)

        cmd = _build_metadata_cmd(tolerant_input, safe_metadata_mode)
        _pass_used = 1
        returncode, stderr_text = await _run_ffmpeg(cmd, timeout=timeout)

        # ── Fix: Do not fallback if the process timed out or we couldn't get a semaphore slot ──
        if "TIMED_OUT" in stderr_text:
            await cleanup_files(output_path)
            if has_intro and injected_subs: await cleanup_files(*[s[0] for s in injected_subs])
            _log("error", "add_metadata timed out", user_id=user_id, stderr=stderr_text)
            raise TimeoutError("Audio conversion took too long or server is extremely busy. Please try again later or disable Audio Conversion.")

        if returncode != 0 and not tolerant_input and _looks_like_corrupt_media(stderr_text):
            _log("warning", "add_metadata detected corrupt media, retrying tolerant mode",
                 user_id=user_id, stderr=stderr_text[:300])
            await cleanup_files(output_path)
            tolerant_input = True
            safe_metadata_mode = not has_intro
            cmd = _build_metadata_cmd(tolerant_input, safe_metadata_mode)
            returncode, stderr_text = await _run_ffmpeg(cmd, timeout=timeout)
            _pass_used = "1b"

        # Pass 2: Preserve subtitle streams too. If the destination cannot
        # support one, fail safely instead of delivering a file without it.
        if returncode != 0:
            _log("warning", "add_metadata pass 1 failed, retrying with subtitle preservation",
                 user_id=user_id, stderr=stderr_text[:300])
            await cleanup_files(output_path)
            _pass_used = 2
            safe_cmd = [
                ffmpeg, "-hide_banner", *_ffmpeg_input_tolerance_flags(tolerant_input), "-i", input_path,
                "-map", "0:v?", "-map", "0:a?", "-map", "0:s?", "-ignore_unknown",
            ] + purge_flags + [
                "-c", "copy",
            ]

            # Add metadata only if present
            for tag, db_key in [
                ("title", "title"),
                ("artist", "artist"),
                ("author", "author"),
                ("encoded_by", "encoded_by"),
                ("comment", "commentz"),
                ("album", "album"),
                ("genre", "genre"),
                ("composer", "composer"),
                ("publisher", "publisher"),
                ("copyright", "copyright")
            ]:
                val = md.get(db_key)
                if val:
                    safe_cmd.extend(["-metadata", f"{tag}={_clean_meta_string(val)}"])

            if md.get("video"):
                safe_cmd.extend(["-metadata:s:v", f"title={_clean_meta_string(md.get('video'))}"])

            if md.get("audio"):
                safe_cmd.extend(["-metadata:s:a", f"title={_clean_meta_string(md.get('audio'))}"])

            # Process custom tags correctly (split by comma, then key=value)
            if md.get("custom_tag"):
                for pair in md["custom_tag"].split(","):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        safe_cmd.extend(["-metadata", f"{k.strip()}={_clean_meta_string(v.strip())}"])

            safe_cmd.extend([
                "-loglevel", "error",
                "-y", output_path,
            ])
            returncode, stderr_text = await _run_ffmpeg(safe_cmd, timeout=timeout)

        # Pass 3: Minimal metadata fallback, still retaining subtitles.
        if returncode != 0:
            _log("warning", "add_metadata pass 2 failed, retrying minimal copy",
                 user_id=user_id, stderr=stderr_text[:300])
            await cleanup_files(output_path)
            _pass_used = 3
            nuclear_cmd = [
                ffmpeg, "-hide_banner", *_ffmpeg_input_tolerance_flags(tolerant_input), "-i", input_path,
                "-map", "0:v?", "-map", "0:a?", "-map", "0:s?", "-c", "copy",
                "-map_metadata", "-1",
                "-loglevel", "error",
                "-y", output_path,
            ]
            returncode, stderr_text = await _run_ffmpeg(nuclear_cmd, timeout=timeout)

        _log("info", "add_metadata completed", pass_used=_pass_used, returncode=returncode, user_id=user_id)

        if returncode != 0:
            await cleanup_files(output_path)
            if stderr_text == "TIMED_OUT":
                _log("error", "add_metadata timed out", user_id=user_id)
                if has_intro and injected_subs: await cleanup_files(*[s[0] for s in injected_subs])
                return input_path
            _log("error", "add_metadata fatal failure", stderr=stderr_text[:500])
            if has_intro and injected_subs: await cleanup_files(*[s[0] for s in injected_subs])
            return input_path  # Final fallback: return original file rather than failing task
        if not await aiofiles.os.path.exists(output_path):
            raise RuntimeError("add_metadata: output file not created")

        try:
            output_stat = await aiofiles.os.stat(output_path)
            output_size = output_stat.st_size
        except Exception as exc:
            _log("error", "add_metadata: failed to stat output", path=output_path, exc=str(exc))
            output_size = 0

        if output_size == 0:
            await cleanup_files(output_path)
            if has_intro and injected_subs: await cleanup_files(*[s[0] for s in injected_subs])
            _log("error", "add_metadata produced empty file", user_id=user_id)
            return input_path

        # File size is not enough: a remux can be nearly the same size while
        # silently omitting subtitle tracks. Never deliver that output.
        if input_subtitle_count:
            output_subtitle_count = _subtitle_stream_count(await _probe_streams(output_path))
            if output_subtitle_count < input_subtitle_count:
                _log(
                    "error", "add_metadata: subtitle loss detected; using original file",
                    input_subtitles=input_subtitle_count,
                    output_subtitles=output_subtitle_count,
                    pass_used=_pass_used,
                )
                await cleanup_files(output_path)
                return input_path

        # SIZE INTEGRITY GUARD: metadata injection should NEVER lose significant streams.
        SIZE_RATIO_FLOOR = _size_ratio_floor(has_errors, tolerant_input, 0.85)
        if input_size > 0:
            ratio = output_size / input_size
            if ratio < SIZE_RATIO_FLOOR:
                _log(
                    "error",
                    "add_metadata: size ratio below threshold — falling back to input file",
                    input_bytes=input_size,
                    output_bytes=output_size,
                    ratio=f"{ratio:.2%}",
                    stderr=stderr_text[:500],
                    tolerant_input=tolerant_input,
                )
                await cleanup_files(output_path)
                if has_intro and injected_subs: await cleanup_files(*[s[0] for s in injected_subs])
                return input_path   # safe fallback: return input untouched rather than a corrupt file

        # Log warnings even on success
        if stderr_text:
            _log("warning", "ffmpeg add_metadata stderr", stderr=stderr_text[:500])

        _log("info", "add_metadata complete",
             input_bytes=input_size, output_bytes=output_size,
             ratio=f"{output_size/input_size:.2%}" if input_size else "unknown")

    finally:
        if _audio_pre_encoded:
            try:
                await cleanup_files(_audio_pre_encoded)
            except Exception:
                pass
        if has_intro and injected_subs:
            try:
                await cleanup_files(*[s[0] for s in injected_subs])
            except Exception:
                pass

    return output_path





async def convert_to_mkv(
    input_path: str,
    output_path: str,
    timeout: int = 300,
    has_errors: bool = False,
) -> str:
    ffmpeg = _find_ffmpeg()
    if not ffmpeg:
        raise RuntimeError("ffmpeg not in PATH")
    if not await aiofiles.os.path.exists(input_path):
        raise FileNotFoundError(f"Input not found: {input_path}")

    input_subtitle_count = _subtitle_stream_count(await _probe_streams(input_path))

    # Capture input size BEFORE conversion for integrity check
    try:
        input_stat = await aiofiles.os.stat(input_path)
        input_size = input_stat.st_size
    except Exception as exc:
        _log("warning", "failed to stat input_path", path=input_path, exc=str(exc))
        input_size = 0

    output_dir = os.path.dirname(output_path)
    if output_dir:
        await aiofiles.os.makedirs(output_dir, exist_ok=True)

    # Only use err_detect if the file is already known-corrupt (Fix: passed from extract_media_info)
    err_detect_flags = ["-err_detect", "ignore_err"] if has_errors else []

    cmd = [
        ffmpeg, "-hide_banner",
        *err_detect_flags,
        "-i", input_path,
        "-map", "0", "-ignore_unknown",
        "-c", "copy",
        "-loglevel", "error",
        "-y", output_path,
    ]



    
    returncode, stderr_text = await _run_ffmpeg(cmd, timeout=timeout)

    # HARDENING: If it failed, try a safer mapping
    if returncode != 0:
        _log("warning", "convert_to_mkv first pass failed, retrying safe map", stderr=stderr_text[:200])
        await cleanup_files(output_path)
        
        safe_cmd = [
            ffmpeg, "-hide_banner",
            *err_detect_flags,
            "-i", input_path,
            "-map", "0:v?", "-map", "0:a?", "-map", "0:s?", "-ignore_unknown",
            "-c", "copy",
            "-loglevel", "error",
            "-y", output_path,
        ]
        returncode, stderr_text = await _run_ffmpeg(safe_cmd)

    if returncode != 0:
        await cleanup_files(output_path)
        if stderr_text == "TIMED_OUT":
            raise RuntimeError(f"convert_to_mkv timed out after {timeout}s")
        
        # FINAL ATTEMPT: Force subtitle translation (Fix 8: Codec incompatibility)
        _log("warning", "convert_to_mkv second pass failed, forcing subtitle translation", stderr=stderr_text[:200])
        sub_codec = "mov_text" if output_path.endswith(".mp4") else "srt"
        trans_cmd = [
            ffmpeg, "-hide_banner", *err_detect_flags, "-i", input_path,
            "-map", "0:v?", "-map", "0:a?", "-map", "0:s?", "-ignore_unknown",
            "-c:v", "copy", "-c:a", "copy", "-c:s", sub_codec,
            "-loglevel", "error", "-y", output_path,
        ]
        returncode, stderr_text = await _run_ffmpeg(trans_cmd)
        
        if returncode != 0:
            await cleanup_files(output_path)
            _log("warning", "ffmpeg convert_to_mkv fatal, falling back to original", stderr=stderr_text[:300])
            return input_path

    if not await aiofiles.os.path.exists(output_path):
        raise RuntimeError("convert_to_mkv: output not created")

    try:
        output_stat = await aiofiles.os.stat(output_path)
        output_size = output_stat.st_size
    except Exception as exc:
        _log("error", "failed to stat output_path", path=output_path, exc=str(exc))
        output_size = 0

    if output_size == 0:
        await cleanup_files(output_path)
        raise RuntimeError("convert_to_mkv: output is empty")

    if input_subtitle_count:
        output_subtitle_count = _subtitle_stream_count(await _probe_streams(output_path))
        if output_subtitle_count < input_subtitle_count:
            _log(
                "error", "convert_to_mkv: subtitle loss detected; using original file",
                input_subtitles=input_subtitle_count,
                output_subtitles=output_subtitle_count,
            )
            await cleanup_files(output_path)
            return input_path

    # SIZE INTEGRITY GUARD: output must be at least 70% of input
    SIZE_RATIO_FLOOR = 0.70
    if input_size > 0:
        ratio = output_size / input_size
        if ratio < SIZE_RATIO_FLOOR:
            _log(
                "error",
                "convert_to_mkv: output suspiciously small — stream loss detected, falling back",
                input_bytes=input_size,
                output_bytes=output_size,
                ratio=f"{ratio:.2%}",
                stderr=stderr_text[:300],
            )
            await cleanup_files(output_path)
            return input_path   # safe fallback: upload original intact file
        
        _log(
            "info", "convert_to_mkv complete",
            input_bytes=input_size,
            output_bytes=output_size,
            ratio=f"{ratio:.2%}",
        )
    
    # Log any warnings even on success
    if stderr_text:
        _log("debug", "ffmpeg convert_to_mkv stderr", stderr=stderr_text[:500])

    return output_path



def _truncate_utf8(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ""

    used = 0
    chars: list[str] = []
    for ch in text:
        ch_len = len(ch.encode("utf-8"))
        if used + ch_len > max_bytes:
            break
        chars.append(ch)
        used += ch_len
    return "".join(chars)


def _shorten_filename(filename: str, max_length: int = 255) -> str:
    """
    Enforce Telegram's hard 255-byte UTF-8 filename limit.
    No other arbitrary UI limits are applied.
    """
    filename = re.sub(r"\s+", " ", filename).strip()

    # ── UTF-8 byte-length enforcement (Telegram hard limit = 255 bytes) ──────
    if len(filename.encode("utf-8")) <= max_length:
        return filename

    base, ext = os.path.splitext(filename)
    ext_bytes = len(ext.encode("utf-8"))
    max_base_bytes = max_length - ext_bytes
    if max_base_bytes <= 0:
        return _truncate_utf8(filename, max_length)
    return _truncate_utf8(base, max_base_bytes).rstrip() + ext


def _format_exc_message(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text
    exc_name = exc.__class__.__name__.strip() or "UnknownError"
    return exc_name


def _has_effective_metadata_request(user_data: Optional[Dict[str, Any]]) -> bool:
    if not user_data:
        return False

    text_fields = (
        "title", "video", "audio", "subtitle", "artist", "author",
        "encoded_by", "custom_tag", "commentz", "album", "genre",
        "composer", "publisher", "copyright",
    )
    if any(str(user_data.get(field, "")).strip() for field in text_fields):
        return True

    feature_flags = ("metadata_purge", "intro_sub", "track_renaming")
    return any(bool(user_data.get(flag)) for flag in feature_flags)


def _extract_caption_filename_candidate(caption: Optional[str]) -> str:
    if not caption:
        return ""
    first_line = caption.strip().splitlines()[0].strip()
    if not first_line or "." not in first_line:
        return ""
    return os.path.basename(first_line.replace("\\", "/")).strip()


def _filename_parse_key(name: str) -> str:
    stem = os.path.splitext(os.path.basename(str(name).strip()))[0].lower()
    return re.sub(r"[^a-z0-9]+", "", stem)


def _is_better_caption_filename(current_name: str, caption_name: str) -> bool:
    if not current_name or not caption_name:
        return False

    current_key = _filename_parse_key(current_name)
    caption_key = _filename_parse_key(caption_name)
    if not current_key or not caption_key or current_key == caption_key:
        return False

    if len(caption_key) <= len(current_key):
        return False

    shared_prefix = os.path.commonprefix([current_key, caption_key])
    if current_key.startswith(shared_prefix):
        current_cov = len(shared_prefix) / max(len(current_key), 1)
    else:
        current_cov = 0.0

    current_ext = os.path.splitext(current_name)[1].lower()
    caption_ext = os.path.splitext(caption_name)[1].lower()
    ext_matches = bool(current_ext and caption_ext and current_ext == caption_ext)
    metadata_hint = bool(re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b|\b\d{3,4}p\b|\bWEB[ .-]?DL\b|\bE-?Sub\b", caption_name))

    return (
        ext_matches
        and current_cov >= 0.7
        and len(caption_key) - len(current_key) >= 8
        and metadata_hint
    )






# =============================================================================
# Token refund — idempotent via job_ledger
# =============================================================================
async def _refund_token_for_job(user_id: int, job_id: str) -> None:
    """Idempotent token refund. Uses job_ledger to ensure single refund."""
    try:
        should_refund = await job_ledger.refund_token_once(job_id)
        if should_refund:
            await DARKXSIDE78.refund_token(user_id, 1)
            _log("info", "Token refunded via job_ledger", user_id=user_id, job_id=job_id)
    except Exception as exc:
        _log("error", "Token refund failed", user_id=user_id, job_id=job_id, exc=str(exc))


# =============================================================================
# Main handler
# =============================================================================

def _supported_media_info(message: Message) -> Optional[Tuple[str, str, int]]:
    if message.document:
        return (
            message.document.file_id,
            message.document.file_name or "file",
            message.document.file_size or 0,
        )
    if message.video:
        return (
            message.video.file_id,
            message.video.file_name or "video",
            message.video.file_size or 0,
        )
    if message.audio:
        return (
            message.audio.file_id,
            message.audio.file_name or "audio",
            message.audio.file_size or 0,
        )
    return None


async def _enqueue_single_message(client: Client, message: Message, user_data: Optional[dict] = None) -> None:
    user_id = message.from_user.id
    media = _supported_media_info(message)
    if not media:
        return
    # --- VERIFICATION & TOKEN CHECK (PRE-ENQUEUE) ---
    if user_data is None:
        user_data = await DARKXSIDE78.get_user_prefs_batch(user_id)
    user_data = user_data or {}

    is_premium = bool(user_data.get("is_premium", False))
    if not is_premium:
        short_enabled = await DARKXSIDE78.get_variable("short_enabled", False)
        if short_enabled:
            short_mode = await DARKXSIDE78.get_variable("short_mode", "link")
            if short_mode == "24":
                # Verification mode (24h)
                verify_expiry = user_data.get("verify_expiry")
                if isinstance(verify_expiry, str):
                    try: verify_expiry = datetime.fromisoformat(verify_expiry)
                    except: verify_expiry = None
                elif isinstance(verify_expiry, (int, float)):
                    try: verify_expiry = datetime.fromtimestamp(verify_expiry, pytz.utc)
                    except: verify_expiry = None
                    
                if verify_expiry and getattr(verify_expiry, "tzinfo", None) is None:
                    try: verify_expiry = verify_expiry.replace(tzinfo=pytz.utc)
                    except: pass
                
                if not verify_expiry or datetime.now(pytz.utc) > verify_expiry:
                    await send_token_message(client, user_id, message_to_reply=message)
                    return
            else:
                # Token mode (Per-link): check tokens minus tasks already in queue
                tokens = int(user_data.get("token", 0))
                status = await task_queue.get_queue_status(user_id)
                active_tasks = status.get("total", 0)
                
                if tokens - active_tasks <= 0:
                    await send_token_message(client, user_id, message_to_reply=message)
                    return
    # -----------------------------------------------

    file_id, file_name, _file_size = media
    short_cancel_id = f"c{next(_cancel_id_gen)}"
    task_id = f"{user_id}:{message.chat.id}:{message.id}:{short_cancel_id}"

    await task_queue.register_cancel_id(short_cancel_id, task_id)
    task_queue.init_file_state(task_id, file_name)

    try:
        await job_ledger.create_job(
            user_id=user_id, chat_id=message.chat.id, file_id=file_id,
            file_name=file_name, message_id=message.id, job_id=task_id,
        )
    except Exception as exc:
        _log("warning", "create_job failed; continuing enqueue", task_id=task_id, exc=str(exc))

    # --- FIX: use an Event instead of a boolean + sleep race ---
    status_msg_ready = asyncio.Event()
    status_box = {"msg": None}
    
    # HIGH FIX: Snapshot user_data by value, not by reference.
    # The album loop may call _enqueue_single_message() multiple times sharing the
    # same user_data dict. Python closures capture by reference, so any mutation
    # of user_data between invocations (or between enqueue and execution) would
    # affect every already-queued task. A shallow copy is safe here because all
    # values are scalars / immutable strings from the DB batch result.
    _user_data_snapshot = dict(user_data) if user_data else {}

    def coro_factory(context: dict):
        async def _run():
            try:
                await asyncio.wait_for(status_msg_ready.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass 

            return await process_file_core(
                client, message, task_id,
                status_box["msg"] or message,
                short_cancel_id, user_data=_user_data_snapshot,
                attempt_context=context
            )
        return _run()

    position = await task_queue.add_task(user_id, task_id, file_id, message, coro_factory)

    if position == -1:
        await task_queue.remove_cancel_id(short_cancel_id)
        await task_queue._pop_file_state(task_id)
        status_msg_ready.set() # unblock waiting coro
        return
        
    # Send queue message immediately — no sleep, no race.
    m = await _safe_reply(
        message, 
        f"<b>Aᴅᴅᴇᴅ ᴛᴏ ᴛʜᴇ ǫᴜᴇᴜᴇ {position}</b>"
    )
    if m:
        status_box["msg"] = m
        task_queue.set_status_message(task_id, message.chat.id, m.id)
        try: 
            asyncio.create_task(job_ledger.set_status_message_id(task_id, m.id))
        except Exception: 
            pass

    # Signal the coro it can now proceed
    status_msg_ready.set()


async def _auto_rename_files_album_aware(client: Client, message: Message) -> None:
    user = message.from_user
    user_id = user.id if user else 0

    try:
        if not user:
            return

        try:
            await DARKXSIDE78.add_user(
                user_id, username=user.username, first_name=user.first_name
            )
        except Exception as exc:
            _log("warning", "add_user failed; continuing enqueue", user_id=user_id, exc=str(exc))

        if await admin_controls.system_svc.get_admin_mode() and user_id not in ADMINS:
            await _safe_reply(message, "Admin mode is active - only admins can use this bot!")
            return

        user_data = await DARKXSIDE78.get_user_prefs_batch(user_id)
        _log("debug", "user_data_keys", keys=list((user_data or {}).keys()), 
             is_premium=(user_data or {}).get("is_premium"),
             premium=(user_data or {}).get("premium"),
             track_renaming=(user_data or {}).get("track_renaming"))

        # ----------------------------------------

        media_group_id = getattr(message, "media_group_id", None)
        if media_group_id:
            group_key = f"{message.chat.id}:{media_group_id}"
            if group_key in _media_group_seen:
                return
            _media_group_seen[group_key] = time.monotonic()

            try:
                group_messages = await client.get_media_group(message.chat.id, message.id)
            except Exception as exc:
                _log(
                    "warning",
                    "get_media_group failed, falling back to single",
                    user_id=user_id,
                    exc=str(exc),
                )
                group_messages = [message]

            for group_message in group_messages:
                if not _supported_media_info(group_message):
                    continue
                try:
                    await _enqueue_single_message(client, group_message, user_data=user_data)
                except Exception as exc:
                    _log(
                        "error",
                        "Failed to enqueue album file",
                        user_id=user_id,
                        message_id=getattr(group_message, "id", None),
                        exc=str(exc),
                    )
            return

        await _enqueue_single_message(client, message, user_data=user_data)

    except Exception as exc:
        _log("error", "auto_rename_files handler crashed", user_id=user_id, exc=str(exc))
        await _safe_reply(message, "Failed to add this file to the queue. Please try again.")

@Client.on_message(
    filters.private & (filters.document | filters.video | filters.audio)
)
@check_fsub_and_ban_media
async def auto_rename_files(client: Client, message: Message) -> None:
    await _auto_rename_files_album_aware(client, message)




# =============================================================================
# Core processor
# =============================================================================

def _make_cancel_button(short_id: str) -> InlineKeyboardMarkup:
    cancel_kwargs = {
        "text": "Cᴀɴᴄᴇʟ",
        "callback_data": short_id,
    }
    try:
        cancel_btn = InlineKeyboardButton(**cancel_kwargs, style=ButtonStyle.DANGER)
    except Exception:
        try:
            cancel_btn = InlineKeyboardButton(**cancel_kwargs, color=ButtonStyle.DANGER)
        except Exception:
            cancel_btn = InlineKeyboardButton(**cancel_kwargs)

    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Mᴏʀᴇ Bᴏᴛs", url="https://telegram.me/ShadowedTomb"),
        cancel_btn,
    ]])

async def _resolve_thumbnail(
    client: Client,
    task_id: str,
    user_id: int,
    user_data: dict,
    title: str,
    source_text: str,
    message: Message,
    media_type: str,
) -> Optional[str]:
    thumb_path = None
    thumb_engine = user_data.get("thumb_engine", "off")

    if thumb_engine != "off":
        try:
            anime_title = title or extract_anime_title(source_text)
            if anime_title and anime_title.lower() != "unknown":
                _log("debug", "Attempting automated thumbnail", 
                     user_id=user_id, engine=thumb_engine, title=anime_title)
                
                result = await search_media_info(anime_title, engine=thumb_engine)
                if result.get("success") and result.get("poster_url"):
                    sess = await _get_http_session()
                    try:
                        _thumb_timeout = aiohttp.ClientTimeout(total=15, connect=5)
                        async with sess.get(result["poster_url"], timeout=_thumb_timeout) as resp:
                            if resp.status == 200:
                                raw_bytes = await resp.read()
                                if 0 < len(raw_bytes) <= 5 * 1024 * 1024:
                                    tp = await _prepare_thumb_path(task_id, "auto")
                                    async with aiofiles.open(tp, "wb") as fh:
                                        await fh.write(raw_bytes)
                                    if os.path.exists(tp) and os.path.getsize(tp) > 0:
                                        thumb_path = tp
                                        _log("info", "Automated thumbnail resolved",
                                             user_id=user_id, source=result.get("source"))
                                    else:
                                        await cleanup_files(tp)
                    except asyncio.TimeoutError:
                        _log("warning", "Thumbnail fetch timed out", user_id=user_id, url=result["poster_url"])
            else:
                _log("debug", "Skipping automated thumb: title unknown", user_id=user_id)
        except Exception as exc:
            _log("warning", "Automated thumbnail resolution failed", 
                 user_id=user_id, engine=thumb_engine, exc=str(exc))

    if not thumb_path:
        stored_thumb_id = user_data.get("file_id")
        if stored_thumb_id:
            _log("debug", "Falling back to manual thumbnail (/sthumb)", user_id=user_id)
            thumb_path = await _download_thumb_file(
                client, stored_thumb_id,
                await _prepare_thumb_path(task_id, "manual"),
                user_id=user_id, label="manual",
            )
        if not thumb_path:
            _log("debug", "No engine/manual thumb; attempting native extraction", user_id=user_id)
            native_thumbs = None
            if message.video and getattr(message.video, "thumbs", None):
                native_thumbs = message.video.thumbs
            elif message.document and getattr(message.document, "thumbs", None):
                native_thumbs = message.document.thumbs
            elif message.audio and getattr(message.audio, "thumbs", None):
                native_thumbs = message.audio.thumbs
            
            if native_thumbs and len(native_thumbs) > 0:
                thumb_path = await _download_thumb_file(
                    client, native_thumbs[-1].file_id,
                    await _prepare_thumb_path(task_id, "native"),
                    user_id=user_id, label="native",
                )

    if thumb_path:
        thumb_path = await process_thumbnail(thumb_path, media_type)
        if thumb_path and (not os.path.exists(thumb_path) or os.path.getsize(thumb_path) == 0):
            _log("warning", "Thumbnail missing or empty after processing — skipping", user_id=user_id)
            await cleanup_files(thumb_path)
            thumb_path = None

    return thumb_path

async def process_file_core(
    client: Client,
    message: Message,
    task_id: str,
    msg: Optional[Message] = None,
    short_cancel_id: Optional[str] = None,
    user_data: Optional[dict] = None,
    attempt_context: Optional[dict] = None,
) -> None:
    user = message.from_user
    if not user:
        raise RuntimeError("Missing from_user on incoming message")
    user_id = user.id

    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name or "file"
        media_type = "document"
        file_ext = os.path.splitext(file_name)[1].lower()
        file_size = message.document.file_size or 0
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or "video"
        media_type = "video"
        file_ext = ".mp4"
        file_size = message.video.file_size or 0
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or "audio"
        media_type = "audio"
        file_ext = os.path.splitext(file_name)[1].lower() if file_name else ".mp3"
        file_size = message.audio.file_size or 0
    else:
        return

    if not short_cancel_id:
        short_cancel_id = f"c{next(_cancel_id_gen)}"
        await task_queue.register_cancel_id(short_cancel_id, task_id)
        task_queue.init_file_state(task_id, file_name)

    attempt_context = attempt_context or {}
    upload_stage_reached = False
    should_cleanup = True

    file_path: Optional[str] = None
    download_path: Optional[str] = None
    metadata_path: Optional[str] = None
    thumb_path: Optional[str] = None
    output_path: Optional[str] = None
    current_pdf: Optional[str] = None
    final_filename: str = file_name

    cancel_button = InlineKeyboardMarkup([[
        InlineKeyboardButton("🚫 Cᴀɴᴄᴇʟ 🚫", callback_data=f"cancel_{short_cancel_id}")
    ]])

    class _SkipPrepare(Exception): pass

    try:
        try:
            if "prepared" in attempt_context:
                p = attempt_context["prepared"]
                final_filename = p["final_filename"]
                file_path = p["file_path"]
                file_size = p["file_size"]
                media_info = p["media_info"]
                media_preference = p["media_preference"]
                media_type = p["media_type"]
                caption = p["caption"]
                thumb_path = p["thumb_path"]
                download_path = p["download_path"]
                metadata_path = p["metadata_path"]
                current_pdf = p["current_pdf"]
                output_path = p["output_path"]
                file_ext = p["file_ext"]
                file_name = p["file_name"]
                upload_stage_reached = True
                raise _SkipPrepare()

                # --- VERIFICATION & TOKEN CHECK (PRE-STATUS) ---
            if user_data is None:
                user_data = await DARKXSIDE78.get_user_prefs_batch(user_id)
            user_data = user_data or {}

            is_premium = bool(user_data.get("is_premium") or user_data.get("premium"))
            token_deducted = False

            async def _maybe_refund() -> None:
                if token_deducted:
                    await _refund_token_for_job(user_id, task_id)

            if not is_premium:
                short_enabled = await DARKXSIDE78.get_variable("short_enabled", False)
                if short_enabled:
                    short_mode = await DARKXSIDE78.get_variable("short_mode", "link")
                    if short_mode == "24":
                        verify_expiry = user_data.get("verify_expiry")
                        if isinstance(verify_expiry, str):
                            try: verify_expiry = datetime.fromisoformat(verify_expiry)
                            except: verify_expiry = None
                        if verify_expiry and verify_expiry.tzinfo is None:
                            verify_expiry = verify_expiry.replace(tzinfo=pytz.utc)

                        if not verify_expiry or datetime.now(pytz.utc) > verify_expiry:
                            await send_token_message(client, user_id, message_to_reply=message)
                            await job_ledger.mark_failed(task_id, "Verification required")
                            raise asyncio.CancelledError()
                    else:
                        # Per-link mode: Deduct token BEFORE sending status
                        should_charge = await job_ledger.charge_token_once(task_id)
                        if should_charge:
                            success = await DARKXSIDE78.deduct_token(user_id, 1)
                            if not success:
                                await job_ledger.refund_token_once(task_id)
                                await _safe_reply(message, "<b><blockquote>✗ ʏᴏᴜ ʜᴀᴠᴇ 𝟶 ᴛᴏᴋᴇɴs !</blockquote>\n<blockquote>›› Pʟᴇᴀsᴇ ʙᴜʏ ᴘʀᴇᴍɪᴜᴍ ᴏʀ ɢᴇᴛ ᴍᴏʀᴇ ᴛᴏᴋᴇɴs ᴜsɪɴɢ /gentoken ᴛᴏ ᴄᴏɴᴛɪɴᴜᴇ.</blockquote></b>")
                                await send_token_message(client, user_id, message_to_reply=message)
                                await job_ledger.mark_failed(task_id, "Insufficient tokens")
                                raise asyncio.CancelledError()
                        token_deducted = True

        # -----------------------------------------------

            cancel_button = _make_cancel_button(short_cancel_id)

            if not msg or msg == message:
                new_msg = await _safe_reply(message, "<b>Pʀᴇᴘᴀʀɪɴɢ ᴛᴏ Dᴏᴡɴʟᴏᴀᴅ...</b>")
                if new_msg:
                    msg = new_msg

            if msg and msg != message:
                task_queue.set_status_message(task_id, message.chat.id, msg.id)
                asyncio.create_task(job_ledger.set_status_message_id(task_id, msg.id))
            elif not msg:
                msg = message
            # path variables already initialized at the top


            # -- Preferences (from batch query — zero additional DB calls) ------
            media_preference: str = user_data.get("media_type", "document")
            format_template: Optional[str] = user_data.get("format_template")
            metadata_source: str = user_data.get("metadata_source", "filename")

            if not format_template:
                await _safe_edit(
                    msg,
                    "<blockquote>ⓘ 𝗔𝗨𝗧𝗢 𝗥𝗘𝗡𝗔𝗠𝗘 𝗙𝗢𝗥𝗠𝗔𝗧 𝗡𝗢𝗧 𝗦𝗘𝗧</blockquote>\n\n"
                    "◍  ɴᴏ ᴀᴜᴛᴏ ʀᴇɴᴀᴍᴇ ꜰᴏʀᴍᴀᴛ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ꜰᴏʀ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ. ᴘʟᴇᴀꜱᴇ ꜱᴇᴛ ᴀ ᴠᴀʟɪᴅ ʀᴇɴᴀᴍᴇ ꜰᴏʀᴍᴀᴛ ᴛᴏ ᴇɴᴀʙʟᴇ ᴀᴜᴛᴏ ʀᴇɴᴀᴍɪɴɢ ꜰᴇᴀᴛᴜʀᴇꜱ.\n\n"
                    "<blockquote><b>➥ ᴜꜱᴇ : /format\n◍ ᴄʜᴇᴄᴋ ᴀʟʟ ꜱᴜᴘᴘᴏʀᴛᴇᴅ ꜰᴏʀᴍᴀᴛ ᴅᴇᴛᴀɪʟꜱ</b></blockquote>"
                )
                await _maybe_refund()
                raise asyncio.CancelledError()

            _PYROGRAM_PLACEHOLDERS = {"video", "audio", "file", "document"}
            _fn_stem = os.path.splitext(file_name)[0].lower()

            # Forwarded captions usually belong to the source channel, so keep
            # filename-first behavior unless the caption exposes a strongly-matching,
            # richer filename than Telegram's truncated media name.
            _is_forwarded = bool(
                getattr(message, "forward_origin", None) or
                getattr(message, "forward_date", None)
            )

            caption_filename = _extract_caption_filename_candidate(message.caption)
            trusted_caption_filename = _is_better_caption_filename(file_name, caption_filename)

            if metadata_source == "caption" and message.caption and not _is_forwarded:
                source_text = message.caption
            elif _fn_stem in _PYROGRAM_PLACEHOLDERS and message.caption and not _is_forwarded:
                # Pyrogram placeholder filename — caption has the real name (only if NOT forwarded)
                source_text = message.caption
                _log("info", "Placeholder filename detected, falling back to caption",
                     placeholder=file_name, task_id=task_id)
            elif trusted_caption_filename:
                source_text = caption_filename
                _log("info", "Truncated Telegram filename detected, using caption filename for parsing",
                     file_name=file_name, caption_name=caption_filename, task_id=task_id)
            else:
                source_text = file_name

            # Sync file_name if Telegram sent a placeholder or a trusted truncated variant
            if (
                (_fn_stem in _PYROGRAM_PLACEHOLDERS and message.caption and not _is_forwarded)
                or trusted_caption_filename
            ):
                if caption_filename and len(caption_filename) > 3:
                    file_name = caption_filename if "." in caption_filename else file_name



            # -- Anti-NSFW Pre-Download Check --
            if await check_anti_nsfw(source_text, message) or await check_anti_nsfw(file_name, message):
                await _safe_delete(msg)
                await _maybe_refund()
                raise asyncio.CancelledError()

            # -- Analyse -------------------------------------------------------
            engine = PeakTokenEngineV2_REXY(source_text)
            parsed_data = engine.parse()
            title = parsed_data.get('title')
            season = parsed_data.get('season')
            episode = parsed_data.get('episode')
            chapter = parsed_data.get('chapter')
            volume = parsed_data.get('volume')
            year = parsed_data.get('year') or ""
            group = parsed_data.get('group') or ""
            quality = parsed_data.get('quality') or ""

            cbz_to_pdf_mode = bool(user_data.get("cbz_to_pdf_mode", False))
            is_cbz = file_ext.lower() == ".cbz"

            if is_cbz and cbz_to_pdf_mode:
                ext = ".pdf"
            elif media_type == "video" and media_preference == "document":
                ext = ".mkv"
            elif media_type == "document" and media_preference == "video":
                # Keep the temporary download path aligned with the source container;
                # the user-facing video output is remuxed to MP4 later when needed.
                ext = ".mkv" if file_ext.lower() == ".mkv" else ".mp4"
            elif file_ext.lower() == ".pdf" or (is_cbz and cbz_to_pdf_mode):
                ext = ".pdf"
            else:
                # Fallback: if video preference is set, use original extension if it's a common video format
                if media_preference == "video" and file_ext.lower() in (".mkv", ".mp4", ".webm", ".mov"):
                    ext = file_ext
                else:
                    ext = file_ext or (".mp4" if media_type == "video" else ".mp3")

            uid_hash = hashlib.sha256(
                f"{file_id}_{time.monotonic_ns()}".encode()
            ).hexdigest()[:16]
            download_path = f"downloads/{uid_hash}{ext}"
            metadata_path = f"metadata/{uid_hash}{ext}"
            output_path = f"processed/{uid_hash}{ext}"

            for d in ("downloads", "metadata", "processed"):
                await aiofiles.os.makedirs(d, exist_ok=True)

            if task_queue.is_cancelled(task_id):
                raise asyncio.CancelledError()

            # -- Download ------------------------------------------------------
            # Pre-flight: ensure enough disk space before starting (2× headroom)
            if not await _has_disk_space(file_size):
                await _safe_edit(msg, "<blockquote>✗ <b>Sᴇʀᴠᴇʀ ᴅɪsᴋ is ꜰᴜʟʟ. Tʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ.</blockquote></b>")
                await _maybe_refund()
                return

            await job_ledger.update_status(task_id, STATUS_DOWNLOADING)
            await _safe_edit(msg, "<b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ...</b>")

            _dl_start = time.time()
            _dl_msg = msg   # FIX C1+C4: snapshot msg so closure is not affected by later reassignments
            async def _dl_progress(current: int, total: int) -> None:
                await progress_for_pyrogram(
                    current, total,
                    f"<b><blockquote expandable>❐ {file_name}</blockquote></b>\n"
                    f"<blockquote><b>➥ Dᴏᴡɴʟᴏᴀᴅɪɴɢ</b>...</blockquote>",
                    _dl_msg, _dl_start, client, cancel_button,
                )

            await _dl_progress(0, max(int(file_size or 0), 1))

            dl_progress = make_cancel_aware_progress(
                task_id, task_queue, _dl_progress
            )

            # Prevent partial file corruption / force clean re-download
            if download_path and await aiofiles.os.path.exists(download_path):
                await aiofiles.os.remove(download_path)

            download_retries = 3
            for attempt in range(download_retries):
                try:
                    if task_queue.is_cancelled(task_id):
                        raise asyncio.CancelledError()

                    file_path = await asyncio.wait_for(
                        client.download_media(
                            message,
                            file_name=download_path,
                            progress=dl_progress,
                        ),
                        timeout=_dl_timeout(file_size),
                    )

                    # Verify download integrity
                    if not file_path or not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                        raise Exception("DOWNLOAD_ZERO_BYTES: File size equals to 0 B")

                    break  # success
                except asyncio.TimeoutError:
                    if attempt == download_retries - 1:
                        await _safe_edit(msg, f"<blockquote expandable>❐ {file_name}</blockquote>\n<blockquote>✗ <b>Dᴏᴡɴʟᴏᴀᴅ ᴛɪᴍᴇᴅ ᴏᴜᴛ.</b></blockquote>")
                        raise
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    exc_str = str(exc)
                    # Gracefully handle if the user deleted the source message
                    if "MessageIdInvalid" in exc_str or "MESSAGE_ID_INVALID" in exc_str or "MESSAGE_DELETED" in exc_str:
                        _log("info", "Source message deleted by user", task_id=task_id, user_id=user_id)
                        await _safe_edit(msg, f"<blockquote expandable>❐ {file_name}</blockquote>\n<blockquote>ⓘ <b>Sᴏᴜʀᴄᴇ ᴍᴇssᴀɢᴇ ᴡᴀs ᴅᴇʟᴇᴛᴇᴅ. Sᴋɪᴘᴘɪɴɢ...</b></blockquote>")
                        await _maybe_refund()
                        raise asyncio.CancelledError()

                    # Handle Pyrogram's DC migration race condition AND silent 0-byte drops
                    if "AUTH_BYTES_INVALID" in exc_str or "ImportAuthorization" in exc_str or "DOWNLOAD_ZERO_BYTES" in exc_str:
                        if attempt < download_retries - 1:
                            _log("warning", f"Download glitch ({exc_str}). Retrying in 2s... ({attempt+1}/{download_retries})", task_id=task_id)
                            await asyncio.sleep(2)
                            continue

                    # If we're out of retries or it's a different error
                    if attempt == download_retries - 1:
                        await _safe_edit(msg, f"<blockquote expandable>❐ {file_name}</blockquote>\n<blockquote>Dᴏᴡɴʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {exc}</blockquote>")
                        raise
            if task_queue.is_cancelled(task_id):
                raise asyncio.CancelledError()

            await asyncio.sleep(0.2)
            await _safe_edit(msg, "<b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ Cᴏᴍᴘʟᴇᴛᴇ</b>")

            # -- CBZ to PDF Conversion (Manga Automation) ----------------------
            if is_cbz and cbz_to_pdf_mode:
                try:
                    await _safe_edit(msg, "<b>Cᴏɴᴠᴇʀᴛɪɴɢ CBZ ᴛᴏ PDF...</b>")
                    # Use a temporary suffix; cleanup_files handles download_path if we swap it.
                    conversion_out = f"{file_path}.converted.pdf"
                    try:
                        # Run the hardened conversion with progress UI support
                        await pdf_svc.cbz_to_pdf(file_path, conversion_out, msg=msg)

                        if os.path.exists(conversion_out):
                            # Promote the PDF: remove original CBZ, update trackers
                            if os.path.exists(file_path):
                                await aiofiles.os.remove(file_path)

                            file_path = conversion_out
                            download_path = conversion_out
                            # Update metadata trackers for downstream consistency
                            file_size = os.path.getsize(file_path)
                            file_ext = ".pdf"
                            _log("info", "CBZ to PDF conversion successful", task_id=task_id)
                        else:
                            raise ValueError("Conversion engine failed to produce an output file.")
                    except Exception as inner_e:
                        # Cleanup partial conversion file on internal failure
                        if os.path.exists(conversion_out):
                            with suppress(Exception): await aiofiles.os.remove(conversion_out)
                        raise inner_e
                except Exception as e:
                    _log("error", "CBZ to PDF stage failed", task_id=task_id, exc=str(e))
                    await _safe_edit(msg, f"<b>✗ CBZ ᴛᴏ PDF Fᴀɪʟᴇᴅ:</b> <code>{str(e)[:150]}</code>")
                    # We fail the task here because delivering a CBZ when the user expects a PDF (and potentially banners) is inconsistent.
                    raise e

            # -- Verify Download Integrity (Fix 3) -----------------------------
            try:
                _actual_size = os.path.getsize(file_path)
                _log("info", "Download verified", 
                     task_id=task_id, downloaded_bytes=_actual_size, 
                     declared_bytes=file_size)

                if file_size > 0:
                    ratio = _actual_size / file_size
                    if ratio < 0.95 or ratio > 1.05:
                        _log("warning", "Download size mismatch vs Telegram metadata",
                             declared=file_size, actual=_actual_size, ratio=f"{ratio:.2%}")
            except Exception as exc:
                _log("warning", "Failed to verify download size ground truth", task_id=task_id, exc=str(exc))


            # -- Analyse -------------------------------------------------------
            media_info = await extract_media_info_cached(file_path, file_id, file_size)

            # Precedence: ffprobe first for technicals, parser first for semantics
            res_val = media_info.get("resolution") or quality or ""
            v_codec = media_info.get("video_codec") or parsed_data.get('video_codec') or ""
            a_codec = media_info.get("audio_codec") or parsed_data.get('audio_codec') or ""
            a_layout = media_info.get("audio_layout") or parsed_data.get('audio_layout') or ""

            langs = media_info.get("all_languages") or parsed_data.get('languages', [])
            primary_lang = langs[0] if langs else (parsed_data.get('language') or "")

            # Derived Sleek Audio Tag
            audio_label = get_audio_label_from_info(media_info, parsed_data.get('release_flags'))

            size = humanbytes(file_size)

            # Descriptive Audio Tracks label synthesis
            at_count = media_info.get("audio_tracks_count", 0)
            if at_count >= 4: at_label = "Multi Audio"
            elif at_count == 3: at_label = "Tri Audio"
            elif at_count == 2: at_label = "Dual Audio"
            else: at_label = parsed_data.get('audio_tracks') or ""

            # CRITICAL FIX: include the four file-level placeholders that users expect to
            # work in filename format templates.  Previously these were only expanded inside
            # add_metadata() for *metadata field values*.  When used in a format template
            # (e.g. "{title} {filename}") they survived the substitution loop untouched and
            # were then silently wiped by the clean-shell guard below.
            replacements: Dict[str, str] = {
                # ── file identity ──────────────────────────────────────────────
                "{filename}": file_name,
                "{basename}": os.path.splitext(file_name)[0],
                "{ext}": file_ext.lstrip("."),
                "{user_id}": str(user_id),
                # ── parsed metadata ────────────────────────────────────────────
                "{season}": season or "", "{Season}": season or "", "{SEASON}": season or "",
                "{episode}": episode or "", "{Episode}": episode or "", "{EPISODE}": episode or "",
                "{episode_start}": parsed_data.get('episode_start') or "", "{episode_end}": parsed_data.get('episode_end') or "",
                "{chapter}": chapter or "", "{Chapter}": chapter or "", "{CHAPTER}": chapter or "",
                "{volume}": volume or "", "{Volume}": volume or "", "{VOLUME}": volume or "",
                "{quality}": quality or "", "{Quality}": quality or "", "{QUALITY}": quality or "",
                "{resolution}": res_val, "{Resolution}": res_val, "{RESOLUTION}": res_val,
                "{audio}": audio_label, "{Audio}": audio_label, "{AUDIO}": audio_label,
                "{audio_codec}": a_codec, "{audio_layout}": a_layout,
                "{audio_tracks}": at_label,
                "{audio_tracks_count}": str(at_count),
                "{video_codec}": v_codec, "{bit_depth}": parsed_data.get('bit_depth') or "",
                "{source}": parsed_data.get('source') or "", "{edition}": parsed_data.get('edition') or "",
                "{release_flags}": parsed_data.get('release_flags') or "",
                "{title}": title or "", "{Title}": title or "", "{TITLE}": title or "",
                "{year}": year, "{Year}": year, "{YEAR}": year,
                "{group}": group, "{Group}": group, "{GROUP}": group,
                "{language}": primary_lang,
                "{languages}": " + ".join(langs),
                "{episode_type}": parsed_data.get('episode_type') or "",
                "{type}": parsed_data.get('type') or "",
                "{episode_absolute}": parsed_data.get('episode_absolute') or "",
                "{part}": parsed_data.get('part') or "",
                "{season_episode}": parsed_data.get('season_episode') or "",
                "{mode}": parsed_data.get('mode', 'MOVIE'),
                "{size}": size, "{Size}": size, "{SIZE}": size,
            }

            new_filename = format_template
            for ph, val in replacements.items():
                new_filename = new_filename.replace(ph, str(val))

            # -- Clean-Shell Guard: Purge unreplaced {tags} and artifacts --
            new_filename = re.sub(r"\{[a-zA-Z0-9_]+\}", "", new_filename)
            new_filename = re.sub(r"\[\s*\]|\(\s*\)", "", new_filename)
            new_filename = re.sub(r"\s*\[\s*-\s*\]\s*", " ", new_filename)
            new_filename = re.sub(r"\s*\(\s*-\s*\)\s*", " ", new_filename)
            new_filename = re.sub(r"\s*-\s*-\s*", " - ", new_filename)
            new_filename = re.sub(r"\s{2,}", " ", new_filename).strip(" .-_")
            if not new_filename.lower().endswith(ext.lower()):
                new_filename += ext

            # Rely on default max_length (200) instead of artificially cutting at 100
            final_filename = _shorten_filename(new_filename)
            final_filename = os.path.basename(final_filename.replace("\\", "/"))

            thumb_task = asyncio.create_task(
                _resolve_thumbnail(
                    client, task_id, user_id, user_data, title, source_text, message, media_type
                )
            )

            # -- Anti-NSFW Post-Format Check --
            if await check_anti_nsfw(final_filename, message):
                await _safe_delete(msg)
                await _maybe_refund()
                raise asyncio.CancelledError()

            new_dl = os.path.join("downloads", uid_hash, final_filename)
            new_md = os.path.join("metadata", uid_hash, final_filename)
            new_out = os.path.join("processed", uid_hash, final_filename)
            for path in (new_dl, new_md, new_out):
                await aiofiles.os.makedirs(os.path.dirname(path), exist_ok=True)
            if file_path and await aiofiles.os.path.exists(file_path):
                await aiofiles.os.rename(file_path, new_dl)
            file_path = new_dl
            download_path = new_dl
            metadata_path = new_md
            output_path = new_out

            if task_queue.is_cancelled(task_id):
                raise asyncio.CancelledError()

            await job_ledger.update_status(task_id, STATUS_PROCESSING)
            await _safe_edit(msg, "<b>Pʀᴏᴄᴇssɪɴɢ ғɪʟᴇ...</b>")

            # -- Smart Bypass Decision (Fix 21: CPU Optimisation) -------------
            meta_mode = user_data.get("metadata", "Off")
            is_meta_on = (
                meta_mode is True
                or str(meta_mode).lower() in ("on", "true", "1", "yes")
            )
            has_effective_metadata = _has_effective_metadata_request(user_data)
            needs_conversion = (media_type != media_preference)

            # We only NEED ffmpeg if we are injecting metadata OR converting containers.
            # Everything else (Rename, Thumb Preview) can be done via Bypass.
            needs_ffmpeg = needs_conversion or (is_meta_on and has_effective_metadata)

            if file_ext == ".pdf":
                # -- PDF Special Path (No FFmpeg) -----------------------------
                # PDF settings from batch query
                pdf_banner_on = bool(user_data.get("pdf_banner_mode", False))
                pdf_banner_file = user_data.get("pdf_banner")
                pdf_banner_last_file = user_data.get("pdf_banner_last")
                pdf_lock_on = bool(user_data.get("pdf_lock_mode", False))
                pdf_lock_password = user_data.get("pdf_lock_password")
                pdf_banner_placement = user_data.get("pdf_banner_placement", "first")
                pdf_banner_url = user_data.get("pdf_banner_url")
                current_pdf = download_path

                if pdf_banner_on and (pdf_banner_file or pdf_banner_last_file):
                    try:
                        await _safe_edit(msg, "<b>Aᴅᴅɪɴɢ PDF Bᴀɴɴᴇʀ...</b>")
                        temp_banner = f"{download_path}_banner.jpg"
                        temp_banner_last = f"{download_path}_banner_last.jpg"

                        page_args = []
                        if pdf_banner_placement == "both":
                            page_args = ["first", "last"]
                        else:
                            page_args = [pdf_banner_placement]

                        # Download necessary banners
                        if pdf_banner_file:
                            await client.download_media(pdf_banner_file, file_name=temp_banner)
                        if pdf_banner_last_file and "last" in page_args:
                            await client.download_media(pdf_banner_last_file, file_name=temp_banner_last)

                        bannered = current_pdf.replace(".pdf", "_bannered.pdf")
                        await pdf_svc.add_banner(
                            current_pdf, bannered, 
                            banner_path=temp_banner if pdf_banner_file else temp_banner_last, 
                            page_args=page_args, 
                            banner_last_path=temp_banner_last if os.path.exists(temp_banner_last) else None,
                            banner_url=pdf_banner_url,
                            banner_id=pdf_banner_file or pdf_banner_last_file
                        )

                        await cleanup_files(current_pdf, temp_banner, temp_banner_last, temp_banner + ".pdf", temp_banner_last + ".pdf")
                        current_pdf = bannered
                    except Exception as exc:
                        _log("error", "PDF banner failed", user_id=user_id, exc=str(exc))

                if pdf_lock_on and pdf_lock_password:
                    try:
                        await _safe_edit(msg, "<b>Lᴏᴄᴋɪɴɢ PDF...</b>")
                        locked = current_pdf.replace(".pdf", "_locked.pdf")
                        await pdf_svc.lock_pdf(current_pdf, locked, pdf_lock_password)
                        await cleanup_files(current_pdf)
                        current_pdf = locked
                    except Exception as exc:
                        _log("error", "PDF lock failed", user_id=user_id, exc=str(exc))

                await aiofiles.os.rename(current_pdf, output_path)
                current_pdf = None
                file_path = output_path
                download_path = None

            elif not needs_ffmpeg:
                # -- Smart Bypass Path (Rename Only) --------------------------
                _log("info", "Smart Bypass activated: skipping ffmpeg stage", task_id=task_id)
                # Ensure output directory exists before move
                await aiofiles.os.makedirs(os.path.dirname(output_path), exist_ok=True)

                if await _safe_move(download_path, output_path):
                    file_path = output_path
                    download_path = None  # Moved successfully; do NOT delete via cleanup
                else:
                    _log("error", "Smart Bypass move failed — falling back to download_path", task_id=task_id)
                    file_path = download_path
                    output_path = None  # Move failed; output was never created

                # FIX H2: Sync metadata_path extension on bypass path too
                # (was only happening in the heavy FFmpeg branch, causing wrong-ext on bypass)
                actual_ext = os.path.splitext(file_path)[1]
                if actual_ext and metadata_path and actual_ext != os.path.splitext(metadata_path)[1]:
                    metadata_path = os.path.splitext(metadata_path)[0] + actual_ext

            else:
                # -- Heavy Path (FFmpeg Required) -----------------------------
                if media_type == "video" and media_preference == "document":
                    # Ensure the output has .mkv extension so ffmpeg uses the right muxer
                    output_path = os.path.splitext(new_out)[0] + ".mkv"
                    try:
                        await convert_to_mkv(
                            download_path, output_path,
                            has_errors=media_info.get("has_errors", False)
                        )
                        if os.path.exists(output_path):
                            file_path = output_path
                        else:
                            file_path = download_path
                    except Exception as exc:
                        await _safe_edit(msg, f"<blockquote expandable>❐ {final_filename}</blockquote>\n<blockquote>Vɪᴅᴇᴏ ᴄᴏɴᴠᴇʀsɪᴏɴ ғᴀɪʟᴇᴅ: {exc}</blockquote>")
                        raise
                elif media_type == "document" and media_preference == "video":
                    # Remux MKV/document sources to MP4 so the result can be uploaded
                    # as a real Telegram video instead of staying a document.
                    output_path = os.path.splitext(new_out)[0] + ".mp4"
                    try:
                        await convert_to_mkv(
                            download_path, output_path,
                            has_errors=media_info.get("has_errors", False)
                        )
                        if os.path.exists(output_path):
                            file_path = output_path
                        else:
                            file_path = download_path
                    except Exception as exc:
                        await _safe_edit(msg, f"<blockquote expandable>❐ {final_filename}</blockquote>\n<blockquote>Vɪᴅᴇᴏ ᴄᴏɴᴠᴇʀsɪᴏɴ ғᴀɪʟᴇᴅ: {exc}</blockquote>")
                        raise
                else:
                    file_path = download_path

                # MEDIUM FIX: Sync metadata_path extension to the actual file extension
                # for ALL paths (video conversion AND direct audio).  Previously this only
                # ran inside the video-conversion branch so audio tasks with a mismatched
                # extension (e.g. Pyrogram placeholder ".m4a" for an actual ".mp3") wrote
                # metadata to the wrong path and add_metadata silently used the wrong file.
                actual_ext = os.path.splitext(file_path)[1]
                if actual_ext and actual_ext != os.path.splitext(metadata_path)[1]:
                    metadata_path = os.path.splitext(metadata_path)[0] + actual_ext

                # -- Metadata injection -------------------------------------------
                if is_meta_on and has_effective_metadata and (
                    media_type in ("video", "audio") or (media_type == "document" and file_ext != ".pdf")
                ):
                    try:
                        final_media_info = media_info
                        _log("info", "pre-metadata state",
                             file_path=file_path,
                             metadata_path=metadata_path,
                             is_meta_on=is_meta_on,
                             meta_mode=meta_mode,
                             file_exists=os.path.exists(file_path),
                             has_errors=final_media_info.get("has_errors", False))

                        if user_data.get("audio_conv_enabled", False):
                            await _safe_edit(msg, "<b>Aᴅᴅɪɴɢ Mᴇᴛᴀᴅᴀᴛᴀ & Cᴏɴᴠᴇʀᴛɪɴɢ Aᴜᴅɪᴏ...</b>\n\n<blockquote><i>(Tʜɪs ᴍᴀʏ ᴛᴀᴋᴇ ᴀ ғᴇᴡ ᴍɪɴᴜᴛᴇs ᴅᴇᴘᴇɴᴅɪɴɢ ᴏɴ ғɪʟᴇ sɪᴢᴇ)</i></blockquote>")
                        else:
                            await _safe_edit(msg, "<b>Aᴅᴅɪɴɢ Mᴇᴛᴀᴅᴀᴛᴀ...</b>")

                        from plugins.audio_ui import resolve_audio_transcode
                        transcode_res = await resolve_audio_transcode(
                            user_id, file_path, user_data,
                            probed_streams=final_media_info.get("streams"),
                        )

                        if transcode_res.get("skip") and transcode_res.get("skip_reason"):
                            try:
                                await message.reply(transcode_res["skip_reason"], parse_mode=ParseMode.HTML)
                            except Exception as e:
                                _log("warning", f"Failed to send skip_reason: {e}")

                        _meta_t0 = time.monotonic()
                        file_path = await add_metadata(
                            file_path,
                            metadata_path,
                            user_id,
                            user_data=user_data,
                            has_errors=media_info.get("has_errors", False) or final_media_info.get("has_errors", False),
                            probed_streams=final_media_info.get("streams"),
                            transcode_res=transcode_res,
                        )
                        _log("info", "add_metadata wall time", seconds=f"{time.monotonic() - _meta_t0:.1f}")
                        if file_path == metadata_path:
                            metadata_path = None
                    except Exception as exc:
                        _log("error", "add_metadata exception caught in core", user_id=user_id, exc=str(exc))
                        # Fallback to current file_path (don't fail the whole task)
                        pass

            # -- Stats update --------------------------------------------------
            await _safe_edit(msg, "<b>Pʀᴇᴘᴀʀɪɴɢ Uᴘʟᴏᴀᴅ...</b>")
            try:
                await DARKXSIDE78.update_rename_stats(user_id, file_size)
            except Exception as exc:
                _log("warning", "rename stats update failed", user_id=user_id, exc=str(exc))

            if task_queue.is_cancelled(task_id):
                raise asyncio.CancelledError()

            # -- Caption -------------------------------------------------------
            mime = mimetypes.guess_type(file_path)[0] or ""
            ext_only = os.path.splitext(file_path)[1][1:]
            file_date = (
                message.date.strftime("%Y-%m-%d %H:%M")
                if hasattr(message, "date")
                else datetime.now().strftime("%Y-%m-%d %H:%M")
            )
            channel = ""
            fc = None
            f_orig = getattr(message, "forward_origin", None)
            if f_orig and hasattr(f_orig, "chat"):
                fc = f_orig.chat

            if fc:
                channel = f"@{fc.username}" if getattr(fc, "username", None) else str(fc.id)

            _video_exts = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".flv"}
            if hasattr(message, "duration") and message.duration:
                duration_str = time.strftime("%H:%M:%S", time.gmtime(message.duration))
            elif file_ext in _video_exts:
                secs = media_info.get("duration_seconds", 0)
                duration_str = time.strftime("%H:%M:%S", time.gmtime(secs)) if secs > 0 else ""
            else:
                duration_str = ""

            try:
                stat_info = await aiofiles.os.stat(file_path)
                filesize_str = humanbytes(stat_info.st_size)
            except Exception:
                filesize_str = ""

            caption_template = user_data.get("caption")
            if caption_template:
                caption = caption_template
                cap_replacements = replacements.copy()
                cap_replacements.update({
                    "{filename}": final_filename, "{filesize}": filesize_str,
                    "{duration}": duration_str, "{mime}": mime, "{ext}": ext_only,
                    "{date}": file_date, "{channel}": channel
                })
                for ph, val in cap_replacements.items():
                    caption = caption.replace(ph, str(val))
            else:
                caption = f"**{final_filename}**"

            thumb_path = await thumb_task

            attempt_context["prepared"] = {
                "final_filename": final_filename,
                "file_path": file_path,
                "file_size": file_size,
                "media_info": media_info,
                "media_preference": media_preference,
                "media_type": media_type,
                "caption": caption,
                "thumb_path": thumb_path,
                "download_path": download_path,
                "metadata_path": metadata_path,
                "current_pdf": current_pdf,
                "output_path": output_path,
                "file_ext": file_ext,
                "file_name": file_name,
            }

        except _SkipPrepare:
            pass

        upload_stage_reached = True

        if task_queue.is_cancelled(task_id):
            raise asyncio.CancelledError()

        # -- Upload --------------------------------------------------------
        await job_ledger.update_status(task_id, STATUS_UPLOADING)
        await _safe_edit(msg, "<b>Uᴘʟᴏᴀᴅɪɴɢ...</b>")

        _ul_start = time.time()
        _ul_msg = msg   # FIX C1+C4: snapshot msg so closure is not affected by later reassignments
        async def _ul_progress(current: int, total: int) -> None:
            await progress_for_pyrogram(
                current, total,
                f"<blockquote expandable>❐ <b>{final_filename}</b></blockquote>\n"
                f"<blockquote><b>➥ Uᴘʟᴏᴀᴅɪɴɢ</b>...</blockquote>",
                _ul_msg, _ul_start, client, cancel_button,
            )

        ul_progress = make_cancel_aware_progress(task_id, task_queue, _ul_progress)
        try:
            upload_size = max(int((await aiofiles.os.stat(file_path)).st_size), 1)
        except Exception:
            upload_size = max(int(file_size or 0), 1)
        await _ul_progress(0, upload_size)

        _doc_exts = {
            ".pdf", ".txt", ".doc", ".docx", ".xls", ".xlsx",
            ".ppt", ".pptx", ".odt", ".rtf", ".csv", ".epub",
            ".mobi", ".zip", ".rar", ".7z", ".xml", ".html",
            ".json", ".md", ".log", ".ini", ".bat", ".sh",
        }
        upload_timeout = _ul_timeout(file_size)
        upload_duration = int(media_info.get("duration_seconds", 0) or 0) or None
        upload_width = int(media_info.get("video_width", 0) or 0) or None
        upload_height = int(media_info.get("video_height", 0) or 0) or None

        try:
            streamable_exts = {".mp4", ".m4v", ".mov"}
            upload_ext = os.path.splitext(file_path)[1].lower()
            supports_streaming = upload_ext in streamable_exts
            prefer_video_upload = media_preference == "video" and supports_streaming
            if media_type == "video" and prefer_video_upload:
                sent_msg = await asyncio.wait_for(
                    client.send_video(
                        chat_id=message.chat.id,
                        video=file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=ul_progress,
                        duration=upload_duration,
                        width=upload_width,
                        height=upload_height,
                        supports_streaming=supports_streaming,
                    ),
                    timeout=upload_timeout,
                )
            elif media_type == "document" and prefer_video_upload:
                sent_msg = await asyncio.wait_for(
                    client.send_video(
                        chat_id=message.chat.id,
                        video=file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=ul_progress,
                        duration=upload_duration,
                        width=upload_width,
                        height=upload_height,
                        supports_streaming=supports_streaming,
                    ),
                    timeout=upload_timeout,
                )
            elif file_ext in _doc_exts:
                sent_msg = await asyncio.wait_for(
                    client.send_document(
                        chat_id=message.chat.id,
                        document=file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=ul_progress,
                    ),
                    timeout=upload_timeout,
                )
            elif media_type == "audio":
                sent_msg = await asyncio.wait_for(
                    client.send_audio(
                        chat_id=message.chat.id,
                        audio=file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=ul_progress,
                    ),
                    timeout=upload_timeout,
                )
            else:
                sent_msg = await asyncio.wait_for(
                    client.send_document(
                        chat_id=message.chat.id,
                        document=file_path,
                        caption=caption,
                        thumb=thumb_path,
                        progress=ul_progress,
                    ),
                    timeout=upload_timeout,
                )
            thumb_path = None
        except asyncio.TimeoutError:
            await _safe_edit(msg, f"<blockquote expandable>❐ {final_filename}</blockquote>\n<blockquote>✗ <b>Uᴘʟᴏᴀᴅ ᴛɪᴍᴇᴅ ᴏᴜᴛ.</blockquote></b>")
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            err_text = _format_exc_message(exc)
            await _safe_edit(msg, f"<blockquote expandable>❐ {final_filename}</blockquote>\n<blockquote>Uᴘʟᴏᴀᴅ ғᴀɪʟᴇᴅ: {err_text}</blockquote>")
            raise

        # -- Post-upload ---------------------------------------------------
        # MEDIUM FIX: auto_del was fetched again here despite already being
        # available in user_data (from the pre-enqueue batch gather).  The
        # extra DB call per file multiplies by concurrent task count.
        try:
            auto_del = bool(user_data.get("auto_delete", False))
            if auto_del:
                ids_to_del = [message.id]
                if sent_msg:
                    ids_to_del.append(sent_msg.id)
                await task_queue.add_to_delete(user_id, ids_to_del)
        except Exception as exc:
            _log("warning", "auto_delete batch collect failed", user_id=user_id, exc=str(exc))
            
        try:
            await DARKXSIDE78.add_rename_history(
                user_id=user_id,
                original_name=file_name,
                renamed_name=final_filename,
                rename_timestamp=datetime.now(),
            )
        except Exception as exc:
            _log("warning", "add_rename_history failed", user_id=user_id, exc=str(exc))

        if Config.DUMP:
            try:
                ist = pytz.timezone("Asia/Kolkata")
                ts = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S IST")
                full_name = user.first_name + (
                    f" {user.last_name}" if user.last_name else ""
                )
                uname = f"@{user.username}" if user.username else "N/A"
                dump_caption = (
                    f"<blockquote>❐ 𝗨𝗦𝗘𝗥 𝗗𝗘𝗧𝗔𝗜𝗟𝗦</blockquote>\n"
                    f"<blockquote expandable>◍ ID : {user_id}\n◍ Nᴀᴍᴇ : {full_name}\n"
                    f"◍ Usᴇʀɴᴀᴍᴇ : {uname}\n"
                    f"◍ Pʀᴇᴍɪᴜᴍ : {'🗸' if is_premium else '✘'}\n"
                    f"◍ Tɪᴍᴇ : {ts}</blockquote>\n"
                    f"<blockquote expandable>➥ Oʀɪɢɪɴᴀʟ Fɪʟᴇɴᴀᴍᴇ : {file_name}</blockquote>\n"
                    f"<blockquote expandable>➥ Rᴇɴᴀᴍᴇᴅ Fɪʟᴇɴᴀᴍᴇ : {final_filename}</blockquote>"
                )
                await asyncio.sleep(0.2)
                # Copy the RENAMED file (sent_msg) — not the raw original
                try:
                    await client.copy_message(
                        chat_id=Config.DUMP_CHANNEL,
                        from_chat_id=message.chat.id,
                        message_id=sent_msg.id,
                        caption=dump_caption,
                        parse_mode=ParseMode.HTML,
                    )
                except PeerIdInvalid:
                    try:
                        await client.get_chat(Config.DUMP_CHANNEL)
                        await client.copy_message(
                            chat_id=Config.DUMP_CHANNEL,
                            from_chat_id=message.chat.id,
                            message_id=sent_msg.id,
                            caption=dump_caption,
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as copy_err:
                        _log("warning", "copy_message to dump failed after resolution", user_id=user_id, exc=str(copy_err))
                except Exception as copy_exc:
                    _log("warning", "copy_message to dump failed, trying re-upload",
                         user_id=user_id, exc=str(copy_exc))
                    # MEDIUM FIX: Always use file_path directly and guard with os.path.exists.
                    # Previously `file_path or download_path` could resolve to a path that
                    # add_metadata already renamed/moved, producing a stale reference.
                    _dump_file = file_path
                    if _dump_file and os.path.exists(_dump_file):
                        await client.send_document(
                            chat_id=Config.DUMP_CHANNEL,
                            document=_dump_file,
                            caption=dump_caption,
                            parse_mode=ParseMode.HTML,
                            thumb=thumb_path if thumb_path and os.path.exists(str(thumb_path)) else None,
                        )  # FIX H6: removed undefined doc_card_size_kwargs (NameError crash)
                    else:
                        _log("error", "Dump fallback: file already gone — both copy and re-upload failed",
                             user_id=user_id, path=_dump_file)
            except Exception as exc:
                _log("error", "Dump channel failed", user_id=user_id, exc=str(exc))

        # Post-upload I/O should never be able to delete the delivered file.
        output_path = None
        await job_ledger.mark_done(task_id)
        await _safe_delete(msg)

    except asyncio.CancelledError:
        _log("info", "process_file cancelled", task_id=task_id, user_id=user_id)
        await job_ledger.mark_cancelled(task_id)
        await _maybe_refund()   # FIX 1: idempotent
        if task_queue.is_cancelled(task_id):
            try:
                await _safe_edit(msg, f"<blockquote expandable>❐ <b>{final_filename}</blockquote></b>\n<blockquote>ⓘ Cᴀɴᴄᴇʟʟᴇᴅ</blockquote>")
                await asyncio.sleep(1)
                await _safe_delete(msg)
            except Exception:
                pass
        raise  # Propagate — processor must NOT credit success

    except Exception as exc:
        if upload_stage_reached and _is_retryable(exc):
            attempt = attempt_context.get("attempt", 0) if attempt_context else 0
            max_retries = attempt_context.get("max_retries", 3) if attempt_context else 3
            if attempt < max_retries - 1:
                should_cleanup = False
                raise exc

        err_text = _format_exc_message(exc)
        _log("error", "process_file error", task_id=task_id, user_id=user_id, exc=err_text)
        await job_ledger.mark_failed(task_id, err_text[:500])
        await _maybe_refund()
        try:
            await _safe_edit(msg, f"<blockquote expandable>❐ {final_filename}</blockquote>\n<blockquote>Eʀʀᴏʀ: {err_text[:200]}</blockquote>")
        except Exception:
            try:
                await message.reply_text(f"Eʀʀᴏʀ: {err_text[:200]}")
            except Exception:
                pass

    finally:
        _progress_last_sample.pop(task_id, None)
        if should_cleanup:
            await cleanup_files(
                download_path, metadata_path, thumb_path,
                output_path, current_pdf,
            )
            attempt_context.clear()
        # FIX L3: shutil already imported at module level — no need for redundant import in finally
        shutil.rmtree(os.path.abspath(os.path.join("thumbs", str(task_id))), ignore_errors=True)
        await task_queue.purge_cancel_ids_for_task(task_id)



# =============================================================================
# Cancel callback handler
# =============================================================================

@Client.on_callback_query(filters.regex(r"^c\d+$"))
async def handle_file_cancel(client: Client, callback_query: CallbackQuery) -> None:
    try:
        short_cancel_id = callback_query.data
        task_id = await task_queue.resolve_cancel_id(short_cancel_id)
        if not task_id:
            await callback_query.answer(
                "ⓘ ᴛᴀsᴋ ɴᴏᴛ ғᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴄᴏᴍᴘʟᴇᴛᴇᴅ", show_alert=True
            )
            return

        try:
            expected_uid = int(task_id.split(":")[0])
        except (ValueError, IndexError):
            await callback_query.answer("✗ ɪɴᴠᴀʟɪᴅ ᴛᴀsᴋ", show_alert=True)
            return

        if callback_query.from_user.id != expected_uid:
            await callback_query.answer("✗ ᴛʜɪs ɪs ɴᴏᴛ ʏᴏᴜʀ ғɪʟᴇ !", show_alert=True)
            return

        filename = task_queue.get_filename(task_id)
        success = await task_queue.cancel_file(task_id, client)
        if success:
            await task_queue.remove_cancel_id(short_cancel_id)
            await callback_query.answer(f"✓ ᴄᴀɴᴄᴇʟʟᴇᴅ : {filename}", show_alert=True)
        else:
            await callback_query.answer(
                "ⓘ ғɪʟᴇs ᴀʟʀᴇᴀᴅʏ ᴄᴏᴍᴘʟᴇᴛᴇᴅ ᴏʀ ᴄᴀɴᴄᴇʟʟᴇᴅ", show_alert=True
            )
    except Exception as exc:
        _log("error", "handle_file_cancel", exc=str(exc))
        await callback_query.answer("✗ ᴄᴀɴᴄᴇʟʟᴀᴛɪᴏɴ ғᴀɪʟᴇᴅ", show_alert=True)


async def _cancel_id_sweeper() -> None:
    while True:
        try:
            await asyncio.sleep(CANCEL_ID_SWEEP_INTERVAL)
            instance = getattr(task_queue, "_instance", None)
            if instance is not None:
                await instance._sweep_cancel_id_map()
        except asyncio.CancelledError: break
        except Exception as exc:
            _log("warning", "cancel_id sweeper failed", exc=str(exc))

async def _file_state_sweeper() -> None:
    while True:
        try:
            await asyncio.sleep(600)
            instance = getattr(task_queue, "_instance", None)
            if instance is not None:
                await instance._sweep_file_states()

            # MEDIUM FIX: Sweep _progress_last_sample for task IDs that were
            # cancelled BEFORE process_file_core was entered (e.g. during global
            # semaphore wait). Those tasks never reach the finally block that
            # would normally call _progress_last_sample.pop(), so the dict grows
            # unboundedly on long-running bots with many cancelled tasks.
            live: set = set()
            if instance is not None:
                async with instance._fs_lock:
                    live = set(instance._file_states.keys())
            stale_progress = [k for k in list(_progress_last_sample) if k not in live]
            for k in stale_progress:
                _progress_last_sample.pop(k, None)
            if stale_progress:
                _log("debug", "Swept stale _progress_last_sample entries",
                     count=len(stale_progress))

        except asyncio.CancelledError: break
        except Exception as exc:
            _log("warning", "file_state sweeper failed", exc=str(exc))

async def _user_stats_sweeper() -> None:
    while True:
        try:
            await asyncio.sleep(900)
            instance = getattr(task_queue, "_instance", None)
            if instance is not None:
                await instance._sweep_user_stats()
        except asyncio.CancelledError: break
        except Exception as exc:
            _log("warning", "user_stats sweeper failed", exc=str(exc))

async def init(*args, **kwargs):
    """Starts all background maintenance tasks for the rename module."""
    try:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=64))
        # Initialize Proxy instance if needed
        _ = task_queue.get_accurate_position

        # ── START PROGRESS ENGINE WORKERS ──────────────────────────────────
        # Must be called before any file is processed. Without this the
        # render_dispatcher and edit_workers never run and no progress bar
        # ever appears in Telegram.
        from helper.utils import start_background_tasks
        start_background_tasks()
        # ───────────────────────────────────────────────────────────────────
        
        loop.create_task(_cancel_id_sweeper())
        loop.create_task(_media_group_cache_evictor())
        loop.create_task(_verify_cache_evictor())
        loop.create_task(_file_state_sweeper())
        loop.create_task(_user_stats_sweeper())
        loop.create_task(_working_storage_sweeper())
        _log("info", "Background maintenance tasks (file_rename) started.")
    except Exception as e:
        _log("error", f"Failed to start file_rename background tasks: {e}")

# =============================================================================
# Module load sentinel — if this line is reached, all imports and top-level
# code succeeded. If /queue or rename never work, check logs ABOVE this line.
# =============================================================================
_log("info", "auto_rename module loaded successfully",
     handlers=["auto_rename_files", "queue_status", "cancel_queue",
                "verify_cancel_code", "handle_file_cancel"])
