# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== BROADCAST ENGINE V8 — PRODUCTION GRADE ====================

from __future__ import annotations

import asyncio
import re as _re
import time
import random
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, CallbackQuery,
)
from pyrogram.errors import FloodWait, UserDeactivated, UserIsBlocked, RPCError

from config import Config
from helper.database import DARKXSIDE78

log = logging.getLogger("Broadcast")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

WORKER_COUNT_INIT = 3
WORKER_COUNT_MIN  = 1
WORKER_COUNT_MAX  = 6

SEND_DELAY        = 0.12         
JITTER_RANGE      = (0.03, 0.10) 
SEND_DELAY_MAX    = SEND_DELAY * 6

GLOBAL_RPS_LIMIT  = 20.0

QUEUE_MAXSIZE     = WORKER_COUNT_MAX * 150
PRODUCER_BATCH    = 500
AUTODEL_SEM_LIMIT = 30
BATCH_SIZE        = 5_000
REST_SECONDS      = 300
KILL_THRESHOLD    = 600         

ROLLING_WINDOW_SECS = 30.0
SPARKLINE_SAMPLES   = 5

CHECKPOINT_EVERY    = 200        
CHECKPOINT_MAX_AGE  = 86400     

# Scaling config
SCALE_CHECK_SECS   = 15.0        
SCALE_COOLDOWN_SECS = 30.0      
SCALE_UP_ERR_THRESH = 0.04     
SCALE_DN_ERR_THRESH = 0.20       

# FloodWait coordination
FW_PRESSURE_WINDOW = 120       
FW_PRESSURE_COUNT  = 3         
FW_BASE_BACKOFF    = 1.0      
FW_MAX_BACKOFF     = 60.0       

# Circuit breaker
CB_SAMPLE_MIN      = 100        
CB_ERR_THRESHOLD   = 0.60       
CB_HOLD_SECS       = 60.0       

UPDATE_INTERVAL_ULTRA = 1.0
UPDATE_INTERVAL_DASH  = 4.0

IMG_HEADER    = "https://i.ibb.co/9991gsH2/photo-2026-05-07-16-45-15-7637192085606498324.jpg"
IMG_COMPLETED = "https://i.ibb.co/pvr2R1P6/photo-2026-05-07-16-45-15-7637192205865582616.jpg"
IMG_CANCELLED = "https://i.ibb.co/Dgv0YZ9r/photo-2026-05-07-16-45-16-7637192270290092040.jpg"
IMG_DRYRUN    = "https://i.ibb.co/9991gsH2/photo-2026-05-07-16-45-15-7637192085606498324.jpg"

_BASE_FILTER      = {"ban_status.is_banned": {"$ne": True}}
_SPARK_CHARS      = ["▁", "▃", "▅", "▇", "█"]

# Module-level state
_global_kill_until: float                 = 0.0
autodel_sem         = None  

# Mappings to DB
users = DARKXSIDE78.col
_checkpoints = DARKXSIDE78.broadcast_checkpoints

def _sudo_filter():
    async def filter_func(_, __, m: Message):
        if not m.from_user: return False
        if m.from_user.id == Config.OWNER_ID: return True
        return await DARKXSIDE78.is_admin(m.from_user.id)
    return filters.create(filter_func)


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE ENGINE COMPONENTS
# ═══════════════════════════════════════════════════════════════════════════════

class GlobalRateLimiter:
    """Refined Token Bucket for global throughput control."""
    def __init__(self, rps: float) -> None:
        self._rps      = rps
        self._tokens   = rps          
        self._last_ref = time.monotonic()
        self._lock     = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_ref
            self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
            self._last_ref = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            sleep_for = (1.0 - self._tokens) / self._rps

        await asyncio.sleep(sleep_for)
        async with self._lock:
            self._tokens = max(0.0, self._tokens - 1.0)

    def set_rps(self, rps: float) -> None:
        self._rps    = max(0.5, rps)
        self._tokens = min(self._tokens, self._rps)


class FloodWaitCoordinator:

    def __init__(self) -> None:
        self._pause_event  = asyncio.Event()   
        self._resume_until = 0.0               
        self._lock         = asyncio.Lock()
        self._consecutive  = 0                
        self._last_fw_ts   = 0.0

        # Telemetry
        self._last_value:  int   = 0
        self._last_ts:     float = 0.0
        self._timestamps:  deque[float] = deque()
        self.total_hits:   int   = 0

    async def handle(self, fw_secs: int) -> None:
        now = time.monotonic()

        async with self._lock:
            self.total_hits += 1
            self._last_value = fw_secs
            self._last_ts    = now
            self._timestamps.append(now)

            if now - self._last_fw_ts < 60.0:
                self._consecutive += 1
            else:
                self._consecutive = 0
            self._last_fw_ts = now

            backoff_jitter = min(
                FW_BASE_BACKOFF * (2 ** self._consecutive),
                FW_MAX_BACKOFF,
            )
            required_until = now + fw_secs + backoff_jitter

            if required_until <= self._resume_until:
                return   

            self._resume_until = required_until
            self._pause_event.set()  
            
            # Fix: Spawn a fire‑and‑forget clearing task to avoid hangs on worker cancellation
            asyncio.create_task(self._clear_after(required_until))

    async def _clear_after(self, until: float) -> None:
        """Background task that waits until the resume time and clears the event."""
        try:
            delay = until - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                # Double check that no other worker has extended the wait
                if time.monotonic() >= self._resume_until:
                    self._pause_event.clear()
                    log.info("[FW] Pause cleared — workers resuming")
        except asyncio.CancelledError:
            pass  # Task cancelled on shutdown — fine

    async def wait_if_paused(self) -> None:
        """Workers call this before every send. Returns immediately if not paused."""
        while self._pause_event.is_set():
            await asyncio.sleep(0.1)

    def pressure(self) -> int:
        now    = time.monotonic()
        cutoff = now - FW_PRESSURE_WINDOW
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        return len(self._timestamps)

    def under_pressure(self) -> bool:
        return self.pressure() >= FW_PRESSURE_COUNT

    def last_str(self) -> str:
        if self._last_value == 0:
            return ""
        age = int(time.monotonic() - self._last_ts)
        age_str = f"{age}s ᴀɢᴏ" if age < 60 else f"{age // 60}ᴍ ᴀɢᴏ"
        return f"Lᴀsᴛ Fᴡ: {self._last_value}s ({age_str})"


class CircuitBreaker:
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"

    def __init__(self) -> None:
        self._state      = self.CLOSED
        self._tripped_at = 0.0
        self._testing    = False  
        self._lock       = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._state == self.OPEN

    @property
    def is_testing(self) -> bool:
        return self._state == self.HALF_OPEN and self._testing

    async def check(self, error_rate: float, sample_count: int) -> None:
        async with self._lock:
            now = time.monotonic()

            if self._state == self.CLOSED:
                if sample_count >= CB_SAMPLE_MIN and error_rate > CB_ERR_THRESHOLD:
                    self._state      = self.OPEN
                    self._tripped_at = now
                    log.error(
                        f"[CB] OPEN — error_rate={error_rate:.2%} "
                        f"samples={sample_count} threshold={CB_ERR_THRESHOLD:.0%}"
                    )

            elif self._state == self.OPEN:
                if now - self._tripped_at >= CB_HOLD_SECS:
                    self._state   = self.HALF_OPEN
                    self._testing = False   
                    log.info("[CB] HALF_OPEN — ready to probe")

    async def try_probe(self) -> bool:
        if self._state != self.HALF_OPEN:
            return False
        async with self._lock:
            if self._state != self.HALF_OPEN or self._testing:
                return False
            self._testing = True
            log.info("[CB] HALF_OPEN — probe send claimed")
            return True

    async def on_result(self, success: bool) -> None:
        async with self._lock:
            if self._state != self.HALF_OPEN:
                return
            if success:
                self._state   = self.CLOSED
                self._testing = False
                log.info("[CB] CLOSED — recovery confirmed")
            else:
                self._state      = self.OPEN
                self._testing    = False
                self._tripped_at = time.monotonic()
                log.warning("[CB] OPEN — probe failed, resetting hold timer")

    def state_str(self) -> str:
        return {"closed": "✅", "open": "🔴", "half_open": "🟡"}[self._state]


class ScalerGuard:
    def __init__(self, cooldown: float = SCALE_COOLDOWN_SECS) -> None:
        self._cooldown  = cooldown
        self._last_time = 0.0

    def can_scale(self) -> bool:
        return time.monotonic() - self._last_time >= self._cooldown

    def record(self) -> None:
        self._last_time = time.monotonic()

    def remaining(self) -> float:
        return max(0.0, self._cooldown - (time.monotonic() - self._last_time))


class WorkerCursorRegistry:
    def __init__(self) -> None:
        self._cursors: dict[int, int] = {}   
        self._lock = asyncio.Lock()

    async def register(self, worker_id: int, initial_cursor: int = 0) -> None:
        async with self._lock:
            self._cursors[worker_id] = initial_cursor

    async def update(self, worker_id: int, user_id: int) -> None:
        async with self._lock:
            self._cursors[worker_id] = user_id

    async def unregister(self, worker_id: int) -> None:
        async with self._lock:
            self._cursors.pop(worker_id, None)

    async def low_watermark(self) -> int:
        async with self._lock:
            return min(self._cursors.values()) if self._cursors else 0


class RollingRate:

    def __init__(self, window: float = ROLLING_WINDOW_SECS) -> None:
        self._window = window
        self._events: deque[float] = deque()

    def record(self) -> None:
        self._events.append(time.monotonic())

    def rate(self) -> float:
        now    = time.monotonic()
        cutoff = now - self._window
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        n = len(self._events)
        if n < 2:
            return 0.0
        span = now - self._events[0]
        return n / span if span > 0 else 0.0

    def rate_per_min(self) -> int:
        return int(self.rate() * 60)


class Sparkline:
    def __init__(self, samples: int = SPARKLINE_SAMPLES) -> None:
        self._samples = samples
        self._history: deque[int] = deque(maxlen=samples)

    def push(self, rpm: int) -> None:
        self._history.append(rpm)

    def spark(self) -> str:
        if not self._history:
            return "▁" * self._samples
        lo, hi = min(self._history), max(self._history)
        rng    = hi - lo or 1
        chars  = [_SPARK_CHARS[int((v - lo) / rng * 4)] for v in self._history]
        while len(chars) < self._samples:
            chars.insert(0, _SPARK_CHARS[0])
        return "".join(chars)


# ═══════════════════════════════════════════════════════════════════════════════
#  CHECKPOINT
# ═══════════════════════════════════════════════════════════════════════════════

async def _checkpoint_save(
    b_id:          int,
    watermark:     int,
    success:       int,
    total:         int,
    src_chat_id:   int,
    src_msg_id:    int,
    args_raw:      str,
) -> None:
    doc = {
        "watermark":   watermark,
        "success":     success,
        "total":       total,
        "src_chat_id": src_chat_id,
        "src_msg_id":  src_msg_id,
        "args_raw":    args_raw,
        "saved_at":    time.time(),
        "version":     8,         
    }
    try:
        await _checkpoints.replace_one(
            {"_id": "active"},
            {"_id": "active", **doc},
            upsert=True,
        )
        log.debug(f"[CKPT] Saved: watermark={watermark} success={success}/{total}")
    except Exception as e:
        log.warning(f"[CKPT] Save failed: {e}")


async def _checkpoint_load() -> Optional[dict]:
    try:
        return await _checkpoints.find_one({"_id": "active"})
    except Exception as e:
        log.warning(f"[CKPT] Load failed: {e}")
        return None


async def _checkpoint_clear() -> None:
    try:
        await _checkpoints.delete_one({"_id": "active"})
        log.info("[CKPT] Cleared")
    except Exception as e:
        log.warning(f"[CKPT] Clear failed: {e}")


def _checkpoint_validate(doc: dict) -> tuple[bool, str]:
    required = {"watermark", "success", "total", "src_chat_id", "src_msg_id", "saved_at"}
    missing  = required - set(doc.keys())
    if missing:
        return False, f"Missing fields: {missing}"

    age = time.time() - float(doc.get("saved_at", 0))
    if age > CHECKPOINT_MAX_AGE:
        return False, f"Checkpoint is {age / 3600:.1f}h old (max {CHECKPOINT_MAX_AGE / 3600:.0f}h)"

    if int(doc.get("success", 0)) > int(doc.get("total", 1)):
        return False, "success > total (corrupt checkpoint)"

    if int(doc.get("watermark", -1)) < 0:
        return False, "negative watermark"

    return True, "ok"


# ═══════════════════════════════════════════════════════════════════════════════
#  ARG PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_autodel(value: str) -> Optional[int]:
    value = value.lower().strip()
    if value.isdigit():
        return int(value)
    total, matched = 0, False
    for amount, unit in _re.findall(r"(\d+)([hms])", value):
        matched = True
        n = int(amount)
        total += n * {"h": 3600, "m": 60, "s": 1}[unit]
    return total if matched else None


def _parse_limit(value: str) -> Optional[float]:
    value = value.strip()
    if value.endswith("%"):
        try:
            pct = float(value[:-1])
            return -pct if 0 < pct <= 100 else None
        except ValueError:
            return None
    try:
        v = int(value)
        return float(v) if v > 0 else None
    except ValueError:
        return None


def parse_broadcast_args(raw_parts: list[str]) -> dict:
    result: dict = {
        "style": "ultra", "interval": None, "dry": False,
        "silent": False,  "pin": False,     "pin_loud": False,
        "autodel": None,  "limit": None,
    }
    i, parts = 0, list(raw_parts)
    while i < len(parts):
        tok = parts[i].lower()
        if tok in ("ultra", "dash"):
            result["style"] = tok
        elif tok == "interval" and i + 1 < len(parts):
            try:
                v = float(parts[i + 1])
                if v >= 0.5:
                    result["interval"] = v
                i += 1
            except ValueError:
                pass
        elif tok == "dry":     result["dry"] = True
        elif tok == "silent":  result["silent"] = True
        elif tok == "pin_loud": result["pin"] = result["pin_loud"] = True
        elif tok == "pin":     result["pin"] = True
        elif tok == "autodel" and i + 1 < len(parts):
            parsed = _parse_autodel(parts[i + 1])
            if parsed and parsed > 0:
                result["autodel"] = parsed
            i += 1
        else:
            parsed = _parse_limit(tok)
            if parsed is not None:
                result["limit"] = parsed
        i += 1
    return result


def resolve_limit(args: dict, db_total: int) -> Optional[int]:
    lim = args["limit"]
    if lim is None:
        return None
    if isinstance(lim, float) and lim < 0:
        return max(1, int(db_total * abs(lim) / 100))
    return int(lim)


def _mode_str(args: dict) -> str:
    parts = []
    if args["silent"]:   parts.append("Sɪʟᴇɴᴛ")
    if args["pin_loud"]: parts.append("Pɪɴ(ʟᴏᴜᴅ)")
    elif args["pin"]:    parts.append("Pɪɴ")
    if args["autodel"]:
        s = args["autodel"]
        if s >= 3600 and s % 3600 == 0: parts.append(f"Aᴜᴛᴏᴅᴇʟ({s // 3600}ʜ)")
        elif s >= 60 and s % 60 == 0:   parts.append(f"Aᴜᴛᴏᴅᴇʟ({s // 60}ᴍ)")
        else:                            parts.append(f"Aᴜᴛᴏᴅᴇʟ({s}s)")
    if args["limit"] is not None:
        lim = args["limit"]
        parts.append(f"Lɪᴍɪᴛ({abs(lim):.0f}%)" if (isinstance(lim, float) and lim < 0) else f"Lɪᴍɪᴛ({int(lim):,})")
    if args["interval"] is not None:
        parts.append(f"Uɪ({args['interval']}s)")
    return " | ".join(parts) if parts else "Nᴏʀᴍᴀʟ"


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _current_error_rate(stats: dict) -> float:
    done = sum(stats[k] for k in ("success", "failed", "deactivated", "blocked"))
    return 0.0 if done == 0 else (stats["failed"] + stats["blocked"] + stats["deactivated"]) / done


def _adaptive_delay(stats: dict) -> float:
    err = _current_error_rate(stats)
    if err < 0.05:
        return 0.0
    return min(SEND_DELAY * (1 + err * 2), SEND_DELAY_MAX)


def _wall_clock_eta(remaining: int, rps: float) -> str:
    if rps <= 0:
        return "Cᴀʟᴄᴜʟᴀᴛɪɴɢ..."
    return f"~{datetime.fromtimestamp(time.time() + remaining / rps).strftime('%H:%M')}"


async def _mark_user(uid: int, *, blocked: bool = False, deactivated: bool = False) -> None:
    # We will use DARKXSIDE78 standard functions for blocking/deactivating
    if deactivated or blocked:
        try:
            await DARKXSIDE78.delete_user(uid)
        except Exception as e:
            log.warning(f"[DB] _mark_user({uid}): {e}")

async def _limited_autodel(app: Client, chat_id: int, msg_id: int, delay: int) -> None:
    global autodel_sem
    if not autodel_sem:
        autodel_sem  = asyncio.Semaphore(AUTODEL_SEM_LIMIT)
    async with autodel_sem:
        await asyncio.sleep(delay)
        try:
            await app.delete_messages(chat_id, msg_id)
            log.debug(f"[AUTODEL] Deleted msg={msg_id} chat={chat_id}")
        except Exception as e:
            log.warning(f"[AUTODEL] Failed msg={msg_id} chat={chat_id}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  SAFE COPY (rate-limited + FW-coordinated)
# ═══════════════════════════════════════════════════════════════════════════════

async def safe_copy(
    app: Client,
    chat_id:  int,
    from_chat: int,
    msg_id:   int,
    rate_lim: GlobalRateLimiter,
    fw_coord: FloodWaitCoordinator,
    disable_notification: bool = False,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> object:
    global _global_kill_until

    await fw_coord.wait_if_paused()

    await rate_lim.acquire()

    try:
        return await app.copy_message(
            chat_id=chat_id,
            from_chat_id=from_chat,
            message_id=msg_id,
            disable_notification=disable_notification,
            reply_markup=reply_markup,
        )
    except FloodWait as fw:
        if fw.value > KILL_THRESHOLD:
            _global_kill_until = time.time() + 21600
            log.critical(f"[FW] {fw.value}s exceeds kill threshold → 6h global kill")
        raise   # always re-raise; worker decides retry vs fail
    except RPCError:
        raise


# ═══════════════════════════════════════════════════════════════════════════════
#  PRODUCER
# ═══════════════════════════════════════════════════════════════════════════════

async def producer(
    queue:        asyncio.Queue,
    is_cancelled: Callable[[], bool],
    limit:        Optional[int],
    done_event:   asyncio.Event,
    resume_from:  int = 0,
) -> None:
    PROJECTION = {"_id": 1}
    queued, last_id = 0, resume_from

    async def _fetch(after_id: int, cap: int) -> list[dict]:
        filt = {**_BASE_FILTER, "_id": {"$gt": after_id}}
        return await users.find(filt, PROJECTION).sort("_id", 1).limit(cap).to_list(length=cap)

    try:
        while True:
            if is_cancelled():
                break
            remaining = (limit - queued) if limit is not None else PRODUCER_BATCH
            if remaining <= 0:
                break
            cap   = min(PRODUCER_BATCH, remaining)
            batch = await _fetch(last_id, cap)
            if not batch:
                break
            for doc in batch:
                if is_cancelled() or (limit is not None and queued >= limit):
                    break
                await queue.put(int(doc["_id"]))
                last_id = int(doc["_id"])
                queued += 1
            if len(batch) < cap:
                break   # last page
    except asyncio.CancelledError:
        log.debug(f"[PRODUCER] Cancelled after {queued} users")
    except Exception as e:
        log.error(f"[PRODUCER] Error after {queued} users: {e}", exc_info=True)
    finally:
        done_event.set()
        log.info(f"[PRODUCER] Done: queued={queued} last_id={last_id}")


# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER
# ═══════════════════════════════════════════════════════════════════════════════

async def worker(
    worker_id:    int,
    app:          Client,
    queue:        asyncio.Queue,
    stats:        dict,
    rolling:      RollingRate,
    fw_coord:     FloodWaitCoordinator,
    rate_lim:     GlobalRateLimiter,
    circuit_bkr:  CircuitBreaker,
    cursor_reg:   WorkerCursorRegistry,
    is_cancelled: Callable[[], bool],
    producer_done: asyncio.Event,
    src_chat_id:  int,
    src_msg_id:   int,
    silent:       bool,
    pin:          bool,
    pin_loud:     bool,
    autodel:      Optional[int],
    autodel_tasks: set,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    MAX_FW_RETRIES = 2

    await cursor_reg.register(worker_id)
    log.debug(f"[W{worker_id}] Started")

    try:
        while True:
            # Scale-down check: worker retires gracefully if above target
            if worker_id >= stats["target_workers"] and not is_cancelled():
                log.debug(f"[W{worker_id}] Scaling down (target={stats['target_workers']})")
                break

            # Non-blocking get with timeout for periodic checks
            try:
                user_id = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if producer_done.is_set() and queue.empty():
                    break
                if worker_id >= stats["target_workers"]:
                    break
                continue
            except asyncio.CancelledError:
                break

            if is_cancelled():
                queue.task_done()
                continue

            # Circuit breaker: fail fast if breaker is open or testing (HALF_OPEN, not our probe)
            if circuit_bkr.is_open or circuit_bkr.is_testing:
                stats["failed"] += 1
                queue.task_done()
                log.debug(f"[W{worker_id}] CB {circuit_bkr.state_str()} — skipping uid={user_id}")
                continue

            # Try to claim the HALF_OPEN probe slot (only one worker gets it)
            is_probe = await circuit_bkr.try_probe()

            sent       = None
            fw_retries = 0
            success    = False
            probe_done = False # Fix: track if probe outcome was reported

            try:
                while True:
                    try:
                        sent = await safe_copy(
                            app, user_id, src_chat_id, src_msg_id,
                            rate_lim, fw_coord,
                            disable_notification=silent,
                            reply_markup=reply_markup,
                        )
                        stats["success"] += 1
                        rolling.record()
                        await cursor_reg.update(worker_id, user_id)
                        success = True
                        
                        # Fix: Mark probe success immediately if successful
                        if is_probe:
                            await circuit_bkr.on_result(True)
                            probe_done = True
                        break

                    except FloodWait as fw:
                        fw_retries += 1
                        stats["fw_total"] += 1

                        if fw.value > KILL_THRESHOLD:
                            stats["failed"] += 1
                            break

                        if fw_retries > MAX_FW_RETRIES:
                            stats["failed"] += 1
                            log.warning(f"[W{worker_id}] Max FW retries for uid={user_id}")
                            break

                        # Coordinate pause across all workers
                        await fw_coord.handle(fw.value)
                        
                        # Fix: Worker MUST honor the FloodWait delay itself to prevent reflooding
                        await asyncio.sleep(fw.value)

                    except UserDeactivated:
                        stats["deactivated"] += 1
                        asyncio.create_task(_mark_user(user_id, deactivated=True))
                        break
                    except UserIsBlocked:
                        stats["blocked"] += 1
                        asyncio.create_task(_mark_user(user_id, blocked=True))
                        break
                    except RPCError as e:
                        stats["failed"] += 1
                        log.debug(f"[W{worker_id}] RPCError uid={user_id}: {type(e).__name__}")
                        break
                    except asyncio.CancelledError:
                        queue.task_done()
                        raise
                    except Exception as e:
                        stats["failed"] += 1
                        log.warning(f"[W{worker_id}] Unexpected error uid={user_id}: {e}")
                        break

            finally:
                # Fix: Always report probe failure if we exited without reporting success
                if is_probe and not probe_done:
                    await circuit_bkr.on_result(False)

            if sent is not None:
                if pin:
                    try:
                        await app.pin_chat_message(
                            user_id, sent.id,
                            disable_notification=not pin_loud,
                            both_sides=True,
                        )
                    except Exception:
                        pass

                if autodel and autodel > 0 and sent is not None:
                    t = asyncio.create_task(
                        _limited_autodel(app, user_id, sent.id, autodel)
                    )
                    autodel_tasks.add(t)
                    t.add_done_callback(autodel_tasks.discard)

                delay = _adaptive_delay(stats)
                await asyncio.sleep(delay + random.uniform(*JITTER_RANGE))

            queue.task_done()

    except asyncio.CancelledError:
        log.debug(f"[W{worker_id}] Cancelled")
    except Exception as e:
        log.error(f"[W{worker_id}] Fatal error: {e}", exc_info=True)
    finally:
        await cursor_reg.unregister(worker_id)
        log.debug(f"[W{worker_id}] Exited")


# ═══════════════════════════════════════════════════════════════════════════════
#  SCALING LOOP
# ═══════════════════════════════════════════════════════════════════════════════

async def scaling_loop(
    worker_tasks:  list[asyncio.Task],
    stats:         dict,
    fw_coord:      FloodWaitCoordinator,
    rate_lim:      GlobalRateLimiter,
    circuit_bkr:   CircuitBreaker,
    is_cancelled:  Callable[[], bool],
    producer_done: asyncio.Event,
    spawn_worker:  Callable[[int], asyncio.Task],
) -> None:
    guard = ScalerGuard(SCALE_COOLDOWN_SECS)

    while not (producer_done.is_set() and all(t.done() for t in worker_tasks)):
        await asyncio.sleep(SCALE_CHECK_SECS)
        if is_cancelled():
            break

        err_rate    = _current_error_rate(stats)
        fw_pressure = fw_coord.under_pressure()
        current     = stats["target_workers"]
        total_done  = sum(stats[k] for k in ("success", "failed", "deactivated", "blocked"))

        # Circuit breaker check
        await circuit_bkr.check(err_rate, total_done)

        # Adapt global RPS — clamp err_rate input so math stays in [0,1]
        _e = min(err_rate, 1.0)
        _p = min(_e * (2.0 if fw_pressure else 1.0), 1.0)
        target_rps = max(GLOBAL_RPS_LIMIT * 0.2, GLOBAL_RPS_LIMIT * (1.0 - _p))
        rate_lim.set_rps(round(target_rps, 1))

        if not guard.can_scale():
            continue

        if (err_rate < SCALE_UP_ERR_THRESH
                and not fw_pressure
                and not circuit_bkr.is_open
                and current < WORKER_COUNT_MAX):
            stats["target_workers"] = current + 1
            worker_tasks.append(spawn_worker(len(worker_tasks)))
            guard.record()
            log.info(f"[SCALER] UP → {stats['target_workers']} (err={err_rate:.2%} fw={fw_pressure})")

        elif ((err_rate > SCALE_DN_ERR_THRESH or fw_pressure or circuit_bkr.is_open)
              and current > WORKER_COUNT_MIN):
            stats["target_workers"] = current - 1
            guard.record()
            log.info(
                f"[SCALER] DOWN → {stats['target_workers']} "
                f"(err={err_rate:.2%} fw={fw_pressure} cb={circuit_bkr.state_str()})"
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  CHECKPOINT LOOP
# ═══════════════════════════════════════════════════════════════════════════════

async def checkpoint_loop(
    stats:         dict,
    cursor_reg:    WorkerCursorRegistry,
    is_cancelled:  Callable[[], bool],
    producer_done: asyncio.Event,
    b_id:          int,
    src_chat_id:   int,
    src_msg_id:    int,
    args_raw:      str,
) -> None:
    last_saved = 0
    last_watermark = 0 # Fix: Maintain the last valid watermark independently
    try:
        while True:
            await asyncio.sleep(5.0)

            if is_cancelled():
                break

            if stats["success"] - last_saved >= CHECKPOINT_EVERY:
                watermark = await cursor_reg.low_watermark()
                if watermark > 0:
                    last_watermark = watermark
                
                await _checkpoint_save(
                    b_id, last_watermark, stats["success"],
                    stats["total"], src_chat_id, src_msg_id, args_raw,
                )
                last_saved = stats["success"]

            # Exit condition: producer done and no unsaved progress
            if producer_done.is_set() and stats["success"] == last_saved:
                break

    except asyncio.CancelledError:
        pass
    finally:
        # Final save on exit — catches any sends since last periodic save
        if stats["success"] > last_saved:
            watermark = await cursor_reg.low_watermark()
            # Fix: Ensure we use the last valid watermark if low_watermark returns 0 (workers gone)
            final_watermark = watermark if watermark > 0 else last_watermark
            
            await _checkpoint_save(
                b_id, final_watermark, stats["success"],
                stats["total"], src_chat_id, src_msg_id, args_raw,
            )
            log.debug(f"[CKPT] Final save: success={stats['success']} watermark={final_watermark}")


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE BROADCAST RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class BroadcastJob:
    """Encapsulates the full state and orchestration of a single broadcast."""
    id:           int
    start_ts:     float
    stats:        dict
    args:         dict
    args_raw:     str
    ui_msg:       Message
    
    # Engine objects
    queue:        asyncio.Queue
    fw_coord:     FloodWaitCoordinator
    rate_lim:     GlobalRateLimiter
    circuit_bkr:  CircuitBreaker
    cursor_reg:   WorkerCursorRegistry
    rolling:      RollingRate
    sparkline:    Sparkline
    
    # Cancellation control
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    
    # Task registry
    tasks:        list[asyncio.Task] = field(default_factory=list)
    autodel_tasks: set[asyncio.Task] = field(default_factory=set)

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def get_progress_pct(self) -> int:
        done = sum(self.stats[k] for k in ("success", "failed", "deactivated", "blocked"))
        return int((done / self.stats["total"]) * 100) if self.stats["total"] else 0

    def get_eta_str(self) -> str:
        rate = self.rolling.rate()
        if rate <= 0: return "Cᴀʟᴄᴜʟᴀᴛɪɴɢ..."
        done = sum(self.stats[k] for k in ("success", "failed", "deactivated", "blocked"))
        remaining = self.stats["total"] - done
        return _wall_clock_eta(remaining, rate)


class BroadcastService:
    """
    Orchestrator for the broadcast subsystem.
    Manages job lifecycle, monitoring, and cleanup.
    """
    def __init__(self) -> None:
        self._jobs: dict[int, BroadcastJob] = {}
        self._lock = asyncio.Lock()

    async def start(
        self,
        bot:            Client,
        trigger_msg:    Message,
        args:           dict,
        effective:      int,
        db_total:       int,
        prod_limit:     Optional[int],
        resume_from:    int,
        already_sent:   int,
        b_id:           int,
        clamp_note:     str,
    ) -> None:
        """Initializes and launches a new broadcast job."""
        
        # 1. UI Setup
        is_resume = resume_from > 0
        ui_msg = await trigger_msg.reply_photo(
            IMG_HEADER,
            caption=(
                f"<b><blockquote>{'Rᴇsᴜᴍɪɴɢ' if is_resume else 'Fᴏᴜɴᴅ'} {db_total:,} ᴜsᴇʀs</blockquote></b>\n"
                f"<blockquote>Sᴇɴᴅɪɴɢ ᴛᴏ: {effective:,}{clamp_note}"
                + (f"\nAʟʀᴇᴀᴅʏ sᴇɴᴛ: {already_sent:,} | Rᴇsᴜᴍɪɴɢ ᴀᴛ ᴜɪᴅ > {resume_from}" if is_resume else "")
                + f"\nBᴀᴛᴄʜᴇs ᴏғ {BATCH_SIZE:,}...</blockquote>"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"cancel_bcast:{b_id}"),
                InlineKeyboardButton("Cʟᴏsᴇ",  callback_data="close_bcast"),
            ]]),
        )

        # 2. Stats & Objects
        stats = {
            "success": already_sent, "failed": 0, "deactivated": 0, "blocked": 0,
            "total": effective + already_sent,
            "target_workers": WORKER_COUNT_INIT,
            "phase": "Sᴇɴᴅɪɴɢ",
            "fw_total": 0,
            "drain_ok": True,
        }
        
        args_raw = " ".join(filter(None, [
            args["style"] if args["style"] != "ultra" else "",
            "silent"   if args["silent"]   else "",
            "pin_loud" if args["pin_loud"] else ("pin" if args["pin"] else ""),
            f"autodel {args['autodel']}" if args["autodel"] else "",
            str(int(effective)) if prod_limit else "",
        ]))

        job = BroadcastJob(
            id=b_id,
            start_ts=time.perf_counter(),
            stats=stats,
            args=args,
            args_raw=args_raw,
            ui_msg=ui_msg,
            queue=asyncio.Queue(maxsize=QUEUE_MAXSIZE),
            fw_coord=FloodWaitCoordinator(),
            rate_lim=GlobalRateLimiter(GLOBAL_RPS_LIMIT),
            circuit_bkr=CircuitBreaker(),
            cursor_reg=WorkerCursorRegistry(),
            rolling=RollingRate(),
            sparkline=Sparkline()
        )

        async with self._lock:
            self._jobs[b_id] = job

        # 3. Task Launch
        def _is_job_cancelled(): return job.is_cancelled()

        job.tasks = [
            asyncio.create_task(producer(
                job.queue, _is_job_cancelled, prod_limit, 
                producer_done := asyncio.Event(), resume_from
            )),
            asyncio.create_task(checkpoint_loop(
                job.stats, job.cursor_reg, _is_job_cancelled, 
                producer_done, b_id, trigger_msg.chat.id, 
                trigger_msg.reply_to_message.id, args_raw
            ))
        ]

        def _spawn_w(wid: int):
            return asyncio.create_task(worker(
                wid, bot, job.queue, job.stats, job.rolling, job.fw_coord, 
                job.rate_lim, job.circuit_bkr, job.cursor_reg, _is_job_cancelled, 
                producer_done, trigger_msg.chat.id, trigger_msg.reply_to_message.id,
                args["silent"], args["pin"], args["pin_loud"], args["autodel"], job.autodel_tasks,
                trigger_msg.reply_to_message.reply_markup
            ))

        worker_tasks = [_spawn_w(i) for i in range(WORKER_COUNT_INIT)]
        job.tasks.append(asyncio.create_task(scaling_loop(
            worker_tasks, job.stats, job.fw_coord, job.rate_lim, 
            job.circuit_bkr, _is_job_cancelled, producer_done, _spawn_w
        )))
        
        # 4. Monitor Loop
        try:
            await self._monitor(job, worker_tasks, producer_done)
        except Exception as e:
            log.error(f"[SERVICE] Monitor crash on {b_id}: {e}", exc_info=True)
        finally:
            await self._cleanup(job, worker_tasks)

    async def cancel(self, b_id: int):
        async with self._lock:
            if job := self._jobs.get(b_id):
                job.cancel_event.set()
                return True
        return False

    def is_active(self, b_id: int) -> bool:
        return b_id in self._jobs

    async def _monitor(self, job: BroadcastJob, worker_tasks: list[asyncio.Task], producer_done: asyncio.Event):
        last_update = time.perf_counter()
        last_spark  = time.perf_counter()
        batch_baseline = job.stats["success"]
        batch_count = 0
        
        update_interval = (
            job.args["interval"] if job.args["interval"] is not None
            else (UPDATE_INTERVAL_ULTRA if job.args["style"] == "ultra" else UPDATE_INTERVAL_DASH)
        )

        while not all(t.done() for t in worker_tasks):
            if job.is_cancelled(): break
            
            now = time.perf_counter()
            done = sum(job.stats[k] for k in ("success", "failed", "deactivated", "blocked"))

            # Batch Rest Logic
            if done - batch_baseline >= BATCH_SIZE and not job.is_cancelled():
                batch_count += 1
                batch_baseline = done
                await self._handle_rest(job, batch_count)
                job.stats["phase"] = "Sᴇɴᴅɪɴɢ"

            # Telemetry Update
            if now - last_spark >= update_interval:
                job.sparkline.push(job.rolling.rate_per_min())
                last_spark = now
                self._update_phase(job)

            # UI Update
            if now - last_update >= update_interval:
                await self._update_ui(job, done, batch_count)
                last_update = now

            await asyncio.sleep(0.5)

    async def _handle_rest(self, job: BroadcastJob, batch_count: int):
        rest_start = time.perf_counter()
        while not job.is_cancelled():
            elapsed = time.perf_counter() - rest_start
            left = max(0, REST_SECONDS - int(elapsed))
            rm, rs = divmod(left, 60)
            job.stats["phase"] = f"Rᴇsᴛɪɴɢ ({rm}ᴍ {rs}s)"
            try:
                await job.ui_msg.edit_caption(
                    f"<b><blockquote>Bᴀᴛᴄʜ {batch_count} — {job.stats['phase']}</blockquote></b>\n"
                    f"<blockquote>Rᴇsᴜᴍɪɴɢ ᴀᴛ {datetime.fromtimestamp(rest_start + REST_SECONDS).strftime('%H:%M')}</blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception: pass
            if elapsed >= REST_SECONDS: break
            await asyncio.sleep(min(30.0, left or 1))

    def _update_phase(self, job: BroadcastJob):
        cur_w = job.stats["target_workers"]
        prev_w = job.stats.get("_prev_workers", WORKER_COUNT_INIT)
        job.stats["phase"] = (
            f"Sᴄᴀʟɪɴɢ ᴜᴘ → {cur_w}"  if cur_w > prev_w else
            f"Sᴄᴀʟɪɴɢ ᴅᴏᴡɴ → {cur_w}" if cur_w < prev_w else
            "Fᴡ Pʀᴇssᴜʀᴇ"              if job.fw_coord.under_pressure() else
            f"CB {job.circuit_bkr.state_str()}" if job.circuit_bkr.is_open else
            "Sᴇɴᴅɪɴɢ"
        )
        job.stats["_prev_workers"] = cur_w

    async def _update_ui(self, job: BroadcastJob, done: int, batch_count: int):
        percent = job.get_progress_pct()
        rpm = job.rolling.rate_per_min()
        eta = job.get_eta_str()
        bar = "●" * int(percent / 5) + "○" * (20 - int(percent / 5))
        spark = job.sparkline.spark()
        cur_delay = _adaptive_delay(job.stats)
        throttle = f" Dᴇʟᴀʏ×{cur_delay/SEND_DELAY:.1f}" if cur_delay > SEND_DELAY else ""
        mode_str = _mode_str(job.args)
        
        caption = (
            f"<b><blockquote>{job.stats['phase']}{throttle} — {mode_str}</blockquote></b>\n"
            f"<blockquote>[{bar}] {percent}%  Bᴀᴛᴄʜ {batch_count}</blockquote>\n"
            f"<blockquote>{spark} {rpm:,}/ᴍɪɴ | {eta} | W:{job.stats['target_workers']} | CB:{job.circuit_bkr.state_str()}</blockquote>\n"
            f"<blockquote>Sᴜᴄᴄᴇss: {job.stats['success']:,} | Fᴀɪʟᴇᴅ: {job.stats['failed']:,}</blockquote>\n"
            f"<blockquote>Dᴇᴀᴄᴛ: {job.stats['deactivated']:,} | Bʟᴏᴄᴋᴇᴅ: {job.stats['blocked']:,}</blockquote>"
            + (f"\n<blockquote>{job.fw_coord.last_str()}</blockquote>" if job.fw_coord.last_str() else "")
        )
        try:
            await job.ui_msg.edit_caption(caption, parse_mode=ParseMode.HTML, reply_markup=job.ui_msg.reply_markup)
        except Exception: pass

    async def _cleanup(self, job: BroadcastJob, worker_tasks: list[asyncio.Task]):
        # 1. Cancel everything
        for t in job.tasks: 
            if not t.done(): t.cancel()
        for t in worker_tasks:
            if not t.done(): t.cancel()
            
        try:
            await asyncio.wait_for(job.queue.join(), timeout=30.0)
        except asyncio.TimeoutError:
            job.stats["drain_ok"] = False
            
        await asyncio.gather(*job.tasks, *worker_tasks, return_exceptions=True)
        
        # 2. Final UI
        done = sum(job.stats[k] for k in ("success", "failed", "deactivated", "blocked"))
        percent = job.get_progress_pct()
        bar = "●" * int(percent / 5) + "○" * (20 - int(percent / 5))
        
        cancelled = job.is_cancelled()
        img = IMG_CANCELLED if cancelled else IMG_COMPLETED
        title = "Bʀᴏᴀᴅᴄᴀsᴛ Cᴀɴᴄᴇʟʟᴇᴅ" if cancelled else "Bʀᴏᴀᴅᴄᴀsᴛ Cᴏᴍᴘʟᴇᴛᴇᴅ"
        elapsed = time.perf_counter() - job.start_ts
        avg_rate = done / elapsed if elapsed > 0 else 0
        drain_warn = "\n<blockquote>⚠ Qᴜᴇᴜᴇ ᴅʀᴀɪɴ ᴛɪᴍᴇᴅ ᴏᴜᴛ — ᴘᴀʀᴛɪᴀʟ ᴅᴇʟɪᴠᴇʀʏ.</blockquote>" if not job.stats["drain_ok"] else ""

        try:
            await job.ui_msg.edit_media(
                InputMediaPhoto(img, caption=(
                    f"<b><blockquote>{title}</blockquote></b>\n"
                    f"<blockquote>[{bar}] {percent}%</blockquote>\n"
                    f"<blockquote>Sᴜᴄᴄᴇss: {job.stats['success']:,} | Fᴀɪʟᴇᴅ: {job.stats['failed']:,}</blockquote>\n"
                    f"<blockquote>Dᴇᴀᴄᴛ: {job.stats['deactivated']:,} | Bʟᴏᴄᴋᴇᴅ: {job.stats['blocked']:,}</blockquote>\n"
                    f"<blockquote>Dᴜʀᴀᴛɪᴏɴ: {int(elapsed // 60)}ᴍ {int(elapsed % 60)}s"
                    f" | Aᴠɢ: {int(avg_rate * 60):,}/ᴍɪɴ | Pᴇᴀᴋ W: {len(worker_tasks)}</blockquote>"
                    + drain_warn
                )),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ", callback_data="close_bcast")]])
            )
        except Exception: pass

        if not cancelled: await _checkpoint_clear()
        
        async with self._lock:
            self._jobs.pop(job.id, None)
        log.info(f"[SERVICE] {job.id} finished. Final Success: {job.stats['success']}")

# Global Service Instance
broadcast_svc = BroadcastService()


# ═══════════════════════════════════════════════════════════════════════════════
#  HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

@Client.on_message(filters.command("broadcast") & _sudo_filter())
async def broadcast_command(bot_app: Client, m: Message) -> None:
    global _global_kill_until

    # Global kill check
    if time.time() < _global_kill_until:
        rem_h = int((_global_kill_until - time.time()) / 3600)
        rem_m = int((_global_kill_until - time.time()) % 3600 / 60)
        return await m.reply(
            f"<b><blockquote>Gʟᴏʙᴀʟ Kɪʟʟ Aᴄᴛɪᴠᴇ</blockquote></b>\n"
            f"<blockquote>Rᴇsᴜᴍᴇ ɪɴ ~{rem_h}ʜ {rem_m}ᴍ</blockquote>",
            parse_mode=ParseMode.HTML,
        )

    raw_parts = m.text.split()[1:]

    # ── /broadcast resume ─────────────────────────────────────────────
    if raw_parts and raw_parts[0].lower() == "resume":
        if not m.reply_to_message:
            return await m.reply(
                "<b><blockquote>Rᴇᴘʟʏ ᴛᴏ ᴛʜᴇ ᴏʀɪɢɪɴᴀʟ ᴍᴇssᴀɢᴇ.</blockquote></b>",
                parse_mode=ParseMode.HTML,
            )
        ckpt = await _checkpoint_load()
        if not ckpt:
            return await m.reply(
                "<b><blockquote>Nᴏ Cʜᴇᴄᴋᴘᴏɪɴᴛ</blockquote></b>\n"
                "<blockquote>Nᴏ ɪɴᴛᴇʀʀᴜᴘᴛᴇᴅ ʙʀᴏᴀᴅᴄᴀsᴛ ғᴏᴜɴᴅ.</blockquote>",
                parse_mode=ParseMode.HTML,
            )

        ok, reason = _checkpoint_validate(ckpt)
        if not ok:
            return await m.reply(
                f"<b><blockquote>Iɴᴠᴀʟɪᴅ Cʜᴇᴄᴋᴘᴏɪɴᴛ</blockquote></b>\n"
                f"<blockquote>{reason}\nUsᴇ /broadcast ɴᴏʀᴍᴀʟʟʏ ᴛᴏ sᴛᴀʀᴛ ғʀᴇsʜ.</blockquote>",
                parse_mode=ParseMode.HTML,
            )

        args         = parse_broadcast_args(ckpt.get("args_raw", "").split())
        watermark    = int(ckpt["watermark"])
        already_sent = int(ckpt["success"])
        total_orig   = int(ckpt["total"])
        remaining    = total_orig - already_sent
        b_id         = m.id

        age_m = int((time.time() - float(ckpt["saved_at"])) / 60)
        await m.reply(
            f"<b><blockquote>Rᴇsᴜᴍɪɴɢ Bʀᴏᴀᴅᴄᴀsᴛ</blockquote></b>\n"
            f"<blockquote>"
            f"Sᴀᴠᴇᴅ: {age_m}ᴍ ᴀɢᴏ\n"
            f"Aʟʀᴇᴀᴅʏ sᴇɴᴛ: {already_sent:,} / {total_orig:,}\n"
            f"Rᴇᴍᴀɪɴɪɴɢ: ~{remaining:,}\n"
            f"Wᴀᴛᴇʀᴍᴀʀᴋ: ᴜɪᴅ > {watermark} (sᴀғᴇ ᴄᴜʀsᴏʀ)"
            f"</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        await broadcast_svc.start(
            bot_app, m, args,
            effective      = total_orig,
            db_total       = total_orig,
            prod_limit     = remaining if remaining < total_orig else None,
            resume_from    = watermark,
            already_sent   = already_sent,
            b_id           = b_id,
            clamp_note     = "",
        )
        return

    # ── Normal broadcast ──────────────────────────────────────────────
    if not m.reply_to_message:
        return await m.reply(
            "<pre>❐ 𝗕𝗥𝗢𝗔𝗗𝗖𝗔𝗦𝗧 𝗘𝗡𝗚𝗜𝗡𝗘</pre>\n"
            "<blockquote>"
            "◍ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇssᴀɢᴇ ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ"
            "</blockquote>\n"
            "<pre>❐ 𝗨𝗦𝗔𝗚𝗘</pre>\n"
            "<blockquote>"
            "◍ sᴛʏʟᴇ  : <code>ultra</code> · <code>dash</code>\n"
            "◍ ᴍᴏᴅᴇs  : <code>dry</code> · <code>silent</code> · <code>pin</code> · <code>pin_loud</code>\n"
            "◍ ᴅᴇʟᴇᴛᴇ : <code>autodel 1h</code> · <code>30m</code> · <code>60s</code>\n"
            "◍ ʟɪᴍɪᴛ  : <code>500</code> · <code>10%</code>\n"
            "◍ ʀᴇsᴜᴍᴇ : <code>/broadcast resume</code>"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖", callback_data="close_bcast")]]),
        )

    raw_parts = m.text.split()[1:]

    # Style flag (ultra/dash) is required as the first argument
    if not raw_parts or raw_parts[0].lower() not in ("ultra", "dash", "dry", "resume"):
        return await m.reply(
            "<pre>❐ 𝗕𝗥𝗢𝗔𝗗𝗖𝗔𝗦𝗧 𝗘𝗡𝗚𝗜𝗡𝗘</pre>\n"
            "<blockquote>"
            "◍ sᴘᴇᴄɪғʏ <b>ultra</b> ᴏʀ <b>dash</b> ᴀs ᴛʜᴇ ꜰɪʀsᴛ ᴀʀɢᴜᴍᴇɴᴛ"
            "</blockquote>\n"
            "<pre>❐ 𝗘𝗫𝗔𝗠𝗣𝗟𝗘𝗦</pre>\n"
            "<blockquote>"
            "◍ <code>/broadcast ultra</code>\n"
            "◍ <code>/broadcast dash</code>\n"
            "◍ <code>/broadcast ultra silent pin 500</code>\n"
            "◍ <code>/broadcast dash autodel 1h</code>\n"
            "◍ <code>/broadcast dry</code>\n"
            "◍ <code>/broadcast resume</code>"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cʟᴏsᴇ ✖", callback_data="close_bcast")]]),
        )

    args = parse_broadcast_args(raw_parts)
    b_id = m.id

    try:
        db_total = await users.count_documents(_BASE_FILTER)
    except Exception as e:
        log.error(f"[BROADCAST] count_documents failed: {e}")
        await m.reply(
            f"<b><blockquote>Cᴏᴜɴᴛ Fᴀɪʟᴇᴅ</blockquote></b>\n"
            f"<blockquote><code>{e}</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    if db_total == 0:
        any_total = await users.estimated_document_count()
        await m.reply(
            "<b><blockquote>Nᴏ Usᴇʀs</blockquote></b>\n"
            f"<blockquote>{'Dᴀᴛᴀʙᴀsᴇ ɪs ᴇᴍᴘᴛʏ.' if any_total == 0 else f'{any_total:,} ᴅᴏᴄs ᴇxɪsᴛ ʙᴜᴛ 0 ᴘᴀss ᴛʜᴇ ғɪʟᴛᴇʀ.'}"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    limit         = resolve_limit(args, db_total)
    limit_clamped = limit is not None and limit > db_total
    effective     = db_total if (limit is None or limit_clamped) else limit
    prod_limit    = None if limit is None else effective
    clamp_note    = (
        f"\nNᴏᴛᴇ: Lɪᴍɪᴛ {limit:,} > ᴅʙ ({db_total:,}). Sᴇɴᴅɪɴɢ ᴛᴏ ᴀʟʟ."
    ) if limit_clamped else ""

    # Dry run
    if args["dry"]:
        mid_jitter       = (JITTER_RANGE[0] + JITTER_RANGE[1]) / 2
        throughput_per_s = WORKER_COUNT_INIT / (SEND_DELAY + mid_jitter)
        est_s            = int(effective / throughput_per_s)
        est_batches      = (effective + BATCH_SIZE - 1) // BATCH_SIZE
        total_est        = est_s + max(0, est_batches - 1) * REST_SECONDS
        h, rem           = divmod(total_est, 3600)
        m_, s            = divmod(rem, 60)

        await m.reply_photo(
            IMG_DRYRUN,
            caption=(
                "<b><blockquote>Dʀʏ Rᴜɴ Rᴇsᴜʟᴛ</blockquote></b>\n"
                f"<blockquote>"
                f"Mᴏᴅᴇ: {_mode_str(args)}\n"
                f"Dʙ: {db_total:,} | Sᴇɴᴅɪɴɢ ᴛᴏ: {effective:,}{clamp_note}\n"
                f"Bᴀᴛᴄʜᴇs: {est_batches:,} × {BATCH_SIZE:,}\n"
                f"Wᴏʀᴋᴇʀs: {WORKER_COUNT_INIT}→{WORKER_COUNT_MAX} | Rᴘs ᴄᴀᴘ: {GLOBAL_RPS_LIMIT:.0f}\n"
                f"Bᴀsᴇ ʀᴀᴛᴇ: ~{int(throughput_per_s * 60):,}/ᴍɪɴ\n"
                f"Esᴛ. ᴅᴜʀᴀᴛɪᴏɴ: ~{h}ʜ {m_}ᴍ {s}s\n"
                f"Fɪɴɪsʜᴇs ᴀᴛ: {_wall_clock_eta(effective, throughput_per_s)}"
                f"</blockquote>"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Cʟᴏsᴇ", callback_data="close_bcast"),
            ]]),
        )
        return

    await broadcast_svc.start(
        bot_app, m, args,
        effective=effective, db_total=db_total, prod_limit=prod_limit,
        resume_from=0, already_sent=0, b_id=b_id, clamp_note=clamp_note,
    )


@Client.on_callback_query(filters.regex(r"^cancel_bcast:(\d+)$"))
async def cancel_bcast(_, cb: CallbackQuery) -> None:
    b_id = int(cb.data.split(":")[1])
    if not broadcast_svc.is_active(b_id):
        await cb.answer("Nᴏᴛ ғᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴅᴏɴᴇ.", show_alert=True)
        return
    await broadcast_svc.cancel(b_id)
    await cb.answer("Cᴀɴᴄᴇʟʟɪɴɢ...", show_alert=False)
    try:
        await cb.message.edit_caption(
            "<b><blockquote>Cᴀɴᴄᴇʟʟᴀᴛɪᴏɴ Rᴇǫᴜᴇsᴛᴇᴅ...</blockquote></b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^close_bcast$"))
async def close_bcast(_, cb: CallbackQuery) -> None:
    await cb.answer()
    try:
        await cb.message.delete()
    except Exception:
        pass

log.info(
    f"Broadcast V8 loaded — "
    f"FWCoordinator, GlobalRateLimiter({GLOBAL_RPS_LIMIT}rps), "
    f"CircuitBreaker, ScalerGuard, WorkerCursorRegistry, "
    f"autodel tracking, correct shutdown order"
)
