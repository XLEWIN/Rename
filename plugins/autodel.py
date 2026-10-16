import asyncio
import html
import logging
import re
import time
from typing import Optional, Tuple

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from helper.database import DARKXSIDE78
from plugins.admin import admin_svc, safe_edit, _on_cooldown, close_btn, _admin_or_sudo_filter

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class AutoDeleteService:
    def __init__(self):
        self._cache_status: Optional[bool] = None
        self._cache_timer: Optional[int] = None
        self._ts: float = 0.0
        self._ttl: float = 30.0
        self._lock = asyncio.Lock()

    async def get_settings(self) -> Tuple[bool, int]:
        now = time.monotonic()
        async with self._lock:
            if self._cache_status is not None and self._cache_timer is not None and (now - self._ts < self._ttl):
                return self._cache_status, self._cache_timer

        status, timer = await asyncio.gather(
            DARKXSIDE78.get_auto_delete(),
            DARKXSIDE78.get_del_timer(),
        )

        async with self._lock:
            self._cache_status = status
            self._cache_timer = timer
            self._ts = time.monotonic()

        return status, timer

    async def toggle(self) -> bool:
        status, timer = await self.get_settings()
        new_status = not status
        await DARKXSIDE78.set_auto_delete(new_status)
        async with self._lock:
            self._cache_status = new_status
            self._ts = time.monotonic()
        return new_status

    async def set_timer(self, timer: int) -> None:
        await DARKXSIDE78.set_del_timer(timer)
        async with self._lock:
            self._cache_timer = timer
            self._ts = time.monotonic()

autodel_svc = AutoDeleteService()

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

def convert_time(duration_seconds: int) -> str:
    periods = [
        ('Yᴇᴀʀ', 60 * 60 * 24 * 365),
        ('Mᴏɴᴛʜ', 60 * 60 * 24 * 30),
        ('Dᴀʏ', 60 * 60 * 24),
        ('Hᴏᴜʀ', 60 * 60),
        ('Mɪɴᴜᴛᴇ', 60),
        ('Sᴇᴄᴏɴᴅ', 1)
    ]

    parts = []
    for period_name, period_seconds in periods:
        if duration_seconds >= period_seconds:
            num_periods = duration_seconds // period_seconds
            duration_seconds %= period_seconds
            parts.append(f"{num_periods} {period_name}{'s' if num_periods > 1 else ''}")

    if len(parts) == 0:
        return "0 Sᴇᴄᴏɴᴅ"
    elif len(parts) == 1:
        return parts[0]
    else:
        return ', '.join(parts[:-1]) +' ᴀɴᴅ '+ parts[-1]

def parse_timer(text: str) -> Optional[int]:
    """
    Parses flexible timer inputs like:
    10, 10s, 10sec, 60min, 1h, 1day, 24h, 1month.
    """
    text = text.strip().lower()
    if text.isdigit():
        return int(text)

    match = re.match(r"^(\d+)\s*([a-z]+)$", text)
    if not match:
        return None

    val = int(match.group(1))
    unit = match.group(2)

    if unit in ("s", "sec", "secs", "second", "seconds"):
        return val
    elif unit in ("m", "min", "mins", "minute", "minutes"):
        return val * 60
    elif unit in ("h", "hr", "hrs", "hour", "hours"):
        return val * 3600
    elif unit in ("d", "day", "days"):
        return val * 86400
    elif unit in ("mo", "month", "months"):
        return val * 2592000
    elif unit in ("y", "yr", "year", "years"):
        return val * 31536000
    
    return None

async def safe_delete(msg: Message, delay: int = 0):
    if not msg:
        return
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗜 𝗕𝗨𝗜𝗟𝗗𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def build_autodel_markup(enabled: bool) -> InlineKeyboardMarkup:
    btn_text = 'Dɪsᴀʙʟᴇ Mᴏᴅᴇ ✗' if enabled else 'Eɴᴀʙʟᴇ Mᴏᴅᴇ ✓'
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(btn_text, callback_data='chng_autodel'),
            InlineKeyboardButton('Sᴇᴛ Tɪᴍᴇʀ', callback_data='set_timer'),
        ],
        [
            InlineKeyboardButton('Rᴇғʀᴇsʜ', callback_data='autodel_cmd'),
            InlineKeyboardButton('Cʟᴏsᴇ', callback_data='close_msg'),
        ],
    ])

def format_autodel_text(enabled: bool, timer_sec: int) -> str:
    mode_text = "<b>Eɴᴀʙʟᴇᴅ ✓</b>" if enabled else "<b>Dɪsᴀʙʟᴇᴅ ✗</b>"
    timer_str = convert_time(timer_sec)
    return (
        f"<b><blockquote>✦ 𝗔𝗨𝗧𝗢 𝗗𝗘𝗟𝗘𝗧𝗘 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</blockquote></b>\n"
        f"<blockquote>›› ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ ᴍᴏᴅᴇ : {mode_text}</blockquote>\n"
        f"<blockquote>›› ᴅᴇʟᴇᴛᴇ ᴛɪᴍᴇʀ : {timer_str}</blockquote>\n"
        f"────────────────────\n"
        f"<b><blockquote>≡ ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ʙᴜᴛᴛᴏɴs ᴛᴏ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs</blockquote></b>"
    )

# ═══════════════════════════════════════════════════════════
#  | 𝗛𝗔𝗡𝗗𝗟𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

AUTODEL_PIC = "https://i.ibb.co/gbT4r1LD/photo-2026-05-02-19-22-34-7635377075376881688.jpg"

@Client.on_message(filters.command("autodel") & filters.private & _admin_or_sudo_filter())
async def autoDelete_settings(client: Client, message: Message):
    if _on_cooldown(message.from_user.id, "autodel_menu"):
        return

    asyncio.create_task(safe_delete(message, delay=0))

    try:
        status, timer = await autodel_svc.get_settings()

        sent = await message.reply_photo(
            photo=AUTODEL_PIC,  # ← your variable here
            caption=format_autodel_text(status, timer),
            reply_markup=build_autodel_markup(status),
        )

        asyncio.create_task(safe_delete(sent, delay=600))

    except Exception as e:
        logger.error(f"autoDelete_settings error: {e}")
        await message.reply_text(
            f"<b>! Eʀʀᴏʀ Oᴄᴄᴜʀᴇᴅ..</b>\n"
            f"<blockquote>Rᴇᴀsᴏɴ: {html.escape(str(e))}</blockquote>\n",
            reply_markup=close_btn(),
        )


@Client.on_callback_query(filters.regex(r"^autodel_cmd$"))
async def refresh_autodel(client: Client, query: CallbackQuery):
    if not await admin_svc.is_admin(query.from_user.id):
        return await query.answer("Admin only.", show_alert=True)
        
    if _on_cooldown(query.from_user.id, "autodel_refresh"):
        return await query.answer("Calm down, refreshing too fast!", show_alert=False)

    try:
        status, timer = await autodel_svc.get_settings()

        await safe_edit(
            query.message,
            text=format_autodel_text(status, timer),
            reply_markup=build_autodel_markup(status)
        )
        await query.answer("Refreshed ! ✓")
    except Exception as e:
        logger.error(f"refresh_autodel error: {e}")
        await query.answer(f"Error: {e}", show_alert=True)


@Client.on_callback_query(filters.regex(r"^chng_autodel$"))
async def toggle_autodel(client: Client, query: CallbackQuery):
    if not await admin_svc.is_admin(query.from_user.id):
        return await query.answer("Admin only.", show_alert=True)
        
    if _on_cooldown(query.from_user.id, "autodel_toggle"):
        return await query.answer("Calm down, clicking too fast!", show_alert=False)

    try:
        new_status = await autodel_svc.toggle()
        _, timer = await autodel_svc.get_settings()
        
        await safe_edit(
            query.message,
            text=format_autodel_text(new_status, timer),
            reply_markup=build_autodel_markup(new_status)
        )
        await query.answer(f"Auto-delete {'Enabled' if new_status else 'Disabled'}! ✅")
    except Exception as e:
        logger.error(f"toggle_autodel error: {e}")
        await query.answer(f"Error: {e}", show_alert=True)


@Client.on_callback_query(filters.regex(r"^set_timer$"))
async def set_timer_cb(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    if not await admin_svc.is_admin(uid):
        return await query.answer("Admin only.", show_alert=True)
        
    if _on_cooldown(uid, "autodel_set_timer"):
        return await query.answer("Wait before setting timer again.", show_alert=False)
    
    await query.answer()
    
    try:
        _, timer_sec = await autodel_svc.get_settings()
        timer_str = convert_time(timer_sec)
        
        set_msg = await client.ask(
            chat_id=uid,
            text=(
                f"<b><blockquote>⧗ Cᴜʀʀᴇɴᴛ Tɪᴍᴇʀ : {timer_str}</blockquote>\n"
                "<blockquote>Tᴏ ᴄʜᴀɴɢᴇ ᴛɪᴍᴇʀ, sᴇɴᴅ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ (ᴏʀ ᴡɪᴛʜ ᴜɴɪᴛ) ᴡɪᴛʜɪɴ 1 ᴍɪɴᴜᴛᴇ.</blockquote>\n"
                "<blockquote>Fᴏʀ ᴇxᴀᴍᴘʟᴇ: <code>300</code>, <code>60m</code>, <code>1h</code>, <code>1day</code>, <code>1month</code></blockquote></b>"
            ),
            timeout=60,
        )
    except asyncio.TimeoutError:
        await client.send_message(
            uid,
            "<b>! Eʀʀᴏʀ Oᴄᴄᴜʀᴇᴅ..\n<blockquote><i>Rᴇᴀsᴏɴ: 1 minute timeout</i></blockquote></b>",
            disable_notification=True,
        )
        logger.warning("set_timer timed out for uid=%s", uid)
        return
    except Exception as exc:
        logger.error(f"set_timer error: {exc}")
        return

    new_timer = parse_timer(set_msg.text)

    # Allow bounds from 10 seconds to 1 month (2592000 seconds)
    if new_timer is not None and 10 <= new_timer <= 2592000:
        await autodel_svc.set_timer(new_timer)
        await set_msg.reply(
            f"<b><blockquote>Aᴅᴅᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ ✓</blockquote>\n"
            f"<blockquote>⏱ Cᴜʀʀᴇɴᴛ Tɪᴍᴇʀ : {convert_time(new_timer)}</blockquote></b>"
        )
    else:
        retry_kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Sᴇᴛ Dᴇʟᴇᴛᴇ Tɪᴍᴇʀ", callback_data="set_timer")]]
        )
        await set_msg.reply(
            "<b><blockquote>⧗ Tᴏ ᴄʜᴀɴɢᴇ ᴛɪᴍᴇʀ, Pʟᴇᴀsᴇ sᴇɴᴅ ᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ ɪɴ sᴇᴄᴏɴᴅs ᴡɪᴛʜɪɴ 1ᴍɪɴᴜᴛᴇ.</blockquote>\n"
            "<blockquote>Fᴏʀ ᴇxᴀᴍᴘʟᴇ : <code>300</code>, <code>60min</code>, <code>1h</code>, <code>1day</code></blockquote>", 
            reply_markup=retry_kb,
        )

# -----------------------------------------------------------------------------
# Background Worker for Persistent Batch Deletions
# -----------------------------------------------------------------------------

from collections import defaultdict
from pyrogram.errors import FloodWait

_worker_task = None
_MAX_RETRIES = 3
_MAX_MSGS_PER_TASK = 5000  # Soft cap to prevent RAM explosion per chat

async def _process_chat(client: Client, chat_id: int, msg_ids: list[int], semaphore: asyncio.Semaphore) -> tuple[int, int]:
    """Processes a single chat's deletions, returns (deleted_count, failed_count)."""
    # Deduplicate message IDs to prevent wasted calls
    msg_ids = list(set(msg_ids))
    
    # Soft cap per task to avoid RAM spikes if thousands of messages pile up
    msg_ids = msg_ids[:_MAX_MSGS_PER_TASK]
    
    deleted_count = 0
    failed_count = 0
    
    for i in range(0, len(msg_ids), 100):
        chunk = msg_ids[i:i + 100]
        retries = 0
        chunk_size = 100
        
        while retries < _MAX_RETRIES and chunk:
            try:
                async with semaphore:
                    await client.delete_messages(chat_id, chunk)
                deleted_count += len(chunk)
                await asyncio.sleep(0.5)  # micro-delay to prevent internal rate limits
                break  # success
            except FloodWait as e:
                retries += 1
                logger.warning(f"FloodWait triggered in autodel for chat {chat_id}: sleeping {e.value}s (Retry {retries}/{_MAX_RETRIES})")
                await asyncio.sleep(e.value)
            except Exception as e:
                # Partial failure or total failure
                retries += 1
                logger.warning(f"Batch delete failed for chat {chat_id} (Retry {retries}/{_MAX_RETRIES}): {e}")
                
                # Fallback to smaller chunk size on failure
                if chunk_size == 100:
                    chunk_size = 50
                    chunk = chunk[:50]  # Just try first half to get past bad messages
                elif chunk_size == 50:
                    chunk_size = 10
                    chunk = chunk[:10]
                
                await asyncio.sleep(1.0)
                
        if retries == _MAX_RETRIES:
            logger.error(f"Failed to delete {len(chunk)} messages in chat {chat_id} after {_MAX_RETRIES} retries.")
            failed_count += len(chunk)

    return deleted_count, failed_count


async def _autodel_worker(client: Client):
    # Cap concurrent Telegram delete requests
    semaphore = asyncio.Semaphore(10)
    
    while True:
        try:
            due_jobs = await DARKXSIDE78.get_and_clear_due_deletions()
            if due_jobs:
                # Group by chat_id to minimize API calls
                chat_messages = defaultdict(list)
                for job in due_jobs:
                    chat_id = job.get("chat_id")
                    if chat_id:
                        chat_messages[chat_id].extend(job.get("message_ids", []))
                
                tasks = []
                for chat_id, msg_ids in chat_messages.items():
                    tasks.append(_process_chat(client, chat_id, msg_ids, semaphore))
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                total_deleted = 0
                total_failed = 0
                
                for res in results:
                    if isinstance(res, tuple):
                        total_deleted += res[0]
                        total_failed += res[1]
                    elif isinstance(res, Exception):
                        logger.error(f"Worker task crashed: {res}")
                        total_failed += 1
                
                logger.info(
                    "Processed %d deletion jobs across %d chats | Deleted: %d | Failed: %d", 
                    len(due_jobs), len(chat_messages), total_deleted, total_failed
                )
        except asyncio.CancelledError:
            logger.info("Auto-delete worker cancelled. Shutting down gracefully.")
            break
        except Exception as e:
            logger.error(f"autodel worker error: {e}")
        
        await asyncio.sleep(10)

async def on_load(client: Client):
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(_autodel_worker(client))
        logger.info("Auto-delete persistent worker started.")

async def on_unload():
    global _worker_task
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None
        logger.info("Auto-delete persistent worker stopped.")
