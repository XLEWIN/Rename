# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ᴘᴇᴀᴋ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗖𝗢𝗡𝗧𝗥𝗢𝗟𝗦 • 𝗩𝟮 𝗥𝗘𝗫𝗬              │
#│  (𝗔𝗦𝗬𝗡𝗖 𝗢𝗧𝗔, 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗧𝗘𝗟𝗘𝗠𝗘𝗧𝗥𝗬)           │
#└──────────────────────────────────────────┘

import os
import sys
import time
import html
import asyncio
import logging
import contextlib
from functools import wraps
from typing import Optional, Callable, Awaitable, Tuple, Set, Union

from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified

from helper.database import DARKXSIDE78
from config import Config
from plugins.admin import admin_svc, safe_edit, close_btn

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗧𝗥𝗨𝗖𝗧𝗨𝗥𝗘𝗗 𝗖𝗢𝗡𝗦𝗧𝗔𝗡𝗧𝗦 & 𝗨𝗜 𝗧𝗘𝗫𝗧𝗦 |
# ═══════════════════════════════════════════════════════════

class UI_Texts:
    INITIATING = "<blockquote>⧗ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗜𝗡𝗜𝗧𝗜𝗔𝗟𝗜𝗭𝗜𝗡𝗚...</blockquote>"
    
    SYS_FAULT = (
        "<blockquote>✗ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗙𝗔𝗨𝗟𝗧</blockquote>\n\n"
        "◍  Aɴ ᴜɴᴇxᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴅᴜʀɪɴɢ ᴛʜᴇ ꜱʏꜱᴛᴇᴍ ᴏᴘᴇʀᴀᴛɪᴏɴ.\n\n"
        "<blockquote expandable>➥ <code>{error}</code></blockquote>"
    )
    
    # Admin Mode Panel
    ADMIN_PANEL = (
        "<blockquote>⚙️ 𝗔𝗗𝗠𝗜𝗡 𝗖𝗢𝗡𝗧𝗥𝗢𝗟 𝗣𝗔𝗡𝗘𝗟</blockquote>\n\n"
        "◍  Tᴏɢɢʟᴇ ꜱʏꜱᴛᴇᴍ ᴀᴄᴄᴇꜱꜱɪʙɪʟɪᴛʏ ꜰᴏʀ ᴘᴜʙʟɪᴄ ᴜꜱᴇʀꜱ ɢʟᴏʙᴀʟʟʏ.\n\n"
        "<blockquote>➥ Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜꜱ : {status}</blockquote>"
    )
    
    # Update (OTA)
    OTA_SUCCESS = (
        "<blockquote>✓ 𝗥𝗘𝗣𝗢𝗦𝗜𝗧𝗢𝗥𝗬 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Nᴇᴡ ᴄᴏᴅᴇ ꜰᴇᴛᴄʜᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ. Rᴇʙᴏᴏᴛɪɴɢ ꜱʏꜱᴛᴇᴍ...\n\n"
        "<blockquote expandable>➥ <code>{output}</code></blockquote>"
    )
    OTA_FAILED = (
        "<blockquote>✗ 𝗨𝗣𝗗𝗔𝗧𝗘 𝗙𝗔𝗜𝗟𝗘𝗗</blockquote>\n\n"
        "◍  Gɪᴛ ᴘᴜʟʟ ᴇɴᴄᴏᴜɴᴛᴇʀᴇᴅ ᴀ ᴍᴇʀɢᴇ ᴄᴏɴꜰʟɪᴄᴛ ᴏʀ ɴᴇᴛᴡᴏʀᴋ ᴇʀʀᴏʀ.\n\n"
        "<blockquote expandable>➥ <code>{output}</code></blockquote>"
    )
    
    # Restart
    RESTART_PROMPT = (
        "<blockquote>🔄 𝗦𝗬𝗦𝗧𝗘𝗠 𝗥𝗘𝗕𝗢𝗢𝗧</blockquote>\n\n"
        "◍  Aʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴋɪʟʟ ᴀɴᴅ ʀᴇꜱᴛᴀʀᴛ ᴛʜᴇ ʀᴜɴᴛɪᴍᴇ?\n\n"
        "<blockquote>➥ Tʜɪꜱ ᴀᴄᴛɪᴏɴ ᴡɪʟʟ ᴅɪꜱʀᴜᴘᴛ ᴀʟʟ ᴀᴄᴛɪᴠᴇ ᴜꜱᴇʀ ᴘʀᴏᴄᴇꜱꜱᴇꜱ.</blockquote>"
    )
    RESTARTING = "<blockquote>⧗ 𝗥𝗘𝗕𝗢𝗢𝗧𝗜𝗡𝗚 𝗦𝗬𝗦𝗧𝗘𝗠...</blockquote>"
    
    # Jobs / Ledger
    JOB_LEDGER = (
        "<blockquote>📊 𝗝𝗢𝗕 𝗟𝗘𝗗𝗚𝗘𝗥 𝗧𝗘𝗟𝗘𝗠𝗘𝗧𝗥𝗬</blockquote>\n\n"
        "◍  <b>Tᴏᴛᴀʟ Jᴏʙꜱ :</b> {total}   |   <b>Aᴄᴛɪᴠᴇ :</b> {active}\n"
        "◍  <b>UɴɪQᴜᴇ Uꜱᴇʀꜱ :</b> {users}   |   <b>Rᴇᴛʀɪᴇꜱ :</b> {retries}\n\n"
        "<blockquote expandable>➥ <b>Sᴛᴀᴛᴜꜱ Bʀᴇᴀᴋᴅᴏᴡɴ:</b>\n{breakdown}</blockquote>"
    )

    # Callbacks
    CB_ACCESS_DENIED = "✗ Aᴄᴄᴇꜱꜱ ᴅᴇɴɪᴇᴅ. Aᴅᴍɪɴꜱ ᴏɴʟʏ."
    CB_MODE_UPDATED = "✓ Sʏꜱᴛᴇᴍ Mᴏᴅᴇ Uᴘᴅᴀᴛᴇᴅ"


# Global task registry for clean shutdowns
_active_animations: Set[asyncio.Task] = set()

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class SystemService:
    """Handles high-level system states and shell operations."""
    
    def __init__(self):
        self._mode_cache: Optional[bool] = None
        self._ts: float = 0.0
        self._ttl: float = 30.0

    async def get_admin_mode(self) -> bool:
        now = time.monotonic()
        if self._mode_cache is not None and (now - self._ts < self._ttl):
            return self._mode_cache

        status = await DARKXSIDE78.get_admin_mode()
        self._mode_cache = status
        self._ts = now
        return status

    async def set_admin_mode(self, status: bool) -> None:
        await DARKXSIDE78.set_admin_mode(status)
        self._mode_cache = status
        self._ts = time.monotonic()

    async def update_bot(self) -> Tuple[bool, str]:
        """Non-blocking async wrapper for git pull operations."""
        try:
            process = await asyncio.create_subprocess_exec(
                "git", "pull",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            output = stdout.decode().strip()
            err_output = stderr.decode().strip()
            
            if process.returncode == 0:
                return True, output or "Already up to date."
            return False, err_output or "Unknown git error."
        except Exception as e:
            logger.error(f"[sys] OTA fault: {e}")
            return False, str(e)

    def restart_process(self) -> None:
        """Kills the active runtime for system reboot."""
        os.execl(sys.executable, sys.executable, *sys.argv)

    async def get_job_stats(self) -> dict:
        """Wraps ledger into service layer paradigm."""
        try:
            from helper.job_ledger import job_ledger
            return await job_ledger.get_stats()
        except ImportError:
            return {"error": "Job Ledger module unavailable"}


system_svc = SystemService()


# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗔𝗡𝗜𝗠𝗔𝗧𝗜𝗢𝗡 |
# ═══════════════════════════════════════════════════════════

def admin_only(func: Callable) -> Callable:
    """Decorator to enforce Admin/Owner privileges globally."""
    @wraps(func)
    async def wrapper(client: Client, update: Union[Message, CallbackQuery], *args, **kwargs):
        user_id = update.from_user.id
        if not await admin_svc.is_admin(user_id) and user_id != Config.OWNER_ID:
            if isinstance(update, CallbackQuery):
                await update.answer(UI_Texts.CB_ACCESS_DENIED, show_alert=True)
            return
        return await func(client, update, *args, **kwargs)
    return wrapper


async def play_animation(message: Message, frames: list, stop_event: asyncio.Event, delay: float = 0.5):
    """Handles non-blocking, infinite loop status animations."""
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
                except Exception:
                    stop_event.set()
                    break
    except Exception:
        pass


async def respond_with_error(message: Message, error_msg: str) -> None:
    """Standardized fault response."""
    safe_err = html.escape(str(error_msg))[:400]
    await safe_edit(message, UI_Texts.SYS_FAULT.format(error=safe_err), reply_markup=close_btn())


def build_admin_keyboard(is_on: bool) -> InlineKeyboardMarkup:
    """Builds a pure-GUI toggle switch for Admin Mode."""
    # Top button acts as a visual status indicator
    status_indicator = InlineKeyboardButton(
        "🟢 Aᴄᴛɪᴠᴇ : Pʀɪᴠᴀᴛᴇ (Aᴅᴍɪɴꜱ Oɴʟʏ)" if is_on else "🔴 Aᴄᴛɪᴠᴇ : Pᴜʙʟɪᴄ (Aʟʟ Uꜱᴇʀꜱ)",
        callback_data="ignore_admin_status"
    )
    # Bottom button is the actual interactive toggle
    toggle_btn = InlineKeyboardButton(
        "🔓 Sᴡɪᴛᴄʜ ᴛᴏ Pᴜʙʟɪᴄ Mᴏᴅᴇ" if is_on else "🔒 Sᴡɪᴛᴄʜ ᴛᴏ Pʀɪᴠᴀᴛᴇ Mᴏᴅᴇ",
        callback_data="toggle_admin_mode"
    )
    
    return InlineKeyboardMarkup([
        [status_indicator],
        [toggle_btn],
        [InlineKeyboardButton("✘ Cʟᴏꜱᴇ Pᴀɴᴇʟ", callback_data="close_data")]
    ])


# ═══════════════════════════════════════════════════════════
#  /admin_mode (Interactive Panel)
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("admin_mode") & filters.private)
@admin_only
async def admin_mode_command(client: Client, message: Message):
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    
    try:
        current_mode = await system_svc.get_admin_mode()
        status_str = "<b>Pʀɪᴠᴀᴛᴇ</b>" if current_mode else "<b>Pᴜʙʟɪᴄ</b>"
        
        await message.reply_text(
            UI_Texts.ADMIN_PANEL.format(status=status_str),
            reply_markup=build_admin_keyboard(current_mode),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"[sys] admin_mode fault: {e}", exc_info=True)
        await respond_with_error(message, str(e))


@Client.on_callback_query(filters.regex(r"^ignore_admin_status$"))
async def cb_ignore_status(client: Client, query: CallbackQuery):
    """Silently ignores clicks on the status indicator button."""
    await query.answer()


@Client.on_callback_query(filters.regex(r"^toggle_admin_mode$"))
@admin_only
async def cb_toggle_admin_mode(client: Client, query: CallbackQuery):
    try:
        current_mode = await system_svc.get_admin_mode()
        new_mode = not current_mode
        await system_svc.set_admin_mode(new_mode)
        
        status_str = "<b>Pʀɪᴠᴀᴛᴇ</b>" if new_mode else "<b>Pᴜʙʟɪᴄ</b>"
        
        await query.message.edit_text(
            UI_Texts.ADMIN_PANEL.format(status=status_str),
            reply_markup=build_admin_keyboard(new_mode),
            parse_mode=ParseMode.HTML
        )
        await query.answer(UI_Texts.CB_MODE_UPDATED)
        logger.info(f"[sys] admin_mode toggled to {new_mode} by {query.from_user.id}")
    except MessageNotModified:
        await query.answer()
    except Exception as e:
        await query.answer(f"✗ Eʀʀᴏʀ: {str(e)[:50]}", show_alert=True)


# ═══════════════════════════════════════════════════════════
#  /update (Async OTA & Restart)
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("update") & filters.private)
@admin_only
async def update_bot_command(client: Client, message: Message):
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
    
    stop_event = asyncio.Event()
    frames = [
        "<blockquote>⧗ 𝗣𝗨𝗟𝗟𝗜𝗡𝗚 𝗟𝗔𝗧𝗘𝗦𝗧 𝗖𝗢𝗠𝗠𝗜𝗧𝗦.</blockquote>",
        "<blockquote>⧗ 𝗣𝗨𝗟𝗟𝗜𝗡𝗚 𝗟𝗔𝗧𝗘𝗦𝗧 𝗖𝗢𝗠𝗠𝗜𝗧𝗦..</blockquote>",
        "<blockquote>⧗ 𝗣𝗨𝗟𝗟𝗜𝗡𝗚 𝗟𝗔𝗧𝗘𝗦𝗧 𝗖𝗢𝗠𝗠𝗜𝗧𝗦...</blockquote>",
        "<blockquote>⧗ 𝗣𝗨𝗟𝗟𝗜𝗡𝗚 𝗟𝗔𝗧𝗘𝗦𝗧 𝗖𝗢𝗠𝗠𝗜𝗧𝗦....</blockquote>"
    ]
    
    anim_task = asyncio.create_task(play_animation(mkn, frames, stop_event))
    _active_animations.add(anim_task)
    anim_task.add_done_callback(_active_animations.discard)

    try:
        success, output = await system_svc.update_bot()
        
        stop_event.set()
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(anim_task, timeout=2.0)
        
        safe_output = html.escape(output)[:800]
        
        if success:
            await safe_edit(mkn, UI_Texts.OTA_SUCCESS.format(output=safe_output))
            logger.info(f"[sys] Successful OTA update fired by {message.from_user.id}")
            await asyncio.sleep(2)
            system_svc.restart_process()
        else:
            await safe_edit(mkn, UI_Texts.OTA_FAILED.format(output=safe_output), reply_markup=close_btn())
            logger.warning(f"[sys] OTA update failed: {output[:100]}")

    except Exception as e:
        stop_event.set()
        logger.error(f"[sys] OTA command fault: {e}", exc_info=True)
        await respond_with_error(mkn, str(e))


# ═══════════════════════════════════════════════════════════
#  /restart
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("restart") & filters.private)
@admin_only
async def restart_bot_command(client: Client, message: Message):
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    await message.reply_text(
        UI_Texts.RESTART_PROMPT,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✓ Yᴇꜱ, Rᴇʙᴏᴏᴛ", callback_data="sys_restart_confirm"),
                InlineKeyboardButton("✘ Aʙᴏʀᴛ", callback_data="sys_restart_cancel")
            ]
        ]),
        parse_mode=ParseMode.HTML
    )
    logger.info(f"[sys] Restart prompt invoked by {message.from_user.id}")


@Client.on_callback_query(filters.regex(r"^sys_restart_confirm$"))
@admin_only
async def cb_restart_confirm(client: Client, query: CallbackQuery):
    await query.answer("Kɪʟʟɪɴɢ ᴘʀᴏᴄᴇꜱꜱᴇꜱ...")
    try:
        await safe_edit(query.message, UI_Texts.RESTARTING)
    except:
        pass
    
    logger.warning(f"[sys] Manual restart executed by {query.from_user.id}")
    system_svc.restart_process()


@Client.on_callback_query(filters.regex(r"^sys_restart_cancel$"))
@admin_only
async def cb_restart_cancel(client: Client, query: CallbackQuery):
    await query.answer("Rᴇʙᴏᴏᴛ ᴀʙᴏʀᴛᴇᴅ.")
    try:
        await query.message.delete()
    except:
        pass


# ═══════════════════════════════════════════════════════════
#  /jobs — Ledger Aggregation
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("jobs") & filters.private)
@admin_only
async def admin_jobs_command(client: Client, message: Message):
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    temp = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)

    try:
        stats = await system_svc.get_job_stats()
        
        if "error" in stats:
            return await respond_with_error(temp, stats["error"])
            
        by_status = stats.get("by_status", {})
        if not by_status:
            status_text = "  • <i>Nᴏ ᴀᴄᴛɪᴠᴇ ꜱᴛᴀᴛᴜꜱ ᴅᴀᴛᴀ</i>"
        else:
            status_lines = [f"  • {k.capitalize()} : <code>{v}</code>" for k, v in by_status.items()]
            status_text = "\n".join(status_lines)
            
        reply_text = UI_Texts.JOB_LEDGER.format(
            total=stats.get('total', 0),
            active=stats.get('active', 0),
            users=stats.get('unique_active_users', 0),
            retries=stats.get('avg_retries_on_fail', 0),
            breakdown=status_text
        )
        
        await safe_edit(temp, reply_text, reply_markup=close_btn())
        logger.info(f"[sys] /jobs ledger accessed by {message.from_user.id}")

    except Exception as e:
        logger.error(f"[sys] /jobs fault: {e}", exc_info=True)
        await respond_with_error(temp, str(e))

logger.info("[✓] System Controls Module Loaded — Peak Production Grade")

