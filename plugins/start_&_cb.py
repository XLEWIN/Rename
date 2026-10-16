# © 2026 Rexy (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀＥ • sＣＡＬＥ ʀＥＡᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛＩＯＮ ʀＥғＡＣᴛＯʀ ᴄＯＭᴘʟＥᴛＥ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — START & CALLBACKS ====================

import random
import re
import asyncio
import functools
import logging
from functools import wraps

from datetime import datetime, timedelta
from urllib.parse import quote

from pyrogram import Client, filters, enums
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified, FloodWait
from pyrogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    InputMediaPhoto,
)

from config import Config, Txt
from helper.database import DARKXSIDE78
from helper.db_premium import db
from helper.utils import send_log
from plugins.access_control import guard_user
from plugins.admin import safe_send_message, safe_edit_media
try:
    from plugins.shortener import handle_token_redemption
except (ImportError, ModuleNotFoundError, Exception):
    try:
        from plugins.shortner import handle_token_redemption
    except (ImportError, ModuleNotFoundError, Exception):
        async def handle_token_redemption(client, message, token_id):
            pass

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Helper Decorators & Logic
# ─────────────────────────────────────────────────────────────

def catch_not_modified(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except MessageNotModified:
            pass
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
    return wrapper

async def safe_reply_text(message: Message, text: str, **kwargs):
    """FloodWait-safe reply_text wrapper."""
    for attempt in range(3):
        try:
            return await message.reply_text(text, **kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait in safe_reply_text: sleeping {e.value}s")
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            logger.error(f"safe_reply_text failed: {e}")
            return None

def _format_timedelta(td: timedelta) -> str:
    parts = []
    if td.days: parts.append(f"{td.days}d")
    h, r = divmod(td.seconds, 3600)
    if h: parts.append(f"{h}h")
    m, s = divmod(r, 60)
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts[:2])

def init(*args, **kwargs):
    logger.info("[start] Start & CB plugin initialized.")

#----------------------

_START_CB_RE = re.compile(
    r"^("
    r"home|caption|main|meta|sequence|file_names|"
    r"seeplans|premium_info|free|"
    r"about|premiumx|pp|close"
    r")$"
)

@Client.on_message(filters.private & filters.command("start"))
@guard_user()
async def start(client, message: Message):
    user = message.from_user
    user_id = user.id
    is_new = False
    if not await db.is_user_exist(user_id):
        await db.add_user(user_id, username=user.username, first_name=user.first_name)
        is_new = True

    existing_user = await DARKXSIDE78.col.find_one({"_id": user_id})

    if len(message.command) > 1:
        arg = message.command[1]
        await handle_token_redemption(client, message, arg)
        return

    if is_new:
        try:
            await safe_send_message(
                client,
                chat_id=Config.LOG_CHANNEL,
                text=(
                    f"<blockquote>❐ <b>NEW USER</b></blockquote>\n"
                    f"<blockquote>◍ NAME : {user.mention}\n"
                    f"◍ ID : <code>{user_id}</code>\n"
                    f"◍ JOINED AT : {datetime.utcnow().strftime('%d %B %Y, %I:%M %p')} UTC</blockquote>"
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"[start] log channel write fault: {e}")

    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('ᴅᴇᴠ',   user_id=int(8093380715))
        ],
        [
            InlineKeyboardButton('ᴀʙᴏᴜᴛ', callback_data='about'),
            InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close")
        ]
    ])

    if Config.START_PIC:
        await message.reply_photo(
            photo=Config.START_PIC,
            caption=f"{Txt.START_TXT.format(user.mention)}",
            reply_markup=buttons,
            parse_mode=ParseMode.HTML
        )
    else:
        await message.reply_text(
            text=f"{Txt.START_TXT.format(user.mention)}",
            reply_markup=buttons,
            disable_web_page_preview=True,
            parse_mode=ParseMode.HTML
        )

@Client.on_callback_query(filters.regex(_START_CB_RE))
@catch_not_modified
async def cb_handler(client, query: CallbackQuery):
    data = query.data
    user_id = query.from_user.id

    await query.answer() # Immediate feedback to stop button loading state
    
    logger.debug(f"cb_handler matched: {data}")

    if data == "home":
        text_content = f"{Txt.START_TXT.format(query.from_user.mention)}"
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton('ᴅᴇᴠ',   user_id=int(8093380715))
            ],
            [
                InlineKeyboardButton('ᴀʙᴏᴜᴛ', callback_data='about'),
                InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close")
            ]
        ])

        if Config.START_PIC and query.message.photo:
            await query.message.edit_media(
                media=InputMediaPhoto(media=Config.START_PIC, caption=text_content, parse_mode=ParseMode.HTML),
                reply_markup=buttons
            )
        else:
            await query.message.edit_text(
                text=text_content,
                disable_web_page_preview=True,
                parse_mode=ParseMode.HTML,
                reply_markup=buttons
            )

    elif data == "premiumx":
        await query.message.edit_text(
            text=f"{Txt.PREMIUM_TXT}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("ʙᴀᴄᴋ", callback_data="home"), InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close")]
            ]),
            parse_mode=ParseMode.HTML
        )

    elif data == "about":
        text_content = f"{Txt.ABOUT_TXT}"
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("ʙᴀᴄᴋ", callback_data="home"), InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close")]
        ])

        # Checks if ABOUT_PIC exists and whether the original message configuration allows updating media assets
        if getattr(Config, "ABOUT_PIC", None) and query.message.photo:
            await query.message.edit_media(
                media=InputMediaPhoto(media=Config.ABOUT_PIC, caption=text_content, parse_mode=ParseMode.HTML),
                reply_markup=buttons
            )
        else:
            await query.message.edit_text(
                text=text_content,
                disable_web_page_preview=True,
                reply_markup=buttons,
                parse_mode=ParseMode.HTML
            )

    elif data == "close":
        await query.message.delete()
        try:
            await query.message.reply_to_message.delete()
        except:
            pass

    await query.answer()

__all__ = ["init"]
