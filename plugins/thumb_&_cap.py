# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V2 PRO — USER SETTINGS ====================

import os
import re
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
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified, RPCError, MessageIdInvalid

from helper.database import DARKXSIDE78
from plugins.access_control import guard_user
from plugins.admin import safe_edit, close_btn

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗧𝗥𝗨𝗖𝗧𝗨𝗥𝗘𝗗 𝗖𝗢𝗡𝗦𝗧𝗔𝗡𝗧𝗦 & 𝗨𝗜 𝗧𝗘𝗫𝗧𝗦 |
# ═══════════════════════════════════════════════════════════

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
    FORMAT_ERROR = (
        "<blockquote>✗ 𝗙𝗢𝗥𝗠𝗔𝗧 𝗘𝗥𝗥𝗢𝗥</blockquote>\n\n"
        "◍  Yᴏᴜ ᴅɪᴅ ɴᴏᴛ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɪɴᴘᴜᴛ.\n\n"
        "<blockquote>ⓘ Pʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴛʜᴇ ʀᴇQᴜɪʀᴇᴅ ᴛᴇxᴛ ᴀꜰᴛᴇʀ ᴛʜᴇ ᴄᴏᴍᴍᴀɴᴅ.</blockquote>"
    )
    FILE_TOO_LARGE = (
        "<blockquote>✗ 𝗙𝗜𝗟𝗘 𝗧𝗢𝗢 𝗟𝗔𝗥𝗚𝗘</blockquote>\n\n"
        "◍  Tʜᴇ ᴘʜᴏᴛᴏ ᴇxᴄᴇᴇᴅꜱ ᴛʜᴇ ᴍᴀxɪᴍᴜᴍ ᴀʟʟᴏᴡᴇᴅ ꜱɪᴢᴇ.\n\n"
        "<blockquote>ⓘ Pʟᴇᴀꜱᴇ ᴜꜱᴇ ᴀɴ ɪᴍᴀɢᴇ ᴜɴᴅᴇʀ 5MB.</blockquote>"
    )
    INVALID_MEDIA_THUMB = (
        "<blockquote>ⓘ 𝗜𝗡𝗩𝗔𝗟𝗜𝗗 𝗥𝗘𝗤𝗨𝗘𝗦𝗧</blockquote>\n\n"
        "◍  Pʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴅᴏᴄᴜᴍᴇɴᴛ ᴏʀ ᴠɪᴅᴇᴏ ᴛʜᴀᴛ ʜᴀꜱ ᴀ ᴛʜᴜᴍʙɴᴀɪʟ."
    )
    INVALID_PHOTO_REPLY = (
        "<blockquote>ⓘ 𝗜𝗡𝗩𝗔𝗟𝗜𝗗 𝗥𝗘𝗤𝗨𝗘𝗦𝗧</blockquote>\n\n"
        "◍  Pʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ᴡɪᴛʜ <code>/set_thumb</code> ᴛᴏ ꜱᴇᴛ ɪᴛ ᴀꜱ ʏᴏᴜʀ ᴛʜᴜᴍʙɴᴀɪʟ."
    )
    NO_EXTRACTABLE_THUMB = (
        "<blockquote>ⓘ 𝗡𝗢 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗙𝗢𝗨𝗡𝗗</blockquote>\n\n"
        "◍  Tʜᴇ ʀᴇᴘʟɪᴇᴅ ᴍᴇᴅɪᴀ ᴅᴏᴇꜱ ɴᴏᴛ ᴄᴏɴᴛᴀɪɴ ᴀɴ ᴇxᴛʀᴀᴄᴛᴀʙʟᴇ ᴛʜᴜᴍʙɴᴀɪʟ."
    )
    
    INITIATING = "<blockquote>⧗ 𝗜𝗡𝗜𝗧𝗜𝗔𝗧𝗜𝗡𝗚 𝗣𝗥𝗢𝗖𝗘𝗦𝗦...</blockquote>"
    FETCHING_THUMB = "<blockquote>⧗ 𝗙𝗘𝗧𝗖𝗛𝗜𝗡𝗚 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟...</blockquote>"
    
    # Format Specific (If integrated later)
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
    FORMAT_DEL_SUCCESS = (
        "<blockquote>✓ 𝗙𝗢𝗥𝗠𝗔𝗧 𝗗𝗘𝗟𝗘𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴀᴜᴛᴏ-ʀᴇɴᴀᴍᴇ ᴛᴇᴍᴘʟᴀᴛᴇ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ."
    )
    VIEW_FORMAT = (
        "<blockquote>ⓘ 𝗖𝗨𝗥𝗥𝗘𝗡𝗧 𝗙𝗢𝗥𝗠𝗔𝗧</blockquote>\n\n"
        "◍  Tʜᴇ ꜰᴏʟʟᴏᴡɪɴɢ ꜰᴏʀᴍᴀᴛ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ <code>{format_template}</code></blockquote>"
    )
    
    # Thumb Specific
    NO_THUMB = (
        "<blockquote>ⓘ 𝗡𝗢 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗙𝗢𝗨𝗡𝗗</blockquote>\n\n"
        "◍  Yᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀɴʏ ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙɴᴀɪʟ ꜱᴇᴛ.\n\n"
        "<blockquote>➥ Rᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ᴡɪᴛʜ /set_thumb ᴛᴏ ꜱᴇᴛ ᴏɴᴇ.</blockquote>"
    )
    THUMB_OVERWRITE_WARN = (
        "<blockquote>ⓘ 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗦𝗘𝗧</blockquote>\n\n"
        "◍  A ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙɴᴀɪʟ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ Pʟᴇᴀꜱᴇ ᴜꜱᴇ /del_thumb ᴛᴏ ʀᴇᴍᴏᴠᴇ ɪᴛ ʙᴇꜰᴏʀᴇ ꜱᴇᴛᴛɪɴɢ ᴀ ɴᴇᴡ ᴏɴᴇ.</blockquote>"
    )
    THUMB_SAVE_SUCCESS = (
        "<blockquote>✓ 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗦𝗔𝗩𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙɴᴀɪʟ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ."
    )
    THUMB_DEL_SUCCESS = (
        "<blockquote>✓ 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗗𝗘𝗟𝗘𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴛʜᴜᴍʙɴᴀɪʟ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ."
    )
    VIEW_THUMB = (
        "<blockquote>ⓘ 𝗖𝗨𝗥𝗥𝗘𝗡𝗧 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟</blockquote>\n\n"
        "◍  Tʜɪꜱ ᴛʜᴜᴍʙɴᴀɪʟ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ."
    )
    FETCHED_THUMB = (
        "<blockquote>ⓘ 𝗘𝗫𝗧𝗥𝗔𝗖𝗧𝗘𝗗 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟</blockquote>\n\n"
        "◍  Hᴇʀᴇ ɪꜱ ᴛʜᴇ ᴛʜᴜᴍʙɴᴀɪʟ ᴇxᴛʀᴀᴄᴛᴇᴅ ꜰʀᴏᴍ ʏᴏᴜʀ ᴍᴇᴅɪᴀ."
    )
    
    # Caption Specific
    NO_CAPTION = (
        "<blockquote>ⓘ 𝗡𝗢 𝗖𝗔𝗣𝗧𝗜𝗢𝗡 𝗙𝗢𝗨𝗡𝗗</blockquote>\n\n"
        "◍  Yᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀɴʏ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ꜱᴇᴛ.\n\n"
        "<blockquote>➥ Pʟᴇᴀꜱᴇ ꜱᴇᴛ ᴏɴᴇ ᴜꜱɪɴɢ /setcap</blockquote>"
    )
    CAPTION_OVERWRITE_WARN = (
        "<blockquote>ⓘ 𝗖𝗔𝗣𝗧𝗜𝗢𝗡 𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗦𝗘𝗧</blockquote>\n\n"
        "◍  A ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ɪꜱ ᴀʟʀᴇᴀᴅʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ Pʟᴇᴀꜱᴇ ᴜꜱᴇ /delcap ᴛᴏ ʀᴇᴍᴏᴠᴇ ɪᴛ ʙᴇꜰᴏʀᴇ ꜱᴇᴛᴛɪɴɢ ᴀ ɴᴇᴡ ᴏɴᴇ.</blockquote>"
    )
    CAPTION_SAVE_SUCCESS = (
        "<blockquote>✓ 𝗖𝗔𝗣𝗧𝗜𝗢𝗡 𝗦𝗔𝗩𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ."
    )
    CAPTION_DEL_SUCCESS = (
        "<blockquote>✓ 𝗖𝗔𝗣𝗧𝗜𝗢𝗡 𝗗𝗘𝗟𝗘𝗧𝗘𝗗</blockquote>\n\n"
        "◍  Yᴏᴜʀ ᴄᴜꜱᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ʜᴀꜱ ʙᴇᴇɴ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ʀᴇᴍᴏᴠᴇᴅ."
    )
    VIEW_CAPTION = (
        "<blockquote>ⓘ 𝗖𝗨𝗥𝗥𝗘𝗡𝗧 𝗖𝗔𝗣𝗧𝗜𝗢𝗡</blockquote>\n\n"
        "◍  Tʜᴇ ꜰᴏʟʟᴏᴡɪɴɢ ᴄᴀᴘᴛɪᴏɴ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ᴀɴᴅ ᴀᴄᴛɪᴠᴇ ꜰᴏʀ ʏᴏᴜʀ ꜰɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ.\n\n"
        "<blockquote>➥ <code>{caption}</code></blockquote>"
    )


# ═══════════════════════════════════════════════════════════
#  | 𝗘𝗫𝗖𝗘𝗣𝗧𝗜𝗢𝗡𝗦 & 𝗧𝗔𝗦𝗞 𝗥𝗘𝗚𝗜𝗦𝗧𝗥𝗬 |
# ═══════════════════════════════════════════════════════════

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


class UserSettingsService:
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
                    # Lazy TTL cleanup
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

    async def _fetch_user_field(self, user_id: int, field: str) -> Optional[str]:
        user = await DARKXSIDE78.get_user(user_id)
        return user.get(field) if user else None

    # ─── PUBLIC TYPED API ───
    async def get_thumbnail(self, user_id: int) -> Optional[str]:
        return await self._get(user_id, "thumb", lambda uid: self._fetch_user_field(uid, "file_id"))

    async def set_thumbnail(self, user_id: int, file_id: Optional[str]) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.update_user, user_id, {"$set": {"file_id": file_id}})
            await self._invalidate(user_id, "thumb")

    async def get_caption(self, user_id: int) -> Optional[str]:
        return await self._get(user_id, "caption", lambda uid: self._fetch_user_field(uid, "caption"))

    async def set_caption(self, user_id: int, caption: Optional[str]) -> None:
        async with self._user_locks[user_id]:
            await retry_db(DARKXSIDE78.update_user, user_id, {"$set": {"caption": caption}})
            await self._invalidate(user_id, "caption")

user_svc = UserSettingsService()

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 & 𝗔𝗡𝗜𝗠𝗔𝗧𝗜𝗢𝗡 |
# ═══════════════════════════════════════════════════════════

MAX_SIZE = 5 * 1024 * 1024  # 5MB Safe Input Profile

def sanitize_input(text: str) -> str:
    """Limit length but do NOT escape HTML tags at storage layer"""
    return str(text)[:2048]

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
                await message.edit_text(f"✗ Error: {safe_err_text}")

async def async_remove(filepath: str):
    """Async-safe file deletion"""
    def _remove():
        if os.path.exists(filepath):
            os.remove(filepath)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _remove)


# ═══════════════════════════════════════════════════════════
#  | 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(['get_thumb', 'gthumb']))
@guard_user()
async def get_file_thumb(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)
        
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    if not message.reply_to_message or not (message.reply_to_message.document or message.reply_to_message.video):
        return await message.reply_text(UI_Texts.INVALID_MEDIA_THUMB, parse_mode=ParseMode.HTML)

    media = message.reply_to_message.document or message.reply_to_message.video
    thumb = media.thumbs[0] if getattr(media, "thumbs", None) else getattr(media, "thumbnail", None)

    if not thumb:
        return await message.reply_text(UI_Texts.NO_EXTRACTABLE_THUMB, parse_mode=ParseMode.HTML)

    status_msg = await message.reply_text(UI_Texts.FETCHING_THUMB, parse_mode=ParseMode.HTML)

    downloaded_thumb = None
    try:
        downloaded_thumb = await client.download_media(thumb)
        await client.send_photo(
            chat_id=message.chat.id,
            photo=downloaded_thumb,
            caption=UI_Texts.FETCHED_THUMB,
            parse_mode=ParseMode.HTML
        )
        await status_msg.delete()
    except FloodWait as e:
        await safe_edit(status_msg, UI_Texts.API_RATE_LIMIT.format(time=e.value))
    except Exception as e:
        logger.error(f"[user-cmd] thumb extraction fault (user={user_id}): {e}")
        err_msg = html.escape(str(e))
        await safe_edit(status_msg, UI_Texts.SYS_ERROR.format(error=err_msg))
    finally:
        if downloaded_thumb:
            with contextlib.suppress(Exception):
                await async_remove(downloaded_thumb)


@Client.on_message(filters.private & filters.command(['set_thumb', 'setthumb', 'sthumb']))
@guard_user()
async def set_thumb_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        existing_thumb = await user_svc.get_thumbnail(user_id)
        if existing_thumb:
            return await message.reply_text(UI_Texts.THUMB_OVERWRITE_WARN, parse_mode=ParseMode.HTML)
    except DatabaseUnavailableError as e:
        return await message.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)

    replied = message.reply_to_message
    
    if not replied or not replied.photo:
        return await message.reply(UI_Texts.INVALID_PHOTO_REPLY, parse_mode=ParseMode.HTML)
        
    if replied.photo.file_size and replied.photo.file_size > MAX_SIZE:
        return await message.reply_text(UI_Texts.FILE_TOO_LARGE, parse_mode=ParseMode.HTML)
    
    mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
    logger.info(f"[SET_THUMB] user={user_id}")
    
    await safe_exec(
        lambda: user_svc.set_thumbnail(user_id, file_id=replied.photo.file_id),
        mkn,
        UI_Texts.THUMB_SAVE_SUCCESS
    )


@Client.on_message(filters.private & filters.command(['view_thumb', 'viewthumb', 'vthumb']))
@guard_user()
async def viewthumb(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        thumb = await user_svc.get_thumbnail(user_id)
        if thumb:
            await client.send_photo(
                chat_id=message.chat.id,
                photo=thumb,
                caption=UI_Texts.VIEW_THUMB,
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply_text(UI_Texts.NO_THUMB, parse_mode=ParseMode.HTML)
    except DatabaseUnavailableError as e:
        await message.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)
    except Exception as e:
        err_msg = html.escape(str(e))
        await message.reply_text(UI_Texts.SYS_ERROR.format(error=err_msg), parse_mode=ParseMode.HTML)


@Client.on_message(filters.private & filters.command(['del_thumb', 'delthumb', 'dthumb']))
@guard_user()
async def removethumb(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
    logger.info(f"[DEL_THUMB] user={user_id}")
    
    await safe_exec(
        lambda: user_svc.set_thumbnail(user_id, file_id=None),
        mkn,
        UI_Texts.THUMB_DEL_SUCCESS
    )


# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗔𝗣𝗧𝗜𝗢𝗡 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command(['set_caption', 'setcap', 'scaption']))
@guard_user()
async def add_caption(client: Client, message: Message):
    user_id = message.from_user.id

    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        existing_cap = await user_svc.get_caption(user_id)
        if existing_cap:
            return await message.reply_text(UI_Texts.CAPTION_OVERWRITE_WARN, parse_mode=ParseMode.HTML)
    except DatabaseUnavailableError as e:
        return await message.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)

    if len(message.command) == 1:
        return await message.reply_text(
            "<blockquote expandable><b>✦ Iᴍᴘᴏʀᴛᴀɴᴛ</b> : "
            "Bᴇғᴏʀᴇ ᴜsɪɴɢ /set_caption, ᴘʟᴇᴀsᴇ ᴄʜᴇᴄᴋ ʏᴏᴜʀ "
            "ᴛᴇᴍᴘʟᴀᴛᴇ ᴄᴀʀᴇғᴜʟʟʏ, ᴀs ᴛʜᴇ ʙᴏᴛ ᴡɪʟʟ "
            "ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴀᴘᴘʟʏ ɪᴛ ᴛᴏ ᴀʟʟ ғɪʟᴇs "
            "ʏᴏᴜ sᴇɴᴅ\n\n"
            "<b>Cᴀᴘᴛɪᴏɴ Vᴀʀɪᴀʙʟᴇs :</b>\n\n"
            "<b>◍ Cᴏʀᴇ :</b>\n"
            "- <code>{filename}</code> : Gᴇᴛs ᴛʜᴇ ᴏʀɪɢɪɴᴀʟ ғɪʟᴇ ɴᴀᴍᴇ.\n"
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
            "➥ <code>/set_caption File : {filename}</code>\n\n"
            "➥ <code>/set_caption {title} - Season {season} Episode {episode} [{resolution}] {audio}</code>\n\n"
            "<b><blockquote>≡ Tɪᴘ : Tʜᴇ ʙᴏᴛ ᴡɪʟʟ ᴜsᴇ ᴛʜɪs ᴛᴇᴍᴘʟᴀᴛᴇ ᴛᴏ sᴇᴛ ʏᴏᴜʀ ᴄᴀᴘᴛɪᴏɴ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.</blockquote></b>",
            parse_mode=ParseMode.HTML,
            quote=True
        )

    try:
        raw_caption = message.text.html.split(None, 1)[1]
        caption = sanitize_input(raw_caption)

        mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
        logger.info(f"[SET_CAPTION] user={user_id} caption={caption}")

        await safe_exec(
            lambda: user_svc.set_caption(user_id, caption),
            mkn,
            UI_Texts.CAPTION_SAVE_SUCCESS
        )

    except IndexError:
        await message.reply_text(UI_Texts.FORMAT_ERROR, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception(f"[SET_CAPTION_ERROR] {e}")
        err_msg = html.escape(str(e))
        await message.reply_text(UI_Texts.SYS_ERROR.format(error=err_msg), parse_mode=ParseMode.HTML)


@Client.on_message(filters.private & filters.command(['see_caption', 'viewcap']))
@guard_user()
async def see_caption(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        caption = await user_svc.get_caption(user_id)
        if caption:
            safe_display_cap = html.escape(caption)
            await message.reply_text(
                UI_Texts.VIEW_CAPTION.format(caption=safe_display_cap),
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply_text(UI_Texts.NO_CAPTION, parse_mode=ParseMode.HTML)
    except DatabaseUnavailableError as e:
        await message.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)
    except Exception as e:
         err_msg = html.escape(str(e))
         await message.reply_text(UI_Texts.SYS_ERROR.format(error=err_msg), parse_mode=ParseMode.HTML)


@Client.on_message(filters.private & filters.command('delcap'))
@guard_user()
async def delete_caption(client: Client, message: Message):
    user_id = message.from_user.id
    if not rate_limiter.check(user_id):
        return await message.reply_text(UI_Texts.RATE_LIMIT, quote=True, parse_mode=ParseMode.HTML)

    await client.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        caption = await user_svc.get_caption(user_id)
        if not caption:
            return await message.reply_text(UI_Texts.NO_CAPTION, parse_mode=ParseMode.HTML)
            
        mkn = await message.reply_text(UI_Texts.INITIATING, parse_mode=ParseMode.HTML)
        logger.info(f"[DEL_CAPTION] user={user_id}")
        
        await safe_exec(
            lambda: user_svc.set_caption(user_id, None),
            mkn,
            UI_Texts.CAPTION_DEL_SUCCESS
        )
    except DatabaseUnavailableError as e:
        await message.reply_text(UI_Texts.DB_ERROR.format(error=str(e)), parse_mode=ParseMode.HTML)

logger.info("[✓] User Thumb/Caption Module Loaded — Aesthetic Hardened")
