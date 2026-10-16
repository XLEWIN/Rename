# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

from __future__ import annotations
import logging
import asyncio
import uuid
import time
from functools import wraps
from typing import Dict, List, Optional, Any
from pyrogram import Client, filters, StopPropagation
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
from pyrogram.errors import FloodWait, MessageNotModified

from helper.database import DARKXSIDE78 as db
from plugins.admin import safe_edit, _on_cooldown
from plugins.access_control import check_ban_status

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗦𝗔𝗙𝗘 𝗪𝗥𝗔𝗣𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for Metadata API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[meta] FloodWait in {func.__name__}: sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

@retry_on_flood
async def safe_edit_caption(client: Client, chat_id: int, message_id: int, caption: str, reply_markup=None):
    return await client.edit_message_caption(chat_id, message_id, caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

@retry_on_flood
async def safe_edit_reply_markup(client: Client, chat_id: int, message_id: int, reply_markup=None):
    return await client.edit_message_reply_markup(chat_id, message_id, reply_markup=reply_markup)

@retry_on_flood
async def safe_send_photo(client: Client, chat_id: int, photo: str, caption: str, reply_markup=None):
    return await client.send_photo(chat_id, photo, caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

@retry_on_flood
async def safe_reply_text(message: Message, text: str, **kwargs):
    return await message.reply_text(text, parse_mode=ParseMode.HTML, **kwargs)

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗦𝗧𝗔𝗡𝗧𝗦 & 𝗦𝗧𝗔𝗧𝗘 |
# ═══════════════════════════════════════════════════════════

META_PIC = "https://i.ibb.co/xqKLnRh8/photo-2026-05-02-14-25-54-7635310765376798748.jpg"

META_FIELDS = [
    ("title", "Tɪᴛʟᴇ"),
    ("author", "Aᴜᴛʜᴏʀ"),
    ("artist", "Aʀᴛɪꜱᴛ"),
    ("audio", "Aᴜᴅɪᴏ"),
    ("subtitle", "Sᴜʙᴛɪᴛʟᴇ"),
    ("video", "Vɪᴅᴇᴏ"),
    ("album", "Aʟʙᴜᴍ"),
    ("genre", "Gᴇɴʀᴇ"),
    ("composer", "Cᴏᴍᴘᴏsᴇʀ"),
    ("publisher", "Pᴜʙʟɪsʜᴇʀ"),
    ("copyright", "Cᴏᴘʏʀɪɢʜᴛ"),
    ("encoded_by", "Eɴᴄᴏᴅᴇᴅ Bʏ"),
    ("custom_tag", "Cᴜsᴛᴏᴍ Tᴀɢ"),
    ("commentz", "Cᴏᴍᴍᴇɴᴛ")
]

_meta_state: dict[int, dict] = {}
_meta_tasks: dict[int, asyncio.Task] = {}
_user_locks: dict[int, asyncio.Lock] = {}

class _MetaCache:
    def __init__(self, data: dict):
        self.data = data
        self.timestamp = time.monotonic()

_meta_cache: dict[int, _MetaCache] = {}
CACHE_TTL = 300  # 5 minutes

# ═══════════════════════════════════════════════════════════
#  | 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class MetadataService:
    @staticmethod
    async def get_all_metadata(user_id: int) -> dict:
        """Batch fetch all metadata with caching to prevent DB spam"""
        if user_id not in _user_locks:
            _user_locks[user_id] = asyncio.Lock()
            
        async with _user_locks[user_id]:
            now = time.monotonic()
            if user_id in _meta_cache and (now - _meta_cache[user_id].timestamp) < CACHE_TTL:
                return _meta_cache[user_id].data
                
            prefs = await db.get_user_prefs_batch(user_id)
            mode = await db.get_metadata(user_id)  # Returns "On" or "Off"
            
            data = {
                "mode": mode,
                "title": prefs.get("title", ""),
                "author": prefs.get("author", ""),
                "artist": prefs.get("artist", ""),
                "audio": prefs.get("audio", ""),
                "subtitle": prefs.get("subtitle", ""),
                "video": prefs.get("video", ""),
                "album": prefs.get("album", ""),
                "genre": prefs.get("genre", ""),
                "composer": prefs.get("composer", ""),
                "publisher": prefs.get("publisher", ""),
                "copyright": prefs.get("copyright", ""),
                "encoded_by": prefs.get("encoded_by", ""),
                "custom_tag": prefs.get("custom_tag", ""),
                "commentz": prefs.get("commentz", ""),
                "purge": bool(prefs.get("metadata_purge", False)),
                "tracks": bool(prefs.get("track_renaming", False))
            }
            
            _meta_cache[user_id] = _MetaCache(data)
            return data

    @staticmethod
    async def save_field(user_id: int, field: str, value: str) -> None:
        """Validate, lock, and isolate DB writes, then invalidate cache"""
        if len(value) > 2048:
            raise ValueError("Iɴᴘᴜᴛ ᴇxᴄᴇᴇᴅs ᴍᴀxɪᴍᴜᴍ ʟᴇɴɢᴛʜ ᴏꜰ 2048 ᴄʜᴀʀᴀᴄᴛᴇʀs.")
            
        if user_id not in _user_locks:
            _user_locks[user_id] = asyncio.Lock()
            
        async with _user_locks[user_id]:
            try:
                # Explicit mapping to DB methods to reach production parity
                SETTER_MAP = {
                    "title": "set_title",
                    "author": "set_author",
                    "artist": "set_artist",
                    "audio": "set_audio",
                    "subtitle": "set_subtitle",
                    "video": "set_video",
                    "album": "set_album",
                    "genre": "set_genre",
                    "composer": "set_composer",
                    "publisher": "set_publisher",
                    "copyright": "set_copyright",
                    "encoded_by": "set_encoded_by",
                    "custom_tag": "set_custom_tag",
                    "commentz": "set_commentz"
                }
                setter_name = SETTER_MAP.get(field)
                if not setter_name:
                    raise ValueError(f"Invalid field: {field}")
                    
                setter = getattr(db, setter_name, None)
                if setter:
                    await setter(user_id, value)
                else:
                    logger.error(f"[MetadataService] DB Setter {setter_name} not found")
                    raise RuntimeError("Dᴀᴛᴀʙᴀsᴇ ᴍᴇᴛʜᴏᴅ ᴍɪssɪɴɢ.")
                    
                _meta_cache.pop(user_id, None)  # Invalidate UI cache
                db.clear_user_cache(user_id)    # CRITICAL: Invalidate DB cache so rename engine sees it
            except Exception as e:
                logger.error(f"[MetadataService] DB Write Failed for {field} | {user_id}: {e}")
                raise RuntimeError("Dᴀᴛᴀʙᴀsᴇ ᴇʀʀᴏʀ ᴡʜɪʟᴇ sᴀᴠɪɴɢ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.")

# ─────────────────────────────────────────────────────────────
#  | 𝗛𝗘𝗟𝗣𝗘𝗥 𝗙𝗨𝗡𝗖𝗧𝗜𝗢𝗡𝗦 |
# ─────────────────────────────────────────────────────────────

async def get_metadata_text(user_id: int) -> str:
    import html
    data = await MetadataService.get_all_metadata(user_id)
    
    def _clean(val: str) -> str:
        if not val:
            return "Nᴏᴛ ꜰᴏᴜɴᴅ"
        escaped = html.escape(val)
        if len(escaped) > 30:
            return escaped[:27] + "..."
        return escaped

    return f"""<blockquote><b>✦ 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔 𝗠𝗢𝗗𝗘 : {data['mode']}</b></blockquote>
<blockquote expandable><b>◈ Tɪᴛʟᴇ ▹</b> <code>{_clean(data['title'])}</code>
<b>◈ Aᴜᴛʜᴏʀ ▹</b> <code>{_clean(data['author'])}</code>
<b>◈ Aʀᴛɪꜱᴛ ▹</b> <code>{_clean(data['artist'])}</code>
<b>◈ Aᴜᴅɪᴏ ▹</b> <code>{_clean(data['audio'])}</code>
<b>◈ Sᴜʙᴛɪᴛʟᴇ ▹</b> <code>{_clean(data['subtitle'])}</code>
<b>◈ Vɪᴅᴇᴏ ▹</b> <code>{_clean(data['video'])}</code>
<b>◈ Aʟʙᴜᴍ ▹</b> <code>{_clean(data['album'])}</code>
<b>◈ Gᴇɴʀᴇ ▹</b> <code>{_clean(data['genre'])}</code>
<b>◈ Cᴏᴍᴘᴏsᴇʀ ▹</b> <code>{_clean(data['composer'])}</code>
<b>◈ Pᴜʙʟɪsʜᴇʀ ▹</b> <code>{_clean(data['publisher'])}</code>
<b>◈ Cᴏᴘʏʀɪɢʜᴛ ▹</b> <code>{_clean(data['copyright'])}</code>
<b>◈ Eɴᴄᴏᴅᴇᴅ Bʏ ▹</b> <code>{_clean(data['encoded_by'])}</code>
<b>◈ Cᴜsᴛᴏᴍ Tᴀɢ ▹</b> <code>{_clean(data['custom_tag'])}</code>
<b>◈ Cᴏᴍᴍᴇɴᴛ ▹</b> <code>{_clean(data['commentz'])}</code>
<b>◈ Pᴜʀɢᴇ Mᴏᴅᴇ ▹</b> <code>{'Eɴᴀʙʟᴇᴅ' if data['purge'] else 'Dɪsᴀʙʟᴇᴅ'}</code></blockquote>
<blockquote expandable><b>✦ Pʟᴀᴄᴇʜᴏʟᴅᴇʀs:</b> <code>{{filename}}</code>, <code>{{basename}}</code>, <code>{{ext}}</code>, <code>{{user_id}}</code>, <code>{{track}}</code></blockquote>"""

def _get_field_name(field_id: str) -> str:
    for f in META_FIELDS:
        if f[0] == field_id:
            return f[1]
    return field_id.capitalize()

async def _return_to_edit_panel(client: Client, user_id: int, msg_id: int):
    text = "<b><blockquote>✦ 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔 𝗘𝗗𝗜𝗧 𝗣𝗔𝗡𝗘𝗟</blockquote></b>\n<blockquote>≡ Sᴇʟᴇᴄᴛ ᴀ ꜰɪᴇʟᴅ ᴛᴏ ᴇᴅɪᴛ ɪᴛ ᴅɪʀᴇᴄᴛʟʏ, ᴏʀ ᴜsᴇ ᴛʜᴇ ᴡɪᴢᴀʀᴅ / ᴄʟᴇᴀʀ ᴛᴏᴏʟs.</blockquote>"
    kb = build_meta_edit_menu(user_id)
    try:
        # Priority: Edit caption if media message
        await safe_edit_caption(client, user_id, msg_id, text, kb)
    except Exception:
        try:
            # Fallback: Edit text if plain message
            await client.edit_message_text(user_id, msg_id, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed to return to edit panel: {e}")

async def _temp_alert(message: Message, text: str):
    try:
        msg = await safe_reply_text(message, text, quote=True)
        await asyncio.sleep(3)
        await msg.delete()
    except Exception:
        pass

async def _prompt_wizard_field(client: Client, user_id: int, msg_id: int, fields: list[str]):
    fnames = ", ".join([_get_field_name(f) for f in fields])
    text = f"<b><blockquote>✦ 𝗦𝗘𝗧 𝗔𝗟𝗟 𝗪𝗜𝗭𝗔𝗥𝗗</blockquote></b>\n<blockquote>◍ Sᴇɴᴅ <b>ONE</b> ᴛᴇxᴛ ᴛʜᴀᴛ ᴡɪʟʟ ʙᴇ ᴀᴘᴘʟɪᴇᴅ ᴛᴏ ALL ᴏꜰ ᴛʜᴇsᴇ ꜰɪᴇʟᴅs :\n<b>{fnames}</blockquote></b>\n<blockquote><b>◈ Sᴜᴘᴘᴏʀᴛᴇᴅ Pʟᴀᴄᴇʜᴏʟᴅᴇʀs (Pʀᴇᴍɪᴜᴍ):</b>\n◍ <code>{{filename}}</code>, <code>{{basename}}</code>, <code>{{ext}}</code>, <code>{{track}}</code></blockquote>\n<blockquote><i>≡ (Yᴏᴜ ʜᴀᴠᴇ 60 sᴇᴄᴏɴᴅs. Cʟɪᴄᴋ Cᴀɴᴄᴇʟ ᴛᴏ ᴀʙᴏʀᴛ)</blockquote></i>"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"meta_cancel_input:{user_id}")]])
    await safe_edit_caption(client, user_id, msg_id, text, kb)

async def _timeout_task(client: Client, user_id: int, msg_id: int, version: str):
    await asyncio.sleep(60)
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        state = _meta_state.get(user_id)
        if state and state.get("msg_id") == msg_id and state.get("version") == version:
            _meta_state.pop(user_id, None)
            _meta_tasks.pop(user_id, None)
            await _return_to_edit_panel(client, user_id, msg_id)

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗖𝗞𝗚𝗥𝗢𝗨𝗡𝗗 𝗠𝗔𝗜𝗡𝗧𝗘𝗡𝗔𝗡𝗖𝗘 |
# ═══════════════════════════════════════════════════════════

async def _evict_old_cache():
    while True:
        try:
            await asyncio.sleep(300)
            now = time.monotonic()
            expired = [uid for uid, cache in _meta_cache.items() if now - cache.timestamp > CACHE_TTL]
            for uid in expired:
                _meta_cache.pop(uid, None)
        except asyncio.CancelledError: break
        except Exception as e: logger.error(f"[meta] Cache eviction fault: {e}")

async def _evict_stale_states():
    while True:
        try:
            await asyncio.sleep(120)
            now = time.monotonic()
            for uid in list(_meta_state.keys()):
                state = _meta_state.get(uid)
                if not state: continue
                if now - state.get("set_at", 0) > 120:
                    task = _meta_tasks.pop(uid, None)
                    if task and not task.done(): task.cancel()
                    _meta_state.pop(uid, None)
        except asyncio.CancelledError: break
        except Exception as e: logger.error(f"[meta] State eviction fault: {e}")

async def _evict_old_locks():
    while True:
        try:
            await asyncio.sleep(600)
            for uid in list(_user_locks.keys()):
                if uid not in _meta_cache and uid not in _meta_state:
                    _user_locks.pop(uid, None)
        except asyncio.CancelledError: break
        except Exception as e: logger.error(f"[meta] Lock eviction fault: {e}")

async def init(*args, **kwargs):
    loop = asyncio.get_running_loop()
    loop.create_task(_evict_old_cache())
    loop.create_task(_evict_stale_states())
    loop.create_task(_evict_old_locks())
    logger.info("[meta] Background maintenance tasks started.")

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗜 𝗕𝗨𝗜𝗟𝗗𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def build_meta_main_menu(current: str, purge: bool, tracks: bool, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"Mᴇᴛᴀ: {'Oɴ ◉' if current == 'On' else 'Oɴ'}", callback_data=f'meta_on:{user_id}'),
            InlineKeyboardButton(f"Mᴇᴛᴀ: {'Oғғ ◉' if current == 'Off' else 'Oғғ'}", callback_data=f'meta_off:{user_id}')
        ],
        [
            InlineKeyboardButton(f"Pᴜʀɢᴇ: {'Oɴ ◉' if purge else 'Oɴ'}", callback_data=f'meta_purge_on:{user_id}'),
            InlineKeyboardButton(f"Pᴜʀɢᴇ: {'Oғғ ◉' if not purge else 'Oғғ'}", callback_data=f'meta_purge_off:{user_id}')
        ],
        [
            InlineKeyboardButton(f"Tʀᴀᴄᴋs: {'Oɴ ◉' if tracks else 'Oɴ'}", callback_data=f'meta_tracks_on:{user_id}'),
            InlineKeyboardButton(f"Tʀᴀᴄᴋs: {'Oғғ ◉' if not tracks else 'Oғғ'}", callback_data=f'meta_tracks_off:{user_id}')
        ],
        [InlineKeyboardButton("Mᴇᴛᴀᴅᴀᴛᴀ Hᴇʟᴘ", callback_data=f"meta_edit_panel:{user_id}")]
    ])

def build_meta_edit_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Tɪᴛʟᴇ", callback_data=f"meta_set:title:{user_id}"),
            InlineKeyboardButton("Aᴜᴛʜᴏʀ", callback_data=f"meta_set:author:{user_id}"),
            InlineKeyboardButton("Aʀᴛɪꜱᴛ", callback_data=f"meta_set:artist:{user_id}")
        ],
        [
            InlineKeyboardButton("Aᴜᴅɪᴏ", callback_data=f"meta_set:audio:{user_id}"),
            InlineKeyboardButton("Sᴜʙᴛɪᴛʟᴇ", callback_data=f"meta_set:subtitle:{user_id}"),
            InlineKeyboardButton("Vɪᴅᴇᴏ", callback_data=f"meta_set:video:{user_id}")
        ],
        [
            InlineKeyboardButton("Aʟʙᴜᴍ", callback_data=f"meta_set:album:{user_id}"),
            InlineKeyboardButton("Gᴇɴʀᴇ", callback_data=f"meta_set:genre:{user_id}"),
            InlineKeyboardButton("Cᴏᴍᴘᴏsᴇʀ", callback_data=f"meta_set:composer:{user_id}")
        ],
        [
            InlineKeyboardButton("Pᴜʙʟɪsʜᴇʀ", callback_data=f"meta_set:publisher:{user_id}"),
            InlineKeyboardButton("Cᴏᴘʏʀɪɢʜᴛ", callback_data=f"meta_set:copyright:{user_id}")
        ],
        [
            InlineKeyboardButton("Eɴᴄᴏᴅᴇᴅ Bʏ", callback_data=f"meta_set:encoded_by:{user_id}"),
            InlineKeyboardButton("Cᴜsᴛᴏᴍ Tᴀɢ", callback_data=f"meta_set:custom_tag:{user_id}"),
            InlineKeyboardButton("Cᴏᴍᴍᴇɴᴛ", callback_data=f"meta_set:commentz:{user_id}")
        ],
        [
            InlineKeyboardButton("Sᴇᴛ Aʟʟ Wɪᴢᴀʀᴅ", callback_data=f"meta_wiz_start:{user_id}"),
            InlineKeyboardButton("Cʟᴇᴀʀ", callback_data=f"meta_clear_menu:{user_id}")
        ],
        [
            InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"meta_back_main:{user_id}"),
            InlineKeyboardButton("Cʟᴏsᴇ", callback_data="close_msg")
        ]
    ])

def build_meta_clear_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Aʟʟ", callback_data=f"meta_clear_all:{user_id}"),
            InlineKeyboardButton("Sᴇʟᴇᴄᴛᴇᴅ", callback_data=f"meta_clear_select:{user_id}")
        ],
        [InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"meta_edit_panel:{user_id}")]
    ])

def build_meta_selection_menu(selected: list[str], mode: str, user_id: int) -> InlineKeyboardMarkup:
    kb = []
    
    def _btn(f_id, f_name):
        prefix = "# " if f_id in selected else ""
        return InlineKeyboardButton(f"{prefix}{f_name}", callback_data=f"meta_toggle:{mode}:{f_id}:{user_id}")
    
    kb.append([_btn("title", "Tɪᴛʟᴇ"), _btn("author", "Aᴜᴛʜᴏʀ"), _btn("artist", "Aʀᴛɪꜱᴛ")])
    kb.append([_btn("audio", "Aᴜᴅɪᴏ"), _btn("subtitle", "Sᴜʙᴛɪᴛʟᴇ"), _btn("video", "Vɪᴅᴇᴏ")])
    kb.append([_btn("album", "Aʟʙᴜᴍ"), _btn("genre", "Gᴇɴʀᴇ"), _btn("composer", "Cᴏᴍᴘᴏsᴇʀ")])
    kb.append([_btn("publisher", "Pᴜʙʟɪsʜᴇʀ"), _btn("copyright", "Cᴏᴘʏʀɪɢʜᴛ")])
    kb.append([_btn("encoded_by", "Eɴᴄᴏᴅᴇᴅ Bʏ"), _btn("custom_tag", "Cᴜsᴛᴏᴍ Tᴀɢ"), _btn("commentz", "Cᴏᴍᴍᴇɴᴛ")])
    
    if mode == "clear":
        kb.append([
            InlineKeyboardButton("Dᴇʟᴇᴛᴇ", callback_data=f"meta_exec_clear:{user_id}"),
            InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"meta_edit_panel:{user_id}")
        ])
    else:
        kb.append([
            InlineKeyboardButton("Sᴛᴀʀᴛ Wɪᴢᴀʀᴅ", callback_data=f"meta_exec_wizard:{user_id}"),
            InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"meta_edit_panel:{user_id}")
        ])
        
    return InlineKeyboardMarkup(kb)

# ═══════════════════════════════════════════════════════════
#  | 𝗠𝗔𝗜𝗡 𝗖𝗢𝗠𝗠𝗔𝗡𝗗 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("metadata") & filters.private)
@check_ban_status
async def metadata_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    if _on_cooldown(user_id, "metadata"):
        return await message.reply_text("⧗ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ...", quote=True)

    text = await get_metadata_text(user_id)
    data = await MetadataService.get_all_metadata(user_id)
    keyboard = build_meta_main_menu(data['mode'], data['purge'], data['tracks'], user_id)
    
    try:
        await safe_send_photo(client, message.chat.id, META_PIC, text, keyboard)
    except Exception as e:
        logger.error(f"Failed to send metadata photo: {e}")
        try:
            await client.send_message(message.chat.id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception as e2:
            logger.error(f"Failed to send metadata fallback: {e2}")

# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗔𝗟𝗟𝗕𝗔𝗖𝗞 𝗛𝗔𝗡𝗗𝗟𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def _check_owner(query: CallbackQuery) -> bool:
    try:
        owner_id = int(query.data.split(":")[-1])
        return query.from_user.id == owner_id
    except (ValueError, IndexError):
        return False

@Client.on_callback_query(filters.regex(r"^(meta_on|meta_off|meta_back_main|meta_purge_on|meta_purge_off|meta_tracks_on|meta_tracks_off):(\d+)$"))
async def meta_main_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    user_id = query.from_user.id
    cmd = query.matches[0].group(1)
    
    # Premium check for Purge Mode
    is_premium = await db.is_premium(user_id)
    
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        if cmd == "meta_on":
            await db.set_metadata(user_id, "On")
        elif cmd == "meta_off":
            await db.set_metadata(user_id, "Off")
        elif cmd == "meta_purge_on":
            if not is_premium:
                return await query.answer("⭐ Pᴜʀɢᴇ Mᴏᴅᴇ ɪs ᴀ Pʀᴇᴍɪᴜᴍ ꜰᴇᴀᴛᴜʀᴇ !", show_alert=True)
            await db.set_metadata_purge(user_id, True)
        elif cmd == "meta_purge_off":
            await db.set_metadata_purge(user_id, False)
        elif cmd == "meta_tracks_on":
            if not is_premium:
                return await query.answer("⭐ Tʀᴀᴄᴋ Rᴇɴᴀᴍɪɴɢ ɪs ᴀ Pʀᴇᴍɪᴜᴍ ꜰᴇᴀᴛᴜʀᴇ !", show_alert=True)
            await db.set_track_renaming(user_id, True)
        elif cmd == "meta_tracks_off":
            await db.set_track_renaming(user_id, False)
            
        _meta_cache.pop(user_id, None)  # Invalidate UI cache
        db.clear_user_cache(user_id)    # CRITICAL: Invalidate DB cache so rename engine sees it
    text = await get_metadata_text(user_id)
    data = await MetadataService.get_all_metadata(user_id)
    keyboard = build_meta_main_menu(data['mode'], data['purge'], data['tracks'], user_id)
    
    try:
        await safe_edit_caption(client, user_id, query.message.id, text, keyboard)
    except Exception:
        try:
            await client.edit_message_text(user_id, query.message.id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed to edit metadata main menu: {e}")
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_edit_panel:(\d+)$"))
async def meta_edit_panel_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
    
    user_id = query.from_user.id
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        _meta_state.pop(user_id, None)
        if user_id in _meta_tasks:
            _meta_tasks[user_id].cancel()
            _meta_tasks.pop(user_id, None)
        
    await _return_to_edit_panel(client, user_id, query.message.id)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_set:(.+):(\d+)$"))
async def meta_set_single_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    user_id = query.from_user.id
    field = query.matches[0].group(1)
    fname = _get_field_name(field)
    
    version = str(uuid.uuid4())
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        _meta_state[user_id] = {
            "mode": "single_input",
            "field": field,
            "msg_id": query.message.id,
            "version": version,
            "set_at": time.monotonic()
        }
        
        if user_id in _meta_tasks:
            _meta_tasks[user_id].cancel()
        _meta_tasks[user_id] = asyncio.create_task(_timeout_task(client, user_id, query.message.id, version))
    
    text = f"<b><blockquote>✦ 𝗘𝗗𝗜𝗧 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔</blockquote></b>\n<blockquote>◍ Sᴇɴᴅ ʏᴏᴜʀ ɴᴇᴡ ᴛᴇxᴛ ꜰᴏʀ : <b>{fname}</b></blockquote>\n<blockquote><b>◈ Sᴜᴘᴘᴏʀᴛᴇᴅ Pʟᴀᴄᴇʜᴏʟᴅᴇʀs (Pʀᴇᴍɪᴜᴍ):</b>\n◍ <code>{{filename}}</code>, <code>{{basename}}</code>, <code>{{ext}}</code>, <code>{{track}}</code></blockquote>\n<i><blockquote>≡ (Yᴏᴜ ʜᴀᴠᴇ 60 sᴇᴄᴏɴᴅs. Cʟɪᴄᴋ Cᴀɴᴄᴇʟ ᴛᴏ ᴀʙᴏʀᴛ)</blockquote></i>"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data=f"meta_cancel_input:{user_id}")]])
    await safe_edit_caption(client, user_id, query.message.id, text, kb)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_cancel_input:(\d+)$"))
async def meta_cancel_input_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        _meta_state.pop(user_id, None)
        if user_id in _meta_tasks:
            _meta_tasks[user_id].cancel()
            _meta_tasks.pop(user_id, None)
        
    await _return_to_edit_panel(client, user_id, query.message.id)
    await query.answer("✗ Iɴᴘᴜᴛ ᴄᴀɴᴄᴇʟᴇᴅ.", show_alert=True)

@Client.on_callback_query(filters.regex(r"^meta_clear_menu:(\d+)$"))
async def meta_clear_menu_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
    
    user_id = query.from_user.id
    text = "<b><blockquote>✦ 𝗖𝗟𝗘𝗔𝗥 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔</blockquote></b>\n<blockquote>≡ Cʜᴏᴏsᴇ ᴡʜᴀᴛ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴄʟᴇᴀʀ :</blockquote>"
    kb = build_meta_clear_menu(user_id)
    await safe_edit_caption(client, user_id, query.message.id, text, kb)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_clear_all:(\d+)$"))
async def meta_clear_all_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    user_id = query.from_user.id
    try:
        tasks = [MetadataService.save_field(user_id, field, "") for field, _ in META_FIELDS]
        await asyncio.gather(*tasks)
        await _return_to_edit_panel(client, user_id, query.message.id)
        await query.answer("✓ Aʟʟ ᴍᴇᴛᴀᴅᴀᴛᴀ ꜰɪᴇʟᴅs ᴄʟᴇᴀʀᴇᴅ !", show_alert=True)
    except Exception as e:
        await _return_to_edit_panel(client, user_id, query.message.id)
        await query.answer(f"✗ Fᴀɪʟᴇᴅ : {str(e)[:50]}", show_alert=True)

@Client.on_callback_query(filters.regex(r"^meta_clear_select:(\d+)$"))
async def meta_clear_select_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    user_id = query.from_user.id
    if user_id not in _user_locks: _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        _meta_state[user_id] = {
            "mode": "clear", 
            "selected": [], 
            "msg_id": query.message.id, 
            "version": str(uuid.uuid4()),
            "set_at": time.monotonic()
        }
    
    text = "<b><blockquote>✦ 𝗖𝗟𝗘𝗔𝗥 𝗦𝗘𝗟𝗘𝗖𝗧𝗘𝗗</blockquote></b>\n<blockquote>≡ Tᴀᴘ ᴛʜᴇ ꜰɪᴇʟᴅs ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴅᴇʟᴇᴛᴇ, ᴛʜᴇɴ ᴄʟɪᴄᴋ 'Dᴇʟᴇᴛᴇ'.</blockquote>"
    kb = build_meta_selection_menu([], "clear", user_id)
    await safe_edit_caption(client, user_id, query.message.id, text, kb)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_wiz_start:(\d+)$"))
async def meta_wiz_start_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)

    user_id = query.from_user.id
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        _meta_state[user_id] = {
            "mode": "wizard",
            "selected": [],
            "msg_id": query.message.id,
            "version": str(uuid.uuid4()),
            "set_at": time.monotonic()
        }

    text = "<b><blockquote>✦ 𝗦𝗘𝗧 𝗔𝗟𝗟 𝗪𝗜𝗭𝗔𝗥𝗗</blockquote></b>\n<blockquote>≡ Tᴀᴘ ᴛʜᴇ ꜰɪᴇʟᴅs ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴄᴏɴꜰɪɢᴜʀᴇ, ᴛʜᴇɴ ᴄʟɪᴄᴋ 'Sᴛᴀʀᴛ Wɪᴢᴀʀᴅ'.</blockquote>"
    kb = build_meta_selection_menu([], "wizard", user_id)
    await safe_edit_caption(client, user_id, query.message.id, text, kb)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_toggle:(clear|wizard):([^:]+):(\d+)$"))
async def meta_toggle_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)

    user_id = query.from_user.id
    mode = query.matches[0].group(1)
    field = query.matches[0].group(2)

    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        state = _meta_state.get(user_id)
        if not state or state.get("mode") != mode:
            return await query.answer("ⓘ Sᴛᴀᴛᴇ ᴇxᴘɪʀᴇᴅ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.", show_alert=True)

        selected = state["selected"]
        if field in selected:
            selected.remove(field)
        else:
            selected.append(field)

        kb = build_meta_selection_menu(selected, mode, user_id)
        await safe_edit_reply_markup(client, user_id, query.message.id, kb)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^meta_exec_clear:(\d+)$"))
async def meta_exec_clear_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)

    user_id = query.from_user.id
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()

    async with _user_locks[user_id]:
        state = _meta_state.get(user_id)
        if not state or state.get("mode") != "clear":
            return await query.answer("ⓘ Sᴛᴀᴛᴇ ᴇxᴘɪʀᴇᴅ.", show_alert=True)
        selected = state["selected"]
        if not selected:
            return await query.answer("✗ Nᴏ ꜰɪᴇʟᴅs sᴇʟᴇᴄᴛᴇᴅ !", show_alert=True)
        selected_copy = list(selected)
        _meta_state.pop(user_id, None)

    # save_field acquires the lock itself — must be called OUTSIDE our lock
    try:
        tasks = [MetadataService.save_field(user_id, f, "") for f in selected_copy]
        await asyncio.gather(*tasks)
        await _return_to_edit_panel(client, user_id, query.message.id)
        await query.answer(f"✓ {len(selected_copy)} ꜰɪᴇʟᴅ(s) ᴄʟᴇᴀʀᴇᴅ !", show_alert=True)
    except Exception as e:
        await _return_to_edit_panel(client, user_id, query.message.id)
        await query.answer(f"✗ Fᴀɪʟᴇᴅ : {str(e)[:50]}", show_alert=True)

@Client.on_callback_query(filters.regex(r"^meta_exec_wizard:(\d+)$"))
async def meta_exec_wizard_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)

    user_id = query.from_user.id
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    async with _user_locks[user_id]:
        state = _meta_state.get(user_id)
        if not state or state.get("mode") != "wizard":
            return await query.answer("ⓘ Sᴛᴀᴛᴇ ᴇxᴘɪʀᴇᴅ.", show_alert=True)
        selected = state["selected"]
        if not selected:
            return await query.answer("✗ Nᴏ ꜰɪᴇʟᴅs sᴇʟᴇᴄᴛᴇᴅ !", show_alert=True)

        version = str(uuid.uuid4())
        _meta_state[user_id] = {
            "mode": "wizard_input",
            "fields": selected.copy(),
            "msg_id": query.message.id,
            "version": version,
            "set_at": time.monotonic()
        }
        if user_id in _meta_tasks:
            _meta_tasks[user_id].cancel()
        _meta_tasks[user_id] = asyncio.create_task(
            _timeout_task(client, user_id, query.message.id, version)
        )

    await _prompt_wizard_field(client, user_id, query.message.id, selected)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^close_msg$"))
async def close_msg_cb(client: Client, query: CallbackQuery):
    try:
        await query.message.delete()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════
#  | 𝗚𝗟𝗢𝗕𝗔𝗟 𝗜𝗡𝗣𝗨𝗧 𝗖𝗔𝗧𝗖𝗛𝗘𝗥 (𝗚𝗥𝗢𝗨𝗣 -𝟭𝟬𝟬) |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.text, group=-100)
async def meta_pending_input_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()

    # ── Step 1: Read & clear state atomically under lock ──────────────────────
    # Only fast, non-I/O operations occur here. save_field() is intentionally
    # NOT called inside this block — it acquires the same lock internally,
    # which would deadlock since asyncio.Lock is non-reentrant.
    async with _user_locks[user_id]:
        state = _meta_state.get(user_id)
        if not state:
            return

        mode = state.get("mode")
        if mode not in ("single_input", "wizard_input"):
            return

        # Snapshot needed data, then clear state — all inside the lock.
        field = state.get("field")
        fields = list(state.get("fields", []))
        msg_id = state["msg_id"]
        _meta_state.pop(user_id, None)

        # Cancel timeout task while still holding lock.
        task = _meta_tasks.pop(user_id, None)
        if task:
            task.cancel()

    # ── Step 2: All I/O happens OUTSIDE the lock ──────────────────────────────
    try:
        await message.delete()
    except Exception:
        pass

    text = message.text.strip()

    if not text:
        await _return_to_edit_panel(client, user_id, msg_id)
        asyncio.create_task(_temp_alert(message, "<blockquote>✗ <b>Eᴍᴘᴛʏ ɪɴᴘᴜᴛ sᴋɪᴘᴘᴇᴅ.</blockquote></b>"))
        raise StopPropagation

    if len(text) > 2048:
        await _return_to_edit_panel(client, user_id, msg_id)
        asyncio.create_task(_temp_alert(message, "<blockquote>✗ <b>Iɴᴘᴜᴛ ᴛᴏᴏ ʟᴏɴɢ ! Mᴀx 2048 ᴄʜᴀʀᴀᴄᴛᴇʀs.</blockquote></b>"))
        raise StopPropagation

    try:
        if mode == "single_input":
            # save_field acquires _user_locks[user_id] inside — safe because
            # we released it above before reaching this await.
            await MetadataService.save_field(user_id, field, text)
            await _return_to_edit_panel(client, user_id, msg_id)
            asyncio.create_task(_temp_alert(message, f"<blockquote>✓ <b>{_get_field_name(field)} sᴀᴠᴇᴅ !</blockquote></b>"))

        elif mode == "wizard_input":
            # Each save_field independently acquires/releases the lock — safe.
            save_tasks = [MetadataService.save_field(user_id, f, text) for f in fields]
            await asyncio.gather(*save_tasks)
            await _return_to_edit_panel(client, user_id, msg_id)
            asyncio.create_task(_temp_alert(message, f"<blockquote>✓ <b>Aᴘᴘʟɪᴇᴅ ᴛᴇxᴛ ᴛᴏ {len(fields)} ꜰɪᴇʟᴅ(s) sᴜᴄᴄᴇssꜰᴜʟʟʏ !</blockquote></b>"))

    except Exception as e:
        logger.error(f"[meta_input_handler] Fault: {e}", exc_info=True)
        await _return_to_edit_panel(client, user_id, msg_id)
        asyncio.create_task(_temp_alert(message, "<blockquote>✗ <b>Dᴀᴛᴀʙᴀsᴇ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ.</blockquote></b>"))

    finally:
        raise StopPropagation

__all__ = ["init"]
logger.info("[✓] Metadata Management Module Refactored & Hardened")
