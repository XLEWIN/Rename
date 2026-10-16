# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀᴛɪᴏɴ ᴅᴏɴᴇ                │      @NyxRexy
#│  (ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ)                   │      @ShadowedTomb
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — SHORTENER PANEL ====================

# =========================================================
# 📦 IMPORTS
# =========================================================

import asyncio
from contextlib import suppress
import logging
import re
import time
import random
import string
from dataclasses import dataclass
from functools import wraps
from typing import Optional, Tuple, Any
from datetime import datetime, timedelta
import secrets

import aiohttp
import pytz
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    MessageIdInvalid,
    MessageNotModified,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

try:
    from pyrogram.errors.pyromod.listener_timeout import ListenerTimeout
except ImportError:
    ListenerTimeout = asyncio.TimeoutError

from config import Config
from helper.database import DARKXSIDE78
from plugins.admin import _sudo_filter, close_btn, safe_edit, get_loading_text, retry_on_flood
from plugins.access_control import guard_user

logger = logging.getLogger(__name__)

# =========================================================
# ⚙️ CONSTANTS
# =========================================================

_MAX_SHORTENERS: int = 3
_SLOT_KEYS: list[str] = [str(i) for i in range(1, _MAX_SHORTENERS + 1)]
_SLOT_NAMES: list[str] = ["Pʀɪᴍᴀʀʏ", "Sᴇᴄᴏɴᴅ", "Tʜɪʀᴅ"]

_API_CACHE_TTL: float = 300.0      
_LISTEN_TIMEOUT: int = 60           
_FLOOD_WAIT_CAP: float = 30.0     
_PANEL_PHOTO: str = "https://i.ibb.co/nsjM3qSX/photo-2026-05-05-15-13-39-7636426202743308304.jpg"
_CANCEL_TEXTS: frozenset[str] = frozenset({"cancel", "cᴀɴᴄᴇʟ ✗"})

_TXT_NOT_ADMIN       = "✗ ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀᴅᴍɪɴ !"
_TXT_NOT_ADMIN_SHORT = "✗ Nᴏᴛ ᴀᴅᴍɪɴ !"
_TXT_CANCELLED       = "<b><blockquote>✗ Cᴀɴᴄᴇʟʟᴇᴅ</blockquote></b>"
_TXT_TIMEOUT         = "<b><blockquote>✗ Tɪᴍᴇᴅ ᴏᴜᴛ — ɴᴏ ɪɴᴘᴜᴛ ʀᴇᴄᴇɪᴠᴇᴅ</blockquote></b>"
_TXT_INVALID_URL     = "<b><blockquote>✗ ɪɴᴠᴀʟɪᴅ URL — ᴍᴜsᴛ ʙᴇ https://example.com</blockquote></b>"
_TXT_INVALID_TME     = "<b><blockquote>✗ Mᴜsᴛ ʙᴇ ᴀ ᴠᴀʟɪᴅ https://t.me/... ʟɪɴᴋ</blockquote></b>"
_TXT_INVALID_NUMBER  = "<b><blockquote>✗ ɪɴᴠᴀʟɪᴅ — sᴇɴᴅ ᴀ ᴘᴏsɪᴛɪᴠᴇ ɴᴜᴍʙᴇʀ ɪɴ sᴇᴄᴏɴᴅs</blockquote></b>"
_TXT_NO_WEBSITE      = "<b><blockquote>✗ Sᴇᴛ ᴛʜᴇ ᴡᴇʙsɪᴛᴇ ғɪʀsᴛ ʙᴇғᴏʀᴇ sᴇᴛᴛɪɴɢ ᴛʜᴇ ᴀᴘɪ</blockquote></b>"
_TXT_EMPTY_API       = "<b><blockquote>✗ API ᴋᴇʏ ᴄᴀɴɴᴏᴛ ʙᴇ ᴇᴍᴘᴛʏ</blockquote></b>"
_TXT_API_FAILED      = "<b><blockquote>✗ ᴀᴘɪ ᴠᴀʟɪᴅᴀᴛɪᴏɴ ғᴀɪʟᴇᴅ — ɴᴏᴛ sᴀᴠᴇᴅ</blockquote></b>"
_TXT_ERROR_SETTINGS  = "✗ Eʀʀᴏʀ ʟᴏᴀᴅɪɴɢ sᴇᴛᴛɪɴɢs"
_TXT_ALL_REMOVED     = "✓ Aʟʟ sʜᴏʀᴛᴇɴᴇʀs ʀᴇᴍᴏᴠᴇᴅ"

_ALIAS_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

TOUGH_PREFIXES: list[str] = list(dict.fromkeys([
    # Wuthering Waves
    "Rover_","Jiyan_","Yinlin_","Calcharo_","Encore_","Verina_","Sanhua_","Mortefi_","Danjin_","Lingyang_",
    "Taoqi_","Baizhi_","Chixia_","Yangyang_","Jianxin_","Yuanwu_","Aalto_","Jinhsi_","Changli_","Zhezhi_",
    "XiangliYao_","Camellya_","HavocRover_","SpectroRover_","FusionRover_","Ciaccona_","Zani_","Lupa_",
    "Cervus_","Phrolova_","Lumi_",
    # Genshin Impact
    "Klee_","Qiqi_","Diona_","Sayu_","Nahida_","Keqing_","Ganyu_","HuTao_","YaeMiko_","Furina_","Nilou_",
    "Kokomi_","Yoimiya_","Ayaka_","Kazuha_","Venti_","Xiao_","Zhongli_","RaidenShogun_","Yelan_","Shenhe_",
    "Layla_","Faruzan_","Charlotte_","Freminet_","Lynette_","Navia_","Chevreuse_","Sigewinne_","Clorinde_",
    "Arlecchino_","Chiori_","Emilie_","Mualani_","Kachina_","Kinich_","Citlali_","Mavuika_","Xilonen_",
    "Aether_","Lumine_","Paimon_","Dori_","Candace_","Collei_","Tighnari_","Cyno_","Dehya_","Alhaitham_",
    "Wanderer_","Yaoyao_","Kaveh_","Baizhu_","Kirara_","Lyney_","Neuvillette_","Wriothesley_","Gaming_",
    "Xianyun_","Jean_","Lisa_","Amber_","Barbara_","Noelle_","Sucrose_","Fischl_","Bennett_","Xiangling_",
    "Xingqiu_","Chongyun_","Beidou_","Ningguang_","Gorou_","Itto_","Sara_","YunJin_",
    # Honkai: Star Rail
    "Trailblazer_","March7th_","DanHeng_","Himeko_","Welt_","Kafka_","SilverWolf_","Luocha_","JingYuan_","Blade_",
    "Seele_","Bronya_","Sampo_","Hook_","Natasha_","Serval_","Pela_","Qingque_","Tingyun_","Yanqing_",
    "Bailu_","FuXuan_","Huohuo_","Acheron_","Firefly_","BlackSwan_","Robin_","Boothill_","Feixiao_","Moze_",
    "Jade_","Argenti_","RuanMei_","DrRatio_","Topaz_","Aventurine_","Sparkle_","Gallagher_","Misha_","Sunday_",
    "Jiaoqiu_","Yunli_","Lingsha_","Rappa_",
    # Zenless Zone Zero
    "Wise_","Belle_","Anby_","Nicole_","Billy_","Nekomata_","Corin_","Anton_","Ben_","Grace_",
    "Koleda_","Soldier11_","Ellen_","Lycaon_","Rina_","Piper_","Lucy_","Soukaku_","Burnice_","Caesar_",
    "JaneDoe_","Seth_","Qingyi_","Lighter_","Yanagi_","AstraYao_","Harumasa_",
    # Extra anime/gacha
    "Kanna_","Tohru_","Illya_","Miyu_","Chloe_","RinTohsaka_","SakuraMatou_","Asuka_","ReiAyanami_","Hinata_",
    "Nezuko_","Rem_","Ram_","Emilia_","Megumin_","Aqua_","ZeroTwo_","Taiga_","Rikka_","Ochaco_","Tsuyu_",
    "Momo_","Jiro_","Exusiai_","Amiya_","Ch'en_","Texas_","Lappland_","SilverAsh_","Hoshiguma_","Mudrock_",
    "Arona_","Hoshino_","Shiroko_","Serika_","Nonomi_","Ayane_","Yuuka_","Noa_","Hibiki_","Izuna_",
    "Enterprise_","Belfast_","Akagi_","Kaga_","Ayanami_","Laffey_","Javelin_","Z23_","Atago_","Taihou_",
]))
random.shuffle(TOUGH_PREFIXES)

# =========================================================
# 🧠 DATA MODELS
# =========================================================

@dataclass(slots=True)
class ShortenerSlot:
    website: str | None
    api: str | None
    valid: bool = False

@dataclass(slots=True)
class ShortenerState:
    active: str
    enabled: bool
    mode: str | None
    token_time: int
    bypass: bool
    bypass_time: int
    premium_link: str
    tutorial_link: str
    slots: dict[str, ShortenerSlot]

# =========================================================
# 🧰 CACHE SYSTEM
# =========================================================

class _DBCache:
    def __init__(self):
        self._store = {}
        self._timestamps = {}
    
    def get(self, key: str) -> Tuple[bool, Any]:
        ts = self._timestamps.get(key, 0)
        if time.monotonic() - ts < 300: # 5 mins TTL
            return True, self._store.get(key)
        return False, None
        
    def set(self, key: str, value: Any, ttl: int = 300):
        self._store[key] = value
        self._timestamps[key] = time.monotonic()

    def delete(self, key: str):
        self._store.pop(key, None)
        self._timestamps.pop(key, None)

_db_cache = _DBCache()

# =========================================================
# 🔐 LOCKS & STATE (GLOBAL RUNTIME)
# =========================================================

_user_locks: dict[int, asyncio.Lock] = {}
_lock_last_used: dict[int, float] = {}
_active_listeners: dict[int, asyncio.Task] = {}
_last_click: dict[int, tuple[float, str]] = {}
_last_panel_ids: dict[int, int] = {}
_last_token_msg_time: dict[int, float] = {}
_TOKEN_MSG_COOLDOWN: float = 30.0
_ACTION_COOLDOWN: float = 0.5
_config_lock = asyncio.Lock()

# Production resource tracking
_http_session: Optional[aiohttp.ClientSession] = None
_tasks: list[asyncio.Task] = []
_user_action_time: dict[int, float] = {}

async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        # Use a reasonably sized pool for a bot
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        _http_session = aiohttp.ClientSession(connector=connector)
    return _http_session

# =========================================================
# 🧪 VALIDATION & PARSING
# =========================================================

def parse_duration(text: str) -> Optional[int]:
    """Parse strings like '48h', '2d', '3600' into seconds."""
    text = text.lower().strip()
    if text.isdigit():
        return int(text)
    
    match = re.match(r"^(\d+)\s*([smhd])$", text)
    if not match:
        return None
    
    value, unit = match.groups()
    value = int(value)
    
    multipliers = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }
    return value * multipliers[unit]

async def _is_api_valid_cached(api_key: str, website: str) -> bool:
    """Validate API with robust format support and caching."""
    website = str(website).strip().removeprefix("https://").removeprefix("http://")
    cache_key = f"{website}:{api_key}"
    
    # Check cache
    hit, val = _db_cache.get(f"apicheck_{cache_key}")
    if hit and val is not None:
        return bool(val)

    api_url = f"https://{website}/api"
    result = False
    session = await get_http_session()
    timeout = aiohttp.ClientTimeout(total=10)

    # Production-grade retry (2 attempts)
    for attempt in range(1, 3):
        try:
            async with session.get(
                api_url,
                params={"api": api_key, "url": "https://google.com"},
                timeout=timeout
            ) as resp:
                raw_text = await resp.text()
                logger.info(f"[SHORTENER] [API VALIDATE] {website} attempt={attempt} status={resp.status} body={raw_text[:200]}")
                
                if resp.status == 200:
                    # Try JSON first
                    try:
                        import json
                        data = json.loads(raw_text)
                        result = (
                            data.get("status") == "success"
                            or bool(data.get("shortenedUrl"))
                            or bool(data.get("short_url"))
                            or bool(data.get("shortlink"))
                            or bool(data.get("shortened_url"))
                        )
                    except (json.JSONDecodeError, AttributeError):
                        # Plain text response — if it contains a URL, it's valid
                        result = raw_text.strip().startswith("http")
                        logger.info(f"[SHORTENER] [API VALIDATE] {website} Non-JSON response, result={result}")
                
                # If we got a response (even if invalid result), we stop retrying
                break
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logger.warning(f"[SHORTENER] [API VALIDATE] {website} attempt={attempt} failed: {e}")
            if attempt < 2:
                await asyncio.sleep(1)

    _db_cache.set(f"apicheck_{cache_key}", result, ttl=300)
    return result

# =========================================================
# 🗄️ DATABASE LAYER
# =========================================================

async def _get_shortener_configs() -> dict[str, Any]:
    """Unified fetcher for all shortener settings with atomic caching."""
    KEYS = [
        "active_shortener", "short_enabled", "short_mode", "token_time",
        "bypass_enabled", "bypass_time", "premium_post_link", "tutorial_link",
        "website1", "api1", "website2", "api2", "website3", "api3"
    ]
    
    # Phase 1: Read cache (brief lock, no await)
    result: dict[str, Any] = {}
    missing: list[str] = []
    async with _config_lock:
        for k in KEYS:
            hit, val = _db_cache.get(f"shortener_{k}")
            if hit:
                result[k] = val
            else:
                missing.append(k)
    
    if not missing:
        return result
    
    # Phase 2: Fetch missing from DB (No lock held)
    raw = await asyncio.gather(
        *[DARKXSIDE78.get_variable(k, None) for k in missing],
        return_exceptions=True,
    )
    
    # Phase 3: Write back to cache (brief lock, no await)
    async with _config_lock:
        for k, v in zip(missing, raw):
            value = None if isinstance(v, Exception) else v
            result[k] = value
            _db_cache.set(f"shortener_{k}", value, ttl=300)
    
    return result

async def _load_shortener_state() -> ShortenerState:
    cfg = await _get_shortener_configs()
    
    active = cfg.get("active_shortener", "1")
    short_enabled = cfg.get("short_enabled", False)
    mode = cfg.get("short_mode", "link")
    token_time = cfg.get("token_time", 86400)
    bypass = cfg.get("bypass_enabled", True)
    bypass_time = cfg.get("bypass_time", 60)
    premium_link = cfg.get("premium_post_link", "Not set")
    tutorial_link = cfg.get("tutorial_link", "Not set")

    websites = [cfg.get(f"website{s}") for s in _SLOT_KEYS]
    apis = [cfg.get(f"api{s}") for s in _SLOT_KEYS]

    slots: dict[str, ShortenerSlot] = {
        _SLOT_KEYS[i]: ShortenerSlot(
            website=websites[i],
            api=apis[i],
            valid=bool(websites[i] and apis[i]),
        )
        for i in range(_MAX_SHORTENERS)
    }

    return ShortenerState(
        active=str(active) if active in _SLOT_KEYS else "1",
        enabled=bool(short_enabled),
        mode=mode,
        token_time=int(token_time or 86400),
        bypass=bool(bypass),
        bypass_time=int(bypass_time or 60),
        premium_link=str(premium_link or "Not set"),
        tutorial_link=str(tutorial_link or "Not set"),
        slots=slots,
    )

async def _get_state() -> ShortenerState:
    state = await _load_shortener_state()
    # Normalization logic
    if state.active not in _SLOT_KEYS:
        state.active = "1"
    
    if not state.slots[state.active].valid and state.enabled:
        for slot, data in state.slots.items():
            if data.valid:
                state.active = slot
                await DARKXSIDE78.set_variable("active_shortener", slot)
                _db_cache.delete("shortener_active_shortener")
                break
        else:
            # No valid shorteners, disable
            await DARKXSIDE78.set_variable("short_enabled", False)
            _db_cache.delete("shortener_short_enabled")
            state.enabled = False
    
    return state

# =========================================================
# 🎨 UI BUILDERS
# =========================================================

def _format_token_time(total_seconds: int) -> str:
    if total_seconds <= 0: return "0 seconds"
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if seconds and not (days or hours): parts.append(f"{seconds}s")
    return " ".join(parts) or "0s"

def _build_main_panel_text(state: ShortenerState) -> str:
    time_str = _format_token_time(state.token_time)
    if state.enabled:
        mode_text = f"ᴠᴇʀɪғʏ ({time_str})" if state.mode == "24" else "ᴘᴇʀ ʟɪɴᴋ"
    else:
        mode_text = "✗ ᴅɪsᴀʙʟᴇᴅ"

    active_name = _SLOT_NAMES[int(state.active) - 1]

    lines: list[str] = [
        "<b><blockquote>✦ 𝗦𝗛𝗢𝗥𝗧𝗡𝗘𝗥 𝗖𝗢𝗡𝗧𝗥𝗢𝗟 𝗣𝗔𝗡𝗘𝗟</blockquote></b>",
        f"<blockquote>›› <b>Sᴛᴀᴛᴜs</b> : {'✓ ON' if state.enabled else '✗ OFF'}</blockquote>",
        f"<blockquote>›› <b>Mᴏᴅᴇ</b> : <code>{mode_text}</code>\n›› <b>Vᴀʟɪᴅɪᴛʏ</b> : <code>{time_str}</code></blockquote>",
        f"<blockquote>›› <b>Bʏᴘᴀss Dᴇᴛᴇᴄᴛ</b> : {'✓ ON' if state.bypass else '✗ OFF'} (<code>{state.bypass_time}s</code>)</blockquote>",
        "────────────────────",
        f"<b><blockquote>≡ <b>Aᴄᴛɪᴠᴇ sʟᴏᴛ</b> : {active_name}</blockquote></b>",
        f"<blockquote>›› <b>Pʀᴇᴍɪᴜᴍ ʟɪɴᴋ</b> : <code>{state.premium_link}</code></blockquote>",
        f"<blockquote>›› <b>Tᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ</b> : <code>{state.tutorial_link}</code></blockquote>",
    ]

    for slot in _SLOT_KEYS:
        name = _SLOT_NAMES[int(slot) - 1]
        s = state.slots[slot]
        indicator = "✓" if s.valid else "✗"
        lines.append(
            f"<b><blockquote>• {name} {indicator}</b>\n"
            f"›› <b>Wᴇʙsɪᴛᴇ</b> : <code>{s.website or 'ɴᴏɴᴇ'}</code>\n"
            f"›› <b>Aᴘɪ</b> : <code>{s.api[:6] if s.api else 'ɴᴏɴᴇ'}...</code></blockquote>"
        )

    return "\n".join(lines)

def _build_main_panel_keyboard(state: ShortenerState) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                f"Vᴇʀɪғʏ ᴍᴏᴅᴇ {'✓' if state.enabled and state.mode == '24' else ''}",
                callback_data="short_mode_24",
            ),
            InlineKeyboardButton(
                f"Tᴏᴋᴇɴ ᴍᴏᴅᴇ {'✓' if state.enabled and state.mode == 'link' else ''}",
                callback_data="short_mode_link",
            ),
        ],
        [
            InlineKeyboardButton("Cʜᴀɴɢᴇ ᴡᴇʙsɪᴛᴇ", callback_data="short_edit_web"),
            InlineKeyboardButton("Cʜᴀɴɢᴇ ᴀᴘɪ", callback_data="short_edit_api"),
        ],
        [InlineKeyboardButton("Sᴇᴛ ᴠᴇʀɪғʏ ᴛɪᴍᴇ", callback_data="short_set_time")],
        [InlineKeyboardButton("Sᴇᴛ ᴀᴄᴛɪᴠᴇ sʟᴏᴛ", callback_data="short_set_active")],
        [
            InlineKeyboardButton("Sᴇᴛ ᴘʀᴇᴍɪᴜᴍ ʟɪɴᴋ", callback_data="short_set_prem"),
            InlineKeyboardButton("Sᴇᴛ ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ", callback_data="short_set_tut"),
        ],
        [
            InlineKeyboardButton(f"Bʏᴘᴀss: {'✓' if state.bypass else '✗'}", callback_data="short_toggle_bypass"),
            InlineKeyboardButton("Sᴇᴛ Bʏᴘᴀss Tɪᴍᴇ", callback_data="short_set_bypass_time"),
        ],
        [
            InlineKeyboardButton("Rᴇᴍᴏᴠᴇ Aʟʟ", callback_data="short_reset"),
            InlineKeyboardButton("Cʟᴏsᴇ", callback_data="close_msg")
        ],
    ]
    return InlineKeyboardMarkup(rows)

def _build_slot_select_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        *[
            [InlineKeyboardButton(_SLOT_NAMES[i], callback_data=f"{action}_{i + 1}")]
            for i in range(_MAX_SHORTENERS)
        ],
        [InlineKeyboardButton("Bᴀᴄᴋ", callback_data="shortener_panel")],
    ])

def _build_link_keyboard(has_current: bool, replace_cb: str, remove_cb: str) -> InlineKeyboardMarkup:
    if has_current:
        rows = [
            [
                InlineKeyboardButton("Rᴇᴘʟᴀᴄᴇ", callback_data=replace_cb),
                InlineKeyboardButton("Rᴇᴍᴏᴠᴇ", callback_data=remove_cb),
            ],
            [InlineKeyboardButton("Bᴀᴄᴋ", callback_data="shortener_panel")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("Sᴇᴛ Nᴇᴡ Lɪɴᴋ", callback_data=replace_cb)],
            [InlineKeyboardButton("Bᴀᴄᴋ", callback_data="shortener_panel")],
        ]
    return InlineKeyboardMarkup(rows)

# =========================================================
# 🔧 CORE UTILITIES
# =========================================================

def _make_alias() -> str:
    prefix = random.choice(TOUGH_PREFIXES)
    # Use secrets for cryptographically strong aliases (collision resistant)
    suffix = "".join(secrets.choice(_ALIAS_CHARS) for _ in range(10))
    return f"{prefix}{suffix}"

@retry_on_flood
async def safe_send_message(client: Client, chat_id: int, text: str, **kwargs) -> Message:
    return await client.send_message(chat_id, text, **kwargs)

@retry_on_flood
async def safe_reply_photo(message: Message, photo: str, **kwargs) -> Message:
    return await message.reply_photo(photo, **kwargs)

@retry_on_flood
async def safe_reply_text(message: Message, text: str, **kwargs) -> Message:
    return await message.reply_text(text, **kwargs)

async def _clear_keyboard(client: Client, uid: int):
    with suppress(Exception):
        msg = await safe_send_message(client, uid, ".", reply_markup=ReplyKeyboardRemove())
        await msg.delete()

def get_user_lock(uid: int) -> asyncio.Lock:
    _lock_last_used[uid] = time.monotonic()
    if uid not in _user_locks:
        _user_locks[uid] = asyncio.Lock()
    return _user_locks[uid]

def _is_cancel(text: str) -> bool:
    return text.lower() in _CANCEL_TEXTS

# =========================================================
# 🧠 BUSINESS LOGIC (CORE ENGINE)
# =========================================================

async def get_active_shortener() -> Tuple[Optional[str], Optional[str]]:
    state = await _get_state()
    if not state.enabled:
        return None, None
    slot = state.slots[state.active]
    return slot.website, slot.api

async def get_short_url(long_url: str, alias: Optional[str] = None) -> str:
    if not long_url.startswith("https://"):
        logger.warning("Non-https URL passed to get_short_url — skipping")
        return long_url

    cfg         = await _get_shortener_configs()
    raw_active  = cfg.get("active_shortener") or "1"
    active      = raw_active if str(raw_active) in ("1", "2", "3") else "1"
    order       = [str(active)] + [s for s in ("1", "2", "3") if s != str(active)]
    final_alias = alias or _make_alias()

    async def _try_slot(slot: str) -> Optional[str]:
        api_key = cfg.get(f"api{slot}")
        website = cfg.get(f"website{slot}")

        if not api_key or not website:
            return None

        website = str(website).strip().removeprefix("https://").removeprefix("http://")
        api_url = f"https://{website}/api"
        session = await get_http_session()
        timeout = aiohttp.ClientTimeout(total=10)

        # Retry loop for resilience
        for attempt in range(1, 3):
            try:
                async with session.get(
                    api_url,
                    params={"api": api_key, "url": long_url, "alias": final_alias},
                    timeout=timeout
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"[SHORTENER] Slot {slot} ({website}) HTTP {resp.status} on attempt {attempt}")
                        if attempt < 2:
                            await asyncio.sleep(1)
                            continue
                        return None
                    
                    data  = await resp.json(content_type=None)
                    short = data.get("shortenedUrl", "") if data.get("status") == "success" else ""
                    if short:
                        logger.info(f"[SHORTENER] Slot {slot} ({website}) SUCCESS → {short}")
                        return str(short)
                    
                    # If we got a 200 but no short link, the API key might be invalid for this URL
                    break
            except Exception as e:
                logger.warning(f"[SHORTENER] Slot {slot} ({website}) error on attempt {attempt}: {e}")
                if attempt < 2:
                    await asyncio.sleep(1)
        return None

    tasks = [asyncio.create_task(_try_slot(slot)) for slot in order]
    while tasks:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            res = t.result()
            if res:
                for p in pending:
                    p.cancel()
                return res
        tasks = list(pending)

    logger.warning("All shortener slots failed — returning original URL")
    return long_url

async def create_token_for_user(user_id: int) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[int]]:
    website, api = await get_active_shortener()
    if not website or not api:
        return None, None, None, None

    token_id_len = await DARKXSIDE78.get_variable("token_id_length", 8)
    token_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=token_id_len))
    deep_link = f"https://t.me/{Config.BOT_USERNAME}?start={token_id}"
    short_url = await get_short_url(deep_link)

    if not short_url:
        return None, None, None, None

    _tt = await DARKXSIDE78.get_variable("token_time", 86400)
    token_time = int(_tt) if _tt is not None else 86400
    hours = token_time // 3600

    await DARKXSIDE78.token_links.update_one(
        {"_id": token_id},
        {"$set": {
            "user_id": user_id,
            "tokens": Config.SHORTENER_TOKEN_GEN,
            "expiry": datetime.now() + timedelta(seconds=token_time),
            "created_at": datetime.now(),
            "used": False
        }},
        upsert=True
    )
    return short_url, hours, token_id, token_time

# =========================================================
# 🎧 LISTENER SYSTEM
# =========================================================

async def _listen(client: Client, uid: int, prompt: Message, timeout: int = _LISTEN_TIMEOUT) -> Message | None:
    """Robust listener using pyromod's native client.listen dispatch."""
    task = asyncio.create_task(client.listen(chat_id=uid, timeout=timeout))
    _active_listeners[uid] = task
    try:
        msg: Message = await task
        with suppress(Exception): await prompt.delete()
        text = (msg.text or msg.caption or "").strip()
        if _is_cancel(text):
            await _clear_keyboard(client, uid)
            await safe_send_message(client, uid, _TXT_CANCELLED)
            return None
        if not text:
             await safe_send_message(client, uid, "<b><blockquote>✗ Pʟᴇᴀsᴇ sᴇɴᴅ ᴀ ᴛᴇxᴛ ᴍᴇssᴀɢᴇ</blockquote></b>", reply_markup=ReplyKeyboardRemove())
             return None
        return msg
    except (ListenerTimeout, asyncio.TimeoutError):
        with suppress(Exception): await prompt.delete()
        await _clear_keyboard(client, uid)
        await safe_send_message(client, uid, _TXT_TIMEOUT)
        return None
    except asyncio.CancelledError:
        with suppress(Exception): await prompt.delete()
        raise
    except Exception as e:
        logger.error(f"Listener error for {uid}: {e}")
        with suppress(Exception): await prompt.delete()
        return None
    finally:
        _active_listeners.pop(uid, None)

def safe_handler(func):
    @wraps(func)
    async def wrapper(client, update):
        uid = update.from_user.id if update.from_user else 0
        if not uid: return

        # Per-user action rate limiting
        now = time.monotonic()
        last_action = _user_action_time.get(uid, 0.0)
        if now - last_action < _ACTION_COOLDOWN:
            if isinstance(update, CallbackQuery):
                await update.answer("⚠️ Relax... slow down!", show_alert=False)
            return
        _user_action_time[uid] = now
        
        if isinstance(update, CallbackQuery):
            last_time, last_data = _last_click.get(uid, (0.0, ""))
            if now - last_time < 0.5 and last_data == update.data:
                return
            _last_click[uid] = (now, update.data)

        # Cancel any active listener task for this user
        listener_task = _active_listeners.get(uid)
        if listener_task and not listener_task.done():
            listener_task.cancel()

        async with get_user_lock(uid):
            try:
                await func(client, update)
            except asyncio.CancelledError:
                pass 
            except Exception as exc:
                logger.exception("[SHORTENER] Handler %s failed for %s", func.__name__, uid)
                if isinstance(update, CallbackQuery):
                    with suppress(Exception):
                        await update.answer("❌ Something went wrong. Please try again.", show_alert=True)
                elif isinstance(update, Message):
                    with suppress(Exception):
                        await update.reply("❌ **An internal error occurred while processing this request.**")
    return wrapper

# =========================================================
# 🎮 HANDLERS — PANEL
# =========================================================

async def _refresh_panel(query: CallbackQuery) -> None:
    """Internal panel refresh — NO lock, safe to call from inside a safe_handler."""
    state = await _get_state()
    uid = query.from_user.id
    
    # Store ID to ensure /shortener command can reuse it
    _last_panel_ids[uid] = query.message.id

    await safe_edit(
        query.message,
        text=_build_main_panel_text(state),
        reply_markup=_build_main_panel_keyboard(state),
        parse_mode=ParseMode.HTML
    )

@Client.on_message(filters.command("shortener") & filters.private & _sudo_filter())
@safe_handler
async def cmd_shortener_panel(client: Client, message: Message) -> None:
    uid = message.from_user.id
    state = await _get_state()
    
    # Try to reuse existing panel if it exists
    last_id = _last_panel_ids.get(uid)
    if last_id:
        try:
            await client.edit_message_caption(
                chat_id=uid,
                message_id=last_id,
                caption=_build_main_panel_text(state),
                reply_markup=_build_main_panel_keyboard(state),
                parse_mode=ParseMode.HTML
            )
            # If successful, delete the command message to keep chat clean
            try: await message.delete()
            except: pass
            return
        except Exception:
            # If edit fails (message deleted or too old), send new one
            _last_panel_ids.pop(uid, None)

    msg = await safe_reply_photo(
        message,
        photo=_PANEL_PHOTO,
        caption=_build_main_panel_text(state),
        reply_markup=_build_main_panel_keyboard(state),
        parse_mode=ParseMode.HTML
    )
    if msg:
        _last_panel_ids[uid] = msg.id

@Client.on_callback_query(filters.regex(r"^shortener_panel$") & _sudo_filter())
@safe_handler
async def cb_shortener_panel(client: Client, query: CallbackQuery) -> None:
    await _refresh_panel(query)

# =========================================================
# 🎮 HANDLERS — SETTINGS
# =========================================================

@Client.on_callback_query(filters.regex(r"^short_mode_(24|link)$") & _sudo_filter())
@safe_handler
async def cb_mode_toggle(client: Client, query: CallbackQuery) -> None:
    requested = query.data.split("_")[-1]
    state = await _get_state()
    
    # Toggle off if same mode is clicked while enabled
    if state.enabled and state.mode == requested:
        await asyncio.gather(
            DARKXSIDE78.set_variable("short_enabled", False),
            DARKXSIDE78.set_variable("short_mode", None),
        )
        _db_cache.delete("shortener_short_enabled")
        _db_cache.delete("shortener_short_mode")
        mode_name = "Verify" if requested == "24" else "Per Link"
        await query.answer(f"{mode_name} ᴍᴏᴅᴇ ᴅɪsᴀʙʟᴇᴅ ✗", show_alert=True)
    else:
        # Check if active slot is valid
        if not state.slots[state.active].valid:
            return await query.answer("✗ ᴀᴄᴛɪᴠᴇ sʟᴏᴛ ʜᴀs ɴᴏ ᴠᴀʟɪᴅ ᴡᴇʙsɪᴛᴇ/ᴀᴘɪ !", show_alert=True)
            
        await asyncio.gather(
            DARKXSIDE78.set_variable("short_enabled", True),
            DARKXSIDE78.set_variable("short_mode", requested),
        )
        _db_cache.delete("shortener_short_enabled")
        _db_cache.delete("shortener_short_mode")
        mode_name = "Verify" if requested == "24" else "Per Link"
        await query.answer(f"{mode_name} ᴍᴏᴅᴇ ᴇɴᴀʙʟᴇᴅ ✓", show_alert=True)
    
    await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_active$") & _sudo_filter())
@safe_handler
async def cb_set_active_select(client: Client, query: CallbackQuery) -> None:
    await safe_edit(
        query.message,
        text="<blockquote><b>≡ Sᴇʟᴇᴄᴛ ᴛʜᴇ sʟᴏᴛ ᴛᴏ sᴇᴛ ᴀs ᴀᴄᴛɪᴠᴇ :</b></blockquote>",
        reply_markup=_build_slot_select_keyboard("short_act"),
        parse_mode=ParseMode.HTML
    )

@Client.on_callback_query(filters.regex(r"^short_act_(\d+)$") & _sudo_filter())
@safe_handler
async def cb_set_active_confirm(client: Client, query: CallbackQuery) -> None:
    slot = query.data.split("_")[-1]
    await DARKXSIDE78.set_variable("active_shortener", slot)
    _db_cache.delete("shortener_active_shortener")
    await query.answer(f"✓ Slot {slot} set as active", show_alert=True)
    await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_edit_(web|api)$") & _sudo_filter())
@safe_handler
async def cb_edit_field_select(client: Client, query: CallbackQuery) -> None:
    field = query.data.split("_")[-1]
    await safe_edit(
        query.message,
        text=f"<b>Sᴇʟᴇᴄᴛ sʟᴏᴛ ᴛᴏ ᴜᴘᴅᴀᴛᴇ {field.upper()}:</b>",
        reply_markup=_build_slot_select_keyboard(f"short_upd_{field}"),
        parse_mode=ParseMode.HTML
    )

@Client.on_callback_query(filters.regex(r"^short_upd_(web|api)_(\d+)$") & _sudo_filter())
@safe_handler
async def cb_edit_field_input(client: Client, query: CallbackQuery) -> None:
    parts = query.data.split("_")
    field, slot = parts[2], parts[3]
    uid = query.from_user.id
    
    label = "Wᴇʙsɪᴛᴇ URL (e.g. shareus.io)" if field == "web" else "API Kᴇʏ"
    prompt = await safe_send_message(
        client,
        uid,
        f"<b><blockquote>Sᴇɴᴅ ɴᴇᴡ {label} ғᴏʀ sʟᴏᴛ {slot}</blockquote></b>\n"
        f"<blockquote>Tʏᴘᴇ <code>cancel</code> ᴛᴏ ᴀʙᴏʀᴛ.</blockquote>",
        reply_markup=ReplyKeyboardMarkup([["Cᴀɴᴄᴇʟ ✗"]], resize_keyboard=True)
    )

    try:
        user_msg = await _listen(client, uid, prompt)
        if user_msg is None: return

        text = user_msg.text.strip()
        if not text:
            await safe_reply_text(user_msg, _TXT_EMPTY_API)
            return

        if field == "web":
            clean = text.removeprefix("https://").removeprefix("http://").split("/")[0]
            try:
                await DARKXSIDE78.set_variable(f"website{slot}", clean)
                _db_cache.delete(f"shortener_website{slot}")
                
                # Verify save
                saved = await DARKXSIDE78.get_variable(f"website{slot}")
                if saved != clean:
                    raise Exception("DB Verification Failed — value mismatch after save")
                
                logger.info(f"[SAVE] Slot {slot} Website updated to {clean}")
            except Exception as save_err:
                logger.error(f"[FAIL SAVE] Slot {slot} Website: {save_err}")
                await safe_reply_text(user_msg, f"✗ Sᴀᴠᴇ ғᴀɪʟᴇᴅ: {save_err}")
                await _clear_keyboard(client, uid)
                return
            await _clear_keyboard(client, uid)
            await safe_reply_text(user_msg, f"<b><blockquote>✓ ᴡᴇʙsɪᴛᴇ sʟᴏᴛ {slot} ᴜᴘᴅᴀᴛᴇᴅ.</b></blockquote>")
        else:
            website = await DARKXSIDE78.get_variable(f"website{slot}")
            if not website:
                await _clear_keyboard(client, uid)
                await safe_reply_text(user_msg, _TXT_NO_WEBSITE)
            else:
                await _clear_keyboard(client, uid)
                try:
                    await DARKXSIDE78.set_variable(f"api{slot}", text)
                    _db_cache.delete(f"shortener_api{slot}")
                    
                    # Verify save
                    saved = await DARKXSIDE78.get_variable(f"api{slot}")
                    if saved != text:
                        raise Exception("DB Verification Failed — value mismatch after save")
                        
                    logger.info(f"[SAVE] Slot {slot} API updated")
                except Exception as save_err:
                    logger.error(f"[FAIL SAVE] Slot {slot} API: {save_err}")
                    await safe_reply_text(user_msg, f"✗ Sᴀᴠᴇ ғᴀɪʟᴇᴅ: {save_err}")
                    return
                msg_wait = await safe_send_message(client, uid, "<b><blockquote>⧗ ᴀᴘɪ sᴀᴠᴇᴅ ᴀɴᴅ ᴠᴇʀɪғɪᴇᴅ. ᴠᴀʟɪᴅᴀᴛɪɴɢ ɪɴ ʙᴀᴄᴋɢʀᴏᴜɴᴅ...</b></blockquote>")
                
                async def validate_bg():
                    await asyncio.sleep(2) # Let the initial refresh complete first
                    if await _is_api_valid_cached(text, website):
                        await safe_edit(msg_wait, f"<b><blockquote>✓ ᴀᴘɪ sʟᴏᴛ {slot} ᴠᴀʟɪᴅᴀᴛᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ.</b></blockquote>")
                    else:
                        # Race condition check: only delete if the value hasn't changed since validation started
                        current_api = await DARKXSIDE78.get_variable(f"api{slot}")
                        if current_api == text:
                            await safe_edit(msg_wait, _TXT_API_FAILED + " (Removing invalid API...)")
                            await DARKXSIDE78.set_variable(f"api{slot}", None)
                            _db_cache.delete(f"shortener_api{slot}")
                            await _refresh_panel(query)
                        else:
                            await safe_edit(msg_wait, "⚠️ Validation finished for previous API, but it was already replaced. Cleanup skipped.")
                
                asyncio.create_task(validate_bg())
    except Exception as exc:
        logger.error(f"cb_edit_field_input error: {exc}")
    finally:
        await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_time$") & _sudo_filter())
@safe_handler
async def cb_set_token_time(client: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    prompt = await safe_send_message(
        client,
        uid,
        "<b><blockquote>≡ Sᴇɴᴅ ɴᴇᴡ Tᴏᴋᴇɴ Vᴀʟɪᴅɪᴛʏ ɪɴ sᴇᴄᴏɴᴅs :</b></blockquote>\n<blockquote>◍ (86400 = 24h, 3600 = 1h)</blockquote>",
        reply_markup=ReplyKeyboardMarkup([["Cᴀɴᴄᴇʟ ✗"]], resize_keyboard=True)
    )
    try:
        user_msg = await _listen(client, uid, prompt)
        if user_msg is None: return

        text = user_msg.text.strip()
        seconds = parse_duration(text)
        if seconds is None or seconds <= 0:
            await _clear_keyboard(client, uid)
            await safe_reply_text(user_msg, _TXT_INVALID_NUMBER + " (e.g. 48h, 2d, 3600)")
            return

        try:
            await DARKXSIDE78.set_variable("token_time", seconds)
            _db_cache.delete("shortener_token_time")
            
            # Verify
            saved = await DARKXSIDE78.get_variable("token_time")
            if int(saved or 0) != seconds:
                raise Exception("DB Verification Failed")
                
            logger.info(f"[SAVE] Token time updated to {seconds}s")
        except Exception as save_err:
            logger.error(f"[FAIL SAVE] Token time: {save_err}")
            await safe_reply_text(user_msg, f"✗ Sᴀᴠᴇ ғᴀɪʟᴇᴅ: {save_err}")
            await _clear_keyboard(client, uid)
            return

        time_display = _format_token_time(seconds)
        await _clear_keyboard(client, uid)
        await safe_reply_text(user_msg, f"<b><blockquote>✓ ᴛᴏᴋᴇɴ ᴠᴀʟɪᴅɪᴛʏ sᴇᴛ ᴛᴏ {time_display}.</b></blockquote>")
    except Exception as exc:
        logger.error(f"cb_set_token_time error: {exc}")
    finally:
        await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_toggle_bypass$") & _sudo_filter())
@safe_handler
async def cb_toggle_bypass(client: Client, query: CallbackQuery) -> None:
    state = await _get_state()
    new_val = not state.bypass
    await DARKXSIDE78.set_variable("bypass_enabled", new_val)
    _db_cache.delete("shortener_bypass_enabled")
    await query.answer(f"Bypass Detection {'ᴇɴᴀʙʟᴇᴅ ✓' if new_val else 'ᴅɪsᴀʙʟᴇᴅ ✗'}", show_alert=True)
    await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_bypass_time$") & _sudo_filter())
@safe_handler
async def cb_set_bypass_time(client: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    prompt = await safe_send_message(
        client,
        uid,
        "<b><blockquote>≡ Sᴇɴᴅ ɴᴇᴡ Bʏᴘᴀss Tʜʀᴇsʜᴏʟᴅ ɪɴ sᴇᴄᴏɴᴅs :</b></blockquote>\n<blockquote>◍ (e.g. 60 for 1 minute)</blockquote>",
        reply_markup=ReplyKeyboardMarkup([["Cᴀɴᴄᴇʟ ✗"]], resize_keyboard=True)
    )
    try:
        user_msg = await _listen(client, uid, prompt)
        if user_msg is None: return

        text = user_msg.text.strip()
        if not text.isdigit() or int(text) <= 0:
            await _clear_keyboard(client, uid)
            await safe_reply_text(user_msg, _TXT_INVALID_NUMBER)
            return

        await DARKXSIDE78.set_variable("bypass_time", int(text))
        _db_cache.delete("shortener_bypass_time")
        await _clear_keyboard(client, uid)
        await safe_reply_text(user_msg, f"<b><blockquote>✓ ʙʏᴘᴀss ᴛʜʀᴇsʜᴏʟᴅ sᴇᴛ ᴛᴏ {text}s.</b></blockquote>")
    except Exception as exc:
        logger.error(f"cb_set_bypass_time error: {exc}")
    finally:
        await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_prem$") & _sudo_filter())
@safe_handler
async def cb_set_premium_link(client: Client, query: CallbackQuery) -> None:
    state = await _get_state()
    has_current = state.premium_link and state.premium_link != "Not set"
    await safe_edit(
        query.message,
        text=f"<b><blockquote>≡ Pʀᴇᴍɪᴜᴍ Lɪɴᴋ Mᴀɴᴀɢᴇᴍᴇɴᴛ</b></blockquote>\n<blockquote>◍ Cᴜʀʀᴇɴᴛ : <code>{state.premium_link}</code></blockquote>",
        reply_markup=_build_link_keyboard(has_current, "short_set_prem_input", "short_rem_prem"),
        parse_mode=ParseMode.HTML
    )

@Client.on_callback_query(filters.regex(r"^short_rem_prem$") & _sudo_filter())
@safe_handler
async def cb_remove_premium_link(client: Client, query: CallbackQuery) -> None:
    await DARKXSIDE78.set_variable("premium_post_link", None)
    _db_cache.delete("shortener_premium_post_link")
    await query.answer("✓ ᴘʀᴇᴍɪᴜᴍ ʟɪɴᴋ ʀᴇᴍᴏᴠᴇᴅ", show_alert=True)
    await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_prem_input$") & _sudo_filter())
@safe_handler
async def cb_set_premium_input(client: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    prompt = await safe_send_message(
        client, uid, "<b><blockquote>≡ Sᴇɴᴅ ᴛʜᴇ ᴘʀᴇᴍɪᴜᴍ ʙᴜʏ ᴘᴏsᴛ ʟɪɴᴋ (https://t.me/...) :</b></blockquote>",
        reply_markup=ReplyKeyboardMarkup([["Cᴀɴᴄᴇʟ ✗"]], resize_keyboard=True)
    )
    try:
        user_msg = await _listen(client, uid, prompt)
        if user_msg is None: return

        text = user_msg.text.strip()
        await DARKXSIDE78.set_variable("premium_post_link", text)
        _db_cache.delete("shortener_premium_post_link")
        await _clear_keyboard(client, uid)
        await safe_reply_text(user_msg, "<b><blockquote>✓ ᴘʀᴇᴍɪᴜᴍ ʟɪɴᴋ ᴜᴘᴅᴀᴛᴇᴅ.</b></blockquote>")
    except Exception as exc:
        logger.error(f"cb_set_premium_input error: {exc}")
    finally:
        await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_tut$") & _sudo_filter())
@safe_handler
async def cb_set_tutorial_link(client: Client, query: CallbackQuery) -> None:
    state = await _get_state()
    has_current = state.tutorial_link and state.tutorial_link != "Not set"
    await safe_edit(
        query.message,
        text=f"<b><blockquote>≡ Tᴜᴛᴏʀɪᴀʟ Lɪɴᴋ Mᴀɴᴀɢᴇᴍᴇɴᴛ</b></blockquote>\n<blockquote>◍ Cᴜʀʀᴇɴᴛ : <code>{state.tutorial_link}</code></blockquote>",
        reply_markup=_build_link_keyboard(has_current, "short_set_tut_input", "short_rem_tut"),
        parse_mode=ParseMode.HTML
    )

@Client.on_callback_query(filters.regex(r"^short_rem_tut$") & _sudo_filter())
@safe_handler
async def cb_remove_tutorial_link(client: Client, query: CallbackQuery) -> None:
    await DARKXSIDE78.set_variable("tutorial_link", None)
    _db_cache.delete("shortener_tutorial_link")
    await query.answer("✓ ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ ʀᴇᴍᴏᴠᴇᴅ", show_alert=True)
    await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_set_tut_input$") & _sudo_filter())
@safe_handler
async def cb_set_tutorial_input(client: Client, query: CallbackQuery) -> None:
    uid = query.from_user.id
    prompt = await safe_send_message(
        client, uid, "<b><blockquote>≡ Sᴇɴᴅ ᴛʜᴇ ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ (https://t.me/...) :</b></blockquote>",
        reply_markup=ReplyKeyboardMarkup([["Cᴀɴᴄᴇʟ ✗"]], resize_keyboard=True)
    )
    try:
        user_msg = await _listen(client, uid, prompt)
        if user_msg is None: return

        text = user_msg.text.strip()
        await DARKXSIDE78.set_variable("tutorial_link", text)
        _db_cache.delete("shortener_tutorial_link")
        await _clear_keyboard(client, uid)
        await safe_reply_text(user_msg, "<b><blockquote>✓ ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ ᴜᴘᴅᴀᴛᴇᴅ.</b></blockquote>")
    except Exception as exc:
        logger.error(f"cb_set_tutorial_input error: {exc}")
    finally:
        await _refresh_panel(query)

@Client.on_callback_query(filters.regex(r"^short_reset$") & _sudo_filter())
@safe_handler
async def cb_reset_all(client: Client, query: CallbackQuery) -> None:
    for s in _SLOT_KEYS:
        await DARKXSIDE78.set_variable(f"website{s}", None)
        await DARKXSIDE78.set_variable(f"api{s}", None)
        _db_cache.delete(f"shortener_website{s}")
        _db_cache.delete(f"shortener_api{s}")
    await DARKXSIDE78.set_variable("short_enabled", False)
    _db_cache.delete("shortener_short_enabled")
    await query.answer("✓ All shortener data reset", show_alert=True)
    await _refresh_panel(query)

# =========================================================
# 🎮 HANDLERS — TOKEN SYSTEM
# =========================================================

@Client.on_message(filters.command("gentoken") & filters.private)
@guard_user()
async def generate_token(client: Client, message: Message):
    await send_token_message(client, message.from_user.id, message_to_reply=message)

@Client.on_callback_query(filters.regex(r"^gentoken$"))
async def cb_gentoken(client: Client, query: CallbackQuery):
    await send_token_message(client, query.from_user.id, query=query)

async def send_token_message(client: Client, user_id: int, message_to_reply: Message = None, query: CallbackQuery = None):
    """Unified function to send the verification UI. Handles anti-spam."""
    now = time.monotonic()
    
    # 1. Aggressive cooldown check
    last_sent = _last_token_msg_time.get(user_id, 0.0)
    if now - last_sent < _TOKEN_MSG_COOLDOWN:
        return # Silently ignore spam

    # 2. Set timestamp IMMEDIATELY to block concurrent calls
    _last_token_msg_time[user_id] = now

    # 3. Use per-user lock to serialize link generation
    async with get_user_lock(user_id):
        short_url, hours, _, _ = await create_token_for_user(user_id)
    if not short_url:
        err = "<b><blockquote>✗ Tʜɪs sᴇʀᴠɪᴄᴇ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴅɪsᴀʙʟᴇᴅ ʙʏ ᴀᴅᴍɪɴs.</blockquote></b>"
        if query: await query.answer(err.replace("<b>","").replace("</b>",""), show_alert=True)
        elif message_to_reply: await safe_reply_text(message_to_reply, err)
        return

    short_mode = await DARKXSIDE78.get_variable("short_mode", "link")
    if short_mode == "24":
        desc_text = f"ᴘᴀssɪɴɢ ᴏɴᴇ ᴀᴅ ᴡɪʟʟ ᴠᴇʀɪғʏ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ ғᴏʀ <b>{hours} ʜᴏᴜʀs</b>, ᴀʟʟᴏᴡɪɴɢ ʏᴏᴜ ᴛᴏ ᴜsᴇ ᴛʜᴇ ʙᴏᴛ ᴡɪᴛʜᴏᴜᴛ ʟɪᴍɪᴛs."
        reward_text = f"{hours} Hᴏᴜʀs Vᴇʀɪғɪᴄᴀᴛɪᴏɴ"
    else:
        tokens_amt = Config.SHORTENER_TOKEN_GEN
        desc_text = f"ᴘᴀssɪɴɢ ᴏɴᴇ ᴀᴅ ᴡɪʟʟ ɢɪᴠᴇ ʏᴏᴜ <b>{tokens_amt} ғʀᴇᴇ ᴛᴏᴋᴇɴs</b> ᴛʜᴀᴛ ʏᴏᴜ ᴄᴀɴ ᴜsᴇ ᴛᴏ ʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ғɪʟᴇs ɪɴ ᴛʜᴇ ʙᴏᴛ."
        reward_text = f"{tokens_amt} Tᴏᴋᴇɴs"

    text = (
        "<b><blockquote>✗ Yᴏᴜ ɴᴇᴇᴅ ᴛᴏ ᴠᴇʀɪғʏ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ ᴛᴏ ᴜsᴇ ᴛʜᴇ ʙᴏᴛ!</blockquote></b>\n\n"
        "<blockquote>✦ 𝗚𝗘𝗡𝗘𝗥𝗔𝗧𝗘 𝗧𝗢𝗞𝗘𝗡 - ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ</blockquote>\n"
        "<blockquote><b>›› ᴡʜᴀᴛ ɪs ᴛʜᴇ ᴛᴏᴋᴇɴ !?</b></blockquote>\n\n"
        f"<blockquote>◍ ᴛʜɪs ɪs ᴀɴ ᴀᴅ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ᴛᴏᴋᴇɴ. {desc_text}</blockquote>\n\n"
        f"<blockquote>➥ <b>Rᴇᴡᴀʀᴅ</b> : {reward_text}\n"
        f"➥ Lɪɴᴋ ᴠᴀʟɪᴅ ғᴏʀ {hours} ʜᴏᴜʀs | Oɴᴇ-ᴛɪᴍᴇ ᴜsᴇ ᴏɴʟʏ</blockquote>\n"
        "<blockquote>➤ Tᴏ Cʟᴀɪᴍ : Cʟɪᴄᴋ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴀɴᴅ ᴄᴏᴍᴘʟᴇᴛᴇ ᴛʜᴇ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ.</blockquote>"
    )
    state = await _get_state()
    
    # Build dynamic keyboard
    btns = [[InlineKeyboardButton("ᴠᴇʀɪғʏ ɴᴏᴡ", url=short_url)]]
    
    # Row for Tutorial (if set)
    if state.tutorial_link and state.tutorial_link != "Not set":
        btns[0].append(InlineKeyboardButton("ᴛᴜᴛᴏʀɪᴀʟ", url=state.tutorial_link))
    
    # Row for Premium (if set)
    if state.premium_link and state.premium_link != "Not set":
        btns.append([InlineKeyboardButton("ʙᴜʏ ᴘʀᴇᴍɪᴜᴍ", url=state.premium_link)])

    markup = InlineKeyboardMarkup(btns)

    if query:
        await safe_edit(query.message, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    elif message_to_reply:
        if hasattr(message_to_reply, "edit_text") and message_to_reply.from_user and message_to_reply.from_user.id == client.me.id:
             await safe_edit(message_to_reply, text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
             await safe_reply_text(message_to_reply, text, reply_markup=markup, parse_mode=ParseMode.HTML, protect_content=True)
    else:
        await client.send_message(user_id, text, reply_markup=markup, parse_mode=ParseMode.HTML, protect_content=True)

async def handle_token_redemption(client: Client, message: Message, token_id: str):
    user_id = message.from_user.id
    try:
        token_data = await DARKXSIDE78.token_links.find_one({"_id": token_id})

        if not token_data:
            return await safe_reply_text(message, "<blockquote>Iɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ᴛᴏᴋᴇɴ ʟɪɴᴋ...</blockquote>")

        if token_data.get('used'):
            return await safe_reply_text(message, "<blockquote>Tʜɪs ʟɪɴᴋ ʜᴀs ᴀʟʀᴇᴀᴅʏ ʙᴇᴇɴ ᴜsᴇᴅ...</blockquote>")

        expiry = token_data['expiry']
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry)

        if datetime.now() > expiry:
            return await safe_reply_text(message, "<blockquote>Tᴏᴋᴇɴ ᴇxᴘɪʀᴇᴅ...</blockquote>")

        if token_data['user_id'] != user_id:
            return await safe_reply_text(message, "<blockquote>Tʜɪs ᴛᴏᴋᴇɴ ʟɪɴᴋ ʙᴇʟᴏɴɢs ᴛᴏ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ...</blockquote>")

        # Bypass detection
        state = await _get_state()
        if state.bypass:
            created_at = token_data.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at)
                diff = datetime.now() - created_at
                if timedelta(0) <= diff < timedelta(seconds=state.bypass_time):
                    logger.warning("Bypass detected — completed in %.1fs", diff.total_seconds())
                    try:
                        await safe_reply_text(
                            message,
                            f"<b><blockquote>‼️ Bʏᴘᴀss Dᴇᴛᴇᴄᴛᴇᴅ</b></blockquote>\n"
                            f"<blockquote><i>≡ Cᴏᴍᴘʟᴇᴛᴇᴅ ɪɴ <b>{int(diff.total_seconds())}s</b> ᴘʟᴇᴀsᴇ ᴠᴇʀɪғʏ ᴘʀᴏᴘᴇʀʟʏ.</i></blockquote>", 
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception as e:
                        logger.debug("Ignored exception: %s", e)

        # Atomically mark as used
        res = await DARKXSIDE78.token_links.update_one(
            {"_id": token_id, "used": False},
            {"$set": {"used": True, "used_at": datetime.now()}}
        )

        if res.modified_count > 0:
            short_mode = await DARKXSIDE78.get_variable("short_mode", "link")
            if short_mode == "24":
                _tt = await DARKXSIDE78.get_variable("token_time", 86400)
                token_time = int(_tt) if _tt is not None else 86400
                verify_expiry = datetime.now(pytz.utc) + timedelta(seconds=token_time)
                await DARKXSIDE78.col.update_one({"_id": user_id}, {"$set": {"verify_expiry": verify_expiry}})
                hours = token_time // 3600
                await safe_reply_text(message, f"<blockquote><b>✓ Sᴜᴄᴄᴇss! Yᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ ɪs ɴᴏᴡ ᴠᴇʀɪғɪᴇᴅ ғᴏʀ {hours} ʜᴏᴜʀs.</b></blockquote>")
            else:
                tokens = token_data.get('tokens', Config.SHORTENER_TOKEN_GEN)
                await DARKXSIDE78.col.update_one({"_id": user_id}, {"$inc": {"token": tokens}})
                await safe_reply_text(message, f"<blockquote><b>✓ Sᴜᴄᴄᴇss! {tokens} ᴛᴏᴋᴇɴs ᴀᴅᴅᴇᴅ ᴛᴏ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ.</b></blockquote>")
        else:
            await safe_reply_text(message, "<blockquote>✗ Fᴀɪʟᴇᴅ ᴛᴏ ʀᴇᴅᴇᴇᴍ ᴛᴏᴋᴇɴ. Iᴛ ᴍɪɢʜᴛ ʜᴀᴠᴇ ʙᴇᴇɴ ᴜsᴇᴅ ᴀʟʀᴇᴀᴅʏ.</blockquote>")

    except Exception as e:
        logger.error(f"Error during token redemption: {e}")
        await safe_reply_text(message, "<blockquote>Aɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇssɪɴɢ ʏᴏᴜʀ ʀᴇǫᴜᴇsᴛ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.</blockquote>")

# =========================================================
# 🧹 BACKGROUND TASKS
# =========================================================

async def _cleanup_locks():
    while True:
        try:
            await asyncio.sleep(600)
            now = time.monotonic()
            to_remove = [
                k for k, v in _lock_last_used.items()
                if now - v > 600 and k in _user_locks and not _user_locks[k].locked()
            ]
            for k in to_remove:
                _user_locks.pop(k, None)
                _lock_last_used.pop(k, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lock cleanup error: {e}")

# =========================================================
# 🚀 INIT
# =========================================================

async def init(*args, **kwargs):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    
    # Store tasks for potential graceful shutdown
    _tasks.append(loop.create_task(_cleanup_locks()))
    
    logger.info("[✓] Shortener Module Initialized & Hardened")

async def shutdown():
    """Call this during bot shutdown to cleanup resources."""
    logger.info("[SHORTENER] Shutting down...")
    for t in _tasks:
        if not t.done():
            t.cancel()
    
    if _http_session and not _http_session.closed:
        await _http_session.close()
    logger.info("[SHORTENER] Resources cleaned up.")
