# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — USER-INFO SECTIONS ====================

import asyncio
import html
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pyrogram import filters, Client
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import (
    FloodWait,
    PeerIdInvalid,
    UsernameInvalid,
    UsernameNotOccupied,
    UserDeactivated,
    UserDeactivatedBan,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from plugins.admin import _on_cooldown

logger = logging.getLogger(__name__)

PM = ParseMode.HTML
COOLDOWN_MSG = "⧗ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ..."

# ── ᴍᴇᴅɪᴀ ─────────────────────────────────────────────────────────────────────

USERINFO_PIC = "https://i.ibb.co/353XSH8Q/photo-2026-05-07-16-09-14-7637182675333152796.jpg"

# ── ᴛᴇxᴛ ᴛᴇᴍᴘʟᴀᴛᴇs ─────────────────────────────────────────────────────────────

USERINFO_CAPTION = (
    "<blockquote>❐ 𝗨𝗦𝗘𝗥 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡</blockquote>\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "<blockquote>"
    "<b>◍ Usᴇʀ ɪᴅ   :</b> <code>{user_id}</code>\n"
    "<b>◍ Nᴀᴍᴇ      :</b> {full_name}\n"
    "<b>◍ Mᴇɴᴛɪᴏɴ   :</b> {mention}\n"
    "<b>◍ Usᴇʀɴᴀᴍᴇ  :</b> {username}\n"
    "<b>◍ Lᴀɴɢᴜᴀɢᴇ  :</b> {language}</blockquote>\n"
    "<blockquote>"
    "<b>◍ Tʀᴜsᴛ  :</b> {trust_display}</blockquote>\n"
    "<blockquote>"
    "<b>◍ Sᴛᴀᴛᴜs  :</b> {acc_stat}\n"
    "<b>◍ Dᴄ ɪᴅ   :</b> {dc_id}"
    "</blockquote>\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "<blockquote>"
    "<b>➥ Lᴀsᴛ sᴇᴇɴ :</b> {last_seen}\n"
    "<b>➥ Bɪᴏ :</b> <code>{bio}</code>"
    "</blockquote>\n"
)

GROUPINFO_CAPTION = (
    "<blockquote>❐ 𝗚𝗥𝗢𝗨𝗣 𝗜𝗡𝗙𝗢</blockquote>\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "<blockquote>"
    "<b>›› Rᴀɴᴋ   :</b> {rank}\n"
    "<b>›› Jᴏɪɴᴇᴅ :</b> {join_date}"
    "</blockquote>\n"
    "━━━━━━━━━━━━━━━━━━\n"
)

# ── sᴛᴀᴛɪᴄ ʙᴜᴛᴛᴏɴs (ʙᴜɪʟᴛ ᴏɴᴄᴇ ᴀᴛ ɪᴍᴘᴏʀᴛ) ─────────────────────────────────────

_BUTTONS = InlineKeyboardMarkup([
    [
        InlineKeyboardButton("Dᴇᴠ", url="https://t.me/RexyNxy"),
        InlineKeyboardButton("Cʟᴏsᴇ",      callback_data="close_msg"),
    ]
])

# ── ʀᴀɴᴋ ʟᴏᴏᴋᴜᴘ ───────────────────────────────────────────────────────────────

_RANK_MAP: dict[ChatMemberStatus, str] = {
    ChatMemberStatus.OWNER        : "♛ Oᴡɴᴇʀ",
    ChatMemberStatus.ADMINISTRATOR: "Aᴅᴍɪɴ",
    ChatMemberStatus.MEMBER       : "Mᴇᴍʙᴇʀ",
    ChatMemberStatus.RESTRICTED   : "Rᴇsᴛʀɪᴄᴛᴇᴅ",
    ChatMemberStatus.LEFT         : "Lᴇғᴛ",
    ChatMemberStatus.BANNED       : "Bᴀɴɴᴇᴅ",
}

# ── ᴘᴜʀᴇ ʜᴇʟᴘᴇʀs (ɴᴏ ɪ/ᴏ) ─────────────────────────────────────────────────────

def _safe_escape(text: Optional[str], fallback: str = "N/A") -> str:
    if not text:
        return fallback
    cleaned = str(text).strip()
    return html.escape(cleaned, quote=False) if cleaned else fallback


def _account_status(user) -> str:
    if getattr(user, "is_deleted",    False): return "Dᴇʟᴇᴛᴇᴅ"
    if getattr(user, "is_bot",        False): return "Bᴏᴛ"
    if getattr(user, "is_scam",       False): return "Sᴄᴀᴍ"
    if getattr(user, "is_fake",       False): return "Fᴀᴋᴇ"
    if getattr(user, "is_support",    False): return "Sᴜᴘᴘᴏʀᴛ"
    if getattr(user, "is_verified",   False): return "Vᴇʀɪғɪᴇᴅ"
    if getattr(user, "is_restricted", False): return "Rᴇsᴛʀɪᴄᴛᴇᴅ"
    return "Nᴏʀᴍᴀʟ"


def _compute_trust(user, has_photo: bool) -> int:
    score = 70

    # Hard negatives
    if getattr(user, "is_deleted",    False): score -= 60
    if getattr(user, "is_scam",       False): score -= 50
    if getattr(user, "is_fake",       False): score -= 40
    if getattr(user, "is_restricted", False): score -= 20
    # Soft negatives
    if not user.username:                     score -= 10
    if not has_photo:                         score -= 5

    # Positives
    if getattr(user, "is_support",    False): score += 20
    if getattr(user, "is_verified",   False): score += 30
    if getattr(user, "is_premium",    False): score += 15
    if user.username:                         score += 10
    if has_photo:                             score += 10

    return max(0, min(100, score))


def _trust_display(score: int) -> str:
    filled = round(score / 10)
    bar    = "▓" * filled + "░" * (10 - filled)
    return f"{bar} {score}/100"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_last_seen(user) -> str:
    try:
        status = user.status
        if status is None:
            return "❐ ʜɪᴅᴅᴇɴ"

        s = str(status).upper()

        if "ONLINE" in s:
            return "<b>ᴏɴʟɪɴᴇ ɴᴏᴡ</b>"

        if "OFFLINE" in s:
            was: Optional[datetime] = getattr(status, "was_online", None)
            if was is None:
                return "ʀᴇᴄᴇɴᴛʟʏ"
            if was.tzinfo is None:
                was = was.replace(tzinfo=timezone.utc)
            total = int((_utcnow() - was).total_seconds())
            if total < 60:     return "ᴊᴜsᴛ ɴᴏᴡ"
            if total < 3600:   return f"{total // 60}ᴍ ᴀɢᴏ"
            if total < 86400:  return f"{total // 3600}ʜ ᴀɢᴏ"
            if total < 604800: return f"{total // 86400}ᴅ ᴀɢᴏ"
            return f"{total // 604800}ᴡ ᴀɢᴏ"

        if "RECENTLY" in s: return "ʀᴇᴄᴇɴᴛʟʏ"
        if "WEEK"     in s: return "ᴡɪᴛʜɪɴ ᴀ ᴡᴇᴇᴋ"
        if "MONTH"    in s: return "ᴡɪᴛʜɪɴ ᴀ ᴍᴏɴᴛʜ"
        if "LONG"     in s: return "ʟᴏɴɢ ᴛɪᴍᴇ ᴀɢᴏ"

        return "ʜɪᴅᴅᴇɴ"
    except Exception:
        return "ʜɪᴅᴅᴇɴ"


# ── sᴇʀᴠɪᴄᴇ ʟᴀʏᴇʀ ─────────────────────────────────────────────────────────────

@dataclass
class CachedUserInfo:
    bio: str
    has_photo: bool
    rank: str = "Nᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ"
    join_date: str = "Nᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ"
    timestamp: float = 0.0

class UserInfoService:
    _cache: dict[tuple[int, int], CachedUserInfo] = {}
    _locks: dict[int, asyncio.Lock] = {}
    CACHE_TTL = 60  # 60 seconds

    @classmethod
    async def get_user_info(cls, client: Client, user_id: int, chat_id: Optional[int] = None) -> CachedUserInfo:
        lock = cls._locks.setdefault(user_id, asyncio.Lock())
        async with lock:
            cache_key = (user_id, chat_id or 0)
            now = time.time()
            if cache_key in cls._cache and (now - cls._cache[cache_key].timestamp) < cls.CACHE_TTL:
                return cls._cache[cache_key]

            # Fire all I/O concurrently
            coros = [_fetch_bio(client, user_id), _has_photo(client, user_id)]
            if chat_id:
                coros.append(_group_info(client, chat_id, user_id))

            results = await asyncio.gather(*coros, return_exceptions=True)

            bio = "ʜɪᴅᴅᴇɴ ʙʏ ᴘʀɪᴠᴀᴄʏ"
            if isinstance(results[0], str):
                bio = results[0]
            else:
                logger.warning(f"[UserInfoService] Bio fetch failed for {user_id}: {results[0]}")

            has_photo = False
            if isinstance(results[1], bool):
                has_photo = results[1]
            else:
                logger.warning(f"[UserInfoService] Photo fetch failed for {user_id}: {results[1]}")

            rank, join_date = "Nᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ", "Nᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ"
            if chat_id and len(results) > 2:
                if isinstance(results[2], tuple):
                    rank, join_date = results[2]
                else:
                    logger.warning(f"[UserInfoService] Group info fetch failed for {user_id}: {results[2]}")

            info = CachedUserInfo(bio=bio, has_photo=has_photo, rank=rank, join_date=join_date, timestamp=now)
            cls._cache[cache_key] = info
            return info


# ── ᴀsʏɴᴄ ʜᴇʟᴘᴇʀs (ɪ/ᴏ) ───────────────────────────────────────────────────────

async def _fetch_bio(client: Client, user_id: int) -> str:
    try:
        chat = await client.get_chat(user_id)
        bio  = (chat.bio or "").strip()
        if not bio:
            return "ɴᴏ ʙɪᴏ"
        bio = (bio[:120] + "…") if len(bio) > 120 else bio
        return _safe_escape(bio, fallback="ɴᴏ ʙɪᴏ")
    except FloodWait as e:
        await asyncio.sleep(e.value + 0.5)
        return "ʜɪᴅᴅᴇɴ ʙʏ ᴘʀɪᴠᴀᴄʏ"
    except Exception:
        return "ʜɪᴅᴅᴇɴ ʙʏ ᴘʀɪᴠᴀᴄʏ"


async def _has_photo(client: Client, user_id: int) -> bool:
    try:
        result = client.get_profile_photos(user_id, limit=1)
        if hasattr(result, "__aiter__"):
            async for _ in result:
                return True
            return False
        photos = await result  # type: ignore[misc]
        return bool(photos)
    except Exception:
        return False


async def _resolve_user(client: Client, raw: str):
    try:
        return await client.get_users(int(raw))
    except ValueError:
        pass
    return await client.get_users(raw.lstrip("@"))


async def _group_info(
    client: Client,
    chat_id: int,
    user_id: int,
) -> tuple[str, str]:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        rank   = _RANK_MAP.get(member.status, "❐ Uɴᴋɴᴏᴡɴ")
        joined = (
            getattr(member, "joined_date", None)
            or getattr(member, "joined_at",   None)
        )
        if joined:
            if getattr(joined, "tzinfo", None) is None:
                joined = joined.replace(tzinfo=timezone.utc)
            join_date = joined.strftime("%Y-%m-%d %H:%M UTC")
        else:
            join_date = "Hɪᴅᴅᴇɴ ʙʏ Tᴇʟᴇɢʀᴀᴍ"
        return rank, join_date
    except Exception:
        return "❐ Pᴇʀᴍɪssɪᴏɴ ᴅᴇɴɪᴇᴅ", "Nᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ"


# ── ʜᴀɴᴅʟᴇʀs ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command(["userinfo", "id", "info"]))
async def showid(client: Client, message: Message) -> None:
    uid = message.from_user.id if message.from_user else None
    if uid and _on_cooldown(uid, "info"):
        await message.reply_text(COOLDOWN_MSG, parse_mode=PM, quote=True)
        return

    # ── ʀᴇsᴏʟᴠᴇ ᴛᴀʀɢᴇᴛ ᴜsᴇʀ ───────────────────────────────────────────────────
    if message.reply_to_message and message.reply_to_message.from_user:
        user = message.reply_to_message.from_user

    elif len(message.command) > 1:
        raw = message.command[1]
        try:
            user = await _resolve_user(client, raw)
        except (PeerIdInvalid, UsernameInvalid, UsernameNotOccupied,
                UserDeactivated, UserDeactivatedBan):
            await message.reply_text(
                "<b><blockquote>❐ Uɴᴀʙʟᴇ ᴛᴏ ғᴇᴛᴄʜ ᴛʜᴀᴛ ᴜsᴇʀ.\n"
                "ɪᴅ ɪɴᴠᴀʟɪᴅ, ᴜsᴇʀɴᴀᴍᴇ ɴᴏᴛ ғᴏᴜɴᴅ, "
                "ᴏʀ ᴀᴄᴄᴏᴜɴᴛ ᴅᴇᴀᴄᴛɪᴠᴀᴛᴇᴅ.</blockquote></b>",
                parse_mode=PM,
            )
            return
        except FloodWait as e:
            await asyncio.sleep(e.value + 0.5)
            await message.reply_text(
                "<b><blockquote>≡ Fʟᴏᴏᴅ ᴡᴀɪᴛ — ᴛʀʏ ᴀɢᴀɪɴ sʜᴏʀᴛʟʏ.</blockquote></b>",
                parse_mode=PM,
            )
            return
        except Exception as e:
            logger.warning("showid: get_users failed for %r — %s", raw, e)
            await message.reply_text(
                "<b><blockquote>❐ Fᴀɪʟᴇᴅ ᴛᴏ ʀᴇsᴏʟᴠᴇ ᴜsᴇʀ. Tʀʏ ᴀɢᴀɪɴ.</blockquote></b>",
                parse_mode=PM,
            )
            return

    else:
        user = message.from_user

    if not user:
        return

    # ── ᴇxᴛʀᴀᴄᴛ ᴀɴᴅ ᴇsᴄᴀᴘᴇ sᴛᴀᴛɪᴄ ғɪᴇʟᴅs ─────────────────────────────────────
    user_id = user.id

    raw_name  = (user.first_name or user.last_name or "User").strip()
    full_name = _safe_escape(
        " ".join(filter(None, [user.first_name or "", user.last_name or ""])),
        fallback="N/A",
    )
    mention   = f'<a href="tg://user?id={user_id}">{html.escape(raw_name, quote=False)}</a>'

    username  = _safe_escape(f"@{user.username}") if user.username else "N/A"
    language  = _safe_escape(user.language_code or "Unknown")
    premium   = "✦ Yᴇs" if getattr(user, "is_premium", False) else "Nᴏ"
    dc_id     = str(user.dc_id) if user.dc_id else "N/A"
    last_seen = _format_last_seen(user)   # pre-built HTML — not escaped
    acc_stat  = _account_status(user)

    # ── ꜰᴇᴛᴄʜ ɪɴꜰᴏ ᴠɪᴀ sᴇʀᴠɪᴄᴇ ──────────────────────────────────────────────
    in_group = message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
    chat_id = message.chat.id if in_group else None
    
    info = await UserInfoService.get_user_info(client, user_id, chat_id)
    
    score = _compute_trust(user, info.has_photo)
    trust = _trust_display(score)   # "▓▓▓▓▓▓▓░░░ 70/100"

    # ── ʙᴜɪʟᴅ ᴄᴀᴘᴛɪᴏɴ ─────────────────────────────────────────────────────────
    caption = USERINFO_CAPTION.format(
        user_id       = user_id,
        full_name     = full_name,
        mention       = mention,
        username      = username,
        language      = language,
        premium       = premium,
        acc_stat      = acc_stat,
        trust_display = trust,
        dc_id         = dc_id,
        bio           = info.bio,
        last_seen     = last_seen,
    )

    if in_group:
        caption += GROUPINFO_CAPTION.format(rank=info.rank, join_date=info.join_date)

    # ── sᴇɴᴅ ᴡɪᴛʜ ғʟᴏᴏᴅᴡᴀɪᴛ ʀᴇᴛʀʏ + ɢʀᴀᴄᴇғᴜʟ ᴛᴇxᴛ ғᴀʟʟʙᴀᴄᴋ ────────────────────
    sent = None
    for attempt in range(2):
        try:
            sent = await message.reply_photo(
                photo        = USERINFO_PIC,
                caption      = caption,
                reply_markup = _BUTTONS,
                parse_mode   = PM,
            )
            break
        except FloodWait as e:
            if attempt == 0:
                await asyncio.sleep(e.value + 0.5)
            else:
                logger.error("showid: FloodWait exhausted — %s", e)
                break
        except Exception as e:
            logger.error("showid: reply_photo failed — %s", e)
            try:
                sent = await message.reply_text(
                    caption,
                    reply_markup = _BUTTONS,
                    parse_mode   = PM,
                    quote        = True,
                )
            except Exception as fe:
                logger.error("showid: text fallback also failed — %s", fe)
            break

# ==================== REXY V14 PRO — END OF USER-INFO SECTIONS ====================
