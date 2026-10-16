# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — ACCESS CONTROL ====================

import logging
import time
from functools import wraps
from pyrogram.types import Message

from plugins.ban import ban_svc
from plugins.admin import admin_svc, safe_edit, close_btn
from plugins.force_subs import fsub_svc

logger = logging.getLogger(__name__)

async def respond_with_error(message: Message, error_msg: str, title="❌ Fᴀɪʟᴇᴅ") -> None:
    text = (
        f"<blockquote expandable>{title}</blockquote>\n\n"
        f"<code>{error_msg}</code>"
    )
    await safe_edit(message, text, reply_markup=close_btn())


def guard_user(rate: float = 1.0):
    # Per-instance rate tracking — isolates handlers from each other
    _local_rate: dict = {}
    _local_abuse: dict = {}

    def decorator(func):
        @wraps(func)
        async def wrapper(client, message: Message, *args, **kwargs):
            try:
                user_id = message.from_user.id
            except AttributeError:
                # Fallback for callbacks depending on the context
                user_id = message.message.from_user.id if hasattr(message, "message") else 0

            if not user_id:
                return

            # Rate limiting (skip entirely when rate <= 0)
            if rate > 0:
                now = time.monotonic()
                if user_id in _local_abuse and now < _local_abuse[user_id]:
                    return  # Silent drop if under soft-block

                last = _local_rate.get(user_id, 0)
                if now - last < rate:
                    # If they are aggressively spamming commands
                    if now - last < 0.3:
                        _local_abuse[user_id] = now + 60 # 60s soft-block
                    return

                _local_rate[user_id] = now

            # Ban layer intercept
            is_banned, reason = await ban_svc.is_banned(user_id)
            if is_banned:
                await respond_with_error(
                    message, 
                    reason, 
                    title="🚫 <b>Aᴄᴄᴇss Rᴇᴠᴏᴋᴇᴅ — Bᴀɴɴᴇᴅ</b>"
                )
                return

            # Admin bypass -> FSUB intercept
            if not await admin_svc.is_admin(user_id):
                if not await fsub_svc.is_subscribed(client, user_id):
                    return await fsub_svc.render_not_joined(client, message)

            return await func(client, message, *args, **kwargs)
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════
#  Legacy Portability 
# ══════════════════════════════════════════════════════════

check_ban_status = guard_user()          # rate=1.0 for commands
check_fsub_and_ban = guard_user()        # rate=1.0 for commands
check_fsub_and_ban_media = guard_user(rate=0)  # no rate limit for media

logger.info("[✓] Access Control Wrapper Module Loaded — Strict Production Grade 🔒")
