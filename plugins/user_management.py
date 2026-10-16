
#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — TOKEN SYSTEM ====================

from __future__ import annotations

import html
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pytz
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, OperationFailure
from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import (
    FloodWait,
    MessageNotModified,
    MessageIdInvalid,
    ChatWriteForbidden,
    UserIsBlocked,
    PeerIdInvalid,
)
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from helper.database import DARKXSIDE78
from plugins.admin import _sudo_filter, close_btn, safe_edit, get_loading_text, safe_get_user

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚𝗦 |
# ═══════════════════════════════════════════════════════════

_MAX_TOKEN_TRANSACTION = 100000


# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class TokenService:
    async def add(self, uid: int, amount: int) -> int:
        return await DARKXSIDE78.add_token(uid, amount)

    async def remove(self, uid: int, amount: int) -> int:
        return await DARKXSIDE78.remove_token(uid, amount)

    async def safe_resolve_user(self, client: Client, message: Message, target_txt: str) -> Tuple[Optional[dict], Optional[str]]:
        target_ref = None
        if message.reply_to_message and message.reply_to_message.from_user:
            target_ref = str(message.reply_to_message.from_user.id)
        elif target_txt:
            target_ref = target_txt.strip()

        if not target_ref:
            return None, "No target specified. Please reply to a user or provide an ID/Username."

        if target_ref.startswith("@"):
            if not re.match(r"^@[a-zA-Z0-9_]{4,32}$", target_ref):
                return None, "Malformed username signature."
            user_doc = await DARKXSIDE78.get_user_by_username(target_ref[1:])
            if not user_doc:
                return None, f"Username {target_ref} is not registered in the database."
            return user_doc, None
        else:
            if not re.match(r"^-?\d{5,15}$", target_ref):
                return None, "Malformed ID signature."
            user_doc = await DARKXSIDE78.get_user(int(target_ref))
            if not user_doc:
                return None, f"User ID {target_ref} is not registered in the database."
            return user_doc, None


token_svc = TokenService()


# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗧𝗜𝗟𝗜𝗧𝗜𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

async def safe_edit_callback(cq: CallbackQuery, text: str, markup=None) -> None:
    try:
        await cq.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True
        )
    except MessageNotModified:
        pass
    except FloodWait as e:
        logger.warning(f"[admin] cb FloodWait {e.value}s")
    except (MessageIdInvalid, ChatWriteForbidden, UserIsBlocked, PeerIdInvalid):
        pass
    except Exception as exc:
        logger.error(f"[admin] cb Unknown Edit Fault: {exc}")

async def respond_with_error(message: Message, error_msg: str) -> None:
    text = (
        f"<blockquote expandable>[ ✗ ] 𝗘𝗥𝗥𝗢𝗥 𝗘𝗡𝗖𝗢𝗨𝗡𝗧𝗘𝗥𝗘𝗗</blockquote>\n\n"
        f"<b>Rᴇᴀsᴏɴ:</b> <code>{_e(error_msg)}</code>\n\n"
        f"<i>ᴄᴏɴᴛᴀᴄᴛ ᴅᴇᴠᴇʟᴏᴘᴇʀ ɪғ ᴛʜɪs ᴘᴇʀsɪsᴛs.</i>"
    )
    await safe_edit(message, text, reply_markup=close_btn())

def _e(text: str) -> str: return html.escape(str(text))

def _user_link(doc: dict) -> str:
    uid = doc.get("_id", "?")
    name = _e(doc.get("first_name") or doc.get("username") or str(uid))
    return f'<a href="tg://user?id={uid}">{name}</a>'

def _fmt_dt(dt: Optional[datetime]) -> str:
    if not isinstance(dt, datetime): return "—"
    if dt.year == 9999: return "Lifetime ♾️"
    if dt.tzinfo is None: dt = pytz.utc.localize(dt)
    return dt.strftime("%Y-%m-%d %H:%M UTC")

def _parse_duration_days(arg: str) -> int:
    arg = arg.lower().strip()
    m = re.match(r"(\d+)", arg)
    if not m:
        raise ValueError("Could not parse integer from duration.")
    n = int(m.group(1))
    if n > 3650:
        raise ValueError("Timeout threshold max limit (10 years) exceeded.")
    if arg.endswith(("d", "day", "days")): return n
    if arg.endswith(("m", "month", "months")): return n * 30
    if arg.endswith(("y", "year", "years")): return n * 365
    if arg.isdigit(): return n
    raise ValueError("Missing or invalid unit. Valid suffix: d, m, y.")




# ═══════════════════════════════════════════════════════════
#  | 𝗧𝗢𝗞𝗘𝗡 𝗕𝗔𝗟𝗔𝗡𝗖𝗘𝗦 |
# ═══════════════════════════════════════════════════════════

@Client.on_message(filters.command("addtoken") & filters.private & _sudo_filter())
async def cmd_add_token(client: Client, message: Message) -> None:
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 2 and not message.reply_to_message:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/addtoken &lt;amount&gt; &lt;@username/userid&gt;</code>\n"
                "›› <code>/add_token &lt;amount&gt; (reply)</code>\n\n"
                "<blockquote>◍ Exᴀᴍᴘʟᴇs :\n/addtoken 100 @Rexy/n/addtoken 100 2758066783</blockquote>",
                reply_markup=close_btn()
            )

        try:
            amount = int(args[1])
        except (IndexError, ValueError):
            raise ValueError("Numeric overflow or non-integer string detected in amount block.")

        if amount <= 0:
            raise ValueError("Reverted: Token input cannot be less than or equal to zero.")
        if amount > _MAX_TOKEN_TRANSACTION:
            raise ValueError(f"Constraint Violation: Exceeds transaction limit of {_MAX_TOKEN_TRANSACTION}.")

        target_txt = args[2] if len(args) > 2 else ""
        user, err = await token_svc.safe_resolve_user(client, message, target_txt)

        if err:
            return await respond_with_error(temp, err)
            
        await safe_edit(temp, "<b><i>Cᴏᴍᴍɪᴛᴛɪɴɢ...</i></b>")

        new_balance = await token_svc.add(user["_id"], amount)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗧𝗢𝗞𝗘𝗡 𝗔𝗟𝗟𝗢𝗖𝗔𝗧𝗜𝗢𝗡 𝗩𝗘𝗥𝗜𝗙𝗜𝗘𝗗</blockquote>\n\n"
            f"›› <b>Usᴇʀ :</b> {_user_link(user)} (<code>{user['_id']}</code>)\n"
            f"›› <b>Vᴏʟᴜᴍᴇ Aᴅᴅᴇᴅ :</b> <code>+{amount}</code>\n"
            f"<blockquote>◍ <b>Nᴇᴡ Bᴀʟᴀɴᴄᴇ :</b> <code>{new_balance}</code></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[admin] +{amount} tokens → {user['_id']} by {message.from_user.id}")

    except ValueError as e:
        await respond_with_error(temp, str(e))
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[token] Unknown fault: {e}")
        await respond_with_error(temp, f"Unhandled Node Exception: {e}")


@Client.on_message(filters.command("remtoken") & filters.private & _sudo_filter())
async def cmd_remove_token(client: Client, message: Message) -> None:
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 2 and not message.reply_to_message:
            return await safe_edit(
                temp,
                "➥ 𝗨𝗦𝗔𝗚𝗘 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡\n\n"
                "◍ <b>Cᴏᴍᴍᴀɴᴅ :</b>\n›› <code>/remove_token &lt;amount&gt; &lt;@username/userid&gt;</code>\n"
                "›› <code>/remtoken &lt;amount&gt; (reply)</code>\n\n"
                "<blockquote>◍ Exᴀᴍᴘʟᴇs :\n/remtoken 100 @Rexy/n/remtoken 100 2758066783</blockquote>",
                reply_markup=close_btn()
            )

        try:
            amount = int(args[1])
        except (IndexError, ValueError):
            raise ValueError("Numeric overflow or non-integer string detected in amount block.")

        if amount <= 0:
            raise ValueError("Reverted: Token input cannot be less than or equal to zero.")
        if amount > _MAX_TOKEN_TRANSACTION:
            raise ValueError(f"Constraint Violation: Exceeds transaction limit of {_MAX_TOKEN_TRANSACTION}.")

        target_txt = args[2] if len(args) > 2 else ""
        user, err = await token_svc.safe_resolve_user(client, message, target_txt)

        if err:
            return await respond_with_error(temp, err)
            
        await safe_edit(temp, "<b><i>Cᴏᴍᴍɪᴛᴛɪɴɢ...</i></b>")

        new_balance = await token_svc.remove(user["_id"], amount)
        
        await safe_edit(
            temp,
            f"<blockquote expandable>[ ✓ ] 𝗧𝗢𝗞𝗘𝗡 𝗗𝗘𝗗𝗨𝗖𝗧𝗜𝗢𝗡 𝗩𝗘𝗥𝗜𝗙𝗜𝗘𝗗</blockquote>\n\n"
            f"›› <b>Usᴇʀ :</b> {_user_link(user)} (<code>{user['_id']}</code>)\n"
            f"›› <b>Vᴏʟᴜᴍᴇ Dʀᴏᴘᴘᴇᴅ :</b> <code>-{amount}</code>\n"
            f"<blockquote>◍ <b>Nᴇᴡ Bᴀʟᴀɴᴄᴇ :</b> <code>{new_balance}</code></blockquote>",
            reply_markup=close_btn()
        )
        logger.info(f"[admin] -{amount} tokens → {user['_id']} by {message.from_user.id}")

    except ValueError as e:
        await respond_with_error(temp, str(e))
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        await respond_with_error(temp, f"Database Connection Fault: {e.__class__.__name__}")
    except PyMongoError as e:
        await respond_with_error(temp, f"Execution Failure: {e.__class__.__name__}")
    except Exception as e:
        logger.error(f"[token] Unknown fault: {e}")
        await respond_with_error(temp, f"Unhandled Node Exception: {e}")


@Client.on_message(filters.command(["token", "token_left", "mytokens", "bal"]) & filters.private)
async def cmd_check_tokens(client: Client, message: Message) -> None:
    await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    temp = await message.reply(get_loading_text(), parse_mode=ParseMode.HTML, quote=True)

    try:
        user_id = message.from_user.id
        user_data = await DARKXSIDE78.get_user(user_id)

        if not user_data:
            return await respond_with_error(temp, "Aᴄᴄᴏᴜɴᴛ ᴅᴀᴛᴀ ɴᴏᴛ ɪɴɪᴛɪᴀʟɪᴢᴇᴅ. Pʟᴇᴀsᴇ /start ᴛʜᴇ ʙᴏᴛ.")

        is_premium = user_data.get("is_premium", False)
        premium_expiry = user_data.get("premium_expiry")

        if premium_expiry and isinstance(premium_expiry, str):
            premium_expiry = datetime.fromisoformat(premium_expiry)

        if is_premium and premium_expiry:
            if datetime.utcnow() > premium_expiry:
                is_premium = False
                await DARKXSIDE78.col.update_one({"_id": user_id}, {"$set": {
                    "is_premium": False,
                    "premium_expiry": None
                }})

        token_count = user_data.get("token", Config.DEFAULT_TOKEN)

        status = "✓ 𝗔𝗖𝗧𝗜𝗩𝗘" if is_premium else "✘ 𝗜𝗡𝗔𝗖𝗧𝗜𝗩𝗘"
        
        msg = [
            "<b><blockquote>✦ 𝗔𝗖𝗖𝗢𝗨𝗡𝗧 𝗦𝗧𝗔𝗧𝗨𝗦 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡</blockquote></b>",
            "",
            f"›› <b>Pʀᴇᴍɪᴜᴍ Sᴛᴀᴛᴜs :</b> {status}"
        ]

        if is_premium and premium_expiry:
            msg.append(f"›› <b>Pʀᴇᴍɪᴜᴍ Exᴘɪʀʏ :</b> <code>{_fmt_dt(premium_expiry)}</code>")
        else:
            msg.extend([
                f"›› <b>Aᴠᴀɪʟᴀʙʟᴇ Tᴏᴋᴇɴs :</b> <code>{token_count}</code>",
                "",
                "<blockquote>≡ 𝟭 𝘁𝗼𝗸𝗲𝗻 = 𝟭 𝗳𝗶𝗹𝗲 𝗿𝗲𝗻𝗮𝗺𝗲 𝗼𝗽𝗲𝗿𝗮𝘁𝗶𝗼𝗻.</blockquote>"
            ])

        await safe_edit(temp, "\n".join(msg), reply_markup=close_btn())

    except Exception as e:
        logger.error(f"[token] Check fault: {e}")
        await respond_with_error(temp, f"System resolver exception: {str(e)}")

logger.info("[✓] Token Management Module Loaded — Strict Production Grade")
