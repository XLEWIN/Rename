#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʀᴇsɪʟɪᴇɴᴛ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

from __future__ import annotations


import sys
import asyncio
from contextlib import suppress

# =============================================================
#  | ASYNC EVENT LOOP OPTIMIZATION |
# =============================================================
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

try:
    _loop = asyncio.get_event_loop()
except RuntimeError:
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

# =============================================================
#  | CORE IMPORTS |
# =============================================================
import os
import time
import logging
import logging.config
import signal
from datetime import datetime, timedelta
from typing import Optional, List, Union, Set
from pathlib import Path

from pytz import timezone
from pyrogram import Client, utils 
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait,
    UserIsBlocked,
    PeerIdInvalid,
    ChatWriteForbidden,
    RPCError
)

# -------------------------------------------------------------
# Prevents double-patching RecursionError while ensuring listen support
# -------------------------------------------------------------
if not hasattr(Client, "listen"):
    with suppress(ImportError):
        import pyromod

# ─────────────────────────────────────────────────────────────
#  | LEGACY PEER ID OVERRIDE |
# ─────────────────────────────────────────────────────────────
utils.MIN_CHANNEL_ID = -100999999999999

from aiohttp import web

from route import web_server
from config import Config
from helper.job_ledger import job_ledger
from helper.utils import start_background_tasks, stop_background_tasks

# ─────────────────────────────────────────────────────────────
#  | KURIGRAM ANTI-QUOTE / ANTI-REPLY ENFORCER |
# ─────────────────────────────────────────────────────────────
from pyrogram import Client

# Save Kurigram's custom underlying send_message method
_native_send_message = Client.send_message

async def _clean_send_message(self, chat_id, text, *args, **kwargs):
    # Strip any automatically injected reply/quote parameters from Kurigram's logic
    kwargs.pop("reply_to_message_id", None)
    if "reply_parameters" in kwargs:
        kwargs.pop("reply_parameters", None)
        
    # Also strip Kurigram's custom default quote argument if it exists
    if "quote" in kwargs:
        kwargs["quote"] = False

    return await _native_send_message(self, chat_id, text, *args, **kwargs)

# Override the client method globally
Client.send_message = _clean_send_message

# Do the exact same patch for photos/media to protect your start layout
_native_send_photo = Client.send_photo
async def _clean_send_photo(self, chat_id, photo, *args, **kwargs):
    kwargs.pop("reply_to_message_id", None)
    if "reply_parameters" in kwargs:
        kwargs.pop("reply_parameters", None)
    if "quote" in kwargs:
        kwargs["quote"] = False
        
    return await _native_send_photo(self, chat_id, photo, *args, **kwargs)

Client.send_photo = _clean_send_photo


# ─────────────────────────────────────────────────────────────
#  | 𝗟𝗢𝗚𝗚𝗜𝗡𝗚 𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡 |
# ─────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

try:
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
except Exception as e:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.error(f"Failed to load logging.conf: {e}")

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  | 𝗖𝗢𝗡𝗦𝗧𝗔𝗡𝗧 & 𝗧𝗜𝗠𝗘𝗭𝗢𝗡𝗘 |
# ─────────────────────────────────────────────────────────────

IST = timezone("Asia/Kolkata")

MAX_FILE_SIZE = (
    4 * 1024 * 1024 * 1024  
    if getattr(Config, "USER_SESSION_STRING", "").strip()
    else 2 * 1024 * 1024 * 1024  
)

def get_client_name() -> str:
    if getattr(Config, "USER_SESSION_STRING", "").strip():
        return "premium_user"
    return getattr(Config, "SESSION_NAME", "default_bot")

# ─────────────────────────────────────────────────────────────
#  | <b>𝗦𝗔𝗙𝗘 𝗟𝗜𝗦𝗧𝗘𝗡𝗘𝗥 𝗗𝗜𝗖𝗧</b> |
# ─────────────────────────────────────────────────────────────
class ListenerDict(dict):
    """
    Custom dictionary to prevent pyromod/pyrofork/kurigram KeyErrors.
    Automatically initializes missing Enum keys with empty lists.
    """
    def __missing__(self, key):
        val = []
        self[key] = val
        return val

# ─────────────────────────────────────────────────────────────
#  | <b>𝗕𝗢𝗧 𝗖𝗟𝗔𝗦𝗦</b> |
# ─────────────────────────────────────────────────────────────

class Bot(Client):
    """
    Production-grade Telegram Bot Client with enhanced stability, 
    observability, and resource management.
    """
    
    # HARD-FORCE concurrent transmissions at the class level just in case kwargs get stripped
    MAX_CONCURRENT_TRANSMISSIONS = 8


    def __init__(self):
        client_name = get_client_name()
        
        params = {
            "name": client_name,
            "api_id": Config.API_ID,
            "api_hash": Config.API_HASH,
            "workers": Config.WORKERS,
            "max_concurrent_transmissions": Config.CONCURRENT_TRANSMISSIONS,
            "sleep_threshold": Config.SLEEP_THRESHOLD,
            "ipv6": False,
        }

        session_string = getattr(Config, "USER_SESSION_STRING", "").strip()
        if session_string:
            params["session_string"] = session_string
        else:
            params["bot_token"] = Config.BOT_TOKEN

        super().__init__(**params)
        self.max_concurrent_transmissions = Config.CONCURRENT_TRANSMISSIONS

        # --- BULLETPROOF ENGINE SYNC ---
        # Gracefully handle internal listener mappings across all 3 frameworks
        self.listeners = ListenerDict(getattr(self, "listeners", {}))

        self.start_time = time.time()
        self.max_file_size = MAX_FILE_SIZE
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        self._stop_lock = asyncio.Lock()
        self._admin_ids: Set[int] = {Config.OWNER_ID}
        
        self.max_retries = getattr(Config, "MAX_RETRIES", 3)
        self.retry_delay = getattr(Config, "RETRY_DELAY", 2)
        self.message_timeout = getattr(Config, "MESSAGE_TIMEOUT", 30)
        
        self.web_runner: Optional[web.AppRunner] = None
        self.web_site: Optional[web.TCPSite] = None
        self.job_handlers: dict = {}
        
        self.total_messages_sent = 0
        self.total_errors = 0
        
        logger.info(f"Bot instance initialized (Name: {client_name})")

    @property
    def admin_ids(self) -> Set[int]:
        return self._admin_ids

    def get_uptime(self) -> str:
        try:
            uptime_seconds = int(time.time() - self.start_time)
            return str(timedelta(seconds=uptime_seconds))
        except Exception as e:
            logger.error(f"Error calculating uptime: {e}")
            return "Unknown"

    def get_current_ist_time(self) -> str:
        try:
            return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception as e:
            logger.error(f"Error getting IST time: {e}")
            return "Unknown"

    def create_startup_keyboard(self) -> InlineKeyboardMarkup:
        try:
            return InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("ᴜᴘᴅᴀᴛᴇs", url=f"https://t.me/{Config.BOT_CHANNEL_USERNAME}"),
                        InlineKeyboardButton("ᴄʜᴇᴄᴋ ʙᴏᴛ", url=f"https://t.me/{Config.BOT_USERNAME}"),
                    ],
                    [
                        InlineKeyboardButton("Refactor Dev", url=f"https://t.me/{Config.OWNER_USERNAME}")
                    ],
                ]
            )
        except Exception as e:
            logger.error(f"Error creating startup keyboard: {e}")
            return InlineKeyboardMarkup([])

    def get_bot_stats(self) -> str:
        return (
            f"📊 Sᴛᴀᴛɪsᴛɪᴄs:\n"
            f"  • Uᴘᴛɪᴍᴇ: {self.get_uptime()}\n"
            f"  • Mᴇssᴀɢᴇs Sᴇɴᴛ: {self.total_messages_sent}\n"
            f"  • Eʀʀᴏʀs: {self.total_errors}\n"
            f"  • Mᴀx Fɪʟᴇ Sɪᴢᴇ: {self.max_file_size / (1024**3):.1f}GB"
        )

    async def send_message_with_retry(self, chat_id: int, text: str, **kwargs) -> Optional[object]:
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                message = await asyncio.wait_for(
                    self.send_message(chat_id, text, **kwargs),
                    timeout=self.message_timeout
                )
                self.total_messages_sent += 1
                return message
            except FloodWait as e:
                logger.warning(f"FloodWait: sleeping for {e.value} seconds")
                await asyncio.sleep(e.value)
                retry_count += 1
            except UserIsBlocked:
                logger.warning(f"Bot is blocked by user {chat_id}")
                break
            except (PeerIdInvalid, ChatWriteForbidden) as e:
                logger.error(f"Permission error for chat {chat_id}: {e}")
                break
            except asyncio.TimeoutError:
                logger.error(f"Message timeout for chat {chat_id} (Attempt {retry_count+1})")
                retry_count += 1
                if retry_count <= self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
            except RPCError as e:
                logger.error(f"RPC error for chat {chat_id}: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error sending message to {chat_id}: {e}", exc_info=True)
                break
        self.total_errors += 1
        return None

    async def send_photo_with_retry(self, chat_id: int, photo: str, **kwargs) -> Optional[object]:
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                message = await asyncio.wait_for(
                    self.send_photo(chat_id, photo, **kwargs),
                    timeout=self.message_timeout
                )
                self.total_messages_sent += 1
                return message
            except FloodWait as e:
                logger.warning(f"FloodWait: sleeping for {e.value} seconds")
                await asyncio.sleep(e.value)
                retry_count += 1
            except UserIsBlocked:
                logger.warning(f"Bot is blocked by user {chat_id}")
                break
            except (PeerIdInvalid, ChatWriteForbidden) as e:
                logger.error(f"Permission error for chat {chat_id}: {e}")
                break
            except asyncio.TimeoutError:
                logger.error(f"Photo send timeout for chat {chat_id} (Attempt {retry_count+1})")
                retry_count += 1
                if retry_count <= self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
            except RPCError as e:
                logger.error(f"RPC error for chat {chat_id}: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error sending photo to {chat_id}: {e}", exc_info=True)
                break
        self.total_errors += 1
        return None

    async def send_restart_notification(self) -> None:
        try:
            logger.info("Sending restart notification to owner...")
            await self.send_message_with_retry(
                Config.OWNER_ID,
                text="<blockquote><b>🤖 ʙᴏᴛ ʀᴇsᴛᴀʀᴛᴇᴅ</b></blockquote>"
            )
        except Exception as e:
            logger.error(f"Error in send_restart_notification: {e}", exc_info=True)

    async def send_startup_message(self, chat_id: Union[int, str]) -> None:
        try:
            caption = (
                f"<blockquote><b>≡ {Config.BOT_NAME} ɪs ᴀᴡᴀᴋᴇ ᴀɢᴀɪɴ</b></blockquote>\n"
                f"<blockquote>◍ Sʟᴇᴇᴘ ʟᴏsᴛ sᴏᴍᴇᴡʜᴇʀᴇ ᴀғᴛᴇʀ ᴍɪᴅɴɪɢʜᴛ</blockquote>\n"
                f"<blockquote>◍ Uᴘᴛɪᴍᴇ: {self.get_uptime()}</blockquote>"
            )
            await self.send_photo_with_retry(
                chat_id=chat_id,
                photo=Config.START_PIC,
                caption=caption,
                reply_markup=self.create_startup_keyboard(),
            )
        except Exception as e:
            logger.error(f"Error in send_startup_message for {chat_id}: {e}", exc_info=True)

    async def broadcast_startup_messages(self, chat_ids: List[Union[int, str]]) -> None:
        if not chat_ids:
            return
        sem = asyncio.Semaphore(5)
        async def bounded_send(chat_id):
            async with sem:
                await self.send_startup_message(chat_id)
                await asyncio.sleep(0.5)
        tasks = [bounded_send(chat_id) for chat_id in chat_ids if chat_id]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def setup_webhook(self) -> None:
        if not Config.WEBHOOK:
            logger.info("Webhook is disabled")
            return
        try:
            async def bot_health_probe():
                return self.is_running

            app = await web_server(health_probe=bot_health_probe)
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()

            # Log the exact bind decision so port-routing mismatches are
            # visible in Railway logs at a glance (Railway routes to $PORT).
            logger.info(
                f"Web bind env: PORT={os.environ.get('PORT')!r} "
                f"WEBHOOK_PORT={os.environ.get('WEBHOOK_PORT')!r} "
                f"-> binding 0.0.0.0:{Config.WEBHOOK_PORT}"
            )
            self.web_site = web.TCPSite(self.web_runner, "0.0.0.0", Config.WEBHOOK_PORT)
            await self.web_site.start()
            logger.info(f"✅ Webhook server started successfully on port {Config.WEBHOOK_PORT}")
        except Exception as e:
            logger.error(
                f"Failed to setup webhook: {e} — the process cannot stay healthy "
                f"without the web server (Railway will restart the deploy).",
                exc_info=True,
            )
            raise

    def _install_exception_handler(self) -> None:
        loop = asyncio.get_running_loop()
        def handle_exception(loop, context):
            exc = context.get("exception")
            msg = context.get("message", "Unhandled asyncio exception")
            logger.error(f"Async Error: {msg}", exc_info=exc)
            self.total_errors += 1
        loop.set_exception_handler(handle_exception)

    def _setup_async_signals(self) -> None:
        if sys.platform == 'win32':
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._handle_shutdown_signal(s)))

    async def _handle_shutdown_signal(self, sig) -> None:
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        self.shutdown_event.set()
        if self.is_running:
             await self.stop()

    def _validate_config(self) -> None:
        critical_fields = ["API_ID", "API_HASH", "OWNER_ID"]
        missing = [f for f in critical_fields if not getattr(Config, f, None)]
        if not getattr(Config, "BOT_TOKEN", None) and not getattr(Config, "USER_SESSION_STRING", "").strip():
            missing.append("BOT_TOKEN or USER_SESSION_STRING")
        if missing:
            raise ValueError(f"Bot cannot start without: {', '.join(missing)}")

    async def start(self) -> None:
        try:
            logger.info("Starting bot initialization sequence...")
            self._validate_config()
            
            await super().start()
            self.is_running = True
            logger.info("✅ Pyrogram client started")

            from pyrogram import filters
            @self.on_message(filters.command("ping") & filters.private)
            async def ping_handler(client, message):
                start = time.time()
                reply = await message.reply_text("<code>Pinging...</code>")
                delta_ms = (time.time() - start) * 1000
                await reply.edit_text(
                    f"<b>🏓 Pᴏɴɢ</b>\n"
                    f"◍ Lᴀᴛᴇɴᴄʏ: <code>{delta_ms:.2f}ms</code>\n"
                    f"◍ Uᴘᴛɪᴍᴇ: <code>{self.get_uptime()}</code>"
                )

            from loader import load_plugins_async
            await load_plugins_async(client=self, root=Path(__file__).parent.resolve())

            me = await self.get_me()
            self.mention = me.mention
            self.username = me.username

            self._install_exception_handler()
            self._setup_async_signals()
            await self.send_restart_notification()
            await self.setup_webhook()

            # Recovery + background infrastructure must NEVER abort startup.
            # An exception here used to kill the process, which put Railway
            # into a deploy → restart → deploy loop. Log and continue instead.
            try:
                rename_fn = self.job_handlers.get("file_rename")
                if not rename_fn:
                    with suppress(ImportError):
                        from plugins.file_rename import auto_rename_files
                        rename_fn = auto_rename_files

                if rename_fn:
                    await job_ledger.startup(client=self, enqueue_fn=rename_fn)

                start_background_tasks()
            except Exception as _e:
                logger.error(
                    f"Startup recovery/background init failed (non-fatal, bot keeps running): {_e}",
                    exc_info=True,
                )
            _bg_inits = [
                ("admin",       "plugins.admin",       "init"),
                ("force_subs",  "plugins.force_subs",  "init"),
                ("premium",     "plugins.premium",     "init"),
                ("pdftool",     "plugins.pdftool",     "init"),
                ("metadata",    "plugins.metadata",    "init"),
                ("intro_ui",    "plugins.intro_ui",    "init"),
                ("bot_stats",   "plugins.bot_stats",   "init"),
                ("file_rename", "plugins.file_rename", "init"),
                ("leaderboard", "plugins.leaderboard", "init"),
                ("shortener",   "plugins.shortener",   "init"),
            ]
            import importlib as _il
            for _name, _mod, _fn in _bg_inits:
                try:
                    try:
                        _m = _il.import_module(_mod)
                    except ModuleNotFoundError:
                        _mod_fb = _mod.replace("shortener", "shortner")
                        if _mod_fb != _mod:
                            _m = _il.import_module(_mod_fb)
                        else:
                            raise
                    _init = getattr(_m, _fn, None)
                    if _init:
                        res = _init(self)
                        if asyncio.iscoroutine(res):
                            await res
                except Exception as _e:
                    logger.error(f"Background worker init failed [{_name}]: {_e}")

            with suppress(Exception):
                from helper.database import DARKXSIDE78
                admin_ids = await DARKXSIDE78.get_admin_ids()
                self._admin_ids = set(admin_ids) if admin_ids else {Config.OWNER_ID}
                Config.ADMINS = list(self._admin_ids)

            try:
                from helper.database import DARKXSIDE78
                from helper.db_premium import db as db_premium
                from helper.fsub import Rexy
                await DARKXSIDE78.ensure_indexes()
                await db_premium.ensure_indexes()
                await Rexy.ensure_indexes()
            except Exception as e:
                logger.error(f"Index creation bypassed: {e}")

            with suppress(Exception):
                from helper.database import DARKXSIDE78
                await DARKXSIDE78.remove_expired_premium_users()

            for channel_id in [Config.LOG_CHANNEL, Config.DUMP_CHANNEL]:
                if channel_id:
                    try:
                        await self.get_chat(channel_id)
                    except ValueError:
                        logger.error(f"⚠️ Peer ID {channel_id} fell back to legacy handling definitions.")
                    except Exception as e:
                        logger.warning(f"⚠️ Could not resolve channel {channel_id}: {e}")

            await self.broadcast_startup_messages([Config.LOG_CHANNEL, Config.SUPPORT_CHANNEL_USERNAME])
            logger.info("🚀 Bot is now fully operational and listening")
            
        except Exception as e:
            logger.critical(f"Failed to start bot: {e}", exc_info=True)
            self.is_running = False
            raise

    async def stop(self, *args) -> None:
        async with self._stop_lock:
            if not self.is_running:
                return
            try:
                self.is_running = False
                with suppress(Exception):
                    await self.send_message_with_retry(
                        Config.OWNER_ID,
                        text=f"<blockquote><b>🛑 ʙᴏᴛ sʜᴜᴛᴅᴏᴡɴ</b></blockquote>\n<blockquote>◍ Uᴘᴛɪᴍᴇ: {self.get_uptime()}</blockquote>"
                    )
                
                with suppress(Exception):
                    await stop_background_tasks()
                if hasattr(job_ledger, 'stop_periodic_cleanup'):
                    with suppress(Exception):
                        job_ledger.stop_periodic_cleanup()
                
                if self.web_site:
                    with suppress(Exception):
                        await self.web_site.stop()
                if self.web_runner:
                    with suppress(Exception):
                        await self.web_runner.cleanup()
                
                await super().stop()
                logger.info("Bot shutdown completed successfully")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")

# ─────────────────────────────────────────────────────────────
#  | 𝗘𝗡𝗧𝗥𝗬 𝗣𝗢𝗜𝗡𝗧 |
# ─────────────────────────────────────────────────────────────

def main():
    loop = asyncio.get_event_loop()
    bot = None
    max_attempts = 3

    # Retry startup in-process instead of dying on the first transient error
    # (Telegram DC flap, Mongo hiccup, brief port conflict). Each attempt
    # gives the platform a live process to probe; exiting immediately just
    # puts Railway into a deploy-restart loop.
    for attempt in range(1, max_attempts + 1):
        try:
            bot = Bot()
            logger.info(
                f"Bot instance initialized safely "
                f"(attempt {attempt}/{max_attempts}). Commencing runtime engine..."
            )
            loop.run_until_complete(bot.start())
            break
        except KeyboardInterrupt:
            logger.info("Received execution stop signal via console...")
            return
        except Exception as e:
            logger.critical(
                f"Fatal error in core execution loop (attempt {attempt}/{max_attempts}): {e}",
                exc_info=True,
            )
            if attempt < max_attempts:
                delay = 15 * attempt
                logger.info(f"Retrying startup in {delay}s...")
                time.sleep(delay)
            else:
                logger.critical(
                    "Startup failed after all attempts — exiting with non-zero code "
                    "so the platform's restart policy applies backoff."
                )
                sys.exit(1)

    try:
        loop.run_until_complete(bot.shutdown_event.wait())
    except KeyboardInterrupt:
        logger.info("Received execution stop signal via console...")
    finally:
        if bot is not None and bot.is_running:
            with suppress(Exception):
                loop.run_until_complete(bot.stop())
        logger.info("Bot process completely terminated")

if __name__ == "__main__":
    main()
