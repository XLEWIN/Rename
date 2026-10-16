
import re, os, time
from os import environ, getenv
id_pattern = re.compile(r'^.\d+$') 

#------------------------------------
#  PaaS-safe environment helpers
#------------------------------------
# Railway / Render / Heroku inject these variables at runtime.  A variable
# that exists but is empty ("") is a very common dashboard accident and used
# to crash the bot with `ValueError: invalid literal for int()`.
# These helpers treat an empty/whitespace value as "not set" and fall back to
# the default instead of killing the process.

def _clean(key):
    """Return the stripped value of an env var, or None when empty/absent."""
    value = os.environ.get(key)
    if value is None:
        return None
    value = value.strip()
    return value or None

def env_str(key, default=""):
    return _clean(key) or default

def env_int(key, default=0, *alias_keys):
    """Read an int; `alias_keys` are checked in order before the default."""
    for candidate in (key,) + alias_keys:
        raw = _clean(candidate)
        if raw is None:
            continue
        try:
            return int(raw)
        except ValueError:
            # Broken value -> keep scanning, finally fall back to the default
            pass
    return default

def env_float(key, default=0.0, *alias_keys):
    for candidate in (key,) + alias_keys:
        raw = _clean(candidate)
        if raw is None:
            continue
        try:
            return float(raw)
        except ValueError:
            pass
    return default

def env_bool(key, default=False, *alias_keys):
    """Truthy parsing that actually understands 'false' / '0' / 'no'."""
    for candidate in (key,) + alias_keys:
        raw = _clean(candidate)
        if raw is None:
            continue
        return raw.lower() in {"1", "true", "t", "yes", "y", "on"}
    return default

#------------------------------------

class Config(object):
    # ================== Telegram API ==================
    API_ID           = os.environ.get("API_ID", "28264594")
    API_HASH         = os.environ.get("API_HASH", "94ca8a089020a2290fd29a41f18acb94")

    # ------------------ Bot Tokens ------------------
    # Space separated -> multiple bots can be started from one service.
    BOT_TOKENS       = os.environ.get("BOT_TOKENS", "7809338788:AAF1sh7Ovwcor8q8n7nAVAGXdLwZrGiRqRY").split()
    BOT_TOKEN        = BOT_TOKENS[0] if BOT_TOKENS else os.environ.get("BOT_TOKEN", "")
    USER_SESSION_STRING = env_str("USER_SESSION_STRING", "")

    #------------------------------------

    # ================== Database ==================
    DB_NAME          = env_str("DB_NAME", "Renamer")
    DB_URL           = env_str("DB_URL", "mongodb+srv://nijaw84832_db_user:QQGS6thzuZB8QVm3@cluster0.peeuekl.mongodb.net/?retryWrites=true&w=majority")

    #------------------------------------

    # ================== Bot Uptime ==================
    BOT_UPTIME       = time.time()

    #------------------------------------

    # ================== Media / Pictures ==================
    START_PIC        = env_str("START_PIC", "https://i.ibb.co/QFN7k0hB/photo-2026-05-02-19-03-00-7635372024495341596.jpg")
    FORCE_PIC        = env_str("FORCE_PIC", "https://i.ibb.co/VcyqZ1cj/photo-2026-05-02-18-57-28-7635370602861166620.jpg")
    ABOUT_PIC        = env_str("ABOUT_PIC", "https://i.ibb.co/DPG5NNTF/photo-2026-05-21-14-18-33-7642349340601614360.jpg")

    #------------------------------------

    # ================== Owner & Admins ==================
    OWNER_ID         = env_int("OWNER_ID", 8258172543)
    ADMINS           = [8093380715]      # Populated from DB at startup
    ADMIN            = [8093380715]      # Populated from DB at startup

    #------------------------------------

    # ================== Channels & Logging ==================
    LOG_CHANNEL                 = env_int("LOG_CHANNEL", -1002530917983)
    DUMP_CHANNEL                = env_int("DUMP_CHANNEL", -1002896839922)
    DUMP                        = True
    BOT_CHANNEL_NAME            = env_str("BOT_CHANNEL_NAME", "ᴇɴᴛᴇʀ")
    BOT_CHANNEL_USERNAME        = env_str("BOT_CHANNEL_USERNAME", "ShadowedTomb")
    SUPPORT_CHANNEL_NAME        = env_str("SUPPORT_CHANNEL_NAME", "ɢᴇᴛ ʜᴇʟᴘ")
    SUPPORT_CHANNEL_USERNAME    = env_str("SUPPORT_CHANNEL_USERNAME", "iAvoidGay")

    # ================== Bot Identity ==================
    BOT_NAME                    = env_str("BOT_NAME", "AutoRename")
    BOT_USERNAME                = env_str("BOT_USERNAME", "Kaorukowaguri_bot")
    OWNER_NAME                  = env_str("OWNER_NAME", "レクシー")
    OWNER_USERNAME              = env_str("OWNER_USERNAME", "NyxRexy")
    DEVELOPER_USERNAME          = env_str("DEVELOPER_USERNAME", "NyxRexy")
    DEVELOPER_NAME              = env_str("DEVELOPER_NAME", "レクシー")

    # ================== Owner Payment / Contact ==================
    OWNER_LNK                   = env_str('OWNER_LNK', 'https://t.me/Hail_Arka')

    #------------------------------------

    # ================== Token & Limits ==================
    PREMIUM_USERS_PER_PAGE      = 15
    SHORTENER_TOKEN_GEN         = 250
    DEFAULT_TOKEN               = 200
    ADMIN_OR_PREMIUM_TASK_LIMIT = 2
    CON_LIMIT_ADMIN             = 2
    NORMAL_TASK_LIMIT           = 2
    GLOBAL_CONCURRENT_LIMIT     = env_int("GLOBAL_CONCURRENT_LIMIT", 50)
    # Max simultaneous per-track audio encodes (2 avoids CPU throttle on small VPS)
    AUDIO_ENCODE_PARALLEL_LIMIT = env_int("AUDIO_ENCODE_PARALLEL_LIMIT", 2)
    UPDATE_TIME                 = 1
    ADMIN_MODE                  = False
    ADMIN_USAGE_MODE            = False
    EDIT_API_CONCURRENCY        = env_int("EDIT_API_CONCURRENCY", 16)
    SEND_API_CONCURRENCY        = env_int("SEND_API_CONCURRENCY", 12)

    #------------------------------------

    # ================== Flood Wait & Retries ==================
    FLOODWAIT_RETRIES           = 5
    FLOODWAIT_WAIT              = 30

    #------------------------------------

    # ================== Session & Webhook ==================
    SESSION_NAME                = "Renamer"
    # The bot doubles as the Railway web service: it binds 0.0.0.0:<port> and
    # serves /health so Railway can health-check and route traffic to it.
    #   1. WEBHOOK_PORT  - explicit override (optional)
    #   2. PORT          - injected by Railway/Render/Heroku at runtime
    #   3. 8023          - local / docker default (matches EXPOSE in Dockerfile)
    WEBHOOK                     = env_bool("WEBHOOK", True)
    WEBHOOK_PORT                = env_int("WEBHOOK_PORT", 8023, "PORT")

    # Advanced Pyrogram Tuning
    WORKERS                    = env_int("WORKERS", 12)
    CONCURRENT_TRANSMISSIONS   = env_int("CONCURRENT_TRANSMISSIONS", 8)
    SLEEP_THRESHOLD            = env_int("SLEEP_THRESHOLD", 120)

    # Retry & Timeout Settings
    MAX_RETRIES                = env_int("MAX_RETRIES", 5)
    RETRY_DELAY                = env_int("RETRY_DELAY", 3)
    MESSAGE_TIMEOUT            = env_int("MESSAGE_TIMEOUT", 30)

    # Progress Engine Rate Limiting
    PROGRESS_MIN_PERCENT_DELTA = env_float("PROGRESS_MIN_PERCENT_DELTA", 5.0)

    # ================== Local working-file cleanup ==================
    # Interrupted uploads can leave media in these temporary folders.  The
    # rename worker removes files after every normal job; this is a safety net
    # for crashes/restarts and failed upload retries.
    STORAGE_CLEANUP_ENABLED      = env_bool("STORAGE_CLEANUP_ENABLED", True)
    STORAGE_CLEANUP_MAX_AGE      = env_int("STORAGE_CLEANUP_MAX_AGE", 90 * 60)
    STORAGE_CLEANUP_INTERVAL     = env_int("STORAGE_CLEANUP_INTERVAL", 10 * 60)

    # ================== External Media APIs ==================
    TMDB_API_KEY               = env_str("TMDB_API_KEY", "50146de41ca7e0b494d11425ec59dcee")
    FANART_API_KEY             = env_str("FANART_API_KEY", "7617689151bf014863fa24c36ee495bb")

    # ================== AI / LLM ==================
    GROQ_API_KEY               = env_str("GROQ_API_KEY", "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")

    #------------------------------------

class Txt(object):
    # ================== Bot Messages ==================
    START_TXT = """<b><blockquote>›› Hᴇʏ {} ×</blockquote></b>
<blockquote><b>➤ ᴘᴜʀᴘᴏꜱᴇ ᴏꜰ ᴛʜᴇ ʙᴏᴛ : ᴛʜɪꜱ ʙᴏᴛ ᴍᴀᴋᴇꜱ ʀᴇɴᴀᴍɪɴɢ ᴀɴɪᴍᴇ ᴀɴᴅ ꜱᴇʀɪᴇꜱ ꜰɪʟᴇꜱ ᴇᴀꜱʏ ᴀɴᴅ ꜱᴛʀᴇꜱꜱ-ꜰʀᴇᴇ.</blockquote></b>"""

    # ================== About Section ==================
    ABOUT_TXT = f"""<pre>❐ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗢𝗩𝗘𝗥𝗩𝗜𝗘𝗪</pre>
<blockquote expandable>◍ <b>ᴠᴇʀꜱɪᴏɴ</b> : V2.𝟶.𝟶𝟶𝟶𝟷 ᴘʀᴏ
◍ <b>ꜱʏꜱᴛᴇᴍ</b> : ᴘᴇʀꜱɪꜱᴛᴇɴᴛ ᴊᴏʙ ᴇɴɢɪɴᴇ
◍ <b>ᴄᴏʀᴇ</b> : ʀᴇxʏ ᴘᴀʀꜱᴇ ᴄᴏʀᴇ · ᴘᴊᴇ · ᴘᴅꜰ-x
◍ <b>ᴀʀᴄʜ</b> : ᴄᴏɴᴄᴜʀʀᴇɴᴄʏ-ꜱᴀꜰᴇ · ꜰᴀᴜʟᴛ-ᴛᴏʟᴇʀᴀɴᴛ
◍ <b>ᴍᴇᴍᴏʀʏ</b> : ᴘᴇʀꜱɪꜱᴛᴇɴᴛ ꜱᴛᴀᴛᴇ ᴛʀᴀᴄᴋɪɴɢ</blockquote>
<pre>❐ 𝗨𝗡𝗜𝗧 𝗣𝗥𝗢𝗙𝗜𝗟𝗘</pre>
<blockquote expandable>◍ <b>ᴅᴇᴠᴇʟᴏᴘᴇʀ</b> : <a href="https://t.me/{Config.DEVELOPER_USERNAME}">{Config.DEVELOPER_NAME}</a>
◍ <b>ᴍᴀsᴛᴇʀ</b> :  <a href="https://t.me/{Config.OWNER_USERNAME}">{Config.OWNER_NAME}</a>
◍ <b>ᴍᴀɪɴ ᴄʜᴀɴɴᴇʟ</b> : <a href="https://t.me/{Config.BOT_CHANNEL_USERNAME}">{Config.BOT_CHANNEL_NAME}</a>
◍ <b>ꜱᴜᴘᴘᴏʀᴛ</b> : <a href="https://t.me/{Config.SUPPORT_CHANNEL_USERNAME}">{Config.SUPPORT_CHANNEL_NAME}</a>
◍ <b>ꜱᴏᴜʀᴄᴇ</b> : <a href="https://t.me/{Config.BOT_CHANNEL_USERNAME}">ᴇxᴘʟᴏʀᴇ</a></blockquote>"""

    # ================== Progress Bar Template ==================
    PROGRESS_BAR = """
<blockquote>›› {1} / {2}   •   {0}%
›› {3}/s     •     {4}</blockquote>
"""
