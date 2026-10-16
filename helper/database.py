import asyncio
import logging
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, TypeVar

import pytz
import motor.motor_asyncio
from pymongo import ReturnDocument
from pymongo.errors import (
    AutoReconnect,
    NetworkTimeout,
    ServerSelectionTimeoutError,
    OperationFailure,
)

from config import Config

logger = logging.getLogger(__name__)

SLOW_QUERY_THRESHOLD = 0.15  # seconds

# =============================================================================
# Retry Infrastructure (shared across all DB layers)
# =============================================================================
_DB_TRANSIENT_ERRORS = (AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError)
_DB_MAX_RETRIES = 3
_DB_BASE_WAIT = 0.25  # seconds
_DB_MAX_WAIT = 10.0   # seconds

# Global configuration cache
_VAR_CACHE: dict[str, Any] = {}
_VAR_CACHE_TS: dict[str, float] = {}
_VAR_CACHE_TTL: float = 60.0  # 1 minute cache for global settings


def _mongo_retry(max_retries: int = _DB_MAX_RETRIES, base_wait: float = _DB_BASE_WAIT):
    def decorator(func: Callable):
        from functools import wraps
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except _DB_TRANSIENT_ERRORS as exc:
                    last_exc = exc
                    if attempt == max_retries:
                        logger.error(
                            f"[{func.__name__}] MongoDB connection failed after "
                            f"{max_retries} retries: {exc}"
                        )
                        raise
                    wait = min(
                        base_wait * (2 ** attempt) * (0.8 + 0.4 * random.random()),
                        _DB_MAX_WAIT,
                    )
                    logger.warning(
                        f"[{func.__name__}] Transient DB error (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {wait:.2f}s: {exc}"
                    )
                    await asyncio.sleep(wait)
        return wrapper
    return decorator


@asynccontextmanager
async def _timed(label: str):
    """Context manager that logs slow DB operations."""
    t0 = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - t0
        if elapsed > SLOW_QUERY_THRESHOLD:
            logger.warning(f"SLOW_QUERY [{label}]: {elapsed:.3f}s")


class Database:
    def __init__(self, uri: str, database_name: str) -> None:
        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            maxPoolSize=100,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
        )
        _db = self._client[database_name]
        # Single canonical collection — db_premium.Rohit also points here.
        self.col = _db["user"]
        self.rename_logs = _db["rename_logs"]
        self.token_links = _db["token_links"]
        self.global_settings = _db["global_settings"]
        self.auto_delete_queue = _db["auto_delete_queue"]
        logger.info(f"Database: connected to '{database_name}.user'")

    async def ensure_indexes(self) -> None:
        """Create performance indexes. Call once on startup — idempotent."""
        try:
            # User collection
            await self.col.create_index("username")
            await self.col.create_index("token")
            await self.col.create_index("ban_status.is_banned")
            await self.col.create_index(
                [("is_premium", 1), ("premium_expiry", 1)]
            )
            await self.col.create_index("has_free_trial")

            # Rename logs
            await self.rename_logs.create_index("user_id")

            # Token links — TTL auto-delete expired links after 48h buffer
            await self.token_links.create_index("user_id")
            await self.token_links.create_index("used")
            try:
                await self.token_links.create_index(
                    "expiry", expireAfterSeconds=172800  # 48h after expiry
                )
            except Exception as e:
                # Handle IndexOptionsConflict (code 85)
                if getattr(e, "code", None) == 85:
                    logger.warning("IndexOptionsConflict for 'expiry'. Recreating index...")
                    await self.token_links.drop_index("expiry_1")
                    await self.token_links.create_index(
                        "expiry", expireAfterSeconds=172800
                    )
                else:
                    raise

            # Auto-Delete Queue
            await self.auto_delete_queue.create_index("delete_at")

            logger.info("Database: indexes ensured ✅")
        except Exception as exc:
            logger.error(f"Database.ensure_indexes: {exc}", exc_info=True)

    async def ping(self) -> bool:
        try:
            await self._client.admin.command("ping")
            return True
        except Exception as exc:
            logger.error(f"Database.ping failed: {exc}")
            return False

    # =========================================================================
    # User lifecycle
    # =========================================================================

    def _new_user_doc(
        self,
        user_id: int,
        username: str = "",
        first_name: str = "",
    ) -> dict:
        return {
            "_id": user_id,
            "join_date": datetime.now(pytz.utc).isoformat(),
            "file_id": None,
            "caption": None,
            "metadata": False,
            "metadata_code": "Telegram : @DARKXSIDE78",
            "format_template": None,
            "rename_count": 0,
            "daily_count": 0,
            "first_name": first_name,
            "username": username,
            "token_tasks": [],
            "is_premium": False,
            "premium_expiry": None,
            "token": Config.DEFAULT_TOKEN,
            "has_free_trial": False,
            "media_type": "document",
            "anilist_thumb": False,
            "thumb_engine": "off",
            "audio_conv_enabled": False,
            "audio_format": "aac",
            "audio_bitrate": "128k",
            "ban_status": {"is_banned": False, "ban_duration": 0, "ban_reason": ""},
        }

    @_mongo_retry()
    async def is_user_exist(self, user_id: Any) -> bool:
        try:
            async with _timed(f"is_user_exist({user_id})"):
                doc = await self.col.find_one({"_id": int(user_id)}, {"_id": 1})
            return doc is not None
        except Exception as exc:
            logger.error(f"is_user_exist({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def add_user(
        self,
        user_id: Any,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        token: Optional[int] = None,
    ) -> None:
        """
        Upsert — safe to call multiple times, never creates duplicates.
        On first insert: full document with defaults.
        On subsequent calls: updates username/first_name if provided.
        """
        uid = int(user_id)
        try:
            doc = self._new_user_doc(uid, username or "", first_name or "")
            if token is not None:
                doc["token"] = token
            set_on_insert = {k: v for k, v in doc.items() if k not in ("username", "first_name")}
            update: dict = {"$setOnInsert": set_on_insert}
            set_always: dict = {}
            if username is not None:
                set_always["username"] = username
            if first_name is not None:
                set_always["first_name"] = first_name
            if set_always:
                update["$set"] = set_always
            await self.col.update_one({"_id": uid}, update, upsert=True)
        except Exception as exc:
            logger.error(f"add_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def get_user(self, user_ref: Any) -> Optional[dict]:
        try:
            if isinstance(user_ref, str) and user_ref.startswith("@"):
                return await self.col.find_one({"username": user_ref[1:]})
            return await self.col.find_one({"_id": int(user_ref)})
        except Exception as exc:
            logger.error(f"get_user({user_ref}): {exc}")
            return None

    @_mongo_retry()
    async def update_user(self, user_id: Any, update_data: dict) -> None:
        try:
            await self.col.update_one({"_id": int(user_id)}, update_data)
        except Exception as exc:
            logger.error(f"update_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def update_user_pref(self, user_id: Any, key: str, value: Any) -> None:
        """
        Atomically update a single user preference field.
        Clears the in-memory cache to ensure consistency.
        """
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {key: value}},
                upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"update_user_pref({user_id}, {key}): {exc}")
            raise

    @_mongo_retry()
    async def add_referral(self, referrer_id: Any, new_user_id: int) -> None:
        try:
            await self.col.update_one(
                {"_id": int(referrer_id)},
                {"$addToSet": {"referrals": new_user_id}},
            )
        except Exception as exc:
            logger.error(f"add_referral({referrer_id}): {exc}")
            raise

    @_mongo_retry()
    async def get_user_by_referral_code(self, code: str) -> Optional[dict]:
        try:
            return await self.col.find_one({"referral_code": code})
        except Exception as exc:
            logger.error(f"get_user_by_referral_code({code}): {exc}")
            return None

    @_mongo_retry()
    async def delete_user(self, user_id: Any) -> None:
        try:
            await self.col.delete_one({"_id": int(user_id)})
        except Exception as exc:
            logger.error(f"delete_user({user_id}): {exc}")

    @_mongo_retry()
    async def total_users_count(self) -> int:
        try:
            return await self.col.count_documents({})
        except Exception as exc:
            logger.error(f"total_users_count: {exc}")
            return 0

    def get_all_users(self):
        return self.col.find({})

    # =========================================================================
    # Premium — FIXED: was using self.premium_data (never defined → AttributeError)
    # =========================================================================

    @_mongo_retry()
    async def is_premium(self, user_id: Any) -> bool:
        try:
            async with _timed(f"is_premium({user_id})"):
                doc = await self.col.find_one(
                    {"_id": int(user_id)}, {"is_premium": 1, "premium_expiry": 1}
                )
            if not doc or not doc.get("is_premium"):
                return False
            expiry = doc.get("premium_expiry")
            if expiry:
                if isinstance(expiry, datetime) and expiry.tzinfo is None:
                    expiry = pytz.utc.localize(expiry)
                if isinstance(expiry, datetime) and expiry < datetime.now(pytz.utc):
                    await self.remove_premium_user(user_id)
                    return False
            return True
        except Exception as exc:
            logger.error(f"is_premium({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def add_premium_user(self, user_id: Any, expiry: datetime) -> None:
        try:
            if isinstance(expiry, datetime) and expiry.tzinfo is None:
                expiry = pytz.utc.localize(expiry)
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {"is_premium": True, "premium_expiry": expiry}},
                upsert=True,
            )
            logger.info(f"add_premium_user({user_id}): until {expiry}")
        except Exception as exc:
            logger.error(f"add_premium_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def remove_premium_user(self, user_id: Any) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
        except Exception as exc:
            logger.error(f"remove_premium_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def remove_expired_premium_users(self) -> int:
        try:
            result = await self.col.update_many(
                {"is_premium": True, "premium_expiry": {"$lte": datetime.now(pytz.utc)}},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
            return result.modified_count
        except Exception as exc:
            logger.error(f"remove_expired_premium_users: {exc}")
            return 0

    # =========================================================================
    # Ban system
    # =========================================================================

    @_mongo_retry()
    async def is_user_banned(self, user_id: Any) -> tuple[bool, Optional[str]]:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"ban_status": 1})
            if not doc:
                return False, None
            bs = doc.get("ban_status", {})
            return bs.get("is_banned", False), bs.get("ban_reason", "No reason provided")
        except Exception as exc:
            logger.error(f"is_user_banned({user_id}): {exc}")
            return False, None

    @_mongo_retry()
    async def ban_user(self, user_id: Any, reason: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {
                    "ban_status.is_banned": True,
                    "ban_status.banned_on": datetime.now(pytz.utc).isoformat(),
                    "ban_status.ban_reason": reason,
                }}
            )
        except Exception as exc:
            logger.error(f"ban_user({user_id}): {exc}")
            raise
            
    @_mongo_retry()
    async def unban_user(self, user_id: Any) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {
                    "ban_status.is_banned": False,
                    "ban_status.banned_on": None,
                    "ban_status.ban_reason": None,
                }}
            )
        except Exception as exc:
            logger.error(f"unban_user({user_id}): {exc}")
            raise

    # =========================================================================
    # Token system
    # =========================================================================

    @_mongo_retry()
    async def get_token(self, user_id: Any) -> int:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"token": 1})
            return int(doc.get("token", Config.DEFAULT_TOKEN)) if doc else Config.DEFAULT_TOKEN
        except Exception as exc:
            logger.error(f"get_token({user_id}): {exc}")
            return Config.DEFAULT_TOKEN

    @_mongo_retry()
    async def set_token(self, user_id: Any, token: int) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {"token": int(token)}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_token({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def add_token(self, user_id: Any, amount: int) -> int:
        try:
            result = await self.col.find_one_and_update(
                {"_id": int(user_id)},
                {"$inc": {"token": amount}},
                return_document=ReturnDocument.AFTER,
            )
            return result.get("token", 0) if result else 0
        except Exception as exc:
            logger.error(f"add_token({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def remove_token(self, user_id: Any, amount: int) -> int:
        try:
            result = await self.col.find_one_and_update(
                {"_id": int(user_id)},
                [{"$set": {"token": {"$max": [0, {"$subtract": ["$token", amount]}]}}}],
                return_document=ReturnDocument.AFTER,
            )
            return result.get("token", 0) if result else 0
        except Exception as exc:
            logger.error(f"remove_token({user_id}): {exc}")
            raise
            
    @_mongo_retry()
    async def deduct_token(self, user_id: Any, amount: int = 1) -> bool:
        """
        Atomically deduct tokens if balance >= amount.
        Uses find_one_and_update for single-roundtrip atomicity.
        Returns True on success, False if insufficient balance.
        """
        try:
            async with _timed(f"deduct_token({user_id})"):
                result = await self.col.find_one_and_update(
                    {"_id": int(user_id), "token": {"$gte": amount}},
                    {"$inc": {"token": -amount}},
                    return_document=ReturnDocument.AFTER,
                )
            if result:
                logger.info(
                    f"deduct_token({user_id}): -{amount}, remaining={result.get('token', '?')}"
                )
                return True
            logger.warning(f"deduct_token({user_id}): insufficient tokens")
            return False
        except Exception as exc:
            logger.error(f"deduct_token({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def refund_token(self, user_id: Any, amount: int = 1) -> None:
        """Best-effort token refund. Logs but never raises."""
        try:
            result = await self.col.find_one_and_update(
                {"_id": int(user_id)},
                {"$inc": {"token": amount}},
                return_document=ReturnDocument.AFTER,
            )
            if result:
                logger.info(
                    f"refund_token({user_id}): +{amount}, balance={result.get('token', '?')}"
                )
            else:
                logger.warning(f"refund_token({user_id}): user not found")
        except Exception as exc:
            logger.error(f"refund_token({user_id}): {exc}")

    # =========================================================================
    # Token links
    # =========================================================================

    @_mongo_retry()
    async def create_token_link(self, user_id: Any, token_id: str, tokens: int) -> None:
        expiry = datetime.now(pytz.utc) + timedelta(hours=24)
        try:
            await self.token_links.update_one(
                {"_id": token_id},
                {"$set": {
                    "user_id": int(user_id),
                    "tokens": tokens,
                    "used": False,
                    "expiry": expiry,
                    "created_at": datetime.now(pytz.utc),
                }},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"create_token_link({token_id}): {exc}")
            raise

    @_mongo_retry()
    async def get_token_link(self, token_id: str) -> Optional[dict]:
        try:
            return await self.token_links.find_one({"_id": token_id})
        except Exception as exc:
            logger.error(f"get_token_link({token_id}): {exc}")
            return None

    @_mongo_retry()
    async def mark_token_used(self, token_id: str) -> None:
        try:
            await self.token_links.update_one(
                {"_id": token_id},
                {"$set": {"used": True, "used_at": datetime.now(pytz.utc)}},
            )
        except Exception as exc:
            logger.error(f"mark_token_used({token_id}): {exc}")
            raise

    # =========================================================================
    # Rename history
    # =========================================================================

    @_mongo_retry()
    async def add_rename_history(
        self,
        user_id: Any,
        original_name: str,
        renamed_name: str,
        rename_timestamp: Optional[datetime] = None,
    ) -> None:
        ts = rename_timestamp or datetime.now(pytz.utc)
        try:
            await self.rename_logs.update_one(
                {"_id": int(user_id)},
                {
                    "$push": {"history": {
                        "original_name": original_name,
                        "renamed_name": renamed_name,
                        "timestamp": ts,
                    }},
                    "$inc": {"rename_count": 1},
                    "$set": {"last_rename": ts, "user_id": int(user_id)},
                },
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"add_rename_history({user_id}): {exc}")

    @_mongo_retry()
    async def get_rename_history(self, user_id: Any) -> list:
        try:
            doc = await self.rename_logs.find_one({"_id": int(user_id)})
            return doc.get("history", []) if doc else []
        except Exception as exc:
            logger.error(f"get_rename_history({user_id}): {exc}")
            return []

    # =========================================================================
    # User preferences (each is a simple get/set pair)
    # =========================================================================

    async def get_thumbnail(self, user_id: Any) -> Optional[str]:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"file_id": 1})
            return doc.get("file_id") if doc else None
        except Exception as exc:
            logger.error(f"get_thumbnail({user_id}): {exc}")
            return None

    async def set_thumbnail(self, user_id: Any, file_id: Any) -> None:
        try:
            await self.col.update_one({"_id": int(user_id)}, {"$set": {"file_id": file_id}})
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_thumbnail({user_id}): {exc}")

    async def get_caption(self, user_id: Any) -> Optional[str]:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"caption": 1})
            return doc.get("caption") if doc else None
        except Exception as exc:
            logger.error(f"get_caption({user_id}): {exc}")
            return None

    async def set_caption(self, user_id: Any, caption: str) -> None:
        try:
            await self.col.update_one({"_id": int(user_id)}, {"$set": {"caption": caption}})
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_caption({user_id}): {exc}")

    async def get_format_template(self, user_id: Any) -> Optional[str]:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"format_template": 1})
            return doc.get("format_template") if doc else None
        except Exception as exc:
            logger.error(f"get_format_template({user_id}): {exc}")
            return None

    async def set_format_template(self, user_id: Any, fmt: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"format_template": fmt}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_format_template({user_id}): {exc}")

    async def get_media_preference(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"media_type": 1})
            return doc.get("media_type", "document") if doc else "document"
        except Exception as exc:
            logger.error(f"get_media_preference({user_id}): {exc}")
            return "document"

    async def set_media_preference(self, user_id: Any, media_type: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"media_type": media_type}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_media_preference({user_id}): {exc}")

    async def get_metadata_source(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"metadata_source": 1})
            return doc.get("metadata_source", "filename") if doc else "filename"
        except Exception as exc:
            logger.error(f"get_metadata_source({user_id}): {exc}")
            return "filename"

    async def set_metadata_source(self, user_id: Any, source: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"metadata_source": source}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_metadata_source({user_id}): {exc}")

    async def get_metadata(self, user_id: Any) -> Any:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"metadata": 1})
            return doc.get("metadata", "Off") if doc else "Off"
        except Exception as exc:
            logger.error(f"get_metadata({user_id}): {exc}")
            return "Off"

    async def set_metadata(self, user_id: Any, metadata: Any) -> None:
        try:
            await self.col.update_one({"_id": int(user_id)}, {"$set": {"metadata": metadata}})
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_metadata({user_id}): {exc}")

    async def get_metadata_purge(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"metadata_purge": 1})
            return bool(doc.get("metadata_purge", False)) if doc else False
        except Exception as exc:
            logger.error(f"get_metadata_purge({user_id}): {exc}")
            return False

    async def set_metadata_purge(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"metadata_purge": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_metadata_purge({user_id}): {exc}")

    async def get_track_renaming(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"track_renaming": 1})
            return bool(doc.get("track_renaming", False)) if doc else False
        except Exception as exc:
            logger.error(f"get_track_renaming({user_id}): {exc}")
            return False

    async def set_track_renaming(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"track_renaming": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_track_renaming({user_id}): {exc}")

    async def set_intro_sub(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"intro_sub": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_intro_sub({user_id}): {exc}")

    async def set_intro_text(self, user_id: Any, text: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"intro_text": text}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_intro_text({user_id}): {exc}")

    async def set_intro_color(self, user_id: Any, color_hex: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"intro_color": color_hex}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_intro_color({user_id}): {exc}")

    async def set_intro_pos(self, user_id: Any, pos: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"intro_pos": pos}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_intro_pos({user_id}): {exc}")

    async def set_sub_pos(self, user_id: Any, pos: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_pos": pos}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_pos({user_id}): {exc}")

    async def set_sub_font_size(self, user_id: Any, size: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_font_size": size}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_font_size({user_id}): {exc}")

    async def set_sub_shadow(self, user_id: Any, shadow: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_shadow": shadow}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_shadow({user_id}): {exc}")

    async def set_intro_timer(self, user_id: Any, seconds: Any) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"intro_timer": str(seconds)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_intro_timer({user_id}): {exc}")

    async def set_sub_color(self, user_id: Any, color: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_color": color}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_color({user_id}): {exc}")

    async def set_sub_bord_status(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_bord_status": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_bord_status({user_id}): {exc}")

    async def set_sub_bord_width(self, user_id: Any, width: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"sub_bord_width": width}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_sub_bord_width({user_id}): {exc}")

    async def get_anilist_thumb_status(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"anilist_thumb": 1})
            return bool(doc.get("anilist_thumb")) if doc else False
        except Exception as exc:
            logger.error(f"get_anilist_thumb_status({user_id}): {exc}")
            return False

    async def set_anilist_thumb_status(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"anilist_thumb": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_anilist_thumb_status({user_id}): {exc}")

    async def get_thumb_engine(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"thumb_engine": 1, "anilist_thumb": 1})
            if not doc: return "off"
            engine = doc.get("thumb_engine")
            if engine: return engine
            # Backward compatibility
            return "anilist" if doc.get("anilist_thumb") else "off"
        except Exception as exc:
            logger.error(f"get_thumb_engine({user_id}): {exc}")
            return "off"

    async def set_thumb_engine(self, user_id: Any, engine: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, 
                {"$set": {"thumb_engine": engine, "anilist_thumb": (engine != "off")}}, 
                upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_thumb_engine({user_id}): {exc}")

    # ── Audio Conversion Settings ─────────────────────────────────────────────

    async def get_audio_conv_enabled(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"audio_conv_enabled": 1})
            return bool(doc.get("audio_conv_enabled", False)) if doc else False
        except Exception as exc:
            logger.error(f"get_audio_conv_enabled({user_id}): {exc}")
            return False

    async def set_audio_conv_enabled(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"audio_conv_enabled": bool(status)}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_audio_conv_enabled({user_id}): {exc}")

    async def get_audio_format(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"audio_format": 1})
            return doc.get("audio_format", "aac") if doc else "aac"
        except Exception as exc:
            logger.error(f"get_audio_format({user_id}): {exc}")
            return "aac"

    async def set_audio_format(self, user_id: Any, fmt: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"audio_format": fmt}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_audio_format({user_id}): {exc}")

    async def get_audio_bitrate(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one({"_id": int(user_id)}, {"audio_bitrate": 1})
            return doc.get("audio_bitrate", "128k") if doc else "128k"
        except Exception as exc:
            logger.error(f"get_audio_bitrate({user_id}): {exc}")
            return "128k"

    async def set_audio_bitrate(self, user_id: Any, bitrate: str) -> None:
        try:
            await self.col.update_one(
                {"_id": int(user_id)}, {"$set": {"audio_bitrate": bitrate}}, upsert=True
            )
            self.clear_user_cache(user_id)
        except Exception as exc:
            logger.error(f"set_audio_bitrate({user_id}): {exc}")

    def clear_user_cache(self, user_id: Any) -> None:
        """Invalidate the in-memory preference cache for a specific user."""
        try:
            uid = int(user_id)
            self._USER_CACHE.pop(uid, None)
        except Exception:
            pass

    # ---- Metadata tag fields (title/author/artist/audio/subtitle/video/encoded_by/custom_tag/commentz) ----

    async def get_title(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"title": 1})
        return doc.get("title", "") if doc else ""

    async def set_title(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"title": value}})

    async def get_author(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"author": 1})
        return doc.get("author", "") if doc else ""

    async def set_author(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"author": value}})

    async def get_artist(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"artist": 1})
        return doc.get("artist", "") if doc else ""

    async def set_artist(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"artist": value}})

    async def get_audio(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"audio": 1})
        return doc.get("audio", "") if doc else ""

    async def set_audio(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"audio": value}})

    async def get_subtitle(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"subtitle": 1})
        return doc.get("subtitle", "") if doc else ""

    async def set_subtitle(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"subtitle": value}})

    async def get_video(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"video": 1})
        return doc.get("video", "") if doc else ""

    async def set_video(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"video": value}})

    async def get_encoded_by(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"encoded_by": 1})
        return doc.get("encoded_by", "") if doc else ""

    async def set_encoded_by(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"encoded_by": value}})

    async def get_album(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"album": 1})
        return doc.get("album", "") if doc else ""

    async def set_album(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"album": value}})

    async def get_genre(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"genre": 1})
        return doc.get("genre", "") if doc else ""

    async def set_genre(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"genre": value}})

    async def get_composer(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"composer": 1})
        return doc.get("composer", "") if doc else ""

    async def set_composer(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"composer": value}})

    async def get_publisher(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"publisher": 1})
        return doc.get("publisher", "") if doc else ""

    async def set_publisher(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"publisher": value}})

    async def get_copyright(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"copyright": 1})
        return doc.get("copyright", "") if doc else ""

    async def set_copyright(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"copyright": value}})

    async def get_custom_tag(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"custom_tag": 1})
        return doc.get("custom_tag", "") if doc else ""

    async def set_custom_tag(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"custom_tag": value}})

    async def get_commentz(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"commentz": 1})
        return doc.get("commentz", "") if doc else ""

    async def set_commentz(self, user_id: Any, value: str) -> None:
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"commentz": value}})

    # ---- PDF settings ----

    async def get_pdf_lock_password(self, user_id: Any) -> Optional[str]:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_lock_password": 1})
        return doc.get("pdf_lock_password") if doc else None

    async def set_pdf_lock_password(self, user_id: Any, password: str) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_lock_password": password}}, upsert=True
        )

    async def get_pdf_banner(self, user_id: Any) -> Optional[str]:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_banner": 1})
        return doc.get("pdf_banner") if doc else None

    async def set_pdf_banner(self, user_id: Any, file_id: str) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_banner": file_id}}, upsert=True
        )

    async def get_pdf_banner_placement(self, user_id: Any) -> str:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_banner_placement": 1})
        return doc.get("pdf_banner_placement", "first") if doc else "first"

    async def set_pdf_banner_placement(self, user_id: Any, placement: str) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_banner_placement": placement}}, upsert=True
        )

    async def get_pdf_banner_mode(self, user_id: Any) -> bool:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_banner_mode": 1})
        return bool(doc.get("pdf_banner_mode")) if doc else False

    async def set_pdf_banner_mode(self, user_id: Any, mode: bool) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_banner_mode": bool(mode)}}, upsert=True
        )

    async def get_pdf_lock_mode(self, user_id: Any) -> bool:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_lock_mode": 1})
        return bool(doc.get("pdf_lock_mode")) if doc else False

    async def set_pdf_lock_mode(self, user_id: Any, mode: bool) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_lock_mode": bool(mode)}}, upsert=True
        )

    async def get_pdf_remove_pages(self, user_id: Any) -> list:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_remove_pages": 1})
        return doc.get("pdf_remove_pages", []) if doc else []

    async def set_pdf_remove_pages(self, user_id: Any, pages: list) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_remove_pages": pages}}, upsert=True
        )

    async def get_pdf_banner_last(self, user_id: Any) -> Optional[str]:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_banner_last": 1})
        return doc.get("pdf_banner_last") if doc else None

    async def set_pdf_banner_last(self, user_id: Any, file_id: Optional[str]) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_banner_last": file_id}}, upsert=True
        )

    async def get_pdf_banner_url(self, user_id: Any) -> Optional[str]:
        doc = await self.col.find_one({"_id": int(user_id)}, {"pdf_banner_url": 1})
        return doc.get("pdf_banner_url") if doc else None

    async def set_pdf_banner_url(self, user_id: Any, url: Optional[str]) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"pdf_banner_url": url}}, upsert=True
        )

    async def get_cbz_to_pdf_mode(self, user_id: Any) -> bool:
        doc = await self.col.find_one({"_id": int(user_id)}, {"cbz_to_pdf_mode": 1})
        return bool(doc.get("cbz_to_pdf_mode")) if doc else False
    async def set_cbz_to_pdf_mode(self, user_id: Any, mode: bool) -> None:
        await self.col.update_one(
            {"_id": int(user_id)}, {"$set": {"cbz_to_pdf_mode": bool(mode)}}, upsert=True
        )

    # =========================================================================
    # Batch preference loader — single round-trip for the rename pipeline
    # =========================================================================

    _USER_CACHE: dict[int, tuple[dict, float]] = {}
    _USER_CACHE_TTL: float = 30.0  # seconds

    @_mongo_retry()
    async def get_user_prefs_batch(self, user_id: Any) -> dict:
        """
        Fetch ALL rename-relevant user preferences in a single MongoDB query.
        Implements a short-lived memory cache (30s) to handle rapid batch bursts.
        """
        uid = int(user_id)
        now = time.monotonic()
        
        # Performance: Serve from memory cache if within TTL
        if uid in self._USER_CACHE:
            cached_data, ts = self._USER_CACHE[uid]
            if now - ts < self._USER_CACHE_TTL:
                return cached_data

        try:
            fields = {
                "format_template": 1, "media_type": 1, "metadata_source": 1,
                "caption": 1, "file_id": 1, "anilist_thumb": 1, "thumb_engine": 1, "is_premium": 1,
                "premium_expiry": 1, "verify_expiry": 1, "token": 1,
                # ── On/Off toggle ── was missing, causing metadata to never apply
                "metadata": 1, "metadata_purge": 1, "track_renaming": 1, "intro_sub": 1,
                # Metadata tag fields
                "title": 1, "video": 1, "audio": 1, "subtitle": 1,
                "artist": 1, "author": 1, "encoded_by": 1,
                "album": 1, "genre": 1, "composer": 1, "publisher": 1, "copyright": 1,
                "custom_tag": 1, "commentz": 1,
                # Intro Sub customization
                "intro_text": 1, "intro_color": 1, "intro_pos": 1,
                "intro_timer": 1, "sub_color": 1, "sub_bord_status": 1, "sub_bord_width": 1, "sub_bord_color": 1,
                "sub_pos": 1, "sub_font_size": 1, "sub_shadow": 1,
                # PDF settings
                "pdf_banner_mode": 1, "pdf_banner": 1,
                "pdf_banner_last": 1, "pdf_banner_url": 1,
                "pdf_lock_mode": 1, "pdf_lock_password": 1,
                "pdf_banner_placement": 1, "cbz_to_pdf_mode": 1,
                # Audio conversion settings
                "audio_conv_enabled": 1, "audio_format": 1, "audio_bitrate": 1,
            }
            async with _timed(f"get_user_prefs_batch({user_id})"):
                doc = await self.col.find_one({"_id": uid}, fields) or {}
            
            # Update cache
            self._USER_CACHE[uid] = (doc, now)

            # Periodic cache pruning
            if len(self._USER_CACHE) > 2000:
                self._USER_CACHE.clear()

            # Populate thumb_engine fallback for backward compatibility
            if "thumb_engine" not in doc or doc["thumb_engine"] is None:
                doc["thumb_engine"] = "anilist" if doc.get("anilist_thumb") else "off"

            return doc
        except Exception as exc:
            logger.error(f"get_user_prefs_batch({user_id}): {exc}")
            return {}


    # =========================================================================
    # Admin management — MongoDB-backed dynamic admin list
    # =========================================================================

    @property
    def admin_col(self):
        return self._client[Config.DB_NAME]["admins"]

    @_mongo_retry()
    async def add_admin(self, user_id: int, added_by: int, username: str = "", first_name: str = "") -> bool:
        try:
            await self.admin_col.update_one(
                {"_id": int(user_id)},
                {"$set": {
                    "added_by": added_by,
                    "added_at": datetime.now(pytz.utc),
                    "username": username,
                    "first_name": first_name,
                }},
                upsert=True,
            )
            return True
        except Exception as exc:
            logger.error(f"add_admin({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def remove_admin(self, user_id: int) -> bool:
        try:
            result = await self.admin_col.delete_one({"_id": int(user_id)})
            return result.deleted_count > 0
        except Exception as exc:
            logger.error(f"remove_admin({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def is_admin(self, user_id: int) -> bool:
        if user_id == Config.OWNER_ID:
            return True
        try:
            doc = await self.admin_col.find_one({"_id": int(user_id)}, {"_id": 1})
            return doc is not None
        except Exception as exc:
            logger.error(f"is_admin({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def get_all_admins(self) -> list[dict]:
        try:
            docs = await self.admin_col.find().to_list(length=200)
            return docs
        except Exception as exc:
            logger.error(f"get_all_admins: {exc}")
            return []

    @_mongo_retry()
    async def get_admin_ids(self) -> list[int]:
        try:
            docs = await self.admin_col.find({}, {"_id": 1}).to_list(length=200)
            ids = [doc["_id"] for doc in docs]
            if Config.OWNER_ID not in ids:
                ids.append(Config.OWNER_ID)
            return ids
        except Exception as exc:
            logger.error(f"get_admin_ids: {exc}")
            return [Config.OWNER_ID]

    # =========================================================================
    # Auto-Delete Settings
    # =========================================================================

    @_mongo_retry()
    async def get_auto_delete(self) -> bool:
        try:
            doc = await self.global_settings.find_one({"_id": "auto_delete"})
            return bool(doc.get("status", False)) if doc else False
        except Exception as exc:
            logger.error(f"get_auto_delete: {exc}")
            return False

    @_mongo_retry()
    async def set_auto_delete(self, status: bool) -> None:
        try:
            await self.global_settings.update_one(
                {"_id": "auto_delete"},
                {"$set": {"status": status}},
                upsert=True
            )
        except Exception as exc:
            logger.error(f"set_auto_delete: {exc}")

    @_mongo_retry()
    async def get_del_timer(self) -> int:
        try:
            doc = await self.global_settings.find_one({"_id": "del_timer"})
            return int(doc.get("seconds", 600)) if doc else 600
        except Exception as exc:
            logger.error(f"get_del_timer: {exc}")
            return 600

    @_mongo_retry()
    async def set_del_timer(self, seconds: int) -> None:
        try:
            await self.global_settings.update_one(
                {"_id": "del_timer"},
                {"$set": {"seconds": seconds}},
                upsert=True
            )
        except Exception as exc:
            logger.error(f"set_del_timer: {exc}")

    @_mongo_retry()
    async def schedule_auto_delete(self, chat_id: int, message_ids: list[int], delay: int) -> None:
        try:
            delete_at = datetime.now(pytz.utc) + timedelta(seconds=delay)
            await self.auto_delete_queue.insert_one({
                "chat_id": chat_id,
                "message_ids": message_ids,
                "delete_at": delete_at
            })
        except Exception as exc:
            logger.error(f"schedule_auto_delete: {exc}")

    @_mongo_retry()
    async def get_and_clear_due_deletions(self) -> list[dict]:
        try:
            now = datetime.now(pytz.utc)
            # Find all documents where delete_at is past
            cursor = self.auto_delete_queue.find({"delete_at": {"$lte": now}})
            docs = await cursor.to_list(length=100)
            
            if docs:
                doc_ids = [doc["_id"] for doc in docs]
                await self.auto_delete_queue.delete_many({"_id": {"$in": doc_ids}})
            
            return docs
        except Exception as exc:
            logger.error(f"get_and_clear_due_deletions: {exc}")
            return []


    # =========================================================================
    # Admin-mode setting (used by SystemService in admin_controls.py)
    # =========================================================================

    @_mongo_retry()
    async def get_admin_mode(self) -> bool:
        """Fetch global admin_mode flag."""
        try:
            from config import Config as _C
            doc = await self.global_settings.find_one({"_id": "admin_mode"})
            return bool(doc.get("status", _C.ADMIN_USAGE_MODE)) if doc else _C.ADMIN_USAGE_MODE
        except Exception as exc:
            logger.error(f"get_admin_mode: {exc}")
            return False

    @_mongo_retry()
    async def set_admin_mode(self, status: bool) -> None:
        """Persist global admin_mode flag."""
        try:
            await self.global_settings.update_one(
                {"_id": "admin_mode"},
                {"$set": {"status": status}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_admin_mode: {exc}")
            raise

    # =========================================================================
    # Rich statistics aggregates (used by StatsService in bot_stats.py)
    # =========================================================================

    @_mongo_retry()
    async def get_db_stats_aggregate(self) -> dict:
        """
        Single-pass aggregate for all stats dashboard fields.
        Returns a dict with all counters; callers should never need .col directly.
        """
        try:
            from datetime import timedelta
            dt_now = datetime.now(pytz.utc)

            # Parallel fire all simple counts
            (
                total_users, premium_users, banned_users, metadata_on,
                with_auto_rename, with_captions, with_thumbnails,
                document_users, video_users, token_links_count,
            ) = await asyncio.gather(
                self.col.count_documents({}),
                self.col.count_documents({"is_premium": True}),
                self.col.count_documents({"ban_status.is_banned": True}),
                self.col.count_documents({"metadata": True}),
                self.col.count_documents({"format_template": {"$exists": True, "$ne": None}}),
                self.col.count_documents({"caption": {"$exists": True, "$ne": None}}),
                self.col.count_documents({"file_id": {"$exists": True, "$ne": None}}),
                self.col.count_documents({"media_type": "document"}),
                self.col.count_documents({"media_type": "video"}),
                self.token_links.count_documents({}),
            )

            agg = await self.col.aggregate([
                {"$group": {
                    "_id": None,
                    "total_renames": {"$sum": "$rename_count"},
                    "total_storage": {"$sum": "$total_renamed_size"},
                    "total_tokens":  {"$sum": "$token"},
                }}
            ]).to_list(1)

            total_admins = await self.admin_col.count_documents({})

            active_24h = active_7d = 0
            has_last_active = await self.col.find_one({"last_active": {"$exists": True}})
            if has_last_active:
                active_24h, active_7d = await asyncio.gather(
                    self.col.count_documents({"last_active": {"$gte": dt_now - timedelta(hours=24)}}),
                    self.col.count_documents({"last_active": {"$gte": dt_now - timedelta(days=7)}}),
                )

            return {
                "total_users":      total_users,
                "premium_users":    premium_users,
                "banned_users":     banned_users,
                "metadata_on":      metadata_on,
                "with_auto_rename": with_auto_rename,
                "with_captions":    with_captions,
                "with_thumbnails":  with_thumbnails,
                "document_users":   document_users,
                "video_users":      video_users,
                "token_links":      token_links_count,
                "total_admins":     total_admins,
                "total_renames":    agg[0].get("total_renames", 0) if agg else 0,
                "total_storage":    agg[0].get("total_storage", 0) if agg else 0,
                "total_tokens":     agg[0].get("total_tokens",  0) if agg else 0,
                "active_24h":       active_24h,
                "active_7d":        active_7d,
            }
        except Exception as exc:
            logger.error(f"get_db_stats_aggregate: {exc}")
            return {}

    # =========================================================================
    # Leaderboard helpers
    # =========================================================================

    @_mongo_retry()
    async def leaderboard_top(self, pipeline: list) -> list:
        """Run a rename_logs aggregate pipeline for leaderboard display."""
        try:
            return await self.rename_logs.aggregate(pipeline).to_list(10)
        except Exception as exc:
            logger.error(f"leaderboard_top: {exc}")
            return []

    @_mongo_retry()
    async def leaderboard_user_rank(self, pipeline: list) -> list:
        """Run a single-user rank pipeline against rename_logs."""
        try:
            return await self.rename_logs.aggregate(pipeline).to_list(1)
        except Exception as exc:
            logger.error(f"leaderboard_user_rank: {exc}")
            return []

    @_mongo_retry()
    async def leaderboard_user_doc(self, user_id: int) -> Optional[dict]:
        """Fetch a user's rename_logs document."""
        try:
            return await self.rename_logs.find_one({"_id": user_id})
        except Exception as exc:
            logger.error(f"leaderboard_user_doc({user_id}): {exc}")
            return None

    @_mongo_retry()
    async def leaderboard_rank_count(self, filter_: dict) -> int:
        """Count users ranking above a threshold in rename_logs."""
        try:
            return await self.rename_logs.count_documents(filter_)
        except Exception as exc:
            logger.error(f"leaderboard_rank_count: {exc}")
            return 0

    @_mongo_retry()
    async def get_user_first_name(self, user_id: int) -> str:
        """Fetch just the first_name field for a user."""
        try:
            doc = await self.col.find_one({"_id": user_id}, {"first_name": 1})
            return doc.get("first_name", "") if doc else ""
        except Exception as exc:
            logger.error(f"get_user_first_name({user_id}): {exc}")
            return ""

    # =========================================================================
    # Referral & last_active tracking (used by start_&_cb.py)
    # =========================================================================

    @_mongo_retry()
    async def get_user_by_referral_code(self, referral_code: str) -> Optional[dict]:
        """Find the user who owns a given referral code."""
        try:
            return await self.col.find_one({"referral_code": referral_code})
        except Exception as exc:
            logger.error(f"get_user_by_referral_code({referral_code}): {exc}")
            return None

    @_mongo_retry()
    async def update_last_active(self, user_id: Any) -> None:
        """Stamp last_active on the user document."""
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$set": {"last_active": datetime.now(pytz.utc)}},
            )
        except Exception as exc:
            logger.error(f"update_last_active({user_id}): {exc}")

    @_mongo_retry()
    async def add_referral_token_bonus(self, referrer_id: Any, amount: int) -> None:
        """Atomically credit referral bonus tokens to the referrer."""
        try:
            await self.col.update_one(
                {"_id": int(referrer_id)},
                {"$inc": {"token": amount}},
            )
        except Exception as exc:
            logger.error(f"add_referral_token_bonus({referrer_id}): {exc}")
            raise

    @_mongo_retry()
    async def upsert_referral_user(self, user_id: Any, doc: dict) -> None:
        """Insert-or-update a user doc (used during referral registration)."""
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$setOnInsert": doc},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"upsert_referral_user({user_id}): {exc}")
            raise

    # =========================================================================
    # Broadcast checkpoint (used by broadcast.py)
    # =========================================================================

    @property
    def broadcast_checkpoints(self):
        """Return the broadcast_checkpoints collection (retry applied at method level)."""
        return self._client[Config.DB_NAME]["broadcast_checkpoints"]

    # =========================================================================
    # Ban helpers — used by BanService to avoid raw collection access
    # =========================================================================

    @_mongo_retry()
    async def count_banned_users(self) -> int:
        """Return total count of banned users."""
        try:
            return await self.col.count_documents({"ban_status.is_banned": True})
        except Exception as exc:
            logger.error(f"count_banned_users: {exc}")
            return 0

    @_mongo_retry()
    async def get_banlist_page(self, page: int, page_size: int = 10) -> list[dict]:
        """Return one page of banned user documents."""
        try:
            skip = page * page_size
            return await self.col.find(
                {"ban_status.is_banned": True},
                {"_id": 1, "first_name": 1, "username": 1, "ban_status": 1},
            ).skip(skip).limit(page_size).to_list(length=page_size)
        except Exception as exc:
            logger.error(f"get_banlist_page(page={page}): {exc}")
            return []

    @_mongo_retry()
    async def get_user_by_username(self, username: str) -> Optional[dict]:
        """Resolve user doc by bare username (no @ prefix)."""
        try:
            return await self.col.find_one({"username": username})
        except Exception as exc:
            logger.error(f"get_user_by_username({username}): {exc}")
            return None

    # =========================================================================
    # Global token-mode setting — used by TokenService
    # =========================================================================

    @_mongo_retry()
    async def get_token_mode(self) -> Optional[dict]:
        """Fetch the global token_mode document."""
        try:
            return await self.global_settings.find_one({"_id": "token_mode"})
        except Exception as exc:
            logger.error(f"get_token_mode: {exc}")
            return None

    @_mongo_retry()
    async def set_token_mode(self, status: bool, expiry: Optional[datetime]) -> None:
        """Persist the global token_mode status and optional expiry."""
        try:
            await self.global_settings.update_one(
                {"_id": "token_mode"},
                {"$set": {"status": status, "expiry": expiry}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_token_mode: {exc}")
            raise

    @_mongo_retry()
    async def update_rename_stats(self, user_id: Any, file_size: int) -> None:
        """Atomically increment rename counters on user document."""
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {
                    "$inc": {
                        "rename_count":       1,
                        "total_renamed_size": file_size,
                        "daily_count":        1,
                    },
                    "$max": {"max_file_size": file_size},
                },
            )
        except Exception as exc:
            logger.error(f"update_rename_stats({user_id}): {exc}")

    @_mongo_retry()
    async def get_variable(self, key: str, default: Any = None) -> Any:
        """Fetch a global configuration variable with fast in-memory caching."""
        now = time.monotonic()
        if key in _VAR_CACHE and (now - _VAR_CACHE_TS.get(key, 0) < _VAR_CACHE_TTL):
            return _VAR_CACHE[key]
            
        try:
            doc = await self.global_settings.find_one({"_id": f"var_{key}"})
            val = doc.get("value", default) if doc else default
            _VAR_CACHE[key] = val
            _VAR_CACHE_TS[key] = now
            return val
        except Exception as exc:
            logger.error(f"get_variable({key}): {exc}")
            return default

    @_mongo_retry()
    async def set_variable(self, key: str, value: Any) -> None:
        """Persist a global configuration variable and invalidate cache."""
        try:
            await self.global_settings.update_one(
                {"_id": f"var_{key}"},
                {"$set": {"value": value}},
                upsert=True,
            )
            # Invalidate cache
            _VAR_CACHE.pop(key, None)
            _VAR_CACHE_TS.pop(key, None)
        except Exception as exc:
            logger.error(f"set_variable({key}): {exc}")
            raise
# =============================================================================
# Singleton
# =============================================================================
DARKXSIDE78 = Database(Config.DB_URL, Config.DB_NAME)
