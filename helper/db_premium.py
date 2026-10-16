# =============================================================================
# helper/db_premium.py
# =============================================================================
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import pytz
import motor.motor_asyncio
from pymongo import ReturnDocument

from config import Config
from helper.database import _timed, _mongo_retry

logger = logging.getLogger(__name__)


class Rohit:

    def __init__(self, uri: str, database_name: str) -> None:
        # Motor client is lazy — no sync network call here.
        # Call await db.ping() from your startup coroutine to verify connectivity.
        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            uri,
            maxPoolSize=100,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
        )
        self.db = self._client[database_name]
        self.col = self.db["user"]
        self.token_links = self.db["token_links"]
        logger.info(f"Rohit: using database '{database_name}'")

    async def ping(self) -> bool:
        """Verify connectivity. Call this on bot startup."""
        try:
            await self._client.admin.command("ping")
            logger.info("Rohit: MongoDB ping OK")
            return True
        except Exception as exc:
            logger.error(f"Rohit: MongoDB ping failed: {exc}")
            return False

    async def ensure_indexes(self) -> None:
        """
        Create performance indexes. Call once on startup.
        Idempotent — existing indexes are silently skipped.
        """
        try:
            # Compound index for premium expiry sweep queries
            await self.col.create_index(
                [("is_premium", 1), ("premium_expiry", 1)]
            )
            await self.col.create_index("token")
            await self.col.create_index("has_free_trial")
            await self.col.create_index("username")
            await self.col.create_index("ban_status.is_banned")

            # Token links — TTL auto-delete 48h after expiry
            await self.token_links.create_index(
                "expiry", expireAfterSeconds=172800
            )
            await self.token_links.create_index("used")
            await self.token_links.create_index("user_id")
            logger.info("Rohit: indexes ensured ✅")
        except Exception as exc:
            logger.error(f"Rohit: index creation error: {exc}", exc_info=True)

    # =========================================================================
    # Internal helpers
    # =========================================================================

    @staticmethod
    def _uid(user_id: Any) -> int:
        """Normalise user_id to int. Raises ValueError on bad input."""
        return int(user_id)

    # =========================================================================
    # User lifecycle
    # =========================================================================

    def _new_user_doc(
        self,
        user_id: int,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
        token: Optional[int] = None,
    ) -> dict:
        return {
            "_id": user_id,
            "join_date": datetime.now(pytz.utc).isoformat(),
            "file_id": None,
            "caption": None,
            "metadata": True,
            "metadata_code": "Telegram : @DARKXSIDE78",
            "format_template": None,
            "rename_count": 0,
            "daily_count": 0,
            "first_name": first_name or "",
            "username": username or "",
            "token_tasks": [],
            "is_premium": False,
            "premium_expiry": None,
            "token": token if token is not None else Config.DEFAULT_TOKEN,
            "has_free_trial": False,
            "trial_expiry_time": None,
            "media_type": "document",
            "anilist_thumb": False,
            "ban_status": {"is_banned": False, "ban_duration": 0, "ban_reason": ""},
        }

    @_mongo_retry()
    async def is_user_exist(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one({"_id": self._uid(user_id)}, {"_id": 1})
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
        Upsert user — no check-then-insert race.
        On first insert: full document created with defaults.
        On subsequent calls: username/first_name updated if provided.
        """
        uid = self._uid(user_id)
        try:
            doc = self._new_user_doc(uid, username, first_name, token)
            # setOnInsert creates the doc only if it doesn't exist yet.
            # $set updates mutable fields (username, first_name) on every call.
            set_on_insert = {k: v for k, v in doc.items() if k not in ("username", "first_name")}
            set_always: dict = {}
            if username is not None:
                set_always["username"] = username
            if first_name is not None:
                set_always["first_name"] = first_name

            update: dict = {"$setOnInsert": set_on_insert}
            if set_always:
                update["$set"] = set_always

            await self.col.update_one({"_id": uid}, update, upsert=True)
        except Exception as exc:
            logger.error(f"add_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def ensure_user(
        self,
        user_id: Any,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
    ) -> None:
        """Thin alias for add_user — always safe to call, never duplicates."""
        await self.add_user(user_id, username, first_name)

    @_mongo_retry()
    async def get_user(self, user_ref: Any) -> Optional[dict]:
        """Fetch user by int ID or '@username' string."""
        try:
            if isinstance(user_ref, str) and user_ref.startswith("@"):
                return await self.col.find_one({"username": user_ref[1:]})
            return await self.col.find_one({"_id": self._uid(user_ref)})
        except Exception as exc:
            logger.error(f"get_user({user_ref}): {exc}")
            return None

    @_mongo_retry()
    async def update_user(self, user_id: Any, update_data: dict) -> None:
        """
        Apply a raw MongoDB update dict to the user document.
        BUG FIX: was missing int() cast — queries silently missed every record.
        """
        uid = self._uid(user_id)
        try:
            await self.col.update_one({"_id": uid}, update_data)
        except Exception as exc:
            logger.error(f"update_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def delete_user(self, user_id: Any) -> None:
        try:
            await self.col.delete_one({"_id": self._uid(user_id)})
        except Exception as exc:
            logger.error(f"delete_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def total_users_count(self) -> int:
        try:
            return await self.col.count_documents({})
        except Exception as exc:
            logger.error(f"total_users_count: {exc}")
            return 0

    def get_all_users(self):
        """Return a Motor cursor — iterate with async for."""
        return self.col.find({})

    # =========================================================================
    # Token system
    # =========================================================================

    @_mongo_retry()
    async def get_token(self, user_id: Any) -> int:
        try:
            doc = await self.col.find_one({"_id": self._uid(user_id)}, {"token": 1})
            return int(doc.get("token", Config.DEFAULT_TOKEN)) if doc else Config.DEFAULT_TOKEN
        except Exception as exc:
            logger.error(f"get_token({user_id}): {exc}")
            return Config.DEFAULT_TOKEN

    @_mongo_retry()
    async def set_token(self, user_id: Any, token: int) -> None:
        try:
            await self.col.update_one(
                {"_id": self._uid(user_id)},
                {"$set": {"token": int(token)}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_token({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def consume_token(self, user_id: Any, amount: int = 1) -> bool:
        """
        Atomically decrement tokens only if balance >= amount.
        Returns True on success, False if insufficient tokens.
        """
        try:
            async with _timed(f"consume_token({user_id})"):
                result = await self.col.find_one_and_update(
                    {"_id": self._uid(user_id), "token": {"$gte": amount}},
                    {"$inc": {"token": -amount}},
                    return_document=ReturnDocument.AFTER,
                )
            if result:
                logger.info(
                    f"consume_token({user_id}): -{amount}, remaining={result.get('token', '?')}"
                )
                return True
            logger.warning(f"consume_token({user_id}): insufficient tokens")
            return False
        except Exception as exc:
            logger.error(f"consume_token({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def refund_token(self, user_id: Any, amount: int = 1) -> None:
        """Refund tokens on task failure/cancellation."""
        try:
            result = await self.col.update_one(
                {"_id": self._uid(user_id)},
                {"$inc": {"token": amount}},
            )
            if result.modified_count:
                logger.info(f"refund_token({user_id}): +{amount}")
            else:
                logger.warning(f"refund_token({user_id}): user not found")
        except Exception as exc:
            logger.error(f"refund_token({user_id}): {exc}")
            raise

    # =========================================================================
    # Token link system
    # =========================================================================

    @_mongo_retry()
    async def create_token_link(
        self, user_id: Any, token_id: str, tokens: int
    ) -> None:
        try:
            expiry = datetime.now(pytz.utc) + timedelta(hours=24)
            await self.token_links.update_one(
                {"_id": token_id},
                {
                    "$set": {
                        "user_id": self._uid(user_id),
                        "tokens": tokens,
                        "used": False,
                        "expiry": expiry,
                        "created_at": datetime.now(pytz.utc),
                    }
                },
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
    async def redeem_token_link(self, token_id: str, user_id: Any) -> bool:
        uid = self._uid(user_id)
        try:
            # Atomic claim: marks used=True only if currently unused & not expired.
            token_doc = await self.token_links.find_one_and_update(
                {
                    "_id": token_id,
                    "used": False,
                    "expiry": {"$gt": datetime.now(pytz.utc)},
                },
                {
                    "$set": {
                        "used": True,
                        "used_at": datetime.now(pytz.utc),
                        "redeemed_by": uid,
                    }
                },
                return_document=ReturnDocument.BEFORE,  # we need tokens count from the pre-update doc
            )
            if not token_doc:
                logger.warning(f"redeem_token_link({token_id}): invalid/expired/already used")
                return False

            # Now safe to credit tokens — the link is already marked used.
            await self.col.update_one(
                {"_id": uid},
                {"$inc": {"token": token_doc["tokens"]}},
                upsert=True,
            )
            logger.info(
                f"redeem_token_link({token_id}): user {uid} got {token_doc['tokens']} tokens"
            )
            return True
        except Exception as exc:
            logger.error(f"redeem_token_link({token_id}, {user_id}): {exc}")
            return False

    # =========================================================================
    # Permission gate (single source of truth)
    # =========================================================================

    @_mongo_retry()
    async def can_process(self, user_id: Any) -> bool:
        """
        True if user may process a file:
        - active premium bypasses token check
        - otherwise token balance > 0
        """
        try:
            async with _timed(f"can_process({user_id})"):
                if await self.ensure_premium_valid(user_id):
                    return True
                return await self.get_token(user_id) > 0
        except Exception as exc:
            logger.error(f"can_process({user_id}): {exc}")
            return False

    # =========================================================================
    # Premium system
    # =========================================================================

    @_mongo_retry()
    async def ensure_premium_valid(self, user_id: Any) -> bool:
        """
        Check premium validity and auto-expire if needed.
        All callers should use this — never read is_premium field directly.
        """
        try:
            async with _timed(f"ensure_premium_valid({user_id})"):
                doc = await self.col.find_one(
                    {"_id": self._uid(user_id)},
                    {"is_premium": 1, "premium_expiry": 1},
                )
            if not doc or not doc.get("is_premium"):
                return False
            expiry = doc.get("premium_expiry")
            if expiry:
                if isinstance(expiry, datetime) and expiry.tzinfo is None:
                    expiry = pytz.utc.localize(expiry)
                if isinstance(expiry, datetime) and expiry < datetime.now(pytz.utc):
                    await self.remove_premium_user(user_id)
                    logger.info(f"ensure_premium_valid({user_id}): expired, removed")
                    return False
            return True
        except Exception as exc:
            logger.error(f"ensure_premium_valid({user_id}): {exc}")
            return False

    async def is_premium(self, user_id: Any) -> bool:
        """Alias for ensure_premium_valid — auto-expires stale premium."""
        return await self.ensure_premium_valid(user_id)

    @_mongo_retry()
    async def add_premium_user(self, user_id: Any, expiry: datetime) -> None:
        try:
            if expiry.tzinfo is None:
                expiry = pytz.utc.localize(expiry)
            await self.col.update_one(
                {"_id": self._uid(user_id)},
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
                {"_id": self._uid(user_id)},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
        except Exception as exc:
            logger.error(f"remove_premium_user({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def remove_expired_premium_users(self) -> int:
        """Bulk expire sweep — call this from a daily cron."""
        try:
            result = await self.col.update_many(
                {"is_premium": True, "premium_expiry": {"$lte": datetime.now(pytz.utc)}},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
            logger.info(f"remove_expired_premium_users: {result.modified_count} expired")
            return result.modified_count
        except Exception as exc:
            logger.error(f"remove_expired_premium_users: {exc}")
            return 0

    @_mongo_retry()
    async def list_premium_users_paginated(self, skip: int, limit: int) -> list:
        try:
            return await self.col.find(
                {"is_premium": True},
                {"_id": 1, "first_name": 1, "username": 1, "premium_expiry": 1}
            ).skip(skip).limit(limit).to_list(length=limit)
        except Exception as exc:
            logger.error(f"list_premium_users_paginated: {exc}")
            return []

    @_mongo_retry()
    async def count_premium_users(self) -> int:
        try:
            return await self.col.count_documents({"is_premium": True})
        except Exception as exc:
            logger.error(f"count_premium_users: {exc}")
            return 0

    # =========================================================================
    # Free trial system
    # =========================================================================

    @_mongo_retry()
    async def has_claimed_free_trial(self, user_id: Any) -> bool:
        """Single method for trial check — duplicate check_trial_status removed."""
        try:
            doc = await self.col.find_one(
                {"_id": self._uid(user_id)}, {"has_free_trial": 1}
            )
            return bool(doc.get("has_free_trial")) if doc else False
        except Exception as exc:
            logger.error(f"has_claimed_free_trial({user_id}): {exc}")
            return False

    # Removed duplicate: check_trial_status was identical to has_claimed_free_trial.

    @_mongo_retry()
    async def give_free_trial(self, user_id: Any, minutes: int = 5) -> None:
        try:
            expiry = datetime.now(pytz.utc) + timedelta(minutes=minutes)
            await self.col.update_one(
                {"_id": self._uid(user_id)},
                {
                    "$set": {
                        "has_free_trial": True,
                        "trial_expiry_time": expiry,
                        "is_premium": True,
                        "premium_expiry": expiry,
                    }
                },
                upsert=True,
            )
            logger.info(f"give_free_trial({user_id}): {minutes}m until {expiry}")
        except Exception as exc:
            logger.error(f"give_free_trial({user_id}): {exc}")
            raise

    @_mongo_retry()
    async def get_all_trial_users(self) -> list[int]:
        try:
            docs = await self.col.find(
                {"has_free_trial": True}, {"_id": 1}
            ).to_list(length=None)
            return [d["_id"] for d in docs]
        except Exception as exc:
            logger.error(f"get_all_trial_users: {exc}")
            return []

    # =========================================================================
    # Media preference
    # =========================================================================

    @_mongo_retry()
    async def get_media_preference(self, user_id: Any) -> str:
        try:
            doc = await self.col.find_one(
                {"_id": self._uid(user_id)}, {"media_type": 1}
            )
            return doc.get("media_type", "document") if doc else "document"
        except Exception as exc:
            logger.error(f"get_media_preference({user_id}): {exc}")
            return "document"

    @_mongo_retry()
    async def set_media_preference(self, user_id: Any, media_type: str) -> None:
        try:
            await self.col.update_one(
                {"_id": self._uid(user_id)},
                {"$set": {"media_type": media_type}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_media_preference({user_id}): {exc}")
            raise

    # =========================================================================
    # AniList integration
    # =========================================================================

    @_mongo_retry()
    async def get_anilist_thumb_status(self, user_id: Any) -> bool:
        try:
            doc = await self.col.find_one(
                {"_id": self._uid(user_id)}, {"anilist_thumb": 1}
            )
            return bool(doc.get("anilist_thumb")) if doc else False
        except Exception as exc:
            logger.error(f"get_anilist_thumb_status({user_id}): {exc}")
            return False

    @_mongo_retry()
    async def set_anilist_thumb_status(self, user_id: Any, status: bool) -> None:
        try:
            await self.col.update_one(
                {"_id": self._uid(user_id)},
                {"$set": {"anilist_thumb": bool(status)}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"set_anilist_thumb_status({user_id}): {exc}")
            raise

    # =========================================================================
    # Daily counters
    # =========================================================================

    @_mongo_retry()
    async def reset_daily_counters(self) -> int:
        """Bulk reset — call via cron."""
        try:
            result = await self.col.update_many({}, {"$set": {"daily_count": 0}})
            logger.info(f"reset_daily_counters: {result.modified_count} users reset")
            return result.modified_count
        except Exception as exc:
            logger.error(f"reset_daily_counters: {exc}")
            return 0


# =============================================================================
# Singleton
# =============================================================================
db = Rohit(Config.DB_URL, Config.DB_NAME)
