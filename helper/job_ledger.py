#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Callable

from pymongo import ReturnDocument
import motor.motor_asyncio

from config import Config
from helper.database import DARKXSIDE78

logger = logging.getLogger(__name__)

# Job statuses
STATUS_QUEUED = "queued"
STATUS_DOWNLOADING = "downloading"
STATUS_PROCESSING = "processing"
STATUS_UPLOADING = "uploading"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

# Terminal statuses — jobs in these states are NOT recovered on restart
TERMINAL_STATUSES = frozenset({STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED})

# Max retries before permanent failure
MAX_JOB_RETRIES = 3

# How long to keep completed jobs before cleanup (hours)
JOB_RETENTION_HOURS = 24

# Periodic cleanup interval (seconds)
CLEANUP_INTERVAL_SECONDS = 3600  # 1 hour


def _now() -> datetime:
    return datetime.now(timezone.utc)


class JobLedger:

    def __init__(self, uri: str, database_name: str) -> None:
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self._db = self._client[database_name]
        self.col = self._db["jobs"]
        self._cleanup_task: Optional[asyncio.Task] = None
        logger.info(f"JobLedger: connected to '{database_name}.jobs'")

    async def ensure_indexes(self) -> None:
        """Create indexes for fast queries. Call once on startup."""
        try:
            await self.col.create_index("job_id", unique=True)
            await self.col.create_index("user_id")
            await self.col.create_index("status")
            await self.col.create_index("created_at")
            await self.col.create_index("updated_at")
            logger.info("JobLedger: indexes created")
        except Exception as exc:
            logger.error(f"JobLedger: index creation failed: {exc}")

    # =========================================================================
    # Job lifecycle
    # =========================================================================

    async def create_job(
        self,
        user_id: int,
        chat_id: int,
        file_id: str,
        file_name: str,
        message_id: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> Optional[str]:
        job_id = job_id or f"{user_id}:{file_id}"
        now = _now()
        try:
            # Prevent duplicate active job creattion
        
            existing = await self.get_job(job_id)
            if existing and existing.get("status") not in TERMINAL_STATUSES:
                logger.debug(f"create_job: active job already exists {job_id}")
                return job_id

            # Completely overwrite any previous failed/done job with the same ID
            await self.col.replace_one(
                {"job_id": job_id},
                {
                    "job_id": job_id,
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "file_id": file_id,
                    "file_name": file_name,
                    "message_id": message_id,
                    "status": STATUS_QUEUED,
                    "token_charged": False,
                    "retry_count": 0,
                    "error": None,
                    "created_at": now,
                    "updated_at": now,
                },
                upsert=True,
            )
            logger.debug(f"create_job: {job_id} ({file_name})")
            return job_id
        except Exception as exc:
            logger.error(f"create_job({job_id}): {exc}")
            return None

    async def update_status(self, job_id: str, status: str) -> bool:
        """
        Update job status. Only transitions FROM non-terminal states.
        Returns True if the update was applied.
        """
        try:
            result = await self.col.update_one(
                {
                    "job_id": job_id,
                    "status": {"$nin": list(TERMINAL_STATUSES)},
                },
                {
                    "$set": {
                        "status": status,
                        "updated_at": _now(),
                    }
                },
            )
            updated = result.modified_count > 0
            if updated:
                logger.debug(f"update_status({job_id}): → {status}")
            return updated
        except Exception as exc:
            logger.error(f"update_status({job_id}, {status}): {exc}")
            return False

    async def set_status_message_id(self, job_id: str, status_msg_id: int) -> bool:
        """Store the status message ID so we can delete it on bot restart if it gets stuck."""
        try:
            result = await self.col.update_one(
                {"job_id": job_id},
                {"$set": {"status_msg_id": status_msg_id}}
            )
            return result.modified_count > 0
        except Exception as exc:
            logger.error(f"set_status_message_id({job_id}, {status_msg_id}): {exc}")
            return False

    async def mark_done(self, job_id: str) -> bool:
        """Mark job as successfully completed."""
        return await self.update_status(job_id, STATUS_DONE)

    async def mark_failed(self, job_id: str, error: str = "") -> bool:
        """Mark job as permanently failed."""
        try:
            result = await self.col.update_one(
                {
                    "job_id": job_id,
                    "status": {"$nin": list(TERMINAL_STATUSES)},
                },
                {
                    "$set": {
                        "status": STATUS_FAILED,
                        "error": error[:500],
                        "updated_at": _now(),
                    }
                },
            )
            if result.modified_count > 0:
                logger.info(f"mark_failed({job_id}): {error[:100]}")
                return True
            return False
        except Exception as exc:
            logger.error(f"mark_failed({job_id}): {exc}")
            return False

    async def mark_cancelled(self, job_id: str) -> bool:
        """Mark job as cancelled by user."""
        return await self.update_status(job_id, STATUS_CANCELLED)

    async def mark_all_user_cancelled(self, user_id: int) -> int:
        """Mark ALL non-terminal jobs for a user as cancelled in MongoDB."""
        try:
            result = await self.col.update_many(
                {
                    "user_id": user_id,
                    "status": {"$nin": list(TERMINAL_STATUSES)},
                },
                {
                    "$set": {
                        "status": STATUS_CANCELLED,
                        "updated_at": _now(),
                    }
                }
            )
            if result.modified_count > 0:
                logger.info(f"mark_all_user_cancelled({user_id}): {result.modified_count} jobs marked cancelled")
            return result.modified_count
        except Exception as exc:
            logger.error(f"mark_all_user_cancelled({user_id}): {exc}")
            return 0

    # =========================================================================
    # Idempotent token charging
    # =========================================================================

    async def charge_token_once(self, job_id: str) -> bool:
        try:
            result = await self.col.find_one_and_update(
                {"job_id": job_id, "token_charged": False},
                {"$set": {"token_charged": True, "updated_at": _now()}},
                return_document=ReturnDocument.AFTER,
            )
            if result:
                logger.info(f"charge_token_once({job_id}): charged")
                return True
            # Already charged or job doesn't exist
            logger.debug(f"charge_token_once({job_id}): already charged or missing")
            return False
        except Exception as exc:
            logger.error(f"charge_token_once({job_id}): {exc}")
            return False

    async def refund_token_once(self, job_id: str) -> bool:
        try:
            result = await self.col.find_one_and_update(
                {"job_id": job_id, "token_charged": True},
                {"$set": {"token_charged": False, "updated_at": _now()}},
                return_document=ReturnDocument.AFTER,
            )
            if result:
                logger.info(f"refund_token_once({job_id}): refunded")
                return True
            logger.debug(f"refund_token_once({job_id}): nothing to refund")
            return False
        except Exception as exc:
            logger.error(f"refund_token_once({job_id}): {exc}")
            return False

    # =========================================================================
    # Restart recovery
    # =========================================================================

    async def get_stale_jobs(self) -> list[dict]:
        """Fetch all jobs that are not in a terminal state."""
        try:
            cursor = self.col.find({
                "status": {"$nin": list(TERMINAL_STATUSES)},
            })
            jobs = await cursor.to_list(length=500)
            logger.info(f"get_stale_jobs: found {len(jobs)} recoverable jobs")
            return jobs
        except Exception as exc:
            logger.error(f"get_stale_jobs: {exc}")
            return []

    async def should_retry(self, job_id: str) -> bool:
        """
        Check if job can be retried. Atomically increments retry_count.
        Returns True if retry_count < MAX_JOB_RETRIES after increment.
        """
        try:
            result = await self.col.find_one_and_update(
                {
                    "job_id": job_id,
                    "retry_count": {"$lt": MAX_JOB_RETRIES},
                },
                {
                    "$inc": {"retry_count": 1},
                    "$set": {
                        "status": STATUS_QUEUED,
                        "updated_at": _now(),
                    },
                },
                return_document=ReturnDocument.AFTER,
            )
            if result:
                logger.info(
                    f"should_retry({job_id}): retry #{result['retry_count']}"
                )
                return True
            logger.warning(f"should_retry({job_id}): max retries exceeded")
            return False
        except Exception as exc:
            logger.error(f"should_retry({job_id}): {exc}")
            return False

    async def get_job(self, job_id: str) -> Optional[dict]:
        """Fetch a single job by job_id."""
        try:
            return await self.col.find_one({"job_id": job_id})
        except Exception as exc:
            logger.error(f"get_job({job_id}): {exc}")
            return None

    async def get_user_active_jobs(self, user_id: int) -> list[dict]:
        """Fetch all non-terminal jobs for a specific user."""
        try:
            cursor = self.col.find({
                "user_id": user_id,
                "status": {"$nin": list(TERMINAL_STATUSES)},
            })
            return await cursor.to_list(length=100)
        except Exception as exc:
            logger.error(f"get_user_active_jobs({user_id}): {exc}")
            return []

    async def get_user_job_history(
        self, user_id: int, limit: int = 20
    ) -> list[dict]:
        """Fetch recent job history for a user (most recent first)."""
        try:
            cursor = (
                self.col.find({"user_id": user_id})
                .sort("updated_at", -1)
                .limit(limit)
            )
            return await cursor.to_list(length=limit)
        except Exception as exc:
            logger.error(f"get_user_job_history({user_id}): {exc}")
            return []

    # =========================================================================
    # Recovery engine — called on bot startup
    # =========================================================================

    async def recover_stale_jobs(
        self,
        client: Any,
        enqueue_fn: Optional[Callable] = None,
    ) -> dict[str, int]:

        stale_jobs = await self.get_stale_jobs()
        stats = {"recovered": 0, "failed": 0, "refunded": 0, "notified": 0, "skipped": 0}

        if not stale_jobs:
            logger.info("recover_stale_jobs: no stale jobs found — clean restart")
            return stats

        logger.info(f"recover_stale_jobs: processing {len(stale_jobs)} stale jobs")

        for job in stale_jobs:
            job_id = job.get("job_id", "unknown")
            user_id = job.get("user_id")
            chat_id = job.get("chat_id")
            file_name = job.get("file_name", "Unknown file")
            retry_count = job.get("retry_count", 0)
            status = job.get("status", "unknown")
            token_charged = job.get("token_charged", False)

            status_msg_id = job.get("status_msg_id")

            # Clean up the stuck status message from the previous run
            if client and chat_id and status_msg_id:
                try:
                    await client.delete_messages(int(chat_id), int(status_msg_id))
                except Exception as exc:
                    logger.warning(f"recover_stale_jobs: failed to delete old status msg {status_msg_id}: {exc}")

            try:
                message_id = job.get("message_id")
                recovered = False

                if client and chat_id and message_id:
                    try:
                        msg = await client.get_messages(int(chat_id), int(message_id))
                        # In pyrogram, deleted messages return an empty Message object
                        if msg and not getattr(msg, "empty", False):
                            
                            # Refund the token FIRST so the new queue attempt can charge correctly
                            if token_charged and user_id:
                                if await self.refund_token_once(job_id):
                                    await DARKXSIDE78.refund_token(user_id, 1)
                                    stats["refunded"] += 1
                                    
                            if enqueue_fn:
                                # FIX: await instead of create_task to ensure recovery is tracked
                                await enqueue_fn(client, msg)
                            recovered = True
                            
                            # Mark old job as cancelled/done so it doesn't stay 'failed'
                            await self.mark_failed(job_id, "Re-queued on restart")
                            stats["recovered"] += 1
                            
                    except Exception as fetch_exc:
                        logger.warning(f"Failed to fetch original message for {job_id}: {fetch_exc}")

                if not recovered:
                    # 1. Mark as failed permanently (could not recover)
                    await self.mark_failed(job_id, "Bot restarted and original message lost")
                    stats["failed"] += 1
    
                    # 2. Refund token if it was charged
                    if token_charged and user_id:
                        if await self.refund_token_once(job_id):
                            await DARKXSIDE78.refund_token(user_id, 1)
                            stats["refunded"] += 1
    
                    # 3. Notify user
                    if client and chat_id:
                        try:
                            await client.send_message(
                                chat_id=int(chat_id),
                                text=(
                                    "<b>⚠️ Bᴏᴛ Rᴇsᴛᴀʀᴛᴇᴅ</b>\n\n"
                                    f"<blockquote>Yᴏᴜʀ ᴘʀᴇᴠɪᴏᴜs ғɪʟᴇ ᴡᴀs ɪɴᴛᴇʀʀᴜᴘᴛᴇᴅ.\n\n"
                                    f"📄 <code>{file_name[:64]}...</code>\n\n"
                                    "Pʟᴇᴀsᴇ sᴇɴᴅ ᴛʜᴇ ғɪʟᴇ ᴀɢᴀɪɴ ᴀs I ᴄᴏᴜʟᴅɴ'ᴛ ʀᴇᴄᴏᴠᴇʀ ɪᴛ.\n"
                                    "Aɴʏ ᴛᴏᴋᴇɴ ᴄʜᴀʀɢᴇᴅ ʜᴀs ʙᴇᴇɴ ʀᴇғᴜɴᴅᴇᴅ.</blockquote>"
                                ),
                            )
                            stats["notified"] += 1
                        except Exception as notify_exc:
                            logger.warning(
                                f"recover_stale_jobs: notify failed for {job_id}: {notify_exc}"
                            )
            except Exception as job_exc:
                logger.error(
                    f"recover_stale_jobs: error handling {job_id}: {job_exc}"
                )

        logger.info(
            f"recover_stale_jobs complete: "
            f"recovered={stats['recovered']}, "
            f"failed={stats['failed']}, "
            f"refunded={stats['refunded']}, "
            f"notified={stats['notified']}, "
            f"skipped={stats['skipped']}"
        )
        return stats

    # =========================================================================
    # Statistics
    # =========================================================================

    async def get_stats(self) -> dict[str, Any]:
        """
        Get aggregate job statistics for admin dashboard.
        Returns counts by status plus overall metrics.
        """
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1},
                    }
                }
            ]
            status_counts = {}
            async for doc in self.col.aggregate(pipeline):
                status_counts[doc["_id"]] = doc["count"]

            total = sum(status_counts.values())
            active = sum(
                status_counts.get(s, 0)
                for s in (STATUS_QUEUED, STATUS_DOWNLOADING, STATUS_PROCESSING, STATUS_UPLOADING)
            )

            # Unique users with active jobs
            unique_users = await self.col.distinct(
                "user_id",
                {"status": {"$nin": list(TERMINAL_STATUSES)}},
            )

            # Average retry count for failed jobs
            retry_pipeline = [
                {"$match": {"status": STATUS_FAILED}},
                {"$group": {"_id": None, "avg_retries": {"$avg": "$retry_count"}}},
            ]
            retry_cursor = self.col.aggregate(retry_pipeline)
            retry_stats = await retry_cursor.to_list(length=1)
            avg_retries = (
                round(retry_stats[0]["avg_retries"], 1) if retry_stats else 0
            )

            return {
                "total": total,
                "active": active,
                "by_status": status_counts,
                "unique_active_users": len(unique_users),
                "avg_retries_on_fail": avg_retries,
            }
        except Exception as exc:
            logger.error(f"get_stats: {exc}")
            return {
                "total": 0, "active": 0, "by_status": {},
                "unique_active_users": 0, "avg_retries_on_fail": 0,
            }

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def cleanup_old_jobs(self, hours: int = JOB_RETENTION_HOURS) -> int:
        """Delete terminal jobs older than `hours`. Call from periodic task."""
        cutoff = _now() - timedelta(hours=hours)
        try:
            result = await self.col.delete_many(
                {
                    "status": {"$in": list(TERMINAL_STATUSES)},
                    "updated_at": {"$lt": cutoff},
                }
            )
            if result.deleted_count:
                logger.info(
                    f"cleanup_old_jobs: deleted {result.deleted_count} old jobs"
                )
            return result.deleted_count
        except Exception as exc:
            logger.error(f"cleanup_old_jobs: {exc}")
            return 0



    # =========================================================================
    # Periodic cleanup background task
    # =========================================================================

    async def _periodic_cleanup_loop(self) -> None:
        """
        Background loop that periodically cleans up old completed jobs.
        Runs every CLEANUP_INTERVAL_SECONDS.
        """
        logger.info("JobLedger: periodic cleanup loop started")
        while True:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
                deleted = await self.cleanup_old_jobs()
                if deleted:
                    logger.info(
                        f"JobLedger periodic cleanup: {deleted} old jobs removed"
                    )
            except asyncio.CancelledError:
                logger.info("JobLedger: periodic cleanup loop cancelled")
                break
            except Exception as exc:
                logger.error(f"JobLedger periodic cleanup error: {exc}")
                # Don't crash the loop on transient errors
                await asyncio.sleep(60)

    def start_periodic_cleanup(self) -> None:
        """Start the background cleanup task. Safe to call multiple times."""
        if self._cleanup_task and not self._cleanup_task.done():
            logger.debug("JobLedger: periodic cleanup already running")
            return
        self._cleanup_task = asyncio.create_task(
            self._periodic_cleanup_loop(),
            name="job-ledger-cleanup",
        )
        logger.info("JobLedger: periodic cleanup task started")

    def stop_periodic_cleanup(self) -> None:
        """Cancel the background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            logger.info("JobLedger: periodic cleanup task cancelled")

    # =========================================================================
    # Full startup sequence — call this from bot.py
    # =========================================================================

    async def startup(self, client: Any = None, enqueue_fn: Optional[Callable] = None) -> dict[str, int]:
        logger.info("JobLedger: running startup sequence...")

        # Step 1: Indexes
        await self.ensure_indexes()

        if client is not None:
            if not getattr(client, "is_connected", True):
                logger.info("JobLedger: client not connected yet, delaying recovery...")
                
                async def _delayed_recovery():
                    # Wait for client to connect
                    while not getattr(client, "is_connected", False):
                        await asyncio.sleep(1)
                    await self.recover_stale_jobs(client, enqueue_fn)
                    
                asyncio.create_task(_delayed_recovery())
                self.start_periodic_cleanup()
                return {"status": "delayed"}
            else:
                try:
                    await client.get_me()
                except Exception as exc:
                    logger.warning(f"startup: client get_me failed, skipping recovery: {exc}")
                    client = None

        # Step 2: Recover stale jobs
        recovery_stats = await self.recover_stale_jobs(client, enqueue_fn)

        # Step 3: Start periodic cleanup
        self.start_periodic_cleanup()

        logger.info("JobLedger: startup sequence complete")
        return recovery_stats


# =============================================================================
# Singleton
# =============================================================================
job_ledger = JobLedger(Config.DB_URL, Config.DB_NAME)
