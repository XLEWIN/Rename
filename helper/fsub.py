from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError
from config import Config
from helper.database import _mongo_retry
from datetime import datetime
import logging
import pytz

logger = logging.getLogger(__name__)

MONGO_URI = Config.DB_URL
DB_NAME = Config.DB_NAME

class RexyFsub:
    def __init__(self):
        self.client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=50,
            minPoolSize=5,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=20000,
        )
        self.db = self.client["file_sequence_bot"]
        self.database = self.client[DB_NAME]
        self.settings = self.db["settings"]
        self.fsub_data = self.database['fsub']   
        self.rqst_fsub_data = self.database['request_forcesub']
        self.rqst_fsub_Channel_data = self.database['request_forcesub_channel']
        self.channel_data = self.database['channels']

    @_mongo_retry()
    async def ensure_indexes(self):
        """Create indexes for fsub collections. Call once on startup."""
        try:
            # user_ids array lookups benefit from a multikey index
            await self.rqst_fsub_Channel_data.create_index("user_ids")
            logger.info("RexyFsub: indexes ensured ✅")
        except Exception as e:
            logger.error(f"RexyFsub: index creation error: {e}", exc_info=True)

    @_mongo_retry()
    async def channel_exist(self, channel_id: int):
        try:
            found = await self.fsub_data.find_one({'_id': channel_id}, {'_id': 1})
            return bool(found)
        except Exception as e:
            logger.error(f"channel_exist({channel_id}): {e}")
            return False

    @_mongo_retry()
    async def add_channel(self, channel_id: int):
        try:
            if not await self.channel_exist(channel_id):
                await self.fsub_data.insert_one({'_id': channel_id})
        except Exception as e:
            logger.error(f"add_channel({channel_id}): {e}")

    @_mongo_retry()
    async def rem_channel(self, channel_id: int):
        try:
            await self.fsub_data.delete_one({'_id': channel_id})
        except Exception as e:
            logger.error(f"rem_channel({channel_id}): {e}")

    @_mongo_retry()
    async def show_channels(self):
        try:
            channel_docs = await self.fsub_data.find({}, {'_id': 1}).to_list(length=500)
            return [doc['_id'] for doc in channel_docs]
        except Exception as e:
            logger.error(f"show_channels: {e}")
            return []

    @_mongo_retry()
    async def get_channel_mode(self, channel_id: int):
        try:
            data = await self.fsub_data.find_one({'_id': channel_id}, {'mode': 1})
            return data.get("mode", "off") if data else "off"
        except Exception as e:
            logger.error(f"get_channel_mode({channel_id}): {e}")
            return "off"

    @_mongo_retry()
    async def set_channel_mode(self, channel_id: int, mode: str):
        try:
            await self.fsub_data.update_one(
                {'_id': channel_id},
                {'$set': {'mode': mode}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"set_channel_mode({channel_id}): {e}")

    @_mongo_retry()
    async def req_user(self, channel_id: int, user_id: int):
        try:
            await self.rqst_fsub_Channel_data.update_one(
                {'_id': int(channel_id)},
                {'$addToSet': {'user_ids': int(user_id)}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"req_user({channel_id}, {user_id}): {e}")

    @_mongo_retry()
    async def del_req_user(self, channel_id: int, user_id: int):
        try:
            await self.rqst_fsub_Channel_data.update_one(
                {'_id': channel_id}, 
                {'$pull': {'user_ids': user_id}}
            )
        except Exception as e:
            logger.error(f"del_req_user({channel_id}, {user_id}): {e}")

    @_mongo_retry()
    async def req_user_exist(self, channel_id: int, user_id: int):
        try:
            found = await self.rqst_fsub_Channel_data.find_one({
                '_id': int(channel_id),
                'user_ids': int(user_id)
            }, {'_id': 1})
            return bool(found)
        except Exception as e:
            logger.error(f"req_user_exist({channel_id}, {user_id}): {e}")
            return False  

    @_mongo_retry()
    async def reqChannel_exist(self, channel_id: int):
        try:
            found = await self.fsub_data.find_one({'_id': channel_id}, {'_id': 1})
            return bool(found)
        except Exception as e:
            logger.error(f"reqChannel_exist({channel_id}): {e}")
            return False

    @_mongo_retry()
    async def get_request_fsub(self) -> bool:
        """Fetch the global request-fsub mode flag."""
        try:
            doc = await self.rqst_fsub_data.find_one({"_id": "global_req_mode"})
            return doc.get("enabled", False) if doc else False
        except Exception as e:
            logger.error(f"get_request_fsub: {e}")
            return False

    @_mongo_retry()
    async def set_request_fsub(self, enabled: bool) -> None:
        """Persist the global request-fsub mode flag."""
        try:
            await self.rqst_fsub_data.update_one(
                {"_id": "global_req_mode"},
                {"$set": {"enabled": enabled}},
                upsert=True,
            )
        except Exception as e:
            logger.error(f"set_request_fsub({enabled}): {e}")
            raise

Rexy = RexyFsub()
