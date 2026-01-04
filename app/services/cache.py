# app/services/cache.py

import logging
import json
from typing import Optional, Any
import redis.asyncio as redis
from app.config import settings

logger = logging.getLogger(__name__)

class RedisCache:
    """
    Simple Async Redis Wrapper for VolGuard.
    Handles connection errors gracefully (falls back to None).
    """
    def __init__(self):
        # decode_responses=True ensures we get Strings, not Bytes
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def get(self, key: str) -> Optional[str]:
        """Get raw string from Redis"""
        try:
            return await self.redis.get(key)
        except Exception as e:
            logger.warning(f"⚠️ Redis Read Error: {e}")
            return None

    async def set(self, key: str, value: str, ttl: int = 300):
        """Set string in Redis with TTL"""
        try:
            await self.redis.setex(key, ttl, value)
        except Exception as e:
            logger.warning(f"⚠️ Redis Write Error: {e}")

    async def close(self):
        await self.redis.close()

# Global Instance
cache = RedisCache()
