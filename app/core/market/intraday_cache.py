import json
import redis
from datetime import datetime

class IntradayCache:
    def __init__(self, redis_url: str):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.ttl = 60 * 60 * 24  # 24 hours

    def save_candle(self, symbol: str, interval: str, timestamp: str, data: dict):
        key = f"intraday:{symbol}:{interval}"
        self.redis.hset(key, timestamp, json.dumps(data))
        self.redis.expire(key, self.ttl)

    def load_candles(self, symbol: str, interval: str):
        key = f"intraday:{symbol}:{interval}"
        raw = self.redis.hgetall(key)
        return {k: json.loads(v) for k, v in raw.items()}
