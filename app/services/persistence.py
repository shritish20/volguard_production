# app/services/persistence.py

import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from app.database import AsyncSessionLocal
from app.services.cache import cache

logger = logging.getLogger(__name__)

class PersistenceService:
    def __init__(self):
        pass

    async def save_daily_candle(self, symbol: str, data: dict):
        """Upsert daily candle to Postgres"""
        query = text("""
            INSERT INTO historical_candles (symbol, timestamp, open, high, low, close, volume, oi)
            VALUES (:symbol, :timestamp, :open, :high, :low, :close, :volume, :oi)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi;
        """)
        
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(query, {
                    "symbol": symbol,
                    "timestamp": data["timestamp"],
                    "open": data["open"],
                    "high": data["high"],
                    "low": data["low"],
                    "close": data["close"],
                    "volume": data.get("volume", 0),
                    "oi": data.get("oi", 0)
                })
                await session.commit()
            except Exception as e:
                logger.error(f"Failed to save candle: {e}")

    async def load_daily_history(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """
        Fetch history with Redis Caching (1 Hour TTL).
        """
        cache_key = f"hist:{symbol}:{days}"
        
        # 1. Try Redis
        cached_json = await cache.get(cache_key)
        if cached_json:
            try:
                # Assuming JSON was saved with orient='split' or default
                return pd.read_json(cached_json) 
            except ValueError:
                pass # JSON error, fall through to DB

        # 2. Fetch from DB
        cutoff = datetime.now() - timedelta(days=days)
        query = text("""
            SELECT timestamp, open, high, low, close, volume, oi
            FROM historical_candles
            WHERE symbol = :symbol AND timestamp >= :cutoff
            ORDER BY timestamp ASC
        """)

        async with AsyncSessionLocal() as session:
            result = await session.execute(query, {"symbol": symbol, "cutoff": cutoff})
            rows = result.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        # 3. Save to Redis (Async)
        if not df.empty:
            await cache.set(cache_key, df.to_json(), ttl=3600)

        return df
