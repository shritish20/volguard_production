# app/services/persistence.py

import logging
import pandas as pd
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import insert
from app.database import AsyncSessionLocal
from app.models.market_data import HistoricalCandle

logger = logging.getLogger(__name__)

class PersistenceService:
    """
    Handles all interactions between VolGuard and Postgres/Redis.
    Separates 'Fetching' (Client) from 'Storing' (Service).
    """

    async def save_daily_candles(self, df: pd.DataFrame, symbol: str):
        """
        Saves a DataFrame of candles to Postgres.
        Uses 'UPSERT' logic to handle duplicates gracefully.
        """
        if df.empty:
            return

        async with AsyncSessionLocal() as session:
            try:
                # Convert DataFrame to list of dicts for bulk insert
                records = []
                for _, row in df.iterrows():
                    records.append({
                        "symbol": symbol,
                        "timestamp": row["timestamp"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": int(row["volume"]),
                        "oi": int(row["oi"])
                    })

                # Efficient Postgres Upsert (On Conflict Do Update)
                stmt = insert(HistoricalCandle).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol', 'timestamp'],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                        "oi": stmt.excluded.oi
                    }
                )
                
                await session.execute(stmt)
                await session.commit()
                logger.info(f"Persisted {len(records)} candles for {symbol}")

            except Exception as e:
                logger.error(f"Failed to save daily candles: {e}")
                await session.rollback()

    async def load_daily_history(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """
        Loads history from DB for the VolatilityEngine.
        This allows the system to boot even if Upstox Historical API is down.
        """
        async with AsyncSessionLocal() as session:
            try:
                # Select last N days
                # Note: Logic to filter by date would be better, but simple limit works for now
                stmt = (
                    select(HistoricalCandle)
                    .where(HistoricalCandle.symbol == symbol)
                    .order_by(HistoricalCandle.timestamp.asc())
                )
                result = await session.execute(stmt)
                candles = result.scalars().all()

                if not candles:
                    return pd.DataFrame()

                data = [c.to_dict() for c in candles]
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Filter for requested days (Logic moved to Pandas for simplicity vs SQL date math)
                cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
                df = df[df['timestamp'] >= cutoff]
                
                return df.sort_values('timestamp').reset_index(drop=True)

            except Exception as e:
                logger.error(f"Failed to load history from DB: {e}")
                return pd.DataFrame()
