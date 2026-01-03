import pandas as pd
from datetime import date
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.database.market_data import MarketDailyCandle
from app.core.market.data_client import MarketDataClient

class HistoricalDataStore:
    def __init__(self, market_client: MarketDataClient):
        self.market = market_client

    async def ensure_daily_history(self, symbol: str, instrument_key: str, lookback_days=400):
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(MarketDailyCandle.date)
                .where(MarketDailyCandle.symbol == symbol)
                .order_by(MarketDailyCandle.date.desc())
                .limit(1)
            )
            last_date = result.scalar()

        fetch_from = last_date + pd.Timedelta(days=1) if last_date else None

        df = await self.market.get_history(
            instrument_key,
            days=lookback_days if not fetch_from else (date.today() - fetch_from).days
        )

        if df.empty:
            return

        async with AsyncSessionLocal() as session:
            for ts, row in df.iterrows():
                candle_date = ts.date()
                if last_date and candle_date <= last_date:
                    continue

                session.add(MarketDailyCandle(
                    symbol=symbol,
                    date=candle_date,
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    volume=row.volume
                ))
            await session.commit()

    async def load_history(self, symbol: str, lookback=300) -> pd.DataFrame:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(MarketDailyCandle)
                .where(MarketDailyCandle.symbol == symbol)
                .order_by(MarketDailyCandle.date.desc())
                .limit(lookback)
            )

        rows = result.scalars().all()
        data = [{
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume
        } for r in rows]

        return pd.DataFrame(data).set_index("date").sort_index()
