# app/models/market_data.py

from sqlalchemy import Column, String, Float, DateTime, BigInteger, Index
from app.models.base import Base  # Assuming your Base is defined in app.models.base or app.database

class HistoricalCandle(Base):
    """
    Tier 1 Data Storage: Daily Candles
    """
    __tablename__ = "historical_candles"

    symbol = Column(String, primary_key=True, nullable=False)  # e.g., "NSE_INDEX|Nifty 50"
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    oi = Column(BigInteger)

    # Index for fast retrieval by symbol (though PK handles most)
    __table_args__ = (
        Index('idx_symbol_ts', 'symbol', 'timestamp'),
    )

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "oi": self.oi
        }
