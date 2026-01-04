# app/database.py

import logging
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Boolean, Text, BigInteger, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import settings
from app.models.base import Base  # <--- FIXED: Import Base from shared module

logger = logging.getLogger(__name__)

# ==== ASYNC ENGINE ====
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Base declaration removed here to avoid circular imports.
# It is now imported from app.models.base

# ==== TRADE RECORDS ====
class TradeRecord(Base):
    """Immutable record of every trade execution"""
    __tablename__ = "trade_records"

    id = Column(String, primary_key=True, index=True)
    trade_tag = Column(String, index=True, nullable=False)
    instrument_key = Column(String, nullable=False)
    symbol = Column(String)
    side = Column(String, nullable=False) # BUY / SELL
    quantity = Column(Integer, nullable=False)
    price = Column(Float)
    strike = Column(Float, nullable=True)
    expiry = Column(DateTime, nullable=True)
    lot_size = Column(Integer, nullable=True)
    strategy = Column(String, default="MANUAL")
    status = Column(String, default="OPEN")
    timestamp = Column(DateTime, default=datetime.utcnow)
    reason = Column(Text, nullable=True)
    entry_delta = Column(Float, nullable=True)
    is_hedge = Column(Boolean, default=False)

# ==== DECISION JOURNAL ====
class DecisionJournal(Base):
    """Black box recorder of every decision cycle."""
    __tablename__ = "decision_journal"

    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    cycle_id = Column(String, nullable=False)
    
    spot_price = Column(Float)
    vix = Column(Float)
    action_taken = Column(Boolean)
    regime = Column(String)
    scores = Column(JSON)
    risks = Column(JSON)
    details = Column(JSON)

# ==== HISTORICAL DATA (Smart VolGuard Addition) ====
class HistoricalCandle(Base):
    """Tier 1 Data Storage: Daily Candles"""
    __tablename__ = "historical_candles"

    symbol = Column(String, primary_key=True, nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    oi = Column(BigInteger)

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

# ==== INITIALIZATION ====
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# ==== JOURNAL HELPER ====
async def add_decision_log(cycle_data: dict):
    async with AsyncSessionLocal() as session:
        try:
            log = DecisionJournal(
                id=str(uuid.uuid4()),
                timestamp=datetime.utcnow(),
                cycle_id=cycle_data.get('cycle_id'),
                spot_price=cycle_data.get('spot', 0),
                vix=cycle_data.get('vix', 0),
                action_taken=cycle_data.get('action_taken', False),
                regime=cycle_data.get('regime', 'UNKNOWN'),
                scores=cycle_data.get('scores', {}),
                risks=cycle_data.get('risks', {}),
                details=cycle_data.get('details', {})
            )
            session.add(log)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Decision journal write failed: {e}")
