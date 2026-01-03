from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from datetime import datetime
from app.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class TradeRecord(Base):
    """
    Records every trade execution with full metadata.
    """
    __tablename__ = "trade_records"

    id = Column(String, primary_key=True, index=True)
    trade_tag = Column(String, index=True) # Upstox Order ID
    
    # Core Details
    instrument_key = Column(String, nullable=False)
    symbol = Column(String)
    side = Column(String) # BUY / SELL
    quantity = Column(Integer)
    price = Column(Float)
    
    # Metadata (CRITICAL FIX: Added these columns)
    strike = Column(Float, nullable=True)
    expiry = Column(DateTime, nullable=True)
    lot_size = Column(Integer, nullable=True)
    strategy = Column(String, default="MANUAL") 
    
    # State
    status = Column(String, default="OPEN") 
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Audit
    reason = Column(String, nullable=True)
    entry_delta = Column(Float, nullable=True)

class DecisionJournal(Base):
    __tablename__ = "decision_journal"
    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    cycle_id = Column(String)
    spot_price = Column(Float)
    vix = Column(Float)
    action_taken = Column(Boolean)
    details = Column(JSON)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
