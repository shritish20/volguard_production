from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Boolean, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from datetime import datetime
import uuid
from app.config import settings

# Async Engine
engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class TradeRecord(Base):
    """Immutable record of every trade execution"""
    __tablename__ = "trade_records"

    id = Column(String, primary_key=True, index=True)
    trade_tag = Column(String, index=True)  # Upstox Order ID
    
    # Core Details
    instrument_key = Column(String, nullable=False)
    symbol = Column(String)
    side = Column(String)      # BUY / SELL
    quantity = Column(Integer)
    price = Column(Float)      # Execution Price
    
    # Strategy & Risk Metadata
    strike = Column(Float, nullable=True)
    expiry = Column(DateTime, nullable=True)
    lot_size = Column(Integer, nullable=True)
    strategy = Column(String, default="MANUAL")
    
    # State
    status = Column(String, default="OPEN") # OPEN, CLOSED, REJECTED
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Audit
    reason = Column(Text, nullable=True)     # Why did we trade?
    entry_delta = Column(Float, nullable=True)

class DecisionJournal(Base):
    """
    The 'Black Box Recorder'. 
    Saves the full state of the brain every cycle, even if no trade occurred.
    """
    __tablename__ = "decision_journal"
    
    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    cycle_id = Column(String)
    
    # Market State
    spot_price = Column(Float)
    vix = Column(Float)
    
    # The "Why"
    action_taken = Column(Boolean) # Did we do anything?
    regime = Column(String)        # e.g., AGGRESSIVE_SHORT
    scores = Column(JSON)          # {vol_score: 8.0, struct_score: 5.0...}
    risks = Column(JSON)           # {delta: 0.1, gamma: 0.05}
    
    details = Column(JSON)         # Full dump of logic vars

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def add_decision_log(cycle_data: dict):
    """Helper to write to journal asynchronously"""
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
            # We don't crash on logging failure, just print
            print(f"Journal Error: {e}")
