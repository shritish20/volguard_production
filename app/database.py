"""
Database configuration and connection management.
"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, Text
from datetime import datetime
import uuid
from app.config import settings

# Create async engine
engine = create_async_engine(
    str(settings.DATABASE_URL),
    echo=settings.DEBUG,
    future=True,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True
)

# Create async session factory
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

class DecisionJournal(Base):
    """Immutable journal of every supervisor cycle"""
    __tablename__ = "decision_journals"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    cycle_timestamp = Column(DateTime, nullable=False, index=True)
    market_snapshot = Column(JSON)  # Market data at cycle start
    portfolio_risk = Column(JSON)   # Risk assessment
    capital_metrics = Column(JSON)  # Capital calculations
    adjustments_evaluated = Column(JSON)  # All adjustments considered
    safety_status = Column(JSON)    # Safety controller state
    data_quality = Column(Float)    # Quality score
    execution_mode = Column(String) # PAPER, SEMI_AUTO, FULL_AUTO
    created_at = Column(DateTime, default=datetime.utcnow)

class TradeRecord(Base):
    """All trades executed by the system"""
    __tablename__ = "trade_records"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    trade_tag = Column(String, unique=True, index=True)
    strategy = Column(String)
    regime = Column(String)
    legs = Column(JSON)
    entry_prices = Column(JSON)
    quantities = Column(JSON)
    margin_used = Column(Float)
    net_delta = Column(Float)
    net_gamma = Column(Float)
    net_vega = Column(Float)
    net_theta = Column(Float)
    realized_pnl = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    status = Column(String, default="LIVE")  # LIVE, ADJUSTED, CLOSED
    opened_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    closed_reason = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class SafetyViolation(Base):
    """Record of all safety violations"""
    __tablename__ = "safety_violations"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    violation_type = Column(String)
    severity = Column(String)  # LOW, MEDIUM, HIGH, CRITICAL
    details = Column(JSON)
    system_state = Column(String)
    execution_mode = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

async def get_db() -> AsyncSession:
    """Get database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
