# app/database.py
import logging
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, Integer, JSON, Boolean, Text, BigInteger, Index, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from app.config import settings
from app.models.base import Base # Must map to your shared Base file

logger = logging.getLogger(__name__)

# ==== ASYNC ENGINE ====
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=10, # Increased for higher concurrency
    max_overflow=20,
    pool_recycle=1800
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# ==== TRADE RECORDS (Aligned with New Executor) ====
class TradeRecord(Base):
    """
    Immutable record of every trade execution.
    Aligned with VolGuard 3.0 Executor Schema.
    """
    __tablename__ = "trade_records"

    id = Column(String, primary_key=True, index=True)
    trade_tag = Column(String, index=True, nullable=False) # Usually Order ID
    instrument_key = Column(String, nullable=False)
    symbol = Column(String)
    side = Column(String, nullable=False) # BUY / SELL
    quantity = Column(Integer, nullable=False)
    entry_price = Column(Float) # Renamed from 'price' for clarity

    # Metadata
    strike = Column(Float, nullable=True)
    expiry = Column(DateTime, nullable=True)
    lot_size = Column(Integer, nullable=True)
    strategy = Column(String, default="MANUAL")

    # State & Audit
    status = Column(String, default="OPEN") # OPEN / CLOSED / REJECTED
    timestamp = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    reason = Column(Text, nullable=True)

    # Risk
    entry_delta = Column(Float, nullable=True) # Snapshot of delta at entry
    is_hedge = Column(Boolean, default=False)

# ==== DECISION JOURNAL ====
class DecisionJournal(Base):
    """Black box recorder of every decision cycle."""
    __tablename__ = "decision_journal"

    id = Column(String, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    cycle_id = Column(String, nullable=False, index=True)
    spot_price = Column(Float)
    vix = Column(Float)
    action_taken = Column(Boolean)
    regime = Column(String)
    scores = Column(JSON)
    risks = Column(JSON)
    details = Column(JSON)
    error = Column(Text, nullable=True) # Added to capture cycle crashes

# ==== INITIALIZATION ====
async def init_db():
    """
    Initialize database connection and verify tables exist.
    
    NOTE: Tables are created via Alembic migrations, not here.
    This function only verifies connectivity.
    """
    async with engine.begin() as conn:
        # ✅ IMPORTANT: DO NOT auto-create tables here
        # Tables should be created via Alembic migrations
        # await conn.run_sync(Base.metadata.create_all)
        
        # Just verify database connectivity
        result = await conn.execute(text("SELECT 1"))
        val = result.scalar()
        if val == 1:
            logger.info("✅ Database connection verified")
        else:
            logger.error("❌ Database connection test failed")
            raise RuntimeError("Database connection test failed")
        
        # Check if tables exist (optional validation)
        try:
            result = await conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            ))
            table_count = result.scalar()
            logger.info(f"📊 Database has {table_count} tables")
            
            if table_count == 0:
                logger.warning("⚠️ No tables found! Run: alembic upgrade head")
        except Exception as e:
            logger.warning(f"Could not verify table count: {e}")

# ==== JOURNAL HELPER ====
async def add_decision_log(cycle_data: dict):
    """Fire-and-forget logging to DB"""
    async with AsyncSessionLocal() as session:
        try:
            log = DecisionJournal(
                id=str(uuid.uuid4()),
                timestamp=datetime.utcnow(),
                cycle_id=cycle_data.get('cycle_id', 'UNKNOWN'),
                spot_price=cycle_data.get('spot', 0),
                vix=cycle_data.get('vix', 0),
                action_taken=cycle_data.get('action_taken', False),
                regime=cycle_data.get('regime', 'UNKNOWN'),
                scores=cycle_data.get('scores', {}),
                risks=cycle_data.get('risks', {}),
                details=cycle_data.get('details', {}),
                error=cycle_data.get('error', None)
            )
            session.add(log)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Decision journal write failed: {e}")
