# app/schemas/trade.py

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Literal
from datetime import datetime


# ======================================================
# BASE TRADE MODEL
# ======================================================
class TradeBase(BaseModel):
    instrument_key: str
    symbol: Optional[str] = None

    action: Literal["ENTRY", "EXIT"]
    side: Literal["BUY", "SELL"]

    quantity: int
    entry_price: float

    strategy: str = "MANUAL"
    trade_tag: Optional[str] = None

    status: Literal["OPEN", "CLOSED", "REJECTED"] = "OPEN"

    # Instrument metadata
    strike: Optional[float] = None
    expiry: Optional[datetime] = None
    lot_size: Optional[int] = None
    entry_delta: Optional[float] = None

    # Risk context
    is_hedge: bool = False

    # Reason / audit
    reason: Optional[str] = None


# ======================================================
# TRADE CREATE (ENGINE → DB)
# ======================================================
class TradeCreate(TradeBase):
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ======================================================
# TRADE RESPONSE (DB → API)
# ======================================================
class TradeResponse(TradeBase):
    id: str

    created_at: datetime
    executed_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ======================================================
# TRADE UPDATE (EXIT / STATUS CHANGE)
# ======================================================
class TradeUpdate(BaseModel):
    status: Optional[Literal["OPEN", "CLOSED", "REJECTED"]] = None
    closed_at: Optional[datetime] = None
    closed_reason: Optional[str] = None
