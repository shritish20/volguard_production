from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class TradeBase(BaseModel):
    instrument_key: str
    symbol: Optional[str] = None
    side: str
    quantity: int
    price: float
    strategy: Optional[str] = "MANUAL"
    status: str = "OPEN"
    
    # NEW FIELDS (Aligned with Database)
    strike: Optional[float] = None
    expiry: Optional[datetime] = None
    lot_size: Optional[int] = None
    entry_delta: Optional[float] = None

class TradeCreate(TradeBase):
    trade_tag: str

class TradeResponse(TradeBase):
    id: str
    trade_tag: str
    timestamp: datetime
    reason: Optional[str] = None
    
    class Config:
        from_attributes = True

class TradeUpdate(BaseModel):
    status: Optional[str] = None
    closed_at: Optional[datetime] = None
    closed_reason: Optional[str] = None
