from sqlalchemy import Column, String, Date, Float, BigInteger
from app.database import Base

class MarketDailyCandle(Base):
    __tablename__ = "market_daily_candles"

    symbol = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
