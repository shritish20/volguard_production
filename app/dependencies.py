# app/dependencies.py

from fastapi import Depends, HTTPException, Header
from typing import Generator, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from app.config import settings

# Import your NEW Core Services
from app.core.auth.token_manager import TokenManager
from app.core.market.data_client import MarketDataClient
from app.services.persistence import PersistenceService

# Global Token Manager (Singleton for API)
# We initialize this once. In production, this should share state with the Supervisor
# or read the token from the .env file updated by the TokenManager script.
token_manager = TokenManager(
    access_token=settings.UPSTOX_ACCESS_TOKEN,
    refresh_token=settings.UPSTOX_REFRESH_TOKEN,
    client_id=settings.UPSTOX_CLIENT_ID,
    client_secret=settings.UPSTOX_CLIENT_SECRET
)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database session dependency"""
    async with AsyncSessionLocal() as session:
        yield session

def get_token() -> str:
    """Ensures we always have a valid token for API calls"""
    # In a real distributed setup, this might read from Redis.
    # For now, we validate what we have.
    if not token_manager.validate_token():
        raise HTTPException(status_code=401, detail="Upstox Token Expired")
    return token_manager.get_token()

def get_market_client(token: str = Depends(get_token)) -> MarketDataClient:
    """Injects the new Smart Market Client"""
    return MarketDataClient(token)

def get_persistence_service() -> PersistenceService:
    return PersistenceService()

def verify_admin_secret(x_admin_key: str = Header(...)):
    """Simple Admin Auth"""
    if x_admin_key != settings.ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Admin Key")
    return x_admin_key
