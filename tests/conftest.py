import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from app.config import settings

# 1. Shared Event Loop
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. Mock Redis (Critical for Idempotency)
@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.set.return_value = True  # Lock acquired
    redis.get.return_value = None
    return redis

# 3. Mock Upstox Client (Critical for Market Data)
@pytest.fixture
def mock_market_client():
    client = AsyncMock()
    # Default valid response
    client.get_live_quote.return_value = {
        "NSE_INDEX|Nifty 50": 21500.0,
        "NSE_INDEX|India VIX": 14.5
    }
    client.get_option_chain.return_value = [] # Return empty list or valid DF structure
    return client

# 4. Mock Executor (Critical for Trading)
@pytest.fixture
def mock_executor():
    exc = AsyncMock()
    exc.verify_order_status.return_value = {"status": "complete", "filled_qty": 50}
    return exc
