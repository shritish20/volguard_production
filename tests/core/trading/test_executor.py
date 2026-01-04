import pytest
from unittest.mock import AsyncMock, patch
from app.core.trading.executor import TradeExecutor

@pytest.fixture
def executor():
    with patch("httpx.Limits"):
        exec_instance = TradeExecutor("token")
        # Mock Redis to ALWAYS return True (Lock Acquired)
        exec_instance.redis = AsyncMock()
        exec_instance.redis.set.return_value = True 
        return exec_instance

@pytest.mark.asyncio
async def test_idempotency_lock(executor):
    """Ensure trade proceeds if Lock is acquired"""
    
    # Mock the internal order placement to return a success
    executor._place_order_v3 = AsyncMock(return_value="ORDER_123")
    executor.verify_order_status = AsyncMock(return_value={"verified": True, "status": "complete"})
    executor._persist_trade = AsyncMock()

    adj = {
        "instrument_key": "NSE_FO|12345", 
        "quantity": 50, 
        "side": "BUY", 
        "strategy": "TEST",
        "cycle_id": "CYCLE_1"
    }
    
    res = await executor.execute_adjustment(adj)
    
    assert res["status"] == "PLACED"
    assert res["order_id"] == "ORDER_123"
