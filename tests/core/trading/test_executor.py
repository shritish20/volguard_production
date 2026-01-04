import pytest
from unittest.mock import AsyncMock, patch
from app.core.trading.executor import TradeExecutor

# We mock httpx.AsyncClient entirely to avoid network calls during init
@pytest.fixture
def executor():
    with patch("httpx.AsyncClient") as mock_client_cls:
        # The mock client instance
        mock_client_instance = AsyncMock()
        mock_client_cls.return_value = mock_client_instance
        
        exec_instance = TradeExecutor("token")
        
        # Mock Redis
        exec_instance.redis = AsyncMock()
        exec_instance.redis.set.return_value = True 
        
        return exec_instance

@pytest.mark.asyncio
async def test_idempotency_lock(executor):
    """Ensure trade proceeds if Lock is acquired"""
    # Setup internal methods
    executor._place_order_v3 = AsyncMock(return_value="ORDER_123")
    executor.verify_order_status = AsyncMock(return_value={"verified": True, "status": "complete"})
    executor._persist_trade = AsyncMock()
    executor._fetch_ltp_v3 = AsyncMock(return_value=100.0) # Handle Limit Logic

    adj = {
        "instrument_key": "NSE_FO|12345", 
        "quantity": 50, 
        "side": "BUY", 
        "strategy": "TEST",
        "cycle_id": "CYCLE_1",
        "action": "ENTRY"
    }
    
    res = await executor.execute_adjustment(adj)
    
    assert res["status"] == "PLACED"
    assert res["order_id"] == "ORDER_123"
