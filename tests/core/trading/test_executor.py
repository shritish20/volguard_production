import pytest
from unittest.mock import AsyncMock
from app.core.trading.executor import TradeExecutor

@pytest.mark.asyncio
async def test_idempotency_lock(mock_redis):
    """Ensure we don't double order"""
    exec = TradeExecutor("token")
    exec.redis = mock_redis
    
    # Mock internals
    exec._place_order_v3 = AsyncMock(return_value="ORD-1")
    exec._persist_trade = AsyncMock()
    exec.verify_order_status = AsyncMock(return_value={"status": "complete"})

    adj = {"cycle_id": "1", "action": "ENTRY", "strategy": "TEST"}

    # 1. Lock available (First pass)
    mock_redis.set.return_value = True
    res = await exec.execute_adjustment(adj)
    assert res["status"] == "PLACED"

    # 2. Lock taken (Second pass - Dupe)
    mock_redis.set.return_value = False
    res = await exec.execute_adjustment(adj)
    assert res["status"] == "SKIPPED"
