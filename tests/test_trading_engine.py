import pytest
from app.core.trading.engine import TradingEngine
from app.core.trading.executor import TradeExecutor
from unittest.mock import patch

def test_trading_engine_basics():
    engine = TradingEngine()
    assert isinstance(engine, TradingEngine)
    # Check for core methods we know exist
    assert hasattr(engine, 'analyze_market') or hasattr(engine, 'evaluate_signals')

@pytest.mark.asyncio
async def test_trade_executor_basics():
    # Patch httpx.Limits to prevent instantiation error in tests
    with patch("httpx.Limits"):
        executor = TradeExecutor(access_token="test_token")
        assert executor.client is not None
        await executor.close()
