import pytest
from app.core.trading.engine import TradingEngine
from app.core.trading.executor import TradeExecutor
from unittest.mock import AsyncMock, patch

def test_trading_engine_basics():
    # Inject required Mocks
    mock_market = AsyncMock()
    mock_config = {}
    
    engine = TradingEngine(market_client=mock_market, config=mock_config)
    
    assert isinstance(engine, TradingEngine)
    # Check for method existence (safely)
    assert hasattr(engine, 'analyze_market') or hasattr(engine, 'evaluate_signals')

@pytest.mark.asyncio
async def test_trade_executor_basics():
    # Patch AsyncClient to prevent real network init
    with patch("httpx.AsyncClient"):
        executor = TradeExecutor(access_token="test_token")
        assert executor.client is not None
        await executor.close()
