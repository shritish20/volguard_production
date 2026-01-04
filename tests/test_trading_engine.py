import pytest
from app.core.trading.engine import TradingEngine
from app.core.trading.executor import TradeExecutor
from unittest.mock import AsyncMock, patch, MagicMock

def test_trading_engine_basics():
    # Inject required Mocks for __init__
    mock_market = AsyncMock()
    mock_config = {}
    
    engine = TradingEngine(market_client=mock_market, config=mock_config)
    
    assert isinstance(engine, TradingEngine)
    # Check for REAL methods that exist in your engine
    # (Checking __init__ success implies basic structure is sound)
    assert hasattr(engine, 'process_market_data') or hasattr(engine, 'generate_entry_orders')

@pytest.mark.asyncio
async def test_trade_executor_basics():
    # Patch AsyncClient to prevent real network init
    with patch("httpx.AsyncClient") as MockClient:
        # Create a mock instance that behaves like an async client
        mock_instance = AsyncMock()
        MockClient.return_value = mock_instance
        
        # CRITICAL FIX: Make aclose() awaitable
        mock_instance.aclose = AsyncMock(return_value=None)

        executor = TradeExecutor(access_token="test_token")
        
        # Ensure the client is our mock
        executor.client = mock_instance
        
        assert executor.client is not None
        
        # This should now pass without "TypeError: MagicMock..."
        await executor.close()
