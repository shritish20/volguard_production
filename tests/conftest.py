import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
import pandas as pd
from app.config import settings

# --- SCOPE: SESSION ---
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# --- MOCKS ---
@pytest.fixture
def mock_redis():
    """Mock Redis for Idempotency Checks"""
    mock = AsyncMock()
    mock.set.return_value = True
    mock.get.return_value = None
    return mock

@pytest.fixture
def mock_market_client():
    client = AsyncMock()
    client.get_live_quote.return_value = {
        "NSE_INDEX|Nifty 50": 21500.0,
        "NSE_INDEX|India VIX": 14.5
    }
    client.get_option_chain.return_value = []
    return client

@pytest.fixture
def mock_executor():
    exc = AsyncMock()
    exc.verify_order_status.return_value = {
        "status": "complete", 
        "filled_quantity": 50, 
        "average_price": 100.0,
        "verified": True
    }
    exc.execute_adjustment.return_value = {
        "status": "PLACED",
        "order_id": "test_order_123"
    }
    return exc

@pytest.fixture
def mock_supervisor_dependencies(mock_market_client, mock_executor):
    """Bundles all dependencies for Supervisor instantiation"""
    return {
        "market": mock_market_client,
        "risk": AsyncMock(),
        "adj": AsyncMock(),
        "executor": mock_executor,
        "engine": AsyncMock(),
        "ws": AsyncMock()
    }

@pytest.fixture
def mock_option_chain():
    """Returns a DataFrame with ALL required columns (Fixed KeyError)"""
    return pd.DataFrame({
        'strike': [21400, 21500, 21600],
        'ce_iv': [15.0, 14.5, 14.0],
        'pe_iv': [16.0, 15.5, 15.0],
        'ce_delta': [0.6, 0.5, 0.4],
        'pe_delta': [-0.4, -0.5, -0.6],
        'ce_gamma': [0.001, 0.002, 0.001],
        'pe_gamma': [0.001, 0.002, 0.001],
        'ce_theta': [-10, -12, -10],
        'pe_theta': [-10, -12, -10],
        'ce_vega': [5, 6, 5],
        'pe_vega': [5, 6, 5],
        'ce_oi': [100000, 200000, 150000],  # ADDED
        'pe_oi': [150000, 200000, 100000]   # ADDED
    })

@pytest.fixture
def mock_position():
    return {
        "symbol": "NIFTY21500CE",
        "quantity": 50,
        "side": "BUY",
        "average_price": 100.0,
        "current_price": 110.0,
        "pnl": 500.0,
        "greeks": {"delta": 0.5, "gamma": 0.001}
    }

@pytest.fixture
def test_settings():
    return settings
