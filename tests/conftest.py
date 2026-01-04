import pytest
import pytest_asyncio
from unittest.mock import Mock, AsyncMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from app.config import Settings

@pytest.fixture
def test_settings():
    return Settings(
        UPSTOX_ACCESS_TOKEN="test_token",
        UPSTOX_BASE_V2="https://test.upstox.com/v2",
        UPSTOX_BASE_V3="https://test.upstox.com/v3",
        BASE_CAPITAL=1000000,
        MAX_DAILY_LOSS=20000,
        MAX_NET_DELTA=0.40,
        MAX_GAMMA=0.15,
        MAX_VEGA=1000,
        ADMIN_SECRET="test_admin_secret",
        ENVIRONMENT="development"  # Use allowed environment
    )

@pytest.fixture
def mock_market_data():
    return {
        "spot": 21500.50,
        "vix": 14.2,
        "live_greeks": {}
    }

@pytest.fixture
def mock_option_chain():
    # Strikes aligned with spot 21500
    data = {
        'strike': [21000, 21100, 21200, 21300, 21400, 21500, 21600, 21700, 21800, 21900],
        'ce_key': [f"NSE_INDEX:Nifty 50-{i}-CE" for i in range(10)],
        'pe_key': [f"NSE_INDEX:Nifty 50-{i}-PE" for i in range(10)],
        'ce_iv': [0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24],
        'pe_iv': [0.25, 0.24, 0.23, 0.22, 0.21, 0.20, 0.19, 0.18, 0.17, 0.16],
        'ce_delta': [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05],
        'pe_delta': [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6, -0.7, -0.8, -0.9, -0.95],
        'ce_oi': [1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500],
        'pe_oi': [5500, 5000, 4500, 4000, 3500, 3000, 2500, 2000, 1500, 1000],
        'ce_gamma': [0.01] * 10,
        'pe_gamma': [0.01] * 10
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_position():
    return {
        "position_id": "test_position_1",
        "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
        "symbol": "NIFTY23DEC21500CE",
        "quantity": -50,
        "side": "SELL",
        "average_price": 120.50,
        "current_price": 115.25,
        "pnl": 262.50,
        "strike": 21500.0,
        "expiry": datetime.now() + timedelta(days=7),
        "lot_size": 50,
        "option_type": "CE"
    }

@pytest.fixture
def mock_supervisor_dependencies():
    market = AsyncMock()
    market.get_live_quote.return_value = {"NSE_INDEX|Nifty 50": 21500.50, "NSE_INDEX|India VIX": 14.2}
    market.get_spot_price.return_value = 21500.50
    market.get_vix.return_value = 14.2
    
    executor = AsyncMock()
    executor.get_positions.return_value = []
    
    adj = AsyncMock()
    adj.evaluate_portfolio.return_value = []
    
    engine = AsyncMock()
    engine.generate_entry_orders.return_value = []
    
    return {
        "market": market,
        "risk": AsyncMock(),
        "adj": adj,
        "executor": executor,
        "engine": engine,
        "ws": AsyncMock()
    }
