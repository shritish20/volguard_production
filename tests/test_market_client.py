"""
Market Data Client Tests - API integration and data fetching
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY

# === MARKET DATA CLIENT TESTS ===
@pytest.fixture
def market_client():
    """Create MarketDataClient with mock token"""
    client = MarketDataClient(
        access_token="test_token",
        base_url_v2="https://test.upstox.com/v2",
        base_url_v3="https://test.upstox.com/v3"
    )
    return client

@pytest.mark.asyncio
async def test_get_history_success():
    """Test successful historical data fetch"""
    client = MarketDataClient("token", "v2", "v3")
    
    # Mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {
            "candles": [
                ["2024-01-01T09:15:00+05:30", 21000, 21100, 20900, 21050, 1000000, 500000],
                ["2024-01-02T09:15:00+05:30", 21050, 21150, 21000, 21100, 1100000, 550000]
            ]
        }
    }
    
    # Mock client.get to return response immediately (not coroutine)
    with patch.object(client.client, 'get', return_value=mock_response):
        df = await client.get_history(NIFTY_KEY, days=10)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'close' in df.columns
        assert df.index.name == 'timestamp'
        assert df['close'].iloc[0] == 21050

@pytest.mark.asyncio
async def test_get_history_empty():
    """Test historical data fetch with empty response"""
    client = MarketDataClient("token", "v2", "v3")
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"candles": []}}
    
    with patch.object(client.client, 'get', return_value=mock_response):
        df = await client.get_history(NIFTY_KEY)
        assert df.empty

@pytest.mark.asyncio
async def test_get_live_quote_success():
    """Test successful live quote fetch"""
    client = MarketDataClient("token", "v2", "v3")
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {
            "NSE_INDEX|Nifty 50": {"last_price": 21500.50},
            "NSE_INDEX|India VIX": {"last_price": 14.20}
        }
    }
    
    with patch.object(client.client, 'get', return_value=mock_response):
        quotes = await client.get_live_quote([NIFTY_KEY, VIX_KEY])
        
        assert NIFTY_KEY in quotes
        assert VIX_KEY in quotes
        assert quotes[NIFTY_KEY] == 21500.50
        assert quotes[VIX_KEY] == 14.20

@pytest.mark.asyncio
async def test_get_spot_price():
    """Test spot price fetch"""
    client = MarketDataClient("token", "v2", "v3")
    
    with patch.object(client, 'get_live_quote') as mock_quote:
        mock_quote.return_value = {NIFTY_KEY: 21500.50}
        
        spot = await client.get_spot_price()
        assert spot == 21500.50

@pytest.mark.asyncio
async def test_get_vix():
    """Test VIX fetch"""
    client = MarketDataClient("token", "v2", "v3")
    
    with patch.object(client, 'get_live_quote') as mock_quote:
        mock_quote.return_value = {VIX_KEY: 14.20}
        
        vix = await client.get_vix()
        assert vix == 14.20

@pytest.mark.asyncio
async def test_get_option_chain_success():
    """Test option chain fetch"""
    client = MarketDataClient("token", "v2", "v3")
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {
                "strike_price": 21500,
                "call_options": {
                    "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
                    "option_greeks": {"iv": 0.15, "delta": 0.45, "gamma": 0.012},
                    "market_data": {"oi": 1000}
                },
                "put_options": {
                    "instrument_key": "NSE_INDEX:Nifty 50-21500-PE",
                    "option_greeks": {"iv": 0.18, "delta": -0.55, "gamma": 0.011},
                    "market_data": {"oi": 1500}
                }
            }
        ]
    }
    
    with patch.object(client.client, 'get', return_value=mock_response):
        chain = await client.get_option_chain("2024-12-26")
        
        assert isinstance(chain, pd.DataFrame)
        assert len(chain) == 1
        assert chain.iloc[0]['strike'] == 21500
        assert chain.iloc[0]['ce_iv'] == 0.15
        assert chain.iloc[0]['pe_iv'] == 0.18

@pytest.mark.asyncio
async def test_close_client():
    """Test client cleanup"""
    client = MarketDataClient("token", "v2", "v3")
    
    with patch.object(client.client, 'aclose') as mock_close:
        await client.close()
        mock_close.assert_called_once()
