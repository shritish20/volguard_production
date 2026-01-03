"""
Market Data Client Tests - API integration and data fetching
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
from unittest.mock import AsyncMock, patch
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY

# === MARKET DATA CLIENT TESTS ===
@pytest.fixture
def market_client():
    """Create MarketDataClient with mock token"""
    return MarketDataClient(
        access_token="test_token",
        base_url_v2="https://test.upstox.com/v2",
        base_url_v3="https://test.upstox.com/v3"
    )

@pytest.mark.asyncio
async def test_close_client():
    """Test client cleanup"""
    client = MarketDataClient("token", "v2", "v3")
    
    with patch.object(client.client, 'aclose') as mock_close:
        await client.close()
        mock_close.assert_called_once()

@pytest.mark.asyncio
async def test_get_history_success(market_client):
    """Test successful historical data fetch"""
    mock_response = {
        "data": {
            "candles": [
                ["2024-01-01T09:15:00+05:30", 21000, 21100, 20900, 21050, 1000000, 500000],
                ["2024-01-02T09:15:00+05:30", 21050, 21150, 21000, 21100, 1100000, 550000]
            ]
        }
    }
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        df = await market_client.get_history(NIFTY_KEY, days=10)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'close' in df.columns
        assert df.index.name == 'timestamp'
        assert df['close'].iloc[0] == 21050

@pytest.mark.asyncio
async def test_get_history_empty(market_client):
    """Test historical data fetch with empty response"""
    mock_response = {"data": {"candles": []}}
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        df = await market_client.get_history(NIFTY_KEY)
        
        assert df.empty
        assert isinstance(df, pd.DataFrame)

@pytest.mark.asyncio
async def test_get_history_retry(market_client):
    """Test retry logic on failure"""
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        # First call fails, second succeeds
        mock_get.side_effect = [
            Exception("First failure"),
            AsyncMock(status_code=200, json=AsyncMock(return_value={"data": {"candles": []}}))
        ]
        
        df = await market_client.get_history(NIFTY_KEY)
        
        assert mock_get.call_count == 2  # Retried once
        assert df.empty

@pytest.mark.asyncio
async def test_get_live_quote_success(market_client):
    """Test successful live quote fetch"""
    mock_response = {
        "data": {
            "NSE_INDEX|Nifty 50": {"last_price": 21500.50},
            "NSE_INDEX|India VIX": {"last_price": 14.20}
        }
    }
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        quotes = await market_client.get_live_quote([NIFTY_KEY, VIX_KEY])
        
        assert NIFTY_KEY in quotes
        assert VIX_KEY in quotes
        assert quotes[NIFTY_KEY] == 21500.50
        assert quotes[VIX_KEY] == 14.20

@pytest.mark.asyncio
async def test_get_live_quote_zero_nifty(market_client):
    """Test zero Nifty price detection"""
    mock_response = {
        "data": {
            "NSE_INDEX|Nifty 50": {"last_price": 0.0},  # Zero price
            "NSE_INDEX|India VIX": {"last_price": 14.20}
        }
    }
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        with pytest.raises(ValueError, match="Received Zero/Null price for NIFTY Index"):
            await market_client.get_live_quote([NIFTY_KEY, VIX_KEY])

@pytest.mark.asyncio
async def test_get_spot_price(market_client):
    """Test spot price fetch"""
    with patch.object(market_client, 'get_live_quote') as mock_quote:
        mock_quote.return_value = {NIFTY_KEY: 21500.50}
        
        spot = await market_client.get_spot_price()
        assert spot == 21500.50

@pytest.mark.asyncio
async def test_get_spot_price_fallback(market_client):
    """Test spot price fetch with failure"""
    with patch.object(market_client, 'get_live_quote', side_effect=Exception("API Error")):
        spot = await market_client.get_spot_price()
        assert spot == 0.0  # Fallback value

@pytest.mark.asyncio
async def test_get_vix(market_client):
    """Test VIX fetch"""
    with patch.object(market_client, 'get_live_quote') as mock_quote:
        mock_quote.return_value = {VIX_KEY: 14.20}
        
        vix = await market_client.get_vix()
        assert vix == 14.20

@pytest.mark.asyncio
async def test_get_option_chain_success(market_client):
    """Test option chain fetch"""
    mock_response = {
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
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        chain = await market_client.get_option_chain("2024-12-26")
        
        assert isinstance(chain, pd.DataFrame)
        assert len(chain) == 1
        assert chain.iloc[0]['strike'] == 21500
        assert chain.iloc[0]['ce_iv'] == 0.15
        assert chain.iloc[0]['pe_iv'] == 0.18
        assert chain.iloc[0]['ce_delta'] == 0.45
        assert chain.iloc[0]['pe_delta'] == -0.55

@pytest.mark.asyncio
async def test_get_option_chain_empty(market_client):
    """Test empty option chain response"""
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"data": []}
        
        chain = await market_client.get_option_chain("2024-12-26")
        assert chain.empty

@pytest.mark.asyncio
async def test_get_option_chain_api_failure(market_client):
    """Test option chain API failure"""
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 500
        
        chain = await market_client.get_option_chain("2024-12-26")
        assert chain.empty

@pytest.mark.asyncio
async def test_get_expiries_and_lot_success(market_client):
    """Test expiry and lot size fetch"""
    mock_response = {
        "data": [
            {"expiry": "2024-12-26", "lot_size": "50"},
            {"expiry": "2025-01-30", "lot_size": "50"}
        ]
    }
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        weekly, monthly, lot = await market_client.get_expiries_and_lot()
        
        assert weekly == "2024-12-26"
        assert monthly == "2025-01-30"
        assert lot == 50

@pytest.mark.asyncio
async def test_get_expiries_and_lot_no_future_dates(market_client):
    """Test expiry fetch with no future dates"""
    mock_response = {
        "data": [
            {"expiry": "2023-12-01", "lot_size": "50"},  # Past date
            {"expiry": "2023-12-15", "lot_size": "50"}   # Past date
        ]
    }
    
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        weekly, monthly, lot = await market_client.get_expiries_and_lot()
        
        assert weekly is None
        assert monthly is None
        assert lot == 50  # Should still get lot size

@pytest.mark.asyncio
async def test_get_expiries_and_lot_api_failure(market_client):
    """Test expiry fetch with API failure"""
    with patch.object(market_client.client, 'get', new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 500
        
        weekly, monthly, lot = await market_client.get_expiries_and_lot()
        
        assert weekly is None
        assert monthly is None
        assert lot == 0
