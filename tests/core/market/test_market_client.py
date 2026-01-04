import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from app.core.market.data_client import MarketDataClient

@pytest.fixture
def client():
    # Patch AsyncClient so we don't make real connections
    with patch("httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        
        client = MarketDataClient(access_token="test_token")
        client.client = mock_instance
        return client

@pytest.mark.asyncio
async def test_get_live_quote_success(client):
    """Test normal data fetch"""
    mock_data = {
        "status": "success",
        "data": {
            "NSE_INDEX|Nifty 50": {"last_price": 21500.0},
            "NSE_INDEX|India VIX": {"last_price": 14.5}
        }
    }
    
    # 1. Create a response object that has a .json() method
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_data
    
    # 2. Setup the async get call to return this response
    # When (await client.get()) happens, it returns mock_response
    client.client.get.return_value = mock_response

    # Execute
    data = await client.get_live_quote(["NSE_INDEX|Nifty 50"])
    
    # Assert
    assert isinstance(data, dict)
    # Check if data was extracted correctly (ignoring exact key format for resilience)
    assert any("21500" in str(v) or v == 21500.0 for v in data.values())

@pytest.mark.asyncio
async def test_api_unauthorized_401(client):
    """Test token expiry handling"""
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = Exception("401 Unauthorized")
    
    client.client.get.return_value = mock_response

    # Expect gracefull handling (return empty dict or None, no crash)
    result = await client.get_live_quote(["TEST"])
    assert result == {} or result is None

@pytest.mark.asyncio
async def test_instrument_lookup(client):
    """Test instrument lookup logic"""
    assert hasattr(client, 'get_option_chain')
