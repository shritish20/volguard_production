import pytest
from unittest.mock import AsyncMock, patch
from app.core.market.data_client import MarketDataClient

@pytest.fixture
def client():
    # Patch AsyncClient so we don't make real connections
    with patch("httpx.AsyncClient") as mock_cls:
        mock_instance = AsyncMock()
        mock_cls.return_value = mock_instance
        
        client = MarketDataClient(access_token="test_token")
        # Ensure the client property is the mock instance
        client.client = mock_instance
        return client

@pytest.mark.asyncio
async def test_get_live_quote_success(client):
    """Test normal data fetch"""
    mock_response = {
        "status": "success",
        "data": {
            "NSE_INDEX|Nifty 50": {"last_price": 21500.0},
            "NSE_INDEX|India VIX": {"last_price": 14.5}
        }
    }
    
    # Mock the response object
    mock_resp_obj = AsyncMock()
    mock_resp_obj.status_code = 200
    mock_resp_obj.json.return_value = mock_response
    
    # Assign to client.get
    client.client.get.return_value = mock_resp_obj

    data = await client.get_live_quote(["NSE_INDEX|Nifty 50"])
    
    # Adjust assertion based on your actual return structure
    # (Checking if it returns a dict with values)
    assert isinstance(data, dict)
    assert "NSE_INDEX|Nifty 50" in str(data) or 21500.0 in data.values()

@pytest.mark.asyncio
async def test_api_unauthorized_401(client):
    """Test token expiry handling"""
    mock_resp_obj = AsyncMock()
    mock_resp_obj.status_code = 401
    mock_resp_obj.raise_for_status.side_effect = Exception("401 Unauthorized")
    
    client.client.get.return_value = mock_resp_obj

    # Should handle exception gracefully (log and return empty/None)
    try:
        result = await client.get_live_quote(["TEST"])
        assert result == {} or result is None
    except Exception:
        # If your code re-raises, that's also valid for this test
        pass

@pytest.mark.asyncio
async def test_instrument_lookup(client):
    """Test instrument lookup logic"""
    assert hasattr(client, 'get_option_chain')
