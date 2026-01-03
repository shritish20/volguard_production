"""
Approval System Tests - Manual approval workflow for SEMI_AUTO mode
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from app.services.approval_system import ManualApprovalSystem, ApprovalRequest

# === APPROVAL SYSTEM TESTS ===
@pytest.mark.asyncio
async def test_request_approval():
    """Test creating approval request"""
    system = ManualApprovalSystem()
    
    adjustment = {
        "action": "DELTA_HEDGE",
        "instrument_key": "NSE_INDEX:Nifty 50-FUT",
        "quantity": 50,
        "side": "BUY",
        "strategy": "HEDGE",
        "reason": "Delta breach"
    }
    
    market = {
        "spot": 21500.50,
        "vix": 14.2,
        "timestamp": datetime.now().isoformat()
    }
    
    # Mock the session
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        req_id = await system.request_approval(adjustment, market)
        
        assert req_id is not None
        assert isinstance(req_id, str)

@pytest.mark.asyncio
async def test_check_approval_status():
    """Test checking approval status"""
    system = ManualApprovalSystem()
    
    # Create a mock request
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    mock_request.expires_at = datetime.utcnow() + timedelta(minutes=5)
    
    # Mock session
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        status = await system.check_approval_status("test-id")
        assert status == "PENDING"

@pytest.mark.asyncio
async def test_approve_request():
    """Test approving a request"""
    system = ManualApprovalSystem()
    
    # Create a mock request that's pending
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    
    # Mock session
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        result = await system.approve_request("test-id")
        assert result == True
        assert mock_request.status == "APPROVED"

@pytest.mark.asyncio
async def test_reject_request():
    """Test rejecting a request"""
    system = ManualApprovalSystem()
    
    # Create a mock request that's pending
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    
    # Mock session
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        result = await system.reject_request("test-id")
        assert result == True
        assert mock_request.status == "REJECTED"

def test_approval_request_model():
    """Test ApprovalRequest database model"""
    # Create instance
    request = ApprovalRequest(
        id="test-id-123",
        timestamp=datetime.utcnow(),
        expires_at=datetime.utcnow() + timedelta(minutes=5),
        adjustment={"action": "TEST"},
        market_snapshot={"spot": 21500},
        status="PENDING"
    )
    
    assert request.id == "test-id-123"
    assert request.status == "PENDING"
    assert isinstance(request.timestamp, datetime)
    assert isinstance(request.expires_at, datetime)
    assert request.adjustment == {"action": "TEST"}
    assert request.market_snapshot == {"spot": 21500}
    assert request.decision_time is None
