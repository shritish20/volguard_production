"""
Approval System Tests - Manual approval workflow for SEMI_AUTO mode
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from app.services.approval_system import ManualApprovalSystem, ApprovalRequest
from app.database import AsyncSessionLocal, Base, engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

# === APPROVAL SYSTEM TESTS ===
@pytest.fixture
async def approval_system():
    """Create approval system with test database"""
    # Create in-memory SQLite database for testing
    test_engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False
    )
    
    # Create tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Create session factory
    TestSessionLocal = sessionmaker(
        test_engine,
        class_=AsyncMock,  # Use AsyncMock for async session
        expire_on_commit=False
    )
    
    # Mock AsyncSessionLocal
    with patch('app.services.approval_system.AsyncSessionLocal', TestSessionLocal):
        system = ManualApprovalSystem()
        yield system
    
    # Cleanup
    await test_engine.dispose()

@pytest.fixture
def sample_adjustment():
    """Sample trade adjustment for testing"""
    return {
        "action": "DELTA_HEDGE",
        "instrument_key": "NSE_INDEX:Nifty 50-FUT",
        "quantity": 50,
        "side": "BUY",
        "strategy": "HEDGE",
        "reason": "Delta breach"
    }

@pytest.fixture
def sample_market_snapshot():
    """Sample market snapshot"""
    return {
        "spot": 21500.50,
        "vix": 14.2,
        "timestamp": datetime.now().isoformat()
    }

@pytest.mark.asyncio
async def test_request_approval(sample_adjustment, sample_market_snapshot):
    """Test creating approval request"""
    # Mock the session to avoid database issues
    mock_session = AsyncMock()
    mock_request = MagicMock(id="test-id-123")
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        system = ManualApprovalSystem()
        
        # Mock session operations
        mock_session.add = MagicMock()
        mock_session.commit = AsyncMock()
        
        req_id = await system.request_approval(
            adjustment=sample_adjustment,
            market=sample_market_snapshot
        )
        
        assert req_id is not None
        assert len(req_id) > 10  # Some ID

@pytest.mark.asyncio
async def test_check_approval_status():
    """Test checking approval status"""
    system = ManualApprovalSystem()
    
    # Mock database query
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    mock_request.expires_at = datetime.utcnow() + timedelta(minutes=5)
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        status = await system.check_approval_status("test-id")
        assert status == "PENDING"

@pytest.mark.asyncio
async def test_approve_request():
    """Test approving a request"""
    system = ManualApprovalSystem()
    
    # Mock request
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        result = await system.approve_request("test-id")
        assert result == True
        assert mock_request.status == "APPROVED"
        mock_session.commit.assert_called_once()

@pytest.mark.asyncio
async def test_reject_request():
    """Test rejecting a request"""
    system = ManualApprovalSystem()
    
    # Mock request
    mock_request = MagicMock()
    mock_request.status = "PENDING"
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock(return_value=mock_request)
    mock_session.commit = AsyncMock()
    
    with patch('app.services.approval_system.AsyncSessionLocal', return_value=mock_session):
        result = await system.reject_request("test-id")
        assert result == True
        assert mock_request.status == "REJECTED"
        mock_session.commit.assert_called_once()

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
