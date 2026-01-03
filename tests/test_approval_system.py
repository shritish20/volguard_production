"""
Approval System Tests - Manual approval workflow for SEMI_AUTO mode
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch
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
        class_=AsyncSessionLocal.__class__,
        expire_on_commit=False
    )
    
    # Patch the database session
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
async def test_request_approval(approval_system, sample_adjustment, sample_market_snapshot):
    """Test creating approval request"""
    req_id = await approval_system.request_approval(
        adjustment=sample_adjustment,
        market=sample_market_snapshot
    )
    
    assert req_id is not None
    assert len(req_id) == 36  # UUID length
    
    # Verify request was saved
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        assert request is not None
        assert request.status == "PENDING"
        assert request.adjustment == sample_adjustment
        assert request.market_snapshot == sample_market_snapshot
        
        # Check expiry time (should be 5 minutes from now)
        expected_expiry = datetime.utcnow() + timedelta(minutes=5)
        time_diff = abs((request.expires_at - expected_expiry).total_seconds())
        assert time_diff < 2  # Within 2 seconds

@pytest.mark.asyncio
async def test_check_approval_status_pending(approval_system, sample_adjustment, sample_market_snapshot):
    """Test checking pending approval"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    status = await approval_system.check_approval_status(req_id)
    assert status == "PENDING"

@pytest.mark.asyncio
async def test_check_approval_status_expired(approval_system, sample_adjustment, sample_market_snapshot):
    """Test expired approval"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Manually set expiry to past
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        request.expires_at = datetime.utcnow() - timedelta(minutes=1)
        await session.commit()
    
    status = await approval_system.check_approval_status(req_id)
    assert status == "EXPIRED"
    
    # Verify auto-update to EXPIRED
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        assert request.status == "EXPIRED"

@pytest.mark.asyncio
async def test_check_approval_status_nonexistent(approval_system):
    """Test checking non-existent approval"""
    status = await approval_system.check_approval_status("NON_EXISTENT_ID")
    assert status == "UNKNOWN"

@pytest.mark.asyncio
async def test_approve_request(approval_system, sample_adjustment, sample_market_snapshot):
    """Test approving a request"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Initial status should be PENDING
    status = await approval_system.check_approval_status(req_id)
    assert status == "PENDING"
    
    # Approve
    result = await approval_system.approve_request(req_id)
    assert result == True
    
    # Check updated status
    status = await approval_system.check_approval_status(req_id)
    assert status == "APPROVED"
    
    # Verify decision time
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        assert request.status == "APPROVED"
        assert request.decision_time is not None

@pytest.mark.asyncio
async def test_approve_already_approved(approval_system, sample_adjustment, sample_market_snapshot):
    """Test approving already approved request"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Approve once
    result1 = await approval_system.approve_request(req_id)
    assert result1 == True
    
    # Try to approve again
    result2 = await approval_system.approve_request(req_id)
    assert result2 == False  # Should fail

@pytest.mark.asyncio
async def test_approve_expired_request(approval_system, sample_adjustment, sample_market_snapshot):
    """Test approving expired request"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Manually expire
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        request.expires_at = datetime.utcnow() - timedelta(minutes=1)
        await session.commit()
    
    # Auto-expire
    await approval_system.check_approval_status(req_id)
    
    # Try to approve expired request
    result = await approval_system.approve_request(req_id)
    assert result == False

@pytest.mark.asyncio
async def test_reject_request(approval_system, sample_adjustment, sample_market_snapshot):
    """Test rejecting a request"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Reject
    result = await approval_system.reject_request(req_id)
    assert result == True
    
    # Check updated status
    status = await approval_system.check_approval_status(req_id)
    assert status == "REJECTED"
    
    # Verify decision time
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        assert request.status == "REJECTED"
        assert request.decision_time is not None

@pytest.mark.asyncio
async def test_reject_already_decided(approval_system, sample_adjustment, sample_market_snapshot):
    """Test rejecting already decided request"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # First approve
    await approval_system.approve_request(req_id)
    
    # Try to reject
    result = await approval_system.reject_request(req_id)
    assert result == False  # Should fail

@pytest.mark.asyncio
async def test_concurrent_approval_checks(approval_system, sample_adjustment, sample_market_snapshot):
    """Test concurrent approval status checks"""
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Run multiple checks concurrently
    tasks = [
        approval_system.check_approval_status(req_id),
        approval_system.check_approval_status(req_id),
        approval_system.check_approval_status(req_id)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # All should return PENDING
    assert all(status == "PENDING" for status in results)

@pytest.mark.asyncio
async def test_approval_persistence(approval_system, sample_adjustment, sample_market_snapshot):
    """Test approval persistence across system instances"""
    # Create first instance and request
    req_id = await approval_system.request_approval(sample_adjustment, sample_market_snapshot)
    
    # Create new instance (simulating system restart)
    new_system = ManualApprovalSystem()
    
    # Should still find the request
    status = await new_system.check_approval_status(req_id)
    assert status == "PENDING"
    
    # Approve via new instance
    result = await new_system.approve_request(req_id)
    assert result == True
    
    # Check via original instance
    status = await approval_system.check_approval_status(req_id)
    assert status == "APPROVED"

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

@pytest.mark.asyncio
async def test_approval_workflow_integration(approval_system):
    """Test complete approval workflow"""
    # 1. Request approval
    adjustment = {
        "action": "ENTRY",
        "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
        "quantity": 50,
        "side": "SELL",
        "strategy": "STRANGLE"
    }
    
    market = {"spot": 21500.50, "vix": 14.2}
    
    req_id = await approval_system.request_approval(adjustment, market)
    assert await approval_system.check_approval_status(req_id) == "PENDING"
    
    # 2. Wait (simulate) and check status
    # Status should still be PENDING
    assert await approval_system.check_approval_status(req_id) == "PENDING"
    
    # 3. Approve
    assert await approval_system.approve_request(req_id) == True
    assert await approval_system.check_approval_status(req_id) == "APPROVED"
    
    # 4. Try to approve again (should fail)
    assert await approval_system.approve_request(req_id) == False
    assert await approval_system.reject_request(req_id) == False  # Already decided
    
    # 5. Check final state
    async with AsyncSessionLocal() as session:
        request = await session.get(ApprovalRequest, req_id)
        assert request.status == "APPROVED"
        assert request.decision_time is not None
        assert request.adjustment == adjustment
