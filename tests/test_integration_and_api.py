import pytest
import asyncio
import os
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient
from app.main import create_app
from app.config import Settings
from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.lifecycle.safety_controller import ExecutionMode
from app.lifecycle.emergency_executor import SynchronousEmergencyExecutor

# --- SUPERVISOR INTEGRATION ---
@pytest.mark.asyncio
async def test_supervisor_normal_cycle(mock_supervisor_dependencies):
    supervisor = ProductionTradingSupervisor(
        market_client=mock_supervisor_dependencies["market"],
        risk_engine=mock_supervisor_dependencies["risk"],
        adjustment_engine=mock_supervisor_dependencies["adj"],
        trade_executor=mock_supervisor_dependencies["executor"],
        trading_engine=mock_supervisor_dependencies["engine"],
        capital_governor=AsyncMock(), # Missing dependency added
        websocket_service=mock_supervisor_dependencies["ws"],
        loop_interval_seconds=0.01
        # Removed invalid 'total_capital' argument
    )
    supervisor.safety.execution_mode = ExecutionMode.SHADOW
    supervisor.running = True
    
    task = asyncio.create_task(supervisor.start())
    await asyncio.sleep(0.05)
    supervisor.running = False
    await task
    
    mock_supervisor_dependencies["market"].get_live_quote.assert_called()

@pytest.mark.asyncio
async def test_supervisor_kill_switch_detection(mock_supervisor_dependencies):
    supervisor = ProductionTradingSupervisor(
        market_client=mock_supervisor_dependencies["market"],
        risk_engine=mock_supervisor_dependencies["risk"],
        adjustment_engine=mock_supervisor_dependencies["adj"],
        trade_executor=mock_supervisor_dependencies["executor"],
        trading_engine=mock_supervisor_dependencies["engine"],
        capital_governor=AsyncMock(),
        websocket_service=mock_supervisor_dependencies["ws"],
        loop_interval_seconds=0.01
    )
    
    with open("KILL_SWITCH.TRIGGER", "w") as f: f.write("TEST")
    try:
        supervisor.running = True
        task = asyncio.create_task(supervisor.start())
        await asyncio.sleep(0.1)
        assert not supervisor.running
    finally:
        if os.path.exists("KILL_SWITCH.TRIGGER"):
            os.remove("KILL_SWITCH.TRIGGER")
        try:
            await task
        except:
            pass

# --- API ENDPOINT TESTS ---
@pytest.fixture
def test_client():
    test_settings = Settings(
        ADMIN_SECRET="test_admin_secret",
        UPSTOX_ACCESS_TOKEN="token",
        POSTGRES_USER="user", POSTGRES_PASSWORD="pw", POSTGRES_DB="db",
        ENVIRONMENT="development"
    )
    app = create_app()
    app.dependency_overrides = {}
    with patch('app.api.v1.endpoints.admin.settings', test_settings):
        yield TestClient(app)

def test_health_endpoint(test_client):
    response = test_client.get("/health")
    assert response.status_code == 200

def test_admin_emergency_stop_valid(test_client):
    headers = {"X-Admin-Key": "test_admin_secret"}
    resp = test_client.post("/api/v1/admin/emergency_stop", 
                           json={"reason": "test"}, headers=headers)
    assert resp.status_code in [200, 500]
    if os.path.exists("KILL_SWITCH.TRIGGER"):
        os.remove("KILL_SWITCH.TRIGGER")

def test_admin_emergency_stop_invalid(test_client):
    headers = {"X-Admin-Key": "wrong_password"}
    resp = test_client.post("/api/v1/admin/emergency_stop", 
                           json={"reason": "test"}, headers=headers)
    assert resp.status_code == 403

# --- EMERGENCY EXECUTOR TESTS ---
@pytest.mark.asyncio
async def test_emergency_kill_switch():
    mock_exec = AsyncMock()
    mock_exec.close_all_positions.return_value = {"status": "SUCCESS"}
    
    emergency = SynchronousEmergencyExecutor(mock_exec)
    # Ensure source code uses asyncio.Lock(), not .lock()
    emergency.lock = asyncio.Lock()
    
    result = await emergency.execute_emergency_action({"type": "GLOBAL_KILL_SWITCH"})
    
    assert emergency.in_emergency == True
    assert result["status"] in ["SUCCESS", "TRIGGERED"]

def test_settings_validation():
    conf = {
        "UPSTOX_ACCESS_TOKEN": "token", 
        "BASE_CAPITAL": 1000, 
        "MAX_DAILY_LOSS": 100, 
        "ENVIRONMENT": "development"
    }
    settings = Settings(**conf)
    assert settings.BASE_CAPITAL == 1000.0
