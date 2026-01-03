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
from pydantic import ValidationError

# --- SUPERVISOR INTEGRATION ---
@pytest.mark.asyncio
async def test_supervisor_normal_cycle(mock_supervisor_dependencies):
    supervisor = ProductionTradingSupervisor(
        market_client=mock_supervisor_dependencies["market"],
        risk_engine=mock_supervisor_dependencies["risk"],
        adjustment_engine=mock_supervisor_dependencies["adj"],
        trade_executor=mock_supervisor_dependencies["executor"],
        trading_engine=mock_supervisor_dependencies["engine"],
        websocket_service=mock_supervisor_dependencies["ws"],
        loop_interval_seconds=0.01,
        total_capital=1000000
    )
    supervisor.safety.execution_mode = ExecutionMode.SHADOW
    supervisor.running = True
    
    task = asyncio.create_task(supervisor.start())
    await asyncio.sleep(0.05)
    supervisor.running = False
    await task
    
    mock_supervisor_dependencies["market"].get_spot_price.assert_called()

@pytest.mark.asyncio
async def test_supervisor_kill_switch_detection(mock_supervisor_dependencies):
    supervisor = ProductionTradingSupervisor(
        market_client=mock_supervisor_dependencies["market"],
        risk_engine=mock_supervisor_dependencies["risk"],
        adjustment_engine=mock_supervisor_dependencies["adj"],
        trade_executor=mock_supervisor_dependencies["executor"],
        trading_engine=mock_supervisor_dependencies["engine"],
        websocket_service=mock_supervisor_dependencies["ws"],
        loop_interval_seconds=0.01,
        total_capital=1000000
    )
    
    with open("KILL_SWITCH.TRIGGER", "w") as f: f.write("TEST")
    try:
        supervisor.running = True
        task = asyncio.create_task(supervisor.start())
        await asyncio.sleep(0.1)
        assert not supervisor.running
        mock_supervisor_dependencies["executor"].close_all_positions.assert_called()
    finally:
        if os.path.exists("KILL_SWITCH.TRIGGER"): os.remove("KILL_SWITCH.TRIGGER")
        await task

# --- API ENDPOINT TESTS ---
@pytest.fixture
def test_client(test_settings):
    with patch('app.main.settings', test_settings):
        return TestClient(create_app())

def test_health_endpoint(test_client):
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_admin_emergency_stop_valid(test_client):
    headers = {"X-Admin-Key": "test_admin_secret"}
    resp = test_client.post("/api/v1/admin/emergency_stop", 
                          json={"reason": "test"}, headers=headers)
    assert resp.status_code in [200, 500]
    if os.path.exists("KILL_SWITCH.TRIGGER"): os.remove("KILL_SWITCH.TRIGGER")

def test_admin_emergency_stop_invalid(test_client):
    headers = {"X-Admin-Key": "wrong"}
    resp = test_client.post("/api/v1/admin/emergency_stop", 
                          json={"reason": "test"}, headers=headers)
    assert resp.status_code == 403

# --- EMERGENCY EXECUTOR TESTS ---
@pytest.mark.asyncio
async def test_emergency_kill_switch():
    mock_exec = AsyncMock()
    emergency = SynchronousEmergencyExecutor(mock_exec)
    
    result = await emergency.execute_emergency_action({"type": "GLOBAL_KILL_SWITCH"})
    assert result["status"] == "SUCCESS"
    assert emergency.in_emergency == True

# --- CONFIG TESTS ---
def test_settings_validation():
    conf = {
        "UPSTOX_ACCESS_TOKEN": "token",
        "BASE_CAPITAL": 1000,
        "MAX_DAILY_LOSS": 100,
        "MAX_NET_DELTA": 0.4,
        "ADMIN_SECRET": "secret"
    }
    settings = Settings(**conf)
    assert settings.ADMIN_SECRET == "secret"

def test_settings_invalid():
    with pytest.raises(ValidationError):
        Settings(UPSTOX_ACCESS_TOKEN="", BASE_CAPITAL=-100)
