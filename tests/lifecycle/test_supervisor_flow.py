import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path
from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.lifecycle.safety_controller import SystemState

@pytest.mark.asyncio
async def test_kill_switch_activation():
    """Test file-based kill switch"""
    Path("state").mkdir(exist_ok=True)
    kill_file = Path("state/KILL_SWITCH.TRIGGER")
    if kill_file.exists(): kill_file.unlink()

    sup = ProductionTradingSupervisor(
        AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), None
    )

    # 1. Healthy
    assert sup._check_kill_switch() is False

    # 2. Kill Activated
    kill_file.write_text("EMERGENCY")
    assert sup._check_kill_switch() is True
    
    # Cleanup
    kill_file.unlink()

@pytest.mark.asyncio
async def test_supervisor_loop_logic():
    """Test one iteration of the run loop"""
    sup = ProductionTradingSupervisor(
        AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock(), None
    )
    
    # Mock dependencies
    sup.market.get_live_quote.return_value = {"spot": 21500, "vix": 15}
    sup.risk.run_stress_tests.return_value = {"STATUS": "PASS"}
    sup.exec.get_positions.return_value = []
    
    # Run ONE single cycle logic (bypassing the while True loop)
    await sup._run_loop()
    
    # Assertions
    sup.market.get_live_quote.assert_awaited()
    sup.risk.run_stress_tests.assert_awaited()
