import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from app.core.supervisor import ProductionTradingSupervisor
from app.core.safety.safety_controller import ExecutionMode

@pytest.mark.asyncio
async def test_kill_switch_activation(mock_supervisor_dependencies):
    """Ensure supervisor stops when running is set to False"""
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
    
    supervisor.running = True
    task = asyncio.create_task(supervisor.start())
    await asyncio.sleep(0.02)
    supervisor.running = False
    await task
    assert supervisor.running is False

@pytest.mark.asyncio
async def test_supervisor_loop_logic(mock_supervisor_dependencies):
    """Test that the main loop calls market data fetch"""
    
    # 1. Setup Mock Market Client to work in loop
    mock_market = mock_supervisor_dependencies["market"]
    # Ensure get_live_quote returns a dict (not awaited coroutine error)
    mock_market.get_live_quote.return_value = {
        "NSE_INDEX|Nifty 50": 21500.0, 
        "NSE_INDEX|India VIX": 14.5
    }

    supervisor = ProductionTradingSupervisor(
        market_client=mock_market,
        risk_engine=mock_supervisor_dependencies["risk"],
        adjustment_engine=mock_supervisor_dependencies["adj"],
        trade_executor=mock_supervisor_dependencies["executor"],
        trading_engine=mock_supervisor_dependencies["engine"],
        capital_governor=AsyncMock(),
        websocket_service=mock_supervisor_dependencies["ws"],
        loop_interval_seconds=0.001  # Very fast loop
    )
    supervisor.safety.execution_mode = ExecutionMode.PAPER
    supervisor.running = True

    # 2. Start Loop
    task = asyncio.create_task(supervisor.start())
    
    # 3. Wait enough time for at least one cycle
    await asyncio.sleep(0.05)
    
    # 4. Stop Loop
    supervisor.running = False
    await task

    # 5. Verify Market Call
    # We check if it was called at least once
    assert mock_market.get_live_quote.call_count >= 1
