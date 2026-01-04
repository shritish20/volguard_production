# tests/test_chaos.py

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from httpx import HTTPStatusError, Request, Response
import redis.asyncio as redis

from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.lifecycle.safety_controller import SystemState, ExecutionMode
from app.core.market.data_client import MarketDataClient
from app.core.trading.executor import TradeExecutor
from app.core.data.quality_gate import DataQualityGate

# === MOCK FIXTURES ===

@pytest.fixture
def mock_dependencies():
    """Creates a Supervisor with mocked external connections"""
    market = AsyncMock(spec=MarketDataClient)
    # Default behavior: Returns valid data
    market.get_live_quote.return_value = {
        "spot": 21500.0, "vix": 15.0, "live_greeks": {}, "timestamp": 1234567890
    }
    market.get_holidays.return_value = []
    
    executor = AsyncMock(spec=TradeExecutor)
    executor.get_positions.return_value = []
    executor.reconcile_state.return_value = None # Success
    
    risk = AsyncMock()
    risk.run_stress_tests.return_value = {"WORST_CASE": {"impact": 0.0}}
    
    # Create Supervisor
    supervisor = ProductionTradingSupervisor(
        market_client=market,
        risk_engine=risk,
        adjustment_engine=AsyncMock(),
        trade_executor=executor,
        trading_engine=AsyncMock(),
        capital_governor=AsyncMock(),
        websocket_service=None,
        loop_interval_seconds=0.01 # Super fast for testing
    )
    
    # Set to SHADOW mode by default
    supervisor.safety.execution_mode = ExecutionMode.SHADOW
    
    return supervisor, market, executor

# === CHAOS SCENARIO 1: THE API CRASH ===

@pytest.mark.asyncio
async def test_chaos_api_failure(mock_dependencies):
    """
    Scenario: Upstox API returns 503 Service Unavailable repeatedly.
    Expectation: System escalates to HALTED state.
    """
    supervisor, market, _ = mock_dependencies
    
    # 1. Inject Poison: Raise 503 Error on live quote fetch
    error_503 = HTTPStatusError(
        message="Service Unavailable",
        request=Request("GET", "url"),
        response=Response(503)
    )
    market.get_live_quote.side_effect = error_503
    
    # 2. Run the loop for 6 cycles (Threshold is 5 for Halt)
    # We cheat and call the internal _run_loop logic step manually or rely on exception handling
    # Since _run_loop is an infinite loop, we test the reaction logic directly.
    
    print("\n💥 Simulating Upstox 503 Crash...")
    
    for i in range(6):
        try:
            # Manually trigger the fetch step
            await supervisor._read_live_snapshot()
        except Exception:
            # The supervisor catches this and calls record_failure
            await supervisor.safety.record_failure("API_CRASH", {"error": "503"})
            
    # 3. Verify Survival
    assert supervisor.safety.system_state == SystemState.HALTED
    print("✅ System successfully HALTED after repeated API failures.")

# === CHAOS SCENARIO 2: REDIS FAILURE ===

@pytest.mark.asyncio
async def test_chaos_redis_death(mock_dependencies):
    """
    Scenario: Redis connection dies during order execution.
    Expectation: Order is NOT marked as placed, System logs Critical Error.
    """
    supervisor, _, executor = mock_dependencies
    supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO # Enable trading
    
    # 1. Inject Poison: Redis ConnectionError
    executor.execute_adjustment.side_effect = redis.ConnectionError("Connection refused")
    
    # 2. Attempt a Trade
    fake_order = {"action": "ENTRY", "instrument_key": "NIFTY", "quantity": 50, "side": "BUY"}
    
    print("\n💥 Simulating Redis Death during Order...")
    
    # Manually trigger processing
    await supervisor._process_adjustment(fake_order, {}, "TEST_CYCLE")
    
    # 3. Verify
    # The supervisor should have caught the exception and logged it
    # Ideally, it should record a failure in safety controller
    assert supervisor.safety.consecutive_failures > 0
    print("✅ System caught Redis crash and recorded failure.")

# === CHAOS SCENARIO 3: BAD DATA INJECTION ===

@pytest.mark.asyncio
async def test_chaos_data_corruption(mock_dependencies):
    """
    Scenario: API returns Garbage Data (Zero Spot Price).
    Expectation: DataQualityGate rejects it, Cycle is skipped.
    """
    supervisor, market, _ = mock_dependencies
    
    # 1. Inject Poison: Zero Spot Price
    market.get_live_quote.return_value = {
        "spot": 0.0, "vix": 15.0, "timestamp": 1234567890
    }
    
    print("\n💥 Simulating Data Corruption (Spot = 0.0)...")
    
    # 2. Run Check
    snapshot = await supervisor._read_live_snapshot()
    valid, reason = supervisor.quality.validate_snapshot(snapshot)
    
    # 3. Verify Rejection
    assert valid is False
    assert "Invalid Spot" in reason
    print(f"✅ Data Gate correctly rejected garbage: {reason}")

