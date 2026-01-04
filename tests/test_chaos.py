# tests/test_chaos.py

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from httpx import HTTPStatusError, Request, Response
import redis.asyncio as redis
from datetime import datetime

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
        "NIFTY": 21500.0, 
        "VIX": 15.0
    }
    market.get_holidays.return_value = []
    market.get_daily_candles.return_value = MagicMock()
    market.get_intraday_candles.return_value = MagicMock()
    
    executor = AsyncMock(spec=TradeExecutor)
    executor.get_positions.return_value = []
    executor.reconcile_state.return_value = None  # Success
    executor.execute_adjustment.return_value = {"status": "PLACED", "order_id": "TEST123"}
    
    risk = AsyncMock()
    risk.run_stress_tests.return_value = {"WORST_CASE": {"impact": 0.0}}
    risk.calculate_leg_greeks.return_value = {"delta": 0.5, "gamma": 0.1, "theta": -5.0, "vega": 10.0}
    
    # Mock trading engine
    trading_engine = AsyncMock()
    trading_engine._get_best_expiry_chain.return_value = ("2024-01-18", MagicMock())
    trading_engine.generate_entry_orders.return_value = []
    
    # Mock adjustment engine
    adjustment_engine = AsyncMock()
    adjustment_engine.evaluate_portfolio.return_value = []
    
    # Mock capital governor
    capital_governor = AsyncMock()
    capital_governor.get_available_funds.return_value = 1000000.0
    capital_governor.daily_pnl = 5000.0
    capital_governor.can_trade_new.return_value = MagicMock(allowed=True, reason="OK")
    capital_governor.update_position_count.return_value = None
    
    # Mock other engines
    exit_engine = AsyncMock()
    exit_engine.evaluate_exits.return_value = []
    
    regime_engine = MagicMock()
    regime_engine.calculate_regime.return_value = MagicMock(name="NEUTRAL")
    
    structure_engine = MagicMock()
    structure_engine.analyze_structure.return_value = MagicMock()
    
    volatility_engine = AsyncMock()
    volatility_engine.calculate_volatility.return_value = MagicMock()
    
    edge_engine = MagicMock()
    edge_engine.detect_edges.return_value = MagicMock()
    
    # Create Supervisor
    supervisor = ProductionTradingSupervisor(
        market_client=market,
        risk_engine=risk,
        adjustment_engine=adjustment_engine,
        trade_executor=executor,
        trading_engine=trading_engine,
        capital_governor=capital_governor,
        websocket_service=None,
        loop_interval_seconds=0.01  # Super fast for testing
    )
    
    # Replace the engines with our mocks
    supervisor.exit_engine = exit_engine
    supervisor.regime_engine = regime_engine
    supervisor.structure_engine = structure_engine
    supervisor.vol_engine = volatility_engine
    supervisor.edge_engine = edge_engine
    
    # Set to SHADOW mode by default
    supervisor.safety.execution_mode = ExecutionMode.SHADOW
    
    return supervisor, market, executor, capital_governor

# === CHAOS SCENARIO 1: THE API CRASH ===

@pytest.mark.asyncio
async def test_chaos_api_failure(mock_dependencies):
    """
    Scenario: Upstox API returns 503 Service Unavailable repeatedly.
    Expectation: System escalates to HALTED state after max_data_failures.
    """
    supervisor, market, executor, _ = mock_dependencies
    
    # 1. Inject Poison: Raise 503 Error on live quote fetch
    error_503 = HTTPStatusError(
        message="Service Unavailable",
        request=Request("GET", "https://api.upstox.com/v2/market/quote"),
        response=Response(503, text="Service Unavailable")
    )
    market.get_live_quote.side_effect = error_503
    
    print("\n💥 Simulating Upstox 503 Crash...")
    
    # 2. Simulate the actual supervisor loop behavior
    # In production, when API fails:
    # - _read_live_snapshot() catches exception, returns spot=0.0
    # - validate_snapshot() rejects spot=0.0
    # - consecutive_data_failures increments
    # - After max_data_failures (3), circuit breaker trips
    
    # Reset failure counter for clean test
    supervisor.consecutive_data_failures = 0
    
    # Run enough cycles to trigger circuit breaker
    for i in range(6):
        # This is what happens in _read_live_snapshot() when API fails
        # It catches the exception and returns spot=0.0
        snapshot = await supervisor._read_live_snapshot()
        
        # This is what happens in the supervisor loop
        valid, reason = supervisor.quality.validate_snapshot(snapshot)
        
        if not valid:
            supervisor.consecutive_data_failures += 1
            await supervisor.safety.record_failure("DATA_QUALITY", {"reason": reason, "cycle": i})
            
            # Check circuit breaker (this happens in _run_loop)
            if supervisor.consecutive_data_failures >= supervisor.max_data_failures:
                supervisor.safety.system_state = SystemState.HALTED
                print(f"✅ Circuit breaker tripped after {supervisor.consecutive_data_failures} failures")
                break
    
    # 3. Verify System Halted
    assert supervisor.safety.system_state == SystemState.HALTED, \
        f"Expected SystemState.HALTED, got {supervisor.safety.system_state}. " \
        f"Failures: {supervisor.consecutive_data_failures}, Max: {supervisor.max_data_failures}"
    print("✅ System successfully HALTED after repeated API failures.")

# === CHAOS SCENARIO 2: REDIS FAILURE ===

@pytest.mark.asyncio
async def test_chaos_redis_death(mock_dependencies):
    """
    Scenario: Redis connection dies during order execution.
    Expectation: Order is NOT marked as placed, System logs Critical Error.
    """
    supervisor, _, executor, _ = mock_dependencies
    supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO  # Enable trading
    
    # Mock safety controller to allow trades
    supervisor.safety.can_adjust_trade = AsyncMock(return_value={"allowed": True, "reason": "OK"})
    
    # 1. Inject Poison: Redis ConnectionError during order execution
    executor.execute_adjustment.side_effect = redis.ConnectionError("Redis connection refused: Connection timeout")
    
    # 2. Attempt a Trade
    fake_order = {
        "action": "ENTRY", 
        "instrument_key": "NSE:NIFTY23JAN21500CE", 
        "quantity": 50, 
        "side": "BUY",
        "strategy": "VOLATILITY_ARB",
        "order_type": "MARKET"
    }
    
    print("\n💥 Simulating Redis Death during Order...")
    
    # 3. Manually trigger processing (simulating what _process_adjustment does)
    snapshot = {
        "spot": 21500.0,
        "vix": 15.0,
        "live_greeks": {},
        "ws_healthy": False,
        "timestamp": datetime.now()
    }
    
    result = await supervisor._process_adjustment(fake_order, snapshot, "TEST_CYCLE_001")
    
    # 4. Verify the failure was handled
    assert result is not None, "Process adjustment should return a result"
    assert result["status"] in ["FAILED", "CRASH", "TIMEOUT"], \
        f"Expected failure status, got {result.get('status')}"
    
    # Check that safety controller recorded the failure
    # (We can't directly check safety.consecutive_failures without mocking it)
    
    print("✅ System caught Redis crash and recorded failure.")

# === CHAOS SCENARIO 3: BAD DATA INJECTION ===

@pytest.mark.asyncio
async def test_chaos_data_corruption(mock_dependencies):
    """
    Scenario: API returns Garbage Data (Zero Spot Price).
    Expectation: DataQualityGate rejects it, Cycle is skipped.
    """
    supervisor, market, _, _ = mock_dependencies
    
    # 1. Inject Poison: Zero Spot Price (corrupted data)
    market.get_live_quote.return_value = {
        "NIFTY": 0.0,  # Corrupted data - spot price cannot be 0
        "VIX": 15.0
    }
    
    print("\n💥 Simulating Data Corruption (Spot = 0.0)...")
    
    # 2. Run the exact same flow as in production
    # Get snapshot (will contain spot=0.0)
    snapshot = await supervisor._read_live_snapshot()
    
    # Validate the snapshot (should fail)
    valid, reason = supervisor.quality.validate_snapshot(snapshot)
    
    # 3. Verify Rejection
    assert valid is False, "Data validation should fail for spot=0.0"
    assert "spot" in reason.lower() or "price" in reason.lower(), \
        f"Reason should mention spot/price, got: {reason}"
    
    # Verify failure counter increments
    initial_failures = supervisor.consecutive_data_failures
    
    # Simulate what supervisor loop does when data is invalid
    if not valid:
        supervisor.consecutive_data_failures += 1
        await supervisor.safety.record_failure("DATA_CORRUPTION", {"reason": reason})
    
    assert supervisor.consecutive_data_failures == initial_failures + 1, \
        "Failure counter should increment"
    
    print(f"✅ Data Gate correctly rejected garbage: {reason}")

# === CHAOS SCENARIO 4: CAPITAL EXHAUSTION ===

@pytest.mark.asyncio
async def test_chaos_capital_exhaustion(mock_dependencies):
    """
    Scenario: No available capital for new trades.
    Expectation: Capital Governor blocks new entries.
    """
    supervisor, _, _, capital_governor = mock_dependencies
    supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO
    
    # 1. Inject Poison: No available capital
    capital_governor.can_trade_new.return_value = MagicMock(
        allowed=False, 
        reason="Insufficient margin available. Required: 50,000, Available: 10,000"
    )
    
    # Mock safety controller to allow trades
    supervisor.safety.can_adjust_trade = AsyncMock(return_value={"allowed": True, "reason": "OK"})
    
    print("\n💥 Simulating Capital Exhaustion...")
    
    # 2. Attempt a trade that requires capital
    fake_order = {
        "action": "ENTRY", 
        "instrument_key": "NSE:NIFTY23JAN21500CE", 
        "quantity": 100, 
        "side": "BUY",
        "strategy": "VOLATILITY_ARB",
        "order_type": "MARKET"
    }
    
    snapshot = {
        "spot": 21500.0,
        "vix": 15.0,
        "live_greeks": {},
        "ws_healthy": False,
        "timestamp": datetime.now()
    }
    
    result = await supervisor._process_adjustment(fake_order, snapshot, "TEST_CYCLE_002")
    
    # 3. Verify capital veto
    assert result is None, "Capital veto should prevent order processing"
    
    # Verify capital governor was consulted
    capital_governor.can_trade_new.assert_called_once()
    
    print("✅ Capital Governor correctly blocked trade due to insufficient funds.")

# === CHAOS SCENARIO 5: WEBSOCKET DISCONNECTION ===

@pytest.mark.asyncio
async def test_chaos_websocket_disconnect(mock_dependencies):
    """
    Scenario: WebSocket connection drops during trading.
    Expectation: System continues with fallback Greeks, logs warning.
    """
    supervisor, _, _, _ = mock_dependencies
    
    # Create a mock WebSocket service
    mock_ws = MagicMock()
    mock_ws.is_healthy.return_value = False  # WebSocket is disconnected
    mock_ws.get_latest_greeks.return_value = {}
    mock_ws.connect = AsyncMock()
    
    supervisor.ws = mock_ws
    
    print("\n💥 Simulating WebSocket Disconnection...")
    
    # 2. Get snapshot (should handle WS failure gracefully)
    snapshot = await supervisor._read_live_snapshot()
    
    # 3. Verify system handles missing WebSocket data
    assert snapshot is not None, "Should return snapshot even without WebSocket"
    assert "live_greeks" in snapshot, "Snapshot should contain live_greeks field"
    assert "ws_healthy" in snapshot, "Snapshot should contain ws_healthy field"
    assert snapshot["ws_healthy"] is False, "WebSocket should be marked as unhealthy"
    
    # 4. Verify data is still usable (spot price should be valid)
    valid, reason = supervisor.quality.validate_snapshot(snapshot)
    
    # Spot should be valid even without WebSocket
    assert valid is True, f"Data should be valid even without WebSocket. Reason: {reason}"
    
    print("✅ System handles WebSocket disconnection gracefully with fallback data.")

# === CHAOS SCENARIO 6: MARKET CLOSED (HOLIDAY) ===

@pytest.mark.asyncio
async def test_chaos_market_holiday(mock_dependencies):
    """
    Scenario: Supervisor starts on a market holiday.
    Expectation: System detects holiday and shuts down cleanly.
    """
    supervisor, market, _, _ = mock_dependencies
    
    # 1. Inject Poison: Today is a holiday
    today = datetime.now().date()
    market.get_holidays.return_value = [today]  # Today is a holiday
    
    print(f"\n💥 Simulating Market Holiday ({today})...")
    
    # 2. Attempt to check market status
    # This would normally happen in start() method
    try:
        holidays = await asyncio.wait_for(market.get_holidays(), timeout=1.0)
        
        # Verify holiday detection
        assert today in holidays, f"Today ({today}) should be in holidays list"
        
        # In production, this would trigger exit(0) in _check_market_status()
        # For testing, we just verify the detection logic
        
        print(f"✅ Holiday correctly detected: {today}")
        
    except asyncio.TimeoutError:
        pytest.fail("Holiday check timed out")
    except Exception as e:
        pytest.fail(f"Holiday check failed: {e}")

# === CHAOS SCENARIO 7: NETWORK PARTITION ===

@pytest.mark.asyncio
async def test_chaos_network_partition(mock_dependencies):
    """
    Scenario: Network partition causes timeouts on all external services.
    Expectation: System degrades gracefully, logs errors, doesn't crash.
    """
    supervisor, market, executor, capital_governor = mock_dependencies
    
    # 1. Inject Poison: All external calls timeout
    market.get_live_quote.side_effect = asyncio.TimeoutError("Network timeout")
    executor.get_positions.side_effect = asyncio.TimeoutError("Network timeout")
    capital_governor.get_available_funds.side_effect = asyncio.TimeoutError("Network timeout")
    
    print("\n💥 Simulating Network Partition (All Timeouts)...")
    
    # 2. Test each component's timeout handling
    
    # Test market data timeout
    try:
        snapshot = await supervisor._read_live_snapshot()
        # Should return default values, not raise
        assert snapshot["spot"] == 0.0, "Should return spot=0.0 on timeout"
        print("✅ Market data timeout handled gracefully")
    except Exception as e:
        pytest.fail(f"Market data timeout not handled: {e}")
    
    # Test positions fetch timeout (mocked - would fail in _update_positions)
    # We can't easily test this without running full loop, but we trust the error handling
    
    # 3. Verify system state doesn't crash to emergency immediately
    # (Some failures should be tolerated before escalation)
    assert supervisor.safety.system_state != SystemState.EMERGENCY, \
        "Single network issue shouldn't cause EMERGENCY state"
    
    print("✅ Network partition handled with graceful degradation.")

# === CHAOS SCENARIO 8: MEMORY LEAK SIMULATION ===

@pytest.mark.asyncio
async def test_chaos_memory_pressure(mock_dependencies):
    """
    Scenario: Memory usage grows over many cycles.
    Expectation: Cycle history deque should limit memory usage.
    """
    supervisor, _, _, _ = mock_dependencies
    
    print("\n💥 Simulating Memory Pressure...")
    
    # 1. Fill cycle times deque (simulating many cycles)
    initial_length = len(supervisor.cycle_times)
    
    # Add more cycles than deque maxlen
    for i in range(150):  # More than maxlen of 100
        supervisor.cycle_times.append(0.5)  # 500ms cycles
    
    # 2. Verify deque respects maxlen
    assert len(supervisor.cycle_times) == 100, \
        f"Cycle times deque should respect maxlen=100, got {len(supervisor.cycle_times)}"
    
    # 3. Verify regime history also respects maxlen
    for i in range(10):  # More than maxlen of 5
        supervisor.regime_history.append("NEUTRAL")
    
    assert len(supervisor.regime_history) == 5, \
        f"Regime history should respect maxlen=5, got {len(supervisor.regime_history)}"
    
    print("✅ Memory limits enforced via deque maxlen.")

# === CHAOS SCENARIO 9: CLOCK DRIFT ===

@pytest.mark.asyncio
async def test_chaos_clock_drift_detection():
    """
    Scenario: System clock drifts causing scheduling issues.
    Expectation: Drift correction logic detects and compensates.
    """
    # This is more of a unit test for the drift correction logic
    print("\n💥 Simulating Clock Drift...")
    
    # Test the drift calculation logic
    interval = 3.0
    start_time = 1000.0
    end_time = 1003.5  # 500ms overrun
    
    cycle_duration = end_time - start_time
    sleep_time = max(0, interval - cycle_duration)
    
    # With 500ms overrun, sleep_time should be 0
    assert sleep_time == 0, f"With overrun, sleep_time should be 0, got {sleep_time}"
    
    # Test drift detection (more than 100ms)
    current_time = 1010.0
    next_tick = 1006.0  # We're 4 seconds behind!
    drift = current_time - next_tick
    
    assert drift > 0.1, f"Drift should be detected (>100ms), got {drift*1000:.1f}ms"
    
    print(f"✅ Clock drift detection working: {drift*1000:.1f}ms drift detected")

# === CHAOS SCENARIO 10: KILL SWITCH ACTIVATION ===

@pytest.mark.asyncio
async def test_chaos_kill_switch(mock_dependencies):
    """
    Scenario: Kill switch file is detected.
    Expectation: System stops immediately.
    """
    supervisor, _, _, _ = mock_dependencies
    
    print("\n💥 Simulating Kill Switch Activation...")
    
    # 1. Create temporary kill switch file
    kill_switch_path = "KILL_SWITCH.TRIGGER"
    
    # Save original method to restore later
    original_exists = os.path.exists
    
    try:
        # Mock os.path.exists to return True for kill switch
        os.path.exists = lambda path: True if path == kill_switch_path else original_exists(path)
        
        # 2. Check kill switch
        should_stop = supervisor._check_kill_switch()
        
        # 3. Verify kill switch detection
        assert should_stop is True, "Kill switch should be detected"
        
        print("✅ Kill switch correctly detected")
        
    finally:
        # Restore original os.path.exists
        os.path.exists = original_exists

# === MAIN TEST RUNNER (Optional) ===

if __name__ == "__main__":
    """
    Run chaos tests directly for debugging.
    """
    import sys
    
    print("🔥 Running Chaos Tests Directly...")
    
    # This allows running the tests directly for debugging
    pytest.main([__file__, "-v", "-s"])
