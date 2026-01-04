import pytest
import asyncio
import pandas as pd
import numpy as np
from unittest.mock import patch

from app.lifecycle.safety_controller import SafetyController, SystemState, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.regime import RegimeEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.risk.engine import RiskEngine
from app.core.data.quality_gate import DataQualityGate
from app.schemas.analytics import VolMetrics, StructMetrics, EdgeMetrics, ExtMetrics

@pytest.mark.asyncio
async def test_safety_controller_initial_state():
    controller = SafetyController()
    assert controller.system_state == SystemState.NORMAL
    assert controller.execution_mode == ExecutionMode.SHADOW

@pytest.mark.asyncio
async def test_can_adjust_trade_halted():
    controller = SafetyController()
    controller.system_state = SystemState.HALTED
    result = await controller.can_adjust_trade({"action": "TEST"})
    assert result["allowed"] == False

@pytest.mark.asyncio
async def test_record_failure_escalation():
    controller = SafetyController()
    for i in range(5):
        await controller.record_failure("API_ERROR", {"attempt": i})
    assert controller.system_state == SystemState.HALTED

@pytest.mark.asyncio
async def test_can_trade_new_insufficient_capital():
    governor = CapitalGovernor(access_token="test", total_capital=1000000)
    governor.daily_pnl = -25000 
    result = await governor.can_trade_new([{"strategy": "ENTRY"}])
    assert result.allowed == False
    assert "Loss Reached" in result.reason

@pytest.mark.asyncio
async def test_can_trade_new_hedge_allowed():
    governor = CapitalGovernor(access_token="test", total_capital=1000000)
    governor.position_count = 10 
    leg = {
        "action": "EXIT", 
        "strategy": "HEDGE",
        "instrument_key": "NSE_INDEX|Nifty 50",
        "quantity": 50,
        "side": "BUY"
    }
    with patch.object(governor, 'predict_margin_requirement', return_value=0.0):
        with patch.object(governor, 'get_available_funds', return_value=100000.0):
             result = await governor.can_trade_new([leg])
    assert result.allowed == True

@pytest.mark.asyncio
async def test_volatility_engine_calculation():
    engine = VolatilityEngine()
    
    dates = pd.date_range(end=pd.Timestamp.now(), periods=400)
    
    # Daily Data
    nh = pd.DataFrame({
        'close': np.random.randn(400) + 20000, 
        'high': 20100, 
        'low': 19900,
        'timestamp': dates
    })
    nh = nh.reset_index(drop=True)
    
    # Intraday Data (FIX: Added timestamp column here)
    vh = pd.DataFrame({
        'close': np.random.randn(400) + 12,
        'high': np.random.randn(400) + 13, # Added High/Low for Parkinson Vol
        'low': np.random.randn(400) + 11,
        'volume': 1000,
        'oi': 5000,
        'timestamp': dates 
    })
    vh = vh.reset_index(drop=True)
    
    result = await engine.calculate_volatility(nh, vh, 21500, 14.2)
    assert isinstance(result, VolMetrics)
    assert result.spot > 0

def test_structure_engine_calculation(mock_option_chain):
    engine = StructureEngine()
    result = engine.analyze_structure(mock_option_chain, 21500.0, 50)
    assert isinstance(result, StructMetrics)
    assert result.pcr > 0

def test_regime_engine_calculation():
    engine = RegimeEngine()
    vol = VolMetrics(21500, 14.2, 85, 12, 13, 12, 13, 12, 13, 25, 35, 45, False)
    st = StructMetrics(2.5e8, "STICKY", 1.1, 21450, 50, 0.5, "NEUTRAL")
    ed = EdgeMetrics(12, 13, 0.5, 0.5, 0.3, 0.4, 0.6, 0.4, 0.5, "SHORT_GAMMA")
    ex = ExtMetrics(0, 0, 0, [], False)
    
    result = engine.calculate_regime(vol, st, ed, ex)
    assert result.name in ["AGGRESSIVE_SHORT", "MODERATE_SHORT", "LONG_VOL", "NEUTRAL", "CASH"]

@pytest.mark.asyncio
async def test_evaluate_portfolio_delta_breach(test_settings):
    config = test_settings.model_dump()
    engine = AdjustmentEngine(delta_threshold=15.0)
    portfolio_risk = {"aggregate_metrics": {"delta": 60.0}}
    market = {"spot": 21500}
    
    adjs = await engine.evaluate_portfolio(portfolio_risk, market)
        
    assert len(adjs) == 1
    assert adjs[0]["action"] == "ENTRY"
    assert adjs[0]["strategy"] == "DELTA_HEDGE"

@pytest.mark.asyncio
async def test_stress_test_calculation(mock_position):
    engine = RiskEngine()
    snapshot = {"spot": 21500, "vix": 14.0}
    positions = {"p1": mock_position}
    
    result = await engine.run_stress_tests({}, snapshot, positions)
    assert "WORST_CASE" in result
    assert len(result["matrix"]) == 15

def test_validate_snapshot_valid():
    gate = DataQualityGate()
    is_valid, reason = gate.validate_snapshot({"spot": 21500.50, "vix": 14.2})
    assert is_valid == True

def test_validate_snapshot_zero_spot():
    gate = DataQualityGate()
    is_valid, reason = gate.validate_snapshot({"spot": 0.0, "vix": 14.2})
    assert is_valid == False

def test_validate_snapshot_negative_vix():
    gate = DataQualityGate()
    is_valid, reason = gate.validate_snapshot({"spot": 21500.0, "vix": -1.0})
    assert is_valid == False

def test_validate_structure_empty():
    gate = DataQualityGate()
    is_valid, reason = gate.validate_structure(pd.DataFrame())
    assert is_valid == False
    assert "Empty Option Chain" in reason
