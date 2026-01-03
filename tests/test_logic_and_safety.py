import pytest
import asyncio
import time
from unittest.mock import patch, Mock
import pandas as pd
import numpy as np
from app.lifecycle.safety_controller import SafetyController, SystemState, ExecutionMode
from app.core.risk.capital_governor import CapitalGovernor
from app.core.analytics.volatility import VolatilityEngine
from app.core.analytics.structure import StructureEngine
from app.core.analytics.edge import EdgeEngine
from app.core.analytics.regime import RegimeEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.risk.engine import RiskEngine
from app.core.risk.stress_tester import StressTester
from app.core.data.quality_gate import DataQualityGate
from app.schemas.analytics import VolMetrics, StructMetrics, EdgeMetrics, ExtMetrics

# --- SAFETY CONTROLLER TESTS ---
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

# --- CAPITAL GOVERNOR TESTS ---
def test_can_trade_new_insufficient_capital():
    governor = CapitalGovernor(total_capital=1000000)
    governor.update_state(margin=800000, count=5)
    allowed, reason = governor.can_trade_new(300000, {"strategy": "ENTRY"})
    assert allowed == False
    assert "Insufficient Capital" in reason

def test_can_trade_new_hedge_allowed():
    governor = CapitalGovernor(total_capital=1000000)
    governor.update_state(margin=900000, count=9)
    allowed, reason = governor.can_trade_new(200000, {"strategy": "HEDGE"})
    assert allowed == True

# --- ANALYTICS TESTS ---
@pytest.mark.asyncio
async def test_volatility_engine_calculation(mock_market_data):
    engine = VolatilityEngine()
    # Mock data generation
    dates = pd.date_range(end=pd.Timestamp.now(), periods=400)
    nh = pd.DataFrame({'close': np.random.randn(400) + 20000, 'high': 20100, 'low': 19900}, index=dates)
    vh = pd.DataFrame({'close': np.random.randn(400) + 12}, index=dates)
    
    result = await engine.calculate_volatility(nh, vh, 21500, 14.2)
    assert isinstance(result, VolMetrics)
    assert result.spot > 0

def test_structure_engine_calculation(mock_option_chain):
    engine = StructureEngine()
    
    # FIX: Ensure spot is aligned with strikes in mock_option_chain
    # strikes are 21000...21900. Spot at 21500 is perfect.
    result = engine.analyze_structure(mock_option_chain, 21500.0, 50)
    
    assert isinstance(result, StructMetrics)
    assert result.pcr > 0 

def test_regime_engine_calculation():
    engine = RegimeEngine()
    vol = VolMetrics(21500, 14.2, 85, 12, 13, 12, 13, 12, 13, 25, 35, 45, False)
    st = StructMetrics(2.5e8, "STICKY", 1.1, 21450, 50, 0.5, "NEUTRAL")
    ed = EdgeMetrics(12, 13, 0.5, 0.5, 0.3, 0.4, 0.6, 0.4, 0.5, "SHORT_GAMMA")
    ex = ExtMetrics(1500, 500, 0, [], False)
    
    result = engine.calculate_regime(vol, st, ed, ex)
    assert result.name in ["AGGRESSIVE_SHORT", "MODERATE_SHORT", "LONG_VOL / DEFENSIVE", "NEUTRAL"]

# --- ADJUSTMENT ENGINE TESTS ---
@pytest.mark.asyncio
async def test_evaluate_portfolio_delta_breach(test_settings):
    config = test_settings.model_dump()
    engine = AdjustmentEngine(config)
    
    # FIX: Use a realistic Delta Breach.
    # 0.50 Delta is too small to hedge (Round(0.5 / 50) = 0).
    # We use 60.0, which rounds to 1 Lot (50).
    portfolio_risk = {"aggregate_metrics": {"delta": 60.0}} 
    market = {"spot": 21500}
    
    with patch('app.core.trading.adjustment_engine.registry') as mock_reg:
        mock_reg.get_current_future.return_value = "FUT"
        mock_reg.get_instrument_details.return_value = {"lot_size": 50}
        
        adjs = await engine.evaluate_portfolio(portfolio_risk, market)
        
        assert len(adjs) == 1
        assert adjs[0]["action"] == "DELTA_HEDGE"
        assert adjs[0]["quantity"] == 50 # Snapped to nearest lot

# --- RISK ENGINE TESTS ---
def test_check_breaches_gamma_breach(test_settings):
    engine = RiskEngine(test_settings.model_dump())
    metrics = {"gamma": 0.20, "vega": 500} # Gamma limit 0.15
    breaches = engine.check_breaches(metrics)
    assert len(breaches) == 1
    assert breaches[0]["limit"] == "GAMMA"

def test_stress_test_calculation(mock_position):
    tester = StressTester()
    result = tester.simulate_scenarios({"p1": mock_position}, 21500, 14.0)
    assert "WORST_CASE" in result
    assert len(result["matrix"]) == 6

# --- DATA QUALITY TESTS (The Final Check) ---
def test_validate_snapshot_valid():
    gate = DataQualityGate()
    # Good Data
    is_valid, reason = gate.validate_snapshot({"spot": 21500.50, "vix": 14.2})
    assert is_valid == True
    assert reason == "OK"

def test_validate_snapshot_zero_spot():
    gate = DataQualityGate()
    # Dangerous Data (Exchange Glitch)
    is_valid, reason = gate.validate_snapshot({"spot": 0.0, "vix": 14.2})
    assert is_valid == False
    assert "Invalid Spot" in reason

def test_validate_snapshot_negative_vix():
    gate = DataQualityGate()
    # Impossible Data
    is_valid, reason = gate.validate_snapshot({"spot": 21500.0, "vix": -1.0})
    assert is_valid == False
    assert "Invalid VIX" in reason

def test_validate_structure_empty():
    gate = DataQualityGate()
    # API returned empty chain
    is_valid, reason = gate.validate_structure(pd.DataFrame())
    assert is_valid == False
    # FIX: Corrected expected string to match DataQualityGate implementation
    assert "Empty Option Chain" in reason
