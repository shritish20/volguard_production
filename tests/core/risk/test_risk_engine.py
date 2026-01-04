import pytest
import numpy as np
from app.core.risk.engine import RiskEngine

@pytest.fixture
def engine():
    return RiskEngine()

def test_black_scholes_math(engine):
    """Verify Option Pricing Model matches textbook values"""
    # S=100, K=100, T=1yr, r=5%, v=20% -> Call Price ~ 10.45
    price = engine._black_scholes(S=100, K=100, T=1.0, r=0.05, sigma=0.20, flag="CE")
    assert 10.40 < price < 10.50

def test_iv_solver_convergence(engine):
    """Verify we can back-solve IV from Price"""
    # If Call Price is 10.45, IV should be ~20%
    greeks = engine.calculate_leg_greeks(
        price=10.45, spot=100, strike=100, time_years=1.0, r=0.05, opt_type="CE"
    )
    assert 0.19 < greeks['iv'] < 0.21

def test_stress_test_worst_case(engine):
    """Verify portfolio stress testing logic"""
    # Portfolio: Short 1 Call (Delta = -0.5)
    # If market rises, we lose money.
    snapshot = {"spot": 21500}
    positions = {
        "ShortCall": {
            "quantity": 50, "side": "SELL",
            "greeks": {"delta": 0.5, "gamma": 0.0} # Net delta -0.5 per unit
        }
    }
    
    # Run Async Stress Test
    import asyncio
    res = asyncio.run(engine.run_stress_tests({}, snapshot, positions))
    
    # 5% up move = 21500 * 1.05. 
    # Loss approx = Delta * Change * Qty = -0.5 * 1075 * 50 = -26,875
    worst_impact = res["WORST_CASE"]["impact"]
    assert worst_impact < -20000 
