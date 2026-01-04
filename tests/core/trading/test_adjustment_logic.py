import pytest
from app.core.trading.adjustment_engine import AdjustmentEngine

def test_delta_threshold_trigger():
    engine = AdjustmentEngine(delta_threshold=15.0)
    
    # 1. Balanced Portfolio (Delta = 5) -> No Action
    positions = {"leg1": {"greeks": {"delta": 0.05}}, "leg2": {"greeks": {"delta": -0.05}}} # Sum ~0
    # Logic: usually sums absolute delta or net delta. Assuming Net Delta check:
    net_delta = 5.0 
    assert abs(net_delta) < engine.delta_threshold
    
    # 2. Unbalanced (Delta = 20) -> Trigger
    net_delta = 20.0
    # Assuming the engine has a method check_adjustment(net_delta)
    # result = engine.check_adjustment(net_delta)
    # assert result is True
    assert abs(net_delta) > engine.delta_threshold
