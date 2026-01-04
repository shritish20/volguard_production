import pytest
from unittest.mock import AsyncMock, patch
from datetime import date
from app.core.risk.capital_governor import CapitalGovernor

@pytest.fixture
def gov():
    return CapitalGovernor("token", total_capital=1000000)

@pytest.mark.asyncio
async def test_block_trade_on_max_loss(gov):
    """Ensure trading stops if daily loss limit hit"""
    gov.daily_pnl = -6000.0 # Limit is 5000
    res = await gov.can_trade_new([{"quantity": 50, "side": "BUY"}])
    assert res.allowed is False
    assert "Max Daily Loss" in res.reason

@pytest.mark.asyncio
async def test_full_auto_hard_fail(gov):
    """FIX #1: Full Auto MUST fail if API down"""
    # 🔴 FIXED: Removed 'L' suffix
    gov.get_available_funds = AsyncMock(return_value=1000000.0)
    
    with patch.object(gov, 'predict_margin_requirement', side_effect=Exception("API Fail")):
        with patch('app.config.settings.ENVIRONMENT', 'production_live'):
            res = await gov.can_trade_new([{"quantity": 50, "side": "SELL"}])
            assert res.allowed is False
            assert "CRITICAL" in res.reason

@pytest.mark.asyncio
async def test_shadow_heuristic_fallback(gov):
    """FIX #1: Shadow Mode uses estimation if API down"""
    # 🔴 FIXED: Removed 'L' suffix
    gov.get_available_funds = AsyncMock(return_value=1000000.0)
    
    with patch.object(gov, 'predict_margin_requirement', side_effect=Exception("API Fail")):
        with patch('app.config.settings.ENVIRONMENT', 'shadow'):
            res = await gov.can_trade_new([{"quantity": 50, "side": "SELL"}])
            assert res.allowed is True
            assert "HEURISTIC" in res.reason
