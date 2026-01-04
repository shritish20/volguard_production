import pytest
import pandas as pd
from app.core.trading.engine import TradingEngine
from app.core.trading.executor import TradeExecutor

def test_trading_engine_basics():
    """Test trading engine basic functionality"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=None, config=config)
    assert engine is not None
    # FIX: Check the config dictionary, not a class attribute
    assert engine.config.get("BASE_CAPITAL") == 1000000
    assert hasattr(engine, 'generate_entry_orders')

def test_strike_selection_logic():
    """Test the strike selection logic (simplified)"""
    chain_data = pd.DataFrame({
        'strike': [21000, 21100, 21200, 21300, 21400, 21500],
        'ce_delta': [0.9, 0.8, 0.7, 0.6, 0.5, 0.4],
        'pe_delta': [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6]
    })
    target_delta = 0.25
    chain_data['call_diff'] = abs(chain_data['ce_delta'] - target_delta)
    call_strike = chain_data.loc[chain_data['call_diff'].idxmin(), 'strike']
    assert call_strike == 21500 

    chain_data['put_diff'] = abs(chain_data['pe_delta'].abs() - target_delta)
    put_strike = chain_data.loc[chain_data['put_diff'].idxmin(), 'strike']
    assert put_strike in [21100, 21200]

@pytest.mark.asyncio
async def test_trade_executor_basics():
    """Test trade executor basic functionality"""
    executor = TradeExecutor(access_token="test_token")
    assert executor is not None
    assert hasattr(executor, 'get_positions')
    assert hasattr(executor, 'execute_adjustment')
    assert hasattr(executor, 'close_all_positions')

def test_close_position_logic():
    """Test position closing logic"""
    position = {"quantity": -50, "side": "SELL"}
    if position["side"] == "SELL":
        close_side = "BUY"
    else:
        close_side = "SELL"
    
    close_quantity = abs(position["quantity"])
    assert close_side == "BUY"
    assert close_quantity == 50
