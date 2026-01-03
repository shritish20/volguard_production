"""
Trading Engine Tests - Order generation and execution logic
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from app.core.trading.engine import TradingEngine
from app.core.trading.executor import TradeExecutor
from app.services.instrument_registry import registry as instrument_registry

# === TRADING ENGINE TESTS ===
@pytest.fixture
def mock_market_client():
    """Mock MarketDataClient"""
    client = AsyncMock()
    client.get_option_chain.return_value = pd.DataFrame({
        'strike': [21000, 21100, 21200, 21300, 21400, 21500],
        'ce_key': [f"NSE_INDEX:Nifty 50-{s}-CE" for s in [21000, 21100, 21200, 21300, 21400, 21500]],
        'pe_key': [f"NSE_INDEX:Nifty 50-{s}-PE" for s in [21000, 21100, 21200, 21300, 21400, 21500]],
        'ce_delta': [0.9, 0.8, 0.7, 0.6, 0.5, 0.4],
        'pe_delta': [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6],
        'ce_iv': [0.15, 0.16, 0.17, 0.18, 0.19, 0.20],
        'pe_iv': [0.20, 0.19, 0.18, 0.17, 0.16, 0.15],
        'ce_oi': [1000, 1500, 2000, 2500, 3000, 3500],
        'pe_oi': [3500, 3000, 2500, 2000, 1500, 1000],
        'ce_gamma': [0.01] * 6,
        'pe_gamma': [0.01] * 6
    })
    return client

@pytest.mark.asyncio
async def test_generate_entry_orders_aggressive_short(mock_market_client):
    """Test order generation for AGGRESSIVE_SHORT regime"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=mock_market_client, config=config)
    
    # Mock registry to return expiry
    with patch.object(instrument_registry, '_InstrumentRegistry__data', pd.DataFrame()), \
         patch.object(instrument_registry, 'load_master'):
        
        # Mock get_nearest_weekly_expiry
        with patch.object(engine, '_TradingEngine__get_nearest_weekly_expiry') as mock_expiry:
            mock_expiry.return_value = "2024-12-26"
            
            regime = {"name": "AGGRESSIVE_SHORT"}
            market_snapshot = {"spot": 21500.50}
            
            orders = await engine.generate_entry_orders(regime, market_snapshot)
            
            # Should generate 2 orders (strangle: sell call + sell put)
            assert len(orders) == 2
            
            # Verify order structure
            for order in orders:
                assert "instrument_key" in order
                assert "quantity" in order
                assert "side" in order
                assert order["side"] == "SELL"
                assert order["strategy"] == "STRANGLE"

@pytest.mark.asyncio
async def test_generate_entry_orders_neutral_regime(mock_market_client):
    """Test no orders for NEUTRAL regime"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=mock_market_client, config=config)
    
    regime = {"name": "NEUTRAL"}
    market_snapshot = {"spot": 21500.50}
    
    orders = await engine.generate_entry_orders(regime, market_snapshot)
    assert len(orders) == 0

@pytest.mark.asyncio
async def test_generate_entry_orders_empty_chain(mock_market_client):
    """Test handling of empty option chain"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=mock_market_client, config=config)
    
    # Mock empty chain
    mock_market_client.get_option_chain.return_value = pd.DataFrame()
    
    with patch.object(instrument_registry, '_InstrumentRegistry__data', pd.DataFrame()), \
         patch.object(instrument_registry, 'load_master'), \
         patch.object(engine, '_TradingEngine__get_nearest_weekly_expiry', return_value="2024-12-26"):
        
        regime = {"name": "AGGRESSIVE_SHORT"}
        market_snapshot = {"spot": 21500.50}
        
        orders = await engine.generate_entry_orders(regime, market_snapshot)
        assert len(orders) == 0

def test_find_strike_by_delta():
    """Test strike selection by delta"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=None, config=config)
    
    chain_data = pd.DataFrame({
        'strike': [21000, 21100, 21200, 21300, 21400, 21500],
        'ce_key': ["CE1", "CE2", "CE3", "CE4", "CE5", "CE6"],
        'pe_key': ["PE1", "PE2", "PE3", "PE4", "PE5", "PE6"],
        'ce_delta': [0.9, 0.8, 0.7, 0.6, 0.5, 0.4],
        'pe_delta': [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6]
    })
    
    # Test call strike selection (target delta 0.25)
    # Closest is 0.4 (strike 21500)
    call_result = engine._TradingEngine__find_strike_by_delta(chain_data, 0.25, "CE")
    assert call_result is not None
    assert call_result["strike"] == 21500
    assert call_result["delta"] == 0.4
    
    # Test put strike selection (target delta 0.25 absolute)
    # Closest is 0.3 (strike 21200, delta -0.3)
    put_result = engine._TradingEngine__find_strike_by_delta(chain_data, 0.25, "PE")
    assert put_result is not None
    assert put_result["strike"] == 21200
    assert put_result["delta"] == -0.3

def test_create_order_packet():
    """Test order packet creation"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=None, config=config)
    
    # Mock registry
    with patch('app.core.trading.engine.registry') as mock_reg:
        mock_reg.get_instrument_details.return_value = {"lot_size": 50}
        
        leg_data = {
            "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
            "strike": 21500.0,
            "delta": 0.25
        }
        
        order = engine._TradingEngine__create_order_packet(
            leg_data=leg_data,
            side="SELL",
            strategy_tag="STRANGLE"
        )
        
        assert order["instrument_key"] == "NSE_INDEX:Nifty 50-21500-CE"
        assert order["quantity"] == 50
        assert order["side"] == "SELL"
        assert order["strategy"] == "STRANGLE"
        assert order["strike"] == 21500.0
        assert "reason" in order

def test_create_order_packet_fallback_lot():
    """Test order packet with fallback lot size"""
    config = {"BASE_CAPITAL": 1000000}
    engine = TradingEngine(market_client=None, config=config)
    
    # Mock registry returning no lot size
    with patch('app.core.trading.engine.registry') as mock_reg:
        mock_reg.get_instrument_details.return_value = {"lot_size": 0}
        
        leg_data = {
            "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
            "strike": 21500.0,
            "delta": 0.25
        }
        
        order = engine._TradingEngine__create_order_packet(
            leg_data=leg_data,
            side="BUY",
            strategy_tag="HEDGE"
        )
        
        # Should use fallback lot size (25)
        assert order["quantity"] == 25

# === TRADE EXECUTOR TESTS ===
@pytest.fixture
def mock_trade_executor():
    """Mock TradeExecutor"""
    with patch('app.core.trading.executor.upstox_client') as mock_upstox:
        executor = TradeExecutor(access_token="test_token")
        
        # Mock API responses
        mock_position = Mock()
        mock_position.instrument_token = "NSE_INDEX:Nifty 50-21500-CE"
        mock_position.trading_symbol = "NIFTY23DEC21500CE"
        mock_position.quantity = "-50"
        mock_position.buy_price = "0.0"
        mock_position.sell_price = "120.50"
        mock_position.last_price = "115.25"
        mock_position.pnl = "262.50"
        
        executor.portfolio_api.get_positions.return_value = Mock(data=[mock_position])
        
        mock_quote = Mock()
        mock_quote.data = {"NSE_INDEX:Nifty 50-21500-CE": Mock(last_price="115.25")}
        executor.quote_api.ltp.return_value = mock_quote
        
        return executor

@pytest.mark.asyncio
async def test_get_positions(mock_trade_executor):
    """Test position fetching"""
    # Mock registry
    with patch('app.core.trading.executor.registry') as mock_reg:
        mock_reg.get_instrument_details.return_value = {
            "strike": 21500.0,
            "expiry": "2023-12-28",
            "lot_size": 50
        }
        
        positions = await mock_trade_executor.get_positions()
        
        assert len(positions) == 1
        position = positions[0]
        
        assert position["instrument_key"] == "NSE_INDEX:Nifty 50-21500-CE"
        assert position["symbol"] == "NIFTY23DEC21500CE"
        assert position["quantity"] == -50
        assert position["side"] == "SELL"
        assert position["average_price"] == 120.50
        assert position["current_price"] == 115.25
        assert position["pnl"] == 262.50
        assert position["strike"] == 21500.0
        assert position["lot_size"] == 50
        assert position["option_type"] == "CE"

@pytest.mark.asyncio
async def test_get_positions_empty():
    """Test empty position response"""
    executor = TradeExecutor(access_token="test_token")
    
    with patch.object(executor.portfolio_api, 'get_positions') as mock_get:
        mock_get.return_value = Mock(data=None)
        
        positions = await executor.get_positions()
        assert positions == []

@pytest.mark.asyncio
async def test_execute_adjustment_future_hedge():
    """Test future hedge execution"""
    executor = TradeExecutor(access_token="test_token")
    
    adjustment = {
        "instrument_key": "NIFTY_FUT_CURRENT",
        "quantity": 50,
        "side": "BUY",
        "strategy": "HEDGE"
    }
    
    # Mock registry to return future key
    with patch('app.core.trading.executor.registry') as mock_reg, \
         patch.object(executor.quote_api, 'ltp') as mock_ltp, \
         patch.object(executor.order_api, 'place_order') as mock_order:
        
        mock_reg.get_current_future.return_value = "NSE_INDEX:Nifty 50-FUT"
        mock_ltp.return_value = Mock(data={"NSE_INDEX:Nifty 50-FUT": Mock(last_price=21500.0)})
        
        mock_resp = Mock()
        mock_resp.data = Mock(order_id="TEST_ORDER_123")
        mock_order.return_value = mock_resp
        
        result = await executor.execute_adjustment(adjustment)
        
        assert result["status"] == "SUCCESS"
        assert result["order_id"] == "TEST_ORDER_123"
        
        # Verify order was placed with correct parameters
        mock_order.assert_called_once()
        call_args = mock_order.call_args[0][0]
        assert call_args.quantity == 50
        assert call_args.transaction_type == "BUY"
        assert call_args.instrument_token == "NSE_INDEX:Nifty 50-FUT"

@pytest.mark.asyncio
async def test_execute_adjustment_market_order_fallback():
    """Test market order fallback when LTP fails"""
    executor = TradeExecutor(access_token="test_token")
    
    adjustment = {
        "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
        "quantity": 50,
        "side": "SELL",
        "strategy": "CLOSE"
    }
    
    with patch.object(executor.quote_api, 'ltp', side_effect=Exception("API Error")), \
         patch.object(executor.order_api, 'place_order') as mock_order:
        
        mock_resp = Mock()
        mock_resp.data = Mock(order_id="MARKET_ORDER_456")
        mock_order.return_value = mock_resp
        
        result = await executor.execute_adjustment(adjustment)
        
        assert result["status"] == "SUCCESS"
        assert result["order_id"] == "MARKET_ORDER_456"
        
        # Should fall back to MARKET order
        call_args = mock_order.call_args[0][0]
        assert call_args.order_type == "MARKET"

@pytest.mark.asyncio
async def test_close_all_positions():
    """Test emergency position closure"""
    executor = TradeExecutor(access_token="test_token")
    
    # Mock get_positions to return a position
    with patch.object(executor, 'get_positions') as mock_get_pos, \
         patch.object(executor, 'execute_adjustment') as mock_exec:
        
        mock_get_pos.return_value = [{
            "instrument_key": "NSE_INDEX:Nifty 50-21500-CE",
            "quantity": -50,
            "side": "SELL"
        }]
        
        await executor.close_all_positions("KILL_SWITCH")
        
        # Should call execute_adjustment with inverse side
        mock_exec.assert_called_once()
        call_args = mock_exec.call_args[0][0]
        
        assert call_args["instrument_key"] == "NSE_INDEX:Nifty 50-21500-CE"
        assert call_args["quantity"] == 50  # Absolute value
        assert call_args["side"] == "BUY"  # Inverse of SELL
        assert call_args["strategy"] == "KILL_SWITCH"
