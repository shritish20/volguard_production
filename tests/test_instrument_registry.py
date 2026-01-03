"""
Instrument Registry Tests - Instrument lookup and data management
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import gzip
import json
import os
from unittest.mock import Mock, patch, MagicMock
from app.services.instrument_registry import InstrumentRegistry

# === INSTRUMENT REGISTRY TESTS ===
@pytest.fixture
def sample_instrument_data():
    """Create sample instrument data"""
    return [
        {
            "instrument_key": "NSE_INDEX:Nifty 50",
            "trading_symbol": "NIFTY",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "INDEX",
            "lot_size": 1,
            "expiry": None,
            "strike_price": None
        },
        {
            "instrument_key": "NSE_INDEX:Nifty 50-FUT-2024-12-26",
            "trading_symbol": "NIFTY24DECFUT",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "FUT",
            "lot_size": 50,
            "expiry": "2024-12-26T00:00:00",
            "strike_price": None
        }
    ]

def test_registry_singleton():
    """Test InstrumentRegistry is a singleton"""
    # Clear singleton for test
    InstrumentRegistry._InstrumentRegistry__instance = None
    
    registry1 = InstrumentRegistry()
    registry2 = InstrumentRegistry()
    
    assert registry1 is registry2
    assert id(registry1) == id(registry2)

def test_get_instrument_details():
    """Test instrument details lookup"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Create mock data
    mock_data = pd.DataFrame([{
        'instrument_key': 'TEST_KEY',
        'trading_symbol': 'TEST',
        'strike_price': 21500.0,
        'lot_size': 50,
        'expiry': '2024-12-26',
        'name': 'Nifty 50'
    }])
    
    # Set mock data
    registry._InstrumentRegistry__data = mock_data
    
    details = registry.get_instrument_details("TEST_KEY")
    
    assert details["symbol"] == "TEST"
    assert details["strike"] == 21500.0
    assert details["lot_size"] == 50
    assert details["name"] == "Nifty 50"

def test_get_current_future():
    """Test current future lookup"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Create mock data with futures
    mock_data = pd.DataFrame([
        {
            'instrument_key': 'FUT1',
            'name': 'Nifty 50',
            'exchange': 'NSE',
            'instrument_type': 'FUT',
            'expiry': pd.Timestamp('2024-12-26')
        },
        {
            'instrument_key': 'FUT2',
            'name': 'Nifty 50',
            'exchange': 'NSE',
            'instrument_type': 'FUT',
            'expiry': pd.Timestamp('2025-01-30')
        }
    ])
    
    registry._InstrumentRegistry__data = mock_data
    
    # Mock current date
    with patch('app.services.instrument_registry.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2024, 12, 1)
        
        future_key = registry.get_current_future("NIFTY")
        
        # Should return nearest future
        assert future_key == "FUT1"

def test_get_option_symbols():
    """Test option symbol lookup"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Create mock data with options
    mock_data = pd.DataFrame([
        {
            'instrument_key': 'OPT1',
            'name': 'Nifty 50',
            'exchange': 'NSE',
            'instrument_type': 'CE',
            'expiry': pd.Timestamp('2024-12-26')
        },
        {
            'instrument_key': 'OPT2',
            'name': 'Nifty 50',
            'exchange': 'NSE',
            'instrument_type': 'PE',
            'expiry': pd.Timestamp('2024-12-26')
        }
    ])
    
    registry._InstrumentRegistry__data = mock_data
    
    # Mock current date
    with patch('app.services.instrument_registry.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2024, 12, 1)
        
        option_symbols = registry.get_option_symbols("NIFTY")
        
        assert len(option_symbols) == 2
        assert 'OPT1' in option_symbols
        assert 'OPT2' in option_symbols

def test_registry_edge_cases():
    """Test edge cases in registry"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Test with empty DataFrame
    registry._InstrumentRegistry__data = pd.DataFrame()
    
    # Should return empty dict
    details = registry.get_instrument_details("ANY_KEY")
    assert details == {}
    
    # Should return None
    future = registry.get_current_future("NIFTY")
    assert future is None
    
    # Should return empty list
    options = registry.get_option_symbols("NIFTY")
    assert options == []
