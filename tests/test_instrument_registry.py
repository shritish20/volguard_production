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
from unittest.mock import Mock, patch
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
        },
        {
            "instrument_key": "NSE_INDEX:Nifty 50-21500-CE-2024-12-26",
            "trading_symbol": "NIFTY24DEC21500CE",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "CE",
            "lot_size": 50,
            "expiry": "2024-12-26T00:00:00",
            "strike_price": 21500.0
        },
        {
            "instrument_key": "NSE_INDEX:Nifty 50-21500-PE-2024-12-26",
            "trading_symbol": "NIFTY24DEC21500PE",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "PE",
            "lot_size": 50,
            "expiry": "2024-12-26T00:00:00",
            "strike_price": 21500.0
        },
        {
            "instrument_key": "NSE_INDEX:Nifty 50-FUT-2025-01-30",
            "trading_symbol": "NIFTY25JANFUT",
            "name": "Nifty 50",
            "exchange": "NSE",
            "instrument_type": "FUT",
            "lot_size": 50,
            "expiry": "2025-01-30T00:00:00",
            "strike_price": None
        }
    ]

@pytest.fixture
def temp_gzip_file(sample_instrument_data):
    """Create temporary gzipped instrument file"""
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.json.gz', delete=False) as f:
        with gzip.open(f, 'wt', encoding='utf-8') as gz:
            json.dump(sample_instrument_data, gz)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)

def test_registry_singleton():
    """Test InstrumentRegistry is a singleton"""
    registry1 = InstrumentRegistry()
    registry2 = InstrumentRegistry()
    
    assert registry1 is registry2
    assert id(registry1) == id(registry2)

def test_load_master_from_file(temp_gzip_file, sample_instrument_data):
    """Test loading instrument data from file"""
    registry = InstrumentRegistry()
    
    # Clear singleton instance for test
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    registry.load_master(temp_gzip_file)
    
    assert registry._InstrumentRegistry__data is not None
    assert len(registry._InstrumentRegistry__data) == len(sample_instrument_data)
    
    # Verify data types
    df = registry._InstrumentRegistry__data
    assert 'instrument_key' in df.columns
    assert 'trading_symbol' in df.columns
    assert 'expiry' in df.columns
    
    # Verify expiry parsing
    assert pd.api.types.is_datetime64_any_dtype(df['expiry'])

def test_load_master_missing_file():
    """Test loading when file doesn't exist"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    
    # Mock download to avoid actual download
    with patch.object(registry, 'download_master') as mock_download:
        # Create empty file path
        temp_dir = tempfile.mkdtemp()
        empty_file = os.path.join(temp_dir, "empty.json.gz")
        
        try:
            registry.load_master(empty_file)
            # Should call download_master
            mock_download.assert_called_once_with(empty_file)
        finally:
            import shutil
            shutil.rmtree(temp_dir)

def test_get_instrument_details(temp_gzip_file):
    """Test instrument details lookup"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    registry.load_master(temp_gzip_file)
    
    # Test existing instrument
    details = registry.get_instrument_details("NSE_INDEX:Nifty 50-21500-CE-2024-12-26")
    
    assert details["symbol"] == "NIFTY24DEC21500CE"
    assert details["strike"] == 21500.0
    assert details["lot_size"] == 50
    assert details["name"] == "Nifty 50"
    
    # Test non-existent instrument
    details = registry.get_instrument_details("NON_EXISTENT")
    assert details == {}

def test_get_current_future(temp_gzip_file):
    """Test current future lookup"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    registry.load_master(temp_gzip_file)
    
    # Mock current date to be before Dec 26
    with patch('app.services.instrument_registry.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2024, 12, 1)
        
        future_key = registry.get_current_future("NIFTY")
        
        # Should return nearest future (Dec 26)
        assert future_key == "NSE_INDEX:Nifty 50-FUT-2024-12-26"

def test_get_current_future_no_match(temp_gzip_file):
    """Test future lookup with no matching instruments"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    registry.load_master(temp_gzip_file)
    
    # Mock current date far in future
    with patch('app.services.instrument_registry.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2026, 1, 1)
        
        future_key = registry.get_current_future("NIFTY")
        
        # No futures after 2026-01-01
        assert future_key is None

def test_get_option_symbols(temp_gzip_file):
    """Test option symbol lookup"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
    registry = InstrumentRegistry()
    registry.load_master(temp_gzip_file)
    
    # Mock current date
    with patch('app.services.instrument_registry.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2024, 12, 1)
        
        option_symbols = registry.get_option_symbols("NIFTY")
        
        # Should return both CE and PE instruments
        assert len(option_symbols) == 2
        assert "NSE_INDEX:Nifty 50-21500-CE-2024-12-26" in option_symbols
        assert "NSE_INDEX:Nifty 50-21500-PE-2024-12-26" in option_symbols

def test_download_master():
    """Test master file download"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    
    registry = InstrumentRegistry()
    
    # Create temp file for download
    temp_dir = tempfile.mkdtemp()
    temp_file = os.path.join(temp_dir, "test.json.gz")
    
    try:
        # Mock HTTP response
        with patch('app.services.instrument_registry.httpx.Client') as mock_client:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.content = gzip.compress(
                json.dumps([{"test": "data"}]).encode('utf-8')
            )
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            
            registry.download_master(temp_file)
            
            # Verify file was created
            assert os.path.exists(temp_file)
            assert os.path.getsize(temp_file) > 0
            
    finally:
        import shutil
        shutil.rmtree(temp_dir)

def test_download_master_failure():
    """Test download failure handling"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    
    registry = InstrumentRegistry()
    
    # Mock HTTP failure
    with patch('app.services.instrument_registry.httpx.Client') as mock_client, \
         pytest.raises(Exception):
        
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Download failed")
        mock_client.return_value.__enter__.return_value.get.return_value = mock_response
        
        registry.download_master("/tmp/test.json.gz")

def test_registry_edge_cases():
    """Test edge cases in registry"""
    registry = InstrumentRegistry()
    
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    InstrumentRegistry._InstrumentRegistry__data = None
    
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
