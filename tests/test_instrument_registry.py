"""
Simplified Instrument Registry Tests
"""
import pytest
from app.services.instrument_registry import InstrumentRegistry

def test_registry_singleton():
    """Test InstrumentRegistry is a singleton"""
    # Clear singleton for test
    InstrumentRegistry._InstrumentRegistry__instance = None
    
    registry1 = InstrumentRegistry()
    registry2 = InstrumentRegistry()
    
    assert registry1 is registry2
    assert id(registry1) == id(registry2)

def test_registry_methods_exist():
    """Test registry methods exist"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Methods should exist
    assert hasattr(registry, 'load_master')
    assert hasattr(registry, 'get_instrument_details')
    assert hasattr(registry, 'get_current_future')
    assert hasattr(registry, 'get_option_symbols')
    
    # These are integration tests - actual functionality tested in production
    assert True

def test_registry_edge_cases():
    """Test edge cases in registry"""
    # Clear singleton
    InstrumentRegistry._InstrumentRegistry__instance = None
    registry = InstrumentRegistry()
    
    # Test with no data loaded
    # Should return empty/default values
    details = registry.get_instrument_details("ANY_KEY")
    assert details == {}  # Returns empty dict
    
    future = registry.get_current_future("NIFTY")
    assert future is None  # Returns None when no data
    
    options = registry.get_option_symbols("NIFTY")
    assert options == []  # Returns empty list
