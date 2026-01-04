import pytest
from app.services.instrument_registry import registry, InstrumentRegistry

def test_registry_instance():
    """Test the global registry instance"""
    assert isinstance(registry, InstrumentRegistry)

def test_registry_methods_exist():
    """Test registry methods exist on the global instance"""
    assert hasattr(registry, 'load_master')
    assert hasattr(registry, 'get_instrument_details')
    assert hasattr(registry, 'get_current_future')
    # get_option_symbols removed as it does not exist in source

def test_registry_edge_cases():
    """Test edge cases in registry"""
    # Test with no data loaded (or mocking empty state)
    details = registry.get_instrument_details("NON_EXISTENT_KEY")
    assert details == {}
    
    future = registry.get_current_future("INVALID_UNDERLYING")
    assert future is None
