import pytest
import pandas as pd
from app.core.data.quality_gate import DataQualityGate

def test_atm_iv_validation():
    gate = DataQualityGate()
    
    # Data where OTM has 0 IV (Normal) but ATM has Valid IV
    df = pd.DataFrame({
        'strike': [20000, 21500, 23000],
        'spot': [21500, 21500, 21500],
        'ce_iv': [0.0, 15.0, 0.0],
        'pe_iv': [0.0, 15.0, 0.0]
    })
    
    valid, reason = gate.validate_structure(df)
    assert valid is True # Should pass because ATM is valid

    # Data where ATM has 0 IV (Bad)
    df.loc[1, 'ce_iv'] = 0.0
    valid, reason = gate.validate_structure(df)
    assert valid is False
    assert "Critical" in reason
