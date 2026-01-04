from dataclasses import dataclass
from typing import Optional

@dataclass
class MarginCheckResult:
    """
    Result of margin validation check.
    Used to pass decision data between CapitalGovernor and Supervisor.
    """
    allowed: bool
    reason: str
    required_margin: float = 0.0
    available_margin: float = 0.0
    brokerage_estimate: float = 0.0
    
    def __bool__(self):
        """Allow truthiness checks (e.g. 'if result:')"""
        return self.allowed

    def __repr__(self):
        return f"MarginCheckResult(allowed={self.allowed}, reason='{self.reason}')"
