"""
Capital-at-Risk Governor.
"""
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dataclass
class CapitalLimit:
    """Capital limit definition"""
    limit_type: str
    limit_value: float
    current_value: float = 0.0
    breach_threshold: float = 0.8
    last_reset: datetime = field(default_factory=datetime.utcnow)

@dataclass
class CapitalBreach:
    """Capital breach record"""
    timestamp: datetime
    limit_type: str
    current_value: float
    limit_value: float
    breach_percentage: float
    action_taken: str
    details: Dict

class CapitalGovernor:
    """
    Capital-at-risk governor - final gate before trading.
    """
    
    def __init__(self, total_capital: float):
        self.total_capital = total_capital
        
        # Define limits
        self.limits = {
            "DAILY_LOSS": CapitalLimit(
                limit_type="DAILY_LOSS",
                limit_value=total_capital * 0.02,  # 2%
                breach_threshold=0.8
            ),
            "POSITION_CAP": CapitalLimit(
                limit_type="POSITION_CAP",
                limit_value=total_capital * 0.30,  # 30%
                breach_threshold=0.9
            ),
            "WORST_CASE_LOSS": CapitalLimit(
                limit_type="WORST_CASE_LOSS",
                limit_value=total_capital * 0.05,  # 5%
                breach_threshold=0.7
            ),
            "CONCENTRATION_RISK": CapitalLimit(
                limit_type="CONCENTRATION_RISK",
                limit_value=total_capital * 0.10,  # 10%
                breach_threshold=0.8
            )
        }
        
        # State
        self.current_pnl = 0.0
        self.position_values: Dict[str, float] = {}
        self.strike_concentration: Dict[float, float] = {}
        self.breach_history: List[CapitalBreach] = []
    
    def update_portfolio_state(
        self,
        positions: Dict[str, Dict],
        stress_results: Dict,
        market_state: Dict
    ) -> Tuple[Dict, List[CapitalBreach]]:
        """
        Update capital calculations and check for breaches.
        """
        self.current_pnl = self._calculate_pnl(positions)
        self.position_values = self._calculate_position_values(positions, market_state)
        self.strike_concentration = self._calculate_strike_concentration(positions)
        
        breaches = []
        
        # Check all limits
        daily_breach = self._check_daily_loss()
        if daily_breach:
            breaches.append(daily_breach)
        
        position_breach = self._check_position_cap()
        if position_breach:
            breaches.append(position_breach)
        
        worst_case_breach = self._check_worst_case(stress_results)
        if worst_case_breach:
            breaches.append(worst_case_breach)
        
        concentration_breach = self._check_concentration()
        if concentration_breach:
            breaches.append(concentration_breach)
        
        # Build metrics
        metrics = {
            "total_capital": self.total_capital,
            "current_pnl": self.current_pnl,
            "pnl_percentage": (self.current_pnl / self.total_capital * 100) if self.total_capital > 0 else 0,
            "capital_utilized": sum(self.position_values.values()),
            "utilization_percentage": (sum(self.position_values.values()) / self.total_capital * 100) if self.total_capital > 0 else 0,
            "worst_case_percentage": stress_results.get("WORST_CASE", {}).get("impact", 0) / self.total_capital * 100,
            "limits": {k: v.__dict__ for k, v in self.limits.items()}
        }
        
        return metrics, breaches
    
    def _calculate_pnl(self, positions: Dict[str, Dict]) -> float:
        total = 0.0
        for position in positions.values():
            total += position.get("unrealized_pnl", 0)
        return total
    
    def _calculate_position_values(self, positions: Dict[str, Dict], market: Dict) -> Dict[str, float]:
        values = {}
        for pos_id, position in positions.items():
            quantity = abs(position.get("quantity", 0))
            price = position.get("current_price", 0) or market.get("spot", 0)
            values[pos_id] = quantity * price
        return values
    
    def _calculate_strike_concentration(self, positions: Dict[str, Dict]) -> Dict[float, float]:
        concentration = {}
        for position in positions.values():
            strike = position.get("strike", 0)
            quantity = abs(position.get("quantity", 0))
            price = position.get("current_price", 0)
            value = quantity * price
            
            if strike > 0:
                concentration[strike] = concentration.get(strike, 0) + value
        
        return concentration
    
    def _check_daily_loss(self) -> Optional[CapitalBreach]:
        limit = self.limits["DAILY_LOSS"]
        loss = abs(min(0, self.current_pnl))
        breach_pct = loss / limit.limit_value
        
        if breach_pct >= 1.0:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="DAILY_LOSS",
                current_value=loss,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="HALT_TRADING",
                details={"pnl": self.current_pnl, "level": "FULL"}
            )
        elif breach_pct >= limit.breach_threshold:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="DAILY_LOSS",
                current_value=loss,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="WARN_ONLY",
                details={"pnl": self.current_pnl, "level": "WARNING"}
            )
        
        return None
    
    def _check_position_cap(self) -> Optional[CapitalBreach]:
        limit = self.limits["POSITION_CAP"]
        total_value = sum(self.position_values.values())
        breach_pct = total_value / limit.limit_value
        
        if breach_pct >= 1.0:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="POSITION_CAP",
                current_value=total_value,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="PREVENT_NEW_TRADES",
                details={"position_count": len(self.position_values), "level": "FULL"}
            )
        elif breach_pct >= limit.breach_threshold:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="POSITION_CAP",
                current_value=total_value,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="WARN_ONLY",
                details={"position_count": len(self.position_values), "level": "WARNING"}
            )
        
        return None
    
    def _check_worst_case(self, stress_results: Dict) -> Optional[CapitalBreach]:
        limit = self.limits["WORST_CASE_LOSS"]
        worst_case = stress_results.get("WORST_CASE", {})
        impact = abs(worst_case.get("impact", 0))
        breach_pct = impact / limit.limit_value
        
        if breach_pct >= 1.0:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="WORST_CASE_LOSS",
                current_value=impact,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="REDUCE_EXPOSURE_IMMEDIATELY",
                details={
                    "scenario": worst_case.get("scenario", "unknown"),
                    "level": "FULL",
                    "details": worst_case
                }
            )
        elif breach_pct >= limit.breach_threshold:
            return CapitalBreach(
                timestamp=datetime.utcnow(),
                limit_type="WORST_CASE_LOSS",
                current_value=impact,
                limit_value=limit.limit_value,
                breach_percentage=breach_pct * 100,
                action_taken="WARN_AND_REDUCE",
                details={
                    "scenario": worst_case.get("scenario", "unknown"),
                    "level": "WARNING",
                    "details": worst_case
                }
            )
        
        return None
    
    def _check_concentration(self) -> Optional[CapitalBreach]:
        limit = self.limits["CONCENTRATION_RISK"]
        
        for strike, value in self.strike_concentration.items():
            breach_pct = value / limit.limit_value
            
            if breach_pct >= 1.0:
                return CapitalBreach(
                    timestamp=datetime.utcnow(),
                    limit_type="CONCENTRATION_RISK",
                    current_value=value,
                    limit_value=limit.limit_value,
                    breach_percentage=breach_pct * 100,
                    action_taken="REDUCE_CONCENTRATION",
                    details={
                        "strike": strike,
                        "value": value,
                        "level": "FULL"
                    }
                )
            elif breach_pct >= limit.breach_threshold:
                return CapitalBreach(
                    timestamp=datetime.utcnow(),
                    limit_type="CONCENTRATION_RISK",
                    current_value=value,
                    limit_value=limit.limit_value,
                    breach_percentage=breach_pct * 100,
                    action_taken="WARN_ONLY",
                    details={
                        "strike": strike,
                        "value": value,
                        "level": "WARNING"
                    }
                )
        
        return None
    
    def can_trade_new(self, trade_size: float, trade_details: Dict) -> Tuple[bool, str]:
        """
        Final gate: Can we take a new trade?
        """
        # Check position cap
        proposed_total = sum(self.position_values.values()) + trade_size
        if proposed_total > self.limits["POSITION_CAP"].limit_value:
            return False, f"Position cap would be breached: {proposed_total:.0f} > {self.limits['POSITION_CAP'].limit_value:.0f}"
        
        # Check concentration
        strike = trade_details.get("strike", 0)
        if strike > 0:
            current_at_strike = self.strike_concentration.get(strike, 0)
            proposed_at_strike = current_at_strike + trade_size
            if proposed_at_strike > self.limits["CONCENTRATION_RISK"].limit_value:
                return False, f"Concentration limit would be breached at strike {strike}"
        
        # Check daily loss warning
        if abs(self.current_pnl) > self.limits["DAILY_LOSS"].limit_value * 0.8:
            return False, f"Daily loss warning active: {self.current_pnl:.0f}"
        
        return True, "Capital limits allow trade"
    
    def get_capital_status(self) -> Dict:
        """Get current capital status"""
        return {
            "total_capital": self.total_capital,
            "current_pnl": self.current_pnl,
            "pnl_percentage": (self.current_pnl / self.total_capital * 100) if self.total_capital > 0 else 0,
            "capital_utilized": sum(self.position_values.values()),
            "utilization_percentage": (sum(self.position_values.values()) / self.total_capital * 100) if self.total_capital > 0 else 0,
            "strikes_with_concentration": len([v for v in self.strike_concentration.values() if v > self.total_capital * 0.05]),
            "recent_breaches": len([b for b in self.breach_history if (datetime.utcnow() - b.timestamp) < timedelta(hours=24)]),
            "limits": {
                k: {
                    "limit": v.limit_value,
                    "current_usage": self._get_current_usage(k),
                    "usage_percentage": (self._get_current_usage(k) / v.limit_value * 100) if v.limit_value > 0 else 0
                }
                for k, v in self.limits.items()
            }
        }
    
    def _get_current_usage(self, limit_type: str) -> float:
        if limit_type == "DAILY_LOSS":
            return abs(min(0, self.current_pnl))
        elif limit_type == "POSITION_CAP":
            return sum(self.position_values.values())
        elif limit_type == "CONCENTRATION_RISK":
            return max(self.strike_concentration.values()) if self.strike_concentration else 0
        else:
            return 0.0
