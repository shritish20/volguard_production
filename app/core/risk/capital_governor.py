from typing import Dict, Tuple, List
import logging

class CapitalGovernor:
    def __init__(self, total_capital: float, max_positions: int = 10):
        self.total_capital = total_capital
        self.max_positions = max_positions
        self.current_margin = 0.0
        self.position_count = 0

    def update_state(self, margin, count):
        self.current_margin = margin
        self.position_count = count

    def can_trade_new(self, estimated_margin: float, order_details: Dict) -> Tuple[bool, str]:
        # FIX: Allow if it's a Hedge/Close
        if order_details.get("strategy") in ["HEDGE", "CLOSE", "KILL_SWITCH"]:
            return True, "Hedge Allowed"

        if self.position_count >= self.max_positions:
            return False, "Max Positions Reached"

        if (self.total_capital - self.current_margin) < estimated_margin:
            return False, "Insufficient Capital"
            
        return True, "OK"
