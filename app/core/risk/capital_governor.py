# app/core/risk/capital_governor.py

from typing import Dict, Tuple
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class CapitalGovernor:
    """
    Final capital & margin authority.
    No strategy logic. No analytics. Hard risk gates only.
    """

    def __init__(self, total_capital: float, max_positions: int = None):
        self.total_capital = total_capital
        self.max_positions = max_positions or settings.MAX_POSITIONS

        self.current_margin = 0.0
        self.position_count = 0
        self.daily_pnl = 0.0

        # Risk parameters
        self.margin_buffer = settings.MARGIN_BUFFER
        self.max_daily_loss = settings.MAX_DAILY_LOSS

    # --------------------------------------------------
    # STATE UPDATE
    # --------------------------------------------------
    def update_state(self, margin_used: float, position_count: int, daily_pnl: float = 0.0):
        self.current_margin = max(margin_used, 0.0)
        self.position_count = max(position_count, 0)
        self.daily_pnl = daily_pnl

    # --------------------------------------------------
    # ENTRY PERMISSION
    # --------------------------------------------------
    def can_trade_new(self, estimated_margin: float, order_details: Dict) -> Tuple[bool, str]:

        action = order_details.get("action")
        strategy = order_details.get("strategy", "")

        # --------------------------------------------------
        # ALWAYS ALLOW SAFETY ACTIONS
        # --------------------------------------------------
        if action in ["EXIT", "CLOSE"] or strategy in ["HEDGE", "KILL_SWITCH"]:
            return True, "Safety Action Allowed"

        # --------------------------------------------------
        # SANITY CHECK
        # --------------------------------------------------
        if estimated_margin <= 0:
            logger.error("Estimated margin invalid")
            return False, "Invalid Margin Estimate"

        # --------------------------------------------------
        # DAILY LOSS STOP
        # --------------------------------------------------
        if abs(self.daily_pnl) >= self.max_daily_loss:
            return False, "Max Daily Loss Reached"

        # --------------------------------------------------
        # POSITION COUNT LIMIT
        # --------------------------------------------------
        if self.position_count >= self.max_positions:
            return False, "Max Positions Reached"

        # --------------------------------------------------
        # MARGIN AVAILABILITY WITH BUFFER
        # --------------------------------------------------
        effective_capital = self.total_capital * (1.0 - self.margin_buffer)
        available_margin = effective_capital - self.current_margin

        if available_margin < estimated_margin:
            return False, "Insufficient Capital (Buffer Protected)"

        return True, "Capital OK"
