# app/core/trading/exit_engine.py

import logging
from typing import List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class ExitEngine:
    """
    Handles all exit decisions.
    EXIT logic has higher priority than ENTRY in the Supervisor.
    """

    def __init__(
        self,
        profit_target_pct: float = 0.70,
        stop_loss_pct: float = 2.0,
        min_dte: int = 7,
    ):
        self.profit_target = profit_target_pct
        self.stop_loss = stop_loss_pct
        self.min_dte = min_dte

    # --------------------------------------------------
    # EXIT EVALUATION
    # --------------------------------------------------
    async def evaluate_exits(
        self,
        positions: List[Dict],
        snapshot: Dict,
    ) -> List[Dict]:

        exits = []

        for pos in positions:
            # Skip non-option instruments
            if "average_price" not in pos or "FUT" in pos.get("symbol", ""):
                continue

            qty = abs(pos.get("quantity", 0))
            if qty <= 0:
                continue

            entry_price = float(pos["average_price"])
            current_price = float(pos["current_price"])
            side = pos["side"]

            # --------------------------------------------------
            # UNIT-CONSISTENT PnL (per position)
            # --------------------------------------------------
            if side == "SELL":
                pnl = (entry_price - current_price) * qty
                max_profit = entry_price * qty
            else:  # BUY (HEDGE)
                pnl = (current_price - entry_price) * qty
                max_profit = None

            # --------------------------------------------------
            # 1️⃣ PROFIT TARGET (SELLERS ONLY)
            # --------------------------------------------------
            if side == "SELL" and pnl >= max_profit * self.profit_target:
                exits.append(self._create_exit(pos, "PROFIT_TARGET", pnl))
                continue

            # --------------------------------------------------
            # 2️⃣ STOP LOSS (ALL POSITIONS)
            # --------------------------------------------------
            if side == "SELL" and pnl <= -(max_profit * self.stop_loss):
                exits.append(self._create_exit(pos, "STOP_LOSS", pnl))
                continue

            # --------------------------------------------------
            # 3️⃣ GAMMA / EXPIRY RISK
            # --------------------------------------------------
            if self._days_to_expiry(pos.get("expiry")) <= self.min_dte:
                exits.append(self._create_exit(pos, "EXPIRY_RISK", pnl))
                continue

            # --------------------------------------------------
            # 4️⃣ EMERGENCY VOL SPIKE EXIT (OPTIONAL HOOK)
            # --------------------------------------------------
            if snapshot.get("vix", 0) > 30 and side == "SELL":
                exits.append(self._create_exit(pos, "VOLATILITY_SPIKE", pnl))

        return exits

    # --------------------------------------------------
    # EXIT ORDER CREATION
    # --------------------------------------------------
    def _create_exit(self, pos: Dict, reason: str, pnl: float) -> Dict:
        return {
            "action": "EXIT",
            "instrument_key": pos["instrument_key"],
            "quantity": abs(pos["quantity"]),
            "side": "BUY" if pos["side"] == "SELL" else "SELL",
            "strategy": "EXIT",
            "reason": f"{reason} | PnL={pnl:.2f}",
        }

    # --------------------------------------------------
    # DTE CALCULATION
    # --------------------------------------------------
    def _days_to_expiry(self, expiry) -> int:
        if not expiry:
            return 999

        try:
            if isinstance(expiry, str):
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"):
                    try:
                        expiry_dt = datetime.strptime(expiry, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return 999
            else:
                expiry_dt = expiry

            return max((expiry_dt - datetime.now()).days, 0)
        except Exception:
            return 999
