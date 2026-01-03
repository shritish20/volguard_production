import logging
from typing import List, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ExitEngine:
    """
    Exit-only engine.
    No firefighting. No rolling. Clean exits only.
    """

    def __init__(
        self,
        profit_target_pct: float = 0.70,   # 70% premium capture
        stop_loss_pct: float = 2.0,        # 200% of collected premium
        min_dte: int = 7,                  # Existing gamma safety
        force_exit_dte: int = 1            # 🔒 NEW: Mandatory T-1 square-off
    ):
        self.profit_target = profit_target_pct
        self.stop_loss = stop_loss_pct
        self.min_dte = min_dte
        self.force_exit_dte = force_exit_dte

    async def evaluate_exits(
        self,
        positions: List[Dict],
        snapshot: Dict
    ) -> List[Dict]:

        exits = []

        for pos in positions:
            # Skip non-option instruments
            if 'expiry' not in pos or 'average_price' not in pos:
                continue

            qty = abs(pos.get("quantity", 0))
            if qty == 0:
                continue

            entry_price = float(pos["average_price"])
            current_price = float(pos.get("current_price", entry_price))
            lot_size = pos.get("lot_size", 1)

            entry_premium = entry_price * lot_size

            # PnL per lot
            if pos["side"] == "SELL":
                pnl = (entry_price - current_price) * lot_size
            else:
                pnl = (current_price - entry_price) * lot_size

            dte = self._days_to_expiry(pos.get("expiry"))

            # --------------------------------------------------
            # RULE 1: FORCE EXIT T-1 (ABSOLUTE)
            # --------------------------------------------------
            if dte <= self.force_exit_dte:
                exits.append(self._create_exit(
                    pos,
                    "FORCE_EXIT_T-1",
                    pnl
                ))
                continue

            # --------------------------------------------------
            # RULE 2: PROFIT TARGET
            # --------------------------------------------------
            if pos["side"] == "SELL" and pnl >= (entry_premium * self.profit_target):
                exits.append(self._create_exit(
                    pos,
                    "PROFIT_TARGET",
                    pnl
                ))
                continue

            # --------------------------------------------------
            # RULE 3: STOP LOSS
            # --------------------------------------------------
            if pnl <= -(entry_premium * self.stop_loss):
                exits.append(self._create_exit(
                    pos,
                    "STOP_LOSS",
                    pnl
                ))
                continue

            # --------------------------------------------------
            # RULE 4: GAMMA RISK (EARLY EXIT)
            # --------------------------------------------------
            if dte <= self.min_dte:
                exits.append(self._create_exit(
                    pos,
                    "GAMMA_RISK_EXIT",
                    pnl
                ))
                continue

        return exits

    def _create_exit(self, pos: Dict, reason: str, pnl: float) -> Dict:
        return {
            "action": "CLOSE_POSITION",
            "instrument_key": pos["instrument_key"],
            "quantity": abs(pos["quantity"]),
            "side": "BUY" if pos["side"] == "SELL" else "SELL",
            "strategy": "EXIT",
            "reason": f"{reason} | PnL/lot={pnl:.2f}"
        }

    def _days_to_expiry(self, expiry) -> int:
        try:
            if isinstance(expiry, str):
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"):
                    try:
                        expiry = datetime.strptime(expiry, fmt)
                        break
                    except ValueError:
                        continue

            if isinstance(expiry, datetime):
                return (expiry.date() - datetime.now().date()).days
        except Exception:
            pass

        return 999
