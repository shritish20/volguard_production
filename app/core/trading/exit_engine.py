import logging
from typing import List, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ExitEngine:
    """
    Seller-first exit engine.
    NO firefighting.
    NO last-day risk.
    """

    def __init__(
        self,
        profit_target_pct: float = 0.70,
        stop_loss_multiple: float = 2.0,
        hard_expiry_days: int = 1
    ):
        """
        :param profit_target_pct: % of premium captured (seller logic)
        :param stop_loss_multiple: multiple of credit where we exit (2x default)
        :param hard_expiry_days: force exit N days before expiry (T-1 default)
        """
        self.profit_target = profit_target_pct
        self.stop_loss_multiple = stop_loss_multiple
        self.hard_expiry_days = hard_expiry_days

    async def evaluate_exits(
        self,
        positions: List[Dict],
        snapshot: Dict
    ) -> List[Dict]:
        exits = []

        for pos in positions:
            # Skip invalid positions
            if not pos.get("quantity") or "average_price" not in pos:
                continue

            qty = abs(int(pos["quantity"]))
            if qty == 0:
                continue

            entry_price = float(pos["average_price"])
            current_price = float(pos.get("current_price", 0))
            lot_size = pos.get("lot_size", 1)
            side = pos.get("side")

            entry_credit = entry_price * lot_size

            # ------------------------------
            # PnL Calculation
            # ------------------------------
            if side == "SELL":
                pnl = (entry_price - current_price) * lot_size
            else:  # Hedge / Long
                pnl = (current_price - entry_price) * lot_size

            dte = self._days_to_expiry(pos.get("expiry"))

            # ======================================================
            # RULE 1 — HARD EXPIRY EXIT (T-1)
            # ======================================================
            if dte <= self.hard_expiry_days:
                exits.append(self._exit(
                    pos,
                    "HARD_EXPIRY_EXIT",
                    pnl
                ))
                continue

            # ======================================================
            # RULE 2 — PROFIT TARGET (SELLER)
            # ======================================================
            if side == "SELL" and pnl >= (entry_credit * self.profit_target):
                exits.append(self._exit(
                    pos,
                    "PROFIT_TARGET_REACHED",
                    pnl
                ))
                continue

            # ======================================================
            # RULE 3 — STOP LOSS (ABSOLUTE)
            # ======================================================
            if pnl <= -(entry_credit * self.stop_loss_multiple):
                exits.append(self._exit(
                    pos,
                    "STOP_LOSS_TRIGGERED",
                    pnl
                ))
                continue

        return exits

    # ======================================================
    # Helpers
    # ======================================================
    def _exit(self, pos: Dict, reason: str, pnl: float) -> Dict:
        logger.warning(
            f"EXIT {reason} | {pos.get('symbol')} | PnL/Lot: {pnl:.2f}"
        )

        return {
            "action": "CLOSE_POSITION",
            "instrument_key": pos["instrument_key"],
            "quantity": abs(pos["quantity"]),
            "side": "BUY" if pos["side"] == "SELL" else "SELL",
            "strategy": "EXIT",
            "reason": f"{reason} | PnL/Lot={pnl:.2f}"
        }

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

            return (expiry_dt.date() - datetime.now().date()).days

        except Exception:
            return 999
