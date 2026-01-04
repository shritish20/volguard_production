# app/core/trading/exit_engine.py

import logging
import asyncio
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)

class ExitEngine:
    """
    VolGuard Smart Exit Engine (VolGuard 3.0)
    
    Responsibilities:
    1. PROFIT TAKER: Exits winners early to free capital (Target: 70% of Max Profit).
    2. STOP LOSS: Hard exit if a single leg bleeds too much (Target: 200% of Credit).
    3. TIME EXIT: Auto-closes near expiry (0 DTE) to avoid gamma explosions.
    """

    def __init__(self):
        # Configurable Thresholds
        self.take_profit_pct = 0.70  # Capture 70% of max potential
        self.stop_loss_pct = 2.00    # Stop at 200% loss (3x price)
        self.time_exit_hour = 14     # 2:00 PM
        self.time_exit_minute = 30   # 2:30 PM

    async def evaluate_exits(
        self, 
        positions: List[Dict], 
        snapshot: Dict
    ) -> List[Dict]:
        """
        Scans all open positions for exit signals.
        Returns a list of EXIT orders.
        """
        exits = []
        
        for pos in positions:
            # Skip if already closing or invalid
            if pos.get("quantity", 0) == 0:
                continue

            reason = None
            
            # Data Normalization
            qty = abs(pos["quantity"])
            entry_price = float(pos.get("average_price", 0.0))
            current_price = float(pos.get("current_price", 0.0))
            pnl = float(pos.get("pnl", 0.0))
            side = pos.get("side") # BUY or SELL
            
            # 1. TIME EXIT (0 DTE Safety)
            # Avoid getting stuck in Gamma traps or delivery settlement
            if self._is_expiry_danger_zone(pos.get("expiry")):
                reason = "0 DTE Safety Exit"
            
            # 2. SELLER LOGIC (Short Options)
            elif side == "SELL":
                # Max Profit = Entry Price (since we sold it)
                # Current Profit = Entry - Current
                # Take Profit Check
                if entry_price > 0:
                    profit_pct = (entry_price - current_price) / entry_price
                    if profit_pct >= self.take_profit_pct:
                        reason = f"Take Profit ({profit_pct*100:.1f}%)"
                
                # Stop Loss Check
                # Loss = Current - Entry
                # If Current > 3 * Entry (200% loss)
                if current_price > (entry_price * (1 + self.stop_loss_pct)):
                    reason = f"Stop Loss (Price > {1+self.stop_loss_pct}x)"

            # 3. BUYER LOGIC (Long Options / Hedges)
            elif side == "BUY":
                # Hedges usually don't have Take Profits, they expire worthless or profit huge.
                # But we can exit if they lost 90% value (Dead Hedge) to clean up?
                # For VolGuard, we usually keep hedges until the core exits.
                # Implementation: Logic here depends on if it's a "Hedge" or "Speculation".
                pass

            # 4. GENERATE ORDER
            if reason:
                logger.info(f"Generating EXIT for {pos['symbol']}: {reason}")
                
                # Determine Exit Side
                exit_side = "BUY" if side == "SELL" else "SELL"
                
                exits.append({
                    "action": "EXIT",
                    "instrument_key": pos["instrument_key"],
                    "quantity": qty,
                    "side": exit_side,
                    "strategy": "EXIT_ENGINE",
                    "reason": reason,
                    "price": 0.0, # Market/Smart Limit
                    "is_hedge": False
                })

        return exits

    def _is_expiry_danger_zone(self, expiry_str: str) -> bool:
        """
        Checks if today is expiry day AND time is past cutoff.
        """
        if not expiry_str: return False
        
        try:
            today = datetime.now().date()
            exp_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            
            if exp_date == today:
                now = datetime.now().time()
                if now.hour > self.time_exit_hour or (now.hour == self.time_exit_hour and now.minute >= self.time_exit_minute):
                    return True
        except Exception:
            pass
            
        return False
