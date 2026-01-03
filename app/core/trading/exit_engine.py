import logging
from typing import List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

class ExitEngine:
    def __init__(self, profit_target_pct=0.70, stop_loss_pct=2.0, min_dte=7):
        """
        :param profit_target_pct: Close if 70% of premium is captured.
        :param stop_loss_pct: Close if loss is 200% of initial premium.
        :param min_dte: Close if Days to Expiry is <= 7 (Avoid Gamma risk).
        """
        self.profit_target = profit_target_pct
        self.stop_loss = stop_loss_pct
        self.min_dte = min_dte
    
    async def evaluate_exits(self, positions: List[Dict], snapshot: Dict) -> List[Dict]:
        exits = []
        for pos in positions:
            # Skip Futures or positions without entry price data
            if 'average_price' not in pos or 'FUT' in pos.get('symbol', ''):
                continue
            
            # Calculate PnL per lot logic
            qty = abs(pos['quantity'])
            if qty == 0: continue

            # For Option Sellers:
            # Entry Premium (Credit) = Avg Price * Lot Size
            # Current Cost (Debit) = Current Price * Lot Size
            # PnL = Entry - Current
            
            entry_price = float(pos['average_price'])
            current_price = float(pos['current_price'])
            
            entry_premium = entry_price * pos['lot_size']
            
            # Seller Logic (Short)
            if pos['side'] == 'SELL':
                pnl_per_lot = (entry_price - current_price) * pos['lot_size']
            # Buyer Logic (Long - Hedges)
            else:
                pnl_per_lot = (current_price - entry_price) * pos['lot_size']
            
            # 1. Profit Target (Take 70% of max profit)
            # Only for sellers usually, but logic holds
            if pos['side'] == 'SELL' and pnl_per_lot >= (entry_premium * self.profit_target):
                exits.append(self._create_exit(pos, "PROFIT_TARGET", pnl_per_lot))
            
            # 2. Stop Loss (Loss > 2x Premium)
            # e.g. Collected 100, Price is now 300. Loss is 200.
            elif pnl_per_lot <= -(entry_premium * self.stop_loss):
                exits.append(self._create_exit(pos, "STOP_LOSS", pnl_per_lot))
            
            # 3. Time Decay / Expiry Risk
            elif self._days_to_expiry(pos.get('expiry')) <= self.min_dte:
                exits.append(self._create_exit(pos, "EXPIRY_RISK", pnl_per_lot))
        
        return exits
    
    def _create_exit(self, pos, reason, pnl):
        return {
            "action": "CLOSE_POSITION",
            "instrument_key": pos['instrument_key'],
            "quantity": abs(pos['quantity']),
            "side": "BUY" if pos['side'] == "SELL" else "SELL",
            "strategy": "EXIT",
            "reason": f"{reason}: PnL per lot={pnl:.2f}"
        }
    
    def _days_to_expiry(self, expiry):
        if not expiry:
            return 999
        try:
            if isinstance(expiry, str):
                # Handle Upstox format
                for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"]:
                    try:
                        expiry_dt = datetime.strptime(expiry, fmt)
                        break
                    except: continue
                else:
                    return 999
            else:
                expiry_dt = expiry
                
            return (expiry_dt - datetime.now()).days
        except:
            return 999
