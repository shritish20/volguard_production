# app/core/trading/executor.py

import logging
import httpx
import asyncio
import uuid
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import redis.asyncio as redis
from sqlalchemy.future import select

from tenacity import retry, stop_after_attempt, wait_fixed
from app.config import settings
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry

logger = logging.getLogger(__name__)

class TradeExecutor:
    """
    VolGuard Smart Trade Executor (VolGuard 3.0) - PRODUCTION HARDENED

    Architecture:
    - V3: Order Placement (with Freeze Slicing), Modifications, Cancellations
    - V2: Position Reporting (Short Term / F&O)
    - Redis: Distributed State & Idempotency (Persists across restarts)
    - Postgres: Permanent Audit Trail
    """

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_v3 = settings.UPSTOX_BASE_V3
        self.base_v2 = settings.UPSTOX_BASE_V2
        
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Async Client
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

        # Redis Connection for Idempotency & Locking
        # We use decode_responses=True to handle strings directly
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.IDEMPOTENCY_TTL = 86400  # 24 Hours

    async def close(self):
        """Cleanup resources."""
        await self.client.aclose()
        await self.redis.close()

    # ==================================================================
    # 0. BOOT RECONCILIATION (The "Anti-Amnesia" Protocol)
    # ==================================================================
    async def reconcile_state(self):
        """
        CRITICAL: Syncs Broker State (Truth) with Database (Record).
        Run this on Supervisor startup to fix 'Ghost' or 'Orphan' trades caused by crashes.
        """
        logger.info("⚔️ Starting State Reconciliation...")

        try:
            # 1. Fetch Truth (Upstox)
            broker_positions = await self.get_positions()
            broker_map = {p['instrument_key']: p for p in broker_positions}

            async with AsyncSessionLocal() as session:
                # 2. Fetch Records (DB - Only OPEN trades)
                result = await session.execute(
                    select(TradeRecord).where(TradeRecord.status == "OPEN")
                )
                db_trades = result.scalars().all()
                db_map = {t.instrument_key: t for t in db_trades}

                # Case A: Ghost Trades (DB says OPEN, Broker says CLOSED)
                # Action: Mark as CLOSED in DB.
                for key, trade in db_map.items():
                    if key not in broker_map:
                        logger.warning(f"👻 Ghost Trade Found: {key}. Marking CLOSED.")
                        trade.status = "CLOSED"
                        trade.closed_at = datetime.utcnow()
                        trade.reason = "RECONCILIATION_AUTO_CLOSE"
                
                # Case B: Orphan Trades (Broker says OPEN, DB has no record)
                # Action: Insert "GHOST_RECOVERY" record so AdjustmentEngine sees it.
                for key, pos in broker_map.items():
                    if key not in db_map:
                        logger.warning(f"🧟 Orphan Trade Found: {key} (Qty: {pos['quantity']}). Injecting DB Record.")
                        
                        # Create synthetic record
                        orphan_trade = TradeRecord(
                            id=str(uuid.uuid4()),
                            trade_tag=f"RECOVERY_{int(time.time())}",
                            instrument_key=key,
                            symbol=pos['symbol'],
                            quantity=pos['quantity'],
                            side=pos['side'],
                            entry_price=pos['average_price'],
                            strategy="MANUAL_RECOVERY", # Assume manual if unknown
                            status="OPEN",
                            timestamp=datetime.utcnow(),
                            reason="RECONCILIATION_INJECTION"
                        )
                        session.add(orphan_trade)

                await session.commit()
                logger.info("✅ State Reconciliation Complete.")

        except Exception as e:
            logger.critical(f"❌ Reconciliation Failed: {e}")
            # We do NOT stop here, but alerting is mandatory in a real system
            raise e

    # ==================================================================
    # 1. POSITIONS (V2 - SOURCE OF TRUTH)
    # ==================================================================
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_positions(self) -> List[Dict]:
        """
        Fetches current F&O positions.
        Endpoint: /v2/portfolio/short-term-positions
        """
        url = f"{self.base_v2}/portfolio/short-term-positions"
        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
            data = resp.json().get("data", [])

            positions = []
            for p in data:
                # Filter out closed positions (quantity 0)
                net_qty = int(p.get("net_quantity", 0))
                if net_qty == 0:
                    continue

                # Normalize Data
                instrument_token = p.get("instrument_token")
                details = registry.get_instrument_details(instrument_token)
                
                side = "BUY" if net_qty > 0 else "SELL"
                
                # Safe price extraction
                buy_price = float(p.get("buy_price", 0) or 0)
                sell_price = float(p.get("sell_price", 0) or 0)
                avg_price = buy_price if net_qty > 0 else sell_price

                positions.append({
                    "position_id": instrument_token,
                    "instrument_key": instrument_token,
                    "symbol": p.get("trading_symbol"),
                    "quantity": abs(net_qty),
                    "side": side,
                    "average_price": avg_price,
                    "current_price": float(p.get("last_price", 0.0)),
                    "pnl": float(p.get("pnl", 0.0)),
                    "strike": details.get("strike"),
                    "expiry": details.get("expiry"),
                    "lot_size": details.get("lot_size"),
                    "option_type": "CE" if "CE" in str(p.get("trading_symbol")) else "PE",
                    "product": p.get("product")
                })
            return positions

        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    # ==================================================================
    # 2. EXECUTION ENGINE (V3)
    # ==================================================================
    async def execute_adjustment(self, adj: Dict) -> Dict:
        """
        Main Entry Point for Trading.
        Handles Futures resolution, Distributed Idempotency, and Execution.
        """
        instrument_key = adj.get("instrument_key")
        qty = abs(int(adj.get("quantity", 0)))
        side = adj.get("side")
        strategy = adj.get("strategy", "AUTO")
        is_hedge = adj.get("is_hedge", False)

        if qty <= 0 or side not in ("BUY", "SELL"):
            return {"status": "FAILED", "reason": "Invalid Order Params"}

        # FIXED: Logic moved inside TRY block to handle Redis crashes gracefully
        try:
            # 1. Resolve Dynamic Futures
            if instrument_key == "NIFTY_FUT_CURRENT":
                instrument_key = registry.get_current_future("NIFTY")
                if not instrument_key:
                    return {"status": "FAILED", "reason": "Future Not Found"}

            # 2. Distributed Idempotency Check (Redis)
            # Key: "idempotency:KEY:QTY:SIDE:STRATEGY"
            cycle_id = adj.get("cycle_id", "NO_CYCLE")
            idem_key = f"idempotency:{cycle_id}:{instrument_key}:{qty}:{side}"
            
            # ATOMIC LOCK: setnx (Set if Not Exists)
            is_new = await self.redis.set(idem_key, "PENDING", ex=self.IDEMPOTENCY_TTL, nx=True)
            
            if not is_new:
                logger.warning(f"🛑 Duplicate order blocked by Redis: {idem_key}")
                return {"status": "DUPLICATE"}

            # 3. Determine Order Type & Price
            target_price = float(adj.get("price", 0.0))
            order_type = "MARKET"

            # Smart Limit Logic: Avoid Market Orders on Options to prevent slippage
            if target_price <= 0 and "FUT" not in instrument_key: 
                ltp = await self._fetch_ltp_v3(instrument_key)
                if ltp > 0:
                    buffer = 0.03 # 3% buffer for guaranteed fill (Pseudo-Market)
                    if side == "BUY":
                        target_price = round(ltp * (1 + buffer), 1)
                    else:
                        target_price = round(ltp * (1 - buffer), 1)
                    order_type = "LIMIT"
                else:
                    # Fallback if LTP unavailable
                    order_type = "MARKET"
            elif target_price > 0:
                order_type = "LIMIT"

            # 4. Execute V3 Order
            order_id = await self._place_order_v3(
                instrument_key, qty, side, order_type, target_price
            )

            # 5. Update Idempotency Key with Order ID
            await self.redis.set(idem_key, f"PLACED:{order_id}", ex=self.IDEMPOTENCY_TTL)

            # 6. Persist to DB
            await self._persist_trade(order_id, instrument_key, qty, side, strategy, is_hedge, target_price)

            return {
                "status": "PLACED",
                "order_id": order_id,
                "type": order_type,
                "price": target_price
            }

        except Exception as e:
            # Now even Redis failures are caught here
            logger.error(f"Execution Failed: {e}")
            return {"status": "FAILED", "error": str(e)}

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def _place_order_v3(self, key: str, qty: int, side: str, order_type: str, price: float) -> str:
        """
        Sends Order to Upstox V3 API.
        Enables 'slice' for handling large quantities automatically.
        """
        url = f"{self.base_v3}/order/place"
        
        payload = {
            "quantity": qty,
            "product": "D",  # Intraday
            "validity": "DAY",
            "price": price,
            "tag": "VolGuard",
            "instrument_token": key,
            "order_type": order_type,
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
            "slice": True  # SMART FEATURE: Auto-slice large orders
        }

        resp = await self.client.post(url, json=payload)
        resp_json = resp.json()

        if resp.status_code == 200 and resp_json.get("status") == "success":
            return resp_json.get("data", {}).get("order_id")
        else:
            errors = resp_json.get("errors", [])
            err_msg = errors[0].get("message") if errors else "Unknown Error"
            # Log full response for debugging
            logger.error(f"Upstox Order Fail Payload: {payload} | Response: {resp.text}")
            raise RuntimeError(f"Upstox Error: {err_msg}")

    # ==================================================================
    # 3. UTILITIES
    # ==================================================================
    async def _fetch_ltp_v3(self, key: str) -> float:
        """Quick LTP fetch for Limit Pricing"""
        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": key}
        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                # Handle varying response formats (key with or without Exchange prefix)
                for k, v in data.items():
                    if key in k or k in key:
                        return float(v.get("last_price", 0.0))
            return 0.0
        except Exception:
            return 0.0

    async def _persist_trade(self, order_id, token, qty, side, strategy, is_hedge, price):
        """Log trade to Postgres"""
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()),
                    trade_tag=str(order_id),
                    instrument_key=token,
                    quantity=qty,
                    side=side,
                    entry_price=price,
                    strategy=strategy,
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size"),
                    status="OPEN",
                    is_hedge=is_hedge,
                    timestamp=datetime.utcnow()
                )
                session.add(trade)
                await session.commit()
        except Exception as e:
            # We don't crash execution if logging fails, but it is critical.
            logger.critical(f"FATAL: Trade Persistence Failed for {order_id}: {e}")

    async def close_all_positions(self, reason: str):
        """Panic Button: Close everything"""
        logger.critical(f"🚨 EXECUTING PANIC CLOSE: {reason}")
        positions = await self.get_positions()
        results = []
        
        for p in positions:
            # Reverse side
            exit_side = "SELL" if p["side"] == "BUY" else "BUY"
            
            res = await self.execute_adjustment({
                "instrument_key": p["instrument_key"],
                "quantity": p["quantity"],
                "side": exit_side,
                "strategy": f"PANIC_{reason}",
                "price": 0.0, # Market Exit
                "cycle_id": f"PANIC_{int(time.time())}" # Unique ID for panic
            })
            results.append(res)
            
        return results
