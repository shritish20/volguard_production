# app/core/trading/executor.py

import logging
import httpx
import asyncio
import uuid
import time
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import redis.asyncio as redis
from sqlalchemy.future import select
from tenacity import retry, stop_after_attempt, wait_fixed

from app.config import settings
from app.database import AsyncSessionLocal, TradeRecord
from app.services.instrument_registry import registry
from app.core.auth.token_manager import TokenManager  # NEW: Token Manager

logger = logging.getLogger(__name__)

class TradeExecutor:
    """  
    VolGuard 5.0 Smart Trade Executor - Enhanced for Hybrid Logic
    
    CRITICAL UPGRADES:
    1. ✅ Token Manager Integration (Dynamic token refresh)
    2. ✅ exit_all_positions() method for SafetyController panic button
    3. ✅ Margin reporting for CapitalGovernor audit
    4. ✅ Enhanced error handling with hybrid logic awareness
    """  

    def __init__(self, token_manager: TokenManager):  # CHANGED: TokenManager instead of access_token
        self.token_manager = token_manager
        self.base_v3 = settings.UPSTOX_BASE_V3  
        self.base_v2 = settings.UPSTOX_BASE_V2  
        self.IDEMPOTENCY_TTL = 3600  # 1 Hour

        # Async Client (headers will be dynamic via token_manager)
        self.client = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
        )

        # Redis Connection for Idempotency & Locking
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    def _get_headers(self) -> Dict[str, str]:
        """Get current headers from TokenManager"""
        return self.token_manager.get_headers()

    async def close(self):
        """Cleanup resources."""
        await self.client.aclose()
        await self.redis.close()

    # ------------------------------------------------------------------
    # NEW: EXIT ALL POSITIONS (for SafetyController panic button)
    # ------------------------------------------------------------------
    async def exit_all_positions(self, reason: str = "EMERGENCY") -> Dict:
        """
        🚨 PANIC BUTTON: Atomic exit of ALL positions
        
        Called by SafetyController.trigger_full_stop()
        
        Args:
            reason: Emergency reason (e.g., "MARGIN_MISMATCH", "MANUAL_TRIGGER")
            
        Returns:
            Execution results with summary
        """
        logger.critical(f"🔴 EXECUTING ATOMIC EXIT: {reason}")
        
        try:
            # Get current positions
            positions = await self.get_positions()
            
            if not positions:
                logger.info("✅ No active positions to exit")
                return {
                    "status": "COMPLETED",
                    "reason": reason,
                    "positions_exited": 0,
                    "message": "No active positions found"
                }
            
            logger.warning(f"🔄 Exiting {len(positions)} positions...")
            
            # Use bulk exit API if available
            bulk_result = await self._try_bulk_exit(positions, reason)
            
            if bulk_result.get("success"):
                logger.info(f"✅ Bulk exit successful: {bulk_result}")
                return {
                    "status": "COMPLETED",
                    "reason": reason,
                    "positions_exited": bulk_result.get("exited_count", 0),
                    "method": "BULK_API",
                    "details": bulk_result
                }
            else:
                # Fallback to individual exits
                logger.warning("Bulk exit failed, using individual exits...")
                individual_results = await self._exit_positions_individually(positions, reason)
                
                exited_count = sum(1 for r in individual_results if r.get("status") == "PLACED")
                
                return {
                    "status": "COMPLETED" if exited_count == len(positions) else "PARTIAL",
                    "reason": reason,
                    "positions_exited": exited_count,
                    "total_positions": len(positions),
                    "method": "INDIVIDUAL",
                    "individual_results": individual_results
                }
                
        except Exception as e:
            logger.critical(f"💥 Atomic exit failed: {e}")
            return {
                "status": "FAILED",
                "reason": reason,
                "error": str(e),
                "positions_exited": 0
            }

    async def _try_bulk_exit(self, positions: List[Dict], reason: str) -> Dict:
        """
        Try to use Upstox's bulk exit API
        
        Returns:
            Result dictionary
        """
        try:
            headers = self._get_headers()
            
            # This endpoint might vary - check Upstox documentation
            exit_url = "https://api.upstox.com/v2/order/positions/exit"
            
            # Prepare exit orders
            exit_orders = []
            for pos in positions:
                exit_orders.append({
                    "instrument_token": pos["instrument_key"],
                    "transaction_type": "SELL" if pos["side"] == "BUY" else "BUY",
                    "quantity": abs(pos["quantity"]),
                    "order_type": "MARKET",
                    "product": "I",
                    "tag": f"EMERGENCY_EXIT_{reason}"
                })
            
            # Try bulk API if supported
            payload = {
                "orders": exit_orders,
                "tag": f"VolGuard_Emergency_{reason}"
            }
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    exit_url,
                    json=payload,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "success": True,
                        "exited_count": len(exit_orders),
                        "response": data
                    }
                else:
                    logger.warning(f"Bulk exit API returned {response.status_code}")
                    return {"success": False, "error": f"HTTP {response.status_code}"}
                    
        except Exception as e:
            logger.debug(f"Bulk exit failed, falling back to individual: {e}")
            return {"success": False, "error": str(e)}

    async def _exit_positions_individually(self, positions: List[Dict], reason: str) -> List[Dict]:
        """
        Fallback: Exit each position individually
        """
        results = []
        
        for pos in positions:
            try:
                exit_side = "SELL" if pos["side"] == "BUY" else "BUY"
                
                result = await self.execute_adjustment({
                    "instrument_key": pos["instrument_key"],
                    "quantity": pos["quantity"],
                    "side": exit_side,
                    "strategy": f"PANIC_{reason}",
                    "action": "EXIT",  # Mark as exit for idempotency
                    "price": 0.0,  # Market order
                    "cycle_id": f"PANIC_{int(time.time())}_{pos['instrument_key']}",
                    "is_hedge": False,
                    "reason": reason
                })
                
                results.append({
                    "instrument": pos["instrument_key"],
                    "result": result
                })
                
            except Exception as e:
                logger.error(f"Failed to exit position {pos['instrument_key']}: {e}")
                results.append({
                    "instrument": pos["instrument_key"],
                    "error": str(e),
                    "status": "FAILED"
                })
        
        return results

    # ------------------------------------------------------------------
    # Enhanced Reconciliation
    # ------------------------------------------------------------------
    async def reconcile_state(self):
        """
        Enhanced reconciliation with margin tracking
        """
        logger.info("🔧 Starting Enhanced State Reconciliation...")

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

                # 3. Calculate Checksums
                broker_checksum = self._calculate_position_checksum(broker_positions)
                db_checksum = self._calculate_db_checksum(db_trades)

                reconciliation_report = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "broker_positions": len(broker_positions),
                    "db_open_trades": len(db_trades),
                    "broker_checksum": broker_checksum,
                    "db_checksum": db_checksum,
                    "ghost_trades": [],
                    "orphan_trades": [],
                    "mismatches": []
                }

                # Case A: Ghost Trades (DB says OPEN, Broker says CLOSED)
                for key, trade in db_map.items():
                    if key not in broker_map:
                        logger.warning(f"👻 Ghost Trade Found: {key}. Marking CLOSED.")
                        trade.status = "CLOSED"
                        trade.closed_at = datetime.utcnow()
                        trade.reason = "RECONCILIATION_AUTO_CLOSE"
                        reconciliation_report["ghost_trades"].append(key)

                # Case B: Orphan Trades (Broker says OPEN, DB has no record)
                for key, pos in broker_map.items():
                    if key not in db_map:
                        logger.error(f"🔴 Orphan Trade Found: {key} (Qty: {pos['quantity']}). Injecting DB Record.")
                        orphan_trade = TradeRecord(
                            id=str(uuid.uuid4()),
                            trade_tag=f"RECOVERY_{int(time.time())}",
                            instrument_key=key,
                            symbol=pos['symbol'],
                            quantity=pos['quantity'],
                            side=pos['side'],
                            entry_price=pos['average_price'],
                            strategy="MANUAL_RECOVERY",
                            status="OPEN",
                            timestamp=datetime.utcnow(),
                            reason="RECONCILIATION_INJECTION"
                        )
                        session.add(orphan_trade)
                        reconciliation_report["orphan_trades"].append(key)

                # Case C: Quantity Mismatches
                for key in set(broker_map.keys()) & set(db_map.keys()):
                    broker_qty = broker_map[key]['quantity']
                    db_qty = db_map[key].quantity
                    
                    if abs(broker_qty) != abs(db_qty):
                        logger.error(f"🔴 QUANTITY MISMATCH: {key} - Broker: {broker_qty}, DB: {db_qty}")
                        reconciliation_report["mismatches"].append({
                            "instrument": key,
                            "broker_qty": broker_qty,
                            "db_qty": db_qty
                        })

                await session.commit()
                
                # Save Report
                await self._save_reconciliation_report(reconciliation_report)

                # Enhanced logging for hybrid logic
                total_discrepancies = (
                    len(reconciliation_report["ghost_trades"]) +
                    len(reconciliation_report["orphan_trades"]) +
                    len(reconciliation_report["mismatches"])
                )

                if total_discrepancies > 0:
                    logger.error(f"🔴 Reconciliation found {total_discrepancies} discrepancies")
                    if settings.ENVIRONMENT in ['production_live', 'production_semi', 'FULL_AUTO']:
                         raise RuntimeError(f"CRITICAL: Reconciliation found {total_discrepancies} discrepancies. System Halted.")

                logger.info(f"✅ State Reconciliation Complete - {total_discrepancies} discrepancies handled")

        except Exception as e:
            logger.critical(f"🔴 FATAL: Reconciliation Failed: {e}")
            raise e

    def _calculate_position_checksum(self, positions: List[Dict]) -> str:
        """Calculate checksum of broker positions"""
        sorted_positions = sorted(positions, key=lambda p: p.get('instrument_key', ''))
        checksum_data = []
        for p in sorted_positions:
            checksum_data.append(f"{p.get('instrument_key')}:{p.get('quantity')}:{p.get('side')}")
        checksum_string = "|".join(checksum_data)
        return hashlib.sha256(checksum_string.encode()).hexdigest()[:16]

    def _calculate_db_checksum(self, trades: List) -> str:
        """Calculate checksum of database trades"""
        sorted_trades = sorted(trades, key=lambda t: t.instrument_key)
        checksum_data = []
        for t in sorted_trades:
            checksum_data.append(f"{t.instrument_key}:{t.quantity}:{t.side}")
        checksum_string = "|".join(checksum_data)
        return hashlib.sha256(checksum_string.encode()).hexdigest()[:16]

    async def _save_reconciliation_report(self, report: Dict):
        """Save reconciliation report to audit log"""
        journal_dir = Path("journal")
        journal_dir.mkdir(exist_ok=True)
        filename = f"reconciliation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = journal_dir / filename
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)

    # ------------------------------------------------------------------
    # Enhanced Execution with Margin Reporting
    # ------------------------------------------------------------------
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_positions(self) -> List[Dict]:
        """
        Enhanced position fetching with token manager
        """
        url = f"{self.base_v2}/portfolio/short-term-positions"
        try:
            headers = self._get_headers()
            resp = await self.client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json().get("data", [])

            positions = []
            for p in data:
                net_qty = int(p.get("net_quantity", 0))
                if net_qty == 0:
                    continue

                instrument_token = p.get("instrument_token")
                details = registry.get_instrument_details(instrument_token)

                side = "BUY" if net_qty > 0 else "SELL"
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
                    "product": p.get("product"),
                    "m2m": float(p.get("m2m", 0.0))  # Added for dashboard
                })
            return positions

        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            raise e

    async def execute_adjustment(self, adj: Dict) -> Dict:
        """
        Enhanced execution with margin reporting
        
        NEW: Returns required_margin for CapitalGovernor learning
        """
        instrument_key = adj.get("instrument_key")
        qty = abs(int(adj.get("quantity", 0)))
        side = adj.get("side")
        strategy = adj.get("strategy", "AUTO")
        action = adj.get("action", "ENTRY")
        is_hedge = adj.get("is_hedge", False)

        if qty <= 0 or side not in ("BUY", "SELL"):
            return {"status": "FAILED", "reason": "Invalid Order Params"}

        try:
            # 1. Resolve Dynamic Futures
            if instrument_key == "NIFTY_FUT_CURRENT":
                instrument_key = registry.get_current_future("NIFTY")
                if not instrument_key:
                    return {"status": "FAILED", "reason": "Future Not Found"}

            # 2. Distributed Idempotency Check
            cycle_id = adj.get("cycle_id", "NO_CYCLE")
            timestamp_ms = int(time.time() * 1000)
            
            idem_key = f"idempotency:{cycle_id}:{instrument_key}:{qty}:{side}:{action}:{strategy}:{timestamp_ms}"
            
            redis_available = True
            is_new = False

            try:
                is_new = await self.redis.set(idem_key, "PENDING", ex=self.IDEMPOTENCY_TTL, nx=True)

                if not is_new:
                    logger.warning(f"🛑 Duplicate order blocked by Redis: {idem_key}")
                    return {"status": "DUPLICATE", "reason": "Order already processed"}

            except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
                logger.critical(f"⚠️ REDIS FAILURE: {e}")
                redis_available = False
                
                if settings.ENVIRONMENT in ['production_live', 'FULL_AUTO']:
                    return {"status": "FAILED", "reason": "CRITICAL: Idempotency unavailable in production"}
                
                is_new = True

            # 3. Determine Order Type & Price
            target_price = float(adj.get("price", 0.0))
            order_type = "MARKET"

            # Smart Limit Logic (enhanced for hybrid strategy)
            if target_price <= 0 and "FUT" not in instrument_key:
                ltp = await self._fetch_ltp_v3(instrument_key)
                if ltp > 0:
                    # Dynamic buffer based on strategy
                    buffer = self._get_dynamic_buffer(strategy, side)
                    if side == "BUY":
                        target_price = round(ltp * (1 + buffer), 1)
                    else:
                        target_price = round(ltp * (1 - buffer), 1)
                    order_type = "LIMIT"
                else:
                    order_type = "MARKET"
            elif target_price > 0:
                order_type = "LIMIT"

            # 4. Execute V3 Order
            order_id = await self._place_order_v3(
                instrument_key, qty, side, order_type, target_price
            )

            # 5. Calculate estimated margin (for CapitalGovernor)
            required_margin = await self._estimate_margin_requirement(
                instrument_key, qty, side, target_price
            )

            # 6. Update Idempotency Key
            if redis_available:
                try:
                    await self.redis.set(idem_key, f"PLACED:{order_id}", ex=self.IDEMPOTENCY_TTL)
                except Exception as e:
                    logger.error(f"Failed to update Redis after order placement: {e}")

            # 7. Persist to DB
            await self._persist_trade(
                order_id, instrument_key, qty, side, strategy, is_hedge, target_price
            )

            # 8. Verify Order Status
            verification = await self.verify_order_status(order_id)
            if not verification.get("verified", False):
                logger.error(f"⚠️ Could not verify order {order_id} - manual check required")
            elif verification.get("status") == "rejected":
                logger.error(f"❌ Order {order_id} was REJECTED by broker")
                await self._update_trade_status(order_id, "REJECTED", verification)
            else:
                logger.info(f"✅ Order {order_id} verified: {verification.get('status')}")

            # 9. Return enhanced result
            result = {
                "status": "PLACED",
                "order_id": order_id,
                "type": order_type,
                "price": target_price,
                "required_margin": required_margin,  # NEW: For CapitalGovernor
                "quantity": qty,
                "side": side,
                "instrument_key": instrument_key,
                "verification": verification,
                "idempotency_key": idem_key,
                "timestamp": datetime.now().isoformat()
            }

            # Add warning if Redis was down
            if not redis_available:
                result["warning"] = "REDIS_UNAVAILABLE"

            return result

        except Exception as e:
            logger.error(f"Execution Failed: {e}", exc_info=True)
            return {"status": "FAILED", "error": str(e)}

    def _get_dynamic_buffer(self, strategy: str, side: str) -> float:
        """
        Get dynamic price buffer based on strategy type
        """
        # Hybrid logic strategies get tighter buffers
        if "IRON_CONDOR" in strategy or "IRON_FLY" in strategy:
            return 0.02  # 2% for defined risk strategies
        elif "STRANGLE" in strategy:
            return 0.03  # 3% for undefined risk
        elif side == "BUY":
            return 0.01  # 1% for buying (we want to get filled)
        else:
            return 0.025  # 2.5% default for selling

    async def _estimate_margin_requirement(self, instrument_key: str, qty: int, 
                                          side: str, price: float) -> float:
        """
        Estimate margin requirement for CapitalGovernor learning
        
        Returns:
            Estimated margin requirement
        """
        try:
            # Simple estimation based on lot size and price
            details = registry.get_instrument_details(instrument_key)
            lot_size = details.get("lot_size", 50)
            lots = qty / lot_size
            
            if side == "SELL":
                # For selling options, margin is higher
                # Rough estimate: 20% of notional value
                notional = price * qty
                return notional * 0.20
            else:
                # For buying options, just the premium
                return price * qty * 1.05  # 5% buffer
                
        except Exception as e:
            logger.debug(f"Margin estimation failed: {e}")
            return 0.0

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
    async def _place_order_v3(self, key: str, qty: int, side: str, 
                             order_type: str, price: float) -> str:
        """
        Sends Order to Upstox V3 API with dynamic headers
        """
        url = f"{self.base_v3}/order/place"
        headers = self._get_headers()

        payload = {
            "quantity": qty,
            "product": "D",
            "validity": "DAY",
            "price": price,
            "tag": "VolGuard_5.0",  # Updated tag
            "instrument_token": key,
            "order_type": order_type,
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0.0,
            "is_amo": False,
            "slice": True
        }

        resp = await self.client.post(url, json=payload, headers=headers)
        resp.json_data = resp.json()

        if resp.status_code == 200 and resp.json_data.get("status") == "success":
            return resp.json_data.get("data", {}).get("order_id")
        else:
            errors = resp.json_data.get("errors", [])
            err_msg = errors[0].get("message") if errors else "Unknown Error"
            logger.error(f"Upstox Order Fail Payload: {payload} | Response: {resp.text}")
            raise RuntimeError(f"Upstox Error: {err_msg}")

    async def _fetch_ltp_v3(self, key: str) -> float:
        """Quick LTP fetch with dynamic headers"""
        url = f"{self.base_v3}/market-quote/ltp"
        params = {"instrument_key": key}
        headers = self._get_headers()

        try:
            resp = await self.client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                for k, v in data.items():
                    if key in k or k in key:
                        return float(v.get("last_price", 0.0))
                return 0.0
        except Exception:
            return 0.0

    async def _persist_trade(self, order_id, token, qty, side, strategy, is_hedge, price):
        """Enhanced trade persistence with hybrid logic metadata"""
        try:
            details = registry.get_instrument_details(token)
            async with AsyncSessionLocal() as session:
                trade = TradeRecord(
                    id=str(uuid.uuid4()),
                    trade_tag=str(order_id),
                    instrument_key=token,
                    symbol=details.get("trading_symbol", "UNKNOWN"),
                    quantity=qty,
                    side=side,
                    entry_price=price,
                    strategy=strategy,
                    strike=details.get("strike"),
                    expiry=details.get("expiry"),
                    lot_size=details.get("lot_size"),
                    status="OPEN",
                    is_hedge=is_hedge,
                    timestamp=datetime.utcnow(),
                    metadata={  # NEW: Store hybrid logic metadata
                        "version": "5.0",
                        "execution_type": "HYBRID_LOGIC",
                        "timestamp": datetime.now().isoformat()
                    }
                )
                session.add(trade)
                await session.commit()

        except Exception as e:
            logger.critical(f"FATAL: Trade Persistence Failed for {order_id}: {e}")

    async def verify_order_status(self, order_id: str, max_retries: int = 3) -> Dict:
        """
        Enhanced order verification
        """
        url = f"{self.base_v3}/order/details"
        params = {"order_id": order_id}
        headers = self._get_headers()

        for attempt in range(max_retries):
            try:
                resp = await self.client.get(url, params=params, headers=headers)
                resp.raise_for_status()

                data = resp.json().get("data", {})
                status = data.get("status", "UNKNOWN")
                filled_qty = int(data.get("filled_quantity", 0))

                return {
                    "order_id": order_id,
                    "status": status,
                    "filled_quantity": filled_qty,
                    "average_price": float(data.get("average_price", 0.0)),
                    "verified": True,
                    "timestamp": datetime.now().isoformat()
                }

            except Exception as e:
                logger.warning(f"Order verification attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.0)

        return {"order_id": order_id, "verified": False, "status": "UNKNOWN"}

    async def _update_trade_status(self, order_id: str, status: str, details: Dict):
        """Update trade record status in database"""
        async with AsyncSessionLocal() as session:
            try:
                from sqlalchemy import update
                stmt = update(TradeRecord).where(
                    TradeRecord.trade_tag == str(order_id)
                ).values(
                    status=status,
                    reason=details.get("reason", "Status update"),
                    closed_at=datetime.utcnow() if status in ["REJECTED", "CLOSED"] else None,
                    metadata=details.get("metadata", {})
                )
                await session.execute(stmt)
                await session.commit()
            except Exception as e:
                logger.error(f"Failed to update trade status: {e}")

    # ------------------------------------------------------------------
    # Legacy method for backward compatibility
    # ------------------------------------------------------------------
    async def close_all_positions(self, reason: str):
        """
        Legacy method - redirects to new exit_all_positions
        """
        logger.warning(f"Using legacy close_all_positions, redirecting to exit_all_positions")
        return await self.exit_all_positions(reason)
