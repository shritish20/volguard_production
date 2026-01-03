# run_production.py

import asyncio
import logging
import subprocess
import sys
import signal
import os
import time

from app.utils.logging import setup_logging
logger = setup_logging()

_shutdown = False


def _handle_shutdown(sig, frame):
    global _shutdown
    logger.warning(f"Received shutdown signal ({sig}). Preparing graceful exit...")
    _shutdown = True


# Register signal handlers (Docker / K8s safe)
signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


async def main():
    # ======================================================
    # 1. TOKEN REFRESH (STRICT)
    # ======================================================
    logger.info("🔐 Running token refresh sequence...")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/token_manager.py"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error("❌ Token refresh script failed.")
            logger.error(result.stderr)
            return
        logger.info("✅ Token refresh completed.")
    except Exception as e:
        logger.critical(f"❌ Token manager execution failed: {e}")
        return

    # ======================================================
    # 2. DELAYED IMPORTS (MANDATORY)
    # ======================================================
    from app.config import settings
    from app.database import init_db
    from app.lifecycle.supervisor import ProductionTradingSupervisor
    from app.core.market.data_client import MarketDataClient
    from app.core.risk.engine import RiskEngine
    from app.core.trading.adjustment_engine import AdjustmentEngine
    from app.core.trading.executor import TradeExecutor
    from app.core.trading.engine import TradingEngine
    from app.core.market.websocket_client import UpstoxFeedService
    from app.lifecycle.safety_controller import ExecutionMode

    logger.info(
        f"🚀 Booting {settings.PROJECT_NAME} | Env={settings.ENVIRONMENT}"
    )

    # ======================================================
    # 3. DATABASE INIT
    # ======================================================
    logger.info("📦 Initializing database...")
    await init_db()

    # ======================================================
    # 4. MARKET CLIENT
    # ======================================================
    market = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3,
    )

    # Hard token validation
    try:
        price = await market.get_spot_price()
        if price <= 0:
            raise RuntimeError("Spot price invalid")
        logger.info("✅ Upstox token validated.")
    except Exception as e:
        logger.critical(f"❌ Upstox token invalid: {e}")
        return

    # ======================================================
    # 5. ENGINE INITIALIZATION
    # ======================================================
    config = settings.model_dump()

    risk = RiskEngine(config)
    adj = AdjustmentEngine(config)
    executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    engine = TradingEngine(market, config)

    # ======================================================
    # 6. WEBSOCKET (OPTIONAL, GUARDED)
    # ======================================================
    ws = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        try:
            ws = UpstoxFeedService(settings.UPSTOX_ACCESS_TOKEN)
            await ws.connect()
            logger.info("📡 WebSocket connected.")
        except Exception as e:
            logger.error(f"WebSocket disabled due to error: {e}")
            ws = None

    # ======================================================
    # 7. SUPERVISOR
    # ======================================================
    sup = ProductionTradingSupervisor(
        market_client=market,
        risk_engine=risk,
        adjustment_engine=adj,
        trade_executor=executor,
        trading_engine=engine,
        websocket_service=ws,
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL,
        total_capital=settings.BASE_CAPITAL,
    )

    # Execution Mode
    if settings.ENVIRONMENT == "production_live":
        logger.warning("⚠️ FULL_AUTO MODE (REAL MONEY)")
        sup.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("SEMI_AUTO MODE (Approvals Required)")
        sup.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("SHADOW MODE (Default)")
        sup.safety.execution_mode = ExecutionMode.SHADOW

    # ======================================================
    # 8. RUN LOOP (AUTO-RESTART)
    # ======================================================
    logger.info("🧠 Supervisor started.")

    try:
        while not _shutdown:
            try:
                await sup.start()
            except Exception as e:
                logger.critical("Supervisor crashed. Restarting in 5s.", exc_info=True)
                await asyncio.sleep(5)
    finally:
        logger.info("🛑 Shutting down gracefully...")
        await market.close()
        if ws:
            await ws.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
