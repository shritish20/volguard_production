import asyncio
import logging
import subprocess
import sys
# NOTE: We do NOT import app.config here to prevent loading old env vars

# Configure JSON Logging for Production
from app.utils.logging import setup_logging
logger = setup_logging()

async def main():
    # --- 1. AUTO LOGIN SEQUENCE ---
    logger.info("🔐 Running Token Manager...")
    try:
        # Run script to refresh token in .env
        subprocess.run([sys.executable, "scripts/token_manager.py"], check=False)
        logger.info("Token refresh sequence complete.")
    except Exception as e:
        logger.warning(f"⚠️ Token Manager issue: {e}")

    # --- 2. DELAYED IMPORTS (Critical for Auto-Login) ---
    # We import these NOW so they pick up the fresh .env values
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

    logger.info(f"Booting VolGuard: {settings.PROJECT_NAME} (Env: {settings.ENVIRONMENT})")

    # --- 3. Initialize Database ---
    logger.info("Initializing Database...")
    await init_db()

    # --- 4. Initialize Clients ---
    market = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )

    # Validate Token immediately
    try:
        await market.get_spot_price()
        logger.info("✅ Upstox Token Validated.")
    except Exception:
        logger.critical("❌ UPSTOX TOKEN INVALID. Bot cannot start.")
        return

    # --- 5. Initialize Engines ---
    config_dict = settings.model_dump()
    risk = RiskEngine(config_dict)
    adj = AdjustmentEngine(config_dict)
    executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    engine = TradingEngine(market, config_dict)

    # --- 6. WebSocket Service ---
    ws = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        ws = UpstoxFeedService(settings.UPSTOX_ACCESS_TOKEN)

    # --- 7. Initialize Supervisor ---
    sup = ProductionTradingSupervisor(
        market_client=market,
        risk_engine=risk,
        adjustment_engine=adj,
        trade_executor=executor,
        trading_engine=engine,
        websocket_service=ws,
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL,
        total_capital=settings.BASE_CAPITAL
    )

    # --- 8. Set Execution Mode ---
    if settings.ENVIRONMENT == "production_live":
        logger.warning("⚠️ SYSTEM STARTING IN FULL_AUTO MODE - REAL MONEY AT RISK ⚠️")
        sup.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("System starting in SEMI_AUTO mode - Approvals Required")
        sup.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("System starting in SHADOW mode (Safety Default)")
        sup.safety.execution_mode = ExecutionMode.SHADOW

    # --- 9. Start the Loop ---
    try:
        await sup.start()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Fatal Startup Error: {e}", exc_info=True)
    finally:
        await market.close()
        if ws: await ws.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
