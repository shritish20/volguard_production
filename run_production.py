import asyncio
import logging
from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.core.market.data_client import MarketDataClient
from app.core.risk.engine import RiskEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.market.websocket_client import UpstoxFeedService
from app.lifecycle.safety_controller import ExecutionMode
from app.database import init_db
from app.config import settings

# Configure JSON Logging for Production
from app.utils.logging import setup_logging
logger = setup_logging()

async def main():
    logger.info(f"Booting VolGuard: {settings.PROJECT_NAME} (Env: {settings.ENVIRONMENT})")

    # 1. Initialize Database
    logger.info("Initializing Database...")
    await init_db()

    # 2. Initialize Clients
    market = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )
    
    # 3. Initialize Engines
    # Convert settings to dict for engines that expect config dicts
    config_dict = settings.model_dump()
    
    risk = RiskEngine(config_dict)
    adj = AdjustmentEngine(config_dict)
    executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    engine = TradingEngine(market, config_dict)
    
    # 4. WebSocket Service
    ws = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        ws = UpstoxFeedService(settings.UPSTOX_ACCESS_TOKEN)

    # 5. Initialize Supervisor
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

    # 6. Set Execution Mode based on Environment
    # CRITICAL: We default to SHADOW unless explicitly set to production_live
    if settings.ENVIRONMENT == "production_live":
        logger.warning("⚠️ SYSTEM STARTING IN FULL_AUTO MODE - REAL MONEY AT RISK ⚠️")
        sup.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("System starting in SEMI_AUTO mode - Approvals Required")
        sup.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("System starting in SHADOW mode (Safety Default)")
        sup.safety.execution_mode = ExecutionMode.SHADOW

    # 7. Start the Loop
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
