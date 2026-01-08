import asyncio
import logging
import signal
import sys

from app.config import settings
from app.database import init_db
from app.utils.logging import setup_logging
from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.lifecycle.safety_controller import ExecutionMode

# Core Components
from app.core.market.data_client import MarketDataClient, NIFTY_KEY, VIX_KEY
from app.core.market.websocket_client import MarketDataFeed
from app.core.risk.engine import RiskEngine
from app.core.risk.capital_governor import CapitalGovernor
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.trading.adjustment_engine import AdjustmentEngine

# NEW: VolGuard 4.1 Dependency
from app.services.instrument_registry import registry

# Initialize Structured Logging
logger = setup_logging()

async def shutdown(signal_name, loop, supervisor, resources):
    """Graceful Shutdown Handler with Resource Cleanup"""
    logger.info(f"🛑 Received exit signal {signal_name.name}...")
    
    if supervisor:
        logger.info("Stopping Supervisor loop...")
        supervisor.running = False
        
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # Close Resources
    logger.info("Closing Resources...")
    for name, res in resources.items():
        if res:
            try:
                if hasattr(res, 'close'):
                    if asyncio.iscoroutinefunction(res.close):
                        await res.close()
                    else:
                        res.close()
                elif hasattr(res, 'disconnect'):
                    if asyncio.iscoroutinefunction(res.disconnect):
                        await res.disconnect()
                    else:
                        res.disconnect()
                logger.info(f"✅ {name} closed.")
            except Exception as e:
                logger.error(f"❌ Failed to close {name}: {e}")

    loop.stop()

async def main():
    logger.info(f"🚀 Starting VolGuard 4.1 Supervisor [Env: {settings.ENVIRONMENT}]")

    # 1. Database Initialization
    logger.info("Connecting to Database...")
    await init_db()

    # 2. VolGuard 4.1 Prerequisite: Load Instrument Registry
    # We do this BEFORE initializing engines so Lot Sizes are available immediately.
    logger.info("📦 Pre-loading Instrument Master (VolGuard 4.1)...")
    try:
        # Run in thread to avoid blocking loop if download is slow
        await asyncio.to_thread(registry.load_master)
    except Exception as e:
        logger.error(f"⚠️ Registry pre-load failed: {e}. Supervisor will retry in loop.")

    # 3. Initialize Market Data Client
    market_client = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )
    
    # 4. Initialize Trade Executor
    trade_executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    
    # 5. Initialize WebSocket Service
    websocket_service = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        logger.info("🔌 Initializing WebSocket Service...")
        
        websocket_service = MarketDataFeed(
            access_token=settings.UPSTOX_ACCESS_TOKEN,
            instrument_keys=[NIFTY_KEY, VIX_KEY],  # Core subscriptions
            mode="full",
            auto_reconnect_enabled=True,
            reconnect_interval=10,
            max_retries=5
        )
        logger.info(f"✅ WebSocket configured for {NIFTY_KEY} & {VIX_KEY}")
    else:
        logger.warning("⚠️ WebSocket DISABLED - Using REST API fallback only")

    # 6. Initialize Risk & Trading Engines
    logger.info("⚙️ Initializing VolGuard Engines...")
    
    risk_engine = RiskEngine(max_portfolio_loss=settings.MAX_DAILY_LOSS)
    
    cap_governor = CapitalGovernor(
        access_token=settings.UPSTOX_ACCESS_TOKEN,
        total_capital=settings.BASE_CAPITAL,
        max_daily_loss=settings.MAX_DAILY_LOSS,
        max_positions=settings.MAX_POSITIONS
    )
    
    # This engine now uses the new Mandate logic
    trading_engine = TradingEngine(market_client, settings.model_dump())
    
    adj_engine = AdjustmentEngine(delta_threshold=15.0)

    # 7. Initialize Supervisor
    logger.info("🧠 Booting Production Supervisor...")
    supervisor = ProductionTradingSupervisor(
        market_client=market_client,
        risk_engine=risk_engine,
        adjustment_engine=adj_engine,
        trade_executor=trade_executor,
        trading_engine=trading_engine,
        capital_governor=cap_governor,
        websocket_service=websocket_service,
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL
    )

    # 8. Set Execution Mode
    if settings.ENVIRONMENT == "production_live":
        logger.warning("🚨 !!! RUNNING IN FULL_AUTO MODE - REAL TRADING ENABLED !!! 🚨")
        supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("⚠️ Running in SEMI_AUTO Mode - Manual Approvals Required")
        supervisor.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("✅ Running in SHADOW Mode - Monitoring Only")
        supervisor.safety.execution_mode = ExecutionMode.SHADOW

    # 9. Setup Signal Handlers
    loop = asyncio.get_running_loop()
    resources = {
        "MarketClient": market_client,
        "WebsocketService": websocket_service,
        "TradeExecutor": trade_executor
    }
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda s=signame: asyncio.create_task(shutdown(s, loop, supervisor, resources))
        )

    # 10. Start Supervisor Loop
    logger.info("🎯 Starting Supervisor Loop...")
    logger.info(f"📊 Loop Interval: {settings.SUPERVISOR_LOOP_INTERVAL}s")
    logger.info(f"💰 Base Capital: ₹{settings.BASE_CAPITAL:,.0f}")
    logger.info("=" * 60)
    
    try:
        await supervisor.start()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.critical(f"💥 Supervisor crashed: {e}", exc_info=True)
        await shutdown(signal.SIGTERM, loop, supervisor, resources)

if __name__ == "__main__":
    # Ensure event loop policy is correct for Windows (if developing there)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
