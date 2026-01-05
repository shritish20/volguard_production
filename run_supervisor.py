# run_supervisor.py
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
    
    # Close Resources (Handle various close method names)
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
    logger.info(f"🚀 Starting VolGuard Supervisor [Env: {settings.ENVIRONMENT}]")

    # 1. Database Initialization
    logger.info("Connecting to Database...")
    await init_db()

    # 2. Initialize Market Data Client
    market_client = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )
    
    # 3. Initialize Trade Executor (Redis connection handled internally)
    trade_executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    
    # 4. Initialize WebSocket Service with CRITICAL FIX
    websocket_service = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        logger.info("🔌 Initializing WebSocket Service...")
        
        # ✅ CRITICAL FIX: Subscribe to NIFTY + VIX for live market data
        websocket_service = MarketDataFeed(
            access_token=settings.UPSTOX_ACCESS_TOKEN,
            instrument_keys=[NIFTY_KEY, VIX_KEY],  # ✅ Core market data subscription
            mode="full",  # Get complete data including Greeks
            auto_reconnect_enabled=True,  # Enable SDK auto-reconnect
            reconnect_interval=10,  # Retry every 10 seconds
            max_retries=5  # Max 5 reconnection attempts
        )
        
        logger.info(f"✅ WebSocket configured with subscriptions:")
        logger.info(f"   • {NIFTY_KEY}")
        logger.info(f"   • {VIX_KEY}")
        logger.info(f"   • Mode: full (includes Greeks)")
        logger.info(f"   • Auto-reconnect: Enabled (10s interval, 5 retries)")
    else:
        logger.warning("⚠️ WebSocket DISABLED - Using REST API fallback only")

    # 5. Initialize Risk & Trading Engines
    logger.info("⚙️ Initializing Trading Engines...")
    
    # Risk Engine (expects float for max_portfolio_loss)
    risk_engine = RiskEngine(max_portfolio_loss=settings.MAX_DAILY_LOSS)
    
    # Capital Governor
    cap_governor = CapitalGovernor(
        access_token=settings.UPSTOX_ACCESS_TOKEN,
        total_capital=settings.BASE_CAPITAL,
        max_daily_loss=settings.MAX_DAILY_LOSS,
        max_positions=settings.MAX_POSITIONS
    )
    
    # Trading Engine (gets full config for strategy parameters)
    trading_engine = TradingEngine(market_client, settings.model_dump())
    
    # Adjustment Engine (delta hedging threshold)
    adj_engine = AdjustmentEngine(delta_threshold=15.0)

    # 6. Initialize Supervisor
    logger.info("🧠 Booting Production Supervisor...")
    supervisor = ProductionTradingSupervisor(
        market_client=market_client,
        risk_engine=risk_engine,
        adjustment_engine=adj_engine,
        trade_executor=trade_executor,
        trading_engine=trading_engine,
        capital_governor=cap_governor,
        websocket_service=websocket_service,  # ✅ WebSocket now properly configured
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL
    )

    # 7. Set Execution Mode based on Environment
    if settings.ENVIRONMENT == "production_live":
        logger.warning("🚨 !!! RUNNING IN FULL_AUTO MODE - REAL TRADING ENABLED !!! 🚨")
        logger.warning("🚨 !!! ALL TRADES WILL BE EXECUTED AUTOMATICALLY !!! 🚨")
        supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("⚠️ Running in SEMI_AUTO Mode - Manual Approvals Required")
        supervisor.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("✅ Running in SHADOW Mode - Monitoring Only (Safe)")
        supervisor.safety.execution_mode = ExecutionMode.SHADOW

    # 8. Setup Signal Handlers for Graceful Shutdown
    loop = asyncio.get_running_loop()
    resources = {
        "MarketClient": market_client,
        "WebsocketService": websocket_service,
        "TradeExecutor": trade_executor  # Now included for proper cleanup
    }
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda s=signame: asyncio.create_task(shutdown(s, loop, supervisor, resources))
        )

    # 9. Start Supervisor Loop
    logger.info("🎯 Starting Supervisor Loop...")
    logger.info(f"📊 Loop Interval: {settings.SUPERVISOR_LOOP_INTERVAL}s")
    logger.info(f"💰 Base Capital: ₹{settings.BASE_CAPITAL:,.0f}")
    logger.info(f"🛑 Max Daily Loss: ₹{settings.MAX_DAILY_LOSS:,.0f}")
    logger.info(f"📈 Max Positions: {settings.MAX_POSITIONS}")
    logger.info("=" * 60)
    
    try:
        await supervisor.start()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.critical(f"💥 Supervisor crashed: {e}", exc_info=True)
        # Attempt emergency cleanup
        await shutdown(signal.SIGTERM, loop, supervisor, resources)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        pass  # Handled by signal handler
