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
from app.core.market.data_client import MarketDataClient
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

    # 2. Initialize Clients
    market_client = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )
    
    # Initialize Executor (Now connects to Redis automatically)
    trade_executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    
    websocket_service = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        # Pass empty keys initially; Supervisor will subscribe dynamically
        websocket_service = MarketDataFeed(
            settings.UPSTOX_ACCESS_TOKEN, 
            [] 
        )

    # 3. Initialize Engines (FIXED PARAMETERS)
    # 🔴 FIX: RiskEngine expects a float, not a dict
    risk_engine = RiskEngine(max_portfolio_loss=settings.MAX_PORTFOLIO_LOSS)
    
    cap_governor = CapitalGovernor(
        access_token=settings.UPSTOX_ACCESS_TOKEN,
        total_capital=settings.BASE_CAPITAL,
        max_daily_loss=settings.MAX_DAILY_LOSS,
        max_positions=settings.MAX_POSITIONS
    )
    
    # Trading Engine gets the full config dump for strategy parameters
    trading_engine = TradingEngine(market_client, settings.model_dump())
    
    adj_engine = AdjustmentEngine(delta_threshold=15.0) 

    # 4. Boot Supervisor
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

    # 5. Set Execution Mode based on Config
    if settings.ENVIRONMENT == "production_live":
        logger.warning("!!! ⚠️ RUNNING IN FULL_AUTO MODE - REAL TRADING ENABLED !!!")
        supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("Running in SEMI_AUTO Mode - Approvals Required")
        supervisor.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("Running in SHADOW Mode - Monitoring Only")
        supervisor.safety.execution_mode = ExecutionMode.SHADOW

    # 6. Signal Handling
    loop = asyncio.get_running_loop()
    resources = {
        "MarketClient": market_client,
        "WebsocketService": websocket_service,
        # "TradeExecutor": trade_executor # Executor relies on Redis, handled by container
    }
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda s=signame: asyncio.create_task(shutdown(s, loop, supervisor, resources))
        )

    # 7. Start Loop
    try:
        await supervisor.start()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.critical(f"Supervisor crashed: {e}", exc_info=True)
        # Attempt emergency cleanup
        await shutdown(signal.SIGTERM, loop, supervisor, resources)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # Handled by signal handler
