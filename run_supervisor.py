import asyncio
import logging
import signal
import sys

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
from app.utils.logging import setup_logging

# Initialize Structured Logging
logger = setup_logging()

async def shutdown(signal_name, loop, supervisor):
    """Graceful Shutdown Handler"""
    logger.info(f"Received exit signal {signal_name.name}...")
    if supervisor:
        logger.info("Stopping Supervisor loop...")
        supervisor.running = False
        # Allow time for the loop to finish its current cycle
        await asyncio.sleep(2) 
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main():
    logger.info(f"Starting VolGuard Supervisor [Env: {settings.ENVIRONMENT}]")
    
    # 1. Database Initialization
    logger.info("Connecting to Database...")
    await init_db()

    # 2. Market Data Client
    logger.info("Initializing Market Client...")
    market = MarketDataClient(
        settings.UPSTOX_ACCESS_TOKEN,
        settings.UPSTOX_BASE_V2,
        settings.UPSTOX_BASE_V3
    )

    # 3. WebSocket Service (Optional but Recommended)
    ws = None
    if settings.SUPERVISOR_WEBSOCKET_ENABLED:
        logger.info("Initializing WebSocket Feed...")
        ws = UpstoxFeedService(settings.UPSTOX_ACCESS_TOKEN)

    # 4. Core Engines Setup
    logger.info("Booting Risk & Trading Engines...")
    config_dict = settings.model_dump()
    
    risk_engine = RiskEngine(config_dict)
    adj_engine = AdjustmentEngine(config_dict)
    trading_engine = TradingEngine(market, config_dict)
    trade_executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)

    # 5. Initialize The Supervisor
    supervisor = ProductionTradingSupervisor(
        market_client=market,
        risk_engine=risk_engine,
        adjustment_engine=adj_engine,
        trade_executor=trade_executor,
        trading_engine=trading_engine,
        websocket_service=ws,
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL,
        total_capital=settings.BASE_CAPITAL
    )

    # 6. Mode Configuration
    # Strict safety defaults: We force SHADOW unless environment explicitly overrides
    if settings.ENVIRONMENT == "production_live":
        logger.warning("!!! RUNNING IN FULL_AUTO MODE - REAL TRADING ENABLED !!!")
        supervisor.safety.execution_mode = ExecutionMode.FULL_AUTO
    elif settings.ENVIRONMENT == "production_semi":
        logger.info("Running in SEMI_AUTO Mode - Approvals Required")
        supervisor.safety.execution_mode = ExecutionMode.SEMI_AUTO
    else:
        logger.info("Running in SHADOW Mode - Monitoring Only")
        supervisor.safety.execution_mode = ExecutionMode.SHADOW

    # 7. Signal Handling for Graceful Exit (Docker/Ctrl+C)
    loop = asyncio.get_running_loop()
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda s=signame: asyncio.create_task(shutdown(s, loop, supervisor))
        )

    # 8. Start the Main Loop
    try:
        await supervisor.start()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as e:
        logger.critical(f"Supervisor crashed: {e}", exc_info=True)
    finally:
        logger.info("Cleaning up resources...")
        await market.close()
        if ws: await ws.disconnect()
        logger.info("VolGuard Supervisor Shutdown Complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass # Handled by signal handler above
