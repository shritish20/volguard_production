# run_production.py

import asyncio
import logging
import os
from dotenv import load_dotenv

# Import Config (Single Source of Truth)
from app.config import settings

# Import Core Components
from app.core.auth.token_manager import TokenManager
from app.services.instrument_registry import registry
from app.core.market.data_client import MarketDataClient
from app.core.market.websocket_client import MarketDataFeed
from app.core.risk.engine import RiskEngine
from app.core.risk.capital_governor import CapitalGovernor
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.lifecycle.supervisor import ProductionTradingSupervisor

# Setup Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/volguard_production.log"), # Log to folder
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VolGuardMain")

async def main():
    logger.info(f"🚀 Booting VolGuard {settings.VERSION} [{settings.ENVIRONMENT}]")

    # 1. Auth & Token Check
    # We use settings credentials, but TokenManager handles the logic
    auth = TokenManager(
        access_token=settings.UPSTOX_ACCESS_TOKEN,
        refresh_token=settings.UPSTOX_REFRESH_TOKEN,
        client_id=settings.UPSTOX_CLIENT_ID,
        client_secret=settings.UPSTOX_CLIENT_SECRET
    )
    
    if not auth.validate_token():
        logger.critical("❌ Authentication Failed. Check .env credentials.")
        return
    
    valid_token = auth.get_token()
    logger.info("✅ Authentication Successful")

    # 2. Load Master Registry (The Map)
    try:
        registry.load_master()
    except Exception as e:
        logger.critical(f"❌ Registry Load Failed: {e}")
        return

    # 3. Initialize Core Systems (Using Config for Limits)
    logger.info("⚙️  Initializing VolGuard Cores...")
    
    # Clients
    market_client = MarketDataClient(valid_token)
    ws_client = MarketDataFeed(valid_token, ["NSE_INDEX|Nifty 50", "NSE_INDEX|India VIX"])
    
    # Engines (Powered by Settings)
    risk_engine = RiskEngine(
        max_portfolio_loss=settings.MAX_DAILY_LOSS
    )
    
    cap_governor = CapitalGovernor(
        valid_token, 
        total_capital=settings.BASE_CAPITAL,
        max_daily_loss=settings.MAX_DAILY_LOSS,
        max_positions=settings.MAX_POSITIONS
    )
    
    executor = TradeExecutor(valid_token)
    
    # Trading Engine gets the full config dict for deeper settings
    trading_engine = TradingEngine(
        market_client, 
        settings.model_dump()
    )
    
    # Adjustment Engine (Thresholds from config if available, else smart defaults)
    adj_engine = AdjustmentEngine(delta_threshold=15.0)

    # 4. Boot Supervisor
    supervisor = ProductionTradingSupervisor(
        market_client=market_client,
        risk_engine=risk_engine,
        adjustment_engine=adj_engine,
        trade_executor=executor,
        trading_engine=trading_engine,
        capital_governor=cap_governor,
        websocket_service=ws_client,
        loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL
    )

    try:
        logger.info(">>> STARTING SUPERVISOR LOOP <<<")
        await supervisor.start()
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown Signal Received.")
    except Exception as e:
        logger.critical(f"☠️  Fatal Crash: {e}", exc_info=True)
    finally:
        # Graceful Cleanup
        logger.info("Cleaning up resources...")
        await market_client.close()
        await cap_governor.close()
        await executor.close()
        logger.info("✅ VolGuard Shutdown Complete.")

if __name__ == "__main__":
    asyncio.run(main())
