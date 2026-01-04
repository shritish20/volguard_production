# run_production.py

import asyncio
import logging
import os
from dotenv import load_dotenv

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

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("volguard_production.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VolGuardMain")

async def main():
    # 1. Load Config
    load_dotenv()
    ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
    REFRESH_TOKEN = os.getenv("UPSTOX_REFRESH_TOKEN") # Optional
    CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
    CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
    
    if not ACCESS_TOKEN:
        logger.critical("No Access Token found in .env")
        return

    # 2. Auth & Token Check
    auth = TokenManager(ACCESS_TOKEN, REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET)
    if not auth.validate_token():
        logger.critical("Authentication Failed. Exiting.")
        return
    
    valid_token = auth.get_token()

    # 3. Load Master Registry (The Map)
    try:
        registry.load_master()
    except Exception as e:
        logger.critical(f"Registry Load Failed: {e}")
        return

    # 4. Initialize Core Systems
    logger.info("Initializing VolGuard 3.0 Cores...")
    
    # Clients
    market_client = MarketDataClient(valid_token)
    ws_client = MarketDataFeed(valid_token, ["NSE_INDEX|Nifty 50", "NSE_INDEX|India VIX"])
    
    # Engines
    risk_engine = RiskEngine(max_portfolio_loss=50000)
    cap_governor = CapitalGovernor(valid_token, total_capital=200000) # Fetches real funds dynamically
    executor = TradeExecutor(valid_token)
    
    # Trading Engine (Passes Client & Config)
    trading_engine = TradingEngine(market_client, {"DEFAULT_LOT_SIZE": 50})
    
    # Adjustment Engine
    adj_engine = AdjustmentEngine(delta_threshold=15.0)

    # 5. Boot Supervisor
    supervisor = ProductionTradingSupervisor(
        market_client=market_client,
        risk_engine=risk_engine,
        adjustment_engine=adj_engine,
        trade_executor=executor,
        trading_engine=trading_engine,
        capital_governor=cap_governor,
        websocket_service=ws_client
    )

    try:
        logger.info(">>> STARTING SUPERVISOR <<<")
        await supervisor.start()
    except KeyboardInterrupt:
        logger.info("Shutdown Signal Received.")
    except Exception as e:
        logger.critical(f"Fatal Crash: {e}")
    finally:
        # Cleanup
        await market_client.close()
        await cap_governor.close()
        await executor.close()
        logger.info("VolGuard Shutdown Complete.")

if __name__ == "__main__":
    asyncio.run(main())
