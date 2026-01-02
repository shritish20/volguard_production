"""
Production entry point.
"""
import asyncio
import logging
from pathlib import Path
import sys

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.core.market.data_client import MarketDataClient
from app.core.risk.engine import RiskEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.trading.executor import TradeExecutor
from app.services.websocket_client import GreekWebSocket
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/production_supervisor.log')
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Start production supervisor"""
    logger.info("=" * 60)
    logger.info("🚀 VOLGUARD PRODUCTION TRADING SUPERVISOR")
    logger.info("=" * 60)
    
    try:
        # Initialize components
        logger.info("Initializing components...")
        
        market_client = MarketDataClient(
            access_token=settings.UPSTOX_ACCESS_TOKEN,
            base_url_v2=settings.UPSTOX_BASE_V2,
            base_url_v3=settings.UPSTOX_BASE_V3
        )
        
        risk_engine = RiskEngine(settings.dict())
        adjustment_engine = AdjustmentEngine(settings.dict())
        trade_executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
        
        # Initialize WebSocket
        websocket_service = None
        if settings.SUPERVISOR_WEBSOCKET_ENABLED:
            instrument_keys = await market_client.get_active_option_instruments()
            websocket_service = GreekWebSocket(
                access_token=settings.UPSTOX_ACCESS_TOKEN,
                instrument_keys=instrument_keys[:settings.WEBSOCKET_MAX_INSTRUMENTS]
            )
        
        # Create supervisor
        supervisor = ProductionTradingSupervisor(
            market_client=market_client,
            risk_engine=risk_engine,
            adjustment_engine=adjustment_engine,
            trade_executor=trade_executor,
            websocket_service=websocket_service,
            total_capital=settings.BASE_CAPITAL,
            loop_interval_seconds=settings.SUPERVISOR_LOOP_INTERVAL
        )
        
        logger.info("✅ All components initialized")
        logger.info(f"📊 Starting with execution mode: {supervisor.safety.execution_mode.value}")
        logger.info(f"⏱️  Loop interval: {settings.SUPERVISOR_LOOP_INTERVAL} seconds")
        logger.info(f"💰 Base capital: ₹{settings.BASE_CAPITAL:,}")
        
        # Start supervisor
        await supervisor.start()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
