import asyncio
import logging
from app.lifecycle.supervisor import ProductionTradingSupervisor
from app.core.market.data_client import MarketDataClient
from app.core.risk.engine import RiskEngine
from app.core.trading.adjustment_engine import AdjustmentEngine
from app.core.trading.executor import TradeExecutor
from app.core.trading.engine import TradingEngine
from app.core.market.websocket_client import UpstoxFeedService
from app.config import settings

logging.basicConfig(level=logging.INFO)

async def main():
    market = MarketDataClient(settings.UPSTOX_ACCESS_TOKEN, settings.UPSTOX_BASE_V2, settings.UPSTOX_BASE_V3)
    risk = RiskEngine(settings.model_dump())
    adj = AdjustmentEngine(settings.model_dump())
    executor = TradeExecutor(settings.UPSTOX_ACCESS_TOKEN)
    engine = TradingEngine(market, settings.model_dump())
    ws = UpstoxFeedService(settings.UPSTOX_ACCESS_TOKEN) if settings.SUPERVISOR_WEBSOCKET_ENABLED else None

    sup = ProductionTradingSupervisor(market, risk, adj, executor, engine, ws)
    await sup.start()

if __name__ == "__main__":
    asyncio.run(main())
