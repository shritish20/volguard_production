# app/main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
from prometheus_client import make_asgi_app

from app.config import settings
from app.database import engine, init_db
from app.api.v1.router import router as api_router

# 🔑 AUTHORITATIVE INSTRUMENT REGISTRY
from app.services.instrument_registry import registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager for startup/shutdown.
    This runs ONCE per worker.
    """

    # --------------------------------------------------
    # STARTUP
    # --------------------------------------------------
    logger.info("🚀 Starting VolGuard API...")

    # 1️⃣ Initialize Database
    await init_db()
    logger.info("✅ Database initialized")

    # 2️⃣ Load Upstox Instrument Master (AUTHORITATIVE)
    registry.load_master(force_refresh=False)
    logger.info("✅ Instrument master loaded into registry")

    yield  # 👈 Application is now LIVE

    # --------------------------------------------------
    # SHUTDOWN
    # --------------------------------------------------
    logger.info("🛑 Shutting down VolGuard API...")
    await engine.dispose()
    logger.info("✅ Database engine disposed")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        lifespan=lifespan,
    )

    # --------------------------------------------------
    # MIDDLEWARE
    # --------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[""],  # tighten in prod
        allow_credentials=True,
        allow_methods=[""],
        allow_headers=[""],
    )

    # --------------------------------------------------
    # ROUTERS
    # --------------------------------------------------
    app.include_router(api_router, prefix=settings.API_V1_STR)

    # --------------------------------------------------
    # PROMETHEUS METRICS
    # --------------------------------------------------
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # --------------------------------------------------
    # HEALTH CHECK
    # --------------------------------------------------
    @app.get("/health")
    async def health_check():
        return {
            "status": "healthy",
            "env": settings.ENVIRONMENT,
            "instrument_master_loaded": not registry.master_df.empty,
        }

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
