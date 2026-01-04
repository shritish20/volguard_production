from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
from prometheus_client import make_asgi_app

from app.config import settings
from app.database import engine, init_db
from app.api.v1.router import router as api_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for startup/shutdown"""
    logger.info("Starting VolGuard API...")

    # Initialize DB (Creates tables if missing)
    await init_db()

    yield

    logger.info("Shutting down VolGuard API...")
    await engine.dispose()

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        lifespan=lifespan
    )

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[""],  # Tighten this for real production
        allow_credentials=True,
        allow_methods=[""],
        allow_headers=[""],
    )

    # Routers
    app.include_router(api_router, prefix=settings.API_V1_STR)

    # Prometheus metrics endpoint
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "env": "production"}

    return app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
