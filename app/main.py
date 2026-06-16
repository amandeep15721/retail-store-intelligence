from __future__ import annotations
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import init_db
from app.ingestion import router as ingestion_router
from app.metrics import router as metrics_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app.
    This is where you can add startup and shutdown events for the app.
    """
    # Startup code here
    await init_db()  # Initialize the database connection
    
    yield
    # Shutdown code here
    # nothing to clean up yet (SQLite connections are closed per-request)


app = FastAPI(
    title="Store Intelligence API",
    description="Real time retail-store analytics derived from CCTV events",
    version="0.1.0",
)


app.include_router(ingestion_router)
app.include_router(metrics_router)

@app.get("/")
async def root() -> dict:
    """Simple liveness check -- confirms the app booted and DB init ran."""
    return {"service": "store-intelligence api", "status": "ok"}