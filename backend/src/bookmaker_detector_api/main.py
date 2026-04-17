from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bookmaker_detector_api.api import api_router
from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    if settings.api_env.strip().lower() == "production":
        with postgres_connection():
            pass
    yield


app = FastAPI(
    title="Bookmaker Mistake Detector API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api_cors_origin_list,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "Bookmaker Mistake Detector API",
        "environment": settings.api_env,
    }
