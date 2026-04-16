from fastapi import FastAPI

from bookmaker_detector_api.api import api_router
from bookmaker_detector_api.config import settings

app = FastAPI(
    title="Bookmaker Mistake Detector API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)
app.include_router(api_router)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "Bookmaker Mistake Detector API",
        "environment": settings.api_env,
    }

