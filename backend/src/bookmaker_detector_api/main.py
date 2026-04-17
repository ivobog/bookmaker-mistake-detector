from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from bookmaker_detector_api.api import api_router
from bookmaker_detector_api.config import settings

app = FastAPI(
    title="Bookmaker Mistake Detector API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
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
