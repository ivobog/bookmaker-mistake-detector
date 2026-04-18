from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from bookmaker_detector_api.api import api_router
from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.workflow_logging import (
    reset_request_workflow_context,
    set_request_workflow_context,
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    if settings.use_postgres_stable_read_mode:
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


@app.middleware("http")
async def attach_request_trace_context(request: Request, call_next):
    request_trace_id = request.headers.get("X-Request-ID", "").strip() or uuid4().hex
    token = set_request_workflow_context(
        request_trace_id=request_trace_id,
        request_method=request.method,
        request_path=request.url.path,
    )
    request.state.request_trace_id = request_trace_id
    try:
        response = await call_next(request)
    finally:
        reset_request_workflow_context(token)
    response.headers["X-Request-ID"] = request_trace_id
    return response


app.include_router(api_router)


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "Bookmaker Mistake Detector API",
        "environment": settings.api_env,
    }
