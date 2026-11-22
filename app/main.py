from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .manager import BackendManager, BackendUnavailable
from .settings import (
    RESPONSE_DELAY_MAX_MS,
    STARTUP_DELAY_MAX_MS,
    Settings,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"


class SearchRequest(BaseModel):
    query: str = Field(default="", max_length=500)
    limit: int = Field(default=50, ge=1, le=200)
    ready_timeout_ms: int | None = Field(default=None, ge=100, le=60000)


class SettingsUpdate(BaseModel):
    startup_delay_ms: int | None = Field(default=None, ge=0, le=STARTUP_DELAY_MAX_MS)
    response_delay_ms: int | None = Field(default=None, ge=0, le=RESPONSE_DELAY_MAX_MS)
    default_ready_timeout_ms: int | None = Field(default=None, ge=100, le=60000)


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings.from_env()
    app_settings.validate()
    manager = BackendManager(app_settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        app_settings.ensure_runtime_config()
        yield
        await manager.close()

    app = FastAPI(title="Log Search Gateway", lifespan=lifespan)
    app.state.settings = app_settings
    app.state.manager = manager

    @app.middleware("http")
    async def no_store(request: Request, call_next):  # noqa: ANN001
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(BackendUnavailable)
    async def backend_unavailable_handler(
        _request: Request, exc: BackendUnavailable
    ) -> JSONResponse:
        status = 504 if exc.kind in ("ready_timeout", "startup_timeout") else 503
        return JSONResponse(
            status_code=status,
            content={
                "error": exc.kind,
                "detail": exc.detail,
            },
        )

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "service": "log-search-gateway"}

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        return await manager.snapshot()

    @app.get("/api/settings")
    async def get_settings() -> dict[str, Any]:
        return app_settings.public()

    @app.put("/api/settings")
    async def put_settings(update: SettingsUpdate) -> dict[str, Any]:
        if update.startup_delay_ms is not None:
            app_settings.startup_delay_ms = update.startup_delay_ms
        if update.response_delay_ms is not None:
            app_settings.response_delay_ms = update.response_delay_ms
        if update.default_ready_timeout_ms is not None:
            app_settings.default_ready_timeout_ms = update.default_ready_timeout_ms
        app_settings.save()
        return app_settings.public()

    @app.get("/api/search")
    async def search_get(
        q: str = Query(default=""),
        limit: int = Query(default=50, ge=1, le=200),
        ready_timeout_ms: int | None = Query(default=None, ge=100, le=60000),
    ) -> dict[str, Any]:
        return await manager.search(q, limit, ready_timeout_ms)

    @app.post("/api/search")
    async def search_post(payload: SearchRequest) -> dict[str, Any]:
        return await manager.search(
            payload.query, payload.limit, payload.ready_timeout_ms
        )

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


app = create_app()


__all__ = ["app", "create_app"]
