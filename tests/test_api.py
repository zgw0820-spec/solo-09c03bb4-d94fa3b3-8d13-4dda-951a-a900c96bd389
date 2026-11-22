from __future__ import annotations

import asyncio

import httpx
import pytest

from app.main import create_app
from tests.conftest import make_settings


@pytest.fixture
async def client(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0, idle_timeout_s=0.5)
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as http:
            yield http, settings


async def test_health_and_index(client):
    http, _ = client
    assert (await http.get("/api/health")).json()["ok"] is True
    page = await http.get("/")
    assert page.status_code == 200
    assert "故障日志检索" in page.text
    js = await http.get("/static/app.js")
    assert js.status_code == 200


async def test_search_get_and_post(client):
    http, _ = client
    get_resp = await http.get("/api/search", params={"q": "timeout", "limit": 3})
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["count"] >= 1
    assert all("timeout" in row["raw"].lower() for row in body["results"])

    post_resp = await http.post(
        "/api/search",
        json={"query": "ERROR", "limit": 5, "ready_timeout_ms": 3000},
    )
    assert post_resp.status_code == 200
    assert post_resp.json()["count"] >= 1
    assert post_resp.headers["cache-control"] == "no-store"


async def test_validation_errors(client):
    http, _ = client
    assert (await http.post("/api/search", json={"limit": 0})).status_code == 422
    assert (await http.post("/api/search", json={"limit": 201})).status_code == 422
    resp = await http.post("/api/search", json={"ready_timeout_ms": 10})
    assert resp.status_code == 422


async def test_status_reports_lifecycle(client):
    http, _ = client
    assert (await http.get("/api/status")).json()["state"] == "stopped"
    await http.get("/api/search", params={"q": "gateway", "limit": 2})
    status = (await http.get("/api/status")).json()
    assert status["state"] == "ready"
    assert status["backend_pid"]
    assert status["counters"]["starts"] == 1
    # Idle reaping is visible through the status API.
    for _ in range(60):
        await asyncio.sleep(0.1)
        if (await http.get("/api/status")).json()["state"] == "stopped":
            break
    status = (await http.get("/api/status")).json()
    assert status["state"] == "stopped"
    assert status["counters"]["reaps"] == 1


async def test_timeout_is_504_with_kind(client):
    http, settings = client
    settings.startup_delay_ms = 1500
    resp = await http.get(
        "/api/search", params={"q": "a", "ready_timeout_ms": 200}
    )
    assert resp.status_code == 504
    assert resp.json()["error"] == "ready_timeout"
    # The shared start is unaffected; a patient request still succeeds.
    resp = await http.get(
        "/api/search", params={"q": "a", "ready_timeout_ms": 5000}
    )
    assert resp.status_code == 200


async def test_settings_persist_and_apply_to_running_backend(client):
    http, settings = client
    resp = await http.put(
        "/api/settings",
        json={"response_delay_ms": 120, "startup_delay_ms": 250},
    )
    assert resp.status_code == 200
    assert resp.json()["response_delay_ms"] == 120
    assert settings.config_file.exists()
    assert (await http.get("/api/settings")).json()["startup_delay_ms"] == 250

    await http.get("/api/search", params={"q": "a", "limit": 1})
    loop = asyncio.get_running_loop()
    started = loop.time()
    await http.get("/api/search", params={"q": "a", "limit": 1})
    assert loop.time() - started >= 0.1


async def test_settings_bounds(client):
    http, _ = client
    assert (
        await http.put("/api/settings", json={"startup_delay_ms": 99999})
    ).status_code == 422
    assert (
        await http.put("/api/settings", json={"response_delay_ms": -1})
    ).status_code == 422


async def test_backend_failure_surfaces_as_503(tmp_path):
    settings = make_settings(tmp_path)
    app = create_app(settings)
    app.state.manager._child_env = {"BACKEND_FAIL_START": "1"}
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as http:
            resp = await http.get("/api/search", params={"q": "a"})
            assert resp.status_code == 503
            assert resp.json()["error"] == "startup_failed"
            status = (await http.get("/api/status")).json()
            assert status["state"] == "stopped"
            assert status["last_error"]["kind"] == "startup_failed"
