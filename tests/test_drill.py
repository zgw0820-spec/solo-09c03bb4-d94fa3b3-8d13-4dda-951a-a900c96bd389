from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from app.manager import BackendManager, DrillError
from app.main import create_app
from tests.conftest import make_settings

DRILL_LOG = Path(__file__).resolve().parent.parent / "data" / "drill_gateway_logs.jsonl"


def _stamps() -> list[int]:
    from datetime import datetime

    values = []
    for line in DRILL_LOG.read_text(encoding="utf-8").splitlines():
        if line.strip():
            ts = json.loads(line)["ts"]
            values.append(round(datetime.fromisoformat(ts).timestamp() * 1000))
    return values


@pytest.fixture
async def mgr(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    settings.drill_log_file = DRILL_LOG
    manager = BackendManager(settings)
    try:
        yield manager
    finally:
        await manager.close()


async def test_start_anchors_clock_at_first_line(mgr):
    snap = await mgr.drill_start()
    stamps = _stamps()
    assert snap["started"] is True
    assert snap["playing"] is True
    assert snap["first_at_ms"] == min(stamps)
    assert snap["last_at_ms"] == max(stamps)
    assert snap["now_ms"] >= snap["first_at_ms"]
    # The clock advances with real time.
    await asyncio.sleep(0.15)
    later = await mgr.drill_snapshot()
    assert later["now_ms"] - snap["now_ms"] >= 120


async def test_pause_and_resume(mgr):
    await mgr.drill_start()
    paused = await mgr.drill_control("pause")
    assert paused["playing"] is False
    frozen = paused["now_ms"]
    await asyncio.sleep(0.2)
    still = await mgr.drill_snapshot()
    assert still["now_ms"] == frozen

    resumed = await mgr.drill_control("resume")
    assert resumed["playing"] is True
    await asyncio.sleep(0.15)
    moving = await mgr.drill_snapshot()
    assert moving["now_ms"] - frozen >= 120


async def test_rewind_moves_clock_and_keeps_play_state(mgr):
    await mgr.drill_start()
    stamps = _stamps()
    await asyncio.sleep(0.5)
    await mgr.drill_control("pause")
    snap = await mgr.drill_snapshot()
    target = min(stamps) + 200
    assert target < snap["now_ms"]
    rewound = await mgr.drill_control("seek", target)
    assert rewound["now_ms"] == target
    assert rewound["playing"] is False
    await asyncio.sleep(0.15)
    assert (await mgr.drill_snapshot())["now_ms"] == target

    await mgr.drill_control("resume")
    await asyncio.sleep(0.15)
    assert (await mgr.drill_snapshot())["now_ms"] > target


async def test_seek_is_clamped_to_log_span(mgr):
    await mgr.drill_start()
    stamps = _stamps()
    before = await mgr.drill_control("seek", min(stamps) - 60_000)
    assert before["now_ms"] == min(stamps)
    after = await mgr.drill_control("seek", max(stamps) + 60_000)
    assert after["now_ms"] == max(stamps)


async def test_forward_seek_does_not_void_or_add_rewind(mgr):
    await mgr.drill_start()
    await mgr.drill_control("pause")
    snap = await mgr.drill_snapshot()
    target = snap["last_at_ms"]
    forward = await mgr.drill_control("seek", target)
    assert forward["now_ms"] == target
    assert forward["generation"] == snap["generation"]
    assert forward["voided"] is False
    assert forward["rewinds"] == []


async def test_rewind_voids_inflight_even_after_clock_passes_old_point(mgr):
    await mgr.drill_start()
    await asyncio.sleep(0.8)
    await mgr.drill_control("pause")
    at = (await mgr.drill_snapshot())["now_ms"]
    # Freeze the search inside the backend call with a slow response delay.
    mgr.settings.response_delay_ms = 600
    mgr.settings.write_runtime_config()
    task = asyncio.create_task(mgr.drill_search("timeout", limit=5))
    # Let acceptance happen and the call go in flight.
    for _ in range(100):
        if (await mgr.drill_snapshot())["in_flight"]:
            break
        await asyncio.sleep(0.01)
    assert (await mgr.drill_snapshot())["in_flight"] == 1

    # Rewind to the start, then play the clock forward past the accepted
    # instant again before the backend replies.  The old result must still be
    # voided even though the clock now shows a time after it.
    first = (await mgr.drill_snapshot())["first_at_ms"]
    assert first < at
    await mgr.drill_control("seek", first)
    await mgr.drill_control("resume")
    assert (await mgr.drill_snapshot())["now_ms"] < at
    await asyncio.sleep(max(0.0, (at - first) / 1000.0 + 0.05))
    after = await mgr.drill_snapshot()
    assert after["now_ms"] >= at

    with pytest.raises(DrillError) as exc:
        await task
    assert exc.value.kind == "drill_rewound"
    assert exc.value.status == 409
    assert after["voided"] is True
    assert after["rewinds"][-1]["voided"] == 1


async def test_pause_does_not_change_accepted_instant(mgr):
    """A search accepted while playing keeps its instant if the drill pauses."""
    await mgr.drill_start()
    mgr.settings.response_delay_ms = 500
    mgr.settings.write_runtime_config()
    task = asyncio.create_task(mgr.drill_search("gateway", limit=50))
    for _ in range(100):
        if (await mgr.drill_snapshot())["in_flight"]:
            break
        await asyncio.sleep(0.01)
    await mgr.drill_control("pause")
    result = await task
    accepted = result["drill"]["accepted_at_ms"]
    # Results only include lines at or before the accepted instant.
    for row in result["results"]:
        assert row["ts_ms"] <= accepted
    assert result["at_ms"] == accepted


async def test_drill_search_only_returns_log_up_to_instant(mgr):
    await mgr.drill_start()
    await mgr.drill_control("pause")
    stamps = _stamps()
    # Pick an instant that lands between the first timeouts and the breaker.
    target = stamps[4]  # 5th line
    await mgr.drill_control("seek", target)
    payload = await mgr.drill_search("gateway", limit=200)
    assert payload["drill"]["accepted_at_ms"] == target
    visible = [row for row in payload["results"] if row.get("ts_ms") is not None]
    assert visible
    assert max(row["ts_ms"] for row in visible) <= target
    texts = " ".join(row["raw"] for row in payload["results"])
    assert "circuit breaker OPEN" not in texts
    assert "upstream timeout" in texts

    # Moving forward (still paused) reveals the breaker lines.
    await mgr.drill_control("seek", stamps[7])
    later = await mgr.drill_search("gateway", limit=200)
    later_texts = " ".join(row["raw"] for row in later["results"])
    assert "circuit breaker OPEN" in later_texts
    # ...while recovery is still in the future.
    assert "recovery:" not in later_texts


async def test_drill_and_normal_search_share_child(mgr):
    normal = await mgr.search("timeout", limit=3)
    await mgr.drill_start()
    drill = await mgr.drill_search("timeout", limit=3)
    assert normal["backend_pid"] == drill["backend_pid"]
    snap = await mgr.snapshot()
    assert snap["counters"]["starts"] == 1


async def test_drill_clock_survives_idle_reap(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0, idle_timeout_s=0.3)
    settings.drill_log_file = DRILL_LOG
    manager = BackendManager(settings)
    try:
        await manager.drill_start()
        await manager.drill_control("pause")
        frozen = (await manager.drill_snapshot())["now_ms"]
        # A drill search wakes the child, then idle reaping returns to stopped.
        await manager.drill_search("timeout", limit=2)
        pid = (await manager.snapshot())["backend_pid"]
        for _ in range(100):
            if (await manager.snapshot())["state"] == "stopped":
                break
            await asyncio.sleep(0.05)
        snap = await manager.snapshot()
        assert snap["state"] == "stopped"
        assert snap["backend_pid"] is None
        assert (await manager.drill_snapshot())["now_ms"] == frozen
        assert (await manager.drill_snapshot())["started"] is True

        # Resuming after the reap keeps counting from the frozen point.
        await manager.drill_control("resume")
        await asyncio.sleep(0.15)
        assert (await manager.drill_snapshot())["now_ms"] > frozen

        # And a normal search still works after the drill clock kept running.
        await manager.drill_control("pause")
        again = await manager.search("timeout", limit=1)
        assert again["backend_pid"] != pid
        assert (await manager.drill_snapshot())["started"] is True
    finally:
        await manager.close()


async def test_drill_search_before_start_is_rejected(mgr):
    with pytest.raises(DrillError) as exc:
        await mgr.drill_search("timeout")
    assert exc.value.kind == "drill_not_started"


# --------------------------------------------------------------------- HTTP
@pytest.fixture
async def api(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0, idle_timeout_s=0.5)
    settings.drill_log_file = DRILL_LOG
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as http:
            yield http


async def test_http_drill_flow(api):
    assert (await api.get("/api/drill")).json()["started"] is False
    assert (await api.post("/api/drill/control", json={"action": "pause"})).status_code == 409
    started = await api.post("/api/drill/start")
    assert started.status_code == 200
    assert started.json()["playing"] is True

    # The drill log spans 30s: let the clock advance before pausing so a seek
    # back to +5s is a real rewind.
    await asyncio.sleep(1.2)
    paused = await api.post("/api/drill/control", json={"action": "pause"})
    assert paused.json()["playing"] is False
    now_ms = paused.json()["now_ms"]

    found = await api.post("/api/drill/search", json={"query": "timeout", "limit": 10})
    assert found.status_code == 200
    body = found.json()
    assert body["drill"]["accepted_at_ms"] == now_ms
    assert body["drill"]["accepted_at"]
    assert all(row["ts_ms"] <= now_ms for row in body["results"])

    target = paused.json()["first_at_ms"] + 500
    rewound = await api.post(
        "/api/drill/control", json={"action": "seek", "at_ms": target}
    )
    assert rewound.status_code == 200
    assert rewound.json()["now_ms"] == target
    assert rewound.json()["rewinds"][-1]["to_ms"] == target

    # Status carries the drill snapshot for the page to render.
    status = (await api.get("/api/status")).json()
    assert status["drill"]["started"] is True
    assert status["drill"]["now_ms"] == target


async def test_http_rewind_voids_inflight(api):
    await api.post("/api/drill/start")
    await api.post("/api/drill/control", json={"action": "pause"})
    snap = (await api.get("/api/drill")).json()
    await api.put("/api/settings", json={"response_delay_ms": 700})
    task = asyncio.create_task(
        api.post("/api/drill/search", json={"query": "timeout", "limit": 10})
    )
    for _ in range(100):
        if (await api.get("/api/drill")).json()["in_flight"]:
            break
        await asyncio.sleep(0.01)
    await api.post(
        "/api/drill/control", json={"action": "seek", "at_ms": snap["first_at_ms"]}
    )
    resp = await task
    assert resp.status_code == 409
    assert resp.json()["error"] == "drill_rewound"
    await api.put("/api/settings", json={"response_delay_ms": 0})


async def test_http_control_validation(api):
    await api.post("/api/drill/start")
    assert (await api.post("/api/drill/control", json={"action": "nope"})).status_code == 422
    seek_without_at = await api.post("/api/drill/control", json={"action": "seek"})
    assert seek_without_at.status_code == 400
    assert seek_without_at.json()["error"] == "bad_request"
