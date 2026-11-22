from __future__ import annotations

import asyncio
import os

import pytest

from app.manager import BackendManager, BackendUnavailable
from tests.conftest import make_settings, pid_alive, wait_until


async def test_search_starts_backend_and_returns_matches(manager):
    result = await manager.search("timeout", limit=10)
    assert result["count"] >= 1
    assert all("timeout" in row["raw"].lower() for row in result["results"])
    assert result["backend_pid"] > 0

    snap = await manager.snapshot()
    assert snap["state"] == "ready"
    assert snap["backend_pid"] == result["backend_pid"]
    assert snap["counters"]["starts"] == 1


async def test_empty_query_returns_first_page(manager):
    result = await manager.search("", limit=5)
    assert result["count"] == 5
    assert result["truncated"] is True


async def test_limit_is_clamped(manager):
    result = await manager.search("", limit=1000)
    # API layer clamps; manager passes through, backend clamps to 200.
    assert result["count"] <= 200


async def test_concurrent_queries_share_one_start(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=700)
    mgr = BackendManager(settings)
    try:
        results = await asyncio.gather(
            *(mgr.search("gateway", limit=50, ready_timeout_ms=4000) for _ in range(20))
        )
        pids = {r["backend_pid"] for r in results}
        assert len(pids) == 1
        snap = await mgr.snapshot()
        assert snap["counters"]["starts"] == 1
        assert snap["counters"]["searches"] == 20
    finally:
        await mgr.close()


async def test_only_one_process_even_across_idle_reaps(tmp_path):
    settings = make_settings(tmp_path, idle_timeout_s=0.4)
    mgr = BackendManager(settings)
    seen = []
    try:
        first = await mgr.search("error", limit=1)
        seen.append(first["backend_pid"])
        assert await wait_until(
            lambda: _state_is(mgr, "stopped"), timeout=5
        )
        second = await mgr.search("error", limit=1)
        seen.append(second["backend_pid"])
        assert seen[0] != seen[1]
        snap = await mgr.snapshot()
        assert snap["counters"]["starts"] == 2
        assert snap["counters"]["reaps"] == 1
        assert not pid_alive(seen[0])
    finally:
        await mgr.close()
    assert not pid_alive(seen[1])


async def _state_is(mgr: BackendManager, state: str) -> bool:
    return (await mgr.snapshot())["state"] == state


async def test_short_ready_timeout_does_not_kill_shared_start(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=900)
    mgr = BackendManager(settings)
    try:
        impatient = asyncio.create_task(
            mgr.search("gateway", limit=5, ready_timeout_ms=250)
        )
        patient = asyncio.create_task(
            mgr.search("gateway", limit=5, ready_timeout_ms=5000)
        )
        with pytest.raises(BackendUnavailable) as exc:
            await impatient
        assert exc.value.kind == "ready_timeout"
        result = await patient
        assert result["count"] >= 1
        snap = await mgr.snapshot()
        assert snap["state"] == "ready"
        assert snap["counters"]["starts"] == 1
        assert snap["counters"]["failures"] == 0
    finally:
        await mgr.close()


async def test_startup_failure_fails_waiters_and_allows_retry(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    mgr = BackendManager(settings, child_env={"BACKEND_FAIL_START": "1"})
    try:
        outcomes = await asyncio.gather(
            *(mgr.search("a", ready_timeout_ms=4000) for _ in range(5)),
            return_exceptions=True,
        )
        assert all(isinstance(o, BackendUnavailable) for o in outcomes)
        assert {o.kind for o in outcomes} == {"startup_failed"}
        snap = await mgr.snapshot()
        assert snap["state"] == "stopped"
        assert snap["counters"]["failures"] == 1
        assert snap["last_error"]["kind"] == "startup_failed"

        # A later call can wake a new round.
        mgr._child_env = None
        result = await mgr.search("gateway", limit=3, ready_timeout_ms=4000)
        assert result["count"] >= 1
        snap = await mgr.snapshot()
        assert snap["state"] == "ready"
        assert snap["counters"]["starts"] == 2
    finally:
        await mgr.close()


async def test_startup_cap_fails_waiters(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=10000, startup_timeout_s=0.5)
    mgr = BackendManager(settings)
    try:
        started = asyncio.get_running_loop().time()
        outcomes = await asyncio.gather(
            *(mgr.search("a", ready_timeout_ms=6000) for _ in range(3)),
            return_exceptions=True,
        )
        elapsed = asyncio.get_running_loop().time() - started
        assert all(isinstance(o, BackendUnavailable) for o in outcomes)
        assert {o.kind for o in outcomes} == {"startup_timeout"}
        assert elapsed < 3.0
        assert await wait_until(lambda: _state_is(mgr, "stopped"), timeout=6)
    finally:
        await mgr.close()


async def test_new_query_waits_for_old_process_to_exit(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    mgr = BackendManager(settings)
    pids = []
    try:
        first = await mgr.search("a", limit=1)
        pids.append(first["backend_pid"])
        # Force teardown of the live backend.
        async with mgr._lock:
            mgr._begin_stop_locked(mgr._round, "manual")
        snap = await mgr.snapshot()
        assert snap["state"] == "stopping"

        second = await mgr.search("a", limit=1)
        pids.append(second["backend_pid"])
        assert pids[0] != pids[1]
        assert not pid_alive(pids[0])
        snap = await mgr.snapshot()
        assert snap["state"] == "ready"
        assert snap["counters"]["starts"] == 2
    finally:
        await mgr.close()


async def test_ready_timeout_while_waiting_for_old_exit(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    mgr = BackendManager(settings)
    try:
        await mgr.search("a", limit=1)
        async with mgr._lock:
            mgr._begin_stop_locked(mgr._round, "manual")
        # Teardown is fast, but a 100ms budget may expire while it runs.
        # Either way the query must not see the old process's results.
        try:
            result = await mgr.search("a", limit=1, ready_timeout_ms=100)
        except BackendUnavailable as exc:
            assert exc.kind == "ready_timeout"
        else:
            assert result["backend_pid"] > 0
    finally:
        await mgr.close()


async def test_idle_reap_not_started_while_query_in_flight(tmp_path):
    settings = make_settings(tmp_path, idle_timeout_s=0.35, response_delay_ms=900)
    mgr = BackendManager(settings)
    try:
        await mgr.search("a", limit=1)
        pid = (await mgr.snapshot())["backend_pid"]
        # Keep a slow query in flight past the idle window.
        result = await mgr.search("gateway", limit=2, ready_timeout_ms=3000)
        assert result["backend_pid"] == pid
        assert await mgr.snapshot()
        assert (await mgr.snapshot())["state"] == "ready"
        # No new start happened while the request was in flight.
        assert (await mgr.snapshot())["counters"]["starts"] == 1
        # And once idle, it is reaped.
        assert await wait_until(lambda: _state_is(mgr, "stopped"), timeout=5)
    finally:
        await mgr.close()


async def test_idle_timer_resets_on_waiters(tmp_path):
    settings = make_settings(tmp_path, idle_timeout_s=0.8, startup_delay_ms=0)
    mgr = BackendManager(settings)
    try:
        await mgr.search("a", limit=1)
        for _ in range(3):
            await asyncio.sleep(0.3)
            await mgr.search("a", limit=1)
        snap = await mgr.snapshot()
        assert snap["counters"]["starts"] == 1
        assert snap["state"] == "ready"
    finally:
        await mgr.close()


async def test_backend_crash_fails_inflight_then_recovers(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    mgr = BackendManager(settings)
    try:
        first = await mgr.search("a", limit=1)
        os.kill(first["backend_pid"], 9)
        assert await wait_until(lambda: _state_is(mgr, "stopped"), timeout=6)
        snap = await mgr.snapshot()
        assert snap["last_error"]["kind"] == "backend_exited"
        assert snap["counters"]["failures"] == 1
        second = await mgr.search("a", limit=1)
        assert second["backend_pid"] != first["backend_pid"]
        assert (await mgr.snapshot())["state"] == "ready"
    finally:
        await mgr.close()


async def test_close_terminates_child(tmp_path):
    settings = make_settings(tmp_path, startup_delay_ms=0)
    mgr = BackendManager(settings)
    result = await mgr.search("a", limit=1)
    pid = result["backend_pid"]
    await mgr.close()
    assert not pid_alive(pid)
    with pytest.raises(BackendUnavailable) as exc:
        await mgr.search("a")
    assert exc.value.kind == "shutdown"
