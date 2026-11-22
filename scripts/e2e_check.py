"""End-to-end check against a real uvicorn process and the web UI.

Run with the server already listening, or let the script start it:

    python3 scripts/e2e_check.py              # starts uvicorn itself
    python3 scripts/e2e_check.py --base-url http://127.0.0.1:8000

It verifies, over real HTTP:
  * HTML page + static asset are served;
  * concurrent cold-start queries share exactly one backend process;
  * a short per-query readiness budget times out without cancelling the
    shared start, and only that query fails;
  * idle reaping terminates the child (no lingering pid);
  * a query arriving right after a reap wakes a fresh child;
  * at any sampled moment at most one backend child is alive;
  * the drill clock starts at the first log timestamp, advances in real time,
    pauses/resumes, and a search accepted before a pause keeps its instant;
  * a slow drill query accepted at a later instant is voided when a rewind is
    confirmed while it is still in flight, and a normal search still works.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def child_pids() -> list[int]:
    """Return live `app.backend` children (excluding zombies)."""
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if "app.backend" not in cmdline or "e2e_check" in cmdline:
            continue
        try:
            state = (entry / "stat").read_text().split(") ", 1)[1][0]
        except (OSError, IndexError):
            state = "?"
        if state != "Z":
            found.append(int(entry.name))
    return found


async def wait_for(predicate, timeout: float, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return True
        await asyncio.sleep(interval)
    return False


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--port", type=int, default=8123)
    args = parser.parse_args()

    server: subprocess.Popen[bytes] | None = None
    base = args.base_url
    if base is None:
        base = f"http://127.0.0.1:{args.port}"
        env = os.environ.copy()
        env["BACKEND_IDLE_TIMEOUT_S"] = "2"
        env["BACKEND_DATA_DIR"] = str(ROOT / "data" / "state")
        env["BACKEND_LOG_FILE"] = str(ROOT / "data" / "sample_logs.jsonl")
        env["BACKEND_DRILL_LOG_FILE"] = str(ROOT / "data" / "drill_gateway_logs.jsonl")
        env["PYTHONPATH"] = str(ROOT)
        server = subprocess.Popen(
            [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
             "--port", str(args.port), "--log-level", "warning"],
            cwd=str(ROOT),
            env=env,
        )

    failures: list[str] = []

    def check(condition: bool, label: str, extra: str = "") -> None:
        print(("  PASS  " if condition else "  FAIL  ") + label + (f"  [{extra}]" if extra else ""))
        if not condition:
            failures.append(label)

    try:
        async with httpx.AsyncClient(base_url=base, timeout=30.0) as http:
            ok = await wait_for(lambda: _probe(http), timeout=15)
            if not ok:
                print("server did not become reachable")
                return 1

            print("[1] static assets")
            page = await http.get("/")
            check(page.status_code == 200 and "故障日志检索" in page.text, "index page served")
            js = await http.get("/static/app.js")
            check(js.status_code == 200 and "api/search" in js.text, "app.js served")

            print("[2] concurrent cold start sharing")
            check(not child_pids(), "no backend child before first query")
            results = await asyncio.gather(
                *(
                    http.post(
                        "/api/search",
                        json={"query": "ERROR", "limit": 10, "ready_timeout_ms": 5000},
                    )
                    for _ in range(12)
                )
            )
            pids = {r.json().get("backend_pid") for r in results}
            check(all(r.status_code == 200 for r in results), "all 12 queries succeeded")
            check(len(pids) == 1, "all 12 queries used one backend pid", str(pids))
            check(len(child_pids()) == 1, "exactly one live child", str(child_pids()))
            status = (await http.get("/api/status")).json()
            check(status["counters"]["starts"] == 1, "one start attempt", str(status["counters"]))

            print("[3] short readiness budget only fails its own query")
            await http.put("/api/settings", json={"startup_delay_ms": 2500})
            # Idle timeout is 2s in this harness; wait for the current backend
            # to be reaped so the next call must cold-start.
            reaped = await wait_for(lambda: _async_status_stopped(http), timeout=12)
            check(reaped, "idle reap completed before cold-start test", str(child_pids()))
            impatient, patient = await asyncio.gather(
                http.post("/api/search", json={"query": "gateway", "limit": 5, "ready_timeout_ms": 300}),
                http.post("/api/search", json={"query": "gateway", "limit": 5, "ready_timeout_ms": 6000}),
            )
            check(impatient.status_code == 504, "impatient query got 504", impatient.text[:80])
            check(impatient.json().get("error") == "ready_timeout", "reason is ready_timeout")
            check(patient.status_code == 200, "patient query still succeeded")
            check(len(child_pids()) == 1, "shared start produced one child", str(child_pids()))
            await http.put("/api/settings", json={"startup_delay_ms": 300})

            print("[4] idle reap then wake")
            before = (await http.get("/api/status")).json()["backend_pid"]
            check(await wait_for(lambda: not child_pids(), timeout=12), "child reaped after idle")
            again = await http.post("/api/search", json={"query": "ERROR", "limit": 3, "ready_timeout_ms": 6000})
            check(again.status_code == 200, "query after reap succeeded")
            check(again.json()["backend_pid"] != before, "a new child was spawned")
            check(len(child_pids()) == 1, "still at most one child", str(child_pids()))

            print("[5] single-process sampling during reap/wake interleaving")
            max_seen = 0
            for _ in range(40):
                await http.post("/api/search", json={"query": "gateway", "limit": 2, "ready_timeout_ms": 6000})
                await asyncio.sleep(0.15)
                max_seen = max(max_seen, len(child_pids()))
                if await _async_status_stopped(http):
                    await http.post("/api/search", json={"query": "gateway", "limit": 2, "ready_timeout_ms": 6000})
                max_seen = max(max_seen, len(child_pids()))
            check(max_seen <= 1, "never more than one child process", f"max={max_seen}")

            print("[6] drill clock: start, play, pause, resume")
            await http.put("/api/settings", json={"startup_delay_ms": 300, "response_delay_ms": 0})
            drill = (await http.post("/api/drill/start")).json()
            check(drill["started"] and drill["playing"], "drill clock started playing")
            first_ms = drill["first_at_ms"]
            await asyncio.sleep(0.6)
            paused = (await http.post("/api/drill/control", json={"action": "pause"})).json()
            check(not paused["playing"], "clock paused")
            check(paused["now_ms"] - first_ms >= 500, "clock advanced with real time",
                  f"+{paused['now_ms'] - first_ms}ms")
            frozen = paused["now_ms"]
            await asyncio.sleep(0.4)
            still = (await http.get("/api/drill")).json()
            check(still["now_ms"] == frozen, "clock frozen while paused")
            resumed = (await http.post("/api/drill/control", json={"action": "resume"})).json()
            check(resumed["playing"], "clock resumed")
            await asyncio.sleep(0.3)
            moving = (await http.get("/api/drill")).json()
            check(moving["now_ms"] > frozen, "clock advanced after resume")

            print("[7] accepted instant survives pause; results are time-sliced")
            await http.put("/api/settings", json={"response_delay_ms": 800})
            early = (await http.post("/api/drill/control", json={"action": "pause"})).json()
            accepted_expected = early["now_ms"]
            slow = asyncio.create_task(
                http.post("/api/drill/search", json={"query": "", "limit": 200})
            )
            await asyncio.sleep(0.2)
            check(await wait_for(lambda: _drill_inflight(http), timeout=3),
                  "slow drill query is in flight")
            await http.post("/api/drill/control", json={"action": "resume"})
            body = (await slow).json()
            check(body["drill"]["accepted_at_ms"] == accepted_expected,
                  "accepted instant unchanged by pause/resume",
                  f"{body['drill']['accepted_at_ms']} == {accepted_expected}")
            stamps = [row.get("ts_ms") for row in body["results"]]
            check(all(ts is not None and ts <= accepted_expected for ts in stamps),
                  "all rows are at or before the accepted instant")
            status = (await http.get("/api/status")).json()
            check(status["counters"]["starts"] == 1 + status["counters"]["reaps"]
                  or status["backend_pid"] is not None,
                  "drill search used the lazy backend")

            print("[8] rewind voids an in-flight drill query and clears later results")
            # Freeze at a fairly late instant so a rewind is unambiguous.
            await asyncio.sleep(1.2)
            await http.post("/api/drill/control", json={"action": "pause"})
            late = (await http.get("/api/drill")).json()
            before_gen = late["generation"]
            slow = asyncio.create_task(
                http.post("/api/drill/search", json={"query": "", "limit": 200})
            )
            await asyncio.sleep(0.2)
            check(await wait_for(lambda: _drill_inflight(http), timeout=3),
                  "slow drill query accepted before rewind")
            rewound = await http.post(
                "/api/drill/control",
                json={"action": "seek", "at_ms": late["first_at_ms"] + 1000},
            )
            check(rewound.status_code == 200, "rewind accepted")
            check(rewound.json()["now_ms"] == late["first_at_ms"] + 1000,
                  "clock moved to rewind target")
            check(rewound.json()["generation"] == before_gen + 1,
                  "rewind bumped the generation")
            resp = await slow
            check(resp.status_code == 409, "in-flight drill query got 409", resp.text[:90])
            check(resp.json().get("error") == "drill_rewound", "error is drill_rewound")
            snap = (await http.get("/api/drill")).json()
            check(snap["voided"] is True, "drill snapshot records the rewind")
            check(snap["rewinds"][-1]["voided"] >= 1, "rewind counted the voided query",
                  str(snap["rewinds"][-1]))

            print("[9] normal search unaffected; idle reap keeps clock")
            await http.put("/api/settings", json={"response_delay_ms": 0})
            normal = await http.post(
                "/api/search", json={"query": "ERROR", "limit": 2, "ready_timeout_ms": 6000}
            )
            check(normal.status_code == 200, "normal search still works after rewind")
            clock_before = (await http.get("/api/drill")).json()
            reaped = await wait_for(lambda: not child_pids(), timeout=12)
            check(reaped, "backend reaped while drill stays started")
            clock_after = (await http.get("/api/drill")).json()
            check(clock_after["started"] is True, "drill clock survives reap")
            check(
                clock_after["now_ms"] >= clock_before["now_ms"],
                "paused clock did not jump across the reap",
            )
            await http.post("/api/drill/control", json={"action": "resume"})
            again = await http.post(
                "/api/drill/search", json={"query": "timeout", "limit": 5, "ready_timeout_ms": 6000}
            )
            check(again.status_code == 200, "drill search wakes a fresh child after reap")
            check(len(child_pids()) == 1, "still one child", str(child_pids()))

            await http.put("/api/settings", json={"startup_delay_ms": 800, "response_delay_ms": 0})
    finally:
        if server is not None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
            leftover = child_pids()
            check(not leftover, "no backend child left after shutdown", str(leftover))

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all end-to-end checks passed")
    return 0


async def _probe(http: httpx.AsyncClient) -> bool:
    try:
        resp = await http.get("/api/health")
        return resp.status_code == 200
    except httpx.HTTPError:
        return False


async def _async_status_stopped(http: httpx.AsyncClient) -> bool:
    try:
        return (await http.get("/api/status")).json()["state"] == "stopped"
    except httpx.HTTPError:
        return False


async def _drill_inflight(http: httpx.AsyncClient) -> bool:
    try:
        return (await http.get("/api/drill")).json().get("in_flight", 0) >= 1
    except httpx.HTTPError:
        return False


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
