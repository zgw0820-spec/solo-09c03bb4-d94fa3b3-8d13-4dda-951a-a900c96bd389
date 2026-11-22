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
  * at any sampled moment at most one backend child is alive.
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


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
