"""Stress the reap/wake interleaving: many waves of concurrent queries with a
very short idle timeout, asserting that the live backend child count never
exceeds one and that no query sees a stale/failed backend unexpectedly.

    python3 scripts/stress_interleave.py --waves 25 --concurrency 16
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
sys.path.insert(0, str(ROOT / "scripts"))
from e2e_check import child_pids  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--waves", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--port", type=int, default=8127)
    args = parser.parse_args()

    base = f"http://127.0.0.1:{args.port}"
    env = os.environ.copy()
    env["BACKEND_IDLE_TIMEOUT_S"] = "1"
    env["BACKEND_DATA_DIR"] = str(ROOT / "data" / "state")
    env["PYTHONPATH"] = str(ROOT)
    server = subprocess.Popen(
        [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(args.port), "--log-level", "warning"],
        cwd=str(ROOT), env=env,
    )
    started = time.monotonic()
    max_children = 0
    errors: list[str] = []
    ok = 0
    try:
        async with httpx.AsyncClient(base_url=base, timeout=30.0) as http:
            for _ in range(60):
                try:
                    if (await http.get("/api/health")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.25)
            await http.put("/api/settings", json={"startup_delay_ms": 250})

            for wave in range(args.waves):
                # Vary the gap so some waves start cold (post-reap) and others
                # hit a live backend; rotate patience budgets as well.
                if wave % 3 == 0:
                    await asyncio.sleep(1.25)  # past the 1s idle window
                elif wave % 3 == 1:
                    await asyncio.sleep(0.15)
                requests = []
                for _ in range(args.concurrency):
                    budget = random.choice([250, 400, 1500, 4000, 8000])
                    requests.append(
                        http.post(
                            "/api/search",
                            json={
                                "query": random.choice(["ERROR", "gateway", "timeout", ""]),
                                "limit": random.randint(1, 40),
                                "ready_timeout_ms": budget,
                            },
                        )
                    )
                # Sample the child count aggressively while the wave runs.
                sample_task = asyncio.create_task(_sample(0.01, 1.5))
                responses = await asyncio.gather(*requests, return_exceptions=True)
                sampled = await sample_task
                max_children = max(max_children, sampled)
                for resp in responses:
                    if isinstance(resp, Exception):
                        errors.append(f"wave {wave}: {resp!r}")
                    elif resp.status_code == 200:
                        ok += 1
                    elif resp.status_code in (503, 504):
                        body = resp.json()
                        if body.get("error") not in ("ready_timeout",):
                            errors.append(f"wave {wave}: {resp.status_code} {body}")
                    else:
                        errors.append(f"wave {wave}: HTTP {resp.status_code}")
                live = len(child_pids())
                max_children = max(max_children, live)
                if live > 1:
                    errors.append(f"wave {wave}: {live} children alive at wave end")
            final = (await http.get("/api/status")).json()
    finally:
        server.send_signal(subprocess.signal.SIGTERM)
        try:
            server.wait(timeout=20)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        leftover = child_pids()
        if leftover:
            errors.append(f"children left after shutdown: {leftover}")

    elapsed = time.monotonic() - started
    print(f"waves={args.waves} concurrency={args.concurrency} "
          f"ok={ok} namespace-errors={len(errors)} max_children={max_children} "
          f"elapsed={elapsed:.1f}s")
    print("starts/failures/reaps:", final["counters"])
    if errors:
        for err in errors[:15]:
            print("  ERROR", err)
        return 1
    if max_children > 1:
        print("FAIL: more than one backend child observed")
        return 1
    print("PASS: single backend process maintained throughout")
    return 0


async def _sample(interval: float, duration: float) -> int:
    peak = 0
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        peak = max(peak, len(child_pids()))
        await asyncio.sleep(interval)
    return peak


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
