"""Lazy, single-instance lifecycle management for the log-search backend.

Invariants
----------
* At most one backend child process exists at any time.  A child is spawned by
  exactly one "round" task which is also the only task allowed to terminate and
  reap it.  A new round can only be created after the previous round has
  finished reaping (state ``stopped`` with no current round).
* Concurrent callers that need a backend share a single start attempt.  Each
  caller has its own readiness deadline measured from submission; hitting that
  deadline only abandons that caller, never the shared attempt.
* A failed start attempt (spawn error, child exit, or the fixed startup cap)
  fails every caller *still waiting on that attempt*.  Later callers may wake a
  new attempt once the failed process has fully exited.
* The backend is reaped after ``idle_timeout_s`` of continuous zero waiters and
  zero in-flight searches.
* Once teardown starts (state ``stopping``), new callers wait for the old
  process to exit before a new one can be spawned.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import httpx

from .settings import PROJECT_ROOT, Settings

TERMINATE_GRACE_S = 3.0


class BackendUnavailable(RuntimeError):
    """Raised when a search cannot obtain or keep a working backend."""

    def __init__(self, kind: str, detail: str = ""):
        super().__init__(detail or kind)
        self.kind = kind
        self.detail = detail


class _Aborted(Exception):
    """Internal: teardown was requested while the round was still starting."""


class _StartupTimeout(Exception):
    """Internal: the shared start attempt exceeded the fixed startup cap."""


@dataclass
class Round:
    id: int
    created_at: float
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    settled: asyncio.Event = field(default_factory=asyncio.Event)
    exited: asyncio.Event = field(default_factory=asyncio.Event)
    proc: asyncio.subprocess.Process | None = None
    port: int | None = None
    error_kind: str | None = None
    error_detail: str = ""
    failure_recorded: bool = False
    stop_reason: str | None = None
    waiters: int = 0
    exit_waiters: int = 0
    in_flight: int = 0
    ready_at: float | None = None
    exited_at: float | None = None


class BackendManager:
    def __init__(self, settings: Settings, child_env: dict[str, str] | None = None):
        self.settings = settings
        self._child_env = child_env
        self._lock = asyncio.Lock()
        self._round: Round | None = None
        self._state = "stopped"  # stopped | starting | ready | stopping
        self._next_round_id = 1
        self._idle_task: asyncio.Task[Any] | None = None
        self._idle_deadline: float | None = None
        self._round_task: asyncio.Task[Any] | None = None
        self._closed = False
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3.0, read=60.0, write=5.0, pool=5.0),
            trust_env=False,
        )
        self._counters = {"starts": 0, "failures": 0, "reaps": 0, "searches": 0}
        self._last_error: dict[str, Any] | None = None
        self._events: deque[dict[str, Any]] = deque(maxlen=80)

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _now() -> float:
        return asyncio.get_running_loop().time()

    def _record(self, event: str, rnd: Round | None = None, **detail: Any) -> None:
        entry: dict[str, Any] = {"at": time.time(), "event": event}
        if rnd is not None:
            entry["round"] = rnd.id
            if rnd.proc is not None:
                entry["pid"] = rnd.proc.pid
        entry.update(detail)
        self._events.append(entry)

    # ------------------------------------------------------------------ idle
    def _cancel_idle_locked(self) -> None:
        if self._idle_task is not None and not self._idle_task.done():
            self._idle_task.cancel()
        self._idle_task = None
        self._idle_deadline = None

    def _maybe_schedule_idle_locked(self) -> None:
        rnd = self._round
        if self._closed or rnd is None or rnd.stop.is_set():
            return
        if self._state not in ("starting", "ready"):
            return
        if rnd.waiters or rnd.in_flight or rnd.exit_waiters:
            return
        if self._idle_task is not None and not self._idle_task.done():
            return
        self._idle_deadline = self._now() + self.settings.idle_timeout_s
        self._idle_task = asyncio.create_task(self._idle_reaper(rnd))

    async def _idle_reaper(self, rnd: Round) -> None:
        try:
            await asyncio.sleep(self.settings.idle_timeout_s)
        except asyncio.CancelledError:
            return
        async with self._lock:
            if self._idle_task is not asyncio.current_task():
                return
            self._idle_task = None
            self._idle_deadline = None
            if self._closed or self._round is not rnd:
                return
            if self._state not in ("starting", "ready") or rnd.stop.is_set():
                return
            if rnd.waiters or rnd.in_flight or rnd.exit_waiters:
                # Activity resumed at the last moment; the releasing caller
                # reschedules when it is done.
                return
            self._begin_stop_locked(rnd, "idle")

    # ------------------------------------------------------------- transitions
    def _new_round_locked(self) -> Round:
        rnd = Round(id=self._next_round_id, created_at=self._now())
        self._next_round_id += 1
        self._round = rnd
        self._state = "starting"
        self._counters["starts"] += 1
        self._cancel_idle_locked()
        self._record("start_requested", rnd)
        self._round_task = asyncio.create_task(self._run_round(rnd))
        return rnd

    def _begin_stop_locked(self, rnd: Round, reason: str) -> None:
        if self._round is not rnd or rnd.stop.is_set():
            return
        rnd.stop_reason = reason
        rnd.stop.set()
        if self._state != "stopping":
            self._state = "stopping"
        self._cancel_idle_locked()
        self._record("stop_begin", rnd, reason=reason)

    def _abort_waiters_locked(self, rnd: Round, kind: str, detail: str) -> None:
        """Wake everyone still waiting on ``rnd`` without counting it as a
        backend failure (used for shutdown and manual teardown)."""
        if rnd.error_kind is None:
            rnd.error_kind = kind
            rnd.error_detail = detail
        rnd.settled.set()

    async def _record_failure_locked(self, rnd: Round, kind: str, detail: str) -> None:
        if rnd.error_kind is None:
            rnd.error_kind = kind
            rnd.error_detail = detail
        if not rnd.failure_recorded:
            rnd.failure_recorded = True
            self._counters["failures"] += 1
            self._last_error = {
                "at": time.time(),
                "kind": kind,
                "detail": detail,
                "round": rnd.id,
            }
            self._record("failure", rnd, kind=kind, detail=detail)
        # Wake everyone still waiting on this attempt; they observe error_kind.
        rnd.settled.set()
        if self._round is rnd and not rnd.stop.is_set():
            self._begin_stop_locked(rnd, kind)

    async def _record_failure(self, rnd: Round, kind: str, detail: str) -> None:
        async with self._lock:
            await self._record_failure_locked(rnd, kind, detail)

    # --------------------------------------------------------------- round loop
    async def _run_round(self, rnd: Round) -> None:
        proc: asyncio.subprocess.Process | None = None
        try:
            if rnd.stop.is_set():
                raise _Aborted

            env = os.environ.copy()
            if self._child_env:
                env.update(self._child_env)
            argv = [
                sys.executable,
                "-m",
                "app.backend",
                "--log-file",
                str(self.settings.log_file),
                "--runtime-config",
                str(self.settings.runtime_config_file),
                "--startup-delay-ms",
                str(self.settings.startup_delay_ms),
                "--response-delay-ms",
                str(self.settings.response_delay_ms),
            ]
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(PROJECT_ROOT),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            rnd.proc = proc
            self._record("spawned", rnd, pid=proc.pid)
            if rnd.stop.is_set():
                raise _Aborted

            # Race the whole startup sequence against a teardown request so a
            # start attempt is interruptible without leaking the child.
            startup = asyncio.create_task(self._startup(rnd, proc))
            stopper = asyncio.create_task(rnd.stop.wait())
            done, _ = await asyncio.wait(
                {startup, stopper}, return_when=asyncio.FIRST_COMPLETED
            )
            if stopper in done and startup not in done:
                startup.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await startup
                return
            stopper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopper
            # ``startup`` may also have finished with an error at the same
            # moment teardown was requested; in that case _record_failure has
            # already been applied below. Surface it rather than masking it.
            await startup  # validates readiness / raises timeout or failure

            # Serve until the child dies on its own or teardown is requested.
            exit_task = asyncio.create_task(proc.wait())
            stopper = asyncio.create_task(rnd.stop.wait())
            done, _ = await asyncio.wait(
                {exit_task, stopper}, return_when=asyncio.FIRST_COMPLETED
            )
            if exit_task in done and stopper not in done:
                code = exit_task.result()
                await self._record_failure(
                    rnd,
                    "backend_exited",
                    f"backend exited unexpectedly (code {code})",
                )
            stopper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopper
            if not exit_task.done():
                exit_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await exit_task
        except _Aborted:
            pass
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, _StartupTimeout) as exc:
            await self._record_failure(
                rnd, "startup_timeout", str(exc) or "backend not ready within startup cap"
            )
        except Exception as exc:  # noqa: BLE001 - surfaced to callers
            await self._record_failure(rnd, "startup_failed", str(exc))
        finally:
            await self._reap_child(rnd, proc)
            async with self._lock:
                rnd.exited_at = self._now()
                if rnd.error_kind is None and rnd.stop_reason == "idle":
                    self._counters["reaps"] += 1
                    self._record("reaped", rnd)
                if self._round is rnd:
                    self._round = None
                    self._state = "stopped"
                rnd.exited.set()
                self._record("exited", rnd, error=rnd.error_kind)

    async def _startup(
        self, rnd: Round, proc: asyncio.subprocess.Process
    ) -> None:
        cap_deadline = rnd.created_at + self.settings.startup_timeout_s
        remaining = cap_deadline - self._now()
        if remaining <= 0:
            raise asyncio.TimeoutError("startup cap elapsed before bind")
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=remaining)
        if not line:
            code = await proc.wait()
            raise RuntimeError(f"backend exited during startup (code {code})")
        try:
            hello = json.loads(line.decode("utf-8", "replace"))
            port = int(hello["port"])
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError(f"bad backend handshake: {line!r}") from exc
        rnd.port = port
        self._record("listening", rnd, port=port)

        while True:
            if rnd.stop.is_set():
                raise _Aborted
            remaining = cap_deadline - self._now()
            if remaining <= 0:
                raise asyncio.TimeoutError(
                    f"backend not ready within {self.settings.startup_timeout_s:g}s"
                )
            if proc.returncode is not None:
                raise RuntimeError(
                    f"backend exited during startup (code {proc.returncode})"
                )
            try:
                resp = await self._client.get(
                    f"http://127.0.0.1:{port}/health",
                    timeout=min(0.5, remaining),
                )
                if resp.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(min(0.03, max(0.005, remaining / 2)))

        async with self._lock:
            if self._closed or self._round is not rnd or rnd.stop.is_set():
                raise _Aborted
            self._state = "ready"
            rnd.ready_at = self._now()
            rnd.ready.set()
            rnd.settled.set()
            self._record("ready", rnd)
            self._maybe_schedule_idle_locked()

    async def _reap_child(
        self, rnd: Round, proc: asyncio.subprocess.Process | None
    ) -> None:
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=TERMINATE_GRACE_S)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
        if proc is not None:
            with contextlib.suppress(Exception):
                await proc.wait()
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    with contextlib.suppress(Exception):
                        stream.close()
            rnd.proc = None

    # ------------------------------------------------------------------ search
    async def _http_search(self, port: int, term: str, limit: int) -> dict[str, Any]:
        try:
            resp = await self._client.get(
                f"http://127.0.0.1:{port}/search",
                params={"q": term, "limit": limit},
            )
            resp.raise_for_status()
            payload = resp.json()
        except httpx.HTTPStatusError as exc:
            raise BackendUnavailable(
                "backend_error", f"backend returned HTTP {exc.response.status_code}"
            ) from exc
        except (httpx.HTTPError, ValueError) as exc:
            raise BackendUnavailable(
                "backend_error", f"backend call failed: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise BackendUnavailable("backend_error", "backend returned unexpected payload")
        return payload

    async def search(
        self, term: str, limit: int = 50, ready_timeout_ms: int | None = None
    ) -> dict[str, Any]:
        submitted = self._now()
        timeout_ms = (
            self.settings.default_ready_timeout_ms
            if ready_timeout_ms is None
            else ready_timeout_ms
        )
        timeout_ms = max(100, min(60000, int(timeout_ms)))
        deadline = submitted + timeout_ms / 1000.0

        while True:
            async with self._lock:
                if self._closed:
                    raise BackendUnavailable("shutdown", "server is shutting down")
                rnd = self._round
                if self._state == "ready" and rnd is not None and rnd.ready.is_set():
                    rnd.in_flight += 1
                    self._cancel_idle_locked()
                    port = rnd.port
                    mode = "search"
                elif self._state == "starting" and rnd is not None:
                    rnd.waiters += 1
                    self._cancel_idle_locked()
                    mode = "wait"
                elif self._state == "stopping" and rnd is not None:
                    rnd.exit_waiters += 1
                    self._cancel_idle_locked()
                    mode = "exit"
                elif self._state == "stopped" and rnd is None:
                    rnd = self._new_round_locked()
                    rnd.waiters += 1
                    mode = "wait"
                else:  # defensive: never expected
                    raise BackendUnavailable(
                        "backend_state", f"unexpected backend state {self._state!r}"
                    )

            if mode == "search":
                self._counters["searches"] += 1
                try:
                    return await self._http_search(port, term, limit)  # type: ignore[arg-type]
                finally:
                    async with self._lock:
                        rnd.in_flight -= 1
                        self._maybe_schedule_idle_locked()

            if mode == "wait":
                timed_out = False
                try:
                    remaining = deadline - self._now()
                    if remaining <= 0:
                        timed_out = True
                    else:
                        try:
                            await asyncio.wait_for(rnd.settled.wait(), timeout=remaining)
                        except asyncio.TimeoutError:
                            timed_out = True
                finally:
                    async with self._lock:
                        rnd.waiters -= 1
                        self._maybe_schedule_idle_locked()

                if rnd.ready.is_set():
                    continue
                if rnd.error_kind is not None:
                    raise BackendUnavailable(rnd.error_kind, rnd.error_detail)
                if timed_out:
                    raise BackendUnavailable(
                        "ready_timeout",
                        f"backend not ready within {timeout_ms}ms",
                    )
                continue

            # mode == "exit": old process is being reaped, never start early.
            try:
                remaining = deadline - self._now()
                if remaining > 0:
                    try:
                        await asyncio.wait_for(rnd.exited.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        pass
            finally:
                async with self._lock:
                    rnd.exit_waiters -= 1
                    self._maybe_schedule_idle_locked()
            if not rnd.exited.is_set():
                raise BackendUnavailable(
                    "ready_timeout",
                    f"previous backend process still exiting after {timeout_ms}ms",
                )
            continue

    # ------------------------------------------------------------------ status
    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            rnd = self._round
            now = self._now()
            alive = bool(rnd and rnd.proc is not None and rnd.proc.returncode is None)
            idle_remaining = None
            if self._idle_deadline is not None and self._idle_task is not None:
                idle_remaining = round(max(0.0, self._idle_deadline - now), 2)
            return {
                "state": self._state,
                "ready": bool(rnd and rnd.ready.is_set() and alive),
                "backend_pid": rnd.proc.pid if alive and rnd.proc else None,
                "port": rnd.port if alive and rnd else None,
                "round_id": rnd.id if rnd else None,
                "uptime_s": (
                    round(now - rnd.ready_at, 2)
                    if rnd and rnd.ready_at is not None and alive
                    else None
                ),
                "waiters": rnd.waiters if rnd else 0,
                "exit_waiters": rnd.exit_waiters if rnd else 0,
                "in_flight": rnd.in_flight if rnd else 0,
                "idle_reap_in_s": idle_remaining,
                "counters": dict(self._counters),
                "last_error": self._last_error,
                "settings": self.settings.public(),
                "events": list(self._events)[-20:],
            }

    # ----------------------------------------------------------------- shutdown
    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cancel_idle_locked()
            if self._round is not None:
                self._abort_waiters_locked(
                    self._round, "shutdown", "server is shutting down"
                )
                self._begin_stop_locked(self._round, "shutdown")
            round_task = self._round_task
        if round_task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(round_task), timeout=TERMINATE_GRACE_S + 5
                )
            except asyncio.TimeoutError:
                # Last resort: the round task refused to finish, so force-kill
                # whatever child it still owns.
                rnd = self._round
                proc = rnd.proc if rnd is not None else None
                if proc is not None and proc.returncode is None:
                    with contextlib.suppress(ProcessLookupError):
                        proc.kill()
        with contextlib.suppress(Exception):
            await self._client.aclose()


__all__ = ["BackendManager", "BackendUnavailable", "Round"]
