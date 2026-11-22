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
from datetime import datetime
from pathlib import Path
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


class DrillError(RuntimeError):
    """Raised when a drill request violates the drill rules."""

    def __init__(
        self,
        kind: str,
        detail: str = "",
        status: int = 409,
        extra: dict[str, Any] | None = None,
    ):
        super().__init__(detail or kind)
        self.kind = kind
        self.detail = detail
        self.status = status
        self.extra = extra or {}


class DrillClock:
    """A wall-clock time line driven by real elapsed time.

    The clock starts at the timestamp of the first drill log line and advances
    by real time while ``playing``.  Pausing freezes it; resuming continues
    from the frozen point.  Rewinding moves the anchor backwards without
    touching the play state.  A rewind never invalidates the clock itself; the
    manager bumps a separate generation counter so that in-flight searches can
    be voided even after the clock has walked back over the same instant.
    """

    def __init__(self) -> None:
        self.started = False
        self.playing = False
        self.first_at_ms = 0
        self.last_at_ms = 0
        self.tz: Any = None
        self._anchor_ms = 0
        self._anchor_mono = 0.0

    def initialize(
        self, first_at_ms: int, last_at_ms: int, tz: Any, mono: float
    ) -> None:
        self.started = True
        self.playing = True
        self.first_at_ms = int(first_at_ms)
        self.last_at_ms = int(last_at_ms)
        self.tz = tz
        self._anchor_ms = int(first_at_ms)
        self._anchor_mono = mono

    def now_ms(self, mono: float) -> int:
        if not self.started:
            return 0
        if not self.playing:
            return self._anchor_ms
        return self._anchor_ms + int(max(0.0, mono - self._anchor_mono) * 1000)

    def set_playing(self, playing: bool, mono: float) -> None:
        if not self.started or playing == self.playing:
            return
        # Freeze the current value before switching modes.
        self._anchor_ms = self.now_ms(mono)
        self._anchor_mono = mono
        self.playing = playing

    def rewind_to(self, target_ms: int, mono: float) -> None:
        self._anchor_ms = int(target_ms)
        self._anchor_mono = mono

    def iso(self, ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000.0, self.tz).isoformat(
            timespec="milliseconds"
        )

    def reset(self) -> None:
        self.started = False
        self.playing = False
        self.first_at_ms = 0
        self.last_at_ms = 0
        self.tz = None
        self._anchor_ms = 0
        self._anchor_mono = 0.0


@dataclass(eq=False)
class _DrillTicket:
    """One accepted drill search, tracked so a confirmed rewind can void it."""

    generation: int
    accepted_at_ms: int
    voided: asyncio.Event = field(default_factory=asyncio.Event)


def _parse_iso_ms(value: Any) -> tuple[int, Any] | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round(parsed.timestamp() * 1000), parsed.tzinfo


_DRILL_MARKERS = (
    ("first_timeout", "首次超时", ("upstream timeout",)),
    ("breaker_open", "熔断打开", ("circuit breaker OPEN",)),
    ("breaker_half_open", "半开探测", ("HALF_OPEN",)),
    ("breaker_closed", "熔断关闭", ("circuit breaker CLOSED",)),
    ("recovery", "开始恢复", ("recovery:",)),
    ("complete", "演练完成", ("drill complete",)),
)


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
        # ------------------------------------------------------------- drill
        self._drill = DrillClock()
        self._drill_generation = 0
        self._drill_voided = False
        self._drill_voided_at: float | None = None
        self._drill_inflight: set[asyncio.Task[Any]] = set()
        self._drill_events: deque[dict[str, Any]] = deque(maxlen=80)
        self._drill_rewinds: list[dict[str, Any]] = []
        self._drill_markers_cache: list[dict[str, Any]] | None = None

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
                "--drill-log-file",
                str(self.settings.drill_log_file),
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
    async def _call_backend(
        self, port: int, path: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            resp = await self._client.get(
                f"http://127.0.0.1:{port}{path}", params=params
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
                    return await self._call_backend(
                        port, "/search", {"q": term, "limit": limit}
                    )  # type: ignore[arg-type]
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

    # -------------------------------------------------------------------- drill
    @staticmethod
    def _read_log_lines(path: Path) -> list[str]:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return [line.rstrip("\n") for line in handle if line.strip()]
        except OSError:
            return []

    def _read_drill_span(self) -> tuple[int, int, Any] | None:
        path = Path(self.settings.drill_log_file)
        stamps: list[tuple[int, Any]] = []
        for raw in self._read_log_lines(path):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            parsed = _parse_iso_ms(payload.get("ts") if isinstance(payload, dict) else None)
            if parsed is not None:
                stamps.append(parsed)
        if not stamps:
            return None
        first = min(stamps, key=lambda item: item[0])
        last = max(stamps, key=lambda item: item[0])
        return first[0], last[0], first[1]

    def _drill_markers(self) -> list[dict[str, Any]]:
        """First occurrence of each drill stage, in log order."""
        path = Path(self.settings.drill_log_file)
        seen: set[str] = set()
        found: list[dict[str, Any]] = []
        for raw in self._read_log_lines(path):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            text = str(payload.get("message", ""))
            ts = _parse_iso_ms(payload.get("ts"))
            if ts is None:
                continue
            for key, label, needles in _DRILL_MARKERS:
                if key in seen:
                    continue
                if any(needle in text for needle in needles):
                    seen.add(key)
                    found.append({"kind": key, "label": label, "at_ms": ts[0]})
                    break
        return found

    def _drill_note(self, event: str, **detail: Any) -> None:
        entry: dict[str, Any] = {"at": time.time(), "event": event}
        entry.update(detail)
        self._drill_events.append(entry)

    async def drill_start(self) -> dict[str, Any]:
        """Start (or restart) the drill clock at the first log timestamp."""
        span = self._read_drill_span()
        if span is None:
            raise DrillError(
                "drill_log_unavailable",
                f"no parseable log lines in {self.settings.drill_log_file}",
                status=500,
            )
        async with self._lock:
            was_running = self._drill.started
            self._drill.initialize(span[0], span[1], span[2], self._now())
            self._drill_generation += 1
            self._drill_voided = False
            self._drill_voided_at = None
            self._drill_rewinds = []
            self._drill_markers_cache = None
            self._drill_note(
                "started",
                at_ms=span[0],
                generation=self._drill_generation,
                restarted=was_running,
            )
            return self._drill_snapshot_locked()

    async def drill_control(self, action: str, at_ms: int | None = None) -> dict[str, Any]:
        """Pause / resume / seek the drill clock.

        ``seek`` is a rewind: the generation is bumped *before* the clock is
        moved, so a search that was accepted under an earlier, later instant
        can no longer be mistaken for a current result.  Voiding is a one-way
        latch — it holds even if the clock is later resumed and walks forward
        past the same instant again.
        """
        async with self._lock:
            if not self._drill.started:
                raise DrillError("drill_not_started", "start the drill first")
            now = self._now()
            if action == "pause":
                self._drill.set_playing(False, now)
                self._drill_note("paused", at_ms=self._drill.now_ms(now))
            elif action == "resume":
                self._drill.set_playing(True, now)
                self._drill_note("resumed", at_ms=self._drill.now_ms(now))
            elif action == "seek":
                if at_ms is None:
                    raise DrillError("bad_request", "at_ms is required for seek", 400)
                target = max(self._drill.first_at_ms, min(self._drill.last_at_ms, int(at_ms)))
                previous = self._drill.now_ms(now)
                if target < previous:
                    # A rewind latches a new generation *before* the clock
                    # moves.  Tickets accepted under an older generation stay
                    # voided forever, even if the clock later walks back over
                    # the same instant.
                    self._drill_generation += 1
                    self._drill_voided = True
                    self._drill_voided_at = time.time()
                    tickets = list(self._drill_inflight)
                    for ticket in tickets:
                        ticket.voided.set()
                    self._drill.rewind_to(target, now)
                    self._drill_rewinds.append(
                        {
                            "at": time.time(),
                            "from_ms": previous,
                            "to_ms": target,
                            "voided": len(tickets),
                            "generation": self._drill_generation,
                        }
                    )
                    self._drill_note(
                        "rewound",
                        from_ms=previous,
                        to_ms=target,
                        voided=len(tickets),
                        generation=self._drill_generation,
                    )
                else:
                    # Forward jumps do not invalidate anything: every accepted
                    # result still carries the instant it was sliced at.
                    self._drill.rewind_to(target, now)
                    self._drill_note(
                        "skipped_forward", from_ms=previous, to_ms=target
                    )
            else:  # pragma: no cover - validated by the API layer
                raise DrillError("bad_request", f"unknown action {action!r}", 400)
            return self._drill_snapshot_locked()

    async def drill_search(
        self, term: str, limit: int = 50, ready_timeout_ms: int | None = None
    ) -> dict[str, Any]:
        """Search the drill log as of the instant fixed at acceptance.

        The instant and generation are captured once, before waiting for the
        lazily spawned child.  Neither is affected by pause/resume while the
        call is in flight.  A confirmed rewind after acceptance latches the
        ticket, and the call then fails as voided instead of returning rows
        that no longer match the current timeline.
        """
        async with self._lock:
            if not self._drill.started:
                raise DrillError("drill_not_started", "start the drill first")
            at_ms = self._drill.now_ms(self._now())
            accepted_at = self._drill.iso(at_ms)
            generation = self._drill_generation
            ticket = _DrillTicket(generation=generation, accepted_at_ms=at_ms)
            self._drill_inflight.add(ticket)
            note: dict[str, Any] = {
                "at": time.time(),
                "event": "accepted",
                "query": term,
                "accepted_at_ms": at_ms,
                "accepted_at": accepted_at,
                "generation": generation,
            }
            self._drill_events.append(note)

        try:
            payload = await self._run_search(
                term,
                min(200, max(1, limit)),
                ready_timeout_ms,
                path="/drill/search",
                extra={"at_ms": at_ms},
                ticket=ticket,
            )
        finally:
            async with self._lock:
                self._drill_inflight.discard(ticket)
                if ticket.voided.is_set():
                    self._drill_events.append(
                        {
                            "at": time.time(),
                            "event": "discarded",
                            "query": term,
                            "accepted_at_ms": at_ms,
                            "generation": generation,
                        }
                    )

        if ticket.voided.is_set() or generation != self._drill_generation:
            raise DrillError(
                "drill_rewound",
                "the drill clock was rewound after this search was accepted; result discarded",
                status=409,
                extra={
                    "accepted_at_ms": at_ms,
                    "accepted_at": accepted_at,
                    "generation": generation,
                },
            )

        payload["drill"] = {
            "accepted_at_ms": at_ms,
            "accepted_at": accepted_at,
            "generation": generation,
        }
        payload["voided"] = False
        return payload

    async def _run_search(
        self,
        term: str,
        limit: int,
        ready_timeout_ms: int | None,
        *,
        path: str = "/search",
        extra: dict[str, Any] | None = None,
        ticket: _DrillTicket | None = None,
    ) -> dict[str, Any]:
        """Acquire the shared child and call ``path``; optionally bail on void.

        ``ticket`` is only used to abort early once a rewind has voided the
        request; the authoritative check happens after the call returns in
        :meth:`drill_search`.
        """
        submitted = self._now()
        timeout_ms = (
            self.settings.default_ready_timeout_ms
            if ready_timeout_ms is None
            else ready_timeout_ms
        )
        timeout_ms = max(100, min(60000, int(timeout_ms)))
        deadline = submitted + timeout_ms / 1000.0
        params = {"q": term, "limit": limit}
        if extra:
            params.update(extra)

        while True:
            if ticket is not None and ticket.voided.is_set():
                raise DrillError(
                    "drill_rewound",
                    "the drill clock was rewound after this search was accepted; result discarded",
                    status=409,
                )
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
                    payload = await self._call_backend(port, path, params)  # type: ignore[arg-type]
                finally:
                    async with self._lock:
                        rnd.in_flight -= 1
                        self._maybe_schedule_idle_locked()
                if ticket is not None and ticket.voided.is_set():
                    raise DrillError(
                        "drill_rewound",
                        "the drill clock was rewound while this search was in flight; result discarded",
                        status=409,
                    )
                return payload

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

    def _drill_snapshot_locked(self) -> dict[str, Any]:
        now = self._now()
        clock = self._drill
        if not clock.started:
            return {
                "started": False,
                "playing": False,
                "now_ms": None,
                "now": None,
                "first_at_ms": None,
                "last_at_ms": None,
                "first_at": None,
                "last_at": None,
                "generation": self._drill_generation,
                "voided": False,
                "voided_at": None,
                "rewinds": list(self._drill_rewinds),
                "in_flight": 0,
                "markers": [],
                "clock_started_at": None,
                "clock_anchor": None,
                "elapsed_s": None,
                "events": list(self._drill_events)[-12:],
            }
        value = clock.now_ms(now)
        return {
            "started": True,
            "playing": clock.playing,
            "now_ms": value,
            "now": clock.iso(value),
            "first_at_ms": clock.first_at_ms,
            "last_at_ms": clock.last_at_ms,
            "first_at": clock.iso(clock.first_at_ms),
            "last_at": clock.iso(clock.last_at_ms),
            "generation": self._drill_generation,
            "voided": self._drill_voided,
            "voided_at": self._drill_voided_at,
            "rewinds": list(self._drill_rewinds),
            "in_flight": len(self._drill_inflight),
            "markers": self._markers_locked(),
            "clock_anchor": clock._anchor_ms,
            "clock_started_at": clock._anchor_mono,
            "elapsed_s": round(max(0.0, (value - clock.first_at_ms) / 1000.0), 3),
            "events": list(self._drill_events)[-12:],
        }

    def _markers_locked(self) -> list[dict[str, Any]]:
        cached = getattr(self, "_drill_markers_cache", None)
        if cached is None:
            cached = self._drill_markers()
            self._drill_markers_cache = cached
        return [dict(marker, at=self._drill.iso(marker["at_ms"])) for marker in cached]

    async def drill_snapshot(self) -> dict[str, Any]:
        async with self._lock:
            snapshot = self._drill_snapshot_locked()
        snapshot["log_file"] = str(self.settings.drill_log_file)
        return snapshot

    async def drill_reset(self) -> dict[str, Any]:
        async with self._lock:
            self._drill.reset()
            self._drill_generation += 1
            self._drill_voided = False
            self._drill_voided_at = None
            self._drill_rewinds = []
            self._drill_inflight.clear()
            self._drill_note("reset")
            return self._drill_snapshot_locked()

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
                "drill": self._drill_snapshot_locked(),
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


__all__ = ["BackendManager", "BackendUnavailable", "DrillClock", "DrillError", "Round"]
