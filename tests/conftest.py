from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from app.manager import BackendManager
from app.settings import Settings

SAMPLE_LOG = Path(__file__).resolve().parent.parent / "data" / "sample_logs.jsonl"


def make_settings(tmp_path: Path, **overrides) -> Settings:
    settings = Settings(
        log_file=SAMPLE_LOG,
        config_file=tmp_path / "settings.json",
        runtime_config_file=tmp_path / "runtime_config.json",
        startup_delay_ms=0,
        response_delay_ms=0,
        default_ready_timeout_ms=3000,
        startup_timeout_s=5.0,
        idle_timeout_s=10.0,
    )
    for key, value in overrides.items():
        setattr(settings, key, value)
    settings.validate()
    settings.ensure_runtime_config()
    return settings


@pytest.fixture
async def manager(tmp_path):
    settings = make_settings(tmp_path)
    mgr = BackendManager(settings)
    try:
        yield mgr
    finally:
        await mgr.close()


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


def pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A zombie still answers kill(0); check /proc state where available.
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        try:
            return stat.read_text().split(") ", 1)[1][0] != "Z"
        except (OSError, IndexError):
            return True
    return True
