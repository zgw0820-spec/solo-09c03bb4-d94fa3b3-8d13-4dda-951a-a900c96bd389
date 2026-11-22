from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

# Startup delay may approach but never exceed the fixed 5s readiness cap.
STARTUP_DELAY_MAX_MS = 4000
RESPONSE_DELAY_MAX_MS = 15000


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


@dataclass
class Settings:
    """Runtime configuration for the web app and the search backend.

    ``startup_delay_ms`` is only read when a backend child is spawned.  It is
    passed to the child and simulates a slow cold start.

    ``response_delay_ms`` is mirrored into the child runtime config file and is
    read by the child for every request, so changes apply to a running backend.

    The 5s startup cap and 10s idle reap window are intentionally fixed by the
    product requirements, but can be overridden with environment variables for
    automated tests.
    """

    log_file: Path = DEFAULT_DATA_DIR / "sample_logs.jsonl"
    drill_log_file: Path = DEFAULT_DATA_DIR / "drill_gateway_logs.jsonl"
    config_file: Path = DEFAULT_DATA_DIR / "settings.json"
    runtime_config_file: Path = DEFAULT_DATA_DIR / "runtime_config.json"
    startup_delay_ms: int = 800
    response_delay_ms: int = 0
    default_ready_timeout_ms: int = 3000
    startup_timeout_s: float = 5.0
    idle_timeout_s: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.getenv("BACKEND_DATA_DIR", str(DEFAULT_DATA_DIR)))
        log_file = Path(os.getenv("BACKEND_LOG_FILE", str(data_dir / "sample_logs.jsonl")))
        settings = cls(
            log_file=log_file,
            drill_log_file=Path(
                os.getenv(
                    "BACKEND_DRILL_LOG_FILE",
                    str(log_file.parent / "drill_gateway_logs.jsonl"),
                )
            ),
            config_file=Path(os.getenv("BACKEND_CONFIG_FILE", str(data_dir / "settings.json"))),
            runtime_config_file=Path(
                os.getenv("BACKEND_RUNTIME_CONFIG_FILE", str(data_dir / "runtime_config.json"))
            ),
            startup_timeout_s=_env_float("BACKEND_STARTUP_TIMEOUT_S", 5.0),
            idle_timeout_s=_env_float("BACKEND_IDLE_TIMEOUT_S", 10.0),
        )
        settings.load_persistent()
        return settings

    def load_persistent(self) -> None:
        if not self.config_file.exists():
            return
        try:
            payload = json.loads(self.config_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, dict):
            return
        self.startup_delay_ms = _clamp(
            int(payload.get("startup_delay_ms", self.startup_delay_ms)), 0, STARTUP_DELAY_MAX_MS
        )
        self.response_delay_ms = _clamp(
            int(payload.get("response_delay_ms", self.response_delay_ms)), 0, RESPONSE_DELAY_MAX_MS
        )
        self.default_ready_timeout_ms = _clamp(
            int(payload.get("default_ready_timeout_ms", self.default_ready_timeout_ms)),
            100,
            60000,
        )

    def validate(self) -> None:
        self.startup_delay_ms = _clamp(self.startup_delay_ms, 0, STARTUP_DELAY_MAX_MS)
        self.response_delay_ms = _clamp(self.response_delay_ms, 0, RESPONSE_DELAY_MAX_MS)
        self.default_ready_timeout_ms = _clamp(self.default_ready_timeout_ms, 100, 60000)
        self.startup_timeout_s = max(0.05, float(self.startup_timeout_s))
        self.idle_timeout_s = max(0.05, float(self.idle_timeout_s))

    def save(self) -> None:
        self.validate()
        _atomic_write_json(
            self.config_file,
            {
                "startup_delay_ms": self.startup_delay_ms,
                "response_delay_ms": self.response_delay_ms,
                "default_ready_timeout_ms": self.default_ready_timeout_ms,
            },
        )
        self.write_runtime_config()

    def write_runtime_config(self) -> None:
        _atomic_write_json(
            self.runtime_config_file,
            {"response_delay_ms": self.response_delay_ms},
        )

    def ensure_runtime_config(self) -> None:
        self.validate()
        self.write_runtime_config()

    def public(self) -> dict[str, Any]:
        return {
            "startup_delay_ms": self.startup_delay_ms,
            "response_delay_ms": self.response_delay_ms,
            "default_ready_timeout_ms": self.default_ready_timeout_ms,
            "startup_timeout_s": self.startup_timeout_s,
            "idle_timeout_s": self.idle_timeout_s,
            "log_file": str(self.log_file),
            "drill_log_file": str(self.drill_log_file),
        }
