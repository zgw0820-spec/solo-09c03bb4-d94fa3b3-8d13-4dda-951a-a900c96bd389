#!/usr/bin/env python3
"""Clean up state files and stray backend children left by manual runs.

Only touches the project's ``data/state`` directory and processes whose
command line contains ``app.backend`` that were started from this checkout.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e2e_check import child_pids  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    stray = child_pids()
    if stray:
        print("stray backend children:", stray)
        for pid in stray:
            try:
                os.kill(pid, 15)
            except OSError:
                pass
    else:
        print("no stray backend children")

    state = ROOT / "data" / "state"
    if state.exists():
        shutil.rmtree(state)
        print("removed", state)
    for name in ("settings.json", "runtime_config.json"):
        path = ROOT / "data" / name
        if path.exists():
            path.unlink()
            print("removed", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
