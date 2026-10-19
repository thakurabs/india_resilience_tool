"""
Streamlit entry point (thin) for IRT.

Canonical chain:
  `streamlit run main.py` → `india_resilience_tool.app.main.run()` → `india_resilience_tool.app.runtime.run_app()`

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Streamlit executes scripts with the script's directory on sys.path (not the repo root).
# Ensure the repo root is present so `import india_resilience_tool...` works reliably.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _configure_pyproj_data_dir() -> None:
    """Point pyproj at conda's PROJ database before dashboard geodata imports."""
    from pyproj import datadir

    candidates = [
        os.environ.get("PROJ_DATA"),
        os.environ.get("PROJ_LIB"),
    ]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(str(Path(conda_prefix) / "Library" / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "Library" / "share" / "proj"))

    for candidate in candidates:
        if not candidate:
            continue
        proj_db = Path(candidate) / "proj.db"
        if proj_db.exists():
            os.environ.setdefault("PROJ_DATA", str(proj_db.parent))
            os.environ.setdefault("PROJ_LIB", str(proj_db.parent))
            datadir.set_data_dir(str(proj_db.parent))
            return


_configure_pyproj_data_dir()

from india_resilience_tool.app.runtime import run_app
from india_resilience_tool.app.state import ensure_session_state


def run() -> None:
    """
    Run the Streamlit dashboard.

    Behavior-preserving implementation:
    - initializes session_state keys with defaults
    - executes the canonical dashboard runtime entrypoint (per rerun)
    """
    debug = bool(int(os.getenv("IRT_DEBUG", "0")))
    ensure_session_state(perf_default=debug)

    run_app()


if __name__ == "__main__":
    run()
