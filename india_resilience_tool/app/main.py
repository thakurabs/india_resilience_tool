"""
Streamlit entry point (thin) for IRT.

In Step 14 we keep behavior identical by running the existing dashboard implementation
file (dashboard_unfactored_impl.py). Later steps will replace this with true modular
views under india_resilience_tool.app.views.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import os
import runpy
from pathlib import Path

from india_resilience_tool.app.state import ensure_session_state


def _repo_root() -> Path:
    # .../india_resilience_tool/app/main.py -> repo root is parents[2]
    return Path(__file__).resolve().parents[2]


def run() -> None:
    """
    Run the Streamlit dashboard.

    Behavior-preserving implementation:
    - initializes session_state keys with defaults
    - executes the legacy dashboard implementation script
    """
    # Respect legacy DEBUG/perf semantics without forcing it:
    debug = bool(int(os.getenv("IRT_DEBUG", "0")))
    ensure_session_state(perf_default=debug)

    impl_path = _repo_root() / "dashboard_unfactored_impl.py"
    if not impl_path.exists():
        raise FileNotFoundError(f"Missing dashboard implementation file: {impl_path}")

    # Execute legacy dashboard as if it were the Streamlit script.
    runpy.run_path(str(impl_path), run_name="__main__")
