"""
Streamlit entry point (thin) for IRT.

Step 21: stop using runpy. We now call india_resilience_tool.app.dashboard.run_dashboard(),
which executes the legacy dashboard implementation each rerun via importlib.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import os

from india_resilience_tool.app.dashboard import run_dashboard
from india_resilience_tool.app.state import ensure_session_state


def run() -> None:
    """
    Run the Streamlit dashboard.

    Behavior-preserving implementation:
    - initializes session_state keys with defaults
    - executes the legacy dashboard implementation script (per rerun)
    """
    debug = bool(int(os.getenv("IRT_DEBUG", "0")))
    ensure_session_state(perf_default=debug)

    run_dashboard()
