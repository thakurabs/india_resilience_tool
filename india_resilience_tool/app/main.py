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
