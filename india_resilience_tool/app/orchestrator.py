"""
Dashboard orchestrator (thin wrapper) for the India Resilience Tool (IRT).

The canonical runtime entrypoint is `india_resilience_tool.app.runtime.run_app`.
This module remains as a stable import path used by `app/dashboard.py` and
legacy tests/docs.
"""

from __future__ import annotations

from india_resilience_tool.app.runtime import run_app

__all__ = ["run_app"]

