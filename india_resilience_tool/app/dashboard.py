"""
Dashboard runner for the India Resilience Tool (IRT).

This module remains for backward compatibility with earlier refactor steps,
but the canonical runner is now:

  india_resilience_tool.app.main.run() -> orchestrator.run_app()

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.orchestrator import run_app


def run_dashboard() -> None:
    """
    Backward-compatible wrapper.

    Prefer calling india_resilience_tool.app.main.run(), but this remains as an
    alias used by earlier steps/tests.
    """
    run_app()
