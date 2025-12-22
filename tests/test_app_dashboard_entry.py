"""
Smoke test for app.dashboard entrypoint.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import dashboard


def test_run_dashboard_is_callable() -> None:
    assert callable(dashboard.run_dashboard)
