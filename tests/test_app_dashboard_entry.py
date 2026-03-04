"""
Smoke test for the canonical app runtime entrypoint.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import runtime


def test_run_dashboard_is_callable() -> None:
    assert callable(runtime.run_app)
