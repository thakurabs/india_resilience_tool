"""
Smoke test for app.orchestrator entrypoint.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import orchestrator


def test_run_app_is_callable() -> None:
    assert callable(orchestrator.run_app)
