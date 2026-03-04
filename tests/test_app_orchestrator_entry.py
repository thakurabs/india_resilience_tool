"""
Smoke test: legacy orchestrator shim removed; runtime entry remains callable.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import runtime


def test_run_app_is_callable() -> None:
    assert callable(runtime.run_app)
