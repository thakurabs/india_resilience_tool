"""
Smoke import test for app.main.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import main


def test_app_main_has_run() -> None:
    assert callable(main.run)
