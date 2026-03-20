"""
Smoke tests for app.views.rankings_view.

We intentionally avoid importing Streamlit at module import time; the module should
import cleanly in test environments.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.views import rankings_view


def test_rankings_view_exports_renderer() -> None:
    assert callable(rankings_view.render_rankings_view)