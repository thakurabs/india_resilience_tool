"""
Smoke tests for app.views.details_panel.

We intentionally avoid importing Streamlit at module import time; the module should
import cleanly in test environments.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import inspect


def test_details_panel_module_imports() -> None:
    """Test that the details_panel module imports without Streamlit dependency."""
    from india_resilience_tool.app.views import details_panel

    assert details_panel is not None


def test_details_panel_exports_render_details_panel() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.views.details_panel import render_details_panel

    assert callable(render_details_panel)


def test_render_details_panel_accepts_alias_fn_keyword() -> None:
    """The details panel contract should accept alias_fn for optimized trend loading."""
    from india_resilience_tool.app.views.details_panel import render_details_panel

    sig = inspect.signature(render_details_panel)
    assert "alias_fn" in sig.parameters


def test_details_panel_exports_subrenderers() -> None:
    """Test that all sub-renderers are exported and callable."""
    from india_resilience_tool.app.views.details_panel import (
        render_case_study_export,
        render_detailed_statistics,
        render_district_comparison,
        render_river_context,
        render_risk_summary,
        render_scenario_comparison,
        render_trend_over_time,
    )

    assert callable(render_risk_summary)
    assert callable(render_river_context)
    assert callable(render_trend_over_time)
    assert callable(render_scenario_comparison)
    assert callable(render_detailed_statistics)
    assert callable(render_case_study_export)
    assert callable(render_district_comparison)


def test_render_trend_over_time_accepts_alias_fn_keyword() -> None:
    """The trend renderer should accept alias_fn for optimized model-series loading."""
    from india_resilience_tool.app.views.details_panel import render_trend_over_time

    sig = inspect.signature(render_trend_over_time)
    assert "alias_fn" in sig.parameters


def test_render_river_context_uses_plain_language_label() -> None:
    """River context should expose user-facing wording instead of backend jargon."""
    from india_resilience_tool.app.views.details_panel import render_river_context

    source = inspect.getsource(render_river_context)
    assert "Mapped river segments" in source
