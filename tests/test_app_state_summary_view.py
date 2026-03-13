"""Tests for app.views.state_summary_view."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd


def test_state_summary_view_module_imports() -> None:
    """Test that the state_summary_view module imports without Streamlit dependency."""
    from india_resilience_tool.app.views import state_summary_view

    assert state_summary_view is not None


def test_state_summary_view_exports_renderer() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.views.state_summary_view import render_state_summary_view

    assert callable(render_state_summary_view)


def test_compute_position_in_india_single_state_returns_na_rank() -> None:
    from india_resilience_tool.app.views.state_summary_view import _compute_position_in_india

    rank, n = _compute_position_in_india(
        {"telangana": 12.0},
        "Telangana",
        higher_is_worse=True,
    )
    assert rank is None
    assert n == 1


def test_compute_position_in_india_rank_is_never_greater_than_n() -> None:
    from india_resilience_tool.app.views.state_summary_view import _compute_position_in_india

    rank, n = _compute_position_in_india(
        {"telangana": 12.0, "maharashtra": 8.0},
        "Telangana",
        higher_is_worse=True,
    )
    assert rank is not None
    assert n == 2
    assert rank <= n


def test_resolve_state_column_returns_none_without_state_fields() -> None:
    from india_resilience_tool.app.views.state_summary_view import _resolve_state_column

    df = pd.DataFrame({"basin_name": ["Godavari Basin"]})
    assert _resolve_state_column(df) is None


def test_render_state_summary_view_returns_safely_without_state_column(monkeypatch) -> None:
    from india_resilience_tool.app.views.state_summary_view import render_state_summary_view

    streamlit_stub = types.SimpleNamespace(
        subheader=lambda *args, **kwargs: None,
        markdown=lambda *args, **kwargs: None,
        info=lambda *args, **kwargs: None,
    )
    monkeypatch.setitem(sys.modules, "streamlit", streamlit_stub)

    render_state_summary_view(
        selected_state="Telangana",
        variables={"aq_water_stress": {"label": "Aqueduct Water Stress", "units": "index"}},
        variable_slug="aq_water_stress",
        sel_scenario="opt",
        sel_period="2080",
        sel_stat="mean",
        metric_col="aq_water_stress__opt__2080__mean",
        merged_gdf=pd.DataFrame({"basin_name": ["Godavari Basin"]}),
        processed_root=Path("."),
        level="sub_basin",
    )
