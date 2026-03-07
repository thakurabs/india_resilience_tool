"""
Unit tests for Plotly trend chart spaghetti overlay.

These tests are intentionally lightweight and focus on deterministic data transforms
and trace construction (not pixel-perfect rendering).
"""

from __future__ import annotations

import pandas as pd
import pytest


def test_trend_plotly_spaghetti_overlay_adds_model_traces() -> None:
    plotly = pytest.importorskip("plotly.graph_objects")
    go = plotly

    from india_resilience_tool.viz.charts import create_trend_figure_for_index_plotly

    hist = pd.DataFrame(
        {"year": [1990, 1991], "mean": [1.0, 1.1], "p05": [0.8, 0.9], "p95": [1.2, 1.3]}
    )
    scen = pd.DataFrame(
        {"year": [2020, 2021], "mean": [2.0, 2.1], "p05": [1.8, 1.9], "p95": [2.2, 2.3]}
    )

    model_hist = pd.DataFrame(
        {
            "year": [1990, 1991, 1990, 1991],
            "value": [0.95, 1.05, 1.05, 1.15],
            "model": ["m1", "m1", "m2", "m2"],
        }
    )
    model_scen = pd.DataFrame(
        {
            "year": [2020, 2021, 2020, 2021],
            "value": [1.95, 2.05, 2.05, 2.15],
            "model": ["m1", "m1", "m2", "m2"],
        }
    )

    fig = create_trend_figure_for_index_plotly(
        hist_ts=hist,
        scen_ts=scen,
        idx_label="Metric",
        scenario_name="ssp245",
        model_ts_hist=model_hist,
        model_ts_scen=model_scen,
        show_model_members=True,
        max_models=10,
        show_band=False,
    )

    assert isinstance(fig, go.Figure)
    # 2 models (hist) + historical median + 2 models (scen) + scenario median
    assert len(fig.data) == 6
    n_scattergl = sum(1 for t in fig.data if getattr(t, "type", "") == "scattergl")
    assert n_scattergl == 4

