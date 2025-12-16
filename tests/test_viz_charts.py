"""\
Unit tests for viz.charts.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import pandas as pd

from india_resilience_tool.viz.charts import (
    build_scenario_comparison_panel_for_row,
    canonical_period_label,
    create_trend_figure_for_index,
    make_scenario_comparison_figure,
)


def test_canonical_period_label() -> None:
    assert canonical_period_label("1990_2010") == "1990-2010"
    assert canonical_period_label("2020-2040") == "2020-2040"


def test_build_scenario_comparison_panel_for_row() -> None:
    row = pd.Series(
        {
            "m__historical__1990-2010__mean": 10.0,
            "m__ssp245__2020-2040__mean": 12.0,
            "m__ssp585__2020-2040__mean": 15.0,
        }
    )
    schema_items = [
        {
            "metric": "m",
            "scenario": "historical",
            "period": "1990-2010",
            "stat": "mean",
            "column": "m__historical__1990-2010__mean",
        },
        {
            "metric": "m",
            "scenario": "ssp245",
            "period": "2020-2040",
            "stat": "mean",
            "column": "m__ssp245__2020-2040__mean",
        },
        {
            "metric": "m",
            "scenario": "ssp585",
            "period": "2020-2040",
            "stat": "mean",
            "column": "m__ssp585__2020-2040__mean",
        },
    ]

    panel = build_scenario_comparison_panel_for_row(row, schema_items, "m", "mean")
    assert not panel.empty
    assert set(panel.columns).issuperset({"scenario", "period", "value", "column"})
    assert panel.shape[0] == 3


def test_make_scenario_comparison_figure_smoke() -> None:
    panel_df = pd.DataFrame(
        {
            "scenario": ["historical", "ssp245", "ssp585"],
            "period": ["1990-2010", "2020-2040", "2020-2040"],
            "value": [10.0, 12.0, 15.0],
        }
    )

    fig, ax = make_scenario_comparison_figure(
        panel_df,
        metric_label="Metric",
        sel_scenario="ssp585",
        sel_period="2020-2040",
        sel_stat="mean",
        district_name="X",
    )
    assert fig is not None
    assert ax is not None


def test_create_trend_figure_for_index_smoke() -> None:
    hist = pd.DataFrame(
        {"year": [1990, 1991], "mean": [1.0, 1.2], "p05": [0.8, 1.0], "p95": [1.2, 1.4]}
    )
    scen = pd.DataFrame(
        {"year": [2020, 2021], "mean": [2.0, 2.2], "p05": [1.8, 2.0], "p95": [2.2, 2.4]}
    )
    fig = create_trend_figure_for_index(hist, scen, "Metric", "ssp585")
    assert fig is not None
