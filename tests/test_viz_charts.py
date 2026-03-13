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
    build_portfolio_scenario_min_pivot,
    build_scenario_comparison_panel_for_row,
    canonical_period_label,
    create_trend_figure_for_index,
    make_portfolio_heatmap,
    make_portfolio_heatmap_robust_min_percentile,
    make_portfolio_heatmap_scenario_panels,
    make_portfolio_scenario_grouped_bar,
    make_scenario_comparison_figure,
)


def _sample_portfolio_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "State": "Telangana",
                "District": "Nalgonda",
                "Index": "Annual Mean Temperature (TM Mean)",
                "Scenario": "ssp245",
                "Current value": 28.1,
                "Percentile": 60.0,
            },
            {
                "State": "Telangana",
                "District": "Nalgonda",
                "Index": "Annual Mean Temperature (TM Mean)",
                "Scenario": "ssp585",
                "Current value": 29.4,
                "Percentile": 55.0,
            },
            {
                "State": "Telangana",
                "District": "Nalgonda",
                "Index": "Hot Days (TX ≥ 30°C)",
                "Scenario": "ssp245",
                "Current value": 291.0,
                "Percentile": 40.0,
            },
            {
                "State": "Telangana",
                "District": "Nalgonda",
                "Index": "Hot Days (TX ≥ 30°C)",
                "Scenario": "ssp585",
                "Current value": 305.0,
                "Percentile": 65.0,
            },
            {
                "State": "Telangana",
                "District": "Warangal",
                "Index": "Annual Mean Temperature (TM Mean)",
                "Scenario": "ssp245",
                "Current value": 27.5,
                "Percentile": 30.0,
            },
            {
                "State": "Telangana",
                "District": "Warangal",
                "Index": "Annual Mean Temperature (TM Mean)",
                "Scenario": "ssp585",
                "Current value": 28.9,
                "Percentile": 75.0,
            },
            {
                "State": "Telangana",
                "District": "Warangal",
                "Index": "Hot Days (TX ≥ 30°C)",
                "Scenario": "ssp245",
                "Current value": 280.0,
                "Percentile": 20.0,
            },
            {
                "State": "Telangana",
                "District": "Warangal",
                "Index": "Hot Days (TX ≥ 30°C)",
                "Scenario": "ssp585",
                "Current value": 315.0,
                "Percentile": 80.0,
            },
        ]
    )


def test_canonical_period_label() -> None:
    assert canonical_period_label("1990_2010") == "1990-2010"
    assert canonical_period_label("2020-2040") == "2020-2040"
    assert canonical_period_label("2030") == "2030"


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


def test_build_scenario_comparison_panel_for_aqueduct_tokens() -> None:
    row = pd.Series(
        {
            "aq_water_stress__historical__1979-2019__mean": 0.6,
            "aq_water_stress__bau__2030__mean": 0.8,
            "aq_water_stress__opt__2050__mean": 0.7,
            "aq_water_stress__pes__2080__mean": 1.1,
        }
    )
    schema_items = [
        {
            "metric": "aq_water_stress",
            "scenario": "historical",
            "period": "1979-2019",
            "stat": "mean",
            "column": "aq_water_stress__historical__1979-2019__mean",
        },
        {
            "metric": "aq_water_stress",
            "scenario": "bau",
            "period": "2030",
            "stat": "mean",
            "column": "aq_water_stress__bau__2030__mean",
        },
        {
            "metric": "aq_water_stress",
            "scenario": "opt",
            "period": "2050",
            "stat": "mean",
            "column": "aq_water_stress__opt__2050__mean",
        },
        {
            "metric": "aq_water_stress",
            "scenario": "pes",
            "period": "2080",
            "stat": "mean",
            "column": "aq_water_stress__pes__2080__mean",
        },
    ]

    panel = build_scenario_comparison_panel_for_row(row, schema_items, "aq_water_stress", "mean")
    assert not panel.empty
    assert panel["scenario"].astype(str).tolist() == ["historical", "bau", "opt", "pes"]
    assert panel["period"].astype(str).tolist() == ["1979-2019", "2030", "2050", "2080"]


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


def test_build_portfolio_scenario_min_pivot() -> None:
    df = _sample_portfolio_df()
    pv = build_portfolio_scenario_min_pivot(
        df,
        value_col="Percentile",
        scenarios=["ssp245", "ssp585"],
    )
    assert pv.loc["Nalgonda, Telangana", "Hot Days (TX ≥ 30°C)"] == 40.0
    assert pv.loc["Warangal, Telangana", "Hot Days (TX ≥ 30°C)"] == 20.0


def test_portfolio_scenario_visualizations_smoke() -> None:
    df = _sample_portfolio_df()

    fig_panels = make_portfolio_heatmap_scenario_panels(
        df,
        value_col="Percentile",
        scenarios=["ssp245", "ssp585"],
        normalize_per_index=False,
        layout="horizontal",
    )
    assert fig_panels is not None
    cax = next(
        (ax for ax in fig_panels.axes if ax.get_label() == "_portfolio_scenario_panels_colorbar"),
        None,
    )
    assert cax is not None
    tick_labels = [t.get_text().strip() for t in cax.get_yticklabels() if t.get_text().strip()]
    assert tick_labels == ["Very Low", "Low", "Medium", "High", "Very High"]

    fig_panels_v = make_portfolio_heatmap_scenario_panels(
        df,
        value_col="Percentile",
        scenarios=["ssp245", "ssp585"],
        normalize_per_index=False,
        layout="vertical",
        hide_xticklabels_except_last=True,
        hspace=0.12,
    )
    assert fig_panels_v is not None
    # In vertical mode with hide_xticklabels_except_last, the top panel should hide x tick labels.
    non_cbar_axes = [ax for ax in fig_panels_v.axes if ax.get_label() != "_portfolio_scenario_panels_colorbar"]
    assert len(non_cbar_axes) >= 2
    top_ax = non_cbar_axes[0]
    top_labels = [t.get_text().strip() for t in top_ax.get_xticklabels() if t.get_text().strip()]
    assert top_labels == []

    fig_robust = make_portfolio_heatmap_robust_min_percentile(
        df,
        scenarios=["ssp245", "ssp585"],
    )
    assert fig_robust is not None
    cax_r = next(
        (ax for ax in fig_robust.axes if ax.get_label() == "_portfolio_robust_min_colorbar"),
        None,
    )
    assert cax_r is not None
    tick_labels_r = [t.get_text().strip() for t in cax_r.get_yticklabels() if t.get_text().strip()]
    assert tick_labels_r == ["Very Low", "Low", "Medium", "High", "Very High"]

    fig_bar = make_portfolio_scenario_grouped_bar(
        df,
        index_name="Hot Days (TX ≥ 30°C)",
        value_col="Percentile",
        scenarios=["ssp245", "ssp585"],
        horizontal=True,
        show_values=False,
    )
    assert fig_bar is not None


def test_portfolio_heatmap_percentile_uses_risk_class_colorbar() -> None:
    df = _sample_portfolio_df()
    fig = make_portfolio_heatmap(df, value_col="Percentile", normalize_per_index=False)
    assert fig is not None
    cax = next(
        (ax for ax in fig.axes if ax.get_label() == "_portfolio_heatmap_percentile_colorbar"),
        None,
    )
    assert cax is not None
    tick_labels = [t.get_text().strip() for t in cax.get_yticklabels() if t.get_text().strip()]
    assert tick_labels == ["Very Low", "Low", "Medium", "High", "Very High"]


def test_create_trend_figure_for_index_smoke() -> None:
    hist = pd.DataFrame(
        {"year": [1990, 1991], "mean": [1.0, 1.2], "p05": [0.8, 1.0], "p95": [1.2, 1.4]}
    )
    scen = pd.DataFrame(
        {"year": [2020, 2021], "mean": [2.0, 2.2], "p05": [1.8, 2.0], "p95": [2.2, 2.4]}
    )
    fig = create_trend_figure_for_index(hist, scen, "Metric", "ssp585")
    assert fig is not None
