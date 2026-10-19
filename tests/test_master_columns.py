from __future__ import annotations

from india_resilience_tool.data.master_columns import find_baseline_column_for_metric, resolve_metric_column


def test_built_up_static_snapshot_columns_resolve_generically() -> None:
    columns = [
        "built_up_area_km2__snapshot__Current__mean",
        "built_up_area_share_pct__snapshot__Current__mean",
    ]
    assert (
        resolve_metric_column(columns, "built_up_area_km2", "snapshot", "Current", "mean")
        == "built_up_area_km2__snapshot__Current__mean"
    )
    assert (
        resolve_metric_column(columns, "built_up_area_share_pct", "snapshot", "Current", "mean")
        == "built_up_area_share_pct__snapshot__Current__mean"
    )


def test_find_baseline_column_for_metric_honors_preferred_period_order() -> None:
    columns = [
        "r95p_very_wet_precip__historical__1995-2014__mean",
        "r95p_very_wet_precip__historical__1990-2010__mean",
    ]

    assert (
        find_baseline_column_for_metric(
            columns,
            base_metric="r95p_very_wet_precip",
            preferred_period_tokens=("1990-2010", "1995-2014"),
        )
        == "r95p_very_wet_precip__historical__1990-2010__mean"
    )


def test_find_baseline_column_for_metric_normalizes_underscore_periods() -> None:
    columns = [
        "r95p_very_wet_precip__historical__1995-2014__mean",
        "r95p_very_wet_precip__historical__1990_2010__mean",
    ]

    assert (
        find_baseline_column_for_metric(
            columns,
            base_metric="r95p_very_wet_precip",
            preferred_period_tokens=("1990-2010", "1995-2014"),
        )
        == "r95p_very_wet_precip__historical__1990_2010__mean"
    )
