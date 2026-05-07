from __future__ import annotations

from india_resilience_tool.data.master_columns import resolve_metric_column


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
