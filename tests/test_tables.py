"""
Tests for the rankings-table label methodology in viz.tables.

Covers Method A (ordinal percentile) vs Method B (bundle composite 0-100 score)
risk-class assignment, including the self-defensive whole-table fallback.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.metrics import risk_class_from_percentile
from india_resilience_tool.viz.tables import build_rankings_table_df


METRIC_COL = "m"


def _base_frame(values: list[float], bundle: list[float] | None = None) -> pd.DataFrame:
    """Five districts in one state with a clustered metric and optional bundle score."""
    frame = pd.DataFrame(
        {
            "district_name": [f"D{i}" for i in range(len(values))],
            "state_name": ["Telangana"] * len(values),
            METRIC_COL: values,
        }
    )
    if bundle is not None:
        frame["bscore"] = bundle
    return frame


def _build(frame: pd.DataFrame, *, bundle_score_col=None, higher_is_worse=True):
    return build_rankings_table_df(
        frame,
        metric_col=METRIC_COL,
        baseline_col=None,
        selected_state="Telangana",
        risk_class_from_percentile=risk_class_from_percentile,
        bundle_score_col=bundle_score_col,
        higher_is_worse=higher_is_worse,
    )


def test_method_b_label_follows_bundle_not_value() -> None:
    # Tightly clustered values, bundle scores in a DIFFERENT order spanning 0-100.
    values = [50.0, 51.0, 52.0, 53.0, 54.0]
    bundle = [10.0, 90.0, 30.0, 70.0, 50.0]
    frame = _base_frame(values, bundle)

    table_df, _ = _build(frame, bundle_score_col="bscore")
    by_district = table_df.set_index("district_name")

    # risk_class must follow the composite bands, decoupled from value ordering.
    assert by_district.loc["D0", "risk_class"] == "Very Low"   # bundle 10
    assert by_district.loc["D1", "risk_class"] == "Very High"  # bundle 90
    assert by_district.loc["D2", "risk_class"] == "Low"        # bundle 30
    assert by_district.loc["D3", "risk_class"] == "High"       # bundle 70
    assert by_district.loc["D4", "risk_class"] == "Medium"     # bundle 50

    # rank_value still tracks the metric value (D4 highest -> rank 1 = worst).
    assert by_district.loc["D4", "rank_value"] == 1
    assert by_district.loc["D0", "rank_value"] == 5


def test_percentile_value_present_and_decoupled_under_method_b() -> None:
    values = [50.0, 51.0, 52.0, 53.0, 54.0]
    bundle = [10.0, 90.0, 30.0, 70.0, 50.0]
    table_df, _ = _build(_base_frame(values, bundle), bundle_score_col="bscore")

    # percentile_value still computed (displayed "Percentile" column) and tracks value.
    assert "percentile_value" in table_df.columns
    by_district = table_df.set_index("district_name")
    assert by_district.loc["D4", "percentile_value"] == 100.0  # highest value
    # Decoupled from risk_class, which follows the bundle (D4 bundle=50 -> Medium).
    assert by_district.loc["D4", "risk_class"] == "Medium"
    assert "bundle_score" in table_df.columns


def test_method_b_direction_independent() -> None:
    # higher_is_worse=False must NOT flip the composite-based label.
    values = [50.0, 51.0, 52.0, 53.0, 54.0]
    bundle = [10.0, 90.0, 30.0, 70.0, 50.0]
    frame = _base_frame(values, bundle)

    worse_high, _ = _build(frame, bundle_score_col="bscore", higher_is_worse=True)
    worse_low, _ = _build(frame, bundle_score_col="bscore", higher_is_worse=False)

    a = worse_high.set_index("district_name")["risk_class"]
    b = worse_low.set_index("district_name")["risk_class"]
    pd.testing.assert_series_equal(a, b)


def test_whole_table_nan_fallback_to_ordinal() -> None:
    values = [50.0, 51.0, 52.0, 53.0, 54.0]
    bundle = [10.0, 90.0, 30.0, 70.0, np.nan]  # one NaN -> whole table reverts
    frame = _base_frame(values, bundle)

    method_b, _ = _build(frame, bundle_score_col="bscore")
    method_a, _ = _build(_base_frame(values), bundle_score_col=None)

    # Entire table uses the ordinal labels (no stray "Unknown" cell).
    pd.testing.assert_series_equal(
        method_b.set_index("district_name")["risk_class"],
        method_a.set_index("district_name")["risk_class"],
    )
    assert "Unknown" not in set(method_b["risk_class"])


def test_backward_compat_no_bundle_col_is_pure_ordinal() -> None:
    values = [50.0, 51.0, 52.0, 53.0, 54.0]
    # Column absent / not requested reproduces today's ordinal label exactly.
    table_df, _ = _build(_base_frame(values), bundle_score_col=None)
    assert "bundle_score" not in table_df.columns

    by_district = table_df.set_index("district_name")
    # Direct recomputation of the ordinal labels.
    pct = (pd.Series(values).rank(pct=True) * 100.0).tolist()
    expected = [risk_class_from_percentile(p) for p in pct]
    assert list(by_district["risk_class"]) == expected
