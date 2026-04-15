from __future__ import annotations

import math

import pandas as pd

from india_resilience_tool.analysis.bundle_scores import (
    BundleMetricSpec,
    aggregate_state_bundle_scores,
    compute_bundle_score_frame,
    normalize_metric_series,
)


def test_normalize_metric_series_respects_higher_worse_directionality() -> None:
    values = pd.Series([10.0, 20.0, 30.0])

    higher_worse = normalize_metric_series(values, higher_is_worse=True)
    lower_worse = normalize_metric_series(values, higher_is_worse=False)

    assert higher_worse.tolist() == [0.0, 50.0, 100.0]
    assert lower_worse.tolist() == [100.0, 50.0, 0.0]


def test_compute_bundle_score_frame_averages_normalized_metrics_not_raw_values() -> None:
    df = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "district_name": ["One", "Two", "Three"],
            "heat_days": [0.0, 50.0, 100.0],
            "warm_nights": [1000.0, 1000.0, 2000.0],
        }
    )

    out = compute_bundle_score_frame(
        df,
        metric_specs=[
            BundleMetricSpec(slug="heat_days", label="Heat days", column="heat_days", weight=1.0, higher_is_worse=True),
            BundleMetricSpec(slug="warm_nights", label="Warm nights", column="warm_nights", weight=1.0, higher_is_worse=True),
        ],
        id_columns=("state_name", "district_name"),
    )

    scores = dict(zip(out["district_name"], out["bundle_score"]))
    assert scores["One"] == 0.0
    assert scores["Two"] == 25.0
    assert scores["Three"] == 100.0


def test_compute_bundle_score_frame_uses_available_metrics_only_and_preserves_all_missing() -> None:
    df = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "district_name": ["One", "Two", "Three"],
            "metric_a": [1.0, None, None],
            "metric_b": [None, None, None],
        }
    )

    out = compute_bundle_score_frame(
        df,
        metric_specs=[
            BundleMetricSpec(slug="metric_a", label="Metric A", column="metric_a", weight=1.0, higher_is_worse=True),
            BundleMetricSpec(slug="metric_b", label="Metric B", column="metric_b", weight=1.0, higher_is_worse=True),
        ],
        id_columns=("state_name", "district_name"),
    )

    first_row = out[out["district_name"] == "One"].iloc[0]
    missing_row = out[out["district_name"] == "Three"].iloc[0]

    assert first_row["available_metric_count"] == 1
    assert first_row["bundle_score"] == 50.0
    assert missing_row["available_metric_count"] == 0
    assert math.isnan(float(missing_row["bundle_score"]))


def test_aggregate_state_bundle_scores_uses_simple_mean_of_district_scores() -> None:
    district_scores = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "district_name": ["One", "Two", "Three"],
            "bundle_score": [20.0, 80.0, 100.0],
        }
    )

    out = aggregate_state_bundle_scores(district_scores)
    by_state = dict(zip(out["state_name"], out["bundle_score"]))

    assert by_state == {"A": 50.0, "B": 100.0}


def test_compute_bundle_score_frame_uses_weighted_normalized_average() -> None:
    df = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "district_name": ["One", "Two", "Three"],
            "metric_a": [0.0, 50.0, 100.0],
            "metric_b": [0.0, 100.0, 100.0],
        }
    )

    out = compute_bundle_score_frame(
        df,
        metric_specs=[
            BundleMetricSpec(slug="metric_a", label="Metric A", column="metric_a", weight=0.75, higher_is_worse=True),
            BundleMetricSpec(slug="metric_b", label="Metric B", column="metric_b", weight=0.25, higher_is_worse=True),
        ],
        id_columns=("state_name", "district_name"),
    )

    scores = dict(zip(out["district_name"], out["bundle_score"]))
    assert scores["One"] == 0.0
    assert scores["Two"] == 62.5
    assert scores["Three"] == 100.0


def test_compute_bundle_score_frame_renormalizes_weights_for_available_metrics() -> None:
    df = pd.DataFrame(
        {
            "state_name": ["A", "A"],
            "district_name": ["One", "Two"],
            "metric_a": [0.0, 100.0],
            "metric_b": [10.0, None],
        }
    )

    out = compute_bundle_score_frame(
        df,
        metric_specs=[
            BundleMetricSpec(slug="metric_a", label="Metric A", column="metric_a", weight=0.8, higher_is_worse=True),
            BundleMetricSpec(slug="metric_b", label="Metric B", column="metric_b", weight=0.2, higher_is_worse=True),
        ],
        id_columns=("state_name", "district_name"),
    )

    scores = dict(zip(out["district_name"], out["bundle_score"]))
    assert scores["One"] == 20.0
    assert scores["Two"] == 100.0
