"""Regression tests for Cold Risk direction-of-risk handling.

Locks in the contract that cold temperature-magnitude metrics carry
``rank_higher_is_worse=False`` end-to-end (registry -> VARIABLES -> ranking,
normalization, and composite scoring), and that the other Cold Risk components
remain higher-is-worse.

CHG-0001 (audit issue A1-A6).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from india_resilience_tool.analysis.bundle_scores import (  # noqa: E402
    BundleMetricSpec,
    compute_bundle_score_frame,
    compute_metric_driver_frame,
    normalize_metric_series,
    normalized_metric_column,
)
from india_resilience_tool.analysis.metrics import compute_position_stats  # noqa: E402
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG  # noqa: E402
from india_resilience_tool.config.variables import VARIABLES  # noqa: E402


COLD_MAGNITUDE_SLUGS = (
    "tas_winter_mean",
    "tasmin_winter_mean",
    "tnn_annual_min",
    "tasmin_winter_min",
)

COLD_HIGHER_IS_WORSE_SLUGS = (
    "tnle10_cold_nights",
    "tnle5_severe_cold_nights",
    "txle15_cold_days",
    "tx10p_cool_days_pct",
    "tn10p_cool_nights_pct",
    "csdi_cold_spell_days",
    "tnle10_consecutive_cold_nights",
)


# ---------------------------------------------------------------------------
# Registry-level assertions
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("slug", COLD_MAGNITUDE_SLUGS)
def test_cold_magnitude_metrics_are_lower_is_worse_in_registry(slug: str) -> None:
    spec = METRICS_BY_SLUG[slug]
    assert spec.rank_higher_is_worse is False, (
        f"{slug}: cold-magnitude metric must declare rank_higher_is_worse=False"
    )


@pytest.mark.parametrize("slug", COLD_HIGHER_IS_WORSE_SLUGS)
def test_cold_count_and_spell_metrics_remain_higher_is_worse(slug: str) -> None:
    spec = METRICS_BY_SLUG[slug]
    assert spec.rank_higher_is_worse is True, (
        f"{slug}: count/percentile/spell cold metric must remain higher-is-worse"
    )


# ---------------------------------------------------------------------------
# Cold-percentile params: baseline (1990, 2010) and strict-< (exceed_ge=False).
# CHG-0004 (audit issue D11) + CHG-0005 (audit issue C10, Cold-Risk-scoped).
# ---------------------------------------------------------------------------
COLD_PERCENTILE_SLUGS = (
    "tx10p_cool_days_pct",
    "tn10p_cool_nights_pct",
    "csdi_cold_spell_days",
)


@pytest.mark.parametrize("slug", COLD_PERCENTILE_SLUGS)
def test_cold_percentile_metric_uses_1990_2010_baseline(slug: str) -> None:
    spec = METRICS_BY_SLUG[slug]
    baseline = tuple((spec.params or {}).get("baseline_years", ()))
    assert baseline == (1990, 2010), (
        f"{slug}: baseline_years must be (1990, 2010), got {baseline}"
    )


@pytest.mark.parametrize("slug", COLD_PERCENTILE_SLUGS)
def test_cold_percentile_metric_uses_strict_less_than(slug: str) -> None:
    spec = METRICS_BY_SLUG[slug]
    params = spec.params or {}
    assert params.get("direction") == "below", (
        f"{slug}: direction must remain 'below'"
    )
    assert params.get("exceed_ge") is False, (
        f"{slug}: exceed_ge must be False (strict < threshold) per ETCCDI canon"
    )


# ---------------------------------------------------------------------------
# Registry -> dashboard VARIABLES propagation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("slug", COLD_MAGNITUDE_SLUGS)
def test_variables_propagates_lower_is_worse_flag(slug: str) -> None:
    assert slug in VARIABLES, f"{slug}: missing from dashboard VARIABLES"
    assert VARIABLES[slug]["rank_higher_is_worse"] is False, (
        f"{slug}: dashboard VARIABLES did not propagate rank_higher_is_worse=False"
    )


@pytest.mark.parametrize("slug", COLD_HIGHER_IS_WORSE_SLUGS)
def test_variables_propagates_higher_is_worse_flag(slug: str) -> None:
    assert slug in VARIABLES, f"{slug}: missing from dashboard VARIABLES"
    assert VARIABLES[slug]["rank_higher_is_worse"] is True, (
        f"{slug}: dashboard VARIABLES inconsistent for higher-is-worse metric"
    )


# ---------------------------------------------------------------------------
# Direction-aware rank/percentile (analysis.metrics.compute_position_stats)
# ---------------------------------------------------------------------------
def test_compute_position_stats_lower_is_worse_ranks_coldest_first() -> None:
    values = pd.Series([-2.0, 5.0, 12.0])

    coldest = compute_position_stats(values, -2.0, higher_is_worse=False)
    middle = compute_position_stats(values, 5.0, higher_is_worse=False)
    warmest = compute_position_stats(values, 12.0, higher_is_worse=False)

    # Rank: worst (coldest) is rank 1 when lower-is-worse.
    assert coldest.rank == 1
    assert middle.rank == 2
    assert warmest.rank == 3

    # Percentile is direction-aware (worst -> 100). For lower-is-worse this means
    # the coldest value sits at percentile 100 and the warmest at 1/N * 100.
    assert coldest.percentile == pytest.approx(100.0, rel=1e-9)
    assert middle.percentile == pytest.approx(2.0 / 3.0 * 100.0, rel=1e-9)
    assert warmest.percentile == pytest.approx(1.0 / 3.0 * 100.0, rel=1e-9)

    # Direction asymmetry sanity: higher_is_worse=True flips rank order.
    coldest_hi = compute_position_stats(values, -2.0, higher_is_worse=True)
    assert coldest_hi.rank == 3
    assert coldest_hi.percentile == pytest.approx(1.0 / 3.0 * 100.0, rel=1e-9)


# ---------------------------------------------------------------------------
# Direction-aware normalization (analysis.bundle_scores.normalize_metric_series)
# ---------------------------------------------------------------------------
def test_normalize_metric_series_inverts_for_lower_is_worse() -> None:
    raw = pd.Series([-2.0, 5.0, 12.0])

    higher = normalize_metric_series(raw, higher_is_worse=True)
    lower = normalize_metric_series(raw, higher_is_worse=False)

    # Higher-is-worse: hottest -> 100, coldest -> 0.
    assert higher.iloc[0] == pytest.approx(0.0)
    assert higher.iloc[2] == pytest.approx(100.0)

    # Lower-is-worse (cold magnitude): coldest -> 100, warmest -> 0.
    assert lower.iloc[0] == pytest.approx(100.0)
    assert lower.iloc[2] == pytest.approx(0.0)


def test_normalize_metric_series_all_identical_returns_fifty() -> None:
    raw = pd.Series([3.0, 3.0, 3.0])
    norm = normalize_metric_series(raw, higher_is_worse=False)
    assert norm.tolist() == [50.0, 50.0, 50.0]


# ---------------------------------------------------------------------------
# End-to-end bundle score: cold geography must score higher than warm one
# ---------------------------------------------------------------------------
def test_compute_bundle_score_frame_directionality_for_cold_components() -> None:
    df = pd.DataFrame(
        {
            "district": ["cold_dist", "warm_dist"],
            # Lower-is-worse cold-magnitude metric.
            "winter_tasmin_mean_C": [4.0, 18.0],
            # Higher-is-worse cold-night count.
            "days_tn_le_10C": [40, 2],
        }
    )

    specs = [
        BundleMetricSpec(
            slug="tasmin_winter_mean",
            label="Winter Min Temperature (DJF Mean)",
            column="winter_tasmin_mean_C",
            weight=0.5,
            higher_is_worse=False,
        ),
        BundleMetricSpec(
            slug="tnle10_cold_nights",
            label="Cold Nights (TN <= 10C)",
            column="days_tn_le_10C",
            weight=0.5,
            higher_is_worse=True,
        ),
    ]

    out = compute_bundle_score_frame(df, metric_specs=specs, id_columns=["district"])
    out_by_district = out.set_index("district")

    cold_score = out_by_district.loc["cold_dist", "bundle_score"]
    warm_score = out_by_district.loc["warm_dist", "bundle_score"]

    assert cold_score > warm_score, (
        f"cold district ({cold_score}) must score higher cold-risk than warm district ({warm_score}) "
        "after directional normalization"
    )

    # Both components should pull the cold district toward 100 and the warm district toward 0.
    norm_tasmin = normalized_metric_column("tasmin_winter_mean")
    norm_nights = normalized_metric_column("tnle10_cold_nights")
    assert out_by_district.loc["cold_dist", norm_tasmin] == pytest.approx(100.0)
    assert out_by_district.loc["warm_dist", norm_tasmin] == pytest.approx(0.0)
    assert out_by_district.loc["cold_dist", norm_nights] == pytest.approx(100.0)
    assert out_by_district.loc["warm_dist", norm_nights] == pytest.approx(0.0)


def test_compute_metric_driver_frame_uses_normalized_contribution() -> None:
    df = pd.DataFrame(
        {
            "district": ["cold_dist", "warm_dist"],
            "winter_tasmin_mean_C": [2.0, 20.0],
            "days_tn_le_10C": [1, 1],
        }
    )

    specs = [
        BundleMetricSpec(
            slug="tasmin_winter_mean",
            label="Winter Min Temperature (DJF Mean)",
            column="winter_tasmin_mean_C",
            weight=0.5,
            higher_is_worse=False,
        ),
        BundleMetricSpec(
            slug="tnle10_cold_nights",
            label="Cold Nights (TN <= 10C)",
            column="days_tn_le_10C",
            weight=0.5,
            higher_is_worse=True,
        ),
    ]

    bundle_frame = compute_bundle_score_frame(df, metric_specs=specs, id_columns=["district"])
    cold_only = bundle_frame[bundle_frame["district"] == "cold_dist"].copy()

    drivers = compute_metric_driver_frame(cold_only, metric_specs=specs)

    # The cold-magnitude metric should dominate the cold district's driver list.
    assert not drivers.empty
    top_driver = drivers.iloc[0]
    assert top_driver["metric_slug"] == "tasmin_winter_mean", (
        f"expected tasmin_winter_mean to be top driver for cold geography, got {top_driver['metric_slug']}"
    )
    assert top_driver["normalized_score"] == pytest.approx(100.0)
