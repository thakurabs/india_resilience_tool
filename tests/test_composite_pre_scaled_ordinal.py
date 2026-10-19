"""Tests for the pre_scaled_ordinal composite scoring mode (CHG-0239).

The Water Risk composite maps fixed ordinal class codes to an absolute 0-100 score
(1..4 -> 0/33.33/66.67/100), independent of the in-scope data distribution, and
enforces a strict contract: every scored component must carry contiguous integer
class_labels. It is also district-only and must never build block masters.
"""

from __future__ import annotations

import pandas as pd
import pytest

from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec
from india_resilience_tool.compute.composite_metrics import (
    _compute_pre_scaled_ordinal_score_frame,
    _ordinal_class_bounds,
    _pre_scaled_ordinal_series,
    build_composite_metrics,
)


def test_pre_scaled_ordinal_maps_classes_to_absolute_scores() -> None:
    out = _pre_scaled_ordinal_series(
        pd.Series([1, 2, 3, 4]), min_code=1, max_code=4, higher_is_worse=True
    )
    assert [round(v, 2) for v in out] == [0.0, 33.33, 66.67, 100.0]


def test_pre_scaled_ordinal_is_absolute_for_single_class_state() -> None:
    # A state where every district is class 3 must score 66.67 (absolute), NOT the
    # 50.0 that data-driven min-max normalization would assign to a constant series.
    out = _pre_scaled_ordinal_series(
        pd.Series([3, 3, 3]), min_code=1, max_code=4, higher_is_worse=True
    )
    assert [round(v, 2) for v in out] == [66.67, 66.67, 66.67]


def test_ordinal_class_bounds_reads_registry_labels() -> None:
    assert _ordinal_class_bounds("water_scarcity_percapita") == (1, 4)
    assert _ordinal_class_bounds("water_scarcity_deterioration_2050") == (0, 3)


def test_ordinal_class_bounds_raises_without_integer_class_labels() -> None:
    with pytest.raises(ValueError):
        _ordinal_class_bounds("tas_annual_mean")  # no class_labels


def test_score_frame_raises_when_scored_component_lacks_class_labels() -> None:
    wide = pd.DataFrame({"district_key": ["a", "b"], "tas_annual_mean": [1.0, 2.0]})
    specs = [
        BundleMetricSpec(
            slug="tas_annual_mean",
            label="tas",
            column="tas_annual_mean",
            weight=1.0,
            higher_is_worse=True,
        )
    ]
    with pytest.raises(ValueError):
        _compute_pre_scaled_ordinal_score_frame(
            wide, metric_specs=specs, id_columns=["district_key"]
        )


def test_score_frame_scores_water_scarcity_absolutely() -> None:
    wide = pd.DataFrame(
        {"district_key": ["a", "b", "c", "d"], "water_scarcity_percapita": [1, 2, 3, 4]}
    )
    specs = [
        BundleMetricSpec(
            slug="water_scarcity_percapita",
            label="scarcity",
            column="water_scarcity_percapita",
            weight=1.0,
            higher_is_worse=True,
        )
    ]
    frame = _compute_pre_scaled_ordinal_score_frame(
        wide, metric_specs=specs, id_columns=["district_key"]
    )
    assert [round(v, 2) for v in frame["bundle_score"]] == [0.0, 33.33, 66.67, 100.0]


def test_composite_water_risk_is_district_only(tmp_path) -> None:
    # Block level is not in composite_water_risk supported_levels, so the builder
    # must produce no block outputs (returns nothing) even in dry-run.
    written_block = build_composite_metrics(
        levels=["block"],
        states=["Telangana"],
        composite_slugs=["composite_water_risk"],
        data_dir=tmp_path,
        dry_run=True,
    )
    assert written_block == []
