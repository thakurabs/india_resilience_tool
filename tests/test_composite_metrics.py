from __future__ import annotations

import math

import pandas as pd

from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec, compute_bundle_score_frame
from india_resilience_tool.compute.composite_metrics import (
    build_composite_metrics,
    compute_composite_master_frame,
)
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG


def _write_component_master(
    tmp_path,
    *,
    slug: str,
    state_name: str,
    filename: str,
    df: pd.DataFrame,
) -> None:
    root = tmp_path / "processed" / slug / state_name
    root.mkdir(parents=True, exist_ok=True)
    df.to_csv(root / filename, index=False)


def test_compute_composite_master_frame_matches_current_district_weighted_method(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    id_frame = pd.DataFrame(
        {
            "state": [state_name, state_name, state_name],
            "district": ["A", "B", "C"],
            "district_key": ["a", "b", "c"],
        }
    )
    values = {
        "spi3_count_events_lt_minus1": [1.0, 4.0, None],
        "spi6_count_events_lt_minus1": [2.0, 6.0, None],
        "spi12_count_events_lt_minus1": [3.0, 8.0, None],
    }
    for slug, raw in values.items():
        df = id_frame.copy()
        df[f"{slug}__ssp585__2040-2060__mean"] = raw
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    wide = id_frame.copy()
    for slug, raw in values.items():
        wide[slug] = raw
    expected = compute_bundle_score_frame(
        wide.rename(columns={"state": "state_name", "district": "district_name"}),
        metric_specs=[
            BundleMetricSpec(
                slug="spi3_count_events_lt_minus1",
                label="SPI3",
                column="spi3_count_events_lt_minus1",
                weight=0.20,
                higher_is_worse=True,
            ),
            BundleMetricSpec(
                slug="spi6_count_events_lt_minus1",
                label="SPI6",
                column="spi6_count_events_lt_minus1",
                weight=0.30,
                higher_is_worse=True,
            ),
            BundleMetricSpec(
                slug="spi12_count_events_lt_minus1",
                label="SPI12",
                column="spi12_count_events_lt_minus1",
                weight=0.50,
                higher_is_worse=True,
            ),
        ],
        id_columns=("state_name", "district_name"),
    )
    expected_scores = dict(zip(expected["district_name"], expected["bundle_score"]))
    observed_scores = dict(zip(out["district"], out["composite_drought_risk__ssp585__2040-2060__mean"]))

    assert observed_scores["A"] == expected_scores["A"]
    assert observed_scores["B"] == expected_scores["B"]
    assert math.isnan(float(observed_scores["C"]))


def test_compute_composite_master_frame_uses_schema_intersection_for_available_pairs(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_block.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    id_frame = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "A"],
            "block": ["B1", "B2"],
            "block_key": ["b1", "b2"],
        }
    )
    for slug in spec.component_metric_slugs:
        df = id_frame.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0, 2.0]
        if slug != "spi12_count_events_lt_minus1":
            df[f"{slug}__ssp585__2040-2060__mean"] = [3.0, 4.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert "composite_drought_risk__ssp245__2020-2040__mean" in out.columns
    assert "composite_drought_risk__ssp585__2040-2060__mean" not in out.columns


def test_build_composite_metrics_writes_legacy_csv_and_parquet(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    base = pd.DataFrame(
        {
            "state": [state_name],
            "district": ["A"],
            "district_key": ["a"],
        }
    )
    for slug in spec.component_metric_slugs:
        df = base.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    written = build_composite_metrics(
        levels=("district",),
        states=(state_name,),
        composite_slugs=(spec.composite_slug,),
        data_dir=tmp_path,
        overwrite=True,
        dry_run=False,
        quiet=True,
    )

    target = tmp_path / "processed" / spec.composite_slug / state_name / filename
    assert target in written
    assert target.exists()
    assert target.with_suffix(".parquet").exists()


def test_compute_composite_master_frame_uses_registry_periods_metric_col_for_component_columns(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Heat Stress")
    assert spec is not None

    id_frame = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "B"],
            "district_key": ["a", "b"],
        }
    )
    for metric_slug in spec.component_metric_slugs:
        registry_spec = METRICS_BY_SLUG[metric_slug]
        metric_base = registry_spec.periods_metric_col or registry_spec.value_col or metric_slug
        df = id_frame.copy()
        df[f"{metric_base}__ssp585__2040-2060__mean"] = [1.0, 2.0]
        _write_component_master(tmp_path, slug=metric_slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert "composite_heat_stress__ssp585__2040-2060__mean" in out.columns
    assert out["composite_heat_stress__ssp585__2040-2060__mean"].notna().all()


def test_compute_composite_master_frame_derives_missing_district_keys_from_names(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    base = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "B"],
        }
    )
    for slug in spec.component_metric_slugs:
        df = base.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0, 2.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert "district_key" in out.columns
    assert out["district_key"].tolist() == ["telangana|a", "telangana|b"]
    assert "composite_drought_risk__ssp245__2020-2040__mean" in out.columns


def test_compute_composite_master_frame_derives_missing_block_keys_from_names(tmp_path) -> None:
    state_name = "Telangana"
    filename = "master_metrics_by_block.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    base = pd.DataFrame(
        {
            "state": [state_name, state_name],
            "district": ["A", "A"],
            "block": ["North", "South"],
        }
    )
    for slug in spec.component_metric_slugs:
        df = base.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0, 2.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="block",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert "block_key" in out.columns
    assert out["block_key"].tolist() == ["telangana|a|north", "telangana|a|south"]
    assert "composite_drought_risk__ssp245__2020-2040__mean" in out.columns
