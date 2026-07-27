from __future__ import annotations

import math

import pandas as pd

from india_resilience_tool.compute.composite_metrics import (
    build_composite_metrics,
    compute_composite_master_frame,
    parse_args,
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
        "spi3_max_spell_lt_minus1": [1.0, 5.0, None],
        "spi6_max_spell_lt_minus1": [2.0, 7.0, None],
        "spi12_max_spell_lt_minus1": [3.0, 9.0, None],
    }
    for slug, raw in values.items():
        df = id_frame.copy()
        df[f"{slug}__historical__1990-2010__mean"] = raw
        df[f"{slug}__ssp585__2040-2060__mean"] = raw
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    observed_scores = dict(zip(out["district"], out["composite_drought_risk__ssp585__2040-2060__mean"]))

    assert observed_scores["A"] == 0.0
    assert observed_scores["B"] == 100.0
    assert math.isnan(float(observed_scores["C"]))


def test_drought_risk_composite_spec_uses_per_period_normalization() -> None:
    """CHG-0061: Drought Risk uses per-period cohort normalization like the other
    thematic bundles. Guards against regressing to the baseline-anchored mode,
    which floored end-century scores to 0 when the projected SPI drought field
    fell entirely below the 1990-2010 inter-district baseline envelope."""
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None
    assert spec.normalization == "per_period"


def test_drought_composite_per_period_keeps_spatial_spread_when_future_below_history(tmp_path) -> None:
    """Regression for the uniform-map bug: when every projected component value sits
    far below the historical baseline (the real end-century SSP5-8.5 case), per-period
    normalization must still produce a graded 0-100 spatial score rather than collapsing
    every district to 0 (the retired baseline-anchored floor)."""
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
    # Future values lie entirely below the historical envelope (80-100): under the
    # retired baseline-anchored mode all three clip to 0; under per-period they
    # spread across the cohort min-max to 0 / 50 / 100.
    for slug in spec.component_metric_slugs:
        df = id_frame.copy()
        df[f"{slug}__historical__1990-2010__mean"] = [80.0, 90.0, 100.0]
        df[f"{slug}__ssp585__2060-2080__mean"] = [3.0, 5.0, 7.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(spec, level="district", state_name=state_name, data_dir=tmp_path)
    by_district = out.set_index("district")
    col = "composite_drought_risk__ssp585__2060-2080__mean"

    assert by_district.loc["A", col] == 0.0
    assert by_district.loc["B", col] == 50.0
    assert by_district.loc["C", col] == 100.0
    # Core guard: the score is not a degenerate single value across the state.
    assert out[col].nunique(dropna=True) == 3
    assert float(out[col].max()) > 0.0


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


def test_build_composite_metrics_rejects_retired_agriculture_slug_as_normal_target(tmp_path) -> None:
    try:
        build_composite_metrics(
            levels=("district",),
            states=("Telangana",),
            composite_slugs=("composite_agriculture_growing_conditions",),
            data_dir=tmp_path,
            quiet=True,
        )
    except ValueError as exc:
        assert "Unsupported composite metric selection" in str(exc)
    else:
        raise AssertionError("retired agriculture composite slug should be rejected")


def test_build_composite_metrics_prune_retired_honors_dry_run(tmp_path) -> None:
    retired_root = tmp_path / "processed" / "composite_agriculture_growing_conditions"
    retired_root.mkdir(parents=True)
    marker = retired_root / "marker.txt"
    marker.write_text("legacy", encoding="utf-8")
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None
    base = pd.DataFrame({"state": [state_name], "district": ["A"], "district_key": ["a"]})
    for slug in spec.component_metric_slugs:
        df = base.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    dry_run_paths = build_composite_metrics(
        levels=("district",),
        states=(state_name,),
        composite_slugs=(spec.composite_slug,),
        data_dir=tmp_path,
        dry_run=True,
        prune_retired=True,
        quiet=True,
    )
    assert retired_root in dry_run_paths
    assert marker.exists()

    pruned_paths = build_composite_metrics(
        levels=("district",),
        states=(state_name,),
        composite_slugs=(spec.composite_slug,),
        data_dir=tmp_path,
        dry_run=False,
        prune_retired=True,
        quiet=True,
    )
    assert retired_root in pruned_paths
    assert not retired_root.exists()


def test_composite_metrics_parse_args_accepts_prune_retired() -> None:
    args = parse_args(["--prune-retired", "--dry-run"])
    assert args.prune_retired is True


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


def test_compute_composite_master_frame_propagates_idw_provenance(tmp_path) -> None:
    """CHG-0306(c): a composite unit drawing on any idw-filled component reads
    ``climate_fill_method="idw"``; units with only native components read
    ``"native"``. Guards the provenance read against being taken from
    ``_build_wide_component_frame`` (which strips the flag)."""
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
    component_slugs = list(spec.component_metric_slugs)
    for i, slug in enumerate(component_slugs):
        df = id_frame.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0, 2.0, 3.0]
        # Only the first component is idw, and only for district A. The composite
        # for A must still read idw (any-component rule); B and C read native.
        if i == 0:
            df["climate_fill_method"] = ["idw", "native", "native"]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(
        spec,
        level="district",
        state_name=state_name,
        data_dir=tmp_path,
    )

    assert "climate_fill_method" in out.columns
    by_district = dict(zip(out["district"], out["climate_fill_method"]))
    assert by_district == {"A": "idw", "B": "native", "C": "native"}


def test_compute_composite_master_frame_no_provenance_column_when_absent(tmp_path) -> None:
    """When no component master carries the flag, the composite output is
    byte-identical (no ``climate_fill_method`` column added)."""
    state_name = "Telangana"
    filename = "master_metrics_by_district.csv"
    spec = get_composite_metric_for_bundle("Drought Risk")
    assert spec is not None

    id_frame = pd.DataFrame(
        {"state": [state_name, state_name], "district": ["A", "B"], "district_key": ["a", "b"]}
    )
    for slug in spec.component_metric_slugs:
        df = id_frame.copy()
        df[f"{slug}__ssp245__2020-2040__mean"] = [1.0, 2.0]
        _write_component_master(tmp_path, slug=slug, state_name=state_name, filename=filename, df=df)

    out = compute_composite_master_frame(spec, level="district", state_name=state_name, data_dir=tmp_path)
    assert "climate_fill_method" not in out.columns
