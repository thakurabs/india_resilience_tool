from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import Polygon

from india_resilience_tool.compute.spi_adapter import Distribution, SPIResult
import india_resilience_tool.compute.spi_adapter as spi_adapter
from tools.pipeline import compute_indices_multiprocess as CMP


def _daily_series() -> xr.DataArray:
    time = pd.date_range("2030-01-01", periods=3, freq="D")
    return xr.DataArray([300.0, 301.0, 302.0], coords={"time": time}, dims=["time"])


def _monthly_series() -> xr.DataArray:
    time = pd.date_range("2030-01-01", periods=12, freq="MS")
    return xr.DataArray(np.linspace(-1.0, 1.0, 12), coords={"time": time}, dims=["time"])


def test_add_unit_fields_from_key_populates_sub_basin_fields() -> None:
    row: dict[str, object] = {}
    CMP._add_unit_fields_from_key(row, "Godavari Basin||Pranhita", "sub_basin")
    assert row == {"basin": "Godavari Basin", "sub_basin": "Pranhita"}


def test_tx90p_helper_emits_basin_fields_for_hydro(monkeypatch) -> None:
    monkeypatch.setattr(
        CMP,
        "_collect_daily_mean_by_unit",
        lambda *args, **kwargs: {"Godavari Basin": _daily_series()},
    )
    monkeypatch.setattr(
        CMP,
        "_compute_tx90p_etccdi_yearly",
        lambda **kwargs: {2030: 12.5},
    )

    rows = CMP._compute_tx90p_rows_for_metric(
        metric={"slug": "tx90p_hot_days_pct", "var": "tasmax", "value_col": "tx90p"},
        model="ACCESS-CM2",
        scenario="historical",
        scenario_conf={},
        year_to_paths={2030: {"tasmax": Path("ignored.nc")}},
        masks={"Godavari Basin": xr.DataArray([True])},
        level="basin",
    )

    assert rows == [
        {
            "year": 2030,
            "value": 12.5,
            "tx90p": 12.5,
            "source_file": "",
            "basin": "Godavari Basin",
        }
    ]


def test_validate_output_unit_fields_raises_clear_error_for_invalid_hydro_rows() -> None:
    df = pd.DataFrame({"year": [2030], "value": [1.0], "basin": [np.nan]})

    with pytest.raises(ValueError) as excinfo:
        CMP._validate_output_unit_fields(
            df,
            level="basin",
            slug="spi3_drought_index",
            model="ACCESS-CM2",
            scenario="historical",
            stage_label="yearly outputs",
        )

    message = str(excinfo.value)
    assert "Invalid hydro identity values" in message
    assert "spi3_drought_index" in message
    assert "ACCESS-CM2" in message
    assert "historical" in message


def test_load_boundaries_rejects_blank_hydro_identity(monkeypatch) -> None:
    gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": [""], "hydro_level": ["basin"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    monkeypatch.setattr(CMP.gpd, "read_file", lambda path: gdf)

    with pytest.raises(ValueError, match="Hydro boundary inputs contain blank identity values"):
        CMP.load_boundaries(Path("ignored.geojson"), level="basin")


def test_monthly_spi_csv_path_supports_sub_basins() -> None:
    out = spi_adapter._monthly_spi_csv_path(
        metric_root_path=Path("/tmp/metric"),
        state_name="hydro",
        level_folder="sub_basins",
        level="sub_basin",
        unit_key="Godavari Basin||Pranhita",
        model="ACCESS-CM2",
        scenario="ssp245",
    )

    assert out == Path(
        "/tmp/metric/hydro/sub_basins/Godavari_Basin/Pranhita/ACCESS-CM2/ssp245/Pranhita_monthly.csv"
    )


def test_compute_spi_rows_climate_indices_emits_sub_basin_fields(monkeypatch) -> None:
    result = SPIResult(
        monthly_spi=_monthly_series(),
        annual_spi=xr.DataArray([0.0], coords={"year": [2030]}, dims=["year"]),
        scale_months=3,
        distribution=Distribution.GAMMA,
        calibration_years=(1981, 2010),
        data_years=(2030, 2030),
        valid_months=12,
    )
    monkeypatch.setattr(spi_adapter, "CLIMATE_INDICES_AVAILABLE", True)
    monkeypatch.setattr(spi_adapter, "compute_spi_for_unit", lambda **kwargs: result)

    rows = spi_adapter.compute_spi_rows_climate_indices(
        metric={
            "slug": "spi3_drought_index",
            "var": "pr",
            "value_col": "spi3_index",
            "params": {"distribution": "gamma", "min_months_per_year": 1},
        },
        model="ACCESS-CM2",
        scenario="historical",
        scenario_conf={"periods": {}},
        scen_monthly_by_unit={"Godavari Basin||Pranhita": _monthly_series()},
        calib_monthly_by_unit={"Godavari Basin||Pranhita": _monthly_series()},
        masks={"Godavari Basin||Pranhita": xr.DataArray([True])},
        level="sub_basin",
        baseline_years=(1981, 2010),
        scale_months=3,
        year_to_paths={},
    )

    assert rows
    assert rows[0]["basin"] == "Godavari Basin"
    assert rows[0]["sub_basin"] == "Pranhita"
    assert "district" not in rows[0]


def test_build_processing_task_plan_tracks_metric_specific_unrunnable_reasons(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(
        CMP,
        "METRICS",
        [
            {"slug": "tas_annual_mean", "var": "tas"},
            {"slug": "spi3_drought_index", "var": "pr"},
        ],
    )
    monkeypatch.setattr(CMP, "MODELS", ["CanESM5"])
    monkeypatch.setattr(CMP, "SCENARIOS", {"historical": {"subdir": "historical/tas", "periods": {}}})

    def _fake_var_data_dir(_data_root: Path, _subdir: str, varname: str, model: str) -> Path:
        path = tmp_path / f"{varname}_{model}"
        if varname == "tas":
            path.mkdir(parents=True, exist_ok=True)
        return path

    monkeypatch.setattr(CMP, "var_data_dir", _fake_var_data_dir)
    monkeypatch.setattr(
        CMP,
        "yearly_files_for_dir",
        lambda path: {2030: path / "2030.nc"} if path.exists() else {},
    )

    plan = CMP.build_processing_task_plan(
        metrics_filter=["tas_annual_mean", "spi3_drought_index"],
        models_filter=["CanESM5"],
        scenarios_filter=["historical"],
        level="basin",
        state="hydro",
    )

    assert [task.slug for task in plan.tasks] == ["tas_annual_mean"]
    assert plan.skipped_reasons_by_metric["spi3_drought_index"] == ("no_available_years",)
    assert plan.skipped_counts_by_reason["no_available_years"] == 1


def test_task_completion_marker_validates_output_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(CMP, "_boundary_signature", lambda level, state: ("/tmp/boundary.geojson", 123))

    task = CMP.ProcessingTask(
        metric_idx=0,
        slug="tas_annual_mean",
        model="CanESM5",
        scenario="historical",
        scenario_conf={},
        task_id=0,
        total_tasks=1,
        level="district",
        state_name="Telangana",
        required_vars=("tas",),
        common_years_hash="abc123",
        scope_name="Telangana",
    )
    out_dir = tmp_path / "processed" / "tas_annual_mean" / "Telangana" / "districts" / "Hanumakonda" / "CanESM5" / "historical"
    out_dir.mkdir(parents=True)
    (out_dir / "Hanumakonda_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")
    (out_dir / "Hanumakonda_periods.csv").write_text("period,value\n2030-2040,1.0\n", encoding="utf-8")

    CMP._write_task_completion_marker(task, output_meta={"yearly_file_count": 1, "period_file_count": 1})

    assert CMP.task_completion_marker_valid(task) is True

    (out_dir / "Hanumakonda_yearly.csv").unlink()

    assert CMP.task_completion_marker_valid(task) is False


def test_build_processing_task_plan_marks_no_tasks_after_filters_per_metric(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(CMP, "METRICS", [{"slug": "tas_annual_mean", "var": "tas"}])
    monkeypatch.setattr(CMP, "MODELS", ["CanESM5"])
    monkeypatch.setattr(CMP, "SCENARIOS", {"historical": {"subdir": "historical/tas", "periods": {}}})
    monkeypatch.setattr(CMP, "var_data_dir", lambda *_args, **_kwargs: tmp_path / "tas")
    monkeypatch.setattr(CMP, "yearly_files_for_dir", lambda _path: {2030: tmp_path / "2030.nc"})

    plan = CMP.build_processing_task_plan(
        metrics_filter=["tas_annual_mean"],
        models_filter=["MissingModel"],
        scenarios_filter=["historical"],
        level="basin",
        state="hydro",
    )

    assert plan.tasks == ()
    assert plan.skipped_reasons_by_metric["tas_annual_mean"] == ("no_tasks_after_filters",)
    assert "no_tasks_after_filters" not in plan.skipped_counts_by_reason


def test_ensemble_completion_marker_validates_expected_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(CMP, "_boundary_signature", lambda level, state: ("/tmp/boundary.geojson", 123))

    out_dir = (
        tmp_path
        / "processed"
        / "tas_annual_mean"
        / "hydro"
        / "basins"
        / "ensembles"
        / "Godavari Basin"
        / "historical"
    )
    out_dir.mkdir(parents=True)
    out_file = out_dir / "Godavari Basin_yearly_ensemble.csv"
    out_file.write_text("year,ensemble_mean\n2030,1.0\n", encoding="utf-8")
    stale_scenario_dir = out_dir.parent / "ssp245"
    stale_scenario_dir.mkdir(parents=True)
    (stale_scenario_dir / "Godavari Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,2.0\n",
        encoding="utf-8",
    )

    CMP._write_ensemble_completion_marker(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
        expected_output_count=1,
    )

    assert CMP.ensemble_completion_marker_valid(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    ) is True

    out_file.unlink()

    assert CMP.ensemble_completion_marker_valid(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    ) is False


def test_compute_ensembles_for_metric_treats_zero_hydro_outputs_as_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    marker_calls: list[dict[str, object]] = []

    monkeypatch.setattr(CMP, "metric_root", lambda slug: tmp_path / slug)
    monkeypatch.setattr(
        CMP,
        "compute_ensembles_generic",
        lambda *args, **kwargs: CMP.EnsembleBuildStats(
            written_count=0,
            expected_output_count=0,
            missing_expected_output_count=0,
            skipped_input_count=0,
            failure_count=0,
            errors=(),
        ),
    )
    monkeypatch.setattr(
        CMP,
        "_write_ensemble_completion_marker",
        lambda **kwargs: marker_calls.append(kwargs),
    )

    result = CMP._compute_ensembles_for_metric(
        ("tas_annual_mean", "basin", "hydro", ("ACCESS-CM2",), ("historical",))
    )

    assert result.status == "failed"
    assert "expected=0" in result.summary
    assert marker_calls == []


def test_ensemble_filter_scope_marker_isolation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(CMP, "_boundary_signature", lambda level, state: ("/tmp/boundary.geojson", 123))

    out_dir = (
        tmp_path
        / "processed"
        / "tas_annual_mean"
        / "hydro"
        / "basins"
        / "ensembles"
        / "Godavari Basin"
        / "historical"
    )
    out_dir.mkdir(parents=True)
    (out_dir / "Godavari Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,1.0\n",
        encoding="utf-8",
    )

    CMP._write_ensemble_completion_marker(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
        expected_output_count=1,
    )

    assert CMP.ensemble_completion_marker_valid(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    ) is True
    assert CMP.ensemble_completion_marker_valid(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=None,
        allowed_scenarios=None,
    ) is False


def test_clean_ensemble_yearly_frame_rejects_ambiguous_value_columns() -> None:
    df = pd.DataFrame(
        {
            "year": [2030],
            "value_a": [1.0],
            "value_b": [2.0],
        }
    )

    cleaned, reason = CMP._clean_ensemble_yearly_frame(
        df,
        metadata_columns={"year", "model", "scenario", "source_file"},
        model_name="ACCESS-CM2",
    )

    assert cleaned is None
    assert reason == "ambiguous_value_column"


def test_filter_hydro_units_to_climate_extent_keeps_only_intersecting_basins() -> None:
    gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["03", "99"],
            "basin_name": ["Godavari Basin", "Indus Basin"],
        },
        geometry=[
            Polygon([(76.7, 15.2), (77.0, 15.2), (77.0, 15.5), (76.7, 15.5)]),
            Polygon([(70.0, 30.0), (71.0, 30.0), (71.0, 31.0), (70.0, 31.0)]),
        ],
        crs="EPSG:4326",
    )
    sample_ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.ones((1, 2, 2), dtype=float),
                coords={"time": [0], "lat": [15.125, 20.875], "lon": [76.625, 81.875]},
                dims=("time", "lat", "lon"),
            )
        }
    )

    eligible_gdf, excluded_df = CMP._filter_hydro_units_to_climate_extent(
        gdf,
        sample_ds,
        level="basin",
        slug="tas_annual_mean",
        model="ACCESS-CM2",
        scenario="historical",
    )

    assert eligible_gdf["basin_name"].tolist() == ["Godavari Basin"]
    assert excluded_df[["basin_name", "eligible_for_processing", "reason"]].to_dict("records") == [
        {
            "basin_name": "Indus Basin",
            "eligible_for_processing": False,
            "reason": "outside_climate_extent",
        }
    ]


def test_compute_basin_ensembles_respects_model_and_scenario_filters(
    tmp_path: Path,
) -> None:
    level_root = tmp_path / "hydro" / "basins"
    good_dir = level_root / "Godavari_Basin" / "ACCESS-CM2" / "historical"
    stale_scenario_dir = level_root / "Godavari_Basin" / "ACCESS-CM2" / "ssp245"
    stale_model_dir = level_root / "Godavari_Basin" / "CanESM5" / "historical"
    for directory in (good_dir, stale_scenario_dir, stale_model_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (good_dir / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")
    (stale_scenario_dir / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")
    (stale_model_dir / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,3.0\n", encoding="utf-8")

    stats = CMP._compute_basin_ensembles(
        level_root,
        level_root / "ensembles",
        slug="tas_annual_mean",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    )

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert stats.failure_count == 0
    assert stats.missing_expected_output_count == 0
    assert (level_root / "ensembles" / "Godavari_Basin" / "historical" / "Godavari_Basin_yearly_ensemble.csv").exists()
    assert not (level_root / "ensembles" / "Godavari_Basin" / "ssp245").exists()


def test_run_pipeline_parallel_skip_existing_uses_filter_aware_ensemble_markers(
    monkeypatch,
) -> None:
    task = CMP.ProcessingTask(
        metric_idx=0,
        slug="tas_annual_mean",
        model="ACCESS-CM2",
        scenario="historical",
        scenario_conf={},
        task_id=0,
        total_tasks=1,
        level="basin",
        state_name="hydro",
        required_vars=("tas",),
        common_years_hash="abc123",
        scope_name="hydro",
    )
    monkeypatch.setattr(
        CMP,
        "build_processing_task_plan",
        lambda **kwargs: CMP.ProcessingTaskPlan(
            level="basin",
            scope_name="hydro",
            selected_metrics=("tas_annual_mean",),
            tasks=(task,),
            skipped_counts_by_reason={},
            skipped_reasons_by_metric={},
        ),
    )
    marker_checks: list[dict[str, object]] = []
    monkeypatch.setattr(CMP, "task_completion_marker_valid", lambda _task: True)
    monkeypatch.setattr(
        CMP,
        "ensemble_completion_marker_valid",
        lambda **kwargs: marker_checks.append(kwargs) or False,
    )
    monkeypatch.setattr(CMP, "get_boundary_path", lambda level: Path("ignored.geojson"))
    monkeypatch.setattr(CMP, "load_boundaries", lambda *args, **kwargs: gpd.GeoDataFrame())
    monkeypatch.setattr(
        CMP,
        "_compute_ensembles_for_metric",
        lambda args: CMP.EnsembleJobResult(
            slug=args[0],
            level=args[1],
            scope_name=args[2],
            status="success",
            written_count=1,
            expected_output_count=1,
            missing_expected_output_count=0,
            skipped_input_count=0,
            failure_count=0,
            summary="ok",
            errors=(),
            skipped_reasons=(),
        ),
    )

    result = CMP.run_pipeline_parallel(
        num_workers=1,
        metrics_filter=["tas_annual_mean"],
        models_filter=["ACCESS-CM2"],
        scenarios_filter=["historical"],
        level="basin",
        state="hydro",
        skip_existing=True,
    )

    assert result.compute_failed_count == 0
    assert len(result.ensemble_results) == 1
    assert marker_checks == [
        {
            "slug": "tas_annual_mean",
            "level": "basin",
            "scope_name": "hydro",
            "allowed_models": ("ACCESS-CM2",),
            "allowed_scenarios": ("historical",),
        }
    ]


def test_main_returns_nonzero_and_logs_compact_summary_on_ensemble_failure(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    failed_job = CMP.EnsembleJobResult(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        status="failed",
        written_count=0,
        expected_output_count=1,
        missing_expected_output_count=1,
        skipped_input_count=2,
        failure_count=1,
        summary="hydro/basin/tas_annual_mean: expected=1, wrote=0, missing=1, skipped_inputs=2, failures=1, first_error=boom",
        errors=("boom",),
        skipped_reasons=("bad csv",),
    )
    run_result = CMP.PipelineRunResult(
        level="basin",
        scope_name="hydro",
        compute_failed_count=0,
        ensemble_results=(failed_job,),
    )

    monkeypatch.setattr(CMP, "run_pipeline_parallel", lambda **kwargs: run_result)

    rc = CMP.main(["--level", "basin", "--workers", "1"])

    captured = capsys.readouterr().out
    assert rc == 1
    assert "ENSEMBLE FAILURE SUMMARY" in captured
    assert "hydro/basin/tas_annual_mean" in captured


def test_compute_basin_ensembles_ignores_stale_out_of_extent_inputs_via_coverage_qc(
    tmp_path: Path,
) -> None:
    level_root = tmp_path / "hydro" / "basins"
    good_dir = level_root / "Godavari_Basin" / "ACCESS-CM2" / "historical"
    stale_dir = level_root / "Indus_Basin" / "ACCESS-CM2" / "historical"
    for directory in (good_dir, stale_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (good_dir / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")
    (stale_dir / "Indus_Basin_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "basin_id": "03",
                "basin_name": "Godavari Basin",
                "coverage_fraction": 1.0,
                "coverage_ok": True,
                "eligible_for_processing": True,
                "covered_cells": 4,
                "total_cells": 4,
                "reason": "ok",
            },
            {
                "basin_id": "01",
                "basin_name": "Indus Basin",
                "coverage_fraction": 0.0,
                "coverage_ok": False,
                "eligible_for_processing": False,
                "covered_cells": 0,
                "total_cells": 0,
                "reason": "outside_climate_extent",
            },
        ]
    ).to_csv(level_root / "coverage_qc_ACCESS-CM2_historical.csv", index=False)

    stats = CMP._compute_basin_ensembles(
        level_root,
        level_root / "ensembles",
        slug="tas_annual_mean",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    )

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert stats.failure_count == 0
    assert stats.missing_expected_output_count == 0
    assert (level_root / "ensembles" / "Godavari_Basin" / "historical" / "Godavari_Basin_yearly_ensemble.csv").exists()
    assert not (level_root / "ensembles" / "Indus_Basin").exists()


def test_ensemble_output_count_ignores_out_of_scope_hydro_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")

    level_root = tmp_path / "processed" / "tas_annual_mean" / "hydro" / "basins"
    historical_dir = level_root / "ensembles" / "Godavari_Basin" / "historical"
    future_dir = level_root / "ensembles" / "Godavari_Basin" / "ssp245"
    stale_dir = level_root / "ensembles" / "Indus_Basin" / "historical"
    for directory in (historical_dir, future_dir, stale_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (historical_dir / "Godavari_Basin_yearly_ensemble.csv").write_text("year,ensemble_mean\n2030,1.0\n", encoding="utf-8")
    (future_dir / "Godavari_Basin_yearly_ensemble.csv").write_text("year,ensemble_mean\n2030,2.0\n", encoding="utf-8")
    (stale_dir / "Indus_Basin_yearly_ensemble.csv").write_text("year,ensemble_mean\n2030,3.0\n", encoding="utf-8")

    pd.DataFrame(
        [
            {
                "basin_id": "03",
                "basin_name": "Godavari Basin",
                "coverage_fraction": 1.0,
                "coverage_ok": True,
                "eligible_for_processing": True,
                "covered_cells": 4,
                "total_cells": 4,
                "reason": "ok",
            },
            {
                "basin_id": "01",
                "basin_name": "Indus Basin",
                "coverage_fraction": 0.0,
                "coverage_ok": False,
                "eligible_for_processing": False,
                "covered_cells": 0,
                "total_cells": 0,
                "reason": "outside_climate_extent",
            },
        ]
    ).to_csv(level_root / "coverage_qc_ACCESS-CM2_historical.csv", index=False)

    count = CMP._ensemble_output_count(
        slug="tas_annual_mean",
        level="basin",
        scope_name="hydro",
        allowed_models=("ACCESS-CM2",),
        allowed_scenarios=("historical",),
    )

    assert count == 1


def test_prune_excluded_hydro_ensemble_outputs_removes_out_of_extent_units(
    tmp_path: Path,
) -> None:
    ensembles_root = tmp_path / "tas_annual_mean" / "hydro" / "basins" / "ensembles"
    keep_dir = ensembles_root / "Godavari_Basin" / "historical"
    stale_dir = ensembles_root / "Indus_Basin" / "historical"
    keep_dir.mkdir(parents=True, exist_ok=True)
    stale_dir.mkdir(parents=True, exist_ok=True)
    (keep_dir / "Godavari_Basin_yearly_ensemble.csv").write_text("year,ensemble_mean\n2030,1.0\n", encoding="utf-8")
    stale_file = stale_dir / "Indus_Basin_yearly_ensemble.csv"
    stale_file.write_text("year,ensemble_mean\n2030,2.0\n", encoding="utf-8")

    excluded_df = pd.DataFrame(
        [
            {
                "basin_name": "Indus Basin",
                "eligible_for_processing": False,
                "reason": "outside_climate_extent",
            }
        ]
    )

    CMP._prune_excluded_hydro_ensemble_outputs(
        tmp_path / "tas_annual_mean",
        state_name="hydro",
        level="basin",
        scenario="historical",
        excluded_coverage_df=excluded_df,
        slug="tas_annual_mean",
    )

    assert (keep_dir / "Godavari_Basin_yearly_ensemble.csv").exists()
    assert not stale_file.exists()
