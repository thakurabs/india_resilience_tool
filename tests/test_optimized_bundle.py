from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from shapely.geometry import Polygon

from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.data.optimized_bundle import (
    optimized_geometry_path,
    optimized_master_sources_from_metric_root,
)
from tools.optimized.build_processed_optimised import (
    BuildPlan,
    BuildProgress,
    BuildTask,
    _build_execution_plan,
    audit_processed_optimised_parity,
    build_processed_optimised_bundle,
    default_build_workers_80pct,
    resolve_build_workers,
    _write_geometry_bundle,
)


def _write_admin_legacy_metric_fixture(tmp_path: Path, *, slug: str = "txx_annual_max") -> None:
    legacy_root = tmp_path / "processed" / slug / "Telangana"
    legacy_root.mkdir(parents=True)
    (legacy_root / "master_metrics_by_district.csv").write_text(
        f"state,district,{slug}__ssp245__2030-2040__mean\nTelangana,Hanumakonda,1.0\n",
        encoding="utf-8",
    )
    (legacy_root / "master_metrics_by_block.csv").write_text(
        f"state,district,block,{slug}__ssp245__2030-2040__mean\nTelangana,Hanumakonda,Atmakur,2.0\n",
        encoding="utf-8",
    )

    district_model_dir = legacy_root / "districts" / "Hanumakonda" / "ModelA" / "ssp245"
    district_model_dir.mkdir(parents=True)
    (district_model_dir / "Hanumakonda_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")

    district_ensemble_dir = legacy_root / "districts" / "ensembles" / "Hanumakonda" / "ssp245"
    district_ensemble_dir.mkdir(parents=True)
    (district_ensemble_dir / "Hanumakonda_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,1.5\n",
        encoding="utf-8",
    )

    block_model_dir = legacy_root / "blocks" / "Hanumakonda" / "Atmakur" / "ModelA" / "ssp245"
    block_model_dir.mkdir(parents=True)
    (block_model_dir / "Atmakur_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")

    block_ensemble_dir = legacy_root / "blocks" / "ensembles" / "Hanumakonda" / "Atmakur" / "ssp245"
    block_ensemble_dir.mkdir(parents=True)
    (block_ensemble_dir / "Atmakur_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,2.5\n",
        encoding="utf-8",
    )


def _write_hydro_legacy_metric_fixture(tmp_path: Path, *, slug: str = "txx_annual_max") -> None:
    hydro_root = tmp_path / "processed" / slug / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        f"basin_id,basin_name,{slug}__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,3.0\n",
        encoding="utf-8",
    )
    (hydro_root / "master_metrics_by_sub_basin.csv").write_text(
        f"basin_id,basin_name,subbasin_id,subbasin_name,{slug}__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,GODAVARI-1,Pranhita,2.0\n",
        encoding="utf-8",
    )

    basin_ensemble_dir = hydro_root / "basins" / "ensembles" / "Godavari Basin" / "ssp245"
    basin_ensemble_dir.mkdir(parents=True)
    (basin_ensemble_dir / "Godavari Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean,ensemble_median\n2030,3.5,3.4\n",
        encoding="utf-8",
    )

    sub_ensemble_dir = hydro_root / "sub_basins" / "ensembles" / "Godavari Basin" / "Pranhita" / "ssp245"
    sub_ensemble_dir.mkdir(parents=True)
    (sub_ensemble_dir / "Pranhita_yearly_ensemble.csv").write_text(
        "year,ensemble_mean,ensemble_median\n2030,2.5,2.4\n",
        encoding="utf-8",
    )


def _write_geometry_fixture(tmp_path: Path) -> None:
    district_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "DISTRICT": ["Hanumakonda"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    district_gdf.to_file(tmp_path / "districts_4326.geojson", driver="GeoJSON")

    block_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "District": ["Hanumakonda"], "Sub_dist": ["Atmakur"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    block_gdf.to_file(tmp_path / "blocks_4326.geojson", driver="GeoJSON")

    basin_gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": ["Godavari Basin"], "hydro_level": ["basin"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    basin_gdf.to_file(tmp_path / "basins.geojson", driver="GeoJSON")

    subbasin_gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["GODAVARI-1"],
            "subbasin_code": ["G1"],
            "subbasin_name": ["Pranhita"],
            "hydro_level": ["sub_basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    subbasin_gdf.to_file(tmp_path / "subbasins.geojson", driver="GeoJSON")


def _read_output_tables(bundle_root: Path, *, slug: str) -> dict[str, pd.DataFrame]:
    paths = {
        "district_master": bundle_root / "metrics" / slug / "masters" / "admin" / "district" / "state=Telangana.parquet",
        "block_master": bundle_root / "metrics" / slug / "masters" / "admin" / "block" / "state=Telangana.parquet",
        "basin_master": bundle_root / "metrics" / slug / "masters" / "hydro" / "basin" / "master.parquet",
        "sub_basin_master": bundle_root / "metrics" / slug / "masters" / "hydro" / "sub_basin" / "master.parquet",
        "district_yearly_ensemble": bundle_root / "metrics" / slug / "yearly_ensemble" / "admin" / "district" / "state=Telangana.parquet",
        "block_yearly_ensemble": bundle_root / "metrics" / slug / "yearly_ensemble" / "admin" / "block" / "state=Telangana.parquet",
        "basin_yearly_ensemble": bundle_root / "metrics" / slug / "yearly_ensemble" / "hydro" / "basin" / "master.parquet",
        "sub_basin_yearly_ensemble": bundle_root / "metrics" / slug / "yearly_ensemble" / "hydro" / "sub_basin" / "master.parquet",
        "district_yearly_models": bundle_root / "metrics" / slug / "yearly_models" / "admin" / "district" / "state=Telangana.parquet",
        "block_yearly_models": bundle_root / "metrics" / slug / "yearly_models" / "admin" / "block" / "state=Telangana.parquet",
    }
    return {name: pd.read_parquet(path) for name, path in paths.items()}


def _read_manifest(bundle_root: Path) -> dict:
    return json.loads((bundle_root / "bundle_manifest.json").read_text(encoding="utf-8"))


def test_list_available_states_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    level_dir = metric_root / "masters" / "admin" / "district"
    level_dir.mkdir(parents=True)
    (level_dir / "state=Telangana.parquet").write_bytes(b"")
    (level_dir / "state=Odisha.parquet").write_bytes(b"")

    states = list_available_states_from_processed_root(str(metric_root))

    assert states == ["Odisha", "Telangana"]


def test_optimized_master_sources_from_metric_root_for_all_states(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    level_dir = metric_root / "masters" / "admin" / "block"
    level_dir.mkdir(parents=True)
    telangana = level_dir / "state=Telangana.parquet"
    odisha = level_dir / "state=Odisha.parquet"
    telangana.write_bytes(b"")
    odisha.write_bytes(b"")

    sources = optimized_master_sources_from_metric_root(
        metric_root,
        level="block",
        selected_state="All",
    )

    assert sources == (odisha, telangana)


def test_write_geometry_bundle_normalizes_raw_admin_columns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    district_gdf = gpd.GeoDataFrame(
        {
            "STATE_UT": ["Telangana"],
            "DISTRICT": ["Hanumakonda"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    district_gdf.to_file(tmp_path / "districts_4326.geojson", driver="GeoJSON")

    block_gdf = gpd.GeoDataFrame(
        {
            "STATE_UT": ["Telangana"],
            "District": ["Hanumakonda"],
            "Sub_dist": ["Atmakur"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    block_gdf.to_file(tmp_path / "blocks_4326.geojson", driver="GeoJSON")

    basin_gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "hydro_level": ["basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    basin_gdf.to_file(tmp_path / "basins.geojson", driver="GeoJSON")

    subbasin_gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["GODAVARI-1"],
            "subbasin_code": ["G1"],
            "subbasin_name": ["Pranhita"],
            "hydro_level": ["sub_basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    subbasin_gdf.to_file(tmp_path / "subbasins.geojson", driver="GeoJSON")

    plan = BuildPlan(
        summaries_seed=(),
        master_tasks=(),
        yearly_model_jobs=(),
        yearly_ensemble_jobs=(),
        context_tasks=(),
        geometry_tasks=_build_execution_plan(
            data_dir=tmp_path,
            metrics=[],
            include_geometry=True,
            include_context=False,
        ).geometry_tasks,
        manifest_task=BuildTask(stage="manifest", label="bundle manifest"),
    )
    progress = BuildProgress(plan, enabled=False)

    _write_geometry_bundle(data_dir=tmp_path, tasks=plan.geometry_tasks, progress=progress)

    district_path = optimized_geometry_path(level="district", state="Telangana", data_dir=tmp_path)
    block_path = optimized_geometry_path(level="block", state="Telangana", data_dir=tmp_path)
    block_index_path = tmp_path / "processed_optimised" / "context" / "admin_block_index.parquet"
    hydro_index_path = tmp_path / "processed_optimised" / "context" / "hydro_subbasin_index.parquet"

    assert district_path.exists()
    assert block_path.exists()
    assert block_index_path.exists()
    assert hydro_index_path.exists()

    district_out = gpd.read_file(district_path)
    assert "area_m2" in district_out.columns

    basin_path = optimized_geometry_path(level="basin", data_dir=tmp_path)
    basin_out = gpd.read_file(basin_path)
    assert {"basin_id", "basin_name", "hydro_level", "area_m2"}.issubset(set(basin_out.columns))

    subbasin_path = optimized_geometry_path(level="sub_basin", basin_id="GODAVARI", data_dir=tmp_path)
    subbasin_out = gpd.read_file(subbasin_path)
    assert {
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_code",
        "subbasin_name",
        "hydro_level",
        "area_m2",
    }.issubset(set(subbasin_out.columns))

    block_index = pd.read_parquet(block_index_path)
    assert {"state_name", "district_name", "block_name"}.issubset(set(block_index.columns))

    hydro_index = pd.read_parquet(hydro_index_path)
    assert {"basin_id", "basin_name", "subbasin_id", "subbasin_name"}.issubset(set(hydro_index.columns))


def test_build_execution_plan_counts_exact_tasks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    metric_root = tmp_path / "processed" / "txx_annual_max" / "Telangana"
    metric_root.mkdir(parents=True)
    (metric_root / "master_metrics_by_district.csv").write_text(
        "state,district,txx_annual_max__ssp245__2030-2040__mean\nTelangana,Hanumakonda,1.0\n",
        encoding="utf-8",
    )
    (metric_root / "master_metrics_by_block.csv").write_text(
        "state,district,block,txx_annual_max__ssp245__2030-2040__mean\nTelangana,Hanumakonda,Atmakur,1.0\n",
        encoding="utf-8",
    )

    hydro_root = tmp_path / "processed" / "txx_annual_max" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,txx_annual_max__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,1.0\n",
        encoding="utf-8",
    )

    district_yearly = metric_root / "districts" / "Hanumakonda" / "ModelA" / "ssp245"
    district_yearly.mkdir(parents=True)
    (district_yearly / "tas_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")

    block_yearly = metric_root / "blocks" / "Hanumakonda" / "Atmakur" / "ModelA" / "ssp245"
    block_yearly.mkdir(parents=True)
    (block_yearly / "tas_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")

    (tmp_path / "district_subbasin_crosswalk.csv").write_text("district,subbasin\nA,B\n", encoding="utf-8")

    district_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "DISTRICT": ["Hanumakonda"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    district_gdf.to_file(tmp_path / "districts_4326.geojson", driver="GeoJSON")

    block_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "District": ["Hanumakonda"], "Sub_dist": ["Atmakur"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    block_gdf.to_file(tmp_path / "blocks_4326.geojson", driver="GeoJSON")

    basin_gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": ["Godavari Basin"], "hydro_level": ["basin"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    basin_gdf.to_file(tmp_path / "basins.geojson", driver="GeoJSON")

    subbasin_gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["GODAVARI-1"],
            "subbasin_code": ["G1"],
            "subbasin_name": ["Pranhita"],
            "hydro_level": ["sub_basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    subbasin_gdf.to_file(tmp_path / "subbasins.geojson", driver="GeoJSON")

    plan = _build_execution_plan(data_dir=tmp_path, metrics=["txx_annual_max"])

    assert plan.stage_totals() == {
        "masters": 3,
        "yearly-models": 4,
        "yearly-ensemble": 0,
        "context": 1,
        "geometry": 6,
        "manifest": 1,
    }
    assert plan.total_tasks == 15


def test_build_progress_failure_summary_reports_remaining() -> None:
    plan = BuildPlan(
        summaries_seed=(),
        master_tasks=(
            BuildTask(stage="masters", label="m1"),
            BuildTask(stage="masters", label="m2"),
        ),
        yearly_model_jobs=(),
        yearly_ensemble_jobs=(),
        context_tasks=(),
        geometry_tasks=(),
        manifest_task=BuildTask(stage="manifest", label="manifest"),
    )
    progress = BuildProgress(plan, enabled=False)

    first = plan.master_tasks[0]
    second = plan.master_tasks[1]
    progress.start_task(first)
    progress.finish_task(first)
    progress.start_task(second)

    summary = progress.failure_summary()

    assert "completed_tasks=1" in summary
    assert "remaining_tasks=2" in summary
    assert "current=m2" in summary


def test_default_build_workers_80pct(monkeypatch) -> None:
    monkeypatch.setattr("tools.optimized.build_processed_optimised.os.cpu_count", lambda: 10)

    assert default_build_workers_80pct() == 8
    assert resolve_build_workers(None) == 8
    assert resolve_build_workers(1) == 1
    assert resolve_build_workers(3) == 3


def test_resolve_build_workers_rejects_non_positive() -> None:
    with pytest.raises(ValueError):
        resolve_build_workers(0)

    with pytest.raises(ValueError):
        resolve_build_workers(-2)


def test_build_processed_optimised_writes_admin_and_hydro_yearly_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    legacy_root = tmp_path / "processed" / "txx_annual_max" / "Telangana"
    legacy_root.mkdir(parents=True)
    (legacy_root / "master_metrics_by_district.csv").write_text(
        "state,district,txx_annual_max__ssp245__2030-2040__mean\nTelangana,Hanumakonda,1.0\n",
        encoding="utf-8",
    )

    district_model_dir = legacy_root / "districts" / "Hanumakonda" / "ModelA" / "ssp245"
    district_model_dir.mkdir(parents=True)
    (district_model_dir / "Hanumakonda_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")

    district_ensemble_dir = legacy_root / "districts" / "ensembles" / "Hanumakonda" / "ssp245"
    district_ensemble_dir.mkdir(parents=True)
    (district_ensemble_dir / "Hanumakonda_yearly_ensemble.csv").write_text(
        "year,ensemble_mean,ensemble_median\n2030,1.5,1.4\n",
        encoding="utf-8",
    )

    block_model_dir = legacy_root / "blocks" / "Hanumakonda" / "Atmakur" / "ModelA" / "ssp245"
    block_model_dir.mkdir(parents=True)
    (block_model_dir / "Atmakur_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")

    block_ensemble_dir = legacy_root / "blocks" / "ensembles" / "Hanumakonda" / "Atmakur" / "ssp245"
    block_ensemble_dir.mkdir(parents=True)
    (block_ensemble_dir / "Atmakur_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,2.5\n",
        encoding="utf-8",
    )

    hydro_root = tmp_path / "processed" / "txx_annual_max" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,txx_annual_max__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,3.0\n",
        encoding="utf-8",
    )
    basin_ensemble_dir = hydro_root / "basins" / "ensembles" / "Godavari Basin" / "ssp245"
    basin_ensemble_dir.mkdir(parents=True)
    (basin_ensemble_dir / "Godavari_Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,3.5\n",
        encoding="utf-8",
    )

    district_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "DISTRICT": ["Hanumakonda"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    district_gdf.to_file(tmp_path / "districts_4326.geojson", driver="GeoJSON")

    block_gdf = gpd.GeoDataFrame(
        {"STATE_UT": ["Telangana"], "District": ["Hanumakonda"], "Sub_dist": ["Atmakur"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    block_gdf.to_file(tmp_path / "blocks_4326.geojson", driver="GeoJSON")

    basin_gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": ["Godavari Basin"], "hydro_level": ["basin"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    basin_gdf.to_file(tmp_path / "basins.geojson", driver="GeoJSON")

    subbasin_gdf = gpd.GeoDataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["GODAVARI-1"],
            "subbasin_code": ["G1"],
            "subbasin_name": ["Pranhita"],
            "hydro_level": ["sub_basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    subbasin_gdf.to_file(tmp_path / "subbasins.geojson", driver="GeoJSON")

    summaries = build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        overwrite=False,
        include_context=False,
        show_progress=False,
    )

    assert summaries and summaries[0].wrote_yearly_ensemble is True
    district_ensemble = (
        tmp_path / "processed_optimised" / "metrics" / "txx_annual_max" / "yearly_ensemble" / "admin" / "district" / "state=Telangana.parquet"
    )
    block_models = (
        tmp_path / "processed_optimised" / "metrics" / "txx_annual_max" / "yearly_models" / "admin" / "block" / "state=Telangana.parquet"
    )
    hydro_ensemble = (
        tmp_path / "processed_optimised" / "metrics" / "txx_annual_max" / "yearly_ensemble" / "hydro" / "basin" / "master.parquet"
    )

    assert district_ensemble.exists()
    assert block_models.exists()
    assert hydro_ensemble.exists()

    district_df = pd.read_parquet(district_ensemble)
    hydro_df = pd.read_parquet(hydro_ensemble)

    assert district_df["district_key"].tolist() == ["telangana|hanumakonda"]
    assert district_df["mean"].tolist() == [1.5]
    assert hydro_df["basin_name"].tolist() == ["Godavari Basin"]
    assert hydro_df["mean"].tolist() == [3.5]


def test_build_processed_optimised_parallel_matches_serial_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    serial_root = tmp_path / "serial"
    parallel_root = tmp_path / "parallel"
    for root in (serial_root, parallel_root):
        _write_admin_legacy_metric_fixture(root)
        _write_hydro_legacy_metric_fixture(root)
        _write_geometry_fixture(root)

    monkeypatch.setenv("IRT_DATA_DIR", str(serial_root))
    build_processed_optimised_bundle(
        data_dir=serial_root,
        metrics=["txx_annual_max"],
        workers=1,
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )
    serial_tables = _read_output_tables(serial_root / "processed_optimised", slug="txx_annual_max")

    monkeypatch.setenv("IRT_DATA_DIR", str(parallel_root))
    build_processed_optimised_bundle(
        data_dir=parallel_root,
        metrics=["txx_annual_max"],
        workers=2,
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )
    parallel_tables = _read_output_tables(parallel_root / "processed_optimised", slug="txx_annual_max")

    assert serial_tables.keys() == parallel_tables.keys()
    for name in sorted(serial_tables):
        assert_frame_equal(serial_tables[name], parallel_tables[name], check_like=False)


def test_build_execution_plan_adds_hydro_yearly_fallback_from_model_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    hydro_root = tmp_path / "processed" / "tas_annual_mean" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,3.0\n",
        encoding="utf-8",
    )
    basin_model_a = hydro_root / "basins" / "Godavari Basin" / "ModelA" / "ssp245"
    basin_model_a.mkdir(parents=True)
    (basin_model_a / "Godavari Basin_yearly.csv").write_text("year,value\n2030,3.0\n", encoding="utf-8")
    basin_model_b = hydro_root / "basins" / "Godavari Basin" / "ModelB" / "ssp245"
    basin_model_b.mkdir(parents=True)
    (basin_model_b / "Godavari Basin_yearly.csv").write_text("year,value\n2030,5.0\n", encoding="utf-8")

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        include_geometry=False,
        include_context=False,
    )

    hydro_jobs = [job for job in plan.yearly_ensemble_jobs if job.slug == "tas_annual_mean" and job.level == "basin"]
    assert len(hydro_jobs) == 1
    assert hydro_jobs[0].source_mode == "hydro_model_fallback"
    assert len(hydro_jobs[0].sources) == 2
    assert plan.stage_totals()["yearly-ensemble"] == 3


def test_build_processed_optimised_derives_hydro_yearly_from_model_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    hydro_root = tmp_path / "processed" / "tas_annual_mean" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,3.0\n",
        encoding="utf-8",
    )
    (hydro_root / "master_metrics_by_sub_basin.csv").write_text(
        "basin_id,basin_name,subbasin_id,subbasin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,GODAVARI-1,Pranhita,2.0\n",
        encoding="utf-8",
    )

    basin_model_a = hydro_root / "basins" / "Godavari Basin" / "ModelA" / "ssp245"
    basin_model_a.mkdir(parents=True)
    (basin_model_a / "Godavari Basin_yearly.csv").write_text("year,value\n2030,3.0\n", encoding="utf-8")
    basin_model_b = hydro_root / "basins" / "Godavari Basin" / "ModelB" / "ssp245"
    basin_model_b.mkdir(parents=True)
    (basin_model_b / "Godavari Basin_yearly.csv").write_text("year,value\n2030,5.0\n", encoding="utf-8")

    sub_model_a = hydro_root / "sub_basins" / "Godavari Basin" / "Pranhita" / "ModelA" / "ssp245"
    sub_model_a.mkdir(parents=True)
    (sub_model_a / "Pranhita_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")
    sub_model_b = hydro_root / "sub_basins" / "Godavari Basin" / "Pranhita" / "ModelB" / "ssp245"
    sub_model_b.mkdir(parents=True)
    (sub_model_b / "Pranhita_yearly.csv").write_text("year,value\n2030,4.0\n", encoding="utf-8")

    summaries = build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
    )

    assert summaries and summaries[0].wrote_yearly_ensemble is True

    basin_out = (
        tmp_path / "processed_optimised" / "metrics" / "tas_annual_mean" / "yearly_ensemble" / "hydro" / "basin" / "master.parquet"
    )
    sub_out = (
        tmp_path / "processed_optimised" / "metrics" / "tas_annual_mean" / "yearly_ensemble" / "hydro" / "sub_basin" / "master.parquet"
    )

    assert basin_out.exists()
    assert sub_out.exists()

    basin_df = pd.read_parquet(basin_out)
    sub_df = pd.read_parquet(sub_out)

    assert basin_df["basin_id"].tolist() == ["GODAVARI"]
    assert basin_df["mean"].tolist() == [4.0]
    assert basin_df["median"].tolist() == [4.0]

    assert sub_df["subbasin_id"].tolist() == ["GODAVARI-1"]
    assert sub_df["mean"].tolist() == [3.0]
    assert sub_df["median"].tolist() == [3.0]

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        include_geometry=False,
        include_context=False,
        write_report=False,
    )

    assert report["issue_count"] == 0


def test_build_processed_optimised_prefers_hydro_ensemble_csvs_over_model_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    hydro_root = tmp_path / "processed" / "tas_annual_mean" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,3.0\n",
        encoding="utf-8",
    )

    basin_model_a = hydro_root / "basins" / "Godavari Basin" / "ModelA" / "ssp245"
    basin_model_a.mkdir(parents=True)
    (basin_model_a / "Godavari Basin_yearly.csv").write_text("year,value\n2030,30.0\n", encoding="utf-8")
    basin_model_b = hydro_root / "basins" / "Godavari Basin" / "ModelB" / "ssp245"
    basin_model_b.mkdir(parents=True)
    (basin_model_b / "Godavari Basin_yearly.csv").write_text("year,value\n2030,50.0\n", encoding="utf-8")

    basin_ensemble_dir = hydro_root / "basins" / "ensembles" / "Godavari Basin" / "ssp245"
    basin_ensemble_dir.mkdir(parents=True)
    (basin_ensemble_dir / "Godavari Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean,ensemble_median\n2030,3.5,3.4\n",
        encoding="utf-8",
    )

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
    )

    basin_out = (
        tmp_path / "processed_optimised" / "metrics" / "tas_annual_mean" / "yearly_ensemble" / "hydro" / "basin" / "master.parquet"
    )
    basin_df = pd.read_parquet(basin_out)

    assert basin_df["mean"].tolist() == pytest.approx([3.5])
    assert basin_df["median"].tolist() == pytest.approx([3.4])


def test_audit_processed_optimised_parity_reports_missing_yearly_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    legacy_root = tmp_path / "processed" / "txx_annual_max" / "Telangana"
    legacy_root.mkdir(parents=True)
    (legacy_root / "master_metrics_by_district.csv").write_text(
        "state,district,txx_annual_max__ssp245__2030-2040__mean\nTelangana,Hanumakonda,1.0\n",
        encoding="utf-8",
    )
    district_ensemble_dir = legacy_root / "districts" / "ensembles" / "Hanumakonda" / "ssp245"
    district_ensemble_dir.mkdir(parents=True)
    (district_ensemble_dir / "Hanumakonda_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,1.5\n",
        encoding="utf-8",
    )

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        include_geometry=False,
        include_context=False,
        write_report=False,
    )

    assert report["issue_count"] >= 2
    stages = {issue["stage"] for issue in report["issues"]}
    assert "masters" in stages
    assert "yearly-ensemble" in stages


def test_build_processed_optimised_overwrite_preserves_prior_level_outputs_and_rebuilds_manifest_inventory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path)
    _write_admin_legacy_metric_fixture(tmp_path, slug="tas_annual_mean")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max", "tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    parity_report = tmp_path / "processed_optimised" / "parity_report.json"
    parity_report.write_text('{"stale": true}', encoding="utf-8")

    district_master = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "txx_annual_max"
        / "masters"
        / "admin"
        / "district"
        / "state=Telangana.parquet"
    )
    block_master = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "txx_annual_max"
        / "masters"
        / "admin"
        / "block"
        / "state=Telangana.parquet"
    )

    assert district_master.exists()
    assert block_master.exists()

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        levels=["block"],
        overwrite=True,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    assert block_master.exists()
    assert district_master.exists()
    assert not parity_report.exists()

    manifest = _read_manifest(tmp_path / "processed_optimised")
    summaries = {entry["slug"]: entry for entry in manifest["summaries"]}
    assert manifest["artifact_version"] == 2
    assert manifest["summary_semantics"] == "bundle_inventory"
    assert {"txx_annual_max", "tas_annual_mean"}.issubset(set(summaries))
    assert summaries["tas_annual_mean"]["has_masters"] is True
    assert summaries["tas_annual_mean"]["has_yearly_ensemble"] is True
    assert summaries["tas_annual_mean"]["has_yearly_models"] is True


def test_build_processed_optimised_prune_scope_removes_selected_owned_roots_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path)
    _write_admin_legacy_metric_fixture(tmp_path, slug="tas_annual_mean")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max", "tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    stale_selected_block = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "txx_annual_max"
        / "masters"
        / "admin"
        / "block"
        / "stale.txt"
    )
    stale_selected_block.write_text("remove me", encoding="utf-8")

    stale_selected_district = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "txx_annual_max"
        / "masters"
        / "admin"
        / "district"
        / "stale.txt"
    )
    stale_selected_district.write_text("keep me", encoding="utf-8")

    stale_other_metric_block = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "tas_annual_mean"
        / "masters"
        / "admin"
        / "block"
        / "stale.txt"
    )
    stale_other_metric_block.write_text("keep me too", encoding="utf-8")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        levels=["block"],
        overwrite=True,
        prune_scope=True,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    assert not stale_selected_block.exists()
    assert stale_selected_district.exists()
    assert stale_other_metric_block.exists()
    assert (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "txx_annual_max"
        / "masters"
        / "admin"
        / "block"
        / "state=Telangana.parquet"
    ).exists()


def test_build_processed_optimised_full_rebuild_dry_run_preserves_existing_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path)
    _write_geometry_fixture(tmp_path)

    bundle_root = tmp_path / "processed_optimised"
    bundle_root.mkdir(parents=True)
    marker = bundle_root / "marker.txt"
    marker.write_text("keep", encoding="utf-8")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        full_rebuild=True,
        dry_run=True,
        show_progress=False,
        run_audit=False,
    )

    assert marker.exists()


def test_build_processed_optimised_full_rebuild_rejects_suspicious_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("IRT_PROCESSED_OPTIMISED_ROOT", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path)
    _write_geometry_fixture(tmp_path)

    with pytest.raises(ValueError, match="Refusing to delete suspicious optimized bundle root"):
        build_processed_optimised_bundle(
            data_dir=tmp_path,
            full_rebuild=True,
            show_progress=False,
            run_audit=False,
        )


def test_build_processed_optimised_rejects_explicit_empty_scope(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path)

    with pytest.raises(ValueError, match="No buildable legacy processed sources found"):
        build_processed_optimised_bundle(
            data_dir=tmp_path,
            metrics=["txx_annual_max"],
            levels=["basin"],
            overwrite=True,
            include_geometry=False,
            include_context=False,
            show_progress=False,
            run_audit=False,
        )


def test_build_execution_plan_and_audit_filter_to_sub_basin_level(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    hydro_root = tmp_path / "processed" / "tas_annual_mean" / "hydro"
    hydro_root.mkdir(parents=True)
    (hydro_root / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,4.0\n",
        encoding="utf-8",
    )
    (hydro_root / "master_metrics_by_sub_basin.csv").write_text(
        "basin_id,basin_name,subbasin_id,subbasin_name,tas_annual_mean__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,GODAVARI-1,Pranhita,3.0\n",
        encoding="utf-8",
    )

    basin_ensemble_dir = hydro_root / "basins" / "ensembles" / "Godavari Basin" / "ssp245"
    basin_ensemble_dir.mkdir(parents=True)
    (basin_ensemble_dir / "Godavari Basin_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,4.0\n",
        encoding="utf-8",
    )

    sub_ensemble_dir = hydro_root / "sub_basins" / "ensembles" / "Godavari Basin" / "Pranhita" / "ssp245"
    sub_ensemble_dir.mkdir(parents=True)
    (sub_ensemble_dir / "Pranhita_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,3.0\n",
        encoding="utf-8",
    )

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        levels=["sub_basin"],
        include_geometry=False,
        include_context=False,
    )

    assert {task.level for task in plan.master_tasks} == {"sub_basin"}
    assert {job.level for job in plan.yearly_ensemble_jobs} == {"sub_basin"}

    sub_master_out = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "tas_annual_mean"
        / "masters"
        / "hydro"
        / "sub_basin"
        / "master.parquet"
    )
    sub_master_out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "basin_id": ["GODAVARI"],
            "basin_name": ["Godavari Basin"],
            "subbasin_id": ["GODAVARI-1"],
            "subbasin_name": ["Pranhita"],
        }
    ).to_parquet(sub_master_out, index=False)

    sub_yearly_out = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "tas_annual_mean"
        / "yearly_ensemble"
        / "hydro"
        / "sub_basin"
        / "master.parquet"
    )
    sub_yearly_out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "basin_name": ["Godavari Basin"],
            "subbasin_name": ["Pranhita"],
            "scenario": ["ssp245"],
            "year": [2030],
            "mean": [3.0],
        }
    ).to_parquet(sub_yearly_out, index=False)

    manifest_path = tmp_path / "processed_optimised" / "bundle_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}", encoding="utf-8")

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        levels=["sub_basin"],
        include_geometry=False,
        include_context=False,
        write_report=False,
    )

    assert report["issue_count"] == 0
