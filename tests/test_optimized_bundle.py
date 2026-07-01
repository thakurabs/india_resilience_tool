from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from shapely.geometry import Polygon

from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.data.adm2_loader import load_local_adm1_artifact
from india_resilience_tool.data.optimized_bundle import (
    optimized_adm1_path,
    optimized_context_path,
    optimized_geometry_path,
    optimized_master_sources_from_metric_root,
)
from tools.diagnostics.list_optimized_yearly_metrics import list_metrics as list_optimized_yearly_metrics
from tools.optimized.build_processed_optimised import (
    YEARLY_PARALLEL_CHUNK_SIZE,
    BuildPlan,
    BuildProgress,
    BuildTask,
    _build_execution_plan,
    _chunk_tuple,
    _copy_context_artifacts,
    _execute_parallel_chunks,
    _load_legacy_admin_yearly_models,
    _yearly_chunk_size,
    _yearly_executor_kind,
    audit_processed_optimised_parity,
    build_processed_optimised_bundle,
    default_build_workers_80pct,
    resolve_build_workers,
    _write_geometry_bundle,
)


def _write_admin_legacy_metric_fixture(
    tmp_path: Path,
    *,
    slug: str = "txx_annual_max",
    state: str = "Telangana",
    district: str = "Hanumakonda",
    block: str = "Atmakur",
) -> None:
    legacy_root = tmp_path / "processed" / slug / state
    legacy_root.mkdir(parents=True)
    (legacy_root / "master_metrics_by_district.csv").write_text(
        f"state,district,{slug}__ssp245__2030-2040__mean\n{state},{district},1.0\n",
        encoding="utf-8",
    )
    (legacy_root / "master_metrics_by_block.csv").write_text(
        f"state,district,block,{slug}__ssp245__2030-2040__mean\n{state},{district},{block},2.0\n",
        encoding="utf-8",
    )

    district_model_dir = legacy_root / "districts" / district / "ModelA" / "ssp245"
    district_model_dir.mkdir(parents=True)
    (district_model_dir / f"{district}_yearly.csv").write_text("year,value\n2030,1.0\n", encoding="utf-8")

    district_ensemble_dir = legacy_root / "districts" / "ensembles" / district / "ssp245"
    district_ensemble_dir.mkdir(parents=True)
    (district_ensemble_dir / f"{district}_yearly_ensemble.csv").write_text(
        "year,ensemble_mean\n2030,1.5\n",
        encoding="utf-8",
    )

    block_model_dir = legacy_root / "blocks" / district / block / "ModelA" / "ssp245"
    block_model_dir.mkdir(parents=True)
    (block_model_dir / f"{block}_yearly.csv").write_text("year,value\n2030,2.0\n", encoding="utf-8")

    block_ensemble_dir = legacy_root / "blocks" / "ensembles" / district / block / "ssp245"
    block_ensemble_dir.mkdir(parents=True)
    (block_ensemble_dir / f"{block}_yearly_ensemble.csv").write_text(
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


def test_optimized_adm1_path_resolves_under_bundle_root(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    path = optimized_adm1_path(data_dir=tmp_path)
    assert path == tmp_path / "processed_optimised" / "geometry" / "admin" / "adm1.geojson"


def test_optimized_adm1_artifact_load_returns_state_polygons(tmp_path: Path) -> None:
    path = optimized_adm1_path(data_dir=tmp_path)
    path.parent.mkdir(parents=True)
    source = gpd.GeoDataFrame(
        {
            "state_name": ["Odisha", "Telangana"],
            "shapeName": ["Odisha", "Telangana"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    )
    source.to_file(path, driver="GeoJSON")

    loaded = load_local_adm1_artifact(path)

    assert {"state_name", "shapeName", "geometry"}.issubset(set(loaded.columns))
    assert sorted(loaded["state_name"].astype(str).tolist()) == ["Odisha", "Telangana"]
    assert str(loaded.crs).upper().endswith("4326")
    assert len(loaded) == 2


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


def test_optimized_context_copies_rp100_overlay_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "jrc_flood_depth" / "overlay"
    source_dir.mkdir(parents=True)
    (source_dir / "rp100_depth_overlay.png").write_bytes(b"png")
    (source_dir / "rp100_depth_overlay_meta.json").write_text('{"artifact": true}', encoding="utf-8")

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=[],
        include_geometry=False,
        include_context=True,
    )
    overlay_tasks = tuple(
        task for task in plan.context_tasks if "rp100_depth_overlay" in str(task.target_path)
    )
    progress = BuildProgress(
        BuildPlan(
            summaries_seed=(),
            master_tasks=(),
            yearly_model_jobs=(),
            yearly_ensemble_jobs=(),
            context_tasks=overlay_tasks,
            geometry_tasks=(),
            manifest_task=BuildTask(stage="manifest", label="bundle manifest"),
        ),
        enabled=False,
    )

    _copy_context_artifacts(tasks=overlay_tasks, progress=progress)

    assert optimized_context_path(
        "jrc_flood_depth/overlay/rp100_depth_overlay.png",
        data_dir=tmp_path,
    ).read_bytes() == b"png"
    assert optimized_context_path(
        "jrc_flood_depth/overlay/rp100_depth_overlay_meta.json",
        data_dir=tmp_path,
    ).read_text(encoding="utf-8") == '{"artifact": true}'


def test_optimized_context_copies_population_overlay_artifacts_and_manifest_version(tmp_path: Path) -> None:
    source_dir = tmp_path / "population" / "overlay"
    source_dir.mkdir(parents=True)
    (source_dir / "population_exposure_2025_overlay.png").write_bytes(b"png")
    (source_dir / "population_exposure_2025_overlay_meta.json").write_text('{"artifact": true}', encoding="utf-8")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=[],
        include_geometry=False,
        include_context=True,
        show_progress=False,
        run_audit=False,
    )

    assert optimized_context_path(
        "population/overlay/population_exposure_2025_overlay.png",
        data_dir=tmp_path,
    ).read_bytes() == b"png"
    assert optimized_context_path(
        "population/overlay/population_exposure_2025_overlay_meta.json",
        data_dir=tmp_path,
    ).read_text(encoding="utf-8") == '{"artifact": true}'
    assert _read_manifest(tmp_path / "processed_optimised")["artifact_version"] == 3


def test_optimized_context_copies_built_up_overlay_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "built_up_area" / "overlay"
    source_dir.mkdir(parents=True)
    (source_dir / "built_up_area_current_overlay.png").write_bytes(b"png")
    (source_dir / "built_up_area_current_overlay_meta.json").write_text('{"artifact": true}', encoding="utf-8")

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=[],
        include_geometry=False,
        include_context=True,
    )
    built_tasks = tuple(task for task in plan.context_tasks if "built_up_area_current_overlay" in str(task.target_path))

    assert len(built_tasks) == 2
    assert {
        task.target_path.relative_to(tmp_path / "processed_optimised" / "context").as_posix()
        for task in built_tasks
    } == {
        "built_up_area/overlay/built_up_area_current_overlay.png",
        "built_up_area/overlay/built_up_area_current_overlay_meta.json",
    }


def test_optimized_context_copies_lulc_overlay_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "lulc" / "overlay"
    source_dir.mkdir(parents=True)
    (source_dir / "lulc_agri_current_overlay.png").write_bytes(b"png")
    (source_dir / "lulc_agri_current_overlay_meta.json").write_text('{"artifact": true}', encoding="utf-8")

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=[],
        include_geometry=False,
        include_context=True,
    )
    lulc_tasks = tuple(task for task in plan.context_tasks if "lulc_agri_current_overlay" in str(task.target_path))

    assert len(lulc_tasks) == 2
    assert {
        task.target_path.relative_to(tmp_path / "processed_optimised" / "context").as_posix()
        for task in lulc_tasks
    } == {
        "lulc/overlay/lulc_agri_current_overlay.png",
        "lulc/overlay/lulc_agri_current_overlay_meta.json",
    }


def test_optimized_context_copies_rural_facilities_overlay_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "rural_facilities" / "overlay"
    source_dir.mkdir(parents=True)
    for category in ("total", "agro", "education", "health", "service"):
        (source_dir / f"rural_facilities_density_{category}_overlay.png").write_bytes(b"png")
        (source_dir / f"rural_facilities_density_{category}_overlay_meta.json").write_text('{"artifact": true}', encoding="utf-8")

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=[],
        include_geometry=False,
        include_context=True,
    )
    rural_tasks = tuple(
        task for task in plan.context_tasks if "rural_facilities_density_" in str(task.target_path)
    )

    assert len(rural_tasks) == 10
    assert {
        task.target_path.relative_to(tmp_path / "processed_optimised" / "context").as_posix()
        for task in rural_tasks
    } == {
        f"rural_facilities/overlay/rural_facilities_density_{category}_overlay{suffix}"
        for category in ("total", "agro", "education", "health", "service")
        for suffix in (".png", "_meta.json")
    }


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
    adm1_path = optimized_adm1_path(data_dir=tmp_path)

    assert district_path.exists()
    assert block_path.exists()
    assert block_index_path.exists()
    assert hydro_index_path.exists()
    assert adm1_path.exists()

    adm1_out = gpd.read_file(adm1_path)
    assert {"state_name", "shapeName"}.issubset(set(adm1_out.columns))
    assert sorted(adm1_out["state_name"].astype(str).str.strip().tolist()) == ["Telangana"]
    assert str(adm1_out.crs).upper().endswith("4326")

    district_out = gpd.read_file(district_path)
    assert "area_m2" in district_out.columns

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
        # Admin-only geometry after the hydro-family removal: basin (1) and
        # sub-basin (1 per basin_id) geometry tasks are no longer emitted.
        "geometry": 5,
        "glance": 0,
        "manifest": 1,
    }
    assert plan.total_tasks == 14


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
    assert "remaining_tasks=1" in summary
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


def test_build_processed_optimised_writes_admin_yearly_outputs(
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

    summaries = build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        overwrite=False,
        include_geometry=False,
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
    assert district_ensemble.exists()
    assert block_models.exists()

    district_df = pd.read_parquet(district_ensemble)

    assert district_df["district_key"].tolist() == ["telangana|hanumakonda"]
    assert district_df["mean"].tolist() == [1.5]


def test_build_processed_optimised_writes_proposal_bundle_admin_masters(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))

    slug = "composite_health_risk"
    legacy_root = tmp_path / "processed" / slug / "Telangana"
    legacy_root.mkdir(parents=True)
    (legacy_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,"
        "composite_health_risk__ssp585__2040-2060__mean,"
        "composite_health_risk__ssp585__2040-2060__available_rule_count,"
        "composite_health_risk__ssp585__2040-2060__available_rule_weight_fraction,"
        "txx_ge_45__ssp585__2040-2060__score,"
        "txx_ge_45__ssp585__2040-2060__abs_score,"
        "txx_ge_45__ssp585__2040-2060__chg_score,"
        "txx_ge_45__ssp585__2040-2060__imp_score,"
        "wsdi_ge_5__ssp585__2040-2060__score,"
        "debug_internal_counter\n"
        "Telangana,Hanumakonda,telangana|hanumakonda,75.0,2,0.4,100.0,90.0,80.0,70.0,50.0,99\n",
        encoding="utf-8",
    )
    (legacy_root / "master_metrics_by_block.csv").write_text(
        "state,district,block,block_key,"
        "composite_health_risk__ssp585__2040-2060__mean,"
        "composite_health_risk__ssp585__2040-2060__available_rule_count,"
        "composite_health_risk__ssp585__2040-2060__available_rule_weight_fraction,"
        "txx_ge_45__ssp585__2040-2060__score,"
        "txx_ge_45__ssp585__2040-2060__abs_score,"
        "txx_ge_45__ssp585__2040-2060__chg_score,"
        "txx_ge_45__ssp585__2040-2060__imp_score,"
        "wsdi_ge_5__ssp585__2040-2060__score,"
        "debug_internal_counter\n"
        "Telangana,Hanumakonda,Atmakur,telangana|hanumakonda|atmakur,80.0,2,0.4,75.0,70.0,60.0,55.0,25.0,101\n",
        encoding="utf-8",
    )

    summaries = build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=[slug],
        levels=["district", "block"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    assert summaries and summaries[0].wrote_masters is True
    district_master = (
        tmp_path / "processed_optimised" / "metrics" / slug / "masters" / "admin" / "district" / "state=Telangana.parquet"
    )
    block_master = (
        tmp_path / "processed_optimised" / "metrics" / slug / "masters" / "admin" / "block" / "state=Telangana.parquet"
    )

    assert district_master.exists()
    assert block_master.exists()

    district_df = pd.read_parquet(district_master)
    block_df = pd.read_parquet(block_master)

    for df in (district_df, block_df):
        assert "composite_health_risk__ssp585__2040-2060__mean" in df.columns
        assert "composite_health_risk__ssp585__2040-2060__available_rule_count" in df.columns
        assert "composite_health_risk__ssp585__2040-2060__available_rule_weight_fraction" in df.columns
        assert "txx_ge_45__ssp585__2040-2060__score" in df.columns
        assert "txx_ge_45__ssp585__2040-2060__abs_score" in df.columns
        assert "txx_ge_45__ssp585__2040-2060__chg_score" in df.columns
        assert "txx_ge_45__ssp585__2040-2060__imp_score" in df.columns
        assert "wsdi_ge_5__ssp585__2040-2060__score" in df.columns
        assert "debug_internal_counter" not in df.columns

    manifest = _read_manifest(tmp_path / "processed_optimised")
    assert manifest["stats_contract"]["proposal_bundle"] == [
        "mean",
        "score",
        "abs_score",
        "chg_score",
        "imp_score",
        "available_rule_count",
        "available_rule_weight_fraction",
    ]


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

    # Thread executor (opt-in) must produce byte-identical bundle output.
    thread_root = tmp_path / "thread"
    _write_admin_legacy_metric_fixture(thread_root)
    _write_hydro_legacy_metric_fixture(thread_root)
    _write_geometry_fixture(thread_root)
    monkeypatch.setenv("IRT_DATA_DIR", str(thread_root))
    monkeypatch.setenv("IRT_YEARLY_EXECUTOR", "thread")
    build_processed_optimised_bundle(
        data_dir=thread_root,
        metrics=["txx_annual_max"],
        workers=2,
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )
    thread_tables = _read_output_tables(thread_root / "processed_optimised", slug="txx_annual_max")
    for name in sorted(serial_tables):
        assert_frame_equal(serial_tables[name], thread_tables[name], check_like=False)


# --- Yearly-loader executor: chunk-plan, fan-out, and serial-branch unit tests ---


class _SpyProgress:
    """Minimal progress stub recording per-chunk advance_stage calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def advance_stage(self, *, stage: str, label: str, count: int) -> None:
        self.calls.append((stage, label, count))

    def start_task(self, task) -> None:  # pragma: no cover - unused by these paths
        pass


def _scale_chunk(chunk_index: int, chunk: tuple[int, ...], *, factor: int):
    return chunk_index, pd.DataFrame({"v": [x * factor for x in chunk]})


def _boom_worker(chunk_index: int, chunk: tuple[int, ...], **_kwargs):
    raise ValueError("boom")


def test_yearly_chunk_size_selects_per_kind() -> None:
    # process keeps the fixed (large) chunk regardless of worker count
    assert _yearly_chunk_size(33, 28, "process") == YEARLY_PARALLEL_CHUNK_SIZE
    # thread with >1 worker splits per worker (ceil)
    assert _yearly_chunk_size(33, 28, "thread") == 2  # ceil(33/28)
    assert _yearly_chunk_size(10, 4, "thread") == 3  # ceil(10/4)
    # thread with a single worker falls back to the fixed chunk (serial anyway)
    assert _yearly_chunk_size(33, 1, "thread") == YEARLY_PARALLEL_CHUNK_SIZE


def test_yearly_executor_kind_reads_env(monkeypatch) -> None:
    monkeypatch.delenv("IRT_YEARLY_EXECUTOR", raising=False)
    assert _yearly_executor_kind() == "process"
    monkeypatch.setenv("IRT_YEARLY_EXECUTOR", "thread")
    assert _yearly_executor_kind() == "thread"
    monkeypatch.setenv("IRT_YEARLY_EXECUTOR", "THREAD")
    assert _yearly_executor_kind() == "thread"
    monkeypatch.setenv("IRT_YEARLY_EXECUTOR", "garbage")
    assert _yearly_executor_kind() == "process"


def test_execute_parallel_chunks_thread_preserves_chunk_order() -> None:
    chunks = ((1, 2), (3,), (4, 5), (6,))
    progress = _SpyProgress()
    results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-models",
        label_prefix="t",
        chunks=chunks,
        worker_count=4,
        worker_fn=_scale_chunk,
        worker_kwargs={"factor": 10},
        kind="thread",
    )
    assert [idx for idx, _ in results] == [0, 1, 2, 3]
    assert results[2][1]["v"].tolist() == [40, 50]
    assert len(progress.calls) == len(chunks)


def test_execute_parallel_chunks_serial_branch_runs_in_process() -> None:
    chunks = ((1,), (2,), (3,))
    progress = _SpyProgress()
    # worker_count=1 forces max_workers<=1 -> serial in-process, no pool spawn
    results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-models",
        label_prefix="t",
        chunks=chunks,
        worker_count=1,
        worker_fn=_scale_chunk,
        worker_kwargs={"factor": 2},
        kind="process",
    )
    assert [idx for idx, _ in results] == [0, 1, 2]
    assert results[0][1]["v"].tolist() == [2]
    assert len(progress.calls) == len(chunks)


def test_execute_parallel_chunks_serial_branch_propagates_exception() -> None:
    with pytest.raises(ValueError, match="boom"):
        _execute_parallel_chunks(
            progress=_SpyProgress(),
            stage="yearly-models",
            label_prefix="t",
            chunks=((1,),),
            worker_count=1,
            worker_fn=_boom_worker,
            worker_kwargs={},
            kind="process",
        )


def test_execute_parallel_chunks_thread_branch_propagates_exception() -> None:
    with pytest.raises(ValueError, match="boom"):
        _execute_parallel_chunks(
            progress=_SpyProgress(),
            stage="yearly-models",
            label_prefix="t",
            chunks=((1,), (2,), (3,)),
            worker_count=4,
            worker_fn=_boom_worker,
            worker_kwargs={},
            kind="thread",
        )


def _write_district_yearly_models(
    root: Path,
    *,
    slug: str,
    state: str,
    district: str,
    models: list[str],
    scenario: str = "ssp245",
) -> tuple[Path, ...]:
    base = root / "processed" / slug / state / "districts" / district
    paths: list[Path] = []
    for i, model in enumerate(models):
        model_dir = base / model / scenario
        model_dir.mkdir(parents=True)
        path = model_dir / f"{district}_yearly.csv"
        path.write_text(f"year,value\n2030,{float(i)}\n2031,{float(i) + 0.5}\n", encoding="utf-8")
        paths.append(path)
    return tuple(paths)


def _empty_progress() -> BuildProgress:
    plan = BuildPlan(
        summaries_seed=(),
        master_tasks=(),
        yearly_model_jobs=(),
        yearly_ensemble_jobs=(),
        context_tasks=(),
        geometry_tasks=(),
        manifest_task=BuildTask(stage="manifest", label="manifest"),
    )
    return BuildProgress(plan, enabled=False)


def test_load_admin_yearly_models_thread_matches_serial(tmp_path: Path, monkeypatch) -> None:
    paths = _write_district_yearly_models(
        tmp_path,
        slug="txx_annual_max",
        state="Telangana",
        district="Hanumakonda",
        models=["ModelA", "ModelB", "ModelC"],
    )
    # Guard G6: the thread arm must genuinely fan out (>1 chunk), else the
    # ThreadPoolExecutor is never exercised and the parity check is vacuous.
    chunk_size = _yearly_chunk_size(len(paths), 4, "thread")
    assert len(_chunk_tuple(tuple(str(p) for p in paths), chunk_size=chunk_size)) > 1

    monkeypatch.delenv("IRT_YEARLY_EXECUTOR", raising=False)
    serial = _load_legacy_admin_yearly_models(
        slug="txx_annual_max",
        state_name="Telangana",
        level="district",
        csv_paths=paths,
        progress=_empty_progress(),
        workers=1,
    )

    monkeypatch.setenv("IRT_YEARLY_EXECUTOR", "thread")
    threaded = _load_legacy_admin_yearly_models(
        slug="txx_annual_max",
        state_name="Telangana",
        level="district",
        csv_paths=paths,
        progress=_empty_progress(),
        workers=4,
    )

    sort_cols = ["model", "year"]
    assert_frame_equal(
        serial.sort_values(sort_cols).reset_index(drop=True),
        threaded.sort_values(sort_cols).reset_index(drop=True),
    )


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
    assert manifest["artifact_version"] == 3
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

    assert stale_selected_block.exists()
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


def test_build_processed_optimised_state_scope_preserves_other_states_and_shared_globals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, state="Telangana", district="Hanumakonda", block="Atmakur")
    _write_admin_legacy_metric_fixture(tmp_path, state="Karnataka", district="Bengaluru", block="Anekal")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    bundle_root = tmp_path / "processed_optimised"
    global_manifest = bundle_root / "bundle_manifest.json"
    global_manifest.write_text('{"global": true}', encoding="utf-8")
    global_parity = bundle_root / "parity_report.json"
    global_parity.write_text('{"global": true}', encoding="utf-8")
    adm1_path = bundle_root / "geometry" / "admin" / "adm1.geojson"
    adm1_path.parent.mkdir(parents=True, exist_ok=True)
    adm1_path.write_text("shared", encoding="utf-8")

    telangana_master = (
        bundle_root / "metrics" / "txx_annual_max" / "masters" / "admin" / "district" / "state=Telangana.parquet"
    )
    karnataka_master = (
        bundle_root / "metrics" / "txx_annual_max" / "masters" / "admin" / "district" / "state=Karnataka.parquet"
    )
    assert telangana_master.exists()
    assert karnataka_master.exists()

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        levels=["district"],
        states=["telangana"],
        overwrite=True,
        prune_scope=True,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=True,
    )

    assert telangana_master.exists()
    assert karnataka_master.exists()
    assert global_manifest.read_text(encoding="utf-8") == '{"global": true}'
    assert global_parity.read_text(encoding="utf-8") == '{"global": true}'
    assert adm1_path.read_text(encoding="utf-8") == "shared"


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


def test_build_execution_plan_with_state_defaults_to_admin_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, state="Telangana", district="Hanumakonda", block="Atmakur")
    _write_hydro_legacy_metric_fixture(tmp_path)

    plan = _build_execution_plan(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        states=["Telangana"],
        include_geometry=False,
        include_context=False,
    )

    assert {task.level for task in plan.master_tasks} <= {"district", "block"}
    assert {job.level for job in plan.yearly_ensemble_jobs} <= {"district", "block"}


def test_audit_processed_optimised_state_scope_does_not_write_global_report_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, state="Telangana", district="Hanumakonda", block="Atmakur")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    bundle_root = tmp_path / "processed_optimised"
    global_report = bundle_root / "parity_report.json"
    global_report.write_text('{"global": true}', encoding="utf-8")

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        states=["Telangana"],
        include_geometry=False,
        include_context=False,
        write_report=True,
    )

    assert report["issue_count"] == 0
    assert global_report.read_text(encoding="utf-8") == '{"global": true}'


def test_audit_processed_optimised_state_scope_writes_only_explicit_scoped_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, state="Telangana", district="Hanumakonda", block="Atmakur")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    bundle_root = tmp_path / "processed_optimised"
    global_report = bundle_root / "parity_report.json"
    global_report.write_text('{"global": true}', encoding="utf-8")
    scoped_report = bundle_root / "parity_report__admin__Telangana.json"

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["txx_annual_max"],
        levels=["district"],
        states=["Telangana"],
        include_geometry=False,
        include_context=False,
        write_report=False,
        report_path=scoped_report,
    )

    assert report["issue_count"] == 0
    assert global_report.read_text(encoding="utf-8") == '{"global": true}'
    assert scoped_report.exists()


def test_list_optimized_yearly_metrics_reports_state_block_metrics(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, slug="tas_annual_mean")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    assert list_optimized_yearly_metrics(data_dir=tmp_path, level="block", state="Telangana") == ["tas_annual_mean"]


def test_strict_audit_requires_block_yearly_models_when_ensemble_exists(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path))
    _write_admin_legacy_metric_fixture(tmp_path, slug="tas_annual_mean")

    build_processed_optimised_bundle(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        overwrite=False,
        include_geometry=False,
        include_context=False,
        show_progress=False,
        run_audit=False,
    )

    models_path = (
        tmp_path
        / "processed_optimised"
        / "metrics"
        / "tas_annual_mean"
        / "yearly_models"
        / "admin"
        / "block"
        / "state=Telangana.parquet"
    )
    models_path.unlink()

    report = audit_processed_optimised_parity(
        data_dir=tmp_path,
        metrics=["tas_annual_mean"],
        levels=["block"],
        states=["Telangana"],
        include_geometry=False,
        include_context=False,
        write_report=False,
        require_block_yearly_models=True,
    )

    assert any(
        issue.get("severity") == "error"
        and issue.get("reason") == "block_yearly_ensemble_without_yearly_models"
        for issue in report["issues"]
    )
