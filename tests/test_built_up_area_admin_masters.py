from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_origin
from shapely.geometry import Polygon

import tools.geodata.build_built_up_area_admin_masters as built_module
from tools.geodata.build_built_up_area_admin_masters import (
    BUILT_UP_AREA_KM2_COL,
    BUILT_UP_AREA_OVERLAY_ID,
    BUILT_UP_AREA_SHARE_PCT_COL,
    INVALID_VALUE,
    aggregate_built_up_area_to_admin_units,
    build_built_up_area_admin_outputs,
    export_built_up_area_overlay,
    validate_built_up_raster_contract,
)


def _write_raster(path: Path, data: np.ndarray | None = None) -> Path:
    arr = np.array([[0.0, 100.0], [INVALID_VALUE, 300.0]], dtype="float32") if data is None else data
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype="float32",
        crs="EPSG:6933",
        transform=from_origin(0, float(arr.shape[0]), 1, 1),
        nodata=None,
    ) as dst:
        dst.write(arr, 1)
    return path


def _districts() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["Telangana::Demo District"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:6933",
    )


def _blocks() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Demo District", "Demo District"],
            "block_name": ["Top Block", "Bottom Block"],
            "block_key": ["top", "bottom"],
        },
        geometry=[
            Polygon([(0, 1), (2, 1), (2, 2), (0, 2)]),
            Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),
        ],
        crs="EPSG:6933",
    )


def test_built_up_aggregation_valid_zero_invalid_65535_and_share_qa(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "built.tif")
    district_master, district_qa = aggregate_built_up_area_to_admin_units(
        _districts(),
        level="district",
        raster_path=raster_path,
    )
    row = district_master.iloc[0]
    assert float(row[BUILT_UP_AREA_KM2_COL]) == pytest.approx(0.0004)
    assert float(row[BUILT_UP_AREA_SHARE_PCT_COL]) == pytest.approx(10_000.0)
    assert int(district_qa["raster_supported_cell_count"].iloc[0]) == 3
    assert float(district_qa["raster_supported_area_km2"].iloc[0]) == pytest.approx(0.000003)
    assert float(district_qa["support_area_pct_of_polygon"].iloc[0]) == pytest.approx(75.0)

    block_master, block_qa = aggregate_built_up_area_to_admin_units(_blocks(), level="block", raster_path=raster_path)
    totals = {row["block"]: float(row[BUILT_UP_AREA_KM2_COL]) for _, row in block_master.iterrows()}
    assert totals == {"Bottom Block": pytest.approx(0.0003), "Top Block": pytest.approx(0.0001)}
    counts = {row["block"]: int(row["raster_supported_cell_count"]) for _, row in block_qa.iterrows()}
    assert counts == {"Bottom Block": 1, "Top Block": 2}


def test_centroid_inclusion_rule_excludes_edge_cell(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "built.tif", np.array([[10.0]], dtype="float32"))
    tiny_corner = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["tiny"],
        },
        geometry=[Polygon([(0, 0.8), (0.2, 0.8), (0.2, 1), (0, 1)])],
        crs="EPSG:6933",
    )
    master, qa = aggregate_built_up_area_to_admin_units(tiny_corner, level="district", raster_path=raster_path)
    assert float(master[BUILT_UP_AREA_KM2_COL].iloc[0]) == 0.0
    assert int(qa["raster_supported_cell_count"].iloc[0]) == 0


def test_national_total_guardrail_fails_unless_allowed(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "built.tif")
    with pytest.raises(ValueError, match="guardrail"):
        validate_built_up_raster_contract(raster_path)
    summary = validate_built_up_raster_contract(raster_path, allow_total_outlier=True)
    assert summary.national_built_up_area_km2 == pytest.approx(0.0004)
    assert summary.min_value == 0.0
    assert summary.max_value == 300.0


def test_overlay_exports_required_metadata_and_bins(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "built.tif", np.array([[0.0, 50.0, 250.0, 750.0, 1500.0, 4000.0, 6000.0]], dtype="float32"))
    overlay = export_built_up_area_overlay(
        raster_path=raster_path,
        overlay_dir=tmp_path / "overlay",
        overwrite=False,
        dry_run=False,
    )
    meta = json.loads(Path(overlay["meta_path"]).read_text(encoding="utf-8"))
    assert meta["overlay_id"] == BUILT_UP_AREA_OVERLAY_ID
    assert meta["source_raster_name"] == "Cleaned_India_Built_Surface_WGS84.tif"
    assert meta["image_crs"] == "EPSG:3857"
    assert meta["snapshot_period"] == "Current"
    assert meta["display_units"] == "m2/source cell"
    assert meta["invalid_value"] == 65535
    assert meta["bin_edges_m2_per_cell"] == [0.0, 100.0, 500.0, 1000.0, 2500.0, 5000.0]
    assert meta["bin_colors_hex"][1:] == ["#edf8fb", "#b2e2e2", "#66c2a4", "#2ca25f", "#006d2c", "#00441b"]
    assert meta["clipped_above_display_max"] is True
    assert Image.open(overlay["png_path"]).convert("RGBA").size[0] <= 4096


def test_builder_dry_run_skips_missing_blocks_and_reports_planned_outputs(tmp_path: Path, monkeypatch) -> None:
    raster_path = _write_raster(tmp_path / "built.tif")
    (tmp_path / "districts.geojson").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(built_module, "load_district_boundaries", lambda _path: _districts())
    outputs = build_built_up_area_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "missing_blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=tmp_path / "overlay",
        overwrite=False,
        dry_run=True,
        allow_total_outlier=True,
    )
    assert outputs["block_master_df"] is None
    assert outputs["district_master_df"].shape[0] == 1
    assert any("built_up_area_current_overlay.png" in str(path) for path in outputs["planned_paths"])
