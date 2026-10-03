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

import tools.geodata.build_lulc_admin_masters as lulc_module
from tools.geodata.build_lulc_admin_masters import (
    LULC_AGRI_AREA_KM2_COL,
    LULC_AGRI_OVERLAY_ID,
    LULC_AGRI_SHARE_PCT_COL,
    aggregate_lulc_agri_to_admin_units,
    build_lulc_agri_admin_outputs,
    export_lulc_agri_overlay,
    validate_lulc_agri_raster_contract,
)


def _write_raster(path: Path, data: np.ndarray | None = None) -> Path:
    arr = (
        np.array(
            [
                [1, 1, 0, 0],
                [1, 0, 0, 0],
                [0, 0, 1, 1],
                [0, 0, 1, 1],
            ],
            dtype="uint8",
        )
        if data is None
        else data
    )
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=arr.shape[0],
        width=arr.shape[1],
        count=1,
        dtype=str(arr.dtype),
        crs="EPSG:6933",
        transform=from_origin(0, float(arr.shape[0] * 100), 100, 100),
        nodata=0,
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
        geometry=[Polygon([(0, 0), (400, 0), (400, 400), (0, 400)])],
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
            Polygon([(0, 200), (400, 200), (400, 400), (0, 400)]),
            Polygon([(0, 0), (400, 0), (400, 200), (0, 200)]),
        ],
        crs="EPSG:6933",
    )


def test_lulc_aggregation_counts_only_value_one_and_uses_equal_area_share(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "lulc.tif")
    district_master, district_qa = aggregate_lulc_agri_to_admin_units(
        _districts(),
        level="district",
        raster_path=raster_path,
    )
    row = district_master.iloc[0]
    assert int(row["agri_cell_count"]) == 7
    assert float(row[LULC_AGRI_AREA_KM2_COL]) == pytest.approx(0.07)
    assert float(row[LULC_AGRI_SHARE_PCT_COL]) == pytest.approx(43.75)
    assert float(district_qa["raster_extent_support_area_km2"].iloc[0]) == pytest.approx(0.16)
    assert float(district_qa["support_area_pct_of_polygon"].iloc[0]) == pytest.approx(100.0)
    assert bool(district_qa["low_support_coverage"].iloc[0]) is False

    block_master, _block_qa = aggregate_lulc_agri_to_admin_units(_blocks(), level="block", raster_path=raster_path)
    totals = {row["block"]: float(row[LULC_AGRI_AREA_KM2_COL]) for _, row in block_master.iterrows()}
    assert totals == {"Bottom Block": pytest.approx(0.04), "Top Block": pytest.approx(0.03)}


def test_lulc_rejects_unexpected_values_unless_allowed(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "lulc.tif", np.array([[0, 1], [2, 1]], dtype="uint8"))
    with pytest.raises(ValueError, match="outside \\{0, 1\\}"):
        validate_lulc_agri_raster_contract(raster_path, allow_total_outlier=True)
    summary = validate_lulc_agri_raster_contract(
        raster_path,
        allow_total_outlier=True,
        allow_unexpected_values=True,
    )
    assert summary.value_counts == {0: 1, 1: 2}
    assert summary.unexpected_value_count == 1


def test_lulc_centroid_inclusion_excludes_edge_cell(tmp_path: Path) -> None:
    raster_path = _write_raster(tmp_path / "lulc.tif", np.array([[1]], dtype="uint8"))
    tiny_corner = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["tiny"],
        },
        geometry=[Polygon([(0, 80), (20, 80), (20, 100), (0, 100)])],
        crs="EPSG:6933",
    )
    master, qa = aggregate_lulc_agri_to_admin_units(tiny_corner, level="district", raster_path=raster_path)
    assert float(master[LULC_AGRI_AREA_KM2_COL].iloc[0]) == 0.0
    assert int(qa["agri_cell_count"].iloc[0]) == 0


def test_lulc_national_total_guardrail_and_share_outlier(tmp_path: Path, monkeypatch) -> None:
    raster_path = _write_raster(tmp_path / "lulc.tif")
    with pytest.raises(ValueError, match="guardrail"):
        validate_lulc_agri_raster_contract(raster_path)
    summary = validate_lulc_agri_raster_contract(raster_path, allow_total_outlier=True)
    assert summary.national_agri_area_km2 == pytest.approx(0.07)

    tiny = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Tiny District"],
            "district_key": ["tiny"],
        },
        geometry=[Polygon([(0, 300), (50, 300), (50, 350), (0, 350)])],
        crs="EPSG:6933",
    )
    (tmp_path / "districts.geojson").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(lulc_module, "load_district_boundaries", lambda _path: tiny)
    with pytest.raises(ValueError, match="100.01%"):
        build_lulc_agri_admin_outputs(
            raster_path=raster_path,
            districts_path=tmp_path / "districts.geojson",
            blocks_path=None,
            qa_dir=tmp_path / "qa",
            overlay_dir=tmp_path / "overlay",
            overwrite=False,
            dry_run=True,
            allow_total_outlier=True,
            allow_unexpected_values=False,
            allow_share_outlier=False,
        )


def test_lulc_builder_dry_run_and_overlay_metadata(tmp_path: Path, monkeypatch) -> None:
    raster_path = _write_raster(tmp_path / "lulc.tif")
    (tmp_path / "districts.geojson").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(lulc_module, "load_district_boundaries", lambda _path: _districts())
    outputs = build_lulc_agri_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "missing_blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=tmp_path / "overlay",
        overwrite=False,
        dry_run=True,
        allow_total_outlier=True,
        allow_unexpected_values=False,
        allow_share_outlier=False,
    )
    assert outputs["block_master_df"] is None
    assert outputs["district_master_df"].shape[0] == 1
    assert any("lulc_agri_current_overlay.png" in str(path) for path in outputs["planned_paths"])

    overlay = export_lulc_agri_overlay(
        raster_path=raster_path,
        overlay_dir=tmp_path / "overlay",
        overwrite=False,
        dry_run=False,
    )
    meta = json.loads(Path(overlay["meta_path"]).read_text(encoding="utf-8"))
    assert meta["overlay_id"] == LULC_AGRI_OVERLAY_ID
    assert meta["source_raster_name"] == "LULC_2_Agri.tif"
    assert meta["image_crs"] == "EPSG:3857"
    assert meta["display_transform"] == "nearest_binary_class"
    assert meta["valid_value"] == 1
    assert meta["nodata_value"] == 0
    assert meta["valid_color_hex"] == "#2ca25f"
    assert Image.open(overlay["png_path"]).convert("RGBA").size[0] <= 4096
