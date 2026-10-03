from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import pytest
from PIL import Image
from rasterio.transform import from_origin
from shapely.geometry import Polygon

import tools.geodata.build_population_admin_masters as population_module
from tools.geodata.build_population_admin_masters import (
    POPULATION_COLOR_RAMP,
    POPULATION_DENSITY_COL,
    POPULATION_EXPOSURE_OVERLAY_ID,
    POPULATION_TOTAL_COL,
    aggregate_population_to_admin_units,
    build_population_admin_outputs,
    build_population_consistency_qa,
    build_population_national_summary,
)


def _write_test_raster(path: Path) -> Path:
    data = np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as dst:
        dst.write(data, 1)
    return path


def _write_overlay_test_raster(path: Path, *, width: int = 3, height: int = 3) -> Path:
    data = np.zeros((height, width), dtype="float32")
    values = [0.0, 10.0, 50.0, 200.0, 400.0, 750.0, 1500.0, 4000.0, 7500.0, 12000.0]
    flat = data.ravel()
    for idx, value in enumerate(values[: flat.size]):
        flat[idx] = value
    data = flat.reshape((height, width))
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, float(height), 1, 1),
        nodata=0.0,
    ) as dst:
        dst.write(data, 1)
    return path


def _districts_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["Telangana::Demo District"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )


def _blocks_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Demo District", "Demo District"],
            "block_name": ["Top Block", "Bottom Block"],
            "block_key": [
                "Telangana::Demo District::Top Block",
                "Telangana::Demo District::Bottom Block",
            ],
        },
        geometry=[
            Polygon([(0, 1), (2, 1), (2, 2), (0, 2)]),
            Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),
        ],
        crs="EPSG:4326",
    )


def test_aggregate_population_to_districts_and_blocks(tmp_path: Path) -> None:
    raster_path = _write_test_raster(tmp_path / "population.tif")

    district_master_df, district_qa_df = aggregate_population_to_admin_units(
        _districts_gdf(),
        level="district",
        raster_path=raster_path,
    )
    block_master_df, block_qa_df = aggregate_population_to_admin_units(
        _blocks_gdf(),
        level="block",
        raster_path=raster_path,
    )

    assert district_master_df["state"].tolist() == ["Telangana"]
    assert district_master_df["district"].tolist() == ["Demo District"]
    assert float(district_master_df[POPULATION_TOTAL_COL].iloc[0]) == 100.0
    assert district_qa_df["raster_cell_count"].tolist() == [4]

    assert block_master_df["block"].tolist() == ["Bottom Block", "Top Block"]
    totals = {
        row["block"]: float(row[POPULATION_TOTAL_COL])
        for _, row in block_master_df.iterrows()
    }
    assert totals == {"Top Block": 30.0, "Bottom Block": 70.0}
    assert block_qa_df["raster_cell_count"].tolist() == [2, 2]

    for _, row in district_master_df.iterrows():
        assert np.isclose(
            float(row[POPULATION_DENSITY_COL]),
            float(row[POPULATION_TOTAL_COL]) / float(row["district_area_km2"]),
        )
    for _, row in block_master_df.iterrows():
        assert np.isclose(
            float(row[POPULATION_DENSITY_COL]),
            float(row[POPULATION_TOTAL_COL]) / float(row["block_area_km2"]),
        )


def test_population_consistency_and_national_summary(tmp_path: Path) -> None:
    raster_path = _write_test_raster(tmp_path / "population.tif")
    district_master_df, district_qa_df = aggregate_population_to_admin_units(
        _districts_gdf(),
        level="district",
        raster_path=raster_path,
    )
    block_master_df, _ = aggregate_population_to_admin_units(
        _blocks_gdf(),
        level="block",
        raster_path=raster_path,
    )

    consistency_df = build_population_consistency_qa(district_master_df, block_master_df)
    assert consistency_df["difference_abs"].tolist() == [0.0]

    national_summary_df = build_population_national_summary(
        district_master_df,
        block_master_df,
        district_qa_df=district_qa_df,
    )
    row = national_summary_df.iloc[0]
    assert float(row["raster_population_total"]) == 100.0
    assert float(row["district_population_total"]) == 100.0
    assert float(row["block_population_total"]) == 100.0


def _patch_boundary_loaders_and_writers(monkeypatch) -> None:
    monkeypatch.setattr(population_module, "load_district_boundaries", lambda _path: _districts_gdf())
    monkeypatch.setattr(population_module, "load_block_boundaries", lambda _path: _blocks_gdf())
    monkeypatch.setattr(
        population_module,
        "_write_state_slices",
        lambda master_df, *, metric_slug, level, overwrite: {"Telangana": int(master_df.shape[0])},
    )


def test_builder_exports_population_overlay_png_and_metadata(tmp_path: Path, monkeypatch) -> None:
    _patch_boundary_loaders_and_writers(monkeypatch)
    raster_path = _write_overlay_test_raster(tmp_path / "population.tif")
    overlay_dir = tmp_path / "population" / "overlay"

    outputs = build_population_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=overlay_dir,
        overwrite=True,
        dry_run=False,
    )

    overlay = outputs["population_overlay"]
    png_path = overlay["png_path"]
    meta_path = overlay["meta_path"]
    assert png_path == overlay_dir / "population_exposure_2025_overlay.png"
    assert meta_path == overlay_dir / "population_exposure_2025_overlay_meta.json"
    assert png_path.exists()
    assert meta_path.exists()

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta == overlay["metadata"]
    assert meta["overlay_id"] == POPULATION_EXPOSURE_OVERLAY_ID
    assert meta["source_raster_name"] == "ind_pop_2025_CN_1km_R2025A_UA_v1.tif"
    assert meta["display_units"] == "people per source cell"
    assert meta["display_transform"] == "binned_people_per_source_cell"
    assert meta["display_value_min_people_per_cell"] == 0.0
    assert meta["display_value_max_people_per_cell"] == 10000.0
    assert meta["source_positive_max_people_per_cell"] == 7500.0
    assert meta["clipped_above_display_max"] is False
    assert meta["width_px"] == 3
    assert meta["height_px"] == 3
    bounds = meta["bounds_latlon"]
    assert bounds[0][0] == pytest.approx(0.0, abs=0.05)   # south
    assert bounds[0][1] == pytest.approx(0.0, abs=0.05)   # west
    assert bounds[1][0] == pytest.approx(3.0, abs=0.05)   # north
    assert bounds[1][1] == pytest.approx(3.0, abs=0.05)   # east
    assert meta["color_ramp"] == POPULATION_COLOR_RAMP

    image = Image.open(png_path).convert("RGBA")
    assert image.getpixel((0, 0))[3] == 0
    assert image.getpixel((1, 0)) == (255, 247, 188, 255)
    assert image.getpixel((2, 0)) == (254, 227, 145, 255)
    assert image.getpixel((0, 1)) == (254, 196, 79, 255)


def test_population_overlay_clips_above_display_max(tmp_path: Path, monkeypatch) -> None:
    _patch_boundary_loaders_and_writers(monkeypatch)
    raster_path = _write_overlay_test_raster(tmp_path / "population.tif", width=4, height=3)
    outputs = build_population_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=tmp_path / "population" / "overlay",
        overwrite=True,
        dry_run=False,
    )
    metadata = outputs["population_overlay"]["metadata"]
    assert metadata["source_positive_max_people_per_cell"] == 12000.0
    assert metadata["clipped_above_display_max"] is True
    image = Image.open(outputs["population_overlay"]["png_path"]).convert("RGBA")
    assert image.getpixel((1, 2)) == (76, 5, 25, 255)


def test_population_overlay_dry_run_returns_metadata_without_writes(tmp_path: Path, monkeypatch) -> None:
    _patch_boundary_loaders_and_writers(monkeypatch)
    raster_path = _write_overlay_test_raster(tmp_path / "population.tif")
    overlay_dir = tmp_path / "population" / "overlay"
    outputs = build_population_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=overlay_dir,
        overwrite=False,
        dry_run=True,
    )
    assert outputs["population_overlay"]["metadata"]["overlay_id"] == POPULATION_EXPOSURE_OVERLAY_ID
    assert not outputs["population_overlay"]["png_path"].exists()
    assert not outputs["population_overlay"]["meta_path"].exists()


def test_existing_population_overlay_files_require_overwrite(tmp_path: Path, monkeypatch) -> None:
    _patch_boundary_loaders_and_writers(monkeypatch)
    raster_path = _write_overlay_test_raster(tmp_path / "population.tif")
    overlay_dir = tmp_path / "population" / "overlay"
    overlay_dir.mkdir(parents=True)
    (overlay_dir / "population_exposure_2025_overlay.png").write_bytes(b"old")
    with pytest.raises(FileExistsError):
        build_population_admin_outputs(
            raster_path=raster_path,
            districts_path=tmp_path / "districts.geojson",
            blocks_path=tmp_path / "blocks.geojson",
            qa_dir=tmp_path / "qa",
            overlay_dir=overlay_dir,
            overwrite=False,
            dry_run=False,
        )


def test_population_overlay_downsample_caps_dimensions(tmp_path: Path, monkeypatch) -> None:
    _patch_boundary_loaders_and_writers(monkeypatch)
    raster_path = _write_overlay_test_raster(tmp_path / "population.tif", width=5000, height=1)
    outputs = build_population_admin_outputs(
        raster_path=raster_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=tmp_path / "population" / "overlay",
        overwrite=False,
        dry_run=True,
    )
    metadata = outputs["population_overlay"]["metadata"]
    assert metadata["width_px"] <= 4096
    assert metadata["height_px"] <= 4096
    # bounds_latlon not checked — 5000°E test extent is outside valid WGS84 longitude range
