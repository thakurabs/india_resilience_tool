from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from tools.data_acquisition import prepare_jrc_rp100_source as prep


def _write_boundary(tmp_path: Path, geom=None) -> Path:
    gdf = gpd.GeoDataFrame(
        {"district": ["A"], "geometry": [geom or box(9.9995, 1.0, 10.5, 2.0)]},
        crs="EPSG:4326",
    )
    path = tmp_path / "districts.geojson"
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_tile_extents(tmp_path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "filename": [
                "JRC_GLOFAS_RP100_depth_N00E000.tif",
                "JRC_GLOFAS_RP100_depth_N00E010.tif",
                "JRC_GLOFAS_RP100_depth_N20E070.tif",
            ],
            "geometry": [
                box(0.0, 0.0, 10.0, 10.0),
                box(10.0, 0.0, 20.0, 10.0),
                box(70.0, 20.0, 80.0, 30.0),
            ],
        },
        crs="EPSG:4326",
    )
    path = tmp_path / "tile_extents.geojson"
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_official_tile_extents(tmp_path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {
            "id": [184, 187],
            "name": ["N30_E70", "N80_E80"],
            "geometry": [
                box(70.0, 20.0, 80.0, 30.0),
                box(80.0, 80.0, 90.0, 90.0),
            ],
        },
        crs="EPSG:4326",
    )
    path = tmp_path / "official_tile_extents.geojson"
    gdf.to_file(path, driver="GeoJSON")
    return path


def _inventory_for_tiles(tmp_path: Path, tiles: list[prep.TileFootprint]) -> dict[str, object]:
    boundary_path = _write_boundary(tmp_path, geom=box(*tiles[0].bounds))
    return prep.build_inventory(
        dataset_version="2.1.2",
        base_url=prep.DEFAULT_BASE_URL,
        boundary_path=boundary_path,
        tile_footprints=tiles,
        selected_tiles=tiles,
        tile_extents_path=None,
        selection_buffer_degrees=0.0,
    )


def _tiny_tile(tile_id: str, filename: str, west: float, south: float) -> prep.TileFootprint:
    east = west + 4 * prep.NATIVE_PIXEL_DEGREES
    north = south + 4 * prep.NATIVE_PIXEL_DEGREES
    geom = box(west, south, east, north)
    return prep.TileFootprint(
        tile_id=tile_id,
        filename=filename,
        bounds=(west, south, east, north),
        geometry_wkt=geom.wkt,
        source="test",
    )


def _write_native_depth_tile(path: Path, bounds: tuple[float, float, float, float], *, nodata: float = -9999.0) -> None:
    west, south, east, north = bounds
    width = int(round((east - west) / prep.NATIVE_PIXEL_DEGREES))
    height = int(round((north - south) / prep.NATIVE_PIXEL_DEGREES))
    data = np.arange(1, width * height + 1, dtype=np.float32).reshape(height, width)
    data[-1, -1] = nodata
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(west, north, prep.NATIVE_PIXEL_DEGREES, prep.NATIVE_PIXEL_DEGREES),
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


def test_fallback_footprint_from_filename_parses_nominal_10_degree_tile() -> None:
    tile = prep.fallback_footprint_from_filename("RP100_depth_S10W080.tif")

    assert tile.tile_id == "S10W080"
    assert tile.bounds == (-80.0, -10.0, -70.0, 0.0)
    assert tile.source == "filename_fallback"


def test_tile_token_accepts_official_underscore_tile_names() -> None:
    assert prep._tile_token("N70_W180") == "N70W180"
    assert prep._tile_token("N30_E70") == "N30E070"


def test_load_tile_extents_derives_official_rp100_filename_from_id_and_name(tmp_path: Path) -> None:
    tile_extents_path = _write_official_tile_extents(tmp_path)

    footprints = prep.load_tile_extents(tile_extents_path)

    assert [tile.tile_id for tile in footprints] == ["N30E070", "N80E080"]
    assert footprints[0].filename == "ID184_N30_E70_RP100_depth.tif"
    assert footprints[0].url_path == "RP100/ID184_N30_E70_RP100_depth.tif"


def test_tile_extents_selection_uses_one_pixel_boundary_buffer(tmp_path: Path) -> None:
    buffer_degrees = prep.NATIVE_PIXEL_DEGREES
    inside_gap = buffer_degrees * 0.5
    outside_gap = buffer_degrees * 1.5
    boundary_path = _write_boundary(tmp_path, geom=box(10.0, 1.0, 10.2, 2.0))
    footprints = [
        prep.TileFootprint(
            tile_id="WITHIN_BUFFER",
            filename="RP100_depth_N00E000.tif",
            bounds=(-0.5, 0.0, 10.0 - inside_gap, 10.0),
            geometry_wkt=box(-0.5, 0.0, 10.0 - inside_gap, 10.0).wkt,
            source="test",
        ),
        prep.TileFootprint(
            tile_id="TOUCHING",
            filename="RP100_depth_N00E010.tif",
            bounds=(10.0, 0.0, 20.0, 10.0),
            geometry_wkt=box(10.0, 0.0, 20.0, 10.0).wkt,
            source="test",
        ),
        prep.TileFootprint(
            tile_id="OUTSIDE_BUFFER",
            filename="RP100_depth_N00E020.tif",
            bounds=(-20.0, 0.0, 10.0 - outside_gap, 10.0),
            geometry_wkt=box(-20.0, 0.0, 10.0 - outside_gap, 10.0).wkt,
            source="test",
        ),
    ]

    selected = prep.select_intersecting_tiles(
        boundary_path=boundary_path,
        tile_footprints=footprints,
    )

    assert [tile.tile_id for tile in selected] == ["TOUCHING", "WITHIN_BUFFER"]
    selected_without_buffer = prep.select_intersecting_tiles(
        boundary_path=boundary_path,
        tile_footprints=footprints,
        selection_buffer_degrees=0.0,
    )
    assert [tile.tile_id for tile in selected_without_buffer] == ["TOUCHING"]


def test_build_inventory_and_manifest_skeleton_are_deterministic(tmp_path: Path) -> None:
    boundary_path = _write_boundary(tmp_path)
    tile_extents_path = _write_tile_extents(tmp_path)
    footprints = prep.load_tile_extents(tile_extents_path)
    selected = prep.select_intersecting_tiles(boundary_path=boundary_path, tile_footprints=footprints)

    inventory = prep.build_inventory(
        dataset_version="2.1.2",
        base_url="https://example.test/root/",
        boundary_path=boundary_path,
        tile_footprints=footprints,
        selected_tiles=selected,
        tile_extents_path=tile_extents_path,
    )
    manifest = prep.build_source_manifest(
        inventory=inventory,
        acquisition_timestamp_utc="2026-07-14T00:00:00+00:00",
    )

    assert inventory["source_mode"] == "tile_extents"
    assert inventory["expected_tile_ids"] == ["N00E000", "N00E010"]
    assert inventory["expected_tile_count"] == 2
    assert inventory["candidate_tile_count"] == 3
    assert inventory["fallback_footprint_validation_required"] is False
    assert inventory["selected_tiles"][0]["url"] == "https://example.test/root/RP100/JRC_GLOFAS_RP100_depth_N00E000.tif"
    assert manifest["acquisition_status"] == "planned"
    assert manifest["download_implemented"] is False
    assert manifest["rp100_depth_vrt"] == "RP100_depth.vrt"
    assert manifest["rp100_tile_coverage_vrt"] == "RP100_tile_coverage.vrt"
    assert manifest["expected_tile_ids"] == ["N00E000", "N00E010"]
    assert manifest["validated_tile_ids"] == []


def test_cli_dry_run_writes_nothing(tmp_path: Path) -> None:
    boundary_path = _write_boundary(tmp_path)
    tile_extents_path = _write_tile_extents(tmp_path)
    output_dir = tmp_path / "out"

    rc = prep.main(
        [
            "--boundary-path",
            str(boundary_path),
            "--output-dir",
            str(output_dir),
            "--tile-extents-path",
            str(tile_extents_path),
            "--dry-run",
        ]
    )

    assert rc == 0
    assert not (output_dir / "source_inventory.json").exists()
    assert not (output_dir / "source_manifest.json").exists()


def test_cli_writes_manifest_and_refuses_overwrite_without_flag(tmp_path: Path) -> None:
    boundary_path = _write_boundary(tmp_path)
    tile_extents_path = _write_tile_extents(tmp_path)
    output_dir = tmp_path / "out"
    argv = [
        "--boundary-path",
        str(boundary_path),
        "--output-dir",
        str(output_dir),
        "--tile-extents-path",
        str(tile_extents_path),
    ]

    assert prep.main(argv) == 0
    inventory_path = output_dir / "source_inventory.json"
    manifest_path = output_dir / "source_manifest.json"
    assert inventory_path.exists()
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["acquisition_status"] == "planned"

    with pytest.raises(FileExistsError):
        prep.main(argv)

    assert prep.main([*argv, "--overwrite"]) == 0


def test_fallback_mode_marks_validation_required(tmp_path: Path) -> None:
    boundary_path = _write_boundary(tmp_path)
    filenames = [
        "RP100_depth_N00E000.tif",
        "RP100_depth_N00E010.tif",
        "RP100_depth_N20E070.tif",
    ]
    footprints = prep.load_fallback_filenames(filenames)
    selected = prep.select_intersecting_tiles(boundary_path=boundary_path, tile_footprints=footprints)
    inventory = prep.build_inventory(
        dataset_version="2.1.2",
        base_url=prep.DEFAULT_BASE_URL,
        boundary_path=boundary_path,
        tile_footprints=footprints,
        selected_tiles=selected,
        tile_extents_path=None,
    )

    assert inventory["source_mode"] == "filename_fallback"
    assert inventory["fallback_footprint_validation_required"] is True
    assert inventory["expected_tile_ids"] == ["N00E000", "N00E010"]


def test_duplicate_tile_ids_fail_fast() -> None:
    with pytest.raises(ValueError, match="Duplicate JRC tile IDs"):
        prep.load_fallback_filenames(["RP100_depth_N00E000.tif", "copy_N00E000.tif"])


def test_cli_finalize_validates_tiles_and_writes_vrts(tmp_path: Path) -> None:
    output_dir = tmp_path / "jrc_source"
    tile = _tiny_tile("N30E070", "ID184_N30_E70_RP100_depth.tif", 70.0, 20.0)
    inventory = _inventory_for_tiles(tmp_path, [tile])
    output_dir.mkdir(parents=True)
    (output_dir / "source_inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    _write_native_depth_tile(output_dir / "RP100" / tile.filename, tile.bounds)

    rc = prep.main(["--output-dir", str(output_dir), "--finalize", "--overwrite"])

    assert rc == 0
    manifest = json.loads((output_dir / "source_manifest.json").read_text(encoding="utf-8"))
    assert manifest["acquisition_status"] == "validated"
    assert manifest["validated_tile_ids"] == ["N30E070"]
    assert manifest["rp100_depth_vrt"] == "RP100_depth.vrt"
    assert manifest["rp100_tile_coverage_vrt"] == "RP100_tile_coverage.vrt"
    assert manifest["tiles"][0]["path"] == "RP100/ID184_N30_E70_RP100_depth.tif"
    assert manifest["tiles"][0]["coverage_path"] == "RP100_tile_coverage/N30E070_coverage.tif"
    assert "raster_bounds_match_official_tile_extents" in manifest["integrity_basis"]

    with rasterio.open(output_dir / "RP100_depth.vrt") as depth, rasterio.open(output_dir / "RP100_tile_coverage.vrt") as coverage:
        assert depth.crs.to_string() == "EPSG:4326"
        assert coverage.crs.to_string() == "EPSG:4326"
        assert depth.shape == coverage.shape == (4, 4)
        assert tuple(depth.bounds) == tuple(coverage.bounds)
        assert np.isclose(depth.nodata, -9999.0)
        assert np.isclose(coverage.nodata, 0.0)
        assert set(np.unique(coverage.read(1, masked=False)).tolist()) == {1}


def test_finalize_fails_when_expected_tile_file_is_missing(tmp_path: Path) -> None:
    output_dir = tmp_path / "jrc_source"
    tile = _tiny_tile("N30E070", "ID184_N30_E70_RP100_depth.tif", 70.0, 20.0)
    inventory = _inventory_for_tiles(tmp_path, [tile])
    output_dir.mkdir(parents=True)
    (output_dir / "source_inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Expected JRC RP-100 tile is missing"):
        prep.finalize_downloaded_source(
            inventory_path=output_dir / "source_inventory.json",
            output_dir=output_dir,
            overwrite=True,
            acquisition_timestamp_utc="2026-07-14T00:00:00+00:00",
            finalization_timestamp_utc="2026-07-14T00:00:01+00:00",
        )


def test_finalize_fails_when_raster_bounds_do_not_match_inventory(tmp_path: Path) -> None:
    output_dir = tmp_path / "jrc_source"
    tile = _tiny_tile("N30E070", "ID184_N30_E70_RP100_depth.tif", 70.0, 20.0)
    inventory = _inventory_for_tiles(tmp_path, [tile])
    output_dir.mkdir(parents=True)
    (output_dir / "source_inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    shifted_bounds = (tile.bounds[0] + prep.NATIVE_PIXEL_DEGREES, tile.bounds[1], tile.bounds[2] + prep.NATIVE_PIXEL_DEGREES, tile.bounds[3])
    _write_native_depth_tile(output_dir / "RP100" / tile.filename, shifted_bounds)

    with pytest.raises(ValueError, match="raster bounds"):
        prep.finalize_downloaded_source(
            inventory_path=output_dir / "source_inventory.json",
            output_dir=output_dir,
            overwrite=True,
            acquisition_timestamp_utc="2026-07-14T00:00:00+00:00",
            finalization_timestamp_utc="2026-07-14T00:00:01+00:00",
        )
