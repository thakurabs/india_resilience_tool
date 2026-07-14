from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
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


def test_fallback_footprint_from_filename_parses_nominal_10_degree_tile() -> None:
    tile = prep.fallback_footprint_from_filename("RP100_depth_S10W080.tif")

    assert tile.tile_id == "S10W080"
    assert tile.bounds == (-80.0, -10.0, -70.0, 0.0)
    assert tile.source == "filename_fallback"


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
