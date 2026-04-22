from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from tools.geodata.build_jrc_flood_depth_admin_masters import (
    DERIVED_EXTENT_METRIC_SLUG,
    DERIVED_INDEX_METRIC_SLUG,
    JRC_FILE_MAP,
    _build_derived_extent_outputs,
    _build_district_frames,
    _classify_depth_index,
    _classify_extent_index,
    _lookup_severity_index,
    build_jrc_flood_depth_outputs,
)


def _write_boundaries(tmp_path: Path) -> tuple[Path, Path]:
    districts = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Hyderabad", "Warangal"],
            "geometry": [box(0, 2, 4, 4), box(10, 10, 12, 12)],
        },
        crs="EPSG:4326",
    )
    blocks = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "district_name": ["Hyderabad", "Hyderabad", "Warangal"],
            "block_name": ["North", "South", "Outside"],
            "geometry": [box(0, 2, 2, 4), box(2, 2, 4, 4), box(10, 10, 12, 12)],
        },
        crs="EPSG:4326",
    )
    districts_path = tmp_path / "districts_4326.geojson"
    blocks_path = tmp_path / "blocks_4326.geojson"
    districts.to_file(districts_path, driver="GeoJSON")
    blocks.to_file(blocks_path, driver="GeoJSON")
    return districts_path, blocks_path


def _write_variant_boundaries(tmp_path: Path) -> tuple[Path, Path]:
    districts = gpd.GeoDataFrame(
        {
            "state_name": ["TELANGANA", "TELANGANA"],
            "district_name": ["SANGA REDDY", "RANGA REDDY"],
            "geometry": [box(0, 2, 4, 4), box(4, 2, 8, 4)],
        },
        crs="EPSG:4326",
    )
    blocks = gpd.GeoDataFrame(
        {
            "state_name": ["TELANGANA", "TELANGANA", "TELANGANA", "TELANGANA"],
            "district_name": ["Sangareddy", "Sangareddy", "Ranga Reddy", "Ranga Reddy"],
            "block_name": ["North", "South", "East", "West"],
            "geometry": [box(0, 2, 2, 4), box(2, 2, 4, 4), box(4, 2, 6, 4), box(6, 2, 8, 4)],
        },
        crs="EPSG:4326",
    )
    districts_path = tmp_path / "districts_variants.geojson"
    blocks_path = tmp_path / "blocks_variants.geojson"
    districts.to_file(districts_path, driver="GeoJSON")
    blocks.to_file(blocks_path, driver="GeoJSON")
    return districts_path, blocks_path


def _write_district_extends_beyond_blocks_boundaries(tmp_path: Path) -> tuple[Path, Path]:
    districts = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Hyderabad", "Warangal"],
            "geometry": [box(0, 1, 4, 4), box(10, 10, 12, 12)],
        },
        crs="EPSG:4326",
    )
    blocks = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "district_name": ["Hyderabad", "Hyderabad", "Warangal"],
            "block_name": ["North", "South", "Outside"],
            "geometry": [box(0, 2, 2, 4), box(2, 2, 4, 4), box(10, 10, 12, 12)],
        },
        crs="EPSG:4326",
    )
    districts_path = tmp_path / "districts_diagnostic_mismatch.geojson"
    blocks_path = tmp_path / "blocks_diagnostic_mismatch.geojson"
    districts.to_file(districts_path, driver="GeoJSON")
    blocks.to_file(blocks_path, driver="GeoJSON")
    return districts_path, blocks_path


def _write_misaligned_boundaries(tmp_path: Path) -> tuple[Path, Path]:
    districts = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Missing District"],
            "geometry": [box(0, 2, 4, 4)],
        },
        crs="EPSG:4326",
    )
    blocks = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Different District"],
            "block_name": ["Only Block"],
            "geometry": [box(0, 2, 4, 4)],
        },
        crs="EPSG:4326",
    )
    districts_path = tmp_path / "districts_misaligned.geojson"
    blocks_path = tmp_path / "blocks_misaligned.geojson"
    districts.to_file(districts_path, driver="GeoJSON")
    blocks.to_file(blocks_path, driver="GeoJSON")
    return districts_path, blocks_path


def _write_duplicate_join_boundaries(tmp_path: Path) -> tuple[Path, Path]:
    districts = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Sanga Reddy", "Sangareddy"],
            "geometry": [box(0, 2, 2, 4), box(2, 2, 4, 4)],
        },
        crs="EPSG:4326",
    )
    blocks = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Sangareddy"],
            "block_name": ["Only Block"],
            "geometry": [box(0, 2, 4, 4)],
        },
        crs="EPSG:4326",
    )
    districts_path = tmp_path / "districts_duplicate_join.geojson"
    blocks_path = tmp_path / "blocks_duplicate_join.geojson"
    districts.to_file(districts_path, driver="GeoJSON")
    blocks.to_file(blocks_path, driver="GeoJSON")
    return districts_path, blocks_path


def _write_rasters(
    source_dir: Path,
    *,
    nodata: float = -9999.0,
    write_full_valid_mask: bool = False,
    invalid_mask_coords: set[tuple[int, int]] | None = None,
    data: np.ndarray | None = None,
) -> None:
    source_dir.mkdir(parents=True, exist_ok=True)
    if data is None:
        data = np.array(
            [
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.0, 5.0, 0.0],
                [0.0, 0.0, 0.0, 0.0],
                [nodata, nodata, nodata, nodata],
            ],
            dtype=np.float32,
        )
    else:
        data = np.asarray(data, dtype=np.float32)
    transform = from_origin(0, 4, 1, 1)
    for filename in JRC_FILE_MAP.values():
        with rasterio.open(
            source_dir / filename,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
            nodata=nodata,
        ) as dst:
            dst.write(data, 1)
            if write_full_valid_mask:
                dst.write_mask(np.full(data.shape, 255, dtype=np.uint8))
            elif invalid_mask_coords:
                mask = np.full(data.shape, 255, dtype=np.uint8)
                for row_idx, col_idx in invalid_mask_coords:
                    mask[row_idx, col_idx] = 0
                dst.write_mask(mask)


def _build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    overwrite: bool,
    nodata: float = -9999.0,
    boundaries_writer=_write_boundaries,
    write_full_valid_mask: bool = False,
    invalid_mask_coords: set[tuple[int, int]] | None = None,
    data: np.ndarray | None = None,
):
    data_dir = tmp_path / "irt_data"
    monkeypatch.setenv("IRT_DATA_DIR", str(data_dir))
    source_dir = tmp_path / "jrc"
    districts_path, blocks_path = boundaries_writer(tmp_path)
    _write_rasters(
        source_dir,
        nodata=nodata,
        write_full_valid_mask=write_full_valid_mask,
        invalid_mask_coords=invalid_mask_coords,
        data=data,
    )
    qa_dir = data_dir / "jrc_flood_depth" / "qa"
    return build_jrc_flood_depth_outputs(
        source_dir=source_dir,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        assume_units="m",
        overwrite=overwrite,
        dry_run=False,
    )


def _qa_dir(tmp_path: Path) -> Path:
    return tmp_path / "irt_data" / "jrc_flood_depth" / "qa"


def test_jrc_builder_writes_expected_telangana_outputs_and_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(tmp_path, monkeypatch, overwrite=True)

    rp10 = outputs["jrc_flood_depth_rp10"]
    derived = outputs[DERIVED_INDEX_METRIC_SLUG]
    derived_extent = outputs[DERIVED_EXTENT_METRIC_SLUG]
    block_master = rp10["block_master_df"]
    district_master = rp10["district_master_df"]
    block_qa = rp10["block_qa_df"]
    district_qa = rp10["district_qa_df"]
    derived_block_master = derived["block_master_df"]
    derived_district_master = derived["district_master_df"]
    derived_block_qa = derived["block_qa_df"]
    derived_district_qa = derived["district_qa_df"]
    extent_block_master = derived_extent["block_master_df"]
    extent_district_master = derived_extent["district_master_df"]
    extent_block_qa = derived_extent["block_qa_df"]
    extent_district_qa = derived_extent["district_qa_df"]
    run_summary = outputs["run_summary_df"]

    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"
    derived_col = "jrc_flood_depth_index_rp100__snapshot__Current__mean"
    extent_col = "jrc_flood_extent_rp100__snapshot__Current__mean"
    assert block_master.columns.tolist() == [
        "state",
        "district",
        "block",
        "block_key",
        "block_area_km2",
        value_col,
    ]
    assert district_master.columns.tolist() == [
        "state",
        "district",
        "district_key",
        "district_area_km2",
        value_col,
    ]

    north = float(block_master.loc[block_master["block"] == "North", value_col].iloc[0])
    south = float(block_master.loc[block_master["block"] == "South", value_col].iloc[0])
    outside = block_master.loc[block_master["block"] == "Outside", value_col].iloc[0]
    assert north == pytest.approx(1.95)
    assert south == pytest.approx(4.9)
    assert pd.isna(outside)

    hyderabad = float(district_master.loc[district_master["district"] == "Hyderabad", value_col].iloc[0])
    warangal = district_master.loc[district_master["district"] == "Warangal", value_col].iloc[0]
    assert hyderabad == pytest.approx(3.72)
    assert pd.isna(warangal)
    assert float(derived_block_master.loc[derived_block_master["block"] == "North", derived_col].iloc[0]) == 5.0
    assert float(derived_block_master.loc[derived_block_master["block"] == "South", derived_col].iloc[0]) == 5.0
    assert pd.isna(derived_block_master.loc[derived_block_master["block"] == "Outside", derived_col].iloc[0])
    assert float(derived_district_master.loc[derived_district_master["district"] == "Hyderabad", derived_col].iloc[0]) == 5.0
    assert pd.isna(derived_district_master.loc[derived_district_master["district"] == "Warangal", derived_col].iloc[0])
    assert float(extent_block_master.loc[extent_block_master["block"] == "North", extent_col].iloc[0]) == pytest.approx(0.5)
    assert float(extent_block_master.loc[extent_block_master["block"] == "South", extent_col].iloc[0]) == pytest.approx(0.75)
    assert pd.isna(extent_block_master.loc[extent_block_master["block"] == "Outside", extent_col].iloc[0])
    assert float(extent_district_master.loc[extent_district_master["district"] == "Hyderabad", extent_col].iloc[0]) == pytest.approx(0.625)
    assert pd.isna(extent_district_master.loc[extent_district_master["district"] == "Warangal", extent_col].iloc[0])

    assert bool(block_qa.loc[block_qa["block"] == "North", "coverage_pass"].iloc[0]) is True
    assert bool(block_qa.loc[block_qa["block"] == "Outside", "coverage_pass"].iloc[0]) is False
    assert bool(district_qa.loc[district_qa["district"] == "Hyderabad", "delta_warn"].iloc[0]) is True
    assert float(block_qa.loc[block_qa["block"] == "North", "mean_valid_depth_m"].iloc[0]) == pytest.approx(0.75)
    assert float(block_qa.loc[block_qa["block"] == "North", "p95_positive_depth_m"].iloc[0]) == pytest.approx(1.95)
    assert pd.isna(block_qa.loc[block_qa["block"] == "Outside", "mean_valid_depth_m"].iloc[0])
    assert float(block_qa.loc[block_qa["block"] == "South", "p95_positive_depth_m"].iloc[0]) == pytest.approx(4.9)
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "district_valid_support_fraction"].iloc[0]) == pytest.approx(1.0)
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "district_flooded_supported_area_km2"].iloc[0]) > 0.0
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "covered_block_min_p95_positive_depth_m"].iloc[0]) == pytest.approx(1.95)
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "covered_block_max_p95_positive_depth_m"].iloc[0]) == pytest.approx(4.9)
    assert derived_block_qa.columns.tolist() == [
        "state",
        "district",
        "block",
        "block_key",
        "raw_rp100_depth_m",
        "coverage_pass",
        "raw_rp100_extent_fraction",
        "depth_class_index",
        "depth_class_label",
        "extent_class_index",
        "extent_class_label",
        "class_index",
        "class_label",
    ]
    assert derived_district_qa.columns.tolist() == [
        "state",
        "district",
        "district_key",
        "raw_rp100_depth_m",
        "district_valid_support_fraction",
        "raw_rp100_extent_fraction",
        "depth_class_index",
        "depth_class_label",
        "extent_class_index",
        "extent_class_label",
        "class_index",
        "class_label",
    ]
    north_index = derived_block_qa.loc[derived_block_qa["block"] == "North"].iloc[0]
    assert float(north_index["raw_rp100_extent_fraction"]) == pytest.approx(0.5)
    assert int(north_index["depth_class_index"]) == 4
    assert north_index["depth_class_label"] == "High"
    assert int(north_index["extent_class_index"]) == 5
    assert north_index["extent_class_label"] == "Extreme"
    assert int(north_index["class_index"]) == 5
    assert north_index["class_label"] == "Extreme"
    assert extent_block_qa.columns.tolist() == [
        "state",
        "district",
        "block",
        "block_key",
        "block_area_km2",
        "total_in_polygon_cell_count",
        "valid_in_polygon_cell_count",
        "positive_valid_cell_count",
        "valid_support_fraction_of_block_area",
        "flooded_support_fraction_of_block_area",
        "valid_supported_area_km2",
        "flooded_supported_area_km2",
        "has_raster_overlap",
        "has_valid_support",
        extent_col,
        "publishable",
    ]
    assert extent_district_qa.columns.tolist() == [
        "state",
        "district",
        "district_key",
        "district_area_km2",
        "district_valid_supported_area_km2",
        "district_flooded_supported_area_km2",
        "covered_valid_support_fraction",
        "covered_block_count",
        "uncovered_block_count",
        extent_col,
        "publishable",
        "direct_extent_fraction_from_raster",
        "delta_vs_direct_extent_fraction",
        "delta_warn",
    ]
    assert bool(extent_block_qa.loc[extent_block_qa["block"] == "North", "publishable"].iloc[0]) is True
    assert bool(extent_block_qa.loc[extent_block_qa["block"] == "Outside", "publishable"].iloc[0]) is False
    assert float(extent_district_qa.loc[extent_district_qa["district"] == "Hyderabad", "direct_extent_fraction_from_raster"].iloc[0]) == pytest.approx(0.625)
    assert bool(extent_district_qa.loc[extent_district_qa["district"] == "Hyderabad", "delta_warn"].iloc[0]) is False
    assert run_summary["metric_slug"].tolist() == sorted([*JRC_FILE_MAP, DERIVED_INDEX_METRIC_SLUG, DERIVED_EXTENT_METRIC_SLUG])
    derived_summary = run_summary.loc[run_summary["metric_slug"] == DERIVED_INDEX_METRIC_SLUG].iloc[0]
    assert derived_summary["metric_kind"] == "derived_severity_matrix"
    assert derived_summary["source_metric_slug"] == "jrc_flood_depth_rp100"
    assert derived_summary["component_metric_slugs"] == "jrc_flood_depth_rp100;jrc_flood_extent_rp100"
    assert derived_summary["severity_method"] == "rp100_depth_extent_matrix_v1"
    extent_summary = run_summary.loc[run_summary["metric_slug"] == DERIVED_EXTENT_METRIC_SLUG].iloc[0]
    assert extent_summary["metric_kind"] == "derived_extent"
    assert extent_summary["source_metric_slug"] == "jrc_flood_depth_rp100"
    assert extent_summary["severity_method"] == ""
    assert (run_summary["depth_method"] == "block_p95_positive__district_flooded_area_weighted_v2").all()
    assert (run_summary["district_area_denominator"] == "district_polygon_area_epsg6933_v2").all()
    assert (run_summary["percentile_method"] == "q95_linear__positive_depth_only_v2").all()
    assert (run_summary["blocks_total"] == 3).all()
    assert (run_summary["districts_total"] == 2).all()

    qa_dir = _qa_dir(tmp_path)
    assert (qa_dir / "jrc_flood_depth_rp10_block_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_rp10_district_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_index_rp100_block_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_index_rp100_district_qa.csv").exists()
    assert (qa_dir / "jrc_flood_extent_rp100_block_qa.csv").exists()
    assert (qa_dir / "jrc_flood_extent_rp100_district_qa.csv").exists()
    assert (qa_dir / "admin_boundary_join_qa.csv").exists()
    assert (qa_dir / "run_summary.csv").exists()


def test_jrc_builder_refuses_existing_outputs_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _build(tmp_path, monkeypatch, overwrite=True)
    with pytest.raises(FileExistsError):
        _build(tmp_path, monkeypatch, overwrite=False)


def test_jrc_builder_overwrite_rewrites_run_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _build(tmp_path, monkeypatch, overwrite=True)
    qa_dir = _qa_dir(tmp_path)
    pd.DataFrame([{"metric_slug": "broken"}]).to_csv(qa_dir / "run_summary.csv", index=False)

    _build(tmp_path, monkeypatch, overwrite=True)

    run_summary = pd.read_csv(qa_dir / "run_summary.csv")
    assert run_summary.shape[0] == 6
    assert sorted(run_summary["metric_slug"].tolist()) == sorted([*JRC_FILE_MAP, DERIVED_INDEX_METRIC_SLUG, DERIVED_EXTENT_METRIC_SLUG])


def test_jrc_builder_accepts_zero_nodata_metadata_when_zero_depth_is_needed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        nodata=0.0,
        write_full_valid_mask=True,
    )
    run_summary = outputs["run_summary_df"]
    assert run_summary.shape[0] == 6


def test_jrc_builder_returns_zero_for_covered_dry_blocks_inside_raster_extent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dry_data = np.array(
        [
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [-9999.0, -9999.0, -9999.0, -9999.0],
        ],
        dtype=np.float32,
    )
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        data=dry_data,
    )
    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"
    block_master = outputs["jrc_flood_depth_rp10"]["block_master_df"]
    district_master = outputs["jrc_flood_depth_rp10"]["district_master_df"]

    north = float(block_master.loc[block_master["block"] == "North", value_col].iloc[0])
    south = float(block_master.loc[block_master["block"] == "South", value_col].iloc[0])
    hyderabad = float(district_master.loc[district_master["district"] == "Hyderabad", value_col].iloc[0])
    block_qa = outputs["jrc_flood_depth_rp10"]["block_qa_df"]

    assert north == 0.0
    assert south == 0.0
    assert hyderabad == 0.0
    assert float(block_qa.loc[block_qa["block"] == "North", "mean_valid_depth_m"].iloc[0]) == 0.0
    assert float(block_qa.loc[block_qa["block"] == "South", "mean_valid_depth_m"].iloc[0]) == 0.0
    assert float(block_qa.loc[block_qa["block"] == "North", "p95_positive_depth_m"].iloc[0]) == 0.0
    assert float(block_qa.loc[block_qa["block"] == "South", "p95_positive_depth_m"].iloc[0]) == 0.0

    extent_col = "jrc_flood_extent_rp100__snapshot__Current__mean"
    extent_block_master = outputs[DERIVED_EXTENT_METRIC_SLUG]["block_master_df"]
    extent_district_master = outputs[DERIVED_EXTENT_METRIC_SLUG]["district_master_df"]
    assert float(extent_block_master.loc[extent_block_master["block"] == "North", extent_col].iloc[0]) == 0.0
    assert float(extent_block_master.loc[extent_block_master["block"] == "South", extent_col].iloc[0]) == 0.0
    assert float(extent_district_master.loc[extent_district_master["district"] == "Hyderabad", extent_col].iloc[0]) == 0.0


def test_jrc_builder_matches_variant_telangana_district_names_via_normalized_join_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        boundaries_writer=_write_variant_boundaries,
    )
    district_master = outputs["jrc_flood_depth_rp10"]["district_master_df"]
    assert district_master["district"].tolist() == ["RANGA REDDY", "SANGA REDDY"]
    assert district_master["jrc_flood_depth_rp10__snapshot__Current__mean"].notna().sum() == 1

    join_qa = outputs["admin_join_qa_df"]
    assert set(join_qa["status"].tolist()) == {"matched"}


def test_jrc_builder_fails_preflight_when_telangana_districts_do_not_align(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="boundary alignment failed before raster aggregation"):
        _build(
            tmp_path,
            monkeypatch,
            overwrite=True,
            boundaries_writer=_write_misaligned_boundaries,
        )

    qa_path = _qa_dir(tmp_path) / "admin_boundary_join_qa.csv"
    join_qa = pd.read_csv(qa_path)
    assert set(join_qa["status"].tolist()) == {"missing_in_blocks", "missing_in_districts"}


def test_jrc_builder_fails_preflight_on_duplicate_normalized_district_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="duplicate_within_source=2"):
        _build(
            tmp_path,
            monkeypatch,
            overwrite=True,
            boundaries_writer=_write_duplicate_join_boundaries,
        )

    qa_path = _qa_dir(tmp_path) / "admin_boundary_join_qa.csv"
    join_qa = pd.read_csv(qa_path)
    duplicate_rows = join_qa.loc[join_qa["status"] == "duplicate_within_source"]
    assert duplicate_rows.shape[0] == 2


def test_jrc_builder_treats_zero_nodata_rasters_as_covered_when_polygons_overlap_extent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        nodata=0.0,
        invalid_mask_coords={(0, 0), (0, 1), (1, 0), (1, 1)},
    )
    block_master = outputs["jrc_flood_depth_rp10"]["block_master_df"]
    district_master = outputs["jrc_flood_depth_rp10"]["district_master_df"]
    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"

    north = float(block_master.loc[block_master["block"] == "North", value_col].iloc[0])
    south = float(block_master.loc[block_master["block"] == "South", value_col].iloc[0])
    hyderabad = float(district_master.loc[district_master["district"] == "Hyderabad", value_col].iloc[0])
    run_summary = outputs["run_summary_df"]

    assert north == 0.0
    assert south == pytest.approx(4.9)
    assert hyderabad == pytest.approx(4.9)
    assert (run_summary["blocks_covered"] > 0).all()
    raw_and_index = run_summary.loc[run_summary["metric_slug"] != DERIVED_EXTENT_METRIC_SLUG]
    assert (raw_and_index["districts_covered"] > 0).all()


def test_jrc_extent_returns_nan_when_polygon_overlaps_raster_but_has_no_valid_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        invalid_mask_coords={(0, 0), (0, 1), (1, 0), (1, 1)},
    )
    extent_col = "jrc_flood_extent_rp100__snapshot__Current__mean"
    extent_block_master = outputs[DERIVED_EXTENT_METRIC_SLUG]["block_master_df"]
    extent_block_qa = outputs[DERIVED_EXTENT_METRIC_SLUG]["block_qa_df"]

    north = extent_block_master.loc[extent_block_master["block"] == "North", extent_col].iloc[0]
    south = extent_block_master.loc[extent_block_master["block"] == "South", extent_col].iloc[0]
    assert pd.isna(north)
    assert float(south) == pytest.approx(0.75)
    north_qa = extent_block_qa.loc[extent_block_qa["block"] == "North"].iloc[0]
    assert bool(north_qa["has_raster_overlap"]) is True
    assert bool(north_qa["has_valid_support"]) is False
    assert bool(north_qa["publishable"]) is False


def test_jrc_extent_uses_total_polygon_area_for_block_and_district_rollup() -> None:
    extent_col = "jrc_flood_extent_rp100__snapshot__Current__mean"
    raw_output = {
        "block_qa_df": pd.DataFrame(
            [
                {
                    "state": "Telangana",
                    "district": "Hyderabad",
                    "block": "LargeSparse",
                    "block_key": "large",
                    "block_area_km2": 100.0,
                    "total_in_polygon_cell_count": 100,
                    "valid_in_polygon_cell_count": 10,
                    "positive_valid_cell_count": 10,
                    "valid_support_fraction_of_block_area": 0.1,
                    "flooded_support_fraction_of_block_area": 0.1,
                    "valid_supported_area_km2": 10.0,
                    "flooded_supported_area_km2": 10.0,
                },
                {
                    "state": "Telangana",
                    "district": "Hyderabad",
                    "block": "SmallDense",
                    "block_key": "small",
                    "block_area_km2": 20.0,
                    "total_in_polygon_cell_count": 20,
                    "valid_in_polygon_cell_count": 20,
                    "positive_valid_cell_count": 0,
                    "valid_support_fraction_of_block_area": 1.0,
                    "flooded_support_fraction_of_block_area": 0.0,
                    "valid_supported_area_km2": 20.0,
                    "flooded_supported_area_km2": 0.0,
                },
            ]
        ),
        "district_qa_df": pd.DataFrame(
            [
                {
                    "state": "Telangana",
                    "district": "Hyderabad",
                    "district_key": "telangana::hyderabad",
                    "direct_total_in_polygon_cell_count": 30,
                    "direct_valid_in_polygon_cell_count": 30,
                    "direct_positive_valid_cell_count": 10,
                }
            ]
        ),
        "district_master_df": pd.DataFrame(
            [
                {
                    "state": "Telangana",
                    "district": "Hyderabad",
                    "district_key": "telangana::hyderabad",
                    "district_area_km2": 30.0,
                }
            ]
        ),
    }

    derived = _build_derived_extent_outputs(raw_output=raw_output)
    district_master = derived["district_master_df"]
    district_qa = derived["district_qa_df"]

    assert float(district_master.loc[0, extent_col]) == pytest.approx(10.0 / 30.0)
    assert float(district_qa.loc[0, "covered_valid_support_fraction"]) == pytest.approx(1.0)


def test_jrc_builder_uses_authoritative_district_polygon_area_as_denominator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mismatch_data = np.array(
        [
            [1.0, 2.0, 3.0, 4.0],
            [0.0, 0.0, 5.0, 0.0],
            [9.0, 9.0, 9.0, 9.0],
            [-9999.0, -9999.0, -9999.0, -9999.0],
        ],
        dtype=np.float32,
    )
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        boundaries_writer=_write_district_extends_beyond_blocks_boundaries,
        data=mismatch_data,
    )
    district_qa = outputs["jrc_flood_depth_rp10"]["district_qa_df"]
    hyderabad = district_qa.loc[district_qa["district"] == "Hyderabad"].iloc[0]

    assert float(hyderabad["chosen_value_m"]) == pytest.approx(3.72)
    assert float(hyderabad["district_area_km2"]) > float(hyderabad["child_block_area_sum_km2"])
    assert float(hyderabad["child_block_area_gap_km2"]) > 0.0
    assert float(hyderabad["district_valid_support_fraction"]) < 1.0
    assert float(hyderabad["direct_p95_positive_depth_m"]) == pytest.approx(9.0)
    assert float(hyderabad["delta_vs_direct_p95_positive_m"]) < 0.0


def test_jrc_builder_fails_when_flooded_blocks_are_missing_p95_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path / "irt_data"))
    source_dir = tmp_path / "jrc"
    districts_path, blocks_path = _write_boundaries(tmp_path)
    _write_rasters(source_dir)
    district_gdf = gpd.read_file(districts_path)
    block_gdf = gpd.read_file(blocks_path)
    district_gdf["district_key"] = ["hyderabad", "warangal"]
    district_projected = district_gdf.to_crs("EPSG:6933")
    block_projected = block_gdf.to_crs("EPSG:6933")
    district_gdf["district_area_km2"] = district_projected.geometry.area / 1_000_000.0
    block_gdf["block_area_km2"] = block_projected.geometry.area / 1_000_000.0
    north_area = float(block_gdf.loc[block_gdf["block_name"] == "North", "block_area_km2"].iloc[0])
    south_area = float(block_gdf.loc[block_gdf["block_name"] == "South", "block_area_km2"].iloc[0])
    block_master_df = pd.DataFrame(
        [
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "North",
                "block_key": "north",
                "block_area_km2": north_area,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 1.0,
            },
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "South",
                "block_key": "south",
                "block_area_km2": south_area,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 1.0,
            },
        ]
    )
    block_qa_df = pd.DataFrame(
        [
                {
                    "block_key": "north",
                    "mean_valid_depth_m": 2.0,
                    "p95_positive_depth_m": np.nan,
                    "valid_supported_area_km2": north_area,
                    "flooded_supported_area_km2": north_area,
                },
                {
                    "block_key": "south",
                    "mean_valid_depth_m": 2.0,
                    "p95_positive_depth_m": 2.0,
                    "valid_supported_area_km2": south_area,
                    "flooded_supported_area_km2": south_area,
                },
            ]
        )
    with rasterio.open(source_dir / JRC_FILE_MAP["jrc_flood_depth_rp10"]) as dataset:
        with pytest.raises(ValueError, match="missing p95_positive_depth_m"):
            _build_district_frames(
                district_gdf=district_gdf.iloc[[0]].copy(),
                block_master_df=block_master_df,
                block_qa_df=block_qa_df,
                dataset=dataset,
                metric_slug="jrc_flood_depth_rp10",
            )


def test_jrc_builder_uses_linear_p95_interpolation_for_multi_cell_positive_samples(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(tmp_path, monkeypatch, overwrite=True)
    block_qa = outputs["jrc_flood_depth_rp10"]["block_qa_df"]

    assert float(block_qa.loc[block_qa["block"] == "North", "p95_positive_depth_m"].iloc[0]) == pytest.approx(1.95)
    assert float(block_qa.loc[block_qa["block"] == "South", "p95_positive_depth_m"].iloc[0]) == pytest.approx(4.9)


def test_jrc_builder_returns_single_positive_cell_depth_as_block_p95(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    single_positive = np.array(
        [
            [0.0, 0.0, 0.0, 0.0],
            [7.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [-9999.0, -9999.0, -9999.0, -9999.0],
        ],
        dtype=np.float32,
    )
    outputs = _build(tmp_path, monkeypatch, overwrite=True, data=single_positive)
    block_qa = outputs["jrc_flood_depth_rp10"]["block_qa_df"]
    block_master = outputs["jrc_flood_depth_rp10"]["block_master_df"]
    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"

    assert float(block_qa.loc[block_qa["block"] == "North", "p95_positive_depth_m"].iloc[0]) == pytest.approx(7.0)
    assert float(block_master.loc[block_master["block"] == "North", value_col].iloc[0]) == pytest.approx(7.0)


def test_jrc_builder_returns_nan_for_overlap_without_valid_support(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        invalid_mask_coords={(0, 0), (0, 1), (1, 0), (1, 1)},
    )
    block_master = outputs["jrc_flood_depth_rp10"]["block_master_df"]
    block_qa = outputs["jrc_flood_depth_rp10"]["block_qa_df"]
    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"

    north = block_master.loc[block_master["block"] == "North", value_col].iloc[0]
    assert pd.isna(north)
    north_qa = block_qa.loc[block_qa["block"] == "North"].iloc[0]
    assert bool(north_qa["coverage_pass"]) is False
    assert pd.isna(north_qa["dashboard_value_m"])


def test_jrc_builder_returns_district_depth_when_valid_support_exists_even_if_sparse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = _build(
        tmp_path,
        monkeypatch,
        overwrite=True,
        invalid_mask_coords={(0, 0)},
    )
    district_master = outputs["jrc_flood_depth_rp10"]["district_master_df"]
    district_qa = outputs["jrc_flood_depth_rp10"]["district_qa_df"]
    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"

    hyderabad = float(district_master.loc[district_master["district"] == "Hyderabad", value_col].iloc[0])
    assert hyderabad > 0.0
    hyderabad_qa = district_qa.loc[district_qa["district"] == "Hyderabad"].iloc[0]
    assert float(hyderabad_qa["district_flooded_supported_area_km2"]) > 0.0
    assert float(hyderabad_qa["district_valid_support_fraction"]) < 0.995


def test_classify_depth_index_uses_exact_rp100_thresholds() -> None:
    assert _classify_depth_index(np.nan) is None
    assert _classify_depth_index(0.0) == 1
    assert _classify_depth_index(0.2) == 1
    assert _classify_depth_index(0.20001) == 2
    assert _classify_depth_index(0.5) == 2
    assert _classify_depth_index(0.50001) == 3
    assert _classify_depth_index(1.0) == 3
    assert _classify_depth_index(1.00001) == 4
    assert _classify_depth_index(2.5) == 4
    assert _classify_depth_index(2.50001) == 5


def test_classify_extent_index_uses_exact_rp100_thresholds() -> None:
    assert _classify_extent_index(np.nan) is None
    assert _classify_extent_index(0.0) == 1
    assert _classify_extent_index(0.01) == 1
    assert _classify_extent_index(0.01001) == 2
    assert _classify_extent_index(0.05) == 2
    assert _classify_extent_index(0.05001) == 3
    assert _classify_extent_index(0.15) == 3
    assert _classify_extent_index(0.15001) == 4
    assert _classify_extent_index(0.25) == 4
    assert _classify_extent_index(0.25001) == 5


def test_lookup_severity_index_uses_full_depth_extent_matrix() -> None:
    expected = (
        (1, 2, 2, 3, 4),
        (2, 2, 3, 4, 4),
        (2, 3, 4, 4, 5),
        (3, 4, 4, 5, 5),
        (4, 5, 5, 5, 5),
    )
    for extent_class, row in enumerate(expected, start=1):
        for depth_class, score in enumerate(row, start=1):
            assert _lookup_severity_index(extent_class, depth_class) == score


def test_invalid_depth_and_extent_inputs_raise() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        _classify_depth_index(-0.1)
    with pytest.raises(ValueError, match="within \\[0, 1\\]"):
        _classify_extent_index(-0.01)
    with pytest.raises(ValueError, match="within \\[0, 1\\]"):
        _classify_extent_index(1.01)
    with pytest.raises(ValueError, match="expects classes 1..5"):
        _lookup_severity_index(6, 1)
