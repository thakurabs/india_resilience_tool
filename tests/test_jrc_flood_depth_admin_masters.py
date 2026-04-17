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
    DERIVED_INDEX_METRIC_SLUG,
    JRC_FILE_MAP,
    _classify_depth_index,
    _build_district_frames,
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
    block_master = rp10["block_master_df"]
    district_master = rp10["district_master_df"]
    block_qa = rp10["block_qa_df"]
    district_qa = rp10["district_qa_df"]
    derived_block_master = derived["block_master_df"]
    derived_district_master = derived["district_master_df"]
    derived_block_qa = derived["block_qa_df"]
    derived_district_qa = derived["district_qa_df"]
    run_summary = outputs["run_summary_df"]

    value_col = "jrc_flood_depth_rp10__snapshot__Current__mean"
    derived_col = "jrc_flood_depth_index_rp100__snapshot__Current__mean"
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
    assert north == 2.0
    assert south == 5.0
    assert pd.isna(outside)

    hyderabad = float(district_master.loc[district_master["district"] == "Hyderabad", value_col].iloc[0])
    warangal = district_master.loc[district_master["district"] == "Warangal", value_col].iloc[0]
    assert hyderabad == pytest.approx(3.5)
    assert pd.isna(warangal)
    assert float(derived_block_master.loc[derived_block_master["block"] == "North", derived_col].iloc[0]) == 4.0
    assert float(derived_block_master.loc[derived_block_master["block"] == "South", derived_col].iloc[0]) == 5.0
    assert pd.isna(derived_block_master.loc[derived_block_master["block"] == "Outside", derived_col].iloc[0])
    assert float(derived_district_master.loc[derived_district_master["district"] == "Hyderabad", derived_col].iloc[0]) == 5.0
    assert pd.isna(derived_district_master.loc[derived_district_master["district"] == "Warangal", derived_col].iloc[0])

    assert bool(block_qa.loc[block_qa["block"] == "North", "coverage_pass"].iloc[0]) is True
    assert bool(block_qa.loc[block_qa["block"] == "Outside", "coverage_pass"].iloc[0]) is False
    assert bool(district_qa.loc[district_qa["district"] == "Hyderabad", "delta_warn"].iloc[0]) is True
    assert float(block_qa.loc[block_qa["block"] == "North", "mean_valid_depth_m"].iloc[0]) == pytest.approx(0.75)
    assert pd.isna(block_qa.loc[block_qa["block"] == "Outside", "mean_valid_depth_m"].iloc[0])
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "comparable_block_mean_value_m"].iloc[0]) == pytest.approx(1.875)
    assert float(district_qa.loc[district_qa["district"] == "Hyderabad", "comparable_block_max_value_m"].iloc[0]) == pytest.approx(5.0)
    assert derived_block_qa.columns.tolist() == [
        "state",
        "district",
        "block",
        "block_key",
        "raw_rp100_depth_m",
        "coverage_pass",
        "class_index",
        "class_label",
    ]
    assert derived_district_qa.columns.tolist() == [
        "state",
        "district",
        "district_key",
        "raw_rp100_depth_m",
        "covered_area_fraction",
        "class_index",
        "class_label",
    ]
    assert run_summary["metric_slug"].tolist() == sorted([*JRC_FILE_MAP, DERIVED_INDEX_METRIC_SLUG])
    derived_summary = run_summary.loc[run_summary["metric_slug"] == DERIVED_INDEX_METRIC_SLUG].iloc[0]
    assert derived_summary["metric_kind"] == "derived_index"
    assert derived_summary["source_metric_slug"] == "jrc_flood_depth_rp100"
    assert (run_summary["blocks_total"] == 3).all()
    assert (run_summary["districts_total"] == 2).all()

    qa_dir = _qa_dir(tmp_path)
    assert (qa_dir / "jrc_flood_depth_rp10_block_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_rp10_district_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_index_rp100_block_qa.csv").exists()
    assert (qa_dir / "jrc_flood_depth_index_rp100_district_qa.csv").exists()
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
    assert run_summary.shape[0] == 5
    assert sorted(run_summary["metric_slug"].tolist()) == sorted([*JRC_FILE_MAP, DERIVED_INDEX_METRIC_SLUG])


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
    assert run_summary.shape[0] == 5


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
    assert district_master["jrc_flood_depth_rp10__snapshot__Current__mean"].notna().all()

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
    assert south == 5.0
    assert hyderabad == pytest.approx(2.5)
    assert (run_summary["blocks_covered"] > 0).all()
    assert (run_summary["districts_covered"] > 0).all()


def test_jrc_builder_keeps_district_polygon_direct_mean_as_diagnostic_only(
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

    assert float(hyderabad["chosen_value_m"]) == pytest.approx(3.5)
    assert float(hyderabad["comparable_block_mean_value_m"]) == pytest.approx(1.875)
    assert float(hyderabad["direct_mean_value_m"]) > float(hyderabad["chosen_value_m"])
    assert bool(hyderabad["delta_warn"]) is True
    assert bool(hyderabad["lower_bound_check_pass"]) is True


def test_jrc_builder_fails_when_chosen_value_falls_below_comparable_block_mean(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path / "irt_data"))
    source_dir = tmp_path / "jrc"
    districts_path, _ = _write_boundaries(tmp_path)
    _write_rasters(source_dir)
    district_gdf = gpd.read_file(districts_path)
    district_gdf["district_key"] = ["hyderabad", "warangal"]
    block_master_df = pd.DataFrame(
        [
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "North",
                "block_key": "north",
                "block_area_km2": 1.0,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 1.0,
            },
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "South",
                "block_key": "south",
                "block_area_km2": 1.0,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 1.0,
            },
        ]
    )
    block_qa_df = pd.DataFrame(
        [
            {"block_key": "north", "mean_valid_depth_m": 2.0},
            {"block_key": "south", "mean_valid_depth_m": 2.0},
        ]
    )
    with rasterio.open(source_dir / JRC_FILE_MAP["jrc_flood_depth_rp10"]) as dataset:
        with pytest.raises(ValueError, match="falls below comparable covered-block mean"):
            _build_district_frames(
                district_gdf=district_gdf.iloc[[0]].copy(),
                block_master_df=block_master_df,
                block_qa_df=block_qa_df,
                dataset=dataset,
                metric_slug="jrc_flood_depth_rp10",
            )


def test_jrc_builder_fails_when_chosen_value_exceeds_comparable_block_max(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IRT_DATA_DIR", str(tmp_path / "irt_data"))
    source_dir = tmp_path / "jrc"
    districts_path, _ = _write_boundaries(tmp_path)
    _write_rasters(source_dir)
    district_gdf = gpd.read_file(districts_path)
    district_gdf["district_key"] = ["hyderabad", "warangal"]
    block_master_df = pd.DataFrame(
        [
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "North",
                "block_key": "north",
                "block_area_km2": 1.0,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 6.0,
            },
            {
                "state": "Telangana",
                "district": "Hyderabad",
                "block": "South",
                "block_key": "south",
                "block_area_km2": 1.0,
                "jrc_flood_depth_rp10__snapshot__Current__mean": 6.0,
            },
        ]
    )
    block_qa_df = pd.DataFrame(
        [
            {"block_key": "north", "mean_valid_depth_m": 2.0},
            {"block_key": "south", "mean_valid_depth_m": 3.0},
        ]
    )
    with rasterio.open(source_dir / JRC_FILE_MAP["jrc_flood_depth_rp10"]) as dataset:
        with pytest.raises(ValueError, match="exceeds comparable covered-block max"):
            _build_district_frames(
                district_gdf=district_gdf.iloc[[0]].copy(),
                block_master_df=block_master_df,
                block_qa_df=block_qa_df,
                dataset=dataset,
                metric_slug="jrc_flood_depth_rp10",
            )


def test_classify_depth_index_uses_exact_rp100_thresholds() -> None:
    nan_index, nan_label = _classify_depth_index(np.nan)
    assert pd.isna(nan_index)
    assert nan_label == ""
    assert _classify_depth_index(0.0) == (1.0, "Very Low")
    assert _classify_depth_index(0.25) == (1.0, "Very Low")
    assert _classify_depth_index(0.25001) == (2.0, "Low")
    assert _classify_depth_index(0.50) == (2.0, "Low")
    assert _classify_depth_index(0.50001) == (3.0, "Moderate")
    assert _classify_depth_index(1.20) == (3.0, "Moderate")
    assert _classify_depth_index(1.20001) == (4.0, "High")
    assert _classify_depth_index(2.50) == (4.0, "High")
    assert _classify_depth_index(2.50001) == (5.0, "Extreme")
