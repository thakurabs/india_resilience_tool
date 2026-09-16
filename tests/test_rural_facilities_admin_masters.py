from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Polygon

import tools.geodata.build_rural_facilities_admin_masters as rural_module
from tools.geodata.build_rural_facilities_admin_masters import (
    COUNT_METRIC_SLUGS,
    POPULATION_DENOMINATOR_COL,
    RATE_METRIC_SLUGS,
    REQUIRED_SOURCE_COLUMNS,
    assign_points_to_blocks,
    build_rural_facilities_admin_outputs,
    normalize_rural_facility_points,
)


def _source_rows() -> gpd.GeoDataFrame:
    rows = []
    for family, lon, lat in [
        ("agro", 78.1, 17.1),
        ("education", 78.2, 17.1),
        ("health", 78.1, 17.2),
        ("service", 120.0, 17.2),
    ]:
        row = {col: "x" for col in REQUIRED_SOURCE_COLUMNS}
        row.update(
            {
                "facilityna": f"{family} facility",
                "fileupload": "2020-01-01",
                "longitude": lon,
                "lattitude": lat,
                "facility_family": family,
                "source_shapefile_name": f"{family}.shp",
                "source_row_id": len(rows),
            }
        )
        rows.append(row)
    return gpd.GeoDataFrame(rows, geometry=[None] * len(rows), crs="EPSG:4326")


def _blocks() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Demo District", "Demo District"],
            "block_name": ["A Block", "B Block"],
            "block_key": ["Telangana::Demo District::A Block", "Telangana::Demo District::B Block"],
        },
        geometry=[
            Polygon([(78.0, 17.0), (78.5, 17.0), (78.5, 17.5), (78.0, 17.5)]),
            Polygon([(79.0, 17.0), (79.5, 17.0), (79.5, 17.5), (79.0, 17.5)]),
        ],
        crs="EPSG:4326",
    )


def _districts() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["DEMO DISTRICT"],
            "district_key": ["Telangana::DEMO DISTRICT"],
        },
        geometry=[Polygon([(78.0, 17.0), (79.5, 17.0), (79.5, 17.5), (78.0, 17.5)])],
        crs="EPSG:4326",
    )


def test_invalid_coordinates_are_qa_only_and_do_not_affect_counts() -> None:
    points, invalid = normalize_rural_facility_points(_source_rows())
    assigned, unmatched, ambiguous = assign_points_to_blocks(points, _blocks())

    assert invalid.shape[0] == 1
    assert assigned.shape[0] == 3
    assert unmatched.empty
    assert ambiguous.empty
    assert sorted(assigned["facility_family"].tolist()) == ["agro", "education", "health"]


def test_block_assignment_marks_boundary_overlap_ambiguous() -> None:
    raw = _source_rows().iloc[[0]].copy()
    raw["longitude"] = 78.5
    raw["lattitude"] = 17.25
    overlapping = _blocks().copy()
    overlapping.loc[1, "geometry"] = Polygon([(78.5, 17.0), (79.0, 17.0), (79.0, 17.5), (78.5, 17.5)])
    points, _invalid = normalize_rural_facility_points(raw)
    assigned, unmatched, ambiguous = assign_points_to_blocks(points, overlapping)

    assert assigned.empty
    assert unmatched.empty
    assert ambiguous["point_index"].nunique() == 1


def test_build_outputs_zero_count_admin_units_and_per_capita(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rural_module, "load_rural_facilities_sources", lambda _path: _source_rows())
    monkeypatch.setattr(rural_module, "load_district_boundaries", lambda _path: _districts())
    monkeypatch.setattr(rural_module, "load_block_boundaries", lambda _path: _blocks())
    denom_block = pd.DataFrame(
        {
            "state": ["Telangana", "Telangana"],
            "district": ["Demo District", "Demo District"],
            "block": ["A Block", "B Block"],
            POPULATION_DENOMINATOR_COL: [1000.0, 0.0],
        }
    )
    denom_district = pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["DEMO DISTRICT"],
            POPULATION_DENOMINATOR_COL: [3000.0],
        }
    )

    def _denoms(*, level, states, data_dir):
        return denom_block if level == "block" else denom_district

    monkeypatch.setattr(rural_module, "_load_population_denominators", _denoms)
    monkeypatch.setattr(rural_module, "_write_state_slices", lambda *args, **kwargs: None)
    monkeypatch.setattr(rural_module, "export_rural_facilities_density_overlays", lambda **kwargs: [])

    outputs = build_rural_facilities_admin_outputs(
        source_dir=tmp_path,
        districts_path=tmp_path / "districts.geojson",
        blocks_path=tmp_path / "blocks.geojson",
        qa_dir=tmp_path / "qa",
        overlay_dir=tmp_path / "overlay",
        overwrite=True,
        dry_run=False,
    )

    block_counts = dict(zip(outputs.block_master["block"], outputs.block_master[COUNT_METRIC_SLUGS["total"]]))
    assert block_counts == {"A Block": 3, "B Block": 0}
    block_rates = dict(zip(outputs.block_master["block"], outputs.block_master[RATE_METRIC_SLUGS["total"]]))
    assert block_rates["A Block"] == pytest.approx(300.0)
    assert np.isnan(block_rates["B Block"])
    assert outputs.district_master["district"].iloc[0] == "DEMO DISTRICT"
    assert float(outputs.district_master[COUNT_METRIC_SLUGS["total"]].iloc[0]) == 3.0
    assert float(outputs.district_master[RATE_METRIC_SLUGS["total"]].iloc[0]) == pytest.approx(100.0)
    assert (tmp_path / "qa" / "rural_facilities_population_denominator_issues.csv").exists()


def test_population_denominator_failure_happens_before_writes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(rural_module, "load_rural_facilities_sources", lambda _path: _source_rows())
    monkeypatch.setattr(rural_module, "load_district_boundaries", lambda _path: _districts())
    monkeypatch.setattr(rural_module, "load_block_boundaries", lambda _path: _blocks())
    monkeypatch.setattr(
        rural_module,
        "_load_population_denominators",
        lambda **kwargs: (_ for _ in ()).throw(FileNotFoundError("missing denominator")),
    )

    with pytest.raises(FileNotFoundError, match="missing denominator"):
        build_rural_facilities_admin_outputs(
            source_dir=tmp_path,
            districts_path=tmp_path / "districts.geojson",
            blocks_path=tmp_path / "blocks.geojson",
            qa_dir=tmp_path / "qa",
            overlay_dir=tmp_path / "overlay",
            overwrite=True,
            dry_run=False,
        )
    assert not (tmp_path / "qa").exists()
