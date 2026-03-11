from __future__ import annotations

import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, Polygon

from india_resilience_tool.data.river_topology import build_hydro_river_summary
from tools.geodata.build_river_subbasin_diagnostics import build_river_subbasin_diagnostics_df
from tools.geodata.build_river_topology import build_river_topology_artifacts


def _cleaned_river_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_001", "riv_002"],
            "source_uid_river": ["101", "102"],
            "source_uid_is_duplicate": [False, False],
            "river_name_clean": ["Pranhita", "Wardha"],
            "basin_name_clean": ["Godavari", "Godavari"],
            "subbasin_name_clean": ["Pranhita and others", "Wardha"],
            "state_names_clean": ["Telangana", "Maharashtra"],
            "length_km_source": [3.0, 2.0],
            "geometry_type_clean": ["LineString", "MultiLineString"],
        },
        geometry=[
            LineString([(0, 0), (1, 0)]),
            MultiLineString([[(1, 0), (2, 0)], [(2, 0), (3, 0)]]),
        ],
        crs="EPSG:4326",
    )


def _basin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01"],
            "basin_name": ["Godavari Basin"],
            "hydro_level": ["basin"],
        },
        geometry=[Polygon([(-1, -1), (4, -1), (4, 1), (-1, 1)])],
        crs="EPSG:4326",
    )


def _subbasin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01", "B01"],
            "basin_name": ["Godavari Basin", "Godavari Basin"],
            "subbasin_id": ["SB01", "SB02"],
            "subbasin_code": ["SB01", "SB02"],
            "subbasin_name": ["Pranhita and others", "Wardha"],
            "hydro_level": ["sub_basin", "sub_basin"],
        },
        geometry=[
            Polygon([(-1, -1), (1.5, -1), (1.5, 1), (-1, 1)]),
            Polygon([(1.5, -1), (4, -1), (4, 1), (1.5, 1)]),
        ],
        crs="EPSG:4326",
    )


def test_build_river_topology_artifacts_explodes_lines_and_builds_nodes() -> None:
    reaches, nodes, adjacency, qa_df, missing_df, _missing_gdf, summary = build_river_topology_artifacts(
        _cleaned_river_gdf(),
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
    )

    assert reaches["reach_id"].is_unique
    assert len(reaches) == 3
    assert len(nodes) >= 2
    assert "start_node_id" in reaches.columns
    assert "end_node_id" in reaches.columns
    assert len(adjacency) >= 2
    assert summary["reach_count"] == 3
    assert qa_df["reach_id"].isin(reaches["reach_id"]).all()
    assert missing_df.empty
    assert "diagnostic_endpoint_start" in reaches.columns
    assert "diagnostic_crosses_multiple_subbasins" in reaches.columns


def test_build_river_topology_assigns_majority_hydro_context() -> None:
    reaches, _nodes, _adj, _qa, _missing_df, _missing_gdf, _summary = build_river_topology_artifacts(
        _cleaned_river_gdf(),
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
    )

    assert set(reaches["basin_id"].astype(str)) == {"B01"}
    assert set(reaches["subbasin_id"].astype(str)) <= {"SB01", "SB02"}


def test_build_river_topology_does_not_flag_short_segments_as_self_loops() -> None:
    short = gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_short"],
            "source_uid_river": ["201"],
            "source_uid_is_duplicate": [False],
            "river_name_clean": ["Short Reach"],
            "basin_name_clean": ["Godavari"],
            "subbasin_name_clean": ["Pranhita and others"],
            "state_names_clean": ["Telangana"],
            "length_km_source": [0.05],
            "geometry_type_clean": ["LineString"],
        },
        geometry=[LineString([(0, 0), (0.0005, 0)])],
        crs="EPSG:4326",
    )
    reaches, _nodes, _adj, qa_df, missing_df, _missing_gdf, summary = build_river_topology_artifacts(
        short,
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=250.0,
    )
    assert bool(reaches.iloc[0]["issue_self_loop"]) is False
    assert summary["self_loop_count"] == 0
    assert qa_df.empty
    assert missing_df.empty
    assert bool(reaches.iloc[0]["diagnostic_endpoint_start"]) is True
    assert bool(reaches.iloc[0]["diagnostic_endpoint_end"]) is True


def test_build_river_topology_keeps_cross_subbasin_as_diagnostic_only() -> None:
    crossing = gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_cross"],
            "source_uid_river": ["301"],
            "source_uid_is_duplicate": [False],
            "river_name_clean": ["Boundary Reach"],
            "basin_name_clean": ["Godavari"],
            "subbasin_name_clean": ["Pranhita and others"],
            "state_names_clean": ["Telangana"],
            "length_km_source": [4.0],
            "geometry_type_clean": ["LineString"],
        },
        geometry=[LineString([(0.5, 0), (2.5, 0)])],
        crs="EPSG:4326",
    )
    reaches, _nodes, _adj, qa_df, missing_df, _missing_gdf, summary = build_river_topology_artifacts(
        crossing,
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
    )

    assert bool(reaches.iloc[0]["diagnostic_crosses_multiple_subbasins"]) is True
    assert summary["crosses_multiple_subbasins_count"] == 1
    assert bool(reaches.iloc[0]["issue_missing_subbasin_assignment"]) is False
    assert qa_df.empty
    assert missing_df.empty


def test_build_river_topology_repairs_near_boundary_misses_with_fallback() -> None:
    near_boundary = gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_near"],
            "source_uid_river": ["401"],
            "source_uid_is_duplicate": [False],
            "river_name_clean": ["Near Boundary"],
            "basin_name_clean": ["Godavari"],
            "subbasin_name_clean": ["Wardha"],
            "state_names_clean": ["Telangana"],
            "length_km_source": [1.0],
            "geometry_type_clean": ["LineString"],
        },
        geometry=[LineString([(4.0005, -0.2), (4.0005, 0.2)])],
        crs="EPSG:4326",
    )
    reaches, _nodes, _adj, qa_df, missing_df, _missing_gdf, summary = build_river_topology_artifacts(
        near_boundary,
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
        nearest_assignment_threshold_km=1.0,
    )

    row = reaches.iloc[0]
    assert row["basin_assignment_method"] == "nearest_boundary_fallback"
    assert row["subbasin_assignment_method"] == "nearest_boundary_fallback"
    assert row["basin_id"] == "B01"
    assert row["subbasin_id"] == "SB02"
    assert float(row["basin_assignment_distance_km"]) < 1.0
    assert float(row["subbasin_assignment_distance_km"]) < 1.0
    assert summary["fixed_basin_assignment_by_fallback_count"] == 1
    assert summary["fixed_subbasin_assignment_by_fallback_count"] == 1
    assert qa_df.empty
    assert missing_df.empty
    hydro_summary = build_hydro_river_summary(reaches, level="sub_basin", basin_id="B01", subbasin_id="SB02")
    assert hydro_summary is not None
    assert hydro_summary["fallback_segment_count"] == 1


def test_build_river_topology_keeps_far_misses_unassigned_and_exports_diagnostics() -> None:
    far_away = gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_far"],
            "source_uid_river": ["402"],
            "source_uid_is_duplicate": [False],
            "river_name_clean": ["Far Away"],
            "basin_name_clean": ["Unknown"],
            "subbasin_name_clean": ["Unknown"],
            "state_names_clean": ["Unknown"],
            "length_km_source": [1.0],
            "geometry_type_clean": ["LineString"],
        },
        geometry=[LineString([(10.0, 10.0), (10.5, 10.5)])],
        crs="EPSG:4326",
    )
    reaches, _nodes, _adj, qa_df, missing_df, missing_gdf, summary = build_river_topology_artifacts(
        far_away,
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
        nearest_assignment_threshold_km=1.0,
    )

    row = reaches.iloc[0]
    assert row["basin_assignment_method"] == "unassigned"
    assert row["subbasin_assignment_method"] == "unassigned"
    assert bool(row["issue_missing_basin_assignment"]) is True
    assert bool(row["issue_missing_subbasin_assignment"]) is True
    assert len(qa_df) == 1
    assert len(missing_df) == 1
    assert len(missing_gdf) == 1
    assert missing_df.iloc[0]["nearest_basin_name"] == "Godavari Basin"
    assert missing_df.iloc[0]["nearest_subbasin_name"] in {"Pranhita and others", "Wardha"}
    assert missing_df.iloc[0]["remediation_status"] == "still_unassigned"
    assert summary["missing_basin_assignment_count"] == 1
    assert summary["missing_subbasin_assignment_count"] == 1


def test_build_hydro_river_summary_returns_top_named_rivers() -> None:
    reaches, _nodes, _adj, _qa, _missing_df, _missing_gdf, _summary = build_river_topology_artifacts(
        _cleaned_river_gdf(),
        _basin_gdf(),
        _subbasin_gdf(),
        snap_tolerance_m=1.0,
    )
    summary = build_hydro_river_summary(
        reaches,
        level="basin",
        basin_id="B01",
    )
    assert summary is not None
    assert summary["reach_count"] == 3
    assert summary["fallback_segment_count"] == 0
    assert summary["top_named_rivers"][0]["river_name"] in {"Pranhita", "Wardha"}


def test_build_river_subbasin_diagnostics_df_marks_exact_matches() -> None:
    river_display = gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_001", "riv_002"],
            "source_uid_river": ["101", "102"],
            "river_name_clean": ["Pranhita", "Wardha"],
            "basin_name_clean": ["Godavari", "Godavari"],
            "subbasin_name_clean": ["Pranhita and others", "Major River"],
            "state_names_clean": ["Telangana", "Maharashtra"],
            "length_km_source": [10.0, 5.0],
        },
        geometry=[LineString([(0, 0), (1, 0)]), LineString([(1, 0), (2, 0)])],
        crs="EPSG:4326",
    )
    diagnostics = build_river_subbasin_diagnostics_df(_subbasin_gdf(), river_display)
    pranhita = diagnostics.loc[diagnostics["subbasin_id"] == "SB01"].iloc[0]
    wardha = diagnostics.loc[diagnostics["subbasin_id"] == "SB02"].iloc[0]
    assert pranhita["match_status"] == "matched"
    assert int(pranhita["matched_river_feature_count"]) == 1
    assert wardha["match_status"] == "review_required"
