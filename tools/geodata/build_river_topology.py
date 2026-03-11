#!/usr/bin/env python3
"""
Build topology-ready river reach, node, adjacency, and QA artifacts for IRT.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from india_resilience_tool.data.hydro_loader import ensure_epsg4326, ensure_hydro_columns
from india_resilience_tool.data.river_topology import ensure_river_reach_columns
from paths import (
    BASINS_PATH,
    RIVER_ADJACENCY_PATH,
    RIVER_MISSING_ASSIGNMENTS_GEOJSON_PATH,
    RIVER_MISSING_ASSIGNMENTS_PATH,
    RIVER_NETWORK_PATH,
    RIVER_NODES_PATH,
    RIVER_REACHES_PATH,
    RIVER_TOPOLOGY_QA_PATH,
    SUBBASINS_PATH,
)


_ASSIGN_EPSG = 6933


def _ensure_line_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("River network parquet has no CRS.")
    bad = ~gdf.geom_type.isin(["LineString", "MultiLineString"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"River network parquet contains non-line geometries: {bad_types}.")
    return gdf.to_crs(epsg=4326)


def _explode_to_lines(cleaned_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    work = cleaned_gdf.copy().explode(index_parts=True, ignore_index=True)
    bad = ~work.geom_type.eq("LineString")
    if bad.any():
        bad_types = sorted(work.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"Expected exploded LineString parts, found: {bad_types}.")

    work["river_part_index"] = (
        work.groupby("river_feature_id", dropna=False).cumcount().astype("int64") + 1
    )
    work["reach_id"] = (
        work["river_feature_id"].astype(str)
        + "__seg"
        + work["river_part_index"].astype(str).str.zfill(4)
    )
    if work["reach_id"].duplicated().any():
        raise ValueError("Failed to create unique reach_id values.")
    return work.reset_index(drop=True)


def _coerce_nullable_bool(series: pd.Series) -> pd.Series:
    """Return a plain bool Series without pandas object fillna warnings."""
    return pd.Series(series, copy=False).astype("boolean").fillna(False).astype(bool)


def _endpoint_series(gdf_6933: gpd.GeoDataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in gdf_6933.iterrows():
        geom = row.geometry
        if not isinstance(geom, LineString):
            raise ValueError("Topology builder expects single-part LineString geometries after explode.")
        start_x, start_y = geom.coords[0]
        end_x, end_y = geom.coords[-1]
        rows.append({"reach_id": row["reach_id"], "endpoint_role": "start", "x": float(start_x), "y": float(start_y)})
        rows.append({"reach_id": row["reach_id"], "endpoint_role": "end", "x": float(end_x), "y": float(end_y)})
    return pd.DataFrame(rows)


def _build_nodes(endpoints_df: pd.DataFrame, *, snap_tolerance_m: float) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    tol = float(snap_tolerance_m)
    if tol <= 0:
        raise ValueError("snap_tolerance_m must be > 0.")

    work = endpoints_df.copy()
    work["snap_ix"] = (work["x"] / tol).round().astype("int64")
    work["snap_iy"] = (work["y"] / tol).round().astype("int64")
    work["__snap_key"] = work["snap_ix"].astype(str) + "|" + work["snap_iy"].astype(str)

    grouped = (
        work.groupby("__snap_key", dropna=False)
        .agg(x=("x", "mean"), y=("y", "mean"), degree=("reach_id", "nunique"))
        .reset_index()
        .sort_values(by=["y", "x", "__snap_key"])
        .reset_index(drop=True)
    )
    grouped["node_id"] = ["node_" + str(i + 1).zfill(6) for i in range(len(grouped))]
    grouped["node_type"] = grouped["degree"].map(
        lambda deg: "endpoint" if int(deg) <= 1 else "confluence_candidate" if int(deg) == 2 else "junction"
    )
    node_gdf = gpd.GeoDataFrame(
        grouped[["node_id", "degree", "node_type"]].copy(),
        geometry=gpd.points_from_xy(grouped["x"], grouped["y"]),
        crs=f"EPSG:{_ASSIGN_EPSG}",
    ).to_crs(epsg=4326)

    endpoint_nodes = work.merge(grouped[["__snap_key", "node_id"]], on="__snap_key", how="left")
    return node_gdf.reset_index(drop=True), endpoint_nodes.reset_index(drop=True)


def _append_exact_nodes_for_same_reach_collisions(
    nodes_gdf: gpd.GeoDataFrame,
    endpoint_nodes: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Prevent short reaches from collapsing into self-loops due to coarse snapping.

    If a reach's start and end were snapped to the same node but their original
    coordinates differ, reassign those endpoints to exact-coordinate nodes.
    """
    role_pivot = (
        endpoint_nodes.pivot_table(
            index="reach_id",
            columns="endpoint_role",
            values=["node_id", "x", "y"],
            aggfunc="first",
        )
        .reset_index()
    )
    role_pivot.columns = [
        "reach_id" if col == ("reach_id", "") else f"{col[0]}_{col[1]}"
        for col in role_pivot.columns
    ]
    if role_pivot.empty:
        return nodes_gdf, endpoint_nodes

    same_node = role_pivot["node_id_start"].astype(str) == role_pivot["node_id_end"].astype(str)
    endpoint_distance = (
        (role_pivot["x_start"] - role_pivot["x_end"]).pow(2)
        + (role_pivot["y_start"] - role_pivot["y_end"]).pow(2)
    ).pow(0.5)
    affected_reach_ids = role_pivot.loc[same_node & endpoint_distance.gt(0.001), "reach_id"].astype(str)
    if affected_reach_ids.empty:
        return nodes_gdf, endpoint_nodes

    adjusted = endpoint_nodes.copy()
    affected_mask = adjusted["reach_id"].astype(str).isin(set(affected_reach_ids.tolist()))
    adjusted["__exact_key"] = None
    affected = adjusted.loc[affected_mask].copy()
    affected["__exact_key"] = (
        affected["x"].round(3).astype(str) + "|" + affected["y"].round(3).astype(str)
    )
    adjusted.loc[affected_mask, "__exact_key"] = affected["__exact_key"].values
    exact_groups = (
        affected.groupby("__exact_key", dropna=False)
        .agg(x=("x", "mean"), y=("y", "mean"), degree=("reach_id", "nunique"))
        .reset_index()
        .sort_values(by=["y", "x", "__exact_key"])
        .reset_index(drop=True)
    )
    if nodes_gdf.empty:
        start_idx = 0
    else:
        extracted = nodes_gdf["node_id"].astype(str).str.extract(r"(\d+)$")[0].dropna()
        start_idx = int(extracted.astype(int).max()) if not extracted.empty else 0
    exact_groups["node_id"] = [
        "node_" + str(start_idx + i + 1).zfill(6) for i in range(len(exact_groups))
    ]
    exact_groups["node_type"] = exact_groups["degree"].map(
        lambda deg: "endpoint" if int(deg) <= 1 else "confluence_candidate" if int(deg) == 2 else "junction"
    )

    new_nodes = gpd.GeoDataFrame(
        exact_groups[["node_id", "degree", "node_type"]].copy(),
        geometry=gpd.points_from_xy(exact_groups["x"], exact_groups["y"]),
        crs=f"EPSG:{_ASSIGN_EPSG}",
    ).to_crs(epsg=4326)

    adjusted = adjusted.merge(exact_groups[["__exact_key", "node_id"]], on="__exact_key", how="left", suffixes=("", "__exact"))
    exact_mask = adjusted["__exact_key"].notna() & adjusted["node_id__exact"].notna()
    adjusted.loc[exact_mask, "node_id"] = adjusted.loc[exact_mask, "node_id__exact"].astype(str)
    adjusted = adjusted.drop(columns=["__exact_key", "node_id__exact"], errors="ignore")

    nodes_out = pd.concat([nodes_gdf, new_nodes], ignore_index=True)
    return nodes_out.reset_index(drop=True), adjusted.reset_index(drop=True)


def _build_adjacency(endpoint_nodes: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    dedup = endpoint_nodes[["reach_id", "node_id"]].drop_duplicates().copy()
    for node_id, node_rows in dedup.groupby("node_id", dropna=False):
        reach_ids = sorted(set(node_rows["reach_id"].astype(str).tolist()))
        if len(reach_ids) < 2:
            continue
        for from_id in reach_ids:
            for to_id in reach_ids:
                if from_id == to_id:
                    continue
                rows.append(
                    {
                        "from_reach_id": from_id,
                        "to_reach_id": to_id,
                        "shared_node_id": str(node_id),
                        "relation_type": "shared_node",
                    }
                )
    if not rows:
        return pd.DataFrame(columns=["from_reach_id", "to_reach_id", "shared_node_id", "relation_type"])
    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)


def _project_for_assignment(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError("Assignment GeoDataFrame has no CRS.")
    return gdf.to_crs(epsg=_ASSIGN_EPSG)


def _representative_points(reaches_4326: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    points = reaches_4326[["reach_id", "geometry"]].copy()
    points_proj = _project_for_assignment(points)
    points_proj.geometry = points_proj.geometry.representative_point()
    return points_proj


def _choose_top_assignment(group: pd.DataFrame, *, id_col: str, name_col: str) -> dict[str, Any]:
    grouped = (
        group.groupby([id_col, name_col], dropna=False)["intersection_length_km"]
        .sum()
        .reset_index()
        .sort_values(by=["intersection_length_km", id_col], ascending=[False, True])
        .reset_index(drop=True)
    )
    first = grouped.iloc[0]
    total = float(grouped["intersection_length_km"].sum())
    return {
        "id": str(first[id_col]).strip() or None,
        "name": str(first[name_col]).strip() or None,
        "share": float(first["intersection_length_km"] / total) if total > 0 else None,
        "crosses_multiple": bool(len(grouped) > 1),
    }


def _spatial_assign_lines(
    reaches_4326: gpd.GeoDataFrame,
    polygons_4326: gpd.GeoDataFrame,
    *,
    id_col: str,
    name_col: str,
    prefix: str,
) -> pd.DataFrame:
    reaches = reaches_4326[["reach_id", "geometry"]].copy()
    polygons = polygons_4326[[id_col, name_col, "geometry"]].copy()
    reaches_proj = _project_for_assignment(reaches)
    polygons_proj = _project_for_assignment(polygons)
    intersections = gpd.overlay(reaches_proj, polygons_proj, how="intersection", keep_geom_type=False)
    if intersections.empty:
        return pd.DataFrame(
            {
                "reach_id": reaches_4326["reach_id"].astype(str),
                f"{prefix}_id": None,
                f"{prefix}_name": None,
                f"{prefix}_assignment_share": None,
                f"crosses_multiple_{prefix}s": False,
                f"{prefix}_assignment_method": "unassigned",
            }
        )

    intersections["intersection_length_km"] = intersections.geometry.length / 1000.0
    intersections = intersections.loc[intersections["intersection_length_km"] > 0].copy()
    if intersections.empty:
        return pd.DataFrame(
            {
                "reach_id": reaches_4326["reach_id"].astype(str),
                f"{prefix}_id": None,
                f"{prefix}_name": None,
                f"{prefix}_assignment_share": None,
                f"crosses_multiple_{prefix}s": False,
                f"{prefix}_assignment_method": "unassigned",
            }
        )

    assignments: list[dict[str, Any]] = []
    for reach_id, group in intersections.groupby("reach_id", dropna=False):
        top = _choose_top_assignment(group, id_col=id_col, name_col=name_col)
        assignments.append(
            {
                "reach_id": str(reach_id),
                f"{prefix}_id": top["id"],
                f"{prefix}_name": top["name"],
                f"{prefix}_assignment_share": top["share"],
                f"crosses_multiple_{prefix}s": top["crosses_multiple"],
                f"{prefix}_assignment_method": "spatial_majority",
            }
        )

    out = pd.DataFrame(assignments)
    out = reaches_4326[["reach_id"]].merge(out, on="reach_id", how="left")
    out[f"{prefix}_assignment_method"] = out[f"{prefix}_assignment_method"].fillna("unassigned")
    out[f"crosses_multiple_{prefix}s"] = _coerce_nullable_bool(out[f"crosses_multiple_{prefix}s"])
    return out


def _nearest_assignment_candidates(
    reaches_4326: gpd.GeoDataFrame,
    polygons_4326: gpd.GeoDataFrame,
    *,
    id_col: str,
    name_col: str,
    prefix: str,
    max_distance_km: Optional[float] = None,
) -> pd.DataFrame:
    if reaches_4326.empty:
        return pd.DataFrame(
            columns=[
                "reach_id",
                f"{prefix}_candidate_id",
                f"{prefix}_candidate_name",
                f"{prefix}_candidate_distance_km",
            ]
        )

    points_proj = _representative_points(reaches_4326)
    polygons_proj = _project_for_assignment(polygons_4326[[id_col, name_col, "geometry"]].copy())
    join_kwargs: dict[str, Any] = {
        "how": "left",
        "distance_col": "candidate_distance_m",
    }
    if max_distance_km is not None:
        join_kwargs["max_distance"] = float(max_distance_km) * 1000.0
    nearest = gpd.sjoin_nearest(
        points_proj,
        polygons_proj,
        **join_kwargs,
    )
    nearest = (
        nearest[["reach_id", id_col, name_col, "candidate_distance_m"]]
        .sort_values(by=["reach_id", "candidate_distance_m", id_col], ascending=[True, True, True])
        .drop_duplicates(subset=["reach_id"], keep="first")
        .reset_index(drop=True)
    )
    return nearest.rename(
        columns={
            id_col: f"{prefix}_candidate_id",
            name_col: f"{prefix}_candidate_name",
            "candidate_distance_m": f"{prefix}_candidate_distance_m",
        }
    )


def _apply_nearest_assignment_fallback(
    reaches_4326: gpd.GeoDataFrame,
    polygons_4326: gpd.GeoDataFrame,
    *,
    id_col: str,
    name_col: str,
    prefix: str,
    max_distance_km: float,
) -> gpd.GeoDataFrame:
    out = reaches_4326.copy()
    target_id_col = f"{prefix}_id"
    target_name_col = f"{prefix}_name"
    target_share_col = f"{prefix}_assignment_share"
    target_method_col = f"{prefix}_assignment_method"
    target_distance_col = f"{prefix}_assignment_distance_km"

    if target_distance_col not in out.columns:
        out[target_distance_col] = pd.NA
    if target_share_col not in out.columns:
        out[target_share_col] = pd.NA

    unresolved = out.loc[
        out[target_id_col].isna() | out[target_id_col].astype(str).str.strip().eq("")
    ].copy()
    if unresolved.empty:
        return out

    candidates = _nearest_assignment_candidates(
        unresolved,
        polygons_4326,
        id_col=id_col,
        name_col=name_col,
        prefix=prefix,
        max_distance_km=max_distance_km,
    )
    if candidates.empty:
        return out

    candidates[target_distance_col] = pd.to_numeric(
        candidates[f"{prefix}_candidate_distance_m"],
        errors="coerce",
    ) / 1000.0
    candidates = candidates.drop(columns=[f"{prefix}_candidate_distance_m"])

    update_mask = candidates[f"{prefix}_candidate_id"].notna() & candidates[f"{prefix}_candidate_name"].notna()
    if not update_mask.any():
        return out

    updates = candidates.loc[update_mask, ["reach_id", f"{prefix}_candidate_id", f"{prefix}_candidate_name", target_distance_col]].rename(
        columns={
            f"{prefix}_candidate_id": target_id_col,
            f"{prefix}_candidate_name": target_name_col,
        }
    )
    out = out.merge(updates, on="reach_id", how="left", suffixes=("", "__fallback"))
    fallback_mask = out[f"{target_id_col}__fallback"].notna()
    out.loc[fallback_mask, target_id_col] = out.loc[fallback_mask, f"{target_id_col}__fallback"].astype(str)
    out.loc[fallback_mask, target_name_col] = out.loc[fallback_mask, f"{target_name_col}__fallback"].astype(str)
    out.loc[fallback_mask, target_method_col] = "nearest_boundary_fallback"
    out.loc[fallback_mask, target_distance_col] = out.loc[fallback_mask, f"{target_distance_col}__fallback"]
    out = out.drop(
        columns=[
            f"{target_id_col}__fallback",
            f"{target_name_col}__fallback",
            f"{target_distance_col}__fallback",
        ],
        errors="ignore",
    )
    return out


def _assign_reaches_to_hydro(
    reaches_4326: gpd.GeoDataFrame,
    basin_gdf: gpd.GeoDataFrame,
    subbasin_gdf: gpd.GeoDataFrame,
    *,
    nearest_assignment_threshold_km: float,
) -> gpd.GeoDataFrame:
    basin_assign = _spatial_assign_lines(
        reaches_4326,
        basin_gdf,
        id_col="basin_id",
        name_col="basin_name",
        prefix="basin",
    )
    sub_assign = _spatial_assign_lines(
        reaches_4326,
        subbasin_gdf,
        id_col="subbasin_id",
        name_col="subbasin_name",
        prefix="subbasin",
    )
    merged = reaches_4326.merge(basin_assign, on="reach_id", how="left").merge(sub_assign, on="reach_id", how="left")
    merged["pre_fallback_missing_basin_assignment"] = (
        merged["basin_id"].isna() | merged["basin_id"].astype(str).str.strip().eq("")
    )
    merged["pre_fallback_missing_subbasin_assignment"] = (
        merged["subbasin_id"].isna() | merged["subbasin_id"].astype(str).str.strip().eq("")
    )
    merged["basin_assignment_distance_km"] = pd.NA
    merged["subbasin_assignment_distance_km"] = pd.NA
    merged = _apply_nearest_assignment_fallback(
        merged,
        basin_gdf,
        id_col="basin_id",
        name_col="basin_name",
        prefix="basin",
        max_distance_km=nearest_assignment_threshold_km,
    )
    merged = _apply_nearest_assignment_fallback(
        merged,
        subbasin_gdf,
        id_col="subbasin_id",
        name_col="subbasin_name",
        prefix="subbasin",
        max_distance_km=nearest_assignment_threshold_km,
    )
    return merged


def _build_missing_assignment_artifacts(
    reaches_4326: gpd.GeoDataFrame,
    basin_gdf: gpd.GeoDataFrame,
    subbasin_gdf: gpd.GeoDataFrame,
) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    missing = reaches_4326.loc[
        reaches_4326["issue_missing_basin_assignment"] | reaches_4326["issue_missing_subbasin_assignment"]
    ].copy()
    if missing.empty:
        base_cols = [
            "reach_id",
            "river_feature_id",
            "river_name_clean",
            "basin_name_clean",
            "subbasin_name_clean",
            "state_names_clean",
            "reach_length_km",
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_name",
            "representative_lon",
            "representative_lat",
            "nearest_basin_id",
            "nearest_basin_name",
            "nearest_basin_distance_km",
            "nearest_subbasin_id",
            "nearest_subbasin_name",
            "nearest_subbasin_distance_km",
            "remediation_status",
        ]
        return pd.DataFrame(columns=base_cols), gpd.GeoDataFrame(columns=base_cols + ["geometry"], geometry="geometry", crs="EPSG:4326")

    rep_points_proj = _representative_points(missing)
    rep_points_4326 = rep_points_proj.to_crs(epsg=4326)
    rep_lookup = rep_points_4326.copy()
    rep_lookup["representative_lon"] = rep_lookup.geometry.x
    rep_lookup["representative_lat"] = rep_lookup.geometry.y
    rep_lookup = rep_lookup[["reach_id", "representative_lon", "representative_lat"]]

    basin_candidates = _nearest_assignment_candidates(
        missing,
        basin_gdf,
        id_col="basin_id",
        name_col="basin_name",
        prefix="nearest_basin",
        max_distance_km=None,
    )
    subbasin_candidates = _nearest_assignment_candidates(
        missing,
        subbasin_gdf,
        id_col="subbasin_id",
        name_col="subbasin_name",
        prefix="nearest_subbasin",
        max_distance_km=None,
    )

    out = missing.merge(rep_lookup, on="reach_id", how="left")
    out = out.merge(basin_candidates, on="reach_id", how="left")
    out = out.merge(subbasin_candidates, on="reach_id", how="left")
    for col in ("nearest_basin_candidate_distance_m", "nearest_subbasin_candidate_distance_m"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["nearest_basin_distance_km"] = out["nearest_basin_candidate_distance_m"] / 1000.0
    out["nearest_subbasin_distance_km"] = out["nearest_subbasin_candidate_distance_m"] / 1000.0

    out["remediation_status"] = "still_unassigned"
    out.loc[
        out["issue_missing_basin_assignment"] & ~out["issue_missing_subbasin_assignment"],
        "remediation_status",
    ] = "missing_basin_only"
    out.loc[
        out["issue_missing_subbasin_assignment"] & ~out["issue_missing_basin_assignment"],
        "remediation_status",
    ] = "missing_subbasin_only"

    csv_cols = [
        "reach_id",
        "river_feature_id",
        "river_name_clean",
        "basin_name_clean",
        "subbasin_name_clean",
        "state_names_clean",
        "reach_length_km",
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_name",
        "representative_lon",
        "representative_lat",
        "nearest_basin_candidate_id",
        "nearest_basin_candidate_name",
        "nearest_basin_distance_km",
        "nearest_subbasin_candidate_id",
        "nearest_subbasin_candidate_name",
        "nearest_subbasin_distance_km",
        "remediation_status",
    ]
    csv_df = out[csv_cols].rename(
        columns={
            "nearest_basin_candidate_id": "nearest_basin_id",
            "nearest_basin_candidate_name": "nearest_basin_name",
            "nearest_subbasin_candidate_id": "nearest_subbasin_id",
            "nearest_subbasin_candidate_name": "nearest_subbasin_name",
        }
    ).reset_index(drop=True)

    debug_gdf = out[
        [
            "reach_id",
            "river_feature_id",
            "river_name_clean",
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_name",
            "remediation_status",
            "geometry",
        ]
    ].copy()
    debug_gdf = gpd.GeoDataFrame(debug_gdf, geometry="geometry", crs=reaches_4326.crs)
    return csv_df, debug_gdf.reset_index(drop=True)


def build_river_topology_artifacts(
    cleaned_gdf: gpd.GeoDataFrame,
    basin_gdf: gpd.GeoDataFrame,
    subbasin_gdf: gpd.GeoDataFrame,
    *,
    snap_tolerance_m: float = 250.0,
    nearest_assignment_threshold_km: float = 1.0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, gpd.GeoDataFrame, dict[str, Any]]:
    """Build canonical reach, node, adjacency, QA, and summary artifacts."""
    cleaned = _ensure_line_crs(cleaned_gdf)
    basins = ensure_hydro_columns(ensure_epsg4326(basin_gdf), level="basin")
    subbasins = ensure_hydro_columns(ensure_epsg4326(subbasin_gdf), level="sub_basin")

    reaches = _explode_to_lines(cleaned)
    reaches_proj = reaches.to_crs(epsg=_ASSIGN_EPSG)
    endpoints_df = _endpoint_series(reaches_proj)
    nodes_gdf, endpoint_nodes = _build_nodes(endpoints_df, snap_tolerance_m=float(snap_tolerance_m))
    nodes_gdf, endpoint_nodes = _append_exact_nodes_for_same_reach_collisions(
        nodes_gdf,
        endpoint_nodes,
    )
    node_lookup = endpoint_nodes.pivot_table(index="reach_id", columns="endpoint_role", values="node_id", aggfunc="first").reset_index()
    node_lookup.columns.name = None
    node_lookup = node_lookup.rename(columns={"start": "start_node_id", "end": "end_node_id"})

    reaches["reach_length_km"] = reaches_proj.geometry.length / 1000.0
    reaches = reaches.merge(node_lookup, on="reach_id", how="left")
    reaches = _assign_reaches_to_hydro(
        reaches,
        basins,
        subbasins,
        nearest_assignment_threshold_km=nearest_assignment_threshold_km,
    )
    pre_fallback_missing_basin = _coerce_nullable_bool(reaches["pre_fallback_missing_basin_assignment"])
    pre_fallback_missing_subbasin = _coerce_nullable_bool(reaches["pre_fallback_missing_subbasin_assignment"])
    reaches["issue_missing_basin_assignment"] = reaches["basin_id"].isna() | reaches["basin_id"].astype(str).str.strip().eq("")
    reaches["issue_missing_subbasin_assignment"] = reaches["subbasin_id"].isna() | reaches["subbasin_id"].astype(str).str.strip().eq("")
    reaches["diagnostic_crosses_multiple_basins"] = _coerce_nullable_bool(reaches["crosses_multiple_basins"])
    reaches["diagnostic_crosses_multiple_subbasins"] = _coerce_nullable_bool(reaches["crosses_multiple_subbasins"])
    reaches["issue_self_loop"] = (
        reaches["start_node_id"].notna()
        & reaches["end_node_id"].notna()
        & reaches["start_node_id"].astype(str).eq(reaches["end_node_id"].astype(str))
    )

    adjacency_df = _build_adjacency(endpoint_nodes)
    node_degree_map = nodes_gdf.set_index("node_id")["degree"].to_dict()
    reaches["diagnostic_endpoint_start"] = reaches["start_node_id"].map(node_degree_map).fillna(0).astype(int).le(1)
    reaches["diagnostic_endpoint_end"] = reaches["end_node_id"].map(node_degree_map).fillna(0).astype(int).le(1)
    reaches["issue_dangling_start"] = reaches["start_node_id"].isna() | reaches["start_node_id"].astype(str).str.strip().eq("")
    reaches["issue_dangling_end"] = reaches["end_node_id"].isna() | reaches["end_node_id"].astype(str).str.strip().eq("")

    qa_cols = [
        "reach_id",
        "river_feature_id",
        "river_name_clean",
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_name",
        "reach_length_km",
        "issue_missing_basin_assignment",
        "issue_missing_subbasin_assignment",
        "diagnostic_crosses_multiple_basins",
        "diagnostic_crosses_multiple_subbasins",
        "issue_dangling_start",
        "issue_dangling_end",
        "diagnostic_endpoint_start",
        "diagnostic_endpoint_end",
        "issue_self_loop",
    ]
    issue_cols = [col for col in qa_cols if col.startswith("issue_")]
    qa_df = reaches.loc[reaches[issue_cols].any(axis=1), qa_cols].copy().reset_index(drop=True)
    missing_assignments_df, missing_assignments_gdf = _build_missing_assignment_artifacts(
        reaches,
        basins,
        subbasins,
    )

    summary = {
        "source_feature_count": int(len(cleaned)),
        "reach_count": int(len(reaches)),
        "node_count": int(len(nodes_gdf)),
        "junction_count": int(nodes_gdf["node_type"].astype(str).eq("junction").sum()),
        "confluence_candidate_count": int(nodes_gdf["node_type"].astype(str).eq("confluence_candidate").sum()),
        "adjacency_count": int(len(adjacency_df)),
        "missing_basin_assignment_before_fallback_count": int(pre_fallback_missing_basin.sum()),
        "missing_subbasin_assignment_before_fallback_count": int(pre_fallback_missing_subbasin.sum()),
        "missing_basin_assignment_count": int(reaches["issue_missing_basin_assignment"].sum()),
        "missing_subbasin_assignment_count": int(reaches["issue_missing_subbasin_assignment"].sum()),
        "fixed_basin_assignment_by_fallback_count": int(
            (~reaches["issue_missing_basin_assignment"] & reaches["basin_assignment_method"].astype(str).eq("nearest_boundary_fallback")).sum()
        ),
        "fixed_subbasin_assignment_by_fallback_count": int(
            (~reaches["issue_missing_subbasin_assignment"] & reaches["subbasin_assignment_method"].astype(str).eq("nearest_boundary_fallback")).sum()
        ),
        "diagnostic_endpoint_start_count": int(reaches["diagnostic_endpoint_start"].sum()),
        "diagnostic_endpoint_end_count": int(reaches["diagnostic_endpoint_end"].sum()),
        "crosses_multiple_basins_count": int(reaches["diagnostic_crosses_multiple_basins"].sum()),
        "crosses_multiple_subbasins_count": int(reaches["diagnostic_crosses_multiple_subbasins"].sum()),
        "self_loop_count": int(reaches["issue_self_loop"].sum()),
        "qa_row_count": int(len(qa_df)),
        "missing_assignment_row_count": int(len(missing_assignments_df)),
    }

    reaches = ensure_river_reach_columns(reaches)
    return (
        reaches.reset_index(drop=True),
        nodes_gdf.reset_index(drop=True),
        adjacency_df.reset_index(drop=True),
        qa_df,
        missing_assignments_df.reset_index(drop=True),
        missing_assignments_gdf.reset_index(drop=True),
        summary,
    )


def _print_summary(summary: dict[str, Any]) -> None:
    print("RIVER TOPOLOGY BUILD")
    print(f"Source features: {summary['source_feature_count']}")
    print(f"Reach count: {summary['reach_count']}")
    print(f"Node count: {summary['node_count']}")
    print(f"Junctions: {summary['junction_count']}")
    print(f"Confluence candidates: {summary['confluence_candidate_count']}")
    print(f"Adjacency rows: {summary['adjacency_count']}")
    print(f"Missing basin assignments before fallback: {summary['missing_basin_assignment_before_fallback_count']}")
    print(f"Missing sub-basin assignments before fallback: {summary['missing_subbasin_assignment_before_fallback_count']}")
    print(f"Missing basin assignments: {summary['missing_basin_assignment_count']}")
    print(f"Missing sub-basin assignments: {summary['missing_subbasin_assignment_count']}")
    print(f"Fixed basin assignments by fallback: {summary['fixed_basin_assignment_by_fallback_count']}")
    print(f"Fixed sub-basin assignments by fallback: {summary['fixed_subbasin_assignment_by_fallback_count']}")
    print(f"Self-loops: {summary['self_loop_count']}")
    print(f"Crosses multiple basins (diagnostic): {summary['crosses_multiple_basins_count']}")
    print(f"Crosses multiple sub-basins (diagnostic): {summary['crosses_multiple_subbasins_count']}")
    print(f"Endpoint starts (diagnostic): {summary['diagnostic_endpoint_start_count']}")
    print(f"Endpoint ends (diagnostic): {summary['diagnostic_endpoint_end_count']}")
    print(f"QA rows: {summary['qa_row_count']}")
    print(f"Missing-assignment rows: {summary['missing_assignment_row_count']}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build topology-ready river reach artifacts for IRT.")
    parser.add_argument(
        "--river-network",
        type=str,
        default=str(RIVER_NETWORK_PATH),
        help="Path to cleaned river_network.parquet. Default: IRT_DATA_DIR/river_network.parquet",
    )
    parser.add_argument(
        "--basins",
        type=str,
        default=str(BASINS_PATH),
        help="Path to canonical basins.geojson. Default: IRT_DATA_DIR/basins.geojson",
    )
    parser.add_argument(
        "--subbasins",
        type=str,
        default=str(SUBBASINS_PATH),
        help="Path to canonical subbasins.geojson. Default: IRT_DATA_DIR/subbasins.geojson",
    )
    parser.add_argument(
        "--out-reaches",
        type=str,
        default=str(RIVER_REACHES_PATH),
        help="Output path for river_reaches.parquet. Default: IRT_DATA_DIR/river_reaches.parquet",
    )
    parser.add_argument(
        "--out-nodes",
        type=str,
        default=str(RIVER_NODES_PATH),
        help="Output path for river_nodes.parquet. Default: IRT_DATA_DIR/river_nodes.parquet",
    )
    parser.add_argument(
        "--out-adjacency",
        type=str,
        default=str(RIVER_ADJACENCY_PATH),
        help="Output path for river_adjacency.parquet. Default: IRT_DATA_DIR/river_adjacency.parquet",
    )
    parser.add_argument(
        "--out-qa",
        type=str,
        default=str(RIVER_TOPOLOGY_QA_PATH),
        help="Output path for river_topology_qa.csv. Default: IRT_DATA_DIR/river_topology_qa.csv",
    )
    parser.add_argument(
        "--out-missing-assignments",
        type=str,
        default=str(RIVER_MISSING_ASSIGNMENTS_PATH),
        help="Output path for river_missing_assignments.csv. Default: IRT_DATA_DIR/river_missing_assignments.csv",
    )
    parser.add_argument(
        "--out-missing-assignments-geojson",
        type=str,
        default=str(RIVER_MISSING_ASSIGNMENTS_GEOJSON_PATH),
        help="Output path for river_missing_assignments.geojson. Default: IRT_DATA_DIR/river_missing_assignments.geojson",
    )
    parser.add_argument(
        "--snap-tolerance-m",
        type=float,
        default=250.0,
        help="Endpoint snapping tolerance in meters. Default: 250",
    )
    parser.add_argument(
        "--nearest-assignment-threshold-km",
        type=float,
        default=1.0,
        help="Maximum distance for nearest-boundary fallback assignment in km. Default: 1.0",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files if they already exist.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing outputs.")
    args = parser.parse_args(argv)

    river_network_path = Path(args.river_network).expanduser().resolve()
    basins_path = Path(args.basins).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()
    out_reaches = Path(args.out_reaches).expanduser().resolve()
    out_nodes = Path(args.out_nodes).expanduser().resolve()
    out_adjacency = Path(args.out_adjacency).expanduser().resolve()
    out_qa = Path(args.out_qa).expanduser().resolve()
    out_missing_assignments = Path(args.out_missing_assignments).expanduser().resolve()
    out_missing_assignments_geojson = Path(args.out_missing_assignments_geojson).expanduser().resolve()

    for in_path, label in (
        (river_network_path, "River network parquet"),
        (basins_path, "Basins GeoJSON"),
        (subbasins_path, "Sub-basins GeoJSON"),
    ):
        if not in_path.exists():
            raise FileNotFoundError(f"{label} not found: {in_path}")

    if float(args.snap_tolerance_m) <= 0:
        raise ValueError("--snap-tolerance-m must be > 0.")
    if float(args.nearest_assignment_threshold_km) < 0:
        raise ValueError("--nearest-assignment-threshold-km must be >= 0.")

    if not args.dry_run:
        for out_path in (
            out_reaches,
            out_nodes,
            out_adjacency,
            out_qa,
            out_missing_assignments,
            out_missing_assignments_geojson,
        ):
            if out_path.exists() and not args.overwrite:
                raise FileExistsError(f"Output already exists: {out_path}. Re-run with --overwrite.")

    cleaned_gdf = gpd.read_parquet(str(river_network_path))
    basin_gdf = gpd.read_file(str(basins_path))
    subbasin_gdf = gpd.read_file(str(subbasins_path))
    reaches, nodes, adjacency, qa_df, missing_assignments_df, missing_assignments_gdf, summary = build_river_topology_artifacts(
        cleaned_gdf,
        basin_gdf,
        subbasin_gdf,
        snap_tolerance_m=float(args.snap_tolerance_m),
        nearest_assignment_threshold_km=float(args.nearest_assignment_threshold_km),
    )
    _print_summary(summary)

    if args.dry_run:
        print("Dry run complete. No files written.")
        return 0

    for path in (
        out_reaches,
        out_nodes,
        out_adjacency,
        out_qa,
        out_missing_assignments,
        out_missing_assignments_geojson,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)

    reaches.to_parquet(out_reaches, index=False)
    nodes.to_parquet(out_nodes, index=False)
    adjacency.to_parquet(out_adjacency, index=False)
    qa_df.to_csv(out_qa, index=False)
    missing_assignments_df.to_csv(out_missing_assignments, index=False)
    if missing_assignments_gdf.empty:
        out_missing_assignments_geojson.write_text(
            json.dumps({"type": "FeatureCollection", "features": []}, indent=2),
            encoding="utf-8",
        )
    else:
        missing_assignments_gdf.to_file(out_missing_assignments_geojson, driver="GeoJSON")

    print(f"Wrote river reaches: {out_reaches}")
    print(f"Wrote river nodes: {out_nodes}")
    print(f"Wrote river adjacency: {out_adjacency}")
    print(f"Wrote topology QA CSV: {out_qa}")
    print(f"Wrote missing assignments CSV: {out_missing_assignments}")
    print(f"Wrote missing assignments GeoJSON: {out_missing_assignments_geojson}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
