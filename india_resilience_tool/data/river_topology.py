"""
Streamlit-free loaders and summaries for topology-ready river artifacts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional, Union

import geopandas as gpd
import pandas as pd


PathLike = Union[str, Path]
HydroRiverLevel = Literal["basin", "sub_basin"]

_REACH_REQUIRED = [
    "reach_id",
    "river_feature_id",
    "river_name_clean",
    "basin_id",
    "basin_name",
    "subbasin_id",
    "subbasin_name",
    "start_node_id",
    "end_node_id",
    "reach_length_km",
    "geometry",
]


def ensure_river_reach_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Validate the canonical river reach artifact."""
    missing = [col for col in _REACH_REQUIRED if col not in gdf.columns]
    if missing:
        raise ValueError(f"River reaches artifact is missing required columns: {missing}.")

    out = gdf.copy()
    for col in (
        "reach_id",
        "river_feature_id",
        "river_name_clean",
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_name",
        "start_node_id",
        "end_node_id",
    ):
        out[col] = out[col].fillna("").astype(str).str.strip()

    out["reach_length_km"] = pd.to_numeric(out["reach_length_km"], errors="coerce")

    if out["reach_id"].eq("").any():
        raise ValueError("River reaches artifact contains blank reach_id values.")
    if out["reach_id"].duplicated().any():
        raise ValueError("River reaches artifact contains duplicate reach_id values.")
    if out["reach_length_km"].isna().any():
        raise ValueError("River reaches artifact contains invalid reach_length_km values.")

    return out


def load_river_reaches(path: PathLike) -> gpd.GeoDataFrame:
    """Load the canonical river reach artifact."""
    gdf = gpd.read_parquet(str(path))
    return ensure_river_reach_columns(gdf)


def _top_named_rivers(df: pd.DataFrame, *, limit: int = 3) -> list[dict[str, Any]]:
    work = df.copy()
    work["river_name_clean"] = work["river_name_clean"].fillna("").astype(str).str.strip()
    work.loc[work["river_name_clean"].eq(""), "river_name_clean"] = "Unnamed river"
    grouped = (
        work.groupby("river_name_clean", dropna=False)["reach_length_km"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    return [
        {
            "river_name": str(row["river_name_clean"]),
            "total_length_km": float(row["reach_length_km"]),
        }
        for _, row in grouped.head(limit).iterrows()
    ]


def build_hydro_river_summary(
    reaches_gdf: gpd.GeoDataFrame,
    *,
    level: HydroRiverLevel,
    basin_id: str,
    subbasin_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Build a compact hydro-facing river summary from canonical reaches."""
    reaches = ensure_river_reach_columns(reaches_gdf)
    basin_key = str(basin_id or "").strip()
    if not basin_key:
        return None

    work = reaches.loc[reaches["basin_id"].astype(str).str.strip() == basin_key].copy()
    if level == "sub_basin":
        subbasin_key = str(subbasin_id or "").strip()
        if not subbasin_key:
            return None
        work = work.loc[work["subbasin_id"].astype(str).str.strip() == subbasin_key].copy()

    if work.empty:
        return None

    total_length_km = float(work["reach_length_km"].sum())
    fallback_segment_count = 0
    if "basin_assignment_method" in work.columns:
        fallback_segment_count = int(
            work["basin_assignment_method"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("nearest_boundary_fallback")
            .sum()
        )
    if level == "sub_basin" and "subbasin_assignment_method" in work.columns:
        fallback_segment_count = int(
            work["subbasin_assignment_method"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("nearest_boundary_fallback")
            .sum()
        )
    named_count = int(
        work["river_name_clean"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )
    return {
        "level": level,
        "reach_count": int(len(work)),
        "river_feature_count": int(work["river_feature_id"].astype(str).nunique()),
        "named_river_count": named_count,
        "total_length_km": total_length_km,
        "fallback_segment_count": fallback_segment_count,
        "top_named_rivers": _top_named_rivers(work, limit=3),
    }
