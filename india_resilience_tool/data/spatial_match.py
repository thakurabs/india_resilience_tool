"""
Spatial click / point matching helpers (Streamlit-free).

This module centralizes the logic used to:
- extract clicked feature and click coordinates from a Streamlit-Folium return dict
- match a clicked feature name or a clicked point to a row in the merged GeoDataFrame

Contracts:
- No Streamlit imports.
- Prefer robust matching and safe fallbacks; return None when matching fails.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional, Tuple

from shapely.geometry import Point


def extract_clicked_feature(returned: Mapping[str, Any]) -> Optional[dict]:
    """Extract a clicked GeoJSON feature from a streamlit-folium return payload."""
    if not isinstance(returned, Mapping):
        return None
    for k in ("last_active_drawing", "last_object_clicked", "last_object"):
        v = returned.get(k)
        if isinstance(v, dict):
            return v
    return None


def extract_click_coords(returned: Mapping[str, Any]) -> Optional[Tuple[float, float]]:
    """
    Extract click coordinates (lat, lon) from a streamlit-folium return payload.
    """
    if not isinstance(returned, Mapping):
        return None

    for k in ("last_clicked", "latlng", "last_latlng"):
        val = returned.get(k)
        if isinstance(val, dict) and ("lat" in val or "lng" in val):
            lat = val.get("lat") or val.get("latitude") or val.get("y")
            lng = val.get("lng") or val.get("longitude") or val.get("x")
            if lat is not None and lng is not None:
                try:
                    return (float(lat), float(lng))
                except Exception:
                    return None
        if isinstance(val, (list, tuple)) and len(val) >= 2:
            try:
                return (float(val[0]), float(val[1]))
            except Exception:
                continue
    return None


def extract_name_from_feature(feat: Any) -> Optional[str]:
    """
    Heuristic name extraction from a GeoJSON feature.

    This preserves legacy behavior (district-centric) used by the dashboard.
    """
    if not isinstance(feat, dict):
        return None
    props = feat.get("properties") or feat
    for key in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
        if isinstance(props, dict) and props.get(key):
            return str(props.get(key))
    if isinstance(props, dict):
        for k, v in props.items():
            if isinstance(v, str) and len(v) > 2 and "shape" not in str(k).lower():
                return v
    return None


def _match_row_by_district_name(merged, name: str):
    if merged is None or getattr(merged, "empty", True):
        return None
    if not name:
        return None
    try:
        series = merged["district_name"].astype(str)
    except Exception:
        return None

    name_l = str(name).lower()
    mask = series.str.lower() == name_l
    if mask.any():
        return merged[mask].iloc[0:1]

    mask2 = series.str.lower().str.contains(name_l)
    if mask2.any():
        return merged[mask2].iloc[0:1]
    return None


def _match_row_by_point(merged, lat: float, lon: float):
    if merged is None or getattr(merged, "empty", True):
        return None
    try:
        pt = Point(float(lon), float(lat))
        contains_mask = merged.geometry.contains(pt)
        if contains_mask.any():
            return merged[contains_mask].iloc[0:1]

        centroids = merged.geometry.centroid
        dists = centroids.distance(pt)
        idx = dists.idxmin()
        return merged.loc[[idx]]
    except Exception:
        return None


def resolve_matched_row(
    *,
    merged,
    level: str,
    clicked_feature: Optional[dict],
    click_coords: Optional[Tuple[float, float]],
    selected_district: str,
    selected_block: str,
) -> Any:
    """
    Resolve the 'matched row' (single-row GeoDataFrame slice) for the details panel.

    Matching order (legacy-preserving):
      1) clicked feature name -> district_name match
      2) click point -> polygon contains / centroid nearest
      3) selected_district (+ selected_block in block mode)
    """
    clicked_name = extract_name_from_feature(clicked_feature) if clicked_feature else None

    matched_row = None
    if clicked_name:
        matched_row = _match_row_by_district_name(merged, str(clicked_name))

    if (matched_row is None or getattr(matched_row, "empty", True)) and click_coords is not None:
        lat, lon = click_coords
        matched_row = _match_row_by_point(merged, float(lat), float(lon))

    if (matched_row is None or getattr(matched_row, "empty", True)) and selected_district != "All":
        sel_district_norm = str(selected_district).split(",")[0].strip().lower()
        try:
            district_series = merged["district_name"].astype(str).str.strip().str.lower()
            mask = district_series == sel_district_norm
            if (not mask.any()) and sel_district_norm:
                mask = district_series.str.contains(re.escape(sel_district_norm), na=False)

            if str(level).strip().lower() == "block" and selected_block != "All":
                sel_block_norm = str(selected_block).split(",")[0].strip().lower()
                if "block_name" in merged.columns:
                    block_series = merged["block_name"].astype(str).str.strip().str.lower()
                    block_mask = block_series == sel_block_norm
                    if (not block_mask.any()) and sel_block_norm:
                        block_mask = block_series.str.contains(re.escape(sel_block_norm), na=False)
                    mask = mask & block_mask

            if mask.any():
                matched_row = merged[mask].iloc[0:1]
        except Exception:
            pass

    return matched_row

