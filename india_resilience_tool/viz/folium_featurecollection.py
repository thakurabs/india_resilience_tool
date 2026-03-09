"""
Folium FeatureCollection helpers (Streamlit-free).

These helpers patch a cached (geometry-only) GeoJSON FeatureCollection with
per-run properties (fillColor + metric values + tooltip strings) and build a
Folium tooltip with stable field ordering.

Contract:
  - No Streamlit imports.
  - Must not change ranking/baseline methodology; it only moves data into
    feature properties for rendering.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

import pandas as pd
import folium


def ensure_geojson_by_state_has_all(geojson_by_state: Mapping[str, dict]) -> dict[str, dict]:
    """
    Ensure `geojson_by_state["all"]` exists.

    Some cached implementations return only per-state FeatureCollections. The
    legacy dashboard expects an "all" fallback.
    """
    if "all" in geojson_by_state:
        return dict(geojson_by_state)

    all_features: list[dict] = []
    for k in sorted(geojson_by_state.keys()):
        fc = geojson_by_state.get(k) or {}
        all_features.extend(fc.get("features", []) or [])

    out = dict(geojson_by_state)
    out["all"] = {"type": "FeatureCollection", "features": all_features}
    return out


def filter_fc_by_district(
    fc: dict,
    *,
    selected_district: str,
    selected_basin: str = "All",
    level: str = "district",
    alias_fn: Callable[[str], str],
) -> dict:
    """Filter a feature collection to the active district or basin selection."""
    level_norm = str(level).strip().lower()
    if level_norm == "basin" and selected_basin and selected_basin != "All":
        basin_key = alias_fn(selected_basin)
        features = [
            f
            for f in fc.get("features", [])
            if alias_fn(((f.get("properties") or {}).get("basin_name", ""))) == basin_key
        ]
        fc = dict(fc)
        fc["features"] = features
        return fc

    if level_norm == "sub_basin" and selected_basin and selected_basin != "All":
        basin_key = alias_fn(selected_basin)
        features = [
            f
            for f in fc.get("features", [])
            if alias_fn(((f.get("properties") or {}).get("basin_name", ""))) == basin_key
        ]
        fc = dict(fc)
        fc["features"] = features
        return fc

    if not selected_district or selected_district == "All":
        return fc

    dist_key = alias_fn(selected_district)
    features = [
        f
        for f in fc.get("features", [])
        if alias_fn(((f.get("properties") or {}).get("district_name", ""))) == dist_key
    ]
    fc = dict(fc)
    fc["features"] = features
    return fc


def _feature_key_for_row(
    row: Mapping[str, Any],
    *,
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
) -> str:
    if level == "block":
        return (
            f"{alias_fn(row.get('state_name', ''))}|"
            f"{alias_fn(row.get('district_name', ''))}|"
            f"{alias_fn(row.get('block_name', ''))}"
        )
    if level == "sub_basin":
        return alias_fn(row.get("subbasin_id", ""))
    if level == "basin":
        return alias_fn(row.get("basin_id", ""))
    return alias_fn(row.get("district_name", ""))


def build_props_map_from_gdf(
    prop_gdf: pd.DataFrame,
    *,
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
    metric_col: str,
    map_value_col: str,
) -> Tuple[dict[str, dict], list[str], list[str]]:
    """
    Build a mapping of feature_key -> property dict to patch into GeoJSON features.

    Returns:
      (props_map, value_cols, text_cols)
    """
    prop_work = prop_gdf.copy()

    is_block_level = str(level).strip().lower() == "block"

    if is_block_level and "block_name" not in prop_work.columns and "block" in prop_work.columns:
        prop_work["block_name"] = prop_work["block"]
    if str(level).strip().lower() == "sub_basin" and "subbasin_name" not in prop_work.columns and "subbasin" in prop_work.columns:
        prop_work["subbasin_name"] = prop_work["subbasin"]

    if feature_key_col not in prop_work.columns:
        prop_work[feature_key_col] = prop_work.apply(
            lambda r: _feature_key_for_row(
                r,
                level=str(level).strip().lower(),
                alias_fn=alias_fn,
                feature_key_col=feature_key_col,
            ),
            axis=1,
        )

    value_cols: list[str] = []
    for c in (
        metric_col,
        map_value_col,
        "_baseline_value",
        "_delta_abs",
        "_delta_pct",
        "_rank_in_state",
        "_percentile_state",
    ):
        if c and (c not in value_cols) and (c in prop_work.columns):
            value_cols.append(str(c))

    text_cols: list[str] = []
    for c in ("_risk_class", "_tooltip_value", "_tooltip_baseline", "_tooltip_delta", "_tooltip_rank"):
        if c in prop_work.columns:
            text_cols.append(c)

    keep_cols: list[str] = []
    if str(level).strip().lower() == "sub_basin":
        for c in ("subbasin_name", "subbasin_id", "subbasin_code", "basin_name", "basin_id"):
            if c in prop_work.columns:
                keep_cols.append(c)
    elif str(level).strip().lower() == "basin":
        for c in ("basin_name", "basin_id"):
            if c in prop_work.columns:
                keep_cols.append(c)
    if is_block_level and "block_name" in prop_work.columns:
        keep_cols.append("block_name")
    if "district_name" in prop_work.columns:
        keep_cols.append("district_name")
    if "state_name" in prop_work.columns:
        keep_cols.append("state_name")
    keep_cols.append(feature_key_col)
    if "fillColor" in prop_work.columns:
        keep_cols.append("fillColor")
    keep_cols.extend(value_cols)
    keep_cols.extend(text_cols)

    prop_work = prop_work[keep_cols].copy()

    props_map: dict[str, dict] = {}
    for _, r in prop_work.iterrows():
        k = r.get(feature_key_col)
        if not isinstance(k, str) or not k:
            continue

        upd: dict = {
            "district_name": r.get("district_name"),
            "state_name": r.get("state_name") if "state_name" in prop_work.columns else None,
        }
        if str(level).strip().lower() == "sub_basin":
            upd["subbasin_name"] = r.get("subbasin_name")
            upd["subbasin_id"] = r.get("subbasin_id")
            upd["subbasin_code"] = r.get("subbasin_code")
            upd["basin_name"] = r.get("basin_name")
            upd["basin_id"] = r.get("basin_id")
        elif str(level).strip().lower() == "basin":
            upd["basin_name"] = r.get("basin_name")
            upd["basin_id"] = r.get("basin_id")
        if is_block_level and "block_name" in prop_work.columns:
            upd["block_name"] = r.get("block_name")

        fill = r.get("fillColor")
        upd["fillColor"] = fill if isinstance(fill, str) and fill else "#cccccc"

        for c in value_cols:
            v = r.get(c)
            upd[c] = None if pd.isna(v) else v

        for c in text_cols:
            v = r.get(c)
            upd[c] = None if pd.isna(v) else v

        props_map[k] = upd

    return props_map, value_cols, text_cols


def patch_fc_properties(
    fc: dict,
    *,
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
    props_map: Mapping[str, Mapping[str, Any]],
    ensure_text_fields: Sequence[str] = (
        "_risk_class",
        "_tooltip_value",
        "_tooltip_baseline",
        "_tooltip_delta",
        "_tooltip_rank",
    ),
) -> dict:
    """
    Patch feature properties (fillColor + value columns + tooltip fields) in-place.

    Returns the same dict for convenience.
    """
    is_block_level = str(level).strip().lower() == "block"

    for feat in fc.get("features", []):
        props = feat.get("properties") or {}

        k = props.get(feature_key_col)
        if not isinstance(k, str) or not k:
            if is_block_level:
                props["block_name"] = (
                    props.get("block_name")
                    or props.get("block")
                    or props.get("adm3_name")
                    or props.get("name")
                )
                props["district_name"] = (
                    props.get("district_name")
                    or props.get("district")
                    or props.get("adm2_name")
                    or props.get("shapeName_2")
                    or props.get("shapeName_1")
                )
                props["state_name"] = (
                    props.get("state_name")
                    or props.get("state")
                    or props.get("adm1_name")
                    or props.get("shapeName_0")
                    or props.get("shapeGroup")
                )
                k = (
                    f"{alias_fn(props.get('state_name', ''))}|"
                    f"{alias_fn(props.get('district_name', ''))}|"
                    f"{alias_fn(props.get('block_name', ''))}"
                )
            elif str(level).strip().lower() == "sub_basin":
                props["subbasin_name"] = props.get("subbasin_name") or props.get("name")
                props["subbasin_id"] = props.get("subbasin_id")
                props["basin_name"] = props.get("basin_name")
                props["basin_id"] = props.get("basin_id")
                k = alias_fn(props.get("subbasin_id", ""))
            elif str(level).strip().lower() == "basin":
                props["basin_name"] = props.get("basin_name") or props.get("name")
                props["basin_id"] = props.get("basin_id")
                k = alias_fn(props.get("basin_id", ""))
            else:
                k = alias_fn(props.get("district_name", ""))

            props[feature_key_col] = k

        upd = props_map.get(str(k))
        if upd:
            props.update(dict(upd))
        else:
            props.setdefault("fillColor", "#cccccc")

        for c in ensure_text_fields:
            props.setdefault(c, None)

        feat["properties"] = props

    return fc


def build_geojson_tooltip(
    *,
    level: str,
    map_mode: str,
    has_baseline: bool,
    rank_scope_label: str,
) -> folium.features.GeoJsonTooltip:
    """
    Build a stable GeoJsonTooltip for the patched FeatureCollection.
    """
    is_block_level = str(level).strip().lower() == "block"
    main_label = "Δ vs 1990–2010" if map_mode == "Change from 1990-2010 baseline" else "Value"

    if is_block_level:
        tooltip_fields = ["block_name", "district_name", "state_name", "_tooltip_value"]
        tooltip_aliases = ["Block", "District", "State", main_label]
    elif str(level).strip().lower() == "sub_basin":
        tooltip_fields = ["subbasin_name", "basin_name", "_tooltip_value"]
        tooltip_aliases = ["Sub-basin", "Basin", main_label]
    elif str(level).strip().lower() == "basin":
        tooltip_fields = ["basin_name", "_tooltip_value"]
        tooltip_aliases = ["Basin", main_label]
    else:
        tooltip_fields = ["district_name", "state_name", "_tooltip_value"]
        tooltip_aliases = ["District", "State", main_label]

    if has_baseline:
        tooltip_fields += ["_tooltip_baseline", "_tooltip_delta"]
        tooltip_aliases += ["Baseline (1990–2010)", "Δ vs baseline"]

    tooltip_fields += ["_risk_class", "_tooltip_rank"]
    tooltip_aliases += ["Risk class", f"Rank in {rank_scope_label}"]

    return folium.features.GeoJsonTooltip(
        fields=tooltip_fields,
        aliases=tooltip_aliases,
        localize=True,
        sticky=True,
    )
