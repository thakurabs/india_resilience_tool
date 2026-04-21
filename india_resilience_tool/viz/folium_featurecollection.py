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

import hashlib
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

import folium
import pandas as pd


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
    selected_subbasin: str = "All",
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
    if level_norm == "sub_basin" and selected_subbasin and selected_subbasin != "All":
        subbasin_key = alias_fn(selected_subbasin)
        features = [
            f
            for f in fc.get("features", [])
            if alias_fn(((f.get("properties") or {}).get("subbasin_name", ""))) == subbasin_key
        ]
        fc = dict(fc)
        fc["features"] = features
        return fc
    if level_norm in {"basin", "sub_basin"}:
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


def filter_fc_by_feature_keys(
    fc: dict,
    *,
    feature_keys: Sequence[str],
    level: str,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> dict:
    """
    Filter a feature collection to a small explicit set of feature identifiers.

    For district overlays, keys may be either:
    - `state::district`
    - `district`

    For block overlays, keys may be:
    - `state::district::block`
    - `block`

    For hydro overlays, the cached `__key` field is used directly.
    """
    allowed = [str(v).strip() for v in feature_keys if str(v).strip()]
    out = dict(fc)
    if not allowed:
        out["features"] = []
        return out

    level_norm = str(level).strip().lower()
    features = list(fc.get("features", []) or [])

    if level_norm == "district":
        allowed_pairs: set[tuple[str, str]] = set()
        allowed_districts: set[str] = set()
        for item in allowed:
            if "::" in item:
                state_part, district_part = item.split("::", 1)
                allowed_pairs.add((alias_fn(state_part), alias_fn(district_part)))
            else:
                allowed_districts.add(alias_fn(item))

        kept = []
        for feat in features:
            props = feat.get("properties") or {}
            state_key = alias_fn(props.get("state_name", ""))
            district_key = alias_fn(props.get("district_name", ""))
            if (state_key, district_key) in allowed_pairs or district_key in allowed_districts:
                kept.append(feat)
        out["features"] = kept
        return out

    if level_norm == "block":
        allowed_triplets: set[tuple[str, str, str]] = set()
        allowed_blocks: set[str] = set()
        for item in allowed:
            if item.count("::") >= 2:
                state_part, district_part, block_part = item.split("::", 2)
                allowed_triplets.add(
                    (alias_fn(state_part), alias_fn(district_part), alias_fn(block_part))
                )
            else:
                allowed_blocks.add(alias_fn(item))

        kept = []
        for feat in features:
            props = feat.get("properties") or {}
            state_key = alias_fn(props.get("state_name", ""))
            district_key = alias_fn(props.get("district_name", ""))
            block_key = alias_fn(props.get("block_name", ""))
            composite = (state_key, district_key, block_key)
            if composite in allowed_triplets or block_key in allowed_blocks:
                kept.append(feat)
        out["features"] = kept
        return out

    allowed_keys = {alias_fn(v) for v in allowed}
    kept = []
    for feat in features:
        props = feat.get("properties") or {}
        feature_key = props.get(key_col)
        if not isinstance(feature_key, str) or not feature_key:
            if level_norm == "sub_basin":
                feature_key = alias_fn(props.get("subbasin_id", ""))
            elif level_norm == "basin":
                feature_key = alias_fn(props.get("basin_id", ""))
            else:
                feature_key = alias_fn(props.get("district_name", ""))
        if alias_fn(feature_key) in allowed_keys:
            kept.append(feat)

    out["features"] = kept
    return out


def clone_featurecollection_for_patch(fc: Mapping[str, Any]) -> dict[str, Any]:
    """
    Clone a FeatureCollection cheaply for property patching.

    Geometry objects are reused as-is; only the top-level dict, feature list,
    feature dicts, and properties dicts are cloned.
    """
    out = dict(fc)
    features_out: list[dict[str, Any]] = []
    for feature in fc.get("features", []) or []:
        feature_out = dict(feature)
        props = feature.get("properties")
        feature_out["properties"] = dict(props) if isinstance(props, Mapping) else {}
        features_out.append(feature_out)
    out["features"] = features_out
    return out


def props_map_signature(props_map: Mapping[str, Mapping[str, Any]]) -> str:
    """Return a stable signature for a patched-properties payload."""
    hasher = hashlib.sha1()
    for feature_key in sorted(str(k) for k in props_map.keys()):
        hasher.update(feature_key.encode("utf-8"))
        props = props_map.get(feature_key) or {}
        for prop_key, prop_value in sorted(props.items(), key=lambda item: str(item[0])):
            hasher.update(str(prop_key).encode("utf-8"))
            if hasattr(prop_value, "item"):
                prop_value = prop_value.item()
            hasher.update(repr(prop_value).encode("utf-8"))
    return hasher.hexdigest()


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return a string series with nulls normalized to empty strings."""
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="object")
    return frame[column].where(frame[column].notna(), "").astype(str)


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
    level_norm = str(level).strip().lower()

    is_block_level = level_norm == "block"

    if is_block_level and "block_name" not in prop_work.columns and "block" in prop_work.columns:
        prop_work["block_name"] = prop_work["block"]
    if level_norm == "sub_basin" and "subbasin_name" not in prop_work.columns and "subbasin" in prop_work.columns:
        prop_work["subbasin_name"] = prop_work["subbasin"]

    if feature_key_col not in prop_work.columns:
        if is_block_level:
            state_key = _string_series(prop_work, "state_name").map(alias_fn)
            district_key = _string_series(prop_work, "district_name").map(alias_fn)
            block_key = _string_series(prop_work, "block_name").map(alias_fn)
            prop_work[feature_key_col] = state_key.str.cat(district_key, sep="|").str.cat(block_key, sep="|")
        elif level_norm == "sub_basin":
            prop_work[feature_key_col] = _string_series(prop_work, "subbasin_id").map(alias_fn)
        elif level_norm == "basin":
            prop_work[feature_key_col] = _string_series(prop_work, "basin_id").map(alias_fn)
        else:
            prop_work[feature_key_col] = _string_series(prop_work, "district_name").map(alias_fn)

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
    if level_norm == "sub_basin":
        for c in ("subbasin_name", "subbasin_id", "subbasin_code", "basin_name", "basin_id"):
            if c in prop_work.columns:
                keep_cols.append(c)
    elif level_norm == "basin":
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

    keep_cols = list(dict.fromkeys(keep_cols))
    prop_work = prop_work[keep_cols].copy()
    prop_work = prop_work.where(prop_work.notna(), None)

    props_map: dict[str, dict] = {}
    for record in prop_work.to_dict("records"):
        k = record.get(feature_key_col)
        if not isinstance(k, str) or not k:
            continue

        upd: dict = {
            "district_name": record.get("district_name"),
            "state_name": record.get("state_name") if "state_name" in prop_work.columns else None,
        }
        if level_norm == "sub_basin":
            upd["subbasin_name"] = record.get("subbasin_name")
            upd["subbasin_id"] = record.get("subbasin_id")
            upd["subbasin_code"] = record.get("subbasin_code")
            upd["basin_name"] = record.get("basin_name")
            upd["basin_id"] = record.get("basin_id")
        elif level_norm == "basin":
            upd["basin_name"] = record.get("basin_name")
            upd["basin_id"] = record.get("basin_id")
        if is_block_level and "block_name" in prop_work.columns:
            upd["block_name"] = record.get("block_name")

        fill = record.get("fillColor")
        upd["fillColor"] = fill if isinstance(fill, str) and fill else "#cccccc"

        for c in value_cols:
            v = record.get(c)
            upd[c] = None if pd.isna(v) else v

        for c in text_cols:
            v = record.get(c)
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
    metric_slug: str | None = None,
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

    metric_slug_norm = str(metric_slug or "").strip().lower()
    if metric_slug_norm != "jrc_flood_depth_index_rp100":
        tooltip_fields.append("_risk_class")
        tooltip_aliases.append("Risk class")
    tooltip_fields.append("_tooltip_rank")
    tooltip_aliases.append(f"Rank in {rank_scope_label}")

    return folium.features.GeoJsonTooltip(
        fields=tooltip_fields,
        aliases=tooltip_aliases,
        localize=True,
        sticky=True,
    )
