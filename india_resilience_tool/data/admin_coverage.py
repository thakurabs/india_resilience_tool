"""Pure admin/hydro feature-key and render-coverage helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

import pandas as pd


_NULL_KEY_PARTS = {"", "<na>", "nan", "none", "nat"}


@dataclass(frozen=True)
class CoverageDiagnostics:
    """Authoritative render-time feature coverage diagnostics."""

    total_feature_keys: int
    matched_feature_keys: int
    missing_master_row_keys: tuple[str, ...]
    null_value_keys: tuple[str, ...]
    broken_join_keys: tuple[str, ...]
    coverage_pct: float


@dataclass(frozen=True)
class AdminMasterSourceAudit:
    """Cached completeness audit used only for admin source-family selection."""

    distinct_state_keys: tuple[str, ...]
    valued_state_keys: tuple[str, ...]
    duplicate_identity_keys: tuple[str, ...]
    malformed_identity_row_count: int


def _string_series(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    """Return the first available string-like column, preserving missing values."""
    for column in columns:
        if column in frame.columns:
            out = frame[column].astype("string").str.strip()
            lower = out.str.lower()
            return out.where(out.notna() & ~lower.isin(_NULL_KEY_PARTS))
    return pd.Series(pd.NA, index=frame.index, dtype="string")


def _normalize_series(series: pd.Series, *, alias_fn: Callable[[str], str]) -> pd.Series:
    """Normalize one string series while preserving missing values."""
    out = series.map(lambda value: alias_fn(str(value)) if pd.notna(value) else pd.NA)
    out = out.astype("string").str.strip()
    lower = out.str.lower()
    return out.where(out.notna() & ~lower.isin(_NULL_KEY_PARTS))


def build_feature_key_series(
    frame: pd.DataFrame,
    *,
    level: str,
    alias_fn: Callable[[str], str],
) -> pd.Series:
    """Return canonical feature keys for one admin or hydro frame."""
    level_norm = str(level or "district").strip().lower()

    if level_norm == "block":
        state_key = _normalize_series(_string_series(frame, ("state_name", "state")), alias_fn=alias_fn)
        district_key = _normalize_series(_string_series(frame, ("district_name", "district")), alias_fn=alias_fn)
        block_key = _normalize_series(_string_series(frame, ("block_name", "block")), alias_fn=alias_fn)
        valid = state_key.notna() & district_key.notna() & block_key.notna()
        keys = state_key.str.cat(district_key, sep="|").str.cat(block_key, sep="|")
        return keys.where(valid, pd.NA)

    if level_norm == "sub_basin":
        return _normalize_series(_string_series(frame, ("subbasin_id",)), alias_fn=alias_fn)

    if level_norm == "basin":
        return _normalize_series(_string_series(frame, ("basin_id",)), alias_fn=alias_fn)

    state_key = _normalize_series(_string_series(frame, ("state_name", "state")), alias_fn=alias_fn)
    district_key = _normalize_series(_string_series(frame, ("district_name", "district")), alias_fn=alias_fn)
    valid = state_key.notna() & district_key.notna()
    keys = state_key.str.cat(district_key, sep="|")
    return keys.where(valid, pd.NA)


def build_feature_key_from_properties(
    props: Mapping[str, Any],
    *,
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
) -> Optional[str]:
    """Return one canonical feature key from GeoJSON properties."""
    level_norm = str(level or "district").strip().lower()

    def _normalize_value(*candidates: str) -> Optional[str]:
        for candidate in candidates:
            value = props.get(candidate)
            if value is None:
                continue
            normalized = alias_fn(str(value))
            if normalized and normalized not in _NULL_KEY_PARTS:
                return normalized
        return None

    if level_norm == "block":
        state_key = _normalize_value("state_name", "state", "adm1_name", "shapeName_0", "shapeGroup")
        district_key = _normalize_value("district_name", "district", "adm2_name", "shapeName_2", "shapeName_1")
        block_key = _normalize_value("block_name", "block", "adm3_name", "subdistrict_name", "name")
        if state_key and district_key and block_key:
            return f"{state_key}|{district_key}|{block_key}"
    elif level_norm == "sub_basin":
        subbasin_key = _normalize_value("subbasin_id")
        if subbasin_key:
            return subbasin_key
    elif level_norm == "basin":
        basin_key = _normalize_value("basin_id")
        if basin_key:
            return basin_key
    else:
        state_key = _normalize_value("state_name", "state", "adm1_name", "shapeName_0", "shapeGroup")
        district_key = _normalize_value("district_name", "district")
        if state_key and district_key:
            return f"{state_key}|{district_key}"

    explicit_key = props.get(feature_key_col)
    if explicit_key is None:
        return None
    normalized = alias_fn(str(explicit_key))
    return normalized if normalized and normalized not in _NULL_KEY_PARTS else None


def feature_keys_from_featurecollection(
    feature_collection: Mapping[str, Any],
    *,
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
) -> tuple[str, ...]:
    """Return ordered unique feature keys from the exact rendered FeatureCollection."""
    ordered_keys: list[str] = []
    seen: set[str] = set()
    for feature in feature_collection.get("features", []) or []:
        props = feature.get("properties") or {}
        key = build_feature_key_from_properties(
            props,
            level=level,
            alias_fn=alias_fn,
            feature_key_col=feature_key_col,
        )
        if not key or key in seen:
            continue
        seen.add(key)
        ordered_keys.append(key)
    return tuple(ordered_keys)


def display_value_present_mask(
    frame: pd.DataFrame,
    *,
    map_value_col: str,
    metric_col: str,
    baseline_col: Optional[str],
) -> pd.Series:
    """Return whether each master row can produce a non-null rendered value."""
    if map_value_col in frame.columns:
        return frame[map_value_col].notna()

    if map_value_col in {"_current_value", metric_col}:
        return frame.get(metric_col, pd.Series(pd.NA, index=frame.index)).notna()

    if map_value_col == "_baseline_value":
        return frame.get(str(baseline_col or ""), pd.Series(pd.NA, index=frame.index)).notna()

    if map_value_col == "_delta_abs":
        if not baseline_col:
            return pd.Series(False, index=frame.index, dtype=bool)
        return frame.get(metric_col, pd.Series(pd.NA, index=frame.index)).notna() & frame.get(
            baseline_col,
            pd.Series(pd.NA, index=frame.index),
        ).notna()

    if map_value_col == "_delta_pct":
        if not baseline_col:
            return pd.Series(False, index=frame.index, dtype=bool)
        baseline = pd.to_numeric(frame.get(baseline_col), errors="coerce")
        current = frame.get(metric_col, pd.Series(pd.NA, index=frame.index))
        return current.notna() & baseline.notna() & (baseline != 0)

    return pd.Series(False, index=frame.index, dtype=bool)


def compute_coverage_diagnostics(
    *,
    feature_collection: Mapping[str, Any],
    level: str,
    alias_fn: Callable[[str], str],
    feature_key_col: str,
    props_map: Mapping[str, Mapping[str, Any]],
    master_df: pd.DataFrame,
    map_value_col: str,
    metric_col: str,
    baseline_col: Optional[str],
) -> CoverageDiagnostics:
    """Classify rendered features into matched, missing-row, null-value, and broken-join buckets."""
    feature_keys = feature_keys_from_featurecollection(
        feature_collection,
        level=level,
        alias_fn=alias_fn,
        feature_key_col=feature_key_col,
    )
    feature_key_set = set(feature_keys)
    props_key_set = {
        str(key).strip()
        for key in props_map.keys()
        if isinstance(key, str) and str(key).strip()
    }
    matched_feature_keys = tuple(key for key in feature_keys if key in props_key_set)

    if master_df.empty:
        missing_master_row_keys = tuple(sorted(feature_key_set))
        return CoverageDiagnostics(
            total_feature_keys=len(feature_keys),
            matched_feature_keys=len(matched_feature_keys),
            missing_master_row_keys=missing_master_row_keys,
            null_value_keys=(),
            broken_join_keys=(),
            coverage_pct=(100.0 * len(matched_feature_keys) / len(feature_keys)) if feature_keys else 100.0,
        )

    master_keys = build_feature_key_series(master_df, level=level, alias_fn=alias_fn)
    master_key_set = set(master_keys.dropna().astype(str).tolist())
    value_mask = display_value_present_mask(
        master_df,
        map_value_col=map_value_col,
        metric_col=metric_col,
        baseline_col=baseline_col,
    )
    master_value_key_set = set(master_keys[value_mask].dropna().astype(str).tolist())

    missing_master_row_keys = tuple(sorted(key for key in feature_key_set if key not in master_key_set))
    null_value_keys = tuple(sorted(key for key in feature_key_set if key in master_key_set and key not in master_value_key_set))
    broken_join_keys = tuple(sorted(key for key in feature_key_set if key in master_value_key_set and key not in props_key_set))

    return CoverageDiagnostics(
        total_feature_keys=len(feature_keys),
        matched_feature_keys=len(matched_feature_keys),
        missing_master_row_keys=missing_master_row_keys,
        null_value_keys=null_value_keys,
        broken_join_keys=broken_join_keys,
        coverage_pct=(100.0 * len(matched_feature_keys) / len(feature_keys)) if feature_keys else 100.0,
    )


def audit_admin_master_source(
    master_df: pd.DataFrame,
    *,
    level: str,
    alias_fn: Callable[[str], str],
    variable_slug: str,
) -> AdminMasterSourceAudit:
    """Return source-selection heuristics from actual loaded admin master content."""
    state_keys = _normalize_series(_string_series(master_df, ("state", "state_name")), alias_fn=alias_fn)
    feature_keys = build_feature_key_series(master_df, level=level, alias_fn=alias_fn)
    feature_key_counts = feature_keys.dropna().astype(str).value_counts()
    duplicate_identity_keys = tuple(sorted(feature_key_counts[feature_key_counts > 1].index.tolist()))

    value_cols = [column for column in master_df.columns if str(column).startswith(f"{variable_slug}__")]
    if value_cols:
        valued_mask = master_df[value_cols].notna().any(axis=1)
    else:
        valued_mask = pd.Series(False, index=master_df.index, dtype=bool)

    distinct_state_keys = tuple(sorted({str(value) for value in state_keys.dropna().tolist()}))
    valued_state_keys = tuple(sorted({str(value) for value in state_keys[valued_mask].dropna().tolist()}))

    return AdminMasterSourceAudit(
        distinct_state_keys=distinct_state_keys,
        valued_state_keys=valued_state_keys,
        duplicate_identity_keys=duplicate_identity_keys,
        malformed_identity_row_count=int(feature_keys.isna().sum()),
    )
