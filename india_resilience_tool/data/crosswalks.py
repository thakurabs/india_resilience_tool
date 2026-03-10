"""
Read-optimized admin-hydro crosswalk helpers for IRT.

This module currently supports a canonical district ↔ sub-basin crosswalk
artifact used for deterministic context and explanation in the dashboard.
It is intentionally Streamlit-free.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Optional, Sequence, Union

import pandas as pd


PathLike = Union[str, Path]
CrosswalkDirection = Literal["district_to_subbasin", "subbasin_to_district"]
_FRACTION_EPSILON = 1e-6

DISTRICT_SUBBASIN_REQUIRED_COLUMNS: Sequence[str] = (
    "district_name",
    "state_name",
    "subbasin_id",
    "subbasin_name",
    "basin_id",
    "basin_name",
    "intersection_area_km2",
    "district_area_fraction_in_subbasin",
    "subbasin_area_fraction_in_district",
)


@dataclass(frozen=True)
class CrosswalkOverlap:
    """One ordered overlap row in a rendered context summary."""

    counterpart_id: str
    counterpart_name: str
    counterpart_state_name: Optional[str]
    basin_id: str
    basin_name: str
    intersection_area_km2: float
    selected_fraction: float
    counterpart_fraction: float


@dataclass(frozen=True)
class CrosswalkContext:
    """Structured, render-ready admin↔hydro context for one selected unit."""

    direction: CrosswalkDirection
    selected_name: str
    section_title: str
    overlap_count: int
    classification: str
    dominant_counterpart_id: str
    dominant_counterpart_name: str
    dominant_counterpart_fraction: float
    primary_basin_id: str
    primary_basin_name: str
    all_counterpart_ids: tuple[str, ...]
    overlaps: tuple[CrosswalkOverlap, ...]
    explanation: str
    coordination_note: Optional[str] = None


def load_district_subbasin_crosswalk(path: PathLike) -> pd.DataFrame:
    """
    Load and validate the canonical district ↔ sub-basin crosswalk CSV.

    The artifact must contain one row per district–sub-basin overlap.
    """
    df = pd.read_csv(path)
    return ensure_district_subbasin_crosswalk(df)


def ensure_district_subbasin_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a district ↔ sub-basin crosswalk DataFrame."""
    missing = [col for col in DISTRICT_SUBBASIN_REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "District-subbasin crosswalk is missing required columns: "
            f"{missing}."
        )

    out = df.copy()
    for col in ("district_name", "state_name", "subbasin_id", "subbasin_name", "basin_id", "basin_name"):
        out[col] = out[col].astype(str).str.strip()

    numeric_cols = (
        "intersection_area_km2",
        "district_area_fraction_in_subbasin",
        "subbasin_area_fraction_in_district",
    )
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        if out[col].isna().any():
            raise ValueError(f"District-subbasin crosswalk contains invalid numeric values in '{col}'.")

    if (out["intersection_area_km2"] < 0).any():
        raise ValueError("District-subbasin crosswalk contains negative intersection areas.")

    for col in ("district_area_fraction_in_subbasin", "subbasin_area_fraction_in_district"):
        bad = (out[col] < (0.0 - _FRACTION_EPSILON)) | (out[col] > (1.0 + _FRACTION_EPSILON))
        if bad.any():
            max_val = float(out.loc[bad, col].max())
            min_val = float(out.loc[bad, col].min())
            raise ValueError(
                "District-subbasin crosswalk contains materially out-of-range fractions "
                f"in '{col}' (min={min_val:.8f}, max={max_val:.8f})."
            )
        out[col] = out[col].clip(lower=0.0, upper=1.0)

    dup_mask = out.duplicated(subset=["state_name", "district_name", "subbasin_id"], keep=False)
    if dup_mask.any():
        raise ValueError(
            "District-subbasin crosswalk contains duplicate district–sub-basin pairs."
        )

    return out.reset_index(drop=True)


def build_district_hydro_context(
    crosswalk_df: pd.DataFrame,
    *,
    district_name: str,
    state_name: Optional[str],
    alias_fn: Callable[[str], str],
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build hydro context for a selected district."""
    if crosswalk_df is None or crosswalk_df.empty:
        return None

    district_key = alias_fn(str(district_name or "").strip())
    if not district_key:
        return None

    district_mask = crosswalk_df["district_name"].astype(str).map(alias_fn) == district_key
    if state_name:
        state_key = alias_fn(str(state_name).strip())
        district_mask = district_mask & (
            crosswalk_df["state_name"].astype(str).map(alias_fn) == state_key
        )

    district_rows = crosswalk_df.loc[district_mask].copy()
    if district_rows.empty:
        return None

    district_rows = district_rows.sort_values(
        by=[
            "district_area_fraction_in_subbasin",
            "subbasin_area_fraction_in_district",
            "intersection_area_km2",
            "subbasin_name",
        ],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    top = district_rows.iloc[0]
    overlap_count = int(district_rows.shape[0])
    dominant_fraction = float(top["district_area_fraction_in_subbasin"])
    classification = _classify_district_pattern(overlap_count, dominant_fraction)

    basin_totals = (
        district_rows.groupby(["basin_id", "basin_name"], dropna=False)["district_area_fraction_in_subbasin"]
        .sum()
        .reset_index()
        .sort_values(by=["district_area_fraction_in_subbasin", "basin_name"], ascending=[False, True])
    )
    primary_basin = basin_totals.iloc[0]

    overlaps = tuple(
        CrosswalkOverlap(
            counterpart_id=str(r["subbasin_id"]),
            counterpart_name=str(r["subbasin_name"]),
            counterpart_state_name=None,
            basin_id=str(r["basin_id"]),
            basin_name=str(r["basin_name"]),
            intersection_area_km2=float(r["intersection_area_km2"]),
            selected_fraction=float(r["district_area_fraction_in_subbasin"]),
            counterpart_fraction=float(r["subbasin_area_fraction_in_district"]),
        )
        for _, r in district_rows.head(top_n).iterrows()
    )

    explanation = _explain_district_hydro_context(
        district_name=str(top["district_name"]),
        dominant_subbasin=str(top["subbasin_name"]),
        dominant_fraction=dominant_fraction,
        overlap_count=overlap_count,
        classification=classification,
    )
    coordination_note = _coordination_note(
        overlap_count=overlap_count,
        source_level="district",
        target_level="sub-basins",
    )

    return CrosswalkContext(
        direction="district_to_subbasin",
        selected_name=str(top["district_name"]),
        section_title="Hydrology context",
        overlap_count=overlap_count,
        classification=classification,
        dominant_counterpart_id=str(top["subbasin_id"]),
        dominant_counterpart_name=str(top["subbasin_name"]),
        dominant_counterpart_fraction=dominant_fraction,
        primary_basin_id=str(primary_basin["basin_id"]),
        primary_basin_name=str(primary_basin["basin_name"]),
        all_counterpart_ids=tuple(str(v) for v in district_rows["subbasin_id"].tolist()),
        overlaps=overlaps,
        explanation=explanation,
        coordination_note=coordination_note,
    )


def build_subbasin_admin_context(
    crosswalk_df: pd.DataFrame,
    *,
    subbasin_id: Optional[str],
    subbasin_name: Optional[str],
    alias_fn: Callable[[str], str],
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build admin context for a selected sub-basin."""
    if crosswalk_df is None or crosswalk_df.empty:
        return None

    subbasin_rows = _filter_subbasin_rows(
        crosswalk_df,
        subbasin_id=subbasin_id,
        subbasin_name=subbasin_name,
        alias_fn=alias_fn,
    )
    if subbasin_rows.empty:
        return None

    subbasin_rows = subbasin_rows.sort_values(
        by=[
            "subbasin_area_fraction_in_district",
            "district_area_fraction_in_subbasin",
            "intersection_area_km2",
            "district_name",
        ],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    top = subbasin_rows.iloc[0]
    overlap_count = int(subbasin_rows.shape[0])
    dominant_fraction = float(top["subbasin_area_fraction_in_district"])
    classification = _classify_subbasin_pattern(overlap_count, dominant_fraction)

    overlaps = tuple(
        CrosswalkOverlap(
            counterpart_id=_district_counterpart_id(r),
            counterpart_name=str(r["district_name"]),
            counterpart_state_name=str(r.get("state_name", "")).strip() or None,
            basin_id=str(r["basin_id"]),
            basin_name=str(r["basin_name"]),
            intersection_area_km2=float(r["intersection_area_km2"]),
            selected_fraction=float(r["subbasin_area_fraction_in_district"]),
            counterpart_fraction=float(r["district_area_fraction_in_subbasin"]),
        )
        for _, r in subbasin_rows.head(top_n).iterrows()
    )

    explanation = _explain_subbasin_admin_context(
        subbasin_name=str(top["subbasin_name"]),
        dominant_district=str(top["district_name"]),
        dominant_fraction=dominant_fraction,
        overlap_count=overlap_count,
        classification=classification,
    )
    coordination_note = _coordination_note(
        overlap_count=overlap_count,
        source_level="sub-basin",
        target_level="districts",
    )

    return CrosswalkContext(
        direction="subbasin_to_district",
        selected_name=str(top["subbasin_name"]),
        section_title="Administrative context",
        overlap_count=overlap_count,
        classification=classification,
        dominant_counterpart_id=_district_counterpart_id(top),
        dominant_counterpart_name=str(top["district_name"]),
        dominant_counterpart_fraction=dominant_fraction,
        primary_basin_id=str(top["basin_id"]),
        primary_basin_name=str(top["basin_name"]),
        all_counterpart_ids=tuple(_district_counterpart_id(r) for _, r in subbasin_rows.iterrows()),
        overlaps=overlaps,
        explanation=explanation,
        coordination_note=coordination_note,
    )


def _filter_subbasin_rows(
    crosswalk_df: pd.DataFrame,
    *,
    subbasin_id: Optional[str],
    subbasin_name: Optional[str],
    alias_fn: Callable[[str], str],
) -> pd.DataFrame:
    subbasin_id_key = alias_fn(str(subbasin_id or "").strip())
    if subbasin_id_key:
        mask = crosswalk_df["subbasin_id"].astype(str).map(alias_fn) == subbasin_id_key
        rows = crosswalk_df.loc[mask].copy()
        if not rows.empty:
            return rows

    subbasin_name_key = alias_fn(str(subbasin_name or "").strip())
    if not subbasin_name_key:
        return pd.DataFrame(columns=crosswalk_df.columns)
    mask = crosswalk_df["subbasin_name"].astype(str).map(alias_fn) == subbasin_name_key
    return crosswalk_df.loc[mask].copy()


def _district_counterpart_id(row: pd.Series) -> str:
    state = str(row.get("state_name", "")).strip()
    district = str(row.get("district_name", "")).strip()
    if state and district:
        return f"{state}::{district}"
    return district


def _classify_district_pattern(overlap_count: int, dominant_fraction: float) -> str:
    if overlap_count <= 1:
        return "single_subbasin"
    if dominant_fraction >= 0.75:
        return "dominant_subbasin"
    if dominant_fraction >= 0.5:
        return "mostly_one_subbasin"
    return "fragmented_across_subbasins"


def _classify_subbasin_pattern(overlap_count: int, dominant_fraction: float) -> str:
    if overlap_count <= 1:
        return "single_district"
    if dominant_fraction >= 0.75:
        return "concentrated_in_one_district"
    if dominant_fraction >= 0.5:
        return "mostly_one_district"
    return "distributed_across_districts"


def _explain_district_hydro_context(
    *,
    district_name: str,
    dominant_subbasin: str,
    dominant_fraction: float,
    overlap_count: int,
    classification: str,
) -> str:
    pct = f"{dominant_fraction * 100:.0f}%"
    if classification == "single_subbasin":
        return f"{district_name} lies within one mapped sub-basin: {dominant_subbasin}."
    if classification == "dominant_subbasin":
        return (
            f"Most of {district_name} lies in {dominant_subbasin} ({pct}), "
            "so hydro signals there are likely the most relevant context."
        )
    if classification == "mostly_one_subbasin":
        return (
            f"A majority of {district_name} lies in {dominant_subbasin} ({pct}), "
            "but other sub-basins also contribute to its hydro context."
        )
    return (
        f"{district_name} is split across {overlap_count} sub-basins, so its hydro context "
        "is distributed rather than concentrated in one system."
    )


def _explain_subbasin_admin_context(
    *,
    subbasin_name: str,
    dominant_district: str,
    dominant_fraction: float,
    overlap_count: int,
    classification: str,
) -> str:
    pct = f"{dominant_fraction * 100:.0f}%"
    if classification == "single_district":
        return f"{subbasin_name} overlaps one mapped district: {dominant_district}."
    if classification == "concentrated_in_one_district":
        return (
            f"Most of {subbasin_name} lies in {dominant_district} ({pct}), "
            "so action in that district is likely especially consequential."
        )
    if classification == "mostly_one_district":
        return (
            f"A majority of {subbasin_name} lies in {dominant_district} ({pct}), "
            "but meaningful consequence is still distributed across other districts."
        )
    return (
        f"{subbasin_name} spans {overlap_count} districts, so action will likely require "
        "coordination across multiple jurisdictions."
    )


def _coordination_note(*, overlap_count: int, source_level: str, target_level: str) -> Optional[str]:
    if overlap_count <= 1:
        return None
    return (
        f"This {source_level} intersects {overlap_count} {target_level}, "
        "so interpretation and action may need to account for multiple linked geographies."
    )
