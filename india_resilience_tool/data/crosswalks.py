"""
Read-optimized polygon crosswalk helpers for IRT.

This module validates precomputed admin↔hydro overlap tables and builds
deterministic, render-ready context objects for the dashboard. It is
intentionally Streamlit-free.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Mapping, Optional, Sequence, Union

import pandas as pd

from india_resilience_tool.utils.processed_io import read_table


PathLike = Union[str, Path]
CrosswalkLevel = Literal["district", "block", "basin", "sub_basin"]
CrosswalkDirection = str
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

BLOCK_SUBBASIN_REQUIRED_COLUMNS: Sequence[str] = (
    "block_name",
    "district_name",
    "state_name",
    "subbasin_id",
    "subbasin_name",
    "basin_id",
    "basin_name",
    "intersection_area_km2",
    "block_area_fraction_in_subbasin",
    "subbasin_area_fraction_in_block",
)

DISTRICT_BASIN_REQUIRED_COLUMNS: Sequence[str] = (
    "district_name",
    "state_name",
    "basin_id",
    "basin_name",
    "intersection_area_km2",
    "district_area_fraction_in_basin",
    "basin_area_fraction_in_district",
)

BLOCK_BASIN_REQUIRED_COLUMNS: Sequence[str] = (
    "block_name",
    "district_name",
    "state_name",
    "basin_id",
    "basin_name",
    "intersection_area_km2",
    "block_area_fraction_in_basin",
    "basin_area_fraction_in_block",
)

_PAIR_CONFIG: dict[tuple[CrosswalkLevel, CrosswalkLevel], dict[str, object]] = {
    ("district", "sub_basin"): {
        "required_columns": DISTRICT_SUBBASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "subbasin_id"],
        "selected_fraction_col": "district_area_fraction_in_subbasin",
        "counterpart_fraction_col": "subbasin_area_fraction_in_district",
        "source_kind": "admin",
        "selected_section_title": "Hydrology context",
        "dominant_label": "Dominant sub-basin",
        "selected_fraction_label": "District share",
        "counterpart_fraction_label": "Sub-basin share",
        "highlight_action_label": "Highlight related sub-basins",
        "open_action_label": "Open sub-basin",
    },
    ("block", "sub_basin"): {
        "required_columns": BLOCK_SUBBASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "block_name", "subbasin_id"],
        "selected_fraction_col": "block_area_fraction_in_subbasin",
        "counterpart_fraction_col": "subbasin_area_fraction_in_block",
        "source_kind": "admin",
        "selected_section_title": "Hydrology context",
        "dominant_label": "Dominant sub-basin",
        "selected_fraction_label": "Block share",
        "counterpart_fraction_label": "Sub-basin share",
        "highlight_action_label": "Highlight related sub-basins",
        "open_action_label": "Open sub-basin",
    },
    ("district", "basin"): {
        "required_columns": DISTRICT_BASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "basin_id"],
        "selected_fraction_col": "district_area_fraction_in_basin",
        "counterpart_fraction_col": "basin_area_fraction_in_district",
        "source_kind": "admin",
        "selected_section_title": "Basin context",
        "dominant_label": "Dominant basin",
        "selected_fraction_label": "District share",
        "counterpart_fraction_label": "Basin share",
        "highlight_action_label": "Highlight related basins",
        "open_action_label": "Open basin",
    },
    ("block", "basin"): {
        "required_columns": BLOCK_BASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "block_name", "basin_id"],
        "selected_fraction_col": "block_area_fraction_in_basin",
        "counterpart_fraction_col": "basin_area_fraction_in_block",
        "source_kind": "admin",
        "selected_section_title": "Basin context",
        "dominant_label": "Dominant basin",
        "selected_fraction_label": "Block share",
        "counterpart_fraction_label": "Basin share",
        "highlight_action_label": "Highlight related basins",
        "open_action_label": "Open basin",
    },
    ("sub_basin", "district"): {
        "required_columns": DISTRICT_SUBBASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "subbasin_id"],
        "selected_fraction_col": "subbasin_area_fraction_in_district",
        "counterpart_fraction_col": "district_area_fraction_in_subbasin",
        "source_kind": "hydro",
        "selected_section_title": "Administrative context",
        "dominant_label": "District covering the largest share of this sub-basin",
        "selected_fraction_label": "Share of sub-basin",
        "counterpart_fraction_label": "Share of district in sub-basin",
        "highlight_action_label": "Highlight related districts",
        "open_action_label": "Open district",
    },
    ("sub_basin", "block"): {
        "required_columns": BLOCK_SUBBASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "block_name", "subbasin_id"],
        "selected_fraction_col": "subbasin_area_fraction_in_block",
        "counterpart_fraction_col": "block_area_fraction_in_subbasin",
        "source_kind": "hydro",
        "selected_section_title": "Administrative context",
        "dominant_label": "Block covering the largest share of this sub-basin",
        "selected_fraction_label": "Share of sub-basin",
        "counterpart_fraction_label": "Share of block in sub-basin",
        "highlight_action_label": "Highlight related blocks",
        "open_action_label": "Open block",
    },
    ("basin", "district"): {
        "required_columns": DISTRICT_BASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "basin_id"],
        "selected_fraction_col": "basin_area_fraction_in_district",
        "counterpart_fraction_col": "district_area_fraction_in_basin",
        "source_kind": "hydro",
        "selected_section_title": "Administrative context",
        "dominant_label": "District covering the largest share of this basin",
        "selected_fraction_label": "Share of basin",
        "counterpart_fraction_label": "Share of district in basin",
        "highlight_action_label": "Highlight related districts",
        "open_action_label": "Open district",
    },
    ("basin", "block"): {
        "required_columns": BLOCK_BASIN_REQUIRED_COLUMNS,
        "duplicate_subset": ["state_name", "district_name", "block_name", "basin_id"],
        "selected_fraction_col": "basin_area_fraction_in_block",
        "counterpart_fraction_col": "block_area_fraction_in_basin",
        "source_kind": "hydro",
        "selected_section_title": "Administrative context",
        "dominant_label": "Block covering the largest share of this basin",
        "selected_fraction_label": "Share of basin",
        "counterpart_fraction_label": "Share of block in basin",
        "highlight_action_label": "Highlight related blocks",
        "open_action_label": "Open block",
    },
}


@dataclass(frozen=True)
class CrosswalkOverlap:
    """One ordered overlap row in a rendered context summary."""

    counterpart_id: str
    counterpart_name: str
    counterpart_level: CrosswalkLevel
    counterpart_state_name: Optional[str]
    counterpart_parent_name: Optional[str]
    basin_id: str
    basin_name: str
    intersection_area_km2: float
    selected_fraction: float
    counterpart_fraction: float


@dataclass(frozen=True)
class CrosswalkContext:
    """Structured, render-ready admin↔hydro context for one selected unit."""

    direction: CrosswalkDirection
    selected_level: CrosswalkLevel
    counterpart_level: CrosswalkLevel
    selected_name: str
    section_title: str
    overlap_count: int
    classification: str
    dominant_counterpart_id: str
    dominant_counterpart_name: str
    dominant_counterpart_fraction: float
    dominant_label: str
    primary_basin_id: Optional[str]
    primary_basin_name: Optional[str]
    all_counterpart_ids: tuple[str, ...]
    overlaps: tuple[CrosswalkOverlap, ...]
    explanation: str
    coordination_note: Optional[str] = None
    highlight_action_label: str = "Highlight related units"
    open_action_label: str = "Open related unit"
    selected_fraction_label: str = "Selected share"
    counterpart_fraction_label: str = "Counterpart share"


def load_district_subbasin_crosswalk(path: PathLike) -> pd.DataFrame:
    """Load and validate the canonical district ↔ sub-basin crosswalk CSV."""
    return ensure_district_subbasin_crosswalk(read_table(Path(path)))


def load_block_subbasin_crosswalk(path: PathLike) -> pd.DataFrame:
    """Load and validate the canonical block ↔ sub-basin crosswalk CSV."""
    return ensure_block_subbasin_crosswalk(read_table(Path(path)))


def load_district_basin_crosswalk(path: PathLike) -> pd.DataFrame:
    """Load and validate the canonical district ↔ basin crosswalk CSV."""
    return ensure_district_basin_crosswalk(read_table(Path(path)))


def load_block_basin_crosswalk(path: PathLike) -> pd.DataFrame:
    """Load and validate the canonical block ↔ basin crosswalk CSV."""
    return ensure_block_basin_crosswalk(read_table(Path(path)))


def ensure_district_subbasin_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a district ↔ sub-basin crosswalk DataFrame."""
    return _ensure_crosswalk(df, selected_level="district", counterpart_level="sub_basin")


def ensure_block_subbasin_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a block ↔ sub-basin crosswalk DataFrame."""
    return _ensure_crosswalk(df, selected_level="block", counterpart_level="sub_basin")


def ensure_district_basin_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a district ↔ basin crosswalk DataFrame."""
    return _ensure_crosswalk(df, selected_level="district", counterpart_level="basin")


def ensure_block_basin_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a block ↔ basin crosswalk DataFrame."""
    return _ensure_crosswalk(df, selected_level="block", counterpart_level="basin")


def build_district_hydro_context(
    crosswalk_df: pd.DataFrame,
    *,
    district_name: str,
    state_name: Optional[str],
    alias_fn: Callable[[str], str],
    hydro_level: Literal["basin", "sub_basin"] = "sub_basin",
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build basin or sub-basin context for a selected district."""
    rows = _filter_admin_rows(
        crosswalk_df,
        level="district",
        alias_fn=alias_fn,
        state_name=state_name,
        district_name=district_name,
    )
    return _build_context(
        rows,
        selected_level="district",
        counterpart_level=hydro_level,
        top_n=top_n,
    )


def build_block_hydro_context(
    crosswalk_df: pd.DataFrame,
    *,
    block_name: str,
    district_name: Optional[str],
    state_name: Optional[str],
    alias_fn: Callable[[str], str],
    hydro_level: Literal["basin", "sub_basin"] = "sub_basin",
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build basin or sub-basin context for a selected block."""
    rows = _filter_admin_rows(
        crosswalk_df,
        level="block",
        alias_fn=alias_fn,
        state_name=state_name,
        district_name=district_name,
        block_name=block_name,
    )
    return _build_context(
        rows,
        selected_level="block",
        counterpart_level=hydro_level,
        top_n=top_n,
    )


def build_subbasin_admin_context(
    crosswalk_df: pd.DataFrame,
    *,
    subbasin_id: Optional[str],
    subbasin_name: Optional[str],
    alias_fn: Callable[[str], str],
    admin_level: Literal["district", "block"] = "district",
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build district or block context for a selected sub-basin."""
    rows = _filter_hydro_rows(
        crosswalk_df,
        level="sub_basin",
        alias_fn=alias_fn,
        basin_id=None,
        basin_name=None,
        subbasin_id=subbasin_id,
        subbasin_name=subbasin_name,
    )
    return _build_context(
        rows,
        selected_level="sub_basin",
        counterpart_level=admin_level,
        top_n=top_n,
    )


def build_basin_admin_context(
    crosswalk_df: pd.DataFrame,
    *,
    basin_id: Optional[str],
    basin_name: Optional[str],
    alias_fn: Callable[[str], str],
    admin_level: Literal["district", "block"] = "district",
    top_n: int = 3,
) -> Optional[CrosswalkContext]:
    """Build district or block context for a selected basin."""
    rows = _filter_hydro_rows(
        crosswalk_df,
        level="basin",
        alias_fn=alias_fn,
        basin_id=basin_id,
        basin_name=basin_name,
        subbasin_id=None,
        subbasin_name=None,
    )
    return _build_context(
        rows,
        selected_level="basin",
        counterpart_level=admin_level,
        top_n=top_n,
    )


def _ensure_crosswalk(
    df: pd.DataFrame,
    *,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
) -> pd.DataFrame:
    cfg = _pair_cfg(selected_level, counterpart_level)
    required_columns = list(cfg["required_columns"])
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{_pair_label(selected_level, counterpart_level)} crosswalk is missing required columns: {missing}."
        )

    out = df.copy()
    for col in _string_columns_for_pair(selected_level, counterpart_level):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()

    numeric_cols = [
        "intersection_area_km2",
        str(cfg["selected_fraction_col"]),
        str(cfg["counterpart_fraction_col"]),
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        if out[col].isna().any():
            raise ValueError(
                f"{_pair_label(selected_level, counterpart_level)} crosswalk contains invalid numeric values in '{col}'."
            )

    if (out["intersection_area_km2"] < 0).any():
        raise ValueError(f"{_pair_label(selected_level, counterpart_level)} crosswalk contains negative intersection areas.")

    for col in (str(cfg["selected_fraction_col"]), str(cfg["counterpart_fraction_col"])):
        bad = (out[col] < (0.0 - _FRACTION_EPSILON)) | (out[col] > (1.0 + _FRACTION_EPSILON))
        if bad.any():
            max_val = float(out.loc[bad, col].max())
            min_val = float(out.loc[bad, col].min())
            raise ValueError(
                f"{_pair_label(selected_level, counterpart_level)} crosswalk contains materially out-of-range fractions "
                f"in '{col}' (min={min_val:.8f}, max={max_val:.8f})."
            )
        out[col] = out[col].clip(lower=0.0, upper=1.0)

    dup_mask = out.duplicated(subset=list(cfg["duplicate_subset"]), keep=False)
    if dup_mask.any():
        raise ValueError(
            f"{_pair_label(selected_level, counterpart_level)} crosswalk contains duplicate {selected_level}–{counterpart_level} pairs."
        )

    return out.reset_index(drop=True)


def _build_context(
    rows: pd.DataFrame,
    *,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
    top_n: int,
) -> Optional[CrosswalkContext]:
    if rows is None or rows.empty:
        return None

    cfg = _pair_cfg(selected_level, counterpart_level)
    selected_fraction_col = str(cfg["selected_fraction_col"])
    counterpart_fraction_col = str(cfg["counterpart_fraction_col"])
    source_kind = str(cfg["source_kind"])

    rows = rows.sort_values(
        by=[
            selected_fraction_col,
            counterpart_fraction_col,
            "intersection_area_km2",
            _counterpart_name_col(counterpart_level),
        ],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    top = rows.iloc[0]
    overlap_count = int(rows.shape[0])
    dominant_fraction = float(top[selected_fraction_col])
    classification = _classify_pattern(
        overlap_count=overlap_count,
        dominant_fraction=dominant_fraction,
        source_kind=source_kind,
        counterpart_level=counterpart_level,
    )

    primary_basin_id, primary_basin_name = _primary_basin_for_rows(
        rows,
        selected_level=selected_level,
        counterpart_level=counterpart_level,
    )

    overlaps = tuple(
        _row_to_overlap(
            row=r,
            selected_level=selected_level,
            counterpart_level=counterpart_level,
            selected_fraction_col=selected_fraction_col,
            counterpart_fraction_col=counterpart_fraction_col,
        )
        for _, r in rows.head(top_n).iterrows()
    )

    return CrosswalkContext(
        direction=f"{selected_level}_to_{counterpart_level}",
        selected_level=selected_level,
        counterpart_level=counterpart_level,
        selected_name=_selected_name(top, selected_level=selected_level),
        section_title=str(cfg["selected_section_title"]),
        overlap_count=overlap_count,
        classification=classification,
        dominant_counterpart_id=_counterpart_id(top, level=counterpart_level),
        dominant_counterpart_name=str(top[_counterpart_name_col(counterpart_level)]),
        dominant_counterpart_fraction=dominant_fraction,
        dominant_label=str(cfg["dominant_label"]),
        primary_basin_id=primary_basin_id,
        primary_basin_name=primary_basin_name,
        all_counterpart_ids=tuple(
            _counterpart_id(r, level=counterpart_level) for _, r in rows.iterrows()
        ),
        overlaps=overlaps,
        explanation=_explain_context(
            selected_name=_selected_name(top, selected_level=selected_level),
            dominant_name=str(top[_counterpart_name_col(counterpart_level)]),
            dominant_fraction=dominant_fraction,
            overlap_count=overlap_count,
            classification=classification,
            selected_level=selected_level,
            counterpart_level=counterpart_level,
            source_kind=source_kind,
        ),
        coordination_note=_coordination_note(
            overlap_count=overlap_count,
            selected_level=selected_level,
            counterpart_level=counterpart_level,
        ),
        highlight_action_label=str(cfg["highlight_action_label"]),
        open_action_label=str(cfg["open_action_label"]),
        selected_fraction_label=str(cfg["selected_fraction_label"]),
        counterpart_fraction_label=str(cfg["counterpart_fraction_label"]),
    )


def _row_to_overlap(
    *,
    row: pd.Series,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
    selected_fraction_col: str,
    counterpart_fraction_col: str,
) -> CrosswalkOverlap:
    return CrosswalkOverlap(
        counterpart_id=_counterpart_id(row, level=counterpart_level),
        counterpart_name=str(row[_counterpart_name_col(counterpart_level)]),
        counterpart_level=counterpart_level,
        counterpart_state_name=_counterpart_state_name(row, level=counterpart_level),
        counterpart_parent_name=_counterpart_parent_name(row, level=counterpart_level),
        basin_id=str(row.get("basin_id", "")).strip(),
        basin_name=str(row.get("basin_name", "")).strip(),
        intersection_area_km2=float(row["intersection_area_km2"]),
        selected_fraction=float(row[selected_fraction_col]),
        counterpart_fraction=float(row[counterpart_fraction_col]),
    )


def _pair_cfg(selected_level: CrosswalkLevel, counterpart_level: CrosswalkLevel) -> Mapping[str, object]:
    try:
        return _PAIR_CONFIG[(selected_level, counterpart_level)]
    except KeyError as exc:
        raise ValueError(f"Unsupported crosswalk pair: {selected_level} ↔ {counterpart_level}.") from exc


def _pair_label(selected_level: CrosswalkLevel, counterpart_level: CrosswalkLevel) -> str:
    return f"{_display_level(selected_level)}-{_display_level(counterpart_level)}"


def _string_columns_for_pair(selected_level: CrosswalkLevel, counterpart_level: CrosswalkLevel) -> list[str]:
    cols = ["basin_id", "basin_name"]
    if selected_level in {"district", "block"} or counterpart_level in {"district", "block"}:
        cols.extend(["state_name", "district_name"])
    if selected_level == "block" or counterpart_level == "block":
        cols.append("block_name")
    if selected_level == "sub_basin" or counterpart_level == "sub_basin":
        cols.extend(["subbasin_id", "subbasin_name"])
    return sorted(set(cols))


def _filter_admin_rows(
    crosswalk_df: pd.DataFrame,
    *,
    level: Literal["district", "block"],
    alias_fn: Callable[[str], str],
    state_name: Optional[str],
    district_name: Optional[str],
    block_name: Optional[str] = None,
) -> pd.DataFrame:
    if crosswalk_df is None or crosswalk_df.empty:
        return pd.DataFrame(columns=crosswalk_df.columns if crosswalk_df is not None else [])

    mask = pd.Series(True, index=crosswalk_df.index)
    if state_name:
        state_key = alias_fn(str(state_name).strip())
        mask = mask & (crosswalk_df["state_name"].astype(str).map(alias_fn) == state_key)
    if district_name:
        district_key = alias_fn(str(district_name).strip())
        mask = mask & (crosswalk_df["district_name"].astype(str).map(alias_fn) == district_key)
    if level == "block":
        block_key = alias_fn(str(block_name or "").strip())
        if not block_key:
            return pd.DataFrame(columns=crosswalk_df.columns)
        mask = mask & (crosswalk_df["block_name"].astype(str).map(alias_fn) == block_key)

    return crosswalk_df.loc[mask].copy()


def _filter_hydro_rows(
    crosswalk_df: pd.DataFrame,
    *,
    level: Literal["basin", "sub_basin"],
    alias_fn: Callable[[str], str],
    basin_id: Optional[str],
    basin_name: Optional[str],
    subbasin_id: Optional[str],
    subbasin_name: Optional[str],
) -> pd.DataFrame:
    if crosswalk_df is None or crosswalk_df.empty:
        return pd.DataFrame(columns=crosswalk_df.columns if crosswalk_df is not None else [])

    if level == "basin":
        basin_id_key = alias_fn(str(basin_id or "").strip())
        if basin_id_key:
            mask = crosswalk_df["basin_id"].astype(str).map(alias_fn) == basin_id_key
            rows = crosswalk_df.loc[mask].copy()
            if not rows.empty:
                return rows

        basin_name_key = alias_fn(str(basin_name or "").strip())
        if not basin_name_key:
            return pd.DataFrame(columns=crosswalk_df.columns)
        mask = crosswalk_df["basin_name"].astype(str).map(alias_fn) == basin_name_key
        return crosswalk_df.loc[mask].copy()

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


def _selected_name(row: pd.Series, *, selected_level: CrosswalkLevel) -> str:
    if selected_level == "sub_basin":
        return str(row.get("subbasin_name", "")).strip()
    if selected_level == "basin":
        return str(row.get("basin_name", "")).strip()
    if selected_level == "block":
        return str(row.get("block_name", "")).strip()
    return str(row.get("district_name", "")).strip()


def _counterpart_name_col(level: CrosswalkLevel) -> str:
    if level == "sub_basin":
        return "subbasin_name"
    if level == "basin":
        return "basin_name"
    if level == "block":
        return "block_name"
    return "district_name"


def _counterpart_id(row: pd.Series, *, level: CrosswalkLevel) -> str:
    state = str(row.get("state_name", "")).strip()
    district = str(row.get("district_name", "")).strip()
    block = str(row.get("block_name", "")).strip()
    if level == "sub_basin":
        return str(row.get("subbasin_id", "")).strip()
    if level == "basin":
        return str(row.get("basin_id", "")).strip()
    if level == "block":
        if state and district and block:
            return f"{state}::{district}::{block}"
        return block
    if state and district:
        return f"{state}::{district}"
    return district


def _counterpart_state_name(row: pd.Series, *, level: CrosswalkLevel) -> Optional[str]:
    if level not in {"district", "block"}:
        return None
    state = str(row.get("state_name", "")).strip()
    return state or None


def _counterpart_parent_name(row: pd.Series, *, level: CrosswalkLevel) -> Optional[str]:
    if level == "sub_basin":
        basin_name = str(row.get("basin_name", "")).strip()
        return basin_name or None
    if level == "block":
        district_name = str(row.get("district_name", "")).strip()
        return district_name or None
    return None


def _primary_basin_for_rows(
    rows: pd.DataFrame,
    *,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
) -> tuple[Optional[str], Optional[str]]:
    if selected_level == "basin":
        first = rows.iloc[0]
        return str(first.get("basin_id", "")).strip() or None, str(first.get("basin_name", "")).strip() or None

    if counterpart_level == "basin":
        first = rows.iloc[0]
        return str(first.get("basin_id", "")).strip() or None, str(first.get("basin_name", "")).strip() or None

    basin_totals = (
        rows.groupby(["basin_id", "basin_name"], dropna=False)["intersection_area_km2"]
        .sum()
        .reset_index()
        .sort_values(by=["intersection_area_km2", "basin_name"], ascending=[False, True])
    )
    if basin_totals.empty:
        return None, None
    first = basin_totals.iloc[0]
    return str(first["basin_id"]).strip() or None, str(first["basin_name"]).strip() or None


def _classify_pattern(
    *,
    overlap_count: int,
    dominant_fraction: float,
    source_kind: str,
    counterpart_level: CrosswalkLevel,
) -> str:
    suffix = _classification_suffix(counterpart_level)
    if overlap_count <= 1:
        return f"single_{suffix}"
    if source_kind == "admin":
        if dominant_fraction >= 0.75:
            return f"dominant_{suffix}"
        if dominant_fraction >= 0.5:
            return f"mostly_one_{suffix}"
        return f"fragmented_across_{_plural_suffix(counterpart_level)}"
    if dominant_fraction >= 0.75:
        return f"concentrated_in_one_{suffix}"
    if dominant_fraction >= 0.5:
        return f"mostly_one_{suffix}"
    return f"distributed_across_{_plural_suffix(counterpart_level)}"


def _classification_suffix(level: CrosswalkLevel) -> str:
    if level == "sub_basin":
        return "subbasin"
    return level


def _plural_suffix(level: CrosswalkLevel) -> str:
    if level == "sub_basin":
        return "subbasins"
    if level == "basin":
        return "basins"
    if level == "district":
        return "districts"
    return "blocks"


def _explain_context(
    *,
    selected_name: str,
    dominant_name: str,
    dominant_fraction: float,
    overlap_count: int,
    classification: str,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
    source_kind: str,
) -> str:
    pct = f"{dominant_fraction * 100:.0f}%"
    counterpart_singular = _display_level(counterpart_level)
    counterpart_plural = _display_level_plural(counterpart_level)

    if classification.startswith("single_"):
        verb = "lies within" if source_kind == "admin" else "overlaps"
        return f"{selected_name} {verb} one mapped {counterpart_singular}: {dominant_name}."
    if classification.startswith("dominant_"):
        return (
            f"Most of {selected_name} lies in {dominant_name} ({pct}), "
            "so hydro signals there are likely the most relevant context."
        )
    if classification.startswith("concentrated_in_one_"):
        return (
            f"Most of {selected_name} lies in {dominant_name} ({pct}), "
            "so action there is likely especially consequential."
        )
    if classification.startswith("mostly_one_"):
        if source_kind == "admin":
            return (
                f"A majority of {selected_name} lies in {dominant_name} ({pct}), "
                f"but other {counterpart_plural} also contribute to its hydro context."
            )
        return (
            f"A majority of {selected_name} lies in {dominant_name} ({pct}), "
            f"but meaningful consequence is still distributed across other {counterpart_plural}."
        )
    if source_kind == "admin":
        return (
            f"{selected_name} is split across {overlap_count} {counterpart_plural}, so its hydro context "
            "is distributed rather than concentrated in one system."
        )
    return (
        f"{selected_name} spans {overlap_count} {counterpart_plural}, so action will likely require "
        "coordination across multiple jurisdictions."
    )


def _coordination_note(
    *,
    overlap_count: int,
    selected_level: CrosswalkLevel,
    counterpart_level: CrosswalkLevel,
) -> Optional[str]:
    if overlap_count <= 1:
        return None
    return (
        f"This {_display_level(selected_level)} intersects {overlap_count} {_display_level_plural(counterpart_level)}, "
        "so interpretation and action may need to account for multiple linked geographies."
    )


def _display_level(level: CrosswalkLevel) -> str:
    return "sub-basin" if level == "sub_basin" else level


def _display_level_plural(level: CrosswalkLevel) -> str:
    if level == "sub_basin":
        return "sub-basins"
    if level == "basin":
        return "basins"
    if level == "district":
        return "districts"
    return "blocks"
