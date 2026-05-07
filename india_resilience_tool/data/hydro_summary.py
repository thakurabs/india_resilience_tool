"""Load compact admin hydrological context summaries for the dashboard.

This module is intentionally Streamlit-free. Runtime caching belongs in
``india_resilience_tool.app.summary_cache``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union

import pandas as pd

PathLike = Union[str, Path]

ADMIN_HYDRO_REQUIRED_COLUMNS = (
    "admin_key",
    "admin_level",
    "state_name",
    "district_name",
    "block_name",
    "basin_id",
    "basin_name",
    "basin_frac",
    "subbasin_id",
    "subbasin_name",
    "subbasin_frac",
    "also_intersects_basin_json",
    "drainage_area_km2",
    "primary_river",
    "runoff_coeff",
    "hydro_type",
    "hydro_summary_status",
)


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=list(ADMIN_HYDRO_REQUIRED_COLUMNS))


def load_admin_hydro_summary(path: PathLike) -> pd.DataFrame:
    """Load and lightly validate ``admin_hydro_summary.parquet``.

    Missing or malformed optional context data must not crash the dashboard, so
    this loader returns an empty frame when it cannot provide the required schema.
    """
    p = Path(path)
    if not p.exists():
        return _empty()

    try:
        df = pd.read_parquet(p)
    except Exception:
        return _empty()

    missing = [c for c in ADMIN_HYDRO_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return _empty()

    out = df.copy()
    out["admin_key"] = out["admin_key"].astype(str).str.strip()
    out["admin_level"] = out["admin_level"].astype(str).str.strip().str.lower()
    out["block_name"] = out["block_name"].fillna("").astype(str)

    for c in ("basin_frac", "subbasin_frac", "drainage_area_km2", "runoff_coeff"):
        out[c] = pd.to_numeric(out[c], errors="coerce")

    for c in ("basin_id", "basin_name", "subbasin_id", "subbasin_name", "primary_river", "hydro_type"):
        out[c] = out[c].fillna("").astype(str)

    return out


def slice_hydro_for_admin_key(
    df: pd.DataFrame,
    *,
    admin_key: str,
    admin_level: str,
) -> Optional[pd.Series]:
    """Return the row matching ``(admin_key, admin_level)``, or ``None``."""
    if df is None or df.empty:
        return None
    if "admin_key" not in df.columns or "admin_level" not in df.columns:
        return None

    key = str(admin_key or "").strip()
    level = str(admin_level or "").strip().lower()
    if not key or level not in {"district", "block"}:
        return None

    mask = (df["admin_key"].astype(str).str.strip() == key) & (
        df["admin_level"].astype(str).str.strip().str.lower() == level
    )
    rows = df.loc[mask]
    if rows.empty:
        return None
    return rows.iloc[0]


def parse_hydro_intersections(value: object) -> list[dict]:
    """Parse an ``also_intersects_*_json`` cell into a list of dicts.

    Returns ``[]`` for NaN, empty strings, malformed JSON, and non-list payloads.
    """
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass

    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except Exception:
        return []

    if not isinstance(parsed, list):
        return []
    return [x for x in parsed if isinstance(x, dict)]
