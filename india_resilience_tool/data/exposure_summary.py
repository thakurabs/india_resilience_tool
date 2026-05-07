"""Load lightweight admin exposure summaries for the dashboard.

This module is intentionally Streamlit-free. Runtime caching belongs in
``india_resilience_tool.app.summary_cache``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd

PathLike = Union[str, Path]

ADMIN_EXPOSURE_REQUIRED_COLUMNS = (
    "admin_key",
    "admin_level",
    "state_name",
    "district_name",
    "block_name",
    "pop_2020",
    "parent_pop_2020",
    "parent_level",
    "parent_name",
    "population_share_parent_pct",
)


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=list(ADMIN_EXPOSURE_REQUIRED_COLUMNS))


def load_admin_exposure_summary(path: PathLike) -> pd.DataFrame:
    """Load and lightly validate ``admin_exposure_summary.parquet``.

    Dashboard runtime should degrade gracefully when optional context summaries
    are absent, stale, or malformed. For that reason this loader returns an empty
    frame instead of raising for missing files, unreadable parquet, or missing
    required columns.
    """
    p = Path(path)
    if not p.exists():
        return _empty()

    try:
        df = pd.read_parquet(p)
    except Exception:
        return _empty()

    missing = [c for c in ADMIN_EXPOSURE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return _empty()

    out = df.copy()
    out["admin_key"] = out["admin_key"].astype(str).str.strip()
    out["admin_level"] = out["admin_level"].astype(str).str.strip().str.lower()
    out["block_name"] = out["block_name"].fillna("").astype(str)

    for c in ("pop_2020", "parent_pop_2020", "population_share_parent_pct"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in (
        "rural_facilities_total_count",
        "rural_facilities_agro_count",
        "rural_facilities_education_count",
        "rural_facilities_health_count",
        "rural_facilities_service_count",
        "rural_facilities_total_count_per_100k",
    ):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def slice_exposure_for_admin_key(
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
