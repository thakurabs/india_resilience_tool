"""
Yearly time-series loading + cleaning utilities for IRT.

This module centralizes:
- encoding-robust yearly CSV read
- district yearly ensemble discovery + load
- state yearly ensemble stats discovery + load
- minimal cleaning for chart readiness

Streamlit-free: caching belongs in app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional, Union

import pandas as pd

from india_resilience_tool.data.discovery import (
    discover_district_yearly_file,
    discover_state_yearly_file,
)

PathLike = Union[str, Path]


def read_yearly_csv_robust(path: PathLike) -> pd.DataFrame:
    """
    Read a yearly CSV with encoding fallbacks.
    """
    fpath = Path(path)
    if not fpath.exists():
        return pd.DataFrame()

    for enc in (None, "ISO-8859-1"):
        try:
            return pd.read_csv(fpath, encoding=enc) if enc else pd.read_csv(fpath)
        except Exception:
            pass

    try:
        return pd.read_csv(fpath, encoding="utf-8", errors="replace")
    except Exception:
        return pd.DataFrame()


def _normalize_ensemble_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names from ensemble CSVs to standard names.
    
    Maps:
    - ensemble_mean -> mean
    - ensemble_std -> std
    - ensemble_median -> median
    - ensemble_p05 -> p05
    - ensemble_p95 -> p95
    """
    if df is None or df.empty:
        return df
    
    out = df.copy()
    
    # Column mapping: ensemble format -> standard format
    rename_map = {
        "ensemble_mean": "mean",
        "ensemble_std": "std",
        "ensemble_median": "median",
        "ensemble_p05": "p05",
        "ensemble_p95": "p95",
    }
    
    # Only rename columns that exist
    columns_to_rename = {k: v for k, v in rename_map.items() if k in out.columns}
    
    if columns_to_rename:
        out = out.rename(columns=columns_to_rename)
    
    return out


def prepare_yearly_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning:
      - normalize ensemble column names (ensemble_mean -> mean)
      - ensure year numeric
      - ensure mean numeric
      - drop rows missing year/mean
      - sort by year
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # First normalize column names
    out = _normalize_ensemble_columns(df)

    if "year" in out.columns:
        out["year"] = pd.to_numeric(out["year"], errors="coerce")
    if "mean" in out.columns:
        out["mean"] = pd.to_numeric(out["mean"], errors="coerce")

    required = {"year", "mean"}
    if not required.issubset(set(map(str, out.columns))):
        return pd.DataFrame()

    out = out.dropna(subset=["year", "mean"]).sort_values("year").reset_index(drop=True)
    return out


def load_state_yearly(
    *,
    ts_root: PathLike,
    state_dir: str,
    varcfg: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Load state yearly ensemble stats CSV for a given state_dir.
    """
    f = discover_state_yearly_file(ts_root=ts_root, state_dir=state_dir, varcfg=varcfg)
    if not f:
        return pd.DataFrame()

    df = read_yearly_csv_robust(f)
    
    # Normalize ensemble column names
    df = _normalize_ensemble_columns(df)
    
    return df


def load_district_yearly(
    *,
    ts_root: PathLike,
    state_dir: str,
    district_display: str,
    scenario_name: str,
    varcfg: Mapping[str, Any],
    aliases: Optional[dict[str, str]] = None,
    normalize_fn: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Load a scenario-specific district yearly CSV using robust discovery.

    Behavior-preserving notes:
      - If the loaded file has a 'scenario' column, filter it to scenario_name.
      - If it lacks 'scenario'/'district' columns, infer them from inputs.
      - Requires at least 'year' and 'mean' to return non-empty.
    """
    f = discover_district_yearly_file(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display=district_display,
        scenario_name=scenario_name,
        varcfg=varcfg,
        aliases=aliases,
        normalize_fn=normalize_fn,
    )
    if not f:
        return pd.DataFrame()

    df = read_yearly_csv_robust(f)
    if df.empty:
        return pd.DataFrame()

    # Normalize ensemble column names (ensemble_mean -> mean, etc.)
    df = _normalize_ensemble_columns(df)

    # Infer missing id columns
    if "district" not in df.columns:
        df["district"] = str(district_display).strip()
    if "scenario" not in df.columns:
        df["scenario"] = str(scenario_name).strip()

    # If scenario column exists, filter strictly (matches existing behavior)
    scenario = str(scenario_name).strip().lower()
    try:
        df["scenario"] = df["scenario"].astype(str).str.strip()
        df = df[df["scenario"].str.lower() == scenario]
    except Exception:
        pass

    df = prepare_yearly_series(df)
    if df.empty:
        return pd.DataFrame()

    return df