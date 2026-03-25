"""
Master CSV freshness helpers (app-layer; Streamlit caching).

These helpers are variable-agnostic and are used by the metric ribbon to decide:
- whether the master CSV should be rebuilt
- whether required "state profile" summary files exist for the chosen admin level
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from india_resilience_tool.data.optimized_bundle import is_optimized_metric_root


@st.cache_data(ttl=300)
def latest_processed_periods_mtime(processed_root_str: str, state: str) -> float:
    """
    Return the latest mtime among `*_periods.csv` under `{processed_root}/{state}`.

    Note:
        Streamlit cache keys must be hashable; accept `processed_root_str` rather
        than a Path.
    """
    base = Path(processed_root_str) / str(state)
    if not base.exists():
        return 0.0

    latest = 0.0
    count = 0
    for f in base.rglob("*_periods.csv"):
        try:
            latest = max(latest, f.stat().st_mtime)
            count += 1
            if count >= 50:
                break
        except Exception:
            pass
    return float(latest)


def master_needs_rebuild(master_path: Path | tuple[Path, ...], processed_root: Path, state: str) -> bool:
    """Return True when processed artifacts are newer than the master serving artifact."""
    if is_optimized_metric_root(processed_root):
        if isinstance(master_path, tuple):
            return not master_path or any(not p.exists() for p in master_path)
        return not Path(master_path).exists()

    if isinstance(master_path, tuple):
        if not master_path:
            return True
        return any(master_needs_rebuild(path, processed_root, state) for path in master_path)

    if not Path(master_path).exists():
        return True
    try:
        master_mtime = Path(master_path).stat().st_mtime
    except Exception:
        return True
    return latest_processed_periods_mtime(str(processed_root), state) > (master_mtime + 1.0)


def state_profile_files_missing(processed_root: Path, state: str, level: str) -> bool:
    """Return True when required level-specific state profile files are missing."""
    if is_optimized_metric_root(processed_root):
        return False

    level_norm = str(level or "district").strip().lower()
    state_root = Path(processed_root) / str(state)
    required = [
        state_root / f"state_yearly_ensemble_stats_{level_norm}.csv",
        state_root / f"state_ensemble_stats_{level_norm}.csv",
    ]
    return any(not p.exists() for p in required)
