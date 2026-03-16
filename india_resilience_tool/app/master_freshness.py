"""
Master CSV freshness helpers (app-layer; Streamlit caching).

These helpers are variable-agnostic and are used by the metric ribbon to decide:
- whether the master table should be rebuilt
- whether required "state profile" summary files exist for the chosen admin level
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

@st.cache_data(ttl=300)
def latest_processed_periods_mtime(processed_root_str: str, state: str) -> float:
    """
    Return the latest mtime among model-period Parquet artifacts under `{processed_root}/{state}`.

    Note:
        Streamlit cache keys must be hashable; accept `processed_root_str` rather
        than a Path.
    """
    base = Path(processed_root_str) / str(state)
    if not base.exists():
        return 0.0

    latest = 0.0
    count = 0
    candidate_roots = (
        base / "districts" / "models" / "periods",
        base / "blocks" / "models" / "periods",
        Path(processed_root_str) / "hydro" / "basins" / "models" / "periods",
        Path(processed_root_str) / "hydro" / "sub_basins" / "models" / "periods",
    )
    for root in candidate_roots:
        if not root.exists():
            continue
        for f in root.rglob("*.parquet"):
            try:
                latest = max(latest, f.stat().st_mtime)
                count += 1
                if count >= 200:
                    break
            except Exception:
                pass
        if count >= 200:
            break
    return float(latest)


def master_needs_rebuild(master_path: Path, processed_root: Path, state: str) -> bool:
    """Return True when processed artifacts are newer than the master table."""
    from india_resilience_tool.utils.processed_io import resolve_preferred_table_path

    resolved_master = resolve_preferred_table_path(Path(master_path))
    if not resolved_master.exists():
        return True
    try:
        master_mtime = resolved_master.stat().st_mtime
    except Exception:
        return True
    return latest_processed_periods_mtime(str(processed_root), state) > (master_mtime + 1.0)


def state_profile_files_missing(processed_root: Path, state: str, level: str) -> bool:
    """Return True when required level-specific state profile files are missing."""
    from india_resilience_tool.utils.processed_io import resolve_preferred_table_path

    level_norm = str(level or "district").strip().lower()
    state_root = Path(processed_root) / str(state)
    required = (
        state_root / f"state_yearly_ensemble_stats_{level_norm}.csv",
        state_root / f"state_ensemble_stats_{level_norm}.csv",
    )
    return any(not resolve_preferred_table_path(p).exists() for p in required)
