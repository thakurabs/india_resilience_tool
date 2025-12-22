"""
Unit tests for timeseries discovery + loading.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.data.discovery import discover_district_yearly_file
from india_resilience_tool.analysis.timeseries import load_district_yearly


def test_discover_district_yearly_direct_candidate(tmp_path: Path) -> None:
    ts_root = tmp_path / "processed"
    state_dir = "Telangana"
    scenario = "ssp585"
    district = "Alpha"
    district_u = district.replace(" ", "_")

    f = ts_root / state_dir / district_u / "ensembles" / scenario / f"{district_u}_yearly_ensemble.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"year": [2020, 2021], "mean": [1.0, 2.0]}).to_csv(f, index=False)

    varcfg = {
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv"
        ]
    }

    found = discover_district_yearly_file(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display=district,
        scenario_name=scenario,
        varcfg=varcfg,
    )
    assert found is not None
    assert found.resolve() == f.resolve()


def test_discover_district_yearly_folder_fallback(tmp_path: Path) -> None:
    ts_root = tmp_path / "processed"
    state_dir = "Telangana"
    district_folder = "Alpha"
    f = ts_root / state_dir / district_folder / "district_yearly_ensemble_stats.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"district": ["Alpha"], "scenario": ["ssp585"], "year": [2020], "mean": [3.0]}).to_csv(f, index=False)

    varcfg = {"district_yearly_candidates": []}

    found = discover_district_yearly_file(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display="Alpha",
        scenario_name="ssp585",
        varcfg=varcfg,
    )
    assert found is not None
    assert found.resolve() == f.resolve()


def test_load_district_yearly_infers_missing_cols(tmp_path: Path) -> None:
    ts_root = tmp_path / "processed"
    state_dir = "Telangana"
    scenario = "ssp585"
    district = "Alpha"
    district_u = district.replace(" ", "_")

    f = ts_root / state_dir / district_u / "ensembles" / scenario / f"{district_u}_yearly_ensemble.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    # no district/scenario columns in this file (common in some pipelines)
    pd.DataFrame({"year": [2020, 2021], "mean": [10.0, 12.0]}).to_csv(f, index=False)

    varcfg = {
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv"
        ]
    }

    out = load_district_yearly(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display=district,
        scenario_name=scenario,
        varcfg=varcfg,
    )
    assert out.shape[0] == 2
    assert set(out.columns).issuperset({"district", "scenario", "year", "mean"})
    assert (out["scenario"].astype(str).str.lower() == "ssp585").all()
