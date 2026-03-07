"""
Unit tests for per-model yearly timeseries discovery + loading (spaghetti support).

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.analysis.timeseries import load_unit_yearly_models_from_files
from india_resilience_tool.data.discovery import (
    discover_block_model_yearly_files,
    discover_district_model_yearly_files,
)


def test_discover_district_model_yearly_files(tmp_path: Path) -> None:
    ts_root = tmp_path / "processed"
    state_dir = "Telangana"
    district = "Alpha"
    scenario = "ssp245"

    f1 = ts_root / state_dir / "districts" / district / "ModelA" / scenario / f"{district}_yearly.csv"
    f2 = ts_root / state_dir / "districts" / district / "ModelB" / scenario / f"{district}_yearly.csv"
    f1.parent.mkdir(parents=True, exist_ok=True)
    f2.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"year": [2020], "value": [1.0]}).to_csv(f1, index=False)
    pd.DataFrame({"year": [2020], "value": [2.0]}).to_csv(f2, index=False)

    found = discover_district_model_yearly_files(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display=district,
        scenario_name=scenario,
        varcfg={},
    )
    assert set(found.keys()) == {"ModelA", "ModelB"}
    assert found["ModelA"].resolve() == f1.resolve()
    assert found["ModelB"].resolve() == f2.resolve()


def test_discover_block_model_yearly_files(tmp_path: Path) -> None:
    ts_root = tmp_path / "processed"
    state_dir = "Telangana"
    district = "Alpha"
    block = "Beta"
    scenario = "ssp585"

    f1 = ts_root / state_dir / "blocks" / district / block / "ModelA" / scenario / f"{block}_yearly.csv"
    f1.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"year": [2020], "value": [3.0]}).to_csv(f1, index=False)

    found = discover_block_model_yearly_files(
        ts_root=ts_root,
        state_dir=state_dir,
        district_display=district,
        block_display=block,
        scenario_name=scenario,
        varcfg={},
    )
    assert set(found.keys()) == {"ModelA"}
    assert found["ModelA"].resolve() == f1.resolve()


def test_load_unit_yearly_models_from_files_tidy_output(tmp_path: Path) -> None:
    f1 = tmp_path / "ModelA.csv"
    f2 = tmp_path / "ModelB.csv"

    # ModelA uses canonical "value"
    pd.DataFrame({"year": [1990, 1991], "value": [1.0, 1.2]}).to_csv(f1, index=False)
    # ModelB omits "value" but provides a numeric metric column
    pd.DataFrame({"year": [1990, 1991], "tas": [0.9, 1.1]}).to_csv(f2, index=False)

    out = load_unit_yearly_models_from_files(
        [("ModelA", str(f1)), ("ModelB", str(f2))],
        scenario_name="historical",
        level="district",
        state_dir="Telangana",
        district_display="Alpha",
    )
    assert not out.empty
    assert set(out.columns).issuperset({"year", "value", "model", "scenario", "state", "district"})
    assert set(out["model"].unique()) == {"ModelA", "ModelB"}
    assert (out["scenario"].astype(str).str.lower() == "historical").all()

