from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.analysis.timeseries import load_district_yearly, load_hydro_yearly, load_state_yearly
from india_resilience_tool.data.discovery import (
    discover_district_yearly_file,
    discover_hydro_yearly_file,
    discover_state_period_ensemble_file,
    discover_state_yearly_file,
)


def test_discover_and_load_state_yearly_parquet(tmp_path: Path) -> None:
    state_root = tmp_path / "Telangana"
    state_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"scenario": ["ssp585"], "year": [2030], "ensemble_mean": [1.2]}).to_parquet(
        state_root / "state_yearly_ensemble_stats_district.parquet",
        index=False,
    )
    pd.DataFrame({"scenario": ["ssp585"], "period": ["2020-2040"], "ensemble_mean": [1.3]}).to_parquet(
        state_root / "state_ensemble_stats_district.parquet",
        index=False,
    )

    yearly = discover_state_yearly_file(ts_root=tmp_path, state_dir="Telangana", level="district")
    period = discover_state_period_ensemble_file(ts_root=tmp_path, state_dir="Telangana", level="district")

    assert yearly == state_root / "state_yearly_ensemble_stats_district.parquet"
    assert period == state_root / "state_ensemble_stats_district.parquet"

    df = load_state_yearly(ts_root=tmp_path, state_dir="Telangana", level="district")
    assert list(df["mean"]) == [1.2]


def test_discover_and_load_district_yearly_dataset(tmp_path: Path) -> None:
    dataset_root = tmp_path / "Telangana" / "districts" / "ensembles" / "yearly"
    pd.DataFrame(
        {
            "district": ["Alpha", "Beta"],
            "scenario": ["ssp585", "ssp245"],
            "year": [2030, 2030],
            "mean": [4.0, 2.0],
        }
    ).to_parquet(dataset_root, partition_cols=["scenario"], index=False)

    discovered = discover_district_yearly_file(
        ts_root=tmp_path,
        state_dir="Telangana",
        district_display="Alpha",
        scenario_name="ssp585",
        varcfg={},
    )
    assert discovered == dataset_root

    df = load_district_yearly(
        ts_root=tmp_path,
        state_dir="Telangana",
        district_display="Alpha",
        scenario_name="ssp585",
        varcfg={},
    )
    assert list(df["year"]) == [2030]
    assert list(df["mean"]) == [4.0]


def test_discover_and_load_hydro_yearly_dataset(tmp_path: Path) -> None:
    dataset_root = tmp_path / "hydro" / "basins" / "ensembles" / "yearly"
    pd.DataFrame(
        {
            "basin": ["Godavari", "Krishna"],
            "scenario": ["ssp585", "ssp245"],
            "year": [2030, 2030],
            "mean": [9.0, 3.0],
        }
    ).to_parquet(dataset_root, partition_cols=["scenario"], index=False)

    discovered = discover_hydro_yearly_file(
        ts_root=tmp_path,
        level="basin",
        basin_display="Godavari",
        subbasin_display=None,
        scenario_name="ssp585",
    )
    assert discovered == dataset_root

    df = load_hydro_yearly(
        ts_root=tmp_path,
        level="basin",
        basin_display="Godavari",
        subbasin_display=None,
        scenario_name="ssp585",
    )
    assert list(df["year"]) == [2030]
    assert list(df["mean"]) == [9.0]
