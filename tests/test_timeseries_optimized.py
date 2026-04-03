from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.analysis.timeseries import (
    load_block_yearly,
    load_district_yearly,
    load_hydro_yearly,
    load_state_yearly,
    load_unit_yearly_models,
)


def test_load_district_yearly_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    path = metric_root / "yearly_ensemble" / "admin" / "district" / "state=Telangana.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "district_key": ["telangana|hanumakonda", "telangana|hanumakonda"],
            "scenario": ["historical", "ssp245"],
            "year": [2000, 2030],
            "mean": [1.0, 2.0],
            "median": [0.9, 1.9],
        }
    ).to_parquet(path, index=False)

    out = load_district_yearly(
        ts_root=metric_root,
        state_dir="Telangana",
        district_display="Hanumakonda",
        scenario_name="ssp245",
        varcfg={},
        normalize_fn=lambda s: str(s).strip().lower(),
    )

    assert out["year"].tolist() == [2030]
    assert out["mean"].tolist() == [2.0]


def test_load_state_yearly_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    path = metric_root / "yearly_ensemble" / "admin" / "district" / "state=Telangana.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "district_key": ["telangana|a", "telangana|b"],
            "scenario": ["historical", "historical"],
            "year": [2000, 2000],
            "mean": [1.0, 3.0],
            "median": [0.5, 2.5],
        }
    ).to_parquet(path, index=False)

    out = load_state_yearly(
        ts_root=metric_root,
        state_dir="Telangana",
        level="district",
    )

    assert out["scenario"].tolist() == ["historical"]
    assert out["mean"].tolist() == [2.0]
    assert out["median"].tolist() == [1.5]


def test_load_block_yearly_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    path = metric_root / "yearly_ensemble" / "admin" / "block" / "state=Telangana.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "block_key": ["telangana|hanumakonda|atmakur"],
            "scenario": ["ssp245"],
            "year": [2030],
            "mean": [2.5],
        }
    ).to_parquet(path, index=False)

    out = load_block_yearly(
        ts_root=metric_root,
        state_dir="Telangana",
        district_display="Hanumakonda",
        block_display="Atmakur",
        scenario_name="ssp245",
        varcfg={},
        normalize_fn=lambda s: str(s).strip().lower(),
    )

    assert out["year"].tolist() == [2030]
    assert out["mean"].tolist() == [2.5]


def test_load_unit_yearly_models_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    path = metric_root / "yearly_models" / "admin" / "district" / "state=Telangana.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "district_key": ["telangana|hanumakonda", "telangana|hanumakonda"],
            "scenario": ["ssp245", "ssp245"],
            "model": ["A", "B"],
            "year": [2030, 2030],
            "value": [1.0, 2.0],
        }
    ).to_parquet(path, index=False)

    out = load_unit_yearly_models(
        ts_root=metric_root,
        level="district",
        state_dir="Telangana",
        district_display="Hanumakonda",
        scenario_name="ssp245",
        normalize_fn=lambda s: str(s).strip().lower(),
    )

    assert sorted(out["model"].tolist()) == ["A", "B"]
    assert out["value"].sum() == 3.0


def test_load_hydro_yearly_from_optimized_metric_root(tmp_path: Path) -> None:
    metric_root = tmp_path / "metrics" / "txx_annual_max"
    path = metric_root / "yearly_ensemble" / "hydro" / "basin" / "master.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "basin_name": ["Godavari Basin", "Godavari Basin"],
            "scenario": ["historical", "ssp245"],
            "year": [2000, 2030],
            "mean": [1.0, 2.0],
        }
    ).to_parquet(path, index=False)

    out = load_hydro_yearly(
        ts_root=metric_root,
        level="basin",
        basin_display="Godavari Basin",
        subbasin_display=None,
        scenario_name="ssp245",
    )

    assert out["year"].tolist() == [2030]
    assert out["mean"].tolist() == [2.0]
