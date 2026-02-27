from __future__ import annotations

from pathlib import Path

import pandas as pd

import build_master_metrics as bmm


def test_build_state_summaries_schema_and_year_type() -> None:
    df_all = pd.DataFrame(
        [
            {"state": "S", "district": "D1", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 1.0},
            {"state": "S", "district": "D2", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 3.0},
            {"state": "S", "district": "D1", "scenario": "ssp245", "period": "2020-2040", "model": "m2", "value": 2.0},
            {"state": "S", "district": "D2", "scenario": "ssp245", "period": "2020-2040", "model": "m2", "value": 4.0},
        ]
    )
    df_yearly = pd.DataFrame(
        [
            {"state": "S", "district": "D1", "scenario": "historical", "year": "2000", "model": "m1", "value": 1.0},
            {"state": "S", "district": "D2", "scenario": "historical", "year": "2000", "model": "m1", "value": 3.0},
            {"state": "S", "district": "D1", "scenario": "ssp245", "year": "2030", "model": "m2", "value": 2.0},
            {"state": "S", "district": "D2", "scenario": "ssp245", "year": "2030", "model": "m2", "value": 4.0},
        ]
    )

    model_df, ens_df, yearly_model_df, yearly_ens_df = bmm._build_state_summaries(
        df_all,
        df_yearly,
        "dummy_metric",
        "district",
    )

    assert set(model_df.columns) == {"scenario", "period", "model", "value", "n_units"}
    assert set(ens_df.columns) == {
        "scenario",
        "period",
        "ensemble_mean",
        "ensemble_std",
        "ensemble_median",
        "ensemble_p05",
        "ensemble_p95",
        "n_models",
        "n_units",
    }
    assert set(yearly_model_df.columns) == {"scenario", "year", "model", "value", "n_units"}
    assert set(yearly_ens_df.columns) == {
        "scenario",
        "year",
        "ensemble_mean",
        "ensemble_std",
        "ensemble_median",
        "ensemble_p05",
        "ensemble_p95",
        "n_models",
        "n_units",
    }
    assert pd.api.types.is_integer_dtype(yearly_model_df["year"])
    assert pd.api.types.is_integer_dtype(yearly_ens_df["year"])


def test_build_master_metrics_writes_level_specific_state_files(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "processed"
    state_root = root / "Telangana"
    state_root.mkdir(parents=True)

    all_rows = [
        {"state": "Telangana", "district": "D1", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 1.0},
    ]
    yearly_rows = [
        {"state": "Telangana", "district": "D1", "scenario": "historical", "year": 2000, "model": "m1", "value": 1.0},
    ]

    monkeypatch.setattr(bmm, "_collect_district_data", lambda *args, **kwargs: (all_rows, yearly_rows))
    monkeypatch.setattr(bmm, "_build_wide_master", lambda *args, **kwargs: pd.DataFrame([{"state": "Telangana", "district": "D1"}]))

    outp = state_root / "master_metrics_by_district.csv"
    bmm.build_master_metrics(
        output_root=str(root),
        state="Telangana",
        out_path=str(outp),
        level="district",
        verbose=False,
    )

    assert (state_root / "state_model_averages_district.csv").exists()
    assert (state_root / "state_ensemble_stats_district.csv").exists()
    assert (state_root / "state_yearly_model_averages_district.csv").exists()
    assert (state_root / "state_yearly_ensemble_stats_district.csv").exists()
    assert not (state_root / "state_model_averages.csv").exists()
    assert not (state_root / "state_ensemble_stats.csv").exists()


def test_build_master_metrics_writes_level_specific_state_files_block(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "processed"
    state_root = root / "Telangana"
    state_root.mkdir(parents=True)

    all_rows = [
        {"state": "Telangana", "district": "D1", "block": "B1", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 1.0},
    ]
    yearly_rows = [
        {"state": "Telangana", "district": "D1", "block": "B1", "scenario": "historical", "year": 2000, "model": "m1", "value": 1.0},
    ]

    monkeypatch.setattr(bmm, "_collect_block_data", lambda *args, **kwargs: (all_rows, yearly_rows))
    monkeypatch.setattr(bmm, "_build_wide_master", lambda *args, **kwargs: pd.DataFrame([{"state": "Telangana", "district": "D1", "block": "B1"}]))

    outp = state_root / "master_metrics_by_block.csv"
    bmm.build_master_metrics(
        output_root=str(root),
        state="Telangana",
        out_path=str(outp),
        level="block",
        verbose=False,
    )

    assert (state_root / "state_model_averages_block.csv").exists()
    assert (state_root / "state_ensemble_stats_block.csv").exists()
    assert (state_root / "state_yearly_model_averages_block.csv").exists()
    assert (state_root / "state_yearly_ensemble_stats_block.csv").exists()
