from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.compute import master_builder as bmm


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


def test_build_wide_master_uses_metric_col_name_and_does_not_error() -> None:
    df_all = pd.DataFrame(
        [
            {
                "state": "Telangana",
                "district": "D1",
                "scenario": "historical",
                "period": "1990-2010",
                "model": "m1",
                "value": 1.0,
            },
            {
                "state": "Telangana",
                "district": "D1",
                "scenario": "historical",
                "period": "1990-2010",
                "model": "m2",
                "value": 3.0,
            },
        ]
    )

    out = bmm._build_wide_master(
        df_all=df_all,
        _metric_col_name="tm_mean",
        level="district",
        num_workers=1,
        verbose=False,
    )

    assert not out.empty
    assert "state" in out.columns
    assert "district" in out.columns
    assert "tm_mean__historical__1990-2010__mean" in out.columns


def test_resolve_scope_name_uses_hydro_root_for_hydro_levels() -> None:
    assert bmm._resolve_scope_name("basin", None, verbose=False) == "hydro"
    assert bmm._resolve_scope_name("sub_basin", "Telangana", verbose=False) == "hydro"


def test_resolve_scope_name_requires_admin_state_for_admin_levels() -> None:
    with pytest.raises(ValueError, match="requires a real admin state"):
        bmm._resolve_scope_name("district", None, verbose=False)


def test_discover_scopes_returns_hydro_root_for_basin_data(tmp_path: Path) -> None:
    metric_root = tmp_path / "tas_annual_mean"
    hydro_root = metric_root / "hydro" / "basins" / "Godavari_Basin" / "CanESM5" / "historical"
    hydro_root.mkdir(parents=True)
    (hydro_root / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,1.0\n")

    assert bmm._discover_scopes(metric_root, "basin") == ["hydro"]


def test_build_master_metrics_hydro_uses_hydro_root_without_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "processed"
    hydro_root = root / "hydro"
    hydro_root.mkdir(parents=True)

    calls: dict[str, object] = {}
    all_rows = [
        {"state": "hydro", "basin": "Godavari Basin", "basin_id": "GOD", "basin_name": "Godavari Basin", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 1.0},
    ]
    yearly_rows = [
        {"state": "hydro", "basin": "Godavari Basin", "basin_id": "GOD", "basin_name": "Godavari Basin", "scenario": "historical", "year": 2000, "model": "m1", "value": 1.0},
    ]

    def _collect(state_root: Path, state: str, metric_col_candidates, verbose: bool = True):
        calls["state_root"] = state_root
        calls["state"] = state
        return all_rows, yearly_rows

    monkeypatch.setattr(bmm, "_collect_basin_data", _collect)
    monkeypatch.setattr(
        bmm,
        "_build_wide_master",
        lambda *args, **kwargs: pd.DataFrame([{"state": "hydro", "basin": "Godavari Basin"}]),
    )

    outp = hydro_root / "master_metrics_by_basin.csv"
    bmm.build_master_metrics(
        output_root=str(root),
        state=None,
        out_path=str(outp),
        level="basin",
        verbose=False,
    )

    assert calls["state_root"] == hydro_root
    assert calls["state"] == "hydro"
    assert outp.exists()


def test_build_master_metrics_hydro_ignores_state_argument(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    root = tmp_path / "processed"
    hydro_root = root / "hydro"
    hydro_root.mkdir(parents=True)

    def _collect(*args, **kwargs):
        return (
            [{"state": "hydro", "basin": "Godavari Basin", "basin_id": "GOD", "basin_name": "Godavari Basin", "scenario": "historical", "period": "1990-2010", "model": "m1", "value": 1.0}],
            [{"state": "hydro", "basin": "Godavari Basin", "basin_id": "GOD", "basin_name": "Godavari Basin", "scenario": "historical", "year": 2000, "model": "m1", "value": 1.0}],
        )

    monkeypatch.setattr(bmm, "_collect_basin_data", _collect)
    monkeypatch.setattr(
        bmm,
        "_build_wide_master",
        lambda *args, **kwargs: pd.DataFrame([{"state": "hydro", "basin": "Godavari Basin"}]),
    )

    outp = hydro_root / "master_metrics_by_basin.csv"
    bmm.build_master_metrics(
        output_root=str(root),
        state="Telangana",
        out_path=str(outp),
        level="basin",
        verbose=True,
    )

    captured = capsys.readouterr()
    assert "--state is ignored for hydro level 'basin'" in captured.out
    assert outp.exists()


def test_build_all_master_metrics_hydro_ignores_state_filter(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    processed_root = tmp_path / "processed"
    metric_root = processed_root / "tas_annual_mean"
    hydro_root = metric_root / "hydro" / "basins" / "Godavari_Basin" / "CanESM5" / "historical"
    hydro_root.mkdir(parents=True)
    (hydro_root / "Godavari_Basin_yearly.csv").write_text("year,value\n2030,1.0\n")

    monkeypatch.setattr(
        bmm,
        "_try_import_registries",
        lambda: ({"tas_annual_mean": {"periods_metric_col": "tm_mean"}}, {"tas_annual_mean": object()}),
    )

    calls: list[tuple[str | None, str, str]] = []

    def _build_master_metrics(*args, **kwargs):
        calls.append((args[1], kwargs["level"], kwargs["out_path"]))
        return pd.DataFrame()

    monkeypatch.setattr(bmm, "build_master_metrics", _build_master_metrics)

    bmm.build_all_master_metrics(
        processed_root,
        level="basin",
        metrics_filter=["tas_annual_mean"],
        state_filter=["Telangana"],
        verbose=True,
    )

    captured = capsys.readouterr()
    assert "NOTE: --state is ignored for hydro level 'basin'" in captured.out
    assert calls == [(None, "basin", str(metric_root / "hydro" / "master_metrics_by_basin.csv"))]
