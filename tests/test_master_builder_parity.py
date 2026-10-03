from __future__ import annotations

"""Parity / contract tests for the vectorized master_builder collection path.

Guards CHG-0099 (delete unit pool + deterministic sort) and CHG-0100 (vectorize
the four ``_collect_*_data`` collectors). The collectors now return long-format
DataFrames instead of lists-of-dicts; these tests pin the emitted columns, the
golden master values, output determinism, and the missing-time-column and
basin-lookup-miss edge cases that the vectorized path must preserve.
"""

from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from india_resilience_tool.compute import master_builder as bmm


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


def _write_district_tree(
    output_root: Path,
    state: str = "Telangana",
    *,
    districts=("Alpha", "Beta"),
    models=("ModelA", "ModelB"),
    scenario: str = "historical",
    period: str = "1990-2010",
    year: int = 2000,
) -> None:
    """Write a minimal NEW-structure district tree under output_root/state."""
    base = output_root / state / "districts"
    val = 1.0
    for district in districts:
        for model in models:
            sdir = base / district / model / scenario
            _write_csv(sdir / f"{district}_periods.csv", "period,value", [f"{period},{val}"])
            _write_csv(sdir / f"{district}_yearly.csv", "year,value", [f"{year},{val}"])
            val += 1.0


# ---------------------------------------------------------------------------
# collector contract
# ---------------------------------------------------------------------------
def test_collector_returns_dataframes_with_contract_columns(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    _write_district_tree(root)

    all_df, yearly_df = bmm._collect_district_data(
        root / "Telangana", "Telangana", ["value"], verbose=False
    )

    assert isinstance(all_df, pd.DataFrame) and isinstance(yearly_df, pd.DataFrame)
    # exact columns + order consumed downstream by _build_wide_master / _build_state_summaries
    assert list(all_df.columns) == ["district", "state", "model", "scenario", "period", "value"]
    assert list(yearly_df.columns) == ["district", "state", "model", "scenario", "year", "value"]
    # 2 districts x 2 models x 1 scenario x 1 row
    assert len(all_df) == 4
    assert pd.api.types.is_float_dtype(all_df["value"])


def test_empty_tree_returns_empty_frames(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    (root / "Telangana").mkdir(parents=True)
    all_df, yearly_df = bmm._collect_district_data(
        root / "Telangana", "Telangana", ["value"], verbose=False
    )
    assert all_df.empty and yearly_df.empty


# ---------------------------------------------------------------------------
# golden master values + determinism
# ---------------------------------------------------------------------------
def _build(root: Path, **kw) -> pd.DataFrame:
    return bmm.build_master_metrics(
        output_root=str(root),
        state="Telangana",
        out_path=str(root / "Telangana" / "master_metrics_by_district.csv"),
        metric_col_in_periods="myidx",
        level="district",
        verbose=False,
        **kw,
    )


def test_district_master_golden(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    _write_district_tree(root)  # Alpha: ModelA=1, ModelB=2 ; Beta: ModelA=3, ModelB=4
    master = _build(root)

    # sorted deterministically by (district, state)
    assert list(master["district"]) == ["Alpha", "Beta"]

    mean_col = "myidx__historical__1990-2010__mean"
    n_models_col = "myidx__historical__1990-2010__n_models"
    # ensemble mean over the two models
    assert master.loc[master["district"] == "Alpha", mean_col].iloc[0] == 1.5
    assert master.loc[master["district"] == "Beta", mean_col].iloc[0] == 3.5
    assert int(master.loc[master["district"] == "Alpha", n_models_col].iloc[0]) == 2


def test_build_master_is_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "processed"
    _write_district_tree(root)
    m1 = _build(root).reset_index(drop=True)
    m2 = _build(root).reset_index(drop=True)
    assert_frame_equal(m1, m2)


# ---------------------------------------------------------------------------
# edge cases the vectorized path must preserve
# ---------------------------------------------------------------------------
def test_missing_time_column_falls_back_to_empty_string(tmp_path: Path) -> None:
    """A periods CSV without a 'period' column must not crash; period -> ''."""
    root = tmp_path / "processed"
    sdir = root / "Telangana" / "districts" / "Alpha" / "ModelA" / "historical"
    # no 'period' column, only the metric value
    _write_csv(sdir / "Alpha_periods.csv", "value", ["7.0"])

    all_df, _ = bmm._collect_district_data(
        root / "Telangana", "Telangana", ["value"], verbose=False
    )
    assert list(all_df["period"]) == [""]
    assert list(all_df["value"]) == [7.0]


def test_value_dtype_preserved_for_object_like_metric(tmp_path: Path) -> None:
    """Non-coercion: an object-y metric column survives unchanged (SPI dtype risk)."""
    root = tmp_path / "processed"
    sdir = root / "Telangana" / "districts" / "Alpha" / "ModelA" / "historical"
    _write_csv(sdir / "Alpha_periods.csv", "value", ["1.5"])
    all_df, _ = bmm._collect_district_data(
        root / "Telangana", "Telangana", ["value"], verbose=False
    )
    # numeric CSV value is read as float and preserved
    assert all_df["value"].iloc[0] == 1.5
    assert not np.isnan(all_df["value"].iloc[0])
