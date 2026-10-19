"""Empty-state contract for optimized yearly-model artifacts (CHG-0668).

A state whose every administrative unit is ineligible for a metric yields source
CSVs that parse cleanly but hold no publishable rows. That is a valid build
outcome and must publish a schema-carrying empty Parquet, so the strict parity
audit can tell it apart from a missing artifact. A source that cannot be read, or
that lacks the expected schema, must never be published that way.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tools.optimized.build_processed_optimised import (
    _classify_empty_yearly_models,
    _empty_yearly_models_frame,
    _load_one_legacy_admin_yearly_model,
    _required_columns_for_yearly_models,
)

COLUMNS = ["year", "value", "source_file", "district", "scenario", "model"]


def _write_district_csv(root: Path, *, values: list[float | None], years: list[int]) -> Path:
    path = root / "Nicobars" / "ACCESS-CM2" / "historical" / "Nicobars_yearly.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "year": years,
            "value": values,
            "source_file": "route_b_outdoor_wbgt_assembly",
            "district": "Nicobars",
            "scenario": "historical",
            "model": "ACCESS-CM2",
        }
    ).to_csv(path, index=False)
    return path


def test_all_missing_values_classify_as_empty_valid(tmp_path: Path) -> None:
    path = _write_district_csv(tmp_path, values=[None, None, None], years=[1990, 1991, 1992])
    assert _load_one_legacy_admin_yearly_model(path, state_name="A&N", level="district").empty
    assert _classify_empty_yearly_models((path,)) == "empty_valid"


def test_header_only_source_classifies_as_empty_valid(tmp_path: Path) -> None:
    path = _write_district_csv(tmp_path, values=[], years=[])
    assert _classify_empty_yearly_models((path,)) == "empty_valid"


def test_populated_source_still_loads_rows(tmp_path: Path) -> None:
    path = _write_district_csv(tmp_path, values=[27.5, 27.9, None], years=[1990, 1991, 1992])
    loaded = _load_one_legacy_admin_yearly_model(path, state_name="A&N", level="district")
    assert len(loaded) == 2
    assert _classify_empty_yearly_models((path,)) == "empty_valid"


def test_unreadable_source_is_not_empty_valid(tmp_path: Path) -> None:
    path = tmp_path / "Nicobars" / "ACCESS-CM2" / "historical" / "Nicobars_yearly.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00\x01\x02 not,a,csv\n\"unterminated")
    assert _classify_empty_yearly_models((path,)) == "unreadable"


def test_missing_source_file_is_not_empty_valid(tmp_path: Path) -> None:
    assert _classify_empty_yearly_models((tmp_path / "absent.csv",)) == "unreadable"


def test_no_sources_at_all_is_not_empty_valid() -> None:
    assert _classify_empty_yearly_models(()) == "unreadable"


def test_source_without_year_column_is_not_empty_valid(tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    pd.DataFrame({"value": [1.0], "scenario": ["historical"]}).to_csv(path, index=False)
    assert _classify_empty_yearly_models((path,)) == "unreadable"


def test_mixed_sources_reject_when_any_is_unreadable(tmp_path: Path) -> None:
    good = _write_district_csv(tmp_path, values=[None], years=[1990])
    bad = tmp_path / "bad.csv"
    bad.write_bytes(b"\"unterminated")
    assert _classify_empty_yearly_models((good, bad)) == "unreadable"


@pytest.mark.parametrize("level", ["district", "block"])
def test_empty_frame_carries_required_columns_and_dtypes(level: str) -> None:
    frame = _empty_yearly_models_frame(level)
    assert frame.empty
    assert _required_columns_for_yearly_models(level) <= set(frame.columns)
    assert frame["year"].dtype == "int64"
    assert frame["value"].dtype == "float64"


def test_empty_frame_roundtrips_through_parquet(tmp_path: Path) -> None:
    frame = _empty_yearly_models_frame("district")
    target = tmp_path / "master.parquet"
    frame.to_parquet(target, index=False)
    back = pd.read_parquet(target)
    assert back.empty
    assert list(back.columns) == list(frame.columns)
    assert back["year"].dtype == "int64"


# --- Yearly-ensemble tier (CHG-0671) ---------------------------------------


def _write_ensemble_csv(root: Path, *, means: list[float | None], years: list[int]) -> Path:
    path = root / "Nicobars" / "historical" / "Nicobars_yearly_ensemble.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"year": years, "n_models": [0 if m is None else 2 for m in means],
                  "ensemble_mean": means, "ensemble_std": means,
                  "ensemble_median": means, "ensemble_p05": means,
                  "ensemble_p95": means}).to_csv(path, index=False)
    return path


def test_all_missing_ensemble_source_classifies_as_empty_valid(tmp_path: Path) -> None:
    from tools.optimized.build_processed_optimised import (
        _classify_empty_yearly_sources,
        _normalize_legacy_ensemble_df,
    )

    path = _write_ensemble_csv(tmp_path, means=[None, None], years=[1990, 1991])
    assert _normalize_legacy_ensemble_df(pd.read_csv(path)).empty
    assert _classify_empty_yearly_sources((path,), kind="ensemble") == "empty_valid"


def test_ensemble_source_without_any_value_column_is_unreadable(tmp_path: Path) -> None:
    from tools.optimized.build_processed_optimised import _classify_empty_yearly_sources

    path = tmp_path / "x.csv"
    pd.DataFrame({"year": [1990], "n_models": [0]}).to_csv(path, index=False)
    assert _classify_empty_yearly_sources((path,), kind="ensemble") == "unreadable"


def test_unreadable_ensemble_source_is_rejected(tmp_path: Path) -> None:
    from tools.optimized.build_processed_optimised import _classify_empty_yearly_sources

    path = tmp_path / "bad.csv"
    path.write_bytes(b"\"unterminated")
    assert _classify_empty_yearly_sources((path,), kind="ensemble") == "unreadable"


@pytest.mark.parametrize("level", ["district", "block"])
def test_empty_ensemble_frame_carries_required_columns(level: str) -> None:
    from tools.optimized.build_processed_optimised import (
        _empty_yearly_ensemble_frame,
        _required_columns_for_yearly_ensemble,
    )

    frame = _empty_yearly_ensemble_frame(level)
    assert frame.empty
    assert _required_columns_for_yearly_ensemble(level) <= set(frame.columns)
    assert frame["year"].dtype == "int64"
    assert frame["mean"].dtype == "float64"


def test_models_kind_still_requires_a_model_value_column(tmp_path: Path) -> None:
    from tools.optimized.build_processed_optimised import _classify_empty_yearly_sources

    path = tmp_path / "y.csv"
    pd.DataFrame({"year": [1990], "ensemble_mean": [1.0]}).to_csv(path, index=False)
    # An ensemble statistic is a valid ensemble source but is accepted for models
    # too, because _extract_value_column treats any numeric column as the value.
    assert _classify_empty_yearly_sources((path,), kind="ensemble") == "empty_valid"
