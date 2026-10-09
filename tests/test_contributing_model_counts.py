"""Contributing-model counts in the optimized bundle and the map tooltip (CHG-0673).

An outdoor-WBGT value can rest on fewer models than the nominal roster, because a
model contributes only where it has the whole period (master) or that exact year
(yearly ensemble). The count published beside the value must be the number that
actually contributed, measured at the same aggregation level as the value, and it
must be absent -- not zero, and not the roster size -- for every metric that does
not publish it.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.analysis.map_enrichment import add_tooltip_strings
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG, get_dashboard_variables
from india_resilience_tool.data.master_loader import parse_master_schema_obj
from india_resilience_tool.viz.folium_featurecollection import build_geojson_tooltip
from tools.optimized.build_processed_optimised import (
    _build_yearly_ensemble_from_models,
    _empty_yearly_ensemble_frame,
    _normalize_legacy_ensemble_df,
    _select_master_columns,
    _sources_carry_n_models,
)

ROUTE_B_SLUGS = (
    "wbgt_outdoor_route_b_annual_mean",
    "wbgt_outdoor_route_b_annual_max",
    "wbgt_outdoor_route_b_days_ge_28",
    "wbgt_outdoor_route_b_days_ge_30",
    "wbgt_outdoor_route_b_days_ge_32",
)

VALUE_COL = "wbgt_outdoor_route_b_annual_mean_C"
PREFIX = f"{VALUE_COL}__ssp245__2040-2060__"


def _master(**extra: object) -> pd.DataFrame:
    base = {
        "state": ["Telangana"],
        "district": ["Hyderabad"],
        PREFIX + "mean": [27.5],
        PREFIX + "median": [27.4],
        PREFIX + "std": [0.4],
    }
    base.update(extra)
    return pd.DataFrame(base)


# --- Registry declaration -------------------------------------------------------

def test_route_b_metrics_declare_the_retained_count() -> None:
    for slug in ROUTE_B_SLUGS:
        spec = METRICS_BY_SLUG[slug]
        assert spec.retained_master_statistics == ("n_models",), slug
        # The count must NOT be selectable as a statistic: the ribbon builds its
        # Statistic dropdown straight from supported_statistics.
        assert "n_models" not in spec.supported_statistics, slug


def test_no_other_metric_declares_a_retained_statistic() -> None:
    declaring = {
        slug
        for slug, spec in METRICS_BY_SLUG.items()
        if spec.retained_master_statistics
    }
    assert declaring == set(ROUTE_B_SLUGS)


def test_dashboard_variables_expose_the_declaration() -> None:
    variables = get_dashboard_variables()
    assert variables["wbgt_outdoor_route_b_days_ge_32"]["retained_master_statistics"] == ["n_models"]
    # An existing metric reports an empty list, which the builder treats as "retain
    # nothing" -- so its published columns cannot change.
    assert variables["swbgt_empirical_annual_mean"]["retained_master_statistics"] == []


# --- Master retention -----------------------------------------------------------

def test_master_retains_the_count_when_declared() -> None:
    out = _select_master_columns(
        _master(**{PREFIX + "n_models": [18]}),
        slug="wbgt_outdoor_route_b_annual_mean",
        level="district",
        supported_stats=["mean", "median"],
        retained_stats=["n_models"],
    )
    assert PREFIX + "n_models" in out.columns
    assert int(out[PREFIX + "n_models"].iloc[0]) == 18
    # Spread statistics stay dropped: retention is not a licence to publish std.
    assert PREFIX + "std" not in out.columns


def test_master_drops_the_count_when_not_declared() -> None:
    out = _select_master_columns(
        _master(**{PREFIX + "n_models": [18]}),
        slug="swbgt_empirical_annual_mean",
        level="district",
        supported_stats=["mean", "median"],
        retained_stats=[],
    )
    assert PREFIX + "n_models" not in out.columns
    assert PREFIX + "mean" in out.columns


def test_default_retained_stats_keeps_existing_metrics_byte_identical() -> None:
    frame = _master(**{PREFIX + "n_models": [18]})
    without = _select_master_columns(
        frame, slug="hot_days", level="district", supported_stats=["mean", "median"]
    )
    explicit_empty = _select_master_columns(
        frame, slug="hot_days", level="district", supported_stats=["mean", "median"], retained_stats=()
    )
    pd.testing.assert_frame_equal(without, explicit_empty)


def test_retained_count_is_invisible_to_the_master_schema_parser() -> None:
    """The ribbon's metric/scenario/period/statistic discovery must not see it."""
    schema = parse_master_schema_obj(
        [PREFIX + "mean", PREFIX + "median", PREFIX + "n_models"]
    )
    assert schema.stats == ["mean", "median"]
    assert all(item["column"] != PREFIX + "n_models" for item in schema.items)


# --- Yearly-ensemble counts -----------------------------------------------------

def _models_frame(rows: list[tuple[str, int, float | None]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "district_key": ["telangana|hyderabad"] * len(rows),
            "scenario": ["ssp245"] * len(rows),
            "model": [r[0] for r in rows],
            "year": [r[1] for r in rows],
            "value": [r[2] for r in rows],
        }
    )


def test_ensemble_count_is_the_number_of_contributing_models() -> None:
    out = _build_yearly_ensemble_from_models(
        _models_frame([("A", 2040, 1.0), ("B", 2040, 3.0), ("C", 2040, 5.0)]),
        level="district",
    )
    assert int(out.n_models.iloc[0]) == 3
    assert out["mean"].iloc[0] == pytest.approx(3.0)


def test_ensemble_count_excludes_a_model_present_with_no_value() -> None:
    """A partial year: three models have the year, one of them has NaN."""
    out = _build_yearly_ensemble_from_models(
        _models_frame([("A", 2040, 1.0), ("B", 2040, 3.0), ("C", 2040, None)]),
        level="district",
    )
    assert int(out.n_models.iloc[0]) == 2
    # The mean is over contributing models only; a NaN is never averaged as zero.
    assert out["mean"].iloc[0] == pytest.approx(2.0)


def test_ensemble_count_varies_by_year_at_the_rows_own_level() -> None:
    out = _build_yearly_ensemble_from_models(
        _models_frame(
            [("A", 2040, 1.0), ("B", 2040, 2.0), ("A", 2041, 1.0)]
        ),
        level="district",
    ).set_index("year")
    assert int(out.loc[2040, "n_models"]) == 2
    assert int(out.loc[2041, "n_models"]) == 1


def test_ensemble_count_is_integral_and_tracks_the_value_exactly() -> None:
    out = _build_yearly_ensemble_from_models(
        _models_frame([("A", 2040, 1.0), ("B", 2040, None)]), level="district"
    )
    assert pd.api.types.is_integer_dtype(out.n_models)

    # Zero contributions: every model has the year but none has a value. The row is
    # kept with a missing mean and a count of 0 -- an honest "no model contributed",
    # not a zero value and not a borrowed roster size.
    zero = _build_yearly_ensemble_from_models(
        _models_frame([("A", 2040, None), ("B", 2040, None)]), level="district"
    )
    assert int(zero.n_models.iloc[0]) == 0
    assert pd.isna(zero["mean"].iloc[0])

    # The invariant that matters: a count of 0 never sits beside a published value,
    # and a published value never sits beside a count of 0.
    both = _build_yearly_ensemble_from_models(
        _models_frame(
            [("A", 2040, 1.0), ("B", 2040, 3.0), ("A", 2041, None), ("B", 2041, None)]
        ),
        level="district",
    )
    assert ((both.n_models == 0) == both["mean"].isna()).all()


def test_legacy_ensemble_source_count_is_carried_through() -> None:
    out = _normalize_legacy_ensemble_df(
        pd.DataFrame(
            {
                "year": [2040, 2041],
                "n_models": [18, 17],
                "ensemble_mean": [27.0, 27.2],
                "ensemble_std": [0.3, 0.3],
            }
        )
    )
    assert list(out.columns) == ["year", "mean", "n_models"]
    assert list(out.n_models) == [18, 17]
    assert pd.api.types.is_integer_dtype(out.n_models)


def test_legacy_ensemble_source_without_the_count_is_unchanged() -> None:
    out = _normalize_legacy_ensemble_df(
        pd.DataFrame({"year": [2040], "ensemble_mean": [27.0]})
    )
    assert list(out.columns) == ["year", "mean"]


# --- Empty-state schema parity --------------------------------------------------

def test_empty_ensemble_frame_matches_its_populated_siblings(tmp_path: Path) -> None:
    source = tmp_path / "Nicobars_yearly_ensemble.csv"
    pd.DataFrame(columns=["year", "n_models", "ensemble_mean"]).to_csv(source, index=False)
    assert _sources_carry_n_models((source,)) is True

    empty = _empty_yearly_ensemble_frame("district", with_n_models=True)
    populated = _build_yearly_ensemble_from_models(
        _models_frame([("A", 2040, 1.0)]), level="district"
    )
    assert set(empty.columns) == set(populated.columns)
    assert pd.api.types.is_integer_dtype(empty.n_models)


def test_empty_ensemble_frame_omits_the_count_when_sources_lack_it(tmp_path: Path) -> None:
    source = tmp_path / "x_yearly_ensemble.csv"
    pd.DataFrame(columns=["year", "ensemble_mean"]).to_csv(source, index=False)
    assert _sources_carry_n_models((source,)) is False
    assert "n_models" not in _empty_yearly_ensemble_frame("district").columns


def test_unreadable_source_does_not_claim_a_count(tmp_path: Path) -> None:
    broken = tmp_path / "broken.csv"
    broken.write_bytes(b"\x00\x01\x02")
    assert _sources_carry_n_models((broken,)) is False
    assert _sources_carry_n_models(()) is False


# --- Tooltip surfacing ----------------------------------------------------------

def _merged(**extra: object) -> pd.DataFrame:
    base = {"_current_value": [27.5], "_rank_in_state": [3]}
    base.update(extra)
    return pd.DataFrame(base)


def test_tooltip_reports_the_count_for_the_displayed_slice() -> None:
    out = add_tooltip_strings(
        _merged(**{PREFIX + "n_models": [18]}),
        map_mode="Absolute value",
        model_count_col=PREFIX + "n_models",
    )
    assert out["_tooltip_n_models"].iloc[0] == "18"


@pytest.mark.parametrize("count", [None, 0])
def test_tooltip_shows_a_dash_rather_than_zero_contributors(count: object) -> None:
    """Zero contributors reads as no data, not as a measured 0.

    A unit excluded for insufficient reference support has every model available;
    printing "0" there would assert something false about the roster.
    """
    out = add_tooltip_strings(
        _merged(**{PREFIX + "n_models": [count]}),
        map_mode="Absolute value",
        model_count_col=PREFIX + "n_models",
    )
    assert out["_tooltip_n_models"].iloc[0] == "—"


def test_tooltip_omits_the_column_for_a_metric_without_a_count() -> None:
    out = add_tooltip_strings(_merged(), map_mode="Absolute value", model_count_col=PREFIX + "n_models")
    assert "_tooltip_n_models" not in out.columns
    # And with no column requested at all, exactly as every existing caller did.
    out_legacy = add_tooltip_strings(_merged(), map_mode="Absolute value")
    assert "_tooltip_n_models" not in out_legacy.columns


def test_tooltip_fields_only_advertise_the_count_when_present() -> None:
    without = build_geojson_tooltip(
        level="district", map_mode="Absolute value", has_baseline=False, rank_scope_label="state"
    )
    assert "_tooltip_n_models" not in without.fields
    with_count = build_geojson_tooltip(
        level="district",
        map_mode="Absolute value",
        has_baseline=False,
        rank_scope_label="state",
        has_model_count=True,
    )
    assert with_count.fields[-1] == "_tooltip_n_models"
    assert with_count.aliases[-1] == "Models contributing"


# --- State-level aggregation ----------------------------------------------------

def test_state_count_is_the_minimum_over_contributing_units() -> None:
    """An area-weighted mean of counts is not a count; the minimum is."""
    from tools.optimized.build_state_values import _min_contributing_count

    weighted = pd.DataFrame(
        {
            PREFIX + "mean": [27.0, 28.0, 29.0],
            PREFIX + "n_models": [18, 15, 18],
            "_area_weight": [0.8, 0.1, 0.1],
        }
    )
    value, n_units = _min_contributing_count(
        weighted,
        PREFIX + "n_models",
        stat="n_models",
        metric=VALUE_COL,
        scenario="ssp245",
        period="2040-2060",
    )
    assert value == 15 and n_units == 3
    assert isinstance(value, int)


def test_state_count_ignores_units_with_no_published_value() -> None:
    from tools.optimized.build_state_values import _min_contributing_count

    weighted = pd.DataFrame(
        {
            PREFIX + "mean": [27.0, None],
            PREFIX + "n_models": [18, 0],
        }
    )
    value, n_units = _min_contributing_count(
        weighted, PREFIX + "n_models", stat="n_models",
        metric=VALUE_COL, scenario="ssp245", period="2040-2060",
    )
    assert value == 18 and n_units == 1


def test_state_with_no_published_value_reports_no_count() -> None:
    """An all-ineligible territory reports a missing count, never 0 models."""
    from tools.optimized.build_state_values import _min_contributing_count

    weighted = pd.DataFrame(
        {PREFIX + "mean": [None, None], PREFIX + "n_models": [0, 0]}
    )
    value, n_units = _min_contributing_count(
        weighted, PREFIX + "n_models", stat="n_models",
        metric=VALUE_COL, scenario="ssp245", period="2040-2060",
    )
    assert value is None and n_units == 0


# --- Per-unit "why is this missing" provenance (CHG-0674) ------------------------

def test_registry_declares_the_coverage_provenance_columns() -> None:
    for slug in ROUTE_B_SLUGS:
        assert METRICS_BY_SLUG[slug].retained_master_columns == (
            "support_coverage",
            "support_gap_kind",
            "exclusion_reason",
        ), slug


def test_no_other_metric_declares_retained_plain_columns() -> None:
    declaring = {
        slug for slug, spec in METRICS_BY_SLUG.items() if spec.retained_master_columns
    }
    assert declaring == set(ROUTE_B_SLUGS)


def test_master_retains_declared_provenance_columns() -> None:
    frame = _master(
        support_coverage=[0.601],
        support_gap_kind=["reference"],
        exclusion_reason=["coverage_below_gate_reference_support_dominant"],
    )
    out = _select_master_columns(
        frame,
        slug="wbgt_outdoor_route_b_annual_mean",
        level="district",
        supported_stats=["mean", "median"],
        retained_columns=("support_coverage", "support_gap_kind", "exclusion_reason"),
    )
    for column in ("support_coverage", "support_gap_kind", "exclusion_reason"):
        assert column in out.columns, column
    assert out["exclusion_reason"].iloc[0].startswith("coverage_below_gate")


def test_master_drops_provenance_columns_when_not_declared() -> None:
    frame = _master(support_coverage=[0.601], exclusion_reason=["eligible"])
    out = _select_master_columns(
        frame, slug="hot_days", level="district", supported_stats=["mean", "median"]
    )
    assert "support_coverage" not in out.columns
    assert "exclusion_reason" not in out.columns


@pytest.mark.parametrize(
    "reason,expected_fragment",
    [
        ("coverage_below_gate_reference_support_dominant", "reference coverage"),
        ("coverage_below_gate_grid_gap_dominant", "climate grid coverage"),
    ],
)
def test_exclusion_reason_renders_for_a_reader(reason: str, expected_fragment: str) -> None:
    from india_resilience_tool.analysis.map_enrichment import format_exclusion_reason

    text = format_exclusion_reason(reason, coverage=0.601)
    assert expected_fragment in text
    assert text.startswith("No data:")
    assert "60%" in text


def test_eligible_unit_gets_no_reason_text() -> None:
    """An eligible unit with no value is missing for some other reason; don't guess."""
    from india_resilience_tool.analysis.map_enrichment import format_exclusion_reason

    assert format_exclusion_reason("eligible", coverage=1.0) == ""
    assert format_exclusion_reason(None) == ""
    assert format_exclusion_reason("") == ""


def test_unknown_reason_is_passed_through_not_dropped() -> None:
    from india_resilience_tool.analysis.map_enrichment import format_exclusion_reason

    assert format_exclusion_reason("some_future_reason") == "some_future_reason"


def test_tooltip_carries_the_reason_only_when_published() -> None:
    merged = pd.DataFrame(
        {
            "_current_value": [None],
            "_rank_in_state": [None],
            "support_coverage": [0.0],
            "exclusion_reason": ["coverage_below_gate_reference_support_dominant"],
        }
    )
    out = add_tooltip_strings(merged, map_mode="Absolute value")
    assert "reference coverage" in out["_tooltip_no_data_reason"].iloc[0]

    without = add_tooltip_strings(
        pd.DataFrame({"_current_value": [1.0], "_rank_in_state": [1]}),
        map_mode="Absolute value",
    )
    assert "_tooltip_no_data_reason" not in without.columns


def test_tooltip_fields_only_advertise_the_reason_when_present() -> None:
    with_reason = build_geojson_tooltip(
        level="district",
        map_mode="Absolute value",
        has_baseline=False,
        rank_scope_label="state",
        has_no_data_reason=True,
    )
    assert with_reason.fields[-1] == "_tooltip_no_data_reason"
    without = build_geojson_tooltip(
        level="district", map_mode="Absolute value", has_baseline=False, rank_scope_label="state"
    )
    assert "_tooltip_no_data_reason" not in without.fields


# --- CHG-0675: the count must reach the yearly chart, and only where it exists ---


def _write_yearly_ensemble(path, *, with_counts: bool):
    """A minimal per-state yearly-ensemble parquet, with or without the count."""
    frame = pd.DataFrame({
        "district_key": ["telangana__hyderabad"] * 3,
        "scenario": ["ssp245"] * 3,
        "year": [2020, 2021, 2022],
        "mean": [28.1, 28.4, 28.6],
        "median": [28.0, 28.3, 28.5],
    })
    if with_counts:
        frame["n_models"] = pd.Series([18, 18, 17], dtype="int32")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return frame


def test_optional_columns_are_requested_only_when_the_table_has_them(tmp_path):
    from india_resilience_tool.analysis.timeseries import _optional_table_columns

    with_counts = tmp_path / "with.parquet"
    without_counts = tmp_path / "without.parquet"
    _write_yearly_ensemble(with_counts, with_counts=True)
    _write_yearly_ensemble(without_counts, with_counts=False)

    assert _optional_table_columns(with_counts, ["n_models"]) == ["n_models"]
    # A bundle built before the field existed reports it absent, so the loader
    # never asks pyarrow for a column that would raise.
    assert _optional_table_columns(without_counts, ["n_models"]) == []


def test_optional_columns_degrade_quietly_on_an_unreadable_table(tmp_path):
    from india_resilience_tool.analysis.timeseries import _optional_table_columns

    missing = tmp_path / "nope.parquet"
    assert _optional_table_columns(missing, ["n_models"]) == []
    # No optional columns requested is not an error either.
    assert _optional_table_columns(missing, []) == []


def test_yearly_ensemble_loader_carries_the_count_when_present(tmp_path, monkeypatch):
    """The district chart sees ``n_models`` from a bundle that carries it."""
    import india_resilience_tool.analysis.timeseries as ts

    path = tmp_path / "yearly.parquet"
    _write_yearly_ensemble(path, with_counts=True)
    monkeypatch.setattr(ts, "is_optimized_metric_root", lambda _p: True)
    monkeypatch.setattr(ts, "optimized_yearly_ensemble_path_from_metric_root",
                        lambda *a, **k: path)

    out = ts._load_optimized_yearly_table(
        ts_root=tmp_path, level="district", state_dir="Telangana", kind="ensemble",
        columns=["district_key", "scenario", "year", "mean", "median"],
        optional_columns=["n_models"],
    )
    assert "n_models" in out.columns
    assert list(out["n_models"]) == [18, 18, 17]


def test_yearly_ensemble_loader_is_unchanged_for_a_bundle_without_the_count(
        tmp_path, monkeypatch):
    """An existing metric's bundle loads exactly the columns it always did."""
    import india_resilience_tool.analysis.timeseries as ts

    path = tmp_path / "yearly.parquet"
    _write_yearly_ensemble(path, with_counts=False)
    monkeypatch.setattr(ts, "is_optimized_metric_root", lambda _p: True)
    monkeypatch.setattr(ts, "optimized_yearly_ensemble_path_from_metric_root",
                        lambda *a, **k: path)

    base = ["district_key", "scenario", "year", "mean", "median"]
    without = ts._load_optimized_yearly_table(
        ts_root=tmp_path, level="district", state_dir="Telangana", kind="ensemble",
        columns=list(base))
    with_optional = ts._load_optimized_yearly_table(
        ts_root=tmp_path, level="district", state_dir="Telangana", kind="ensemble",
        columns=list(base), optional_columns=["n_models"])
    pd.testing.assert_frame_equal(without, with_optional)
    assert "n_models" not in with_optional.columns


# --- CHG-0679: a fully excluded selection explains itself instead of erroring ---


def _excluded_frame(reason: str = "coverage_below_gate_reference_support_dominant",
                    *, rows: int = 4, coverage: float = 0.12):
    return pd.DataFrame({
        "district_name": [f"unit {i}" for i in range(rows)],
        "value": [float("nan")] * rows,
        "exclusion_reason": [reason] * rows,
        "support_coverage": [coverage] * rows,
    })


def test_a_fully_excluded_selection_is_explained_not_errored():
    """Every unit excluded is a documented outcome, so it gets a reason."""
    from india_resilience_tool.analysis.map_enrichment import (
        summarise_missing_selection,
    )

    message = summarise_missing_selection(_excluded_frame())
    assert message.startswith("No data"), message
    assert "reference coverage" in message
    assert "4 of 4 units" in message
    assert "12%" in message, "the retained coverage is the evidence; keep it"


def test_an_eligible_unit_without_a_value_still_counts_as_an_error():
    """An eligible unit with no value is an anomaly, not a coverage story.

    This is the guard that keeps the new message from explaining away a real
    defect: if any unit claims eligibility yet carries no value, the caller must
    keep reporting an error.
    """
    from india_resilience_tool.analysis.map_enrichment import (
        summarise_missing_selection,
    )

    frame = _excluded_frame(rows=3)
    frame.loc[0, "exclusion_reason"] = "eligible"
    assert summarise_missing_selection(frame) == ""


def test_a_selection_with_no_reason_column_is_not_explained():
    """A metric whose master carries no provenance cannot borrow an explanation."""
    from india_resilience_tool.analysis.map_enrichment import (
        summarise_missing_selection,
    )

    frame = _excluded_frame().drop(columns=["exclusion_reason"])
    assert summarise_missing_selection(frame) == ""
    assert summarise_missing_selection(pd.DataFrame()) == ""


def test_mixed_exclusion_reasons_are_all_reported():
    """Two kinds of gap in one selection name both, with their counts."""
    from india_resilience_tool.analysis.map_enrichment import (
        summarise_missing_selection,
    )

    frame = _excluded_frame(rows=4)
    frame.loc[2:, "exclusion_reason"] = "coverage_below_gate_grid_gap_dominant"
    message = summarise_missing_selection(frame)
    assert "reference coverage" in message
    assert "climate grid coverage" in message
    assert "2 of 4 units" in message
