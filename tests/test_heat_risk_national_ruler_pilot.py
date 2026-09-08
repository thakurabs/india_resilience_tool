"""Regression guards for the Heat Risk national-ruler pilot (CHG-0349..0360).

Each test pins one defect found in review of a pilot run. All of them were
silent — they produced plausible tables rather than errors — so they are exactly
the class of bug the pilot's own conclusions would have been drawn from.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec
from tools.diagnostics import heat_risk_national_ruler_pilot as pilot


def _spec(slug: str, weight: float) -> BundleMetricSpec:
    return BundleMetricSpec(slug=slug, label=slug, column=slug, weight=weight)


def _write_roster(root, state: str, districts) -> None:
    """Write a minimal district geometry file with the properties the roster needs."""
    path = root / "processed_optimised" / "geometry" / "admin" / "district"
    path.mkdir(parents=True, exist_ok=True)
    features = [
        {
            "type": "Feature",
            "geometry": None,
            "properties": {
                "district_key": key,
                "state_name": state,
                "district_name": name,
                "area_m2": area,
            },
        }
        for key, name, area in districts
    ]
    (path / f"state={state}.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8"
    )


# --- CHG-0350: coverage denominator is the configured bundle weight ------------


def test_coverage_denominator_includes_metric_absent_from_whole_pool():
    """A metric absent everywhere must reduce coverage, not shrink the denominator."""
    specs = [_spec("a", 0.5), _spec("b", 0.3), _spec("missing", 0.2)]
    long_frame = pd.DataFrame(
        {
            "state": ["S"],
            "district": ["D"],
            "district_key": ["s|d"],
            "scenario": ["historical"],
            "period": ["1990-2010"],
            "a": [10.0],
            "b": [20.0],
        }
    )
    rulers = {
        slug: pilot.build_ruler(
            slug, np.array([0.0, 10.0, 20.0, 30.0]), kind="linear", higher_is_worse=True
        )
        for slug in ("a", "b")
    }
    scored = pilot.score_national_frame(
        long_frame,
        metric_specs=specs,
        rulers=rulers,
        id_columns=("state", "district", "district_key"),
        coverage_gate=0.7,
    )
    assert scored["weight_coverage"].iloc[0] == pytest.approx(0.8)


# --- CHG-0351: coverage uses the same finite mask as scoring -------------------


def test_infinite_value_is_not_counted_as_coverage():
    """+inf passes notna() but scores NaN, so it must not inflate coverage."""
    long_frame = pd.DataFrame(
        {
            "state": ["S", "S"],
            "scenario": ["historical", "historical"],
            "period": ["1990-2010", "1990-2010"],
            "a": [10.0, np.inf],
        }
    )
    report = pilot.coverage_report(long_frame, ["a"])
    state_row = report.loc[report["scope"] == "state"].iloc[0]
    assert int(state_row["n_finite"]) == 1


# --- CHG-0352: the pilot is district-only -------------------------------------


def test_block_level_is_rejected_by_the_cli():
    """`--level block` used to crash inside state aggregation; it must not parse."""
    with pytest.raises(SystemExit):
        pilot.main(["--level", "block", "--no-maps"])


# --- CHG-0353: exact empirical mid-rank CDF -----------------------------------


def test_exact_midrank_scores_a_tied_block_at_its_true_midpoint():
    """Ties get 100*(b + c/2)/n, which a 5% quantile grid can only approximate."""
    pool = np.array([0.0] * 33 + list(np.arange(1.0, 68.0)))  # 33 zeros in n=100
    values, scores, counts = pilot._exact_midrank_cdf(pool)
    assert values[0] == 0.0
    assert int(counts[0]) == 33
    assert scores[0] == pytest.approx(100.0 * (0 + 33 / 2) / 100.0)  # 16.5, not the grid's 15.0


def test_grid_approximation_error_is_measured_not_assumed():
    """The 21-knot grid is retained only as a quantified approximation."""
    pool = np.array([0.0] * 33 + list(np.arange(1.0, 68.0)))
    values, scores, _ = pilot._exact_midrank_cdf(pool)
    max_error, mean_error = pilot._grid_approximation_error(pool, values, scores)
    assert max_error > 0.0
    assert 0.0 <= mean_error <= max_error


# --- CHG-0354: canonical roster is the expected universe ----------------------


def test_roster_district_without_master_survives_as_an_uncovered_row(tmp_path):
    """A district in no master must appear NaN, or absence can never be detected."""
    _write_roster(tmp_path, "S", [("s|a", "A", 100.0), ("s|b", "B", 300.0)])
    roster = pilot.load_district_roster(tmp_path)
    long_frame = pd.DataFrame(
        {
            "state": ["S"],
            "district": ["A"],
            "district_key": ["s|a"],
            "scenario": ["historical"],
            "period": ["1990-2010"],
            "m": [5.0],
        }
    )
    expanded, reconciliation = pilot.expand_to_roster(long_frame, roster, ["m"])
    missing = expanded.loc[expanded["district_key"] == "s|b", "m"]
    assert len(missing) == len(pilot.SLICES) and missing.isna().all()
    assert reconciliation.loc[
        reconciliation["district_key"] == "s|b", "status"
    ].tolist() == ["roster_no_master_row"]


def test_national_coverage_totals_are_summed_from_the_state_rows():
    """National and state coverage are derived from one set of rows, so they agree."""
    long_frame = pd.DataFrame(
        {
            "state": ["S1", "S1", "S2", "S2"],
            "scenario": ["historical"] * 4,
            "period": ["1990-2010"] * 4,
            "m": [1.0, np.nan, 2.0, 3.0],
        }
    )
    report = pilot.coverage_report(long_frame, ["m"])
    state_rows = report.loc[report["scope"] == "state"]
    national = report.loc[report["scope"] == "national"].iloc[0]
    assert int(national["n_finite"]) == int(state_rows["n_finite"].sum()) == 3
    assert int(national["n_rows"]) == int(state_rows["n_rows"].sum()) == 4


# --- CHG-0349: area weighting does not depend on the plotting stack -----------


def test_area_weighting_is_available_without_maps(tmp_path):
    """Areas come from the roster's property table, not from the rendered geometry."""
    _write_roster(tmp_path, "S", [("s|a", "A", 100.0), ("s|b", "B", 300.0)])
    roster = pilot.load_district_roster(tmp_path)
    areas = roster.loc[:, ["district_key", "area_m2"]]
    scored = pd.DataFrame(
        {
            "state": ["S", "S"],
            "district_key": ["s|a", "s|b"],
            "scenario": ["historical", "historical"],
            "period": ["1990-2010", "1990-2010"],
            "composite": [0.0, 100.0],
        }
    )
    result = pilot.state_scores(scored, areas=areas, ruler_kind="linear")
    assert result["area_weighted_mean"].iloc[0] == pytest.approx(75.0)
    assert int(result["n_districts_with_area"].iloc[0]) == 2


def test_missing_roster_fails_the_run_by_default(tmp_path):
    """A blank P-12 table reads as a pass, so an absent roster must be an error."""
    code = pilot.main(
        ["--data-dir", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--no-maps"]
    )
    assert code == 2


# --- CHG-0356: a requested state with no geometry shard is an error ------------


def test_missing_requested_state_shard_fails_by_default(tmp_path):
    """Returning only the states that happen to exist silently shrinks the universe."""
    _write_roster(tmp_path, "S", [("s|a", "A", 100.0)])
    with pytest.raises(FileNotFoundError, match="Absentia"):
        pilot.load_district_roster(tmp_path, states=["S", "Absentia"])

    degraded = pilot.load_district_roster(
        tmp_path, states=["S", "Absentia"], require_all=False, verbose=False
    )
    assert degraded["district_key"].tolist() == ["s|a"]


# --- CHG-0357: every retained district needs a usable area --------------------


def test_zero_area_district_fails_the_area_gate(tmp_path):
    """One usable area is not enough: unusable areas vanish from the weighted mean."""
    _write_roster(tmp_path, "S", [("s|a", "A", 100.0), ("s|b", "B", 0.0)])
    code = pilot.main(
        ["--data-dir", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--no-maps"]
    )
    assert code == 2


# --- CHG-0358: "no master row" and "no finite value" are different faults ------


def test_district_with_master_rows_but_all_nan_is_not_reported_as_missing():
    """A regeneration gap must not be reported as a roster/boundary gap."""
    roster = pd.DataFrame(
        {
            "district_key": ["s|a", "s|b"],
            "state": ["S", "S"],
            "district": ["A", "B"],
            "area_m2": [100.0, 200.0],
        }
    )
    long_frame = pd.DataFrame(
        {
            "state": ["S"] * len(pilot.SLICES),
            "district": ["B"] * len(pilot.SLICES),
            "district_key": ["s|b"] * len(pilot.SLICES),
            "scenario": [scenario for scenario, _ in pilot.SLICES],
            "period": [period for _, period in pilot.SLICES],
            "m": [np.nan] * len(pilot.SLICES),
        }
    )
    _, reconciliation = pilot.expand_to_roster(long_frame, roster, ["m"])
    status = reconciliation.set_index("district_key")["status"]
    assert status["s|b"] == "roster_master_no_finite_value"
    assert status["s|a"] == "roster_no_master_row"


# --- CHG-0359: the output contract holds in degraded mode ---------------------


def test_reconciliation_is_written_even_without_a_roster(tmp_path):
    """An advertised artifact must exist, empty, rather than be silently skipped."""
    out_dir = tmp_path / "out"
    pilot.main(
        [
            "--data-dir",
            str(tmp_path),
            "--out-dir",
            str(out_dir),
            "--no-maps",
            "--allow-missing-geometry",
        ]
    )
    written = out_dir / "roster_reconciliation.csv"
    assert written.exists()
    assert list(pd.read_csv(written).columns) == [
        "district_key",
        "state",
        "district",
        "n_slices_with_any_metric",
        "has_master_row",
        "status",
    ]
