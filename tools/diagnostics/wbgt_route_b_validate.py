"""Validate one assembled Route B outdoor-WBGT model against the frozen release spec.

CHG-0663. Read-only. Implements the structural acceptance criteria of
``docs/diagnostics/wbgt_outdoor_route_b_national/RELEASE_SPEC.md`` section 10,
plus the leap-day diagnostic required by section 11.

The checks separate *fitting success* from *scientific validity*: a zero exit
code from the assembling run proves only that it finished. Every criterion here
was declared before any national metric value was inspected, so a threshold is
never invented after seeing a result.

Needs numpy and pandas alone, so it runs in either IRT's geo environment or the
pinned CarbonPlan environment. Default output is JSON on stdout; ``--report``
also writes it beside the model's artifacts.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

#: Declared envelope. Keep in step with RELEASE_SPEC section 3.
PERIODS = {"historical": (1990, 2010), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}

#: Methods a grid-first-only run must report, and the one the release publishes.
GRID_FIRST_METHODS = ("grid_first_series", "grid_first_cell_metrics")
RELEASED_METHOD = "grid_first_cell_metrics"

COUNT_COLUMNS = ("days_ge_28", "days_ge_30", "days_ge_32")

#: Physical screens, not scientific acceptance thresholds.
MEAN_BOUNDS = (-20.0, 60.0)
DAYS_PER_YEAR = 365

#: Counts are area-weighted sums of per-cell annual counts, so a unit whose every
#: cell exceeds the threshold on every day lands a few ULP above 365: the
#: intersection-area weights sum to 1.0 only to floating-point precision. The
#: measured national excess on ACCESS-CM2 was 1.14e-13 days. This tolerance admits
#: that arithmetic and nothing else; a genuine over-count would exceed it by orders
#: of magnitude. Saturation itself is reported as a finding, never hidden behind it.
COUNT_TOLERANCE = 1e-6


class Check:
    """One named structural criterion and the evidence behind its verdict."""

    def __init__(self, name: str, spec: str) -> None:
        self.name, self.spec, self.passed, self.detail = name, spec, True, {}

    def fail(self, **detail) -> "Check":
        self.passed = False
        self.detail.update(detail)
        return self

    def note(self, **detail) -> "Check":
        self.detail.update(detail)
        return self

    def as_dict(self) -> dict:
        return {"check": self.name, "spec": self.spec,
                "verdict": "PASS" if self.passed else "FAIL", **self.detail}


def shards_dir(model_dir: Path) -> Path | None:
    """Locate the fitting shard reports for a model assembled into its own directory."""
    candidate = model_dir.parent.parent / "fit_shards" / model_dir.name
    return candidate if candidate.is_dir() else None


def _expected_years(scenario: str) -> np.ndarray:
    first, last = PERIODS[scenario]
    return np.arange(first, last + 1)


def check_manifest(model_dir: Path, support: dict) -> tuple[Check, dict]:
    """Confirm the run manifest exists and pins the same support and code as the caches."""
    check = Check("manifest", "spec 12: provenance pinned and complete")
    path = model_dir / "run_manifest.json"
    if not path.is_file():
        return check.fail(reason="run_manifest.json absent; the model is not assembled"), {}
    manifest = json.loads(path.read_text())
    recorded = (manifest.get("support") or {}).get("signature_sha256")
    expected = support.get("signature_sha256")
    if recorded and expected and recorded != expected:
        check.fail(support_signature_recorded=recorded, support_signature_expected=expected)
    for required in ("signature_sha256", "code_sha256", "source_lock", "units_eligible",
                     "units_total", "cells_eligible"):
        if required not in manifest:
            check.fail(missing_manifest_key=required)
    check.note(status=manifest.get("status"), aggregation=manifest.get("aggregation"),
               units_eligible=manifest.get("units_eligible"),
               units_total=manifest.get("units_total"),
               cells_eligible=manifest.get("cells_eligible"),
               elapsed_seconds=manifest.get("elapsed_seconds"),
               peak_rss_mb=manifest.get("peak_rss_mb"),
               warnings=manifest.get("warnings", []))
    return check, manifest


def check_roster(yearly: pd.DataFrame, units: pd.DataFrame) -> Check:
    """Every unit, scenario, year and method present exactly once, with no strangers."""
    check = Check("roster", "spec 10.2/10.3: complete model/scenario/year/unit roster, once each")
    expected = {(int(u), s, int(y), m)
                for u in units.unit_index for s in PERIODS
                for y in _expected_years(s) for m in GRID_FIRST_METHODS}
    seen = list(zip(yearly.unit_index.astype(int), yearly.scenario,
                    yearly.year.astype(int), yearly.method))
    counts = pd.Series(seen).value_counts() if seen else pd.Series(dtype=int)
    duplicated = [list(k) for k in counts[counts > 1].index[:10]]
    missing = sorted(expected - set(seen))[:10]
    extra = sorted(set(seen) - expected)[:10]
    if duplicated or missing or extra:
        check.fail(duplicated_sample=duplicated, missing_sample=missing, extra_sample=extra)
    check.note(rows=len(yearly), rows_expected=len(expected),
               units=int(yearly.unit_index.nunique()), units_expected=len(units),
               years_per_scenario={s: int(yearly[yearly.scenario == s].year.nunique())
                                   for s in sorted(PERIODS)},
               methods=sorted(yearly.method.dropna().unique()))
    return check


def check_missing_not_zero(yearly: pd.DataFrame, support: pd.DataFrame) -> Check:
    """Excluded units must be missing in every metric, never zero and never absent."""
    check = Check("missing_not_zero",
                  "spec 10.4/7: excluded units present with missing values, never zero")
    excluded = set(support.loc[~support.eligible, "unit_index"].astype(int))
    if not excluded:
        return check.note(excluded_units=0)
    rows = yearly[yearly.unit_index.astype(int).isin(excluded)]
    absent = sorted(excluded - set(rows.unit_index.astype(int)))[:10]
    if absent:
        check.fail(units_absent_from_output_sample=absent)
    value_columns = [c for c in rows.columns
                     if c not in ("unit_index", "scenario", "year", "method")]
    zeroed = {c: int((rows[c] == 0).sum()) for c in value_columns}
    finite = {c: int(np.isfinite(rows[c].to_numpy(dtype=float)).sum()) for c in value_columns}
    if any(finite.values()):
        check.fail(finite_values_for_excluded_units=finite)
    check.note(excluded_units=len(excluded), excluded_rows=len(rows),
               zero_values_for_excluded_units=zeroed)
    return check


def check_values(yearly: pd.DataFrame, support: pd.DataFrame) -> Check:
    """Physical screens and threshold monotonicity on the released estimand."""
    check = Check("values", "spec 10.5/10.6: bounded, monotonic, max >= mean")
    eligible = set(support.loc[support.eligible, "unit_index"].astype(int))
    rows = yearly[(yearly.method == RELEASED_METHOD)
                  & yearly.unit_index.astype(int).isin(eligible)]
    counts = {c: rows[c].to_numpy(dtype=float) for c in COUNT_COLUMNS}
    mean_c, max_c = rows.mean_c.to_numpy(dtype=float), rows.max_c.to_numpy(dtype=float)
    present = np.isfinite(mean_c)
    saturated = {}
    for name, values in counts.items():
        bad = (np.isfinite(values)
               & ((values < -COUNT_TOLERANCE) | (values > DAYS_PER_YEAR + COUNT_TOLERANCE)))
        if bad.any():
            check.fail(**{f"{name}_out_of_bounds": int(bad.sum()),
                          f"{name}_worst_excess_days": float(
                              np.nanmax(values[bad]) - DAYS_PER_YEAR),
                          f"{name}_observed_range": [float(np.nanmin(values)),
                                                     float(np.nanmax(values))]})
        # A unit-year at the calendar ceiling is a real result -- every day above the
        # threshold -- and is reported rather than absorbed into the tolerance.
        saturated[name] = int(np.nansum(values >= DAYS_PER_YEAR - COUNT_TOLERANCE))
    for high, low in (("days_ge_28", "days_ge_30"), ("days_ge_30", "days_ge_32")):
        pair = np.isfinite(counts[high]) & np.isfinite(counts[low])
        broken = pair & (counts[high] < counts[low] - 1e-9)
        if broken.any():
            check.fail(**{f"{high}_below_{low}": int(broken.sum()),
                          f"{high}_below_{low}_worst": float(np.nanmax(
                              (counts[low] - counts[high])[broken]))})
    both = np.isfinite(mean_c) & np.isfinite(max_c)
    inverted = both & (max_c < mean_c - 1e-9)
    if inverted.any():
        check.fail(max_below_mean=int(inverted.sum()))
    low, high = MEAN_BOUNDS
    implausible = present & ((mean_c < low) | (mean_c > high))
    if implausible.any():
        check.fail(mean_c_outside_bounds=int(implausible.sum()),
                   mean_c_bounds=list(MEAN_BOUNDS))
    incomplete = rows.loc[~present]
    by_place = (incomplete.join(support.set_index("unit_index")["state_name"], on="unit_index")
                .groupby(["state_name", "scenario"]).size().sort_values(ascending=False))
    check.note(eligible_rows=len(rows), finite_rows=int(present.sum()),
               missing_rows=int((~present).sum()),
               incomplete_year_rows_by_state_scenario={f"{s}|{c}": int(n)
                                                       for (s, c), n in by_place.items()},
               incomplete_year_note=("An eligible unit-year with a non-finite input is "
                                     "missing by the complete-year gate, never a low "
                                     "count. Where these concentrate is a finding."),
               mean_c_range=[float(np.nanmin(mean_c)), float(np.nanmax(mean_c))],
               max_c_range=[float(np.nanmin(max_c)), float(np.nanmax(max_c))],
               count_ranges={c: [float(np.nanmin(v)), float(np.nanmax(v))]
                             for c, v in counts.items()},
               exact_zero_counts={c: int(np.nansum(v == 0)) for c, v in counts.items()},
               saturated_at_calendar_ceiling=saturated,
               count_tolerance_days=COUNT_TOLERANCE)
    return check


def check_estimands_distinct(yearly: pd.DataFrame) -> Check:
    """Prove count-first aggregation was not replaced by thresholding a spatial mean."""
    check = Check("estimands_distinct",
                  "spec 10.7: cell-first counts differ from series counts; means agree")
    pivot = yearly.pivot_table(index=["unit_index", "scenario", "year"], columns="method",
                               values=list(COUNT_COLUMNS) + ["mean_c"], dropna=False)
    differing = {}
    for column in COUNT_COLUMNS:
        try:
            cell = pivot[(column, "grid_first_cell_metrics")].to_numpy(dtype=float)
            series = pivot[(column, "grid_first_series")].to_numpy(dtype=float)
        except KeyError:
            return check.fail(reason=f"both methods absent for {column}")
        both = np.isfinite(cell) & np.isfinite(series)
        differing[column] = int((both & (np.abs(cell - series) > 1e-9)).sum())
        check.note(**{f"{column}_max_abs_gap": float(np.nanmax(np.abs(cell - series)[both]))
                      if both.any() else None})
    if not any(differing.values()):
        check.fail(reason="cell-first and series counts are identical everywhere, "
                          "which the two estimands must not be",
                   differing_rows=differing)
    mean_cell = pivot[("mean_c", "grid_first_cell_metrics")].to_numpy(dtype=float)
    mean_series = pivot[("mean_c", "grid_first_series")].to_numpy(dtype=float)
    both = np.isfinite(mean_cell) & np.isfinite(mean_series)
    gap = float(np.nanmax(np.abs(mean_cell - mean_series)[both])) if both.any() else 0.0
    if gap > 1e-6:
        check.fail(mean_c_order_gap_c=gap,
                   reason="annual mean must commute with area weighting")
    check.note(count_rows_differing=differing, mean_c_order_gap_c=gap)
    return check


def check_holdout(model_dir: Path, support: pd.DataFrame) -> Check:
    """Held-out skill must be scored for every eligible unit, with outliers named."""
    check = Check("holdout", "spec 10.10: held-out seasonal/upper-quantile skill reported")
    path = model_dir / "holdout_scores.csv"
    if not path.is_file():
        return check.fail(reason="holdout_scores.csv absent")
    scores = pd.read_csv(path)
    eligible = set(support.loc[support.eligible, "unit_index"].astype(int))
    scored = set(scores.unit_index.astype(int))
    if eligible - scored:
        check.fail(eligible_units_unscored=len(eligible - scored),
                   sample=sorted(eligible - scored)[:10])
    # `complete` is a boolean flag, not a score: including it would try to take a
    # quantile of booleans, and averaging it would read as skill.
    numeric = [c for c in scores.columns
               if c not in ("unit_index", "kind", "complete")
               and pd.api.types.is_numeric_dtype(scores[c])
               and not pd.api.types.is_bool_dtype(scores[c])]
    named = support.set_index("unit_index")[["state_name", "level", "district_name", "block_name"]]
    summary, outliers = {}, {}
    for kind, group in scores.groupby("kind"):
        summary[kind] = {c: {"mean": float(group[c].mean()), "median": float(group[c].median()),
                             "p05": float(group[c].quantile(.05)),
                             "p95": float(group[c].quantile(.95))} for c in numeric}
        bias = ("annual_mean_bias_c" if "annual_mean_bias_c" in numeric else
                next((c for c in numeric if "bias" in c.lower()),
                     numeric[0] if numeric else None))
        if bias is None:
            continue
        worst = group.reindex(group[bias].abs().sort_values(ascending=False).index).head(10)
        outliers[kind] = [{"unit_index": int(r.unit_index), bias: float(getattr(r, bias)),
                           **{k: (None if pd.isna(v) else v)
                              for k, v in named.reindex([int(r.unit_index)]).iloc[0].items()}}
                          for r in worst.itertuples()]
        by_state = group.join(named, on="unit_index").groupby("state_name")[bias]
        summary[kind]["by_state_" + bias] = {s: float(v) for s, v in by_state.mean().items()}
    check.note(kinds=sorted(scores.kind.unique()), scored_units=len(scored),
               eligible_units=len(eligible), statistics=summary, geographic_outliers=outliers)
    return check


def check_leap_grouping(model_dir: Path) -> Check:
    """Measure the day-366 exposure behind xclim's dayofyear/proleptic_gregorian warning.

    The warning is CarbonPlan's inherited grouping choice, not an IRT decision.
    The residual risk is that day-of-year 366 is populated only in leap years, so
    its quantile group is sparser than its neighbours. February 29 is dropped
    before any annual reduction, so the exposure can only reach the release through
    a correction applied to the surrounding days. This records what the release is
    actually exposed to rather than arguing the warning away.
    """
    check = Check("leap_grouping", "spec 11: day-366 exposure measured, not argued")
    counts = {}
    for scenario, (first, last) in PERIODS.items():
        years = range(first, last + 1)
        leap = [y for y in years if (y % 4 == 0 and y % 100 != 0) or y % 400 == 0]
        counts[scenario] = {"years": len(list(years)), "leap_years": len(leap),
                            "leap_year_share": round(len(leap) / len(list(years)), 4)}
    manifest = model_dir / "run_manifest.json"
    warnings = list(json.loads(manifest.read_text()).get("warnings", [])
                    if manifest.is_file() else [])
    # An assembling run that reuses caches emits no fit-time warning of its own, so
    # reading only its manifest would make a recorded warning look resolved. The
    # shard reports are where the fitting warnings actually live.
    shard_dir = shards_dir(model_dir)
    shard_warnings = {}
    if shard_dir and shard_dir.is_dir():
        for report in sorted(shard_dir.glob("shard_*.json")):
            for warning in json.loads(report.read_text()).get("warnings", []) or []:
                shard_warnings[warning] = shard_warnings.get(warning, 0) + 1
    warnings.extend(w for w in shard_warnings if w not in warnings)
    dayofyear = [w for w in warnings if "dayofyear" in w]
    check.note(shard_warning_counts=shard_warnings)
    check.note(reporting_calendar_exposure=counts,
               february_29_dropped_before_annual_reduction=True,
               quantile_window_days=31,
               dayofyear_warnings_recorded=dayofyear,
               interpretation=("Day 366 exists in leap years alone, so its group is sparser; "
                               "the 31-day window draws its neighbours in, and Feb 29 is "
                               "removed before any annual reduction. Zero process failures "
                               "do not by themselves resolve this warning."))
    return check


def validate(model_dir: Path, work_dir: Path) -> dict:
    """Run every structural criterion over one assembled model directory."""
    support_manifest = json.loads((work_dir / "support_manifest.json").read_text())
    units = pd.read_csv(work_dir / "units.csv")
    checks = []
    manifest_check, manifest = check_manifest(model_dir, support_manifest)
    checks.append(manifest_check)
    support_path = model_dir / "unit_support.csv"
    yearly_path = model_dir / "yearly_metrics.csv"
    if not support_path.is_file() or not yearly_path.is_file():
        checks.append(Check("artifacts", "spec 10: assembled artifacts present").fail(
            unit_support=support_path.is_file(), yearly_metrics=yearly_path.is_file()))
        return _report(model_dir, checks, manifest)
    support = pd.read_csv(support_path)
    yearly = pd.read_csv(yearly_path)
    checks.append(check_roster(yearly, units))
    checks.append(check_missing_not_zero(yearly, support))
    checks.append(check_values(yearly, support))
    checks.append(check_estimands_distinct(yearly))
    checks.append(check_holdout(model_dir, support))
    checks.append(check_leap_grouping(model_dir))
    return _report(model_dir, checks, manifest)


def _report(model_dir: Path, checks: list[Check], manifest: dict) -> dict:
    failed = [c.name for c in checks if not c.passed]
    return {"model": model_dir.name, "model_dir": str(model_dir),
            "verdict": "FAIL" if failed else "PASS", "failed_checks": failed,
            "manifest_status": manifest.get("status"),
            "checks": [c.as_dict() for c in checks]}


def main(argv: list[str] | None = None) -> int:
    """Validate one assembled model; a non-zero exit means the release gate failed."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True,
                        help="One assembled model directory, holding run_manifest.json")
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"),
                        help="Campaign work root, holding units.csv and support_manifest.json")
    parser.add_argument("--report", action="store_true",
                        help="Also write validation_report.json beside the model artifacts")
    args = parser.parse_args(argv)
    report = validate(args.model_dir, args.work_dir)
    text = json.dumps(report, indent=2)
    if args.report:
        (args.model_dir / "validation_report.json").write_text(text)
    print(text)
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
