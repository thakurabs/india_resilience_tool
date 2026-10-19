"""Predict Route B outdoor-WBGT missingness from input screening alone, without fitting.

CHG-0667. Read-only. Answers "how much of the national release would a model lose,
and where" for a model whose QDM has not been fitted, by replaying the three
deterministic steps that turn a screened input day into a missing unit-year:

1. ``wbgt_route_b_run.raw_shade`` screens physically impossible drivers -- negative
   or supersaturated specific humidity, non-positive temperature, non-finite values
   -- and sets those cell-days to NaN. This is a deliberate validity gate, not a
   numerical accident, and this module reproduces its exact predicate.
2. ``wbgt_route_b_run.adjust`` returns all-NaN for any QDM application window that
   contains a non-finite day, so it never fabricates a value. One screened day
   therefore blanks that cell for the whole window.
3. ``wbgt_route_b.fixed_aggregate`` yields NaN for a unit when any cell carrying
   **strictly positive** area weight is missing. Zero-weight cells cannot poison
   an unrelated unit.

Because all three steps are deterministic functions of the drivers, the resulting
missingness is exactly predictable from the drivers. Nothing here estimates,
extrapolates or samples: a model's predicted loss is the loss it would actually
have. The prediction is verified against ACCESS-CM2's fitted output by
``--verify-against``.

Reported by administrative level, scenario, QDM window and master reporting
period, with geographic concentration and the effect on ensemble membership --
not by affected cell counts, which do not describe what a user would see.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

#: The QDM application windows, mirroring ``wbgt_route_b_run.FUTURE_WINDOWS``.
FUTURE_WINDOWS = ((2020, 2039), (2040, 2059), (2060, 2080))

#: The historical correction is trained and applied over the whole calibration
#: series in one call, so a screened historical day costs the entire series --
#: but such a cell also fails cell eligibility and is dropped before fitting.
HISTORICAL_WINDOW = (1985, 2014)

#: Reporting envelope, mirroring the release specification.
ENVELOPE = {"historical": (1990, 2010), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}

#: Master reporting periods, inclusive at both ends. They do **not** align with the
#: QDM windows: the period 2020-2040 straddles windows 1 and 2, so a period can lose
#: part of its years and still produce a value from the rest.
PERIODS = {"historical": (("1990-2010", 1990, 2010),),
           "ssp245": (("2020-2040", 2020, 2040), ("2040-2060", 2040, 2060),
                      ("2060-2080", 2060, 2080)),
           "ssp585": (("2020-2040", 2020, 2040), ("2040-2060", 2040, 2060),
                      ("2060-2080", 2060, 2080))}


def screen_mask(arrays: dict) -> np.ndarray:
    """Reproduce ``raw_shade``'s validity predicate exactly, as a time x cell mask.

    Kept byte-for-byte equivalent to the runner's ``invalid`` expression. If the
    runner's screen ever changes, this prediction becomes wrong rather than
    conservative, so the two must be compared rather than assumed equal.
    """
    tas, tasmax, huss = arrays["tas"], arrays["tasmax"], arrays["huss"]
    return (~np.isfinite(tas) | ~np.isfinite(tasmax) | ~np.isfinite(huss)
            | (tas <= 0) | (tasmax <= 0) | (huss < 0) | (huss >= 1))


def _shard_task(job: tuple[str, str, str, int]) -> dict:
    """Screen one model/scenario/year driver shard and return the affected cells."""
    root_text, model, scenario, year = job
    path = Path(root_text) / "drivers" / model / scenario / f"{year}.npz"
    with np.load(path, allow_pickle=False) as handle:
        arrays = {name: handle[name] for name in ("tas", "tasmax", "huss")}
        dates = pd.DatetimeIndex([str(value) for value in handle["dates"]])
    mask = screen_mask(arrays)
    cells = np.flatnonzero(mask.any(axis=0))
    return {"model": model, "scenario": scenario, "year": year,
            "screened_cell_days": int(mask.sum()),
            "cells": cells.tolist(),
            "dates": sorted({str(d.date()) for d in dates[mask.any(axis=1)]})}


def window_of(scenario: str, year: int) -> tuple[int, int] | None:
    """Return the QDM application window a year falls in, or None if outside."""
    if scenario == "historical":
        return HISTORICAL_WINDOW
    for first, last in FUTURE_WINDOWS:
        if first <= year <= last:
            return (first, last)
    return None


def load_support(work_dir: Path, reference_model_dir: Path) -> tuple[pd.DataFrame, np.ndarray, sparse.csr_matrix]:
    """Load the roster, the model-independent cell eligibility and the area matrix.

    Cell eligibility is ``support >= 0.50`` and a finite reference, both independent
    of the model, plus a finite historical shade, which is not. The reference model's
    ``cell_support.csv`` supplies the first two; the historical screen is applied per
    model by the caller, so a model with a screened calibration day is handled
    correctly rather than silently inheriting another model's support.
    """
    units = pd.read_csv(work_dir / "units.csv")
    cell_support = pd.read_csv(reference_model_dir / "cell_support.csv")
    eligible = cell_support.eligible.to_numpy(dtype=bool)
    areas = sparse.load_npz(work_dir / "admin_areas.npz").tocsr()
    return units, eligible, areas


def analyse(model: str, records: list[dict], units: pd.DataFrame, eligible: np.ndarray,
            areas: sparse.csr_matrix) -> dict:
    """Turn one model's screened shards into predicted missing unit-years."""
    historical_cells = {c for r in records if r["scenario"] == "historical"
                        and HISTORICAL_WINDOW[0] <= r["year"] <= HISTORICAL_WINDOW[1]
                        for c in r["cells"]}
    # A cell screened anywhere in the calibration series fails cell eligibility and
    # never enters the fit, so it is removed from support rather than blanked later.
    model_eligible = eligible.copy()
    for cell in historical_cells:
        model_eligible[cell] = False

    weights = areas[:, model_eligible]
    totals = np.asarray(weights.sum(axis=1)).ravel()
    coverage = totals / units.whole_area_m2.to_numpy()
    unit_eligible = coverage >= .95
    # Map the kept cells back to their global column, so a blanked global cell can be
    # located inside the weight matrix.
    kept = np.flatnonzero(model_eligible)
    position = {int(c): i for i, c in enumerate(kept)}
    contributes = (weights > 0).tocsc()

    blanked_units: dict[tuple[str, tuple[int, int]], np.ndarray] = {}
    for record in records:
        scenario = record["scenario"]
        if scenario == "historical":
            continue
        window = window_of(scenario, record["year"])
        if window is None:
            continue
        columns = [position[c] for c in record["cells"] if c in position]
        if not columns:
            continue
        hit = np.zeros(len(units), dtype=bool)
        for column in columns:
            hit |= np.asarray(contributes[:, column].todense()).ravel().astype(bool)
        key = (scenario, window)
        blanked_units[key] = blanked_units.get(key, np.zeros(len(units), dtype=bool)) | hit

    level = units.level.to_numpy()
    rows, by_period = [], {}
    for (scenario, window), hit in sorted(blanked_units.items()):
        affected = hit & unit_eligible
        first, last = window
        envelope_first, envelope_last = ENVELOPE[scenario]
        years = max(first, envelope_first), min(last, envelope_last)
        span = years[1] - years[0] + 1
        for unit_level in ("district", "block"):
            selected = affected & (level == unit_level)
            if not selected.any():
                continue
            rows.append({"model": model, "scenario": scenario,
                         "window": f"{first}-{last}", "level": unit_level,
                         "units_affected": int(selected.sum()),
                         "years_in_window": span,
                         "unit_years_lost": int(selected.sum()) * span,
                         "states": sorted(set(units.state_name[selected]))})
        for label, period_first, period_last in PERIODS[scenario]:
            overlap = max(0, min(period_last, years[1]) - max(period_first, years[0]) + 1)
            if not overlap:
                continue
            key = f"{scenario}|{label}"
            entry = by_period.setdefault(key, {"years_in_period": period_last - period_first + 1,
                                               "years_lost_max": 0, "units_affected": 0,
                                               "fully_lost_units": 0})
            entry["years_lost_max"] = max(entry["years_lost_max"], overlap)
            entry["units_affected"] = max(entry["units_affected"], int(affected.sum()))
            if overlap == entry["years_in_period"]:
                entry["fully_lost_units"] = max(entry["fully_lost_units"], int(affected.sum()))

    total = sum(r["unit_years_lost"] for r in rows)
    eligible_unit_years = int(unit_eligible.sum()) * sum(
        last - first + 1 for first, last in ENVELOPE.values())
    concentration = {}
    for (scenario, window), hit in blanked_units.items():
        for state, count in pd.Series(units.state_name[hit & unit_eligible]).value_counts().items():
            concentration[state] = concentration.get(state, 0) + int(count)
    return {"model": model,
            "screened_cell_days": sum(r["screened_cell_days"] for r in records),
            "screened_cells_total": len({c for r in records for c in r["cells"]}),
            "screened_calibration_cells": len(historical_cells),
            "cells_eligible": int(model_eligible.sum()),
            "units_eligible": int(unit_eligible.sum()),
            "unit_years_lost": total,
            "eligible_unit_years": eligible_unit_years,
            "unit_years_lost_share": round(total / eligible_unit_years, 6) if eligible_unit_years else None,
            "by_window": rows,
            "by_master_period": by_period,
            "state_concentration": dict(sorted(concentration.items(), key=lambda kv: -kv[1])),
            "affected_unit_indices": sorted({int(i) for hit in blanked_units.values()
                                             for i in np.flatnonzero(hit & unit_eligible)})}


def verify(prediction: dict, model_dir: Path) -> dict:
    """Check a prediction against a model that was actually fitted and assembled.

    The prediction is only worth reporting if it reproduces a fitted model exactly.
    A mismatch means the screen or the window map has drifted from the runner.
    """
    yearly = pd.read_csv(model_dir / "yearly_metrics.csv")
    yearly = yearly[yearly.method == "grid_first_cell_metrics"]
    support = pd.read_csv(model_dir / "unit_support.csv")
    eligible = set(support.loc[support.eligible, "unit_index"].astype(int))
    observed = yearly[yearly.unit_index.astype(int).isin(eligible)
                      & ~np.isfinite(yearly.mean_c)]
    predicted = set(prediction["affected_unit_indices"])
    actual = set(observed.unit_index.astype(int))
    return {"predicted_units": len(predicted), "actual_units": len(actual),
            "predicted_unit_years": prediction["unit_years_lost"],
            "actual_unit_years": int(len(observed)),
            "units_predicted_not_observed": sorted(predicted - actual)[:10],
            "units_observed_not_predicted": sorted(actual - predicted)[:10],
            "exact_unit_match": predicted == actual,
            "exact_unit_year_match": prediction["unit_years_lost"] == len(observed)}


def ensemble_effect(predictions: list[dict], units: pd.DataFrame) -> dict:
    """Describe how varying per-model missingness would thin ensemble membership."""
    counts: dict[int, int] = {}
    for prediction in predictions:
        for index in prediction["affected_unit_indices"]:
            counts[index] = counts.get(index, 0) + 1
    if not counts:
        return {"units_with_any_model_missing": 0, "note": "no model loses any unit-year"}
    series = pd.Series(counts)
    named = units.set_index("unit_index")
    return {"models_scanned": len(predictions),
            "units_with_any_model_missing": int(len(series)),
            "models_missing_per_unit": {str(k): int(v) for k, v
                                        in series.value_counts().sort_index().items()},
            "worst_units": [{"unit_index": int(i),
                             "models_missing": int(series[i]),
                             "state": named.state_name.get(i),
                             "level": named.level.get(i),
                             "district": named.district_name.get(i),
                             "block": (None if pd.isna(named.block_name.get(i))
                                       else named.block_name.get(i))}
                            for i in series.sort_values(ascending=False).index[:15]],
            "note": ("A unit-year missing in some models but not others reduces that "
                     "unit-year's n_models rather than failing outright, so the ensemble "
                     "would rest on a varying subset unless the contract forbids it.")}


def main(argv: list[str] | None = None) -> int:
    """Predict missingness for every model whose drivers are extracted."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--reference-model-dir", type=Path,
                        default=Path("scratch/wbgt_route_b_national/assembly/ACCESS-CM2"),
                        help="A model whose cell_support.csv supplies model-independent "
                             "cell eligibility")
    parser.add_argument("--verify-against", type=Path,
                        default=Path("scratch/wbgt_route_b_national/assembly/ACCESS-CM2"),
                        help="An assembled model to check the prediction against exactly")
    parser.add_argument("--models", nargs="+", default=None)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--out", type=Path,
                        default=Path("docs/diagnostics/wbgt_outdoor_route_b_national/"
                                     "exposure_prediction.json"))
    args = parser.parse_args(argv)

    drivers = args.work_dir / "drivers"
    available = []
    for model_dir in sorted(drivers.iterdir()) if drivers.is_dir() else []:
        if model_dir.is_dir() and len(list(model_dir.glob("*/*.npz"))) == 152:
            available.append(model_dir.name)
    models = [m for m in (args.models or available) if m in available]
    skipped = sorted(set(args.models or available) - set(models))

    units, eligible, areas = load_support(args.work_dir, args.reference_model_dir)
    jobs = [(str(args.work_dir), model, scenario, year)
            for model in models for scenario, (first, last) in
            {"historical": (1985, 2014), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}.items()
            for year in range(first, last + 1)]
    per_model: dict[str, list[dict]] = {m: [] for m in models}
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_shard_task, job) for job in jobs]
        for future in as_completed(futures):
            record = future.result()
            done += 1
            if record["screened_cell_days"]:
                per_model[record["model"]].append(record)
            if done % 250 == 0:
                print(f"screened {done}/{len(jobs)} shards", flush=True)

    predictions = [analyse(model, per_model[model], units, eligible, areas) for model in models]
    report = {"change": "CHG-0667",
              "models_predicted": models, "models_without_complete_drivers": skipped,
              "verification": (verify(next(p for p in predictions
                                           if p["model"] == args.verify_against.name),
                                      args.verify_against)
                               if args.verify_against.name in models
                               and (args.verify_against / "yearly_metrics.csv").is_file()
                               else {"status": "no assembled model available to verify against"}),
              "predictions": {p["model"]: p for p in predictions},
              "ensemble_effect": ensemble_effect(predictions, units)}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2))
    summary = {p["model"]: {"unit_years_lost": p["unit_years_lost"],
                            "share": p["unit_years_lost_share"],
                            "units_eligible": p["units_eligible"],
                            "cells_eligible": p["cells_eligible"]}
               for p in predictions}
    print(json.dumps({"verification": report["verification"], "summary": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
