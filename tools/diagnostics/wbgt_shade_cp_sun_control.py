"""Reproduce Route B grid-first outdoor CSVs from the same cached shade.

Run in the pinned CarbonPlan Python environment. The default prints a plan;
``--run`` writes only to an isolated evidence directory. A complete model must
match every eligible 1990–2010 historical and 2020–2080 future cell-first row.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

from tools.diagnostics.wbgt_route_b import annual_metrics, fixed_aggregate
from tools.diagnostics.wbgt_route_b_prepare import PERIODS, verify_support
from tools.diagnostics.wbgt_route_b_run import sun_adjustments, upstream_functions
from tools.pipeline.build_carbonplan_shade_release import (
    checked_support, digest, driver_dates, METRICS,
)

OUTDOOR_METRICS = ("mean_c", "max_c", *METRICS[1:])


def scenario_control(root: Path, evidence: Path, model: str, scenario: str,
                     cells_ok: np.ndarray, unit_ids: np.ndarray, weights: np.ndarray,
                     active_cells: pd.DataFrame, dates: pd.DatetimeIndex,
                     calendar: str, signature: str, mod, upstream: Path,
                     validated: pd.DataFrame, *, chunk_size: int = 48) -> dict:
    """Rebuild one outdoor scenario, using cached shade and signed solar drivers."""
    first, last = PERIODS[scenario]
    kept = (dates.year >= 1990) & (dates.year <= 2010) if scenario == "historical" else np.ones(len(dates), bool)
    selected_dates = dates[kept]
    count = int(cells_ok.sum())
    work = evidence / f"{model}_{scenario}"
    work.mkdir(parents=True, exist_ok=True)
    radiation = np.lib.format.open_memmap(work / "rsds.npy", mode="w+", dtype="float32",
                                           shape=(len(selected_dates), count))
    wind = np.lib.format.open_memmap(work / "wind.npy", mode="w+", dtype="float32",
                                      shape=(len(selected_dates), count))
    offset = 0
    for year in range(first, last + 1):
        path = root / "drivers" / model / scenario / f"{year}.npz"
        with np.load(path, allow_pickle=False) as source:
            shard_dates = pd.DatetimeIndex(source["dates"].tolist())
            use = ((shard_dates.year >= 1990) & (shard_dates.year <= 2010)
                   if scenario == "historical" else np.ones(len(shard_dates), bool))
            rows = int(use.sum())
            if rows:
                radiation[offset:offset + rows] = source["rsds"][use][:, cells_ok]
                wind[offset:offset + rows] = source["sfcWind"][use][:, cells_ok]
                offset += rows
    if offset != len(selected_dates):
        raise ValueError(f"{model}/{scenario}: solar driver dates differ from shade cache")
    years = np.unique(selected_dates.year)
    cell_metrics = {metric: np.full((len(years), count), np.nan) for metric in OUTDOOR_METRICS}
    for start in range(0, count, chunk_size):
        stop = min(start + chunk_size, count)
        shade = []
        for index in range(start, stop):
            path = root / "qdm" / model / "cell" / f"{index}.npz"
            with np.load(path, allow_pickle=False) as source:
                if str(source["signature"]) != signature or len(source[scenario]) != len(dates):
                    raise ValueError(f"{model}/{scenario}: cached shade identity differs: {path}")
                shade.append(source[scenario][kept])
        sun, _ = sun_adjustments(mod, np.asarray(radiation[:, start:stop]),
                                 np.asarray(wind[:, start:stop]), selected_dates, calendar,
                                 active_cells.iloc[start:stop].reset_index(drop=True), upstream)
        annual = annual_metrics(selected_dates, np.column_stack(shade) + sun)
        if not np.array_equal(annual["years"], years):
            raise ValueError(f"{model}/{scenario}: annual calendar differs")
        for metric in OUTDOOR_METRICS:
            cell_metrics[metric][:, start:stop] = annual[metric]
        if start == 0 or stop == count or (stop // chunk_size) % 10 == 0:
            print(f"{model}/{scenario}: solar control {stop}/{count} cells", flush=True)
    predicted = {metric: fixed_aggregate(values, weights) for metric, values in cell_metrics.items()}
    expected = validated.loc[(validated.scenario == scenario) &
                             (validated.method == "grid_first_cell_metrics")]
    active_units = unit_ids.astype(int).tolist()
    if len(active_units) != len(weights):
        raise ValueError("Eligible unit roster and fixed weights differ")
    expected = expected.set_index(["year", "unit_index"])
    maximum = 0.
    mismatches = 0
    checked = 0
    jointly_missing = 0
    for row, year in enumerate(years):
        for col, unit in enumerate(active_units):
            if (int(year), int(unit)) not in expected.index:
                raise ValueError(f"Missing validated outdoor row: {scenario}/{year}/{unit}")
            actual = expected.loc[(int(year), int(unit))]
            for metric in OUTDOOR_METRICS:
                got, want = float(predicted[metric][row, col]), float(actual[metric])
                # A missing contributing cell-year makes the unit-year missing in
                # Route B too; reproducing that missingness is part of the control.
                if not np.isfinite(got) and not np.isfinite(want):
                    jointly_missing += 1
                    checked += 1
                    continue
                if not np.isfinite(got) or not np.isfinite(want):
                    raise ValueError(f"One-sided missing outdoor control: "
                                     f"{scenario}/{year}/{unit}/{metric}")
                difference = abs(got - want)
                maximum = max(maximum, difference)
                mismatches += difference > 5e-13
                checked += 1
    return {"scenario": scenario, "years": [int(years.min()), int(years.max())],
            "eligible_units": len(active_units), "values_checked": checked,
            "jointly_missing_values": jointly_missing,
            "mismatches_above_csv_roundtrip": mismatches, "max_abs_difference": maximum,
            "verdict": "PASS" if mismatches == 0 else "FAIL"}


def main(argv: list[str] | None = None) -> int:
    """Plan or run one complete-model cached-shade outdoor reproduction."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route-b-root", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--upstream", type=Path, default=Path("scratch/carbonplan_reproduction/upstream"))
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--model", default="ACCESS-CM2")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps({"model": args.model, "evidence": str(args.evidence.resolve()),
                      "route_b_root": str(args.route_b_root.resolve()), "writes": args.run}, indent=2),
          flush=True)
    if not args.run:
        return 0
    if args.evidence.resolve().is_relative_to(args.route_b_root.resolve()):
        parser.error("Control evidence must be outside Route B")
    support = verify_support(args.route_b_root)
    units = pd.read_csv(args.route_b_root / "units.csv").fillna({"block_name": ""})
    cells = pd.read_csv(args.route_b_root / "cells.csv")
    areas = sparse.load_npz(args.route_b_root / "admin_areas.npz").tocsr()
    manifest_path = args.route_b_root / "assembly" / args.model / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    cells_ok, units_ok, weights = checked_support(args.route_b_root, args.model,
                                                   units, areas, manifest)
    active_cells = cells.loc[cells_ok].reset_index(drop=True)
    dates = {}
    calendars = set()
    for scenario in PERIODS:
        dates[scenario], calendar = driver_dates(args.route_b_root, args.model, scenario,
            support["signature_sha256"], manifest["drivers"][scenario])
        calendars.add(calendar)
    if len(calendars) != 1:
        raise ValueError("Mixed model calendars")
    mod, _, lock = upstream_functions(args.upstream)
    if lock != manifest["source_lock"]:
        raise ValueError("Pinned upstream source lock differs from Route B")
    validated_path = args.route_b_root / "assembly" / args.model / "yearly_metrics.csv"
    if digest(validated_path) != manifest["artifact_sha256"]["yearly_metrics.csv"]:
        raise ValueError("Validated Route B CSV hash changed")
    validated = pd.read_csv(validated_path)
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    reports = []
    for scenario in PERIODS:
        reports.append(scenario_control(args.route_b_root, evidence, args.model, scenario,
            cells_ok, units.loc[units_ok, "unit_index"].to_numpy(), weights,
            active_cells, dates[scenario], calendars.copy().pop(),
            manifest["signature_sha256"], mod, args.upstream, validated))
    verdict = "PASS" if all(report["verdict"] == "PASS" for report in reports) else "FAIL"
    (evidence / "control.json").write_text(json.dumps({"verdict": verdict, "model": args.model,
        "route_b_manifest_sha256": digest(manifest_path), "reports": reports}, indent=2),
        encoding="utf-8")
    print(json.dumps(reports, indent=2), flush=True)
    return 0 if verdict == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
