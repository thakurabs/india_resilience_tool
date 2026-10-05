"""Summarize completed Route B pilots without making a release/accuracy decision.

CHG-0650. Reads signed per-model diagnostic outputs; writes period metrics,
aggregation gaps, support-labelled W1 comparisons, cost evidence and a report.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse

from tools.diagnostics.wbgt_route_b_prepare import MODEL_DEFAULTS, verify_support

METRICS = ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32")
PERIODS = {"historical": ((1990, 2010),),
           "ssp245": ((2020, 2039), (2040, 2059), (2060, 2080)),
           "ssp585": ((2020, 2039), (2040, 2059), (2060, 2080))}


def period_metrics(yearly: pd.DataFrame) -> pd.DataFrame:
    """Average annual metrics only when every declared reporting year is finite."""
    keys = ["unit_index", "scenario", "method"]
    if yearly.duplicated(keys + ["year"]).any():
        raise ValueError("Duplicate annual metric keys")
    rows = []
    for key, group in yearly.groupby(keys, sort=False):
        for first, last in PERIODS[key[1]]:
            subset = group[group.year.between(first, last)]
            full = sorted(subset.year.tolist()) == list(range(first, last + 1))
            row = dict(zip(keys, key))
            row.update(period=f"{first}-{last}", expected_years=last - first + 1)
            for metric in METRICS:
                values = subset[metric].to_numpy()
                row[metric] = float(values.mean()) if full and np.isfinite(values).all() else np.nan
                row[metric + "_valid_years"] = int(np.isfinite(values).sum())
            rows.append(row)
    return pd.DataFrame(rows)


def paired_gaps(periods: pd.DataFrame) -> pd.DataFrame:
    """Separate aggregation-order gaps from threshold/max estimand differences."""
    keys = ["unit_index", "scenario", "period"]
    indexed = {m: g.set_index(keys) for m, g in periods.groupby("method")}
    comparisons = (("grid_first_series", "region_first_series", "aggregation_order"),
                   ("grid_first_cell_metrics", "grid_first_series", "cell_metric_vs_regional_series"))
    rows = []
    for left, right, label in comparisons:
        delta = indexed[left][list(METRICS)] - indexed[right][list(METRICS)]
        delta["comparison"] = label
        rows.append(delta.reset_index())
    return pd.concat(rows, ignore_index=True)


def w1_comparison(yearly: pd.DataFrame, units: pd.DataFrame, model: str, root: Path) -> pd.DataFrame:
    """Compare matching metric definitions; explicitly deny unverified support parity."""
    keys = ["state_name", "level", "district_name", "block_name"]
    pilot = yearly[(yearly.year == 2005) & (yearly.scenario == "historical") &
                   (yearly.method == "grid_first_cell_metrics")].merge(units, on="unit_index", validate="many_to_one")
    paths = [root / f"{level}_values.csv" for level in ("district", "block")]
    if not all(p.exists() for p in paths):
        return pd.DataFrame([{"model": model, "comparison_status": "W1_ARTIFACTS_UNAVAILABLE"}])
    frames = [pd.read_csv(p).fillna({"block_name": ""}) for p in paths]
    w1 = pd.concat([f for f in frames if len(f)], ignore_index=True) if any(len(f) for f in frames) else frames[0]
    w1 = w1[(w1.model == model) & (w1.year == 2005) & (w1.scenario == "historical") & (w1.candidate == "W1")]
    if w1.empty:
        return pd.DataFrame([{"model": model, "comparison_status": "W1_MODEL_UNAVAILABLE"}])
    compare = pilot.merge(w1, on=keys, suffixes=("_route_b", "_w1"), validate="one_to_many")
    compare["comparison_status"] = "DESCRIPTIVE_ONLY_SUPPORT_NOT_PROVEN_IDENTICAL"
    compare["mean_delta_c"] = compare.mean_c - compare.wbgt_annual_mean_c
    for threshold in (28, 30, 32):
        compare[f"days_ge_{threshold}_delta"] = compare[f"days_ge_{threshold}_route_b"] - compare[f"days_ge_{threshold}_w1"]
    cols = keys + ["run", "comparison_status", "mean_delta_c", "days_ge_28_delta", "days_ge_30_delta", "days_ge_32_delta",
                   "retained_fraction", "valid_fraction_of_full", "method_signature"]
    return compare[cols].assign(model=model)


def matched_w1(cell_table: pd.DataFrame, units: pd.DataFrame, areas: np.ndarray,
               model: str, cache: Path) -> pd.DataFrame:
    """Reaggregate both methods on exactly the same complete 2005 cell support.

    This isolates spatial/metric comparability, not physics: humidity derivation,
    radiation treatment and elevation conventions still differ between methods.
    """
    import xarray as xr

    rows = []
    columns = ["mean_c", "days_ge_28", "days_ge_30", "days_ge_32"]
    for state in sorted(units.state_name.unique()):
        for variant in ("baseline", "combined"):
            path = cache / f"cells_{state.replace(' ', '_')}_{model}_2005_{variant}.nc"
            if not path.exists():
                rows.append({"model": model, "state_name": state, "variant": variant,
                             "status": "W1_CELL_CACHE_UNAVAILABLE"})
                continue
            with xr.open_dataset(path) as ds:
                original = ds.to_dataframe().reset_index().rename(columns={"wbgt_annual_mean_c": "mean_c"})
                signature = ds.attrs["method_signature"]
            common = cell_table.merge(original, on=["lat", "lon"], suffixes=("_route_b", "_w1"), validate="one_to_one")
            valid = (common.complete_year == 1) & (common.valid_days == 365) & (common.expected_days == 365)
            for metric in columns:
                valid &= np.isfinite(common[metric + "_route_b"]) & np.isfinite(common[metric + "_w1"])
            common = common[valid]
            source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            for unit in units[units.state_name == state].itertuples():
                area = areas[unit.unit_index, common.cell.to_numpy(dtype=int)]
                fraction = float(area.sum() / unit.whole_area_m2)
                row = {"model": model, "state_name": state, "variant": variant, "unit_index": unit.unit_index,
                       "level": unit.level, "district_name": unit.district_name, "block_name": unit.block_name,
                       "common_fraction_of_whole_polygon": fraction,
                       "common_cells": int((area > 0).sum()), "w1_method_signature": signature,
                       "w1_cell_cache_sha256": source_hash,
                       "status": "MATCHED_SPATIAL_SUPPORT_METHOD_COMPARISON" if fraction >= .95 else "INSUFFICIENT_COMMON_SUPPORT"}
                for metric in columns:
                    for method in ("route_b", "w1"):
                        row[metric + "_" + method] = float(area @ common[metric + "_" + method] / area.sum()) if fraction >= .95 else np.nan
                    row[metric + "_delta"] = row[metric + "_route_b"] - row[metric + "_w1"]
                rows.append(row)
    return pd.DataFrame(rows)


def report(args: argparse.Namespace) -> dict:
    """Require complete model manifests, then write evidence without acceptance claims."""
    summaries, all_gaps, all_w1, common_w1, holdouts, costs, manifests = [], [], [], [], [], [], {}
    support_manifest = verify_support(args.work_dir)
    areas = sparse.load_npz(args.work_dir / "admin_areas.npz").toarray()
    for model in args.models:
        root = args.out_dir / model
        manifest_path = root / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        if manifest["status"] != "PILOT_COMPLETE_DIAGNOSTIC_ONLY":
            raise ValueError(f"Incomplete model: {model}")
        if manifest["support"]["signature_sha256"] != support_manifest["signature_sha256"]:
            raise ValueError("Report support differs from fitted pilot")
        for name, expected_digest in manifest["artifact_sha256"].items():
            if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected_digest:
                raise ValueError(f"Changed pilot output: {model}/{name}")
        manifests[model] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        units = pd.read_csv(root / "unit_support.csv").fillna({"block_name": ""})
        yearly = pd.read_csv(root / "yearly_metrics.csv")
        expected = len(units) * 143 * 3  # 21 historical + 61 years in each SSP
        if len(yearly) != expected or yearly.duplicated(["unit_index", "scenario", "year", "method"]).any():
            raise ValueError(f"Incomplete/duplicate pilot roster: {model}")
        periods = period_metrics(yearly)
        periods.merge(units, on="unit_index", validate="many_to_one").to_csv(root / "period_metrics.csv", index=False)
        gaps = paired_gaps(periods).merge(units, on="unit_index", validate="many_to_one")
        gaps["model"] = model
        all_gaps.append(gaps)
        all_w1.append(w1_comparison(yearly, units, model, args.w1_dir))
        common_w1.append(matched_w1(pd.read_csv(root / "cell_metrics_2005.csv"), units, areas, model, args.w1_cache))
        holdout = pd.read_csv(root / "holdout_scores.csv").merge(units, on="unit_index", validate="many_to_one")
        holdout["model"] = model
        holdouts.append(holdout)
        timing = pd.read_csv(root / "timings.csv")
        probe_path = root / "probe_timings.csv"
        if probe_path.exists():
            timing = pd.concat([timing, pd.read_csv(probe_path)], ignore_index=True)
        timing = timing[timing.seconds > 0].drop_duplicates(["kind", "series"])
        for kind, sample in timing.groupby("kind"):
            costs.append({"model": model, "kind": kind, "measured_series": len(sample),
                          "mean_seconds_per_series": float(sample.seconds.mean()),
                          "median_seconds_per_series": float(sample.seconds.median()),
                          "p95_seconds_per_series": float(sample.seconds.quantile(.95)),
                          "hours_per_1000_series": float(sample.seconds.mean() * 1000 / 3600),
                          "scope": "full and holdout calibration, historical/future application; excludes solar, I/O and geospatial prep"})
        radiation = pd.read_csv(root / "radiation_diagnostics.csv")
        if radiation.conservation_max_wm2.max() > 1e-8:
            raise ValueError("Recorded radiation gate failed")
        summaries.append({"model": model, "units_eligible": manifest["units_eligible"], "units_total": manifest["units_total"],
                          "cells_eligible": manifest["cells_eligible"], "elapsed_seconds": manifest["elapsed_seconds"],
                          "peak_rss_mb": manifest["peak_rss_mb"],
                          "maximum_radiation_residual_wm2": float(radiation.conservation_max_wm2.max())})
    gaps = pd.concat(all_gaps, ignore_index=True)
    gaps.to_csv(args.out_dir / "aggregation_gaps.csv", index=False)
    pd.concat(all_w1, ignore_index=True).to_csv(args.out_dir / "w1_descriptive_comparison.csv", index=False)
    pd.concat(common_w1, ignore_index=True).to_csv(args.out_dir / "w1_matched_support_comparison.csv", index=False)
    holdout = pd.concat(holdouts, ignore_index=True)
    holdout.to_csv(args.out_dir / "holdout_scores_all.csv", index=False)
    pd.DataFrame(costs).to_csv(args.out_dir / "cost_evidence.csv", index=False)
    gap_summary = []
    for key, group in gaps.groupby(["model", "state_name", "level", "comparison", "scenario", "period"]):
        row = dict(zip(["model", "state_name", "level", "comparison", "scenario", "period"], key))
        for metric in METRICS:
            vals = group[metric].dropna().abs()
            row[metric + "_median_abs"] = float(vals.median()) if len(vals) else None
            row[metric + "_max_abs"] = float(vals.max()) if len(vals) else None
            row[metric + "_n"] = len(vals)
        gap_summary.append(row)
    pd.DataFrame(gap_summary).to_csv(args.out_dir / "aggregation_gap_summary.csv", index=False)
    result = {"status": "PILOT_COMPLETE_REVIEW_REQUIRED", "models": summaries,
              "model_manifest_sha256": manifests,
              "production_changes": False, "independent_observational_validation": False,
              "national_grid_cell_count": None,
              "native_polygon_reference_sensitivity_complete": False,
              "national_cost_note": "Per-1000-series costs are measured. National grid count, all-model scaling and production workload remain unmeasured."}
    (args.out_dir / "summary.json").write_text(json.dumps(result, indent=2))
    lines = ["# Route B paired pilot results — CHG-0650", "",
             "The two-model diagnostic pilot is complete. Production release and composite membership remain undecided.", "",
             "Both paths use the same native-footprint UHE reference coarsened conservatively to NEX cells. CarbonPlan's overlapping 0.25° reference footprints remain a separate reproduction control.", "",
             "| Model | Eligible / total units | Eligible cells | Peak memory MB |", "|---|---:|---:|---:|"]
    for row in summaries:
        lines.append(f"| {row['model']} | {row['units_eligible']} / {row['units_total']} | {row['cells_eligible']} | {row['peak_rss_mb']} |")
    lines += ["", "Held-out shade evaluation (2000–2014, calibration 1985–1999):", "",
              "| Model | Path | Median absolute mean bias °C | Median seasonal MAE °C |", "|---|---|---:|---:|"]
    for (model, kind), group in holdout.groupby(["model", "kind"]):
        lines.append(f"| {model} | {kind} | {group.annual_mean_bias_c.abs().median():.4f} | {group.monthly_climatology_mae_c.median():.4f} |")
    lines += ["", "Read aggregation_gap_summary.csv for state/level/scenario/period gaps. aggregation_order compares identical regional-series estimands; cell_metric_vs_regional_series isolates threshold/max ordering. Period files require every expected year and preserve excluded roster units as NaN.", "",
              "w1_matched_support_comparison.csv reaggregates both methods on identical complete 2005 cells and fixed polygon weights, requiring at least 95% whole-polygon coverage. Missing predecessor model caches are explicit. Physics, humidity derivation and elevation conventions still differ: this is a method comparison, not observational accuracy evidence. w1_descriptive_comparison.csv separately retains the original, unmatched-support admin comparison.", "",
              "cost_evidence.csv records measured calibration/application costs per 1,000 cell or region series. National grid support and the remaining 19 models are unmeasured; extrapolations are not an approved national run budget.", "",
              "Remaining scientific work/decisions: full-period sensitivity to direct native-polygon reference aggregation remains unmeasured; choose aggregation/metric semantics after reviewing these gaps; resolve coastal/island support; assess independent rural/mountain observational evidence; assess application-window sensitivity; decide whether to undertake a broader model pilot. These results do not authorize display/composite publication.", ""]
    (args.out_dir / "README.md").write_text("\n".join(lines))
    return result


def main(argv: list[str] | None = None) -> int:
    """Print a plan by default; --run writes diagnostic summaries only."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_route_b_pilot"))
    parser.add_argument("--w1-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_pilot_qc"))
    parser.add_argument("--w1-cache", type=Path, default=Path("scratch/wbgt_outdoor_pilot_qc"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b"))
    parser.add_argument("--models", nargs="+", choices=MODEL_DEFAULTS, default=list(MODEL_DEFAULTS))
    args = parser.parse_args(argv)
    print(json.dumps(report(args) if args.run else {"action": "plan_only", "models": args.models}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
