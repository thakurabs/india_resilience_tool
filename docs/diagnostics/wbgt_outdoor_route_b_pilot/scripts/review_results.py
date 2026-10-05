"""CHG-0651: summarize completed pilot evidence by model, state and admin level.

Run after wbgt_route_b_report --run. Default is a read-only plan; --run writes
REVIEW.md and review_tables/*.csv beside summary.json. No acceptance rule is fitted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def table(frame: pd.DataFrame) -> str:
    """Render a small Markdown table without optional tabulate dependencies."""
    def value(item) -> str:
        if isinstance(item, (float, np.floating)):
            return f"{item:.3f}" if np.isfinite(item) else "missing"
        return str(item)
    return "\n".join(["| " + " | ".join(frame.columns) + " |",
                      "| " + " | ".join(["---"] * len(frame.columns)) + " |"] +
                     ["| " + " | ".join(map(value, row)) + " |"
                      for row in frame.itertuples(index=False, name=None)])


def review(root: Path) -> None:
    """Write stratified evidence and its limits after checking completion manifests."""
    summary = json.loads((root / "summary.json").read_text())
    if summary["status"] != "PILOT_COMPLETE_REVIEW_REQUIRED":
        raise ValueError("Completed pilot summary required")
    for model, digest in summary["model_manifest_sha256"].items():
        path = root / model / "run_manifest.json"
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError("Model manifest changed since reporting")
        manifest = json.loads(path.read_text())
        for name, expected in manifest["artifact_sha256"].items():
            if hashlib.sha256((path.parent / name).read_bytes()).hexdigest() != expected:
                raise ValueError(f"Model evidence changed: {model}/{name}")
    tables = root / "review_tables"
    tables.mkdir(exist_ok=True)
    lines = ["# Route B real-data review — CHG-0651", "",
             f"{len(summary['model_manifest_sha256'])} model(s); Kerala, Rajasthan and Himachal Pradesh. Calibration 1985–2014; "
             "historical reporting 1990–2010; SSP245/SSP585 reporting 2020–2080. "
             "These are diagnostic method comparisons, not independent outdoor observations.", ""]
    support = []
    for model in summary["model_manifest_sha256"]:
        units = pd.read_csv(root / model / "unit_support.csv")
        for (state, level), group in units.groupby(["state_name", "level"]):
            support.append({"model": model, "state": state, "level": level,
                            "eligible": int(group.eligible.sum()), "total": len(group)})
    support = pd.DataFrame(support)
    support.to_csv(tables / "support.csv", index=False)
    lines += ["## Coverage", "", table(support), "",
              "Units below 95% retained whole-polygon area remain missing. Neither method "
              "gains coverage by filling unsupported cells.", ""]
    holdout = pd.read_csv(root / "holdout_scores_all.csv")
    metrics = ["annual_mean_bias_c", "monthly_climatology_mae_c", "p99_bias_c",
               "days_ge_28_bias", "days_ge_30_bias", "days_ge_32_bias"]
    rows = []
    keys = ["model", "state_name", "level"]
    for key, group in holdout.groupby(keys):
        raw = group[group.kind == "raw"].set_index("unit_index")
        for kind, adjusted in group.groupby("kind"):
            adjusted = adjusted.set_index("unit_index")
            for metric in metrics:
                pair = pd.concat([raw[metric].abs().rename("raw"),
                                  adjusted[metric].abs().rename("adjusted")], axis=1).dropna()
                rows.append({**dict(zip(keys, key)), "path": kind, "metric": metric,
                             "units": len(pair), "raw_median_absolute_error": pair.raw.median(),
                             "median_absolute_error": pair.adjusted.median(),
                             "fraction_improved": (pair.adjusted < pair.raw).mean(),
                             "fraction_worsened": (pair.adjusted > pair.raw).mean()})
    held = pd.DataFrame(rows)
    held.to_csv(tables / "heldout_error_comparison.csv", index=False)
    lines += ["## Held-out shade performance", "",
              "Train 1985–1999, evaluate 2000–2014 against UHE-Daily. Errors compare "
              "distributions and mean annual counts; they do not compare synchronized weather. "
              "The table below shows district seasonal MAE in °C. The CSV includes blocks, "
              "p99 and all three thresholds, with fractions improving and worsening.", "",
              table(held[(held.level == "district") &
                         (held.metric == "monthly_climatology_mae_c")].drop(columns=["level", "metric"])), "",
              "Rare-threshold zeros can produce ties or small absolute errors; these are "
              "not evidence of accurate extreme-event prediction. Reference counts remain "
              "available in holdout_scores_all.csv.", ""]
    gaps = pd.read_csv(root / "aggregation_gaps.csv")
    gap_rows = []
    for key, group in gaps.groupby(keys + ["comparison", "scenario", "period"]):
        for metric in ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32"):
            values = group[metric].dropna()
            gap_rows.append({**dict(zip(keys + ["comparison", "scenario", "period"], key)),
                             "metric": metric, "units": len(values), "median_signed": values.median(),
                             "median_absolute": values.abs().median(),
                             "p95_absolute": values.abs().quantile(.95),
                             "maximum_absolute": values.abs().max()})
    gap = pd.DataFrame(gap_rows)
    gap.to_csv(tables / "aggregation_differences.csv", index=False)
    lines += ["## Aggregation differences", "",
              "Signed aggregation-order differences are grid-first regional series minus "
              "region-first series. Cell-metric differences are area-average cell metrics "
              "minus metrics of the grid-first regional series. The latter changes what is "
              "being measured, even when the underlying daily field is identical.", "",
              "Historical district differences for ≥30 °C days/year:", "",
              table(gap[(gap.level == "district") & (gap.scenario == "historical") &
                        (gap.metric == "days_ge_30")].drop(columns=["level", "scenario", "period", "metric"])), "",
              "The CSV retains all future windows and thresholds. An aggregation-order "
              "comparison alone cannot select the intended exposure metric.", ""]
    w1 = pd.read_csv(root / "w1_matched_support_comparison.csv")
    matched = w1[w1.status == "MATCHED_SPATIAL_SUPPORT_METHOD_COMPARISON"]
    wrows = []
    groups = matched.groupby(["model", "state_name", "level", "variant"]) if len(matched) else []
    for key, group in groups:
        row = dict(zip(["model", "state_name", "level", "variant"], key))
        row["units"] = len(group)
        for metric in ("mean_c", "days_ge_28", "days_ge_30", "days_ge_32"):
            row[metric + "_median_delta"] = group[metric + "_delta"].median()
        wrows.append(row)
    compared = pd.DataFrame(wrows)
    compared.to_csv(tables / "w1_matched_differences.csv", index=False)
    lines += ["## W1 comparison", "",
              "Route B minus W1 on identical complete 2005 cell support, requiring ≥95% "
              "polygon coverage. Only ACCESS-CM2 has predecessor caches. Baseline W1 and "
              "its combined humidity/elevation diagnostic variant are separate comparisons.", "",
              table(compared[compared.level == "district"]) if len(compared) else "No matched comparisons available.", "",
              "Differences include reference correction, humidity, radiation and elevation "
              "treatments. They are not observational accuracy scores.", ""]
    windows, radiation = [], []
    for model in summary["model_manifest_sha256"]:
        windows.append(pd.read_csv(root / model / "application_window_sensitivity.csv").assign(model=model))
        radiation.append(pd.read_csv(root / model / "radiation_diagnostics.csv").assign(model=model))
    windows = pd.concat(windows, ignore_index=True)
    windows.to_csv(tables / "application_windows.csv", index=False)
    radiation = pd.concat(radiation, ignore_index=True)
    clips = radiation.groupby(["model", "scenario", "kind"])[
        ["radiation_clip_fraction", "wind_clip_fraction", "conservation_max_wm2"]].agg(["median", "max"])
    clips.to_csv(tables / "solar_clipping.csv")
    lines += ["## Window sensitivity and solar checks", "",
              "For the three preselected districts per model, compare separate future "
              "application windows with one 2020–2080 window. This is a sensitivity check, "
              "not a fitted choice of windows.", "", table(windows), "",
              f"Maximum recorded radiation conservation residual: {radiation.conservation_max_wm2.max():.3g} W/m² "
              "(gate 1e-8). Solar radiation and wind clipping frequencies are tabulated "
              "separately; conservation does not validate the empirical sun adjustment.", ""]
    cost = pd.read_csv(root / "cost_evidence.csv")
    lines += ["## Measured cost", "", table(cost.drop(columns=["scope"])), "",
              "Costs include full and held-out QDM fitting and historical/future applications. "
              "They exclude solar, I/O and geometry preparation. ACCESS-CM2's recovered "
              "timings include its original fits; its resumed wall time alone understates "
              "total work. A national budget needs the actual cell/region counts and model "
              "roster; multiplying only by the number of scenarios would double-count "
              "applications already included here.", "",
              "## Decision limits", "",
              "The pilot supplies evidence for an aggregation and metric-semantics decision. "
              "It does not establish independent outdoor-WBGT accuracy or authorize publication. "
              "Direct native-polygon reference sensitivity, independent rural/mountain evidence, "
              "coastal exclusions, future-window effects and the remaining model roster still "
              "need explicit treatment before a national production specification.", ""]
    (root / "REVIEW.md").write_text("\n".join(lines))
    print(root / "REVIEW.md")


def main() -> None:
    """Plan or write the diagnostic evidence review."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_route_b_pilot"))
    args = parser.parse_args()
    if args.run:
        review(args.out_dir)
    else:
        print(f"Plan: read completed evidence under {args.out_dir}; write REVIEW.md and review_tables/*.csv")


if __name__ == "__main__":
    main()
