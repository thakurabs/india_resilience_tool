"""Compare the Kerala CarbonPlan-shade pilot with deployed shade-peak-v1 (SPEC.md).

Run from the repository root after run_pilot.py, in either environment (pandas,
scipy). Reads shade-peak-v1 read-only from the national staging tree and writes
durable comparison tables into this diagnostic's directory.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parents[1]
RUN = Path("scratch/carbonplan_kerala/run")
STAGING = Path("scratch/wbgt_shade_national/processed")
STATE, MODEL, SCENARIO = "Kerala", "ACCESS-CM2", "historical"
SLUGS = ("annual_mean", "days_ge_28", "days_ge_30", "days_ge_32")
SIGNATURE_PREFIX = "shade-peak-v1:"
KEYS = ["level", "district", "block"]


def load_irt(kind: str) -> pd.DataFrame:
    """Read shade-peak-v1 period or yearly CSVs for every Kerala unit, matched by content."""
    frames = []
    for slug in SLUGS:
        root = STAGING / f"wbgt_shade_stull_{slug}" / STATE
        for level, pattern in (("district", f"districts/*/{MODEL}/{SCENARIO}/*_{kind}.csv"),
                               ("block", f"blocks/*/*/{MODEL}/{SCENARIO}/*_{kind}.csv")):
            for path in sorted(root.glob(pattern)):
                f = pd.read_csv(path)
                if not f.shade_method_signature.str.startswith(SIGNATURE_PREFIX).all():
                    raise ValueError(f"Unexpected method signature in {path}")
                f["level"], f["slug"] = level, slug
                if "block" not in f:
                    f["block"] = ""
                cols = KEYS + ["slug", "value"] + (["year"] if kind == "yearly" else ["period"])
                frames.append(f[cols])
    out = pd.concat(frames, ignore_index=True)
    if kind == "periods" and not (out.period == "1990-2010").all():
        raise ValueError("Unexpected IRT period")
    return out.rename(columns={"value": "irt_shade_peak_v1"})


def spread(d: pd.Series) -> dict:
    """Distribution of pilot-minus-IRT differences across units."""
    return {"mean": d.mean(), "median": d.median(), "min": d.min(), "max": d.max(),
            "p25": d.quantile(.25), "p75": d.quantile(.75), "mean_abs": d.abs().mean()}


def main() -> None:
    """Join pilot and IRT per unit; write comparison tables and a summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=RUN)
    parser.add_argument("--out-dir", type=Path, default=HERE / "results")
    args = parser.parse_args()
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)

    pilot_p = pd.read_csv(args.run_dir / "unit_periods.csv", keep_default_na=False)
    pilot_y = pd.read_csv(args.run_dir / "unit_yearly.csv", keep_default_na=False)
    irt_p, irt_y = load_irt("periods"), load_irt("yearly")

    period = pilot_p.merge(irt_p, on=KEYS + ["slug"], how="outer", indicator=True)
    unmatched = period[period._merge != "both"][KEYS + ["slug", "_merge"]]
    period = period[period._merge == "both"].drop(columns="_merge")
    period["diff_region_first"] = period.value_region_first - period.irt_shade_peak_v1
    period["diff_unit_series"] = period.value_unit_series - period.irt_shade_peak_v1
    period.to_csv(out / "unit_period_comparison.csv", index=False)
    unmatched.to_csv(out / "unmatched_units.csv", index=False)

    yearly = pilot_y.merge(irt_y, on=KEYS + ["slug", "year"], how="inner")
    yearly["diff_region_first"] = yearly.value_region_first - yearly.irt_shade_peak_v1
    yearly[KEYS + ["slug", "year", "value_region_first", "value_unit_series", "irt_shade_peak_v1",
                   "diff_region_first"]].to_csv(out / "unit_yearly_comparison.csv", index=False)

    rows = []
    for (level, slug), g in period.groupby(["level", "slug"]):
        rho = spearmanr(g.value_region_first, g.irt_shade_peak_v1).correlation \
            if g.irt_shade_peak_v1.nunique() > 1 and g.value_region_first.nunique() > 1 else np.nan
        y = yearly[(yearly.level == level) & (yearly.slug == slug)]
        rows.append({"level": level, "slug": slug, "units": len(g),
                     "pilot_mean": g.value_region_first.mean(), "irt_mean": g.irt_shade_peak_v1.mean(),
                     **{f"diff_{k}": v for k, v in spread(g.diff_region_first).items()},
                     "unit_series_diff_mean": g.diff_unit_series.mean(),
                     "spearman_units": rho,
                     "yearly_correlation_mean": y.groupby(KEYS).apply(
                         lambda h: h.value_region_first.corr(h.irt_shade_peak_v1)).mean(),
                     "units_flagged_coverage": int((g.coverage < .95).sum())})
    summary = pd.DataFrame(rows)
    summary.to_csv(out / "summary.csv", index=False)
    (out / "comparison_manifest.json").write_text(json.dumps({
        "pilot_run": str(args.run_dir), "irt_source": str(STAGING), "model": MODEL, "scenario": SCENARIO,
        "matched_units": {lvl: int(period[period.level == lvl][KEYS].drop_duplicates().shape[0])
                          for lvl in ("district", "block")},
        "unmatched_rows": int(len(unmatched)),
        "difference_convention": "pilot minus shade-peak-v1"}, indent=2))
    with pd.option_context("display.width", 200):
        print(summary.round(3).to_string(index=False))
    print(f"unmatched rows: {len(unmatched)}")


if __name__ == "__main__":
    main()
