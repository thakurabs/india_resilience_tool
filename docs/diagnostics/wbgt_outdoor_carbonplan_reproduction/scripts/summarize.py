"""Roll up the reproduction against published CarbonPlan and against the frozen W1 pilot.

Reads a completed reproduce.py output directory plus the predecessor five-city W1
comparison, and writes the cross-city tables used by README.md. Read-only with
respect to both inputs; all output lands inside the reproduction run directory.

No figures are produced. The headline result is a pair of error magnitudes five
orders of magnitude apart, which a table states exactly and a chart would only
blur; the predecessor comparison's figures remain the place for seasonal shape.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

W1_DIR = Path("docs/diagnostics/wbgt_outdoor_carbonplan_multicity/results")
THRESHOLDS = (28, 30, 32)
PRIMARY = "consistent3600s"
# Raw-shade separation below which two days in one QDM group are indistinguishable
# and their rank order is float-level ambiguous. Diagnostic only; nothing is corrected.
TIE_EPS_C = 1e-4


def residual_scales(out: Path, cities: list[str]) -> pd.DataFrame:
    """Decompose the shade residual by magnitude band across all calibration days."""
    rows = []
    for city in cities:
        d = pd.read_csv(out / city / "shade_daily_1985_2014.csv", parse_dates=["date"]).set_index("date")
        r = (d.reproduced_shade - d.published_shade).abs()
        rows.append({"city": city, "days": len(r),
                     "median_abs_c": float(r.median()), "p99_abs_c": float(r.quantile(.99)),
                     "max_abs_c": float(r.max()),
                     "days_within_1e6": int((r <= 1e-6).sum()),
                     "days_within_1e4": int((r <= 1e-4).sum()),
                     "days_within_1e3": int((r <= 1e-3).sum()),
                     "days_above_1e3": int((r > 1e-3).sum())})
    return pd.DataFrame(rows)


def tie_break_pairs(out: Path, cities: list[str]) -> pd.DataFrame:
    """Find day pairs whose near-tied raw shade led QDM to exchange adjustment factors.

    CarbonPlan's QDM groups by day-of-year with a 31-day window and maps ranked
    quantiles. Two days in one group whose raw values agree to float precision have
    an ambiguous rank order, so each can receive the other's adjustment factor. This
    reports such pairs; it does not re-pair them or alter any score.
    """
    rows = []
    for city in cities:
        d = pd.read_csv(out / city / "shade_daily_1985_2014.csv", parse_dates=["date"]).set_index("date")
        d = d.assign(ours=d.reproduced_shade - d.raw_shade, theirs=d.published_shade - d.raw_shade)
        d["resid"] = (d.reproduced_shade - d.published_shade).abs()
        suspect = d[d.resid > 1e-3]
        doy = d.index.dayofyear
        for ts, row in suspect.iterrows():
            # Same QDM group: day-of-year within the 31-day window, excluding self.
            lo, hi = ts.dayofyear - 15, ts.dayofyear + 15
            group = d[(doy >= lo) & (doy <= hi) & (d.index != ts)]
            near = group[(group.raw_shade - row.raw_shade).abs() <= TIE_EPS_C]
            if near.empty:
                rows.append({"city": city, "date": ts.date(), "raw_shade_c": row.raw_shade,
                             "abs_residual_c": row.resid, "partner": None,
                             "raw_separation_c": np.nan, "factors_exchanged": False,
                             "residual_after_pairing_c": np.nan})
                continue
            # The partner whose published factor best explains our factor.
            partner = (near.theirs - row.ours).abs().idxmin()
            p = near.loc[partner]
            exchanged = (abs(p.theirs - row.ours) < 1e-3) and (abs(row.theirs - p.ours) < 1e-3)
            rows.append({"city": city, "date": ts.date(), "raw_shade_c": row.raw_shade,
                         "abs_residual_c": row.resid, "partner": str(pd.Timestamp(partner).date()),
                         "raw_separation_c": float(abs(p.raw_shade - row.raw_shade)),
                         "factors_exchanged": bool(exchanged),
                         "residual_after_pairing_c": float(abs(p.theirs - row.ours))})
    return pd.DataFrame(rows)


def w1_baseline() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the predecessor frozen-W1 comparison; it is read-only evidence."""
    annual = pd.read_csv(W1_DIR / "annual_summary.csv")
    thresholds = pd.read_csv(W1_DIR / "threshold_summary.csv")
    return annual, thresholds


def head_to_head(frame: pd.DataFrame, cities: list[str], years: list[int]) -> pd.DataFrame:
    """Place the reproduction and W1 side by side against the same published target."""
    annual, _ = w1_baseline()
    repro = frame[(frame.stage == "full_sun_independent") & (frame.interpretation == PRIMARY)].copy()
    repro["year"] = repro.year.astype(int)
    rows = []
    for city in cities:
        for year in years:
            r = repro[(repro.city == city) & (repro.year == year)]
            w = annual[(annual.city == city) & (annual.year == year)]
            if len(r) != 1 or len(w) != 1:
                raise ValueError(f"Missing paired row for {city} {year}")
            r, w = r.iloc[0], w.iloc[0]
            rows.append({
                "city": city, "year": year,
                "published_mean_c": r.reference_mean_c,
                "repro_bias_c": r.bias_c, "repro_mae_c": r.mae_c, "repro_max_abs_c": r.max_abs_c,
                "repro_counts_match": bool(r.counts_match),
                "w1_bias_c": w.matched_mean_difference_c, "w1_mae_c": w.matched_mae_c,
                "w1_rmse_c": w.matched_rmse_c, "w1_correlation": w.matched_correlation,
                "mae_ratio_w1_over_repro": w.matched_mae_c / r.mae_c if r.mae_c else np.nan,
            })
    return pd.DataFrame(rows)


def count_overlap(frame: pd.DataFrame, cities: list[str], years: list[int]) -> pd.DataFrame:
    """Contrast net count agreement with day-level agreement, where W1 cancels."""
    _, thresholds = w1_baseline()
    repro = frame[(frame.stage == "full_sun_independent") & (frame.interpretation == PRIMARY)].copy()
    repro["year"] = repro.year.astype(int)
    rows = []
    for city in cities:
        for year in years:
            r = repro[(repro.city == city) & (repro.year == year)].iloc[0]
            for t in THRESHOLDS:
                w = thresholds[(thresholds.city == city) & (thresholds.year == year)
                               & (thresholds.threshold_c == t)]
                if len(w) != 1:
                    raise ValueError(f"Missing W1 threshold row for {city} {year} {t}")
                w = w.iloc[0]
                rows.append({
                    "city": city, "year": year, "threshold_c": t,
                    "published_days": int(r[f"reference_days_ge_{t}"]),
                    "repro_days": int(r[f"days_ge_{t}"]),
                    "repro_day_level_disagreements": 0 if bool(r.counts_match) else None,
                    "w1_days": int(w.w1_days_complete_year),
                    "w1_net_day_difference": int(w.w1_days_complete_year - w.carbonplan_days_complete_year),
                    "w1_published_only_days": int(w.cp_only_days_on_matched_dates),
                    "w1_only_days": int(w.w1_only_days_on_matched_dates),
                    "w1_day_level_disagreements": int(w.cp_only_days_on_matched_dates
                                                      + w.w1_only_days_on_matched_dates),
                })
    return pd.DataFrame(rows)


def main() -> None:
    """Write the cross-city reproduction tables and the W1 head-to-head comparison."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True,
                        help="A completed reproduce.py output directory")
    args = parser.parse_args()
    out = args.out_dir.resolve()
    manifest = json.loads((out / "run_manifest.json").read_text())
    frame = pd.read_csv(out / "summary_all.csv")
    cities, years = manifest["cities"], manifest["years"]

    scales = residual_scales(out, cities)
    scales.to_csv(out / "residual_scales.csv", index=False)
    ties = tie_break_pairs(out, cities)
    ties.to_csv(out / "tie_break_pairs.csv", index=False)
    h2h = head_to_head(frame, cities, years)
    h2h.to_csv(out / "reproduction_vs_w1.csv", index=False)
    overlap = count_overlap(frame, cities, years)
    overlap.to_csv(out / "threshold_day_level.csv", index=False)

    explained = ties[ties.factors_exchanged] if not ties.empty else ties
    rollup = {
        "verdict": manifest["verdict"],
        "cities": cities, "years": years,
        "city_years_with_all_counts_matched": int(h2h.repro_counts_match.sum()),
        "city_years_scored": int(len(h2h)),
        "worst_repro_max_abs_c": float(h2h.repro_max_abs_c.max()),
        "worst_repro_mae_c": float(h2h.repro_mae_c.max()),
        "worst_w1_mae_c": float(h2h.w1_mae_c.max()),
        "median_mae_ratio_w1_over_repro": float(h2h.mae_ratio_w1_over_repro.median()),
        "calibration_days_total": int(scales.days.sum()),
        "calibration_days_above_1e3": int(scales.days_above_1e3.sum()),
        "tie_break_days_found": int(len(ties)),
        "tie_break_days_with_exchanged_factors": int(len(explained)),
        "tie_break_note": (
            "Days whose raw shade is indistinguishable from another day in the same "
            "day-of-year QDM group, so the rank order driving the adjustment factor is "
            "float-level ambiguous. Reported, not corrected; scores are as measured."),
    }
    (out / "rollup.json").write_text(json.dumps(rollup, indent=2))

    pd.set_option("display.width", 200)
    print("=== residual scales (shade, 1985-2014) ===")
    print(scales.to_string(index=False))
    print("\n=== tie-break pairs ===")
    print(ties.to_string(index=False) if not ties.empty else "none")
    print("\n=== reproduction vs W1 (end-to-end, vs published CarbonPlan) ===")
    print(h2h.to_string(index=False))
    print("\n=== rollup ===")
    print(json.dumps(rollup, indent=2))


if __name__ == "__main__":
    main()
