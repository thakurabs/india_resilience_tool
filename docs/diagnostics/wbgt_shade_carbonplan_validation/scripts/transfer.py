"""Transfer held-out CarbonPlan-unit results to IRT districts and blocks (SPEC.md section 8; CHG-0646).

Run from the repository root in the pinned py3.10 environment after score.py.
Counts are taken per CarbonPlan unit-year FIRST and then area-weighted, so the
admin values are "area-weighted regional exceedance days (fractional)". Nothing
is filled: an uncovered area or an ineligible unit leaves its share uncovered.
``shade-peak-v1`` is read read-only from the national staging tree and compared
descriptively only.
"""
from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from tools.diagnostics import wbgt_shade_cp_validation as V  # noqa: E402

HERE = Path(__file__).resolve().parents[1]
WORK = Path("scratch/carbonplan_shade_validation")
STAGING = Path("scratch/wbgt_shade_national/processed")
TEST = V.Window(2000, 2014)
V1_YEARS = V.Window(2000, 2010)
SLUGS = ("annual_mean", "days_ge_28", "days_ge_30", "days_ge_32")
KEYS = ["level", "admin_key"]
COVERAGE_FLAG = 0.95
MODEL = "ACCESS-CM2"


def read_daily(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, index_col=0, parse_dates=True)
    frame.columns = [int(c) for c in frame.columns]
    frame.index = frame.index.normalize()
    return frame


def unit_years(daily: pd.DataFrame, series: str) -> pd.DataFrame:
    """Per unit-year annual mean and inclusive counts on the IRT calendar; invalid -> NaN."""
    d, valid = V.irt_calendar(daily, TEST)
    out = {"annual_mean": d.groupby(d.index.year).mean().reindex(valid.index).where(valid)}
    for t, c in V.annual_counts(d, valid).items():
        out[f"days_ge_{t}"] = c
    frames = []
    for slug, table in out.items():
        long = table.stack(dropna=False).rename("value").reset_index()
        long.columns = ["year", "processing_id", "value"]
        long["slug"], long["series"] = slug, series
        frames.append(long)
    return pd.concat(frames, ignore_index=True)


def load_v1(state: str) -> pd.DataFrame:
    """shade-peak-v1 yearly values (ACCESS-CM2 historical), read-only, matched by names."""
    frames = []
    for slug in SLUGS:
        root = STAGING / f"wbgt_shade_stull_{slug}" / state
        for level, pattern in (("district", f"districts/*/{MODEL}/historical/*_yearly.csv"),
                               ("block", f"blocks/*/*/{MODEL}/historical/*_yearly.csv")):
            for path in sorted(root.glob(pattern)):
                f = pd.read_csv(path)
                if not f.shade_method_signature.str.startswith("shade-peak-v1:").all():
                    raise ValueError(f"Unexpected method signature in {path}")
                f["level"], f["slug"] = level, slug
                if "block" not in f:
                    f["block"] = ""
                frames.append(f[["level", "district", "block", "slug", "year", "value"]])
    if not frames:
        return pd.DataFrame(columns=["level", "district", "block", "slug", "year", "value"])
    return pd.concat(frames, ignore_index=True)


def block_detail(w: pd.DataFrame) -> pd.DataFrame:
    """Contributing units, dominant unit/weight and identical-weight-vector groups per admin unit."""
    rows = []
    for key, g in w[w.processing_id >= 0].groupby(KEYS + ["district", "block"]):
        g = g.sort_values("w", ascending=False)
        vector = tuple(zip(g.processing_id, g.w.round(9)))
        rows.append({**dict(zip(KEYS + ["district", "block"], key)), "contributing_units": len(g),
                     "dominant_processing_id": int(g.processing_id.iloc[0]),
                     "dominant_hierid": g.hierid.iloc[0], "dominant_weight": float(g.w.iloc[0]),
                     "coverage": float(g.coverage.iloc[0]), "weight_vector": json.dumps(vector)})
    out = pd.DataFrame(rows)
    out["shared_vector_group_size"] = out.groupby(["level", "weight_vector"]).admin_key.transform("size")
    return out.drop(columns="weight_vector")


def ranks(period: pd.DataFrame, state: str) -> pd.DataFrame:
    """Spearman between series across admin units, with tie counts for near-zero metrics."""
    rows = []
    for (level, slug), g in period.groupby(["level", "slug"]):
        p = g.pivot_table(index="admin_key", columns="series", values="value")
        for a, b in (("raw", "uhe"), ("qdm", "uhe"), ("raw", "qdm")):
            if a not in p or b not in p:
                continue
            q = p[[a, b]].dropna()
            rho = spearmanr(q[a], q[b]).correlation if len(q) > 2 and q[a].nunique() > 1 and q[b].nunique() > 1 \
                else np.nan
            rows.append({"state": state, "level": level, "slug": slug, "pair": f"{a} vs {b}", "units": len(q),
                         "spearman": rho, "ties_in_" + a: int(q[a].duplicated(keep=False).sum()),
                         "zeros_in_" + a: int((q[a] == 0).sum()), "zeros_in_" + b: int((q[b] == 0).sum())})
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--runs-dir", type=Path, default=WORK / "runs")
    parser.add_argument("--out-dir", type=Path, default=HERE / "results" / "transfer")
    args = parser.parse_args()
    started = time.time()
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    z = np.load(args.work_dir / "uhe_1985_2014.npz")
    uhe = pd.DataFrame(z["values"], index=pd.DatetimeIndex(z["dates"]), columns=list(z["processing_id"]))
    cw_all = pd.read_csv(args.work_dir / "crosswalk.csv", dtype={"admin_key": str})
    cw_all["block"] = cw_all.block.fillna("")  # districts carry no block; NaN keys would drop from groupby
    checks = json.loads((args.work_dir / "crosswalk_checks.json").read_text())
    units = json.loads((args.work_dir / "units.geojson").read_text())["features"]
    pids = [int(f["properties"]["processing_id"]) for f in units]
    integrity = {"unique_processing_ids": len(pids) == len(set(pids)), "states": {}}
    coverage_tables, details, rank_tables, v1_tables, period_tables = [], [], [], [], []
    for state in ("Kerala", "Rajasthan", "Himachal Pradesh"):
        tag = f"{state.replace(' ', '_')}__{MODEL}__population"
        run = args.runs_dir / tag
        if not (run / "manifest.json").exists():
            integrity["states"][state] = "no completed held-out run; transfer not performed"
            continue
        raw = read_daily(run / f"raw_daily__{tag}.csv.gz")
        qdm = read_daily(run / f"qdm_test_daily__{tag}.csv.gz")
        ref = uhe[[c for c in qdm.columns]]
        values = pd.concat([unit_years(raw[qdm.columns], "raw"), unit_years(qdm, "qdm"),
                            unit_years(ref, "uhe")], ignore_index=True)
        cw = cw_all[cw_all.state == state].copy()
        w = V.crosswalk_weights(cw, KEYS)
        sums = w[w.processing_id >= 0].groupby(KEYS).w.sum()
        integrity["states"][state] = {
            "admin_units": int(w.groupby(KEYS).ngroups),
            "weight_sum_max_dev": float((sums - 1).abs().max()),
            "zero_coverage_units": w[w.reason == "no coverage"][KEYS + ["district", "block"]].to_dict("records"),
            "double_counting": checks.get(state),
            "units_without_series": sorted(set(cw.processing_id[cw.processing_id >= 0]) - set(qdm.columns))}
        admin = []
        for series, g in values.groupby("series"):
            t = V.count_first_transfer(g.drop(columns="series"), w, KEYS)
            t["series"] = series
            admin.append(t)
        admin = pd.concat(admin, ignore_index=True)
        names = w.groupby(KEYS)[["district", "block"]].first().reset_index()
        admin = admin.merge(names, on=KEYS)
        admin["state"] = state
        admin["label"] = np.where(admin.slug.str.startswith("days_ge"),
                                  "area-weighted regional exceedance days (fractional)", "area-weighted annual mean")
        admin.to_csv(out / f"admin_yearly__{state.replace(' ', '_')}.csv.gz", index=False)
        period = (admin.groupby(KEYS + ["district", "block", "slug", "series"])
                  .agg(value=("value", "mean"), years=("value", "count"),
                       min_series_coverage=("series_coverage", "min")).reset_index())
        period["state"] = state
        period_tables.append(period)
        cov = w.groupby(KEYS + ["district", "block"]).agg(coverage=("coverage", "first"),
                                                          reason=("reason", "first")).reset_index()
        cov["state"], cov["flag_below_0.95"] = state, cov.coverage < COVERAGE_FLAG
        coverage_tables.append(cov)
        det = block_detail(w)
        det["state"] = state
        details.append(det)
        rank_tables.append(ranks(period, state))
        v1 = load_v1(state)
        if not v1.empty:
            v1 = v1[(v1.year >= V1_YEARS.start) & (v1.year <= V1_YEARS.end)]
            v1p = v1.groupby(["level", "district", "block", "slug"]).value.mean().rename("v1").reset_index()
            mine = admin[(admin.year >= V1_YEARS.start) & (admin.year <= V1_YEARS.end)]
            mp = mine.pivot_table(index=["level", "district", "block", "slug"], columns="series",
                                  values="value", aggfunc="mean").reset_index()
            mp["block"] = mp.block.fillna("")
            v1p["block"] = v1p.block.fillna("")
            j = mp.merge(v1p, on=["level", "district", "block", "slug"], how="outer", indicator=True)
            j["state"] = state
            v1_tables.append(j)
    pd.concat(coverage_tables).to_csv(out / "coverage.csv", index=False)
    pd.concat(details).to_csv(out / "block_detail.csv", index=False)
    pd.concat(rank_tables).to_csv(out / "rank_agreement.csv", index=False)
    pd.concat(period_tables).to_csv(out / "admin_period_2000_2014.csv", index=False)
    rows = []
    if v1_tables:
        j = pd.concat(v1_tables, ignore_index=True)
        j.to_csv(out / "shade_peak_v1_descriptive_2000_2010.csv", index=False)
        for (state, level, slug), g in j[j._merge == "both"].groupby(["state", "level", "slug"]):
            r = {"state": state, "level": level, "slug": slug, "units": len(g),
                 "note": "descriptive: UHE transferred via CarbonPlan units is a coarse common reference"}
            for col in ("raw", "qdm", "v1"):
                d = g[col] - g["uhe"]
                r[f"{col}_minus_uhe_median"] = d.median()
                r[f"{col}_minus_uhe_median_abs"] = d.abs().median()
                ok = g[[col, "uhe"]].dropna()
                r[f"{col}_spearman_vs_uhe"] = (spearmanr(ok[col], ok.uhe).correlation
                                               if len(ok) > 2 and ok[col].nunique() > 1 and ok.uhe.nunique() > 1
                                               else np.nan)
            rows.append(r)
        integrity["v1_unmatched"] = int((j._merge != "both").sum())
    pd.DataFrame(rows).to_csv(out / "shade_peak_v1_descriptive_summary.csv", index=False)
    integrity.update({"seconds": round(time.time() - started, 1),
                      "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)})
    (out / "transfer_checks.json").write_text(json.dumps(integrity, indent=2, default=str))
    print(json.dumps({k: v for k, v in integrity.items() if k != "states"}, default=str), flush=True)


if __name__ == "__main__":
    main()
