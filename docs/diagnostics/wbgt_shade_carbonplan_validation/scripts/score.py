"""Score held-out runs against UHE-Daily (SPEC.md sections 5, 7 and 9; CHG-0646).

Run from the repository root in the pinned py3.10 environment (numpy/pandas
only) after run_validation.py. Reads holdout run directories, applies IRT's
reporting calendar to every series, and writes durable tables into
``results/``. No value is re-fitted here.
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

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from tools.diagnostics import wbgt_shade_cp_validation as V  # noqa: E402

HERE = Path(__file__).resolve().parents[1]
WORK = Path("scratch/carbonplan_shade_validation")
TEST = V.Window(2000, 2014)
N_YEARS = TEST.end - TEST.start + 1
N_BOOT, SEED = 1000, 20261003


def read_daily(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, index_col=0, parse_dates=True)
    frame.columns = [int(c) for c in frame.columns]
    return frame


def load_uhe(work: Path) -> pd.DataFrame:
    z = np.load(work / "uhe_1985_2014.npz")
    return pd.DataFrame(z["values"], index=pd.DatetimeIndex(z["dates"]), columns=list(z["processing_id"]))


def unit_meta(work: Path) -> pd.DataFrame:
    """Unit type, centroid and dominant IRT district (largest intersection) per processing_id."""
    units = json.loads((work / "units.geojson").read_text())["features"]
    cells = pd.read_csv(work / "cells.csv")
    cen = cells.groupby("processing_id")[["lat", "lon"]].mean()
    rows = [{"processing_id": int(f["properties"]["processing_id"]), "hierid": f["properties"]["hierid"],
             "unit_type": f["properties"]["unit_type"]} for f in units]
    meta = pd.DataFrame(rows).set_index("processing_id").join(cen)
    cw = pd.read_csv(work / "crosswalk.csv")
    d = cw[(cw.level == "district") & (cw.processing_id >= 0)]
    dom = d.sort_values("intersection_m2").groupby("processing_id").tail(1).set_index("processing_id")
    meta["dominant_district"] = dom.state + " / " + dom.district
    home = d.groupby(["processing_id", "state"]).intersection_m2.sum().reset_index()
    meta["home_state"] = home.sort_values("intersection_m2").groupby("processing_id").tail(1).set_index(
        "processing_id").state
    return meta


def irt(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    d, valid = V.irt_calendar(frame, TEST)
    return d, valid.all(axis=0)


def score_run(run: Path, tag: str, uhe: pd.DataFrame, meta: pd.DataFrame, qdm_file: str | None = None,
              label: str = "held-out") -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """Per-unit scores (raw and qdm) and screening rows for one run."""
    raw = read_daily(run / f"raw_daily__{tag}.csv.gz")
    qdm = read_daily(run / (qdm_file or f"qdm_test_daily__{tag}.csv.gz"))
    raw.index, qdm.index = raw.index.normalize(), qdm.index.normalize()
    units = [u for u in qdm.columns if u in raw.columns]
    rd, rv = irt(raw[units])
    qd, qv = irt(qdm[units])
    ud, uv = irt(uhe[units])
    ok = rv & qv & uv
    exclusions = [{"processing_id": int(u), "reason": "incomplete/non-finite test-period unit-year "
                   f"(raw {bool(rv[u])}, qdm {bool(qv[u])}, uhe {bool(uv[u])})"} for u in ok.index[~ok]]
    keep = list(ok.index[ok])
    s_raw = V.unit_scores(rd[keep], ud[keep], N_YEARS)
    s_qdm = V.unit_scores(qd[keep], ud[keep], N_YEARS)
    table = s_raw.add_suffix("__raw").join(s_qdm.add_suffix("__qdm"))
    masks, labels = {}, {}
    for metric in V.PRIMARY_METRICS:
        zz = None
        if metric.startswith("days_ge_"):
            zz = V.zero_zero_mask(s_raw, s_qdm, int(metric.split("_")[2]))
        labels[metric] = V.classify_change(s_raw[metric].to_numpy(), s_qdm[metric].to_numpy(),
                                           V.metric_band(metric), zz)
        table[metric + "__change"] = labels[metric]
        masks[metric] = np.isin(labels[metric], ("improved", "worsened", "unchanged"))
    table = table.join(meta, how="left")
    rows = []
    subsets = {"all": np.ones(len(keep), bool),
               "cities": (table.unit_type == "city").to_numpy(),
               "regions": (table.unit_type == "region").to_numpy()}
    years_m, years_r = rd.index.year.to_numpy(), ud.index.year.to_numpy()
    mon_m, mon_r = rd.index.month.to_numpy(), ud.index.month.to_numpy()
    arrays = (rd[keep].to_numpy(float), qd[keep].to_numpy(float), ud[keep].to_numpy(float))
    all_masks = {(name, k): v & sub for name, sub in subsets.items() for k, v in masks.items()}
    ci = V.bootstrap_median_change(*arrays, years_m, years_r, mon_m, mon_r, all_masks,
                                   n_boot=N_BOOT, seed=SEED)
    for name, sub in subsets.items():
        for metric in V.PRIMARY_METRICS:
            m = all_masks[(name, metric)]
            lab = labels[metric]
            e_raw, e_qdm = s_raw[metric].to_numpy(), s_qdm[metric].to_numpy()
            row = {"subset": name, "metric": metric, "label": label,
                   "units_scored": int(sub.sum()), "units_classified": int(m.sum()),
                   "zero_zero": int(((lab == "zero/zero") & sub).sum())}
            if m.any():
                row.update({
                    "median_abs_err_raw": float(np.median(np.abs(e_raw[m]))),
                    "median_abs_err_qdm": float(np.median(np.abs(e_qdm[m]))),
                    "median_signed_err_raw": float(np.median(e_raw[m])),
                    "median_signed_err_qdm": float(np.median(e_qdm[m])),
                    "ci90_change_lo": ci[(name, metric)][0], "ci90_change_hi": ci[(name, metric)][1],
                    "share_improved": float((lab[m] == "improved").mean()),
                    "share_unchanged": float((lab[m] == "unchanged").mean()),
                    "share_worsened": float((lab[m] == "worsened").mean()),
                    "verdict": V.screening_verdict(e_raw[m], e_qdm[m], lab[m], ci[(name, metric)])})
            else:
                row["verdict"] = "no classified units"
            if metric.startswith("days_ge_"):
                t = int(metric.split("_")[2])
                sup = s_raw[f"support_{t}"].to_numpy()[sub]
                row.update({f"support_{k}": int((sup == k).sum()) for k in ("zero", "sparse", "supported")})
                row["ref_mean_days_median"] = float(np.median(s_raw[f"ref_days_ge_{t}"].to_numpy()[sub]))
            rows.append(row)
    return table, pd.DataFrame(exclusions, columns=["processing_id", "reason"]), rows


def regressions(table: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Worst per-unit regressions per primary metric, with location."""
    out = []
    for metric in V.PRIMARY_METRICS:
        d = (table[metric + "__qdm"].abs() - table[metric + "__raw"].abs()).rename("delta_abs_err")
        sel = table.assign(delta_abs_err=d)[table[metric + "__change"] == "worsened"]
        sel = sel.sort_values("delta_abs_err", ascending=False).head(n)
        for pid, r in sel.iterrows():
            out.append({"metric": metric, "processing_id": pid, "hierid": r.hierid, "unit_type": r.unit_type,
                        "lat": r.lat, "lon": r.lon, "dominant_district": r.dominant_district,
                        "err_raw": r[metric + "__raw"], "err_qdm": r[metric + "__qdm"],
                        "delta_abs_err": r.delta_abs_err})
    return pd.DataFrame(out)


def monthly(table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for m in range(1, 13):
        for kind in ("raw", "qdm"):
            v = table[f"bias_m{m:02d}__{kind}"]
            rows.append({"month": m, "series": kind, "median_bias": v.median(), "p10": v.quantile(.1),
                         "p90": v.quantile(.9), "median_abs_bias": v.abs().median()})
    return pd.DataFrame(rows)


def future_weighting(work_runs: Path) -> pd.DataFrame:
    """Population vs area after QDM in the two published future windows (Kerala ACCESS-CM2)."""
    rows = []
    for name, w in (("ssp245-2030", V.Window(2020, 2039)), ("ssp245-2050", V.Window(2040, 2059))):
        frames = {}
        for wt in ("population", "area"):
            path = work_runs / f"Kerala__ACCESS-CM2__{wt}" / f"qdm_future_{name}__Kerala__ACCESS-CM2__{wt}.csv.gz"
            f = read_daily(path)
            f.index = f.index.normalize()
            d, valid = V.irt_calendar(f, w)
            if not valid.all().all():
                raise ValueError(f"Incomplete future years in {path}")
            frames[wt] = d
        units = sorted(set(frames["population"].columns) & set(frames["area"].columns))
        for u in units:
            p, a = frames["population"][u], frames["area"][u]
            row = {"window": name, "processing_id": u, "annual_mean_diff": p.mean() - a.mean(),
                   "max_abs_daily_diff": float((p - a).abs().max())}
            for q in V.TAIL_QUANTILES:
                row[f"p{round(q * 100)}_diff"] = p.quantile(q) - a.quantile(q)
            for t in V.THRESHOLDS:
                n = w.end - w.start + 1
                row[f"days_ge_{t}_diff"] = ((p >= t).sum() - (a >= t).sum()) / n
            rows.append(row)
    return pd.DataFrame(rows)


def weighting_compare(scores: dict[str, pd.DataFrame], state: str, model: str) -> pd.DataFrame:
    """Per-unit population minus area for raw and held-out QDM test metrics."""
    p, a = scores.get(f"{state}__{model}__population"), scores.get(f"{state}__{model}__area")
    if p is None or a is None:
        return pd.DataFrame()
    units = p.index.intersection(a.index)
    rows = []
    for kind in ("raw", "qdm"):
        for col in ("annual_mean_bias", "p90_bias", "p95_bias", "p99_bias", "days_ge_28", "days_ge_30", "days_ge_32"):
            d = p.loc[units, f"{col}__{kind}"] - a.loc[units, f"{col}__{kind}"]
            band = V.COUNT_BAND_DAYS if col.startswith("days") else V.TEMPERATURE_BAND_C
            rows.append({"state": state, "model": model, "series": kind, "metric": col, "units": len(units),
                         "median_abs_diff": d.abs().median(), "max_abs_diff": d.abs().max(),
                         "units_over_band": int((d.abs() > band).sum()), "band": band})
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--runs-dir", type=Path, default=WORK / "runs")
    parser.add_argument("--fidelity-dir", type=Path, default=WORK / "fidelity")
    parser.add_argument("--out-dir", type=Path, default=HERE / "results")
    args = parser.parse_args()
    started = time.time()
    out = args.out_dir
    (out / "units").mkdir(parents=True, exist_ok=True)
    uhe, meta = load_uhe(args.work_dir), unit_meta(args.work_dir)
    meta.to_csv(out / "unit_metadata.csv")
    screening, excl, scores, wcomp = [], [], {}, []
    for run in sorted(p for p in args.runs_dir.iterdir() if (p / "manifest.json").exists()):
        tag = run.name
        state, model, weighting = tag.split("__")
        table, ex, rows = score_run(run, tag, uhe, meta)
        state_units = table.home_state == state.replace("_", " ")
        table["counted_in_pooled"] = state_units
        scores[tag] = table
        table.to_csv(out / "units" / f"scores__{tag}.csv")
        regressions(table).assign(run=tag).to_csv(out / "units" / f"worst_regressions__{tag}.csv", index=False)
        monthly(table).assign(run=tag).to_csv(out / "units" / f"monthly_bias__{tag}.csv", index=False)
        clusters = (table.assign(**{m: table[m + "__change"] == "worsened" for m in V.PRIMARY_METRICS})
                    .groupby("dominant_district")[list(V.PRIMARY_METRICS)].sum())
        clusters["units"] = table.groupby("dominant_district").size()
        clusters.to_csv(out / "units" / f"worsened_by_district__{tag}.csv")
        for r in rows:
            r.update({"state": state, "model": model, "weights": weighting})
        screening += rows
        ex["run"] = tag
        excl.append(ex)
        print(tag, len(table), "units scored", flush=True)
    # H0 context (in-sample, NOT validation): published-equivalent series, 2000-2014.
    g2 = pd.read_csv(args.fidelity_dir / "g2_daily_kerala_access_pop.csv.gz", parse_dates=["date"])
    tmp = args.fidelity_dir / "_h0"
    tmp.mkdir(exist_ok=True)
    g2.pivot(index="date", columns="processing_id", values="raw").to_csv(tmp / "raw_daily__h0.csv.gz")
    g2.pivot(index="date", columns="processing_id", values="reproduced").to_csv(tmp / "qdm_test_daily__h0.csv.gz")
    h0, _, h0_rows = score_run(tmp, "h0", uhe, meta, label="in-sample context (H0), not validation")
    h0.to_csv(out / "units" / "scores__H0_in_sample__Kerala__ACCESS-CM2__population.csv")
    for r in h0_rows:
        r.update({"state": "Kerala", "model": "ACCESS-CM2", "weights": "population"})
    screening += h0_rows
    cols = ["label", "state", "model", "weights", "subset", "metric"]
    sc = pd.DataFrame(screening)
    sc = sc[cols + [c for c in sc.columns if c not in cols]]
    sc.to_csv(out / "screening_summary.csv", index=False)
    pd.concat(excl).to_csv(out / "holdout_exclusions.csv", index=False)
    pooled = []
    for model in sorted({t.split("__")[1] for t in scores}):
        for weighting in ("population", "area"):
            parts = [t[t.counted_in_pooled] for k, t in scores.items()
                     if k.split("__")[1] == model and k.split("__")[2] == weighting]
            if not parts:
                continue
            allu = pd.concat(parts)
            for metric in V.PRIMARY_METRICS:
                lab = allu[metric + "__change"]
                m = lab.isin(["improved", "worsened", "unchanged"])
                pooled.append({"model": model, "weights": weighting, "metric": metric, "units": int(m.sum()),
                               "states": len(parts),
                               "median_abs_err_raw": allu.loc[m, metric + "__raw"].abs().median(),
                               "median_abs_err_qdm": allu.loc[m, metric + "__qdm"].abs().median(),
                               "share_improved": (lab[m] == "improved").mean(),
                               "share_worsened": (lab[m] == "worsened").mean(),
                               "note": "descriptive pooled summary; per-state verdicts govern"})
    pd.DataFrame(pooled).to_csv(out / "pooled_summary.csv", index=False)
    for state in ("Kerala", "Rajasthan", "Himachal Pradesh"):
        for model in ("ACCESS-CM2", "CMCC-CM2-SR5"):
            wcomp.append(weighting_compare(scores, state.replace(" ", "_"), model))
    pd.concat(wcomp).to_csv(out / "weighting_heldout_comparison.csv", index=False)
    fw = future_weighting(args.runs_dir)
    fw.to_csv(out / "weighting_future_comparison.csv", index=False)
    log = {"seconds": round(time.time() - started, 1),
           "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
           "runs": sorted(scores), "bootstrap": {"B": N_BOOT, "seed": SEED}}
    (out / "score_manifest.json").write_text(json.dumps(log, indent=2))
    print(json.dumps(log), flush=True)


if __name__ == "__main__":
    main()
