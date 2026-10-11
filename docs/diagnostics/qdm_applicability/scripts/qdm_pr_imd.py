"""Precipitation QDM applicability against IMD 0.25 deg gridded rainfall (read-only diagnostic).

Same holdout/future design, QDM settings, metric definitions and materiality floors as
docs/diagnostics/qdm_applicability/scripts/qdm_applicability.py; the reference is IMD,
regridded conservatively onto each NEX cell as the mean of the four IMD boxes it overlaps.
Adds r95p_interannual_variability (CV of yearly R95p) and a rainfall-zone breakdown, and
re-scores the Telangana ERA5 cells against IMD to test the 'ERA5 drizzle' claim.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import imdlib as imd

sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts")
sys.path.insert(0, ".")
import qdm_applicability as Q  # noqa: E402

SER = Path(".scratch/qdm_applicability/series_pr_national")
OUT = Path(".scratch/qdm_applicability/out_imd"); OUT.mkdir(parents=True, exist_ok=True)
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]


def imd_rain():
    r = imd.open_data("rain", 1990, 2010, "yearwise", "D:/projects/irt_data/imd").get_xarray()["rain"]
    return r.where(r > -998)


def imd_cell(r, la, lo):
    """Conservative regrid: mean of the four IMD boxes overlapping the NEX cell centred at (la, lo)."""
    acc = 0
    for a in (-0.125, 0.125):
        for b in (-0.125, 0.125):
            acc = acc + r.sel(lat=la + a, lon=lo + b, method="nearest").values
    return acc / 4.0


def frame(vals, years, months, days):
    return Q._finish(pd.DataFrame({"year": years, "month": months, "day": days, "pr": vals}))


def r95p_cv(e, b):
    wet = b.pr[b.pr >= 1.0]
    thr = np.percentile(wet, 95)
    yearly = np.array([g.pr[(g.pr >= 1.0) & (g.pr >= thr)].sum() for _, g in e.groupby("year")])
    return float(yearly.std(ddof=0) / abs(yearly.mean())) if abs(yearly.mean()) > 1e-6 else float(yearly.std(ddof=0))


def classify(g):
    fam = g.family.iloc[0]
    floor = Q.FLOORS["mm_rel"] * np.nanmean(np.abs(g.ref_test)) if fam == "mm_rel" else Q.FLOORS[fam]
    er = np.nanmean(np.abs(g.raw_test - g.ref_test)); eq = np.nanmean(np.abs(g.qdm_test - g.ref_test))
    sh = np.nanmean(np.abs(g.qdm_fut - g.raw_fut))
    if er - eq >= floor and eq <= 0.5 * er and sh >= floor:
        v = "MATERIAL"
    elif eq > er + 0.5 * floor:
        v = "WORSE"
    elif er < floor and sh >= floor:
        v = "FUTURE-ONLY (unverifiable)"
    elif eq < er and (er - eq) >= 0.5 * floor:
        v = "MARGINAL"
    else:
        v = "NO EFFECT"
    return pd.Series(dict(family=fam, floor=floor, ref=np.nanmean(g.ref_test), err_raw=er, err_qdm=eq,
                          raw_2050=np.nanmean(g.raw_fut), qdm_2050=np.nanmean(g.qdm_fut), shift_2050=sh,
                          chg_raw=np.nanmean(g.raw_fut - g.raw_hist), chg_qdm=np.nanmean(g.qdm_fut - g.qdm_hist),
                          n_units=g.unit.nunique(), verdict=v))


def main():
    r = imd_rain()
    t = pd.to_datetime(r.time.values)
    yrs, mos, dys = t.year.values, t.month.values, t.day.values
    cells = pd.read_csv(SER / "cells.csv")
    metrics = Q.make_pr_metrics()
    metrics["r95p_interannual_variability"] = ("ratio", r95p_cv)

    rows, zone = [], {}
    for model in MODELS:
        H = pd.read_parquet(SER / f"{model}_historical.parquet")
        F = pd.read_parquet(SER / f"{model}_ssp245.parquet")
        for _, c in cells.iterrows():
            key = f"{c.lat:.3f}_{c.lon:.3f}"
            ref = frame(imd_cell(r, c.lat, c.lon), yrs, mos, dys)
            if ref.pr.isna().mean() > 0.01:
                continue
            ann = ref.groupby("year").pr.sum().mean()
            zone[key] = "arid <600" if ann < 600 else "semi-arid 600-1000" if ann < 1000 else "sub-humid 1000-1500" if ann < 1500 else "humid >1500"
            hist = frame(H[key].values, H.year.values, H.month.values, H.day.values)
            fut = frame(F[key].values, F.year.values, F.month.values, F.day.values)
            for row in Q.score_unit(metrics, ref, hist, fut, Q.pr_corrector):
                rows.append(dict(unit=key, model=model, zone=zone[key], **row))
        print("done", model, flush=True)
    d = pd.DataFrame(rows)
    d.to_csv(OUT / "precipitation_imd_unit_rows.csv", index=False)

    nat = d.groupby("metric").apply(classify)
    nat.round(3).to_csv(OUT / "summary_precip_imd.csv")
    byz = d.groupby(["metric", "zone"]).apply(classify)[["err_raw", "err_qdm", "raw_2050", "qdm_2050", "verdict", "n_units"]]
    byz.round(2).to_csv(OUT / "summary_precip_imd_by_zone.csv")
    pd.set_option("display.width", 250, "display.max_rows", 300)
    print(nat.drop(columns=["family"]).round(2).sort_values("verdict").to_string())
    print("\nZone verdicts:\n", byz.verdict.unstack("zone").to_string())

    # Adjudicate the ERA5 drizzle claim on the same Telangana cells, 2003-2010 holdout years
    e = pd.read_csv("docs/diagnostics/qdm_applicability/data/precipitation_unit_rows.csv")
    tel = sorted(e.unit.unique())
    out = []
    for u in tel:
        _, la, lo = u.split("_"); la, lo = float(la), float(lo)
        ref = frame(imd_cell(r, la, lo), yrs, mos, dys)
        if ref.pr.isna().mean() > 0.01:
            continue
        test = ref[ref.year.isin(Q.TEST)]
        for m in ("pr_consecutive_dry_days_lt1mm", "cwd_consecutive_wet_days", "pr_max_1day_precip", "r99p_extreme_wet_precip", "r20mm_very_heavy_precip_days"):
            out.append(dict(unit=u, metric=m, imd=metrics[m][1](test, ref)))
    imdt = pd.DataFrame(out).groupby("metric").imd.mean()
    cmp = e.groupby("metric")[["ref_test", "raw_test"]].mean().rename(columns={"ref_test": "era5", "raw_test": "nex_raw"}).join(imdt, how="inner")
    cmp.round(1).to_csv(OUT / "telangana_era5_vs_imd.csv")
    print("\nTelangana cells, holdout 2003-2010: ERA5 vs IMD vs raw NEX\n", cmp.round(1))


if __name__ == "__main__":
    main()
