"""Wet-bulb metrics: QDM applicability against the national ERA5 daily reference (CHG-0710, test A).

Read-only diagnostic. Same holdout/future design, metric definitions, floors and verdict rule as
§4-§6 (qdm_applicability.py, qdm_pr_imd.classify). The reference is era5_daily_ref_build.py
(IST days, tas = mean hourly T, hurs = mean hourly RH, 4-point mean onto the NEX grid), so
reference and NEX share the deployed method: Stull on daily-mean tas and hurs.

Units: NEX 0.25-degree cells on a 1.5-degree lattice (every 6th cell in lat and lon), kept only
where all 4 surrounding IMD 0.25-degree rain points are valid (= Indian land). 5 models;
historical 1990-2010 and ssp245 2040-2060. Twb is QDM-corrected directly (additive, 31-day window),
as in the site test (qdm_twb.py), and the five twb_* metrics are computed from it.
Cells at latitude >= 30 are reported separately as Himalaya (4-point ERA5 cells mix elevations).

Stage 1 caches NEX/ERA5 Twb series in .scratch/qdm_applicability/series_twb_national/ (skipped
if present); stage 2 scores and writes .scratch/qdm_applicability/out_twb_national/.
"""
from __future__ import annotations

import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import imdlib as imd

sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts")
sys.path.insert(0, ".")
import qdm_applicability as Q  # noqa: E402
from qdm_pr_imd import classify  # noqa: E402

ROOT = Path("D:/projects/irt_data/r1i1p1f1")
REF = Path("D:/projects/irt_data/era5_daily_ref_nexgrid")
IMD = "D:/projects/irt_data/imd"
SER = Path(".scratch/qdm_applicability/series_twb_national"); SER.mkdir(parents=True, exist_ok=True)
OUT = Path(".scratch/qdm_applicability/out_twb_national"); OUT.mkdir(parents=True, exist_ok=True)
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
PERIODS = {"historical": range(1990, 2011), "ssp245": range(2040, 2061)}
METRICS = ["twb_annual_mean", "twb_summer_mean", "twb_annual_max", "twb_days_ge_28", "twb_days_ge_30"]
STEP, OFF = 6, 2


def region(la: float, lo: float) -> str:
    """Coarse region label for reporting (Himalaya first)."""
    if la >= 30:
        return "Himalaya (lat>=30)"
    if lo >= 84:
        return "East + Northeast (lon>=84)"
    if la >= 24:
        return "Northwest + Gangetic plain (lat 24-30)"
    if la >= 18:
        return "Central (lat 18-24)"
    return "South (lat<18)"


def lattice() -> list[tuple[float, float]]:
    """NEX cell centres on the lattice whose 4 surrounding IMD rain points are all valid land."""
    rn = imd.open_data("rain", 2000, 2000, "yearwise", IMD).get_xarray()["rain"]
    land = (rn > -998).any("time")
    lats = np.arange(128) * 0.25 + 6.125
    lons = np.arange(120) * 0.25 + 68.125
    pts = []
    for i in range(OFF, 128, STEP):
        for j in range(OFF, 120, STEP):
            la, lo = lats[i], lons[j]
            ok = True
            for dl in (-0.125, 0.125):
                for dn in (-0.125, 0.125):
                    try:
                        ok &= bool(land.sel(lat=la + dl, lon=lo + dn, method="nearest", tolerance=1e-3))
                    except KeyError:
                        ok = False
            if ok:
                pts.append((float(la), float(lo)))
    return pts


def key(p):
    return f"{p[0]:.3f}_{p[1]:.3f}"


def extract(job):
    """NEX daily Twb (Stull on tas, hurs) at the lattice cells for one model and period."""
    model, exp, pts = job
    path = SER / f"{model}_{exp}.parquet"
    if path.exists():
        return f"{model} {exp}: cached"
    t0 = time.time()
    la = xr.DataArray([p[0] for p in pts], dims="c"); lo = xr.DataArray([p[1] for p in pts], dims="c")
    blocks = []
    for y in PERIODS[exp]:
        v = {}
        for var in ("tas", "hurs"):
            with xr.open_dataset(ROOT / exp / var / model / f"{y}.nc", use_cftime=True) as ds:
                s = ds[var].sel(lat=la, lon=lo, method="nearest")
                assert np.allclose(s.lat.values, la.values) and np.allclose(s.lon.values, lo.values)
                v[var] = np.asarray(s.values, float)
                mo = [t.month for t in ds.time.values]; dy = [t.day for t in ds.time.values]
        tw = Q.stull(v["tas"] - 273.15, v["hurs"])
        df = pd.DataFrame(tw, columns=[key(p) for p in pts]); df.insert(0, "day", dy); df.insert(0, "month", mo); df.insert(0, "year", y)
        blocks.append(df)
    pd.concat(blocks, ignore_index=True).to_parquet(path)
    return f"{model} {exp}: {len(pts)} cells, {time.time() - t0:.0f}s"


def extract_ref(pts):
    path = SER / "era5_ref.parquet"
    if path.exists():
        return
    ds = xr.open_mfdataset([REF / f"{y}.nc" for y in PERIODS["historical"]], engine="h5netcdf", combine="by_coords")
    la = xr.DataArray([p[0] for p in pts], dims="c"); lo = xr.DataArray([p[1] for p in pts], dims="c")
    s = ds.sel(lat=la, lon=lo).load()
    t = pd.DatetimeIndex(s.time.values)
    df = pd.DataFrame(Q.stull(s.tas.values, s.hurs.values), columns=[key(p) for p in pts])
    df.insert(0, "day", t.day); df.insert(0, "month", t.month); df.insert(0, "year", t.year)
    df.to_parquet(path)


def frame(D, k):
    return Q._finish(pd.DataFrame({"year": D.year.values, "month": D.month.values, "day": D.day.values, "twb": D[k].values}))


def score(model):
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=31)
    ref_all = pd.read_parquet(SER / "era5_ref.parquet")
    H, F = pd.read_parquet(SER / f"{model}_historical.parquet"), pd.read_parquet(SER / f"{model}_ssp245.parquet")
    base = Q.make_metrics(); metrics = {m: base[m] for m in METRICS}
    corr = Q.corrector([], ["twb"])
    rows = []
    for k in [c for c in ref_all.columns if "_" in c]:
        ref, hist, fut = frame(ref_all, k), frame(H, k), frame(F, k)
        if ref.twb.isna().any() or hist.twb.isna().any() or fut.twb.isna().any():
            continue
        la, lo = map(float, k.split("_"))
        for r in Q.score_unit(metrics, ref, hist, fut, corr):
            rows.append(dict(unit=k, lat=la, lon=lo, region=region(la, lo), model=model, **r))
    return rows


def main():
    t0 = time.time()
    pts = lattice()
    pd.DataFrame(pts, columns=["lat", "lon"]).to_csv(SER / "cells.csv", index=False)
    print("lattice cells:", len(pts), flush=True)
    extract_ref(pts)
    with ProcessPoolExecutor(6) as ex:
        for r in ex.map(extract, [(m, e, pts) for m in MODELS for e in PERIODS]):
            print(r, flush=True)
    print(f"extraction done {time.time() - t0:.0f}s", flush=True)
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=31)
    rows = []
    with ProcessPoolExecutor(5) as ex:
        for m, r in zip(MODELS, ex.map(score, MODELS)):
            rows += r
            print("scored", m, len(r), flush=True)
    d = pd.DataFrame(rows)
    d.to_csv(OUT / "twb_national_unit_rows.csv", index=False)

    pd.set_option("display.width", 250, "display.max_rows", 300)
    cols = ["ref", "err_raw", "err_qdm", "raw_2050", "qdm_2050", "chg_raw", "chg_qdm", "n_units", "verdict"]
    for lab, sub in [("ALL CELLS", d), ("EXCLUDING HIMALAYA", d[d.lat < 30])]:
        s = sub.groupby("metric").apply(classify)
        sb = sub.assign(b_raw=sub.raw_test - sub.ref_test, b_qdm=sub.qdm_test - sub.ref_test).groupby("metric")[["b_raw", "b_qdm"]].mean()
        s = s.join(sb)
        s.round(3).to_csv(OUT / f"summary_{'all' if lab == 'ALL CELLS' else 'no_himalaya'}.csv")
        print(f"\n{lab}\n" + s[cols + ["b_raw", "b_qdm"]].round(2).reindex(METRICS).to_string(), flush=True)
    r = d.groupby(["metric", "region"]).apply(classify)
    r.round(3).to_csv(OUT / "summary_by_region.csv")
    print("\nBY REGION\n" + r[cols].round(2).to_string(), flush=True)
    print(f"\ntotal {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
