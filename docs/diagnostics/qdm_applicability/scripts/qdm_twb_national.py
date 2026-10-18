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

Test B (CHG-0712): --step 1 scores every land cell; --step N uses an N-cell lattice. Steps other
than 6 cache and write to series_twb_national_s{N}/ and out_twb_national_s{N}/, so test A's files
are never touched. --limit K keeps K cells spread evenly over the lattice and --models M the first
M models (timing probe; writes to *_probe dirs). Cells are also labelled with the State their
centre falls in (states_4326.geojson; nearest State within 0.2 degrees for coastal cells) and a
per-State table is written.

Usage: qdm_twb_national.py [--step N] [--limit K] [--models M] [--workers W]
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import geopandas as gpd
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
STATES = "D:/projects/irt_data/states_4326.geojson"
SER = Path(".scratch/qdm_applicability/series_twb_national")
OUT = Path(".scratch/qdm_applicability/out_twb_national")
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


def lattice(step: int = STEP) -> list[tuple[float, float]]:
    """NEX cell centres on the lattice whose 4 surrounding IMD rain points are all valid land.

    The lattice starts at index offset OFF modulo step (test A: offset 2, step 6); step 1 is every cell.
    """
    rn = imd.open_data("rain", 2000, 2000, "yearwise", IMD).get_xarray()["rain"]
    land = (rn > -998).any("time")
    lats = np.arange(128) * 0.25 + 6.125
    lons = np.arange(120) * 0.25 + 68.125
    corner = np.arange(6.0, 38.01, 0.25), np.arange(68.0, 98.01, 0.25)   # ERA5-style quarter-degree points
    lm = land.astype(float).reindex(lat=corner[0], lon=corner[1], method="nearest", tolerance=1e-3).fillna(0).values > 0
    ok = lm[:-1, :-1] & lm[1:, :-1] & lm[:-1, 1:] & lm[1:, 1:]           # (128, 120): all 4 corners land
    off = OFF % step
    return [(float(lats[i]), float(lons[j])) for i in range(off, 128, step) for j in range(off, 120, step) if ok[i, j]]


def state_of(pts: list[tuple[float, float]]) -> list[str]:
    """State whose polygon holds each cell centre; nearest State within 0.2 degrees otherwise."""
    st = gpd.read_file(STATES)[["state_name", "geometry"]]
    g = gpd.GeoDataFrame(geometry=gpd.points_from_xy([p[1] for p in pts], [p[0] for p in pts]), crs=st.crs)
    j = gpd.sjoin(g, st, how="left", predicate="within")
    j = j[~j.index.duplicated()]
    miss = j.state_name.isna()
    if miss.any():
        with __import__("warnings").catch_warnings():
            __import__("warnings").simplefilter("ignore")
            n = gpd.sjoin_nearest(g[miss], st, how="left", max_distance=0.2)
        n = n[~n.index.duplicated()]
        j.loc[miss, "state_name"] = n.state_name
    return j.state_name.fillna("(none)").tolist()


def key(p):
    return f"{p[0]:.3f}_{p[1]:.3f}"


def extract(job):
    """NEX daily Twb (Stull on tas, hurs) at the lattice cells for one model and period."""
    model, exp, pts, ser = job
    path = ser / f"{model}_{exp}.parquet"
    if path.exists():
        return f"{model} {exp}: cached"
    t0 = time.time()
    la = xr.DataArray([p[0] for p in pts], dims="c"); lo = xr.DataArray([p[1] for p in pts], dims="c")
    blocks = []
    for y in PERIODS[exp]:
        v = {}
        for var in ("tas", "hurs"):
            with xr.open_dataset(ROOT / exp / var / model / f"{y}.nc", use_cftime=True) as ds:
                ii = np.abs(ds.lat.values[None, :] - la.values[:, None]).argmin(1)
                jj = np.abs(ds.lon.values[None, :] - lo.values[:, None]).argmin(1)
                assert np.allclose(ds.lat.values[ii], la.values) and np.allclose(ds.lon.values[jj], lo.values)
                g = ds[var].transpose("time", "lat", "lon").values      # whole grid once, then index
                v[var] = np.asarray(g[:, ii, jj], float)
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
    ds = ds.load()                                                       # ~0.9 GB; then index all cells at once
    s = ds.sel(lat=la, lon=lo)
    t = pd.DatetimeIndex(s.time.values)
    df = pd.DataFrame(Q.stull(s.tas.values, s.hurs.values), columns=[key(p) for p in pts])
    df.insert(0, "day", t.day); df.insert(0, "month", t.month); df.insert(0, "year", t.year)
    df.to_parquet(path)


def frame(D, k):
    return Q._finish(pd.DataFrame({"year": D.year.values, "month": D.month.values, "day": D.day.values, "twb": D[k].values}))


def score(job):
    """Score one model on a chunk of cell columns (reads only those parquet columns)."""
    model, keys, ser = job
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=31)
    cols = ["year", "month", "day"] + list(keys)
    ref_all = pd.read_parquet(ser / "era5_ref.parquet", columns=cols)
    H = pd.read_parquet(ser / f"{model}_historical.parquet", columns=cols)
    F = pd.read_parquet(ser / f"{model}_ssp245.parquet", columns=cols)
    base = Q.make_metrics(); metrics = {m: base[m] for m in METRICS}
    corr = Q.corrector([], ["twb"])
    rows = []
    for k in keys:
        ref, hist, fut = frame(ref_all, k), frame(H, k), frame(F, k)
        if ref.twb.isna().any() or hist.twb.isna().any() or fut.twb.isna().any():
            continue
        la, lo = map(float, k.split("_"))
        for r in Q.score_unit(metrics, ref, hist, fut, corr):
            rows.append(dict(unit=k, lat=la, lon=lo, region=region(la, lo), model=model, **r))
    return model, rows


def main():
    global SER, OUT, MODELS
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", type=int, default=STEP)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--models", type=int, default=len(MODELS))
    ap.add_argument("--workers", type=int, default=5)
    a = ap.parse_args()
    MODELS = MODELS[:a.models]
    probe = a.limit > 0 or a.models < 5
    if a.step != STEP or probe:
        sfx = f"_s{a.step}" + ("_probe" if probe else "")
        SER, OUT = SER.with_name(SER.name + sfx), OUT.with_name(OUT.name + sfx)
    SER.mkdir(parents=True, exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)
    print("series", SER, "out", OUT, "models", MODELS, flush=True)

    t0 = time.time()
    pts = lattice(a.step)
    if a.limit and a.limit < len(pts):
        pts = [pts[i] for i in np.linspace(0, len(pts) - 1, a.limit).round().astype(int)]
    cells = pd.DataFrame(pts, columns=["lat", "lon"]).assign(state=state_of(pts))
    if (SER / "cells.csv").exists():
        old = pd.read_csv(SER / "cells.csv")
        assert np.allclose(old[["lat", "lon"]].values, cells[["lat", "lon"]].values), "cached series are for other cells"
    cells.to_csv(SER / "cells.csv", index=False)
    st = dict(zip(map(key, pts), cells.state))
    print("lattice cells:", len(pts), f"({time.time() - t0:.0f}s)", flush=True)
    extract_ref(pts)
    with ProcessPoolExecutor(6) as ex:
        for r in ex.map(extract, [(m, e, pts, SER) for m in MODELS for e in PERIODS]):
            print(r, flush=True)
    print(f"extraction done {time.time() - t0:.0f}s", flush=True)
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=31)
    keys = [key(p) for p in pts]
    n = max(1, -(-len(keys) * len(MODELS) // (4 * a.workers)))          # ~4 chunks per worker
    jobs = [(m, keys[i:i + n], SER) for m in MODELS for i in range(0, len(keys), n)]
    rows, t1 = [], time.time()
    with ProcessPoolExecutor(a.workers) as ex:
        for i, (m, r) in enumerate(ex.map(score, jobs), 1):
            rows += r
            print(f"scored {i}/{len(jobs)} ({m}, {len(r)} rows) {time.time() - t1:.0f}s", flush=True)
    print(f"scoring done {time.time() - t1:.0f}s", flush=True)
    d = pd.DataFrame(rows)
    d.insert(4, "state", d.unit.map(st))
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
    s = d[d.lat < 30].groupby(["metric", "state"]).apply(classify)
    s.round(3).to_csv(OUT / "summary_by_state_no_himalaya.csv")
    print("\nBY STATE (cells below 30N)\n" + s[cols].round(2).to_string(), flush=True)
    print(f"\ntotal {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
