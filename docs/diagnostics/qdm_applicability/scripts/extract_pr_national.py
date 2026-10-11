"""Read-only: NEX daily pr at a national 2-degree lattice of NEX cells whose 4 overlapping IMD boxes are land.
5 models; historical 1990-2010 and ssp245 2040-2060. Output: one parquet per model-period."""
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd, xarray as xr
import imdlib as imd

ROOT = Path("D:/projects/irt_data/r1i1p1f1")
OUT = Path(".scratch/qdm_applicability/series_pr_national"); OUT.mkdir(parents=True, exist_ok=True)
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
PERIODS = {"historical": range(1990, 2011), "ssp245": range(2040, 2061)}

def lattice():
    r = imd.open_data("rain", 1990, 1990, "yearwise", "D:/projects/irt_data/imd").get_xarray()["rain"]
    land = (r > -998).all("time")
    pts = []
    for la in np.arange(8.125, 37.2, 2.0):
        for lo in np.arange(68.125, 97.2, 2.0):
            ok = all(bool(land.sel(lat=la + a, lon=lo + b, method="nearest")) and
                     abs(float(land.lat.sel(lat=la + a, method="nearest")) - (la + a)) < 1e-6 and
                     abs(float(land.lon.sel(lon=lo + b, method="nearest")) - (lo + b)) < 1e-6
                     for a in (-0.125, 0.125) for b in (-0.125, 0.125))
            if ok:
                pts.append((round(la, 3), round(lo, 3)))
    return pts

def one(job):
    model, exp, pts = job; t0 = time.time()
    lats = xr.DataArray([p[0] for p in pts], dims="cell"); lons = xr.DataArray([p[1] for p in pts], dims="cell")
    blocks = []
    for y in PERIODS[exp]:
        with xr.open_dataset(ROOT / exp / "pr" / model / f"{y}.nc", use_cftime=True) as ds:
            p = ds["pr"].sel(lat=lats, lon=lons, method="nearest")
            assert np.allclose(p.lat.values, lats.values, atol=0.01) and np.allclose(p.lon.values, lons.values, atol=0.01)
            v = np.asarray(p.values, float) * 86400.0
            mo = [t.month for t in ds.time.values]; dy = [t.day for t in ds.time.values]
        df = pd.DataFrame(v, columns=[f"{a:.3f}_{b:.3f}" for a, b in pts]); df.insert(0, "day", dy); df.insert(0, "month", mo); df.insert(0, "year", y)
        blocks.append(df)
    pd.concat(blocks, ignore_index=True).to_parquet(OUT / f"{model}_{exp}.parquet")
    return f"{model} {exp}: {len(pts)} cells, {time.time()-t0:.0f}s"

if __name__ == "__main__":
    pts = lattice()
    pd.DataFrame(pts, columns=["lat", "lon"]).to_csv(OUT / "cells.csv", index=False)
    print("cells:", len(pts), flush=True)
    jobs = [(m, e, pts) for m in MODELS for e in PERIODS]
    with ProcessPoolExecutor(5) as ex:
        for r in ex.map(one, jobs):
            print(r, flush=True)
    print("ALL DONE", flush=True)
