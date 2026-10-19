"""Read-only: NEX daily tasmax, tasmin and pr averaged onto IMD 1-degree temperature boxes.

Each IMD box (centres at x.5) holds exactly 4 x 4 NEX 0.25-degree cells, so the box value is
the plain mean of those 16 cells (skipna=False: a box with any missing cell is NaN).
Boxes: a 2-degree lattice of IMD boxes that are valid on every day of 1990-2010 in both
tmax and tmin. 5 models; historical 1990-2010 and ssp245 2040-2060. One parquet per
model-period-variable, columns = box keys, values in degC (temps) or mm/day (pr).
"""
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd, xarray as xr
import imdlib as imd

ROOT = Path("D:/projects/irt_data/r1i1p1f1")
IMD = "D:/projects/irt_data/imd"
OUT = Path(".scratch/qdm_applicability/series_temp_national"); OUT.mkdir(parents=True, exist_ok=True)
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
PERIODS = {"historical": range(1990, 2011), "ssp245": range(2040, 2061)}
VARS = ["tasmax", "tasmin", "pr"]


def boxes():
    ok = None
    for v in ("tmax", "tmin"):
        x = imd.open_data(v, 1990, 2010, "yearwise", IMD).get_xarray()[v]
        good = (x < 99).all("time")
        ok = good if ok is None else ok & good
    pts = []
    for la in ok.lat.values:
        for lo in ok.lon.values:
            # 2-degree lattice; NEX India subset spans lat 6-38, lon 68-98
            if int(la - 0.5) % 2 == 0 and int(lo - 0.5) % 2 == 0 and 6.0 < la < 38.0 and 68.0 < lo < 98.0 and bool(ok.sel(lat=la, lon=lo)):
                pts.append((float(la), float(lo)))
    return pts


def one(job):
    model, exp, var, pts = job; t0 = time.time()
    blocks = []
    for y in PERIODS[exp]:
        with xr.open_dataset(ROOT / exp / var / model / f"{y}.nc", use_cftime=True) as ds:
            da = ds[var]
            assert abs(float(da.lat[0]) - 6.125) < 1e-6 and abs(float(da.lon[0]) - 68.125) < 1e-6, "NEX grid origin moved"
            box = da.coarsen(lat=4, lon=4, boundary="trim").mean(skipna=False)
            sel = box.sel(lat=xr.DataArray([p[0] for p in pts], dims="b"), lon=xr.DataArray([p[1] for p in pts], dims="b"), method="nearest")
            assert np.allclose(sel.lat.values, [p[0] for p in pts]) and np.allclose(sel.lon.values, [p[1] for p in pts])
            v = np.asarray(sel.values, float)
            v = v - 273.15 if var != "pr" else v * 86400.0
            mo = [t.month for t in ds.time.values]; dy = [t.day for t in ds.time.values]
        df = pd.DataFrame(v, columns=[f"{a:.1f}_{b:.1f}" for a, b in pts]); df.insert(0, "day", dy); df.insert(0, "month", mo); df.insert(0, "year", y)
        blocks.append(df)
    pd.concat(blocks, ignore_index=True).to_parquet(OUT / f"{model}_{exp}_{var}.parquet")
    return f"{model} {exp} {var}: {len(pts)} boxes, {time.time()-t0:.0f}s"


if __name__ == "__main__":
    pts = boxes()
    pd.DataFrame(pts, columns=["lat", "lon"]).to_csv(OUT / "boxes.csv", index=False)
    print("boxes:", len(pts), flush=True)
    jobs = [(m, e, v, pts) for m in MODELS for e in PERIODS for v in VARS]
    with ProcessPoolExecutor(6) as ex:
        for r in ex.map(one, jobs):
            print(r, flush=True)
    print("ALL DONE", flush=True)
