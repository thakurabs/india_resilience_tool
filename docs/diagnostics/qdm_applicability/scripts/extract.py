"""Read-only: NEX daily series at the six ERA5 sites (all vars) + Telangana-grid pr. 5 models, hist 1990-2010, ssp245 2040-2060."""
import sys, time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd, xarray as xr

ROOT = Path("D:/projects/irt_data/r1i1p1f1")
OUT = Path(".scratch/qdm_applicability/series")
SITES = {"kochi": (9.93, 76.27), "kolkata": (22.57, 88.36), "bikaner": (28.02, 73.31),
         "lucknow": (26.85, 80.95), "hyderabad": (17.39, 78.49), "shimla": (31.10, 77.17)}
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
PERIODS = {"historical": range(1990, 2011), "ssp245": range(2040, 2061)}
VARS = ("tas", "tasmax", "tasmin", "hurs", "pr")
TEL = dict(lat=slice(15.8, 19.95), lon=slice(77.2, 81.35))  # Telangana bbox for gridded pr

def one(job):
    model, exp = job; t0 = time.time(); names = list(SITES)
    lats = xr.DataArray([SITES[n][0] for n in names], dims="site")
    lons = xr.DataArray([SITES[n][1] for n in names], dims="site")
    per, telpr = {}, []
    for var in VARS:
        blocks = []
        for y in PERIODS[exp]:
            with xr.open_dataset(ROOT / exp / var / model / f"{y}.nc", use_cftime=True) as ds:
                p = ds[var].sel(lat=lats, lon=lons, method="nearest")
                assert np.all(np.abs(p.lat.values - lats.values) < 0.2) and np.all(np.abs(p.lon.values - lons.values) < 0.2)
                v = np.asarray(p.values, float)
                mo = [t.month for t in ds.time.values]; dy = [t.day for t in ds.time.values]
                if var == "pr":
                    g = ds[var].sel(**TEL).load()
                    telpr.append(g.assign_coords(time=[f"{y}-{m:02d}-{d:02d}" for m, d in zip(mo, dy)]))
            blocks.append(pd.DataFrame({"year": y, "month": mo, "day": dy, **{n: v[:, i] for i, n in enumerate(names)}}))
        per[var] = pd.concat(blocks).melt(id_vars=["year", "month", "day"], var_name="site", value_name=var)
    df = per["tas"]
    for var in VARS[1:]:
        df = df.merge(per[var], on=["year", "month", "day", "site"])
    df["model"] = model; df["experiment"] = exp
    df.to_parquet(OUT / f"{model}_{exp}.parquet")
    xr.concat(telpr, "time").to_netcdf(OUT / f"telpr_{model}_{exp}.nc")
    return f"{model} {exp}: {len(df)} rows, {time.time()-t0:.0f}s"

if __name__ == "__main__":
    jobs = [(m, e) for m in MODELS for e in PERIODS]
    with ProcessPoolExecutor(5) as ex:
        for r in ex.map(one, jobs):
            print(r, flush=True)
    print("ALL DONE", flush=True)
