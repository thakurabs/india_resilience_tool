"""Read-only: nearest-cell daily tas+hurs at the six ERA5 sites, ssp245 2040-2060, one model per process."""
import sys, time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np, pandas as pd, xarray as xr

ROOT = Path("D:/projects/irt_data/r1i1p1f1/ssp245")
OUT = Path(".scratch/twb_check/ssp245_series")
SITES = {"kochi": (9.93, 76.27), "kolkata": (22.57, 88.36), "bikaner": (28.02, 73.31),
         "lucknow": (26.85, 80.95), "hyderabad": (17.39, 78.49), "shimla": (31.10, 77.17)}
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
YEARS = range(2040, 2061)

def one(model):
    t0 = time.time(); names = list(SITES)
    lats = xr.DataArray([SITES[n][0] for n in names], dims="site")
    lons = xr.DataArray([SITES[n][1] for n in names], dims="site")
    per = {}
    for var in ("tas", "hurs"):
        blocks = []
        for y in YEARS:
            with xr.open_dataset(ROOT / var / model / f"{y}.nc", use_cftime=True) as ds:
                p = ds[var].sel(lat=lats, lon=lons, method="nearest")
                assert np.all(np.abs(p.lat.values - lats.values) < 0.2) and np.all(np.abs(p.lon.values - lons.values) < 0.2)
                v = np.asarray(p.values, float)
                mo = [t.month for t in ds.time.values]; dy = [t.day for t in ds.time.values]
            blocks.append(pd.DataFrame({"year": y, "month": mo, "day": dy, **{n: v[:, i] for i, n in enumerate(names)}}))
        per[var] = pd.concat(blocks).melt(id_vars=["year", "month", "day"], var_name="site", value_name=var)
    df = per["tas"].merge(per["hurs"], on=["year", "month", "day", "site"])
    df["model"] = model; df["experiment"] = "ssp245"
    df.to_parquet(OUT / f"{model}.parquet")
    return f"{model}: {len(df)} rows, {time.time()-t0:.0f}s"

if __name__ == "__main__":
    with ProcessPoolExecutor(5) as ex:
        for r in ex.map(one, MODELS):
            print(r, flush=True)
    print("ALL DONE", flush=True)
