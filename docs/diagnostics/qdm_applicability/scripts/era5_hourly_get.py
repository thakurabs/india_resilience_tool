"""Download hourly ERA5 2 m temperature and 2 m dewpoint over India, one CDS request per month,
into irt_data/era5_hourly_india/YYYYMM.nc (CHG-0708).

Usage: era5_hourly_get.py YYYYMM [YYYYMM]     (inclusive month range; one month if one argument)

Area is the NEX India box, 6-38 N, 68-98 E (129 x 121 ERA5 points). 198912 fetches only
31 December, which the first IST day of 1990 needs (it starts at 18:30 UTC on 31 Dec 1989).
New files only: an existing YYYYMM.nc is never replaced. Each download goes to a .part file and
is renamed only after it opens and holds both variables, 24 x days steps and the full grid.
Retries with backoff. Needs cdsapi and the Windows-side ~/.cdsapirc.
"""
import calendar
import os
import sys
import time

import cdsapi
import xarray as xr

DIR = "D:/projects/irt_data/era5_hourly_india"
AREA = [38, 68, 6, 98]                                   # N, W, S, E
NLAT, NLON = 129, 121
VARS = ["2m_temperature", "2m_dewpoint_temperature"]


def months(a, b):
    y, m = divmod(int(a), 100)
    y1, m1 = divmod(int(b), 100)
    while (y, m) <= (y1, m1):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def check(path, ndays):
    """Raise unless the file is a complete month: both variables, every hour, full grid."""
    with xr.open_dataset(path) as ds:
        tname = "valid_time" if "valid_time" in ds.dims else "time"
        assert {"t2m", "d2m"} <= set(ds.data_vars), list(ds.data_vars)
        assert ds.sizes[tname] == 24 * ndays, (ds.sizes[tname], 24 * ndays)
        assert ds.sizes["latitude"] == NLAT and ds.sizes["longitude"] == NLON, dict(ds.sizes)
        return int(ds["t2m"].isnull().sum()), int(ds["d2m"].isnull().sum())


def main():
    os.makedirs(DIR, exist_ok=True)
    client = cdsapi.Client(quiet=True)
    bad = []
    for y, m in months(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else sys.argv[1]):
        path = f"{DIR}/{y}{m:02d}.nc"
        days = [31] if (y, m) == (1989, 12) else list(range(1, calendar.monthrange(y, m)[1] + 1))
        if os.path.exists(path):
            print(y, m, "exists - left untouched", flush=True)
            continue
        req = {"product_type": ["reanalysis"], "variable": VARS, "year": [f"{y}"], "month": [f"{m:02d}"],
               "day": [f"{d:02d}" for d in days], "time": [f"{h:02d}:00" for h in range(24)],
               "data_format": "netcdf", "download_format": "unarchived", "area": AREA}
        part = path + ".part"
        for attempt in range(1, 6):
            try:
                t = time.time()
                client.retrieve("reanalysis-era5-single-levels", req).download(part)
                nan = check(part, len(days))
                os.rename(part, path)
                print(y, m, "downloaded", os.path.getsize(path), "bytes", f"{time.time() - t:.0f}s",
                      "attempt", attempt, "NaN t2m/d2m", nan, flush=True)
                break
            except Exception as e:
                print(y, m, "attempt", attempt, "failed:", type(e).__name__, str(e)[:200], flush=True)
                if os.path.exists(part):
                    os.remove(part)                      # only ever removes this run's own .part
                time.sleep(60 * attempt)
        else:
            bad.append(f"{y}{m:02d}")
    print("FAILED MONTHS:", bad, flush=True)


if __name__ == "__main__":
    main()
