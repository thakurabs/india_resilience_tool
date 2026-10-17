"""Build the daily ERA5 humidity reference on the NEX 0.25-degree grid from the hourly download
(era5_hourly_get.py), into irt_data/era5_daily_ref_nexgrid/YYYY.nc (CHG-0709).

Usage: era5_daily_ref_build.py YYYY [YYYY]     (inclusive year range, 1990-2010)

Matches how NEX defines a day, so QDM compares like with like:
- days are IST calendar days (UTC+5:30): IST day D = the 24 hourly values 19:00 UTC on D-1 .. 18:00 UTC on D;
- tas = mean of hourly 2 m temperature (degC);
- hurs = mean of hourly RH (%), RH = 100 e_s(Td) / e_s(T), Magnus over water (6.112, 17.62, 243.12),
  the formula used for the six-site Open-Meteo reference. Taking RH from daily-mean T and Td instead
  is 0.4-1.4 RH points too dry (CHG-0708 notes).
Regridding: ERA5 points sit on whole and quarter degrees, NEX cell centres on x.125, so each NEX
cell is the plain mean of the 4 surrounding ERA5 points (128 x 120 cells, 6.125-37.875 N,
68.125-97.875 E, latitude ascending like NEX). Daily means are taken before regridding; both are linear.
New files only: an existing YYYY.nc is never replaced. Each year is written to a .part file and
renamed only after it reopens with the full calendar, grid and no NaN.
"""
import os
import sys
import time

import numpy as np
import pandas as pd
import xarray as xr

SRC = "D:/projects/irt_data/era5_hourly_india"
DST = "D:/projects/irt_data/era5_daily_ref_nexgrid"
IST = pd.Timedelta(hours=5, minutes=30)


def es(x):
    """Saturation vapour pressure over water (hPa), Magnus; x in degC."""
    return 6.112 * np.exp(17.62 * x / (243.12 + x))


def hours(y, m):
    """Hourly t2m and d2m (degC, latitude ascending) for every hour of IST month y-m."""
    py, pm = (y - 1, 12) if m == 1 else (y, m - 1)
    t0 = pd.Timestamp(y, m, 1) - IST + pd.Timedelta(minutes=30)          # 19:00 UTC on the eve
    t1 = (pd.Timestamp(y, m, 1) + pd.offsets.MonthBegin(1)) - IST - pd.Timedelta(minutes=30)
    parts = []
    for yy, mm in ((py, pm), (y, m)):
        with xr.open_dataset(f"{SRC}/{yy}{mm:02d}.nc") as ds:
            s = ds[["t2m", "d2m"]].sel(valid_time=slice(t0, t1)).isel(latitude=slice(None, None, -1)).load()
            parts.append(s)
    s = xr.concat(parts, "valid_time")
    assert s.latitude.values[0] == 6.0 and s.longitude.values[0] == 68.0, "unexpected ERA5 grid"
    vt = pd.DatetimeIndex(s.valid_time.values)
    assert vt.is_monotonic_increasing and vt.is_unique and vt[0] == t0 and vt[-1] == t1, (vt[0], vt[-1])
    return s.t2m.values - 273.15, s.d2m.values - 273.15


def four_point(a):
    """Mean of the 4 ERA5 points around each NEX cell centre: (..., 129, 121) -> (..., 128, 120)."""
    return 0.25 * (a[..., :-1, :-1] + a[..., 1:, :-1] + a[..., :-1, 1:] + a[..., 1:, 1:])


def build_year(y):
    path = f"{DST}/{y}.nc"
    if os.path.exists(path):
        print(y, "exists - left untouched", flush=True)
        return
    t = time.time()
    tas, hurs, days = [], [], []
    for m in range(1, 13):
        T, Td = hours(y, m)
        nd = T.shape[0] // 24
        assert T.shape[0] == 24 * nd
        rh = 100.0 * es(Td) / es(T)
        tas.append(four_point(T.reshape(nd, 24, *T.shape[1:]).mean(axis=1)))
        hurs.append(four_point(rh.reshape(nd, 24, *rh.shape[1:]).mean(axis=1)))
        days.append(pd.date_range(pd.Timestamp(y, m, 1), periods=nd, freq="D"))
    time_ix = days[0].append(days[1:])
    lat = np.arange(128) * 0.25 + 6.125
    lon = np.arange(120) * 0.25 + 68.125
    ds = xr.Dataset(
        {"tas": (("time", "lat", "lon"), np.concatenate(tas).astype("float32"),
                 {"units": "degC", "long_name": "IST daily mean of hourly ERA5 2 m temperature"}),
         "hurs": (("time", "lat", "lon"), np.concatenate(hurs).astype("float32"),
                  {"units": "%", "long_name": "IST daily mean of hourly ERA5 2 m relative humidity (Magnus, over water)"})},
        coords={"time": time_ix, "lat": lat, "lon": lon},
        attrs={"source": "ERA5 reanalysis-era5-single-levels hourly t2m, d2m (CHG-0708)",
               "method": "IST days (UTC+5:30); daily means of hourly values; NEX 0.25-deg grid by 4-point mean (CHG-0709)"})
    os.makedirs(DST, exist_ok=True)
    part = path + ".part"
    enc = {v: {"zlib": True, "complevel": 1} for v in ("tas", "hurs")}
    ds.to_netcdf(part, engine="h5netcdf", encoding=enc)
    with xr.open_dataset(part, engine="h5netcdf") as chk:
        nd = 366 if pd.Timestamp(y, 12, 31).dayofyear == 366 else 365
        assert chk.sizes == {"time": nd, "lat": 128, "lon": 120}, dict(chk.sizes)
        assert int(chk.tas.isnull().sum()) == 0 and int(chk.hurs.isnull().sum()) == 0
        assert float(chk.hurs.max()) <= 100.0 + 1e-2, float(chk.hurs.max())   # float32 rounding where Td = T
    os.rename(part, path)
    print(y, "written", os.path.getsize(path), "bytes", f"{time.time() - t:.0f}s", flush=True)


def main():
    a = int(sys.argv[1]); b = int(sys.argv[2]) if len(sys.argv) > 2 else a
    assert 1990 <= a <= b <= 2010, "hourly data covers IST years 1990-2010"
    for y in range(a, b + 1):
        build_year(y)


if __name__ == "__main__":
    main()
