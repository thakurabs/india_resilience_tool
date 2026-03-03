#!/usr/bin/env python3
"""
derive_hurs_from_era5_tas_tdps.py

Derive relative humidity at 2m (hurs, %) from ERA5 daily-statistics NetCDFs:
- tas  : 2m_temperature (daily_mean) stored as monthly files in <NC_DIR>\\tas\\
- tdps : 2m_dewpoint_temperature (daily_mean) stored as monthly files in <NC_DIR>\\tdps\\

Outputs:
- hurs monthly files saved to <NC_DIR>\\hurs\\
  with naming: era5_daily_hurs_daily_mean_YYYYMM_tel.nc

Important:
- This script assumes both tas and tdps are on the same grid and have the same time coordinate.
- Unit handling:
    * If inputs are in Kelvin, they are converted to Celsius for the RH formula.
    * If already in Celsius, used as-is.
- Missing values are preserved (NaNs propagate).
- RH is clipped to [0, 100] (%).

Dependencies: numpy, xarray

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import xarray as xr


TAS_PATTERN = re.compile(r"^era5_daily_tas_daily_mean_(\d{6})_tel\.nc$")
TDPS_PATTERN = re.compile(r"^era5_daily_tdps_daily_mean_(\d{6})_tel\.nc$")


def _infer_celsius(da: xr.DataArray) -> xr.DataArray:
    """
    Return DataArray in Celsius.

    Accepts Kelvin (K) or Celsius (C, degC). If units are missing, assumes Kelvin if values
    look like Kelvin (median > 100), else assumes Celsius.
    """
    units = (da.attrs.get("units") or "").strip().lower()

    if units in {"k", "kelvin"}:
        out = da - 273.15
        out.attrs = dict(da.attrs)
        out.attrs["units"] = "degC"
        return out

    if units in {"c", "degc", "degree_celsius", "degrees_celsius", "celsius"}:
        out = da.copy()
        out.attrs = dict(da.attrs)
        out.attrs["units"] = "degC"
        return out

    # Units missing/unknown: infer from magnitude
    try:
        sample = da
        # Avoid loading entire array: take small sample if possible
        if "time" in da.dims:
            sample = da.isel(time=slice(0, min(10, da.sizes["time"])))
        med = float(np.nanmedian(sample.values))
    except Exception:
        med = 300.0  # default conservative assumption: Kelvin-like

    if med > 100.0:
        out = da - 273.15
        out.attrs = dict(da.attrs)
        out.attrs["units"] = "degC"
        out.attrs["units_inferred"] = "kelvin_assumed"
        return out

    out = da.copy()
    out.attrs = dict(da.attrs)
    out.attrs["units"] = "degC"
    out.attrs["units_inferred"] = "celsius_assumed"
    return out


def _saturation_vapor_pressure_hpa(temp_c: xr.DataArray) -> xr.DataArray:
    """
    Saturation vapor pressure (hPa) using Magnus-Tetens approximation.

    e_s(T) = 6.112 * exp((17.67 * T) / (T + 243.5))  for T in degC

    Returns in hPa.
    """
    es = 6.112 * np.exp((17.67 * temp_c) / (temp_c + 243.5))
    es.attrs = {}
    es.attrs["units"] = "hPa"
    return es


def _compute_rh_percent(tas_c: xr.DataArray, td_c: xr.DataArray) -> xr.DataArray:
    """
    Compute RH (%) from air temperature and dewpoint temperature (both in degC).

    RH = 100 * e(Td) / e_s(T)

    Returns RH clipped to [0, 100].
    """
    e_td = _saturation_vapor_pressure_hpa(td_c)
    e_s = _saturation_vapor_pressure_hpa(tas_c)
    rh = 100.0 * (e_td / e_s)

    # Clip while preserving NaNs
    rh = rh.clip(min=0.0, max=100.0)
    rh.attrs = {}
    rh.attrs["units"] = "%"
    return rh


def _pick_main_data_var(ds: xr.Dataset) -> xr.DataArray:
    """
    Heuristic to select the main variable if dataset contains a single data var.
    If multiple, prefer standard ERA5 names when present.
    """
    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars.values()))

    preferred = ["t2m", "2m_temperature", "d2m", "2m_dewpoint_temperature", "tas", "tdps"]
    for name in preferred:
        if name in ds.data_vars:
            return ds[name]

    # Fallback: pick first
    return next(iter(ds.data_vars.values()))


def derive_hurs_monthly(
    nc_dir: Path,
    overwrite: bool = False,
) -> None:
    """
    For each YYYYMM present in both tas and tdps folders, derive hurs and write output.

    Expects:
      <nc_dir>/tas/era5_daily_tas_daily_mean_YYYYMM_tel.nc
      <nc_dir>/tdps/era5_daily_tdps_daily_mean_YYYYMM_tel.nc

    Writes:
      <nc_dir>/hurs/era5_daily_hurs_daily_mean_YYYYMM_tel.nc
    """
    tas_dir = nc_dir / "tas"
    tdps_dir = nc_dir / "tdps"
    hurs_dir = nc_dir / "hurs"

    if not tas_dir.exists():
        raise FileNotFoundError(f"Missing folder: {tas_dir}")
    if not tdps_dir.exists():
        raise FileNotFoundError(f"Missing folder: {tdps_dir}")

    hurs_dir.mkdir(parents=True, exist_ok=True)

    tas_files: Dict[str, Path] = {}
    for p in tas_dir.glob("*.nc"):
        m = TAS_PATTERN.match(p.name)
        if m:
            tas_files[m.group(1)] = p

    tdps_files: Dict[str, Path] = {}
    for p in tdps_dir.glob("*.nc"):
        m = TDPS_PATTERN.match(p.name)
        if m:
            tdps_files[m.group(1)] = p

    yyyymm_all = sorted(set(tas_files.keys()) & set(tdps_files.keys()))
    if not yyyymm_all:
        raise RuntimeError("No matching YYYYMM pairs found between tas and tdps folders.")

    processed = 0
    skipped = 0

    for yyyymm in yyyymm_all:
        out_path = hurs_dir / f"era5_daily_hurs_daily_mean_{yyyymm}_tel.nc"
        if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
            print(f"[SKIP] Exists: {out_path}")
            skipped += 1
            continue

        tas_path = tas_files[yyyymm]
        tdps_path = tdps_files[yyyymm]
        print(f"[READ] {yyyymm} | tas={tas_path.name} | tdps={tdps_path.name}")

        ds_tas = xr.open_dataset(tas_path)
        ds_tdps = xr.open_dataset(tdps_path)

        da_tas = _pick_main_data_var(ds_tas)
        da_tdps = _pick_main_data_var(ds_tdps)

        # Basic alignment on shared coords (time/lat/lon)
        da_tas, da_tdps = xr.align(da_tas, da_tdps, join="exact")

        tas_c = _infer_celsius(da_tas)
        td_c = _infer_celsius(da_tdps)

        rh = _compute_rh_percent(tas_c, td_c)
        rh.name = "hurs"

        # Carry some helpful metadata
        rh.attrs["long_name"] = "Near-surface relative humidity"
        rh.attrs["standard_name"] = "relative_humidity"
        rh.attrs["derived_from"] = "ERA5 2m_temperature and 2m_dewpoint_temperature (daily_mean)"
        rh.attrs["method"] = "Magnus-Tetens saturation vapor pressure; RH=100*e(Td)/e_s(T)"

        out_ds = rh.to_dataset()

        # Preserve coordinate attrs if present
        for coord in out_ds.coords:
            try:
                out_ds[coord].attrs = dict(ds_tas[coord].attrs)
            except Exception:
                pass

        # Write NetCDF
        # Use conservative encoding to avoid surprises; let xarray decide chunks.
        encoding = {"hurs": {"zlib": True, "complevel": 4}}
        tmp_path = out_path.with_suffix(".nc.tmp")

        print(f"[WRITE] {out_path.name}")
        out_ds.to_netcdf(tmp_path, encoding=encoding)
        tmp_path.replace(out_path)

        ds_tas.close()
        ds_tdps.close()
        processed += 1

    print("\n=== Summary ===")
    print(f"Processed: {processed}")
    print(f"Skipped  : {skipped}")
    print(f"Output dir: {hurs_dir}")


def main() -> None:
    nc_dir = Path(r"D:\projects\irt_data\era5\nc")
    derive_hurs_monthly(nc_dir=nc_dir, overwrite=False)


if __name__ == "__main__":
    main()
