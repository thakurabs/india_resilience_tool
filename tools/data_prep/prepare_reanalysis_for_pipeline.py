# prepare_reanalysis_for_pipeline.py
#!/usr/bin/env python3
"""Prepare ERA5 + IMD precipitation for compute_indices_multiprocess.py ingestion.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import xarray as xr

from paths import DATA_ROOT

ERA5_DEFAULT_FILE_GLOB = "era5_daily_total_precipitation_daily_sum_*.nc"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_open_mfdataset(paths: list[Path]) -> xr.Dataset:
    """Open many NetCDF files in a robust way (no dask required)."""
    if not paths:
        raise FileNotFoundError("No NetCDF files provided.")

    engines = [None, "netcdf4", "h5netcdf", "scipy"]
    last_err: Optional[Exception] = None
    for eng in engines:
        try:
            if eng is None:
                return xr.open_mfdataset([str(p) for p in paths], combine="by_coords")
            return xr.open_mfdataset([str(p) for p in paths], combine="by_coords", engine=eng)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to open datasets with engines {engines}: {last_err}")


def _rename_time_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        ren["valid_time"] = "time"
    if "latitude" in ds.coords or "latitude" in ds.dims:
        ren["latitude"] = "lat"
    if "longitude" in ds.coords or "longitude" in ds.dims:
        ren["longitude"] = "lon"
    return ds.rename(ren) if ren else ds


def _subset_bbox(ds: xr.Dataset, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> xr.Dataset:
    ds = _rename_time_lat_lon(ds)
    if "lat" not in ds.coords or "lon" not in ds.coords:
        raise KeyError("Dataset must have lat/lon coordinates after renaming.")

    # enforce ascending to keep downstream rasterization stable
    if ds["lat"].values[0] > ds["lat"].values[-1]:
        ds = ds.sortby("lat")
    if ds["lon"].values[0] > ds["lon"].values[-1]:
        ds = ds.sortby("lon")

    return ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))


def _to_mm_day(da: xr.DataArray) -> xr.DataArray:
    """Convert known precip units to mm/day daily totals."""
    units = (da.attrs.get("units", "") or "").strip().lower()

    if units in {"m", "meter", "metre", "meters", "metres", "m of water equivalent"}:
        out = da * 1000.0
        out.attrs["units"] = "mm/day"
        out.attrs["note"] = "Converted from meters to mm/day (daily totals)."
        return out

    if units in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        out = da.copy()
        out.attrs["units"] = "mm/day"
        out.attrs["note"] = "Interpreted as mm/day (daily totals)."
        return out

    if units in {"kg m-2 s-1", "kg m-2 s^-1", "kg/m^2/s"}:
        out = da * 86400.0
        out.attrs["units"] = "mm/day"
        out.attrs["note"] = "Converted from kg m-2 s-1 to mm/day."
        return out

    out = da.copy()
    out.attrs["units"] = da.attrs.get("units", "UNKNOWN")
    out.attrs["note"] = f"Units not converted (units='{da.attrs.get('units','')}'). Expected mm/day daily totals."
    return out


def _standardize_pr_dataset(ds: xr.Dataset, pr_var_candidates: Iterable[str]) -> xr.Dataset:
    ds = _rename_time_lat_lon(ds)
    pr_var = next((v for v in pr_var_candidates if v in ds.data_vars), None)
    if pr_var is None:
        raise KeyError(
            f"Could not find precip variable. Candidates={list(pr_var_candidates)}; vars={list(ds.data_vars)}"
        )

    pr = _to_mm_day(ds[pr_var])

    out = pr.to_dataset(name="pr")
    out["pr"].attrs.setdefault("long_name", "Daily total precipitation")

    if out["lat"].values[0] > out["lat"].values[-1]:
        out = out.sortby("lat")
    if out["lon"].values[0] > out["lon"].values[-1]:
        out = out.sortby("lon")

    if "time" not in out.coords and "time" not in out.dims:
        raise KeyError("Standardized dataset has no 'time' coordinate.")
    return out


def stage_era5_to_data_root(
    era5_nc_dir: Path,
    years: list[int],
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    model_name: str = "ERA5",
    file_glob: str = ERA5_DEFAULT_FILE_GLOB,
) -> Path:
    """Stage ERA5 monthly NetCDFs into yearly files under DATA_ROOT/historical/pr/<model_name>/"""
    if not era5_nc_dir.exists():
        raise FileNotFoundError(f"ERA5 NetCDF directory does not exist: {era5_nc_dir}")

    rx = re.compile(r"(\d{4})(\d{2})")
    year_to_files: dict[int, list[Path]] = defaultdict(list)
    for p in sorted(era5_nc_dir.glob(file_glob)):
        m = rx.search(p.name)
        if not m:
            continue
        y = int(m.group(1))
        if y in years:
            year_to_files[y].append(p)

    out_dir = DATA_ROOT / "historical" / "pr" / model_name
    _ensure_dir(out_dir)

    for y in years:
        files = year_to_files.get(y, [])
        if not files:
            raise FileNotFoundError(f"No ERA5 files found for year={y} in {era5_nc_dir} using glob '{file_glob}'")

        target = out_dir / f"{y}.nc"
        if target.exists() and target.stat().st_size > 0:
            print(f"[SKIP] ERA5 yearly exists: {target}")
            continue

        ds = _safe_open_mfdataset(files)
        ds = _subset_bbox(ds, lat_min, lat_max, lon_min, lon_max)
        ds = _standardize_pr_dataset(ds, pr_var_candidates=["tp", "pr", "precip", "rain", "precipitation"])

        # remove duplicated days if any
        t = ds["time"].values
        if len(np.unique(t)) != len(t):
            ds = ds.sel(time=np.unique(t))

        ds.to_netcdf(target)
        print(f"[WRITE] ERA5 -> {target}")

    return out_dir


def stage_imd_to_data_root(
    imd_source: Path,
    years: list[int],
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    model_name: str = "IMD",
) -> Path:
    """
    Stage IMD daily rainfall into yearly files under DATA_ROOT/historical/pr/<model_name>/.

    Supports:
      - NetCDF inputs (.nc)
      - IMD .grd format (downloaded by imdlib), e.g. <file_dir>/rain/*.grd
    """
    if not imd_source.exists():
        raise FileNotFoundError(f"IMD source path does not exist: {imd_source}")

    out_dir = DATA_ROOT / "historical" / "pr" / model_name
    _ensure_dir(out_dir)

    ds: xr.Dataset

    if imd_source.is_dir():
        # 1) Try NetCDF first
        nc_paths = sorted([p for p in imd_source.rglob("*.nc") if p.is_file()])
        if nc_paths:
            ds = _safe_open_mfdataset(nc_paths)
        else:
            # 2) If no NetCDF, try IMD .grd via imdlib
            grd_paths = sorted([p for p in imd_source.rglob("*.grd") if p.is_file()])
            if not grd_paths:
                raise FileNotFoundError(
                    f"No .nc or .grd files found under IMD directory: {imd_source}"
                )

            try:
                import imdlib as imd
            except ImportError as e:
                raise ImportError(
                    "Found IMD .grd files but imdlib is not installed.\n"
                    "Install with:\n"
                    "  pip install imdlib\n"
                ) from e

            # imdlib expects file_dir as the parent directory that contains the 'rain' folder.
            # If user passed ...\\imd\\rain, set file_dir to ...\\imd
            file_dir = imd_source
            if imd_source.name.lower() == "rain":
                file_dir = imd_source.parent

            start_yr, end_yr = min(years), max(years)
            data = imd.open_data("rain", start_yr, end_yr, "yearwise", str(file_dir))
            ds = data.get_xarray()

            # Normalize coordinate names
            rename_map = {}
            if "lat" in ds.coords:
                rename_map["lat"] = "latitude"
            if "lon" in ds.coords:
                rename_map["lon"] = "longitude"
            if rename_map:
                ds = ds.rename(rename_map)

            # Replace IMD missing marker (-999) with NaN (common in imdlib outputs)
            if "rain" in ds.data_vars:
                ds = ds.where(ds["rain"] != -999.0)

    else:
        # Single file input (NetCDF)
        ds = xr.open_dataset(imd_source)

    # Standardize + subset
    ds = _subset_bbox(ds, lat_min, lat_max, lon_min, lon_max)
    ds = _standardize_pr_dataset(ds, pr_var_candidates=["rain", "rf", "precip", "precipitation", "pr"])

    # Ensure datetime64 time where possible (safe if already datetime64)
    if "time" not in ds.coords and "time" not in ds.dims:
        raise KeyError("IMD dataset has no 'time' coordinate after standardization.")
    if not np.issubdtype(ds["time"].dtype, np.datetime64):
        # Best-effort decode for CF-style time units if present
        units = ds["time"].attrs.get("units", None)
        if units is not None:
            ds["time"] = xr.conventions.times.decode_cf_datetime(ds["time"], units)

    for y in years:
        target = out_dir / f"{y}.nc"
        if target.exists() and target.stat().st_size > 0:
            print(f"[SKIP] IMD yearly exists: {target}")
            continue

        yearly = ds.sel(time=slice(f"{y}-01-01", f"{y}-12-31"))
        if yearly["time"].size == 0:
            raise ValueError(f"No IMD data found for year={y} using source={imd_source}")

        yearly.to_netcdf(target)
        print(f"[WRITE] IMD -> {target}")

    return out_dir


def _parse_years(years_str: str) -> list[int]:
    parts = [p.strip() for p in years_str.split(",") if p.strip()]
    years: list[int] = []
    for p in parts:
        if "-" in p:
            a, b = p.split("-", 1)
            years.extend(list(range(int(a), int(b) + 1)))
        else:
            years.append(int(p))
    years = sorted(set(years))
    if not years:
        raise ValueError("No years parsed.")
    return years


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage ERA5 + IMD daily precipitation into IRT format under DATA_ROOT/historical/pr/<MODEL>/YEAR.nc"
    )
    parser.add_argument("--years", default="1980-1985")
    parser.add_argument("--bbox", default="15.0,21.0,76.5,82.0", help="lat_min,lat_max,lon_min,lon_max")
    parser.add_argument(
        "--era5-nc-dir",
        type=str,
        default=str((DATA_ROOT.parent / "era5" / "nc").resolve()),
    )
    parser.add_argument("--era5-file-glob", type=str, default=ERA5_DEFAULT_FILE_GLOB)
    parser.add_argument(
        "--imd-source",
        type=str,
        default=str((DATA_ROOT.parent / "imd").resolve()),
        help="IMD source: directory of .nc files or a single .nc file.",
    )
    parser.add_argument("--skip-era5", action="store_true")
    parser.add_argument("--skip-imd", action="store_true")
    args = parser.parse_args()

    years = _parse_years(args.years)
    lat_min, lat_max, lon_min, lon_max = [float(x) for x in args.bbox.split(",")]

    if not args.skip_era5:
        stage_era5_to_data_root(
            era5_nc_dir=Path(args.era5_nc_dir),
            years=years,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            model_name="ERA5",
            file_glob=args.era5_file_glob,
        )

    if not args.skip_imd:
        stage_imd_to_data_root(
            imd_source=Path(args.imd_source),
            years=years,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            model_name="IMD",
        )

    print("\nDone staging reanalysis/observations.")
    print(f"DATA_ROOT = {DATA_ROOT}")
    print(f"ERA5 staged at: {DATA_ROOT / 'historical' / 'pr' / 'ERA5'}")
    print(f"IMD  staged at: {DATA_ROOT / 'historical' / 'pr' / 'IMD'}")


if __name__ == "__main__":
    main()
