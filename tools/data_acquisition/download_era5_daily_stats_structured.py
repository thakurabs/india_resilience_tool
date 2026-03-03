#!/usr/bin/env python3
"""
download_era5_daily_stats_structured.py

Download ERA5 (0.25°) daily statistics from CDS for a given bbox and save extracted
NetCDFs into per-variable subfolders:

  D:\\projects\\irt_data\\era5\\nc\\tas\\   era5_daily_tas_daily_mean_YYYYMM_tel.nc
  D:\\projects\\irt_data\\era5\\nc\\pr\\    era5_daily_pr_daily_sum_YYYYMM_tel.nc
  D:\\projects\\irt_data\\era5\\nc\\tasmax\\era5_daily_tasmax_daily_max_YYYYMM_tel.nc
  D:\\projects\\irt_data\\era5\\nc\\tasmin\\era5_daily_tasmin_daily_min_YYYYMM_tel.nc
  D:\\projects\\irt_data\\era5\\nc\\tdps\\  era5_daily_tdps_daily_mean_YYYYMM_tel.nc

This script targets the CDS dataset:
  derived-era5-single-levels-daily-statistics

Notes:
- Requests NetCDF output delivered as ZIP; script extracts the single .nc from the ZIP.
- Occasionally CDS returns a NetCDF file even when requested as ZIP; extractor handles this.
- For accumulated variables like total_precipitation, prefer 1_hourly to avoid undercounting.
- For daily max/min temperature, prefer 1_hourly to avoid missing true extremes.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import calendar
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import cdsapi


# Match your climate model domain extents
LAT_MIN, LAT_MAX = 15.0, 21.0
LON_MIN, LON_MAX = 76.5, 82.0

# CDS 'area' format is [north, west, south, east]
AREA_TELANGANA_DOMAIN = [LAT_MAX, LON_MIN, LAT_MIN, LON_MAX]


@dataclass(frozen=True)
class VariableConfig:
    """
    Configuration for a single output alias.

    Attributes:
        cds_variable: ERA5 variable name in CDS for this dataset (e.g., "2m_temperature")
        daily_statistic: "daily_mean" | "daily_sum" | "daily_max" | "daily_min"
        frequency: "1_hourly" | "6_hourly" (etc.)
    """

    cds_variable: str
    daily_statistic: str
    frequency: str


def _days_in_month(year: int, month: int) -> List[str]:
    """Return day strings ['01', ...] valid for the given year-month."""
    last_day = calendar.monthrange(year, month)[1]
    return [f"{d:02d}" for d in range(1, last_day + 1)]


def _default_file_stem(alias: str, daily_statistic: str, year: int, month: int) -> str:
    """Create a consistent file stem for outputs."""
    return f"era5_daily_{alias}_{daily_statistic}_{year}{month:02d}_tel"


def _extract_single_nc_from_zip(zip_path: Path, target_nc_path: Path) -> None:
    """
    Extract a single NetCDF file into target_nc_path.

    CDS usually delivers NetCDF-in-ZIP for this dataset, but occasionally the response is
    already a NetCDF file saved with a .zip extension. This function handles both cases:
      - ZIP archive containing .nc -> extract to target_nc_path
      - NetCDF file (classic or netCDF4/HDF5) -> move/rename to target_nc_path
    """
    target_nc_path.parent.mkdir(parents=True, exist_ok=True)

    if not zip_path.exists() or zip_path.stat().st_size == 0:
        raise FileNotFoundError(f"Missing or empty download: {zip_path}")

    with zip_path.open("rb") as f:
        head4 = f.read(4)

    is_zip = head4.startswith(b"PK")
    is_netcdf_classic = head4 in (b"CDF\x01", b"CDF\x02")
    is_netcdf4_hdf5 = head4 == b"\x89HDF"

    # Case A: Already NetCDF (but maybe named .zip)
    if is_netcdf_classic or is_netcdf4_hdf5:
        if target_nc_path.exists():
            target_nc_path.unlink()
        shutil.move(str(zip_path), str(target_nc_path))
        return

    # Case B: ZIP containing NetCDF
    if is_zip:
        with zipfile.ZipFile(zip_path, "r") as zf:
            nc_members = [m for m in zf.namelist() if m.lower().endswith(".nc")]
            if not nc_members:
                raise FileNotFoundError(f"No .nc found inside zip: {zip_path}")

            member = nc_members[0]
            tmp_extract_dir = target_nc_path.parent / "_tmp_extract"
            tmp_extract_dir.mkdir(parents=True, exist_ok=True)

            zf.extract(member, path=str(tmp_extract_dir))
            extracted_path = tmp_extract_dir / member

            if target_nc_path.exists():
                target_nc_path.unlink()
            extracted_path.replace(target_nc_path)

            # Cleanup temp directory (best-effort)
            try:
                for p in sorted(tmp_extract_dir.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink()
                    else:
                        p.rmdir()
                tmp_extract_dir.rmdir()
            except Exception:
                pass
        return

    raise zipfile.BadZipFile(f"Downloaded file is neither ZIP nor NetCDF (head={head4!r}): {zip_path}")


def download_era5_daily_stats_monthly_structured(
    out_dir: Path,
    years: List[int],
    variable_settings: Dict[str, VariableConfig],
    area: List[float],
    time_zone: str = "utc+00:00",
    delete_zip_after_extract: bool = False,
) -> None:
    """
    Download ERA5 daily-statistics by year+month and save extracted NetCDF into per-alias folders.

    Directory layout created:
      <out_dir>\\zips\\<alias>\\  (raw downloads)
      <out_dir>\\nc\\<alias>\\    (extracted .nc files)

    Args:
        out_dir: Root output dir (e.g., D:\\projects\\irt_data\\era5)
        years: Years to download.
        variable_settings: alias -> VariableConfig (cds_variable, daily_statistic, frequency)
        area: [north, west, south, east] bounding box.
        time_zone: Time zone for daily aggregation (usually "utc+00:00").
        delete_zip_after_extract: If True, remove ZIP after successful extraction.
    """
    dataset = "derived-era5-single-levels-daily-statistics"
    client = cdsapi.Client()

    zips_root = out_dir / "zips"
    nc_root = out_dir / "nc"
    zips_root.mkdir(parents=True, exist_ok=True)
    nc_root.mkdir(parents=True, exist_ok=True)

    months = list(range(1, 13))

    for year in years:
        for month in months:
            month_str = f"{month:02d}"
            day_list = _days_in_month(year, month)

            for alias, cfg in variable_settings.items():
                daily_statistic = cfg.daily_statistic
                frequency = cfg.frequency
                cds_variable = cfg.cds_variable

                stem = _default_file_stem(alias, daily_statistic, year, month)

                alias_zip_dir = zips_root / alias
                alias_nc_dir = nc_root / alias
                alias_zip_dir.mkdir(parents=True, exist_ok=True)
                alias_nc_dir.mkdir(parents=True, exist_ok=True)

                target_nc = alias_nc_dir / f"{stem}.nc"
                target_zip = alias_zip_dir / f"{stem}.zip"

                # Skip if extracted NetCDF already exists
                if target_nc.exists() and target_nc.stat().st_size > 0:
                    print(f"[SKIP] NetCDF exists: {target_nc}")
                    continue

                request = {
                    "product_type": "reanalysis",
                    "variable": [cds_variable],
                    "year": str(year),
                    "month": [month_str],
                    "day": day_list,
                    "daily_statistic": daily_statistic,
                    "time_zone": time_zone,
                    "frequency": frequency,
                    "area": area,
                    "data_format": "netcdf",
                    "download_format": "zip",
                }

                print(
                    f"[GET] {year}-{month_str} | alias={alias} | var={cds_variable} | {daily_statistic} | "
                    f"freq={frequency} -> {target_zip}"
                )
                client.retrieve(dataset, request, str(target_zip))

                print(f"[EXTRACT] {target_zip.name} -> {target_nc.name}")
                _extract_single_nc_from_zip(target_zip, target_nc)

                if delete_zip_after_extract and target_zip.exists():
                    try:
                        target_zip.unlink()
                        print(f"[CLEAN] Deleted zip: {target_zip}")
                    except Exception:
                        print(f"[WARN] Could not delete zip: {target_zip}")


def main() -> None:
    out_dir = Path(r"D:\projects\irt_data\era5")
    years = list(range(1951, 2026))

    # Alias mapping to support compute_indices_multiprocess.py inputs.
    # - pr uses total_precipitation daily_sum with 1_hourly (avoid undercounting)
    # - tas daily_mean can be 6_hourly
    # - tasmax/tasmin daily extremes should be 1_hourly
    # - tdps (dewpoint) daily_mean can be 6_hourly
    variable_settings: Dict[str, VariableConfig] = {
        "pr": VariableConfig(cds_variable="total_precipitation", daily_statistic="daily_sum", frequency="1_hourly"),
        "tas": VariableConfig(cds_variable="2m_temperature", daily_statistic="daily_mean", frequency="6_hourly"),
        "tasmax": VariableConfig(cds_variable="2m_temperature", daily_statistic="daily_max", frequency="1_hourly"),
        "tasmin": VariableConfig(cds_variable="2m_temperature", daily_statistic="daily_min", frequency="1_hourly"),
        "tdps": VariableConfig(cds_variable="2m_dewpoint_temperature", daily_statistic="daily_mean", frequency="6_hourly"),
    }

    download_era5_daily_stats_monthly_structured(
        out_dir=out_dir,
        years=years,
        variable_settings=variable_settings,
        area=AREA_TELANGANA_DOMAIN,
        time_zone="utc+00:00",
        delete_zip_after_extract=False,
    )


if __name__ == "__main__":
    main()
