# india_resilience_tool/compute/spi_adapter.py
"""
SPI Adapter for the India Resilience Tool (IRT).

This module provides an adapter layer for computing SPI (Standardized Precipitation Index)
using the `climate-indices` package (https://github.com/monocongo/climate_indices).

The adapter:
- Accepts xarray DataArrays of monthly precipitation totals (as produced by the IRT pipeline)
- Calls the scientifically-validated `climate_indices.indices.spi()` function
- Returns results compatible with the existing IRT output structure

Additional IRT behavior:
- Writes per-unit monthly SPI CSVs (e.g., *_monthly.csv) alongside yearly outputs
- Supports annual aggregations derived from monthly SPI:
    - mean (legacy behavior)
    - count_months_lt / count_months_gt using thresholds (e.g., -1, -2, +1, +2)
    - count_events_lt for contiguous monthly SPI runs below a threshold
- Can reuse an existing monthly CSV on disk as a cache if metadata matches.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from india_resilience_tool.utils.naming import hydro_fs_token, safe_fs_component
from india_resilience_tool.utils.processed_io import path_exists, read_csv, write_csv

# Import from climate-indices package
try:
    from climate_indices import compute as ci_compute
    from climate_indices import indices as ci_indices

    CLIMATE_INDICES_AVAILABLE = True
except ImportError:
    CLIMATE_INDICES_AVAILABLE = False
    ci_indices = None
    ci_compute = None

logger = logging.getLogger(__name__)


class Distribution(Enum):
    """
    Distribution types supported for SPI fitting.

    - GAMMA: Standard choice for precipitation data (recommended for most cases)
    - PEARSON: Pearson Type III, better for extremes but can be more data-hungry
    """

    GAMMA = "gamma"
    PEARSON = "pearson"


@dataclass
class SPIResult:
    """
    Container for SPI computation results.

    Attributes:
        monthly_spi: xarray DataArray of monthly SPI values with time coordinate
        annual_spi: xarray DataArray of annual mean SPI values with year coordinate
        scale_months: SPI timescale (3/6/12 etc.)
        distribution: The distribution used for fitting
        calibration_years: (start_year, end_year) used for calibration
        data_years: (first_year, last_year) in the scenario data
        valid_months: Number of non-NaN monthly SPI values
    """

    monthly_spi: xr.DataArray
    annual_spi: xr.DataArray
    scale_months: int
    distribution: Distribution
    calibration_years: Tuple[int, int]
    data_years: Tuple[int, int]
    valid_months: int


def _check_climate_indices_available() -> None:
    """Raise ImportError if climate-indices package is not available."""
    if not CLIMATE_INDICES_AVAILABLE:
        raise ImportError(
            "The 'climate-indices' package is required for this functionality. "
            "Install it with: pip install climate-indices"
        )


def _get_ci_distribution(dist: Distribution):
    """Convert our Distribution enum to climate_indices Distribution enum."""
    _check_climate_indices_available()
    if dist == Distribution.GAMMA:
        return ci_indices.Distribution.gamma
    if dist == Distribution.PEARSON:
        return ci_indices.Distribution.pearson
    raise ValueError(f"Unknown distribution: {dist}")


def _extract_year_from_time(time_coord: xr.DataArray) -> int:
    """Extract the year from an xarray time coordinate."""
    try:
        return int(time_coord.dt.year.values[0])
    except Exception:
        return int(pd.Timestamp(time_coord.values[0]).year)


def _validate_monthly_data(
    monthly_precip: xr.DataArray,
    min_months: int = 24,
) -> Tuple[bool, str]:
    """
    Basic validation of monthly precipitation DataArray.
    """
    if monthly_precip is None:
        return False, "monthly_precip is None"
    if "time" not in monthly_precip.dims:
        return False, "monthly_precip must have a 'time' dimension"

    n_months = int(monthly_precip.sizes.get("time", 0))
    if n_months < min_months:
        return False, f"Insufficient data: {n_months} months (need at least {min_months})"

    valid_count = int((~np.isnan(monthly_precip.values)).sum())
    if valid_count < min_months:
        return False, f"Too many NaN values: only {valid_count} valid months"

    return True, ""


def _to_month_start_datetime_index(time_values: np.ndarray) -> pd.DatetimeIndex:
    """
    Convert an xarray time coordinate (datetime64 or cftime) to month-start pandas timestamps.
    """
    try:
        idx = pd.DatetimeIndex(time_values)
        # Use period "M" then convert to month-start timestamps.
        return idx.to_period("M").to_timestamp(how="start")
    except Exception:
        # CFTime calendars aren't convertible by pandas. Reconstruct from year/month.
        return pd.DatetimeIndex([pd.Timestamp(int(t.year), int(t.month), 1) for t in time_values])

def _require_contiguous_months(times_ms: pd.DatetimeIndex) -> bool:
    expected = pd.date_range(times_ms.min(), times_ms.max(), freq="MS")
    return len(expected) == len(times_ms) and np.array_equal(expected.values, times_ms.values)


def _trim_to_full_years(series: xr.DataArray) -> Optional[xr.DataArray]:
    """
    Trim a monthly series to complete Jan..Dec years so len%12==0 and start aligns to Jan.
    """
    times = pd.DatetimeIndex(series["time"].values)
    start_year = int(times[0].year) + (1 if times[0].month != 1 else 0)
    end_year = int(times[-1].year) - (1 if times[-1].month != 12 else 0)
    if end_year < start_year:
        return None
    return series.sel(time=slice(f"{start_year}-01-01", f"{end_year}-12-01"))


def compute_spi_climate_indices(
    monthly_precip: np.ndarray,
    data_start_year: int,
    calibration_start_year: int,
    calibration_end_year: int,
    scale_months: int = 3,
    distribution: Distribution = Distribution.GAMMA,
) -> np.ndarray:
    """
    Compute SPI using the climate-indices package (low-level wrapper).
    """
    _check_climate_indices_available()

    values = np.asarray(monthly_precip, dtype=np.float64)
    if values.ndim != 1:
        raise ValueError(f"monthly_precip must be 1D, got shape {values.shape}")
    if values.size < scale_months:
        raise ValueError(f"Insufficient data: {values.size} months for scale={scale_months}")
    if values.size % 12 != 0:
        raise ValueError("monthly_precip length must be a multiple of 12 (complete years)")

    if np.isnan(values).any():
        logger.warning("SPI monthly precipitation contains NaNs; coercing NaN→0 for computation.")
        values = np.nan_to_num(values, nan=0.0)

    ci_dist = _get_ci_distribution(distribution)

    return ci_indices.spi(
        values=values,
        scale=scale_months,
        distribution=ci_dist,
        data_start_year=data_start_year,
        calibration_year_initial=calibration_start_year,
        calibration_year_final=calibration_end_year,
        periodicity=ci_compute.Periodicity.monthly,
    )


def compute_spi_for_unit(
    monthly_precip: xr.DataArray,
    calibration_monthly_precip: Optional[xr.DataArray],
    baseline_years: Tuple[int, int],
    scale_months: int = 3,
    distribution: Distribution = Distribution.GAMMA,
    min_months_per_year: int = 9,
) -> Optional[SPIResult]:
    """
    Compute SPI for a single spatial unit (district or block) using climate-indices.

    Guardrails:
      - coerce time to month-start (supports cftime calendars)
      - require contiguous monthly series (no missing months)
      - trim to complete Jan..Dec years (len%12==0)
      - coerce NaNs to 0 with a warning (treat as no rainfall)
    """
    _check_climate_indices_available()

    is_valid, error_msg = _validate_monthly_data(monthly_precip)
    if not is_valid:
        logger.warning(f"Invalid scenario data: {error_msg}")
        return None

    use_separate_calibration = (
        calibration_monthly_precip is not None
        and calibration_monthly_precip is not monthly_precip
    )

    if use_separate_calibration:
        is_valid, error_msg = _validate_monthly_data(calibration_monthly_precip)
        if not is_valid:
            logger.warning(f"Invalid calibration data: {error_msg}. Using scenario data.")
            use_separate_calibration = False

    def _normalize_monthly_da(da: xr.DataArray) -> xr.DataArray:
        da = da.sortby("time")
        times_ms = _to_month_start_datetime_index(da["time"].values)
        da = da.assign_coords(time=times_ms).sortby("time")
        # Drop duplicate months if overlap exists
        _, unique_idx = np.unique(da["time"].values, return_index=True)
        return da.isel(time=np.sort(unique_idx))

    scen_norm = _normalize_monthly_da(monthly_precip)
    scen_times = pd.DatetimeIndex(scen_norm["time"].values)
    scen_start_year = _extract_year_from_time(scen_norm["time"][:1])
    scen_end_year = _extract_year_from_time(scen_norm["time"][-1:])
    scen_first_ts = scen_times.min()

    if use_separate_calibration:
        calib_norm = _normalize_monthly_da(calibration_monthly_precip)
        calib_times = pd.DatetimeIndex(calib_norm["time"].values)

        if not _require_contiguous_months(calib_times):
            logger.warning("SPI calibration series must be contiguous monthly (no missing months). Skipping unit.")
            return None
        if not _require_contiguous_months(scen_times):
            logger.warning("SPI scenario series must be contiguous monthly (no missing months). Skipping unit.")
            return None

        full_index = pd.date_range(calib_times.min(), scen_times.max(), freq="MS")

        # Build a contiguous combined series. Months between calibration and scenario
        # are filled with 0 precipitation to satisfy the contiguous-series contract.
        calib_s = pd.Series(calib_norm.values.astype(float), index=calib_times)
        scen_s = pd.Series(scen_norm.values.astype(float), index=scen_times)
        combined_s = calib_s.reindex(full_index)
        combined_s.update(scen_s)
        combined = xr.DataArray(
            combined_s.to_numpy(dtype=float),
            coords={"time": full_index},
            dims=["time"],
        ).sortby("time")
    else:
        combined = scen_norm
        if not _require_contiguous_months(scen_times):
            logger.warning("SPI requires a contiguous monthly series (no missing months). Skipping unit.")
            return None

    combined = _trim_to_full_years(combined)
    if combined is None:
        logger.warning("SPI requires at least one complete Jan–Dec year after trimming. Skipping unit.")
        return None

    times_ms = pd.DatetimeIndex(combined["time"].values)
    data_start_year = int(times_ms[0].year)

    values = combined.values.astype(np.float64)
    if np.isnan(values).any():
        logger.warning("SPI precipitation series contains NaNs; coercing NaN→0.")
        values = np.nan_to_num(values, nan=0.0)
    if values.size < scale_months or values.size % 12 != 0:
        logger.warning(
            f"SPI requires complete years (len%12==0) and len>=scale. Got len={values.size}, scale={scale_months}. Skipping unit."
        )
        return None

    try:
        spi_values = compute_spi_climate_indices(
            monthly_precip=values,
            data_start_year=data_start_year,
            calibration_start_year=baseline_years[0],
            calibration_end_year=baseline_years[1],
            scale_months=scale_months,
            distribution=distribution,
        )
    except Exception as e:
        logger.warning(f"SPI computation failed: {e}")
        return None

    spi_monthly = xr.DataArray(
        spi_values,
        coords={"time": combined["time"]},
        dims=["time"],
        name="spi",
    )

    # If we used separate calibration, keep only scenario months (>= scenario first year)
    if use_separate_calibration:
        spi_monthly = spi_monthly.sel(time=spi_monthly["time"] >= scen_first_ts)
        # Prevent rolling-window bleed across the calibration→scenario boundary by
        # invalidating the first (scale-1) scenario months.
        k = max(0, int(scale_months) - 1)
        if k > 0 and spi_monthly.sizes.get("time", 0) > 0:
            k_eff = min(k, int(spi_monthly.sizes.get("time", 0)))
            spi_monthly = spi_monthly.copy()
            spi_monthly.data[:k_eff] = np.nan

    spi_annual = _annualize_spi_xarray(spi_monthly, min_months_per_year=min_months_per_year)
    if spi_annual is None or spi_annual.sizes.get("year", 0) == 0:
        logger.warning("No valid annual SPI values after aggregation")
        return None

    valid_months = int((~np.isnan(spi_monthly.values)).sum())

    return SPIResult(
        monthly_spi=spi_monthly,
        annual_spi=spi_annual,
        scale_months=scale_months,
        distribution=distribution,
        calibration_years=baseline_years,
        data_years=(scen_start_year, scen_end_year),
        valid_months=valid_months,
    )


def compare_spi_implementations(
    *,
    monthly_precip: xr.DataArray,
    baseline_years: Tuple[int, int],
    scale_months: int = 3,
    min_months_per_year: int = 9,
) -> dict:
    """
    Compare SPI outputs across supported distribution implementations.

    This is a diagnostic helper primarily intended for tests and debugging.

    Returns:
        Dict with keys:
          - "gamma": SPIResult | None
          - "pearson": SPIResult | None
          - "comparison": {"correlation": float, "n": int}
    """
    _check_climate_indices_available()

    gamma = compute_spi_for_unit(
        monthly_precip=monthly_precip,
        calibration_monthly_precip=None,
        baseline_years=baseline_years,
        scale_months=scale_months,
        distribution=Distribution.GAMMA,
        min_months_per_year=min_months_per_year,
    )
    pearson = compute_spi_for_unit(
        monthly_precip=monthly_precip,
        calibration_monthly_precip=None,
        baseline_years=baseline_years,
        scale_months=scale_months,
        distribution=Distribution.PEARSON,
        min_months_per_year=min_months_per_year,
    )

    corr = float("nan")
    n = 0
    if gamma is not None and pearson is not None:
        a = np.asarray(gamma.monthly_spi.values, dtype=float)
        b = np.asarray(pearson.monthly_spi.values, dtype=float)
        mask = np.isfinite(a) & np.isfinite(b)
        n = int(mask.sum())
        if n >= 2:
            try:
                corr = float(np.corrcoef(a[mask], b[mask])[0, 1])
            except Exception:
                corr = float("nan")

    return {
        "gamma": gamma,
        "pearson": pearson,
        "comparison": {"correlation": corr, "n": n},
    }


def _annualize_spi_xarray(
    spi_monthly: xr.DataArray,
    min_months_per_year: int = 9,
) -> Optional[xr.DataArray]:
    """
    Aggregate monthly SPI to annual values (mean over months).
    """
    if spi_monthly is None or spi_monthly.sizes.get("time", 0) == 0:
        return None

    grouped = spi_monthly.groupby("time.year")
    counts = grouped.count(dim="time")
    means = grouped.mean(dim="time", skipna=True)
    means = means.where(counts >= min_months_per_year, drop=True)

    if means.sizes.get("year", 0) == 0:
        return None

    means.name = "spi_annual"
    return means


def _annualize_spi_counts_xarray(
    spi_monthly: xr.DataArray,
    threshold: float,
    mode: str,
    min_months_per_year: int = 9,
) -> Optional[xr.DataArray]:
    """
    Aggregate monthly SPI to annual counts of months crossing a threshold.
    """
    if spi_monthly is None or spi_monthly.sizes.get("time", 0) == 0:
        return None

    grouped = spi_monthly.groupby("time.year")
    valid_counts = grouped.count(dim="time")

    if mode == "count_months_lt":
        hits = (spi_monthly < threshold).where(~np.isnan(spi_monthly))
    elif mode == "count_months_gt":
        hits = (spi_monthly > threshold).where(~np.isnan(spi_monthly))
    else:
        return None

    annual_counts = hits.groupby("time.year").sum(dim="time", skipna=True)
    annual_counts = annual_counts.where(valid_counts >= min_months_per_year, drop=True)

    if annual_counts.sizes.get("year", 0) == 0:
        return None

    annual_counts.name = "spi_annual_count"
    return annual_counts


def _count_events_1d(mask: np.ndarray, min_len: int = 1) -> int:
    """Return the number of contiguous True runs meeting ``min_len``."""
    arr = np.asarray(mask, dtype=bool)
    if arr.size == 0:
        return 0

    events = 0
    run_len = 0
    for value in arr:
        if value:
            run_len += 1
        else:
            if run_len >= min_len:
                events += 1
            run_len = 0
    if run_len >= min_len:
        events += 1
    return events


def _annualize_spi_event_counts_xarray(
    spi_monthly: xr.DataArray,
    threshold: float,
    min_months_per_year: int = 9,
) -> Optional[xr.DataArray]:
    """Aggregate monthly SPI to annual counts of contiguous below-threshold events."""
    if spi_monthly is None or spi_monthly.sizes.get("time", 0) == 0:
        return None

    grouped = spi_monthly.groupby("time.year")
    valid_counts = grouped.count(dim="time")
    event_rows: list[tuple[int, int]] = []

    for year, values in grouped:
        if int(valid_counts.sel(year=year).item()) < int(min_months_per_year):
            continue
        flags = (values < threshold).fillna(False).values
        event_rows.append((int(year), _count_events_1d(flags, min_len=1)))

    if not event_rows:
        return None

    annual_counts = xr.DataArray(
        [count for _, count in event_rows],
        coords={"year": [year for year, _ in event_rows]},
        dims=["year"],
        name="spi_annual_count",
    )
    return annual_counts


def _safe_component(name: str) -> str:
    return safe_fs_component(name)


def _parse_unit_key(level: str, unit_key: str) -> tuple[str, Optional[str]]:
    if level in {"block", "sub_basin"}:
        if "||" in unit_key:
            primary_name, secondary_name = unit_key.split("||", 1)
            return primary_name, secondary_name
        return "Unknown", unit_key
    return unit_key, None


def _add_unit_fields_from_key(row: dict[str, object], *, level: str, unit_key: str) -> None:
    """Populate level-specific identity fields from a pipeline unit key."""
    if level == "block":
        district, block = _parse_unit_key(level, unit_key)
        row["district"] = district
        row["block"] = block or unit_key
        return

    if level == "sub_basin":
        basin, sub_basin = _parse_unit_key(level, unit_key)
        row["basin"] = basin
        row["sub_basin"] = sub_basin or unit_key
        return

    if level == "basin":
        row["basin"] = unit_key
        return

    row["district"] = unit_key


def _monthly_spi_csv_path(
    *,
    metric_root_path: Path,
    state_name: str,
    level_folder: str,
    level: str,
    unit_key: str,
    model: str,
    scenario: str,
) -> Path:
    primary_name, secondary_name = _parse_unit_key(level, unit_key)
    primary_safe = _safe_component(primary_name)

    if level == "block":
        block_safe = _safe_component(secondary_name or unit_key)
        out_dir = metric_root_path / state_name / level_folder / primary_safe / block_safe / model / scenario
        return out_dir / f"{block_safe}_monthly.csv"

    if level == "sub_basin":
        primary_safe = hydro_fs_token(primary_name)
        sub_basin_safe = hydro_fs_token(secondary_name or unit_key)
        out_dir = metric_root_path / state_name / level_folder / primary_safe / sub_basin_safe / model / scenario
        return out_dir / f"{sub_basin_safe}_monthly.csv"

    out_dir = metric_root_path / state_name / level_folder / primary_safe / model / scenario
    return out_dir / f"{primary_safe}_monthly.csv"


def _load_monthly_spi_csv(
    path: Path,
    *,
    expected_scale_months: int,
    expected_distribution: Distribution,
    expected_baseline_years: Tuple[int, int],
    expected_model: str,
    expected_scenario: str,
) -> Optional[xr.DataArray]:
    if not path_exists(path):
        return None

    try:
        df = read_csv(path)
    except Exception as e:
        logger.warning(f"Failed to read monthly SPI CSV '{path}': {e}")
        return None

    required = {
        "time",
        "spi",
        "scale_months",
        "distribution",
        "baseline_start_year",
        "baseline_end_year",
        "model",
        "scenario",
    }
    if not required.issubset(set(df.columns)):
        return None

    try:
        scale_ok = int(df["scale_months"].iloc[0]) == int(expected_scale_months)
        dist_ok = str(df["distribution"].iloc[0]).strip().lower() == expected_distribution.value
        base_ok = (
            int(df["baseline_start_year"].iloc[0]) == int(expected_baseline_years[0])
            and int(df["baseline_end_year"].iloc[0]) == int(expected_baseline_years[1])
        )
        model_ok = str(df["model"].iloc[0]).strip() == expected_model
        scen_ok = str(df["scenario"].iloc[0]).strip() == expected_scenario
        if not (scale_ok and dist_ok and base_ok and model_ok and scen_ok):
            return None

        times = pd.to_datetime(df["time"], errors="coerce")
        spi_vals = pd.to_numeric(df["spi"], errors="coerce").astype(float).values
        mask = ~times.isna()
        if mask.sum() == 0:
            return None

        times = times[mask]
        spi_vals = spi_vals[mask.values]

        return xr.DataArray(
            spi_vals,
            coords={"time": pd.DatetimeIndex(times).to_period("M").to_timestamp(how="start")},
            dims=["time"],
            name="spi",
        ).sortby("time")
    except Exception:
        return None


def _write_monthly_spi_csv(
    path: Path,
    *,
    spi_monthly: xr.DataArray,
    scale_months: int,
    distribution: Distribution,
    baseline_years: Tuple[int, int],
    model: str,
    scenario: str,
    level: str,
    unit_key: str,
) -> None:
    times = pd.DatetimeIndex(spi_monthly["time"].values)
    times_ms = times.to_period("M").to_timestamp(how="start")
    data: dict[str, object] = {
        "time": times_ms,
        "year": times_ms.year.astype(int),
        "month": times_ms.month.astype(int),
        "spi": spi_monthly.values.astype(float),
        "scale_months": int(scale_months),
        "distribution": distribution.value,
        "baseline_start_year": int(baseline_years[0]),
        "baseline_end_year": int(baseline_years[1]),
        "model": model,
        "scenario": scenario,
    }
    _add_unit_fields_from_key(data, level=level, unit_key=unit_key)
    df = pd.DataFrame(data)
    write_csv(df, path, index=False)


def compute_spi_rows_climate_indices(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    scen_monthly_by_unit: dict[str, xr.DataArray],
    calib_monthly_by_unit: dict[str, xr.DataArray],
    masks: dict[str, xr.DataArray],
    level: str,
    baseline_years: Tuple[int, int],
    scale_months: int,
    year_to_paths: dict[int, dict[str, Path]],
    *,
    metric_root_path: Optional[Path] = None,
    state_name: str = "",
    level_folder: str = "",
) -> list[dict]:
    """
    Compute SPI-derived yearly rows for all spatial units.

    Writes per-unit monthly SPI CSVs alongside yearly outputs:
      - districts: {district_safe}_monthly.csv
      - blocks:    {block_safe}_monthly.csv
      - basins:    {basin_safe}_monthly.csv
      - sub-basins:{sub_basin_safe}_monthly.csv

    If a compatible monthly file exists, it is reused as a cache.
    """
    _check_climate_indices_available()

    slug = metric["slug"]
    value_col = metric["value_col"]
    varname = (metric.get("var") or "pr").strip()

    dist_str = str(metric.get("params", {}).get("distribution", "gamma")).lower()
    distribution = Distribution.PEARSON if dist_str == "pearson" else Distribution.GAMMA

    min_months_per_year = int(metric.get("params", {}).get("min_months_per_year", 9))
    annual_agg = str(metric.get("params", {}).get("annual_aggregation", "mean")).lower()

    write_monthly_csv = bool(metric.get("params", {}).get("write_monthly_csv", True))
    use_monthly_cache = bool(metric.get("params", {}).get("use_monthly_cache", True))
    enable_monthly_paths = bool(metric_root_path and str(state_name).strip() and str(level_folder).strip())

    rows: list[dict] = []

    for unit_key in masks.keys():
        monthly_path: Optional[Path] = None
        if enable_monthly_paths:
            monthly_path = _monthly_spi_csv_path(
                metric_root_path=Path(metric_root_path),
                state_name=str(state_name),
                level_folder=str(level_folder),
                level=level,
                unit_key=unit_key,
                model=model,
                scenario=scenario,
            )

        spi_monthly: Optional[xr.DataArray] = None
        if use_monthly_cache and monthly_path is not None:
            spi_monthly = _load_monthly_spi_csv(
                monthly_path,
                expected_scale_months=scale_months,
                expected_distribution=distribution,
                expected_baseline_years=baseline_years,
                expected_model=model,
                expected_scenario=scenario,
            )

        if spi_monthly is None:
            scen_mon = scen_monthly_by_unit.get(unit_key)
            if scen_mon is None:
                continue

            calib_mon = calib_monthly_by_unit.get(unit_key)

            use_separate_calib = (
                calib_mon is not None
                and calib_mon is not scen_mon
                and scenario != "historical"
            )

            result = compute_spi_for_unit(
                monthly_precip=scen_mon,
                calibration_monthly_precip=calib_mon if use_separate_calib else None,
                baseline_years=baseline_years,
                scale_months=scale_months,
                distribution=distribution,
                min_months_per_year=min_months_per_year,
            )
            if result is None:
                logger.debug(f"[{slug}] No SPI result for unit={unit_key}")
                continue

            spi_monthly = result.monthly_spi

            if write_monthly_csv and monthly_path is not None:
                try:
                    _write_monthly_spi_csv(
                        monthly_path,
                        spi_monthly=spi_monthly,
                        scale_months=scale_months,
                        distribution=distribution,
                        baseline_years=baseline_years,
                        model=model,
                        scenario=scenario,
                        level=level,
                        unit_key=unit_key,
                    )
                except Exception as e:
                    logger.warning(f"[{slug}] Failed to write monthly SPI CSV for unit={unit_key}: {e}")

        # Annual aggregation selection
        spi_yearly: Optional[xr.DataArray]
        if annual_agg == "mean":
            spi_yearly = _annualize_spi_xarray(spi_monthly, min_months_per_year=min_months_per_year)
        elif annual_agg in {"count_months_lt", "count_months_gt"}:
            threshold = float(metric.get("params", {}).get("threshold"))
            spi_yearly = _annualize_spi_counts_xarray(
                spi_monthly=spi_monthly,
                threshold=threshold,
                mode=annual_agg,
                min_months_per_year=min_months_per_year,
            )
        elif annual_agg == "count_events_lt":
            threshold = float(metric.get("params", {}).get("threshold"))
            spi_yearly = _annualize_spi_event_counts_xarray(
                spi_monthly=spi_monthly,
                threshold=threshold,
                min_months_per_year=min_months_per_year,
            )
        else:
            logger.warning(f"[{slug}] Unknown annual_aggregation='{annual_agg}'. Skipping.")
            continue

        if spi_yearly is None or spi_yearly.sizes.get("year", 0) == 0:
            continue

        for y in spi_yearly["year"].values:
            y_int = int(y)
            v = float(spi_yearly.sel(year=y).item())

            source_path = ""
            try:
                source_path = str(year_to_paths.get(y_int, {}).get(varname, ""))
            except Exception:
                pass

            row = {
                "year": y_int,
                "value": v,
                value_col: v,
                "source_file": source_path,
            }
            _add_unit_fields_from_key(row, level=level, unit_key=unit_key)

            rows.append(row)

    logger.info(
        f"[{slug}] SPI-derived metric (dist={distribution.value}, scale={scale_months}, agg={annual_agg}) "
        f"for {len(masks)} units, {len(rows)} rows"
    )
    return rows
