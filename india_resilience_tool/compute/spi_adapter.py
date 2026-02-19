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
        # Do not treat missing months as zero rainfall.
        raise ValueError("monthly_precip contains NaNs; provide a complete monthly series")

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
      - refuse NaNs (do NOT coerce NaN→0)
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

    if use_separate_calibration:
        scen_times = monthly_precip["time"]
        scen_start_year = _extract_year_from_time(scen_times[:1])
        scen_end_year = _extract_year_from_time(scen_times[-1:])

        combined = xr.concat([calibration_monthly_precip, monthly_precip], dim="time")
        combined = combined.sortby("time")

        # Drop duplicate months if overlap exists
        _, unique_idx = np.unique(combined["time"].values, return_index=True)
        combined = combined.isel(time=np.sort(unique_idx))
    else:
        combined = monthly_precip.sortby("time")
        scen_start_year = _extract_year_from_time(monthly_precip["time"][:1])
        scen_end_year = _extract_year_from_time(monthly_precip["time"][-1:])

    # Normalize time to month-start (supports numpy datetime64 and cftime)
    times_ms = _to_month_start_datetime_index(combined["time"].values)
    combined = combined.assign_coords(time=times_ms).sortby("time")

    if not _require_contiguous_months(pd.DatetimeIndex(combined["time"].values)):
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
        logger.warning("SPI precipitation series contains NaNs. Skipping unit (do not coerce NaN→0).")
        return None
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
        spi_monthly = spi_monthly.sel(time=spi_monthly["time"].dt.year >= scen_start_year)

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


def _safe_component(name: str) -> str:
    return str(name).strip().replace(" ", "_").replace("/", "_")


def _parse_unit_key(level: str, unit_key: str) -> tuple[str, Optional[str]]:
    if level == "block":
        if "||" in unit_key:
            district, block = unit_key.split("||", 1)
            return district, block
        return "Unknown", unit_key
    return unit_key, None


def _monthly_spi_cache_path(
    *,
    metric_root_path: Path,
    state_name: str,
    level_folder: str,
    level: str,
    unit_key: str,
    model: str,
    scenario: str,
) -> Path:
    district, block = _parse_unit_key(level, unit_key)
    district_safe = _safe_component(district)

    if level == "block":
        block_safe = _safe_component(block or unit_key)
        out_dir = (
            metric_root_path
            / state_name
            / level_folder
            / "raw"
            / "spi_monthly"
            / f"model={model}"
            / f"scenario={scenario}"
            / f"district={district_safe}"
            / f"block={block_safe}"
        )
        return out_dir / "data.parquet"

    out_dir = (
        metric_root_path
        / state_name
        / level_folder
        / "raw"
        / "spi_monthly"
        / f"model={model}"
        / f"scenario={scenario}"
        / f"district={district_safe}"
    )
    return out_dir / "data.parquet"


def _load_monthly_spi_table(
    path: Path,
    *,
    expected_scale_months: int,
    expected_distribution: Distribution,
    expected_baseline_years: Tuple[int, int],
    expected_model: str,
    expected_scenario: str,
) -> Optional[xr.DataArray]:
    if not path.exists():
        return None

    try:
        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    except Exception as e:
        logger.warning(f"Failed to read monthly SPI cache '{path}': {e}")
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


def _write_monthly_spi_parquet(
    path: Path,
    *,
    spi_monthly: xr.DataArray,
    scale_months: int,
    distribution: Distribution,
    baseline_years: Tuple[int, int],
    model: str,
    scenario: str,
    district: str,
    block: Optional[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    times = pd.DatetimeIndex(spi_monthly["time"].values)
    times_ms = times.to_period("M").to_timestamp(how="start")
    df = pd.DataFrame(
        {
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
            "district": district,
            "block": block or "",
        }
    )
    df.to_parquet(path, index=False, compression="zstd")


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
    metric_root_path: Path,
    state_name: str,
    level_folder: str,
) -> list[dict]:
    """
    Compute SPI-derived yearly rows for all spatial units.

    Writes per-unit monthly SPI caches alongside yearly outputs (Parquet):
      - districts: raw/spi_monthly/model=.../scenario=.../district=.../data.parquet
      - blocks:    raw/spi_monthly/model=.../scenario=.../district=.../block=.../data.parquet

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

    rows: list[dict] = []

    for unit_key in masks.keys():
        district, block = _parse_unit_key(level, unit_key)

        monthly_path = _monthly_spi_cache_path(
            metric_root_path=metric_root_path,
            state_name=state_name,
            level_folder=level_folder,
            level=level,
            unit_key=unit_key,
            model=model,
            scenario=scenario,
        )

        spi_monthly: Optional[xr.DataArray] = None
        if use_monthly_cache:
            spi_monthly = _load_monthly_spi_table(
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

            if write_monthly_csv:
                try:
                    _write_monthly_spi_parquet(
                        monthly_path,
                        spi_monthly=spi_monthly,
                        scale_months=scale_months,
                        distribution=distribution,
                        baseline_years=baseline_years,
                        model=model,
                        scenario=scenario,
                        district=district,
                        block=block,
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
                "district": district if level == "block" else unit_key,
            }
            if level == "block":
                row["block"] = block or unit_key

            rows.append(row)

    logger.info(
        f"[{slug}] SPI-derived metric (dist={distribution.value}, scale={scale_months}, agg={annual_agg}) "
        f"for {len(masks)} units, {len(rows)} rows"
    )
    return rows
