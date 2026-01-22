"""
SPI Adapter for the India Resilience Tool (IRT).

This module provides an adapter layer for computing SPI (Standardized Precipitation Index)
using the `climate-indices` package (https://github.com/monocongo/climate_indices).

The adapter:
- Accepts xarray DataArrays of monthly precipitation totals (as produced by the IRT pipeline)
- Calls the scientifically-validated `climate_indices.indices.spi()` function
- Returns results compatible with the existing IRT output structure

Key advantages of using `climate-indices`:
- Peer-reviewed, NOAA-developed implementation
- Supports both Gamma and Pearson Type III distributions
- Proper handling of zero-inflation in precipitation data
- Numba-accelerated for performance

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

import numpy as np
import xarray as xr

# Import from climate-indices package
try:
    from climate_indices import indices as ci_indices
    from climate_indices import compute as ci_compute
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
    - PEARSON: Pearson Type III, better for extreme values but requires more data
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
        scale_months: The timescale used for SPI computation
        distribution: The distribution used for fitting
        calibration_years: Tuple of (start_year, end_year) used for calibration
        data_years: Tuple of (first_year, last_year) in the data
        valid_months: Number of valid (non-NaN) monthly SPI values
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
    elif dist == Distribution.PEARSON:
        return ci_indices.Distribution.pearson
    else:
        raise ValueError(f"Unknown distribution: {dist}")


def _extract_year_from_time(time_coord: xr.DataArray) -> int:
    """Extract the year from an xarray time coordinate."""
    try:
        return int(time_coord.dt.year.values[0])
    except Exception:
        # Fallback for different time formats
        import pandas as pd
        return pd.Timestamp(time_coord.values[0]).year


def _validate_monthly_data(
    monthly_precip: xr.DataArray,
    min_months: int = 24,
) -> Tuple[bool, str]:
    """
    Validate monthly precipitation data for SPI computation.
    
    Args:
        monthly_precip: DataArray with 'time' dimension containing monthly totals
        min_months: Minimum number of months required
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if monthly_precip is None:
        return False, "monthly_precip is None"
    
    if "time" not in monthly_precip.dims:
        return False, "monthly_precip must have a 'time' dimension"
    
    n_months = monthly_precip.sizes.get("time", 0)
    if n_months < min_months:
        return False, f"Insufficient data: {n_months} months (need at least {min_months})"
    
    # Check for all-NaN data
    valid_count = int((~np.isnan(monthly_precip.values)).sum())
    if valid_count < min_months:
        return False, f"Too many NaN values: only {valid_count} valid months"
    
    return True, ""


def compute_spi_climate_indices(
    monthly_precip: np.ndarray,
    data_start_year: int,
    calibration_start_year: int,
    calibration_end_year: int,
    scale_months: int = 3,
    distribution: Distribution = Distribution.GAMMA,
) -> np.ndarray:
    """
    Compute SPI using the climate-indices package.
    
    This is a low-level function that directly wraps `climate_indices.indices.spi()`.
    For most use cases, prefer `compute_spi_for_unit()` which handles xarray data.
    
    Args:
        monthly_precip: 1D numpy array of monthly precipitation totals (mm).
                        Length must be a multiple of 12 (complete years).
        data_start_year: The year corresponding to the first value in the array.
        calibration_start_year: Start year for the calibration/baseline period.
        calibration_end_year: End year for the calibration/baseline period (inclusive).
        scale_months: SPI timescale (1, 3, 6, 12, etc.). Default is 3.
        distribution: Distribution to use for fitting. Default is GAMMA.
        
    Returns:
        1D numpy array of SPI values (same length as input).
        First (scale_months - 1) values will be NaN due to rolling accumulation.
        
    Raises:
        ImportError: If climate-indices package is not installed.
        ValueError: If input data is invalid.
        
    Example:
        >>> precip = np.random.gamma(2, 50, size=360)  # 30 years of monthly data
        >>> spi = compute_spi_climate_indices(
        ...     monthly_precip=precip,
        ...     data_start_year=1981,
        ...     calibration_start_year=1981,
        ...     calibration_end_year=2010,
        ...     scale_months=3,
        ...     distribution=Distribution.GAMMA,
        ... )
    """
    _check_climate_indices_available()
    
    # Validate inputs
    if not isinstance(monthly_precip, np.ndarray):
        monthly_precip = np.asarray(monthly_precip, dtype=np.float64)
    
    if monthly_precip.ndim != 1:
        raise ValueError(f"monthly_precip must be 1D, got shape {monthly_precip.shape}")
    
    if len(monthly_precip) < scale_months:
        raise ValueError(
            f"Insufficient data: {len(monthly_precip)} months for scale={scale_months}"
        )
    
    # Convert distribution enum
    ci_dist = _get_ci_distribution(distribution)
    
    # Call the climate-indices SPI function
    spi_values = ci_indices.spi(
        values=monthly_precip,
        scale=scale_months,
        distribution=ci_dist,
        data_start_year=data_start_year,
        calibration_year_initial=calibration_start_year,
        calibration_year_final=calibration_end_year,
        periodicity=ci_compute.Periodicity.monthly,
    )
    
    return spi_values


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
    
    This function handles the complexity of:
    - Extracting data start year from xarray time coordinates
    - Handling separate calibration data for SSP scenarios
    - Concatenating historical + scenario data when needed
    - Converting between xarray and numpy
    - Annualizing monthly SPI to yearly values
    
    Args:
        monthly_precip: DataArray of monthly precipitation totals (mm) with 'time' dim.
                        This is the data for which SPI will be computed.
        calibration_monthly_precip: DataArray for calibration period. If None or same
                                    as monthly_precip, calibration uses monthly_precip.
                                    For SSP scenarios, this should be historical data.
        baseline_years: Tuple of (start_year, end_year) for the calibration period.
        scale_months: SPI timescale (1, 3, 6, 12, etc.). Default is 3.
        distribution: Distribution for fitting. Default is GAMMA.
        min_months_per_year: Minimum months required for annual aggregation. Default 9.
        
    Returns:
        SPIResult containing monthly and annual SPI values, or None if computation fails.
        
    Example:
        >>> # For historical scenario (calibration on same data)
        >>> result = compute_spi_for_unit(
        ...     monthly_precip=hist_monthly,
        ...     calibration_monthly_precip=None,  # Use same data
        ...     baseline_years=(1981, 2010),
        ...     scale_months=3,
        ... )
        
        >>> # For SSP scenario (calibration on historical)
        >>> result = compute_spi_for_unit(
        ...     monthly_precip=ssp_monthly,
        ...     calibration_monthly_precip=hist_monthly,
        ...     baseline_years=(1981, 2010),
        ...     scale_months=3,
        ... )
    """
    _check_climate_indices_available()
    
    # Validate scenario data
    is_valid, error_msg = _validate_monthly_data(monthly_precip)
    if not is_valid:
        logger.warning(f"Invalid scenario data: {error_msg}")
        return None
    
    # Determine calibration data
    use_separate_calibration = (
        calibration_monthly_precip is not None 
        and calibration_monthly_precip is not monthly_precip
    )
    
    if use_separate_calibration:
        is_valid, error_msg = _validate_monthly_data(calibration_monthly_precip)
        if not is_valid:
            logger.warning(f"Invalid calibration data: {error_msg}. Using scenario data.")
            use_separate_calibration = False
    
    # Strategy: The climate-indices package computes SPI on a single array and
    # uses calibration_year_initial/final to determine which portion to use for
    # fitting the distribution. For SSP scenarios where we need to fit on historical
    # data, we concatenate: [historical | scenario] and set calibration years
    # to the historical portion.
    
    if use_separate_calibration:
        # Concatenate calibration (historical) + scenario data
        # Ensure no overlap by filtering
        calib_times = calibration_monthly_precip["time"]
        scen_times = monthly_precip["time"]
        
        # Get year ranges
        calib_start_year = _extract_year_from_time(calib_times[:1])
        scen_start_year = _extract_year_from_time(scen_times[:1])
        scen_end_year = _extract_year_from_time(scen_times[-1:])
        
        # Concatenate (calibration data first, then scenario)
        combined = xr.concat([calibration_monthly_precip, monthly_precip], dim="time")
        combined = combined.sortby("time")
        
        # Remove duplicates if any
        _, unique_idx = np.unique(combined["time"].values, return_index=True)
        combined = combined.isel(time=np.sort(unique_idx))
        
        data_start_year = _extract_year_from_time(combined["time"][:1])
        
        # Convert to numpy
        combined_values = combined.values.astype(np.float64)
        
        # Replace NaN with 0 (climate-indices expects this for missing precip)
        combined_values = np.nan_to_num(combined_values, nan=0.0)
        
    else:
        # Use scenario data for both calibration and computation
        combined = monthly_precip
        data_start_year = _extract_year_from_time(monthly_precip["time"][:1])
        scen_start_year = data_start_year
        scen_end_year = _extract_year_from_time(monthly_precip["time"][-1:])
        
        combined_values = monthly_precip.values.astype(np.float64)
        combined_values = np.nan_to_num(combined_values, nan=0.0)
    
    # Compute SPI
    try:
        spi_values = compute_spi_climate_indices(
            monthly_precip=combined_values,
            data_start_year=data_start_year,
            calibration_start_year=baseline_years[0],
            calibration_end_year=baseline_years[1],
            scale_months=scale_months,
            distribution=distribution,
        )
    except Exception as e:
        logger.warning(f"SPI computation failed: {e}")
        return None
    
    # Create monthly SPI DataArray with same time coordinates as combined
    spi_monthly = xr.DataArray(
        spi_values,
        coords={"time": combined["time"]},
        dims=["time"],
        name="spi",
    )
    
    # If we used separate calibration, extract only the scenario portion
    if use_separate_calibration:
        # Filter to scenario years only
        spi_monthly = spi_monthly.sel(
            time=spi_monthly["time"].dt.year >= scen_start_year
        )
    
    # Annualize: compute mean SPI per year with minimum months threshold
    spi_annual = _annualize_spi_xarray(spi_monthly, min_months_per_year)
    
    if spi_annual is None or spi_annual.sizes.get("year", 0) == 0:
        logger.warning("No valid annual SPI values after aggregation")
        return None
    
    # Count valid months
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
    
    Args:
        spi_monthly: DataArray with 'time' dimension containing monthly SPI
        min_months_per_year: Minimum valid months required for a year
        
    Returns:
        DataArray with 'year' dimension, or None if no valid years
    """
    if spi_monthly is None or spi_monthly.sizes.get("time", 0) == 0:
        return None
    
    # Group by year
    grouped = spi_monthly.groupby("time.year")
    
    # Count non-NaN values per year
    counts = grouped.count(dim="time")
    
    # Compute mean per year
    means = grouped.mean(dim="time", skipna=True)
    
    # Mask years with insufficient data
    means = means.where(counts >= min_months_per_year, drop=True)
    
    if means.sizes.get("year", 0) == 0:
        return None
    
    means.name = "spi_annual"
    return means


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
    year_to_paths: dict[int, dict[str, str]],
) -> list[dict]:
    """
    Compute SPI for all spatial units and return rows in pipeline format.
    
    This function is designed to be a drop-in replacement for the existing
    `_compute_spi_spei_rows()` function in `compute_indices_multiprocess.py`.
    
    Args:
        metric: Metric definition dict from metrics_registry
        model: Climate model name
        scenario: Scenario name (historical, ssp245, ssp585)
        scenario_conf: Scenario configuration with periods
        scen_monthly_by_unit: Dict mapping unit_key -> monthly precip DataArray for scenario
        calib_monthly_by_unit: Dict mapping unit_key -> monthly precip DataArray for calibration
        masks: Dict mapping unit_key -> spatial mask DataArray
        level: Admin level ("district" or "block")
        baseline_years: Tuple of (start, end) years for calibration
        scale_months: SPI timescale
        year_to_paths: Dict mapping year -> {varname: path} for source file tracking
        
    Returns:
        List of row dicts with keys: year, value, {value_col}, source_file, district, [block]
    """
    _check_climate_indices_available()
    
    slug = metric["slug"]
    value_col = metric["value_col"]
    varname = (metric.get("var") or "pr").strip()
    
    # Get distribution from params (default to gamma)
    dist_str = metric.get("params", {}).get("distribution", "gamma").lower()
    distribution = Distribution.PEARSON if dist_str == "pearson" else Distribution.GAMMA
    
    min_months_per_year = int(metric.get("params", {}).get("min_months_per_year", 9))
    
    rows: list[dict] = []
    
    for unit_key in masks.keys():
        scen_mon = scen_monthly_by_unit.get(unit_key)
        calib_mon = calib_monthly_by_unit.get(unit_key)
        
        # Determine if we need separate calibration
        use_separate_calib = (
            calib_mon is not None 
            and calib_mon is not scen_mon
            and scenario != "historical"
        )
        
        # Compute SPI
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
        
        spi_yearly = result.annual_spi
        
        if spi_yearly.sizes.get("year", 0) == 0:
            continue
        
        # Emit rows in the same schema as other metrics
        for y in spi_yearly["year"].values:
            y_int = int(y)
            v = float(spi_yearly.sel(year=y).item())
            
            # Get source file path
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
            
            # Parse unit_key based on level
            if level == "block":
                if "||" in unit_key:
                    district, block = unit_key.split("||", 1)
                    row["district"] = district
                    row["block"] = block
                else:
                    row["district"] = "Unknown"
                    row["block"] = unit_key
            else:
                row["district"] = unit_key
            
            rows.append(row)
    
    logger.info(
        f"[{slug}] Computed SPI ({distribution.value}, scale={scale_months}) "
        f"for {len(masks)} units, {len(rows)} rows"
    )
    
    return rows


# -----------------------------------------------------------------------------
# Utility functions for testing and validation
# -----------------------------------------------------------------------------

def compare_spi_implementations(
    monthly_precip: xr.DataArray,
    baseline_years: Tuple[int, int],
    scale_months: int = 3,
) -> dict:
    """
    Compare SPI values from climate-indices (gamma) vs (pearson).
    
    Useful for validating implementation and understanding distribution differences.
    
    Args:
        monthly_precip: Monthly precipitation DataArray
        baseline_years: Calibration period
        scale_months: SPI timescale
        
    Returns:
        Dict with 'gamma' and 'pearson' SPIResult objects and comparison stats
    """
    results = {}
    
    for dist in [Distribution.GAMMA, Distribution.PEARSON]:
        try:
            result = compute_spi_for_unit(
                monthly_precip=monthly_precip,
                calibration_monthly_precip=None,
                baseline_years=baseline_years,
                scale_months=scale_months,
                distribution=dist,
            )
            results[dist.value] = result
        except Exception as e:
            logger.warning(f"Failed to compute SPI with {dist.value}: {e}")
            results[dist.value] = None
    
    # Compute comparison statistics if both succeeded
    if results.get("gamma") and results.get("pearson"):
        gamma_annual = results["gamma"].annual_spi
        pearson_annual = results["pearson"].annual_spi
        
        # Align by year
        common_years = np.intersect1d(
            gamma_annual["year"].values,
            pearson_annual["year"].values
        )
        
        if len(common_years) > 0:
            g_vals = gamma_annual.sel(year=common_years).values
            p_vals = pearson_annual.sel(year=common_years).values
            
            results["comparison"] = {
                "correlation": float(np.corrcoef(g_vals, p_vals)[0, 1]),
                "mean_abs_diff": float(np.mean(np.abs(g_vals - p_vals))),
                "max_abs_diff": float(np.max(np.abs(g_vals - p_vals))),
                "common_years": len(common_years),
            }
    
    return results


if __name__ == "__main__":
    # Simple test to verify the module loads correctly
    print("SPI Adapter Module")
    print(f"climate-indices available: {CLIMATE_INDICES_AVAILABLE}")
    
    if CLIMATE_INDICES_AVAILABLE:
        print("Testing with synthetic data...")
        
        # Generate synthetic monthly precipitation (30 years)
        np.random.seed(42)
        n_months = 360  # 30 years
        precip = np.random.gamma(shape=2, scale=50, size=n_months)
        
        # Create time coordinate
        import pandas as pd
        times = pd.date_range("1981-01-01", periods=n_months, freq="MS")
        
        monthly_da = xr.DataArray(
            precip,
            coords={"time": times},
            dims=["time"],
            name="pr",
        )
        
        # Test compute_spi_for_unit
        result = compute_spi_for_unit(
            monthly_precip=monthly_da,
            calibration_monthly_precip=None,
            baseline_years=(1981, 2010),
            scale_months=3,
            distribution=Distribution.GAMMA,
        )
        
        if result:
            print(f"SPI computed successfully!")
            print(f"  Scale: {result.scale_months} months")
            print(f"  Distribution: {result.distribution.value}")
            print(f"  Calibration: {result.calibration_years}")
            print(f"  Data years: {result.data_years}")
            print(f"  Valid months: {result.valid_months}")
            print(f"  Annual SPI shape: {result.annual_spi.shape}")
            print(f"  Annual SPI range: [{float(result.annual_spi.min()):.2f}, {float(result.annual_spi.max()):.2f}]")
        else:
            print("SPI computation failed!")
    else:
        print("climate-indices package not installed. Run: pip install climate-indices")