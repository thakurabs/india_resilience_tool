"""
Comprehensive synthetic unit tests for IRT climate index compute functions.

This module validates the logical correctness of compute functions in
compute_indices_multiprocess.py using synthetic time-series data where the
expected answer is known (or can be made deterministic).

Design goals:
- Deterministic outcomes (avoid fragile percentile/tie behaviors).
- Validate tricky areas: percentiles, spells, precip percentiles, unit conversion,
  seasonal filtering, edge cases.
- Enforce bundle/registry/compute consistency.
- Enforce that every metric present in current bundles is exercised at least once
  (a smoke-coverage check), so metrics can't silently drift untested.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pytest
import xarray as xr

from india_resilience_tool.config.metrics_registry import PIPELINE_METRICS_RAW, get_pipeline_bundles


# =============================================================================
# TOLERANCES
# =============================================================================
TOL_TEMP = 1e-6       # °C-equivalent (differences in K are identical)
TOL_PCT = 1e-6        # Percentages/ratios
TOL_INDEX = 1e-6      # Simplified indices in this codebase
TOL_PRECIP = 1e-6     # mm


# =============================================================================
# MODULE LOADING (robust, relative to repo root)
# =============================================================================
def _repo_root() -> Path:
    """Find repository root (assumes tests/ is directly under repo root)."""
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.pipeline import compute_indices_multiprocess as CMP  # noqa: E402

# Hardening: ensure we imported the correct module file
assert Path(CMP.__file__).resolve() == (_ROOT / "tools" / "pipeline" / "compute_indices_multiprocess.py").resolve()


def _pipeline_by_slug() -> dict[str, dict[str, Any]]:
    return {m["slug"]: m for m in PIPELINE_METRICS_RAW if "slug" in m}


def _bundle_slugs() -> set[str]:
    slugs: set[str] = set()
    for _bname, items in get_pipeline_bundles().items():
        for s in items:
            slugs.add(s)
    return slugs


# =============================================================================
# SYNTHETIC DATA FACTORIES
# =============================================================================
def kelvin(celsius: float) -> float:
    return celsius + 273.15


def _cftime_daily_range(start_date: str, n_days: int) -> xr.CFTimeIndex:
    return xr.date_range(start_date, periods=n_days, freq="D", use_cftime=True)


def make_constant_series(
    value: float,
    n_days: int = 365,
    start_date: str = "2000-01-01",
    lat: list[float] | None = None,
    lon: list[float] | None = None,
    units: str | None = None,
) -> tuple[xr.DataArray, xr.DataArray]:
    lat = lat or [17.0, 17.5]
    lon = lon or [78.0, 78.5]
    time = _cftime_daily_range(start_date, n_days)

    data = xr.DataArray(
        np.full((n_days, len(lat), len(lon)), value, dtype=np.float64),
        coords={"time": time, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
    )
    if units is not None:
        data.attrs["units"] = units

    mask = xr.DataArray(
        np.ones((len(lat), len(lon)), dtype=bool),
        coords={"lat": lat, "lon": lon},
        dims=("lat", "lon"),
    )
    return data, mask


def make_step_series(
    values: list[float],
    days_per_value: list[int],
    start_date: str = "2000-01-01",
    lat: list[float] | None = None,
    lon: list[float] | None = None,
    units: str | None = None,
) -> tuple[xr.DataArray, xr.DataArray]:
    if len(values) != len(days_per_value):
        raise ValueError("values and days_per_value must have same length")

    lat = lat or [17.0, 17.5]
    lon = lon or [78.0, 78.5]
    n_days = sum(days_per_value)
    time = _cftime_daily_range(start_date, n_days)

    daily_values: list[float] = []
    for val, n in zip(values, days_per_value):
        daily_values.extend([val] * n)

    arr = np.array(daily_values, dtype=np.float64)
    data = xr.DataArray(
        np.broadcast_to(arr[:, None, None], (n_days, len(lat), len(lon))).copy(),
        coords={"time": time, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
    )
    if units is not None:
        data.attrs["units"] = units

    mask = xr.DataArray(
        np.ones((len(lat), len(lon)), dtype=bool),
        coords={"lat": lat, "lon": lon},
        dims=("lat", "lon"),
    )
    return data, mask


def make_ramp_series(
    start_value: float,
    end_value: float,
    n_days: int = 365,
    start_date: str = "2000-01-01",
    lat: list[float] | None = None,
    lon: list[float] | None = None,
    units: str | None = None,
) -> tuple[xr.DataArray, xr.DataArray]:
    lat = lat or [17.0, 17.5]
    lon = lon or [78.0, 78.5]
    time = _cftime_daily_range(start_date, n_days)

    daily_values = np.linspace(start_value, end_value, n_days).astype(np.float64)
    data = xr.DataArray(
        np.broadcast_to(daily_values[:, None, None], (n_days, len(lat), len(lon))).copy(),
        coords={"time": time, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
    )
    if units is not None:
        data.attrs["units"] = units

    mask = xr.DataArray(
        np.ones((len(lat), len(lon)), dtype=bool),
        coords={"lat": lat, "lon": lon},
        dims=("lat", "lon"),
    )
    return data, mask


def make_spike_series(
    base_value: float,
    spike_value: float,
    spike_indices: list[int],
    n_days: int = 365,
    start_date: str = "2000-01-01",
    lat: list[float] | None = None,
    lon: list[float] | None = None,
    units: str | None = None,
) -> tuple[xr.DataArray, xr.DataArray]:
    lat = lat or [17.0, 17.5]
    lon = lon or [78.0, 78.5]
    time = _cftime_daily_range(start_date, n_days)

    daily_values = np.full(n_days, base_value, dtype=np.float64)
    for idx in spike_indices:
        if 0 <= idx < n_days:
            daily_values[idx] = spike_value

    data = xr.DataArray(
        np.broadcast_to(daily_values[:, None, None], (n_days, len(lat), len(lon))).copy(),
        coords={"time": time, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
    )
    if units is not None:
        data.attrs["units"] = units

    mask = xr.DataArray(
        np.ones((len(lat), len(lon)), dtype=bool),
        coords={"lat": lat, "lon": lon},
        dims=("lat", "lon"),
    )
    return data, mask


def make_monthly_value_year(
    month_to_value: dict[int, float],
    start_date: str = "2000-01-01",
    n_days: int = 366,
    lat: list[float] | None = None,
    lon: list[float] | None = None,
    units: str | None = None,
) -> tuple[xr.DataArray, xr.DataArray]:
    """
    Create a year-like daily series where each month has a constant value.
    Default n_days=366 because 2000 is a leap year in CFTime calendars commonly used.
    """
    lat = lat or [17.0, 17.5]
    lon = lon or [78.0, 78.5]
    time = _cftime_daily_range(start_date, n_days)

    months = xr.DataArray(time, dims=("time",)).dt.month.values
    vals = np.array([month_to_value.get(int(m), np.nan) for m in months], dtype=np.float64)

    data = xr.DataArray(
        np.broadcast_to(vals[:, None, None], (n_days, len(lat), len(lon))).copy(),
        coords={"time": time, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
    )
    if units is not None:
        data.attrs["units"] = units

    mask = xr.DataArray(
        np.ones((len(lat), len(lon)), dtype=bool),
        coords={"lat": lat, "lon": lon},
        dims=("lat", "lon"),
    )
    return data, mask


# =============================================================================
# TIER A: REGISTRY / BUNDLE CONSISTENCY
# =============================================================================
class TestTierARegistryConsistency:
    def test_all_bundle_slugs_exist_in_pipeline_registry(self) -> None:
        pipeline = _pipeline_by_slug()
        missing: list[str] = []
        for bundle_name, slugs in get_pipeline_bundles().items():
            for slug in slugs:
                if slug not in pipeline:
                    missing.append(f"{bundle_name}: {slug}")
        assert not missing, "Bundle contains slugs missing from PIPELINE_METRICS_RAW:\n" + "\n".join(missing)

    def test_all_pipeline_metrics_have_compute_functions(self) -> None:
        missing_compute: list[str] = []
        for metric in PIPELINE_METRICS_RAW:
            slug = metric.get("slug", "(no slug)")
            compute_name = metric.get("compute")
            if not compute_name:
                missing_compute.append(f"{slug}: has no compute function defined")
                continue
            if getattr(CMP, compute_name, None) is None:
                missing_compute.append(f"{slug}: compute '{compute_name}' not found in compute_indices_multiprocess.py")
        assert not missing_compute, "Missing compute functions:\n" + "\n".join(missing_compute)

    def test_multi_var_metrics_declare_vars_list(self) -> None:
        """
        For any metric that declares 'vars', it must be a list/tuple and length >= 2.
        This avoids silent single-var execution of multi-var indices.
        """
        pipeline = _pipeline_by_slug()
        bad: list[str] = []
        for slug, metric in pipeline.items():
            if "vars" in metric:
                req = metric.get("vars")
                if not isinstance(req, (list, tuple)) or len(req) < 2:
                    bad.append(f"{slug}: vars={req!r}")
        assert not bad, "Invalid multi-var metric declarations:\n" + "\n".join(bad)


# =============================================================================
# TIER B: THRESHOLD-BASED DAY COUNTS (deterministic)
# =============================================================================
class TestTierBThresholdCounts:
    def test_count_days_above_threshold_all_above(self) -> None:
        data, mask = make_constant_series(kelvin(40), n_days=365)
        assert CMP.count_days_above_threshold(data, mask, thresh_k=kelvin(32)) == 365

    def test_count_days_above_threshold_none_above(self) -> None:
        data, mask = make_constant_series(kelvin(20), n_days=365)
        assert CMP.count_days_above_threshold(data, mask, thresh_k=kelvin(32)) == 0

    def test_count_days_above_threshold_boundary_strict(self) -> None:
        data, mask = make_constant_series(kelvin(32), n_days=365)
        assert CMP.count_days_above_threshold(data, mask, thresh_k=kelvin(32)) == 0

    def test_count_days_ge_threshold_boundary_inclusive(self) -> None:
        data, mask = make_constant_series(kelvin(30), n_days=365)
        assert CMP.count_days_ge_threshold(data, mask, thresh_k=kelvin(30)) == 365

    def test_count_days_below_threshold_boundary_strict(self) -> None:
        data, mask = make_constant_series(kelvin(0), n_days=365)
        assert CMP.count_days_below_threshold(data, mask, thresh_k=kelvin(0)) == 0


# =============================================================================
# TIER C: TEMPERATURE EXTREMES (deterministic)
# =============================================================================
class TestTierCTemperatureExtremes:
    def test_tx_x_constant(self) -> None:
        data, mask = make_constant_series(kelvin(30), n_days=365)
        assert abs(CMP.annual_max_temperature(data, mask) - 30.0) < TOL_TEMP

    def test_tn_n_with_dip(self) -> None:
        data, mask = make_spike_series(
            base_value=kelvin(15),
            spike_value=kelvin(-5),
            spike_indices=[30],
            n_days=365,
        )
        assert abs(CMP.annual_min_temperature(data, mask) - (-5.0)) < TOL_TEMP


# =============================================================================
# TIER D: PERCENTILE-BASED INDICES (deterministic, avoids interpolation fragility)
# =============================================================================
class TestTierDPercentileIndicesDeterministic:
    def test_percentile_days_above_90_is_10pct_in_bimodal(self) -> None:
        """
        90 days at 0°C, 10 days at 100°C.
        90th percentile is effectively the low mode; strict '>' should select 10 days => 10%.
        """
        data, mask = make_step_series(
            values=[kelvin(0), kelvin(100)],
            days_per_value=[90, 10],
            start_date="2000-01-01",
        )
        pct = CMP.percentile_days_above(data, mask, percentile=90)
        assert abs(pct - 10.0) < 1e-9

    def test_percentile_days_below_10_is_10pct_in_bimodal(self) -> None:
        """
        10 days at -50°C, 90 days at 20°C.
        10th percentile is effectively the low mode; strict '<' should select 10 days => 10%.
        """
        data, mask = make_step_series(
            values=[kelvin(-50), kelvin(20)],
            days_per_value=[10, 90],
            start_date="2000-01-01",
        )
        pct = CMP.percentile_days_below(data, mask, percentile=10)
        assert abs(pct - 10.0) < 1e-9

    def test_percentile_days_above_constant_is_zero(self) -> None:
        data, mask = make_constant_series(kelvin(25), n_days=100)
        assert CMP.percentile_days_above(data, mask, percentile=90) == 0.0

    def test_percentile_days_below_constant_is_zero(self) -> None:
        data, mask = make_constant_series(kelvin(25), n_days=100)
        assert CMP.percentile_days_below(data, mask, percentile=10) == 0.0


# =============================================================================
# TIER E: SPELL INDICES (deterministic, avoid threshold ambiguity)
# =============================================================================
class TestTierESpellIndicesDeterministic:
    def test_wsdi_single_spell_counted_exact(self) -> None:
        """
        Construct data so percentile threshold is the cool value:
        350 days at 20°C, then 15 days at 50°C contiguous.
        For pct=90, threshold is ~20°C; strict '>' selects the 15 hot days.
        With min_spell_days=6, the full 15-day spell should be counted.
        """
        data, mask = make_step_series(
            values=[kelvin(20), kelvin(50)],
            days_per_value=[350, 15],
        )
        res = CMP.warm_spell_duration_index(data, mask, percentile=90, min_spell_days=6)
        assert res == 15

    def test_csdi_single_spell_counted_exact(self) -> None:
        """
        15 cold days at -10°C contiguous, rest warm 25°C.
        For pct=10, threshold is ~25°C; strict '<' selects the 15 cold days.
        """
        data, mask = make_step_series(
            values=[kelvin(-10), kelvin(25)],
            days_per_value=[15, 350],
        )
        res = CMP.cold_spell_duration_index(data, mask, percentile=10, min_spell_days=6)
        assert res == 15

    def test_longest_consecutive_run_above_threshold(self) -> None:
        data, mask = make_step_series(
            values=[kelvin(35), kelvin(25), kelvin(35), kelvin(25)],
            days_per_value=[5, 5, 10, 345],
        )
        res = CMP.longest_consecutive_run_above_threshold(data, mask, thresh_k=kelvin(30), min_len=1)
        assert res == 10


# =============================================================================
# TIER F: MULTI-VARIABLE (DTR/ETR + wet-bulb) + unit sanity for wet-bulb threshold
# =============================================================================
class TestTierFMultiVariable:
    def test_dtr_constant_offset(self) -> None:
        tasmax, mask = make_constant_series(310.0, n_days=365)  # K
        tasmin, _ = make_constant_series(300.0, n_days=365)     # K
        res = CMP.daily_temperature_range(tasmax, tasmin, mask)
        assert abs(res - 10.0) < TOL_TEMP

    def test_etr_with_extremes(self) -> None:
        # Base: tasmax=300K, tasmin=290K; add one tasmax spike and one tasmin dip
        lat = [17.0, 17.5]
        lon = [78.0, 78.5]
        time = _cftime_daily_range("2000-01-01", 365)

        tasmax_vals = np.full(365, 300.0)
        tasmax_vals[100] = 320.0
        tasmin_vals = np.full(365, 290.0)
        tasmin_vals[200] = 270.0

        tasmax = xr.DataArray(
            np.broadcast_to(tasmax_vals[:, None, None], (365, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        tasmin = xr.DataArray(
            np.broadcast_to(tasmin_vals[:, None, None], (365, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        mask = xr.DataArray(
            np.ones((2, 2), dtype=bool),
            coords={"lat": lat, "lon": lon},
            dims=("lat", "lon"),
        )

        res = CMP.extreme_temperature_range(tasmax, tasmin, mask)
        assert abs(res - 50.0) < TOL_TEMP  # 320 - 270

    def test_wet_bulb_reference_point_stull(self) -> None:
        """
        Literature-style anchor point for Stull approximation.
        T=20°C, RH=50% -> Tw approx 13.7°C (allow approximation tolerance).
        """
        tas, mask = make_constant_series(kelvin(20), n_days=10)  # K
        hurs, _ = make_constant_series(50.0, n_days=10)          # %
        res = CMP.wet_bulb_annual_mean_stull(tas, hurs, mask)
        assert abs(res - 13.7) < 0.5

    def test_wet_bulb_days_ge_threshold_stull(self) -> None:
        """
        5 days very hot/humid (Tw well above 30), 5 days mild (Tw well below 30) -> 5 days.
        """
        lat = [17.0, 17.5]
        lon = [78.0, 78.5]
        time = _cftime_daily_range("2000-01-01", 10)

        tas_vals = np.concatenate([np.full(5, kelvin(35)), np.full(5, kelvin(20))])
        hurs_vals = np.concatenate([np.full(5, 90.0), np.full(5, 50.0)])

        tas = xr.DataArray(
            np.broadcast_to(tas_vals[:, None, None], (10, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        hurs = xr.DataArray(
            np.broadcast_to(hurs_vals[:, None, None], (10, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        mask = xr.DataArray(np.ones((2, 2), dtype=bool), coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))

        res = CMP.wet_bulb_days_ge_threshold_stull(tas, hurs, mask, thresh_c=30.0)
        assert res == 5

    def test_wet_bulb_depression_days_le_threshold_stull_monotonic(self) -> None:
        """
        Low depression (humid) days should increase monotonically with a looser threshold.
        """
        lat = [17.0, 17.5]
        lon = [78.0, 78.5]
        time = _cftime_daily_range("2000-01-01", 10)

        tas_vals = np.concatenate([np.full(5, kelvin(35)), np.full(5, kelvin(20))])
        hurs_vals = np.concatenate([np.full(5, 90.0), np.full(5, 50.0)])

        tas = xr.DataArray(
            np.broadcast_to(tas_vals[:, None, None], (10, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        hurs = xr.DataArray(
            np.broadcast_to(hurs_vals[:, None, None], (10, 2, 2)).copy(),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        mask = xr.DataArray(np.ones((2, 2), dtype=bool), coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))

        severe = CMP.wet_bulb_depression_days_le_threshold_stull(tas, hurs, mask, thresh_c=3.0)
        humid = CMP.wet_bulb_depression_days_le_threshold_stull(tas, hurs, mask, thresh_c=6.0)

        assert severe == 5
        assert humid == 5
        assert severe <= humid



# =============================================================================
# TIER G/H/I/J: PRECIP: units conversion + extremes + spells + percentiles (tight)
# =============================================================================
class TestTierGHIJPrecipitation:
    def _pr_kgm2s(self, mm_per_day: float) -> float:
        # 1 kg/m^2/s = 86400 mm/day
        return mm_per_day / 86400.0

    def test_pr_units_conversion_when_kgm2s(self) -> None:
        """
        If units are kg m-2 s-1, conversion should occur and counts should match mm/day logic.
        """
        data, mask = make_constant_series(self._pr_kgm2s(10.0), n_days=365, units="kg m-2 s-1")
        assert CMP.count_rainy_days(data, mask, thresh_mm=2.5) == 365
        assert abs(CMP.rx1day(data, mask) - 10.0) < TOL_PRECIP
        assert abs(CMP.rx5day(data, mask) - 50.0) < TOL_PRECIP

    def test_pr_no_conversion_when_already_mm_day(self) -> None:
        """
        If units indicate mm/day (or are absent), we should NOT apply kg/m^2/s conversion.
        """
        data, mask = make_constant_series(10.0, n_days=365, units="mm/day")
        # If code incorrectly converts, this would become 864000 mm/day and break tests.
        assert CMP.count_rainy_days(data, mask, thresh_mm=2.5) == 365
        assert abs(CMP.rx1day(data, mask) - 10.0) < TOL_PRECIP
        assert abs(CMP.rx5day(data, mask) - 50.0) < TOL_PRECIP

    def test_cwd_cdd_patterns(self) -> None:
        # 10 wet (>1mm), 5 dry, 20 wet, rest dry -> CWD = 20
        wet = self._pr_kgm2s(5.0)
        dry = self._pr_kgm2s(0.0)
        data, mask = make_step_series(values=[wet, dry, wet, dry], days_per_value=[10, 5, 20, 330], units="kg m-2 s-1")
        assert CMP.consecutive_wet_days(data, mask, wet_thresh_mm=1.0) == 20

        # 5 wet, 30 dry, 5 wet, 325 dry -> CDD = 325
        data2, mask2 = make_step_series(values=[wet, dry, wet, dry], days_per_value=[5, 30, 5, 325], units="kg m-2 s-1")
        assert CMP.consecutive_dry_days(data2, mask2, dry_thresh_mm=1.0) == 325

    def test_r95p_exact_bimodal(self) -> None:
        """
        Make percentile unambiguous:
        95 wet days at 1mm, 5 wet days at 100mm (all wet).
        95th percentile of wet days ~ 1mm. Strict '>' selects only 100mm days.
        Expected R95p = 5 * 100 = 500mm.
        """
        low = self._pr_kgm2s(1.0)
        high = self._pr_kgm2s(100.0)
        data, mask = make_step_series(values=[low, high], days_per_value=[95, 5], units="kg m-2 s-1")
        r95p = CMP.percentile_precipitation_total(data, mask, percentile=95)
        assert abs(r95p - 500.0) < TOL_PRECIP

    def test_sdii_exact(self) -> None:
        """
        100 days 20mm, 100 days 10mm, 165 days dry (0).
        SDII = (100*20 + 100*10) / 200 = 15 mm/day.
        """
        v1 = self._pr_kgm2s(20.0)
        v2 = self._pr_kgm2s(10.0)
        v0 = self._pr_kgm2s(0.0)
        data, mask = make_step_series(values=[v1, v2, v0], days_per_value=[100, 100, 165], units="kg m-2 s-1")
        sdii = CMP.simple_daily_intensity_index(data, mask, wet_day_thresh_mm=1.0)
        assert abs(sdii - 15.0) < TOL_PRECIP

    def test_prcptot_exact(self) -> None:
        """
        100 days at 10mm, 265 dry -> PRCPTOT = 1000mm (wet day thresh=1mm).
        """
        vwet = self._pr_kgm2s(10.0)
        vdry = self._pr_kgm2s(0.0)
        data, mask = make_step_series(values=[vwet, vdry], days_per_value=[100, 265], units="kg m-2 s-1")
        prcptot = CMP.total_wet_day_precipitation(data, mask, wet_thresh_mm=1.0)
        assert abs(prcptot - 1000.0) < TOL_PRECIP


# =============================================================================
# TIER K: DROUGHT INDICES (explicitly validate current simplified behavior)
# =============================================================================
class TestTierKSimplifiedDroughtIndices:
    def test_spi_simplified_constant_returns_zero(self) -> None:
        """
        This validates the simplified SPI implementation currently in the codebase.
        It is not a full literature-grade SPI validation.
        """
        data, mask = make_constant_series(5.0 / 86400.0, n_days=365, units="kg m-2 s-1")
        res = CMP.standardised_precipitation_index(data, mask, scale_months=3)
        assert abs(res) < TOL_INDEX

    def test_spei_simplified_matches_spi(self) -> None:
        data, mask = make_constant_series(5.0 / 86400.0, n_days=365, units="kg m-2 s-1")
        spi = CMP.standardised_precipitation_index(data, mask, scale_months=3)
        spei = CMP.standardised_precipitation_evapotranspiration_index(data, mask, scale_months=3)
        assert abs(spi - spei) < TOL_INDEX


# =============================================================================
# TIER L: SEASONAL FILTERING (must actually test month selection)
# =============================================================================
class TestTierLSeasonalFiltering:
    def test_seasonal_mean_djf_filters_months(self) -> None:
        """
        DJF should use Dec/Jan/Feb only.
        Build year with DJF=10°C and all other months=30°C.
        Winter mean should be 10°C, not 30°C.
        """
        month_vals_c = {m: 30.0 for m in range(1, 13)}
        for m in (12, 1, 2):
            month_vals_c[m] = 10.0

        data, mask = make_monthly_value_year(
            month_to_value={m: kelvin(v) for m, v in month_vals_c.items()},
            start_date="2000-01-01",
            n_days=366,
        )
        res = CMP.seasonal_mean(data, mask, months=[12, 1, 2])
        assert abs(res - 10.0) < TOL_TEMP

    def test_seasonal_mean_mam_filters_months(self) -> None:
        """
        MAM should use Mar/Apr/May only.
        Set MAM=35°C, other months=15°C; MAM mean should be 35°C.
        """
        month_vals_c = {m: 15.0 for m in range(1, 13)}
        for m in (3, 4, 5):
            month_vals_c[m] = 35.0

        data, mask = make_monthly_value_year(
            month_to_value={m: kelvin(v) for m, v in month_vals_c.items()},
            start_date="2000-01-01",
            n_days=366,
        )
        res = CMP.seasonal_mean(data, mask, months=[3, 4, 5])
        assert abs(res - 35.0) < TOL_TEMP


# =============================================================================
# TIER M: EDGE CASES / ROBUSTNESS
# =============================================================================
class TestTierMEdgeCases:
    def test_empty_data(self) -> None:
        lat = [17.0]
        lon = [78.0]
        time = _cftime_daily_range("2000-01-01", 0)

        data = xr.DataArray(
            np.array([]).reshape(0, 1, 1),
            coords={"time": time, "lat": lat, "lon": lon},
            dims=("time", "lat", "lon"),
        )
        mask = xr.DataArray(np.ones((1, 1), dtype=bool), coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))

        assert CMP.count_days_above_threshold(data, mask, thresh_k=300) == 0
        assert CMP.count_days_below_threshold(data, mask, thresh_k=300) == 0
        assert math.isnan(CMP.annual_max_temperature(data, mask))
        assert math.isnan(CMP.annual_min_temperature(data, mask))

    def test_nan_handling(self) -> None:
        lat = [17.0, 17.5]
        lon = [78.0, 78.5]
        time = _cftime_daily_range("2000-01-01", 10)

        vals = np.full((10, 2, 2), kelvin(30), dtype=np.float64)
        vals[5, :, :] = np.nan

        data = xr.DataArray(vals, coords={"time": time, "lat": lat, "lon": lon}, dims=("time", "lat", "lon"))
        mask = xr.DataArray(np.ones((2, 2), dtype=bool), coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))

        res = CMP.annual_max_temperature(data, mask)
        assert not math.isnan(res)
        assert abs(res - 30.0) < TOL_TEMP

    def test_all_masked(self) -> None:
        data, _ = make_constant_series(kelvin(30), n_days=10)
        lat = list(data["lat"].values)
        lon = list(data["lon"].values)
        mask = xr.DataArray(np.zeros((len(lat), len(lon)), dtype=bool), coords={"lat": lat, "lon": lon}, dims=("lat", "lon"))
        assert math.isnan(CMP.annual_max_temperature(data, mask))


# =============================================================================
# COVERAGE ENFORCEMENT: every bundle metric compute must be exercised at least once
# =============================================================================
@dataclass(frozen=True)
class _VarSpec:
    name: str
    kind: str  # "temp_k", "rh_pct", "pr_kgm2s"


def _var_spec(varname: str) -> _VarSpec:
    v = varname.lower()
    if v in {"tas", "tasmax", "tasmin"}:
        return _VarSpec(name=varname, kind="temp_k")
    if v in {"hurs"}:
        return _VarSpec(name=varname, kind="rh_pct")
    if v in {"pr"}:
        return _VarSpec(name=varname, kind="pr_kgm2s")
    # Default: treat unknowns as temperature-like to at least exercise signature
    return _VarSpec(name=varname, kind="temp_k")


def _make_data_for_var(spec: _VarSpec) -> tuple[xr.DataArray, xr.DataArray]:
    if spec.kind == "temp_k":
        return make_constant_series(kelvin(25), n_days=30)  # short is enough for smoke
    if spec.kind == "rh_pct":
        return make_constant_series(50.0, n_days=30)
    if spec.kind == "pr_kgm2s":
        # 10 mm/day in kg m-2 s-1
        return make_constant_series(10.0 / 86400.0, n_days=30, units="kg m-2 s-1")
    return make_constant_series(kelvin(25), n_days=30)


def _required_vars(metric: dict[str, Any]) -> list[str]:
    if isinstance(metric.get("vars"), (list, tuple)) and metric["vars"]:
        return list(metric["vars"])
    if metric.get("var"):
        return [str(metric["var"])]
    return []


def _call_compute_for_metric(metric: dict[str, Any]) -> None:
    """
    Exercise the compute function once with generic synthetic inputs.
    This is not a correctness proof by itself, but enforces that every bundle metric is at least executed.
    """
    compute_name = metric.get("compute")
    if not compute_name:
        raise AssertionError(f"Metric {metric.get('slug')} has no compute function name")

    fn = getattr(CMP, compute_name, None)
    if fn is None:
        raise AssertionError(f"Compute function '{compute_name}' not found for slug={metric.get('slug')}")

    params = metric.get("params", {}) or {}
    req_vars = _required_vars(metric)

    if len(req_vars) == 0:
        # Some metrics might not declare var; treat as a failure so it doesn't hide.
        raise AssertionError(f"Metric {metric.get('slug')} declares no var/vars")

    if len(req_vars) == 1:
        da, mask = _make_data_for_var(_var_spec(req_vars[0]))
        _ = fn(da, mask, **params)
    else:
        da1, mask1 = _make_data_for_var(_var_spec(req_vars[0]))
        da2, mask2 = _make_data_for_var(_var_spec(req_vars[1]))
        # Ensure same mask (they should be identical shapes)
        mask = mask1
        _ = fn(da1, da2, mask, **params)


class TestBundleMetricSmokeCoverage:
    def test_every_bundle_metric_is_exercised(self) -> None:
        """
        For each slug currently present in any bundle:
        - Ensure it exists in PIPELINE_METRICS_RAW
        - Ensure its compute can be called once with synthetic data without raising
        """
        pipeline = _pipeline_by_slug()
        slugs = sorted(_bundle_slugs())

        missing: list[str] = []
        failures: list[str] = []
        for slug in slugs:
            metric = pipeline.get(slug)
            if metric is None:
                missing.append(slug)
                continue
            try:
                _call_compute_for_metric(metric)
            except Exception as e:  # noqa: BLE001
                failures.append(f"{slug}: {type(e).__name__}: {e}")

        assert not missing, "Bundle slugs missing from registry:\n" + "\n".join(missing)
        assert not failures, "Bundle metric compute functions failed to execute (smoke):\n" + "\n".join(failures)


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "--tb=short"]))
