"""Grid-first Drought Risk v2 compute helpers."""

from __future__ import annotations

import calendar
import importlib.metadata
import json
import math
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.gridfirst_spatial import (
    _hash_paths,
    read_grid_metric_cache,
    write_grid_metric_cache,
)
from india_resilience_tool.compute.heat_risk_gridfirst import concat_years
from india_resilience_tool.compute.spi_adapter import Distribution, compute_spi_climate_indices


DROUGHT_GRIDFIRST_METHOD_VERSION = "drought-risk-v2-gridfirst-1"
DROUGHT_GRIDFIRST_SLUGS = frozenset(
    {
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "spi6_max_spell_lt_minus1",
        "spi12_max_spell_lt_minus1",
    }
)


def climate_indices_version() -> str:
    """Return the installed climate-indices version for cache sidecars."""
    try:
        return importlib.metadata.version("climate-indices")
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def pr_to_mm_per_day_strict(da: xr.DataArray) -> xr.DataArray:
    """Convert precipitation to mm/day with strict unit parsing."""
    units = str(getattr(da, "attrs", {}).get("units", "") or "").strip().lower().replace(" ", "")
    flux_units = {"kgm-2s-1", "kgm**-2s**-1", "kg/m2/s", "kg/m^2/s"}
    daily_units = {"mm/day", "mmd-1", "mm/d", "mm"}
    if units in flux_units:
        return da * 86400.0
    if units in daily_units:
        return da
    raise ValueError(f"Unsupported precipitation units for Drought Risk v2: {units or '<blank>'}")


def daily_to_monthly_totals(da: xr.DataArray, *, min_daily_coverage: float = 0.90) -> xr.DataArray:
    """Aggregate daily precipitation to monthly totals with a finite-day coverage floor."""
    daily = pr_to_mm_per_day_strict(da)
    totals = daily.resample(time="MS").sum(skipna=True)
    counts = daily.resample(time="MS").count()
    required = xr.DataArray(
        [math.ceil(min_daily_coverage * calendar.monthrange(int(t.dt.year), int(t.dt.month))[1]) for t in totals["time"]],
        coords={"time": totals["time"]},
        dims=("time",),
    )
    return totals.where(counts >= required)


def _to_contiguous_monthly_index(monthly: xr.DataArray) -> xr.DataArray:
    """Reindex monthly precipitation to a contiguous month-start time axis.

    Baseline and scenario inputs are routinely non-contiguous in calendar time.
    Without this reindex, climate-indices treats the concatenated array as if
    months were adjacent, corrupting both calendar labels and rolling windows.
    """
    if monthly.sizes.get("time", 0) == 0:
        return monthly
    times = pd.DatetimeIndex(pd.to_datetime(monthly["time"].values)).to_period("M").to_timestamp(how="start")
    monthly = monthly.assign_coords(time=times).sortby("time")
    _, unique_idx = np.unique(monthly["time"].values, return_index=True)
    monthly = monthly.isel(time=np.sort(unique_idx))
    full_index = pd.date_range(monthly["time"].values.min(), monthly["time"].values.max(), freq="MS")
    return monthly.reindex(time=full_index)


def _trim_to_full_calendar_years(monthly: xr.DataArray) -> xr.DataArray:
    """Trim a contiguous monthly series so climate-indices sees Jan-Dec years."""
    if monthly.sizes.get("time", 0) == 0:
        return monthly
    times = pd.DatetimeIndex(monthly["time"].values)
    start_year = int(times[0].year) + (1 if times[0].month != 1 else 0)
    end_year = int(times[-1].year) - (1 if times[-1].month != 12 else 0)
    if end_year < start_year:
        return monthly.isel(time=slice(0, 0))
    return monthly.sel(time=slice(f"{start_year}-01-01", f"{end_year}-12-01"))


def _compute_spi_series(
    monthly_precip_1d: np.ndarray,
    *,
    data_start_year: int,
    baseline_years: tuple[int, int],
    scale_months: int,
    distribution: Distribution,
) -> np.ndarray:
    """Compute one monthly SPI series with climate-indices, preserving NaN failure as all-NaN."""
    values = np.asarray(monthly_precip_1d, dtype=float)
    try:
        return compute_spi_climate_indices(
            monthly_precip=values,
            data_start_year=int(data_start_year),
            calibration_start_year=int(baseline_years[0]),
            calibration_end_year=int(baseline_years[1]),
            scale_months=int(scale_months),
            distribution=distribution,
        )
    except Exception:
        return np.full(values.shape, np.nan, dtype=float)


def _baseline_coverage_ok(
    values: np.ndarray,
    times: pd.DatetimeIndex,
    *,
    baseline_years: tuple[int, int],
    min_fraction: float,
) -> bool:
    years = np.arange(int(baseline_years[0]), int(baseline_years[1]) + 1)
    required = int(math.ceil(float(min_fraction) * len(years)))
    s = pd.Series(np.isfinite(values), index=times)
    for month in range(1, 13):
        mask = (s.index.year >= years[0]) & (s.index.year <= years[-1]) & (s.index.month == month)
        if int(s.loc[mask].sum()) < required:
            return False
    return True


def compute_spi_grid(
    monthly_precip: xr.DataArray,
    *,
    baseline_years: tuple[int, int] = (1981, 2010),
    scale_months: int = 3,
    distribution: Distribution = Distribution.GAMMA,
    min_baseline_years_per_calendar_month_fraction: float = 0.83,
) -> xr.DataArray:
    """Compute SPI per grid cell from a monthly precipitation cube."""
    monthly = _trim_to_full_calendar_years(_to_contiguous_monthly_index(monthly_precip))
    if monthly.sizes.get("time", 0) < int(scale_months):
        return xr.DataArray(
            np.empty((0,) + tuple(monthly_precip.shape[1:]), dtype=float),
            coords={"time": [], "lat": monthly_precip["lat"], "lon": monthly_precip["lon"]},
            dims=monthly_precip.dims,
            name="spi",
        )
    times = pd.DatetimeIndex(monthly["time"].values)
    data_start_year = int(times[0].year)
    vals = np.asarray(monthly.values, dtype=float)
    out = np.full(vals.shape, np.nan, dtype=float)
    for lat_i in range(vals.shape[1]):
        for lon_i in range(vals.shape[2]):
            series = vals[:, lat_i, lon_i]
            if not _baseline_coverage_ok(
                series,
                times,
                baseline_years=baseline_years,
                min_fraction=min_baseline_years_per_calendar_month_fraction,
            ):
                continue
            out[:, lat_i, lon_i] = _compute_spi_series(
                series,
                data_start_year=data_start_year,
                baseline_years=baseline_years,
                scale_months=scale_months,
                distribution=distribution,
            )
    return xr.DataArray(out, coords=monthly.coords, dims=monthly.dims, name="spi")


def _run_count_and_max(flags: np.ndarray, *, min_event_months: int = 1) -> tuple[int, int]:
    count = 0
    max_run = 0
    run = 0
    for flag in np.asarray(flags, dtype=bool):
        if flag:
            run += 1
            continue
        if run >= min_event_months:
            count += 1
            max_run = max(max_run, run)
        run = 0
    if run >= min_event_months:
        count += 1
        max_run = max(max_run, run)
    return count, max_run


def annual_spi_metric_grid(
    spi: xr.DataArray,
    *,
    annual_aggregation: str,
    threshold: float = -1.0,
    min_months_per_year: int = 9,
    min_event_months: int = 1,
) -> xr.Dataset:
    """Convert monthly SPI grid to annual event-count or max-spell grid metrics."""
    years: list[int] = []
    arrays: list[np.ndarray] = []
    for year, year_da in spi.groupby("time.year"):
        values = np.asarray(year_da.values, dtype=float)
        valid = np.isfinite(values)
        out = np.full(values.shape[1:], np.nan, dtype=float)
        for lat_i in range(values.shape[1]):
            for lon_i in range(values.shape[2]):
                if int(valid[:, lat_i, lon_i].sum()) < int(min_months_per_year):
                    continue
                flags = valid[:, lat_i, lon_i] & (values[:, lat_i, lon_i] < float(threshold))
                count, max_run = _run_count_and_max(flags, min_event_months=int(min_event_months))
                out[lat_i, lon_i] = float(count if annual_aggregation == "count_events_lt" else max_run)
        years.append(int(year))
        arrays.append(out)
    data = np.stack(arrays, axis=0) if arrays else np.empty((0,) + tuple(spi.shape[1:]), dtype=float)
    metric = xr.DataArray(data, coords={"year": years, "lat": spi["lat"], "lon": spi["lon"]}, dims=("year", "lat", "lon"))
    return xr.Dataset({"value": metric})


def period_rollup_grid(
    annual: xr.DataArray,
    *,
    period_name: str,
    years: tuple[int, int],
    rollup: str,
    min_years_per_period_fraction: float = 0.75,
) -> xr.Dataset:
    """Roll annual grid metrics to one period before polygon aggregation."""
    y0, y1 = int(years[0]), int(years[1])
    requested = y1 - y0 + 1
    required = int(math.ceil(float(min_years_per_period_fraction) * requested))
    subset = annual.sel(year=[y for y in annual["year"].values if y0 <= int(y) <= y1])
    counts = subset.count(dim="year")
    if rollup == "period_max":
        values = subset.max(dim="year", skipna=True)
    else:
        values = subset.mean(dim="year", skipna=True)
    values = values.where(counts >= required)
    return xr.Dataset({"value": values, "years_used_count": counts, "years_requested": xr.zeros_like(counts) + requested}).assign_attrs(period=period_name)


def aggregate_grid_values_with_retention(
    cell_values: xr.DataArray,
    weights: pd.DataFrame,
    *,
    min_polygon_cell_weight_fraction: float = 0.50,
) -> dict[str, tuple[float, float]]:
    """Area-weight cells to polygons, dropping NaNs and enforcing retained-weight floor."""
    if weights.empty:
        return {}
    flat = np.asarray(cell_values.values, dtype=float).reshape(-1)
    tmp = weights[["unit_key", "cell_index", "area_m2"]].copy()
    tmp["cell_value"] = flat[tmp["cell_index"].to_numpy(dtype=int)]
    out: dict[str, tuple[float, float]] = {}
    for unit, group in tmp.groupby("unit_key", sort=False):
        total = float(group["area_m2"].sum())
        finite = group[np.isfinite(group["cell_value"])]
        retained = float(finite["area_m2"].sum()) / total if total > 0 else 0.0
        if retained < float(min_polygon_cell_weight_fraction) or finite.empty:
            out[str(unit)] = (np.nan, retained)
            continue
        value = float((finite["area_m2"].astype(float) * finite["cell_value"].astype(float)).sum() / finite["area_m2"].sum())
        out[str(unit)] = (value, retained)
    return out


def aggregate_grid_counts(
    cell_counts: xr.DataArray,
    weights: pd.DataFrame,
    *,
    min_polygon_cell_weight_fraction: float = 0.50,
) -> dict[str, tuple[int | float, float]]:
    """Area-weight finite per-cell year counts to polygon-specific count metadata."""
    weighted = aggregate_grid_values_with_retention(
        cell_counts,
        weights,
        min_polygon_cell_weight_fraction=min_polygon_cell_weight_fraction,
    )
    out: dict[str, tuple[int | float, float]] = {}
    for unit, (value, retained) in weighted.items():
        out[unit] = (int(math.floor(value)) if np.isfinite(value) else np.nan, retained)
    return out


def drought_grid_metric_cache_path(
    cache_root: Path,
    *,
    slug: str,
    model: str,
    grid_id: str,
    distribution: Distribution,
    scenario: str,
    year: int | str,
) -> Path:
    """Return a distribution/grid-aware Drought Risk grid-metric cache path."""
    return (
        Path(cache_root)
        / "grid_metrics"
        / slug
        / model
        / grid_id
        / f"dist={distribution.value}"
        / scenario
        / f"{year}.nc"
    )


def drought_period_cache_path(
    cache_root: Path,
    *,
    slug: str,
    model: str,
    grid_id: str,
    distribution: Distribution,
    scenario: str,
    period: str,
) -> Path:
    """Return a distribution/grid-aware Drought Risk period grid cache path."""
    return (
        Path(cache_root)
        / "grid_metrics"
        / slug
        / model
        / grid_id
        / f"dist={distribution.value}"
        / scenario
        / "periods"
        / f"{period}.nc"
    )


def _add_unit_fields(row: dict[str, object], *, level: str, unit_key: str) -> None:
    if level == "block" and "||" in unit_key:
        row["district"], row["block"] = unit_key.split("||", 1)
    elif level == "sub_basin" and "||" in unit_key:
        row["basin"], row["sub_basin"] = unit_key.split("||", 1)
    elif level == "basin":
        row["basin"] = unit_key
    else:
        row["district"] = unit_key


def compute_drought_risk_rows_for_metric(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    scenario_conf: Mapping[str, object],
    year_to_paths: Mapping[int, Mapping[str, Path]],
    baseline_year_to_paths: Mapping[int, Mapping[str, Path]],
    weights: pd.DataFrame,
    level: str = "district",
    cache_root: Path | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Compute yearly and pre-rolled period rows for a Drought Risk v2 metric."""
    params = dict(metric.get("params") or {})
    slug = str(metric.get("slug") or "")
    value_col = str(metric.get("value_col") or "value")
    scale_months = int(params.get("scale_months", 3))
    baseline_years = tuple(int(v) for v in params.get("baseline_years", (1981, 2010)))
    distribution = Distribution(str(params.get("distribution", "gamma")))
    min_polygon_fraction = float(params.get("min_polygon_cell_weight_fraction", 0.50))
    grid_id = str(params.get("grid_id") or "unknown-grid")

    years_needed = sorted(set(baseline_year_to_paths) | set(year_to_paths))
    da = concat_years({**baseline_year_to_paths, **year_to_paths}, "pr", years_needed)
    monthly = daily_to_monthly_totals(da)
    spi = compute_spi_grid(
        monthly,
        baseline_years=baseline_years,
        scale_months=scale_months,
        distribution=distribution,
        min_baseline_years_per_calendar_month_fraction=float(params.get("min_baseline_years_per_calendar_month_fraction", 0.83)),
    )
    annual_ds = annual_spi_metric_grid(
        spi,
        annual_aggregation=str(params.get("annual_aggregation", "count_events_lt")),
        threshold=float(params.get("threshold", -1.0)),
        min_months_per_year=int(params.get("min_months_per_year", 9)),
        min_event_months=int(params.get("min_event_months", 1)),
    )
    input_file_hashes = _hash_paths([p for mapping in year_to_paths.values() for p in mapping.values()])

    if cache_root is not None:
        for year in annual_ds["year"].values:
            year_int = int(year)
            if year_int not in year_to_paths:
                continue
            sidecar = {
                "methodology_version": DROUGHT_GRIDFIRST_METHOD_VERSION,
                "climate_indices_version": climate_indices_version(),
                "slug": slug,
                "model": model,
                "grid_id": grid_id,
                "distribution": distribution.value,
                "scenario": scenario,
                "year": year_int,
                "input_file_hashes": input_file_hashes,
            }
            path = drought_grid_metric_cache_path(
                Path(cache_root),
                slug=slug,
                model=model,
                grid_id=grid_id,
                distribution=distribution,
                scenario=scenario,
                year=year_int,
            )
            existing = read_grid_metric_cache(path, expected_sidecar=sidecar)
            if existing is None:
                write_grid_metric_cache(annual_ds.sel(year=[year_int]), path, sidecar=sidecar)

    rows: list[dict[str, object]] = []
    for year in sorted(year_to_paths):
        if year not in set(int(y) for y in annual_ds["year"].values):
            continue
        values = aggregate_grid_values_with_retention(
            annual_ds["value"].sel(year=year),
            weights,
            min_polygon_cell_weight_fraction=min_polygon_fraction,
        )
        for unit_key, (value, retained) in values.items():
            row = {"year": int(year), "value": value, value_col: value, "retained_weight_fraction": retained}
            _add_unit_fields(row, level=level, unit_key=unit_key)
            rows.append(row)

    period_rows: list[dict[str, object]] = []
    for period_name, period_years in dict(scenario_conf.get("periods") or {}).items():
        rollup = str(params.get("period_rollup", "period_mean"))
        min_period_fraction = float(params.get("min_years_per_period_fraction", 0.75))
        period_sidecar = {
            "methodology_version": DROUGHT_GRIDFIRST_METHOD_VERSION,
            "climate_indices_version": climate_indices_version(),
            "slug": slug,
            "model": model,
            "grid_id": grid_id,
            "distribution": distribution.value,
            "scenario": scenario,
            "period": str(period_name),
            "period_years": [int(period_years[0]), int(period_years[1])],
            "rollup": rollup,
            "min_years_per_period_fraction": min_period_fraction,
            "input_file_hashes": input_file_hashes,
        }
        period_ds = None
        period_path = None
        if cache_root is not None:
            period_path = drought_period_cache_path(
                Path(cache_root),
                slug=slug,
                model=model,
                grid_id=grid_id,
                distribution=distribution,
                scenario=scenario,
                period=str(period_name),
            )
            period_ds = read_grid_metric_cache(period_path, expected_sidecar=period_sidecar)
        if period_ds is None:
            period_ds = period_rollup_grid(
                annual_ds["value"],
                period_name=str(period_name),
                years=tuple(period_years),
                rollup=rollup,
                min_years_per_period_fraction=min_period_fraction,
            )
            if period_path is not None:
                write_grid_metric_cache(period_ds, period_path, sidecar=period_sidecar)
        values = aggregate_grid_values_with_retention(
            period_ds["value"],
            weights,
            min_polygon_cell_weight_fraction=min_polygon_fraction,
        )
        year_counts = aggregate_grid_counts(
            period_ds["years_used_count"],
            weights,
            min_polygon_cell_weight_fraction=min_polygon_fraction,
        )
        for unit_key, (value, retained) in values.items():
            years_used, _count_retained = year_counts.get(unit_key, (np.nan, retained))
            row = {
                "period": str(period_name),
                "value": value,
                value_col: value,
                "years_used_count": years_used,
                "years_requested": int(np.nanmax(period_ds["years_requested"].values)),
                "retained_weight_fraction": retained,
            }
            _add_unit_fields(row, level=level, unit_key=unit_key)
            period_rows.append(row)
    return rows, period_rows
