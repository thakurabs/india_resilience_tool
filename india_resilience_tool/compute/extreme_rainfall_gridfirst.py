"""Grid-first Extreme Rainfall | Flash Flood Risk v2 compute helpers."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import (
    aggregate_grid_values_with_retention,
    pr_to_mm_per_day_strict,
)
from india_resilience_tool.compute.gridfirst_spatial import (
    GridSpec,
    _hash_paths,
    coverage_from_weights,
    dataset_grid_spec,
    read_grid_metric_cache,
    read_spatial_weights_cache,
    write_grid_metric_cache,
    write_spatial_weights_cache,
)
from india_resilience_tool.compute.heat_risk_gridfirst import concat_years


EXTREME_RAINFALL_GRIDFIRST_METHOD_VERSION = "extreme-rainfall-v2-gridfirst-1"
EXTREME_RAINFALL_GRIDFIRST_SLUGS = frozenset(
    {
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "r20mm_very_heavy_precip_days",
        "r95p_very_wet_precip",
        "r95ptot_contribution_pct",
        "cwd_consecutive_wet_days",
        "pr_consecutive_dry_days_lt1mm",
    }
)
R95P_BASELINE_YEARS = (1990, 2010)
R95P_PERCENTILE = 95
R95P_WET_DAY_MM = 1.0
R95P_QUANTILE_METHOD = "linear"
R95P_STRICT_EXCEEDANCE = True
MIN_DAILY_COVERAGE_FRACTION = 0.90
MIN_POLYGON_CELL_WEIGHT_FRACTION = 0.50


def _drop_feb29(da: xr.DataArray) -> xr.DataArray:
    try:
        return da.sel(time=~((da["time"].dt.month == 2) & (da["time"].dt.day == 29)))
    except Exception:
        return da


def _annual_valid_mask(daily: xr.DataArray, *, min_fraction: float = MIN_DAILY_COVERAGE_FRACTION) -> xr.DataArray:
    """Return per-cell coverage mask using the loaded non-leap timestamps."""
    expected = int(daily.sizes.get("time", 0))
    if expected <= 0:
        return xr.zeros_like(daily.isel(time=0, drop=True), dtype=bool) if "time" in daily.dims else daily.notnull()
    required = int(math.ceil(float(min_fraction) * expected))
    return daily.notnull().sum(dim="time") >= required


def _rx5day_strict(daily: xr.DataArray) -> xr.DataArray:
    rolling = daily.rolling(time=5, min_periods=5).construct("window")
    valid_window = rolling.notnull().all(dim="window")
    sums = rolling.sum(dim="window", skipna=False)
    return sums.where(valid_window).max(dim="time", skipna=True)


def _cwd(daily: xr.DataArray) -> xr.DataArray:
    values = np.asarray(daily.values, dtype=float)
    out = np.zeros(values.shape[1:], dtype=float)
    for lat_i in range(values.shape[1]):
        for lon_i in range(values.shape[2]):
            run = 0
            best = 0
            for value in values[:, lat_i, lon_i]:
                if np.isfinite(value) and value >= 1.0:
                    run += 1
                    best = max(best, run)
                else:
                    run = 0
            out[lat_i, lon_i] = float(best)
    return xr.DataArray(out, coords={"lat": daily["lat"], "lon": daily["lon"]}, dims=("lat", "lon"))


def _cdd(daily: xr.DataArray) -> xr.DataArray:
    values = np.asarray(daily.values, dtype=float)
    out = np.zeros(values.shape[1:], dtype=float)
    for lat_i in range(values.shape[1]):
        for lon_i in range(values.shape[2]):
            run = 0
            best = 0
            for value in values[:, lat_i, lon_i]:
                if np.isfinite(value) and value < 1.0:
                    run += 1
                    best = max(best, run)
                else:
                    run = 0
            out[lat_i, lon_i] = float(best)
    return xr.DataArray(out, coords={"lat": daily["lat"], "lon": daily["lon"]}, dims=("lat", "lon"))


def annual_extreme_rainfall_grid(
    daily: xr.DataArray,
    *,
    slug: str,
    threshold: xr.DataArray | None = None,
) -> xr.Dataset:
    """Compute one annual cellwise Extreme Rainfall v2 metric grid."""
    daily = _drop_feb29(pr_to_mm_per_day_strict(daily))
    coverage_ok = _annual_valid_mask(daily)
    if slug == "pr_max_1day_precip":
        value = daily.max(dim="time", skipna=True)
    elif slug == "pr_max_5day_precip":
        value = _rx5day_strict(daily)
    elif slug == "r20mm_very_heavy_precip_days":
        value = (daily >= 20.0).where(daily.notnull(), False).sum(dim="time")
    elif slug == "cwd_consecutive_wet_days":
        value = _cwd(daily)
    elif slug == "pr_consecutive_dry_days_lt1mm":
        value = _cdd(daily)
    elif slug in {"r95p_very_wet_precip", "r95ptot_contribution_pct"}:
        if threshold is None:
            raise ValueError(f"{slug} requires baseline R95p threshold grid")
        exceed = daily > threshold
        r95p = daily.where(exceed & daily.notnull()).sum(dim="time", skipna=True)
        wet_total = daily.where((daily >= R95P_WET_DAY_MM) & daily.notnull()).sum(dim="time", skipna=True)
        wet_days = ((daily >= R95P_WET_DAY_MM) & daily.notnull()).sum(dim="time")
        if slug == "r95p_very_wet_precip":
            value = r95p
        else:
            value = xr.where(wet_days > 0, 100.0 * r95p / wet_total, 0.0)
        value = value.where(threshold.notnull())
    else:
        raise ValueError(f"Unsupported Extreme Rainfall grid-first slug: {slug}")
    return xr.Dataset({"value": value.where(coverage_ok)})


def compute_r95p_threshold_grid(
    baseline_daily: xr.DataArray,
    *,
    wet_day_mm: float = R95P_WET_DAY_MM,
    percentile: int = R95P_PERCENTILE,
    quantile_method: str = R95P_QUANTILE_METHOD,
) -> xr.Dataset:
    """Compute per-cell R95p wet-day thresholds from the admin v2 baseline."""
    daily = _drop_feb29(pr_to_mm_per_day_strict(baseline_daily))
    wet = daily.where(daily >= float(wet_day_mm))
    threshold = wet.quantile(float(percentile) / 100.0, dim="time", method=quantile_method, skipna=True)
    if "quantile" in threshold.dims:
        threshold = threshold.squeeze("quantile", drop=True)
    wet_counts = wet.notnull().sum(dim="time")
    threshold = threshold.where(wet_counts > 0)
    return xr.Dataset({"value": threshold})


def extreme_rainfall_grid_metric_cache_path(
    cache_root: Path,
    *,
    slug: str,
    model: str,
    grid_id: str,
    scenario: str,
    year: int,
) -> Path:
    """Return the annual Extreme Rainfall v2 grid-metric cache path."""
    return Path(cache_root) / "grid_metrics" / slug / model / grid_id / scenario / f"{int(year)}.nc"


def r95p_threshold_cache_path(cache_root: Path, *, model: str, grid_id: str) -> Path:
    """Return the R95p threshold cache path for the locked admin v2 baseline."""
    return Path(cache_root) / "thresholds" / model / grid_id / "baseline=1990-2010" / "p95.nc"


def _add_unit_fields(row: dict[str, object], *, level: str, unit_key: str) -> None:
    if level == "block" and "||" in unit_key:
        row["district"], row["block"] = unit_key.split("||", 1)
    elif level == "sub_basin" and "||" in unit_key:
        row["basin"], row["sub_basin"] = unit_key.split("||", 1)
    elif level == "basin":
        row["basin"] = unit_key
    else:
        row["district"] = unit_key


def _flatten_paths(year_to_paths: Mapping[int, Mapping[str, Path]], years: Sequence[int], var: str) -> list[Path]:
    return [year_to_paths[int(year)][var] for year in years if int(year) in year_to_paths and var in year_to_paths[int(year)]]


def _threshold_grid(
    *,
    model: str,
    grid_id: str,
    baseline_year_to_paths: Mapping[int, Mapping[str, Path]],
    var: str,
    cache_root: Path | None,
) -> xr.Dataset:
    baseline_years = [
        year for year in sorted(baseline_year_to_paths) if R95P_BASELINE_YEARS[0] <= int(year) <= R95P_BASELINE_YEARS[1]
    ]
    if not baseline_years:
        raise ValueError("R95p/R95pTOT admin v2 requires historical baseline files for 1990-2010")
    input_hashes = _hash_paths(_flatten_paths(baseline_year_to_paths, baseline_years, var))
    sidecar = {
        "method_version": EXTREME_RAINFALL_GRIDFIRST_METHOD_VERSION,
        "artifact_type": "r95p-threshold",
        "model": model,
        "scenario": "historical",
        "grid_id": grid_id,
        "baseline_years": [R95P_BASELINE_YEARS[0], R95P_BASELINE_YEARS[1]],
        "wet_day_mm": R95P_WET_DAY_MM,
        "quantile_method": R95P_QUANTILE_METHOD,
        "percentile": R95P_PERCENTILE,
        "input_file_hashes": input_hashes,
    }
    cache_path = r95p_threshold_cache_path(Path(cache_root), model=model, grid_id=grid_id) if cache_root is not None else None
    if cache_path is not None:
        cached = read_grid_metric_cache(cache_path, expected_sidecar=sidecar)
        if cached is not None:
            return cached
    baseline_da = concat_years(baseline_year_to_paths, var, baseline_years)
    ds = compute_r95p_threshold_grid(baseline_da)
    if cache_path is not None:
        write_grid_metric_cache(ds, cache_path, sidecar=sidecar)
    return ds


def compute_extreme_rainfall_rows_for_metric(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year_to_paths: Mapping[int, Mapping[str, Path]],
    baseline_year_to_paths: Mapping[int, Mapping[str, Path]],
    weights: pd.DataFrame,
    level: str = "district",
    cache_root: Path | None = None,
) -> list[dict[str, object]]:
    """Compute yearly admin v2 Extreme Rainfall rows.

    The entrypoint accepts registry-style metric mappings but intentionally
    ignores R95p/R95pTOT registry baseline, quantile, and exceedance params:
    admin v2 locks those semantics to 1990-2010, linear quantile, wet days
    >=1 mm/day, and strict exceedance. Hydro continues to use the legacy path.
    """
    slug = str(metric.get("slug") or "")
    if slug not in EXTREME_RAINFALL_GRIDFIRST_SLUGS:
        raise ValueError(f"Unsupported Extreme Rainfall grid-first slug: {slug}")
    var = str(metric.get("var") or "pr")
    value_col = str(metric.get("value_col") or "value")
    grid_id = str(dict(metric.get("params") or {}).get("grid_id") or "unknown-grid")
    needs_threshold = slug in {"r95p_very_wet_precip", "r95ptot_contribution_pct"}
    threshold_ds = None
    baseline_hashes: dict[str, str] | None = None
    if needs_threshold:
        if not baseline_year_to_paths:
            raise ValueError("R95p/R95pTOT admin v2 requires non-empty historical baseline paths")
        threshold_ds = _threshold_grid(
            model=model,
            grid_id=grid_id,
            baseline_year_to_paths=baseline_year_to_paths,
            var=var,
            cache_root=cache_root,
        )
        baseline_years = [
            year for year in sorted(baseline_year_to_paths) if R95P_BASELINE_YEARS[0] <= int(year) <= R95P_BASELINE_YEARS[1]
        ]
        baseline_hashes = _hash_paths(_flatten_paths(baseline_year_to_paths, baseline_years, var))

    rows: list[dict[str, object]] = []
    for year in sorted(year_to_paths):
        source_path = year_to_paths[int(year)][var]
        eval_hashes = _hash_paths([source_path])
        sidecar: dict[str, object] = {
            "method_version": EXTREME_RAINFALL_GRIDFIRST_METHOD_VERSION,
            "slug": slug,
            "model": model,
            "scenario": scenario,
            "year": int(year),
            "grid_id": grid_id,
            "eval_input_hash": eval_hashes,
        }
        if baseline_hashes is not None:
            sidecar["baseline_input_hash"] = baseline_hashes
        cache_path = (
            extreme_rainfall_grid_metric_cache_path(
                Path(cache_root),
                slug=slug,
                model=model,
                grid_id=grid_id,
                scenario=scenario,
                year=int(year),
            )
            if cache_root is not None
            else None
        )
        ds = read_grid_metric_cache(cache_path, expected_sidecar=sidecar) if cache_path is not None else None
        if ds is None:
            da = concat_years(year_to_paths, var, [int(year)])
            ds = annual_extreme_rainfall_grid(
                da,
                slug=slug,
                threshold=threshold_ds["value"] if threshold_ds is not None else None,
            )
            if cache_path is not None:
                write_grid_metric_cache(ds, cache_path, sidecar=sidecar)
        values = aggregate_grid_values_with_retention(
            ds["value"],
            weights,
            min_polygon_cell_weight_fraction=MIN_POLYGON_CELL_WEIGHT_FRACTION,
        )
        for unit_key, (value, retained) in values.items():
            row = {
                "year": int(year),
                "value": value,
                value_col: value,
                "source_file": str(source_path),
                "scenario": scenario,
                "retained_weight_fraction": retained,
            }
            _add_unit_fields(row, level=level, unit_key=unit_key)
            rows.append(row)
    return rows
