"""Grid-first Heat Stress v2 compute helpers.

This module computes retained Heat Stress v2 indicators on climate grid cells
first, then area-weights those annual cell fields to admin polygons. It owns
only the Heat Stress-specific slugs; shared percentile metrics remain in the
Heat Risk v2 grid-first path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.gridfirst_spatial import (
    _hash_paths,
    assert_grid_matches,
    grid_metric_cache_path as _shared_grid_metric_cache_path,
    normalize_lat_lon,
    read_grid_metric_cache,
    subset_grid_by_index,
    write_grid_metric_cache,
)
from india_resilience_tool.compute.heat_risk_gridfirst import aggregate_cell_values


HEAT_STRESS_GRIDFIRST_METHOD_VERSION = "heat-stress-v2-gridfirst-2"
HEAT_STRESS_GRIDFIRST_SLUGS = frozenset(
    {
        "twb_annual_mean",
        "twb_summer_mean",
        "twb_annual_max",
        "twb_days_ge_28",
        "twb_days_ge_30",
        "tasmin_tropical_nights_gt28",
        # Heat Stress diagnostics (CHG-0012): grid-first WBGT/SWBGT.
        # Visible under Heat Stress, NOT scored in composite_heat_stress.
        "wbgt_shade_stull_annual_mean",
        "wbgt_shade_stull_days_ge_28",
        "wbgt_shade_stull_days_ge_30",
        "wbgt_shade_stull_days_ge_32",
        "swbgt_empirical_annual_mean",
        "swbgt_empirical_days_ge_28",
        "swbgt_empirical_days_ge_30",
        "swbgt_empirical_days_ge_32",
    }
)
WBGT_SHADE_DAY_COUNT_SLUGS = frozenset(
    {"wbgt_shade_stull_days_ge_28", "wbgt_shade_stull_days_ge_30", "wbgt_shade_stull_days_ge_32"}
)
SWBGT_EMPIRICAL_DAY_COUNT_SLUGS = frozenset(
    {"swbgt_empirical_days_ge_28", "swbgt_empirical_days_ge_30", "swbgt_empirical_days_ge_32"}
)
HEAT_STRESS_AGGREGATION_METHOD = "gridfirst_area_weighted_mean"


def stull_twb_c(tas_c: xr.DataArray | np.ndarray | float, hurs: xr.DataArray | np.ndarray | float):
    """Return Stull wet-bulb temperature in deg C for array-like inputs.

    Relative humidity may be supplied as either 0-1 or 0-100. Values are clipped
    to 0-100 before applying the Stull (2011) approximation.
    """

    rh = hurs
    try:
        rh_values = np.asarray(rh, dtype=float)
        if rh_values.size and np.nanmax(rh_values) <= 1.5:
            rh = rh * 100.0
    except (TypeError, ValueError, FloatingPointError):
        pass
    rh = np.clip(rh, 0.0, 100.0) if not isinstance(rh, xr.DataArray) else rh.clip(min=0.0, max=100.0)
    return (
        tas_c * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(tas_c + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * (rh**1.5) * np.arctan(0.023101 * rh)
        - 4.686035
    )


def wbgt_shade_stull_cell_c(
    tas_c: xr.DataArray | np.ndarray | float,
    hurs: xr.DataArray | np.ndarray | float,
):
    """Shaded WBGT (Stull form) per cell, in deg C.

    WBGT_shade = 0.7 * Twb_stull(tas_c, hurs) + 0.3 * tas_c.
    Operates element-wise; NaN inputs propagate as NaN.
    """

    twb_c = stull_twb_c(tas_c, hurs)
    return 0.7 * twb_c + 0.3 * tas_c


def swbgt_empirical_cell_c(
    tas_c: xr.DataArray | np.ndarray | float,
    hurs: xr.DataArray | np.ndarray | float,
):
    """Simplified outdoor sWBGT (ACSM/BoM form) per cell, in deg C.

    sWBGT = 0.567 * tas_c + 0.393 * e + 3.94, where e is actual vapour
    pressure in hPa computed via the Magnus form on tas_c with RH normalised
    to [0, 100]. NaN inputs propagate as NaN.
    """

    rh = hurs
    try:
        rh_values = np.asarray(rh, dtype=float)
        if rh_values.size and np.nanmax(rh_values) <= 1.5:
            rh = rh * 100.0
    except (TypeError, ValueError, FloatingPointError):
        pass
    rh = np.clip(rh, 0.0, 100.0) if not isinstance(rh, xr.DataArray) else rh.clip(min=0.0, max=100.0)
    es_hpa = 6.112 * np.exp((17.62 * tas_c) / (243.12 + tas_c))
    e_hpa = es_hpa * (rh / 100.0)
    return 0.567 * tas_c + 0.393 * e_hpa + 3.94


def heat_stress_grid_metric_cache_path(
    cache_root: Path,
    *,
    slug: str,
    model: str,
    grid_id: str,
    scenario: str,
    year: int | str,
) -> Path:
    """Return the Heat Stress v2 private annual grid metric cache path."""

    return _shared_grid_metric_cache_path(Path(cache_root), slug=slug, model=model, scenario=f"{grid_id}/{scenario}", year=year)


def _drop_feb29(da: xr.DataArray) -> xr.DataArray:
    if "time" not in da.coords:
        return da
    mask = ~((da["time"].dt.month == 2) & (da["time"].dt.day == 29))
    return da.sel(time=da["time"][mask])


def _open_year_dataarray(
    path: Path, var: str, *, index_range: tuple[int, int, int, int] | None = None
) -> xr.DataArray:
    ds = normalize_lat_lon(xr.open_dataset(path))
    try:
        if var not in ds:
            raise KeyError(f"Variable {var!r} not found in {path}")
        return subset_grid_by_index(ds[var], index_range).load()
    finally:
        ds.close()


def _concat_year_var(
    year_to_paths: Mapping[int, Mapping[str, Path]],
    var: str,
    years: Sequence[int],
    *,
    index_range: tuple[int, int, int, int] | None = None,
) -> xr.DataArray:
    arrays = [
        _open_year_dataarray(year_to_paths[int(year)][var], var, index_range=index_range)
        for year in years
        if int(year) in year_to_paths
    ]
    if not arrays:
        raise ValueError(f"No yearly files available for variable {var}")
    return xr.concat(arrays, dim="time").sortby("time")


def _twb_daily_for_year(
    year_to_paths: Mapping[int, Mapping[str, Path]],
    year: int,
    *,
    index_range: tuple[int, int, int, int] | None = None,
) -> xr.DataArray:
    tas_k = _drop_feb29(_concat_year_var(year_to_paths, "tas", [year], index_range=index_range))
    hurs = _drop_feb29(_concat_year_var(year_to_paths, "hurs", [year], index_range=index_range))
    return stull_twb_c(tas_k - 273.15, hurs)


def _cell_values_for_metric(
    metric: Mapping[str, object],
    year_to_paths: Mapping[int, Mapping[str, Path]],
    year: int,
    *,
    index_range: tuple[int, int, int, int] | None = None,
    grid: "GridSpec | None" = None,
) -> xr.DataArray:
    slug = str(metric.get("slug") or "")
    params = dict(metric.get("params") or {})

    if slug == "tasmin_tropical_nights_gt28":
        tasmin_raw = _concat_year_var(year_to_paths, "tasmin", [year], index_range=index_range)
        assert_grid_matches(tasmin_raw, grid, name=f"[{slug}] tasmin {year}")
        tasmin_c = _drop_feb29(tasmin_raw) - 273.15
        return (tasmin_c > 28.0).fillna(False).sum(dim="time").astype(float)

    twb = _twb_daily_for_year(year_to_paths, year, index_range=index_range)
    # twb is tas+hurs combined; a coord mismatch between the two subset
    # variables would have realigned to a smaller intersection grid, so this one
    # check guards both inputs at once.
    assert_grid_matches(twb, grid, name=f"[{slug}] twb {year}")
    if slug == "twb_annual_mean":
        return twb.mean(dim="time", skipna=True)
    if slug == "twb_annual_max":
        return twb.max(dim="time", skipna=True)
    if slug == "twb_summer_mean":
        if "months" not in params:
            raise ValueError("twb_summer_mean requires metric params['months']")
        months = [int(month) for month in params["months"]]
        summer = twb.sel(time=twb["time"].dt.month.isin(months))
        if summer.sizes.get("time", 0) == 0:
            return twb.isel(time=0, drop=True) * np.nan
        return summer.mean(dim="time", skipna=True)
    if slug in {"twb_days_ge_28", "twb_days_ge_30"}:
        threshold = float(params.get("thresh_c", 28.0 if slug == "twb_days_ge_28" else 30.0))
        return (twb >= threshold).fillna(False).sum(dim="time").astype(float)

    if slug.startswith("wbgt_shade_stull") or slug.startswith("swbgt_empirical"):
        tas_raw = _concat_year_var(year_to_paths, "tas", [year], index_range=index_range)
        hurs_raw = _concat_year_var(year_to_paths, "hurs", [year], index_range=index_range)
        assert_grid_matches(tas_raw, grid, name=f"[{slug}] tas {year}")
        assert_grid_matches(hurs_raw, grid, name=f"[{slug}] hurs {year}")
        tas_k = _drop_feb29(tas_raw)
        hurs = _drop_feb29(hurs_raw)
        tas_c = tas_k - 273.15
        if slug.startswith("wbgt_shade_stull"):
            field = wbgt_shade_stull_cell_c(tas_c, hurs)
        else:
            field = swbgt_empirical_cell_c(tas_c, hurs)
        if slug in {"wbgt_shade_stull_annual_mean", "swbgt_empirical_annual_mean"}:
            return field.mean(dim="time", skipna=True)
        if slug in WBGT_SHADE_DAY_COUNT_SLUGS or slug in SWBGT_EMPIRICAL_DAY_COUNT_SLUGS:
            if "thresh_c" not in params:
                raise ValueError(f"{slug} requires metric params['thresh_c']")
            threshold = float(params["thresh_c"])
            return (field >= threshold).fillna(False).sum(dim="time").astype(float)

    raise ValueError(f"Unsupported Heat Stress grid-first slug: {slug}")


def _jsonable(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _grid_sidecar(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year: int,
    grid_id: str,
    input_paths: Sequence[Path],
    value_col: str,
) -> dict[str, object]:
    return {
        "method_version": HEAT_STRESS_GRIDFIRST_METHOD_VERSION,
        "artifact_type": "annual-grid-first-metric",
        "slug": str(metric.get("slug") or ""),
        "model": model,
        "scenario": scenario,
        "year": int(year),
        "grid_id": grid_id,
        "value_col": value_col,
        "compute": str(metric.get("compute") or ""),
        "params": _jsonable(dict(metric.get("params") or {})),
        "input_file_hashes": _hash_paths(input_paths),
        "baseline": None,
        "methodology_note": (
            "Heat Stress v2 annual per-cell metric field before polygon aggregation; "
            "covers Twb, Shaded WBGT, and Outdoor sWBGT cell fields under method version "
            "heat-stress-v2-gridfirst-2."
        ),
    }


def _add_unit_fields(row: dict[str, object], unit_key: str, level: str) -> None:
    if level == "block" and "||" in unit_key:
        row["district"], row["block"] = unit_key.split("||", 1)
    elif level == "sub_basin" and "||" in unit_key:
        row["basin"], row["sub_basin"] = unit_key.split("||", 1)
    elif level == "basin":
        row["basin"] = unit_key
    else:
        row["district"] = unit_key


def compute_heat_stress_rows_for_metric(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year_to_paths: Mapping[int, Mapping[str, Path]],
    weights: pd.DataFrame,
    level: str = "district",
    cache_root: Path | None = None,
    index_range: tuple[int, int, int, int] | None = None,
    grid: "GridSpec | None" = None,
) -> list[dict[str, object]]:
    """Compute yearly Heat Stress v2 rows from per-cell indicators.

    Unknown registry params are ignored except where a metric explicitly
    requires them, such as ``twb_summer_mean`` requiring ``months``.

    ``index_range`` is the positional ``(lat0, lat1, lon0, lon1)`` bbox subset
    (per-state memory fix) forwarded to every variable load (tas, hurs, tasmin).
    ``grid`` is the matching subset ``GridSpec``; each loaded variable is
    validated against it and it gates aggregation. The grid-keyed cache path
    already isolates grids via ``params['grid_id']``. Both ``None`` reproduce
    the full-grid path.
    """

    slug = str(metric.get("slug") or "")
    if slug not in HEAT_STRESS_GRIDFIRST_SLUGS:
        raise ValueError(f"Unsupported Heat Stress grid-first slug: {slug}")
    value_col = str(metric.get("value_col") or "value")
    grid_id = str(dict(metric.get("params") or {}).get("grid_id") or "")
    rows: list[dict[str, object]] = []

    for year in sorted(year_to_paths):
        paths = list(year_to_paths[int(year)].values())
        sidecar = _grid_sidecar(
            metric=metric,
            model=model,
            scenario=scenario,
            year=int(year),
            grid_id=grid_id,
            input_paths=paths,
            value_col=value_col,
        )
        cache_path: Path | None = None
        grid_ds: xr.Dataset | None = None
        if cache_root is not None:
            cache_path = heat_stress_grid_metric_cache_path(
                Path(cache_root),
                slug=slug,
                model=model,
                grid_id=grid_id,
                scenario=scenario,
                year=int(year),
            )
            grid_ds = read_grid_metric_cache(cache_path, expected_sidecar=sidecar)
        if grid_ds is None:
            cell_values = _cell_values_for_metric(
                metric, year_to_paths, int(year), index_range=index_range, grid=grid
            )
            grid_ds = xr.Dataset({value_col: cell_values.rename(value_col)})
            if cache_path is not None:
                write_grid_metric_cache(grid_ds, cache_path, sidecar=sidecar)

        values = aggregate_cell_values(grid_ds[value_col], weights, grid=grid)
        for unit_key, value in values.items():
            row: dict[str, object] = {
                "year": int(year),
                "value": float(value) if np.isfinite(value) else np.nan,
                value_col: float(value) if np.isfinite(value) else np.nan,
                "source_file": json.dumps([str(path) for path in paths]),
                "method_version": HEAT_STRESS_GRIDFIRST_METHOD_VERSION,
                "aggregation_method": HEAT_STRESS_AGGREGATION_METHOD,
            }
            _add_unit_fields(row, str(unit_key), level)
            if scenario:
                row["scenario"] = scenario
            rows.append(row)
    return rows
