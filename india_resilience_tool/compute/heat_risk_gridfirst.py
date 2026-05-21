"""Grid-first Heat Risk v2 compute helpers.

This module computes Heat Risk indicators per climate grid cell before
area-weighted polygon aggregation. It intentionally has no Streamlit or
``tools`` imports so it can be unit-tested and reused by pipeline entrypoints.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from pyproj import Transformer, datadir
from shapely.geometry import box
from shapely.ops import transform as shapely_transform


HEAT_RISK_GRIDFIRST_SLUGS = frozenset(
    {
        "txx_annual_max",
        "txge35_extreme_heat_days",
        "tnx_annual_max",
        "tnle10_cold_nights",
        "tx90p_hot_days_pct",
        "tn90p_warm_nights_pct",
        "wsdi_warm_spell_days",
        "hwfi_tmean_90p",
        "hwfi_events_tmean_90p",
        "hwa_heatwave_amplitude",
    }
)

GRIDFIRST_METHOD_VERSION = "heat-risk-v2-gridfirst-1"
DEFAULT_ANALYSIS_CRS = "EPSG:6933"
DEFAULT_BASELINE_YEARS = (1990, 2010)
GRIDFIRST_BASELINE_THRESHOLD_COMPUTES = frozenset(
    {
        "tx90p_etccdi",
        "warm_spell_duration_index",
        "heatwave_frequency_percentile",
        "heatwave_event_count_percentile",
        "heatwave_amplitude",
    }
)


def _configure_pyproj_data_dir() -> None:
    """Point pyproj at conda's PROJ database when its bundled path is unusable."""
    candidates = [
        os.environ.get("PROJ_DATA"),
        os.environ.get("PROJ_LIB"),
    ]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(str(Path(conda_prefix) / "Library" / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "Library" / "share" / "proj"))

    for candidate in candidates:
        if not candidate:
            continue
        proj_db = Path(candidate) / "proj.db"
        if proj_db.exists():
            datadir.set_data_dir(str(proj_db.parent))
            return


_configure_pyproj_data_dir()


@dataclass(frozen=True)
class GridSpec:
    """Small immutable description of a regular lat/lon climate grid."""

    lat: tuple[float, ...]
    lon: tuple[float, ...]

    @property
    def shape(self) -> tuple[int, int]:
        return (len(self.lat), len(self.lon))

    @property
    def grid_id(self) -> str:
        payload = json.dumps(
            {
                "lat": [round(v, 8) for v in self.lat],
                "lon": [round(v, 8) for v in self.lon],
                "version": GRIDFIRST_METHOD_VERSION,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def normalize_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    """Normalize common climate coordinate names to ``lat`` and ``lon``."""

    ren: dict[str, str] = {}
    for cand in ("latitude", "y"):
        if cand in ds.dims or cand in ds.coords:
            ren[cand] = "lat"
    for cand in ("longitude", "x"):
        if cand in ds.dims or cand in ds.coords:
            ren[cand] = "lon"
    return ds.rename(ren) if ren else ds


def dataset_grid_spec(ds: xr.Dataset) -> GridSpec:
    """Return a ``GridSpec`` from a normalized xarray dataset."""

    ds = normalize_lat_lon(ds)
    if "lat" not in ds.coords or "lon" not in ds.coords:
        raise ValueError("Dataset must contain lat/lon coordinates")
    return GridSpec(
        lat=tuple(float(v) for v in np.asarray(ds["lat"].values, dtype=float)),
        lon=tuple(float(v) for v in np.asarray(ds["lon"].values, dtype=float)),
    )


def _coord_edges(values: Sequence[float]) -> np.ndarray:
    vals = np.asarray(values, dtype=float)
    if vals.size == 0:
        raise ValueError("Grid coordinate is empty")
    if vals.size == 1:
        step = 0.5
        return np.asarray([vals[0] - step, vals[0] + step], dtype=float)
    mids = (vals[:-1] + vals[1:]) / 2.0
    first = vals[0] - (mids[0] - vals[0])
    last = vals[-1] + (vals[-1] - mids[-1])
    return np.concatenate([[first], mids, [last]]).astype(float)


def _unit_key(row: pd.Series, level: str) -> str:
    if level == "block":
        district = str(row.get("district_name") or row.get("District") or row.get("DISTRICT") or "").strip()
        block = str(row.get("block_name") or row.get("Sub_dist") or row.get("BLOCK") or "").strip()
        return f"{district}||{block}" if district else block
    if level == "sub_basin":
        basin = str(row.get("basin_name") or "").strip()
        sub = str(row.get("subbasin_name") or "").strip()
        return f"{basin}||{sub}" if basin else sub
    if level == "basin":
        return str(row.get("basin_name") or "").strip()
    return str(row.get("district_name") or row.get("DISTRICT") or row.get("District") or "").strip()


def boundary_content_hash(boundary_path: Path) -> str:
    """Return a stable hash for a boundary file used in spatial-weight sidecars."""

    path = Path(boundary_path)
    h = hashlib.sha256()
    if path.is_file():
        h.update(path.read_bytes())
    else:
        h.update(str(path).encode("utf-8"))
        if path.exists():
            h.update(str(path.stat().st_mtime_ns).encode("ascii"))
    return h.hexdigest()


# `input_file_signature` moved to ``gridfirst_spatial``.


def build_area_weights(
    gdf: gpd.GeoDataFrame,
    grid: GridSpec,
    *,
    level: str = "district",
    analysis_crs: str = DEFAULT_ANALYSIS_CRS,
) -> pd.DataFrame:
    """Build sparse area-overlap weights for polygons and climate grid cells."""

    if gdf.empty:
        return pd.DataFrame(columns=["unit_key", "cell_index", "lat_index", "lon_index", "area_m2"])

    source = gdf.copy()
    if source.crs is None:
        source = source.set_crs("EPSG:4326")
    units = source.to_crs(analysis_crs)
    to_analysis = Transformer.from_crs("EPSG:4326", analysis_crs, always_xy=True).transform

    lat_edges = _coord_edges(grid.lat)
    lon_edges = _coord_edges(grid.lon)
    cell_rows: list[tuple[int, int, int, object]] = []
    for i in range(len(grid.lat)):
        y0, y1 = sorted((float(lat_edges[i]), float(lat_edges[i + 1])))
        for j in range(len(grid.lon)):
            x0, x1 = sorted((float(lon_edges[j]), float(lon_edges[j + 1])))
            geom = shapely_transform(to_analysis, box(x0, y0, x1, y1))
            cell_rows.append((i * len(grid.lon) + j, i, j, geom))

    rows: list[dict[str, object]] = []
    for _, row in units.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        unit_key = _unit_key(row, level)
        if not unit_key:
            continue
        for cell_index, lat_index, lon_index, cell_geom in cell_rows:
            if not geom.intersects(cell_geom):
                continue
            inter = geom.intersection(cell_geom)
            area = float(inter.area) if not inter.is_empty else 0.0
            if area <= 0.0:
                continue
            rows.append(
                {
                    "unit_key": unit_key,
                    "cell_index": int(cell_index),
                    "lat_index": int(lat_index),
                    "lon_index": int(lon_index),
                    "area_m2": area,
                }
            )
    return pd.DataFrame(rows)


def write_spatial_weights_cache(
    weights: pd.DataFrame,
    *,
    output_path: Path,
    grid: GridSpec,
    level: str,
    boundary_path: Path | None = None,
    analysis_crs: str = DEFAULT_ANALYSIS_CRS,
) -> None:
    """Write spatial weights and a JSON sidecar for cache validation."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    weights.to_parquet(output_path, index=False)
    sidecar = {
        "method_version": GRIDFIRST_METHOD_VERSION,
        "grid_id": grid.grid_id,
        "level": level,
        "crs_epsg": int(str(analysis_crs).split(":")[-1]),
        "boundary_file_hash": boundary_content_hash(boundary_path) if boundary_path else None,
    }
    output_path.with_suffix(output_path.suffix + ".json").write_text(
        json.dumps(sidecar, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def read_spatial_weights_cache(
    path: Path,
    *,
    grid: GridSpec,
    level: str,
    boundary_path: Path | None = None,
    analysis_crs: str = DEFAULT_ANALYSIS_CRS,
) -> pd.DataFrame | None:
    """Read a spatial-weight cache when its sidecar matches the requested grid."""

    path = Path(path)
    sidecar_path = path.with_suffix(path.suffix + ".json")
    if not path.exists() or not sidecar_path.exists():
        return None
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    expected_boundary_hash = boundary_content_hash(boundary_path) if boundary_path else None
    if sidecar.get("method_version") != GRIDFIRST_METHOD_VERSION:
        return None
    if sidecar.get("grid_id") != grid.grid_id or sidecar.get("level") != level:
        return None
    if int(sidecar.get("crs_epsg", -1)) != int(str(analysis_crs).split(":")[-1]):
        return None
    if boundary_path and sidecar.get("boundary_file_hash") != expected_boundary_hash:
        return None
    return pd.read_parquet(path)


# `_drop_feb29`, `_noleap_doy`, `_quantile`, `compute_doy_thresholds`,
# `threshold_cache_path`, `read_threshold_cache`, `write_threshold_cache`,
# `_run_lengths`, `aggregate_cell_values`, `aggregate_percent_days`,
# `aggregate_daily_area_mean`, `open_year_dataarray`, `concat_years`,
# `input_file_signature`, and the four `_cellwise_*` helpers were moved to
# ``gridfirst_spatial`` so Cold Risk v2 can share them. They are re-exported
# at the bottom of this module for back-compat with existing callers (Drought,
# Extreme Rainfall, Heat Stress).


def grid_metric_cache_path(cache_root: Path, *, slug: str, model: str, scenario: str, year: int) -> Path:
    """Return the private annual grid-first metric NetCDF path."""

    return Path(cache_root) / "grid_metrics" / slug / model / scenario / f"{int(year)}.nc"


def _jsonable(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _grid_metric_sidecar(
    *,
    metric: Mapping[str, object],
    model: str,
    scenario: str,
    year: int,
    eval_input_signature: str,
    baseline_input_signature: str | None,
    baseline_years: tuple[int, int],
    percentile: int,
    window_days: int,
    quantile_method: str,
    value_col: str,
) -> dict[str, object]:
    return {
        "method_version": GRIDFIRST_METHOD_VERSION,
        "artifact_type": "annual-grid-first-metric",
        "slug": str(metric.get("slug") or ""),
        "model": model,
        "scenario": scenario,
        "year": int(year),
        "var": str(metric.get("var") or ""),
        "value_col": value_col,
        "compute": str(metric.get("compute") or ""),
        "params": _jsonable(dict(metric.get("params") or {})),
        "eval_input_hash": eval_input_signature,
        "baseline_input_hash": baseline_input_signature,
        "baseline_years": list(baseline_years),
        "percentile": int(percentile),
        "window_days": int(window_days),
        "quantile_method": quantile_method,
        "methodology_note": "Heat Risk v2 annual per-cell metric field before polygon aggregation",
    }


# `read_threshold_cache` / `write_threshold_cache` moved to ``gridfirst_spatial``.


def read_grid_metric_cache(path: Path, *, expected_sidecar: Mapping[str, object]) -> xr.Dataset | None:
    """Read an annual grid-first metric cache when its sidecar matches inputs."""

    path = Path(path)
    sidecar_path = path.with_suffix(path.suffix + ".json")
    if not path.exists() or not sidecar_path.exists():
        return None
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    for key, value in expected_sidecar.items():
        if sidecar.get(key) != value:
            return None
    ds = xr.open_dataset(path)
    try:
        return ds.load()
    finally:
        ds.close()


def write_grid_metric_cache(
    ds: xr.Dataset,
    path: Path,
    *,
    sidecar: Mapping[str, object],
) -> None:
    """Write an annual grid-first metric field and invalidation sidecar."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps(dict(sidecar), indent=2, sort_keys=True),
        encoding="utf-8",
    )


# All cellwise/spatial primitives now live in ``gridfirst_spatial``.


def coverage_from_weights(gdf: gpd.GeoDataFrame, weights: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Build coverage-QC rows from the sparse area weights."""

    rows: list[dict[str, object]] = []
    area_by_unit = weights.groupby("unit_key")["area_m2"].sum().to_dict() if not weights.empty else {}
    units_6933 = gdf.to_crs(DEFAULT_ANALYSIS_CRS) if gdf.crs is not None else gdf.set_crs("EPSG:4326").to_crs(DEFAULT_ANALYSIS_CRS)
    for _, row in units_6933.iterrows():
        key = _unit_key(row, level)
        geom_area = float(row.geometry.area) if row.geometry is not None and not row.geometry.is_empty else 0.0
        covered = float(area_by_unit.get(key, 0.0))
        frac = covered / geom_area if geom_area > 0 else 0.0
        qc: dict[str, object] = {
            "unit_key": key,
            "coverage_fraction": frac,
            "coverage_ok": frac > 0.0,
            "coverage_threshold": 0.0,
            "covered_cells": int((weights["unit_key"] == key).sum()) if not weights.empty else 0,
            "total_cells": int((weights["unit_key"] == key).sum()) if not weights.empty else 0,
            "eligible_for_processing": frac > 0.0,
            "overlap_area_m2": covered,
            "polygon_area_m2": geom_area,
        }
        if level == "block":
            if "||" in key:
                qc["district"], qc["block"] = key.split("||", 1)
        elif level == "district":
            qc["district"] = key
        elif level == "basin":
            qc["basin_name"] = key
        elif level == "sub_basin" and "||" in key:
            qc["basin_name"], qc["subbasin_name"] = key.split("||", 1)
        rows.append(qc)
    return pd.DataFrame(rows)


def _metric_cell_values(
    *,
    metric: Mapping[str, object],
    eval_da: xr.DataArray,
    threshold: xr.DataArray | None,
) -> xr.DataArray | tuple[xr.DataArray, xr.DataArray]:
    compute = str(metric.get("compute") or "")
    params = dict(metric.get("params") or {})
    exceed_ge = bool(params.get("exceed_ge", False))
    min_spell_days = int(params.get("min_spell_days", 5))

    if compute == "annual_max_temperature":
        return eval_da.max(dim="time", skipna=True) - 273.15
    if compute in {"count_days_ge_threshold", "count_days_above_threshold"}:
        thresh_k = float(params["thresh_k"])
        comparator = eval_da >= thresh_k if compute == "count_days_ge_threshold" else eval_da > thresh_k
        return comparator.where(eval_da.notnull()).sum(dim="time", skipna=True)
    if compute in {"count_days_le_threshold", "count_days_below_threshold"}:
        thresh_k = float(params["thresh_k"])
        comparator = eval_da <= thresh_k if compute == "count_days_le_threshold" else eval_da < thresh_k
        return comparator.where(eval_da.notnull()).sum(dim="time", skipna=True)
    if compute == "tx90p_etccdi":
        if threshold is None:
            raise ValueError("tx90p_etccdi requires threshold")
        direction = str(params.get("direction", "above"))
        return _cellwise_percent_days(eval_da, threshold, exceed_ge=exceed_ge, direction=direction)
    if compute == "warm_spell_duration_index":
        if threshold is None:
            raise ValueError("warm_spell_duration_index requires threshold")
        return _cellwise_spell_days(eval_da, threshold, min_spell_days=min_spell_days, exceed_ge=exceed_ge)
    if compute == "heatwave_frequency_percentile":
        if threshold is None:
            raise ValueError("heatwave_frequency_percentile requires threshold")
        return _cellwise_spell_days(eval_da, threshold, min_spell_days=min_spell_days, exceed_ge=exceed_ge)
    if compute == "heatwave_event_count_percentile":
        if threshold is None:
            raise ValueError("heatwave_event_count_percentile requires threshold")
        return _cellwise_spell_events(eval_da, threshold, min_spell_days=min_spell_days, exceed_ge=exceed_ge)
    if compute == "heatwave_amplitude":
        if threshold is None:
            raise ValueError("heatwave_amplitude requires threshold")
        return _cellwise_heatwave_amplitude(eval_da, threshold, min_spell_days=min_spell_days, exceed_ge=exceed_ge)
    raise ValueError(f"Unsupported Heat Risk grid-first compute: {compute}")


def _grid_metric_dataset(
    cell_payload: xr.DataArray | tuple[xr.DataArray, xr.DataArray],
    *,
    value_col: str,
) -> xr.Dataset:
    """Convert annual cellwise metric payloads into an inspectable dataset."""

    if isinstance(cell_payload, tuple):
        exceed_days, valid_days = cell_payload
        values = xr.where(valid_days > 0, 100.0 * exceed_days / valid_days, np.nan)
        return xr.Dataset(
            {
                value_col: values.rename(value_col),
                "exceed_days": exceed_days.rename("exceed_days"),
                "valid_days": valid_days.rename("valid_days"),
            }
        )
    return xr.Dataset({value_col: cell_payload.rename(value_col)})


def _aggregate_grid_metric_dataset(ds: xr.Dataset, *, value_col: str, weights: pd.DataFrame) -> dict[str, float]:
    if "exceed_days" in ds and "valid_days" in ds:
        return aggregate_percent_days(ds["exceed_days"], ds["valid_days"], weights)
    return aggregate_cell_values(ds[value_col], weights)


def compute_heat_risk_rows_for_metric(
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
    """Compute yearly Heat Risk rows from per-cell indicators and area weights.

    Unknown registry params are ignored intentionally: this entrypoint consumes
    only baseline/percentile/window/operator/spell settings relevant to v2.
    """

    var = str(metric.get("var") or "")
    if not var:
        raise ValueError(f"Metric {metric.get('slug')} has no variable")
    value_col = str(metric.get("value_col") or "value")
    params = dict(metric.get("params") or {})
    baseline_years = tuple(int(v) for v in params.get("baseline_years", DEFAULT_BASELINE_YEARS))
    percentile = int(params.get("percentile", params.get("pct", 90)))
    window_days = int(params.get("window_days", 5))
    quantile_method = str(params.get("quantile_method", "linear"))
    smooth = params.get("smooth")
    smooth_int = int(smooth) if smooth is not None else None
    compute = str(metric.get("compute") or "")

    baseline_input_sig: str | None = None
    threshold: xr.DataArray | None = None
    if compute in GRIDFIRST_BASELINE_THRESHOLD_COMPUTES:
        baseline_years_available = [
            year for year in sorted(baseline_year_to_paths) if baseline_years[0] <= int(year) <= baseline_years[1]
        ]
        baseline_da = concat_years(baseline_year_to_paths, var, baseline_years_available)
        baseline_input_sig = input_file_signature([baseline_year_to_paths[year][var] for year in baseline_years_available])
        cache_path: Path | None = None
        if cache_root is not None:
            cache_path = threshold_cache_path(
                Path(cache_root),
                model=model,
                var=var,
                baseline_label=f"{baseline_years[0]}-{baseline_years[1]}",
            )
            threshold = read_threshold_cache(
                cache_path,
                input_signature=baseline_input_sig,
                baseline_years=baseline_years,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
            )
        if threshold is None:
            threshold = compute_doy_thresholds(
                baseline_da,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                smooth=smooth_int,
            )
            if cache_path is not None:
                write_threshold_cache(
                    threshold,
                    cache_path,
                    input_signature=baseline_input_sig,
                    baseline_years=baseline_years,
                    percentile=percentile,
                    window_days=window_days,
                    quantile_method=quantile_method,
                )

    rows: list[dict[str, object]] = []
    for year in sorted(year_to_paths):
        eval_input_sig = input_file_signature([year_to_paths[int(year)][var]])
        grid_ds: xr.Dataset | None = None
        grid_cache_path: Path | None = None
        grid_sidecar: dict[str, object] | None = None
        if cache_root is not None:
            grid_cache_path = grid_metric_cache_path(
                Path(cache_root),
                slug=str(metric.get("slug") or ""),
                model=model,
                scenario=scenario,
                year=int(year),
            )
            grid_sidecar = _grid_metric_sidecar(
                metric=metric,
                model=model,
                scenario=scenario,
                year=int(year),
                eval_input_signature=eval_input_sig,
                baseline_input_signature=baseline_input_sig,
                baseline_years=baseline_years,
                percentile=percentile,
                window_days=window_days,
                quantile_method=quantile_method,
                value_col=value_col,
            )
            grid_ds = read_grid_metric_cache(grid_cache_path, expected_sidecar=grid_sidecar)
        if grid_ds is None:
            eval_da = concat_years(year_to_paths, var, [year])
            cell_payload = _metric_cell_values(metric=metric, eval_da=eval_da, threshold=threshold)
            grid_ds = _grid_metric_dataset(cell_payload, value_col=value_col)
            if grid_cache_path is not None and grid_sidecar is not None:
                write_grid_metric_cache(grid_ds, grid_cache_path, sidecar=grid_sidecar)
        values = _aggregate_grid_metric_dataset(grid_ds, value_col=value_col, weights=weights)
        source_file = str(year_to_paths[int(year)][var])
        for unit_key, value in values.items():
            row: dict[str, object] = {
                "year": int(year),
                "value": float(value) if np.isfinite(value) else np.nan,
                value_col: float(value) if np.isfinite(value) else np.nan,
                "source_file": source_file,
            }
            if level == "block" and "||" in unit_key:
                left, right = unit_key.split("||", 1)
                row["district"] = left
                row["block"] = right
            elif level == "sub_basin" and "||" in unit_key:
                left, right = unit_key.split("||", 1)
                row["basin"] = left
                row["sub_basin"] = right
            elif level == "basin":
                row["basin"] = unit_key
            else:
                row["district"] = unit_key
            if scenario:
                row["scenario"] = scenario
            rows.append(row)
    return rows


# Shared grid-first helpers are re-exported here for backward compatibility.
# All cellwise compute, IO, and cache helpers now live in
# ``gridfirst_spatial``. Heat Risk (this module), Drought Risk, Extreme
# Rainfall, Heat Stress, and Cold Risk import them through here or directly.
from india_resilience_tool.compute.gridfirst_spatial import (  # noqa: E402,F401
    DEFAULT_ANALYSIS_CRS,
    GridSpec,
    _cellwise_heatwave_amplitude,
    _cellwise_percent_days,
    _cellwise_spell_days,
    _cellwise_spell_events,
    _configure_pyproj_data_dir,
    _drop_feb29,
    _hash_paths,
    _noleap_doy,
    _quantile,
    _run_lengths,
    aggregate_cell_values,
    aggregate_daily_area_mean,
    aggregate_percent_days,
    build_area_weights,
    compute_doy_thresholds,
    concat_years,
    coverage_from_weights,
    dataset_grid_spec,
    grid_metric_cache_path,
    input_file_signature,
    open_year_dataarray,
    read_grid_metric_cache,
    read_spatial_weights_cache,
    read_threshold_cache,
    threshold_cache_path,
    write_grid_metric_cache,
    write_spatial_weights_cache,
    write_threshold_cache,
)
