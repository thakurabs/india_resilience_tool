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
        "tnx_annual_max",
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


def input_file_signature(paths: Sequence[Path]) -> str:
    """Hash file paths, sizes, and mtimes for lightweight cache invalidation."""

    h = hashlib.sha256()
    for path in sorted(Path(p) for p in paths):
        h.update(str(path).encode("utf-8"))
        try:
            stat = path.stat()
            h.update(str(stat.st_size).encode("ascii"))
            h.update(str(stat.st_mtime_ns).encode("ascii"))
        except OSError:
            h.update(b"missing")
    return h.hexdigest()


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


def _drop_feb29(da: xr.DataArray) -> xr.DataArray:
    if "time" not in da.coords:
        return da
    mask = ~((da["time"].dt.month == 2) & (da["time"].dt.day == 29))
    return da.sel(time=da["time"][mask])


def _noleap_doy(da: xr.DataArray) -> xr.DataArray:
    doy = da["time"].dt.dayofyear
    after_feb = da["time"].dt.month > 2
    leap = da["time"].dt.is_leap_year
    return xr.where(after_feb & leap, doy - 1, doy)


def _quantile(da: xr.DataArray, q: float, *, dim: str, method: str) -> xr.DataArray:
    try:
        return da.quantile(q, dim=dim, method=method, skipna=True)
    except TypeError:
        return da.quantile(q, dim=dim, interpolation=method, skipna=True)


def compute_doy_thresholds(
    baseline: xr.DataArray,
    *,
    percentile: int = 90,
    window_days: int = 5,
    quantile_method: str = "linear",
    smooth: int | None = None,
) -> xr.DataArray:
    """Compute per-cell, per-day-of-year percentile thresholds."""

    base = _drop_feb29(baseline)
    if base.sizes.get("time", 0) == 0:
        raise ValueError("Baseline series is empty after dropping Feb 29")
    if window_days % 2 != 1:
        raise ValueError("window_days must be odd")

    doy = _noleap_doy(base)
    half = window_days // 2
    q = float(percentile) / 100.0
    thresholds: list[xr.DataArray] = []
    for day in range(1, 366):
        window = np.arange(day - half, day + half + 1)
        window = np.where(window < 1, window + 365, window)
        window = np.where(window > 365, window - 365, window)
        sample = base.where(doy.isin(window), drop=True)
        thresholds.append(_quantile(sample, q, dim="time", method=quantile_method))

    out = xr.concat(thresholds, dim="doy").assign_coords(doy=np.arange(1, 366))
    if smooth is not None:
        width = int(smooth)
        if width > 1:
            if width % 2 != 1:
                raise ValueError("smooth must be odd")
            pad = width // 2
            padded = xr.concat([out.isel(doy=slice(-pad, None)), out, out.isel(doy=slice(0, pad))], dim="doy")
            out = padded.rolling(doy=width, center=True, min_periods=1).mean().isel(doy=slice(pad, pad + 365))
            out = out.assign_coords(doy=np.arange(1, 366))
    return out


def threshold_cache_path(cache_root: Path, *, model: str, var: str, baseline_label: str) -> Path:
    """Return the private threshold-cache NetCDF path."""

    return Path(cache_root) / "thresholds" / model / var / f"{baseline_label}.nc"


def read_threshold_cache(
    path: Path,
    *,
    input_signature: str,
    baseline_years: tuple[int, int],
    percentile: int,
    window_days: int,
    quantile_method: str,
) -> xr.DataArray | None:
    """Read a cached threshold cube when its sidecar matches the inputs."""

    path = Path(path)
    sidecar_path = path.with_suffix(path.suffix + ".json")
    if not path.exists() or not sidecar_path.exists():
        return None
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    expected = {
        "method_version": GRIDFIRST_METHOD_VERSION,
        "input_file_hash": input_signature,
        "baseline_years": list(baseline_years),
        "percentile": int(percentile),
        "window_days": int(window_days),
        "quantile_method": quantile_method,
    }
    for key, value in expected.items():
        if sidecar.get(key) != value:
            return None
    ds = xr.open_dataset(path)
    try:
        if "threshold" in ds:
            return ds["threshold"].load()
        first = next(iter(ds.data_vars))
        return ds[first].load()
    finally:
        ds.close()


def write_threshold_cache(
    threshold: xr.DataArray,
    path: Path,
    *,
    input_signature: str,
    baseline_years: tuple[int, int],
    percentile: int,
    window_days: int,
    quantile_method: str,
) -> None:
    """Write a per-cell threshold cache and invalidation sidecar."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    threshold.to_dataset(name="threshold").to_netcdf(path)
    sidecar = {
        "method_version": GRIDFIRST_METHOD_VERSION,
        "input_file_hash": input_signature,
        "baseline_years": list(baseline_years),
        "percentile": int(percentile),
        "window_days": int(window_days),
        "quantile_method": quantile_method,
        "methodology_note": "Heat Risk v2 per-cell DOY percentile threshold cache",
    }
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps(sidecar, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _run_lengths(flags: np.ndarray, min_len: int) -> tuple[int, int, list[np.ndarray]]:
    spells: list[np.ndarray] = []
    total = 0
    max_run = 0
    start: int | None = None
    for i, flag in enumerate(flags):
        if flag and start is None:
            start = i
        if (not flag or i == flags.size - 1) and start is not None:
            end = i + 1 if flag and i == flags.size - 1 else i
            length = end - start
            if length >= min_len:
                idx = np.arange(start, end)
                spells.append(idx)
                total += int(length)
                max_run = max(max_run, int(length))
            start = None
    return max_run, total, spells


def _cellwise_percent_days(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    exceed_ge: bool,
    direction: str = "above",
) -> tuple[xr.DataArray, xr.DataArray]:
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    valid = eva.notnull()
    if direction == "below":
        flags = eva <= thr if exceed_ge else eva < thr
    else:
        flags = eva >= thr if exceed_ge else eva > thr
    return flags.where(valid, False).sum(dim="time"), valid.sum(dim="time")


def _cellwise_spell_days(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    min_spell_days: int,
    exceed_ge: bool,
) -> xr.DataArray:
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    flags = (eva >= thr if exceed_ge else eva > thr).fillna(False).values.astype(bool)
    out = np.full(flags.shape[1:], np.nan, dtype=float)
    for lat_i in range(flags.shape[1]):
        for lon_i in range(flags.shape[2]):
            series = flags[:, lat_i, lon_i]
            valid = np.asarray(eva[:, lat_i, lon_i].notnull().values, dtype=bool)
            if not valid.any():
                continue
            _max_run, total, _spells = _run_lengths(series & valid, min_spell_days)
            out[lat_i, lon_i] = float(total)
    return xr.DataArray(out, coords={"lat": da["lat"], "lon": da["lon"]}, dims=("lat", "lon"))


def _cellwise_spell_events(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    min_spell_days: int,
    exceed_ge: bool,
) -> xr.DataArray:
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    flags = (eva >= thr if exceed_ge else eva > thr).fillna(False).values.astype(bool)
    out = np.full(flags.shape[1:], np.nan, dtype=float)
    for lat_i in range(flags.shape[1]):
        for lon_i in range(flags.shape[2]):
            valid = np.asarray(eva[:, lat_i, lon_i].notnull().values, dtype=bool)
            if not valid.any():
                continue
            _max_run, _total, spells = _run_lengths(flags[:, lat_i, lon_i] & valid, min_spell_days)
            out[lat_i, lon_i] = float(len(spells))
    return xr.DataArray(out, coords={"lat": da["lat"], "lon": da["lon"]}, dims=("lat", "lon"))


def _cellwise_heatwave_amplitude(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    min_spell_days: int,
    exceed_ge: bool,
) -> xr.DataArray:
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    flags = (eva >= thr if exceed_ge else eva > thr).fillna(False).values.astype(bool)
    values = eva.values.astype(float)
    out = np.full(flags.shape[1:], np.nan, dtype=float)
    for lat_i in range(flags.shape[1]):
        for lon_i in range(flags.shape[2]):
            valid = np.isfinite(values[:, lat_i, lon_i])
            if not valid.any():
                continue
            _max_run, _total, spells = _run_lengths(flags[:, lat_i, lon_i] & valid, min_spell_days)
            if not spells:
                continue
            best_peak_k = np.nan
            best_mean_exceed = -np.inf
            for spell in spells:
                event_vals = values[spell, lat_i, lon_i]
                event_thr = thr.values[spell, lat_i, lon_i]
                mean_exceed = float(np.nanmean(event_vals - event_thr))
                if mean_exceed > best_mean_exceed:
                    best_mean_exceed = mean_exceed
                    best_peak_k = float(np.nanmax(event_vals))
            out[lat_i, lon_i] = best_peak_k - 273.15 if np.isfinite(best_peak_k) else np.nan
    return xr.DataArray(out, coords={"lat": da["lat"], "lon": da["lon"]}, dims=("lat", "lon"))


def aggregate_cell_values(cell_values: xr.DataArray, weights: pd.DataFrame) -> dict[str, float]:
    """Area-weight a per-cell field to each polygon unit."""

    if weights.empty:
        return {}
    flat = np.asarray(cell_values.values, dtype=float).reshape(-1)
    tmp = weights[["unit_key", "cell_index", "area_m2"]].copy()
    tmp["cell_value"] = flat[tmp["cell_index"].to_numpy(dtype=int)]
    tmp = tmp[np.isfinite(tmp["cell_value"])]
    if tmp.empty:
        return {str(unit): np.nan for unit in weights["unit_key"].unique()}
    tmp["weighted"] = tmp["area_m2"].astype(float) * tmp["cell_value"].astype(float)
    grouped = tmp.groupby("unit_key", sort=False).agg(weighted=("weighted", "sum"), area=("area_m2", "sum"))
    return {str(k): float(v["weighted"] / v["area"]) if v["area"] > 0 else np.nan for k, v in grouped.iterrows()}


def aggregate_percent_days(
    exceed_days: xr.DataArray,
    valid_days: xr.DataArray,
    weights: pd.DataFrame,
) -> dict[str, float]:
    """Area-weight cellwise exceedance and valid-day counts into percentages."""

    if weights.empty:
        return {}
    exceed_flat = np.asarray(exceed_days.values, dtype=float).reshape(-1)
    valid_flat = np.asarray(valid_days.values, dtype=float).reshape(-1)
    tmp = weights[["unit_key", "cell_index", "area_m2"]].copy()
    idx = tmp["cell_index"].to_numpy(dtype=int)
    tmp["exceed"] = exceed_flat[idx]
    tmp["valid"] = valid_flat[idx]
    tmp = tmp[np.isfinite(tmp["exceed"]) & np.isfinite(tmp["valid"]) & (tmp["valid"] > 0)]
    if tmp.empty:
        return {str(unit): np.nan for unit in weights["unit_key"].unique()}
    tmp["weighted_exceed"] = tmp["area_m2"].astype(float) * tmp["exceed"].astype(float)
    tmp["weighted_valid"] = tmp["area_m2"].astype(float) * tmp["valid"].astype(float)
    grouped = tmp.groupby("unit_key", sort=False).agg(
        weighted_exceed=("weighted_exceed", "sum"),
        weighted_valid=("weighted_valid", "sum"),
    )
    return {
        str(k): float(100.0 * v["weighted_exceed"] / v["weighted_valid"]) if v["weighted_valid"] > 0 else np.nan
        for k, v in grouped.iterrows()
    }


def aggregate_daily_area_mean(da: xr.DataArray, weights: pd.DataFrame) -> dict[str, xr.DataArray]:
    """Return daily area-mean series for each unit, useful for parity tests."""

    out: dict[str, xr.DataArray] = {}
    for unit, group in weights.groupby("unit_key", sort=False):
        cell_indices = group["cell_index"].to_numpy(dtype=int)
        area = group["area_m2"].to_numpy(dtype=float)
        lat_size = da.sizes["lat"]
        lon_size = da.sizes["lon"]
        lat_idx = cell_indices // lon_size
        lon_idx = cell_indices % lon_size
        vals = da.values[:, lat_idx, lon_idx]
        valid = np.isfinite(vals)
        numer = np.nansum(vals * area[None, :], axis=1)
        denom = np.sum(np.where(valid, area[None, :], 0.0), axis=1)
        mean = np.divide(numer, denom, out=np.full(numer.shape, np.nan, dtype=float), where=denom > 0)
        out[str(unit)] = xr.DataArray(mean, coords={"time": da["time"]}, dims=("time",))
    return out


def open_year_dataarray(path: Path, var: str) -> xr.DataArray:
    """Load one yearly NetCDF variable as a normalized in-memory DataArray."""

    ds = normalize_lat_lon(xr.open_dataset(path))
    try:
        if var not in ds:
            raise KeyError(f"Variable '{var}' not found in {path}")
        return ds[var].load()
    finally:
        ds.close()


def concat_years(year_to_paths: Mapping[int, Mapping[str, Path]], var: str, years: Sequence[int]) -> xr.DataArray:
    """Load and concatenate yearly files for one variable."""

    arrays = [open_year_dataarray(year_to_paths[int(year)][var], var) for year in years if int(year) in year_to_paths]
    if not arrays:
        raise ValueError(f"No yearly files available for variable {var}")
    return xr.concat(arrays, dim="time").sortby("time")


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

    threshold: xr.DataArray | None = None
    if compute != "annual_max_temperature":
        baseline_years_available = [
            year for year in sorted(baseline_year_to_paths) if baseline_years[0] <= int(year) <= baseline_years[1]
        ]
        baseline_da = concat_years(baseline_year_to_paths, var, baseline_years_available)
        input_sig = input_file_signature([baseline_year_to_paths[year][var] for year in baseline_years_available])
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
                input_signature=input_sig,
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
                    input_signature=input_sig,
                    baseline_years=baseline_years,
                    percentile=percentile,
                    window_days=window_days,
                    quantile_method=quantile_method,
                )

    rows: list[dict[str, object]] = []
    for year in sorted(year_to_paths):
        eval_da = concat_years(year_to_paths, var, [year])
        cell_payload = _metric_cell_values(metric=metric, eval_da=eval_da, threshold=threshold)
        if isinstance(cell_payload, tuple):
            values = aggregate_percent_days(cell_payload[0], cell_payload[1], weights)
        else:
            values = aggregate_cell_values(cell_payload, weights)
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
