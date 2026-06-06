"""Shared grid-first spatial and cache helpers.

These helpers are intentionally compute-layer only: no Streamlit imports and no
pipeline imports. Heat Risk re-exports the same names for backward
compatibility while Drought Risk v2 imports them directly.
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


DEFAULT_ANALYSIS_CRS = "EPSG:6933"
GRIDFIRST_SPATIAL_METHOD_VERSION = "gridfirst-spatial-1"


def _configure_pyproj_data_dir() -> None:
    """Point pyproj at conda's PROJ database when its bundled path is unusable."""
    candidates = [os.environ.get("PROJ_DATA"), os.environ.get("PROJ_LIB")]
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
                "version": GRIDFIRST_SPATIAL_METHOD_VERSION,
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


def _axis_index_range(
    coord: Sequence[float],
    lo: float,
    hi: float,
    *,
    buffer_cells: float,
) -> tuple[int, int]:
    """Return a half-open positional ``[start, stop)`` range covering ``[lo, hi]``.

    Direction-agnostic: works whether ``coord`` is ascending or descending. The
    buffer is taken from the actual grid spacing (``buffer_cells`` cells, each
    side), so a cell whose center sits up to ~half a cell outside the bounds —
    but whose footprint still overlaps the polygon and would receive nonzero
    area weight — is retained. Raises on a non-monotonic coordinate or when the
    buffered bounds select zero cells.
    """
    vals = np.asarray(coord, dtype=float)
    if vals.size == 0:
        raise ValueError("Grid coordinate is empty")
    if vals.size == 1:
        return 0, 1
    diffs = np.diff(vals)
    if not (np.all(diffs > 0) or np.all(diffs < 0)):
        raise ValueError("Grid coordinate is not monotonic; cannot compute bbox subset")
    spacing = abs(float(vals[1] - vals[0]))
    buf = spacing * float(buffer_cells)
    mask = (vals >= (float(lo) - buf)) & (vals <= (float(hi) + buf))
    idx = np.nonzero(mask)[0]
    if idx.size == 0:
        raise ValueError(
            f"Bounding box [{lo}, {hi}] selects zero grid cells "
            f"(coord range [{float(vals.min())}, {float(vals.max())}], buffer {buf})"
        )
    return int(idx.min()), int(idx.max()) + 1


def bbox_to_index_range(
    sample_lat: Sequence[float],
    sample_lon: Sequence[float],
    bbox: tuple[float, float, float, float],
    *,
    buffer_cells: float = 2.0,
) -> tuple[int, int, int, int]:
    """Map an EPSG:4326 bbox to positional ``(lat0, lat1, lon0, lon1)`` indices.

    ``bbox`` is ``(minx, miny, maxx, maxy)`` (lon=x, lat=y), e.g. a GeoDataFrame's
    ``total_bounds``. The returned half-open ranges are intended for
    ``.isel(lat=slice(lat0, lat1), lon=slice(lon0, lon1))`` and are computed
    once from the sample grid, then reused for every yearly data file so the
    GridSpec and the loaded data array share an identical positional slice.
    """
    minx, miny, maxx, maxy = (float(v) for v in bbox)
    lat0, lat1 = _axis_index_range(sample_lat, miny, maxy, buffer_cells=buffer_cells)
    lon0, lon1 = _axis_index_range(sample_lon, minx, maxx, buffer_cells=buffer_cells)
    return (lat0, lat1, lon0, lon1)


def subset_grid_by_index(obj, index_range: tuple[int, int, int, int] | None):
    """Apply a positional lat/lon subset to a Dataset or DataArray.

    ``index_range`` is ``(lat0, lat1, lon0, lon1)`` as produced by
    :func:`bbox_to_index_range`. ``None`` returns ``obj`` unchanged. Assumes
    :func:`normalize_lat_lon` has already run (``lat``/``lon`` dims present).
    """
    if index_range is None:
        return obj
    lat0, lat1, lon0, lon1 = index_range
    return obj.isel(lat=slice(lat0, lat1), lon=slice(lon0, lon1))


def _assert_grid_alignment(
    cell_values: xr.DataArray,
    weights: pd.DataFrame,
    *,
    grid: "GridSpec | None" = None,
) -> None:
    """Belt-and-braces check that ``cell_values`` matches the weights' grid.

    Spatial weights store ``cell_index = lat_index * len(lon) + lon_index``
    relative to the GridSpec they were built from; aggregation flattens
    ``cell_values`` (C-order) and indexes by ``cell_index``. If a loaded data
    array's grid differs from that GridSpec (e.g. a bbox subset desynced from
    the GridSpec, or a heterogeneous yearly file), the flat indices silently
    address the wrong cells. This guard turns that into a clear, early error.

    The shape/cell_index invariant is always checked. When ``grid`` is supplied,
    the loaded coordinates are additionally compared to the GridSpec within
    tolerance (catches sub-cell coordinate drift at identical shape).
    """
    if weights.empty:
        return
    arr = np.asarray(cell_values.values)
    if arr.ndim < 2:
        raise ValueError("cell field must be at least 2D (lat, lon) for area-weight aggregation")
    lat_count, lon_count = int(arr.shape[-2]), int(arr.shape[-1])
    total = lat_count * lon_count
    ci = weights["cell_index"].to_numpy(dtype=int)
    if ci.size and (int(ci.max()) >= total or int(ci.min()) < 0):
        raise ValueError(
            "Spatial-weight cell_index out of range for the loaded grid "
            f"(grid shape {(lat_count, lon_count)}, total {total}); subset/grid mismatch"
        )
    if {"lat_index", "lon_index"} <= set(weights.columns):
        li = weights["lat_index"].to_numpy(dtype=int)
        lj = weights["lon_index"].to_numpy(dtype=int)
        if li.size and (int(li.max()) >= lat_count or int(lj.max()) >= lon_count):
            raise ValueError(
                "Spatial weights reference cells outside the loaded grid "
                f"(grid shape {(lat_count, lon_count)}); subset/grid mismatch"
            )
        if not np.array_equal(ci, li * lon_count + lj):
            raise ValueError(
                "Spatial-weight cell_index does not match the loaded grid shape "
                f"{(lat_count, lon_count)}; subset/grid mismatch"
            )
    if grid is not None:
        if (lat_count, lon_count) != grid.shape:
            raise ValueError(
                f"Loaded grid shape {(lat_count, lon_count)} != GridSpec {grid.shape}"
            )
        coords = getattr(cell_values, "coords", {})
        if "lat" in coords and "lon" in coords:
            lat_loaded = np.asarray(cell_values["lat"].values, dtype=float)
            lon_loaded = np.asarray(cell_values["lon"].values, dtype=float)
            if not np.allclose(lat_loaded, np.asarray(grid.lat, dtype=float), atol=1e-6) or not np.allclose(
                lon_loaded, np.asarray(grid.lon, dtype=float), atol=1e-6
            ):
                raise ValueError("Loaded grid coordinates differ from GridSpec beyond tolerance")


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


def _hash_paths(paths: Sequence[Path]) -> dict[str, str]:
    """Return sha256 hashes keyed by path string for cache sidecars."""
    out: dict[str, str] = {}
    for path in sorted(Path(p) for p in paths):
        h = hashlib.sha256()
        try:
            h.update(path.read_bytes())
        except OSError:
            h.update(b"missing")
        out[str(path)] = h.hexdigest()
    return out


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


def coverage_from_weights(gdf: gpd.GeoDataFrame, weights: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Build coverage-QC rows from sparse area weights."""
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
        if level == "block" and "||" in key:
            qc["district"], qc["block"] = key.split("||", 1)
        elif level == "district":
            qc["district"] = key
        elif level == "basin":
            qc["basin_name"] = key
        elif level == "sub_basin" and "||" in key:
            qc["basin_name"], qc["subbasin_name"] = key.split("||", 1)
        rows.append(qc)
    return pd.DataFrame(rows)


def write_spatial_weights_cache(
    weights: pd.DataFrame,
    *,
    output_path: Path,
    grid: GridSpec,
    level: str,
    boundary_path: Path | None = None,
    analysis_crs: str = DEFAULT_ANALYSIS_CRS,
    state: str | None = None,
) -> None:
    """Write spatial weights and a JSON sidecar for cache validation.

    ``state`` is recorded so per-state weight caches never alias one another,
    even when the grid is shared (full-India grid → identical ``grid_id`` and
    boundary hash across states). Readers must match it.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    weights.to_parquet(output_path, index=False)
    sidecar = {
        "method_version": GRIDFIRST_SPATIAL_METHOD_VERSION,
        "grid_id": grid.grid_id,
        "level": level,
        "state": state,
        "crs_epsg": int(str(analysis_crs).split(":")[-1]),
        "boundary_file_hash": boundary_content_hash(boundary_path) if boundary_path else None,
    }
    output_path.with_suffix(output_path.suffix + ".json").write_text(json.dumps(sidecar, indent=2, sort_keys=True), encoding="utf-8")


def read_spatial_weights_cache(
    path: Path,
    *,
    grid: GridSpec,
    level: str,
    boundary_path: Path | None = None,
    analysis_crs: str = DEFAULT_ANALYSIS_CRS,
    state: str | None = None,
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
    if sidecar.get("method_version") != GRIDFIRST_SPATIAL_METHOD_VERSION:
        return None
    if sidecar.get("grid_id") != grid.grid_id or sidecar.get("level") != level:
        return None
    if sidecar.get("state") != state:
        return None
    if int(sidecar.get("crs_epsg", -1)) != int(str(analysis_crs).split(":")[-1]):
        return None
    if boundary_path and sidecar.get("boundary_file_hash") != boundary_content_hash(boundary_path):
        return None
    return pd.read_parquet(path)


def grid_metric_cache_path(cache_root: Path, *, slug: str, model: str, scenario: str, year: int | str) -> Path:
    """Return the private grid-first metric NetCDF path."""
    return Path(cache_root) / "grid_metrics" / slug / model / scenario / f"{year}.nc"


def _blob_sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(Path(path).read_bytes())
    return h.hexdigest()


def read_grid_metric_cache(path: Path, *, expected_sidecar: Mapping[str, object]) -> xr.Dataset | None:
    """Read a grid-first metric cache when its sidecar and blob hash match."""
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
    if sidecar.get("cache_blob_sha256") and sidecar.get("cache_blob_sha256") != _blob_sha256(path):
        return None
    ds = xr.open_dataset(path)
    try:
        return ds.load()
    finally:
        ds.close()


def write_grid_metric_cache(ds: xr.Dataset, path: Path, *, sidecar: Mapping[str, object]) -> None:
    """Atomically write a grid-first metric field and invalidation sidecar.

    The sidecar carries the NetCDF blob hash. Readers must validate that hash
    so stale or torn blob/sidecar pairs are treated as cache misses.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_sidecar = path.with_suffix(path.suffix + ".json.tmp")
    ds.to_netcdf(tmp_path)
    final_sidecar = dict(sidecar)
    final_sidecar["cache_blob_sha256"] = _blob_sha256(tmp_path)
    tmp_sidecar.write_text(json.dumps(final_sidecar, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_sidecar, path.with_suffix(path.suffix + ".json"))
    os.replace(tmp_path, path)


# =============================================================================
# Shared compute / IO primitives (formerly in heat_risk_gridfirst.py).
# Moved here so Cold Risk, Heat Risk, Drought Risk, and Extreme Rainfall can
# share a single source of truth. Heat re-exports these for back-compat.
# =============================================================================
import warnings  # noqa: E402  (local to compute primitives below)


def _drop_feb29(da: xr.DataArray) -> xr.DataArray:
    """Remove Feb 29 timestamps so day-of-year math is 365-aligned."""
    if "time" not in da.coords:
        return da
    mask = ~((da["time"].dt.month == 2) & (da["time"].dt.day == 29))
    return da.sel(time=da["time"][mask])


def _noleap_doy(da: xr.DataArray) -> xr.DataArray:
    """Return day-of-year on a no-leap calendar (1..365).

    For dates after Feb in leap years, doy is shifted down by 1 to skip Feb 29.
    """
    doy = da["time"].dt.dayofyear
    after_feb = da["time"].dt.month > 2
    leap = da["time"].dt.is_leap_year
    return xr.where(after_feb & leap, doy - 1, doy)


def _quantile(da: xr.DataArray, q: float, *, dim: str, method: str) -> xr.DataArray:
    """Return a scalar nanquantile while preserving non-reduced dimensions.

    Some xarray/numpy combinations can mishandle all-NaN grid cells when
    reducing a 3D time/lat/lon cube with a scalar quantile, expecting a
    transient ``quantile`` dimension even though NumPy returns a 2D field.
    This explicit path keeps thresholds shape-stable: valid cells match
    ``skipna=True`` quantiles and all-NaN cells remain NaN.
    """
    if dim not in da.dims:
        raise ValueError(f"Cannot compute quantile: dimension {dim!r} not present")
    axis = da.get_axis_num(dim)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="All-NaN slice encountered", category=RuntimeWarning)
            values = np.nanquantile(np.asarray(da.values, dtype=float), q, axis=axis, method=method)
    except TypeError:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="All-NaN slice encountered", category=RuntimeWarning)
            values = np.nanquantile(np.asarray(da.values, dtype=float), q, axis=axis, interpolation=method)
    out_dims = tuple(name for name in da.dims if name != dim)
    out_coords = {name: da.coords[name] for name in out_dims if name in da.coords}
    return xr.DataArray(values, dims=out_dims, coords=out_coords, attrs=da.attrs)


def _run_lengths(flags: np.ndarray, min_len: int) -> tuple[int, int, list[np.ndarray]]:
    """Return (max_run, total_qualifying_days, list_of_qualifying_runs) for a 1D bool array."""
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


def compute_doy_thresholds(
    baseline: xr.DataArray,
    *,
    percentile: int = 90,
    window_days: int = 5,
    quantile_method: str = "linear",
    smooth: int | None = None,
) -> xr.DataArray:
    """Compute per-cell, per-day-of-year percentile thresholds.

    For each day-of-year d in 1..365, pool all baseline values that fall
    within +/-(window_days//2) days of d (with calendar wrap), then take the
    requested percentile per cell. Optionally smooth the resulting 365-day
    threshold curve with a circular rolling mean.
    """
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


def threshold_cache_path(
    cache_root: Path,
    *,
    model: str,
    var: str,
    baseline_label: str,
    grid_id: str | None = None,
) -> Path:
    """Return the per-cell DOY threshold cache NetCDF path.

    When ``grid_id`` is provided the full grid id is appended to the filename so
    caches built on different grids (e.g. a per-state bbox subset vs the full
    India grid) coexist rather than clobber one another. ``None`` preserves the
    legacy path for back-compatibility.
    """
    name = f"{baseline_label}.nc" if grid_id is None else f"{baseline_label}__grid={grid_id}.nc"
    return Path(cache_root) / "thresholds" / model / var / name


def read_threshold_cache(
    path: Path,
    *,
    input_signature: str,
    baseline_years: tuple[int, int],
    percentile: int,
    window_days: int,
    quantile_method: str,
    grid_id: str | None = None,
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
        "input_file_hash": input_signature,
        "baseline_years": list(baseline_years),
        "percentile": int(percentile),
        "window_days": int(window_days),
        "quantile_method": quantile_method,
        "grid_id": grid_id,
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
    grid_id: str | None = None,
) -> None:
    """Write a per-cell threshold cache and invalidation sidecar."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    threshold.to_dataset(name="threshold").to_netcdf(path)
    sidecar = {
        "method_version": GRIDFIRST_SPATIAL_METHOD_VERSION,
        "input_file_hash": input_signature,
        "baseline_years": list(baseline_years),
        "percentile": int(percentile),
        "window_days": int(window_days),
        "quantile_method": quantile_method,
        "grid_id": grid_id,
        "methodology_note": "Per-cell DOY percentile threshold cache",
    }
    path.with_suffix(path.suffix + ".json").write_text(json.dumps(sidecar, indent=2, sort_keys=True), encoding="utf-8")


def open_year_dataarray(
    path: Path,
    var: str,
    *,
    index_range: tuple[int, int, int, int] | None = None,
) -> xr.DataArray:
    """Load one yearly NetCDF variable as a normalized in-memory DataArray.

    When ``index_range`` is given the lat/lon slice is applied lazily (before
    ``.load()``) so only the bounding-box subset is read into RAM.
    """
    ds = normalize_lat_lon(xr.open_dataset(path))
    try:
        if var not in ds:
            raise KeyError(f"Variable '{var}' not found in {path}")
        return subset_grid_by_index(ds[var], index_range).load()
    finally:
        ds.close()


def concat_years(
    year_to_paths: Mapping[int, Mapping[str, Path]],
    var: str,
    years: Sequence[int],
    *,
    index_range: tuple[int, int, int, int] | None = None,
) -> xr.DataArray:
    """Load and concatenate yearly files for one variable, sorted by time.

    ``index_range`` is forwarded to :func:`open_year_dataarray` so every yearly
    file is read with the identical positional lat/lon slice.
    """
    arrays = [
        open_year_dataarray(year_to_paths[int(y)][var], var, index_range=index_range)
        for y in years
        if int(y) in year_to_paths
    ]
    if not arrays:
        raise ValueError(f"No yearly files available for variable {var}")
    return xr.concat(arrays, dim="time").sortby("time")


def aggregate_cell_values(
    cell_values: xr.DataArray,
    weights: pd.DataFrame,
    *,
    grid: "GridSpec | None" = None,
) -> dict[str, float]:
    """Area-weight a per-cell field to each polygon unit."""
    if weights.empty:
        return {}
    _assert_grid_alignment(cell_values, weights, grid=grid)
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
    *,
    grid: "GridSpec | None" = None,
) -> dict[str, float]:
    """Area-weight cellwise exceedance and valid-day counts into percentages."""
    if weights.empty:
        return {}
    _assert_grid_alignment(exceed_days, weights, grid=grid)
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


# =============================================================================
# Cellwise compute helpers (used by Heat Risk, Cold Risk).
# =============================================================================


def _cellwise_percent_days(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    exceed_ge: bool,
    direction: str = "above",
) -> tuple[xr.DataArray, xr.DataArray]:
    """Return (exceed_days, valid_days) per cell for a DOY-threshold percentile metric.

    direction="above": flags days where eva (>= or >) threshold.
    direction="below": flags days where eva (<= or <) threshold.
    exceed_ge=True selects the inclusive comparator; False selects strict.
    """
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    valid = eva.notnull()
    if direction == "below":
        flags = eva <= thr if exceed_ge else eva < thr
    elif direction == "above":
        flags = eva >= thr if exceed_ge else eva > thr
    else:
        raise ValueError(f"Invalid direction='{direction}'. Expected 'above' or 'below'.")
    return flags.where(valid, False).sum(dim="time"), valid.sum(dim="time")


def _cellwise_spell_days(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    min_spell_days: int,
    exceed_ge: bool,
    direction: str = "above",
) -> xr.DataArray:
    """Per-cell total days inside qualifying spells (>=min_spell_days consecutive).

    Generalized for both warm spells (direction="above") and cold spells
    (direction="below"). The exceed_ge flag controls inclusivity at the
    threshold boundary.
    """
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    if direction == "below":
        flags_da = eva <= thr if exceed_ge else eva < thr
    elif direction == "above":
        flags_da = eva >= thr if exceed_ge else eva > thr
    else:
        raise ValueError(f"Invalid direction='{direction}'. Expected 'above' or 'below'.")
    flags = flags_da.fillna(False).values.astype(bool)
    out = np.full(flags.shape[1:], np.nan, dtype=float)
    for lat_i in range(flags.shape[1]):
        for lon_i in range(flags.shape[2]):
            valid = np.asarray(eva[:, lat_i, lon_i].notnull().values, dtype=bool)
            if not valid.any():
                continue
            _max_run, total, _spells = _run_lengths(flags[:, lat_i, lon_i] & valid, min_spell_days)
            out[lat_i, lon_i] = float(total)
    return xr.DataArray(out, coords={"lat": da["lat"], "lon": da["lon"]}, dims=("lat", "lon"))


def _cellwise_spell_events(
    da: xr.DataArray,
    threshold: xr.DataArray,
    *,
    min_spell_days: int,
    exceed_ge: bool,
    direction: str = "above",
) -> xr.DataArray:
    """Per-cell count of qualifying spells (>=min_spell_days)."""
    eva = _drop_feb29(da)
    doy = _noleap_doy(eva)
    thr = threshold.sel(doy=doy)
    if direction == "below":
        flags_da = eva <= thr if exceed_ge else eva < thr
    elif direction == "above":
        flags_da = eva >= thr if exceed_ge else eva > thr
    else:
        raise ValueError(f"Invalid direction='{direction}'. Expected 'above' or 'below'.")
    flags = flags_da.fillna(False).values.astype(bool)
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
    """Per-cell heatwave amplitude (peak temperature of the strongest event, in Celsius)."""
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


# =============================================================================
# Cellwise compute helpers exclusive to Cold Risk v2.
# =============================================================================


def _cellwise_annual_min_temperature(eval_da: xr.DataArray) -> xr.DataArray:
    """Per-cell annual minimum (in Celsius). Input expected in Kelvin."""
    return eval_da.min(dim="time", skipna=True) - 273.15


def _cellwise_longest_consecutive_run_le(
    eval_da: xr.DataArray,
    *,
    thresh_k: float,
) -> xr.DataArray:
    """Per-cell longest consecutive run of days with value <= thresh_k.

    Returns an integer-valued ``xr.DataArray`` on the (lat, lon) grid. Cells
    with no finite eval data return 0 (matches the polygon-mean compute path's
    `longest_consecutive_run_le_threshold` semantics for empty series).
    """
    flags_da = (eval_da <= float(thresh_k))
    flags = flags_da.fillna(False).values.astype(bool)
    out = np.zeros(flags.shape[1:], dtype=float)
    for lat_i in range(flags.shape[1]):
        for lon_i in range(flags.shape[2]):
            valid = np.asarray(eval_da[:, lat_i, lon_i].notnull().values, dtype=bool)
            if not valid.any():
                out[lat_i, lon_i] = 0.0
                continue
            max_run, _total, _spells = _run_lengths(flags[:, lat_i, lon_i] & valid, 1)
            out[lat_i, lon_i] = float(max_run)
    return xr.DataArray(out, coords={"lat": eval_da["lat"], "lon": eval_da["lon"]}, dims=("lat", "lon"))


def _cellwise_djf_cross_year_window(
    *,
    prev_da: xr.DataArray | None,
    cur_da: xr.DataArray,
) -> xr.DataArray | None:
    """Return the daily DJF window per cell: Dec(prev) + Jan/Feb(cur).

    Returns ``None`` when prev_da is missing (caller decides between NaN
    short-circuit and fallback to the historical archive).
    """
    if prev_da is None:
        return None
    p = _drop_feb29(prev_da)
    c = _drop_feb29(cur_da)
    dec_prev = p.sel(time=p["time"].dt.month == 12)
    jf_cur = c.sel(time=c["time"].dt.month.isin([1, 2]))
    if dec_prev.sizes.get("time", 0) == 0 or jf_cur.sizes.get("time", 0) == 0:
        return None
    return xr.concat([dec_prev, jf_cur], dim="time")


def _cellwise_djf_cross_year_mean(
    *,
    prev_da: xr.DataArray | None,
    cur_da: xr.DataArray,
) -> xr.DataArray:
    """Per-cell DJF mean (Dec_{y-1} + Jan/Feb_y) in Celsius. Input in Kelvin.

    All cells return NaN when the cross-year window cannot be formed.
    """
    window = _cellwise_djf_cross_year_window(prev_da=prev_da, cur_da=cur_da)
    if window is None:
        nan_shape = (cur_da.sizes["lat"], cur_da.sizes["lon"])
        return xr.DataArray(
            np.full(nan_shape, np.nan, dtype=float),
            coords={"lat": cur_da["lat"], "lon": cur_da["lon"]},
            dims=("lat", "lon"),
        )
    return window.mean(dim="time", skipna=True) - 273.15


def _cellwise_djf_cross_year_min(
    *,
    prev_da: xr.DataArray | None,
    cur_da: xr.DataArray,
) -> xr.DataArray:
    """Per-cell DJF minimum (Dec_{y-1} + Jan/Feb_y) in Celsius. Input in Kelvin."""
    window = _cellwise_djf_cross_year_window(prev_da=prev_da, cur_da=cur_da)
    if window is None:
        nan_shape = (cur_da.sizes["lat"], cur_da.sizes["lon"])
        return xr.DataArray(
            np.full(nan_shape, np.nan, dtype=float),
            coords={"lat": cur_da["lat"], "lon": cur_da["lon"]},
            dims=("lat", "lon"),
        )
    return window.min(dim="time", skipna=True) - 273.15
