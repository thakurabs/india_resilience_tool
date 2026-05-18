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
) -> None:
    """Write spatial weights and a JSON sidecar for cache validation."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    weights.to_parquet(output_path, index=False)
    sidecar = {
        "method_version": GRIDFIRST_SPATIAL_METHOD_VERSION,
        "grid_id": grid.grid_id,
        "level": level,
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
    """Atomically write a grid-first metric field and invalidation sidecar."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_sidecar = path.with_suffix(path.suffix + ".json.tmp")
    ds.to_netcdf(tmp_path)
    final_sidecar = dict(sidecar)
    final_sidecar["cache_blob_sha256"] = _blob_sha256(tmp_path)
    tmp_sidecar.write_text(json.dumps(final_sidecar, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, path)
    os.replace(tmp_sidecar, path.with_suffix(path.suffix + ".json"))
