"""Folium helpers for optional hydro boundary overlays.

The active overlay is driven by ``st.session_state['active_hydro_boundary_overlay']``
and uses existing hydro identifiers: ``basin_id`` and ``subbasin_id``.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)


def _optimized_context_path(filename: str, *, data_dir: Path) -> Path:
    try:
        from india_resilience_tool.data.optimized_bundle import optimized_context_path

        return Path(optimized_context_path(filename, data_dir=data_dir))
    except Exception:
        return Path(data_dir) / "processed_optimised" / "context" / filename


def _path_mtime(path: Path) -> float:
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _candidate_boundary_paths(
    data_dir: Path,
    hydro_level: str,
    *,
    basin_id: str = "",
) -> list[Path]:
    if hydro_level == "sub_basin":
        names = ("subbasins.geojson", "sub_basins.geojson", "subbasins_4326.geojson")
    else:
        names = ("basins.geojson", "basins_4326.geojson")
    candidates = [Path(data_dir) / n for n in names]
    try:
        import paths  # type: ignore

        if hydro_level == "sub_basin":
            for attr in ("SUBBASINS_PATH", "SUB_BASINS_PATH"):
                p = getattr(paths, attr, None)
                if p is not None:
                    candidates.insert(0, Path(p))
        else:
            p = getattr(paths, "BASINS_PATH", None)
            if p is not None:
                candidates.insert(0, Path(p))
    except Exception:
        pass

    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return unique


def _simplify_for_overlay(gdf: Any, *, hydro_level: str) -> Any:
    tolerance = (
        SIMPLIFY_TOL_SUBBASIN_RENDER
        if hydro_level == "sub_basin"
        else SIMPLIFY_TOL_BASIN_RENDER
    )
    try:
        out = gdf.copy()
        out["geometry"] = out["geometry"].simplify(
            tolerance=float(tolerance),
            preserve_topology=True,
        )
        return out
    except Exception:
        return gdf


@lru_cache(maxsize=64)
def _read_boundary_cached(
    *,
    path_str: str,
    mtime: float,
    hydro_level: str,
    target_id: str,
) -> Optional[Any]:
    _ = mtime
    try:
        import geopandas as gpd
    except Exception:
        return None

    try:
        gdf = gpd.read_file(path_str)
    except Exception:
        return None

    if gdf.empty:
        return None

    if hydro_level == "sub_basin":
        target_col = "subbasin_id"
        alt_cols = ("sub_basin_id", "subbasin", "SUBBASIN_ID", "HYBAS_ID")
    else:
        target_col = "basin_id"
        alt_cols = ("BASIN_ID", "basin", "HYBAS_ID")

    if not target_id:
        return None

    col = target_col if target_col in gdf.columns else next((c for c in alt_cols if c in gdf.columns), None)
    if col is None:
        return None

    subset = gdf[gdf[col].astype(str).str.strip() == target_id]
    if subset.empty:
        return None

    try:
        subset = subset.to_crs("EPSG:4326")
    except Exception:
        pass
    return _simplify_for_overlay(subset, hydro_level=hydro_level).reset_index(drop=True)


def _read_boundary(data_dir: Path, active_overlay: Mapping[str, Any]) -> Optional[Any]:
    hydro_level = str(active_overlay.get("hydro_level") or "basin").strip()
    basin_id = str(active_overlay.get("basin_id") or "").strip()
    if hydro_level == "sub_basin":
        target_id = str(active_overlay.get("subbasin_id") or "").strip()
    else:
        target_id = basin_id
    if not target_id:
        return None

    paths = _candidate_boundary_paths(data_dir, hydro_level, basin_id=basin_id)
    existing = next((p for p in paths if p.exists()), None)
    if existing is None:
        return None

    return _read_boundary_cached(
        path_str=str(existing),
        mtime=_path_mtime(existing),
        hydro_level=hydro_level,
        target_id=target_id,
    )


@lru_cache(maxsize=128)
def _read_overlap_cached(
    *,
    path_str: str,
    mtime: float,
    admin_key: str,
    admin_level: str,
    hydro_level: str,
    basin_id: str,
    subbasin_id: str,
) -> Optional[Any]:
    _ = mtime
    try:
        import geopandas as gpd
    except Exception:
        return None

    try:
        gdf = gpd.read_parquet(path_str)
    except Exception:
        return None

    if gdf.empty:
        return None

    mask = gdf["admin_key"].astype(str).str.strip().eq(admin_key)
    if "admin_level" in gdf.columns:
        mask &= gdf["admin_level"].astype(str).str.strip().str.lower().eq(admin_level)
    if "hydro_level" in gdf.columns:
        mask &= gdf["hydro_level"].astype(str).str.strip().eq(hydro_level)
    if "basin_id" in gdf.columns and basin_id:
        mask &= gdf["basin_id"].astype(str).str.strip().eq(basin_id)
    if hydro_level == "sub_basin" and "subbasin_id" in gdf.columns:
        mask &= gdf["subbasin_id"].fillna("").astype(str).str.strip().eq(subbasin_id)

    subset = gdf.loc[mask]
    if subset.empty:
        return None
    try:
        subset = subset.to_crs("EPSG:4326")
    except Exception:
        pass
    return _simplify_for_overlay(subset, hydro_level=hydro_level).reset_index(drop=True)


def _read_overlap(data_dir: Path, active_overlay: Mapping[str, Any]) -> Optional[Any]:
    path = _optimized_context_path("admin_hydro_overlaps.parquet", data_dir=data_dir)
    if not path.exists():
        return None

    return _read_overlap_cached(
        path_str=str(path),
        mtime=_path_mtime(path),
        admin_key=str(active_overlay.get("admin_key") or "").strip(),
        admin_level=str(active_overlay.get("admin_level") or "").strip().lower(),
        hydro_level=str(active_overlay.get("hydro_level") or "").strip(),
        basin_id=str(active_overlay.get("basin_id") or "").strip(),
        subbasin_id=str(active_overlay.get("subbasin_id") or "").strip(),
    )


def add_hydro_boundary_overlay_to_map(
    *,
    m: Any,
    active_overlay: Optional[Mapping[str, Any]],
    data_dir: Path,
) -> Any:
    """Add active hydro boundary and overlap footprint to a Folium map.

    This helper is intentionally defensive: optional hydro-context files should
    enrich the map when present, but must never break the dashboard when missing.
    """
    if not isinstance(active_overlay, Mapping):
        return m

    try:
        import folium
    except Exception:
        return m

    hydro_name = str(active_overlay.get("hydro_name") or "Hydro boundary")
    hydro_level = str(active_overlay.get("hydro_level") or "basin").strip()

    boundary = _read_boundary(Path(data_dir), active_overlay)
    if boundary is not None and not getattr(boundary, "empty", True):
        folium.GeoJson(
            data=boundary.__geo_interface__,
            name=f"{hydro_name} boundary",
            style_function=lambda _feature: {
                "fillOpacity": 0.0,
                "color": "#2563eb" if hydro_level == "basin" else "#7c3aed",
                "weight": 2.5,
                "opacity": 0.9,
            },
            tooltip=f"{hydro_name} boundary",
            smooth_factor=1.2,
        ).add_to(m)

    overlap = _read_overlap(Path(data_dir), active_overlay)
    if overlap is not None and not getattr(overlap, "empty", True):
        folium.GeoJson(
            data=overlap.__geo_interface__,
            name=f"{hydro_name} overlap",
            style_function=lambda _feature: {
                "fillColor": "#2563eb" if hydro_level == "basin" else "#7c3aed",
                "fillOpacity": 0.18,
                "color": "#1e40af" if hydro_level == "basin" else "#5b21b6",
                "weight": 1.5,
                "opacity": 0.9,
            },
            tooltip=f"{hydro_name} overlap with selected admin unit",
            smooth_factor=1.2,
        ).add_to(m)

    return m
