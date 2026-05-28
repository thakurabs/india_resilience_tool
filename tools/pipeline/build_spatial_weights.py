#!/usr/bin/env python3
"""Build private Heat Risk v2 spatial-weight caches."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import geopandas as gpd
import xarray as xr

from paths import get_paths_config
from india_resilience_tool.compute.heat_risk_gridfirst import (
    DEFAULT_ANALYSIS_CRS,
    build_area_weights,
    dataset_grid_spec,
    normalize_lat_lon,
    read_spatial_weights_cache,
    write_spatial_weights_cache,
)


def _default_sample_netcdf(*, data_dir: Path, var: str = "tasmax") -> Path | None:
    for path in sorted((data_dir / "historical" / var).glob("*/*.nc")):
        return path
    for path in sorted(data_dir.glob(f"**/{var}/**/*.nc")):
        return path
    return None


def _default_boundary(*, level: str, data_dir: Path) -> Path:
    if level == "block":
        return data_dir / "blocks_4326.geojson"
    if level == "district":
        return data_dir / "districts_4326.geojson"
    if level == "basin":
        return data_dir / "basins.geojson"
    if level == "sub_basin":
        return data_dir / "subbasins.geojson"
    raise ValueError(f"Unsupported level: {level}")


def _resolved_output_path(*, output: Path | None, data_dir: Path, level: str, grid_id: str | None = None) -> Path:
    base = output or data_dir / "processed" / "_internal" / "spatial_weights" / f"{level}__auto.parquet"
    base = base.expanduser().resolve()
    if grid_id and base.name.endswith("__auto.parquet"):
        return base.with_name(f"{level}__{grid_id}.parquet")
    return base


def _existing_cache_is_valid(
    *,
    output_path: Path,
    grid,
    level: str,
    boundary: Path,
    analysis_crs: str,
) -> bool:
    if not output_path.exists():
        return False
    try:
        return read_spatial_weights_cache(
            output_path,
            grid=grid,
            level=level,
            boundary_path=boundary,
            analysis_crs=analysis_crs,
        ) is not None
    except Exception:
        return False


def _cache_sidecar_payload(path: Path) -> dict | None:
    sidecar_path = path.with_suffix(path.suffix + ".json")
    if not sidecar_path.exists():
        return None
    try:
        return json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build private sparse area-overlap weights for Heat Risk v2 grid-first "
            "polygon aggregation. The app runtime does not import exactextract."
        )
    )
    parser.add_argument("--level", choices=["district", "block", "basin", "sub_basin"], default="district")
    parser.add_argument("--boundary", type=Path, default=None, help="Boundary GeoJSON/shapefile path.")
    parser.add_argument("--sample-nc", type=Path, default=None, help="Climate NetCDF whose lat/lon grid defines weights.")
    parser.add_argument("--var", default="tasmax", help="Variable used when auto-discovering a sample NetCDF.")
    parser.add_argument("--output", type=Path, default=None, help="Explicit output parquet path.")
    parser.add_argument("--data-dir", type=Path, default=None, help="Override IRT_DATA_DIR-derived data directory.")
    parser.add_argument("--analysis-crs", default=DEFAULT_ANALYSIS_CRS)
    parser.add_argument("--dry-run", action="store_true", help="Print resolved inputs/outputs without writing.")
    parser.add_argument("--overwrite", action="store_true", help="Rebuild when a stale cache already exists.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    data_dir = args.data_dir.expanduser().resolve() if args.data_dir else get_paths_config().data_dir
    boundary = (args.boundary or _default_boundary(level=args.level, data_dir=data_dir)).expanduser().resolve()
    sample_nc = args.sample_nc.expanduser().resolve() if args.sample_nc else _default_sample_netcdf(data_dir=data_dir, var=args.var)
    if sample_nc is None:
        raise FileNotFoundError("No sample NetCDF found. Pass --sample-nc explicitly.")

    with xr.open_dataset(sample_nc) as ds:
        grid = dataset_grid_spec(normalize_lat_lon(ds))
    output = _resolved_output_path(output=args.output, data_dir=data_dir, level=args.level, grid_id=grid.grid_id)

    print(f"level={args.level}")
    print(f"boundary={boundary}")
    print(f"sample_nc={sample_nc}")
    print(f"output={output}")
    if args.dry_run:
        return 0

    output_exists = output.exists() or output.with_suffix(output.suffix + ".json").exists()
    if _existing_cache_is_valid(
        output_path=output,
        grid=grid,
        level=args.level,
        boundary=boundary,
        analysis_crs=args.analysis_crs,
    ):
        print(f"cache_status=valid existing={output}")
        return 0
    if output_exists and not args.overwrite:
        sidecar = _cache_sidecar_payload(output)
        detail = "missing_or_invalid_sidecar" if sidecar is None else "stale_sidecar_or_grid_mismatch"
        print(
            f"stale_cache={output} reason={detail} rerun_with=--overwrite",
            file=sys.stderr,
        )
        return 1

    gdf = gpd.read_file(boundary)
    weights = build_area_weights(gdf, grid, level=args.level, analysis_crs=args.analysis_crs)
    write_spatial_weights_cache(
        weights,
        output_path=output,
        grid=grid,
        level=args.level,
        boundary_path=boundary,
        analysis_crs=args.analysis_crs,
    )
    print(f"wrote_rows={len(weights)}")
    print(f"wrote={output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
