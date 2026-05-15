#!/usr/bin/env python3
"""Build private Heat Risk v2 spatial-weight caches."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import xarray as xr

from paths import BLOCKS_PATH, DATA_ROOT, DISTRICTS_PATH, get_paths_config
from india_resilience_tool.compute.heat_risk_gridfirst import (
    DEFAULT_ANALYSIS_CRS,
    build_area_weights,
    dataset_grid_spec,
    normalize_lat_lon,
    write_spatial_weights_cache,
)


def _import_exactextract() -> None:
    try:
        import exactextract  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "build_spatial_weights requires exactextract as a build-time dependency. "
            "Install it in the conda environment from conda-forge."
        ) from exc


def _default_sample_netcdf(var: str = "tasmax") -> Path | None:
    for path in sorted((DATA_ROOT / "historical" / var).glob("*/*.nc")):
        return path
    for path in sorted(DATA_ROOT.glob(f"**/{var}/**/*.nc")):
        return path
    return None


def _default_boundary(level: str) -> Path:
    if level == "block":
        return BLOCKS_PATH
    if level == "district":
        return DISTRICTS_PATH
    cfg = get_paths_config()
    if level == "basin":
        return cfg.basins_path
    if level == "sub_basin":
        return cfg.subbasins_path
    raise ValueError(f"Unsupported level: {level}")


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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    data_dir = args.data_dir.expanduser().resolve() if args.data_dir else get_paths_config().data_dir
    boundary = (args.boundary or _default_boundary(args.level)).expanduser().resolve()
    sample_nc = args.sample_nc.expanduser().resolve() if args.sample_nc else _default_sample_netcdf(args.var)
    if sample_nc is None:
        raise FileNotFoundError("No sample NetCDF found. Pass --sample-nc explicitly.")
    output = args.output
    if output is None:
        output = data_dir / "processed" / "_internal" / "spatial_weights" / f"{args.level}__auto.parquet"
    output = output.expanduser().resolve()

    print(f"level={args.level}")
    print(f"boundary={boundary}")
    print(f"sample_nc={sample_nc}")
    print(f"output={output}")
    if args.dry_run:
        return 0

    _import_exactextract()

    with xr.open_dataset(sample_nc) as ds:
        grid = dataset_grid_spec(normalize_lat_lon(ds))
    output = output.with_name(f"{args.level}__{grid.grid_id}.parquet") if output.name.endswith("__auto.parquet") else output
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
