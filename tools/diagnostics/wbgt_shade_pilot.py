"""Measured shade pilot using production cell fields, weights and private caches."""

from __future__ import annotations
import argparse
import json
import shutil
import threading
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psutil
import xarray as xr
from india_resilience_tool.compute.gridfirst_spatial import (
    dataset_grid_spec,
    bbox_to_index_range,
    subset_grid_by_index,
    normalize_lat_lon,
    build_area_weights,
)
from india_resilience_tool.compute.heat_stress_gridfirst import (
    compute_heat_stress_rows_for_metric,
)
from india_resilience_tool.compute.wbgt import SHADE_SLUGS, SHADE_METHOD_SIGNATURE


def main(argv: list[str] | None = None) -> int:
    """Run all four metrics in three states, twice, and record measured costs."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, default=Path("scratch/wbgt_shade_pilot"))
    p.add_argument("--model", default="ACCESS-CM2")
    p.add_argument("--year", type=int, default=2005)
    p.add_argument(
        "--roster",
        type=Path,
        default=Path("docs/diagnostics/wbgt_shade_release/roster.json"),
    )
    args = p.parse_args(argv)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        v: args.data_root
        / "r1i1p1f1"
        / "historical"
        / v
        / args.model
        / f"{args.year}.nc"
        for v in ("tas", "tasmax", "hurs")
    }
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    metadata = {}
    for var, path in paths.items():
        with xr.open_dataset(path) as ds:
            metadata[var] = {
                "path": str(path),
                "units": ds[var].attrs.get("units"),
                "calendar": ds.time.dt.calendar,
                "days": ds.sizes["time"],
                "sample": float(ds[var].isel(time=0, lat=0, lon=0)),
            }
    process = psutil.Process()
    peak = [process.memory_info().rss]
    stop = threading.Event()

    def monitor():
        while not stop.wait(0.05):
            peak[0] = max(peak[0], process.memory_info().rss)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    timings = []
    try:
        for level in ("district", "block"):
            boundaries = gpd.read_file(args.data_root / f"{level}s_4326.geojson")
            for state in ("Kerala", "Rajasthan", "Himachal Pradesh"):
                gdf = boundaries[boundaries.state_name.eq(state)].copy()
                if gdf.empty:
                    raise ValueError(f"Missing {state} boundaries")
                with xr.open_dataset(paths["tas"]) as ds:
                    ds = normalize_lat_lon(ds)
                    subset = bbox_to_index_range(
                        ds.lat, ds.lon, tuple(gdf.total_bounds)
                    )
                    grid = dataset_grid_spec(subset_grid_by_index(ds, subset))
                started = time.perf_counter()
                weights = build_area_weights(gdf, grid, level=level)
                weight_seconds = time.perf_counter() - started
                for repeat in range(2):
                    before = process.io_counters()
                    started = time.perf_counter()
                    all_rows = []
                    for slug in sorted(SHADE_SLUGS):
                        rows = compute_heat_stress_rows_for_metric(
                            metric={
                                "slug": slug,
                                "value_col": "value",
                                "params": {"grid_id": grid.grid_id},
                            },
                            model=args.model,
                            scenario="historical",
                            year_to_paths={args.year: paths},
                            weights=weights,
                            level=level,
                            cache_root=args.out_dir / "cache",
                            index_range=subset,
                            grid=grid,
                        )
                        for row in rows:
                            row["slug"] = slug
                        all_rows.extend(rows)
                    elapsed = time.perf_counter() - started
                    after = process.io_counters()
                    frame = pd.DataFrame(all_rows)
                    frame.to_csv(
                        args.out_dir / f"{state}_{level}_{repeat}.csv", index=False
                    )
                    timings.append(
                        dict(
                            state=state,
                            level=level,
                            cached=bool(repeat),
                            seconds=elapsed,
                            weight_seconds=weight_seconds,
                            grid_cells=len(grid.lat) * len(grid.lon),
                            units=len(gdf),
                            rows=len(frame),
                            nan_rows=int(frame.value.isna().sum()),
                            idw_rows=int(frame.climate_fill_method.eq("idw").sum()),
                            incomplete_area_rows=int(
                                frame.valid_area_fraction.lt(1).sum()
                            ),
                            read_bytes=after.read_bytes - before.read_bytes,
                            write_bytes=after.write_bytes - before.write_bytes,
                        )
                    )
                    print(timings[-1], flush=True)
    finally:
        stop.set()
        thread.join()
    roster = json.loads(args.roster.read_text())
    cold = [r for r in timings if not r["cached"]]
    model_years = roster["model_years"]
    free = shutil.disk_usage(args.out_dir).free
    # Deliberately conservative state-unit throughput extrapolation; source-grid
    # size and compression vary by model, so this is a budget, not a guarantee.
    all_units = sum(
        len(gpd.read_file(args.data_root / f"{level}s_4326.geojson"))
        for level in ("district", "block")
    )
    pilot_units = sum(r["units"] for r in cold)
    factor = all_units / pilot_units * model_years
    cache_bytes = sum(
        f.stat().st_size for f in (args.out_dir / "cache").rglob("*") if f.is_file()
    )
    report = {
        "signature": SHADE_METHOD_SIGNATURE,
        "local_input_check": metadata,
        "timings": timings,
        "peak_rss_bytes": peak[0],
        "model_years": model_years,
        "national_units": all_units,
        "recommended_workers": 1,
        "estimated_national_seconds": sum(
            r["seconds"] + r["weight_seconds"] for r in cold
        )
        * factor,
        "estimated_cache_bytes": cache_bytes * factor,
        "free_bytes": free,
        "limitations": "Single model/year pilot; unit-scaled extrapolation excludes master/optimized staging and backups; not sufficient alone for publication disk approval.",
    }
    (args.out_dir / "report.json").write_text(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
