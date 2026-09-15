#!/usr/bin/env python3
"""Read-only cProfile harness for the Drought Risk grid-first SPI hot path.

Purpose
-------
Resolve the single open question behind the ~41h pipeline estimate:
is the per-cell cost the *Python loop machinery* or the *C gamma solver*
inside ``climate_indices.indices.spi``? cProfile's tottime/cumtime split
answers it, and the de-dup micro-benchmark bounds the sibling-slug win.

This script is self-contained: it synthesises a realistic monthly
precipitation cube (no processed-data-dir or boundary dependency) sized so
the baseline-coverage gate passes and the gamma fit actually runs per cell.
It writes nothing and mutates nothing.

Examples
--------
    # Default 25x25 grid, 1981-2014 monthly
    python -m tools.diagnostics.profile_drought_spi

    # Match a real district bundle's grid and horizon
    python -m tools.diagnostics.profile_drought_spi --lat 40 --lon 40 \
        --start-year 1981 --end-year 2100
"""

from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import time
from pstats import SortKey

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import (
    annual_spi_metric_grid,
    compute_spi_grid,
)
from india_resilience_tool.compute.spi_adapter import (
    CLIMATE_INDICES_AVAILABLE,
    Distribution,
)


def build_precip_cube(
    *,
    n_lat: int,
    n_lon: int,
    start_year: int,
    end_year: int,
    seed: int = 0,
) -> xr.DataArray:
    """Synthesise a gamma-distributed monthly precip cube (mm/month).

    Values are strictly positive with a mild seasonal cycle so the
    climate-indices gamma fit converges on every cell — this guarantees we
    profile the fit itself rather than the ``_baseline_coverage_ok`` skip path.
    """
    rng = np.random.default_rng(seed)
    times = pd.date_range(f"{start_year}-01-01", f"{end_year}-12-01", freq="MS")
    n_time = len(times)

    # Seasonal mean (monsoon-ish): higher Jun-Sep.
    month = times.month.to_numpy()
    seasonal = 40.0 + 80.0 * np.exp(-((month - 7.5) ** 2) / 6.0)  # (time,)

    # Gamma noise per cell around the seasonal mean.
    shape_k = 2.0
    scale = (seasonal / shape_k)[:, None, None]  # (time,1,1)
    data = rng.gamma(shape_k, 1.0, size=(n_time, n_lat, n_lon)) * scale

    lat = np.linspace(16.0, 20.0, n_lat)
    lon = np.linspace(77.0, 81.0, n_lon)
    da = xr.DataArray(
        data.astype(float),
        coords={"time": times, "lat": lat, "lon": lon},
        dims=("time", "lat", "lon"),
        name="pr",
    )
    da.attrs["units"] = "mm"
    return da


def _profile(label: str, fn, *, top: int = 25) -> tuple[float, str]:
    """Run ``fn`` under cProfile; return (wall_seconds, formatted_report)."""
    prof = cProfile.Profile()
    t0 = time.perf_counter()
    prof.enable()
    fn()
    prof.disable()
    wall = time.perf_counter() - t0

    buf = io.StringIO()
    stats = pstats.Stats(prof, stream=buf)
    buf.write(f"\n==== {label} (wall {wall:.2f}s) — by tottime ====\n")
    stats.sort_stats(SortKey.TIME).print_stats(top)
    buf.write(f"\n==== {label} — by cumtime ====\n")
    stats.sort_stats(SortKey.CUMULATIVE).print_stats(top)

    # Pull the cumulative time attributed to the climate-indices spi() call,
    # which is the proxy for "time inside the C/scipy gamma solver".
    solver_cum = 0.0
    for (filename, _, func), (_, _, _, ct, _) in stats.stats.items():  # type: ignore[misc]
        if func == "spi" and "climate_indices" in filename:
            solver_cum += ct
    share = (solver_cum / wall * 100.0) if wall > 0 else float("nan")
    buf.write(
        f"\n>>> climate_indices.spi cumulative: {solver_cum:.2f}s "
        f"({share:.1f}% of {label} wall)\n"
        f">>> Interpretation: high % => solver-bound (vectorize via batched "
        f"per-month fit; ~2-5x). low % => Python-loop-bound (reshape+vectorize; ~10x+).\n"
    )
    return wall, buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lat", type=int, default=25, help="grid rows (default 25)")
    ap.add_argument("--lon", type=int, default=25, help="grid cols (default 25)")
    ap.add_argument("--start-year", type=int, default=1981)
    ap.add_argument("--end-year", type=int, default=2014)
    ap.add_argument("--scale-months", type=int, default=3, help="SPI scale (default 3)")
    ap.add_argument("--top", type=int, default=25, help="rows per pstats table")
    args = ap.parse_args()

    if not CLIMATE_INDICES_AVAILABLE:
        raise SystemExit("climate-indices not installed; cannot profile the SPI path.")

    cube = build_precip_cube(
        n_lat=args.lat,
        n_lon=args.lon,
        start_year=args.start_year,
        end_year=args.end_year,
    )
    n_cells = args.lat * args.lon
    n_months = cube.sizes["time"]
    print(
        f"Synthetic cube: {args.lat}x{args.lon} = {n_cells} cells, "
        f"{n_months} months ({args.start_year}-{args.end_year}), "
        f"SPI-{args.scale_months}, dist=gamma"
    )

    # --- Hot path 1: per-cell gamma fit loop -------------------------------
    spi_holder: dict[str, xr.DataArray] = {}

    def _fit() -> None:
        spi_holder["spi"] = compute_spi_grid(
            cube,
            baseline_years=(args.start_year, 2010),
            scale_months=args.scale_months,
            distribution=Distribution.GAMMA,
        )

    fit_wall, fit_report = _profile("compute_spi_grid (gamma fit loop)", _fit, top=args.top)
    print(fit_report)
    spi = spi_holder["spi"]
    finite = int(np.isfinite(spi.values).sum())
    print(f"(sanity) finite SPI values: {finite} / {spi.size}")

    # --- Hot path 2: annual run-count loop, all 3 sibling aggregations -----
    def _annual(agg: str):
        return lambda: annual_spi_metric_grid(
            spi,
            annual_aggregation=agg,
            threshold=-1.0,
            min_months_per_year=9,
            min_event_months=1,
        )

    for agg in ("count_events_lt", "max_spell_lt", "count_months_lt"):
        wall, report = _profile(f"annual_spi_metric_grid [{agg}]", _annual(agg), top=args.top)
        print(report)

    # --- Lever #2: de-dup bound (fit once vs three sibling SPI-3 metrics) --
    t0 = time.perf_counter()
    for _ in range(3):
        compute_spi_grid(
            cube,
            baseline_years=(args.start_year, 2010),
            scale_months=args.scale_months,
            distribution=Distribution.GAMMA,
        )
    three_fits = time.perf_counter() - t0
    print("\n==== de-dup bound (lever #2) ====")
    print(f"3x gamma fit (no cache): {three_fits:.2f}s")
    print(f"1x gamma fit (cached):   {fit_wall:.2f}s")
    print(
        f">>> de-dup saves ~{three_fits - fit_wall:.2f}s per (model,scenario,scale) "
        f"=> {three_fits / fit_wall:.2f}x on the drought-SPI-3 fit portion IF the "
        f"grid is not already cached at (model,scale,scenario) granularity."
    )


if __name__ == "__main__":
    main()
