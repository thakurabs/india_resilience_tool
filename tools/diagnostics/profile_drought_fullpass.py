#!/usr/bin/env python3
"""Full-pass drought profiler: redundancy + within-drought split for ALL slugs.

Where ``profile_drought_realdata`` times ONE bundle, this times the *whole*
gridfirst drought slug set for one (model, scenario) so we can answer the
CHG-0111 gate questions before investing in a monthly-cube cache (CHG-0108):

    1. Redundancy factor  -- how many times the load+resample (the "cube") is
       rebuilt across the slug set that shares it. The pipeline forks per slug
       (``compute_indices_multiprocess`` -> ``compute_drought_risk_rows_for_metric``
       L442-443), so today the cube is rebuilt once per slug.
    2. Within-drought split -- cube (concat_years + daily_to_monthly_totals) vs
       SPI-fit (compute_spi_grid) vs grid aggregation (annual_spi_metric_grid),
       so we can see whether the cube is actually on the critical path or whether
       the SPI gamma fit dominates.
    3. Projected dedup ceilings -- realized speedup if the cube is cached once
       per (model, scenario, grid) [CHG-0108], and additionally if the SPI grid
       is cached once per scale [CHG-0109].

The cube is deterministic for a fixed (model, scenario, grid subset) and is
identical across all slugs that share it, so it is measured ONCE and multiplied
by the slug count rather than literally rebuilt N times (which would only prove
the same number N times). SPI-fit is measured once per distinct scale; grid
aggregation once per slug.

Scope / caveats (read-only -- loads NetCDFs, writes nothing):
- Times grid stages only. Polygon aggregation (stage 5) needs boundary weights
  and is out of scope here; for drought-compute's share of total wall, run the
  full pipeline with the prepare_dashboard per-stage timer (CHG-0097/0098).
- Loads the scenario year set only (mirrors profile_drought_realdata). For
  ``historical`` the SPI baseline (default 1981-2010) is a subset of the loaded
  years, so the cube/SPI are faithful. For ssp scenarios the historical baseline
  would need merging in; pass ``--scenario historical`` for the gate measurement.

Example
-------
    python -m tools.diagnostics.profile_drought_fullpass \
        --model CanESM5 --scenario historical
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from india_resilience_tool.compute.drought_risk_gridfirst import (
    DROUGHT_GRIDFIRST_ADMIN_ONLY_SLUGS,
    DROUGHT_GRIDFIRST_SLUGS,
    annual_spi_metric_grid,
    compute_spi_grid,
    daily_to_monthly_totals,
)
from india_resilience_tool.compute.gridfirst_spatial import concat_years
from india_resilience_tool.compute.spi_adapter import Distribution
from india_resilience_tool.config.paths import DATA_DIR
from tools.diagnostics.profile_drought_realdata import (
    DEFAULT_BUNDLE,
    discover_year_paths,
    parse_slug,
)


def _timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", type=Path, default=None,
                    help="Bundle root; defaults to DATA_DIR/<--bundle>")
    ap.add_argument("--bundle", default=DEFAULT_BUNDLE,
                    help=f"Run-bundle folder under DATA_DIR (default {DEFAULT_BUNDLE})")
    ap.add_argument("--model", default="CanESM5")
    ap.add_argument("--scenario", default="historical")
    ap.add_argument("--baseline", type=int, nargs=2, default=(1981, 2010),
                    metavar=("START", "END"))
    ap.add_argument("--years", type=int, nargs=2, default=None,
                    metavar=("START", "END"), help="subset of available years")
    args = ap.parse_args()

    data_root = args.data_root if args.data_root is not None else DATA_DIR / args.bundle
    slugs = sorted(DROUGHT_GRIDFIRST_SLUGS | DROUGHT_GRIDFIRST_ADMIN_ONLY_SLUGS)
    slug_meta = {s: parse_slug(s) for s in slugs}  # slug -> (scale, agg)
    scales = sorted({scale for scale, _ in slug_meta.values()})

    year_to_paths = discover_year_paths(data_root, args.scenario, args.model)
    years = sorted(year_to_paths)
    if args.years:
        lo, hi = args.years
        years = [y for y in years if lo <= y <= hi]

    print(
        f"Full-pass drought profile: model={args.model} scenario={args.scenario}\n"
        f"Slugs: {len(slugs)} ({', '.join(slugs)})\n"
        f"Scales present: {scales}\n"
        f"Years: {years[0]}-{years[-1]} ({len(years)} files), baseline={tuple(args.baseline)}\n"
        f"Data root: {data_root}"
    )

    # --- Cube: measured ONCE (shared across all slugs) ---------------------
    t_load, da = _timed(lambda: concat_years(year_to_paths, "pr", years))
    print(f"\n  grid: lat={da.sizes.get('lat')} lon={da.sizes.get('lon')} "
          f"time={da.sizes.get('time')} (daily)")
    t_resample, monthly = _timed(lambda: daily_to_monthly_totals(da))
    cube_s = t_load + t_resample
    n_cells = int(monthly.sizes.get("lat", 0)) * int(monthly.sizes.get("lon", 0))
    print(f"  monthly cube: {n_cells} cells x {monthly.sizes.get('time')} months "
          f"| cube={cube_s:.2f}s (load={t_load:.2f}s + resample={t_resample:.2f}s)")

    # --- SPI-fit: measured ONCE per distinct scale -------------------------
    spi_by_scale: dict[int, object] = {}
    spi_fit_s: dict[int, float] = {}
    for scale in scales:
        s, spi = _timed(lambda sc=scale: compute_spi_grid(
            monthly, baseline_years=tuple(args.baseline),
            scale_months=sc, distribution=Distribution.GAMMA))
        spi_by_scale[scale] = spi
        spi_fit_s[scale] = s
        print(f"  SPI-fit scale={scale:>2}: {s:.2f}s")

    # --- Grid aggregation: measured ONCE per slug --------------------------
    agg_s: dict[str, float] = {}
    for slug in slugs:
        scale, agg = slug_meta[slug]
        s, _ = _timed(lambda sc=scale, ag=agg: annual_spi_metric_grid(
            spi_by_scale[sc], annual_aggregation=ag, threshold=-1.0,
            min_months_per_year=9, min_event_months=1))
        agg_s[slug] = s

    # --- Compose the three scenarios --------------------------------------
    n_slugs = len(slugs)
    agg_total = sum(agg_s.values())
    spi_fit_total_perscale = sum(spi_fit_s.values())
    spi_fit_total_perslug = sum(spi_fit_s[slug_meta[s][0]] for s in slugs)

    # today: per-slug fork rebuilds cube AND spi-fit for every slug
    today = n_slugs * cube_s + spi_fit_total_perslug + agg_total
    # CHG-0108: cube cached once; spi-fit still per slug
    cube_cached = cube_s + spi_fit_total_perslug + agg_total
    # CHG-0108 + CHG-0109: cube once, spi-fit once per scale
    cube_and_spi_cached = cube_s + spi_fit_total_perscale + agg_total

    print("\n==== within-drought split (one full pass, grid stages) ====")
    print(f"  cube (load+resample), per build      : {cube_s:7.2f}s")
    print(f"  SPI-fit, per scale ({len(scales)} scales)        : "
          f"{spi_fit_total_perscale:7.2f}s  ({spi_fit_s})")
    print(f"  grid aggregation, sum over {n_slugs} slugs : {agg_total:7.2f}s")

    print("\n==== redundancy (today, per-slug fork) ====")
    print(f"  cube rebuilds            : {n_slugs}x  -> {n_slugs * cube_s:7.2f}s "
          f"({n_slugs * cube_s / today * 100:.0f}% of drought compute)")
    print(f"  SPI-fit rebuilds         : {n_slugs}x  -> {spi_fit_total_perslug:7.2f}s "
          f"({spi_fit_total_perslug / today * 100:.0f}%)")
    print(f"  grid aggregation         : {n_slugs}x  -> {agg_total:7.2f}s "
          f"({agg_total / today * 100:.0f}%)")
    print(f"  TOTAL drought grid compute (today)     : {today:7.2f}s")

    print("\n==== projected dedup ceilings (this state/model/scenario) ====")
    print(f"  CHG-0108 cube cached once : {today:.2f}s -> {cube_cached:.2f}s "
          f"=> {today / cube_cached:.2f}x")
    print(f"  + CHG-0109 SPI per scale  : {today:.2f}s -> {cube_and_spi_cached:.2f}s "
          f"=> {today / cube_and_spi_cached:.2f}x")
    print("\n  Gate: proceed with CHG-0108 only if the cube rebuild % above is a "
          "material share of drought compute AND drought compute is a material\n"
          "  share of total wall (run prepare_dashboard with the CHG-0097/0098 "
          "stage timer for that share).")


if __name__ == "__main__":
    main()
