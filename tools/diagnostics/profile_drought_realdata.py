#!/usr/bin/env python3
"""Stage-by-stage wall-clock timer for ONE real Drought Risk bundle.

Times the grid stages of ``compute_drought_risk_rows_for_metric`` on actual
precipitation NetCDFs for a single (model, scenario), so we can see where the
time really goes on real data and bound the achievable speedup. Read-only:
loads NetCDFs, writes nothing to processed outputs.

Stages timed (1-4; stage 5 polygon aggregation needs boundary weights and is
reported as a follow-up):
    1. concat_years            -- load + concat yearly daily precip
    2. daily_to_monthly_totals -- daily -> monthly totals
    3. compute_spi_grid        -- per-cell gamma fit (raw gate-vs-fit split)
    4. annual_spi_metric_grid  -- annual run-count, all 3 SPI sibling aggs

Example
-------
    python -m tools.diagnostics.profile_drought_realdata \
        --model CanESM5 --scenario historical \
        --slug spi3_count_events_lt_minus1
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np

import india_resilience_tool.compute.drought_risk_gridfirst as drg
from india_resilience_tool.compute.drought_risk_gridfirst import (
    annual_spi_metric_grid,
    compute_spi_grid,
    daily_to_monthly_totals,
)
from india_resilience_tool.compute.gridfirst_spatial import concat_years
from india_resilience_tool.compute.spi_adapter import Distribution
from india_resilience_tool.config.paths import DATA_DIR

# Default run-bundle folder under DATA_DIR (honors IRT_DATA_DIR; platform-correct
# on both WSL and native Windows). Override with --data-root or --bundle.
DEFAULT_BUNDLE = "r1i1p1f1_telangana"


def default_data_root() -> Path:
    """Resolve the default bundle root from the repo's DATA_DIR resolution."""
    return DATA_DIR / DEFAULT_BUNDLE

# slug -> (scale_months, annual_aggregation)
_AGG_BY_SUFFIX = {
    "count_events_lt_minus1": "count_events_lt",
    "max_spell_lt_minus1": "max_spell_lt",
    "count_months_lt_minus1": "count_months_lt",
}


def parse_slug(slug: str) -> tuple[int, str]:
    """Return (scale_months, annual_aggregation) from a drought SPI slug."""
    if slug.startswith("spi3"):
        scale = 3
    elif slug.startswith("spi6"):
        scale = 6
    elif slug.startswith("spi12"):
        scale = 12
    else:
        raise SystemExit(f"Cannot parse SPI scale from slug: {slug}")
    for suffix, agg in _AGG_BY_SUFFIX.items():
        if slug.endswith(suffix):
            return scale, agg
    raise SystemExit(f"Cannot parse aggregation from slug: {slug}")


def discover_year_paths(data_root: Path, scenario: str, model: str) -> dict[int, dict[str, Path]]:
    """Glob {data_root}/{scenario}/pr/{model}/{year}.nc into year_to_paths."""
    pr_dir = data_root / scenario / "pr" / model
    if not pr_dir.is_dir():
        raise SystemExit(f"Precip dir not found: {pr_dir}")
    out: dict[int, dict[str, Path]] = {}
    for p in sorted(pr_dir.glob("*.nc")):
        try:
            out[int(p.stem)] = {"pr": p}
        except ValueError:
            continue
    if not out:
        raise SystemExit(f"No yearly .nc files under {pr_dir}")
    return out


def _timed(label: str, fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result, label


def time_spi_grid_with_split(monthly, *, baseline_years, scale_months):
    """Run compute_spi_grid with raw timers on the gate and the fit.

    Monkeypatches the module-level ``_baseline_coverage_grid`` and
    ``_compute_spi_series`` so we get true (non-cProfile) per-component totals.
    The gate is now vectorized and called once per grid, so ``gate_n`` is 1.
    """
    acc = {"gate_s": 0.0, "gate_n": 0, "fit_s": 0.0, "fit_n": 0}
    orig_gate = drg._baseline_coverage_grid
    orig_fit = drg._compute_spi_series

    def gate(*a, **k):
        t = time.perf_counter()
        try:
            return orig_gate(*a, **k)
        finally:
            acc["gate_s"] += time.perf_counter() - t
            acc["gate_n"] += 1

    def fit(*a, **k):
        t = time.perf_counter()
        try:
            return orig_fit(*a, **k)
        finally:
            acc["fit_s"] += time.perf_counter() - t
            acc["fit_n"] += 1

    drg._baseline_coverage_grid = gate
    drg._compute_spi_series = fit
    t0 = time.perf_counter()
    try:
        spi = compute_spi_grid(
            monthly,
            baseline_years=baseline_years,
            scale_months=scale_months,
            distribution=Distribution.GAMMA,
        )
    finally:
        drg._baseline_coverage_grid = orig_gate
        drg._compute_spi_series = orig_fit
    wall = time.perf_counter() - t0
    return wall, spi, acc


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", type=Path, default=None,
                    help="Bundle root; defaults to DATA_DIR/<--bundle> "
                         "(DATA_DIR honors IRT_DATA_DIR)")
    ap.add_argument("--bundle", default=DEFAULT_BUNDLE,
                    help=f"Run-bundle folder under DATA_DIR (default {DEFAULT_BUNDLE})")
    ap.add_argument("--model", default="CanESM5")
    ap.add_argument("--scenario", default="historical")
    ap.add_argument("--slug", default="spi3_count_events_lt_minus1")
    ap.add_argument("--baseline", type=int, nargs=2, default=(1981, 2010),
                    metavar=("START", "END"))
    ap.add_argument("--years", type=int, nargs=2, default=None,
                    metavar=("START", "END"), help="subset of available years")
    args = ap.parse_args()

    data_root = args.data_root if args.data_root is not None else DATA_DIR / args.bundle
    scale_months, annual_agg = parse_slug(args.slug)
    year_to_paths = discover_year_paths(data_root, args.scenario, args.model)
    years = sorted(year_to_paths)
    if args.years:
        lo, hi = args.years
        years = [y for y in years if lo <= y <= hi]
    print(
        f"Bundle: model={args.model} scenario={args.scenario} slug={args.slug} "
        f"scale={scale_months} agg={annual_agg}\n"
        f"Years: {years[0]}-{years[-1]} ({len(years)} files), baseline={tuple(args.baseline)}\n"
        f"Data root: {data_root}"
    )

    timings: list[tuple[str, float]] = []

    # Stage 1: load + concat daily precip
    s1, da, _ = _timed("1. concat_years (load+concat)",
                        lambda: concat_years(year_to_paths, "pr", years))
    timings.append(("1. concat_years (load+concat)", s1))
    print(f"  grid: lat={da.sizes.get('lat')} lon={da.sizes.get('lon')} "
        f"time={da.sizes.get('time')} (daily)")

    # Stage 2: daily -> monthly totals
    s2, monthly, _ = _timed("2. daily_to_monthly_totals",
                            lambda: daily_to_monthly_totals(da))
    timings.append(("2. daily_to_monthly_totals", s2))
    n_cells = int(monthly.sizes.get("lat", 0)) * int(monthly.sizes.get("lon", 0))
    print(f"  monthly cube: {n_cells} cells x {monthly.sizes.get('time')} months")

    # Stage 3: compute_spi_grid with raw gate-vs-fit split
    s3, spi, acc = time_spi_grid_with_split(
        monthly, baseline_years=tuple(args.baseline), scale_months=scale_months)
    timings.append(("3. compute_spi_grid (gamma fit)", s3))
    overhead3 = s3 - acc["gate_s"] - acc["fit_s"]
    print(
        f"  [raw split] _baseline_coverage_ok: {acc['gate_s']:.2f}s "
        f"({acc['gate_s'] / s3 * 100:.0f}%, n={acc['gate_n']}) | "
        f"_compute_spi_series (fit): {acc['fit_s']:.2f}s "
        f"({acc['fit_s'] / s3 * 100:.0f}%, n={acc['fit_n']}) | "
        f"other: {overhead3:.2f}s ({overhead3 / s3 * 100:.0f}%)"
    )

    # Stage 4: annual aggregation for all 3 SPI sibling aggregations
    s4_by_agg: dict[str, float] = {}
    for agg in ("count_events_lt", "max_spell_lt", "count_months_lt"):
        s, _, _ = _timed(f"4. annual_spi_metric_grid [{agg}]",
                        lambda a=agg: annual_spi_metric_grid(
                            spi, annual_aggregation=a, threshold=-1.0,
                            min_months_per_year=9, min_event_months=1))
        s4_by_agg[agg] = s
    s4 = s4_by_agg[annual_agg]
    timings.append((f"4. annual_spi_metric_grid [{annual_agg}]", s4))

    # --- Summary ----------------------------------------------------------
    total = sum(s for _, s in timings)
    print("\n==== stage breakdown (one bundle, stages 1-4) ====")
    for label, s in timings:
        print(f"  {label:42s} {s:7.2f}s  ({s / total * 100:4.1f}%)")
    print(f"  {'TOTAL (stages 1-4)':42s} {total:7.2f}s")
    print("  5. polygon aggregation             : not timed (needs boundary weights; minor per synthetic profile)")

    # --- Projected speedup ------------------------------------------------
    gate_removed = max(acc["fit_s"] + overhead3, 0.0)  # stage 3 if gate -> ~0
    total_after_gate = total - s3 + gate_removed
    print("\n==== projected speedup (this bundle) ====")
    print(f"  If gate hoisted (stage 3 -> fit+other): "
        f"{total:.2f}s -> {total_after_gate:.2f}s "
        f"({total / total_after_gate:.2f}x)")

    # de-dup: 3 SPI-3 siblings share one SPI grid
    siblings = sum(s4_by_agg.values())
    print(f"  3 SPI-3 sibling slugs, no grid cache: ~{3 * s3 + siblings:.2f}s "
        f"(3x stage3 + 3 aggs)")
    print(f"  3 SPI-3 sibling slugs, grid cached:   ~{s3 + siblings:.2f}s "
        f"(1x stage3 + 3 aggs) "
        f"=> {(3 * s3 + siblings) / (s3 + siblings):.2f}x")


if __name__ == "__main__":
    main()