#!/usr/bin/env python3
"""Read-only per-cell trace of the four gates that NaN SPI3 in arid districts.

Purpose
-------
Resolve FLAG B of ``docs/metric_distribution_review.md`` empirically: for a real
arid district (e.g. Jaisalmer), *which* of the four grid-first drought gates
actually emits the district-level NaN, and by how much? The answer picks the fix:

    * Gate 2 (gamma fit) dominates  -> no SPI signal mathematically exists ->
      only "accept + annotate" or "substitute an arid-robust index" are viable.
    * Gates 3/4 (coverage thresholds) are the deciding cut on cells that DID
      produce finite SPI -> the signal exists and our own gates discarded it ->
      "relax / redesign the gate" is legitimate.

The four gates, in the order the pipeline applies them
(``compute/drought_risk_gridfirst.py``):

    1. baseline coverage   (_baseline_coverage_grid; keys on missingness)
    2. gamma fit           (compute_spi_grid -> climate_indices.indices.spi)
    3. min valid months/yr (annual_spi_metric_grid; < min_months_per_year -> NaN)
    4. polygon weight floor (aggregate_grid_values_with_retention; retained<0.50)

This script REPRODUCES the exact pipeline stages (same helpers, same defaults)
on real precipitation NetCDFs and emits per-gate cell/district telemetry. It is
strictly READ-ONLY: it loads NetCDFs + one boundary file and writes NOTHING to
processed outputs. It only writes a CSV when you explicitly pass ``--out-csv``.

Environment
-----------
Requires the Windows ``irt`` conda env (geopandas/pyproj/climate-indices). WSL
python3 will not have the geo stack. Honors ``IRT_DATA_DIR`` via ``config.paths``.

Examples
--------
    # One arid district, historical baseline, default SPI3 events slug
    python -m tools.diagnostics.drought_spi_arid_nan_trace \
        --state Rajasthan --district Jaisalmer \
        --model CanESM5 --scenario historical

    # Compare a wet control district
    python -m tools.diagnostics.drought_spi_arid_nan_trace \
        --state Meghalaya --district "East Khasi Hills" \
        --model CanESM5 --scenario historical

    # All districts in a state (bbox = state bbox), dump per-cell CSV
    python -m tools.diagnostics.drought_spi_arid_nan_trace \
        --state Rajasthan --out-csv rajasthan_spi3_cells.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import (
    _baseline_coverage_grid,
    _to_contiguous_monthly_index,
    _trim_to_full_calendar_years,
    aggregate_grid_values_with_retention,
    annual_spi_metric_grid,
    compute_spi_grid,
    daily_to_monthly_totals,
    period_rollup_grid,
)
from india_resilience_tool.compute.gridfirst_spatial import (
    bbox_to_index_range,
    build_area_weights,
    concat_years,
    dataset_grid_spec,
    open_year_dataarray,
    subset_grid_by_index,
)
from india_resilience_tool.compute.spi_adapter import (
    CLIMATE_INDICES_AVAILABLE,
    Distribution,
    compute_spi_climate_indices,
)
from india_resilience_tool.config.paths import DATA_DIR, DISTRICTS_PATH

MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# slug suffix -> annual aggregation (mirror of profile_drought_realdata.py)
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


def load_district_boundaries(state: str, district: str | None) -> gpd.GeoDataFrame:
    """Load district polygons, filtered to one state and (optionally) one district.

    Mirrors ``load_boundaries``' state-column resolution + ``district_name``
    standardization without importing the heavy pipeline module.
    """
    gdf = gpd.read_file(DISTRICTS_PATH)
    state_cols = ["STATE_UT", "state_ut", "STATE", "STATE_LGD", "ST_NM", "state_name"]
    state_col = next((c for c in state_cols if c in gdf.columns), None)
    if not state_col:
        raise SystemExit(f"No state column in {DISTRICTS_PATH}")
    norm = (
        gdf[state_col]
        .astype(str)
        .str.normalize("NFKC")
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    gdf = gdf[norm == state.strip().lower()].copy()
    if gdf.empty:
        raise SystemExit(f"No rows for state={state!r} in {DISTRICTS_PATH}")

    # Standardize district_name (same fallbacks as _standardize_district_columns).
    if "district_name" not in gdf.columns:
        for cand in ["DISTRICT", "District", "DIST_NAME", "district"]:
            if cand in gdf.columns:
                gdf["district_name"] = gdf[cand].astype(str).str.strip()
                break
    if "district_name" not in gdf.columns:
        raise SystemExit(f"No district-name column in {DISTRICTS_PATH}")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    if district:
        dnorm = gdf["district_name"].astype(str).str.strip().str.lower()
        sel = gdf[dnorm == district.strip().lower()].copy()
        if sel.empty:
            available = ", ".join(sorted(gdf["district_name"].astype(str).unique())[:40])
            raise SystemExit(
                f"District {district!r} not found in {state}. Available: {available}"
            )
        gdf = sel
    return gdf


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


def sample_grid_axes(sample_path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Return (lat, lon) 1D coordinate arrays from one precip file (no subset)."""
    da = open_year_dataarray(sample_path, "pr", index_range=None)
    return (
        np.asarray(da["lat"].values, dtype=float),
        np.asarray(da["lon"].values, dtype=float),
    )


def pct(numer: int, denom: int) -> str:
    return f"{(100.0 * numer / denom):5.1f}%" if denom else "  n/a"


def trace(args: argparse.Namespace) -> None:
    scale_months, annual_agg = parse_slug(args.slug)
    data_root = args.data_root if args.data_root is not None else DATA_DIR / args.bundle
    baseline = tuple(int(v) for v in args.baseline)

    gdf = load_district_boundaries(args.state, args.district)
    scope = args.district or f"{args.state} (all districts)"
    print("=" * 78)
    print(f"FLAG B SPI arid-NaN trace | scope: {scope}")
    print(f"  slug={args.slug} scale={scale_months} agg={annual_agg} dist=gamma")
    print(f"  model={args.model} scenario={args.scenario} baseline={baseline}")
    print(f"  data_root={data_root}")
    print(f"  min_months_per_year={args.min_months_per_year} "
          f"min_polygon_cell_weight_fraction={args.min_polygon_fraction} "
          f"min_baseline_month_fraction={args.min_baseline_fraction}")
    print("=" * 78)

    # --- Resolve the bbox subset (identical to the pipeline's grid_index_range) ---
    year_to_paths = discover_year_paths(data_root, args.scenario, args.model)
    years = sorted(year_to_paths)
    if args.years:
        lo, hi = args.years
        years = [y for y in years if lo <= y <= hi]
    if not years:
        raise SystemExit("No precip years selected after --years filter.")
    sample_lat, sample_lon = sample_grid_axes(year_to_paths[years[0]]["pr"])
    index_range = bbox_to_index_range(sample_lat, sample_lon, tuple(gdf.total_bounds))

    # --- Load precip -> monthly cube on the bbox subset (pipeline-faithful) ---
    daily = concat_years(year_to_paths, "pr", years, index_range=index_range)
    monthly_precip = daily_to_monthly_totals(daily, min_daily_coverage=args.min_daily_coverage)
    grid = dataset_grid_spec(monthly_precip.to_dataset(name="pr"))
    n_lat = len(grid.lat)
    n_lon = len(grid.lon)
    print(f"\nGrid subset: {n_lat} x {n_lon} = {n_lat * n_lon} cells | "
          f"precip years {years[0]}-{years[-1]} ({len(years)} files)")

    # --- Trim exactly as compute_spi_grid does, then recompute Gate 1 mask ---
    monthly = _trim_to_full_calendar_years(_to_contiguous_monthly_index(monthly_precip))
    times = pd.DatetimeIndex(monthly["time"].values)
    vals = np.asarray(monthly.values, dtype=float)
    coverage_ok = _baseline_coverage_grid(
        vals, times,
        baseline_years=baseline,
        min_fraction=args.min_baseline_fraction,
    )  # (lat, lon) bool
    n_cells = n_lat * n_lon
    n_gate1_ok = int(coverage_ok.sum())

    # =========================== GATE 1 ===========================
    print("\n--- GATE 1: baseline coverage (missingness, not dryness) ---")
    print(f"  cells passing coverage gate: {n_gate1_ok}/{n_cells} "
          f"({pct(n_gate1_ok, n_cells)}) | failing -> stay all-NaN")

    # =========================== GATE 2 ===========================
    spi = compute_spi_grid(
        monthly_precip,
        baseline_years=baseline,
        scale_months=scale_months,
        distribution=Distribution.GAMMA,
        min_baseline_years_per_calendar_month_fraction=args.min_baseline_fraction,
    )
    spi_vals = np.asarray(spi.values, dtype=float)  # (time, lat, lon)
    finite_mon = np.isfinite(spi_vals)

    # Among Gate-1-OK cells: fully-NaN vs partial vs fully-finite SPI series.
    ok_flat = coverage_ok.reshape(-1)
    finite_per_cell = finite_mon.reshape(finite_mon.shape[0], -1).sum(axis=0)  # (cell,)
    n_time = spi_vals.shape[0]
    ok_idx = np.nonzero(ok_flat)[0]
    fully_nan = int(np.sum(finite_per_cell[ok_idx] == 0))
    fully_fin = int(np.sum(finite_per_cell[ok_idx] == n_time))
    partial = len(ok_idx) - fully_nan - fully_fin
    print("\n--- GATE 2: gamma fit (climate_indices.indices.spi) ---")
    print(f"  of {len(ok_idx)} Gate-1-OK cells, monthly-SPI series are:")
    print(f"    fully NaN (gamma degenerate): {fully_nan:5d} ({pct(fully_nan, len(ok_idx))})")
    print(f"    partially NaN             : {partial:5d} ({pct(partial, len(ok_idx))})")
    print(f"    fully finite              : {fully_fin:5d} ({pct(fully_fin, len(ok_idx))})")
    print("  NOTE: the first (scale-1) months of each series are NaN by SPI")
    print("        construction (no accumulation window yet) — a boundary")
    print("        artifact, distinct from gamma degeneracy below.")

    # P3: a fully-NaN cell is NOT proof of gamma degeneracy — _compute_spi_series
    # catches ANY exception and returns all-NaN. Re-run the raw fit per Gate-1-OK
    # cell (climate_indices raises on hard failure) to split the two causes.
    n_exc = 0
    n_ret_all_nan = 0
    n_ret_some_finite = 0
    for cell in ok_idx:
        li, lo = divmod(int(cell), n_lon)
        try:
            series = compute_spi_climate_indices(
                monthly_precip=vals[:, li, lo],
                data_start_year=int(times[0].year),
                calibration_start_year=int(baseline[0]),
                calibration_end_year=int(baseline[1]),
                scale_months=scale_months,
                distribution=Distribution.GAMMA,
            )
        except Exception:
            n_exc += 1
            continue
        if np.isfinite(np.asarray(series, dtype=float)).any():
            n_ret_some_finite += 1
        else:
            n_ret_all_nan += 1
    print("\n  Gate-2 cause split across Gate-1-OK cells "
          "(returned-NaN vs caught-exception):")
    print(f"    climate_indices raised (caught -> all-NaN): {n_exc:5d} "
          f"({pct(n_exc, len(ok_idx))})  <- NOT gamma degeneracy")
    print(f"    returned all-NaN (gamma degenerate)       : {n_ret_all_nan:5d} "
          f"({pct(n_ret_all_nan, len(ok_idx))})")
    print(f"    returned >=1 finite month                 : {n_ret_some_finite:5d} "
          f"({pct(n_ret_some_finite, len(ok_idx))})")

    # Per-calendar-month NaN rate across Gate-1-OK cells: the dry-season signature.
    print("\n  per-calendar-month SPI NaN rate across Gate-1-OK cells "
          "(high in dry months == gamma degeneracy):")
    month_of = times.month.to_numpy()
    ok_mask_3d = coverage_ok[None, :, :]
    for m in range(1, 13):
        tsel = month_of == m
        block = spi_vals[tsel]  # (n_m, lat, lon)
        okb = np.broadcast_to(ok_mask_3d, block.shape)
        denom = int(okb.sum())
        nan_ct = int((~np.isfinite(block) & okb).sum())
        bar = "#" * int(round(40 * nan_ct / denom)) if denom else ""
        print(f"    {MONTH_ABBR[m - 1]}: {pct(nan_ct, denom)}  {bar}")

    # =========================== GATE 3 ===========================
    # Per cell-year valid-month count; how many cell-years drop under the floor.
    print("\n--- GATE 3: min valid months/year (annual_spi_metric_grid) ---")
    year_of = times.year.to_numpy()
    uniq_years = np.unique(year_of)
    dropped_cy = 0
    total_cy = 0
    cells_zero_valid_years = 0
    valid_years_per_cell = np.zeros(len(ok_idx), dtype=int)
    for ci, cell in enumerate(ok_idx):
        li, lo = divmod(int(cell), n_lon)
        vy = 0
        for y in uniq_years:
            tsel = year_of == y
            vcount = int(finite_mon[tsel, li, lo].sum())
            total_cy += 1
            if vcount < int(args.min_months_per_year):
                dropped_cy += 1
            else:
                vy += 1
        valid_years_per_cell[ci] = vy
        if vy == 0:
            cells_zero_valid_years += 1
    print(f"  cell-years dropped (<{args.min_months_per_year} valid months): "
          f"{dropped_cy}/{total_cy} ({pct(dropped_cy, total_cy)})")
    print(f"  Gate-1-OK cells left with ZERO valid years: "
          f"{cells_zero_valid_years}/{len(ok_idx)} ({pct(cells_zero_valid_years, len(ok_idx))})")

    annual_ds = annual_spi_metric_grid(
        spi,
        annual_aggregation=annual_agg,
        threshold=float(args.threshold),
        min_months_per_year=int(args.min_months_per_year),
        min_event_months=int(args.min_event_months),
    )
    # A cell is finite-in-annual for a year if it cleared Gate 3 that year.
    ann_vals = np.asarray(annual_ds["value"].values, dtype=float)  # (year, lat, lon)
    ann_finite_any = np.isfinite(ann_vals).any(axis=0).reshape(-1)
    n_cells_annual_finite = int(ann_finite_any[ok_idx].sum())
    print(f"  Gate-1-OK cells with >=1 finite annual value (survive to Gate 4): "
          f"{n_cells_annual_finite}/{len(ok_idx)} ({pct(n_cells_annual_finite, len(ok_idx))})")

    # =========================== GATE 3.5 ===========================
    # The audited artifact is a PERIOD value (e.g. 1990-2010), which passes through
    # period_rollup_grid's year-coverage floor BEFORE polygon aggregation. Yearly
    # rows skip this gate; the audited period rows do not.
    p0, p1 = int(args.period_years[0]), int(args.period_years[1])
    period_name = f"{p0}-{p1}"
    requested = p1 - p0 + 1
    required_years = int(np.ceil(float(args.min_years_per_period_fraction) * requested))
    print("\n--- GATE 3.5: period-year coverage floor (period_rollup_grid, "
          f">= {args.min_years_per_period_fraction}) ---")
    period_ds = period_rollup_grid(
        annual_ds["value"],
        period_name=period_name,
        years=(p0, p1),
        rollup=args.period_rollup,
        min_years_per_period_fraction=float(args.min_years_per_period_fraction),
    )
    pval_finite = np.isfinite(
        np.asarray(period_ds["value"].values, dtype=float).reshape(-1)
    )
    cells_pass_3_5 = int(pval_finite[ok_idx].sum())
    print(f"  audited period: {period_name} (rollup={args.period_rollup}, "
          f"requested {requested} yrs, need >= {required_years} finite)")
    print(f"  Gate-1-OK cells with finite PERIOD value (clear 3.5): "
          f"{cells_pass_3_5}/{len(ok_idx)} ({pct(cells_pass_3_5, len(ok_idx))})")
    print(f"  finite-in-annual but killed at 3.5: "
          f"{int(n_cells_annual_finite - cells_pass_3_5)} "
          "(had some finite years but < the period floor)")

    # =========================== GATE 4 ===========================
    print("\n--- GATE 4: polygon retained-weight floor "
          f"(>= {args.min_polygon_fraction}) on the PERIOD artifact ---")
    weights = build_area_weights(gdf, grid, level="district")
    if weights.empty:
        print("  build_area_weights returned EMPTY (no cell overlaps polygons). "
              "Check bbox/CRS.")
        return
    per_unit = aggregate_grid_values_with_retention(
        period_ds["value"],
        weights,
        min_polygon_cell_weight_fraction=float(args.min_polygon_fraction),
        grid=grid,
    )
    print(f"  {'district':32s} {'period value':>13s} {'retained':>9s}  verdict")
    rows_for_csv: list[dict[str, object]] = []
    for unit, (value, retained) in sorted(per_unit.items()):
        is_nan = not np.isfinite(value)
        verdict = "NaN (gate4)" if (is_nan and retained >= args.min_polygon_fraction) else (
            "NaN (below floor)" if is_nan else "finite")
        vstr = "   NaN" if is_nan else f"{value:13.3f}"
        print(f"  {unit[:32]:32s} {vstr:>13s} {retained:9.3f}  {verdict}")
        rows_for_csv.append(
            {"district": unit, "period": period_name, "value": value,
             "retained_weight_fraction": retained, "verdict": verdict}
        )

    # =========================== VERDICT ===========================
    print("\n" + "=" * 78)
    print("BINDING-GATE VERDICT")
    if n_gate1_ok == 0:
        print("  -> GATE 1 binds: no cell clears baseline coverage (missing data, "
              "not aridity). Investigate precip file coverage first.")
    elif n_cells_annual_finite == 0:
        print("  -> GATES 2/3 bind: cells clear coverage but NO cell yields a finite")
        print("     annual value. The SPI signal does not survive the gamma fit +")
        print("     min-months floor => relaxing Gates 3.5/4 cannot recover it.")
        print("     Fix space: accept+annotate OR substitute an arid-robust index.")
        print("     (Check the Gate-2 cause split: returned-NaN => gamma degeneracy;")
        print("      caught-exception => a different failure worth its own fix.)")
    elif cells_pass_3_5 == 0:
        print("  -> GATE 3.5 binds: cells produce finite ANNUAL values but none clears")
        print(f"     the period-year floor ({args.min_years_per_period_fraction}) over "
              f"{period_name} => too few finite years per period.")
        print("     Fix space: relax min_years_per_period_fraction, OR treat the")
        print("     upstream Gate 2/3 sparsity as the real driver (see Gate-2 split).")
    else:
        nan_units = [u for u, (v, r) in per_unit.items() if not np.isfinite(v)]
        below = [u for u in nan_units
                 if per_unit[u][1] < args.min_polygon_fraction]
        if nan_units and not below:
            print("  -> GATE 4 binds despite finite cells existing: districts NaN even")
            print("     though retained weight clears the floor — inspect aggregation.")
        elif below:
            print("  -> GATE 4 (weight floor) binds: finite SPI cells EXIST but cover")
            print(f"     < {args.min_polygon_fraction} of district area => the signal is")
            print("     present and our own threshold discards it. Fix space: relaxing /")
            print("     redesigning the coverage gate is legitimate here.")
        else:
            print(f"  -> No district NaN at the audited period ({period_name}) for this")
            print("     scope under this single model. The audited (ensemble-mean) NaN")
            print("     may be an ensemble-combine artifact or affect other districts;")
            print("     re-run across all models and/or on a confirmed NaN district.")
    print("=" * 78)

    if args.out_csv:
        out_path = Path(args.out_csv)
        pd.DataFrame(rows_for_csv).to_csv(out_path, index=False)
        print(f"\n[WROTE] per-district Gate-4 table -> {out_path.resolve()}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True, help="State/UT name (e.g. Rajasthan)")
    ap.add_argument("--district", default=None,
                    help="District name (omit to trace the whole state bbox)")
    ap.add_argument("--model", default="CanESM5")
    ap.add_argument("--scenario", default="historical")
    ap.add_argument("--slug", default="spi3_count_events_lt_minus1",
                    help="Drought SPI slug (spi3/spi6/spi12 * count_events/max_spell/count_months)")
    ap.add_argument("--baseline", type=int, nargs=2, default=(1981, 2010),
                    metavar=("START", "END"))
    ap.add_argument("--years", type=int, nargs=2, default=None, metavar=("START", "END"),
                    help="Subset of available precip years")
    ap.add_argument("--data-root", type=Path, default=None,
                    help="Bundle root; defaults to DATA_DIR/<--bundle> (honors IRT_DATA_DIR)")
    ap.add_argument("--bundle", default="r1i1p1f1_telangana",
                    help="Run-bundle folder under DATA_DIR (default r1i1p1f1_telangana)")
    ap.add_argument("--min-months-per-year", type=int, default=9)
    ap.add_argument("--period-years", type=int, nargs=2, default=(1990, 2010),
                    metavar=("START", "END"),
                    help="Audited period for the Gate-3.5 rollup (default 1990 2010)")
    ap.add_argument("--min-years-per-period-fraction", type=float, default=0.75)
    ap.add_argument("--period-rollup", default="period_mean",
                    choices=["period_mean", "period_max"])
    ap.add_argument("--min-polygon-fraction", type=float, default=0.50)
    ap.add_argument("--min-baseline-fraction", type=float, default=0.83)
    ap.add_argument("--min-daily-coverage", type=float, default=0.90)
    ap.add_argument("--threshold", type=float, default=-1.0)
    ap.add_argument("--min-event-months", type=int, default=1)
    ap.add_argument("--out-csv", default=None,
                    help="Optional: write the per-district Gate-4 table here (the ONLY write)")
    args = ap.parse_args()

    if not CLIMATE_INDICES_AVAILABLE:
        raise SystemExit("climate-indices not installed; cannot run the SPI trace.")
    trace(args)


if __name__ == "__main__":
    main()
