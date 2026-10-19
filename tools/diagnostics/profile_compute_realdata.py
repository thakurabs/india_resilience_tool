#!/usr/bin/env python3
"""Cold-path stage profiler across ALL grid-first compute families.

Times the real per-stage cost of one ``(model, scenario, period)`` for every
grid-first climate family that feeds the dashboard's thematic *and* sectoral
bundles, then quantifies the two structural levers:

    * **De-dup / caching** -- load each variable once and derive each shared
      intermediate (monthly cube, SPI grid, DOY/percentile threshold, wet-bulb
      field) once per ``(model, scenario)`` instead of once per slug. Measured.
    * **Vectorisation**     -- collapse the per-cell Python loops in the
      compute portion. Parametric overlay via ``--vector-factor`` (I/O is left
      untouched; only compute is divided).

Why "cold path": a methodology change bumps each family's ``method_version`` and
invalidates the on-disk per-year caches, so a fresh build pays the full
load+derive+per-slug cost. That is exactly what this profiler measures.

The five families and their fan-out (per model x scenario x level):
    drought (7) | extreme_rainfall (8) | heat_risk (15) | heat_stress (14)
    | cold (11)  ==> 55 slugs total.

Read-only: loads NetCDFs, writes nothing to processed outputs.

Examples
--------
    # All families, one representative (model, scenario)
    python -m tools.diagnostics.profile_compute_realdata \
        --model CanESM5 --scenario historical

    # One family while validating, with a vectorisation overlay
    python -m tools.diagnostics.profile_compute_realdata \
        --family drought --vector-factor 8
"""

from __future__ import annotations

import argparse
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from india_resilience_tool.config.paths import DATA_DIR
from india_resilience_tool.config.metrics_registry import ALL_METRICS_RAW
from india_resilience_tool.compute.gridfirst_spatial import (
    compute_doy_thresholds,
    concat_years,
)
from india_resilience_tool.compute.spi_adapter import Distribution

# --- family entrypoints + stage functions ---------------------------------
import india_resilience_tool.compute.drought_risk_gridfirst as drg
import india_resilience_tool.compute.extreme_rainfall_gridfirst as erg
import india_resilience_tool.compute.heat_risk_gridfirst as hrg
import india_resilience_tool.compute.heat_stress_gridfirst as hsg
import india_resilience_tool.compute.cold_risk_gridfirst as crg

DEFAULT_BUNDLE = "r1i1p1f1_telangana"
ALL_FAMILIES = ("drought", "extreme_rainfall", "heat_risk", "heat_stress", "cold")

# slug -> raw registry metric dict (carries var / compute / params / value_col)
_RAW_BY_SLUG: dict[str, dict] = {
    str(m.get("slug") or ""): m for m in ALL_METRICS_RAW if str(m.get("slug") or "")
}


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def _timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def discover_var_years(data_root: Path, scenario: str, var: str, model: str) -> dict[int, dict[str, Path]]:
    """Glob {data_root}/{scenario}/{var}/{model}/{year}.nc into year_to_paths."""
    vdir = data_root / scenario / var / model
    out: dict[int, dict[str, Path]] = {}
    if not vdir.is_dir():
        return out
    for p in sorted(vdir.glob("*.nc")):
        try:
            out[int(p.stem)] = {var: p}
        except ValueError:
            continue
    return out


def _merge_var_paths(per_var: dict[str, dict[int, dict[str, Path]]]) -> dict[int, dict[str, Path]]:
    """Merge {var: {year: {var: path}}} into {year: {var: path, ...}}."""
    merged: dict[int, dict[str, Path]] = {}
    for ypaths in per_var.values():
        for year, vmap in ypaths.items():
            merged.setdefault(int(year), {}).update(vmap)
    return merged


# ---------------------------------------------------------------------------
# result model + cost projection
# ---------------------------------------------------------------------------
@dataclass
class FamilyResult:
    family: str
    n_eval_years: int
    per_year_scope: bool  # True: load+per-slug repeat per year; False (drought): whole-series once
    # load cost per DISTINCT variable, for ONE year (per_year_scope) or whole series (drought)
    loads: dict[str, float] = field(default_factory=dict)
    # shared intermediates computed once per (model, scenario): label -> seconds
    intermediates: list[tuple[str, float]] = field(default_factory=list)
    # per-slug compute (excl. shared load+intermediate): slug -> seconds, and the var it reads
    per_slug: list[tuple[str, str, float]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def project(self, vector_factor: float) -> dict[str, float]:
        """Return naive / deduped / deduped+vectorised seconds per (model,scenario,level)."""
        n = max(self.n_eval_years, 1) if self.per_year_scope else 1
        interm_total = sum(s for _, s in self.intermediates)
        per_slug_total = sum(s for _, _, s in self.per_slug)

        # NAIVE (today, cold path): every slug reloads its var every year and
        # re-derives its share of the shared intermediates. Approximated as each
        # slug paying one full load of its var + the full intermediate stack
        # once, repeated across years for the load+per-slug portion.
        naive_load = sum(self.loads.get(var, 0.0) for _, var, _ in self.per_slug)
        naive = len(self.per_slug) * interm_total + n * (naive_load + per_slug_total)

        # DEDUPED: load each distinct var once/year, derive each intermediate
        # once, fan out the cheap per-slug reductions.
        dedup_load = sum(self.loads.values())
        deduped = interm_total + n * (dedup_load + per_slug_total)

        # +VECTORISED: divide the *compute* portion (intermediates + per-slug)
        # by the factor; leave I/O (loads) untouched.
        vf = max(vector_factor, 1e-9)
        vector = (interm_total / vf) + n * (dedup_load + per_slug_total / vf)
        return {"naive": naive, "deduped": deduped, "vector": vector}


# ---------------------------------------------------------------------------
# per-family profilers
# ---------------------------------------------------------------------------
def profile_drought(data_root: Path, model: str, scenario: str, baseline: tuple[int, int],
                    year_cap: int | None) -> FamilyResult:
    slugs = sorted(set(drg.DROUGHT_GRIDFIRST_SLUGS) | set(drg.DROUGHT_GRIDFIRST_ADMIN_ONLY_SLUGS))
    ypaths = discover_var_years(data_root, scenario, "pr", model)
    if not ypaths:
        raise SystemExit(f"[drought] no pr files under {data_root}/{scenario}/pr/{model}")
    years = sorted(ypaths)
    if year_cap:
        years = years[:year_cap]
    res = FamilyResult(family="drought", n_eval_years=len(years), per_year_scope=False)

    load_s, da = _timed(lambda: concat_years(ypaths, "pr", years))
    res.loads["pr"] = load_s
    mon_s, monthly = _timed(lambda: drg.daily_to_monthly_totals(da))
    res.intermediates.append(("daily_to_monthly_totals", mon_s))

    # SPI grid is shared by all slugs at a given scale; derive once per scale.
    spi_by_scale: dict[int, object] = {}
    for scale in (3, 6, 12):
        s, spi = _timed(lambda sc=scale: drg.compute_spi_grid(
            monthly, baseline_years=baseline, scale_months=sc, distribution=Distribution.GAMMA))
        spi_by_scale[scale] = spi
        res.intermediates.append((f"compute_spi_grid scale={scale}", s))

    agg_map = {"count_events": "count_events_lt", "max_spell": "max_spell_lt", "count_months": "count_months_lt"}
    for slug in slugs:
        scale = 3 if slug.startswith("spi3") else 6 if slug.startswith("spi6") else 12
        agg = next(a for k, a in agg_map.items() if k in slug)
        spi = spi_by_scale[scale]
        s, _ = _timed(lambda sp=spi, ag=agg: drg.annual_spi_metric_grid(
            sp, annual_aggregation=ag, threshold=-1.0, min_months_per_year=9, min_event_months=1))
        res.per_slug.append((slug, "pr", s))
    res.notes.append("whole-series: monthly+SPI computed once over all years; agg is per-slug.")
    res.notes.append("NO in-run compute cache today -> naive recomputes monthly+SPI per slug.")
    return res


def profile_extreme_rainfall(data_root: Path, model: str, scenario: str,
                             eval_year: int, baseline: tuple[int, int]) -> FamilyResult:
    slugs = sorted(erg.EXTREME_RAINFALL_GRIDFIRST_SLUGS)
    eval_paths = discover_var_years(data_root, scenario, "pr", model)
    base_paths = discover_var_years(data_root, "historical", "pr", model)
    if eval_year not in eval_paths:
        eval_year = max(eval_paths)
    res = FamilyResult(family="extreme_rainfall", n_eval_years=len(eval_paths), per_year_scope=True)

    load_s, daily = _timed(lambda: concat_years(eval_paths, "pr", [eval_year]))
    res.loads["pr"] = load_s

    # R95p threshold from baseline -- shared by the 3 percentile slugs, once.
    base_years = [y for y in sorted(base_paths) if baseline[0] <= y <= baseline[1]]
    threshold = None
    if base_years:
        bload_s, bda = _timed(lambda: concat_years(base_paths, "pr", base_years))
        thr_s, thr_ds = _timed(lambda: erg.compute_r95p_threshold_grid(bda))
        threshold = thr_ds["value"]
        res.intermediates.append(("R95p threshold (baseline load+quantile)", bload_s + thr_s))
    else:
        res.notes.append("no historical baseline -> percentile slugs skipped")

    for slug in slugs:
        needs_thr = slug in {"r95p_very_wet_precip", "r99p_extreme_wet_precip", "r95ptot_contribution_pct"}
        if needs_thr and threshold is None:
            continue
        s, _ = _timed(lambda sl=slug: erg.annual_extreme_rainfall_grid(
            daily, slug=sl, threshold=threshold if needs_thr else None))
        res.per_slug.append((slug, "pr", s))
    res.notes.append("per-year: _cwd/_cdd are per-cell Python loops (vectorisable).")
    return res


def _doy_threshold_for(metric: dict, base_paths: dict, var: str, default_pct: int) -> tuple[object | None, float]:
    params = dict(metric.get("params") or {})
    bl = tuple(int(v) for v in params.get("baseline_years", (1981, 2010)))
    pct = int(params.get("percentile", params.get("pct", default_pct)))
    window = int(params.get("window_days", 5))
    qm = str(params.get("quantile_method", "linear"))
    smooth = params.get("smooth")
    smooth_int = int(smooth) if smooth is not None else None
    base_years = [y for y in sorted(base_paths) if bl[0] <= y <= bl[1]]
    if not base_years:
        return None, 0.0
    bload_s, bda = _timed(lambda: concat_years(base_paths, var, base_years))
    thr_s, thr = _timed(lambda: compute_doy_thresholds(
        bda, percentile=pct, window_days=window, quantile_method=qm, smooth=smooth_int))
    return thr, bload_s + thr_s


def profile_heat_risk(data_root: Path, model: str, scenario: str, eval_year: int) -> FamilyResult:
    slugs = sorted(set(hrg.HEAT_RISK_GRIDFIRST_SLUGS) | set(hrg.HEAT_RISK_GRIDFIRST_ADMIN_ONLY_SLUGS))
    res = FamilyResult(family="heat_risk", n_eval_years=0, per_year_scope=True)
    eval_cache: dict[str, object] = {}
    thr_cache: dict[str, object] = {}
    base_by_var: dict[str, dict] = {}

    for slug in slugs:
        metric = _RAW_BY_SLUG.get(slug)
        if metric is None:
            res.notes.append(f"{slug}: not in registry, skipped")
            continue
        var = str(metric.get("var") or "")
        compute = str(metric.get("compute") or "")
        ev = discover_var_years(data_root, scenario, var, model)
        if not ev:
            res.notes.append(f"{slug}: no {var} files, skipped")
            continue
        res.n_eval_years = max(res.n_eval_years, len(ev))
        ey = eval_year if eval_year in ev else max(ev)
        if var not in eval_cache:
            load_s, eda = _timed(lambda v=var, e=ev, y=ey: concat_years(e, v, [y]))
            res.loads[var] = load_s
            eval_cache[var] = eda
        eda = eval_cache[var]

        threshold = None
        if compute in hrg.GRIDFIRST_BASELINE_THRESHOLD_COMPUTES:
            params = dict(metric.get("params") or {})
            tkey = f"{var}|p{params.get('percentile', params.get('pct', 90))}|w{params.get('window_days', 5)}"
            if tkey not in thr_cache:
                if var not in base_by_var:
                    base_by_var[var] = discover_var_years(data_root, "historical", var, model)
                thr, tcost = _doy_threshold_for(metric, base_by_var[var], var, 90)
                thr_cache[tkey] = thr
                if thr is not None:
                    res.intermediates.append((f"DOY threshold {tkey}", tcost))
            threshold = thr_cache[tkey]
            if threshold is None:
                res.notes.append(f"{slug}: no baseline for threshold, skipped")
                continue
        s, _ = _timed(lambda m=metric, e=eda, t=threshold: hrg._metric_cell_values(metric=m, eval_da=e, threshold=t))
        res.per_slug.append((slug, var, s))
    res.notes.append("per-year: spell/percent-day counters are per-cell loops (vectorisable).")
    return res


def profile_cold(data_root: Path, model: str, scenario: str, eval_year: int) -> FamilyResult:
    slugs = sorted(crg.COLD_RISK_GRIDFIRST_SLUGS)
    res = FamilyResult(family="cold", n_eval_years=0, per_year_scope=True)
    eval_cache: dict[str, object] = {}
    prev_cache: dict[str, object] = {}
    thr_cache: dict[str, object] = {}
    base_by_var: dict[str, dict] = {}

    for slug in slugs:
        metric = _RAW_BY_SLUG.get(slug)
        if metric is None:
            res.notes.append(f"{slug}: not in registry, skipped")
            continue
        var = str(metric.get("var") or "")
        compute = str(metric.get("compute") or "")
        ev = discover_var_years(data_root, scenario, var, model)
        if not ev:
            res.notes.append(f"{slug}: no {var} files, skipped")
            continue
        res.n_eval_years = max(res.n_eval_years, len(ev))
        ey = eval_year if eval_year in ev else max(ev)
        if var not in eval_cache:
            load_s, eda = _timed(lambda v=var, e=ev, y=ey: concat_years(e, v, [y]))
            res.loads[var] = load_s
            eval_cache[var] = eda
        eda = eval_cache[var]

        prev_da = None
        if compute in crg.COLD_RISK_GRIDFIRST_DJF_COMPUTES:
            if (ey - 1) in ev:
                if var not in prev_cache:
                    _, prev_cache[var] = _timed(lambda v=var, e=ev, y=ey: concat_years(e, v, [y - 1]))
                prev_da = prev_cache[var]
            else:
                res.notes.append(f"{slug}: no prev-year {ey-1} for DJF, skipped")
                continue

        threshold = None
        if compute in crg.COLD_RISK_GRIDFIRST_BASELINE_THRESHOLD_COMPUTES:
            params = dict(metric.get("params") or {})
            tkey = f"{var}|p{params.get('percentile', 10)}|w{params.get('window_days', 5)}"
            if tkey not in thr_cache:
                if var not in base_by_var:
                    base_by_var[var] = discover_var_years(data_root, "historical", var, model)
                thr, tcost = _doy_threshold_for(metric, base_by_var[var], var, 10)
                thr_cache[tkey] = thr
                if thr is not None:
                    res.intermediates.append((f"DOY threshold {tkey}", tcost))
            threshold = thr_cache[tkey]
            if threshold is None:
                res.notes.append(f"{slug}: no baseline for threshold, skipped")
                continue
        s, _ = _timed(lambda m=metric, e=eda, p=prev_da, t=threshold: crg._cold_risk_cell_values(
            metric=m, eval_da=e, prev_da=p, threshold=t))
        res.per_slug.append((slug, var, s))
    res.notes.append("per-year: spell/longest-run counters are per-cell loops (vectorisable).")
    return res


def profile_heat_stress(data_root: Path, model: str, scenario: str, eval_year: int) -> FamilyResult:
    """Heat stress fuses load+derive in _cell_values_for_metric; here we derive
    each shared field (twb / wbgt / swbgt) ONCE and time only the per-slug
    reduction, so the de-dup lever is measured cleanly."""
    slugs = sorted(hsg.HEAT_STRESS_GRIDFIRST_SLUGS)
    res = FamilyResult(family="heat_stress", n_eval_years=0, per_year_scope=True)

    tas_p = discover_var_years(data_root, scenario, "tas", model)
    hurs_p = discover_var_years(data_root, scenario, "hurs", model)
    tasmin_p = discover_var_years(data_root, scenario, "tasmin", model)
    if not tas_p or not hurs_p:
        raise SystemExit(f"[heat_stress] need tas+hurs under {data_root}/{scenario}/(tas|hurs)/{model}")
    res.n_eval_years = max(len(tas_p), len(hurs_p), len(tasmin_p) or 0)
    ey = eval_year if eval_year in tas_p else max(tas_p)
    merged = _merge_var_paths({"tas": tas_p, "hurs": hurs_p, **({"tasmin": tasmin_p} if tasmin_p else {})})

    # shared loads
    ld_tas, _ = _timed(lambda: concat_years(tas_p, "tas", [ey]))
    ld_hurs, _ = _timed(lambda: concat_years(hurs_p, "hurs", [ey]))
    res.loads["tas"] = ld_tas
    res.loads["hurs"] = ld_hurs
    if tasmin_p:
        ld_tn, _ = _timed(lambda: concat_years(tasmin_p, "tasmin", [ey]))
        res.loads["tasmin"] = ld_tn

    # shared derived fields (each once)
    twb_s, twb = _timed(lambda: hsg._twb_daily_for_year(merged, ey))
    res.intermediates.append(("twb field (tas+hurs derive)", twb_s))
    tas_raw = concat_years(tas_p, "tas", [ey])
    hurs_raw = concat_years(hurs_p, "hurs", [ey])
    tas_c = hsg._drop_feb29(tas_raw) - 273.15
    hurs = hsg._drop_feb29(hurs_raw)
    wbgt_s, wbgt = _timed(lambda: hsg.wbgt_shade_stull_cell_c(tas_c, hurs))
    res.intermediates.append(("wbgt field derive", wbgt_s))
    swbgt_s, swbgt = _timed(lambda: hsg.swbgt_empirical_cell_c(tas_c, hurs))
    res.intermediates.append(("swbgt field derive", swbgt_s))

    def _reduce(slug: str, field, params: dict):
        if slug.endswith("_annual_mean") or slug == "twb_annual_mean":
            return field.mean(dim="time", skipna=True)
        if slug == "twb_annual_max":
            return field.max(dim="time", skipna=True)
        if slug == "twb_summer_mean":
            months = [int(m) for m in params.get("months", (3, 4, 5, 6))]
            return field.sel(time=field["time"].dt.month.isin(months)).mean(dim="time", skipna=True)
        thr = float(params.get("thresh_c", 28.0))
        return (field >= thr).fillna(False).sum(dim="time").astype(float)

    for slug in slugs:
        metric = _RAW_BY_SLUG.get(slug) or {}
        params = dict(metric.get("params") or {})
        if slug == "tasmin_tropical_nights_gt28":
            if not tasmin_p:
                res.notes.append(f"{slug}: no tasmin, skipped")
                continue
            tn = hsg._drop_feb29(concat_years(tasmin_p, "tasmin", [ey])) - 273.15
            s, _ = _timed(lambda d=tn: (d > 28.0).fillna(False).sum(dim="time").astype(float))
            res.per_slug.append((slug, "tasmin", s))
            continue
        field = twb if slug.startswith("twb") else wbgt if slug.startswith("wbgt") else swbgt
        s, _ = _timed(lambda sl=slug, f=field, p=params: _reduce(sl, f, p))
        var = "tas" if not slug.startswith("twb") else "tas"  # twb/wbgt/swbgt all read tas+hurs
        res.per_slug.append((slug, var, s))
    res.notes.append("twb/wbgt/swbgt fields shared by 5/4/4 slugs; naive re-derives per slug.")
    return res


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------
def print_family(res: FamilyResult, vector_factor: float) -> dict[str, float]:
    print(f"\n========== {res.family.upper()} ==========")
    print(f"  slugs profiled: {len(res.per_slug)} | eval years available: {res.n_eval_years} "
        f"| scope: {'per-year' if res.per_year_scope else 'whole-series'}")
    print("  -- loads (per " + ("year" if res.per_year_scope else "series") + ", per distinct var) --")
    for var, s in res.loads.items():
        print(f"     load {var:8s} {s:7.2f}s")
    if res.intermediates:
        print("  -- shared intermediates (once per model,scenario) --")
        for label, s in res.intermediates:
            print(f"     {label:42s} {s:7.2f}s")
    print("  -- per-slug compute --")
    for slug, var, s in res.per_slug:
        print(f"     {slug:38s} [{var:6s}] {s:7.3f}s")
    proj = res.project(vector_factor)
    print("  -- projection (seconds per model x scenario x level) --")
    print(f"     naive (today, cold path) : {proj['naive']:8.1f}s")
    print(f"     deduped (load+derive once): {proj['deduped']:8.1f}s   "
        f"=> {proj['naive']/max(proj['deduped'],1e-9):.2f}x")
    print(f"     deduped + vectorised (x{vector_factor:g}): {proj['vector']:8.1f}s   "
        f"=> {proj['naive']/max(proj['vector'],1e-9):.2f}x")
    for n in res.notes:
        print(f"     note: {n}")
    return proj


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", type=Path, default=None,
                    help="Bundle root; defaults to DATA_DIR/<--bundle> (honors IRT_DATA_DIR)")
    ap.add_argument("--bundle", default=DEFAULT_BUNDLE, help=f"Run-bundle under DATA_DIR (default {DEFAULT_BUNDLE})")
    ap.add_argument("--model", default="CanESM5")
    ap.add_argument("--scenario", default="historical")
    ap.add_argument("--eval-year", type=int, default=2010, help="representative eval year for per-year families")
    ap.add_argument("--baseline", type=int, nargs=2, default=(1981, 2010), metavar=("START", "END"))
    ap.add_argument("--family", choices=("all", *ALL_FAMILIES), default="all")
    ap.add_argument("--vector-factor", type=float, default=8.0,
                    help="assumed speedup on the per-cell compute portion (I/O untouched)")
    ap.add_argument("--drought-year-cap", type=int, default=None,
                    help="cap drought eval years (faster smoke test; SPI needs a full series for real numbers)")
    args = ap.parse_args()

    data_root = args.data_root if args.data_root is not None else DATA_DIR / args.bundle
    print(f"Data root: {data_root}")
    print(f"Model={args.model} scenario={args.scenario} eval_year={args.eval_year} "
        f"baseline={tuple(args.baseline)} vector_factor={args.vector_factor:g}")

    families = ALL_FAMILIES if args.family == "all" else (args.family,)
    runners = {
        "drought": lambda: profile_drought(data_root, args.model, args.scenario, tuple(args.baseline), args.drought_year_cap),
        "extreme_rainfall": lambda: profile_extreme_rainfall(data_root, args.model, args.scenario, args.eval_year, (1990, 2010)),
        "heat_risk": lambda: profile_heat_risk(data_root, args.model, args.scenario, args.eval_year),
        "heat_stress": lambda: profile_heat_stress(data_root, args.model, args.scenario, args.eval_year),
        "cold": lambda: profile_cold(data_root, args.model, args.scenario, args.eval_year),
    }

    totals: dict[str, float] = {"naive": 0.0, "deduped": 0.0, "vector": 0.0}
    ran: list[str] = []
    for fam in families:
        try:
            res = runners[fam]()
            proj = print_family(res, args.vector_factor)
            for k in totals:
                totals[k] += proj[k]
            ran.append(fam)
        except SystemExit as e:
            print(f"\n########## {fam.upper()} SKIPPED: {e}")
        except Exception as e:  # noqa: BLE001 -- diagnostic: keep other families alive
            print(f"\n########## {fam.upper()} FAILED: {e}")
            print(traceback.format_exc())

    if len(ran) > 1:
        print("\n==================== ROLL-UP (per model x scenario x level) ====================")
        print(f"  families: {', '.join(ran)}")
        print(f"  naive (today, cold path)  : {totals['naive']:9.1f}s  ({totals['naive']/3600:.2f}h)")
        print(f"  deduped                   : {totals['deduped']:9.1f}s  "
            f"=> {totals['naive']/max(totals['deduped'],1e-9):.2f}x")
        print(f"  deduped + vectorised (x{args.vector_factor:g}): {totals['vector']:9.1f}s  "
            f"=> {totals['naive']/max(totals['vector'],1e-9):.2f}x")
        print("\n  Scale to a full build by multiplying by (#models x #scenarios x #levels).")
        print("  NOTE: dedup numbers are MEASURED; vector-factor is a parametric overlay on compute only.")


if __name__ == "__main__":
    main()
