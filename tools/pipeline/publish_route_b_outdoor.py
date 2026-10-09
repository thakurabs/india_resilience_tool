"""Convert assembled Route B outdoor-WBGT diagnostics into the production processed layer.

CHG-0664. Offline: reads the validated diagnostic artifacts and writes the
``processed/<slug>`` tree that ``tools.optimized.build_processed_optimised``
consumes. No climate computation happens here, and no value is recomputed --
every number traces to one ``yearly_metrics.csv`` cell produced by the pinned
Route B runner.

Five slugs are emitted, additively. Nothing is renamed: an empirical output is
never relabelled as outdoor WBGT, and the incumbent ``swbgt_empirical_*`` and
``wbgt_shade_stull_*`` trees are not read, moved or deleted.

Aggregation order is carried through deliberately. Counts and maxima come from
``grid_first_cell_metrics`` -- threshold each cell-day, reduce annually per cell,
then area-weight -- so counts are area-weighted cell exceedance days and may be
fractional. Thresholding a spatial mean (``grid_first_series``) is a comparator
and is never published. The annual mean is identical under both orders.

Excluded units stay in the roster carrying missing values. They are never
dropped and never zero-filled.

The processed layer is millions of small files: a full 18-model roster is about
4.3 million. Where they are written dominates the runtime, and the measurement
that matters is the one taken with the interpreter that actually writes them --
IRT's Windows geo environment, because the optimised build must read the tree back
with the same interpreter.

Measured on 2026-10-06 from that interpreter: **36 files/s** writing to the `D:`
data drive against **636 files/s** to a `C:` path, a 17-fold difference, with
reads at 379 and 412 files/s respectively. A WSL-side measurement of about 7,300
files/s on ext4 is not reachable here and must not be used to plan this step: the
Windows process cannot read a WSL path, so the staged tree has to sit somewhere
the builder can see. ``--processed-root`` therefore defaults to a `C:` staging
directory, and the optimised build is pointed at it with ``IRT_PROCESSED_ROOT``.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
from pathlib import Path
import time

import numpy as np
import pandas as pd

METHOD_VERSION = "route-b-outdoor-v1"

#: Recorded in place of the incumbent's absolute netCDF path list. The real lineage
#: lives in each model's ``run_manifest.json``, which pins the source lock, the
#: support signature and the code hashes; repeating a path list per row would be both
#: enormous and less trustworthy than that manifest.
SOURCE_LABEL = "route_b_outdoor_wbgt_assembly"
AGGREGATION_METHOD = "gridfirst_cell_area_weighted"

#: The published estimand. ``grid_first_series`` is a comparator, never published.
RELEASED_METHOD = "grid_first_cell_metrics"

#: Slug -> (diagnostic column, production value column, unit label).
SLUGS = {
    "wbgt_outdoor_route_b_annual_mean": ("mean_c", "wbgt_outdoor_route_b_annual_mean_C", "°C"),
    "wbgt_outdoor_route_b_annual_max": ("max_c", "wbgt_outdoor_route_b_annual_max_C", "°C"),
    "wbgt_outdoor_route_b_days_ge_28": ("days_ge_28", "wbgt_outdoor_route_b_days_ge_28_days", "days"),
    "wbgt_outdoor_route_b_days_ge_30": ("days_ge_30", "wbgt_outdoor_route_b_days_ge_30_days", "days"),
    "wbgt_outdoor_route_b_days_ge_32": ("days_ge_32", "wbgt_outdoor_route_b_days_ge_32_days", "days"),
}

#: Master periods, inclusive at both ends, exactly as the incumbent spells them.
#: The SSP windows deliberately share their boundary years, matching the
#: incumbent's 21-year ``years_requested``.
PERIODS = {
    "historical": (("1990-2010", 1990, 2010),),
    "ssp245": (("2020-2040", 2020, 2040), ("2040-2060", 2040, 2060), ("2060-2080", 2060, 2080)),
    "ssp585": (("2020-2040", 2020, 2040), ("2040-2060", 2040, 2060), ("2060-2080", 2060, 2080)),
}

ENVELOPE = {"historical": (1990, 2010), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}

#: Unit coverage gate, kept at 0.95 for this release by explicit user ruling. It is
#: recorded in the published coverage table so a reader never has to infer it.
UNIT_COVERAGE_GATE = 0.95

#: A master period mean requires every year of its period from that model. See
#: ``_master_frame``: the alternative publishes a partial mean that is
#: indistinguishable from a complete one in the master, which carries no
#: years-used column.
REQUIRE_COMPLETE_PERIOD = True


def load_assembly(assembly_dir: Path, work_dir: Path, models: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the released estimand for every model, with the roster attached.

    Membership is checked by model identity, scenario and year -- never by row
    count -- so a nominal 19-model ensemble can never be computed from a smaller
    available subset.
    """
    units = pd.read_csv(work_dir / "units.csv").set_index("unit_index")
    frames, eligibility = [], {}
    for model in models:
        path = assembly_dir / model / "yearly_metrics.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Model not assembled: {path}")
        report = assembly_dir / model / "validation_report.json"
        if report.is_file() and json.loads(report.read_text()).get("verdict") != "PASS":
            raise ValueError(f"{model} has a failing validation report; refusing to publish it")
        frame = pd.read_csv(path)
        frame = frame[frame.method == RELEASED_METHOD].drop(columns=["method"])
        for scenario, (first, last) in ENVELOPE.items():
            years = sorted(frame.loc[frame.scenario == scenario, "year"].unique())
            if years != list(range(first, last + 1)):
                raise ValueError(f"{model} {scenario} years are {years[:3]}..{years[-3:]}, "
                                 f"not the declared {first}-{last}")
        frame["model"] = model
        frames.append(frame)
        support = pd.read_csv(assembly_dir / model / "unit_support.csv")
        eligibility[model] = set(support.loc[support.eligible, "unit_index"].astype(int))
    support_table = support.copy()
    shared = set.intersection(*eligibility.values()) if eligibility else set()
    varying = {m: sorted(s ^ shared) for m, s in eligibility.items() if s != shared}
    if varying:
        raise ValueError("Unit eligibility differs between models, so one ensemble would "
                         f"mix supports: {({m: v[:5] for m, v in varying.items()})}")
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.join(units[["state_name", "level", "district_name", "block_name"]],
                             on="unit_index")
    return combined, units, coverage_table(support_table)


def path_safe(name: str) -> str:
    """Sanitise one admin name for a path component, exactly as the pipeline does.

    ``tools/pipeline/compute_indices.py`` writes the processed tree with
    ``name.replace(" ", "_").replace("/", "_")``, and the optimised builder globs
    that tree by position rather than by name, so the convention has to match
    character for character or the two trees disagree. Andhra Pradesh's
    ``Pulicherla H/O Reddivaripalle`` is the case that forces it: the slash would
    otherwise split into two directories and corrupt the filename. The **data
    columns keep the original name**, slash and all, which is what the incumbent
    tree stores and what the roster join needs.
    """
    return str(name).replace(" ", "_").replace("/", "_")


def _quote(value: str) -> str:
    """Quote a CSV field only when it needs it, as pandas does."""
    text = str(value)
    if any(ch in text for ch in (",", '"', "\n")):
        return '"' + text.replace('"', '""') + '"'
    return text


def coverage_table(support: pd.DataFrame) -> pd.DataFrame:
    """Build the per-unit coverage and exclusion table that ships with the metric.

    The user's ruling was to keep the 0.95 gate and make the exclusions legible
    rather than silent, so every unit carries the share of its area that is backed
    by eligible cells, the share the NEX grid covers at all, and an explicit reason.
    Separating the grid gap from the reference gap matters: a reference gap is a
    limitation of UHE-Daily at the coast, while a grid gap is genuine geometry, and
    a reader cannot tell them apart from the coverage number alone.
    """
    table = support[["unit_index", "state_name", "level", "district_name", "block_name",
                     "lgd_code", "grid_area_fraction", "retained_fraction",
                     "retained_fraction_strict_reference", "eligible"]].copy()
    grid_gap = 1.0 - table.grid_area_fraction
    reference_gap = table.grid_area_fraction - table.retained_fraction
    table["grid_gap_fraction"] = grid_gap
    table["reference_gap_fraction"] = reference_gap
    table["unit_coverage_gate"] = UNIT_COVERAGE_GATE
    reason = np.where(
        table.eligible, "eligible",
        np.where(grid_gap > reference_gap,
                 "coverage_below_gate_grid_gap_dominant",
                 "coverage_below_gate_reference_support_dominant"))
    table["exclusion_reason"] = reason
    return table


#: Windows refuses a path over 260 characters unless long paths are enabled, and the
#: optimised builder reads this tree with the Windows interpreter. The longest real
#: staged path measured on the national roster is 196 characters -- Jharkhand /
#: Chatra / ``Shaligram Ram Narayanpur Alias Hunterganj`` under
#: ``EC-Earth3-Veg-LR`` -- so a short ``--processed-root`` has ample headroom and a
#: deep one does not. Checked up front, because discovering it after writing a
#: million files is a wasted hour.
WINDOWS_MAX_PATH = 260


def longest_path_preflight(root: Path, units: pd.DataFrame, slugs: list[str],
                           models: list[str]) -> dict:
    """Measure the longest path this run would write, before it writes anything."""
    slug = max(slugs, key=len)
    model = max(models, key=len)
    scenario = max(ENVELOPE, key=len)
    worst, example = 0, ""
    for row in units.itertuples():
        district = path_safe(row.district_name)
        block = path_safe(row.block_name) if isinstance(row.block_name, str) else ""
        if row.level == "block":
            candidates = (
                root / slug / row.state_name / "blocks" / district
                / block / model / scenario / f"{block}_periods.csv",
                root / slug / row.state_name / "blocks" / "ensembles" / district
                / block / scenario / f"{block}_yearly_ensemble.csv",
            )
        else:
            candidates = (
                root / slug / row.state_name / "districts" / district
                / model / scenario / f"{district}_periods.csv",
                root / slug / row.state_name / "districts" / "ensembles" / district
                / scenario / f"{district}_yearly_ensemble.csv",
            )
        for candidate in candidates:
            length = len(str(candidate))
            if length > worst:
                worst, example = length, str(candidate)
    return {"longest_path_chars": worst, "limit": WINDOWS_MAX_PATH,
            "headroom_chars": WINDOWS_MAX_PATH - worst, "longest_path_example": example}


def _period_values(group: pd.DataFrame, column: str, scenario: str) -> list[dict]:
    """Reduce one unit/model/scenario series to the declared master periods."""
    rows = []
    for label, first, last in PERIODS[scenario]:
        window = group[(group.year >= first) & (group.year <= last)]
        values = window[column].to_numpy(dtype=float)
        finite = values[np.isfinite(values)]
        rows.append({"period": label, "years_requested": last - first + 1,
                     "years_used_count": int(finite.size),
                     "value": float(finite.mean()) if finite.size else np.nan})
    return rows


def _csv_value(value: float) -> str:
    """Render one number the way ``pandas.to_csv`` would, including empty for missing.

    ``repr`` gives the shortest round-tripping form for a float64, which is what
    pandas writes, and a missing value becomes an empty field so a reader parses it
    back as NaN rather than as a zero.
    """
    return "" if value != value else repr(float(value))


def _write_unit_files(root: Path, unit: pd.Series, frame: pd.DataFrame, column: str,
                      value_col: str) -> int:
    """Write the per-unit, per-model, per-scenario yearly and period CSVs.

    Excluded units reach here with an all-missing series. Their files are still
    written, so the roster stays visible and a missing value can never be read back
    as a zero.

    The rows are assembled as text rather than through ``DataFrame.to_csv``. A full
    18-model roster writes roughly 4.3 million of these files, and per-call pandas
    overhead on a handful of rows dominates everything else at that count.
    """
    level, district, block = unit.level, unit.district_name, unit.block_name
    name = path_safe(block if level == "block" else district)
    if level == "block":
        identity_names, identity_values = ("district", "block"), (str(district), str(block))
    else:
        identity_names, identity_values = ("district",), (str(district),)
    identity_csv = ",".join(_quote(v) for v in identity_values)

    yearly_header = ",".join(("year", "value", value_col, "source_file", "method_version",
                              "aggregation_method", *identity_names, "scenario", "model"))
    period_header = ",".join((*identity_names, "value", "period", "years_used_count",
                              "years_requested", value_col, "method_version",
                              "aggregation_method", "model", "scenario"))
    written = 0
    for (model, scenario), group in frame.groupby(["model", "scenario"], sort=True):
        folder = (root / "blocks" / path_safe(district) / path_safe(block)
                  / str(model) / str(scenario) if level == "block"
                  else root / "districts" / path_safe(district) / str(model) / str(scenario))
        folder.mkdir(parents=True, exist_ok=True)
        group = group.sort_values("year")
        years = group.year.to_numpy()
        values = group[column].to_numpy(dtype=float)
        tail = (f"{SOURCE_LABEL},{METHOD_VERSION},{AGGREGATION_METHOD},{identity_csv},"
                f"{_quote(scenario)},{_quote(model)}")
        lines = [yearly_header]
        lines.extend(f"{int(year)},{rendered},{rendered},{tail}"
                     for year, rendered in ((y, _csv_value(v)) for y, v in zip(years, values)))
        (folder / f"{name}_yearly.csv").write_text("\n".join(lines) + "\n")

        rows = [period_header]
        for entry in _period_values(group, column, scenario):
            rendered = _csv_value(entry["value"])
            rows.append(f"{identity_csv},{rendered},{entry['period']},"
                        f"{entry['years_used_count']},{entry['years_requested']},{rendered},"
                        f"{METHOD_VERSION},{AGGREGATION_METHOD},"
                        f"{_quote(model)},{_quote(scenario)}")
        (folder / f"{name}_periods.csv").write_text("\n".join(rows) + "\n")
        written += 2
    return written


def _write_unit_ensemble_files(root: Path, unit: pd.Series, frame: pd.DataFrame,
                               column: str) -> int:
    """Write the per-unit, per-scenario yearly-ensemble CSVs.

    The optimised builder sources its ``yearly_ensemble`` tier from these files
    (``india_resilience_tool/data/discovery.py``), so a metric that omits them
    publishes no ensemble trend at all. Statistics are computed across whichever
    models actually carry a finite value in that year, and ``n_models`` reports
    that real count rather than the configured roster size, so a thinned unit can
    never be read as a full-roster ensemble. A year no model covers is written
    blank with ``n_models`` zero, never as a zero value.

    ``ensemble_std`` uses the pandas default ``ddof=1``, so a single contributing
    model yields a blank spread rather than an apparent zero.
    """
    level, district, block = unit.level, unit.district_name, unit.block_name
    name = path_safe(block if level == "block" else district)
    header = ",".join(("year", "n_models", "ensemble_mean", "ensemble_std",
                       "ensemble_median", "ensemble_p05", "ensemble_p95"))
    written = 0
    for scenario, group in frame.groupby("scenario", sort=True):
        folder = (root / "blocks" / "ensembles" / path_safe(district) / path_safe(block)
                  / str(scenario) if level == "block"
                  else root / "districts" / "ensembles" / path_safe(district) / str(scenario))
        folder.mkdir(parents=True, exist_ok=True)
        by_year = group.groupby("year", sort=True)[column]
        stats = pd.DataFrame({
            "n_models": by_year.count(),
            "ensemble_mean": by_year.mean(),
            "ensemble_std": by_year.std(),
            "ensemble_median": by_year.median(),
            "ensemble_p05": by_year.quantile(.05),
            "ensemble_p95": by_year.quantile(.95),
        })
        lines = [header]
        for year, row in stats.iterrows():
            lines.append(f"{int(year)},{int(row.n_models)},"
                         + ",".join(_csv_value(row[c]) for c in
                                    ("ensemble_mean", "ensemble_std", "ensemble_median",
                                     "ensemble_p05", "ensemble_p95")))
        (folder / f"{name}_yearly_ensemble.csv").write_text("\n".join(lines) + "\n")
        written += 1
    return written


#: Per-unit provenance carried in the master so the dashboard can say *why* a value
#: is missing instead of only that it is. These are plain columns, not ``__<stat>``
#: columns, exactly like the pipeline's own ``climate_fill_method``; the optimised
#: builder retains them by name. Without them the exclusion reason exists only in
#: ``support_coverage.csv``, which ships inside the metric tree but which no part of
#: the application reads (CHG-0674).
COVERAGE_MASTER_COLUMNS = ("support_coverage", "support_gap_kind", "exclusion_reason")


def _master_frame(frame: pd.DataFrame, units: pd.DataFrame, level: str, column: str,
                  value_col: str, models: list[str],
                  coverage: pd.DataFrame | None = None) -> pd.DataFrame:
    """Build one state's master metrics, carrying the per-model values explicitly.

    Vectorised per period rather than per unit. The obvious per-unit loop costs
    roughly ``units x scenarios x periods x models`` small pandas filters, which is
    over a million operations for a national 18-model roster; grouping once per
    period is a few operations regardless of roster size. A model contributes a
    period value only where it has that whole period, and the across-model
    statistics are taken over contributing models alone, so an absent model is never
    averaged in as a zero.
    """
    subset = frame[frame.level == level]
    identity = ["district"] if level == "district" else ["district", "block"]
    index = pd.Index(sorted(subset.unit_index.unique()), name="unit_index")
    master = pd.DataFrame(index=index)
    master["district"] = units.district_name.reindex(index).to_numpy()
    master["state"] = units.state_name.reindex(index).to_numpy()
    if level == "block":
        master["block"] = units.block_name.reindex(index).to_numpy()

    for scenario, periods in PERIODS.items():
        scoped = subset[subset.scenario == scenario]
        for label, first, last in periods:
            window = scoped[(scoped.year >= first) & (scoped.year <= last)]
            grouped = window.groupby(["unit_index", "model"])[column]
            # A period mean is published only where the model has every year of that
            # period. Averaging whatever survives would let a 21-year climatology rest
            # on as little as one year while looking complete in the master, which
            # carries no years-used column to tell the two apart. An incomplete period
            # is missing instead, exactly as an incomplete cell-year is missing rather
            # than a low count.
            expected_years = last - first + 1
            per_model = (grouped.mean().where(grouped.count() == expected_years)
                         .unstack("model").reindex(index=index, columns=models))
            prefix = f"{value_col}__{scenario}__{label}__"
            master[prefix + "mean"] = per_model.mean(axis=1)
            master[prefix + "std"] = per_model.std(axis=1, ddof=1)
            master[prefix + "median"] = per_model.median(axis=1)
            master[prefix + "p05"] = per_model.quantile(.05, axis=1)
            master[prefix + "p95"] = per_model.quantile(.95, axis=1)
            master[prefix + "n_models"] = per_model.notna().sum(axis=1).astype(int)
            master[prefix + "values_per_model"] = [
                json.dumps({m: float(v) for m, v in row.items() if pd.notna(v)})
                for _, row in per_model.iterrows()]
    if master.empty:
        return master

    # Attach the retained fraction and the reason, joined on unit_index so a unit
    # absent from the coverage table gets missing provenance rather than a wrong
    # row. ``support_gap_kind`` names which gap dominates, because a reference gap
    # is a UHE-Daily limitation at the coast while a grid gap is genuine geometry
    # and the coverage number alone cannot tell them apart.
    if coverage is not None and not coverage.empty:
        cov = coverage.set_index("unit_index")
        master["support_coverage"] = cov.retained_fraction.reindex(index).to_numpy()
        master["support_gap_kind"] = np.where(
            cov.eligible.reindex(index).fillna(False).to_numpy(), "none",
            np.where(
                cov.grid_gap_fraction.reindex(index).to_numpy()
                > cov.reference_gap_fraction.reindex(index).to_numpy(),
                "grid", "reference"))
        master["exclusion_reason"] = cov.exclusion_reason.reindex(index).to_numpy()
    return master.sort_values(identity).reset_index(drop=True)


#: One loaded assembly per worker process. Each state/slug job needs the same frame,
#: and ``yearly_metrics.csv`` is about 245 MB per model, so re-reading it per job
#: would cost tens of gigabytes of pointless I/O across a full roster.
_ASSEMBLY_CACHE: dict[tuple, tuple] = {}


def _cached_assembly(assembly_dir: Path, work_dir: Path, models: tuple[str, ...]) -> tuple:
    """Load the assembly once per worker process, keyed by what identifies it."""
    key = (str(assembly_dir), str(work_dir), models)
    if key not in _ASSEMBLY_CACHE:
        _ASSEMBLY_CACHE.clear()
        _ASSEMBLY_CACHE[key] = load_assembly(assembly_dir, work_dir, list(models))
    return _ASSEMBLY_CACHE[key]


def _state_task(job: tuple) -> dict:
    """Write one slug for one state. Returns measured counts for the manifest."""
    (state, slug, column, value_col, assembly_text, work_text, root_text,
     models) = job
    assembly_dir, work_dir, root = Path(assembly_text), Path(work_text), Path(root_text)
    frame, units, coverage = _cached_assembly(assembly_dir, work_dir, tuple(models))
    frame = frame[frame.state_name == state]
    coverage = coverage[coverage.state_name == state]
    # The state directory keeps its name verbatim, spaces and ampersands included:
    # the incumbent processed tree spells it `processed/<slug>/Andhra Pradesh/` and the
    # optimised builder carries that name straight into `state=Andhra Pradesh.parquet`.
    # Sanitising it here would publish `state=Andhra_Pradesh.parquet`, a key the
    # dashboard would not match. Only district and block names are sanitised, exactly
    # as `tools/pipeline/compute_indices.py` does.
    state_root = root / slug / state
    state_root.mkdir(parents=True, exist_ok=True)
    files = 0
    for unit_index, group in frame.groupby("unit_index", sort=False):
        unit = units.loc[unit_index]
        files += _write_unit_files(state_root, unit, group, column, value_col)
        files += _write_unit_ensemble_files(state_root, unit, group, column)
    # The full coverage and exclusion table ships inside the metric tree for exports
    # and diagnostics. The three columns the dashboard needs to explain a blank unit
    # also travel in the master, because nothing in the application reads this file
    # (CHG-0674).
    coverage.to_csv(state_root / "support_coverage.csv", index=False)
    coverage.to_parquet(state_root / "support_coverage.parquet", index=False)
    files += 2
    counts = {}
    for level, filename in (("district", "master_metrics_by_district"),
                            ("block", "master_metrics_by_block")):
        master = _master_frame(frame, units, level, column, value_col, models,
                               coverage=coverage)
        counts[level] = int(len(master))
        if master.empty:
            continue
        master.to_csv(state_root / f"{filename}.csv", index=False)
        master.to_parquet(state_root / f"{filename}.parquet", index=False)
        files += 2
    return {"state": state, "slug": slug, "files": files, "units": counts}


def publish(args: argparse.Namespace) -> dict:
    """Write every requested slug and state, then record a machine-readable manifest."""
    frame, units, coverage = load_assembly(args.assembly_dir, args.work_dir, args.models)
    states = sorted(frame.state_name.dropna().unique())
    preflight = longest_path_preflight(args.processed_root, units, list(args.slugs), args.models)
    if preflight["headroom_chars"] < 0:
        raise SystemExit(
            f"--processed-root is too deep: this run would write a "
            f"{preflight['longest_path_chars']}-character path against a "
            f"{WINDOWS_MAX_PATH}-character limit. Choose a shorter root.\n"
            f"  {preflight['longest_path_example']}")
    print(json.dumps({"path_preflight": preflight}, indent=2), flush=True)
    slugs = {s: SLUGS[s] for s in args.slugs}
    # Ordered state-major so a worker's cached assembly is reused across the five
    # slugs it writes for that state, rather than evicted between them.
    jobs = [(state, slug, column, value_col, str(args.assembly_dir), str(args.work_dir),
             str(args.processed_root), args.models)
            for state in states for slug, (column, value_col, _) in slugs.items()]
    started = time.monotonic()
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_state_task, job): job for job in jobs}
        for done in as_completed(futures):
            results.append(done.result())
            print(f"[{len(results)}/{len(jobs)}] {results[-1]['slug']} {results[-1]['state']}"
                  f" {results[-1]['files']} files", flush=True)
    manifest = {"change": "CHG-0664", "method_version": METHOD_VERSION,
                "path_preflight": preflight,
                "aggregation_method": AGGREGATION_METHOD, "released_method": RELEASED_METHOD,
                "models": args.models, "model_count": len(args.models),
                "slugs": {s: {"value_col": v[1], "units": v[2], "diagnostic_column": v[0]}
                          for s, v in slugs.items()},
                "envelope": ENVELOPE, "periods": {k: [p[0] for p in v] for k, v in PERIODS.items()},
                "states": states, "units_total": int(len(units)),
                "processed_root": str(args.processed_root),
                "unit_coverage_gate": UNIT_COVERAGE_GATE,
                "units_eligible": int(coverage.eligible.sum()),
                "units_excluded": int((~coverage.eligible).sum()),
                "exclusion_reasons": {k: int(v) for k, v
                                      in coverage.exclusion_reason.value_counts().items()},
                "files_written": sum(r["files"] for r in results),
                "elapsed_seconds": round(time.monotonic() - started, 1)}
    (args.processed_root / "route_b_outdoor_release_manifest.json").write_text(
        json.dumps(manifest, indent=2))
    return manifest


def main(argv: list[str] | None = None) -> int:
    """Plan or write the processed layer for the outdoor Route B slugs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Execute; default is a read-only plan")
    parser.add_argument("--assembly-dir", type=Path,
                        default=Path("scratch/wbgt_route_b_national/assembly"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--processed-root", type=Path,
                        default=Path("C:/irt_route_b_stage"),
                        help="Staging root for the processed tree. Keep it on C:. Measured "
                             "from the Windows geo environment on 2026-10-06: 36 small "
                             "files/s to D: against 636 to C:. A WSL ext4 path is faster "
                             "still but unreadable by the optimised builder")
    parser.add_argument("--models", nargs="+", default=None, help="Model names, or 'all'")
    parser.add_argument("--slugs", nargs="+", default=list(SLUGS))
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)
    unknown = sorted(set(args.slugs) - set(SLUGS))
    if unknown:
        parser.error(f"Unknown slugs: {unknown}")
    if not args.models or args.models == ["all"]:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from tools.diagnostics.wbgt_route_b_prepare import ENSEMBLE_MODELS
        args.models = list(ENSEMBLE_MODELS)
    available = [m for m in args.models
                 if (args.assembly_dir / m / "yearly_metrics.csv").is_file()]
    if not args.run:
        print(json.dumps({"action": "plan_only", "slugs": args.slugs,
                          "models_requested": args.models,
                          "models_assembled": available,
                          "models_missing": sorted(set(args.models) - set(available)),
                          "processed_root": str(args.processed_root),
                          "workers": args.workers}, indent=2))
        return 0
    if sorted(available) != sorted(args.models):
        raise SystemExit("Refusing to publish a partial roster: "
                         f"{sorted(set(args.models) - set(available))} are not assembled. "
                         "Pass --models explicitly to stage a deliberate subset.")
    args.processed_root.mkdir(parents=True, exist_ok=True)
    print(json.dumps(publish(args), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
