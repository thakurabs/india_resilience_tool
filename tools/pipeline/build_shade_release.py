"""Budget and resumably stage all four shade metrics; never replace published data."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from india_resilience_tool.data.wbgt_contract import (
    SHADE_METHOD_SIGNATURE, SHADE_SLUGS, require_shade_signature,
)

# Stage labels whose measured cost scales as downstream work rather than per-state compute.
DOWNSTREAM_STAGES = frozenset({"masters", "optimized", "state_values", "parity"})
# Raw NEX variables the shade formula consumes, laid out as <root>/<scenario>/<variable>/<model>/<year>.nc.
RAW_VARIABLES = ("tas", "tasmax", "hurs")
# Published trees that must survive a promotion so a rollback stays possible.
ROLLBACK_BASES = ("processed", "processed_optimised/metrics")


def tree_size(root: Path, *, block_bytes: int = 4096) -> dict[str, int]:
    """Measure logical bytes with running totals, reserving one extra block per file.

    Uses an explicit ``os.scandir`` walk with accumulators rather than materializing a
    size per file: the published shade trees are large enough that a list comprehension
    over ``rglob`` costs on the order of a gigabyte of resident memory.
    """
    total = 0
    files = 0
    pending = [str(root)]
    while pending:
        try:
            entries = list(os.scandir(pending.pop()))
        except (FileNotFoundError, NotADirectoryError, PermissionError):
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                    files += 1
            except OSError:
                continue
    return {"bytes": total, "files": files, "reserved_bytes": total + block_bytes * files}


def write_json(path: Path, value: object) -> None:
    """Replace a report atomically so interrupted runs leave readable status."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def measure_rollback(data_dir: Path, cache_path: Path, *, remeasure: bool) -> dict:
    """Measure published shade trees once and cache them; walking them is minutes each.

    Each tree is written back to the cache as soon as it is measured, so an interrupted
    budget keeps every completed measurement instead of starting over.
    """
    cached: dict = {}
    if cache_path.exists() and not remeasure:
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            cached = {}
    trees = cached.setdefault("trees", {})
    for base in ROLLBACK_BASES:
        for slug in sorted(SHADE_SLUGS):
            relative = f"{base}/{slug}"
            if relative in trees:
                continue
            started = time.time()
            measured = tree_size(data_dir / base / slug)
            measured["measured_unix"] = started
            measured["measured_seconds"] = time.time() - started
            trees[relative] = measured
            write_json(cache_path, cached)
            print(json.dumps({"measured_rollback_tree": relative, **measured}), flush=True)
    cached["reserved_bytes"] = sum(t["reserved_bytes"] for t in trees.values())
    cached["oldest_measurement_unix"] = min(t["measured_unix"] for t in trees.values())
    write_json(cache_path, cached)
    return cached


def volume_free_bytes(paths: dict[str, Path]) -> dict:
    """Report free space per distinct volume so a promotion target is not assumed local."""
    volumes: dict[int, dict] = {}
    for role, path in paths.items():
        probe = path
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        device = os.stat(probe).st_dev
        entry = volumes.setdefault(device, {"roles": [], "path": str(probe),
                                            "free_bytes": shutil.disk_usage(probe).free})
        entry["roles"].append(role)
    return {str(device): entry for device, entry in volumes.items()}


def pilot_model_years(pilot: Path) -> int:
    """Count the pilot's distinct model-years from its own staged output, not a literal."""
    import pandas as pd

    for slug in sorted(SHADE_SLUGS):
        directory = pilot / "processed_optimised/metrics" / slug / "yearly_models/admin/district"
        for path in sorted(directory.glob("state=*.parquet")):
            frame = pd.read_parquet(path, columns=["model", "scenario", "year"])
            if not frame.empty:
                return int(len(frame.drop_duplicates()))
    raise ValueError(
        f"Cannot measure pilot model-years: no staged yearly output under {pilot}. "
        "Re-run the three-state pilot before budgeting a national release."
    )


def split_pilot_timings(timings: list[dict]) -> tuple[float, float]:
    """Separate measured per-state compute from downstream cost by explicit stage label."""
    compute = sum(t["seconds"] for t in timings if t["stage"] not in DOWNSTREAM_STAGES)
    downstream = sum(t["seconds"] for t in timings if t["stage"] in DOWNSTREAM_STAGES)
    if not compute or not downstream:
        raise ValueError("Pilot timings must record both compute and downstream stages")
    return compute, downstream


def budget(data_dir: Path, stage: Path, evidence: Path, pilot: Path, *,
           dry_run: bool = False, remeasure_rollback: bool = False) -> dict:
    """Extrapolate measured pilot work, including downstream files and rollback.

    Creates the stage directory and writes a status record *before* the slow published-tree
    walk, so an interrupted budget is diagnosable rather than invisible.
    """
    import pandas as pd

    stage.mkdir(parents=True, exist_ok=True)
    report_path = stage / "budget.json"
    write_json(report_path, {"status": "measuring", "started_unix": time.time(),
                             "signature": SHADE_METHOD_SIGNATURE, "dry_run": dry_run})

    roster = json.loads((evidence / "release_roster.json").read_text())
    measured = json.loads((evidence / "pilot_report.json").read_text())
    timings = json.loads((evidence / "staged_downstream_timings.json").read_text())
    baseline = pd.read_csv(evidence / "published_baseline.csv")
    states = sorted(baseline.state.unique().tolist())
    units = baseline.drop_duplicates(["state", "level"]).units.sum()
    pilot_units = sum(t["units"] for t in measured["timings"] if not t["cached"])
    pilot_years = pilot_model_years(pilot)
    scale = float(units / pilot_units * roster["model_years"] / pilot_years)
    pilot_size = tree_size(pilot)
    # Includes boundaries and fixed overhead on every scaled model-year: conservative.
    staged = int(pilot_size["reserved_bytes"] * scale)

    pilot_compute, pilot_downstream = split_pilot_timings(timings)
    rollback_report = (None if dry_run
                       else measure_rollback(data_dir, stage / "rollback_sizes.json",
                                            remeasure=remeasure_rollback))
    rollback = None if rollback_report is None else int(rollback_report["reserved_bytes"])

    # Staging lives on the stage volume; the promotion copy and retained previous release
    # land on the published volume, which is not necessarily the same device.
    volumes = volume_free_bytes({"stage": stage, "published": data_dir})
    need = {"stage": staged, "published": staged + (rollback or 0)}
    space_passed: bool | None = None if rollback is None else True
    for entry in volumes.values():
        entry["required_bytes"] = sum(need[role] for role in entry["roles"])
        entry["passed"] = entry["free_bytes"] >= entry["required_bytes"]
        if space_passed is not None and not entry["passed"]:
            space_passed = False

    compute = max(measured["estimated_national_seconds"] * roster["model_years"] /
                  measured["model_years"], pilot_compute * scale)
    result = {
        "status": "measured", "dry_run": dry_run,
        "signature": SHADE_METHOD_SIGNATURE, "states": states, "roster": roster,
        "workers": 1, "national_units": int(units), "pilot_units": pilot_units,
        "pilot_model_years": pilot_years, "model_year_unit_scale": scale, "pilot": pilot_size,
        "measured_pilot_compute_seconds": pilot_compute,
        "measured_pilot_downstream_seconds": pilot_downstream,
        "estimated_compute_seconds": compute,
        "estimated_downstream_seconds": pilot_downstream * scale,
        "estimated_total_hours": (compute + pilot_downstream * scale) / 3600,
        "estimated_staged_reserved_bytes": staged,
        "existing_release_rollback_reserved_bytes": rollback,
        "required_free_bytes_including_promotion_copy":
            None if rollback is None else 2 * staged + rollback,
        "volumes": volumes, "space_passed": space_passed,
        "limitations": "Conservative unit/model-year extrapolation from one model; "
        "includes fixed pilot overhead at every scaled model-year. One worker is measured; "
        "no unmeasured parallel speedup assumed. Rollback sizes are cached from "
        "'rollback_sizes.json'; pass --remeasure-rollback after any change to the published "
        "shade trees. Recheck space before promotion.",
    }
    if dry_run:
        result["limitations"] += (" DRY RUN: published rollback trees were not measured, so "
                                 "no space verdict is available and --build is refused.")
    write_json(report_path, result)
    return result


def preflight_inputs(source_root: Path, roster: dict) -> dict:
    """Confirm every rostered model-year input exists before days of compute begin.

    ``validate_yearly`` demands the exact roster year set, but the compute CLI derives years
    from whatever inputs are present, so a single absent model-year would otherwise surface
    only in post-build validation. One directory listing per scenario/variable/model is enough.
    """
    missing: list[str] = []
    for scenario in sorted(roster["required_years"]):
        years = set(roster["required_years"][scenario])
        for variable in RAW_VARIABLES:
            for model in roster["models"]:
                directory = source_root / scenario / variable / model
                try:
                    present = {int(p.stem) for p in directory.glob("*.nc") if p.stem.isdigit()}
                except OSError:
                    present = set()
                for year in sorted(years - present):
                    missing.append(f"{scenario}/{variable}/{model}/{year}.nc")
    report = {"source_root": str(source_root), "missing_count": len(missing),
              "missing": missing[:200], "models": len(roster["models"]),
              "model_years": roster["model_years"]}
    if missing:
        raise ValueError(
            f"{len(missing)} rostered input file(s) absent under {source_root}; "
            f"first: {', '.join(missing[:5])}. Refusing to start a national build that "
            "cannot satisfy the release roster."
        )
    return report


def validate_yearly(frame, *, models: list[str], years: dict[str, list[int]],
                    unit_column: str, units: set[str]) -> None:
    """Require every expected unit/model/calendar year, retaining NaN values."""
    require_shade_signature(frame, context="staged yearly model output")
    if frame.empty:
        raise ValueError("Empty yearly output: a signature column alone proves nothing")
    keys = [unit_column, "model", "scenario", "year"]
    if frame.duplicated(keys).any() or set(frame[unit_column]) != units:
        raise ValueError("Missing/duplicate yearly administrative identifiers")
    expected = {(m, s, y) for m in models for s, ys in years.items() for y in ys}
    for unit, group in frame.groupby(unit_column):
        actual = set(group[["model", "scenario", "year"]].itertuples(index=False, name=None))
        if actual != expected:
            raise ValueError(f"Incomplete model/year roster for {unit}")


def validate_state_compute(stage: Path, state: str, roster: dict) -> None:
    """Check one state's roster coverage right after its compute stage, not days later."""
    import pandas as pd

    expected = {(m, s, y) for m in roster["models"]
                for s, ys in roster["required_years"].items() for y in ys}
    for slug in sorted(SHADE_SLUGS):
        for level in ("district", "block"):
            path = (stage / "processed" / slug / state /
                    f"state_yearly_model_averages_{level}.csv")
            frame = pd.read_csv(path)
            require_shade_signature(frame, context=str(path))
            if frame.empty:
                raise ValueError(f"Empty staged yearly averages: {path}")
            actual = set(frame[["model", "scenario", "year"]].itertuples(index=False, name=None))
            if actual != expected:
                short = sorted(expected - actual)[:5]
                raise ValueError(
                    f"{path}: staged model/year coverage does not match the release roster "
                    f"({len(expected - actual)} missing, e.g. {short}; "
                    f"{len(actual - expected)} unexpected)"
                )


def canonical_state_keys(stage: Path, level: str, state: str) -> set[str]:
    """Return the boundary roster's expected keys for one state and level.

    Derived from the staged boundary layer rather than from the master under test, so a
    whole district or block missing from an output is detectable. This is the complement of
    the publish-time canonical-roster gate, which only rejects keys that are *not* canonical.
    """
    from tools.optimized.build_processed_optimised import _canonical_admin_keys
    from india_resilience_tool.utils.naming import alias

    source = stage / ("districts_4326.geojson" if level == "district" else "blocks_4326.geojson")
    prefix = f"{alias(str(state))}|"
    return {key for key in _canonical_admin_keys(level, str(source)) if key.startswith(prefix)}


def validate_release(stage: Path, specification: dict) -> dict:
    """Validate national optimized identifiers, complete roster and provenance."""
    import pandas as pd

    root = stage / "processed_optimised" / "metrics"
    counts = {}
    coverage: dict[str, dict[str, int]] = {}
    for slug in sorted(SHADE_SLUGS):
        count = 0
        for level in ("district", "block"):
            state_values = pd.read_parquet(root / slug / "state_values/admin" / level / "all_states.parquet")
            require_shade_signature(state_values, context=f"{slug}/{level} state values")
            if set(state_values.state) != set(specification["states"]):
                raise ValueError(f"Missing state values for {slug}/{level}")
            for state in specification["states"]:
                filename = f"state={state}.parquet"
                master = pd.read_parquet(root / slug / "masters/admin" / level / filename)
                require_shade_signature(master, context=str(filename))
                key = f"{level}_key"
                units = set(master[key])
                if not units or master[key].duplicated().any():
                    raise ValueError(f"Empty/duplicate master units: {slug}/{state}/{level}")
                expected_units = canonical_state_keys(stage, level, state)
                absent = expected_units - units
                if absent:
                    raise ValueError(
                        f"{slug}/{state}/{level}: {len(absent)} canonical boundary unit(s) "
                        f"absent from the staged master, e.g. {sorted(absent)[:5]}"
                    )
                coverage.setdefault(slug, {})[f"{state}/{level}"] = len(expected_units)
                yearly = pd.read_parquet(root / slug / "yearly_models/admin" / level / filename)
                validate_yearly(yearly, models=specification["roster"]["models"],
                                years=specification["roster"]["required_years"],
                                unit_column=key, units=units)
                count += len(yearly)
        counts[slug] = count
    parity = json.loads((stage / "parity.json").read_text())
    if parity["issue_count"] or parity["metrics_considered"] != 4:
        raise ValueError("Strict four-metric parity has not passed")
    return {"signature": SHADE_METHOD_SIGNATURE, "yearly_rows": counts,
            "canonical_units_verified": coverage,
            "states": specification["states"], "roster": specification["roster"],
            "status": "staged_and_validated_not_published"}


def build_spec_of(specification: dict) -> dict:
    """Capture the inputs a staged tree was built from, for resume-safety comparison."""
    return {"signature": specification["signature"], "states": specification["states"],
            "workers": specification["workers"],
            "models": sorted(specification["roster"]["models"]),
            "required_years": {s: sorted(y) for s, y
                               in specification["roster"]["required_years"].items()}}


def guard_build_spec(stage: Path, specification: dict) -> None:
    """Refuse to resume a stage built from a different roster, state list or formula."""
    path = stage / "build_spec.json"
    current = build_spec_of(specification)
    if not path.exists():
        write_json(path, current)
        return
    previous = json.loads(path.read_text(encoding="utf-8"))
    if previous == current:
        return
    changed = sorted(k for k in set(previous) | set(current)
                     if previous.get(k) != current.get(k))
    raise ValueError(
        f"Staged tree at {stage} was built from a different specification "
        f"(changed: {', '.join(changed)}). Resuming would mix outputs across rosters. "
        "Stage the new specification in a fresh directory."
    )


def process_is_running(pid: int) -> bool | None:
    """Report liveness where it can be probed safely; None when it cannot.

    ``os.kill`` is not a safe existence probe on Windows -- CPython routes it to
    ``TerminateProcess`` for signals it does not special-case -- so this only probes on POSIX
    and otherwise reports that the operator must judge the recorded PID.
    """
    if os.name != "posix":
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_lock(stage: Path, *, force: bool) -> Path:
    """Take the build lock, breaking one left by a process that is provably gone."""
    lock = stage / ".build.lock"
    record = json.dumps({"pid": os.getpid(), "started_unix": time.time()})
    if force:
        lock.write_text(record, encoding="utf-8")
        return lock
    try:
        with lock.open("x", encoding="utf-8") as stream:
            stream.write(record)
        return lock
    except FileExistsError:
        pass
    try:
        held = json.loads(lock.read_text(encoding="utf-8"))
        pid = int(held.get("pid", -1))
    except (OSError, ValueError, TypeError):
        pid = -1
    alive = process_is_running(pid) if pid > 0 else False
    if alive is False:
        print(json.dumps({"broke_stale_lock": str(lock), "dead_pid": pid}), flush=True)
        lock.write_text(record, encoding="utf-8")
        return lock
    detail = ("is still running" if alive else
              "cannot be probed on this platform; confirm it is not running, then pass --force")
    raise RuntimeError(f"Build lock {lock} is held by PID {pid}, which {detail}.")


def build(data_dir: Path, stage: Path, specification: dict, *,
          source_root: Path | None = None) -> None:
    """Resume compute markers and run downstream builders with isolated destinations."""
    if not specification["space_passed"]:
        raise ValueError("Insufficient space for staged release, promotion copy and rollback")
    guard_build_spec(stage, specification)
    # A record from an earlier attempt must never outlive the outputs it described.
    (stage / "release_ready.json").unlink(missing_ok=True)
    roster = specification["roster"]
    if source_root is not None:
        write_json(stage / "input_preflight.json", preflight_inputs(source_root, roster))
    slugs = sorted(SHADE_SLUGS)
    metric_args = [part for slug in slugs for part in ("--metric", slug)]
    state_args = [part for state in specification["states"] for part in ("--state", state)]
    status_path = stage / "status.json"
    events: list[dict] = []
    if status_path.exists():
        try:
            events = list(json.loads(status_path.read_text(encoding="utf-8")).get("completed", []))
        except (OSError, ValueError):
            events = []

    def run(label: str, arguments: list[str], source: Path) -> None:
        started = time.time()
        write_json(status_path, {"current_stage": label, "started_unix": started,
                                 "completed": events})
        env = dict(os.environ, IRT_DATA_DIR=str(source))
        env.pop("IRT_COMPUTE_OUTPUT_ROOT", None)
        with (stage / f"{label}.log").open("a", encoding="utf-8") as log:
            result = subprocess.run([sys.executable, "-m", *arguments], env=env,
                                    stdout=log, stderr=subprocess.STDOUT)
        event = {"stage": label, "seconds": time.time() - started, "exit_code": result.returncode}
        events.append(event)
        write_json(status_path, {"completed": events, "failed": bool(result.returncode)})
        print(json.dumps(event), flush=True)
        if result.returncode:
            raise RuntimeError(f"Stage {label} failed; inspect {stage / (label + '.log')}")

    for state in specification["states"]:
        run("compute_" + state.replace(" ", "_"), [
            "tools.pipeline.compute_indices_multiprocess", "--state", state,
            "--models", *roster["models"], "--scenarios", *sorted(roster["required_years"]),
            "--metrics", *slugs, "--workers", "1", "--level", "both",
            "--yearly-cleanup-policy", "preserve", "--skip-existing",
            "--output-root", str(stage / "processed")], data_dir)
        validate_state_compute(stage, state, roster)
    run("masters", ["tools.pipeline.build_master_metrics", "--processed-root",
        str(stage / "processed"), "--level", "both", "--metrics", *slugs, "--workers", "1"], data_dir)
    for level in ("districts", "blocks", "states"):
        shutil.copy2(data_dir / f"{level}_4326.geojson", stage / f"{level}_4326.geojson")
    run("optimized", ["tools.optimized.build_processed_optimised", *metric_args, *state_args,
        "--workers", "1", "--skip-context", "--skip-audit", "--no-progress"], stage)
    run("state_values", ["tools.optimized.build_state_values", *metric_args,
        "--data-dir", str(stage), "--strict"], stage)
    run("parity", ["tools.optimized.audit_processed_optimised_parity", *metric_args,
        *state_args, "--strict", "--report-path", str(stage / "parity.json")], stage)
    write_json(stage / "release_ready.json", validate_release(stage, specification))


def main(argv: list[str] | None = None) -> int:
    """Write a measured budget; optionally run the resumable national staging build."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, default=Path("docs/diagnostics/wbgt_shade_release"))
    parser.add_argument("--pilot", type=Path, default=Path("scratch/wbgt_shade_stage"))
    parser.add_argument("--source-root", type=Path, default=None,
                        help="Raw NEX root (<root>/<scenario>/<variable>/<model>/<year>.nc) to "
                             "preflight against the release roster before compute starts")
    parser.add_argument("--build", action="store_true", help="Run the national build after budget/free-space checks")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip the multi-minute published-tree walk; report no space verdict")
    parser.add_argument("--remeasure-rollback", action="store_true",
                        help="Discard cached published-tree sizes and measure them again")
    parser.add_argument("--force", action="store_true",
                        help="Break a build lock this platform cannot prove is stale")
    args = parser.parse_args(argv)
    if args.dry_run and args.build:
        parser.error("--dry-run produces no space verdict, so it cannot authorize --build")
    data_dir, stage = args.data_dir.resolve(), args.stage.resolve()
    if stage == data_dir or stage.is_relative_to(data_dir) or data_dir.is_relative_to(stage):
        parser.error("Staging must be separate from the published data tree")
    evidence = json.loads((args.evidence / "report.json").read_text())
    if evidence["signature"] != SHADE_METHOD_SIGNATURE or len(evidence["gates"]) != 2 or not all(g["passed"] for g in evidence["gates"]):
        parser.error("Both evaluation-window release gates must pass for the current formula")
    specification = budget(data_dir, stage, args.evidence, args.pilot,
                           dry_run=args.dry_run, remeasure_rollback=args.remeasure_rollback)
    print(json.dumps({k: v for k, v in specification.items() if k not in {"roster", "states"}}, indent=2), flush=True)
    if args.build:
        lock = acquire_lock(stage, force=args.force)
        try:
            build(data_dir, stage, specification, source_root=args.source_root)
        finally:
            lock.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
