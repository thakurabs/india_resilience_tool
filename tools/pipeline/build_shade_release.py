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

    Read failures are counted rather than swallowed. A tree whose root does not exist
    legitimately contributes zero (``root_absent``); a tree that exists but cannot be fully
    walked is reported ``complete=False`` so callers never mistake an unreadable tree for a
    small one and clear a space check on an understated requirement.
    """
    total = 0
    files = 0
    errors = 0
    absent = not root.exists()
    pending = [] if absent else [str(root)]
    while pending:
        try:
            entries = list(os.scandir(pending.pop()))
        except OSError:
            errors += 1
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                    files += 1
            except OSError:
                errors += 1
                continue
    return {"bytes": total, "files": files, "reserved_bytes": total + block_bytes * files,
            "read_errors": errors, "root_absent": absent, "complete": errors == 0}


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

    The cache is bound to the resolved published root it was measured from. A cache written
    for another root -- or by a version that did not record one -- is discarded rather than
    reused, because sizes from a different data directory would silently understate the
    rollback requirement. Trees that could not be fully walked are remeasured on resume.
    """
    resolved_root = str(data_dir.resolve())
    cached: dict = {}
    if cache_path.exists() and not remeasure:
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            cached = {}
    if cached and cached.get("data_root") != resolved_root:
        print(json.dumps({"discarded_rollback_cache": cached.get("data_root"),
                          "current_data_root": resolved_root}), flush=True)
        cached = {}
    cached["data_root"] = resolved_root
    trees = cached.setdefault("trees", {})
    for base in ROLLBACK_BASES:
        for slug in sorted(SHADE_SLUGS):
            relative = f"{base}/{slug}"
            if trees.get(relative, {}).get("complete"):
                continue
            started = time.time()
            measured = tree_size(data_dir / base / slug)
            measured["measured_unix"] = started
            measured["measured_seconds"] = time.time() - started
            trees[relative] = measured
            write_json(cache_path, cached)
            print(json.dumps({"measured_rollback_tree": relative, **measured}), flush=True)
    incomplete = sorted(k for k, t in trees.items() if not t.get("complete"))
    cached["reserved_bytes"] = sum(t["reserved_bytes"] for t in trees.values())
    cached["oldest_measurement_unix"] = min(t["measured_unix"] for t in trees.values())
    cached["incomplete_trees"] = incomplete
    cached["complete"] = not incomplete
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


def boundary_identity(data_dir: Path) -> dict[str, dict[str, int]]:
    """Identify the boundary layers a staged tree was built against.

    Size and modification time detect a replaced layer, which would change the canonical unit
    roster underneath a resumed build. They do not detect an in-place edit that preserves
    both, so this guards against boundary migrations rather than against tampering.
    """
    identity: dict[str, dict[str, int]] = {}
    for level in ("districts", "blocks", "states"):
        path = data_dir / f"{level}_4326.geojson"
        try:
            status = path.stat()
        except OSError:
            identity[level] = {"bytes": -1, "mtime_ns": -1}
            continue
        identity[level] = {"bytes": status.st_size, "mtime_ns": status.st_mtime_ns}
    return identity


def compute_source_root(data_dir: Path) -> Path:
    """Derive the raw archive the spawned compute workers will actually read.

    ``build`` sets ``IRT_DATA_DIR`` to ``data_dir`` for every spawned stage and
    ``config.paths`` resolves the archive beneath it, so the archive name is taken from that
    resolver rather than restated here. Preflighting any other path would verify inputs the
    build never opens.
    """
    from india_resilience_tool.config.paths import get_paths_config

    return data_dir / get_paths_config().data_root.name


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
    rollback_complete = None if rollback_report is None else bool(rollback_report["complete"])

    # Staging lives on the stage volume; the promotion copy and retained previous release
    # land on the published volume, which is not necessarily the same device.
    volumes = volume_free_bytes({"stage": stage, "published": data_dir})
    need = {"stage": staged, "published": staged + (rollback or 0)}
    # An understated requirement must not clear the check: an unreadable published tree is
    # indistinguishable from a small one by size alone, so it withholds the verdict entirely.
    space_passed: bool | None = None if rollback is None or not rollback_complete else True
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
        "data_root": str(data_dir.resolve()), "boundary_source": boundary_identity(data_dir),
        "workers": 1, "national_units": int(units), "pilot_units": pilot_units,
        "pilot_model_years": pilot_years, "model_year_unit_scale": scale, "pilot": pilot_size,
        "measured_pilot_compute_seconds": pilot_compute,
        "measured_pilot_downstream_seconds": pilot_downstream,
        "estimated_compute_seconds": compute,
        "estimated_downstream_seconds": pilot_downstream * scale,
        "estimated_total_hours": (compute + pilot_downstream * scale) / 3600,
        "estimated_staged_reserved_bytes": staged,
        "existing_release_rollback_reserved_bytes": rollback,
        "rollback_measurement_complete": rollback_complete,
        "rollback_incomplete_trees": (None if rollback_report is None
                                      else rollback_report["incomplete_trees"]),
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
    elif not rollback_complete:
        result["limitations"] += (
            " INCOMPLETE ROLLBACK MEASUREMENT: one or more published trees could not be fully "
            "read, so their sizes understate the rollback requirement and no space verdict is "
            "available. Resolve the read failures and re-run; listed trees are remeasured.")
    write_json(report_path, result)
    return result


def preflight_inputs(source_root: Path, roster: dict) -> dict:
    """Confirm every rostered model-year input exists before days of compute begin.

    ``validate_yearly`` demands the exact roster year set, but the compute CLI derives years
    from whatever inputs are present, so a single absent model-year would otherwise surface
    only in post-build validation. One directory listing per scenario/variable/model is enough,
    so this runs on every build rather than only when an archive path is supplied.
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


def validate_state_compute(stage: Path, state: str, roster: dict, *, data_dir: Path) -> None:
    """Validate native per-unit yearly outputs before any masters exist.

    Derive expected units from the same boundaries and filename normalization as
    compute, so a wholly missing unit cannot disappear from the validation roster.
    Missing values are valid records; missing or duplicate years are not.
    """
    import pandas as pd
    from tools.pipeline.compute_indices_multiprocess import load_boundaries, _safe_component

    for level in ("district", "block"):
        boundaries = load_boundaries(data_dir / f"{level}s_4326.geojson",
                                     state_filter=state, level=level)
        columns = ["district_name"] + (["block_name"] if level == "block" else [])
        units = list(boundaries[columns].drop_duplicates().itertuples(index=False, name=None))
        if not units:
            raise ValueError(f"Empty boundary roster: {state}/{level}")
        tokens = [tuple(_safe_component(name) for name in unit) for unit in units]
        if len(set(tokens)) != len(units):
            raise ValueError(f"Boundary filename collision: {state}/{level}")
        for slug in sorted(SHADE_SLUGS):
            root = stage / "processed" / slug / state / f"{level}s"
            for unit, parts in zip(units, tokens):
                directory = root.joinpath(*parts)
                if not directory.is_dir():
                    raise ValueError(f"Missing staged unit directory: {directory}")
                models = {p.name for p in directory.iterdir() if p.is_dir()}
                if models != set(roster["models"]):
                    raise ValueError(f"Model roster mismatch: {directory}")
                for model in roster["models"]:
                    model_dir = directory / model
                    scenarios = {p.name for p in model_dir.iterdir() if p.is_dir()}
                    if scenarios != set(roster["required_years"]):
                        raise ValueError(f"Scenario roster mismatch: {model_dir}")
                    for scenario, years in roster["required_years"].items():
                        path = model_dir / scenario / f"{parts[-1]}_yearly.csv"
                        if not path.is_file():
                            raise ValueError(f"Missing staged yearly output: {path}")
                        frame = pd.read_csv(path)
                        require_shade_signature(frame, context=str(path))
                        required = {"year", "model", "scenario", "value", "district"}
                        if level == "block":
                            required.add("block")
                        if not required.issubset(frame.columns):
                            raise ValueError(f"Missing yearly columns: {path}")
                        if frame.empty or frame["year"].duplicated().any() or set(frame["year"]) != set(years):
                            raise ValueError(f"Incomplete/duplicate yearly roster: {path}")
                        identities = {"model": model, "scenario": scenario, "district": unit[0]}
                        if level == "block":
                            identities["block"] = unit[1]
                        for column, expected in identities.items():
                            if not frame[column].eq(expected).all():
                                raise ValueError(f"Yearly {column} identity mismatch: {path}")


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
            "data_root": specification["data_root"],
            "boundary_source": specification["boundary_source"],
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
    """Take the build lock, breaking one left by a process that is provably gone.

    The holder is probed *before* ``force`` is honoured: two builds writing one stage would
    interleave compute markers and each could delete the other's lock, so a provably live
    holder is refused unconditionally. ``force`` covers only the case this platform cannot
    decide -- an unprobeable PID -- where the operator has to make the call instead.
    """
    lock = stage / ".build.lock"
    record = json.dumps({"pid": os.getpid(), "started_unix": time.time()})
    stage.mkdir(parents=True, exist_ok=True)
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
    if alive is True:
        raise RuntimeError(
            f"Build lock {lock} is held by PID {pid}, which is still running. "
            "--force cannot break a live lock; stop that build first."
        )
    if alive is False:
        print(json.dumps({"broke_stale_lock": str(lock), "dead_pid": pid}), flush=True)
        lock.write_text(record, encoding="utf-8")
        return lock
    if force:
        print(json.dumps({"forced_unprobeable_lock": str(lock), "unprobeable_pid": pid}),
              flush=True)
        lock.write_text(record, encoding="utf-8")
        return lock
    raise RuntimeError(
        f"Build lock {lock} is held by PID {pid}, whose liveness cannot be probed on this "
        "platform; confirm it is not running, then pass --force."
    )


def release_lock(lock: Path) -> None:
    """Release the lock only while this process still owns it.

    An unconditional unlink would let a build that took the lock over a stale record delete a
    lock subsequently written by another process.
    """
    try:
        held = json.loads(lock.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if held.get("pid") == os.getpid():
        lock.unlink(missing_ok=True)


def build(data_dir: Path, stage: Path, specification: dict, *,
          source_root: Path | None = None) -> None:
    """Resume compute markers and run downstream builders with isolated destinations."""
    if not specification["space_passed"]:
        raise ValueError("Insufficient space for staged release, promotion copy and rollback")
    guard_build_spec(stage, specification)
    # A record from an earlier attempt must never outlive the outputs it described.
    (stage / "release_ready.json").unlink(missing_ok=True)
    roster = specification["roster"]
    # Preflight the archive compute will read, not one supplied alongside it: a pass against
    # any other path says nothing about the inputs this build opens. --source-root is kept as
    # an explicit assertion that the operator's expected archive is that same path.
    derived_source = compute_source_root(data_dir)
    if source_root is not None and source_root.resolve() != derived_source.resolve():
        raise ValueError(
            f"--source-root {source_root} is not the archive this build will read "
            f"({derived_source}); compute resolves its inputs from --data-dir. Pass the "
            "matching path or omit --source-root."
        )
    write_json(stage / "input_preflight.json", preflight_inputs(derived_source, roster))
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
        validation_label = "validate_compute_" + state.replace(" ", "_")
        started = time.time()
        write_json(status_path, {"current_stage": validation_label, "started_unix": started,
                                 "completed": events})
        try:
            validate_state_compute(stage, state, roster, data_dir=data_dir)
        except Exception as exc:
            write_json(status_path, {"current_stage": validation_label, "completed": events,
                                     "failed": True, "error": f"{type(exc).__name__}: {exc}"})
            raise
        events.append({"stage": validation_label, "seconds": time.time() - started, "exit_code": 0})
        write_json(status_path, {"completed": events, "failed": False})
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
                        help="Assert the raw NEX archive compute will read; every build "
                             "preflights the archive derived from --data-dir either way")
    parser.add_argument("--build", action="store_true", help="Run the national build after budget/free-space checks")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip the multi-minute published-tree walk; report no space verdict")
    parser.add_argument("--remeasure-rollback", action="store_true",
                        help="Discard cached published-tree sizes and measure them again")
    parser.add_argument("--force", action="store_true",
                        help="Break a build lock whose holder cannot be probed on this "
                             "platform; a provably live holder is always refused")
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
            release_lock(lock)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
