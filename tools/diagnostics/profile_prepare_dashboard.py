#!/usr/bin/env python3
"""End-to-end wall-clock timer for a real ``prepare_dashboard`` run.

Wraps the orchestrator and times every stage it executes -- load+compute, the
exposure/context masters, admin aggregation, bundle assembly, and the optimized
publish -- so we get a MEASURED end-to-end cost for one ``(model, scenario)``
across all sectoral and thematic bundles, and can reconcile the per-stage
Telangana compute numbers against the real full-build wall clock.

How it works
------------
``prepare_dashboard`` plans the build as a list of labelled ``PlannedCommand``s
and runs each one via ``subprocess.run`` inside a single function,
``execute_plan``. Both dispatch paths (``climate-hazards`` and the general
command plan) funnel through it. This wrapper monkeypatches ``execute_plan`` with
a timing version and then calls ``prepare_dashboard.main`` UNCHANGED, so it
inherits the real arg parsing, plan building, readiness gating, and post-run
checks -- it cannot drift from the orchestrator.

Two totals are reported, deliberately:
    * sum-of-stages -- wall-clock summed over the timed ``subprocess.run`` steps;
    * true wall     -- the whole ``prepare_dashboard.main`` call, which also
      includes the climate-readiness pre/post scans and interpreter overhead.
The gap (``unattributed``) is exactly the work that happens OUTSIDE execute_plan,
and it matters when setting this figure against the ~41 h full build.

WRITES OUTPUTS
--------------
This drives the real pipeline; it is NOT read-only. For a genuine cold-path
number pass ``--overwrite`` -- which clobbers processed outputs -- so run it
against a DISPOSABLE bundle (point ``IRT_DATA_DIR`` at a scratch copy). Use
``--dry-run`` or ``--plan-only`` first to see the exact stage list with no writes
and no timing.

Note: timings accumulate in a module-global list, so calling ``main`` twice in
one process appends rather than resets -- fine for one-shot CLI use.

Examples
--------
    # See the stage plan (no writes, no timing):
    python -m tools.diagnostics.profile_prepare_dashboard \
        climate-hazards --level district --models CanESM5 \
        --scenarios historical --plan-only

    # Time a real cold-path run on a scratch bundle, dump per-stage JSON.
    # The leading ``--`` separates wrapper flags from forwarded args and is
    # stripped before forwarding:
    IRT_DATA_DIR=/scratch/irt_data_copy \
    python -m tools.diagnostics.profile_prepare_dashboard \
        --profile-json /tmp/e2e.json -- \
        climate-hazards --level district --models CanESM5 \
        --scenarios historical --overwrite
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import tools.runs.prepare_dashboard as pd

# --- stage classification -------------------------------------------------
# Ordered (prefix, canonical-stage) rules; first match wins. Prefixes are
# matched against PlannedCommand.label. Kept declarative so it degrades to
# "other" (printed explicitly) if labels drift.
STAGE_RULES: tuple[tuple[str, str], ...] = (
    ("blocks-geojson", "00_prep"),
    ("climate-compute", "01_load+compute"),
    ("population-admin-masters", "02_exposure_context"),
    ("lulc-admin-masters", "02_exposure_context"),
    ("built-up-area-admin-masters", "02_exposure_context"),
    ("rural-facilities-admin-masters", "02_exposure_context"),
    ("groundwater-district-masters", "02_exposure_context"),
    ("jrc-flood-depth-admin-masters", "02_exposure_context"),
    ("aqueduct-baseline", "02_exposure_context"),
    ("aqueduct-admin-masters", "02_exposure_context"),
    ("aqueduct-hydro-masters", "02_exposure_context"),
    ("climate-masters", "03_admin_aggregation"),
    ("admin-exposure-summary", "03_admin_aggregation"),
    ("composite-masters", "04_bundle_assembly"),
    ("proposal-bundles", "04_bundle_assembly"),
    ("processed-optimised-build", "05_optimized_publish"),
    ("processed-optimised-audit", "05_optimized_publish"),
    ("aqueduct-validate", "06_validation"),
    ("pytest-validation", "06_validation"),
)


def classify(label: str) -> str:
    """Map a PlannedCommand label to a canonical pipeline stage."""
    for prefix, stage in STAGE_RULES:
        if label.startswith(prefix):
            return stage
    return "99_other"


@dataclass
class TimedStep:
    """One executed step's wall-clock and outcome."""

    idx: int
    label: str
    stage: str
    seconds: float
    returncode: int


# Records accumulate here across any number of execute_plan calls in one run.
_RECORDS: list[TimedStep] = []


def _stage_totals() -> dict[str, float]:
    """Sum recorded wall-clock per canonical stage."""
    out: dict[str, float] = {}
    for r in _RECORDS:
        out[r.stage] = out.get(r.stage, 0.0) + r.seconds
    return out


def _timed_execute_plan(
    plan: Sequence[pd.PlannedCommand], *, dry_run: bool, plan_only: bool
) -> int:
    """Drop-in replacement for prepare_dashboard.execute_plan that times steps.

    Mirrors the original's control flow (print, run, stop-on-failure). When
    planning/dry-running there is nothing to time, so it defers to the original.
    """
    if dry_run or plan_only:
        return _ORIG_EXECUTE_PLAN(plan, dry_run=dry_run, plan_only=plan_only)

    print("PREPARE DASHBOARD RUN [TIMED]")
    print(f"steps: {len(plan)}")
    if not plan:
        print("  Nothing to do.")
        return 0

    for idx, step in enumerate(plan, start=1):
        stage = classify(step.label)
        print(f"[{idx}/{len(plan)}] RUN {step.label}  (stage={stage})")
        print(f"  {shlex.join(step.argv)}")
        t0 = time.perf_counter()
        rc = 0
        try:
            subprocess.run(step.argv, check=True)
        except subprocess.CalledProcessError as exc:
            rc = int(exc.returncode or 1)
        dt = time.perf_counter() - t0
        _RECORDS.append(TimedStep(idx=idx, label=step.label, stage=stage,
                                  seconds=dt, returncode=rc))
        print(f"    -> {dt:9.2f}s  (exit={rc})")
        if rc != 0:
            print(f"STEP FAILED [{idx}/{len(plan)}] {step.label} (exit={rc})")
            return rc
    return 0


_ORIG_EXECUTE_PLAN = pd.execute_plan


@contextmanager
def _patched_executor() -> Iterator[None]:
    """Install the timed executor for the duration of a prepare_dashboard run."""
    pd.execute_plan = _timed_execute_plan  # type: ignore[assignment]
    try:
        yield
    finally:
        pd.execute_plan = _ORIG_EXECUTE_PLAN  # type: ignore[assignment]


# --- reporting ------------------------------------------------------------
def _print_rollup(true_wall: float | None) -> None:
    """Print per-stage / per-step timings plus sum-of-stages vs true wall.

    ``true_wall`` is the full ``prepare_dashboard.main`` duration (None if the
    run never started). ``unattributed = true_wall - sum_of_stages`` is the work
    outside ``execute_plan`` -- readiness scans, diagnostics, interpreter start.
    """
    if not _RECORDS:
        print("\nNo timed steps (planning/dry-run, interrupted early, "
              "or empty plan).")
        return
    by_stage = _stage_totals()
    total = sum(by_stage.values())

    print("\n==================== END-TO-END ROLL-UP "
          "(one model x scenario) ====================")
    print("  -- per stage --")
    for stage in sorted(by_stage):
        secs = by_stage[stage]
        pct = 100.0 * secs / total if total else 0.0
        print(f"     {stage:24s} {secs:10.1f}s  ({pct:5.1f}%)")
    print("  -- per step --")
    for rec in _RECORDS:
        print(f"     [{rec.idx:02d}] {rec.label:38s} {rec.seconds:10.1f}s")
    print("  -- totals --")
    print(f"     sum-of-stages             {total:10.1f}s  ({total/3600:.2f}h)")
    if true_wall is not None:
        print(f"     true wall                 {true_wall:10.1f}s  "
              f"({true_wall/3600:.2f}h)")
        print(f"     unattributed              {true_wall - total:10.1f}s  "
              "<- readiness scans + interpreter overhead, outside execute_plan")
    if "99_other" in by_stage:
        print("  NOTE: '99_other' = labels not in STAGE_RULES; refine the map.")


def _emit_json(path: Path, true_wall: float | None) -> None:
    payload = {
        "sum_of_stages_seconds": sum(r.seconds for r in _RECORDS),
        "true_wall_seconds": true_wall,
        "by_stage": _stage_totals(),
        "steps": [vars(r) for r in _RECORDS],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"  wrote {path}")


def _emit_csv(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["idx", "stage", "label", "seconds", "returncode"])
        for r in _RECORDS:
            writer.writerow([r.idx, r.stage, r.label, f"{r.seconds:.3f}", r.returncode])
    print(f"  wrote {path}")


# --- entrypoint -----------------------------------------------------------
def main(argv: Sequence[str] | None = None) -> int:
    """Strip --profile-* flags, forward the rest to prepare_dashboard.main.

    The roll-up is emitted from a ``finally`` block so a long, interrupted
    cold run still reports the stages that completed.
    """
    ap = argparse.ArgumentParser(
        add_help=False,  # let prepare_dashboard own -h for the forwarded args
        description="Time a real prepare_dashboard run, per stage.")
    ap.add_argument("--profile-json", type=Path, default=None,
                    help="Write per-stage + per-step timings as JSON.")
    ap.add_argument("--profile-csv", type=Path, default=None,
                    help="Write per-step timings as CSV.")
    known, forwarded = ap.parse_known_args(list(argv) if argv is not None else None)
    # parse_known_args leaves a lone "--" separator in place; drop it so the
    # child subparser doesn't reject it as an invalid command.
    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    true_wall: float | None = None
    rc = 0
    try:
        with _patched_executor():
            t0 = time.perf_counter()
            rc = pd.main(forwarded)
            true_wall = time.perf_counter() - t0
    finally:
        _print_rollup(true_wall)
        if known.profile_json is not None:
            _emit_json(known.profile_json, true_wall)
        if known.profile_csv is not None:
            _emit_csv(known.profile_csv)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
