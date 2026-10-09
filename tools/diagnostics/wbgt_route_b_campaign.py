"""Drive the national Route B outdoor-WBGT roster through a resumable staged queue.

CHG-0662. Orchestration only: this module computes nothing itself and never
imports the fitted mathematics. It is deliberately separate from
``wbgt_route_b_run`` and ``wbgt_route_b``, which are covered by the shared cache
signature -- editing either invalidates every fitted cache, including for edits
unrelated to the mathematics, so orchestration must not live beside them.

Seven stages run per model, each resumable and each recording its own exit code,
log path, elapsed time and verdict:

1. ``preflight``  every required driver file exists for the declared envelope.
2. ``drivers``    extract native NEX cells (IRT geo environment; needs netCDF4).
3. ``prepare``    load the drivers once and write per-cell shade series.
4. ``fit``        N signature-checked shard workers write the per-cell QDM caches.
5. ``verify``     the cache set is complete and no cache is stale.
6. ``assemble``   one assembling run turns caches into metrics.
7. ``validate``   the release-gate criteria of RELEASE_SPEC section 10.

A completed stage is skipped, not repeated. A failed ``validate`` stops the queue
advancing to further models, because a methodology or input defect must not be
multiplied across the roster. A lock file per model refuses a second concurrent
launch, so shard workers can never be started twice over the same caches.

Default is a read-only plan. ``--run`` executes.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
STAGES = ("preflight", "drivers", "prepare", "fit", "verify", "assemble", "validate")

#: Declared envelope; kept in step with RELEASE_SPEC section 3 and
#: wbgt_route_b_prepare.PERIODS. Radiation and wind exist only over the
#: reporting baseline and the future window, so they are required there alone.
PERIODS = {"historical": (1985, 2014), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}
ALWAYS = ("tas", "tasmax", "huss")
RADIATION = ("rsds", "sfcWind")
RADIATION_HISTORICAL = (1990, 2010)


def driver_path(data_dir: Path, model: str, scenario: str, year: int, variable: str) -> Path:
    """Mirror ``wbgt_route_b_prepare.driver_path`` without importing its geo dependencies."""
    base = data_dir / "nex_gddp_cmip6_v2_wbgt" if variable in RADIATION else data_dir
    return base / "r1i1p1f1" / scenario / variable / model / f"{year}.nc"


def required_inputs(data_dir: Path, model: str) -> list[Path]:
    """Every driver file the declared envelope needs for one model."""
    paths = []
    for scenario, (first, last) in PERIODS.items():
        for year in range(first, last + 1):
            variables = list(ALWAYS)
            if scenario != "historical" or RADIATION_HISTORICAL[0] <= year <= RADIATION_HISTORICAL[1]:
                variables += list(RADIATION)
            paths.extend(driver_path(data_dir, model, scenario, year, v) for v in variables)
    return paths


def atomic_write(path: Path, text: str) -> None:
    """Replace a state file in one step so an interrupted run never truncates it."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text)
    os.replace(temporary, path)


class State:
    """The campaign's durable record of what has genuinely completed."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data = json.loads(path.read_text()) if path.is_file() else {"models": {}}

    def stage(self, model: str, stage: str) -> dict:
        return self.data["models"].setdefault(model, {}).setdefault(stage, {})

    def done(self, model: str, stage: str) -> bool:
        return self.stage(model, stage).get("verdict") == "PASS"

    def record(self, model: str, stage: str, **fields) -> None:
        self.stage(model, stage).update(fields)
        self.data["updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        atomic_write(self.path, json.dumps(self.data, indent=2))


class Lock:
    """Refuse a second concurrent campaign over the same work root."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> "Lock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            held = self.path.read_text().strip()
            raise SystemExit(f"Campaign already locked by {held}; refusing to start a "
                             f"second run. Remove {self.path} only if that process is gone.")
        os.write(handle, f"pid={os.getpid()} started={time.ctime()}".encode())
        os.close(handle)
        return self

    def __exit__(self, *_) -> None:
        self.path.unlink(missing_ok=True)


def run_command(command: list[str], log: Path, cwd: Path = REPO) -> dict:
    """Run one stage command, capturing its exit code, elapsed time and log path."""
    log.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log.open("w") as handle:
        completed = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, cwd=cwd)
    return {"exit_code": completed.returncode, "elapsed_seconds": round(time.monotonic() - started, 1),
            "log": str(log.relative_to(REPO)) if log.is_relative_to(REPO) else str(log),
            "command": command}


def fit_status(python: str, model: str, work: Path) -> dict:
    """Read the fitting module's own cache accounting; never infer it from file counts."""
    completed = subprocess.run(
        [python, "-B", "-m", "tools.diagnostics.wbgt_route_b_fit", "--status",
         "--model", model, "--work-dir", str(work)],
        capture_output=True, text=True, cwd=REPO)
    if completed.returncode != 0:
        return {"error": completed.stderr.strip()[-2000:]}
    return json.loads(completed.stdout)


def stage_preflight(args, model: str, state: State) -> dict:
    """Confirm every declared input exists before any compute is spent on the model."""
    missing = [str(p) for p in required_inputs(args.data_dir, model) if not p.is_file()]
    return {"verdict": "FAIL" if missing else "PASS", "required_files":
            len(required_inputs(args.data_dir, model)), "missing_files": len(missing),
            "missing_sample": missing[:10]}


def stage_drivers(args, model: str, state: State) -> dict:
    """Extract native NEX cells. Cached shards are verified against their sources, not skipped blind."""
    result = run_command(
        [args.geo_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_prepare",
         "--stage", "drivers", "--run", "--models", model, "--states", "all",
         "--work-dir", str(args.work_dir), "--workers", str(args.driver_workers)],
        args.log_dir / f"{model}_drivers.log")
    return {**result, "verdict": "PASS" if result["exit_code"] == 0 else "FAIL"}


def stage_prepare(args, model: str, state: State) -> dict:
    """Write the per-cell shade series once, so no shard worker recomputes them."""
    result = run_command(
        [args.pinned_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_fit",
         "--prepare", "--model", model, "--work-dir", str(args.work_dir)],
        args.log_dir / f"{model}_prepare.log")
    status = fit_status(args.pinned_python, model, args.work_dir)
    return {**result, "status": status,
            "verdict": "PASS" if result["exit_code"] == 0 and status.get("prepared_series") else "FAIL"}


def stage_fit(args, model: str, state: State) -> dict:
    """Launch the shard workers and wait, recording per-worker exit codes.

    Throughput is recorded per model and per shard count so a concurrency change
    is judged on measured completed series, not on an impression of speed.
    """
    before = fit_status(args.pinned_python, model, args.work_dir)
    if before.get("remaining") == 0:
        return {"verdict": "PASS", "skipped": "already complete", "status": before}
    args.log_dir.mkdir(parents=True, exist_ok=True)
    (args.work_dir / "fit_logs").mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    workers = []
    for shard in range(1, args.shards + 1):
        log = args.work_dir / "fit_logs" / f"{model}_shard{shard:03d}.log"
        handle = log.open("w")
        workers.append((shard, handle, subprocess.Popen(
            [args.pinned_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_fit",
             "--fit", "--model", model, "--shard", str(shard), "--shards", str(args.shards),
             "--work-dir", str(args.work_dir)],
            stdout=handle, stderr=subprocess.STDOUT, cwd=REPO)))
    codes = {}
    for shard, handle, process in workers:
        codes[shard] = process.wait()
        handle.close()
    elapsed = time.monotonic() - started
    after = fit_status(args.pinned_python, model, args.work_dir)
    fitted = (after.get("cached_series", 0) or 0) - (before.get("cached_series", 0) or 0)
    failures = {s: c for s, c in codes.items() if c != 0}
    return {"verdict": "PASS" if not failures and after.get("remaining") == 0 else "FAIL",
            "shards": args.shards, "elapsed_seconds": round(elapsed, 1),
            "failed_shards": failures, "series_fitted": fitted,
            "series_per_hour": round(fitted / elapsed * 3600, 1) if elapsed else None,
            "status_before": before, "status_after": after}


def stage_verify(args, model: str, state: State) -> dict:
    """The cache set must be complete and no cache may be stale under the signature."""
    status = fit_status(args.pinned_python, model, args.work_dir)
    stale = run_command(
        [args.pinned_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_fit",
         "--prune-stale", "--model", model, "--work-dir", str(args.work_dir)],
        args.log_dir / f"{model}_verify.log")
    text = Path(stale["log"] if Path(stale["log"]).is_absolute()
                else REPO / stale["log"]).read_text()
    try:
        audit = json.loads(text)
    except json.JSONDecodeError:
        audit = {"raw": text[-2000:]}
    stale_count = audit.get("stale_caches", audit.get("stale"))
    complete = status.get("remaining") == 0 and bool(status.get("cached_series"))
    return {"verdict": "PASS" if complete and stale_count == 0 else "FAIL",
            "status": status, "stale_audit": audit, "prune_exit_code": stale["exit_code"]}


def stage_assemble(args, model: str, state: State) -> dict:
    """Turn the caches into metrics. Caches read at about zero seconds per series."""
    out = args.out_dir / model
    if (out / "run_manifest.json").is_file() and not args.reassemble:
        return {"verdict": "PASS", "skipped": "run_manifest.json already present",
                "out_dir": str(out)}
    result = run_command(
        [args.pinned_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_run",
         "--run", "--grid-first-only", "--model", model,
         "--work-dir", str(args.work_dir), "--out-dir", str(args.out_dir)],
        args.log_dir / f"{model}_assemble.log")
    return {**result, "out_dir": str(out),
            "verdict": "PASS" if result["exit_code"] == 0
                       and (out / "run_manifest.json").is_file() else "FAIL"}


def stage_validate(args, model: str, state: State) -> dict:
    """Apply the declared release-gate criteria; a failure stops the queue."""
    result = run_command(
        [args.geo_python, "-u", "-B", "-m", "tools.diagnostics.wbgt_route_b_validate",
         "--model-dir", str(args.out_dir / model), "--work-dir", str(args.work_dir),
         "--report"],
        args.log_dir / f"{model}_validate.log")
    report_path = args.out_dir / model / "validation_report.json"
    report = json.loads(report_path.read_text()) if report_path.is_file() else {}
    return {**result, "verdict": report.get("verdict", "FAIL"),
            "failed_checks": report.get("failed_checks", ["validation report absent"]),
            "report": str(report_path)}


HANDLERS = {"preflight": stage_preflight, "drivers": stage_drivers, "prepare": stage_prepare,
            "fit": stage_fit, "verify": stage_verify, "assemble": stage_assemble,
            "validate": stage_validate}


def resolve_models(requested: list[str] | None) -> list[str]:
    """Resolve the roster through the preparation module, so one roster governs both."""
    sys.path.insert(0, str(REPO))
    from tools.diagnostics.wbgt_route_b_prepare import ENSEMBLE_MODELS

    if not requested or requested == ["all"]:
        return list(ENSEMBLE_MODELS)
    unknown = sorted(set(requested) - set(ENSEMBLE_MODELS))
    if unknown:
        raise SystemExit(f"Models lack complete Route B inputs: {unknown}")
    return list(dict.fromkeys(requested))


def main(argv: list[str] | None = None) -> int:
    """Plan or run the staged national campaign. A partial roster is never reported ready."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Execute; default is a read-only plan")
    parser.add_argument("--models", nargs="+", default=None, help="Model names, or 'all'")
    parser.add_argument("--stages", nargs="+", default=list(STAGES),
                        help=f"Subset of {list(STAGES)}, in the declared order")
    parser.add_argument("--shards", type=int, default=24, help="Shard workers per model")
    parser.add_argument("--driver-workers", type=int, default=12)
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--out-dir", type=Path, default=Path("scratch/wbgt_route_b_national/assembly"))
    parser.add_argument("--data-dir", type=Path, default=Path("/mnt/d/projects/irt_data"))
    parser.add_argument("--pinned-python", default="/tmp/irt-carbonplan-py310/bin/python",
                        help="Pinned xclim 0.44 / thermofeel 1.3 environment")
    parser.add_argument("--geo-python",
                        default="/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe",
                        help="IRT geo environment; the drivers stage needs netCDF4")
    parser.add_argument("--reassemble", action="store_true",
                        help="Re-run assembly even where run_manifest.json exists")
    parser.add_argument("--continue-on-failure", action="store_true",
                        help="Keep going after a model fails. Off by default: a methodology "
                             "or input defect must not be multiplied across the roster")
    args = parser.parse_args(argv)
    unknown_stages = [s for s in args.stages if s not in STAGES]
    if unknown_stages:
        parser.error(f"Unknown stages: {unknown_stages}")
    args.stages = [s for s in STAGES if s in args.stages]
    args.log_dir = args.work_dir / "campaign_logs"
    models = resolve_models(args.models)
    state = State(args.work_dir / "campaign_state.json")

    if not args.run:
        plan = {"action": "plan_only", "models": models, "stages": args.stages,
                "shards": args.shards, "driver_workers": args.driver_workers,
                "work_dir": str(args.work_dir), "out_dir": str(args.out_dir),
                "pending": {m: [s for s in args.stages if not state.done(m, s)] for m in models}}
        print(json.dumps(plan, indent=2))
        return 0

    with Lock(args.work_dir / "campaign.lock"):
        summary = {}
        for model in models:
            summary[model] = {}
            for stage in args.stages:
                if state.done(model, stage):
                    summary[model][stage] = "SKIP"
                    continue
                print(f"[{model}] {stage} starting", flush=True)
                record = HANDLERS[stage](args, model, state)
                state.record(model, stage, **record,
                             finished=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
                summary[model][stage] = record["verdict"]
                print(f"[{model}] {stage} {record['verdict']}", flush=True)
                if record["verdict"] != "PASS":
                    break
            if any(v == "FAIL" for v in summary[model].values()) and not args.continue_on_failure:
                print(f"[{model}] stopping the queue: a stage failed and "
                      f"--continue-on-failure was not given", flush=True)
                break
        complete = [m for m in models
                    if all(state.done(m, s) for s in args.stages)]
        print(json.dumps({"models_requested": len(models), "models_complete": len(complete),
                          "complete": complete, "summary": summary,
                          "release_ready": len(complete) == len(models)}, indent=2))
        return 0 if len(complete) == len(models) else 1


if __name__ == "__main__":
    raise SystemExit(main())
