"""Finish the authorized diagnostic pilot after the bounded acquisition completes.

CHG-0650. This coordinator makes no network requests. It waits for complete
local inputs, verifies/reduces the reference, runs smoke/probes, measures cost,
runs the two models sequentially and writes the report. All failures stop the
chain and leave an atomic status file. It never publishes or starts national work.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

from tools.diagnostics.wbgt_route_b_prepare import MODEL_DEFAULTS, PERIODS


def expected_inputs(reference_dir: Path, work_dir: Path) -> tuple[list[Path], list[Path]]:
    """Enumerate every required date and driver shard, not just sentinel files."""
    import pandas as pd

    days = pd.date_range("1985-01-01", "2014-12-31")
    reference = [reference_dir / str(d.year) / f"wbgtmax.{d:%Y-%m-%d}.npz" for d in days]
    drivers = [work_dir / "drivers" / model / scenario / f"{year}.npz"
               for model in MODEL_DEFAULTS for scenario, (first, last) in PERIODS.items()
               for year in range(first, last + 1)]
    return reference, drivers


def probe_hours(probe: dict) -> float:
    """Conservatively extrapolate QDM using the slowest measured series per kind."""
    estimates = []
    for kind in ("cell", "unit"):
        measured = [float(t["seconds"]) for t in probe["timings"] if t["kind"] == kind and t["seconds"] > 0]
        if not measured:
            raise ValueError(f"No uncached {kind} cost evidence; retain the original probe.json")
        estimates.append(max(measured) * int(probe[f"total_{kind}_series"]) / 3600)
    return sum(estimates)


def finish(args: argparse.Namespace) -> None:
    """Run one locked, logged completion chain; persist failure before propagating."""
    import fcntl

    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    status_path = args.out_dir / "execution_status.json"
    state = {"status": "STARTING", "started_utc": datetime.now(timezone.utc).isoformat(),
             "models": list(MODEL_DEFAULTS), "publication": False, "completed_stages": []}

    def status(stage: str, **details) -> None:
        state.update(status=stage, updated_utc=datetime.now(timezone.utc).isoformat(), **details)
        temp = status_path.with_suffix(".part")
        temp.write_text(json.dumps(state, indent=2))
        temp.replace(status_path)
        print(json.dumps({"status": stage, **details}), flush=True)

    def execute(name: str, module: str, options: list[str], hours: float = 24) -> None:
        status(name)
        log = args.work_dir / f"finish_{name.lower()}.log"
        with log.open("a") as stream:
            subprocess.run([sys.executable, "-u", "-m", module, *options], stdout=stream,
                           stderr=subprocess.STDOUT, check=True, timeout=hours * 3600)
        state["completed_stages"].append(name)

    with (args.work_dir / "finish.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another Route B completion coordinator holds the lock") from exc
        try:
            reference, drivers = expected_inputs(args.reference_dir, args.work_dir)
            deadline = time.monotonic() + args.wait_hours * 3600
            while True:
                missing_reference = [p for p in reference if not p.is_file()]
                missing_drivers = [p for p in drivers if not p.is_file()]
                status("WAITING_FOR_INPUTS", reference_complete=len(reference) - len(missing_reference),
                       reference_expected=len(reference), driver_shards_complete=len(drivers) - len(missing_drivers),
                       driver_shards_expected=len(drivers))
                if not missing_reference and not missing_drivers:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Input wait deadline exceeded; acquisition or extraction needs inspection")
                if missing_reference and args.acquisition_log.is_file():
                    stale_seconds = time.time() - args.acquisition_log.stat().st_mtime
                    if stale_seconds > 3600:
                        raise RuntimeError("Acquisition log has not progressed for an hour; inspect before resuming")
                time.sleep(60)
            common = ["--work-dir", str(args.work_dir), "--out-dir", str(args.out_dir), "--upstream", str(args.upstream)]
            execute("VERIFY_REFERENCE", "tools.data_acquisition.uhe_daily",
                    ["--verify", "--start", "1985-01-01", "--end", "2014-12-31", "--output-dir", str(args.reference_dir)])
            execute("PREPARE_REFERENCE", "tools.diagnostics.wbgt_route_b_prepare",
                    ["--stage", "reference", "--run", "--work-dir", str(args.work_dir), "--reference-dir", str(args.reference_dir)])
            execute("SMOKE", "tools.diagnostics.wbgt_route_b_run", ["--smoke", *common])
            for model in MODEL_DEFAULTS:
                probe_path = args.out_dir / model / "probe.json"
                if not probe_path.exists():
                    execute("PROBE_" + model, "tools.diagnostics.wbgt_route_b_run", ["--probe", "--model", model, *common])
                probe = json.loads(probe_path.read_text())
                hours = probe_hours(probe)
                status("COST_CHECK_" + model, estimated_qdm_hours=hours,
                       maximum_qdm_hours=args.max_qdm_hours_per_model)
                if hours > args.max_qdm_hours_per_model:
                    raise RuntimeError(f"Measured {model} QDM cost {hours:.2f} h exceeds diagnostic budget")
                execute("PILOT_" + model, "tools.diagnostics.wbgt_route_b_run", ["--run", "--model", model, *common])
            execute("REPORT", "tools.diagnostics.wbgt_route_b_report", ["--run", "--out-dir", str(args.out_dir), "--work-dir", str(args.work_dir)])
            status("PILOT_COMPLETE_REVIEW_REQUIRED", report=str(args.out_dir / "README.md"))
        except Exception as exc:
            status("FAILED", error_type=type(exc).__name__, error=str(exc))
            raise


def main(argv: list[str] | None = None) -> int:
    """Default is a no-write plan; --run starts the local sequential coordinator."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b"))
    parser.add_argument("--reference-dir", type=Path, default=Path("scratch/uhe_daily"))
    parser.add_argument("--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_route_b_pilot"))
    parser.add_argument("--upstream", type=Path, default=Path("scratch/carbonplan_reproduction/upstream"))
    parser.add_argument("--acquisition-log", type=Path, default=Path("scratch/wbgt_route_b_reference/full_acquisition.log"))
    parser.add_argument("--wait-hours", type=float, default=36)
    parser.add_argument("--max-qdm-hours-per-model", type=float, default=12)
    args = parser.parse_args(argv)
    if args.wait_hours <= 0 or args.max_qdm_hours_per_model <= 0:
        parser.error("Wait and compute limits must be positive")
    if args.run:
        finish(args)
    else:
        print(json.dumps({"action": "plan_only", "models": MODEL_DEFAULTS,
                          "wait_hours": args.wait_hours, "max_qdm_hours_per_model": args.max_qdm_hours_per_model,
                          "network": False, "publication": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
