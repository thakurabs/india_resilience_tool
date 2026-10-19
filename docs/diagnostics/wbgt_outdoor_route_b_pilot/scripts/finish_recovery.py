"""CHG-0651: finish the specifically audited solar-precision recovery, offline.

Default plans only. --run waits at most two hours for the already-running
ACCESS-CM2 solar/output stage, validates its artifacts, cost-gates and runs CMCC,
then reports and reviews both models. Shares the normal coordinator lock.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from tools.diagnostics.wbgt_route_b_finish import probe_hours


def finish() -> None:
    """Continue the bounded recovery with atomic status and failure propagation."""
    work = REPO / "scratch/wbgt_route_b"
    out = REPO / "docs/diagnostics/wbgt_outdoor_route_b_pilot"
    status_path = out / "execution_status.json"
    state = {"change": "CHG-0651", "started_utc": datetime.now(timezone.utc).isoformat(),
             "models": ["ACCESS-CM2", "CMCC-CM2-SR5"], "publication": False,
             "completed_stages": [], "reference_complete": 10957, "driver_shards_complete": 304}

    def status(stage: str, **details) -> None:
        state.update(status=stage, updated_utc=datetime.now(timezone.utc).isoformat(), **details)
        temp = status_path.with_suffix(".part")
        temp.write_text(json.dumps(state, indent=2))
        temp.replace(status_path)
        print(json.dumps({"status": stage, **details}), flush=True)

    def execute(stage: str, arguments: list[str], hours: float = 24) -> None:
        status(stage)
        with (work / f"recovery_{stage.lower()}.log").open("a") as stream:
            subprocess.run([sys.executable, "-u", *arguments], cwd=REPO, stdout=stream,
                           stderr=subprocess.STDOUT, check=True, timeout=hours * 3600)
        state["completed_stages"].append(stage)

    with (work / "finish.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            audit_path = out / "solar_precision_cache_audit.json"
            audit = json.loads(audit_path.read_text())
            if not audit["only_solar_function_changed"] or audit["cache_count"] != len(audit["arrays_unchanged"]):
                raise ValueError("Incomplete recovery audit")
            state["recovery_audit_sha256"] = hashlib.sha256(audit_path.read_bytes()).hexdigest()
            verification = work / "finish_verify_reference.log"
            if "Verified 10957 dates" not in verification.read_text():
                raise ValueError("Missing full reference verification evidence")
            state["prior_reference_verification_log_sha256"] = hashlib.sha256(verification.read_bytes()).hexdigest()
            status("WAITING_FOR_ACCESS_SOLAR_OUTPUTS")
            manifest_path = out / "ACCESS-CM2/run_manifest.json"
            deadline = time.monotonic() + 2 * 3600
            while not manifest_path.exists():
                log = (work / "solar_fix_access.log").read_text()
                if "Traceback (most recent call last)" in log:
                    raise RuntimeError("ACCESS recovery failed; see solar_fix_access.log")
                if time.monotonic() > deadline:
                    raise TimeoutError("ACCESS solar/output recovery exceeded two-hour wait")
                time.sleep(30)
            manifest = json.loads(manifest_path.read_text())
            if manifest["status"] != "PILOT_COMPLETE_DIAGNOSTIC_ONLY" or manifest["signature_sha256"] != audit["new_signature"]:
                raise ValueError("ACCESS manifest does not match audited recovery")
            for name, digest in manifest["artifact_sha256"].items():
                if hashlib.sha256((manifest_path.parent / name).read_bytes()).hexdigest() != digest:
                    raise ValueError(f"Changed ACCESS output: {name}")
            state["completed_stages"].append("PILOT_ACCESS-CM2")
            probe = json.loads((out / "CMCC-CM2-SR5/probe.json").read_text())
            hours = probe_hours(probe)
            status("COST_CHECK_CMCC-CM2-SR5", estimated_qdm_hours=hours, maximum_qdm_hours=12)
            if hours > 12:
                raise RuntimeError("CMCC measured QDM estimate exceeds the existing 12-hour budget")
            execute("PILOT_CMCC-CM2-SR5", ["-m", "tools.diagnostics.wbgt_route_b_run", "--run", "--model", "CMCC-CM2-SR5"])
            execute("REPORT", ["-m", "tools.diagnostics.wbgt_route_b_report", "--run"], hours=1)
            execute("REVIEW", [str(Path(__file__).with_name("review_results.py")), "--run"], hours=1)
            status("PILOT_COMPLETE_REVIEW_REQUIRED", report=str((out / "REVIEW.md").relative_to(REPO)))
        except Exception as exc:
            status("FAILED", error_type=type(exc).__name__, error=str(exc))
            raise


def main() -> None:
    """Plan by default; explicitly run the bounded recovery chain."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.run:
        finish()
    else:
        print("Plan: wait for audited ACCESS results; validate; cost-gate/run CMCC; report/review. No network/publication.")


if __name__ == "__main__":
    main()
