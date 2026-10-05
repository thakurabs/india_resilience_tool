"""CHG-0651: audit and migrate QDM caches after the solar-only precision fix.

Run from the repository root with the pinned Python; default is a read-only plan.
This deliberately accepts only the solar-function change from commit 9979326.
It never changes numerical QDM arrays and refuses any other source/input drift.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from tools.diagnostics import wbgt_route_b_run as runner
from tools.diagnostics.wbgt_route_b_prepare import atomic_npz, verify_support


def without_solar(source: bytes) -> str:
    """Compare every executable statement except the downstream solar function."""
    tree = ast.parse(source)
    tree.body = [node for node in tree.body
                 if not (isinstance(node, ast.FunctionDef) and node.name == "sun_adjustments")]
    return ast.dump(tree, include_attributes=False)


def main() -> None:
    """Verify old/new provenance, optionally replace signatures and save an audit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    root = REPO / "scratch/wbgt_route_b"
    out = REPO / "docs/diagnostics/wbgt_outdoor_route_b_pilot"
    path = Path(runner.__file__)
    relative = path.relative_to(REPO).as_posix()
    old = subprocess.check_output(["git", "show", "9979326:" + relative], cwd=REPO)
    new = path.read_bytes()
    if without_solar(old) != without_solar(new):
        raise ValueError("Changes outside solar function: cache migration forbidden")
    kernel = path.with_name("wbgt_route_b.py")
    original_kernel = subprocess.check_output(
        ["git", "show", "9979326:" + kernel.relative_to(REPO).as_posix()], cwd=REPO)
    if original_kernel != kernel.read_bytes():
        raise ValueError("Changed numerical kernels: cache migration forbidden")
    support = verify_support(root)
    cells = pd.read_csv(root / "cells.csv")
    _, _, lock = runner.upstream_functions(REPO / "scratch/carbonplan_reproduction/upstream")
    _, _, _, reference_hash = runner.load_reference(root)
    model = "ACCESS-CM2"
    climate = {s: runner.load_climate(root, model, s, cells) for s in runner.PERIODS}
    base = {"method": runner.METHOD, "source_lock": lock, "model": model,
            "code_sha256": {path.name: hashlib.sha256(old).hexdigest(),
                            kernel.name: hashlib.sha256(original_kernel).hexdigest()},
            "support": support, "reference": reference_hash,
            "drivers": {s: c["source_hash"] for s, c in climate.items()}}
    old_signature = hashlib.sha256(json.dumps(base, sort_keys=True).encode()).hexdigest()
    base["code_sha256"][path.name] = hashlib.sha256(new).hexdigest()
    new_signature = hashlib.sha256(json.dumps(base, sort_keys=True).encode()).hexdigest()
    targets = sorted((root / "qdm" / model).glob("*/*.npz"))
    # Check the entire cache set before any mutation.
    for target in targets:
        with np.load(target, allow_pickle=False) as z:
            if str(z["signature"]) not in (old_signature, new_signature):
                raise ValueError(f"Unrelated/stale cache: {target}")
    audit = {"change": "CHG-0651", "baseline_commit": "9979326",
             "only_solar_function_changed": True, "old_signature": old_signature,
             "new_signature": new_signature, "cache_count": len(targets),
             "run": args.run, "arrays_unchanged": [], "provenance": base}
    if args.run:
        timings = out / model / "timings.csv"
        retained = out / model / "initial_qdm_timings.csv"
        if not retained.exists():
            shutil.copyfile(timings, retained)
        audit["initial_timings_sha256"] = hashlib.sha256(retained.read_bytes()).hexdigest()
        for target in targets:
            with np.load(target, allow_pickle=False) as z:
                arrays = {k: z[k] for k in z.files if k != "signature"}
            digest = hashlib.sha256(b"".join(arrays[k].tobytes() for k in sorted(arrays))).hexdigest()
            atomic_npz(target, **arrays, signature=np.array(new_signature))
            with np.load(target, allow_pickle=False) as z:
                if any(not np.array_equal(value, z[k], equal_nan=True) for k, value in arrays.items()):
                    raise ValueError(f"Numerical cache mutation: {target}")
            audit["arrays_unchanged"].append({"path": str(target.relative_to(REPO)),
                                               "numerical_sha256": digest})
        (out / "solar_precision_cache_audit.json").write_text(json.dumps(audit, indent=2))
    print(json.dumps({k: v for k, v in audit.items() if k not in ("arrays_unchanged", "provenance")}, indent=2))


if __name__ == "__main__":
    main()
