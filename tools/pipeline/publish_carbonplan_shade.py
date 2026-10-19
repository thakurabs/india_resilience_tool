"""Write the CarbonPlan shade-WBGT processed tree from the fingerprinted release stage.

CHG-0702. Offline: reads each model's ``yearly_metrics.csv.gz`` written by
``tools.pipeline.build_carbonplan_shade_release`` and writes the four
``processed/wbgt_shade_stull_*`` trees that ``tools.optimized.build_processed_optimised
--shade-release`` consumes. No value is recomputed: every number is one staged
unit-year, which is itself an area-weighted mean of per-cell annual metrics read
from the signed Route B QDM cache.

Every yearly, period, ensemble and master file carries
``shade_method_signature = carbonplan-shade-gridfirst-qdm-v1`` so the builder,
the state-value step and the dashboard can tell it from the unsigned legacy
files it replaces.

The writers are Route B's (``tools.pipeline.publish_route_b_outdoor``), so path
sanitising, CSV rendering, period completeness, ensemble statistics, coverage
provenance and the Windows path-length preflight are the same code that
published the outdoor metrics. ``--processed-root`` must be a staging root: the
same measurement that applied to Route B (36 small files/s to D: against 636 to
C: from the Windows interpreter) applies here, and the production tree is
replaced only by the cutover tool.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import time

import pandas as pd

from india_resilience_tool.data.wbgt_contract import CARBONPLAN_SHADE_SIGNATURE, SHADE_SLUGS
from tools.pipeline.publish_route_b_outdoor import (
    PERIODS,
    UNIT_COVERAGE_GATE,
    WINDOWS_MAX_PATH,
    _master_frame,
    _write_unit_ensemble_files,
    _write_unit_files,
    coverage_table,
    longest_path_preflight,
)

METHOD_VERSION = CARBONPLAN_SHADE_SIGNATURE
SOURCE_LABEL = "carbonplan_shade_gridfirst_stage"
STAGE_METHOD = "carbonplan-shade-gridfirst-qdm-v1"

#: Slug -> (staged column, production value column). Value columns are the
#: incumbent's, so masters keep their published names.
SLUGS = {
    "wbgt_shade_stull_annual_mean": ("mean_c", "wbgt_shade_stull_annual_mean_C"),
    "wbgt_shade_stull_days_ge_28": ("days_ge_28", "wbgt_shade_stull_days_ge_28_days"),
    "wbgt_shade_stull_days_ge_30": ("days_ge_30", "wbgt_shade_stull_days_ge_30_days"),
    "wbgt_shade_stull_days_ge_32": ("days_ge_32", "wbgt_shade_stull_days_ge_32_days"),
}

#: Released yearly span: the QDM training years for history, both SSPs to 2080.
ENVELOPE = {"historical": (1985, 2014), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}

SIGNATURE_COLUMNS = {"shade_method_signature": CARBONPLAN_SHADE_SIGNATURE}

assert set(SLUGS) == SHADE_SLUGS


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def verify_stage(stage: Path, models: list[str]) -> dict[str, dict]:
    """Require a PASS status and unchanged output hashes for every model."""
    statuses = {}
    for model in models:
        status_path = stage / model / "status.json"
        if not status_path.is_file():
            raise FileNotFoundError(f"Model not staged: {status_path}")
        status = json.loads(status_path.read_text(encoding="utf-8"))
        if status.get("status") != "PASS" or status.get("identity", {}).get("method") != STAGE_METHOD:
            raise ValueError(f"{model}: staged result is not a PASS {STAGE_METHOD} release")
        name = "yearly_metrics.csv.gz"
        if _digest(stage / model / name) != status["outputs"][name]:
            raise ValueError(f"{model}: staged {name} hash changed since staging")
        statuses[model] = {"status_sha256": _digest(status_path),
                           "yearly_sha256": status["outputs"][name],
                           "qdm_signature": status["identity"]["qdm_signature"]}
    return statuses


def load_stage(stage: Path, work_dir: Path, models: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load every model's staged yearly metrics with the admin roster attached."""
    units = pd.read_csv(work_dir / "units.csv").set_index("unit_index")
    frames, eligibility = [], {}
    support = None
    for model in models:
        frame = pd.read_csv(stage / model / "yearly_metrics.csv.gz")
        for scenario, (first, last) in ENVELOPE.items():
            years = sorted(frame.loc[frame.scenario == scenario, "year"].unique())
            if years != list(range(first, last + 1)):
                raise ValueError(f"{model} {scenario} years are {years[:3]}..{years[-3:]}, "
                                 f"not the declared {first}-{last}")
        if frame.duplicated(["unit_index", "scenario", "year"]).any():
            raise ValueError(f"{model}: duplicate staged unit-years")
        if set(frame.unit_index) != set(units.index):
            raise ValueError(f"{model}: staged unit roster differs from units.csv")
        frame["model"] = model
        frames.append(frame)
        support = pd.read_csv(work_dir / "assembly" / model / "unit_support.csv")
        eligibility[model] = set(support.loc[support.eligible, "unit_index"].astype(int))
    shared = set.intersection(*eligibility.values())
    if any(value != shared for value in eligibility.values()):
        raise ValueError("Unit eligibility differs between models; one ensemble would mix supports")
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.join(units[["state_name", "level", "district_name", "block_name"]],
                             on="unit_index")
    return combined, units, coverage_table(support)


_STAGE_CACHE: dict[tuple, tuple] = {}


def _cached_stage(stage: Path, work_dir: Path, models: tuple[str, ...]) -> tuple:
    """Load the stage once per worker process."""
    key = (str(stage), str(work_dir), models)
    if key not in _STAGE_CACHE:
        _STAGE_CACHE.clear()
        _STAGE_CACHE[key] = load_stage(stage, work_dir, list(models))
    return _STAGE_CACHE[key]


def _state_task(job: tuple) -> dict:
    """Write all four shade slugs for one state."""
    state, stage_text, work_text, root_text, models = job
    frame, units, coverage = _cached_stage(Path(stage_text), Path(work_text), tuple(models))
    frame = frame[frame.state_name == state]
    coverage = coverage[coverage.state_name == state]
    counts: dict[str, dict] = {}
    files = 0
    for slug, (column, value_col) in SLUGS.items():
        state_root = Path(root_text) / slug / state
        state_root.mkdir(parents=True, exist_ok=True)
        for unit_index, group in frame.groupby("unit_index", sort=False):
            unit = units.loc[unit_index]
            files += _write_unit_files(state_root, unit, group, column, value_col,
                                       method_version=METHOD_VERSION, source_label=SOURCE_LABEL,
                                       extra_columns=SIGNATURE_COLUMNS)
            files += _write_unit_ensemble_files(state_root, unit, group, column,
                                                extra_columns=SIGNATURE_COLUMNS)
        coverage.to_csv(state_root / "support_coverage.csv", index=False)
        coverage.to_parquet(state_root / "support_coverage.parquet", index=False)
        files += 2
        counts[slug] = {}
        for level, filename in (("district", "master_metrics_by_district"),
                                ("block", "master_metrics_by_block")):
            master = _master_frame(frame, units, level, column, value_col, models,
                                   coverage=coverage)
            counts[slug][level] = int(len(master))
            if master.empty:
                continue
            master["shade_method_signature"] = CARBONPLAN_SHADE_SIGNATURE
            master.to_csv(state_root / f"{filename}.csv", index=False)
            master.to_parquet(state_root / f"{filename}.parquet", index=False)
            files += 2
    return {"state": state, "files": files, "units": counts}


def publish(args: argparse.Namespace) -> dict:
    """Verify the stage, write every state, then record a manifest."""
    statuses = verify_stage(args.stage, args.models)
    frame, units, coverage = load_stage(args.stage, args.work_dir, args.models)
    preflight = longest_path_preflight(args.processed_root, units.reset_index(),
                                       list(SLUGS), args.models)
    if preflight["headroom_chars"] < 0:
        raise SystemExit(f"--processed-root is too deep: {preflight['longest_path_chars']} > "
                         f"{WINDOWS_MAX_PATH} characters\n  {preflight['longest_path_example']}")
    print(json.dumps({"path_preflight": preflight}, indent=2), flush=True)
    states = sorted(frame.state_name.dropna().unique())
    if args.states:
        unknown = sorted(set(args.states) - set(states))
        if unknown:
            raise SystemExit(f"Unknown states: {unknown}")
        states = sorted(args.states)
    jobs = [(state, str(args.stage), str(args.work_dir), str(args.processed_root), args.models)
            for state in states]
    started = time.monotonic()
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_state_task, job) for job in jobs]
        for done in as_completed(futures):
            results.append(done.result())
            print(f"[{len(results)}/{len(jobs)}] {results[-1]['state']} "
                  f"{results[-1]['files']} files", flush=True)
    manifest = {"change": "CHG-0702", "method_version": METHOD_VERSION,
                "shade_method_signature": CARBONPLAN_SHADE_SIGNATURE,
                "inputs": ["tas", "tasmax", "huss", "elevation"],
                "stage": str(args.stage.resolve()), "stage_models": statuses,
                "path_preflight": preflight, "models": args.models,
                "model_count": len(args.models),
                "slugs": {slug: {"value_col": value_col, "staged_column": column}
                          for slug, (column, value_col) in SLUGS.items()},
                "envelope": ENVELOPE,
                "periods": {key: [p[0] for p in value] for key, value in PERIODS.items()},
                "states": states, "units_total": int(len(units)),
                "unit_coverage_gate": UNIT_COVERAGE_GATE,
                "units_eligible": int(coverage.eligible.sum()),
                "units_excluded": int((~coverage.eligible).sum()),
                "exclusion_reasons": {key: int(value) for key, value
                                      in coverage.exclusion_reason.value_counts().items()},
                "files_written": sum(result["files"] for result in results),
                "elapsed_seconds": round(time.monotonic() - started, 1)}
    (args.processed_root / "carbonplan_shade_release_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    """Plan or write the staged processed tree for the four shade slugs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Execute; default is a read-only plan")
    parser.add_argument("--stage", type=Path, default=Path("scratch/wbgt_shade_carbonplan_release"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--processed-root", type=Path, default=Path("C:/irt_shade_s1"),
                        help="Staging root for the processed tree; keep it on C: and outside production")
    parser.add_argument("--models", nargs="+", default=None)
    parser.add_argument("--states", nargs="+", default=None,
                        help="Write only these states (smoke tests; a release writes all)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Each worker loads the whole stage; 8 exhausted Windows memory "
                             "on the 18-model release, 4 did not")
    args = parser.parse_args(argv)
    if not args.models:
        from tools.diagnostics.wbgt_route_b_prepare import ENSEMBLE_MODELS
        args.models = list(ENSEMBLE_MODELS)
    from india_resilience_tool.config.paths import get_paths_config
    production = get_paths_config().data_dir.resolve()
    root = args.processed_root.resolve()
    if root == production or root.is_relative_to(production):
        parser.error("--processed-root must be a staging root outside the production data tree")
    staged = [m for m in args.models if (args.stage / m / "status.json").is_file()]
    if not args.run:
        print(json.dumps({"action": "plan_only", "models_requested": args.models,
                          "models_staged": staged,
                          "models_missing": sorted(set(args.models) - set(staged)),
                          "processed_root": str(args.processed_root),
                          "workers": args.workers}, indent=2))
        return 0
    if sorted(staged) != sorted(args.models):
        raise SystemExit(f"Refusing a partial roster: {sorted(set(args.models) - set(staged))} not staged")
    args.processed_root.mkdir(parents=True, exist_ok=True)
    print(json.dumps(publish(args), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
