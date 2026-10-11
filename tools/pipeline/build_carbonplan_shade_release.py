"""Stage grid-first CarbonPlan shade annual values from signed Route B QDM caches.

The default command plans the run. ``--run`` writes only to ``--stage`` and is
resumable by model. It never calculates shade, refits QDM or touches production.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd
from scipy import sparse

from india_resilience_tool.config.paths import get_paths_config
from tools.diagnostics.wbgt_route_b import annual_metrics, fixed_aggregate
from tools.diagnostics.wbgt_route_b_prepare import ENSEMBLE_MODELS, PERIODS, verify_support

REPO = Path(__file__).resolve().parents[2]
SPEC = REPO / "docs/diagnostics/wbgt_shade_carbonplan_release/SPEC.md"
METHOD = "carbonplan-shade-gridfirst-qdm-v1"
METRICS = ("mean_c", "days_ge_28", "days_ge_30", "days_ge_32")
SEAMS = ((2039, 2040), (2059, 2060))


def digest(path: Path) -> str:
    """Hash an input or completed output without loading it into memory."""
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def driver_dates(root: Path, model: str, scenario: str, support_signature: str,
                 expected_source_hash: str) -> tuple[pd.DatetimeIndex, str]:
    """Rebuild the signed source calendar and verify Route B's driver digest."""
    dates: list[str] = []
    calendars: set[str] = set()
    hashes: list[str] = []
    first, last = PERIODS[scenario]
    for year in range(first, last + 1):
        path = root / "drivers" / model / scenario / f"{year}.npz"
        with np.load(path, allow_pickle=False) as shard:
            if str(shard["signature"]) != support_signature:
                raise ValueError(f"Driver/support signature mismatch: {path}")
            calendars.add(str(shard["calendar"]))
            year_dates = pd.DatetimeIndex(shard["dates"].tolist()).normalize()
            expected = pd.date_range(f"{year}-01-01", f"{year}-12-31")
            if str(shard["calendar"]) in {"noleap", "365_day"}:
                expected = expected[~((expected.month == 2) & (expected.day == 29))]
            if not year_dates.equals(expected):
                raise ValueError(f"Incomplete or duplicate driver dates: {path}")
            dates.extend(shard["dates"].tolist())
        hashes.append(digest(path))
    if len(calendars) != 1:
        raise ValueError(f"{model}/{scenario}: mixed driver calendars {calendars}")
    source_hash = hashlib.sha256("".join(hashes).encode()).hexdigest()
    if source_hash != expected_source_hash:
        raise ValueError(f"{model}/{scenario}: signed driver digest differs")
    return pd.DatetimeIndex(dates).normalize(), calendars.pop()


def checked_support(root: Path, model: str, units: pd.DataFrame,
                    areas: sparse.spmatrix, manifest: dict) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Use Route B's frozen eligible cells and normalize each eligible unit once."""
    source = root / "assembly" / model
    expected = manifest["artifact_sha256"]
    for name in ("cell_support.csv", "unit_support.csv"):
        if digest(source / name) != expected[name]:
            raise ValueError(f"{model}: signed {name} changed")
    cells = pd.read_csv(source / "cell_support.csv")
    unit_support = pd.read_csv(source / "unit_support.csv")
    if len(cells) != areas.shape[1] or len(unit_support) != areas.shape[0]:
        raise ValueError(f"{model}: support dimensions differ from admin areas")
    if not np.array_equal(cells["cell"].to_numpy(), np.arange(len(cells))):
        raise ValueError(f"{model}: cell order changed")
    if not np.array_equal(unit_support["unit_index"].to_numpy(), units["unit_index"].to_numpy()):
        raise ValueError(f"{model}: unit order changed")
    cells_ok = cells["eligible"].astype(bool).to_numpy()
    eligible_area = np.asarray(areas[:, cells_ok].sum(axis=1)).ravel()
    coverage = eligible_area / units["whole_area_m2"].to_numpy(dtype=float)
    if not np.allclose(coverage, unit_support["retained_fraction"].to_numpy(dtype=float),
                       rtol=0, atol=1e-8):
        raise ValueError(f"{model}: retained-area coverage differs from Route B")
    units_ok = unit_support["eligible"].astype(bool).to_numpy()
    if not np.array_equal(units_ok, coverage >= .95):
        raise ValueError(f"{model}: 0.95 coverage gate differs from Route B")
    weights = areas[units_ok][:, cells_ok].toarray().astype(float)
    if not len(weights) or np.any(weights.sum(axis=1) <= 0):
        raise ValueError(f"{model}: empty eligible area")
    weights /= weights.sum(axis=1, keepdims=True)
    return cells_ok, units_ok, weights


def reduce_cache(root: Path, model: str, cache_key: str,
                 dates: pd.DatetimeIndex, signature: str, n_cells: int,
                 weights: np.ndarray, *, chunk_size: int = 48) -> dict[str, np.ndarray]:
    """Reduce cache shade per cell-year, then apply fixed admin area weights."""
    years = np.unique(dates.year)
    cell_metrics = {name: np.full((len(years), n_cells), np.nan) for name in METRICS}
    for start in range(0, n_cells, chunk_size):
        stop = min(start + chunk_size, n_cells)
        series = []
        for index in range(start, stop):
            path = root / "qdm" / model / "cell" / f"{index}.npz"
            with np.load(path, allow_pickle=False) as cache:
                if str(cache["signature"]) != signature:
                    raise ValueError(f"QDM cache signature mismatch: {path}")
                if cache_key not in cache.files:
                    raise ValueError(f"QDM cache/date length mismatch: {path}/{cache_key}")
                values = cache[cache_key]
                if len(values) != len(dates):
                    raise ValueError(f"QDM cache/date length mismatch: {path}/{cache_key}")
                series.append(values)
        annual = annual_metrics(dates, np.column_stack(series))
        if not np.array_equal(annual["years"], years):
            raise ValueError(f"{model}/{cache_key}: annual years changed")
        for name in METRICS:
            cell_metrics[name][:, start:stop] = annual[name]
    return {"years": years, **{name: fixed_aggregate(values, weights)
                               for name, values in cell_metrics.items()}}


def reduce_all_caches(root: Path, model: str, dates: dict[str, pd.DatetimeIndex],
                      signature: str, n_cells: int, weights: np.ndarray,
                      *, chunk_size: int = 48, read_workers: int = 8
                      ) -> dict[str, dict[str, np.ndarray]]:
    """Read each signed cell cache once for all release and holdout windows."""
    all_dates = {**dates, "holdout": dates["historical"][dates["historical"].year >= 2000]}
    cells = {
        key: {"years": np.unique(axis.year), **{
            metric: np.full((len(np.unique(axis.year)), n_cells), np.nan)
            for metric in METRICS}}
        for key, axis in all_dates.items()
    }
    def load_cell(index: int) -> dict[str, np.ndarray]:
        """Read one independent NPZ with bounded concurrent storage requests."""
        path = root / "qdm" / model / "cell" / f"{index}.npz"
        result = {}
        with np.load(path, allow_pickle=False) as cache:
            if str(cache["signature"]) != signature:
                raise ValueError(f"QDM cache signature mismatch: {path}")
            for key, axis in all_dates.items():
                if key not in cache.files:
                    raise ValueError(f"QDM cache/date length mismatch: {path}/{key}")
                values = cache[key]
                if len(values) != len(axis):
                    raise ValueError(f"QDM cache/date length mismatch: {path}/{key}")
                result[key] = values
        return result

    with ThreadPoolExecutor(max_workers=read_workers) as pool:
        for start in range(0, n_cells, chunk_size):
            started = time.monotonic()
            stop = min(start + chunk_size, n_cells)
            series: dict[str, list[np.ndarray]] = {key: [] for key in all_dates}
            for loaded in pool.map(load_cell, range(start, stop)):
                for key in all_dates:
                    series[key].append(loaded[key])
            for key, axis in all_dates.items():
                annual = annual_metrics(axis, np.column_stack(series[key]))
                if not np.array_equal(annual["years"], cells[key]["years"]):
                    raise ValueError(f"{model}/{key}: annual years changed")
                for metric in METRICS:
                    cells[key][metric][:, start:stop] = annual[metric]
            print(f"{model}: verified {stop}/{n_cells} signed QDM cells "
                  f"in {time.monotonic() - started:.1f}s", flush=True)
    return {
        key: {"years": values["years"], **{
            metric: fixed_aggregate(values[metric], weights) for metric in METRICS}}
        for key, values in cells.items()
    }


def yearly_frame(annual: dict[str, np.ndarray], units: pd.DataFrame,
                 units_ok: np.ndarray, scenario: str) -> pd.DataFrame:
    """Keep every unit/year, with ineligible and missing unit-years as NaN."""
    frames = []
    for row, year in enumerate(annual["years"]):
        frame = pd.DataFrame({"unit_index": units["unit_index"].to_numpy(),
                              "scenario": scenario, "year": int(year)})
        for name in METRICS:
            values = np.full(len(units), np.nan)
            values[units_ok] = annual[name][row]
            frame[name] = values
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def seam_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Report each unit's change across the two future QDM application seams."""
    rows = []
    for scenario in ("ssp245", "ssp585"):
        future = frame.loc[frame.scenario == scenario].set_index(["unit_index", "year"])
        for before, after in SEAMS:
            left = future.xs(before, level="year")
            right = future.xs(after, level="year")
            for name in METRICS:
                change = right[name] - left[name]
                rows.append(pd.DataFrame({"unit_index": change.index,
                                          "scenario": scenario, "year_before": before,
                                          "year_after": after, "metric": name,
                                          "step": change.to_numpy()}))
    return pd.concat(rows, ignore_index=True)


def build_model(root: Path, stage: Path, model: str, units: pd.DataFrame,
                areas: sparse.spmatrix, support_signature: str,
                spec_sha256: str) -> dict:
    """Stage and fingerprint one model; resume only an identical complete result."""
    out = stage / model
    run_manifest = json.loads((root / "assembly" / model / "run_manifest.json").read_text())
    validation = json.loads((root / "assembly" / model / "validation_report.json").read_text())
    if validation.get("verdict") != "PASS" or run_manifest.get("model") != model:
        raise ValueError(f"{model}: Route B model is not validated")
    signature = run_manifest["signature_sha256"]
    identity = {"method": METHOD, "model": model, "qdm_signature": signature,
                "support_signature": support_signature, "spec_sha256": spec_sha256,
                "runner_sha256": digest(Path(__file__)),
                "route_b_manifest_sha256": digest(root / "assembly" / model / "run_manifest.json")}
    completed = out / "status.json"
    if completed.is_file():
        prior = json.loads(completed.read_text())
        if prior.get("identity") != identity:
            raise ValueError(f"{model}: stage identity changed; use a new stage")
        if prior.get("status") == "PASS" and all(
            (out / name).is_file() and digest(out / name) == sha
            for name, sha in prior["outputs"].items()
        ):
            return prior
    cells_ok, units_ok, weights = checked_support(root, model, units, areas, run_manifest)
    calendars: set[str] = set()
    dates = {}
    for scenario in PERIODS:
        dates[scenario], calendar = driver_dates(
            root, model, scenario, support_signature, run_manifest["drivers"][scenario])
        calendars.add(calendar)
    if len(calendars) != 1:
        raise ValueError(f"{model}: scenario calendars disagree")
    annual_by_key = reduce_all_caches(root, model, dates, signature,
                                      int(cells_ok.sum()), weights)
    frames = []
    for scenario in PERIODS:
        frames.append(yearly_frame(annual_by_key[scenario], units, units_ok, scenario))
    released = pd.concat(frames, ignore_index=True)
    held = yearly_frame(annual_by_key["holdout"], units, units_ok, "historical")
    out.mkdir(parents=True, exist_ok=True)
    outputs = {"yearly_metrics.csv.gz": released,
               "holdout_yearly_metrics.csv.gz": held,
               "seam_steps.csv.gz": seam_frame(released)}
    hashes = {}
    for name, frame in outputs.items():
        target = out / name
        temporary = out / f"{name}.part"
        frame.to_csv(temporary, index=False, float_format="%.17g", compression="gzip")
        temporary.replace(target)
        hashes[name] = digest(target)
    result = {"status": "PASS", "identity": identity, "outputs": hashes,
              "calendar": calendars.pop(), "eligible_cells": int(cells_ok.sum()),
              "eligible_units": int(units_ok.sum()), "units_total": len(units),
              "release_years": {s: [int(frame.year.min()), int(frame.year.max())]
                                for s, frame in zip(PERIODS, frames)},
              "holdout_training_years": [1985, 1999],
              "holdout_application_years": [2000, 2014]}
    temporary = out / "status.json.part"
    temporary.write_text(json.dumps(result, indent=2), encoding="utf-8")
    temporary.replace(completed)
    return result


def main(argv: list[str] | None = None) -> int:
    """Plan or stage selected models without writing to production."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route-b-root", type=Path,
                        default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--model", action="append", choices=ENSEMBLE_MODELS)
    parser.add_argument("--run", action="store_true", help="Write the isolated stage")
    args = parser.parse_args(argv)
    root, stage = args.route_b_root.resolve(), args.stage.resolve()
    if stage == root or stage.is_relative_to(root) or root.is_relative_to(stage):
        parser.error("Stage and Route B cache must be separate directories")
    production = get_paths_config().data_dir.resolve()
    if stage == production or stage.is_relative_to(production):
        parser.error("Stage must be outside the production data tree")
    models = tuple(dict.fromkeys(args.model or ENSEMBLE_MODELS))
    plan = {"method": METHOD, "models": models, "route_b_root": str(root),
            "stage": str(stage), "writes": bool(args.run)}
    print(json.dumps(plan, indent=2), flush=True)
    if not args.run:
        return 0
    support = verify_support(root)
    spec_sha256 = digest(SPEC)
    units = pd.read_csv(root / "units.csv").fillna({"block_name": ""})
    areas = sparse.load_npz(root / "admin_areas.npz").tocsr()
    if areas.shape[0] != len(units):
        raise ValueError("Admin areas and unit roster differ")
    for model in models:
        result = build_model(root, stage, model, units, areas,
                             support["signature_sha256"], spec_sha256)
        print(f"{model}: {result['status']} ({result['eligible_units']} eligible units)",
              flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
