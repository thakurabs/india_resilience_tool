"""Fit Route B's grid-first QDM corrections in parallel shards.

CHG-0659. The QDM fit is the whole cost of a national run: about 4,900 eligible cells at
roughly 15 s each is 20 hours per model, and 19 models is over two weeks in series. The fit
is embarrassingly parallel -- one independent, signature-checked, atomically written cache
per cell -- but a single assembling process already peaks near 12 GB, so workers cannot each
hold the climate set in memory.

Two stages solve that:

``--prepare``  one process loads the drivers once, computes the pinned shade formula, applies
              the eligibility mask and writes the per-cell series as plain ``.npy`` (about
              1.4 GB for a national model). Nothing is recomputed per worker.
``--fit``      each shard memory-maps those arrays, slices only its own columns and writes
              ``qdm/<model>/cell/<i>.npz`` exactly as the serial runner would.

The caches produced are the ones ``wbgt_route_b_run --run`` consumes, so the assembling run
afterwards reads them at about zero seconds per series and only computes the metrics. The
cache identity comes from ``wbgt_route_b_run.cache_signature``, so a shard cannot write a
cache the assembling run would silently accept under different inputs.

Default is a read-only plan. Run the fitting stages in the pinned xclim 0.44 environment.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
import warnings

import numpy as np
import pandas as pd
from scipy import sparse

from tools.diagnostics.wbgt_route_b_prepare import PERIODS, verify_support
from tools.diagnostics.wbgt_route_b_run import (
    cache_signature,
    fit_series,
    load_climate,
    load_reference,
    peak_rss_mb,
    upstream_functions,
)

#: Scenario series written by --prepare, in the order fit_series expects to receive them.
SCENARIOS = ("historical", "ssp245", "ssp585")


def inputs_dir(work_dir: Path, model: str) -> Path:
    """Where one model's prepared fit inputs live."""
    return work_dir / "fit_inputs" / model


def prepare(args: argparse.Namespace) -> dict:
    """Write the eligible per-cell reference and shade series once, for workers to memory-map."""
    started = time.monotonic()
    root = args.work_dir
    verify_support(root)
    mod, _, lock = upstream_functions(args.upstream)
    cells = pd.read_csv(root / "cells.csv")
    units = pd.read_csv(root / "units.csv").fillna({"block_name": ""})
    areas = sparse.load_npz(root / "admin_areas.npz").toarray()
    reference, support, ref_dates, reference_hash = load_reference(root)
    climate = {s: load_climate(root, args.model, s, cells) for s in PERIODS}
    calendar = climate["historical"]["calendar"]
    if any(value["calendar"] != calendar for value in climate.values()):
        raise ValueError("Scenario calendars disagree")
    hist = climate["historical"]
    # The eligibility mask must be byte-identical to the assembling run's, or the shards would
    # fit a different set of cells than the run reads back.
    valid = (support >= .50) & np.isfinite(reference).all(axis=0) & np.isfinite(hist["shade"]).all(axis=0)
    if not valid.any():
        raise ValueError("No eligible common support")
    coverage = areas[:, valid].sum(axis=1) / units.whole_area_m2.to_numpy()
    base, signature = cache_signature(
        lock, args.model, json.loads((root / "support_manifest.json").read_text()),
        reference_hash, {s: c["source_hash"] for s, c in climate.items()})
    out = inputs_dir(root, args.model)
    out.mkdir(parents=True, exist_ok=True)

    def write(name: str, array: np.ndarray) -> dict:
        """Write one array atomically, recording the dtype and shape workers must see."""
        target = out / f"{name}.npy"
        temp = target.with_suffix(".npy.part")
        # np.save appends ".npy" to a path that lacks it, which would silently defeat the
        # atomic rename, so the array is written through an open handle instead.
        with open(temp, "wb") as handle:
            np.save(handle, array, allow_pickle=False)
        temp.replace(target)
        return {"shape": list(array.shape), "dtype": str(array.dtype),
                "bytes": target.stat().st_size}

    # Reference stays float64 and shade stays in its source float32: the serial runner fits
    # exactly these dtypes, so casting either one here would change the fitted result.
    written = {"reference": write("reference", reference[:, valid])}
    for scenario in SCENARIOS:
        written[scenario] = write(scenario, climate[scenario]["shade"][:, valid])
    manifest = {
        "change": "CHG-0659", "model": args.model, "signature_sha256": signature,
        "base_signature": base, "calendar": calendar,
        "series": int(valid.sum()), "cells_total": int(len(cells)),
        "eligible_units": int((coverage >= .95).sum()), "units_total": int(len(units)),
        "valid_cell_indices": [int(i) for i in np.flatnonzero(valid)],
        "reference_dates": [str(d.date()) for d in ref_dates],
        "scenario_dates": {s: [str(d.date()) for d in climate[s]["dates"]] for s in SCENARIOS},
        "arrays": written, "peak_rss_mb": peak_rss_mb(),
        "elapsed_seconds": time.monotonic() - started,
    }
    temp = out / "fit_manifest.json.part"
    temp.write_text(json.dumps(manifest, indent=2))
    temp.replace(out / "fit_manifest.json")
    summary = {k: v for k, v in manifest.items()
               if k not in ("base_signature", "valid_cell_indices", "reference_dates",
                            "scenario_dates")}
    print(json.dumps(summary, indent=2), flush=True)
    return manifest


def load_prepared(work_dir: Path, model: str) -> tuple[dict, np.ndarray, dict]:
    """Memory-map one model's prepared series, so a worker holds columns rather than arrays."""
    path = inputs_dir(work_dir, model) / "fit_manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"Run --prepare for {model} first: {path} missing")
    manifest = json.loads(path.read_text())
    arrays = {name: np.load(inputs_dir(work_dir, model) / f"{name}.npy", mmap_mode="r")
              for name in ("reference",) + SCENARIOS}
    for name, array in arrays.items():
        expected = manifest["arrays"][name]
        if list(array.shape) != expected["shape"] or str(array.dtype) != expected["dtype"]:
            raise ValueError(f"Prepared {name} does not match its manifest")
    return manifest, arrays.pop("reference"), arrays


def shard_indices(total: int, index: int, count: int) -> np.ndarray:
    """Deal series round-robin across shards, so each gets a comparable mix of cells.

    Contiguous blocks would hand one shard a whole climate zone; round-robin keeps per-shard
    wall clock close enough that the pool is not waiting on a single straggler.
    """
    if not 1 <= index <= count:
        raise ValueError(f"Shard {index} outside 1..{count}")
    return np.arange(index - 1, total, count)


def fit(args: argparse.Namespace) -> dict:
    """Fit this shard's cells, writing the same caches the serial runner writes."""
    started = time.monotonic()
    mod, train, lock = upstream_functions(args.upstream)
    manifest, reference, scenarios = load_prepared(args.work_dir, args.model)
    signature = manifest["signature_sha256"]
    calendar = manifest["calendar"]
    ref_dates = pd.DatetimeIndex(manifest["reference_dates"])
    dates = {s: pd.DatetimeIndex(manifest["scenario_dates"][s]) for s in SCENARIOS}
    target_dir = args.work_dir / "qdm" / args.model / "cell"
    target_dir.mkdir(parents=True, exist_ok=True)
    indices = shard_indices(manifest["series"], args.shard, args.shards)
    timings, fitted, cached, messages = [], 0, 0, set()
    for position, i in enumerate(indices, 1):
        raw = {s: {"values": np.asarray(scenarios[s][:, i]), "dates": dates[s]} for s in SCENARIOS}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _, seconds = fit_series(train, np.asarray(reference[:, i]), ref_dates, raw,
                                    calendar, signature, target_dir / f"{i}.npz")
        messages.update(str(w.message) for w in caught)
        timings.append({"series": int(i), "seconds": seconds})
        if seconds == 0:
            cached += 1
        else:
            fitted += 1
        print(f"{args.model} shard {args.shard}/{args.shards} cell {i} "
              f"({position}/{len(indices)}): {seconds:.2f}s", flush=True)
    report = {"change": "CHG-0659", "model": args.model, "shard": args.shard,
              "shards": args.shards, "series": len(indices), "fitted": fitted,
              "already_cached": cached, "warnings": sorted(messages),
              "elapsed_seconds": time.monotonic() - started, "peak_rss_mb": peak_rss_mb()}
    shard_dir = args.work_dir / "fit_shards" / args.model
    shard_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(timings).to_csv(shard_dir / f"timings_{args.shard:04d}.csv", index=False)
    temp = shard_dir / f"shard_{args.shard:04d}.json.part"
    temp.write_text(json.dumps(report, indent=2))
    temp.replace(shard_dir / f"shard_{args.shard:04d}.json")
    print(json.dumps(report, indent=2), flush=True)
    return report


def status(args: argparse.Namespace) -> dict:
    """Report how many of a model's cell caches exist, without reading or fitting any."""
    path = inputs_dir(args.work_dir, args.model) / "fit_manifest.json"
    series = json.loads(path.read_text())["series"] if path.is_file() else None
    target_dir = args.work_dir / "qdm" / args.model / "cell"
    present = len(list(target_dir.glob("*.npz"))) if target_dir.is_dir() else 0
    report = {"model": args.model, "prepared_series": series, "cached_series": present,
              "remaining": None if series is None else series - present}
    print(json.dumps(report, indent=2))
    return report


def prune_stale(args: argparse.Namespace) -> dict:
    """List, and with --apply delete, cell caches whose signature is no longer current.

    Editing either pinned-math module deliberately changes the cache identity, so the
    assembling run refuses a stale cache rather than mixing fits from different code. That
    refusal is correct but leaves the caches to clear; this does it deliberately rather than
    by hand. The default is a dry run.
    """
    manifest_path = inputs_dir(args.work_dir, args.model) / "fit_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Run --prepare for {args.model} first: {manifest_path} missing")
    signature = json.loads(manifest_path.read_text())["signature_sha256"]
    target_dir = args.work_dir / "qdm" / args.model / "cell"
    stale, current = [], 0
    for path in sorted(target_dir.glob("*.npz")) if target_dir.is_dir() else []:
        with np.load(path, allow_pickle=False) as cached:
            if str(cached["signature"]) == signature:
                current += 1
            else:
                stale.append(path)
    for path in stale if args.apply else []:
        path.unlink()
    report = {"model": args.model, "signature_sha256": signature, "current_caches": current,
              "stale_caches": len(stale), "deleted": len(stale) if args.apply else 0,
              "action": "deleted" if args.apply else "dry_run",
              "stale_names": [p.name for p in stale[:20]]}
    print(json.dumps(report, indent=2))
    if stale and not args.apply:
        print(f"Dry run: re-run with --apply to delete {len(stale)} stale caches", flush=True)
    return report


def main(argv: list[str] | None = None) -> int:
    """Default to a read-only plan; --prepare, --fit and --status are explicit."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--prepare", action="store_true")
    mode.add_argument("--fit", action="store_true")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--prune-stale", action="store_true",
                      help="Report cell caches whose signature is stale; a dry run unless "
                           "--apply is given")
    parser.add_argument("--apply", action="store_true",
                        help="With --prune-stale, actually delete the stale caches")
    parser.add_argument("--model", default="ACCESS-CM2")
    parser.add_argument("--shard", type=int, default=1)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_national"))
    parser.add_argument("--upstream", type=Path,
                        default=Path("scratch/carbonplan_reproduction/upstream"))
    args = parser.parse_args(argv)
    if args.shards < 1:
        parser.error("--shards must be at least 1")
    if not 1 <= args.shard <= args.shards:
        parser.error(f"--shard must be within 1..{args.shards}")
    if args.prepare:
        prepare(args)
    elif args.fit:
        fit(args)
    elif args.status:
        status(args)
    elif args.prune_stale:
        prune_stale(args)
    else:
        print(json.dumps({"action": "plan_only", "model": args.model,
                          "shard": f"{args.shard}/{args.shards}",
                          "work_dir": str(args.work_dir),
                          "inputs": str(inputs_dir(args.work_dir, args.model))}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
