"""Prepare isolated, resumable inputs for Route B's paired three-state pilot.

Run in IRT's existing Windows geo environment. Stages build spatial support,
reduce acquired native UHE days, and extract local NEX drivers. Network is never
used here. Default is a plan; --run enables writes under the supplied work root.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd
from scipy import sparse

from tools.data_acquisition.uhe_daily import BOUNDS, verify_day
from tools.diagnostics.wbgt_route_b import conservative_matrix, regrid_reference
from tools.diagnostics.wbgt_route_b_reference import STATES, polygon_weights, load_sample

VERSION = "route-b-pilot-inputs-v1"
PERIODS = {"historical": (1985, 2014), "ssp245": (2020, 2080), "ssp585": (2020, 2080)}
MODEL_DEFAULTS = ("ACCESS-CM2", "CMCC-CM2-SR5")

#: Sentinel accepted by ``--states`` and ``--models`` for "the whole roster".
ALL = "all"

#: Models carrying every Route B input over the declared envelope **and** a calendar
#: the chain can align: tas, tasmax and huss for the 1985-2014 calibration, plus rsds
#: and sfcWind over the 1990-2010 reporting baseline and both SSPs 2020-2080.
#: Re-verified against the local archive on 2026-10-06 by direct file existence over
#: every model/scenario/year/variable combination. Six of the 24 tas models are absent:
#: BCC-CSM2-MR lacks rsds and sfcWind; IITM-ESM lacks rsds, sfcWind and tasmax;
#: NESM3 lacks huss, rsds and sfcWind; IPSL-CM6A-LR and MIROC6 carry hurs but not the
#: huss that the pinned CarbonPlan shade formula consumes, and substituting hurs would
#: mix humidity chains inside one ensemble; and KACE-1-0-G has complete inputs but an
#: unalignable 360_day calendar, as recorded below.
ENSEMBLE_MODELS = (
    "ACCESS-CM2", "ACCESS-ESM1-5", "CMCC-CM2-SR5", "CMCC-ESM2", "CanESM5",
    "EC-Earth3", "EC-Earth3-Veg-LR", "GFDL-CM4", "GFDL-ESM4", "INM-CM4-8",
    "INM-CM5-0", "KIOST-ESM", "MPI-ESM1-2-HR", "MPI-ESM1-2-LR",
    "MRI-ESM2-0", "NorESM2-LM", "NorESM2-MM", "TaiESM1",
)

#: KACE-1-0-G carries every required input but runs a **360_day** calendar: 360-day
#: years of 30-day months, including a 30 February. The driver extraction rejected it
#: outright on 2026-10-06, and relaxing that gate would not help. ``annual_metrics``
#: requires a year's dates to equal the real calendar minus 29 February, the UHE
#: reference is on a real calendar, and the pinned QDM groups by day of year -- so a
#: 360-day year cannot be aligned to any of them without inventing 5 to 6 days per
#: year. Admitting it would be fabrication, not a gate relaxation, so it is excluded
#: here with its inputs intact. Every other model in the archive runs
#: proleptic_gregorian, 365_day or noleap, all of which are supported.
CALENDAR_EXCLUDED_MODELS = {"KACE-1-0-G": "360_day calendar"}


def _clean_names(requested: list[str], label: str) -> list[str]:
    """Strip and validate a requested name list, rejecting blanks and SQL quotes."""
    values = [str(value).strip() for value in requested]
    if not values or any(not value for value in values):
        raise ValueError(f"Empty {label} name requested")
    if any("'" in value for value in values):
        raise ValueError(f"Quote characters are not supported in {label} names")
    return values


def resolve_states(requested: list[str] | None) -> tuple[str, ...] | None:
    """Resolve a requested state roster; None means every state in the boundary file.

    The national case returns None rather than a frozen name list so the recorded
    support signature stores the sentinel, and cannot silently go stale when states
    are added to or renamed in the boundary roster underneath it.
    """
    if requested is None:
        return tuple(STATES)
    values = _clean_names(requested, "state")
    if ALL in values:
        if len(values) != 1:
            raise ValueError(f"--states {ALL} cannot be combined with explicit names")
        return None
    return tuple(dict.fromkeys(values))


def resolve_models(requested: list[str] | None) -> tuple[str, ...]:
    """Resolve a requested model roster against the verified complete-input ensemble."""
    if requested is None:
        return tuple(MODEL_DEFAULTS)
    values = _clean_names(requested, "model")
    if ALL in values:
        if len(values) != 1:
            raise ValueError(f"--models {ALL} cannot be combined with explicit names")
        return tuple(ENSEMBLE_MODELS)
    unique = tuple(dict.fromkeys(values))
    unknown = sorted(set(unique) - set(ENSEMBLE_MODELS))
    if unknown:
        raise ValueError(f"Models lack complete Route B inputs: {unknown}")
    return unique


def roster_where(states: tuple[str, ...] | None) -> str | None:
    """Build the OGR attribute filter for a state roster, or None to read every unit."""
    if states is None:
        return None
    return "state_name IN ({})".format(",".join(f"'{state}'" for state in states))


def digest(path: Path) -> str:
    """Hash a small provenance/support artifact."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_jobs(jobs: list, task, workers: int, label: str) -> list:
    """Run independent shard tasks serially or across a process pool, failing on the first error.

    Every stage this drives writes one atomically-named shard per job and verifies a cached
    shard instead of rewriting it, so the only difference parallelism makes is wall clock.
    Results are consumed as they complete; a worker exception propagates rather than being
    logged and skipped, because a partial input set must not look like a finished one.
    """
    total = len(jobs)
    if workers <= 1:
        return [_announce(task(job), index, total) for index, job in enumerate(jobs, 1)]
    from concurrent.futures import ProcessPoolExecutor, as_completed

    print(f"Running {total} {label} across {workers} workers", flush=True)
    results, done = [], 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, job): job for job in jobs}
        try:
            for future in as_completed(futures):
                done += 1
                results.append(_announce(future.result(), done, total))
        except BaseException:
            for future in futures:
                future.cancel()
            raise
    return results


def _announce(result, index: int, total: int):
    """Report one completed shard, tolerating tasks that return a record rather than text."""
    print(f"[{index}/{total}] {result if isinstance(result, str) else result.get('shard', '')}",
          flush=True)
    return result


def verify_support(root: Path) -> dict:
    """Reject changed support artifacts before using cached reference or drivers."""
    manifest = json.loads((root / "support_manifest.json").read_text())
    signature = hashlib.sha256(json.dumps(manifest["signature"], sort_keys=True).encode()).hexdigest()
    if signature != manifest["signature_sha256"]:
        raise ValueError("Spatial support manifest signature changed")
    for name, expected in manifest["artifacts"].items():
        if digest(root / name) != expected:
            raise ValueError(f"Spatial support artifact changed: {name}")
    return manifest


def atomic_npz(path: Path, **arrays) -> None:
    """Write one NPZ atomically, keeping incomplete outputs out of resume paths."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".part")
    try:
        with temporary.open("wb") as stream:
            np.savez_compressed(stream, **arrays)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def build_support(args: argparse.Namespace) -> None:
    """Freeze IRT polygon intersections, native-grid remapping and source identity."""
    import geopandas as gpd
    import xarray as xr

    root = args.work_dir
    root.mkdir(parents=True, exist_ok=True)
    _, _, lat, lon, spacing, _ = load_sample(args.reference_dir)
    grid_path = args.data_dir / "r1i1p1f1/historical/tas/ACCESS-CM2/2005.nc"
    with xr.open_dataset(grid_path) as ds:
        nex_lat, nex_lon = ds.lat.values, ds.lon.values
    boundary_paths = [args.data_dir / f"{name}s_4326.geojson" for name in ("district", "block")]
    states = resolve_states(args.states)
    signature = {"version": VERSION, "states": ALL if states is None else list(states),
                 "reference_spacing": spacing,
                 "boundary_sha256": {p.name: digest(p) for p in boundary_paths},
                 "elevation_sha256": digest(args.elevation_cells),
                 "grid_sha256": hashlib.sha256(nex_lat.tobytes() + nex_lon.tobytes()).hexdigest()}
    signature_text = json.dumps(signature, sort_keys=True)
    manifest_path = root / "support_manifest.json"
    if manifest_path.exists():
        old = json.loads(manifest_path.read_text())
        if old["signature"] != signature:
            raise ValueError("Support source signature changed; choose a new work directory")
        for name, expected in old["artifacts"].items():
            if digest(root / name) != expected:
                raise ValueError(f"Support artifact changed: {name}")
        print("Verified cached spatial support", flush=True)
        return
    entries, units = [], []
    for level, path in zip(("district", "block"), boundary_paths):
        frame = gpd.read_file(path, where=roster_where(states)).to_crs("EPSG:4326")
        if states is None:
            if frame.empty:
                raise ValueError(f"No {level} units found for the national roster")
        elif set(frame.state_name) != set(states):
            raise ValueError(f"Incomplete {level} state roster: "
                             f"{sorted(set(states) - set(frame.state_name))}")
        for row in frame.itertuples():
            geom = gpd.GeoSeries([row.geometry], crs=frame.crs).make_valid().iloc[0]
            index, area, total = polygon_weights(geom, nex_lat, nex_lon, .25)
            if not len(index):
                raise ValueError("Pilot polygon lies wholly outside NEX grid")
            uid = len(units)
            units.append({"unit_index": uid, "state_name": row.state_name, "level": level,
                          "district_name": row.district_name,
                          "block_name": "" if level == "district" else row.block_name,
                          "lgd_code": str(getattr(row, f"{level}_lgd_code", "")),
                          "whole_area_m2": total, "grid_area_fraction": float(area.sum() / total)})
            entries.extend((uid, int(c), float(a)) for c, a in zip(index, area))
        print(f"Prepared {len(frame)} {level} units", flush=True)
    elevation = pd.read_csv(args.elevation_cells)
    groups = elevation.groupby(["lat", "lon"]).elevation_m
    if (groups.nunique() > 1).any():
        raise ValueError("Inconsistent cached CarbonPlan elevation")
    # An intersected cell with no published elevation is dropped from the support, never run
    # at a fabricated sea level. Dropping it lowers the covered area of every unit that
    # touched it, so the existing per-unit coverage gate decides those units on the method's
    # own terms instead of this stage aborting the whole roster. Ocean-dominated cells around
    # the islands and a few coastal slivers are the expected cause; a large share means the
    # wrong elevation table, so that still aborts.
    candidates = sorted({c for _, c, _ in entries})
    cy, cx = np.divmod(candidates, len(nex_lon))
    candidate_elevation = groups.first().reindex(
        list(zip(nex_lat[cy], nex_lon[cx]))).to_numpy()
    supported = np.isfinite(candidate_elevation)
    if not supported.all():
        excluded = pd.DataFrame({"cell_global_index": np.array(candidates)[~supported],
                                 "lat": nex_lat[cy][~supported], "lon": nex_lon[cx][~supported]})
        excluded.to_csv(root / "cells_without_elevation.csv", index=False)
        share = float((~supported).mean())
        if share > args.max_elevation_gap:
            raise ValueError(
                f"{len(excluded)} of {len(candidates)} intersected cells ({share:.1%}) have no "
                f"elevation, above the {args.max_elevation_gap:.1%} cap; check the elevation "
                f"table rather than proceeding. Listed in cells_without_elevation.csv")
        print(f"Excluded {len(excluded)} of {len(candidates)} cells without elevation "
              f"({share:.2%})", flush=True)
    keep = {c for c, ok in zip(candidates, supported) if ok}
    entries = [(u, c, a) for u, c, a in entries if c in keep]
    if not entries:
        raise ValueError("No intersected cell carries an elevation")
    full_indices = sorted({c for _, c, _ in entries})
    index_map = {full: i for i, full in enumerate(full_indices)}
    shape = (len(units), len(full_indices))
    areas = sparse.csr_matrix(([a for _, _, a in entries],
                              ([u for u, _, _ in entries], [index_map[c] for _, c, _ in entries])), shape=shape)
    # Recompute each unit's grid share against the retained cells only, so a unit that lost
    # cells reports its real support rather than the pre-exclusion figure.
    retained_area = np.asarray(areas.sum(axis=1)).ravel()
    for unit, covered in zip(units, retained_area):
        unit["grid_area_fraction"] = float(covered / unit["whole_area_m2"])
    iy, ix = np.divmod(full_indices, len(nex_lon))
    cells = pd.DataFrame({"cell": np.arange(len(iy)), "lat_index": iy, "lon_index": ix,
                          "lat": nex_lat[iy], "lon": nex_lon[ix]})
    cells["elevation_m"] = groups.first().reindex(list(zip(cells.lat, cells.lon))).to_numpy()
    if not np.isfinite(cells.elevation_m).all():
        raise ValueError("Retained cell lost its elevation after filtering")
    mapping = conservative_matrix(lat, lon, spacing, cells.lat.to_numpy(), cells.lon.to_numpy())
    sparse.save_npz(root / "reference_mapping.npz", mapping)
    sparse.save_npz(root / "admin_areas.npz", areas)
    pd.DataFrame(units).to_csv(root / "units.csv", index=False)
    cells.to_csv(root / "cells.csv", index=False)
    atomic_npz(root / "native_reference_grid.npz", lat=lat, lon=lon, spacing=np.array(spacing))
    artifacts = ["reference_mapping.npz", "admin_areas.npz", "units.csv", "cells.csv", "native_reference_grid.npz"]
    manifest = {"signature": signature, "signature_sha256": hashlib.sha256(signature_text.encode()).hexdigest(),
                "units": len(units), "cells": len(cells),
                "cells_excluded_without_elevation": int((~supported).sum()),
                "artifacts": {p: digest(root / p) for p in artifacts}}
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2), flush=True)


def _reference_year_task(job: tuple[str, str, int, str]) -> str:
    """Reduce one reference year in a worker; loads its own mapping so nothing large is pickled.

    Each year is an independent, atomically written shard with a signature and a source hash,
    so workers never contend and a resumed run verifies rather than recomputes.
    """
    root_text, reference_text, year, signature = job
    root, reference_dir = Path(root_text), Path(reference_text)
    target = root / "reference" / f"{year}.npz"
    days = [date(year, 1, 1) + timedelta(days=i)
            for i in range((date(year + 1, 1, 1) - date(year, 1, 1)).days)]
    paths = [reference_dir / str(year) / f"wbgtmax.{d.isoformat()}.npz" for d in days]
    if not all(p.exists() for p in paths):
        raise FileNotFoundError(f"Reference year {year} incomplete; acquisition must finish first")
    source_hash = hashlib.sha256("".join(digest(p) for p in paths).encode()).hexdigest()
    if target.exists():
        with np.load(target, allow_pickle=False) as old:
            if str(old["signature"]) != signature or str(old["source_hash"]) != source_hash:
                raise ValueError(f"Cached reference signature mismatch: {target}")
        return f"Verified cached reference {year}"
    mapping = sparse.load_npz(root / "reference_mapping.npz")
    values, fractions = [], []
    for day, path in zip(days, paths):
        verify_day(path, day, BOUNDS)
        with np.load(path, allow_pickle=False) as z:
            coarse, fraction = regrid_reference(z["wbgtmax_c"], mapping)
        values.append(coarse)
        fractions.append(fraction)
    atomic_npz(target, values=np.array(values), fractions=np.array(fractions),
               dates=np.array([str(d) for d in days]), signature=np.array(signature),
               source_hash=np.array(source_hash))
    return f"Prepared reference {year}: {len(days)} days"


def reference_years(args: argparse.Namespace) -> None:
    """Reduce every acquired native day to NEX cells; missing dates abort, never fill."""
    root = args.work_dir
    signature = verify_support(root)["signature_sha256"]
    (root / "reference").mkdir(parents=True, exist_ok=True)
    jobs = [(str(root), str(args.reference_dir), year, signature) for year in range(1985, 2015)]
    run_jobs(jobs, _reference_year_task, args.workers, "reference years")


def driver_path(data_dir: Path, model: str, scenario: str, year: int, variable: str) -> Path:
    """Resolve the existing local shade or radiation archive without downloading."""
    base = data_dir / ("nex_gddp_cmip6_v2_wbgt" if variable in ("rsds", "sfcWind") else "")
    return base / "r1i1p1f1" / scenario / variable / model / f"{year}.nc"


def _driver_shard_task(job: tuple[str, str, str, str, int, str]) -> dict:
    """Extract one model/scenario/year driver shard in a worker; loads its own cell table."""
    import xarray as xr

    root_text, data_text, model, scenario, year, signature = job
    root, data_dir = Path(root_text), Path(data_text)
    cells = pd.read_csv(root / "cells.csv")
    variables = ["tas", "tasmax", "huss"]
    if scenario != "historical" or 1990 <= year <= 2010:
        variables += ["rsds", "sfcWind"]
    paths = {v: driver_path(data_dir, model, scenario, year, v) for v in variables}
    missing = [str(p) for p in paths.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing drivers: {missing}")
    lineage = {v: {"path": str(p), "bytes": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns}
               for v, p in paths.items()}
    lineage_hash = hashlib.sha256(json.dumps(lineage, sort_keys=True).encode()).hexdigest()
    target = root / "drivers" / model / scenario / f"{year}.npz"
    shard = f"{model} {scenario} {year}"
    if target.exists():
        with np.load(target, allow_pickle=False) as old:
            if str(old["signature"]) != signature or str(old["lineage_hash"]) != lineage_hash:
                raise ValueError(f"Cached driver sources changed: {target}")
        return {"shard": f"Verified cached {shard}", "model": model, "scenario": scenario,
                "year": year, "cached": True}
    arrays, timestamps, calendar = {}, None, None
    for variable, path in paths.items():
        with xr.open_dataset(path) as ds:
            expected_units = {"tas": {"K"}, "tasmax": {"K"}, "huss": {"1", "kg kg-1"},
                              "rsds": {"W m-2"}, "sfcWind": {"m s-1"}}[variable]
            if ds[variable].attrs.get("units") not in expected_units:
                raise ValueError(f"Driver unit mismatch: {path}")
            extracted = ds[variable].sel(lat=xr.DataArray(cells.lat.to_numpy(), dims="cell"),
                                         lon=xr.DataArray(cells.lon.to_numpy(), dims="cell")).transpose("time", "cell")
            cal = ds.time.encoding.get("calendar", "standard")
            if cal not in ("standard", "gregorian", "proleptic_gregorian", "noleap", "365_day"):
                raise ValueError(f"Unsupported calendar: {cal}")
            stamps = np.array([str(t)[:10] for t in extracted.time.values])
            if cal in ("noleap", "365_day"):
                stamps = np.array([f"{t.year:04d}-{t.month:02d}-{t.day:02d}" for t in extracted.time.values])
            expected = pd.date_range(f"{year}-01-01", f"{year}-12-31")
            if cal in ("noleap", "365_day"):
                expected = expected[~((expected.month == 2) & (expected.day == 29))]
            if not np.array_equal(stamps, expected.strftime("%Y-%m-%d").to_numpy()):
                raise ValueError(f"Incomplete or duplicate calendar: {path}")
            if timestamps is not None and (calendar != cal or not np.array_equal(timestamps, stamps)):
                raise ValueError("Driver calendars disagree")
            timestamps, calendar = stamps, cal
            arrays[variable] = extracted.to_numpy()
            lineage[variable].update({"units": ds[variable].attrs.get("units"), "calendar": cal,
                                      "selected_sha256": hashlib.sha256(arrays[variable].tobytes()).hexdigest(),
                                      "nonfinite": int((~np.isfinite(arrays[variable])).sum()),
                                      "variant_label": str(ds.attrs.get("variant_label", ""))})
    atomic_npz(target, **arrays, dates=timestamps, calendar=np.array(calendar),
               signature=np.array(signature), lineage_hash=np.array(lineage_hash),
               lineage=np.array(json.dumps(lineage, sort_keys=True)))
    return {"shard": f"Extracted {shard}", "model": model, "scenario": scenario, "year": year,
            "calendar": calendar, "variables": variables, "path": str(target), "cached": False}


def extract_drivers(args: argparse.Namespace) -> None:
    """Inventory and extract exact native cells, retaining invalid values for gates."""
    root = args.work_dir
    signature = json.loads((root / "support_manifest.json").read_text())["signature_sha256"]
    jobs = [(str(root), str(args.data_dir), model, scenario, year, signature)
            for model in args.models
            for scenario, (first, last) in PERIODS.items()
            for year in range(first, last + 1)]
    for model in args.models:
        for scenario in PERIODS:
            (root / "drivers" / model / scenario).mkdir(parents=True, exist_ok=True)
    records = run_jobs(jobs, _driver_shard_task, args.workers, "driver shards")
    inventory = [r for r in records if not r.get("cached")]
    (root / "extraction_last_run.json").write_text(json.dumps(inventory, indent=2))


def main(argv: list[str] | None = None) -> int:
    """Plan or execute one explicit input-preparation stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("support", "reference", "drivers"), required=True)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--data-dir", type=Path, default=Path("D:/projects/irt_data"))
    parser.add_argument("--reference-dir", type=Path, default=Path("scratch/uhe_daily"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b"))
    parser.add_argument("--elevation-cells", type=Path, default=Path("scratch/wbgt_route_b/elevation_cells.csv"))
    parser.add_argument("--states", nargs="+", default=None,
                        help=f"State names, or '{ALL}' for every state in the boundary roster "
                             "(default: the three pilot states)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Shard workers for the reference and driver stages (default 1). "
                             "Each shard is an independent atomic write, so this changes wall "
                             "clock only. The support stage is a single job and ignores it.")
    parser.add_argument("--max-elevation-gap", type=float, default=0.05,
                        help="Abort if more than this share of intersected cells lack an "
                             "elevation (default 0.05); below it they are excluded and listed")
    parser.add_argument("--models", nargs="+", default=None,
                        help=f"Model names, or '{ALL}' for the {len(ENSEMBLE_MODELS)}-model "
                             "complete-input ensemble (default: the two pilot models)")
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    try:
        states, models = resolve_states(args.states), resolve_models(args.models)
    except ValueError as error:
        parser.error(str(error))
    args.models = list(models)
    if not args.run:
        print(json.dumps({"stage": args.stage, "states": ALL if states is None else list(states),
                          "models": args.models, "workers": args.workers, "periods": PERIODS,
                          "work_dir": str(args.work_dir), "action": "plan_only"}, indent=2))
        return 0
    started = time.monotonic()
    {"support": build_support, "reference": reference_years, "drivers": extract_drivers}[args.stage](args)
    print(f"Stage {args.stage} complete in {time.monotonic() - started:.1f} seconds", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
