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


def digest(path: Path) -> str:
    """Hash a small provenance/support artifact."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
    signature = {"version": VERSION, "states": list(STATES), "reference_spacing": spacing,
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
        where = "state_name IN ('Kerala','Rajasthan','Himachal Pradesh')"
        frame = gpd.read_file(path, where=where).to_crs("EPSG:4326")
        if set(frame.state_name) != set(STATES):
            raise ValueError("Incomplete pilot state roster")
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
    full_indices = sorted({c for _, c, _ in entries})
    index_map = {full: i for i, full in enumerate(full_indices)}
    shape = (len(units), len(full_indices))
    areas = sparse.csr_matrix(([a for _, _, a in entries],
                              ([u for u, _, _ in entries], [index_map[c] for _, c, _ in entries])), shape=shape)
    iy, ix = np.divmod(full_indices, len(nex_lon))
    cells = pd.DataFrame({"cell": np.arange(len(iy)), "lat_index": iy, "lon_index": ix,
                          "lat": nex_lat[iy], "lon": nex_lon[ix]})
    elevation = pd.read_csv(args.elevation_cells)
    groups = elevation.groupby(["lat", "lon"]).elevation_m
    if (groups.nunique() > 1).any():
        raise ValueError("Inconsistent cached CarbonPlan elevation")
    cells["elevation_m"] = groups.first().reindex(list(zip(cells.lat, cells.lon))).to_numpy()
    if not np.isfinite(cells.elevation_m).all():
        missing = cells[~np.isfinite(cells.elevation_m)]
        missing.to_csv(root / "missing_elevation_cells.csv", index=False)
        raise ValueError(f"Missing CarbonPlan elevation for {len(missing)} cells; no sea-level fallback")
    mapping = conservative_matrix(lat, lon, spacing, cells.lat.to_numpy(), cells.lon.to_numpy())
    sparse.save_npz(root / "reference_mapping.npz", mapping)
    sparse.save_npz(root / "admin_areas.npz", areas)
    pd.DataFrame(units).to_csv(root / "units.csv", index=False)
    cells.to_csv(root / "cells.csv", index=False)
    atomic_npz(root / "native_reference_grid.npz", lat=lat, lon=lon, spacing=np.array(spacing))
    artifacts = ["reference_mapping.npz", "admin_areas.npz", "units.csv", "cells.csv", "native_reference_grid.npz"]
    manifest = {"signature": signature, "signature_sha256": hashlib.sha256(signature_text.encode()).hexdigest(),
                "units": len(units), "cells": len(cells), "artifacts": {p: digest(root / p) for p in artifacts}}
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2), flush=True)


def reference_years(args: argparse.Namespace) -> None:
    """Reduce every acquired native day to NEX cells; missing dates abort, never fill."""
    root = args.work_dir
    manifest = verify_support(root)
    signature = manifest["signature_sha256"]
    mapping = sparse.load_npz(root / "reference_mapping.npz")
    for year in range(1985, 2015):
        target = root / "reference" / f"{year}.npz"
        days = [date(year, 1, 1) + timedelta(days=i) for i in range((date(year + 1, 1, 1) - date(year, 1, 1)).days)]
        paths = [args.reference_dir / str(year) / f"wbgtmax.{d.isoformat()}.npz" for d in days]
        if not all(p.exists() for p in paths):
            raise FileNotFoundError(f"Reference year {year} incomplete; acquisition must finish first")
        source_hash = hashlib.sha256("".join(digest(p) for p in paths).encode()).hexdigest()
        if target.exists():
            with np.load(target, allow_pickle=False) as old:
                if str(old["signature"]) != signature or str(old["source_hash"]) != source_hash:
                    raise ValueError("Cached reference signature mismatch")
            print(f"Verified cached reference {year}", flush=True)
            continue
        values, fractions = [], []
        for day, path in zip(days, paths):
            verify_day(path, day, BOUNDS)
            with np.load(path, allow_pickle=False) as z:
                coarse, fraction = regrid_reference(z["wbgtmax_c"], mapping)
            values.append(coarse)
            fractions.append(fraction)
        atomic_npz(target, values=np.array(values), fractions=np.array(fractions),
                   dates=np.array([str(d) for d in days]), signature=np.array(signature), source_hash=np.array(source_hash))
        print(f"Prepared reference {year}: {len(days)} days", flush=True)


def driver_path(data_dir: Path, model: str, scenario: str, year: int, variable: str) -> Path:
    """Resolve the existing local shade or radiation archive without downloading."""
    base = data_dir / ("nex_gddp_cmip6_v2_wbgt" if variable in ("rsds", "sfcWind") else "")
    return base / "r1i1p1f1" / scenario / variable / model / f"{year}.nc"


def extract_drivers(args: argparse.Namespace) -> None:
    """Inventory and extract exact native cells, retaining invalid values for gates."""
    import xarray as xr

    root = args.work_dir
    cells = pd.read_csv(root / "cells.csv")
    signature = json.loads((root / "support_manifest.json").read_text())["signature_sha256"]
    inventory = []
    for model in args.models:
        for scenario, (first, last) in PERIODS.items():
            for year in range(first, last + 1):
                variables = ["tas", "tasmax", "huss"]
                if scenario != "historical" or 1990 <= year <= 2010:
                    variables += ["rsds", "sfcWind"]
                paths = {v: driver_path(args.data_dir, model, scenario, year, v) for v in variables}
                missing = [str(p) for p in paths.values() if not p.is_file()]
                if missing:
                    raise FileNotFoundError(f"Missing pilot drivers: {missing}")
                lineage = {v: {"path": str(p), "bytes": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns} for v, p in paths.items()}
                lineage_hash = hashlib.sha256(json.dumps(lineage, sort_keys=True).encode()).hexdigest()
                target = root / "drivers" / model / scenario / f"{year}.npz"
                if target.exists():
                    with np.load(target, allow_pickle=False) as old:
                        if str(old["signature"]) != signature or str(old["lineage_hash"]) != lineage_hash:
                            raise ValueError(f"Cached driver sources changed: {target}")
                    print(f"Verified cached {model} {scenario} {year}", flush=True)
                    continue
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
                            raise ValueError(f"Unsupported pilot calendar: {cal}")
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
                inventory.append({"model": model, "scenario": scenario, "year": year, "calendar": calendar,
                                  "variables": variables, "path": str(target)})
                print(f"Extracted {model} {scenario} {year}", flush=True)
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
    parser.add_argument("--models", nargs="+", choices=MODEL_DEFAULTS, default=list(MODEL_DEFAULTS))
    args = parser.parse_args(argv)
    if not args.run:
        print(json.dumps({"stage": args.stage, "models": args.models, "periods": PERIODS,
                          "work_dir": str(args.work_dir), "action": "plan_only"}, indent=2))
        return 0
    started = time.monotonic()
    {"support": build_support, "reference": reference_years, "drivers": extract_drivers}[args.stage](args)
    print(f"Stage {args.stage} complete in {time.monotonic() - started:.1f} seconds", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
