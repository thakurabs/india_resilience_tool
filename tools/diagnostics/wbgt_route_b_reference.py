"""Audit native UHE support and reproduce CarbonPlan's reference on sample dates.

CHG-0649. Diagnostic only. No NEX compute or production output writes. Run in
the existing IRT geospatial environment; network is opt-in and capped.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import importlib.util
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

from tools.data_acquisition.uhe_daily import BOUNDS, SAMPLE_DATES, verify_day

STATES = ("Kerala", "Rajasthan", "Himachal Pradesh")
REPO = Path(__file__).resolve().parents[2]
POP_STORE = "inputs/GHS_POP_E2030_GLOBE_R2023A_4326_30ss_V1_0_resampled_to_UHE_daily.zarr"
CONTROL_CITIES = {"Kochi": 7886, "Bikaner": 6558, "Shimla": 6822}


def polygon_weights(geometry, lat: np.ndarray, lon: np.ndarray, spacing: float,
                    crs: str = "EPSG:6933") -> tuple[np.ndarray, np.ndarray, float]:
    """Return flattened native-cell indices and exact projected intersection areas.

    Denominator is the whole polygon area, including portions outside the grid.
    Empty support is explicit. Invalid or empty geometries are rejected.
    """
    import geopandas as gpd
    import shapely

    if geometry.is_empty or not geometry.is_valid or not np.isfinite(geometry.bounds).all():
        raise ValueError("Geometry must be finite, valid, nonempty")
    west, south, east, north = geometry.bounds
    iy = np.flatnonzero((lat + spacing / 2 > south) & (lat - spacing / 2 < north))
    ix = np.flatnonzero((lon + spacing / 2 > west) & (lon - spacing / 2 < east))
    region = gpd.GeoSeries([geometry], crs="EPSG:4326").to_crs(crs).iloc[0]
    if region.area <= 0:
        raise ValueError("Nonpositive polygon area")
    if not len(iy) or not len(ix):
        return np.array([], dtype=int), np.array([], dtype=float), float(region.area)
    yy, xx = np.meshgrid(iy, ix, indexing="ij")
    yy, xx = yy.ravel(), xx.ravel()
    cells = shapely.box(lon[xx] - spacing / 2, lat[yy] - spacing / 2,
                        lon[xx] + spacing / 2, lat[yy] + spacing / 2)
    cells = gpd.GeoSeries(cells, crs="EPSG:4326").to_crs(crs).to_numpy()
    areas = shapely.area(shapely.intersection(cells, region))
    keep = areas > 0
    return (yy * len(lon) + xx)[keep], areas[keep], float(region.area)


def aggregate(values: np.ndarray, indices: np.ndarray, areas: np.ndarray,
              whole_area: float, population: np.ndarray | None = None) -> dict:
    """Report finite support and weighted value without hiding missing area.

    CarbonPlan controls multiply intersection-area weights by cell population;
    wholly zero population falls back to area weights, as in pinned utils.py.
    """
    selected = np.asarray(values).ravel()[indices]
    finite = np.isfinite(selected)
    valid_area = float(areas[finite].sum())
    denominator = areas.copy()
    if population is not None:
        p = np.asarray(population).ravel()[indices]
        if np.any(~np.isfinite(p[finite])) or np.any(p[finite] < 0) or np.any(p[finite] > 1e15):
            raise ValueError("Missing/invalid population on finite reference support")
        product = areas[finite] * p[finite]
        if product.sum() > 0:
            denominator[finite] = product
    denom = float(denominator[finite].sum())
    return {"valid_area_fraction": valid_area / whole_area,
            "grid_area_fraction": float(areas.sum()) / whole_area,
            "intersecting_cells": len(indices), "valid_cells": int(finite.sum()),
            "weighted_mean_c": float(np.dot(selected[finite], denominator[finite]) / denom) if denom else np.nan,
            "min_c": float(selected[finite].min()) if finite.any() else np.nan,
            "max_c": float(selected[finite].max()) if finite.any() else np.nan}


def load_sample(root: Path) -> tuple[list[date], np.ndarray, np.ndarray, np.ndarray, float, list[dict]]:
    """Verify and load all twelve sample dates, requiring identical native grids."""
    arrays, records = [], []
    lat = lon = None
    spacing = None
    for day in SAMPLE_DATES:
        path = root / str(day.year) / f"wbgtmax.{day.isoformat()}.npz"
        record = verify_day(path, day, BOUNDS)
        with np.load(path, allow_pickle=False) as z:
            if lat is None:
                lat, lon, spacing = z["latitude"], z["longitude"], record["resolution_degrees"]
            elif not np.array_equal(lat, z["latitude"]) or not np.array_equal(lon, z["longitude"]):
                raise ValueError(f"Changed source grid: {path}")
            arrays.append(z["wbgtmax_c"])
        records.append(record)
    return list(SAMPLE_DATES), np.stack(arrays), lat, lon, spacing, records


def control_population(work: Path, lat: np.ndarray, lon: np.ndarray, allow_network: bool,
                       max_bytes: int) -> tuple[np.ndarray, dict]:
    """Read only needed native-grid population chunks, under a cumulative byte cap."""
    import requests

    source = REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/prepare_inputs.py"
    spec = importlib.util.spec_from_file_location("route_b_archive", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ledger_path = work / "transfer_ledger.json"
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {"received_bytes": 0, "records": {}}

    class Archive(module.Archive):
        def fetch(self, url: str) -> bytes:
            key = hashlib.sha256(url.encode()).hexdigest() + ".bin"
            path = self.cache / key
            if not path.exists():
                if not allow_network:
                    raise ValueError(f"Missing population cache; use --fetch-population: {url}")
                remaining = max_bytes - ledger["received_bytes"]
                if remaining <= 0:
                    raise ValueError("Population transfer cap exhausted")
                time.sleep(2)
                with requests.get(url, stream=True, timeout=(20, 120)) as response:
                    response.raise_for_status()
                    length = response.headers.get("Content-Length")
                    if length is not None and int(length) > min(remaining, 160_000_000):
                        raise ValueError(f"Population object exceeds remaining/per-object cap: {url}")
                    blocks, size = [], 0
                    try:
                        for block in response.iter_content(65536):
                            size += len(block)
                            ledger["received_bytes"] += len(block)
                            if size > 160_000_000 or ledger["received_bytes"] > max_bytes:
                                raise ValueError("Population transfer limit exceeded")
                            blocks.append(block)
                    finally:
                        ledger_path.write_text(json.dumps(ledger, indent=2))
                    data = b"".join(blocks)
                    if length is not None and size != int(length):
                        raise ValueError("Truncated population object")
                    temporary = path.with_suffix(".part")
                    temporary.write_bytes(data)
                    temporary.replace(path)
            data = path.read_bytes()
            record = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            if url in ledger["records"] and record != ledger["records"][url]:
                raise ValueError("Population cache checksum mismatch")
            ledger["records"][url] = record
            ledger_path.write_text(json.dumps(ledger, indent=2))
            return data

    archive = Archive(work)
    meta = archive.metadata(POP_STORE)
    xs, ys = archive.coord(POP_STORE, meta, "x"), archive.coord(POP_STORE, meta, "y")
    # Use exact coordinates: do not snap a different population grid into place.
    xi = pd.Index(xs).get_indexer(lon)
    yi = pd.Index(ys).get_indexer(lat)
    if np.any(xi < 0) or np.any(yi < 0):
        raise ValueError("Population/UHE native coordinate mismatch")
    pop = archive.take(POP_STORE, meta, "population", [yi, xi])
    return pop, ledger


def run(args: argparse.Namespace) -> dict:
    """Write support, seasonal and matched-footprint reference-control evidence."""
    import geopandas as gpd

    start = time.monotonic()
    days, values, lat, lon, spacing, sources = load_sample(args.reference_dir)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    pop, transfer = control_population(args.work_dir, lat, lon, args.fetch_population, int(args.max_transfer_gb * 1e9))
    coverage, summaries, geometries = [], [], []
    boundaries = {}
    # Pilot roster plus the island UTs, where support failure is most likely.
    levels = () if args.control_only else (("district", "districts_4326.geojson"), ("block", "blocks_4326.geojson"))
    for level, filename in levels:
        path = args.data_dir / filename
        boundaries[filename] = hashlib.sha256(path.read_bytes()).hexdigest()
        frame = gpd.read_file(path).to_crs("EPSG:4326")
        if frame.crs is None:
            raise ValueError("Boundary CRS missing")
        keep = frame.state_name.isin(STATES) | frame.state_name.str.contains("Andaman|Lakshadweep", case=False)
        frame = frame[keep].copy()
        for state in STATES:
            if not (frame.state_name == state).any():
                raise ValueError(f"Missing {level} roster: {state}")
        for row in frame.itertuples():
            key = {"state": row.state_name, "level": level, "district": row.district_name,
                   "unit": row.district_name if level == "district" else row.block_name}
            original = row.geometry
            geom = gpd.GeoSeries([original], crs=frame.crs).make_valid().iloc[0]
            indices, areas, whole = polygon_weights(geom, lat, lon, spacing)
            covs = []
            for day, field in zip(days, values):
                metrics = aggregate(field, indices, areas, whole)
                covs.append(metrics["valid_area_fraction"])
                summaries.append({**key, "date": str(day), **metrics})
            coverage.append({**key, "whole_area_m2": whole, "minimum_valid_area_fraction": min(covs),
                             "maximum_valid_area_fraction": max(covs), "source_geometry_valid": bool(original.is_valid),
                             "intersecting_cells": len(indices), "sample_complete_support": min(covs) >= 0.999999})
            geometries.append({**key, "bounds": list(geom.bounds)})
        print(f"Audited {len(frame)} {level} units", flush=True)
    controls = []
    for city, pid in CONTROL_CITIES.items():
        root = REPO / "scratch/carbonplan_reproduction/cities" / city
        geometry_path = REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_multicity/results" / f"{city}_geometry.geojson"
        frame = gpd.read_file(geometry_path).to_crs("EPSG:4326")
        if len(frame) != 1:
            raise ValueError("Expected one city control polygon")
        footprint = .25 if args.control_footprint == "published-025" else spacing
        idx, area, whole = polygon_weights(frame.geometry.iloc[0], lat, lon, footprint, "ESRI:53034")
        ref = pd.read_csv(root / "reference.csv", parse_dates=["date"]).set_index("date").reference
        for day, field in zip(days, values):
            measured = aggregate(field, idx, area, whole, pop)
            expected = float(ref.loc[pd.Timestamp(day)])
            controls.append({"city": city, "processing_id": pid, "date": str(day), "footprint_degrees": footprint, **measured,
                             "published_reference_c": expected, "residual_c": measured["weighted_mean_c"] - expected})
    if not args.control_only:
        pd.DataFrame(coverage).to_csv(args.out_dir / "admin_support.csv", index=False)
        pd.DataFrame(summaries).to_csv(args.out_dir / "sample_admin_values.csv", index=False)
    controls = pd.DataFrame(controls)
    controls.to_csv(args.out_dir / "reference_controls.csv", index=False)
    max_error = float(controls.residual_c.abs().max())
    finite = bool(np.isfinite(controls.residual_c).all())
    result = {"change_id": "CHG-0649", "status": "REFERENCE_CONTROL_PASS" if finite and max_error <= 1e-3 else "REFERENCE_CONTROL_FAIL",
              "control_tolerance_c": 1e-3, "control_max_abs_c": max_error,
              "control_footprint": args.control_footprint, "control_only": args.control_only,
              "control_rows": len(controls), "sample_dates": len(days),
              "admin_units": len(coverage), "boundaries_sha256": boundaries,
              "valid_mask_stable": bool(np.all(np.isfinite(values) == np.isfinite(values[0]))),
              "units_with_less_than_99pct_support": sum(x["minimum_valid_area_fraction"] < .99 for x in coverage),
              "units_without_support": sum(x["minimum_valid_area_fraction"] == 0 for x in coverage),
              "population_transfer": transfer, "reference_sources": sources,
              "elapsed_seconds": time.monotonic() - start,
              "scientific_status": "PROVISIONAL_REFERENCE_NOT_INDEPENDENTLY_VALIDATED",
              "limitations": ["12 dates cannot establish full-period completeness or extremes", "No independent Indian WBGT observations scored", "Area support is reported, not automatically accepted", "CarbonPlan reference control is a lineage/aggregation check, not observational accuracy"]}
    (args.out_dir / "qc_manifest.json").write_text(json.dumps(result, indent=2, allow_nan=False))
    print(json.dumps({k: v for k, v in result.items() if k not in ("population_transfer", "reference_sources")}, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    """Default to a read-only plan; explicitly opt into QC writes and population fetch."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fetch-population", action="store_true")
    parser.add_argument("--control-only", action="store_true", help="Run only matched-city reference controls")
    parser.add_argument("--control-footprint", choices=("native", "published-025"), default="native",
                        help="Native 0.05-degree cells, or pinned upstream's hardcoded 0.25-degree footprints")
    parser.add_argument("--max-transfer-gb", type=float, default=.2)
    parser.add_argument("--data-dir", type=Path, default=Path("D:/projects/irt_data"))
    parser.add_argument("--reference-dir", type=Path, default=Path("scratch/uhe_daily"))
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b_reference"))
    parser.add_argument("--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_route_b_reference/qc"))
    args = parser.parse_args(argv)
    if not np.isfinite(args.max_transfer_gb) or args.max_transfer_gb <= 0:
        parser.error("Transfer cap must be finite and positive")
    if not args.run:
        print(json.dumps({"action": "plan_only", "states": STATES, "include_islands": True,
                          "dates": [str(d) for d in SAMPLE_DATES], "controls": CONTROL_CITIES,
                          "control_tolerance_c": 1e-3, "out_dir": str(args.out_dir),
                          "population_transfer_cap_gb": args.max_transfer_gb}, indent=2))
        return 0
    verdict = run(args)
    return 0 if verdict["status"] == "REFERENCE_CONTROL_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
