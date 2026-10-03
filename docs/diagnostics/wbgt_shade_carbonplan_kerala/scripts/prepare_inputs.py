"""Prepare bounded inputs for the Kerala CarbonPlan-shade pilot (SPEC.md).

Run in IRT's geo environment from the repository root. Writes only under
scratch/carbonplan_kerala. Public CarbonPlan inputs (CC BY 4.0) are read through
the predecessor reproduction's bounded Zarr reader; chunks already cached by that
reproduction are reused read-only, and new chunks are cached here.

Outputs (under the work dir):
  regions.geojson        CarbonPlan IND hierid regions intersecting Kerala
  cells.csv              region x 0.25 deg cell: area, population, elevation
  region_series.npz      UHE reference and published ACCESS-CM2 shade per region
  drivers.npz            tas, tasmax, huss (time x cell), 1985-2014
  crosswalk.csv          Kerala district/block x region intersection areas
  input_manifest.json    remote and local provenance
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import xarray as xr
from pyproj import Transformer
from shapely.geometry import box, mapping, shape
from shapely.ops import transform, unary_union
from shapely.prepared import prep

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts"))
from prepare_inputs import BASE, Archive  # noqa: E402  (bounded Zarr reader, reused unchanged)

WORK = Path("scratch/carbonplan_kerala")
SHARED_CACHE = Path("scratch/carbonplan_reproduction/cache")
DATA_ROOT = Path("D:/projects/irt_data")
CLIMATE_ROOT = DATA_ROOT / "r1i1p1f1/historical"
STATE = "Kerala"
MODEL = "ACCESS-CM2"
CALIBRATION = (1985, 2014)
DRIVERS = {"tas": "K", "tasmax": "K", "huss": "kg kg-1"}
GEOMETRY_URL = BASE + "inputs/all_regions_and_cities.json"
GEOMETRY_BYTES = 190_178_657
TOTAL_DOWNLOAD_CAP = 2_000_000_000
EQUAL_AREA = "ESRI:53034"
UHE = ("inputs/wbgt-UHE-daily-historical.zarr", "WBGT")
SHADE = ("outputs/zarr/daily/historical-WBGT-shade.zarr", "wbgt-shade")
POPULATION = "inputs/GHS_POP_E2030_GLOBE_R2023A_4326_30ss_V1_0_resampled_to_CP.zarr"
ELEVATION = "inputs/elevation.zarr"


class LayeredArchive(Archive):
    """Archive that reads the predecessor cache first and caps new downloads."""

    def __init__(self, root: Path, shared: Path):
        super().__init__(root)
        self.shared = shared
        self.downloaded = 0

    def fetch(self, url: str) -> bytes:
        """Serve from the shared cache when present; otherwise download under the cap."""
        name = hashlib.sha256(url.encode()).hexdigest() + ".bin"
        shared = self.shared / name
        if shared.exists():
            data = shared.read_bytes()
            self.records[url] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                 "cache": "shared"}
            return data
        local = self.cache / name
        fresh = not local.exists()
        data = super().fetch(url)
        if fresh:
            self.downloaded += len(data)
            if self.downloaded > TOTAL_DOWNLOAD_CAP:
                raise RuntimeError(f"Total download cap exceeded: {self.downloaded} bytes")
        self.records[url]["cache"] = "pilot"
        return data


def kerala_units() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load Kerala districts and blocks from the boundary files used by staging."""
    where = f"state_name = '{STATE}'"
    districts = gpd.read_file(DATA_ROOT / "districts_4326.geojson", where=where)
    blocks = gpd.read_file(DATA_ROOT / "blocks_4326.geojson", where=where)
    if districts.empty or blocks.empty:
        raise ValueError("Kerala boundaries missing")
    for frame in (districts, blocks):
        frame["geometry"] = frame.geometry.make_valid()
    return districts, blocks


def stream_regions(kerala, work: Path) -> tuple[list[dict], dict]:
    """Stream the 190 MB geometry once; store only units (IND regions, cities) touching Kerala.

    CarbonPlan's hierid regions have holes exactly where its city polygons sit, so
    cities and regions together tile the land; both are kept. A city carries a
    synthetic label `city:<UC_NM_MN>` in place of its null hierid.
    """
    target = prep(kerala)
    digest, size, india, cities, kept = hashlib.sha256(), 0, 0, 0, []
    with requests.get(GEOMETRY_URL, timeout=(20, 300), stream=True) as r:
        r.raise_for_status()
        pending = b""
        for block in r.iter_content(1024 * 1024):
            digest.update(block)
            size += len(block)
            pending += block
            *lines, pending = pending.split(b"\n")
            for line in lines:
                is_region, is_city = b'"hierid": "IND' in line, b'"hierid": null' in line
                if not (is_region or is_city):
                    continue
                feature = json.loads(line.strip().rstrip(b","))
                india += is_region
                cities += is_city
                if is_city:
                    p = feature["properties"]
                    name = p.get("UC_NM_MN") or p.get("NAMELSAD20") or str(p["processing_id"])
                    p["hierid"] = f"city:{name}"
                geom = shape(feature["geometry"])
                if target.intersects(geom):
                    kept.append(feature)
        if pending.strip() and (b'"hierid"' in pending):
            raise ValueError("Unparsed trailing geometry record")
    if size != GEOMETRY_BYTES:
        raise ValueError(f"Geometry stream size {size} != {GEOMETRY_BYTES}")
    if not kept:
        raise ValueError("No CarbonPlan region intersects Kerala")
    ids = [f["properties"]["processing_id"] for f in kept]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate processing_id among kept regions")
    (work / "regions.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": kept}))
    return kept, {"url": GEOMETRY_URL, "bytes": size, "sha256": digest.hexdigest(),
                  "india_hierid_features": india, "city_features_global": cities,
                  "kept_features": len(kept),
                  "kept_cities": [f["properties"]["hierid"] for f in kept
                                  if f["properties"]["hierid"].startswith("city:")],
                  "stored": "filtered subset only (SPEC.md declared exception)"}


def region_cells(archive: Archive, regions: list[dict]) -> pd.DataFrame:
    """Area and population weights per region on CarbonPlan's 0.25 deg grid, plus elevation."""
    meta = archive.metadata(POPULATION)
    xs, ys = archive.coord(POPULATION, meta, "x"), archive.coord(POPULATION, meta, "y")
    emeta = archive.metadata(ELEVATION)
    ex, ey = archive.coord(ELEVATION, emeta, "lon"), archive.coord(ELEVATION, emeta, "lat")
    project = Transformer.from_crs("EPSG:4326", EQUAL_AREA, always_xy=True).transform
    rows = []
    for feature in regions:
        props = feature["properties"]
        geom = shape(feature["geometry"]).buffer(0)
        region = transform(project, geom)
        xmin, ymin, xmax, ymax = geom.bounds
        xi = np.flatnonzero((xs + .125 > xmin) & (xs - .125 < xmax))
        yi = np.flatnonzero((ys + .125 > ymin) & (ys - .125 < ymax))
        pop = archive.take(POPULATION, meta, "population", [yi, xi])
        for j, y in enumerate(ys[yi]):
            for i, x in enumerate(xs[xi]):
                area = transform(project, box(x - .125, y - .125, x + .125, y + .125)).intersection(region).area
                if area > 0:
                    rows.append({"hierid": props["hierid"], "processing_id": int(props["processing_id"]),
                                 "lat": float(y), "lon": float(x), "intersection_m2": area,
                                 "population": float(pop[j, i])})
    cells = pd.DataFrame(rows)
    # Population fill_value is 1.8e308 (finite), so an unwritten chunk is caught by magnitude.
    if (~np.isfinite(cells.population) | (cells.population < 0) | (cells.population > 1e15)).any():
        raise ValueError("Invalid population values")
    elevation = {}
    for lat, lon in cells[["lat", "lon"]].drop_duplicates().itertuples(index=False):
        ix, iy = np.flatnonzero(np.isclose(ex, lon)), np.flatnonzero(np.isclose(ey, lat))
        if len(ix) != 1 or len(iy) != 1:
            raise ValueError(f"Elevation grid mismatch at {lat}, {lon}")
        elevation[(lat, lon)] = float(archive.take(ELEVATION, emeta, "elevation", [iy, ix]).item())
    cells["elevation_m"] = [elevation[(a, b)] for a, b in zip(cells.lat, cells.lon)]
    # Notebook 02: no elevation -> no pressure -> null WBGT. Notebook 05 (utils.calc_sparse_weights,
    # mask_nulls) drops null cells BEFORE normalising area fractions. Mirror that order exactly.
    null = ~np.isfinite(cells.elevation_m)
    dropped = cells[null]
    cells = cells[~null].copy()
    if dropped.processing_id.nunique() and set(dropped.processing_id) - set(cells.processing_id):
        raise ValueError("A region lost every cell to the null mask")
    print(f"Null-elevation cells dropped: {len(dropped)} region-cell rows, "
          f"{len(dropped[['lat', 'lon']].drop_duplicates())} unique cells", flush=True)
    grouped = cells.groupby("processing_id")
    cells["area_weight"] = cells.intersection_m2 / grouped.intersection_m2.transform("sum")
    product = cells.area_weight * cells.population
    cells["population_weight"] = product / product.groupby(cells.processing_id).transform("sum")
    return cells


def region_series(archive: Archive, regions: list[dict]) -> dict[str, np.ndarray]:
    """UHE reference (1985-2014) and published ACCESS-CM2 shade per region, by value."""
    out: dict[str, np.ndarray] = {}
    for store, var in (UHE, SHADE):
        frames = {}
        for feature in regions:
            pid = int(feature["properties"]["processing_id"])
            series = archive.city_series(store, var, feature["properties"]["hierid"], pid)
            frames[pid] = series.loc[f"{CALIBRATION[0]}":f"{CALIBRATION[1]}"]
        table = pd.DataFrame(frames)
        expected = pd.date_range(f"{CALIBRATION[0]}-01-01", f"{CALIBRATION[1]}-12-31")
        if not table.index.equals(expected) or not np.isfinite(table.values).all():
            raise ValueError(f"Incomplete {var} series over the calibration window")
        key = "uhe" if store == UHE[0] else "published_shade"
        out[key] = table.values
        out["series_processing_ids"] = np.array(table.columns, dtype=int)
        out["series_dates"] = table.index.strftime("%Y-%m-%d").values.astype("U10")
    return out


def drivers(cells: pd.DataFrame) -> tuple[dict[str, np.ndarray], list[dict]]:
    """Extract tas/tasmax/huss for every unique cell over the calibration window."""
    grid = cells[["lat", "lon"]].drop_duplicates().sort_values(["lat", "lon"]).reset_index(drop=True)
    data = {v: [] for v in DRIVERS}
    dates, records = [], []
    for year in range(CALIBRATION[0], CALIBRATION[1] + 1):
        stamp = None
        for var, unit in DRIVERS.items():
            path = CLIMATE_ROOT / var / MODEL / f"{year}.nc"
            with xr.open_dataset(path) as ds:
                accepted = {unit, "1"} if var == "huss" else {unit}
                if ds[var].attrs.get("units") not in accepted:
                    raise ValueError(f"Unexpected units in {path}")
                a = ds[var].sel(lat=xr.DataArray(grid.lat.values, dims="cell"),
                                lon=xr.DataArray(grid.lon.values, dims="cell")).transpose("time", "cell")
                t = pd.DatetimeIndex(a.time.values)
                if not t.normalize().equals(pd.date_range(f"{year}-01-01", f"{year}-12-31")):
                    raise ValueError(f"Dates not complete: {path}")
                if stamp is not None and not t.equals(stamp):
                    raise ValueError(f"Dates not aligned across variables: {path}")
                stamp = t
                values = a.values
                if not np.isfinite(values).all():
                    raise ValueError(f"Non-finite driver values: {path}")
                data[var].append(values)
                records.append({"path": str(path), "bytes": path.stat().st_size,
                                "selected_values_sha256": hashlib.sha256(values.tobytes()).hexdigest()})
        dates.append(stamp)
        print("  drivers", year, flush=True)
    arrays = {v: np.concatenate(a) for v, a in data.items()}
    arrays["time"] = np.concatenate([d.values for d in dates])
    arrays["lat"], arrays["lon"] = grid.lat.values, grid.lon.values
    return arrays, records


def crosswalk(districts: gpd.GeoDataFrame, blocks: gpd.GeoDataFrame, regions: list[dict]) -> pd.DataFrame:
    """Intersection area of every Kerala district and block with every kept region."""
    reg = gpd.GeoDataFrame(
        [{"hierid": f["properties"]["hierid"], "processing_id": int(f["properties"]["processing_id"])}
         for f in regions],
        geometry=[shape(f["geometry"]).buffer(0) for f in regions], crs="EPSG:4326").to_crs(EQUAL_AREA)
    rows = []
    for level, frame in (("district", districts), ("block", blocks)):
        units = frame.to_crs(EQUAL_AREA)
        for unit in units.itertuples():
            block = getattr(unit, "block_name", None) if level == "block" else None
            hits = reg[reg.intersects(unit.geometry)]
            for region in hits.itertuples():
                area = unit.geometry.intersection(region.geometry).area
                if area > 0:
                    rows.append({"level": level, "district": unit.district_name, "block": block,
                                 "hierid": region.hierid, "processing_id": region.processing_id,
                                 "intersection_m2": area, "unit_area_m2": unit.geometry.area})
            if hits.empty:
                rows.append({"level": level, "district": unit.district_name, "block": block,
                             "hierid": None, "processing_id": -1, "intersection_m2": 0.0,
                             "unit_area_m2": unit.geometry.area})
    out = pd.DataFrame(rows)
    keys = ["level", "district", "block"]
    out["coverage"] = out.groupby(keys, dropna=False).intersection_m2.transform("sum") / out.unit_area_m2
    return out


def main() -> None:
    """Write every pilot input under scratch, with provenance."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    args = parser.parse_args()
    work = args.work_dir.resolve()
    if "scratch" not in work.parts:
        raise ValueError("Inputs must be written into scratch")
    work.mkdir(parents=True, exist_ok=True)
    archive = LayeredArchive(work, SHARED_CACHE.resolve())

    districts, blocks = kerala_units()
    print(f"Kerala: {len(districts)} districts, {len(blocks)} blocks", flush=True)
    kerala = unary_union(districts.geometry.values)
    regions, geometry_record = stream_regions(kerala, work)
    print(f"Units intersecting Kerala: {len(regions)} ({len(geometry_record['kept_cities'])} cities: "
          f"{geometry_record['kept_cities']}); IND regions in file {geometry_record['india_hierid_features']}",
          flush=True)

    cells = region_cells(archive, regions)
    cells.to_csv(work / "cells.csv", index=False)
    print(f"Region cells: {len(cells)} rows, {len(cells[['lat', 'lon']].drop_duplicates())} unique cells",
          flush=True)

    np.savez_compressed(work / "region_series.npz", **region_series(archive, regions))
    print("Region series written", flush=True)

    arrays, local_records = drivers(cells)
    np.savez_compressed(work / "drivers.npz", **arrays)

    cw = crosswalk(districts, blocks, regions)
    cw.to_csv(work / "crosswalk.csv", index=False)
    low = cw[cw.coverage < .95].drop_duplicates(["level", "district", "block"])
    print(f"Crosswalk rows {len(cw)}; units below 0.95 coverage: {len(low)}", flush=True)

    manifest = {"state": STATE, "model": MODEL, "calibration_years": list(CALIBRATION),
                "regions": geometry_record, "new_download_bytes": archive.downloaded,
                "remote_sources": archive.records, "local_sources": local_records,
                "boundaries": {p: {"bytes": (DATA_ROOT / p).stat().st_size}
                               for p in ("districts_4326.geojson", "blocks_4326.geojson")},
                "data_license": "CarbonPlan CC BY 4.0",
                "input_lineage": "Local NEX versions not assumed identical to CarbonPlan's 2023 inputs"}
    (work / "input_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    print(f"Done. New downloads: {archive.downloaded / 1e6:.1f} MB", flush=True)


if __name__ == "__main__":
    main()
