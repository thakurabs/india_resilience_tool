"""Prepare inputs for the CarbonPlan shade-WBGT held-out validation (SPEC.md, CHG-0646).

Run in IRT's geo environment (``irt``) from the repository root. Writes only under
``scratch/carbonplan_shade_validation``. Caches of the predecessor reproduction
(CHG-0631) and the Kerala pilot (CHG-0639/0643) are read, never written.

Stages (each is idempotent; a finished stage is not redone):
  geometry   stream CarbonPlan's 190 MB geometry ONCE, keep units touching the states
  plan       list every remote chunk still needed, HEAD the uncached ones, price them
             against the cumulative 2 GB cap, and stop
  fetch      cells/weights, UHE, published shade (Kerala, ACCESS-CM2) under the cap
  drivers    NEX tas/tasmax/huss inventory + extraction per model (local files only)
  crosswalk  IRT district/block x unit intersections, coverage and overlap checks

Every network byte (geometry stream included) is appended to transfer_ledger.json.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import sys
import time
from pathlib import Path

import cftime
import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import requests
import xarray as xr
from numcodecs import get_codec
from pyproj import Transformer
from shapely.geometry import box, shape
from shapely.ops import transform, unary_union
from shapely.prepared import prep

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts"))
from prepare_inputs import BASE, Archive  # noqa: E402  (bounded Zarr reader, reused unchanged)

WORK = Path("scratch/carbonplan_shade_validation")
READ_ONLY_CACHES = (Path("scratch/carbonplan_reproduction/cache"), Path("scratch/carbonplan_kerala/cache"))
DATA_ROOT = Path("D:/projects/irt_data")
NEX_ROOT = DATA_ROOT / "r1i1p1f1"
STATES = ("Kerala", "Rajasthan", "Himachal Pradesh")
MODELS = ("ACCESS-CM2", "CMCC-CM2-SR5")
PERIODS = {"historical": (1985, 2014), "ssp245": (2020, 2059)}
AW_EXTRA = ("ssp245", (2060, 2080))           # ACCESS-CM2, AW subset cells only
AW_UNITS = (19567, 20346, 20372)
PUBLISHED_STATE, PUBLISHED_MODEL = "Kerala", "ACCESS-CM2"
DRIVERS = {"tas": {"K"}, "tasmax": {"K"}, "huss": {"kg kg-1", "1"}}
GEOMETRY_URL = BASE + "inputs/all_regions_and_cities.json"
GEOMETRY_BYTES = 190_178_657
CAP_BYTES = 2_000_000_000
OBJECT_CAP = 160_000_000
EQUAL_AREA = "ESRI:53034"
UHE = ("inputs/wbgt-UHE-daily-historical.zarr", "WBGT")
SHADE = {"historical": "outputs/zarr/daily/historical-WBGT-shade.zarr",
         "ssp245-2030": "outputs/zarr/daily/ssp245-2030-WBGT-shade.zarr",
         "ssp245-2050": "outputs/zarr/daily/ssp245-2050-WBGT-shade.zarr"}
SHADE_WINDOWS = {"historical": (1985, 2014), "ssp245-2030": (2020, 2039), "ssp245-2050": (2040, 2059)}
POPULATION = "inputs/GHS_POP_E2030_GLOBE_R2023A_4326_30ss_V1_0_resampled_to_CP.zarr"
ELEVATION = "inputs/elevation.zarr"
CALENDAR_DAYS = {"365_day": {365}, "noleap": {365}, "proleptic_gregorian": {365, 366},
                 "standard": {365, 366}, "gregorian": {365, 366}, "360_day": {360}}


class Ledger:
    """Cumulative transfer ledger for this task (SPEC section 10)."""

    def __init__(self, path: Path):
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else {"cap_bytes": CAP_BYTES, "events": []}

    @property
    def total(self) -> int:
        return sum(e["bytes"] for e in self.data["events"])

    def require(self, nbytes: int, what: str) -> None:
        if self.total + nbytes > CAP_BYTES:
            raise RuntimeError(f"Cap: {what} needs {nbytes} B; ledger {self.total} B of {CAP_BYTES}")

    def add(self, url: str, nbytes: int, kind: str) -> None:
        self.data["events"].append({"url": url, "bytes": int(nbytes), "kind": kind,
                                    "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        self.path.write_text(json.dumps(self.data, indent=1))


class CappedArchive(Archive):
    """Archive that reads every cache first and books each new download in the ledger."""

    def __init__(self, root: Path, ledger: Ledger, allow_download: bool):
        super().__init__(root)
        self.ledger, self.allow = ledger, allow_download
        self.read_only = [p.resolve() for p in READ_ONLY_CACHES]

    def cached(self, url: str) -> Path | None:
        name = hashlib.sha256(url.encode()).hexdigest() + ".bin"
        for folder in [self.cache, *self.read_only]:
            if (folder / name).exists():
                return folder / name
        return None

    def fetch(self, url: str) -> bytes:
        hit = self.cached(url)
        if hit is not None:
            data = hit.read_bytes()
            self.records[url] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                 "cache": str(hit.parent)}
            return data
        if not self.allow:
            raise RuntimeError(f"Download not allowed in this stage: {url}")
        size = head_size(url)
        self.ledger.require(size, url)
        data = super().fetch(url)
        self.ledger.add(url, len(data), "zarr-object")
        self.records[url]["cache"] = "validation"
        return data


def head_size(url: str) -> int:
    """Content-Length of a public object; refuses objects above the per-object cap."""
    r = requests.head(url, timeout=(20, 60))
    r.raise_for_status()
    size = int(r.headers["Content-Length"])
    if size > OBJECT_CAP:
        raise ValueError(f"Object exceeds 160 MB cap: {url} ({size} B)")
    return size


def chunk_urls(store: str, meta: dict, var: str, indices: list[np.ndarray]) -> list[str]:
    """Remote chunk URLs an orthogonal ``Archive.take`` would read."""
    m = meta[var + "/.zarray"]
    ids = [np.unique(ix // c) for ix, c in zip(indices, m["chunks"])]
    return [BASE + store + "/" + var + "/" + ".".join(map(str, ci)) for ci in itertools.product(*ids)]


def peak_rss_mb() -> float:
    info = psutil.Process().memory_info()
    return getattr(info, "peak_wset", info.rss) / 1e6


# --------------------------------------------------------------------------- geometry

def state_units() -> dict[str, tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]]:
    """IRT districts and blocks per state (make_valid), from the staging boundary files."""
    out = {}
    for state in STATES:
        where = f"state_name = '{state}'"
        d = gpd.read_file(DATA_ROOT / "districts_4326.geojson", where=where)
        b = gpd.read_file(DATA_ROOT / "blocks_4326.geojson", where=where)
        if d.empty or b.empty:
            raise ValueError(f"Boundaries missing for {state}")
        for f in (d, b):
            f["geometry"] = f.geometry.make_valid()
        out[state] = (d, b)
    return out


def stream_units(unions: dict, work: Path, ledger: Ledger) -> None:
    """Stream the geometry once; keep Indian regions and cities touching any state."""
    target = {s: prep(g) for s, g in unions.items()}
    ledger.require(GEOMETRY_BYTES, GEOMETRY_URL)
    digest, size, kept = hashlib.sha256(), 0, []
    with requests.get(GEOMETRY_URL, timeout=(20, 300), stream=True) as r:
        r.raise_for_status()
        pending = b""
        for chunk in r.iter_content(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            pending += chunk
            *lines, pending = pending.split(b"\n")
            for line in lines:
                is_region, is_city = b'"hierid": "IND' in line, b'"hierid": null' in line
                if not (is_region or is_city):
                    continue
                feature = json.loads(line.strip().rstrip(b","))
                geom = shape(feature["geometry"])
                hits = [s for s, t in target.items() if t.intersects(geom)]
                if not hits:
                    continue
                p = feature["properties"]
                if is_city:
                    name = p.get("UC_NM_MN") or p.get("NAMELSAD20") or str(p["processing_id"])
                    p["hierid"] = f"city:{name}"
                p["unit_type"] = "city" if is_city else "region"
                p["states"] = hits
                kept.append(feature)
        if pending.strip() and b'"hierid"' in pending:
            raise ValueError("Unparsed trailing geometry record")
    ledger.add(GEOMETRY_URL, size, "geometry-stream")
    if size != GEOMETRY_BYTES:
        raise ValueError(f"Geometry stream size {size} != {GEOMETRY_BYTES}")
    ids = [f["properties"]["processing_id"] for f in kept]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate processing_id among kept units")
    (work / "units.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": kept}))
    (work / "geometry_record.json").write_text(json.dumps(
        {"url": GEOMETRY_URL, "bytes": size, "sha256": digest.hexdigest(), "kept": len(kept),
         "stored": "filtered subset only (CHG-0639 declared exception)"}, indent=2))


def load_units(work: Path) -> list[dict]:
    return json.loads((work / "units.geojson").read_text())["features"]


# --------------------------------------------------------------------------- remote planning

def unit_bbox(units: list[dict]) -> tuple[float, float, float, float]:
    b = np.array([shape(f["geometry"]).bounds for f in units])
    return b[:, 0].min(), b[:, 1].min(), b[:, 2].max(), b[:, 3].max()


def grid_indices(archive: Archive, store: str, xname: str, yname: str, bounds) -> tuple:
    meta = archive.metadata(store)
    xs, ys = archive.coord(store, meta, xname), archive.coord(store, meta, yname)
    xmin, ymin, xmax, ymax = bounds
    xi = np.flatnonzero((xs + .125 > xmin) & (xs - .125 < xmax))
    yi = np.flatnonzero((ys + .125 > ymin) & (ys - .125 < ymax))
    return meta, xs, ys, xi, yi


def pid_positions(archive: Archive, store: str, pids: list[int]) -> tuple[dict, np.ndarray, list[int]]:
    meta = archive.metadata(store)
    ids = archive.coord(store, meta, "processing_id")
    pos, missing = [], []
    for p in pids:
        hit = np.flatnonzero(ids == p)
        if len(hit) > 1:
            raise ValueError(f"processing_id {p} duplicated in {store}")
        (pos.append(int(hit[0])) if len(hit) else missing.append(p))
    return meta, np.array(pos, dtype=int), missing


def store_dates(archive: Archive, store: str, meta: dict) -> pd.DatetimeIndex:
    attrs = meta["time/.zattrs"]
    if attrs["calendar"] not in ("gregorian", "proleptic_gregorian", "standard"):
        raise ValueError(f"Unexpected calendar in {store}")
    unit, origin = attrs["units"].split(" since ")
    if unit != "days":
        raise ValueError("Unexpected time unit")
    times = archive.coord(store, meta, "time")
    return (pd.Timestamp(origin) + pd.to_timedelta(times, unit="D")).normalize()


def remote_plan(archive: CappedArchive, units: list[dict]) -> list[dict]:
    """Every chunk the fetch stage reads, with cache status (metadata/coords fetched if needed)."""
    plan = []
    bounds = unit_bbox(units)
    for store, x, y, var in ((POPULATION, "x", "y", "population"), (ELEVATION, "lon", "lat", "elevation")):
        meta, _, _, xi, yi = grid_indices(archive, store, x, y, bounds)
        plan += [{"url": u, "use": var} for u in chunk_urls(store, meta, var, [yi, xi])]
    pids = [int(f["properties"]["processing_id"]) for f in units]
    meta, pos, _ = pid_positions(archive, UHE[0], pids)
    t = np.arange(meta["time/.zarray"]["shape"][0])
    plan += [{"url": u, "use": "uhe"} for u in chunk_urls(UHE[0], meta, UHE[1], [t, pos])]
    kerala = [int(f["properties"]["processing_id"]) for f in units if PUBLISHED_STATE in f["properties"]["states"]]
    for name, store in SHADE.items():
        meta, pos, _ = pid_positions(archive, store, kerala)
        gcm = np.flatnonzero(archive.coord(store, meta, "gcm") == PUBLISHED_MODEL)
        t = np.arange(meta["time/.zarray"]["shape"][0])
        plan += [{"url": u, "use": f"published {name}"} for u in chunk_urls(store, meta, "wbgt-shade", [gcm, t, pos])]
    for row in plan:
        row["cached"] = archive.cached(row["url"]) is not None
    return plan


# --------------------------------------------------------------------------- fetch

def unit_cells(archive: Archive, units: list[dict]) -> pd.DataFrame:
    """Area and population weights per unit on CarbonPlan's 0.25 deg grid, plus elevation."""
    bounds = unit_bbox(units)
    pmeta, pxs, pys, pxi, pyi = grid_indices(archive, POPULATION, "x", "y", bounds)
    pop = archive.take(POPULATION, pmeta, "population", [pyi, pxi])
    emeta, exs, eys, exi, eyi = grid_indices(archive, ELEVATION, "lon", "lat", bounds)
    elev = archive.take(ELEVATION, emeta, "elevation", [eyi, exi])
    project = Transformer.from_crs("EPSG:4326", EQUAL_AREA, always_xy=True).transform
    elev_cache: dict = {}
    rows = []
    for feature in units:
        p = feature["properties"]
        geom = shape(feature["geometry"]).buffer(0)
        region = transform(project, geom)
        xmin, ymin, xmax, ymax = geom.bounds
        for j, y in enumerate(pys[pyi]):
            if not (y + .125 > ymin and y - .125 < ymax):
                continue
            for i, x in enumerate(pxs[pxi]):
                if not (x + .125 > xmin and x - .125 < xmax):
                    continue
                area = transform(project, box(x - .125, y - .125, x + .125, y + .125)).intersection(region).area
                if area <= 0:
                    continue
                key = (float(y), float(x))
                if key not in elev_cache:
                    ix = np.flatnonzero(np.isclose(exs[exi], x))
                    iy = np.flatnonzero(np.isclose(eys[eyi], y))
                    if len(ix) != 1 or len(iy) != 1:
                        raise ValueError(f"Elevation grid mismatch at {key}")
                    elev_cache[key] = float(elev[iy[0], ix[0]])
                rows.append({"processing_id": int(p["processing_id"]), "hierid": p["hierid"],
                             "lat": float(y), "lon": float(x), "intersection_m2": area,
                             "population": float(pop[j, i]), "elevation_m": elev_cache[key]})
    cells = pd.DataFrame(rows)
    if (~np.isfinite(cells.population) | (cells.population < 0) | (cells.population > 1e15)).any():
        raise ValueError("Invalid population values")
    return cells


def weights(cells: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Notebook 05 order: drop null-elevation cells, THEN normalise area and pop x area."""
    null = ~np.isfinite(cells.elevation_m)
    dropped, kept = cells[null].copy(), cells[~null].copy()
    g = kept.groupby("processing_id")
    kept["area_weight"] = kept.intersection_m2 / g.intersection_m2.transform("sum")
    product = kept.area_weight * kept.population
    psum = product.groupby(kept.processing_id).transform("sum")
    kept["population_weight"] = np.where(psum > 0, product / psum.where(psum > 0), np.nan)
    return kept, dropped


def batch_series(archive: Archive, store: str, var: str, pids: list[int], window: tuple[int, int],
                 gcm: str | None = None) -> tuple[pd.DataFrame, list[int]]:
    """Extract many units at once by value (each chunk decoded once); NaN kept as NaN."""
    meta, pos, missing = pid_positions(archive, store, pids)
    present = [p for p in pids if p not in missing]
    dates = store_dates(archive, store, meta)
    keep = np.flatnonzero((dates.year >= window[0]) & (dates.year <= window[1]))
    dims = meta[var + "/.zattrs"]["_ARRAY_DIMENSIONS"]
    index = {"processing_id": pos, "time": keep}
    if "gcm" in dims:
        g = np.flatnonzero(archive.coord(store, meta, "gcm") == gcm)
        if len(g) != 1:
            raise ValueError(f"{gcm} not unique in {store}")
        index["gcm"] = g
    values = archive.take(store, meta, var, [index[d] for d in dims])
    values = values.reshape([len(index[d]) for d in dims])
    order = [dims.index("time"), dims.index("processing_id")]
    values = np.moveaxis(values, order, [0, 1]).reshape(len(keep), len(present))
    frame = pd.DataFrame(values, index=dates[keep], columns=present)
    if not frame.index.is_unique:
        raise ValueError(f"Duplicate dates in {store}")
    return frame, missing


def save_frame(path: Path, frame: pd.DataFrame) -> None:
    """dates x processing_id frame as npz (the pinned py3.10 env has no parquet engine)."""
    np.savez_compressed(path, values=frame.to_numpy(), processing_id=np.array(frame.columns, dtype=int),
                        dates=frame.index.strftime("%Y-%m-%d").to_numpy().astype("U10"))


# --------------------------------------------------------------------------- drivers

def nex_path(scenario: str, var: str, model: str, year: int) -> Path:
    return NEX_ROOT / scenario / var / model / f"{year}.nc"


def drivers_for(model: str, grid: pd.DataFrame, scenario: str, years: tuple[int, int],
                reference_grid: dict) -> tuple[dict, list[dict], dict]:
    """Inventory then extract tas/tasmax/huss at ``grid`` cells; never repairs a defect."""
    data = {v: [] for v in DRIVERS}
    stamps, records, calendar = [], [], None
    issues = []
    for year in range(years[0], years[1] + 1):
        stamp = None
        for var, units in DRIVERS.items():
            path = nex_path(scenario, var, model, year)
            rec = {"model": model, "scenario": scenario, "var": var, "year": year, "path": str(path)}
            if not path.exists():
                rec["issue"] = "missing file"
                issues.append(rec)
                records.append(rec)
                continue
            with xr.open_dataset(path, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)) as ds:
                cal = ds.time.encoding.get("calendar", ds.time.attrs.get("calendar"))
                times = ds.indexes["time"]
                rec.update({"bytes": path.stat().st_size, "calendar": cal, "ndays": len(times),
                            "units": ds[var].attrs.get("units"),
                            "variant_label": ds.attrs.get("variant_label"),
                            "version": ds.attrs.get("version"),
                            "duplicates": int(pd.Index(times).duplicated().sum())})
                problems = []
                if rec["units"] not in units:
                    problems.append(f"units {rec['units']}")
                if cal not in CALENDAR_DAYS or len(times) not in CALENDAR_DAYS[cal]:
                    problems.append(f"calendar/ndays {cal}/{len(times)}")
                days = [(t.year, t.month, t.day) for t in times]
                if rec["duplicates"] or len(set(days)) != len(days):
                    problems.append("duplicate dates")
                if {t.year for t in times} != {year}:
                    problems.append("dates outside file year")
                expected = cftime_range(year, cal)
                if days != expected:
                    problems.append("dates incomplete or out of order")
                for axis in ("lat", "lon"):
                    if not np.array_equal(ds[axis].values, reference_grid[axis]):
                        problems.append(f"{axis} grid differs")
                a = ds[var].sel(lat=xr.DataArray(grid.lat.values, dims="cell"),
                                lon=xr.DataArray(grid.lon.values, dims="cell")).transpose("time", "cell").values
                rec["nonfinite_at_cells"] = int((~np.isfinite(a)).sum())
                if rec["nonfinite_at_cells"]:
                    problems.append("non-finite values at used cells")
                if stamp is not None and list(times) != list(stamp):
                    problems.append("timestamps differ across variables")
                stamp = times
                calendar = calendar or cal
                if cal != calendar:
                    problems.append("calendar changes across files")
                rec["sha256_selected"] = hashlib.sha256(np.ascontiguousarray(a).tobytes()).hexdigest()
                if problems:
                    rec["issue"] = "; ".join(problems)
                    issues.append(rec)
                data[var].append(a)
            records.append(rec)
        if stamp is not None:
            stamps.extend(t.strftime("%Y-%m-%d %H:%M:%S") for t in stamp)
    if issues:
        return {}, records, {"eligible": False, "issues": issues, "calendar": calendar}
    arrays = {v: np.concatenate(a).astype("float32") for v, a in data.items()}
    arrays["time"] = np.array(stamps)
    return arrays, records, {"eligible": True, "issues": [], "calendar": calendar}


def cftime_range(year: int, calendar: str) -> list[tuple[int, int, int]]:
    """Expected (y, m, d) for a full year in ``calendar``."""
    cal = {"365_day": "noleap", "gregorian": "standard"}.get(calendar, calendar)
    start = cftime.datetime(year, 1, 1, calendar=cal)
    out, t = [], start
    while t.year == year:
        out.append((t.year, t.month, t.day))
        t = t + pd.Timedelta(days=1).to_pytimedelta()
    return out


# --------------------------------------------------------------------------- crosswalk

def crosswalk(state: str, districts: gpd.GeoDataFrame, blocks: gpd.GeoDataFrame,
              units: list[dict]) -> tuple[pd.DataFrame, dict]:
    """Intersection of each IRT unit with each CarbonPlan unit, plus overlap diagnostics."""
    reg = gpd.GeoDataFrame(
        [{"processing_id": int(f["properties"]["processing_id"]), "hierid": f["properties"]["hierid"],
          "unit_type": f["properties"]["unit_type"]} for f in units],
        geometry=[shape(f["geometry"]).buffer(0) for f in units], crs="EPSG:4326").to_crs(EQUAL_AREA)
    sindex = reg.sindex
    rows, overlaps = [], []
    # district_lgd_code is NOT unique in Rajasthan (2023 districts inherit the parent's code),
    # so districts are keyed by state::district_name; blocks by their own block_key.
    districts = districts.assign(district_key=state + "::" + districts.district_name)
    lgd_dups = int(districts.district_lgd_code.duplicated(keep=False).sum())
    for level, frame, key in (("district", districts, "district_key"), ("block", blocks, "block_key")):
        if frame[key].duplicated().any():
            raise ValueError(f"{state} {level}: duplicate admin key {key}")
        proj = frame.to_crs(EQUAL_AREA)
        for unit in proj.itertuples():
            g = unit.geometry
            hits = reg.iloc[sindex.query(g, predicate="intersects")]
            parts = []
            for r in hits.itertuples():
                inter = g.intersection(r.geometry)
                if inter.area > 0:
                    parts.append(inter)
                    rows.append({"state": state, "level": level, "admin_key": str(getattr(unit, key)),
                                 "district": unit.district_name,
                                 "block": getattr(unit, "block_name", "") if level == "block" else "",
                                 "processing_id": r.processing_id, "hierid": r.hierid,
                                 "unit_type": r.unit_type, "intersection_m2": inter.area,
                                 "unit_area_m2": g.area})
            if not parts:
                rows.append({"state": state, "level": level, "admin_key": str(getattr(unit, key)),
                             "district": unit.district_name,
                             "block": getattr(unit, "block_name", "") if level == "block" else "",
                             "processing_id": -1, "hierid": None, "unit_type": None,
                             "intersection_m2": 0.0, "unit_area_m2": g.area})
                continue
            summed = sum(p.area for p in parts)
            union = unary_union(parts).area
            overlaps.append({"level": level, "admin_key": str(getattr(unit, key)),
                             "double_counted_fraction": (summed - union) / g.area})
    ov = pd.DataFrame(overlaps)
    return pd.DataFrame(rows), {"max_double_counted_fraction": float(ov.double_counted_fraction.max()),
                                "units_over_0.001": int((ov.double_counted_fraction > 1e-3).sum()),
                                "districts_sharing_lgd_code": lgd_dups}


# --------------------------------------------------------------------------- main

def main() -> None:
    """Run the requested stages, recording provenance, time and peak memory."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stage", choices=("geometry", "plan", "fetch", "drivers", "crosswalk"))
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--states", nargs="+", default=list(STATES), choices=list(STATES))
    parser.add_argument("--models", nargs="+", default=list(MODELS))
    args = parser.parse_args()
    work = args.work_dir.resolve()
    if "scratch" not in work.parts:
        raise ValueError("Work dir must be under scratch")
    work.mkdir(parents=True, exist_ok=True)
    ledger = Ledger(work / "transfer_ledger.json")
    started = time.time()
    log: dict = {"stage": args.stage, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    if args.stage == "geometry":
        if (work / "units.geojson").exists():
            print("units.geojson exists; geometry is not re-streamed", flush=True)
        else:
            units = state_units()
            unions = {s: unary_union(d.geometry.values) for s, (d, _) in units.items()}
            stream_units(unions, work, ledger)
        feats = load_units(work)
        for s in STATES:
            sel = [f for f in feats if s in f["properties"]["states"]]
            print(f"{s}: {len(sel)} units ({sum(f['properties']['unit_type'] == 'city' for f in sel)} cities)")

    elif args.stage == "plan":
        archive = CappedArchive(work, ledger, allow_download=True)  # metadata/coords only
        plan = remote_plan(archive, load_units(work))
        todo = [r for r in plan if not r["cached"]]
        for r in todo:
            r["bytes"] = head_size(r["url"])
        frame = pd.DataFrame(plan)
        if "bytes" not in frame:
            frame["bytes"] = np.nan
        frame.to_csv(work / "remote_plan.csv", index=False)
        need = int(frame.bytes.fillna(0).sum())
        summary = frame.groupby(["use", "cached"]).agg(chunks=("url", "size"), bytes=("bytes", "sum"))
        print(summary.to_string())
        print(f"ledger so far {ledger.total / 1e6:.1f} MB; planned new {need / 1e6:.1f} MB; "
              f"cap {CAP_BYTES / 1e6:.0f} MB; after plan {(ledger.total + need) / 1e6:.1f} MB")
        log.update({"planned_new_bytes": need, "ledger_bytes": ledger.total})

    elif args.stage == "fetch":
        archive = CappedArchive(work, ledger, allow_download=True)
        units = load_units(work)
        cells = unit_cells(archive, units)
        kept, dropped = weights(cells)
        kept.to_csv(work / "cells.csv", index=False)
        dropped.to_csv(work / "cells_dropped_null_elevation.csv", index=False)
        pids = [int(f["properties"]["processing_id"]) for f in units]
        uhe, missing = batch_series(archive, *UHE, pids, (1985, 2014))
        save_frame(work / "uhe_1985_2014.npz", uhe)
        cover = pd.DataFrame({"processing_id": uhe.columns, "finite_days": np.isfinite(uhe.values).sum(0),
                              "expected_days": len(uhe)})
        cover = pd.concat([cover, pd.DataFrame({"processing_id": missing, "finite_days": 0,
                                                "expected_days": len(uhe), "absent_from_store": True})])
        cover.to_csv(work / "uhe_coverage.csv", index=False)
        kerala = [int(f["properties"]["processing_id"]) for f in units
                  if PUBLISHED_STATE in f["properties"]["states"]]
        for name, store in SHADE.items():
            pub, miss = batch_series(archive, store, "wbgt-shade", kerala, SHADE_WINDOWS[name], PUBLISHED_MODEL)
            save_frame(work / f"published_{name}.npz", pub)
            log[f"published_{name}_missing"] = miss
        (work / "fetch_records.json").write_text(json.dumps(archive.records, indent=1))
        log.update({"cells": len(kept), "null_elevation_rows": len(dropped), "uhe_missing": missing,
                    "ledger_bytes": ledger.total})

    elif args.stage == "drivers":
        cells = pd.read_csv(work / "cells.csv")
        grid = cells[["lat", "lon"]].drop_duplicates().sort_values(["lat", "lon"]).reset_index(drop=True)
        with xr.open_dataset(nex_path("historical", "tas", "ACCESS-CM2", 1985)) as ref:
            reference_grid = {"lat": ref.lat.values, "lon": ref.lon.values}
        aw_cells = cells[cells.processing_id.isin(AW_UNITS)][["lat", "lon"]].drop_duplicates()
        aw_cells = aw_cells.sort_values(["lat", "lon"]).reset_index(drop=True)
        inventory, status = [], {}
        jobs = [(m, s, y, grid, f"{m}_{s}_{y[0]}_{y[1]}") for m in args.models for s, y in PERIODS.items()]
        if "ACCESS-CM2" in args.models:
            jobs.append(("ACCESS-CM2", AW_EXTRA[0], AW_EXTRA[1], aw_cells,
                         f"ACCESS-CM2_{AW_EXTRA[0]}_{AW_EXTRA[1][0]}_{AW_EXTRA[1][1]}_aw"))
        for model, scenario, years, g, tag in jobs:
            t0 = time.time()
            arrays, recs, st = drivers_for(model, g, scenario, years, reference_grid)
            inventory += recs
            st["seconds"] = round(time.time() - t0, 1)
            status[tag] = st
            if arrays:
                arrays["lat"], arrays["lon"] = g.lat.values, g.lon.values
                arrays["calendar"] = np.array(st["calendar"])
                np.savez_compressed(work / f"drivers_{tag}.npz", **arrays)
            print(tag, "eligible" if st["eligible"] else f"INELIGIBLE {st['issues'][:3]}",
                  f"{st['seconds']}s", flush=True)
        pd.DataFrame(inventory).to_csv(work / "driver_inventory.csv", index=False)
        (work / "driver_status.json").write_text(json.dumps(status, indent=1, default=str))

    elif args.stage == "crosswalk":
        units = load_units(work)
        frames, checks = [], {}
        for state, (d, b) in state_units().items():
            if state not in args.states:
                continue
            sel = [f for f in units if state in f["properties"]["states"]]
            cw, ov = crosswalk(state, d, b, sel)
            frames.append(cw)
            checks[state] = ov
            print(state, ov, flush=True)
        pd.concat(frames).to_csv(work / "crosswalk.csv", index=False)
        (work / "crosswalk_checks.json").write_text(json.dumps(checks, indent=1))

    log.update({"seconds": round(time.time() - started, 1), "peak_rss_mb": round(peak_rss_mb(), 1),
                "ledger_total_bytes": ledger.total})
    with open(work / "prepare_log.jsonl", "a") as fh:
        fh.write(json.dumps(log, default=str) + "\n")
    print(json.dumps(log, default=str), flush=True)


if __name__ == "__main__":
    main()
