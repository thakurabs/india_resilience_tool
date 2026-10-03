"""Extract bounded per-city inputs for an independently scored CarbonPlan reproduction.

Run in IRT's existing geo environment. Downloads are public CarbonPlan inputs
(CC BY 4.0), cached by URL under the work-dir root and shared across cities, and
recorded with SHA-256. No source files are edited.

Cities are Kochi, Bikaner and Shimla; radiation years default to 2005, 2007 and
2009, the years for which local ACCESS-CM2 rsds and sfcWind exist. The shade
calibration window is fixed at 1985-2014 to match CarbonPlan's notebook 06.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import xarray as xr
from numcodecs import get_codec
from pyproj import Transformer
from shapely.geometry import box, shape
from shapely.ops import transform

BASE = "https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0/"
WORK = Path("scratch/carbonplan_reproduction")
GEOMETRY_DIR = Path("docs/diagnostics/wbgt_outdoor_carbonplan_multicity/results")
CLIMATE_ROOT = Path("D:/projects/irt_data/r1i1p1f1/historical")
OUTDOOR_ROOT = Path("D:/projects/irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1/historical")

# CarbonPlan published processing IDs, verified unique by value in every store.
CITIES = {"Kochi": 7886, "Bikaner": 6558, "Shimla": 6822}
CALIBRATION = (1985, 2014)
RADIATION_YEARS = (2005, 2007, 2009)
SHADE_VARS = ("tas", "tasmax", "huss")
RADIATION_VARS = ("rsds", "sfcWind")
UNITS = {"tas": "K", "tasmax": "K", "huss": "kg kg-1", "rsds": "W m-2", "sfcWind": "m s-1"}


class Archive:
    """Read only requested Zarr v2 chunks, with bounded transfers and provenance."""

    def __init__(self, root: Path):
        self.cache = root / "cache"
        self.cache.mkdir(parents=True, exist_ok=True)
        self.records: dict[str, dict] = {}

    def fetch(self, url: str) -> bytes:
        """Cache a public object; refuse unexpectedly large individual responses."""
        path = self.cache / (hashlib.sha256(url.encode()).hexdigest() + ".bin")
        if not path.exists():
            print("Downloading", url, flush=True)
            with requests.get(url, timeout=(20, 180), stream=True) as r:
                r.raise_for_status()
                chunks, size = [], 0
                for block in r.iter_content(1024 * 1024):
                    size += len(block)
                    if size > 160_000_000:
                        raise ValueError(f"Object exceeds 160 MB cap: {url}")
                    chunks.append(block)
            path.write_bytes(b"".join(chunks))
        data = path.read_bytes()
        self.records[url] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        return data

    def metadata(self, store: str) -> dict:
        """Read consolidated metadata for a specified public store."""
        return json.loads(self.fetch(BASE + store + "/.zmetadata"))["metadata"]

    def take(self, store: str, meta: dict, var: str, indices: list[np.ndarray]) -> np.ndarray:
        """Read an orthogonal subset without downloading unrelated variable chunks."""
        m = meta[var + "/.zarray"]
        if m.get("filters"):
            raise ValueError("Filtered Zarr arrays require an explicit decoder")
        if len(indices) != len(m["shape"]):
            raise ValueError("Dimension mismatch")
        for ix, size in zip(indices, m["shape"]):
            if np.any(ix < 0) or np.any(ix >= size):
                raise IndexError("Zarr subset outside shape")
        out = np.empty(tuple(len(x) for x in indices), dtype=m["dtype"])
        chunk_ids = [np.unique(ix // c) for ix, c in zip(indices, m["chunks"])]
        for ci in itertools.product(*chunk_ids):
            key = ".".join(map(str, ci))
            payload = self.fetch(BASE + store + "/" + var + "/" + key)
            raw = get_codec(m["compressor"]).decode(payload) if m["compressor"] else payload
            data = np.frombuffer(raw, dtype=m["dtype"]).reshape(m["chunks"], order=m["order"])
            positions = [np.flatnonzero(ix // c == k) for ix, c, k in zip(indices, m["chunks"], ci)]
            local = [ix[pos] % c for ix, pos, c in zip(indices, positions, m["chunks"])]
            out[np.ix_(*positions)] = data[np.ix_(*local)]
        return out

    def coord(self, store: str, meta: dict, name: str) -> np.ndarray:
        """Read a complete small coordinate vector."""
        return self.take(store, meta, name, [np.arange(meta[name + "/.zarray"]["shape"][0])])

    def city_series(self, store: str, var: str, city: str, processing_id: int) -> pd.Series:
        """Extract one city by coordinate value, selecting ACCESS-CM2 when a GCM axis exists.

        Store processing_id axes differ in length between the UHE reference and the
        released products, so the ID is always resolved by value, never by position.
        """
        meta = self.metadata(store)
        ids = self.coord(store, meta, "processing_id")
        found = np.flatnonzero(ids == processing_id)
        if len(found) != 1:
            raise ValueError(f"{city} processing_id {processing_id} not unique in {store}: {len(found)} matches")
        dims = meta[var + "/.zattrs"]["_ARRAY_DIMENSIONS"]
        times = self.coord(store, meta, "time")
        attrs = meta["time/.zattrs"]
        if attrs["calendar"] not in ("gregorian", "proleptic_gregorian", "standard"):
            raise ValueError("Unexpected released calendar")
        unit, origin = attrs["units"].split(" since ")
        if unit != "days":
            raise ValueError("Unexpected time unit")
        dates = (pd.Timestamp(origin) + pd.to_timedelta(times, unit="D")).normalize()
        index = {"processing_id": found, "time": np.arange(len(times))}
        if "gcm" in dims:
            gcm = np.flatnonzero(self.coord(store, meta, "gcm") == "ACCESS-CM2")
            if len(gcm) != 1:
                raise ValueError("ACCESS-CM2 not unique")
            index["gcm"] = gcm
        values = self.take(store, meta, var, [index[d] for d in dims]).squeeze()
        if values.shape != (len(dates),) or not dates.is_unique:
            raise ValueError("Invalid extracted series")
        return pd.Series(values, index=dates, name=var)


def geometry_path(city: str) -> Path:
    """Locate the verified published polygon retained by the predecessor comparison."""
    path = GEOMETRY_DIR / f"{city}_geometry.geojson"
    if not path.is_file():
        raise FileNotFoundError(f"Published city polygon missing: {path}")
    return path


def weights(archive: Archive, city: str, processing_id: int) -> pd.DataFrame:
    """Match CarbonPlan's intersection-area times resampled-population weights."""
    path = geometry_path(city)
    obj = json.loads(path.read_text())
    feature = obj["features"][0] if obj.get("type") == "FeatureCollection" else obj
    declared = feature.get("properties", {}).get("processing_id")
    if declared is not None and int(declared) != processing_id:
        raise ValueError(f"{city} polygon declares processing_id {declared}, expected {processing_id}")
    geom = shape(feature.get("geometry", feature))
    store = "inputs/GHS_POP_E2030_GLOBE_R2023A_4326_30ss_V1_0_resampled_to_CP.zarr"
    meta = archive.metadata(store)
    xs, ys = archive.coord(store, meta, "x"), archive.coord(store, meta, "y")
    xmin, ymin, xmax, ymax = geom.bounds
    xi = np.flatnonzero((xs + .125 > xmin) & (xs - .125 < xmax))
    yi = np.flatnonzero((ys + .125 > ymin) & (ys - .125 < ymax))
    pop = archive.take(store, meta, "population", [yi, xi])
    projection = Transformer.from_crs("EPSG:4326", "ESRI:53034", always_xy=True).transform
    region = transform(projection, geom)
    rows = []
    for j, y in enumerate(ys[yi]):
        for i, x in enumerate(xs[xi]):
            cell = transform(projection, box(x-.125, y-.125, x+.125, y+.125))
            area = cell.intersection(region).area
            if area > 0:
                rows.append({"lat": y, "lon": x, "intersection_m2": area, "population": pop[j, i]})
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError(f"{city} polygon intersects no population cell")
    # The store's fill_value is 1.8e308, which is finite, so an unwritten chunk
    # passes an isfinite check and must be caught by magnitude instead.
    if not np.isfinite(frame.population).all() or (frame.population < 0).any() or (frame.population > 1e15).any():
        raise ValueError(f"Invalid population over {city}")
    frame["area_weight"] = frame.intersection_m2 / frame.intersection_m2.sum()
    frame["weight"] = frame.area_weight * frame.population
    total = frame.weight.sum()
    if not np.isfinite(total) or total <= 0:
        raise ValueError(f"{city} has no positive population weight; cannot form CarbonPlan weights")
    frame["weight"] /= total
    estore = "inputs/elevation.zarr"
    emeta = archive.metadata(estore)
    ex, ey = archive.coord(estore, emeta, "lon"), archive.coord(estore, emeta, "lat")
    elevation = []
    for row in frame.itertuples():
        ix, iy = np.flatnonzero(np.isclose(ex, row.lon)), np.flatnonzero(np.isclose(ey, row.lat))
        if len(ix) != 1 or len(iy) != 1:
            raise ValueError("Elevation grid mismatch")
        elevation.append(archive.take(estore, emeta, "elevation", [iy, ix]).item())
    frame["elevation_m"] = elevation
    if not np.isfinite(frame.elevation_m).all():
        raise ValueError("Missing elevation")
    return frame


def local_inputs(cells: pd.DataFrame, city_dir: Path, radiation_years: tuple[int, ...]) -> list[dict]:
    """Extract calibration shade drivers and per-year radiation/wind; preserve source identities."""
    records, frames = [], []
    for year in range(CALIBRATION[0], CALIBRATION[1] + 1):
        wanted = SHADE_VARS + (RADIATION_VARS if year in radiation_years else ())
        data: dict[str, np.ndarray] = {}
        dates = None
        for var in wanted:
            root = OUTDOOR_ROOT if var in RADIATION_VARS else CLIMATE_ROOT
            path = root / var / "ACCESS-CM2" / f"{year}.nc"
            with xr.open_dataset(path) as ds:
                expected = UNITS[var]
                accepted = {expected, "1"} if var == "huss" else {expected}
                if ds[var].attrs.get("units") not in accepted:
                    raise ValueError(f"Unexpected units: {path}: {ds[var].attrs}")
                a = ds[var].sel(lat=xr.DataArray(cells.lat.values, dims="cell"),
                                lon=xr.DataArray(cells.lon.values, dims="cell"))
                a = a.transpose("time", "cell")
                t = pd.DatetimeIndex(a.time.values)
                target = pd.date_range(f"{year}-01-01", f"{year}-12-31")
                if not t.normalize().equals(target) or (dates is not None and not t.equals(dates)):
                    raise ValueError(f"Dates not aligned: {path}")
                dates = t
                values = a.values
                if not np.isfinite(values).all():
                    raise ValueError(f"Missing driver: {path}")
                records.append({"path": str(path), "bytes": path.stat().st_size, "dtype": str(values.dtype),
                                "selected_values_sha256": hashlib.sha256(values.tobytes()).hexdigest(),
                                "time_encoding": dict(ds.time.encoding),
                                "first_timestamp": str(t[0]), "last_timestamp": str(t[-1]),
                                "variable_attrs": dict(ds[var].attrs), "dataset_attrs": dict(ds.attrs)})
                data[var] = values
        for i in range(len(cells)):
            frame = pd.DataFrame({v: a[:, i] for v, a in data.items()}, index=dates)
            frame.index.name = "date"
            frame["cell"] = i
            frames.append(frame.reset_index())
        print("  extracted", year, flush=True)
    pd.concat(frames, ignore_index=True).to_csv(city_dir / "local_drivers.csv", index=False)
    return records


def prepare_city(archive: Archive, city: str, root: Path, radiation_years: tuple[int, ...]) -> None:
    """Write one city's weights, published series and local drivers under the work root."""
    processing_id = CITIES[city]
    city_dir = root / "cities" / city
    city_dir.mkdir(parents=True, exist_ok=True)
    print(f"=== {city} (processing_id {processing_id}) ===", flush=True)
    archive.records = {}
    cells = weights(archive, city, processing_id)
    cells.to_csv(city_dir / "cells.csv", index=False)
    print(cells.to_string(index=False), flush=True)
    for name, store, var in [
        ("reference", "inputs/wbgt-UHE-daily-historical.zarr", "WBGT"),
        ("published_shade", "outputs/zarr/daily/historical-WBGT-shade.zarr", "wbgt-shade"),
        ("published_sun", "outputs/zarr/daily/historical-WBGT-sun.zarr", "wbgt-sun"),
    ]:
        series = archive.city_series(store, var, city, processing_id)
        series.rename(name).rename_axis("date").to_csv(city_dir / (name + ".csv"))
        print(" ", name, len(series), flush=True)
    records = local_inputs(cells, city_dir, radiation_years)
    manifest = {"city": city, "processing_id": processing_id, "model": "ACCESS-CM2",
                "calibration_years": list(CALIBRATION), "radiation_years": list(radiation_years),
                "remote_sources": archive.records, "local_sources": records,
                "geometry_path": str(geometry_path(city)),
                "geometry_sha256": hashlib.sha256(geometry_path(city).read_bytes()).hexdigest(),
                "data_license": "CarbonPlan CC BY 4.0",
                "input_lineage": "Local NEX versions not assumed identical to CarbonPlan's 2023 inputs"}
    (city_dir / "input_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))


def main() -> None:
    """Download/extract bounded inputs per city and save auditable, reusable manifests."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--cities", nargs="+", default=sorted(CITIES), choices=sorted(CITIES))
    parser.add_argument("--radiation-years", nargs="+", type=int, default=list(RADIATION_YEARS))
    args = parser.parse_args()
    root = args.work_dir.resolve()
    if "scratch" not in root.parts:
        raise ValueError("Inputs must be written into scratch")
    years = tuple(sorted(set(args.radiation_years)))
    if not years or min(years) < CALIBRATION[0] or max(years) > CALIBRATION[1]:
        raise ValueError(f"Radiation years must fall inside the calibration window {CALIBRATION}")
    root.mkdir(parents=True, exist_ok=True)
    archive = Archive(root)
    for city in args.cities:
        prepare_city(archive, city, root, years)


if __name__ == "__main__":
    main()
