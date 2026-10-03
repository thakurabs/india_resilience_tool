"""Reproduce the bounded CarbonPlan published WBGT versus IRT W1 city comparison.

Fixed design: ACCESS-CM2 historical; 2005, 2007 and 2009; five contrasting cities.
Only required CarbonPlan Zarr location chunks are requested. W1 uses the frozen outdoor
pilot physics, clip100 RH policy, GMTED cell-mean elevation and city-polygon area weights.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import xarray as xr
from numcodecs import get_codec
from pyproj import Transformer
from shapely.geometry import box, shape
from shapely.ops import transform

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
import tools.diagnostics.wbgt_outdoor_pilot as pilot
import tools.diagnostics.wbgt_outdoor_pilot_qc as qc
from india_resilience_tool.compute.gridfirst_spatial import normalize_lat_lon

BASE = "https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0"
ZARR = BASE + "/outputs/zarr/daily/historical-WBGT-sun.zarr/"
GEO = BASE + "/inputs/all_regions_and_cities.json"
OUT = Path("scratch/wbgt_carbonplan_multicity_results")
WORK = Path("scratch/wbgt_carbonplan_multicity")
YEARS = (2005, 2007, 2009)
MODEL = "ACCESS-CM2"
CITY_NAMES = ("Kochi", "Bikaner", "Shimla", "Hyderabad", "Kolkata")
THRESHOLDS = (28, 30, 32)
PILOT_CACHE = Path("scratch/wbgt_outdoor_pilot_qc/cells_Kerala_ACCESS-CM2_2005_combined.nc")
TILES = tuple(Path("scratch/wbgt_outdoor_pilot_qc/elevation") /
              f"{tile}_20101117_gmted_mea300.tif" for tile in qc.GMTED_TILES)


def fetch(url: str, *, byte_limit: int | None = None, cache_dir: Path | None = None) -> bytes:
    """Read a bounded public object, reusing its isolated URL-keyed cache."""
    cache_path = cache_dir / (hashlib.sha256(url.encode()).hexdigest() + ".bin") if cache_dir else None
    if cache_path is not None and cache_path.is_file():
        payload = cache_path.read_bytes()
        if not byte_limit or len(payload) <= byte_limit:
            return payload
    headers = {"Range": f"bytes=0-{byte_limit-1}"} if byte_limit else {}
    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()
    payload = response.content
    if byte_limit and len(payload) > byte_limit:
        raise ValueError(f"Server ignored bounded request for {url}")
    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(payload)
    return payload


def read_geography(cache_dir: Path) -> tuple[dict[str, dict], bytes]:
    """Resolve five named Indian city polygons by name and coordinate bounds."""
    payload = fetch(GEO, byte_limit=16_777_216, cache_dir=cache_dir)
    text = payload.decode("utf-8")
    decoder = json.JSONDecoder()
    features: dict[str, dict] = {}
    expected_centres = {"Kochi": (76.3, 10.0), "Bikaner": (73.3, 28.0),
                        "Shimla": (77.2, 31.1), "Hyderabad": (78.5, 17.4),
                        "Kolkata": (88.4, 22.6)}
    for name, (x, y) in expected_centres.items():
        marker = '"UC_NM_MN": "' + name + '"'
        position = 0
        while True:
            hit = text.find(marker, position)
            if hit < 0:
                break
            start = text.rfind('{ "type": "Feature"', 0, hit)
            feature, _ = decoder.raw_decode(text[start:])
            centre = shape(feature["geometry"]).centroid
            if abs(centre.x-x) < 1 and abs(centre.y-y) < 1:
                if name in features:
                    raise ValueError(f"Duplicate geographically matched city: {name}")
                features[name] = feature
            position = hit + len(marker)
    missing = set(CITY_NAMES) - features.keys()
    if missing:
        raise ValueError(f"Cities absent from bounded CarbonPlan geography: {sorted(missing)}")
    return features, payload


def coordinate(metadata: dict, name: str, dtype: str, cache_dir: Path) -> tuple[np.ndarray, bytes]:
    """Decode all chunks of one published one-dimensional coordinate."""
    meta = metadata[f"{name}/.zarray"]
    chunks = int(meta["shape"][0] / meta["chunks"][0] +
                 (meta["shape"][0] % meta["chunks"][0] != 0))
    parts, payloads = [], []
    for chunk in range(chunks):
        payload = fetch(ZARR + f"{name}/{chunk}", cache_dir=cache_dir)
        raw = get_codec(meta["compressor"]).decode(payload)
        parts.append(np.frombuffer(raw, dtype=dtype))
        payloads.append(payload)
    return np.concatenate(parts)[:meta["shape"][0]], b"".join(payloads)


def get_carbonplan(features: dict[str, dict], work: Path) -> tuple[pd.DataFrame, dict]:
    """Extract the selected published daily series and retain source identities."""
    cache_dir = work / "source_cache"
    metadata_payload = fetch(ZARR + ".zmetadata", cache_dir=cache_dir)
    metadata = json.loads(metadata_payload)["metadata"]
    if metadata["time/.zattrs"].get("units") != "days since 1985-01-01T12:00:00":
        raise ValueError("CarbonPlan time axis changed")
    if metadata["wbgt-sun/.zattrs"].get("units") != "degC":
        raise ValueError("CarbonPlan WBGT units changed")
    if metadata["wbgt-sun/.zarray"].get("dtype") != "<f8":
        raise ValueError("CarbonPlan daily dtype changed")
    gcms, gcm_payload = coordinate(metadata, "gcm", "<U16", cache_dir)
    pids, pid_payload = coordinate(metadata, "processing_id", "<i8", cache_dir)
    times, time_payload = coordinate(metadata, "time", "<i8", cache_dir)
    model_ix = np.flatnonzero(gcms == MODEL)
    if len(model_ix) != 1:
        raise ValueError(f"Expected one {MODEL} coordinate, found {len(model_ix)}")
    ids = {}
    for city, feature in features.items():
        pid = int(feature["properties"]["processing_id"])
        matches = np.flatnonzero(pids == pid)
        if len(matches) != 1:
            raise ValueError(f"{city}: processing_id {pid} not unique on published axis")
        ids[city] = int(matches[0])
    zmeta = metadata["wbgt-sun/.zarray"]
    if zmeta["chunks"] != [1, 850, 10957]:
        raise ValueError("Published chunk layout changed")
    location_chunk = int(zmeta["chunks"][1])
    series = []
    chunks: dict[int, tuple[bytes, np.ndarray]] = {}
    for city, position in ids.items():
        chunk_no, local = divmod(position, location_chunk)
        if chunk_no not in chunks:
            url = ZARR + f"wbgt-sun/{int(model_ix[0])}.{chunk_no}.0"
            print("Reading CarbonPlan chunk", chunk_no, flush=True)
            payload = fetch(url, cache_dir=cache_dir)
            raw = get_codec(zmeta["compressor"]).decode(payload)
            block = np.frombuffer(raw, dtype="<f8").reshape(location_chunk, len(times))
            chunks[chunk_no] = (payload, block)
        payload, block = chunks[chunk_no]
        values = block[local].copy()
        dates = (pd.Timestamp("1985-01-01 12:00:00") +
                 pd.to_timedelta(times, unit="D")).date.astype(str)
        for year in YEARS:
            mask = np.char.startswith(dates.astype(str), str(year))
            target_dates = dates[mask]
            target_values = values[mask]
            expected = len(pd.date_range(f"{year}-01-01", f"{year}-12-31"))
            if len(target_values) != expected or not np.isfinite(target_values).all():
                raise ValueError(f"{city} CarbonPlan {year}: incomplete or invalid dates")
            series.extend({"city": city, "year": year, "date": date,
                           "carbonplan_wbgt_c": float(value)}
                          for date, value in zip(target_dates, target_values))
    chunk_manifest = {str(ix): {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
                      for ix, (payload, _) in chunks.items()}
    identity = {"zarr_url": ZARR, "geography_url": GEO, "model": MODEL,
                "model_index": int(model_ix[0]),
                "processing_ids": {city:int(feature["properties"]["processing_id"]) for city,feature in features.items()},
                "processing_axis_positions": ids,
                "chunk_payloads": chunk_manifest,
                "zmetadata_sha256": hashlib.sha256(metadata_payload).hexdigest(),
                "coordinate_chunk_sha256": {
                    "gcm": hashlib.sha256(gcm_payload).hexdigest(),
                    "processing_id": hashlib.sha256(pid_payload).hexdigest(),
                    "time": hashlib.sha256(time_payload).hexdigest()},
                "source_units": "degC",
                "bias_adjustment": metadata["wbgt-sun/.zattrs"].get("bias_adjustment"),
                "time_units": metadata["time/.zattrs"].get("units"),
                "chunk_size": location_chunk}
    return pd.DataFrame(series), identity


def city_cells(feature: dict, lat: np.ndarray, lon: np.ndarray) -> tuple[pd.DataFrame, object, float]:
    """Intersect the published footprint with the NEX grid in equal-area coordinates."""
    project = Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True).transform
    polygon = shape(feature["geometry"])
    projected = transform(project, polygon)
    minx, miny, maxx, maxy = polygon.bounds
    lat_ix = np.flatnonzero((lat >= miny-.125) & (lat <= maxy+.125))
    lon_ix = np.flatnonzero((lon >= minx-.125) & (lon <= maxx+.125))
    rows = []
    for i in lat_ix:
        y = lat[i]
        for j in lon_ix:
            x = lon[j]
            cell = box(float(x)-.125, float(y)-.125, float(x)+.125, float(y)+.125)
            area = transform(project, cell).intersection(projected).area
            if area > 0:
                rows.append({"lat": float(y), "lon": float(x), "lat_index": i,
                             "lon_index": j, "overlap_m2": area})
    frame = pd.DataFrame(rows)
    return frame, projected, float(projected.area)


def reconstruct_city_year(feature: dict, city: str, year: int, work: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Run frozen W1 cell physics and aggregate complete daily city support."""
    reference_path = pilot.variable_path("tas", source_root=pilot.DEFAULT_SOURCE_ROOT,
        wbgt_root=pilot.DEFAULT_WBGT_ROOT, model=MODEL, scenario="historical", year=year)
    with xr.open_dataset(reference_path) as raw:
        reference = normalize_lat_lon(raw)
        lat, lon = np.asarray(reference.lat.values), np.asarray(reference.lon.values)
    cells, polygon, city_area = city_cells(feature, lat, lon)
    if cells.empty:
        raise ValueError(f"No NEX cells intersect {city}")
    ir = int(cells.lat_index.min()), int(cells.lat_index.max())+1
    jr = int(cells.lon_index.min()), int(cells.lon_index.max())+1
    bounds = (ir[0], ir[1], jr[0], jr[1])
    cube, days = pilot.load_daily_cube(source_root=pilot.DEFAULT_SOURCE_ROOT,
        wbgt_root=pilot.DEFAULT_WBGT_ROOT, model=MODEL, scenario="historical", year=year,
        index_range=bounds)
    input_slice_hashes = {v: hashlib.sha256(np.ascontiguousarray(data).tobytes()).hexdigest() for v, data in cube.items()}
    cube, flags, flag_stats = pilot.apply_rh_policy(cube, rh_policy="clip100")
    valid, validity_stats = pilot.input_validity_mask(cube)
    target = pilot.drop_feb29(days[days.year == year])
    lat_subset, lon_subset = lat[ir[0]:ir[1]], lon[jr[0]:jr[1]]
    elevation, elev_stats = qc.cell_elevation_field(lat_subset, lon_subset, TILES, cell_size_deg=.25)
    daily_cells = []
    for cell in cells.itertuples():
        i, j = int(cell.lat_index), int(cell.lon_index)
        ii, jj = i-ir[0], j-jr[0]
        series = pd.DataFrame({v: cube[v][:, ii, jj] for v in pilot.m1.REQUIRED_NEX_VARIABLES}, index=days)
        series.loc[~valid[:, ii, jj], :] = np.nan
        elevation_m = float(elevation[ii, jj])
        if not math.isfinite(elevation_m):
            raise ValueError(f"Missing GMTED elevation for {city} grid cell {cell.lat}/{cell.lon}")
        static = pilot.m1.SiteStatic(f"{city}_{i}_{j}", float(cell.lat), float(cell.lon), elevation_m)
        daily = pilot.cell_daily_max_c(series, static, target_days=target)
        daily_cells.append(daily.to_numpy(dtype=float))
        cells.loc[(cells.lat_index == i) & (cells.lon_index == j), "elevation_m"] = elevation_m
        cells.loc[(cells.lat_index == i) & (cells.lon_index == j), "rh_clipped_days"] = int(flags[days.year == year, ii, jj].sum())
    matrix = np.column_stack(daily_cells)
    areas = cells.overlap_m2.to_numpy(float)
    coverage_area = float(areas.sum()/city_area)
    if not .99 <= coverage_area <= 1.01:
        raise ValueError(f"{city} represented area coverage is {coverage_area:.5f}")
    all_cells_valid = np.isfinite(matrix).all(axis=1)
    regional = np.full(len(target), np.nan)
    regional[all_cells_valid] = (matrix[all_cells_valid] @ (areas/areas.sum()))
    frame = pd.DataFrame({"city": city, "year": year, "date": target.date.astype(str),
                          "w1_area_mean_wbgt_c": regional,
                          "w1_all_cells_valid": all_cells_valid})
    cells.insert(0, "year", year)
    cells.insert(0, "city", city)
    diag = {"city": city, "year": year, "cell_count": len(cells),
            "city_area_km2": city_area/1e6, "cell_overlap_area_km2": areas.sum()/1e6,
            "cell_area_coverage_fraction": coverage_area,
            "w1_valid_city_days": int(all_cells_valid.sum()),
            "w1_invalid_city_days": int((~all_cells_valid).sum()),
            "w1_clipped_rh_cell_days": int(cells.rh_clipped_days.sum()),
            "input_slice_sha256": input_slice_hashes,
            "validity_diagnostics": validity_stats, "clip_diagnostics": flag_stats,
            "elevation_diagnostics": elev_stats}
    return frame, cells, diag


def summarize(paired: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Summarize complete-year metrics and matched-date threshold disagreement."""
    annual, monthly, thresholds = [], [], []
    for (city, year), group in paired.groupby(["city", "year"], sort=False):
        cp = group.carbonplan_wbgt_c.to_numpy(float)
        w1 = group.w1_area_mean_wbgt_c.to_numpy(float)
        matched = np.isfinite(cp) & np.isfinite(w1)
        row = {"city": city, "year": int(year), "calendar_days": len(group),
               "carbonplan_valid_days": int(np.isfinite(cp).sum()),
               "w1_valid_days": int(np.isfinite(w1).sum()),
               "matched_valid_days": int(matched.sum()),
               "carbonplan_annual_mean_c": float(cp.mean()) if len(cp)==365 and np.isfinite(cp).all() else np.nan,
               "w1_annual_mean_c": float(w1.mean()) if len(w1)==365 and np.isfinite(w1).all() else np.nan}
        delta = w1[matched]-cp[matched]
        row.update({"w1_minus_cp_annual_mean_c": row["w1_annual_mean_c"]-row["carbonplan_annual_mean_c"],
                    "matched_mean_difference_c": float(delta.mean()) if matched.any() else np.nan,
                    "matched_mae_c": float(np.abs(delta).mean()) if matched.any() else np.nan,
                    "matched_rmse_c": float(np.sqrt(np.mean(delta**2))) if matched.any() else np.nan,
                    "matched_correlation": float(np.corrcoef(cp[matched], w1[matched])[0,1])
                    if matched.sum()>1 and np.std(cp[matched])>0 and np.std(w1[matched])>0 else np.nan})
        annual.append(row)
        group = group.assign(month=pd.to_datetime(group.date).dt.month)
        for month, part in group.groupby("month"):
            expected_days = int(pd.Period(f"{year}-{month:02d}").days_in_month)
            cp_vals = part.carbonplan_wbgt_c.to_numpy(float)
            w1_vals = part.w1_area_mean_wbgt_c.to_numpy(float)
            monthly.append({"city":city,"year":int(year),"month":int(month),
                            "calendar_days":len(part),
                            "carbonplan_valid_days":int(part.carbonplan_wbgt_c.notna().sum()),
                            "w1_valid_days":int(part.w1_area_mean_wbgt_c.notna().sum()),
                            "carbonplan_mean_c":float(part.carbonplan_wbgt_c.mean()),
                            "w1_mean_c":float(part.w1_area_mean_wbgt_c.mean()),
                            **{f"carbonplan_days_ge_{t}":int(np.sum(cp_vals>=t)) if len(part)==expected_days and np.isfinite(cp_vals).all() else np.nan for t in THRESHOLDS},
                            **{f"w1_days_ge_{t}":int(np.sum(w1_vals>=t)) if len(part)==expected_days and np.isfinite(w1_vals).all() else np.nan for t in THRESHOLDS}})
        for threshold in THRESHOLDS:
            cp_valid, w1_valid = np.isfinite(cp), np.isfinite(w1)
            cp_ex = cp >= threshold
            w1_ex = w1 >= threshold
            both_valid = cp_valid & w1_valid
            cp_only = int(np.sum(both_valid & cp_ex & ~w1_ex))
            w1_only = int(np.sum(both_valid & w1_ex & ~cp_ex))
            both = int(np.sum(both_valid & cp_ex & w1_ex))
            neither = int(np.sum(both_valid & ~cp_ex & ~w1_ex))
            full_year = len(group) == 365 and cp_valid.all() and w1_valid.all()
            thresholds.append({"city":city,"year":int(year),"threshold_c":threshold,
                "carbonplan_valid_days":int(cp_valid.sum()),"w1_valid_days":int(w1_valid.sum()),
                "carbonplan_days_complete_year":int(np.sum(cp_ex)) if len(group)==365 and cp_valid.all() else np.nan,
                "w1_days_complete_year":int(np.sum(w1_ex)) if len(group)==365 and w1_valid.all() else np.nan,
                "both_days_complete_year":int(np.sum(cp_ex & w1_ex)) if full_year else np.nan,
                "matched_days":int(both_valid.sum()),"cp_only_days_on_matched_dates":cp_only,
                "w1_only_days_on_matched_dates":w1_only,"both_exceed_days":both,
                "neither_exceed_days":neither,
                "unpaired_cp_exceed_days":int(np.sum(cp_valid & ~w1_valid & cp_ex)),
                "unpaired_w1_exceed_days":int(np.sum(w1_valid & ~cp_valid & w1_ex))})
    return pd.DataFrame(annual), pd.DataFrame(monthly), pd.DataFrame(thresholds)


def main() -> None:
    """Validate inputs and write a fresh isolated comparison evidence directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT)
    parser.add_argument("--work-dir", type=Path, default=WORK,
                        help="Cache directory for bounded CarbonPlan source chunks")
    args = parser.parse_args()
    out, work = args.out_dir, args.work_dir
    if out.exists():
        raise FileExistsError(f"Refusing existing output directory: {out}")
    if not PILOT_CACHE.is_file() or not all(p.is_file() for p in TILES):
        raise FileNotFoundError("Frozen Kochi QC cache or all three local GMTED tiles are required")
    for year in range(min(YEARS)-1, max(YEARS)+2):
        for var in pilot.m1.REQUIRED_NEX_VARIABLES:
            path = pilot.variable_path(var, source_root=pilot.DEFAULT_SOURCE_ROOT, wbgt_root=pilot.DEFAULT_WBGT_ROOT, model=MODEL, scenario="historical", year=year)
            check = pilot.verify_variable(path, var, year)
            if not check.ok:
                raise ValueError(f"Input preflight failed: {check}")
    out.mkdir(parents=True)
    work.mkdir(parents=True, exist_ok=True)
    print("Reading published geography", flush=True)
    features, geo_bytes = read_geography(work / "source_cache")
    print("Published cities:", list(features), flush=True)
    cp, source_identity = get_carbonplan(features, work)
    cp.to_csv(work / "carbonplan_daily.csv", index=False)
    w1_parts, cell_parts, diagnostics = [], [], []
    for city, feature in features.items():
        for year in YEARS:
            print("Reconstructing", city, year, flush=True)
            cache = work / f"w1_{city}_{year}.csv"
            frame, cells, diag = reconstruct_city_year(feature, city, year, work)
            frame.to_csv(cache, index=False)
            cells.to_csv(work / f"cells_{city}_{year}.csv", index=False)
            (work / f"diag_{city}_{year}.json").write_text(json.dumps(diag, indent=2))
            w1_parts.append(frame)
            cell_parts.append(cells)
            diagnostics.append(diag)
    w1 = pd.concat(w1_parts, ignore_index=True)
    if w1.duplicated(["city","year","date"]).any() or cp.duplicated(["city","year","date"]).any():
        raise ValueError("Duplicate city-year daily keys")
    paired = cp.merge(w1, on=["city","year","date"], how="outer", validate="one_to_one")
    old = pd.read_csv("docs/diagnostics/wbgt_outdoor_carbonplan_compare/paired_kochi_daily_2005.csv")
    control = paired[(paired.city == "Kochi") & (paired.year == 2005)].sort_values("date")
    np.testing.assert_allclose(control.w1_area_mean_wbgt_c, old.sort_values("date").w1_kochi_city_area_mean_daily_max_c, atol=1e-10, rtol=0)
    np.testing.assert_allclose(control.carbonplan_wbgt_c, old.sort_values("date").carbonplan_kochi_city_daily_c, atol=1e-10, rtol=0)
    annual, monthly, thresholds = summarize(paired)
    for (city, year), group in monthly.groupby(["city","year"]):
        if group.month.nunique() != 12:
            raise AssertionError(f"Monthly summary missing months: {city} {year}")
        days = len(paired[(paired.city == city) & (paired.year == year)])
        if int(group.carbonplan_valid_days.sum()) != days:
            raise AssertionError(f"CarbonPlan monthly counts fail annual identity: {city} {year}")
    # Reproduce frozen Kochi 2005 published pilot numbers and threshold counts.
    check = annual[(annual.city == "Kochi") & (annual.year == 2005)].iloc[0]
    expected = {"carbonplan_annual_mean_c": 31.0693, "w1_annual_mean_c": 31.2990}
    for key, value in expected.items():
        if abs(float(check[key])-value) > .001:
            raise AssertionError(f"Frozen Kochi 2005 parity failed: {key}={check[key]}")
    kochi_t = thresholds[(thresholds.city == "Kochi") & (thresholds.year == 2005)].set_index("threshold_c")
    for threshold, cp_days, w1_days in ((28,361,363),(30,266,318),(32,110,116)):
        if int(kochi_t.loc[threshold,"carbonplan_days_complete_year"]) != cp_days or int(kochi_t.loc[threshold,"w1_days_complete_year"]) != w1_days:
            raise AssertionError(f"Frozen Kochi 2005 threshold parity failed at {threshold} C")
    cp.to_csv(out / "carbonplan_daily.csv", index=False)
    w1.to_csv(out / "w1_daily.csv", index=False)
    paired.to_csv(out / "paired_daily.csv", index=False)
    pd.concat(cell_parts, ignore_index=True).to_csv(out / "city_cells.csv", index=False)
    annual.to_csv(out / "annual_summary.csv", index=False)
    monthly.to_csv(out / "monthly_summary.csv", index=False)
    thresholds.to_csv(out / "threshold_summary.csv", index=False)
    pd.DataFrame(diagnostics).to_json(out / "coverage_diagnostics.json", orient="records", indent=2)
    snapshot = subprocess.run(["git","rev-parse","--short","HEAD"], check=True,
                              capture_output=True,text=True).stdout.strip()
    with xr.open_dataset(PILOT_CACHE) as frozen:
        method_signature = frozen.attrs["method_signature"]
        elevation_convention = frozen.attrs["elevation_convention"]
    for city, feature in features.items():
        (out / f"{city}_geometry.geojson").write_text(json.dumps(feature))
    manifest = {"carbonplan_data_license": "CC BY 4.0", "elevation_tile_sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in TILES},"git_snapshot": snapshot, "model":MODEL,"scenario":"historical",
        "years":list(YEARS),"cities":list(CITY_NAMES),"thresholds_c":list(THRESHOLDS),
        "carbonplan":source_identity,
        "geography_sha256":hashlib.sha256(geo_bytes).hexdigest(),
        "w1_source_roots":{"nex":"D:/projects/irt_data/r1i1p1f1",
                           "radiation_wind":"D:/projects/irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1"},
        "w1_method_signature":method_signature,
        "elevation_convention":elevation_convention,
        "w1_method":"Frozen outdoor W1 pilot; daily cell max then area weighted city series",
        "w1_rh_policy":"clip hurs values above 100%; retain and report clip flags; other invalid inputs remain missing",
        "w1_elevation":"GMTED2010 30 arc-second mean, cos-latitude-weighted climate-cell mean; m above EGM96",
        "w1_spatial_weights":"EPSG:6933 intersection area within published CarbonPlan city polygons",
        "carbonplan_spatial_weights":"CarbonPlan population-informed city weights",
        "carbonplan_bias_adjustment":source_identity["bias_adjustment"],
        "interpretation":"Published-model-product disagreement, not observational accuracy; grids, member lineage and spatial weights differ.",
        "valid_day_policy":"Annual means/counts require complete years; monthly counts require complete months; paired differences and disagreements use jointly finite dates. No filling.",
        "coverage":diagnostics}
    (out / "manifest.json").write_text(json.dumps(manifest,indent=2))
    print(annual.to_string(index=False))
    print("Wrote reproducible multicity evidence to", out)


if __name__ == "__main__":
    main()
