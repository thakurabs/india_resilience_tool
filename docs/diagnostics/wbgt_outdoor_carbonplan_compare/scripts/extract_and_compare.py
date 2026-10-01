"""One-off bounded comparison of CarbonPlan's released Kochi daily WBGT and IRT W1."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd
import requests
import xarray as xr
from numcodecs import get_codec
from shapely.geometry import box, shape
from shapely.ops import transform
from pyproj import Transformer


BASE = "https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0"
ZARR = BASE + "/outputs/zarr/daily/historical-WBGT-sun.zarr/"
GEO = BASE + "/inputs/all_regions_and_cities.json"
PILOT = Path("scratch/wbgt_outdoor_pilot_qc/cells_Kerala_ACCESS-CM2_2005_combined.nc")
OUT = Path("docs/diagnostics/wbgt_outdoor_carbonplan_compare")
CITY_NAME = "Kochi"
YEAR = 2005


def get(url: str, *, headers: dict[str, str] | None = None) -> bytes:
    response = requests.get(url, headers=headers, timeout=90)
    response.raise_for_status()
    return response.content


def read_city_feature() -> tuple[dict, str]:
    data = get(GEO, headers={"Range": "bytes=0-16777215"})
    if len(data) > 16_777_216:
        raise ValueError("Geography server ignored the bounded range request")
    text = data.decode("utf-8")
    marker = f'"UC_NM_MN": "{CITY_NAME}"'
    pos = text.find(marker)
    if pos < 0:
        raise ValueError(f"{CITY_NAME} is not in the bounded geography range")
    start = text.rfind('{ "type": "Feature"', 0, pos)
    if start < 0:
        raise ValueError("Could not locate city feature start")
    feature, _ = json.JSONDecoder().raw_decode(text[start:])
    if feature["properties"]["UC_NM_MN"] != CITY_NAME:
        raise ValueError("Feature name mismatch")
    return feature, hashlib.sha256(json.dumps(feature, sort_keys=True).encode()).hexdigest()


def read_cp_series(pid: int) -> tuple[pd.DataFrame, dict]:
    metadata_bytes = get(ZARR + ".zmetadata")
    metadata = json.loads(metadata_bytes)["metadata"]
    if metadata["time/.zattrs"].get("units") != "days since 1985-01-01T12:00:00":
        raise ValueError("Published daily time origin changed")
    if metadata["wbgt-sun/.zattrs"].get("units") != "degC":
        raise ValueError("Published WBGT unit changed")
    if metadata["wbgt-sun/.zarray"].get("dtype") != "<f8":
        raise ValueError("Published WBGT dtype changed")
    if metadata["wbgt-sun/.zarray"].get("chunks") != [1, 850, 10957]:
        raise ValueError("Published WBGT chunk layout changed")

    def coordinate(name: str, chunk: int, dtype: str) -> np.ndarray:
        meta = metadata[f"{name}/.zarray"]
        payload = get(ZARR + f"{name}/{chunk}")
        raw = get_codec(meta["compressor"]).decode(payload)
        return np.frombuffer(raw, dtype=dtype)

    gcms = coordinate("gcm", 0, "<U16")
    model = "ACCESS-CM2"
    gcm_index = np.where(gcms == model)[0]
    if len(gcm_index) != 1:
        raise ValueError("Model missing or duplicated in published Zarr")
    pids = coordinate("processing_id", 0, "<i8")
    matches = np.flatnonzero(pids == pid)
    if len(matches) != 1:
        raise ValueError("Published processing ID missing or duplicated")
    position = int(matches[0])
    time = coordinate("time", 0, "<i8")
    meta = metadata["wbgt-sun/.zarray"]
    chunk_size = int(meta["chunks"][1])
    chunk = position // chunk_size
    payload = get(ZARR + f"wbgt-sun/{int(gcm_index[0])}.{chunk}.0")
    raw = get_codec(meta["compressor"]).decode(payload)
    values = np.frombuffer(raw, dtype="<f8").reshape((chunk_size, len(time)))[position % chunk_size].copy()
    dates = pd.Timestamp("1985-01-01 12:00:00") + pd.to_timedelta(time, unit="D")
    frame = pd.DataFrame({"date": dates.date.astype(str), "wbgt_c": values})
    identity = {
        "url": ZARR,
        "model": model,
        "gcm_index": int(gcm_index[0]),
        "processing_id": pid,
        "position_in_processing_id_axis": position,
        "chunk": f"wbgt-sun/{int(gcm_index[0])}.{chunk}.0",
        "chunk_compressed_bytes": len(payload),
        "chunk_sha256": hashlib.sha256(payload).hexdigest(),
        "zmetadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
        "published_units": metadata["wbgt-sun/.zattrs"]["units"],
        "published_bias_adjustment": metadata["wbgt-sun/.zattrs"]["bias_adjustment"],
        "time_units": metadata["time/.zattrs"]["units"],
    }
    return frame, identity


def summarize(values: np.ndarray) -> dict[str, float | int]:
    finite = np.isfinite(values)
    return {
        "valid_days": int(finite.sum()),
        "annual_mean_daily_max_c": float(np.mean(values[finite])) if finite.any() else float("nan"),
        "days_ge_28": int(np.sum(values >= 28)),
        "days_ge_30": int(np.sum(values >= 30)),
        "days_ge_32": int(np.sum(values >= 32)),
    }


def main() -> None:
    global OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT,
                        help="Fresh output directory; existing paths are refused")
    args = parser.parse_args()
    OUT = args.out_dir
    if OUT.exists():
        raise FileExistsError(f"Output directory already exists: {OUT}")
    feature, feature_hash = read_city_feature()
    pid = int(feature["properties"]["processing_id"])
    cp, cp_id = read_cp_series(pid)
    cp_2005 = cp[cp.date.str.startswith(str(YEAR))]
    if len(cp_2005) != 365 or cp_2005.wbgt_c.isna().any():
        raise ValueError("CarbonPlan target year is incomplete")

    city = shape(feature["geometry"])
    area_projection = Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True).transform
    city_area = transform(area_projection, city)
    with xr.open_dataset(PILOT) as dataset:
        if dataset.attrs.get("rh_policy") != "clip100":
            raise ValueError("Pilot is not the specified RH treatment")
        if "gmted" not in dataset.attrs.get("elevation_convention", ""):
            raise ValueError("Pilot has no acquired elevation")
        w1_method_signature = dataset.attrs.get("method_signature")
        rows = []
        for lat in dataset.lat.values:
            for lon in dataset.lon.values:
                cell = box(float(lon) - 0.125, float(lat) - 0.125,
                           float(lon) + 0.125, float(lat) + 0.125)
                area = transform(area_projection, cell).intersection(city_area).area
                if area <= 0:
                    continue
                row = {"lat": float(lat), "lon": float(lon), "overlap_m2": area}
                for metric in ("wbgt_annual_mean_c", "days_ge_28", "days_ge_30", "days_ge_32"):
                    row[metric] = float(dataset[metric].sel(lat=lat, lon=lon).item())
                row["complete_year"] = bool(dataset["complete_year"].sel(lat=lat, lon=lon).item())
                rows.append(row)
    cells = pd.DataFrame(rows)
    if cells.empty:
        raise ValueError("No pilot cell intersects the CarbonPlan city polygon")
    valid = cells[cells.complete_year]
    coverage = float(valid.overlap_m2.sum() / city_area.area)
    if coverage < 0.99 or coverage > 1.01:
        raise ValueError(f"City polygon not fully represented by complete pilot cells: {coverage:.3f}")
    weights = valid.overlap_m2 / valid.overlap_m2.sum()
    w1 = {
        "valid_days": 365,
        "annual_mean_daily_max_c": float(np.average(valid.wbgt_annual_mean_c, weights=weights)),
        **{key: float(np.average(valid[key], weights=weights)) for key in ("days_ge_28", "days_ge_30", "days_ge_32")},
    }
    cp_stats = summarize(cp_2005.wbgt_c.to_numpy())
    records = []
    for metric in ("annual_mean_daily_max_c", "days_ge_28", "days_ge_30", "days_ge_32"):
        records.append({"metric": metric, "carbonplan_city_2005": cp_stats[metric],
                        "irt_w1_city_area_mean_2005": w1[metric],
                        "w1_minus_carbonplan": w1[metric] - cp_stats[metric]})
    cp["year"] = cp.date.str.slice(0, 4).astype(int)
    annual = pd.DataFrame([{"year": int(year), **summarize(group.wbgt_c.to_numpy())}
                           for year, group in cp.groupby("year")])

    OUT.mkdir(parents=True)
    (OUT / "carbonplan_kochi_geometry.geojson").write_text(json.dumps(feature))
    cp.to_csv(OUT / "carbonplan_kochi_daily.csv", index=False)
    annual.to_csv(OUT / "carbonplan_kochi_annual.csv", index=False)
    cells.to_csv(OUT / "w1_intersecting_cells_2005.csv", index=False)
    pd.DataFrame(records).to_csv(OUT / "comparison_2005.csv", index=False)
    snapshot = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                              check=True, capture_output=True, text=True).stdout.strip()
    manifest = {"source": cp_id,
                "carbonplan_data_license": "CC BY 4.0",
                "git_snapshot": f"add_flood_depth@{snapshot}",
                "geography_url": GEO, "feature_sha256": feature_hash,
                "feature_properties": feature["properties"], "city_area_m2": city_area.area,
                "w1_city_valid_fraction": coverage, "w1_source": str(PILOT),
                "w1_source_sha256": hashlib.sha256(PILOT.read_bytes()).hexdigest(),
                "w1_method_signature": w1_method_signature,
                "w1_spatial_weighting": "EPSG:6933 area of city-polygon intersection with 0.25-degree NEX cells",
                "carbonplan_spatial_weighting": "population weighted within city; see CarbonPlan notebook 05_aggregate.ipynb",
                "target_year": YEAR}
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(pd.DataFrame(records).to_string(index=False))
    print("city cells", len(cells), "coverage", coverage)
    print("CarbonPlan 1985-2014 >=32 median", annual.days_ge_32.median())


if __name__ == "__main__":
    main()
