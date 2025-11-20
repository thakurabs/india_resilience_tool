#!/usr/bin/env python3
"""
Compute "number of days above 32°C" (i.e. tas > 305.15 K) per district of Telangana
from NASA NEX-GDDP-CMIP6-like daily tas files that are stored yearwise.

Author: ABS Thakur
email: abs.thakur@resilience.org.in
"""

import os
import glob
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from rasterio import features
import shapely
from typing import Dict, List, Tuple, Optional
from affine import Affine
import logging
import traceback
import subprocess
import json
from statistics import mean


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

# Root where all model/scenario data live
DATA_ROOT = Path(r"D:\projects\irt\r1i1p1f1")

# Shapefile / GeoJSON with all India districts
DISTRICTS_PATH = Path(r"D:\projects\irt\districts_4326.geojson")

# Where we store derived outputs
OUTPUT_ROOT = Path(r"D:\projects\irt\processed\tas_gt32")

# temperature threshold
THRESH_C = 32.0
THRESH_K = THRESH_C + 273.15  # 305.15 K

# scenarios, their subdirs, and averaging windows
SCENARIOS = {
    "historical": {
        "subdir": "historical/tas",
        "periods": {
            "1990-2010": (1990, 2010),
        },
    },
    "ssp245": {
        "subdir": "ssp245/tas",
        "periods": {
            "2020-2040": (2020, 2040),
            "2040-2060": (2040, 2060),
        },
    },
    "ssp585": {
        "subdir": "ssp585/tas",
        "periods": {
            "2020-2040": (2020, 2040),
            "2040-2060": (2040, 2060),
        },
    },
}

# -----------------------
# Dynamic model discovery
# -----------------------
def discover_models(data_root: Path, scenarios: dict) -> list:
    """
    Search data_root/<scenario>/tas/ for model subdirectories and return
    a sorted unique list of model names.
    """
    models = set()
    for scen_name, scen_conf in scenarios.items():
        model_base = data_root / scen_conf["subdir"]
        if not model_base.exists():
            # skip missing scenario directories (we already log these later)
            continue
        # list immediate directories under model_base
        for entry in model_base.iterdir():
            if entry.is_dir():
                models.add(entry.name)
    return sorted(models)

# replace static MODELS with dynamic discovery
MODELS = discover_models(DATA_ROOT, SCENARIOS)
if not MODELS:
    logging.warning("No model directories discovered under DATA_ROOT; check DATA_ROOT and SCENARIOS.")
else:
    logging.info(f"Discovered models: {MODELS}")


# configure logging (put near top of script)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------
def normalize_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    for cand in ["latitude", "y"]:
        if cand in ds.dims:
            ren[cand] = "lat"
    for cand in ["longitude", "x"]:
        if cand in ds.dims:
            ren[cand] = "lon"
    if ren:
        ds = ds.rename(ren)
    return ds


def load_telangana_districts(path: Path) -> gpd.GeoDataFrame:
    """
    Load the all-India districts and filter to Telangana.

    Your file has columns like:
    - OBJECTID
    - STATE_LGD
    - DISTRICT
    - STATE_UT
    """
    gdf = gpd.read_file(path)

    # we expect STATE_UT
    if "STATE_UT" not in gdf.columns:
        raise ValueError(
            f"Expected a 'STATE_UT' column in {path}, found: {list(gdf.columns)}"
        )

    gdf["_state_norm"] = gdf["STATE_UT"].astype(str).str.strip().str.lower()

    # handle possible variants
    tel = gdf[gdf["_state_norm"].isin(["telangana", "telengana", "telangana state"])]
    if tel.empty:
        # show what states are actually there to help debugging
        all_states = sorted(gdf["_state_norm"].unique().tolist())
        raise ValueError(
            "No rows for Telangana/Telengana in districts_4326.geojson. "
            f"Available states in file: {all_states}"
        )

    tel = tel.drop(columns=["_state_norm"])

    # for logging
    print(f"[INFO] Loaded Telangana districts: {len(tel)} rows")
    print("[INFO] District name sample:", tel[["DISTRICT", "STATE_UT"]].head())

    # ensure CRS
    if tel.crs is None:
        tel = tel.set_crs("EPSG:4326")

    return tel


def build_district_masks(telangana_gdf: gpd.GeoDataFrame,
                         sample_ds: xr.Dataset,
                         district_name_col: str = "DISTRICT"):
    """
    Rasterize each Telangana district to the grid of sample_ds.
    Returns: dict[name] -> mask (lat, lon)
    """
    ds = sample_ds
    if district_name_col not in telangana_gdf.columns:
        raise ValueError(
            f"'{district_name_col}' not found in Telangana GDF. "
            f"Available columns: {list(telangana_gdf.columns)}"
        )

    lats = ds["lat"].values
    lons = ds["lon"].values

    # nxn info
    height = lats.size
    width = lons.size

    # NEX is usually lon increasing, lat decreasing (north->south)
    # build affine
    # lon
    if np.all(np.diff(lons) > 0):
        xres = lons[1] - lons[0]
        xoff = lons[0] - xres / 2
    else:
        raise ValueError("Longitude not strictly increasing, please inspect")

    # lat
    if np.all(np.diff(lats) < 0):
        # lats going from north to south
        yres = lats[1] - lats[0]  # negative
        yoff = lats[0] - yres / 2
    else:
        # occasional datasets go south->north
        yres = lats[1] - lats[0]
        yoff = lats[0] - yres / 2

    transform = Affine.translation(xoff, yoff) * Affine.scale(xres, yres)

    masks = {}
    for _, row in telangana_gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        name = str(row[district_name_col]).strip()

        mask = features.rasterize(
            [(geom, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="uint8",
        )
        mask_da = xr.DataArray(
            mask.astype(bool),
            coords={"lat": ds["lat"], "lon": ds["lon"]},
            dims=("lat", "lon"),
            name="mask",
        )
        masks[name] = mask_da

    print(f"[INFO] Built {len(masks)} district masks for Telangana.")
    return masks


def count_days_above_threshold(da_tas: xr.DataArray,
                               mask: xr.DataArray,
                               thresh_k: float) -> int:
    tas_masked = da_tas.where(mask)
    daily_mean = tas_masked.mean(dim=("lat", "lon"), skipna=True)
    return int((daily_mean > thresh_k).sum().item())


def yearly_files_for_dir(dirpath: Path):
    files = glob.glob(str(dirpath / "*.nc"))
    out = {}
    for f in files:
        y = os.path.splitext(os.path.basename(f))[0]
        if y.isdigit():
            out[int(y)] = Path(f)
    return dict(sorted(out.items()))



# configure logging (put near top of script)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- helper: try to open netcdf file safely ----
def try_open_nc(path: Path, try_engines=("netcdf4", "h5netcdf", "scipy")) -> bool:
    """
    Test whether xarray/netCDF can open this file.
    Returns True if open succeeded (and closes it), False otherwise.
    This function avoids loading data; it only opens metadata.
    """
    for eng in try_engines:
        try:
            xr.open_dataset(path, engine=eng).close()
            logging.debug(f"Opened {path} with engine={eng}")
            return True
        except Exception as e:
            logging.debug(f"Engine {eng} failed for {path}: {type(e).__name__}: {e}")
            continue
    return False


def inspect_file_magic(path: Path, nbytes=8) -> str:
    """
    Read first few bytes to get file 'magic' for debugging.
    Common signatures:
      - NetCDF classic: starts with "CDF" (b'CDF')
      - HDF5: starts with b'\x89HDF' 
      - HTML: starts with b'<!DOCTYPE' or b'<html'
    Returns a short hex/text prefix.
    """
    try:
        with open(path, "rb") as f:
            head = f.read(nbytes)
        try:
            txt = head.decode('ascii', errors='ignore')
            return txt + " | hex=" + head.hex()
        except Exception:
            return "hex=" + head.hex()
    except Exception as e:
        return f"could_not_read: {e}"


# ---- helper: build validated year-files list ----
def validated_year_files(data_dir: Path) -> tuple[dict, dict]:
    """
    Scans data_dir for *.nc; returns (valid_year_files, bad_files_info).
    valid_year_files: {year: Path}
    bad_files_info: {year_or_name: {"path": Path, "reason": str, "magic": str}}
    """
    year_files = yearly_files_for_dir(data_dir)  # existing helper from your script
    valid = {}
    bad = {}

    if not year_files:
        return valid, bad

    logging.info(f"Found {len(year_files)} candidate year files in {data_dir}")

    for year, p in year_files.items():
        # quick filesize check (skip zero-byte)
        try:
            sz = p.stat().st_size
        except Exception as e:
            bad[year] = {"path": p, "reason": f"stat_failed: {e}", "magic": None}
            continue

        if sz == 0:
            bad[year] = {"path": p, "reason": "zero_size", "magic": None}
            continue

        # quick open test (metadata only)
        ok = try_open_nc(p)
        if ok:
            valid[year] = p
        else:
            magic = inspect_file_magic(p)
            bad[year] = {"path": p, "reason": "open_failed", "magic": magic}

    logging.info(f"Valid files: {len(valid)} ; Bad files: {len(bad)}")
    return dict(sorted(valid.items())), bad


# ---- updated process_model_for_scenario using validated files ----
def process_model_for_scenario(model: str,
                               scenario: str,
                               scenario_conf: dict,
                               telangana_gdf: gpd.GeoDataFrame,
                               min_years_required_fraction: float = 0.6,
                               min_years_absolute: int = 5):
    data_dir = DATA_ROOT / scenario_conf["subdir"] / model
    if not data_dir.exists():
        logging.warning(f"{data_dir} does not exist, skipping.")
        return

    valid_year_files, bad_files = validated_year_files(data_dir)

    # write a short processing report for debugging
    # put all validation reports under a single folder to keep the output tree tidy
    report_dir = OUTPUT_ROOT / "Telangana" / "validation_reports" / model / scenario
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "file_validation_report.csv"
    
    if bad_files:
        # flatten bad_files to df
        rows = []
        for y, info in bad_files.items():
            rows.append({
                "year_or_name": y,
                "path": str(info["path"]),
                "reason": info["reason"],
                "magic": info["magic"],
                "size_bytes": info["path"].stat().st_size if info["path"].exists() else None
            })
        pd.DataFrame(rows).to_csv(report_path, index=False)
        logging.warning(f"Wrote bad-file report: {report_path}")
    else:
        logging.info("No bad files detected.")

    if not valid_year_files:
        logging.warning(f"No valid .nc files to process in {data_dir}. Skipping model/scenario.")
        return

    # --- show the sample file used to build masks ---
    sample_year, sample_path = next(iter(valid_year_files.items()))
    logging.info(f"Using sample file to build masks: {sample_path}")

    ds_sample = xr.open_dataset(sample_path, engine="netcdf4")  # should work now for valid files
    ds_sample = normalize_lat_lon(ds_sample)
    if "tas" not in ds_sample:
        raise ValueError(f"'tas' variable not found in sample {sample_path}")
    masks = build_district_masks(telangana_gdf, ds_sample, district_name_col="DISTRICT")
    ds_sample.close()

    rows = []
    for year, nc_path in valid_year_files.items():
        # <-- print/log the exact file path being processed -->
        logging.info(f"Processing year {year} file: {nc_path}")

        try:
            ds = xr.open_dataset(nc_path, engine="netcdf4")
        except Exception as e:
            logging.error(f"Failed to open {nc_path} despite earlier validation: {e}")
            # also dump traceback for convenience
            logging.debug(traceback.format_exc())
            continue

        ds = normalize_lat_lon(ds)
        if "tas" not in ds:
            logging.error(f"'tas' missing in {nc_path}, skipping year {year}")
            ds.close()
            continue
        tas = ds["tas"]

        for dist_name, mask_da in masks.items():
            try:
                days = count_days_above_threshold(tas, mask_da, THRESH_K)
            except Exception as e:
                logging.error(f"Error computing days for district '{dist_name}', year {year}, file {nc_path}: {e}")
                logging.debug(traceback.format_exc())
                days = None
            rows.append({
                "district": dist_name,
                "model": model,
                "scenario": scenario,
                "year": year,
                "days_gt_32C": days,
                "source_file": str(nc_path),
            })
        ds.close()

    df_yearly = pd.DataFrame(rows)

    # ----------------------------
    # Aggregate to periods but check coverage
    # ----------------------------
    period_frames = []
    for period_name, (y0, y1) in scenario_conf["periods"].items():
        requested_years = list(range(y0, y1 + 1))
        available_years = sorted([y for y in valid_year_files.keys() if y0 <= y <= y1])
        n_req = len(requested_years)
        n_avail = len(available_years)
        frac = n_avail / n_req if n_req > 0 else 0

        logging.info(f"Period {period_name}: requested {n_req} years, available {n_avail} years (frac={frac:.2f})")

        # decide whether to compute the period mean
        if (n_avail >= min_years_absolute) and (frac >= min_years_required_fraction):
            df_sub = df_yearly[df_yearly["year"].isin(available_years)]
            grp = (
                df_sub.groupby(["district", "model", "scenario"])
                .agg({"days_gt_32C": "mean"})
                .reset_index()
            )
            grp["period"] = period_name
            grp["years_used_count"] = n_avail
            grp["years_requested"] = n_req
            period_frames.append(grp)
        else:
            logging.warning(
                f"Insufficient coverage for period {period_name} ({n_avail}/{n_req} years). "
                "Skipping period aggregation; see report."
            )

    if period_frames:
        df_periods = pd.concat(period_frames, ignore_index=True)
    else:
        df_periods = pd.DataFrame(columns=["district", "model", "scenario", "days_gt_32C", "period"])

    # write per district
    for dist_name in df_yearly["district"].unique():
        out_dir = OUTPUT_ROOT / "Telangana" / dist_name.replace(" ", "_") / model / scenario
        out_dir.mkdir(parents=True, exist_ok=True)

        df_d = df_yearly[df_yearly["district"] == dist_name].sort_values("year")
        df_d.to_csv(out_dir / f"{dist_name.replace(' ', '_')}_yearly.csv", index=False)
        df_p = df_periods[df_periods["district"] == dist_name]
        df_p.to_csv(out_dir / f"{dist_name.replace(' ', '_')}_periods.csv", index=False)

    logging.info(f"Finished processing {model}/{scenario}. Output in {OUTPUT_ROOT}/Telangana/...")

def compute_ensembles_from_outputs(output_root: Path, state: str = "Telangana",
                                   write_yearly_ensemble: bool = True,
                                   write_period_ensemble: bool = True):
    """
    Read per-district / per-model outputs already written by the pipeline and compute:
      - per-district, per-scenario, per-period ensemble stats (mean, std, median, 5/95 pct)
      - optionally, per-district, per-scenario yearly ensemble timeseries (mean across models per year)

    Writes outputs under:
      OUTPUT_ROOT / state / "ensembles" / <district> / <scenario> / ...
    Also writes a master manifest CSV listing which models were used per district/scenario/period.

    This function is robust to missing models and missing years.
    """
    import glob
    from pathlib import Path
    import pandas as pd
    import numpy as np

    root = Path(output_root)
    state_root = root / state
    ensembles_root = state_root / "ensembles"
    ensembles_root.mkdir(parents=True, exist_ok=True)

    # discover districts by scanning the existing district folders
    district_dirs = sorted([p for p in state_root.iterdir() if p.is_dir() and p.name != "validation_reports" and p.name != "ensembles"])
    if not district_dirs:
        print("[WARN] No district directories found under", state_root)
        return

    # We'll produce a master summary list for quick inspection
    master_rows = []

    for ddir in district_dirs:
        district = ddir.name
        print(f"[ENSEMBLE] Processing district: {district}")
        # find all models present under this district
        # expected structure: <state>/<district>/<model>/<scenario>/
        model_dirs = [p for p in ddir.iterdir() if p.is_dir()]
        # collect scenario names by probing models
        scenarios = set()
        for m in model_dirs:
            for s in m.iterdir():
                if s.is_dir():
                    scenarios.add(s.name)
        scenarios = sorted(scenarios)

        for scenario in scenarios:
            # collect per-model period CSVs
            model_periods = []
            model_yearly = []
            models_used_for_period = {}
            models_used_for_years = {}

            for m in model_dirs:
                model_name = m.name
                period_csv = m / scenario / f"{district.replace(' ', '_')}_periods.csv"
                yearly_csv = m / scenario / f"{district.replace(' ', '_')}_yearly.csv"
                if period_csv.exists():
                    try:
                        dfp = pd.read_csv(period_csv)
                        # we expect columns: district, model, scenario, days_gt_32C, period, years_used_count, years_requested
                        dfp["model"] = model_name
                        model_periods.append(dfp)
                    except Exception as e:
                        print(f"[WARN] Could not read {period_csv}: {e}")
                if yearly_csv.exists():
                    try:
                        dfy = pd.read_csv(yearly_csv)
                        dfy["model"] = model_name
                        model_yearly.append(dfy)
                    except Exception as e:
                        print(f"[WARN] Could not read {yearly_csv}: {e}")

            # --- PERIOD ENSEMBLES ---
            if model_periods and write_period_ensemble:
                df_all_periods = pd.concat(model_periods, ignore_index=True)
                # group by period (and district & scenario)
                for period_name, sub in df_all_periods.groupby("period"):
                    vals = sub["days_gt_32C"].dropna().astype(float).to_numpy()
                    models_here = sorted(sub["model"].unique().tolist())
                    if len(vals) == 0:
                        continue
                    out = {
                        "district": district,
                        "scenario": scenario,
                        "period": period_name,
                        "n_models": int(len(vals)),
                        "models": json.dumps(models_here),
                        "ensemble_mean": float(np.mean(vals)),
                        "ensemble_std": float(np.std(vals, ddof=0)),
                        "ensemble_median": float(np.median(vals)),
                        "ensemble_p05": float(np.percentile(vals, 5)),
                        "ensemble_p95": float(np.percentile(vals, 95)),
                        "values_per_model": json.dumps(dict(zip(models_here, vals.tolist()))),
                    }
                    # write per-district/per-scenario/period CSV (or a single CSV per scenario)
                    out_dir = ensembles_root / district / scenario
                    out_dir.mkdir(parents=True, exist_ok=True)
                    # append to a periods_ensemble.csv
                    periods_csv = out_dir / f"{district.replace(' ', '_')}_periods_ensemble.csv"
                    pd.DataFrame([out]).to_csv(periods_csv, mode="a", header=not periods_csv.exists(), index=False)

                    master_rows.append({**out, "type": "period"})

            # --- YEARLY ENSEMBLES (per-year mean across models) ---
            if model_yearly and write_yearly_ensemble:
                # concat yearly across models; pivot so each model is a column keyed by year
                df_year_concat = pd.concat(model_yearly, ignore_index=True)
                # ensure year numeric
                df_year_concat["year"] = df_year_concat["year"].astype(int)
                pivot = df_year_concat.pivot_table(index="year", columns="model", values="days_gt_32C", aggfunc="first")
                # compute statistics across models per year, ignoring NaNs
                yearly_summary = pd.DataFrame({
                    "year": pivot.index,
                    "n_models": pivot.count(axis=1),
                    "ensemble_mean": pivot.mean(axis=1, skipna=True),
                    "ensemble_std": pivot.std(axis=1, ddof=0, skipna=True),
                    "ensemble_median": pivot.median(axis=1, skipna=True),
                    "ensemble_p05": pivot.quantile(0.05, axis=1),
                    "ensemble_p95": pivot.quantile(0.95, axis=1),
                }).reset_index(drop=True)

                # write to disk
                out_dir = ensembles_root / district / scenario
                out_dir.mkdir(parents=True, exist_ok=True)
                yearly_csv = out_dir / f"{district.replace(' ', '_')}_yearly_ensemble.csv"
                yearly_summary.to_csv(yearly_csv, index=False)

                master_rows.append({
                    "district": district,
                    "scenario": scenario,
                    "type": "yearly",
                    "n_years": len(yearly_summary),
                    "yearly_csv": str(yearly_csv),
                })

    # write a master manifest for quick overview
    manifest_path = ensembles_root / "ensembles_manifest.csv"
    if master_rows:
        pd.DataFrame(master_rows).to_csv(manifest_path, index=False)
        print("[ENSEMBLE] Wrote manifest:", manifest_path)
    else:
        print("[ENSEMBLE] No ensembles computed (no per-model outputs found).")

# Example invocation:
# compute_ensembles_from_outputs(OUTPUT_ROOT)

def main():
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    tel_gdf = load_telangana_districts(DISTRICTS_PATH)

    for model in MODELS:
        for scenario, sconf in SCENARIOS.items():
            process_model_for_scenario(model, scenario, sconf, tel_gdf)


if __name__ == "__main__":
    main()
    compute_ensembles_from_outputs(OUTPUT_ROOT)