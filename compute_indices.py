#!/usr/bin/env python3
"""
Uniform, future-proof index pipeline for the India Resilience Tool.

- Metric-per-root layout (e.g., tas_gt32, rain_gt_2p5mm)
- Identical filenames across all metrics:
    <district>_yearly.csv
    <district>_periods.csv
- Validation reports:
    <metric>/<state>/validation_reports/<model>/<scenario>/file_validation_report.csv
- Ensembles (generic, metric-agnostic via `value` column):
    <metric>/<state>/ensembles/<district>/<scenario>/...
- Yearly/Periods CSVs always contain BOTH:
    - generic `value`
    - metric-specific column (e.g., `days_gt_32C`, `days_rain_gt_2p5mm`)

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
from affine import Affine
import logging
import traceback
import json

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

# Root where all model/scenario data live
DATA_ROOT = Path(r"D:\projects\irt_data\r1i1p1f1")

# Shapefile / GeoJSON with all India districts
DISTRICTS_PATH = Path(r"D:\projects\irt_data\districts_4326.geojson")

# Base output root; each metric uses its own subfolder under this
BASE_OUTPUT_ROOT = Path(r"D:\projects\irt_data\processed")

# Scenarios (periods define requested averaging windows)
SCENARIOS = {
    "historical": {
        "subdir": "historical/tas",  # base points at tas; for other vars we swap the tail via helper
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

# Global coverage rules for period aggregation
MIN_YEARS_REQUIRED_FRACTION = 0.6
MIN_YEARS_ABSOLUTE = 5

# -----------------------------------------------------------------------------
# METRIC REGISTRY
# -----------------------------------------------------------------------------
# Add future indices here. Each entry fully governs I/O + computation.
# name: human label (optional, for metadata)
# slug: folder name under BASE_OUTPUT_ROOT
# var:  input CMIP variable for this metric ('tas', 'pr', 'tasmax', 'tasmin', ...)
# value_col: metric-specific column to keep (in addition to generic 'value')
# units: unit string for 'value'
# compute: function key resolved at runtime to compute the yearly day-count
# params: args passed to the compute function
METRICS = [
    {
        "name": "Days > 32 °C (tas)",
        "slug": "tas_gt32",
        "var": "tas",
        "value_col": "days_gt_32C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 32.0 + 273.15},  # 305.15 K
    },
    {
        "name": "Rainy Day > 2.5 mm (pr)",
        "slug": "rain_gt_2p5mm",
        "var": "pr",
        "value_col": "days_rain_gt_2p5mm",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 2.5},
    },
    {
        "name": "Consecutive Summer Days (tasmax > 30 °C)",
        "slug": "tasmax_csd_gt30",
        "var": "tasmax",
        "value_col": "consec_summer_days_gt_30C",
        "units": "days",
        "compute": "max_consecutive_summer_days",
        "params": {"thresh_k": 30.0 + 273.15},
    },
    {
        "name": "Tropical Nights (tasmin > 20 °C)",
        "slug": "tasmin_tropical_nights_gt20",
        "var": "tasmin",
        "value_col": "tropical_nights_gt_20C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 20.0 + 273.15},
    },
    {
        "name": "Heat Wave Duration Index (HWDI)",
        "slug": "hwdi_tasmax_plus5C",       # or whatever slug you're using
        "var": "tasmax",
        "value_col": "hwdi_max_spell_len",
        "units": "days",
        "compute": "heatwave_duration_index",
        "params": {
            "min_spell_days": 5,
        },
    },
    {
        "name": "Heat Wave Frequency Index (HWFI)",
        "slug": "hwfi_tmean_90p",
        "var": "tas",
        "value_col": "hwfi_days_in_spells",
        "units": "days",
        "compute": "heatwave_frequency_index",
        "params": {
            "quantile": 0.9,
            "min_spell_days": 5,
        },
    },
]


# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def metric_root(slug: str) -> Path:
    root = BASE_OUTPUT_ROOT / slug
    root.mkdir(parents=True, exist_ok=True)
    return root

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

def pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    units = (getattr(da, "attrs", {}).get("units", "") or "").strip().lower()
    if units in {"kg m-2 s-1", "kg m-2 s^-1", "kg/m^2/s"}:
        return da * 86400.0
    return da

def load_telangana_districts(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)

    # Find a state column we can use
    candidate_state_cols = ["STATE_UT", "state_ut", "STATE", "STATE_LGD", "ST_NM", "state_name"]
    state_col = next((c for c in candidate_state_cols if c in gdf.columns), None)
    if not state_col:
        raise ValueError(f"Could not find a state column in {path}. Columns: {list(gdf.columns)}")

    # Normalize state strings
    s = gdf[state_col].astype(str)
    s = s.str.normalize("NFKC").str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    gdf["_state_norm"] = s

    # Filter Telangana (handle common variants)
    tel_keys = {"telangana", "telengana", "telangana state"}
    tel = gdf[gdf["_state_norm"].isin(tel_keys)]
    if tel.empty:
        all_states = sorted(gdf["_state_norm"].dropna().unique().tolist())
        raise ValueError(
            "No rows for Telangana/Telengana found after normalization. "
            f"Available states (normalized) include: {all_states[:20]}{' …' if len(all_states)>20 else ''}"
        )

    # Ensure CRS
    if tel.crs is None:
        tel = tel.set_crs("EPSG:4326")

    # Clean up
    tel = tel.drop(columns=["_state_norm"])

    # Light log
    logging.info(f"Loaded Telangana districts: {len(tel)}")
    sample_cols = [c for c in ["DISTRICT", "district", "STATE_UT", state_col] if c in tel.columns]
    if sample_cols:
        logging.info("District/state sample:\n%s", tel[sample_cols].head())

    return tel


def build_district_masks(telangana_gdf: gpd.GeoDataFrame, sample_ds: xr.Dataset,
                         district_name_col: str = "DISTRICT") -> dict:
    if district_name_col not in telangana_gdf.columns:
        raise ValueError(f"'{district_name_col}' not found in GDF.")
    ds = sample_ds
    lats = ds["lat"].values
    lons = ds["lon"].values
    height, width = lats.size, lons.size

    # build affine
    if np.all(np.diff(lons) > 0):
        xres = lons[1] - lons[0]
        xoff = lons[0] - xres / 2
    else:
        raise ValueError("Longitude not strictly increasing.")
    if np.all(np.diff(lats) < 0):
        yres = lats[1] - lats[0]  # negative
        yoff = lats[0] - yres / 2
    else:
        yres = lats[1] - lats[0]
        yoff = lats[0] - yres / 2

    transform = Affine.translation(xoff, yoff) * Affine.scale(xres, yres)

    masks = {}
    for _, row in telangana_gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        name = str(row[district_name_col]).strip()
        mask = features.rasterize([(geom, 1)], out_shape=(height, width), transform=transform,
                                  fill=0, all_touched=True, dtype="uint8")
        mask_da = xr.DataArray(mask.astype(bool), coords={"lat": ds["lat"], "lon": ds["lon"]},
                               dims=("lat", "lon"), name="mask")
        masks[name] = mask_da
    logging.info(f"Built {len(masks)} district masks.")
    return masks

def count_days_above_threshold(da_tas: xr.DataArray, mask: xr.DataArray, thresh_k: float) -> int:
    tas_masked = da_tas.where(mask)
    daily_mean = tas_masked.mean(dim=("lat", "lon"), skipna=True)
    return int((daily_mean > thresh_k).sum().item())

def count_rainy_days(da_pr: xr.DataArray, mask: xr.DataArray, thresh_mm: float = 2.5) -> int:
    pr_mmday = pr_to_mm_per_day(da_pr)
    daily_mean = pr_mmday.where(mask).mean(dim=("lat", "lon"), skipna=True)
    return int((daily_mean > thresh_mm).sum().item())

def _run_length_stats(mask: np.ndarray, min_len: int) -> tuple[int, int]:
    """
    Given a 1D boolean array `mask`, return:
      - max_run: longest length of consecutive True values (only counting runs >= min_len)
      - total_days: total number of True days that belong to runs of length >= min_len
    """
    max_run = 0
    total_days = 0
    current = 0

    for v in mask:
        if v:
            current += 1
        else:
            if current >= min_len:
                total_days += current
                if current > max_run:
                    max_run = current
            current = 0

    # handle trailing run
    if current >= min_len:
        total_days += current
        if current > max_run:
            max_run = current

    return max_run, total_days


def heatwave_duration_index(
    da_tasmax: xr.DataArray,
    mask: xr.DataArray,
    anomaly_thresh_k: float = 5.0,       # kept for signature compatibility, not used directly
    abs_thresh_k: float = 40.0 + 273.15, # 40°C in Kelvin
    min_spell_days: int = 5,
) -> int:
    """
    Heat Wave Duration Index (HWDI) for a single district and year.

    Practical definition used here:
      - Compute district-mean daily tasmax.
      - Define a yearly "hot-day" threshold as the max of:
          * the 90th percentile of daily tasmax for that year, and
          * an absolute threshold abs_thresh_k (default 40°C in K).
      - Flag a "heatwave day" when tasmax >= that threshold.
      - Identify all spells of consecutive heatwave days; HWDI is the
        length of the longest spell with length >= min_spell_days.

    Returns 0 if there are no qualifying spells.

    NOTE: This is a percentile-based variant of the IPCC/IMD-style definition.
    If you later precompute a 30-year DOY climatology, you can swap the
    threshold logic to use (climatology + 5°C) instead.
    """
    tasmax_masked = da_tasmax.where(mask)
    daily_mean = tasmax_masked.mean(dim=("lat", "lon"), skipna=True)

    if "time" not in daily_mean.dims:
        raise ValueError("Expected 'time' dimension in tasmax DataArray for HWDI.")

    # Drop pure-NaN days (just in case)
    daily_mean = daily_mean.dropna(dim="time", how="all")

    if daily_mean.size == 0:
        return 0

    # Yearly 90th percentile of district-mean tasmax
    year_p90 = float(daily_mean.quantile(0.9).item())

    # Heatwave threshold: high absolute temp AND extreme relative to the year's distribution
    thresh = max(abs_thresh_k, year_p90)

    hw = daily_mean >= thresh
    hw = hw.fillna(False)
    arr = np.asarray(hw.values, dtype=bool)

    max_run, _ = _run_length_stats(arr, min_len=min_spell_days)
    return int(max_run)

def heatwave_frequency_index(
    da_tas: xr.DataArray,
    mask: xr.DataArray,
    quantile: float = 0.9,
    min_spell_days: int = 5,
) -> int:
    """
    Heat Wave Frequency Index (HWFI) for a single district and year.

    Practical definition used here (warm-spell days style):
      - Compute district-mean daily tas (Tmean).
      - Compute the yearly 90th percentile threshold of Tmean.
      - Flag "hot-mean" days where tas > threshold.
      - Identify all spells of consecutive hot-mean days with length >= min_spell_days.
      - HWFI is the total number of days belonging to such spells.

    Returns 0 if there are no qualifying spells.

    NOTE: This mirrors the WSDI / warm spell days idea but uses yearly
    percentiles instead of a fixed multi-decade DOY climatology. You
    can later swap in DOY-based thresholds computed from 1990–2010 data.
    """
    tas_masked = da_tas.where(mask)
    daily_mean = tas_masked.mean(dim=("lat", "lon"), skipna=True)

    if "time" not in daily_mean.dims:
        raise ValueError("Expected 'time' dimension in tas DataArray for HWFI.")

    daily_mean = daily_mean.dropna(dim="time", how="all")
    if daily_mean.size == 0:
        return 0

    # Yearly 90th percentile threshold for daily mean temperature
    thresh = float(daily_mean.quantile(quantile).item())

    hot = daily_mean > thresh
    hot = hot.fillna(False)
    arr = np.asarray(hot.values, dtype=bool)

    _, total_days = _run_length_stats(arr, min_len=min_spell_days)
    return int(total_days)

def max_consecutive_summer_days(
    da_tasmax: xr.DataArray,
    mask: xr.DataArray,
    thresh_k: float = 30.0 + 273.15,
) -> int:
    """
    Return the maximum number of consecutive 'summer days' in a year
    for a single district.

    A summer day is defined as a day where the district-mean tasmax > thresh_k.
    """
    # Apply the district mask and compute district-mean tasmax per day
    tasmax_masked = da_tasmax.where(mask)
    daily_mean = tasmax_masked.mean(dim=("lat", "lon"), skipna=True)

    # Boolean series: True on "summer days"
    summer = (daily_mean > thresh_k)

    # Treat NaNs as non-summer days
    summer = summer.fillna(False)

    arr = summer.values
    if arr.size == 0:
        return 0

    # Ensure boolean dtype
    arr = arr.astype(bool)

    # Compute maximum run length of consecutive True values
    max_run = 0
    current = 0
    for v in arr:
        if v:
            current += 1
            if current > max_run:
                max_run = current
        else:
            current = 0

    return int(max_run)

def yearly_files_for_dir(dirpath: Path) -> dict:
    files = glob.glob(str(dirpath / "*.nc"))
    out = {}
    for f in files:
        y = os.path.splitext(os.path.basename(f))[0]
        if y.isdigit():
            out[int(y)] = Path(f)
    return dict(sorted(out.items()))

def var_data_dir(data_root: Path, scenario_subdir: str, varname: str, model: str) -> Path:
    base = Path(scenario_subdir)
    parts = list(base.parts)
    if not parts:
        raise ValueError(f"Invalid scenario_subdir: {scenario_subdir}")
    parts[-1] = varname
    return data_root / Path(*parts) / model

def try_open_nc(path: Path, try_engines=("netcdf4", "h5netcdf", "scipy")) -> bool:
    for eng in try_engines:
        try:
            xr.open_dataset(path, engine=eng).close()
            return True
        except Exception:
            continue
    return False

def inspect_file_magic(path: Path, nbytes=8) -> str:
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

def validated_year_files(data_dir: Path) -> tuple[dict, dict]:
    year_files = yearly_files_for_dir(data_dir)
    valid, bad = {}, {}
    if not year_files:
        return valid, bad
    logging.info(f"Found {len(year_files)} candidate year files in {data_dir}")
    for year, p in year_files.items():
        try:
            sz = p.stat().st_size
        except Exception as e:
            bad[year] = {"path": p, "reason": f"stat_failed: {e}", "magic": None}
            continue
        if sz == 0:
            bad[year] = {"path": p, "reason": "zero_size", "magic": None}
            continue
        if try_open_nc(p):
            valid[year] = p
        else:
            magic = inspect_file_magic(p)
            bad[year] = {"path": p, "reason": "open_failed", "magic": magic}
    logging.info(f"Valid files: {len(valid)} ; Bad files: {len(bad)}")
    return dict(sorted(valid.items())), bad

def existing_processed_years_for_model_scenario(
    metric_root_path: Path,
    state: str,
    model: str,
    scenario: str,
) -> set[int]:
    """
    Return the set of years that have already been processed for this
    (metric, state, model, scenario) combination.

    We inspect the first district that has a <district>_yearly.csv.
    If nothing exists yet, we return an empty set.
    """
    state_dir = metric_root_path / state
    if not state_dir.exists():
        return set()

    for entry in state_dir.iterdir():
        if not entry.is_dir():
            continue
        # Skip non-district dirs
        if entry.name in {"validation_reports", "ensembles"}:
            continue

        yearly_csv = entry / model / scenario / f"{entry.name}_yearly.csv"
        if yearly_csv.exists():
            try:
                df = pd.read_csv(yearly_csv, usecols=["year"])
                years = set(int(y) for y in df["year"].dropna().unique())
                return years
            except Exception as e:
                logging.warning(
                    f"Could not read existing yearly CSV {yearly_csv} for "
                    f"{model}/{scenario}: {e}"
                )
                return set()

    return set()

def discover_models(data_root: Path, scenarios: dict) -> list:
    models = set()
    for _, scen_conf in scenarios.items():
        model_base = data_root / scen_conf["subdir"]
        if not model_base.exists():
            continue
        for entry in model_base.iterdir():
            if entry.is_dir():
                models.add(entry.name)
    return sorted(models)

MODELS = discover_models(DATA_ROOT, SCENARIOS)
if not MODELS:
    logging.warning("No model directories discovered; check DATA_ROOT and SCENARIOS.")
else:
    logging.info(f"Discovered models: {MODELS}")

# -----------------------------------------------------------------------------
# CORE PIPELINE
# -----------------------------------------------------------------------------
def process_metric_for_model_scenario(metric: dict,
                                      model: str,
                                      scenario: str,
                                      scenario_conf: dict,
                                      telangana_gdf: gpd.GeoDataFrame):
    """
    Process ONE metric for ONE (model, scenario).
    Writes standardized CSVs into that metric's root.
    """
    slug       = metric["slug"]
    var        = metric["var"]
    value_col  = metric["value_col"]
    compute_fn = globals()[metric["compute"]]
    params     = metric.get("params", {})

    metric_root_path = metric_root(slug)

    # -- Resolve data dir for this metric's variable --
    data_dir = var_data_dir(DATA_ROOT, scenario_conf["subdir"], var, model)
    if not data_dir.exists():
        logging.warning(f"[{slug}] Missing data dir: {data_dir}; skipping {model}/{scenario}.")
        return

    valid_year_files, bad_files = validated_year_files(data_dir)

    # -- Validation report path (fixed name for all metrics) --
    report_dir = metric_root_path / "Telangana" / "validation_reports" / model / scenario
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "file_validation_report.csv"
    if bad_files:
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
        logging.warning(f"[{slug}] Wrote bad-file report: {report_path}")
    else:
        logging.info(f"[{slug}] No bad files detected.")

    if not valid_year_files:
        logging.warning(f"[{slug}] No valid files to process in {data_dir}.")
        return

    # --- Consistency check: skip if all years already processed ---
    existing_years = existing_processed_years_for_model_scenario(
        metric_root_path,
        state="Telangana",
        model=model,
        scenario=scenario,
    )
    if existing_years:
        current_years = set(valid_year_files.keys())
        if current_years.issubset(existing_years):
            logging.info(
                f"[{slug}] All years {sorted(current_years)} already processed for "
                f"Telangana/{model}/{scenario}; skipping recomputation."
            )
            return
        else:
            missing = sorted(current_years - existing_years)
            logging.info(
                f"[{slug}] Existing yearly outputs for {model}/{scenario}: "
                f"{sorted(existing_years)}; missing years in NetCDF: {missing}. "
                f"Recomputing all years for consistency."
            )

    # -- Build masks from a sample file of THIS metric/variable (grid-safe) --
    sample_year, sample_path = next(iter(valid_year_files.items()))
    logging.info(f"[{slug}] Using sample file for masks: {sample_path}")
    ds_sample = xr.open_dataset(sample_path)
    ds_sample = normalize_lat_lon(ds_sample)
    if var not in ds_sample:
        ds_sample.close()
        raise ValueError(f"[{slug}] '{var}' variable not found in sample {sample_path}")
    masks = build_district_masks(telangana_gdf, ds_sample, district_name_col="DISTRICT")
    ds_sample.close()

    # -- Yearly computation --
    rows = []
    for year, nc_path in valid_year_files.items():
        logging.info(f"[{slug}] Processing {year} :: {nc_path}")
        try:
            ds = xr.open_dataset(nc_path)
        except Exception as e:
            logging.error(f"[{slug}] Failed to open {nc_path}: {e}")
            logging.debug(traceback.format_exc())
            continue

        ds = normalize_lat_lon(ds)
        if var not in ds:
            logging.error(f"[{slug}] '{var}' missing in {nc_path}, skipping year {year}")
            ds.close()
            continue
        da = ds[var]

        for dist_name, mask_da in masks.items():
            try:
                if compute_fn is count_rainy_days:
                    # pr unit safety
                    v = compute_fn(da, mask_da, **params)
                else:
                    v = compute_fn(da, mask_da, **params)
            except Exception as e:
                logging.error(f"[{slug}] Compute error for '{dist_name}', {year}, {nc_path}: {e}")
                logging.debug(traceback.format_exc())
                v = None

            # store both generic and metric-specific columns
            rows.append({
                "district": dist_name,
                "model": model,
                "scenario": scenario,
                "year": year,
                "value": v,                   # generic, metric-agnostic
                value_col: v,                 # metric-specific (legacy-friendly)
                "source_file": str(nc_path),
            })
        ds.close()

    df_yearly = pd.DataFrame(rows)

    # -- Period aggregation with coverage checks --
    period_frames = []
    for period_name, (y0, y1) in scenario_conf["periods"].items():
        requested_years = list(range(y0, y1 + 1))
        available_years = sorted([y for y in valid_year_files.keys() if y0 <= y <= y1])
        n_req, n_avail = len(requested_years), len(available_years)
        frac = (n_avail / n_req) if n_req > 0 else 0.0
        logging.info(f"[{slug}] Period {period_name}: requested {n_req}, available {n_avail} (frac={frac:.2f})")

        if (n_avail >= MIN_YEARS_ABSOLUTE) and (frac >= MIN_YEARS_REQUIRED_FRACTION):
            df_sub = df_yearly[df_yearly["year"].isin(available_years)]
            grp = (
                df_sub.groupby(["district", "model", "scenario"])
                .agg({"value": "mean"})
                .reset_index()
            )
            grp["period"] = period_name
            grp["years_used_count"] = n_avail
            grp["years_requested"] = n_req

            # also include the metric-specific column as the same mean for clarity
            grp[value_col] = grp["value"]

            period_frames.append(grp)
        else:
            logging.warning(f"[{slug}] Insufficient coverage for {period_name}; skipping.")

    if period_frames:
        df_periods = pd.concat(period_frames, ignore_index=True)
    else:
        df_periods = pd.DataFrame(columns=["district", "model", "scenario", "period",
                                           "value", value_col, "years_used_count", "years_requested"])

    # -- Write standardized outputs --
    for dist_name in df_yearly["district"].unique():
        out_dir = metric_root_path / "Telangana" / dist_name.replace(" ", "_") / model / scenario
        out_dir.mkdir(parents=True, exist_ok=True)

        # Yearly
        df_d = df_yearly[df_yearly["district"] == dist_name].sort_values("year")
        df_d.to_csv(out_dir / f"{dist_name.replace(' ', '_')}_yearly.csv", index=False)

        # Periods
        df_p = df_periods[df_periods["district"] == dist_name]
        df_p.to_csv(out_dir / f"{dist_name.replace(' ', '_')}_periods.csv", index=False)

    logging.info(f"[{slug}] Finished {model}/{scenario}. Output in {metric_root_path}/Telangana/...")

def compute_ensembles_generic(output_root: Path, state: str = "Telangana",
                              write_yearly_ensemble: bool = True,
                              write_period_ensemble: bool = True):
    """
    Metric-agnostic ensembles reader:
    - Expects `_yearly.csv` and `_periods.csv`
    - Uses the generic `value` column for stats
    """
    root = Path(output_root)
    state_root = root / state
    ensembles_root = state_root / "ensembles"
    ensembles_root.mkdir(parents=True, exist_ok=True)

    district_dirs = sorted([p for p in state_root.iterdir()
                            if p.is_dir() and p.name not in {"validation_reports", "ensembles"}])
    if not district_dirs:
        print("[ENSEMBLE] No district directories under", state_root)
        return

    master_rows = []
    for ddir in district_dirs:
        district = ddir.name
        model_dirs = [p for p in ddir.iterdir() if p.is_dir()]

        scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
        for scenario in scenarios:
            model_periods, model_yearly = [], []

            for m in model_dirs:
                period_csv = m / scenario / f"{district.replace(' ', '_')}_periods.csv"
                yearly_csv = m / scenario / f"{district.replace(' ', '_')}_yearly.csv"

                if period_csv.exists():
                    try:
                        dfp = pd.read_csv(period_csv)
                        # must have 'value'; if not, try to backfill from known cols
                        if "value" not in dfp.columns:
                            known_value_cols = [c for c in dfp.columns if c not in
                                                {"district","model","scenario","period","years_used_count","years_requested"}]
                            if known_value_cols:
                                dfp["value"] = dfp[known_value_cols[0]]
                        dfp["model"] = m.name
                        model_periods.append(dfp)
                    except Exception as e:
                        print(f"[WARN] Could not read {period_csv}: {e}")

                if yearly_csv.exists():
                    try:
                        dfy = pd.read_csv(yearly_csv)
                        if "value" not in dfy.columns:
                            known_value_cols = [c for c in dfy.columns if c not in
                                                {"district","model","scenario","year","source_file"}]
                            if known_value_cols:
                                dfy["value"] = dfy[known_value_cols[0]]
                        dfy["model"] = m.name
                        model_yearly.append(dfy)
                    except Exception as e:
                        print(f"[WARN] Could not read {yearly_csv}: {e}")

            # Period ensembles
            if model_periods and write_period_ensemble:
                df_all = pd.concat(model_periods, ignore_index=True)
                for period_name, sub in df_all.groupby("period"):
                    vals = sub["value"].dropna().astype(float).to_numpy()
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
                    }
                    out_dir = ensembles_root / district / scenario
                    out_dir.mkdir(parents=True, exist_ok=True)
                    periods_csv = out_dir / f"{district.replace(' ', '_')}_periods_ensemble.csv"
                    pd.DataFrame([out]).to_csv(periods_csv, mode="a", header=not periods_csv.exists(), index=False)

                    master_rows.append({**out, "type": "period"})

            # Yearly ensembles
            if model_yearly and write_yearly_ensemble:
                df_yc = pd.concat(model_yearly, ignore_index=True)
                if "year" not in df_yc.columns:
                    # If year is missing, skip yearly ensemble
                    continue
                df_yc["year"] = df_yc["year"].astype(int)
                pivot = df_yc.pivot_table(index="year", columns="model", values="value", aggfunc="first")

                yearly_summary = pd.DataFrame({
                    "year": pivot.index,
                    "n_models": pivot.count(axis=1),
                    "ensemble_mean": pivot.mean(axis=1, skipna=True),
                    "ensemble_std": pivot.std(axis=1, ddof=0, skipna=True),
                    "ensemble_median": pivot.median(axis=1, skipna=True),
                    "ensemble_p05": pivot.quantile(0.05, axis=1),
                    "ensemble_p95": pivot.quantile(0.95, axis=1),
                }).reset_index(drop=True)

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

    manifest_path = ensembles_root / "ensembles_manifest.csv"
    if master_rows:
        pd.DataFrame(master_rows).to_csv(manifest_path, index=False)
        print("[ENSEMBLE] Wrote manifest:", manifest_path)
    else:
        print("[ENSEMBLE] No ensembles computed (no per-model outputs found).")

# -----------------------------------------------------------------------------
# DRIVER
# -----------------------------------------------------------------------------
def main():
    # Ensure metric roots exist early
    for m in METRICS:
        metric_root(m["slug"])
    tel_gdf = load_telangana_districts(DISTRICTS_PATH)
    # print(MODELS)
    for model in MODELS:
        for scenario, sconf in SCENARIOS.items():
            for metric in METRICS:
                process_metric_for_model_scenario(metric, model, scenario, sconf, tel_gdf)

if __name__ == "__main__":
    errors = []
    try:
        main()
    except Exception as e:
        logging.error(f"Fatal error in main(): {e}")
        logging.debug(traceback.format_exc())
        raise

    # After computation, build ensembles per metric (generic reader on 'value')
    for metric in METRICS:
        try:
            compute_ensembles_generic(metric_root(metric["slug"]), state="Telangana")
        except Exception as e:
            logging.error(f"Ensembles failed for {metric['slug']}: {e}")
            logging.debug(traceback.format_exc())
            errors.append((metric["slug"], str(e)))

    if errors:
        logging.warning(f"Some ensemble steps failed: {errors}")
