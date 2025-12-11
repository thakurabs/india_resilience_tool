#!/usr/bin/env python3
from __future__ import annotations
import io, os, re, json, zipfile, shutil, subprocess, unicodedata, difflib
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from functools import lru_cache
import textwrap

import numpy as np
import pandas as pd
import geopandas as gpd
import streamlit as st
from streamlit_folium import st_folium
import folium
import matplotlib.colors as mcolors
import matplotlib.cm as mpcm
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.ops import transform

from matplotlib.backends.backend_pdf import PdfPages

# -------------------------
# DEBUG
# -------------------------
DEBUG = bool(int(os.getenv("IRT_DEBUG", "0")))

def dbg(*args, **kwargs):
    if DEBUG:
        st.write(*args, **kwargs)

# -------------------------
# CONFIG
# -------------------------
# PROJECT_ROOT = Path(__file__).resolve().parent
# DATA_DIR = Path(r"D:\projects\irt_data\\")
# DATA_DIR.mkdir(parents=True, exist_ok=True)
from paths import DATA_DIR

ADM2_GEOJSON = DATA_DIR / "districts_4326.geojson"
ATTACH_DISTRICT_GEOJSON = str(ADM2_GEOJSON) if ADM2_GEOJSON.exists() else None
OUTDIR = DATA_DIR
LOGO_PATH = "./resilience_actions_logo_transparent.png"

SIMPLIFY_TOL_ADM2 = 0.015
SIMPLIFY_TOL_ADM1 = 0.01

MIN_LON, MAX_LON = 68.0, 97.5
MIN_LAT, MAX_LAT = 5, 45.0

# ---- Figure / font styling for dashboard panels ----
# These are used for the main small-panel figures (trend + scenario comparison)
FIG_SIZE_PANEL: tuple[float, float] = (4.8, 2.4)  # width, height in inches
FIG_DPI_PANEL: int = 150

FONT_SIZE_TITLE: int = 9
FONT_SIZE_LABEL: int = 8
FONT_SIZE_TICKS: int = 8
FONT_SIZE_LEGEND: int = 8

# ---- Variable/Index registry ----
# Each entry maps an "index slug" to:
#  - label: what the user sees in the Index dropdown
#  - periods_metric_col: the base metric name in master CSV (<metric>__<scenario>__<period>__<stat>)
#  - description: human-readable definition used in tooltips
#  - file patterns for district/state yearly series discovery
VARIABLES = {
    "tas_gt32": {
        "label": "Summer Days",
        "group": "temperature",
        "periods_metric_col": "days_gt_32C",
        "description": (
            "Number of days in a year on which the district-average daily maximum near-surface "
            "air temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_csd_gt30": {
        "label": "Consecutive Summer Days",
        "group": "temperature",
        "periods_metric_col": "consec_summer_days_gt_30C",
        "description": (
            "For each year, the maximum length (in days) of any spell of consecutive days "
            "on which the district-average daily maximum temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_csd_events_gt30": {
        "label": "Consecutive Summer Day Events",
        "group": "temperature",
        "periods_metric_col": "csd_events_gt_30C",
        "description": (
            "Number of distinct ‘Consecutive Summer Day’ spells per year, where each spell "
            "is a run of at least 5 consecutive days on which the district-average daily "
            "maximum temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_tropical_nights_gt20": {
        "label": "Tropical Nights",
        "group": "temperature",
        "periods_metric_col": "tropical_nights_gt_20C",
        "description": (
            "Number of nights in a year on which the district-average daily minimum "
            "temperature exceeds 20 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwdi_tasmax_plus5C": {
        "label": "Heat Wave Duration Index (HWDI, #Days)",
        "group": "temperature",
        "periods_metric_col": "hwdi_max_spell_len",
        "description": (
            "For each year, the length (in days) of the longest heat-wave spell. "
            "Heat-wave spells are defined from days on which the district-average daily "
            "maximum temperature is at least about 5 °C warmer than its local historical "
            "normal (i.e. persistent, unusually hot days)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwfi_tmean_90p": {
        "label": "Heat Wave Frequency Index (HWFI, #Days)",
        "group": "temperature",
        "periods_metric_col": "hwfi_days_in_spells",
        "description": (
            "For each year, the total number of days that occur inside heat-wave spells. "
            "Heat-wave spells are identified using the district-average daily mean "
            "temperature exceeding a high threshold (around the 90th percentile of the "
            "historical distribution) and persisting for several consecutive days."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwdi_events_tasmax_plus5C": {
        "label": "Heat Wave Duration Index (HWDI, #Events)",
        "group": "temperature",
        "periods_metric_col": "hwdi_events_count",
        "description": (
            "Number of distinct heat-wave spells per year for the HWDI definition "
            "(spells of unusually hot days based on daily maximum temperature being "
            "roughly ≥5 °C above its local historical normal)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwfi_events_tmean_90p": {
        "label": "Heat Wave Frequency Index (HWFI, #Events)",
        "group": "temperature",
        "periods_metric_col": "hwfi_events_count",
        "description": (
            "Number of distinct heat-wave spells per year for the HWFI definition "
            "(spells of several consecutive days on which the district-average daily "
            "mean temperature exceeds a high percentile threshold, ~90th percentile)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_annual_mean": {
        "label": "Annual Max Temperature",
        "group": "temperature",
        "periods_metric_col": "annual_tasmax_mean_C",
        "description": (
            "Annual mean of daily maximum near-surface air temperature (tasmax), in °C, "
            "averaged over all days in the year for each district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_summer_mean": {
        "label": "Summer Max Temperature",
        "group": "temperature",
        "periods_metric_col": "summer_tasmax_mean_C",
        "description": (
            "Mean of daily maximum temperature (tasmax), in °C, averaged over the "
            "summer season (March–May) for each year and district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_annual_mean": {
        "label": "Annual Min Temperature",
        "group": "temperature",
        "periods_metric_col": "annual_tasmin_mean_C",
        "description": (
            "Annual mean of daily minimum near-surface air temperature (tasmin), in °C, "
            "averaged over all days in the year for each district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_winter_mean": {
        "label": "Winter Min Temperature",
        "group": "temperature",
        "periods_metric_col": "winter_tasmin_mean_C",
        "description": (
            "Mean of daily minimum temperature (tasmin), in °C, averaged over the winter "
            "season (December–February) for each year and district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "rain_gt_2p5mm": {
        "label": "Rainy days",
        "group": "rain",
        "periods_metric_col": "days_rain_gt_2p5mm",
        "description": (
            "Number of days in a year on which the district-average daily rainfall "
            "exceeds 2.5 mm/day."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
        "pr_simple_daily_intensity": {
        "label": "Simple Daily Intensity",
        "group": "rain",
        "periods_metric_col": "simple_daily_intensity_mm_per_day",
        "description": (
            "Ratio of total precipitation to the number of days with precipitation "
            "≥ 1 mm, over the selected period (mm/day)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_max_1day_precip": {
        "label": "Maximum 1-day Precipitation",
        "group": "rain",
        "periods_metric_col": "max_1day_precip_mm",
        "description": (
            "Seasonal or period-wise maximum of district-average daily precipitation "
            "over any single day (mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_max_5day_precip": {
        "label": "Highest Consecutive 5-day Precipitation",
        "group": "rain",
        "periods_metric_col": "max_5day_precip_mm",
        "description": (
            "Maximum total precipitation accumulated over any consecutive 5-day period "
            "within the selected years (mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_5day_precip_events_gt50mm": {
        "label": "Consecutive 5-day Precipitation Events (> 50 mm)",
        "group": "rain",
        "periods_metric_col": "consec_5day_precip_events",
        "description": (
            "Number of separate 5-day periods in which the total precipitation "
            "exceeds 50 mm (events per period)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_heavy_precip_days_gt10mm": {
        "label": "Heavy Precipitation Days (> 10 mm)",
        "group": "rain",
        "periods_metric_col": "heavy_precip_days_gt_10mm",
        "description": (
            "Number of days in the year with district-average daily precipitation "
            "greater than 10 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_very_heavy_precip_days_gt25mm": {
        "label": "Very Heavy Precipitation Days (> 25 mm)",
        "group": "rain",
        "periods_metric_col": "very_heavy_precip_days_gt_25mm",
        "description": (
            "Number of days in the year with district-average daily precipitation "
            "greater than 25 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_consecutive_dry_days_lt1mm": {
        "label": "Consecutive Dry Days (< 1 mm)",
        "group": "rain",
        "periods_metric_col": "consecutive_dry_days",
        "description": (
            "Longest stretch within the period of consecutive dry days with "
            "daily precipitation less than 1 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_consecutive_dry_day_events_gt5": {
        "label": "Consecutive Dry Day Events (> 5 days)",
        "group": "rain",
        "periods_metric_col": "consecutive_dry_day_events",
        "description": (
            "Number of separate periods with more than 5 consecutive dry days "
            "(daily precipitation < 1 mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },

}

INDEX_GROUP_LABELS = {
    "temperature": "Temperature",
    "rain": "Rainfall",
}


# ---------- Name normalization / aliases ----------
def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

NAME_ALIASES = {
    "hanamkonda": "hanumakonda",
    "j b r bhupalpally": "jayashankar bhupalpalli",
    "jayashankar bhupalpally": "jayashankar bhupalpalli",
    "b r ambedkar bhupalpalli": "jayashankar bhupalpalli",
    "bhadradri kothagudem": "bhadradri kothagudem",
    "jogulamba gadwal": "jogulamba gadwal",
}

def alias(s: str) -> str:
    k = normalize_name(s)
    return NAME_ALIASES.get(k, k)

# -------------------------
# Geo load / prep
# -------------------------
@st.cache_data
def load_local_adm2(path: str, tolerance: float = SIMPLIFY_TOL_ADM2) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)

    def drop_z(geom):
        try:
            return transform(lambda x, y, z=None: (x, y), geom)
        except Exception:
            return geom

    gdf["geometry"] = gdf["geometry"].apply(drop_z)
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")

    if "DISTRICT" in gdf.columns:
        gdf["district_name"] = gdf["DISTRICT"].astype(str).str.strip()
    else:
        txt_cols = [c for c in gdf.columns if gdf[c].dtype == object and c != "geometry"]
        gdf["district_name"] = gdf[txt_cols[0]].astype(str).str.strip() if txt_cols else gdf.index.astype(str)

    if "STATE_UT" in gdf.columns:
        gdf["state_name"] = gdf["STATE_UT"].astype(str).str.strip()
    elif "STATE_LGD" in gdf.columns:
        gdf["state_name"] = gdf["STATE_LGD"].astype(str)
    else:
        gdf["state_name"] = "Unknown"

    try:
        gdf = gdf.cx[MIN_LON:MAX_LON, MIN_LAT:MAX_LAT]
    except Exception:
        gdf = gdf[
            gdf.geometry.centroid.x.between(MIN_LON, MAX_LON)
            & gdf.geometry.centroid.y.between(MIN_LAT, MAX_LAT)
        ]

    gdf["geometry"] = gdf["geometry"].simplify(tolerance, preserve_topology=True)
    gdf = gdf[gdf.geometry.area > 0.0003].reset_index(drop=True)
    return gdf

if not ADM2_GEOJSON.exists():
    st.set_page_config(page_title="India Resilience Tool", layout="wide")
    st.error(f"ADM2 geojson not found at {ADM2_GEOJSON}. Place your districts_4326.geojson at this path.")
    st.stop()

adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)
adm2["__key"] = adm2["district_name"].map(alias)

@st.cache_data
def build_adm1_from_adm2(_adm2_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    adm2_gdf = _adm2_gdf.copy()
    adm1 = adm2_gdf.dissolve(by="state_name", as_index=False)
    if "state_name" not in adm1.columns and "index" in adm1.columns:
        adm1 = adm1.rename(columns={"index": "state_name"})
    if "shapeName" not in adm1.columns:
        adm1["shapeName"] = adm1["state_name"]
    return adm1.reset_index(drop=True)

@st.cache_data
def enrich_adm2_with_state_names(
    _adm2_gdf: gpd.GeoDataFrame,
    _adm1_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Attach/clean state_name for each district using a single spatial join.
    Cached so we don't redo expensive GeoPandas operations on every rerun.

    Note: leading underscores on arguments tell Streamlit *not* to hash
    these (GeoDataFrames are unhashable), so they are effectively treated
    as "external state" for caching purposes.
    """
    # Work on local copies so we don't mutate the originals
    adm2_gdf = _adm2_gdf.copy()
    adm1_gdf = _adm1_gdf.copy()

    if "state_name" not in adm2_gdf.columns:
        adm2_gdf["state_name"] = "Unknown"
    if "district_name" not in adm2_gdf.columns:
        adm2_gdf["district_name"] = adm2_gdf.index.astype(str)

    # Representative points for robust join
    adm2_pts = adm2_gdf.copy()
    adm2_pts["geometry"] = adm2_pts.geometry.representative_point()

    try:
        joined = gpd.sjoin(
            adm2_pts[["geometry"]],
            adm1_gdf[["geometry", "shapeName"]],
            how="left",
            predicate="within",
        )
        if "shapeName" in joined.columns:
            mapping = joined["shapeName"].to_dict()
            for adm2_idx, state_name_val in mapping.items():
                if pd.notna(state_name_val):
                    adm2_gdf.at[adm2_idx, "state_name"] = str(state_name_val).strip()
    except Exception:
        # If for some reason sjoin fails, fall back to existing state_name values.
        pass

    # Robust fallback: ensure no missing/blank state_name
    missing = adm2_gdf["state_name"].isna() | (
        adm2_gdf["state_name"].astype(str).str.strip() == ""
    )
    if missing.any():
        for idx in adm2_gdf[missing].index:
            val = adm2_gdf.at[idx, "state_name"]
            if not (pd.notna(val) and str(val).strip()):
                adm2_gdf.at[idx, "state_name"] = "Unknown"

    return adm2_gdf

# -------------------------
# Color helpers (no GeoJSON round-trip)
# -------------------------

import matplotlib as mpl

@lru_cache(maxsize=16)
def _get_cmap_hex_list(cmap_name: str) -> list[str]:
    """
    Small helper to cache colormap → list of hex colors.
    This avoids re-instantiating colormaps on every interaction.
    """
    cmap = mpl.colormaps.get_cmap(cmap_name)
    nsteps = 256
    return [mcolors.to_hex(cmap(i / (nsteps - 1))) for i in range(nsteps)]


def apply_fillcolor(
    merged_gdf: gpd.GeoDataFrame,
    metric_col: str,
    vmin: float,
    vmax: float,
    cmap_name: str = "Reds",
) -> gpd.GeoDataFrame:
    """
    Add a 'fillColor' column directly to the existing GeoDataFrame,
    avoiding expensive GeoJSON → GeoDataFrame conversions.

    - merged_gdf is modified in-place and also returned for convenience.
    - 'metric_col' is the column to color by (e.g. absolute or delta).
    """
    # Robust numeric series (align with merged_gdf index)
    vals = pd.to_numeric(
        merged_gdf.get(metric_col, pd.Series(index=merged_gdf.index, dtype=float)),
        errors="coerce",
    )

    # Default all fills to light grey
    arr = vals.to_numpy(dtype=float)
    fill = np.full(arr.shape, "#cccccc", dtype=object)

    # Handle valid values
    mask_valid = np.isfinite(arr)
    if np.any(mask_valid):
        vmin_eff = vmin
        vmax_eff = vmax

        # If vmin/vmax aren't sensible, fall back to data-driven ones
        if not np.isfinite(vmin_eff) or not np.isfinite(vmax_eff):
            vmin_eff = float(np.nanmin(arr[mask_valid]))
            vmax_eff = float(np.nanmax(arr[mask_valid]))

        if (
            not np.isfinite(vmin_eff)
            or not np.isfinite(vmax_eff)
            or vmin_eff == vmax_eff
        ):
            # Degenerate range – use mid-point for all
            t = np.full(arr.shape, 0.5, dtype=float)
        else:
            t = (arr - vmin_eff) / (vmax_eff - vmin_eff)

        # Clip to [0, 1]
        t = np.clip(t, 0.0, 1.0)

        # Map to colors only for valid entries
        cmap = plt.get_cmap(cmap_name)
        rgba = cmap(t[mask_valid])
        hex_valid = np.array(
            [
                "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))
                for r, g, b, _ in rgba
            ],
            dtype=object,
        )
        fill[mask_valid] = hex_valid

    merged_gdf["fillColor"] = fill
    merged_gdf["_metric_val"] = vals
    return merged_gdf


# -------------------------
# State metrics helper
# -------------------------
def compute_state_metrics_from_merged(
    merged_gdf: gpd.GeoDataFrame, adm1_gdf: gpd.GeoDataFrame, metric_col: str, sel_state: str
):
    ensemble = {"mean": None, "median": None, "p05": None, "p95": None, "std": None, "n_districts": 0}
    per_model = pd.DataFrame()

    sel_state_norm = str(sel_state).strip().lower()
    try:
        row_state = adm1_gdf[adm1_gdf["shapeName"].astype(str).str.strip().str.lower() == sel_state_norm]
        if row_state.empty:
            row_state = adm1_gdf[
                adm1_gdf["shapeName"].astype(str).str.strip().str.lower().str.contains(sel_state_norm, na=False)
            ]
        if not row_state.empty:
            poly = row_state.iloc[0].geometry
            try:
                mask = merged_gdf.geometry.within(poly.buffer(0.001))
            except Exception:
                mask = merged_gdf.geometry.centroid.within(poly.buffer(0.001))
        else:
            mask = pd.Series([False] * len(merged_gdf), index=merged_gdf.index)
    except Exception:
        mask = merged_gdf["state_name"].astype(str).str.strip().str.lower() == sel_state_norm

    if mask.sum() == 0:
        mask = merged_gdf["state_name"].astype(str).str.strip().str.lower() == sel_state_norm

    sel = merged_gdf[mask].copy()
    vals = pd.to_numeric(sel.get(metric_col, pd.Series([], dtype=float)), errors="coerce").dropna().to_numpy()
    if vals.size > 0:
        ensemble.update(
            mean=float(np.nanmean(vals)),
            median=float(np.nanmedian(vals)),
            p05=float(np.nanpercentile(vals, 5)),
            p95=float(np.nanpercentile(vals, 95)),
            std=float(np.nanstd(vals, ddof=0)),
            n_districts=int(vals.size),
        )

    try:
        metric_base = metric_col.rsplit("__", 1)[0]
        vpm_col = f"{metric_base}__values_per_model"
        if vpm_col in sel.columns:
            acc = {}
            for _, r in sel.iterrows():
                raw = r.get(vpm_col)
                d = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(d, dict):
                    for mname, v in d.items():
                        acc.setdefault(mname, []).append(float(v))
            if acc:
                rows = [
                    {"model": m, "value": float(pd.Series(vs).mean()), "n_districts": len(vs)}
                    for m, vs in acc.items()
                ]
                per_model = pd.DataFrame(rows).sort_values("model")
    except Exception:
        per_model = pd.DataFrame()

    return ensemble, per_model, sel

def extract_name_from_feature(feat):
    if not isinstance(feat, dict):
        return None
    props = feat.get("properties") or feat
    for key in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
        if isinstance(props, dict) and props.get(key):
            return props.get(key)
    if isinstance(props, dict):
        for k, v in props.items():
            if isinstance(v, str) and len(v) > 2 and "shape" not in k.lower():
                return v
    return None

# -------------------------
# Helpers for export
# -------------------------

def get_or_build_merged_for_index(
    adm2: gpd.GeoDataFrame,
    df: pd.DataFrame,
    slug: str,
    master_path: Path,
) -> gpd.GeoDataFrame:
    """Return a merged GeoDataFrame for this index, cached by master CSV mtime.

    This does the deterministic ADM2↔district join once per (slug, master_path.mtime)
    and reuses the result on subsequent reruns, instead of re-merging on every interaction.
    It also restricts ADM2 to states that actually appear in the master CSV.
    """
    merged_cache = st.session_state.setdefault("_merged_cache", {})

    try:
        mtime = master_path.stat().st_mtime
    except Exception:
        mtime = None

    cache_entry = merged_cache.get(slug)
    if cache_entry is not None and cache_entry.get("mtime") == mtime:
        return cache_entry["gdf"]

    adm2c = adm2.copy()
    dfc = df.copy()

    # Restrict ADM2 to states that occur in the master CSV (for this index)
    if "state_name" in adm2c.columns and "state" in dfc.columns:
        state_keys = dfc["state"].astype(str).map(alias)
        valid_states = set(state_keys.dropna().tolist())
        if valid_states:
            adm2c = adm2c[
                adm2c["state_name"].astype(str).map(alias).isin(valid_states)
            ].copy()

    # Deterministic district-level join using normalized keys
    if "__key" not in adm2c.columns:
        adm2c["__key"] = adm2c["district_name"].map(alias)
    dfc["__key"] = dfc["district"].map(alias)

    merged = adm2c.merge(dfc, on="__key", how="left", suffixes=("", "_csv")).drop(
        columns=["__key"]
    )

    merged_cache[slug] = {"mtime": mtime, "gdf": merged}
    return merged


# -------------------------
# Master CSV freshness helpers (variable-agnostic)
# -------------------------
def latest_processed_periods_mtime(processed_root: Path, state: str) -> float:
    base = processed_root / state
    if not base.exists():
        return 0.0
    latest = 0.0
    for f in base.rglob("*_periods.csv"):
        try:
            latest = max(latest, f.stat().st_mtime)
        except Exception:
            pass
    return latest

def master_needs_rebuild(master_path: Path, processed_root: Path, state: str) -> bool:
    if not master_path.exists():
        return True
    try:
        master_mtime = master_path.stat().st_mtime
    except Exception:
        return True
    return latest_processed_periods_mtime(processed_root, state) > (master_mtime + 1.0)

@st.cache_data
def load_master_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

# Normalize master columns to <metric>__<scenario>__<period>__<suffix>
def normalize_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns like
        days_gt_32C_ssp585_2020_2040__mean
    to
        days_gt_32C__ssp585__2020-2040__mean

    and likewise for:
        __median, __std, __p05, __p95, __n_models, __models, __values_per_model.
    """
    mapping = {}
    pat = re.compile(
        r"^(.+?)_"
        r"(historical|ssp119|ssp126|ssp245|ssp370|ssp434|ssp460|ssp585)_"
        r"(\d{4})_(\d{4})__"
        r"(mean|median|std|p05|p95|n_models|models|values_per_model)$",
        re.I,
    )

    for c in df.columns:
        s = str(c).strip()
        m = pat.match(s)
        if not m:
            continue
        metric, scen, y0, y1, suffix = m.groups()
        new = f"{metric.strip()}__{scen.lower().strip()}__{y0}-{y1}__{suffix.lower().strip()}"
        mapping[c] = new

    if mapping:
        return df.rename(columns=mapping)
    return df


def parse_master_schema(cols):
    pat = re.compile(
        r"^(?P<metric>[^_][^:]*)__(?P<scenario>[^_]+)__(?P<period>[^_]+)__(?P<stat>mean|median|std|p05|p95)$"
    )
    items = []
    for c in cols:
        m = pat.match(str(c))
        if m:
            items.append(m.groupdict() | {"column": c})
    metrics = sorted(set(i["metric"] for i in items))
    by_metric = {m: [i for i in items if i["metric"] == m] for m in metrics}
    return items, metrics, by_metric


# -------------------------
# Baseline (historical) helper for any index/stat
# -------------------------
def find_baseline_column_for_stat(
    df_cols, base_metric: str, stat: str
) -> Optional[str]:
    """
    Find a 'baseline' column for a metric + stat, preferring:
      - scenario: historical
      - period: 1990-2010 (if present)
    Columns are expected in the form:
      <metric>__<scenario>__<period>__<stat>
    """
    # pattern: metric__scenario__period__stat
    pat = re.compile(
        rf"^{re.escape(base_metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__{re.escape(stat)}$"
    )
    candidates = []
    for c in df_cols:
        m = pat.match(str(c))
        if not m:
            continue
        scen = m.group("scenario").strip().lower()
        if scen != "historical":
            continue
        period = m.group("period").strip()
        candidates.append((c, period))

    if not candidates:
        return None

    # Prefer 1990-2010 if present (allowing for minor variants like 1990_2010)
    for c, p in candidates:
        if p.replace("_", "-") == "1990-2010":
            return c

    # Else, pick lexicographically earliest period
    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]



# -------------------------
# Scenario / period helpers (global, index-agnostic)
# -------------------------
SCENARIO_ORDER = ["historical", "ssp245", "ssp585"]
SCENARIO_DISPLAY = {
    "historical": "Historical",
    "ssp245": "SSP2-4.5",
    "ssp585": "SSP5-8.5",
}

# Fixed period nomenclature used across the dashboard
PERIOD_ORDER = ["1990-2010", "2020-2040", "2040-2060"]

def canonical_period_label(raw: str) -> str:
    """
    Normalize period strings to a canonical 'YYYY-YYYY' representation.
    This lets you handle minor differences like '1990_2010' if they ever appear.
    """
    s = str(raw).strip()
    m = re.match(r"^(\d{4})\D+(\d{4})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return s

def build_scenario_comparison_panel_for_row(
    row: pd.Series,
    schema_items: list[dict],
    metric_name: str,
    sel_stat: str,
) -> pd.DataFrame:
    """
    Build a tidy table with:
      scenario, period, value, column
    for the given metric and statistic, across
    (historical, SSP2-4.5, SSP5-8.5) and periods
    (1990-2010, 2020-2040, 2040-2060) if present.

    This is index-agnostic: works for days_gt_32C, days_rain_gt_2p5mm, etc.
    """
    records = []

    for item in schema_items:
        if item.get("metric") != metric_name:
            continue
        if item.get("stat") != sel_stat:
            continue

        scen_raw = str(item.get("scenario", "")).strip().lower()
        if scen_raw not in SCENARIO_ORDER:
            continue

        period_raw = canonical_period_label(item.get("period", ""))
        if period_raw not in PERIOD_ORDER:
            continue

        col = item.get("column")
        if col not in row.index:
            continue

        val = row.get(col)
        # Robust numeric conversion for a single value
        try:
            val_f = float(pd.to_numeric(val, errors="coerce"))
        except Exception:
            val_f = float("nan")

        if pd.isna(val_f):
            continue

        records.append(
            {
                "scenario": scen_raw,
                "period": period_raw,
                "value": float(val_f),
                "column": col,
            }
        )

    if not records:
        return pd.DataFrame()

    dfp = pd.DataFrame(records)
    dfp["scenario_display"] = dfp["scenario"].map(SCENARIO_DISPLAY).fillna(dfp["scenario"])
    dfp["period"] = pd.Categorical(dfp["period"], PERIOD_ORDER, ordered=True)
    dfp["scenario"] = pd.Categorical(dfp["scenario"], SCENARIO_ORDER, ordered=True)
    dfp = dfp.sort_values(["period", "scenario"]).reset_index(drop=True)
    return dfp


def make_scenario_comparison_figure(
    panel_df: pd.DataFrame,
    metric_label: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    district_name: str,
    ax=None,
    figsize: tuple[float, float] = FIG_SIZE_PANEL,
):
    """
    Build a compact bar chart showing period-mean values for each scenario.

    - Bars are grouped by period (e.g. 1990–2010, 2020–2040, 2040–2060).
    - Within each group, scenarios (historical / SSP2-4.5 / SSP5-8.5) appear
      side by side with clean, symmetric spacing.
    - All bars have the same black outline thickness (no special thicker bar).
    - Numeric value labels are drawn above each bar.

    Font sizes follow the shared dashboard style:
      - Title: FONT_SIZE_TITLE
      - Axis labels: FONT_SIZE_LABEL
      - Tick labels: FONT_SIZE_TICKS
      - Legend: FONT_SIZE_LEGEND
    """
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    if panel_df is None or panel_df.empty:
        return None, None

    # Canonicalise selection for matching (not used for styling now, but kept
    # in case we want to highlight the selected bar later)
    sel_scen_norm = str(sel_scenario).strip().lower()
    sel_period_norm = canonical_period_label(sel_period)

    # Normalise periods and scenario labels in the data
    dfp = panel_df.copy()
    dfp["period"] = dfp["period"].map(canonical_period_label)
    dfp["scenario_norm"] = dfp["scenario"].astype(str).str.strip().str.lower()

    # Colours per scenario
    scenario_colors = {
        "historical": "tab:blue",
        "ssp245": "gold",      # yellow-like
        "ssp585": "tab:red",
    }

    # Build the list of (scenario, period) combos that actually exist
    combos: list[tuple[str, str]] = []
    for scen in SCENARIO_ORDER:
        scen_norm = str(scen).strip().lower()
        for period in PERIOD_ORDER:
            mask = (dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period)
            if mask.any():
                combos.append((scen_norm, period))

    if not combos:
        return None, None

    # Periods that actually appear in the data, in canonical order
    periods_present: list[str] = []
    for period in PERIOD_ORDER:
        if any(p == period for (_, p) in combos):
            periods_present.append(period)

    if not periods_present:
        return None, None

    # Assign x positions with clean grouping by period.
    # Each period group is centred, with scenarios spaced symmetrically.
    group_spacing = 2.0
    within_spacing = 0.6
    x_positions: dict[tuple[str, str], float] = {}

    for p_idx, period in enumerate(periods_present):
        scen_here = [sc for (sc, p) in combos if p == period]
        if not scen_here:
            continue

        n_scen = len(scen_here)
        group_center = p_idx * group_spacing

        for i, scen_norm in enumerate(scen_here):
            # Offset scenarios so the middle of the group stays on group_center
            offset = (i - (n_scen - 1) / 2.0) * within_spacing
            x_positions[(scen_norm, period)] = group_center + offset

    xs: list[float] = []
    ys: list[float] = []
    colors: list[str] = []

    for (scen_norm, period) in combos:
        mask = (dfp["scenario_norm"] == scen_norm) & (dfp["period"] == period)
        if not mask.any():
            continue

        try:
            val = float(dfp.loc[mask, "value"].iloc[0])
        except Exception:
            continue

        x_val = x_positions.get((scen_norm, period))
        if x_val is None:
            continue

        xs.append(x_val)
        ys.append(val)
        colors.append(scenario_colors.get(scen_norm, "grey"))

    if not xs:
        return None, None

    # Create / reuse axis
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize, dpi=FIG_DPI_PANEL)
    else:
        fig = ax.figure

    # Uniform bar width and uniform black outline for all bars
    bar_edgecolor = "black"
    bar_linewidth = 0.9

    bars = ax.bar(
        xs,
        ys,
        color=colors,
        edgecolor=bar_edgecolor,
        linewidth=bar_linewidth,
        width=0.45,
    )

    # X-axis: tick per period group, with human-readable labels
    group_centres: list[float] = []
    group_labels: list[str] = []
    for p_idx, period in enumerate(periods_present):
        group_centres.append(p_idx * group_spacing)
        group_labels.append(period)

    ax.set_xticks(group_centres)
    ax.set_xticklabels(group_labels, fontsize=FONT_SIZE_TICKS)

    # Y-axis label: metric name (units should be baked into metric_label if needed)
    ax.set_ylabel(metric_label, fontsize=FONT_SIZE_LABEL)

    # Subtle horizontal grid for readability
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.tick_params(axis="y", labelsize=FONT_SIZE_TICKS)
    ax.tick_params(axis="x", labelsize=FONT_SIZE_TICKS)

    # Numeric labels above each bar
    for x_val, y_val in zip(xs, ys):
        if y_val is None or (isinstance(y_val, float) and not np.isfinite(y_val)):
            continue
        ax.text(
            x_val,
            y_val,
            f"{y_val:.1f}",
            ha="center",
            va="bottom",
            fontsize=FONT_SIZE_TICKS,
        )

    # Title
    ax.set_title(
        f"Scenario comparison – {district_name}",
        fontsize=FONT_SIZE_TITLE,
        pad=6,
    )

    # Build a compact legend keyed by scenario (not (scenario, period))
    legend_handles: list[mpatches.Patch] = []
    legend_labels: list[str] = []
    scen_seen = {sc for (sc, _) in combos}
    for scen in SCENARIO_ORDER:
        scen_norm = str(scen).strip().lower()
        if scen_norm in scen_seen:
            legend_handles.append(
                mpatches.Patch(color=scenario_colors.get(scen_norm, "grey"))
            )
            legend_labels.append(SCENARIO_DISPLAY.get(scen_norm, scen_norm))
    if legend_handles:
        ax.legend(
            legend_handles,
            legend_labels,
            frameon=False,
            fontsize=FONT_SIZE_LEGEND,
            ncol=len(legend_handles),
            loc="upper left",
            bbox_to_anchor=(0.0, 1.02),
        )

    # Clean spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    return fig, ax

def make_state_boxplot_for_districts(
    sel_districts_gdf: gpd.GeoDataFrame,
    metric_col: str,
    metric_label: str,
    sel_state: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
):
    """
    For a given state selection, build a boxplot where:
      - each box = one district in the state
      - y-values = distribution across models for the selected metric,
        if a <metric>__...__values_per_model column exists;
        otherwise fall back to a single value per district (metric_col).
      - x-axis = districts, ordered by metric_col.
    """
    import matplotlib.pyplot as plt

    if sel_districts_gdf is None or sel_districts_gdf.empty:
        return None

    # Try to find a per-model distribution column for this metric
    metric_base = metric_col.rsplit("__", 1)[0]
    vpm_col = f"{metric_base}__values_per_model"
    use_vpm = vpm_col in sel_districts_gdf.columns

    dist_to_values: dict[str, list[float]] = {}
    central_value: dict[str, float] = {}

    for _, row in sel_districts_gdf.iterrows():
        dist_name = str(row.get("district_name") or "").strip()
        if not dist_name:
            continue

        # Central value for ordering (the current stat)
        try:
            cv = pd.to_numeric(pd.Series([row.get(metric_col)]), errors="coerce").iloc[0]
        except Exception:
            cv = np.nan
        if pd.isna(cv):
            continue
        cv = float(cv)
        central_value[dist_name] = cv

        # Build the distribution for this district
        if use_vpm:
            raw = row.get(vpm_col)
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                # fall back to single value if per-model data missing
                dist_to_values.setdefault(dist_name, []).append(cv)
                continue

            vals = []
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(parsed, dict):
                    vals = [float(v) for v in parsed.values()]
                elif isinstance(parsed, (list, tuple, np.ndarray, pd.Series)):
                    vals = [float(v) for v in parsed]
            except Exception:
                vals = []

            vals = [v for v in vals if pd.notna(v)]
            if not vals:
                vals = [cv]  # again, fall back
            dist_to_values[dist_name] = vals
        else:
            # No values_per_model column at all: treat the single stat as
            # a degenerate "distribution" so we still get a plot.
            dist_to_values[dist_name] = [cv]

    if not dist_to_values:
        return None

    # Debug: how many points per district?
    for d, vals in dist_to_values.items():
        dbg(f"Boxplot debug – {d}: n={len(vals)}, sample={vals[:5]}")

    # Order districts by central value (highest on the left)
    ordered_districts = sorted(
        dist_to_values.keys(),
        key=lambda d: central_value.get(d, 0.0),
        reverse=True,
    )
    data = [dist_to_values[d] for d in ordered_districts]

    n = len(ordered_districts)
    fig_width = min(max(6.0, 0.3 * n), 16.0)

    fig, ax = plt.subplots(figsize=(fig_width, 4.5), dpi=150)
    ax.boxplot(data, positions=range(1, n + 1), showfliers=True)
    ax.set_xticks(range(1, n + 1))
    ax.set_xticklabels(ordered_districts, rotation=90, fontsize=7)

    ax.set_ylabel(metric_label)
    ax.set_title(
        f"{sel_state}: {metric_label}\nScenario: {sel_scenario} · "
        f"Period: {sel_period} · Stat: {sel_stat}"
    )
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()

    return fig

# -------------------------
# Risk class helper (percentile → label)
# -------------------------
def risk_class_from_percentile(p: float) -> str:
    if pd.isna(p):
        return "Unknown"
    if p >= 80:
        return "Very High"
    elif p >= 60:
        return "High"
    elif p >= 40:
        return "Medium"
    elif p >= 20:
        return "Low"
    else:
        return "Very Low"



# -------------------------
# APP START
# -------------------------
st.set_page_config(page_title="India Resilience Tool", layout="wide")

# Initialise analysis mode and portfolio storage in session state
if "analysis_mode" not in st.session_state:
    st.session_state["analysis_mode"] = "Single district focus"

if "portfolio_districts" not in st.session_state:
    # Will store a list of (state_name, district_name) tuples
    st.session_state["portfolio_districts"] = []

# Which main view is active in the left column: map vs rankings
if "active_view" not in st.session_state:
    st.session_state["active_view"] = "🗺 Map view"

# If a downstream control requested to jump to the Rankings table,
# honour it BEFORE the main_view_selector radio is created.
if st.session_state.get("jump_to_rankings", False):
    st.session_state["active_view"] = "📊 Rankings table"
    # Also sync the radio widget state so the UI reflects this jump
    st.session_state["main_view_selector"] = "📊 Rankings table"
    # Reset the flag so it only applies once
    st.session_state["jump_to_rankings"] = False

with st.sidebar:
    try:
        st.image(LOGO_PATH, width=220)
    except Exception:
        pass

    # Read current analysis mode (default: Single district focus)
    analysis_mode_current = st.session_state.get(
        "analysis_mode", "Single district focus"
    )

    # Show hover toggle only in Multi-district portfolio mode
    if analysis_mode_current == "Multi-district portfolio":
        st.checkbox(
            "Enable hover highlight & tooltip",
            key="hover_enabled",
            value=st.session_state.get("hover_enabled", True),
        )
    else:
        # Optional: enforce a default behaviour outside portfolio mode
        st.session_state.setdefault("hover_enabled", True)

    analysis_mode_placeholder = st.empty()  # Single vs multi-district analysis

    state_placeholder = st.empty()
    district_placeholder = st.empty()

    metric_ui_placeholder = st.empty()  # unified "Index" UI
    map_mode_placeholder = st.empty()   # NEW: absolute vs change toggle
    color_slider_placeholder = st.empty()
    st.markdown("---")

    master_controls_placeholder = st.empty()
    st.markdown("---")

st.title("India Resilience Tool")

# Pilot state default
PILOT_STATE = os.getenv("IRT_PILOT_STATE", "Telangana")

# Pilot state default
PILOT_STATE = os.getenv("IRT_PILOT_STATE", "Telangana")

# -------------------------
# Unified Index selection (single dropdown)
# -------------------------
with metric_ui_placeholder.container():
    with st.expander("Metric selection", expanded=True):
        st.markdown("### Metric selection")

        # --- NEW: first pick an index group (Temperature / Rainfall / etc.) ---
        raw_groups = {cfg.get("group", "other") for cfg in VARIABLES.values()}

        # Deterministic ordering: Temperature, Rain, then any others alphabetically
        preferred_order = ["temperature", "rain"]
        all_groups: list[str] = []
        for g in preferred_order:
            if g in raw_groups:
                all_groups.append(g)
        for g in sorted(raw_groups):
            if g not in all_groups:
                all_groups.append(g)

        default_group = st.session_state.get("selected_index_group")
        if default_group not in all_groups:
            default_group = "temperature" if "temperature" in all_groups else all_groups[0]

        selected_group = st.radio(
            "Index group",
            options=all_groups,
            index=all_groups.index(default_group),
            key="selected_index_group",
            format_func=lambda g: INDEX_GROUP_LABELS.get(g, str(g).title()),
        )

        # Filter indices by the chosen group
        index_slugs = [
            slug
            for slug, cfg in VARIABLES.items()
            if cfg.get("group", "other") == selected_group
        ]

        # Safety fallback: if something goes wrong, show all indices
        if not index_slugs:
            index_slugs = list(VARIABLES.keys())

        # Previously selected index might not be in this group; clamp it
        default_slug = st.session_state.get("selected_var", index_slugs[0])
        if default_slug not in index_slugs:
            default_slug = index_slugs[0]

        selected_var = st.selectbox(
            "Index",
            options=index_slugs,
            index=index_slugs.index(default_slug),
            key="selected_var",
            format_func=lambda k: VARIABLES[k]["label"],
        )

        # Resolve per-index config
        VARIABLE_SLUG = selected_var
        VARCFG = VARIABLES[VARIABLE_SLUG]

        # --- NEW: small info button + text description for the selected index ---
        desc = VARCFG.get("description", "").strip()
        if desc:
            # # A tiny ℹ️ button with a tooltip on hover
            # info_col, _ = st.columns([0.12, 0.88])
            # with info_col:
            #     st.button("ℹ️", help=desc)
            # # And a short textual caption under the dropdown
            st.caption(desc)

        PROCESSED_ROOT = Path(
            os.getenv("IRT_PROCESSED_ROOT", DATA_DIR / "processed" / VARIABLE_SLUG)
        ).resolve()
        (PROCESSED_ROOT / PILOT_STATE).mkdir(parents=True, exist_ok=True)
        MASTER_CSV_PATH = PROCESSED_ROOT / PILOT_STATE / "master_metrics_by_district.csv"

        # Rebuilder bound to this index
        def rebuild_master_csv_if_needed(
            force: bool = False, attach_centroid_geojson: str | None = None
        ):
            needs = force or master_needs_rebuild(MASTER_CSV_PATH, PROCESSED_ROOT, PILOT_STATE)
            if not needs:
                return False, "up-to-date"
            try:
                from build_master_metrics import build_master_metrics
            except Exception as e:
                return False, f"builder import failed: {e}"
            try:
                build_master_metrics(
                    str(PROCESSED_ROOT),
                    PILOT_STATE,
                    metric_col_in_periods=VARCFG["periods_metric_col"],
                    out_path=str(MASTER_CSV_PATH),
                    attach_centroid_geojson=attach_centroid_geojson,
                    verbose=True,
                )
                return True, "rebuilt"
            except Exception as e:
                return False, f"rebuild failed: {e}"

        # Ensure master exists/fresh for this index
        try:
            if master_needs_rebuild(MASTER_CSV_PATH, PROCESSED_ROOT, PILOT_STATE):
                with st.spinner("Master CSV missing or stale — rebuilding now..."):
                    ok, msg = rebuild_master_csv_if_needed(
                        force=False, attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON
                    )
                    st.success("Master CSV rebuilt.") if ok else st.error(
                        f"Auto-rebuild failed: {msg}"
                    )
        except Exception as e:
            st.warning(f"Could not check master CSV freshness: {e}")

        if not MASTER_CSV_PATH.exists():
            st.error(
                f"Master CSV not found for {VARIABLES[VARIABLE_SLUG]['label']} at {MASTER_CSV_PATH}. "
                f"Click 'Rebuild now' below."
            )
            st.stop()

        # Load + parse schema (for scenario/period/stat only), cached by file mtime
        def _load_master_and_schema(master_path: Path, slug: str):
            cache = st.session_state.setdefault("_master_cache", {})
            try:
                mtime = master_path.stat().st_mtime
            except Exception:
                mtime = None

            entry = cache.get(slug)
            if entry is not None and entry.get("mtime") == mtime:
                return (
                    entry["df"],
                    entry["schema_items"],
                    entry["metrics"],
                    entry["by_metric"],
                )

            # (Re)load from disk
            with st.spinner("Loading master CSV..."):
                df_local = load_master_csv(str(master_path))

            df_local = normalize_master_columns(df_local)
            schema_items_local, metrics_local, by_metric_local = parse_master_schema(
                df_local.columns
            )

            cache[slug] = {
                "df": df_local,
                "schema_items": schema_items_local,
                "metrics": metrics_local,
                "by_metric": by_metric_local,
                "mtime": mtime,
            }
            return df_local, schema_items_local, metrics_local, by_metric_local

        df, schema_items, metrics, by_metric = _load_master_and_schema(
            MASTER_CSV_PATH, VARIABLE_SLUG
        )
        if not metrics:
            st.error("No ensemble statistic columns found in the master CSV. Did the builder run?")
            st.stop()

        # Choose the internal metric name from the registry (no separate Metric dropdown)
        registry_metric = VARCFG["periods_metric_col"]
        # If normalized columns changed the metric name casing, align it:
        available_metrics = set(metrics)
        if registry_metric not in available_metrics:
            m_lower = {m.lower(): m for m in available_metrics}
            registry_metric = m_lower.get(VARCFG["periods_metric_col"].lower(), next(iter(available_metrics)))

        # Scenario / Period / Statistic pickers remain
        items_for_m = by_metric.get(registry_metric, [])
        all_scenarios = (
            sorted(set(i["scenario"] for i in items_for_m))
            if items_for_m
            else sorted(set(i["scenario"] for i in schema_items))
        )

        # Only allow SSP245 and SSP585 in the UI
        allowed = {"ssp245", "ssp585"}
        scenarios = [s for s in all_scenarios if str(s).strip().lower() in allowed]

        if not scenarios:
            st.error("No SSP245/SSP585 data found for this index in the master CSV.")
            st.stop()

        sel_scenario = st.selectbox("Scenario", scenarios, index=0, key="sel_scenario")


        periods = sorted(
            set(
                i["period"]
                for i in (by_metric.get(registry_metric, []) or schema_items)
                if i["scenario"] == sel_scenario
            )
        )
        sel_period = st.selectbox("Period", periods, index=0, key="sel_period")
        stats = ["mean", "median", "p05", "p95", "std"]
        sel_stat = st.selectbox("Statistic", stats, index=0, key="sel_stat")

# Column chosen to plot
sel_metric = registry_metric  # internal name
metric_col = f"{sel_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
if metric_col not in df.columns:
    st.error(f"Selected column '{metric_col}' not found in master CSV.")
    st.stop()
pretty_metric_label = (
    f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario} · {sel_period} · {sel_stat}"
)

# -------------------------
# Analysis focus (single vs multi-district)
# -------------------------
with analysis_mode_placeholder.container():
    with st.expander("Analysis focus", expanded=True):
        st.markdown(
            "<div style='font-weight:600; font-size:1rem; margin-bottom:-0.35rem;'>Analysis focus</div>",
            unsafe_allow_html=True,
        )
        analysis_mode = st.radio(
            "Analysis focus",
            options=[
                "Single district focus",
                "Multi-district portfolio",
            ],
            index=0,
            key="analysis_mode",
            label_visibility="collapsed",
        )


with map_mode_placeholder.container():
    # Tight "Map mode" label with no extra space before the radio
    with st.expander("Chloropleth settings", expanded=True):
        st.markdown(
            "<div style='font-weight:600; font-size:1rem; margin-bottom:-0.35rem;'>Map mode</div>",
            unsafe_allow_html=True,
        )

        map_mode = st.radio(
            "Map mode",  # non-empty label for accessibility
            options=[
                "Absolute value",
                "Change from 1990-2010 baseline",
            ],
            index=0,
            key="map_mode",
            label_visibility="collapsed",  # keeps UI same as before
        )

# -------------------------
# Master dataset controls (bound to chosen Index)
# -------------------------
with master_controls_placeholder.container():
    st.markdown("### Master dataset")
    col_a, col_b = st.columns([3, 2])
    with col_a:
        auto_check = st.button("Check / Rebuild master (auto)", key="btn_auto_check")
    with col_b:
        force_btn = st.button("Rebuild now", key="btn_force_rebuild")

if auto_check:
    ok, msg = rebuild_master_csv_if_needed(
        force=False,
        attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON,
    )
    if ok:
        st.success("Master CSV rebuilt or already up-to-date.")
    else:
        st.info(f"Master CSV status: {msg}")

if force_btn:
    ok, msg = rebuild_master_csv_if_needed(
        force=True,
        attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON,
    )
    if ok:
        st.success("Master CSV force-rebuilt.")
    else:
        st.error(f"Forced rebuild failed: {msg}")

# -------------------------
# Build adm1 & enrich adm2 state names
# -------------------------
adm1 = build_adm1_from_adm2(adm2)

with st.spinner("Enriching district data with state names..."):
    adm2 = enrich_adm2_with_state_names(adm2, adm1)

# Sync pending selections
if "pending_selected_state" in st.session_state:
    st.session_state["selected_state"] = st.session_state.pop("pending_selected_state")
if "pending_selected_district" in st.session_state:
    st.session_state["selected_district"] = st.session_state.pop("pending_selected_district")

# State/district selectors (Geography block in sidebar)
with state_placeholder.container():
    with st.expander("Geography", expanded=True):
        states = ["All"] + sorted(
            adm1["shapeName"].astype(str).str.strip().unique().tolist()
        )
        if (
            "selected_state" not in st.session_state
            or st.session_state["selected_state"] not in states
        ):
            st.session_state["selected_state"] = (
                "Telangana" if "Telangana" in states else "All"
            )

        selected_state = st.selectbox(
            "State",
            options=states,
            index=states.index(st.session_state["selected_state"]),
            key="selected_state",
        )

        if selected_state != "All":
            sel_state_norm = selected_state.strip().lower()
            state_row = adm1[
                adm1["shapeName"].astype(str).str.strip().str.lower()
                == sel_state_norm
            ]
            if state_row.empty:
                state_row = adm1[
                    adm1["shapeName"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.contains(sel_state_norm, na=False)
                ]
            if not state_row.empty:
                state_geom = state_row.iloc[0].geometry
                try:
                    gdf_state_districts = adm2[
                        adm2.geometry.within(state_geom.buffer(0.001))
                    ].copy()
                except Exception:
                    gdf_state_districts = adm2[
                        adm2.geometry.centroid.within(state_geom.buffer(0.001))
                    ].copy()
                if gdf_state_districts.empty:
                    gdf_state_districts = adm2[
                        adm2["state_name"]
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .str.contains(sel_state_norm, na=False)
                    ].copy()
            else:
                gdf_state_districts = adm2[
                    adm2["state_name"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.contains(sel_state_norm, na=False)
                ].copy()
        else:
            gdf_state_districts = adm2.copy()

        districts = [
            "All"
        ] + sorted(
            gdf_state_districts["district_name"].astype(str).unique().tolist()
        )

        # Ensure we always have a valid value in session state
        if (
            "selected_district" not in st.session_state
            or st.session_state["selected_district"] not in districts
        ):
            st.session_state["selected_district"] = "All"

        analysis_mode = st.session_state.get(
            "analysis_mode", "Single district focus"
        )

        if analysis_mode == "Single district focus":
            # Normal behaviour: user chooses the district from the sidebar
            selected_district = st.selectbox(
                "District",
                options=districts,
                index=districts.index(st.session_state["selected_district"]),
                key="selected_district",
            )
        else:
            # Portfolio mode: freeze district selection to "All" and explain why
            st.session_state["selected_district"] = "All"
            selected_district = "All"
            st.markdown(
                "<div style='font-size:0.9rem;'>"
                "<strong>District selection</strong> is controlled from the "
                "<em>📊 Rankings table</em> (and map, later) when you choose "
                "<strong>Multi-district portfolio</strong> as the analysis focus."
                "</div>",
                unsafe_allow_html=True,
            )

# -------------------------
# Portfolio selection helpers (multi-district)
# -------------------------

if "portfolio_districts" not in st.session_state:
    # List of {"state": ..., "district": ...}
    st.session_state["portfolio_districts"] = []


def _portfolio_normalize(text: str) -> str:
    """Normalize a state/district name for comparison."""
    return str(text or "").strip().lower()


def _portfolio_key(state_name: str, district_name: str) -> tuple[str, str]:
    return (_portfolio_normalize(state_name), _portfolio_normalize(district_name))


def _portfolio_add(state_name: str, district_name: str) -> None:
    """Add a (state, district) pair to the portfolio if not already present."""
    if not state_name or not district_name or district_name == "All":
        return
    items = st.session_state.get("portfolio_districts", [])
    new_norm = _portfolio_key(state_name, district_name)
    for item in items:
        if _portfolio_key(item.get("state"), item.get("district")) == new_norm:
            return
    items.append({"state": state_name, "district": district_name})
    st.session_state["portfolio_districts"] = items


def _portfolio_remove(state_name: str, district_name: str) -> None:
    """Remove a (state, district) pair from the portfolio."""
    items = st.session_state.get("portfolio_districts", [])
    norm = _portfolio_key(state_name, district_name)
    items = [
        item
        for item in items
        if _portfolio_key(item.get("state"), item.get("district")) != norm
    ]
    st.session_state["portfolio_districts"] = items


def _portfolio_clear() -> None:
    """Clear all districts from the portfolio."""
    st.session_state["portfolio_districts"] = []

if "map_center" not in st.session_state:
    st.session_state["map_center"] = [25.0, 82.5]
if "map_zoom" not in st.session_state:
    st.session_state["map_zoom"] = 4.0

if selected_district != "All":
    district_row = gdf_state_districts[gdf_state_districts["district_name"] == selected_district]
    if not district_row.empty:
        centroid = district_row.iloc[0].geometry.centroid
        st.session_state["map_center"] = [centroid.y, centroid.x]
        st.session_state["map_zoom"] = 9
elif selected_state != "All":
    state_row = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
    if not state_row.empty:
        b = state_row.iloc[0].geometry.bounds
        st.session_state["map_center"] = [(b[1] + b[3]) / 2, (b[0] + b[2]) / 2]
        st.session_state["map_zoom"] = 7
else:
    st.session_state["map_center"] = [22.0, 82.5]
    st.session_state["map_zoom"] = 4.8

# Merge attributes
if "district" not in df.columns:
    st.error("Master CSV must contain a 'district' column to join with ADM2.")
    st.stop()

with st.spinner("Preparing merged geometries with CSV attributes..."):
    merged = get_or_build_merged_for_index(
        adm2=adm2,
        df=df,
        slug=VARIABLE_SLUG,
        master_path=MASTER_CSV_PATH,
    )

# --- Baseline column for this metric + stat (used by map & table) ---
baseline_col = find_baseline_column_for_stat(df.columns, sel_metric, sel_stat)

# --- Decide which column the map will actually show ---
map_mode = st.session_state.get("map_mode", "Absolute value")
map_value_col = metric_col  # default: absolute values

if map_mode == "Change from 1990-2010 baseline":
    if baseline_col and (baseline_col in merged.columns):
        # Compute Δ = current - baseline, per district
        merged["_baseline_value"] = pd.to_numeric(
            merged[baseline_col], errors="coerce"
        )
        merged["_current_value"] = pd.to_numeric(
            merged[metric_col], errors="coerce"
        )
        merged["_map_delta"] = merged["_current_value"] - merged["_baseline_value"]
        map_value_col = "_map_delta"
    else:
        st.warning(
            "Baseline (historical 1990-2010) column not found for this metric/stat; "
            "showing absolute values instead."
        )
        map_mode = "Absolute value"
        st.session_state["map_mode"] = map_mode
        map_value_col = metric_col

numeric_vals = pd.to_numeric(
    merged.get(map_value_col, pd.Series([], dtype=float)), errors="coerce"
).dropna()
if numeric_vals.empty:
    st.error("No numeric values found for selected index & selection.")
    st.stop()

# Default min/max from data
vmin_default, vmax_default = float(numeric_vals.min()), float(numeric_vals.max())

# If there is no spread (all values identical), pad the range a bit
if vmin_default == vmax_default:
    # Use a small padding relative to the magnitude, with a sensible floor
    padding = max(abs(vmin_default) * 0.1, 1.0)
    vmin_default -= padding
    vmax_default += padding

with st.sidebar:
    vmin_vmax = color_slider_placeholder.slider(
        "Color range (min → max)",
        min_value=float(vmin_default),
        max_value=float(vmax_default),
        value=(vmin_default, vmax_default),
        step=max((vmax_default - vmin_default) / 200.0, 0.01),
        key="color_range_slider",
    )

vmin, vmax = float(vmin_vmax[0]), float(vmin_vmax[1])

# Choose colormap: sequential for absolute, diverging for change
if map_mode == "Change from 1990-2010 baseline":
    cmap_name = "RdBu_r"   # blue-negative, red-positive
    pretty_metric_label = (
        f"Δ {VARIABLES[VARIABLE_SLUG]['label']} vs 1990–2010 · "
        f"{sel_scenario} · {sel_period} · {sel_stat}"
    )
else:
    cmap_name = "Reds"
    pretty_metric_label = (
        f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario} · {sel_period} · {sel_stat}"
    )

with st.spinner("Computing colors..."):
    merged = apply_fillcolor(
        merged,
        map_value_col,
        vmin,
        vmax,
        cmap_name=cmap_name,
    )


# -------------------------
# Build ranking table (district-level)
# -------------------------
# Filter for ranking: respect selected_state, but ignore selected_district
if selected_state != "All":
    rank_mask = (
        merged["state_name"].astype(str).str.strip().str.lower()
        == selected_state.strip().lower()
    )
    ranking_gdf = merged.loc[rank_mask].copy()
else:
    ranking_gdf = merged.copy()

# Base table: District, State, and index value for current selection
table_df = pd.DataFrame()
if not ranking_gdf.empty and (metric_col in ranking_gdf.columns):
    table_df = ranking_gdf[["district_name", "state_name"]].copy()
    # Absolute value for selected scenario/period/stat
    value_series = pd.to_numeric(
        ranking_gdf[metric_col], errors="coerce"
    )
    table_df["value"] = value_series

    # Baseline & change columns (if available)
    has_baseline = baseline_col and (baseline_col in ranking_gdf.columns)
    if has_baseline:
        baseline_series = pd.to_numeric(
            ranking_gdf[baseline_col], errors="coerce"
        )
        table_df["baseline"] = baseline_series
        # Absolute change
        table_df["delta_abs"] = table_df["value"] - table_df["baseline"]
        # Percent change (avoid division by 0)
        table_df["delta_pct"] = np.where(
            (baseline_series != 0) & (~baseline_series.isna()),
            100.0 * table_df["delta_abs"] / baseline_series,
            np.nan,
        )
    else:
        has_baseline = False

    # Drop rows with no value at all
    table_df = table_df[~table_df["value"].isna()].copy()
    if not table_df.empty:
        # Rank by absolute value (1 = hottest / wettest, highest index)
        table_df["rank_value"] = table_df["value"].rank(
            ascending=False, method="min"
        ).astype(int)

        # Percentile of value within this ranking set (0–100)
        table_df["percentile_value"] = (
            table_df["value"].rank(pct=True) * 100.0
        )

        # Risk class based on percentile
        table_df["risk_class"] = table_df["percentile_value"].apply(
            risk_class_from_percentile
        )

        # Rank by increase (if baseline present)
        if has_baseline and "delta_abs" in table_df.columns:
            valid_delta = table_df["delta_abs"].dropna()
            if not valid_delta.empty:
                table_df["rank_delta"] = table_df["delta_abs"].rank(
                    ascending=False, method="min"
                ).astype(int)

        # Keep aspirational flag if present
        if "aspirational" in ranking_gdf.columns:
            table_df["aspirational"] = ranking_gdf["aspirational"].values
else:
    has_baseline = False


display_gdf = merged.copy()
if selected_state != "All":
    row_state = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
    if not row_state.empty:
        geom = row_state.iloc[0].geometry
        display_gdf = display_gdf[display_gdf.geometry.within(geom.buffer(0.001))]
    else:
        display_gdf = display_gdf[
            display_gdf["state_name"]
            .astype(str)
            .str.contains(selected_state, case=False, na=False)
        ]
if selected_district != "All":
    display_gdf = display_gdf[
        display_gdf["district_name"].astype(str) == selected_district
    ]

m = folium.Map(
    location=st.session_state["map_center"],
    zoom_start=st.session_state["map_zoom"],
    tiles="CartoDB positron",
    control_scale=True,
    min_zoom=4,
    max_zoom=12,
)

try:
    if selected_state != "All" and selected_district == "All":
        row_state = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
        if not row_state.empty:
            b = row_state.iloc[0].geometry.bounds
            fit_bounds = [[b[1], b[0]], [b[3], b[2]]]
            _name = m.get_name()
            bounds_js = (
                f"<script>var {_name} = {_name}; {_name}.fitBounds({fit_bounds});</script>"
            )
            m.get_root().html.add_child(folium.Element(bounds_js))
            st.session_state["map_center"] = [(b[1] + b[3]) / 2, (b[0] + b[2]) / 2]
            st.session_state["map_zoom"] = 7
except Exception:
    pass

_name = m.get_name()
bounds_js = (
    f"<script>var {_name} = {_name}; {_name}.setMaxBounds("
    f"{[[MIN_LAT, MIN_LON], [MAX_LAT, MAX_LON]]});</script>"
)
m.get_root().html.add_child(folium.Element(bounds_js))

def style_fn(feature):
    props = feature.get("properties", {})
    return {
        "fillColor": props.get("fillColor", "#cccccc"),
        "color": "#666666",
        "weight": 0.3,
        "fillOpacity": 0.7,
    }

if map_mode == "Change from 1990-2010 baseline":
    tooltip_fields = ["district_name", map_value_col]
    tooltip_aliases = ["District", "Δ vs 1990–2010"]
else:
    tooltip_fields = ["district_name", metric_col]
    tooltip_aliases = ["District", "Value"]

hover_enabled = st.session_state.get("hover_enabled", True)

tooltip = (
    folium.features.GeoJsonTooltip(
        fields=tooltip_fields,
        aliases=tooltip_aliases,
        localize=True,
        sticky=True,
    )
    if hover_enabled
    else None
)

geo_source = display_gdf if not display_gdf.empty else merged

highlight_fn = None
if hover_enabled:
    highlight_fn = lambda f: {
        "fillColor": "#ffff00",
        "color": "#000",
        "weight": 2,
        "fillOpacity": 0.9,
    }

folium.GeoJson(
    data=json.loads(geo_source.to_json()),
    name="Districts",
    style_function=style_fn,
    tooltip=tooltip,
    highlight_function=highlight_fn,
).add_to(m)

MAP_WIDTH, MAP_HEIGHT = 780, 700
bar_height_px = int(MAP_HEIGHT * 0.92)
bar_width_px = 28
label_font = "12px"
cmap = mpl.colormaps.get_cmap(cmap_name)
legend_colors = _get_cmap_hex_list(cmap_name)
gradient_colors = ", ".join(legend_colors)
legend_html = f"""
<div id="legend-fixed" style="position: fixed; right: 95px; top: 50%; transform: translateY(-50%);
z-index: 9999; pointer-events: none; display: flex; align-items: center; gap: 10px; font-family: Arial, Helvetica, sans-serif;">
  <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
    <div style="display: flex; flex-direction: column; justify-content: space-between; height: {bar_height_px}px; margin-right:8px; font-size:{label_font}; color:#000;">
      <div style="text-align: right;">{vmax:.1f}</div>
      <div style="text-align: right;">{vmin:.1f}</div>
    </div>
    <div id="legend-bar" style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
         box-shadow: 0 2px 6px rgba(0,0,0,0.28); background: linear-gradient(to top, {gradient_colors}); display: block;"></div>
  </div>
  <div id="legend-title" style="writing-mode: vertical-rl; transform: rotate(180deg); font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;">
    {pretty_metric_label}
  </div>
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

# Ensure `returned` always exists, even if the map tab didn't run yet
returned = None

col1, col2 = st.columns([5, 3])

with col1:
    head_col, reset_col = st.columns([4, 1])
    with head_col:
        st.header(pretty_metric_label)
    with reset_col:
        if st.button("⟲ Reset View", key="reset_map_view"):
            st.session_state["pending_selected_state"] = "All"
            st.session_state["pending_selected_district"] = "All"
            st.session_state["map_reset_requested"] = True

    # Main view selector: Map vs Rankings (replaces tabs)
    view_options = ["🗺 Map view", "📊 Rankings table"]

    # Initialise widget + logical view defaults once
    if "main_view_selector" not in st.session_state:
        st.session_state["main_view_selector"] = view_options[0]
    if "active_view" not in st.session_state:
        st.session_state["active_view"] = st.session_state["main_view_selector"]

    # One-shot hook: if some downstream control requested a jump to the
    # Rankings table, honour it *before* the radio is instantiated.
    if st.session_state.get("jump_to_rankings", False):
        st.session_state["main_view_selector"] = "📊 Rankings table"
        st.session_state["active_view"] = "📊 Rankings table"
        st.session_state["jump_to_rankings"] = False

    view = st.radio(
        "View",
        options=view_options,
        key="main_view_selector",
        horizontal=True,
    )

    # Keep logical view in sync with the widget
    st.session_state["active_view"] = view

# ---------- VIEW 1: MAP ----------
    if view == "🗺 Map view":
        analysis_mode = st.session_state.get(
            "analysis_mode", "Single district focus"
        )

        # In Multi-district portfolio mode, draw multi-point markers (if any)
        if analysis_mode == "Multi-district portfolio":
            # Multi-point collection (saved points)
            points = st.session_state.get("point_query_points", [])
            if isinstance(points, list):
                for idx, pt in enumerate(points, start=1):
                    if not isinstance(pt, dict):
                        continue
                    try:
                        lat_p = float(pt.get("lat"))
                        lon_p = float(pt.get("lon"))
                    except (TypeError, ValueError):
                        continue

                    folium.Marker(
                        location=[lat_p, lon_p],
                        tooltip=f"Point {idx}: {lat_p:.4f}, {lon_p:.4f}",
                    ).add_to(m)

            # Active point (current point used in the Climate Profile)
            point_query = st.session_state.get("point_query_latlon")
            if isinstance(point_query, dict):
                try:
                    lat_q = float(point_query.get("lat"))
                    lon_q = float(point_query.get("lon"))
                    folium.Marker(
                        location=[lat_q, lon_q],
                        tooltip=f"Active point: {lat_q:.4f}, {lon_q:.4f}",
                    ).add_to(m)
                except (TypeError, ValueError):
                    # Ignore invalid/partial values silently
                    pass

        returned = st_folium(
            m,
            width=MAP_WIDTH,
            height=MAP_HEIGHT,
            returned_objects=[
                "last_object_clicked",
                "last_clicked",
                "center",
                "zoom",
            ],
        )


        def extract_district_name_from_returned(
            ret,
        ) -> Tuple[Optional[str], Optional[str]]:
            if not ret:
                return None, None
            for key in (
                "last_object_clicked",
                "clicked_feature",
                "last_active_drawing",
                "last_object",
            ):
                feat = ret.get(key)
                if isinstance(feat, dict):
                    props = feat.get("properties") or feat
                    for pk in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
                        val = props.get(pk) if isinstance(props, dict) else None
                        if val:
                            state_val = (
                                props.get("state_name")
                                or props.get("shapeGroup")
                                or props.get("shapeName_0")
                            )
                            return str(val), (str(state_val) if state_val else None)
            return None, None

    # ---------- VIEW 2: RANKINGS TABLE ----------
    elif view == "📊 Rankings table":
        st.subheader("District rankings")

        if table_df is None or table_df.empty:
            st.caption(
                "No ranking data available for this index, scenario, period and selection."
            )
        else:
            # Ranking mode selector
            options = ["Top 20 biggest increases", "All"]
            rank_mode = st.radio(
                "Show:",
                options=options,
                index=0,
                key="rank_mode",
            )

            df_to_show = table_df.copy()


            if rank_mode == "Top 20 biggest increases":
                if has_baseline and ("rank_delta" in df_to_show.columns):
                    df_to_show = df_to_show.dropna(subset=["delta_abs"]).copy()
                    if df_to_show.empty:
                        st.info(
                            "No valid baseline/change values to rank by increase."
                        )
                    else:
                        df_to_show = df_to_show.sort_values("rank_delta").head(20)
                else:
                    st.info(
                        "Baseline not available for this index/stat; showing absolute-value ranking instead."
                    )
                    if "rank_value" in df_to_show.columns:
                        df_to_show = df_to_show.sort_values("rank_value").head(20)
                    else:
                        df_to_show = df_to_show.sort_values("value", ascending=False).head(20)

            else:  # "All"
                if "rank_value" in df_to_show.columns:
                    df_to_show = df_to_show.sort_values("rank_value")
                else:
                    df_to_show = df_to_show.sort_values("value", ascending=False)

            # Decide which columns to display
            display_cols = ["rank_value", "district_name", "state_name", "value"]
            if has_baseline and "baseline" in df_to_show.columns:
                display_cols += ["delta_abs", "delta_pct"]
            if "percentile_value" in df_to_show.columns:
                display_cols.append("percentile_value")
            if "risk_class" in df_to_show.columns:
                display_cols.append("risk_class")
            if "aspirational" in df_to_show.columns:
                display_cols.append("aspirational")

            display_cols = [c for c in display_cols if c in df_to_show.columns]

            df_display = df_to_show[display_cols].rename(
                columns={
                    "rank_value": "Rank (value)",
                    "district_name": "District",
                    "state_name": "State",
                    "value": "Index value",
                    "baseline": "Baseline (1990–2010)",
                    "delta_abs": "Δ vs baseline",
                    "delta_pct": "%Δ vs baseline",
                    "percentile_value": "Percentile",
                    "risk_class": "Risk class",
                    "aspirational": "Aspirational",
                }
            )

            caption_text = (
                f"Ranking based on **{VARIABLES[VARIABLE_SLUG]['label']}**, "
                f"**{sel_scenario}**, **{sel_period}**, **{sel_stat}**. "
                f"Change vs baseline uses historical **1990–2010** where available. "
                + (
                    f"Filtered to state: **{selected_state}**."
                    if selected_state != "All"
                    else "Showing all states."
                )
            )

            analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

            if analysis_mode == "Multi-district portfolio":
                # --- Portfolio-builder view: clickable table with checkboxes ---
                df_port = df_display.copy()

                # Add a boolean column for portfolio selection if not present
                if "Add to portfolio" not in df_port.columns:
                    df_port["Add to portfolio"] = False

                edited_df = st.data_editor(
                    df_port,
                    width="stretch",
                    key=f"rankings_portfolio_editor_{VARIABLE_SLUG}_{sel_scenario}_{sel_period}_{sel_stat}",
                    num_rows="fixed",
                    disabled=[c for c in df_port.columns if c != "Add to portfolio"],
                )

                st.caption(caption_text)

                st.markdown("---")
                st.markdown("#### Portfolio builder (from rankings table)")

                if st.button(
                    "➕ Add checked districts to portfolio",
                    key=f"btn_add_portfolio_from_table_{VARIABLE_SLUG}_{sel_scenario}_{sel_period}_{sel_stat}",
                ):
                    added = 0
                    for _, row in edited_df.iterrows():
                        if not row.get("Add to portfolio"):
                            continue
                        district_label = row.get("District")
                        state_label = row.get("State")
                        if pd.isna(district_label) or pd.isna(state_label):
                            continue
                        _portfolio_add(str(state_label), str(district_label))
                        added += 1

                    if added > 0:
                        st.success(f"Added {added} district(s) to portfolio.")
                    else:
                        st.info("No new districts were added to the portfolio.")

                # Show current portfolio summary below
                portfolio = st.session_state.get("portfolio_districts", [])
                if portfolio:
                    st.markdown("**Current portfolio (districts)**")
                    try:
                        # If you used dicts {"state":..., "district":...}
                        if isinstance(portfolio[0], dict):
                            port_df = pd.DataFrame(portfolio).rename(
                                columns={"state": "State", "district": "District"}
                            )
                        else:
                            # If you used tuple/list (state, district)
                            port_df = pd.DataFrame(
                                portfolio, columns=["State", "District"]
                            )
                    except Exception:
                        port_df = pd.DataFrame(columns=["State", "District"])
                    st.dataframe(
                        port_df,
                        hide_index=True,
                        use_container_width=True,
                    )

                else:
                    st.caption(
                        "No districts in portfolio yet. Check one or more rows in the table "
                        "above and click **Add checked districts to portfolio**."
                    )

            else:
                # --- Default single-district view: simple rankings table ---
                st.dataframe(
                    df_display,
                    width="stretch",
                )
                st.caption(caption_text)


# -------------------------
# Details panel (with risk cards, sparkline + comparison)
# -------------------------
with col2:
    st.header("Climate Profile")

    # Read current analysis mode (single vs multi-district)
    analysis_mode = st.session_state.get(
        "analysis_mode", "Single district focus"
    )

    # --- Point-level query controls (lat–lon and map selection) ---
    try:
        minx, miny, maxx, maxy = merged.total_bounds
        default_lat = float((miny + maxy) / 2.0)
        default_lon = float((minx + maxx) / 2.0)
    except Exception:
        # Fallback to broad defaults if geometry bounds are unavailable
        miny, maxy = -90.0, 90.0
        minx, maxx = -180.0, 180.0
        default_lat, default_lon = 20.0, 78.0

    # Ensure we know the current analysis mode
    analysis_mode = st.session_state.get(
        "analysis_mode", "Single district focus"
    )

    clear_clicked = False

    # Show Point Query controls only in Multi-district portfolio mode
    if analysis_mode == "Multi-district portfolio":
        # Container for multi-point saved list
        if "point_query_points" not in st.session_state:
            st.session_state["point_query_points"] = []

        with st.expander("📍 Point query (lat–lon)", expanded=False):
            # --- Manual lat / lon input ---
            col_lat, col_lon = st.columns(2)
            with col_lat:
                lat_input = st.number_input(
                    "Latitude",
                    min_value=float(miny),
                    max_value=float(maxy),
                    value=float(st.session_state.get("point_query_lat", default_lat)),
                    format="%.4f",
                )
            with col_lon:
                lon_input = st.number_input(
                    "Longitude",
                    min_value=float(minx),
                    max_value=float(maxx),
                    value=float(st.session_state.get("point_query_lon", default_lon)),
                    format="%.4f",
                )

            # --- Single-point actions ---
            col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)

            # 1) Use manually entered lat/lon as current point
            with col_btn1:
                if st.button("Use this point", key="btn_use_latlon"):
                    lat_f = float(lat_input)
                    lon_f = float(lon_input)
                    st.session_state["point_query_lat"] = lat_f
                    st.session_state["point_query_lon"] = lon_f
                    st.session_state["point_query_latlon"] = {"lat": lat_f, "lon": lon_f}

            # 2) Enable one-shot map selection
            with col_btn2:
                if st.button("Select on map", key="btn_select_on_map"):
                    # Next map click will set the point and then turn this flag off
                    st.session_state["point_query_select_on_map"] = True

            # 3) Clear current point selection
            with col_btn3:
                if st.button("Clear point", key="btn_clear_point"):
                    # Clear any previously stored point selection and marker
                    for _k in (
                        "point_query_lat",
                        "point_query_lon",
                        "point_query_latlon",
                        "point_query_select_on_map",
                    ):
                        st.session_state.pop(_k, None)
                    clear_clicked = True

            # 4) Add current point to saved list (multi-point)
            with col_btn4:
                if st.button("Save point", key="btn_save_point"):
                    try:
                        lat_f = float(lat_input)
                        lon_f = float(lon_input)
                    except (TypeError, ValueError):
                        lat_f, lon_f = None, None

                    if lat_f is not None and lon_f is not None:
                        pts = st.session_state.get("point_query_points", [])
                        # Avoid exact duplicates
                        exists = any(
                            abs(p.get("lat") - lat_f) < 1e-6
                            and abs(p.get("lon") - lon_f) < 1e-6
                            for p in pts
                        )
                        if not exists:
                            pts.append({"lat": lat_f, "lon": lon_f})
                            st.session_state["point_query_points"] = pts

            # Helper text when map-selection mode is active
            if st.session_state.get("point_query_select_on_map", False):
                st.info(
                    "Map selection active: click once on the map to choose a point. "
                    "The next click will set the point and turn off selection."
                )

            # ---- Saved multi-point list + portfolio glue ----
            saved_points = st.session_state.get("point_query_points", [])
            if saved_points:
                st.markdown("**Saved points for portfolio selection**")
                saved_points_df = pd.DataFrame(saved_points)
                saved_points_df.index = saved_points_df.index + 1
                st.dataframe(
                    saved_points_df.rename(
                        columns={"lat": "Latitude", "lon": "Longitude"}
                    ),
                    use_container_width=True,
                )

                col_sp1, col_sp2 = st.columns(2)
                with col_sp1:
                    if st.button("Clear saved points", key="btn_clear_saved_points"):
                        st.session_state["point_query_points"] = []

                with col_sp2:
                    if st.button(
                        "Add saved points' districts to portfolio",
                        key="btn_points_to_portfolio",
                    ):
                        added = 0
                        pts = st.session_state.get("point_query_points", [])
                        for p in pts:
                            plat = p.get("lat")
                            plon = p.get("lon")
                            if plat is None or plon is None:
                                continue
                            try:
                                pt = Point(float(plon), float(plat))
                            except (TypeError, ValueError):
                                continue

                            # Use the same geometry logic as the main point query:
                            try:
                                contains_mask = merged.geometry.contains(pt)
                                if contains_mask.any():
                                    row = merged[contains_mask].iloc[0]
                                else:
                                    centroids = merged.geometry.centroid
                                    dists = centroids.distance(pt)
                                    idx = dists.idxmin()
                                    row = merged.loc[idx]
                            except Exception:
                                continue

                            state_name = str(row.get("state_name", "")).strip()
                            district_name = str(row.get("district_name", "")).strip()
                            if state_name and district_name:
                                _portfolio_add(state_name, district_name)
                                added += 1

                        st.success(
                            f"Added {added} district(s) to portfolio from saved points."
                            if added
                            else "No districts were added (could not match points to districts)."
                        )
            else:
                st.caption(
                    "Use **Save point** to build a list of locations and then "
                    "send their districts into the multi-district portfolio."
                )

    clicked_feature = None
    click_coords = None
    if returned:
        for k in (
            "last_object_clicked",
            "clicked_feature",
            "last_active_drawing",
            "last_object",
        ):
            if returned.get(k):
                clicked_feature = returned.get(k)
                break
        for k in ("last_clicked", "latlng", "last_latlng"):
            val = returned.get(k)
            if isinstance(val, dict) and ("lat" in val or "lng" in val):
                lat = val.get("lat") or val.get("latitude") or val.get("y")
                lng = val.get("lng") or val.get("longitude") or val.get("x")
                if lat is not None and lng is not None:
                    click_coords = (float(lat), float(lng))
                    break
            if isinstance(val, (list, tuple)) and len(val) >= 2:
                try:
                    click_coords = (float(val[0]), float(val[1]))
                    break
                except Exception:
                    pass

    if analysis_mode == "Multi-district portfolio":
        # If map selection mode is active, use the next map click as the
        # point-query location and then disable the mode (one-shot behaviour).
        if click_coords is not None and st.session_state.get(
            "point_query_select_on_map", False
        ):
            lat_click, lon_click = click_coords
            st.session_state["point_query_lat"] = lat_click
            st.session_state["point_query_lon"] = lon_click
            st.session_state["point_query_latlon"] = {
                "lat": lat_click,
                "lon": lon_click,
            }
            st.session_state["point_query_select_on_map"] = False
            # Rerun so the newly selected point is rendered immediately
            st.rerun()

        # If we cleared the point selection this run, ignore any stored
        # point-query coordinates.
        if clear_clicked:
            click_coords = None
        # If we have no current map click but do have a stored point query,
        # reuse the stored point for district lookup.
        elif click_coords is None:
            point_query = st.session_state.get("point_query_latlon")
            if isinstance(point_query, dict):
                try:
                    lat_q = float(point_query.get("lat"))
                    lon_q = float(point_query.get("lon"))
                    click_coords = (lat_q, lon_q)
                except (TypeError, ValueError):
                    click_coords = None

    clicked_name2 = extract_name_from_feature(clicked_feature) if clicked_feature else None
    matched_row = None
    if clicked_name2:
        mask = merged["district_name"].astype(str).str.lower() == str(clicked_name2).lower()
        matched_row = merged[mask].iloc[0:1] if mask.any() else None
        if matched_row is None or matched_row.empty:
            mask2 = (
                merged["district_name"]
                .astype(str)
                .str.lower()
                .str.contains(str(clicked_name2).lower())
            )
            if mask2.any():
                matched_row = merged[mask2].iloc[0:1]

    if (matched_row is None or matched_row.empty) and (click_coords is not None):
        lat, lng = click_coords
        pt = Point(float(lng), float(lat))
        try:
            contains_mask = merged.geometry.contains(pt)
            matched_row = merged[contains_mask].iloc[0:1] if contains_mask.any() else None
            if matched_row is None or matched_row.empty:
                centroids = merged.geometry.centroid
                dists = centroids.distance(pt)
                idx = dists.idxmin()
                matched_row = merged.loc[[idx]]
        except Exception:
            matched_row = None

    if (
        matched_row is None
        or matched_row.empty
    ) and st.session_state.get("selected_district", "All") != "All":
        mask = (
            merged["district_name"]
            .astype(str)
            .str.lower()
            == str(st.session_state["selected_district"]).lower()
        )
        if mask.any():
            matched_row = merged[mask].iloc[0:1]

    # -------------- Helper for baseline detection --------------
    def find_baseline_column(
        df_cols, base_metric: str
    ) -> Optional[str]:
        """
        Try to find a 'baseline' column for the same metric:
        Prefer historical 1995-2014; else earliest historical period; else None.
        Columns are in <metric>__<scenario>__<period>__<stat> form.
        """
        pat = re.compile(
            rf"^{re.escape(base_metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__mean$"
        )
        candidates = []
        for c in df_cols:
            m = pat.match(str(c))
            if m and m.group("scenario").lower() == "historical":
                candidates.append((c, m.group("period")))
        if not candidates:
            return None
        # Prefer 1995-2014 if present
        for c, p in candidates:
            if p.replace(" ", "") in ("1995-2014", "1995_2014", "1985-2014"):
                return c
        # else pick lexicographically earliest period
        candidates.sort(key=lambda x: x[1])
        return candidates[0][0]

    # ----------- STATE SUMMARY MODE (no district selected) -----------
    if (matched_row is None or matched_row.empty) and selected_district == "All":
        if selected_state != "All":
            ensemble, per_model_df, sel_districts_gdf = compute_state_metrics_from_merged(
                merged, adm1, metric_col, selected_state
            )

            st.subheader(f"{selected_state} — State summary")
            st.markdown(
                f"**Index:** {VARIABLES[VARIABLE_SLUG]['label']}  \n"
                f"**Scenario:** {sel_scenario}  \n"
                f"**Period:** {sel_period}"
            )

            # --- Expander 2: District-wise distribution across models (boxplot) ---
            with st.expander("District-wise distribution across models", expanded=False):
                st.caption(
                    "This figure can be slow to generate because it uses per-model "
                    "distributions for each district."
                )
                if st.button(
                    "Generate district-wise boxplot",
                    key=f"btn_state_boxplot_{VARIABLE_SLUG}_{selected_state}_{sel_scenario}_{sel_period}_{sel_stat}",
                ):
                    fig_box = make_state_boxplot_for_districts(
                        sel_districts_gdf=sel_districts_gdf,
                        metric_col=metric_col,
                        metric_label=VARIABLES[VARIABLE_SLUG]["label"],
                        sel_state=selected_state,
                        sel_scenario=sel_scenario,
                        sel_period=sel_period,
                        sel_stat=sel_stat,
                    )
                    if fig_box is not None:
                        st.pyplot(fig_box, width="stretch")
                    else:
                        st.info(
                            "Per-model district data is not available for this index, "
                            "so the boxplot could not be generated."
                        )
                # else:
                #     st.info("Click the button above to generate the boxplot when needed.")

            # --- Helper functions for state-level yearly time-series (unchanged) ---
            @st.cache_data
            def _load_state_yearly(ts_root_str: str, state_dir: str) -> pd.DataFrame:
                f = Path(ts_root_str) / state_dir / "state_yearly_ensemble_stats.csv"
                if not f.exists():
                    return pd.DataFrame()
                for enc in (None, "ISO-8859-1"):
                    try:
                        return pd.read_csv(f, encoding=enc) if enc else pd.read_csv(f)
                    except Exception:
                        pass
                return pd.read_csv(f, encoding="utf-8", errors="replace")

            def _make_state_yearly_pdf(
                df_yearly: pd.DataFrame,
                state_name: str,
                scenario_name: str,
                metric_label: str,
                out_dir: Path,
            ) -> Optional[Path]:
                if df_yearly is None or df_yearly.empty:
                    return None
                d = df_yearly.copy()
                d = d[
                    (d["state"].astype(str).str.strip().str.lower() == state_name.strip().lower())
                    & (
                        d["scenario"]
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        == scenario_name.strip().lower()
                    )
                ]
                if d.empty:
                    return None
                for c in ("year", "mean", "p05", "p95"):
                    if c in d.columns:
                        d[c] = pd.to_numeric(d[c], errors="coerce")
                d = d.dropna(subset=["year"]).sort_values("year")
                if d.empty:
                    return None

                fig, ax = plt.subplots(figsize=(7.5, 4.5), dpi=150)
                x = d["year"]
                y = d["mean"]
                ax.plot(x, y, marker="o", linewidth=1.5, label="Mean")

                if "p05" in d.columns and "p95" in d.columns:
                    ax.fill_between(
                        x,
                        d["p05"],
                        d["p95"],
                        alpha=0.2,
                        label="5–95% range",
                    )

                ax.set_xlabel("Year")
                ax.set_ylabel(metric_label)
                ax.set_title(f"{state_name} — {metric_label} ({scenario_name})")
                ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
                ax.legend(frameon=False, ncol=3, fontsize=9)
                out_dir.mkdir(parents=True, exist_ok=True)

                safe = lambda s: "".join(
                    c if c.isalnum() or c in ("-", "_") else "_" for c in str(s)
                )
                pdf_path = (
                    out_dir
                    / f"{safe(state_name)}__{safe(metric_label)}__{safe(scenario_name)}__yearly_timeseries.pdf"
                )
                fig.tight_layout()
                fig.savefig(pdf_path, format="pdf")
                plt.close(fig)
                return pdf_path

            # Only show these state-summary expanders when NOT in multi-district portfolio mode
            analysis_mode_state = st.session_state.get("analysis_mode", "Single district focus")

            if analysis_mode_state != "Multi-district portfolio":

                # --- Expander 1: State summary statistics (like district Risk summary) ---
                with st.expander("State summary statistics", expanded=False):
                    if ensemble.get("n_districts", 0) > 0:
                        stat_rows = [
                            {"Statistic": "mean", "Value": f"{ensemble['mean']:.2f}"},
                            {"Statistic": "median", "Value": f"{ensemble['median']:.2f}"},
                            {"Statistic": "p05", "Value": f"{ensemble['p05']:.2f}"},
                            {"Statistic": "p95", "Value": f"{ensemble['p95']:.2f}"},
                            {"Statistic": "std", "Value": f"{ensemble['std']:.2f}"},
                            {
                                "Statistic": "n_districts",
                                "Value": str(int(ensemble["n_districts"])),
                            },
                        ]
                        st.table(pd.DataFrame(stat_rows).set_index("Statistic"))
                    else:
                        st.info("No numeric district values found for this state & selection.")

                # --- Expander 2: Per-model state averages ---
                with st.expander("Per-model state averages", expanded=False):
                    if not per_model_df.empty:
                        # st.markdown("**Per-model state averages**")
                        st.dataframe(
                            per_model_df.rename(
                                columns={"value": "state_avg", "n_districts": "n_districts_used"}
                            ),
                            width="stretch",
                        )

                    if sel_districts_gdf is not None and not sel_districts_gdf.empty:
                        st.caption(f"Districts used: {len(sel_districts_gdf)}")

                # --- Expander 3: Trend over time (state-average) ---
                with st.expander("Trend over time (state average)", expanded=False):
                    st.caption(
                        "Generates a state-average yearly trend plot and PDF for the "
                        "selected index, scenario, and period."
                    )

                    if st.button(
                        "Generate state-average trend PDF",
                        key=f"btn_state_trend_{VARIABLE_SLUG}_{selected_state}_{sel_scenario}",
                    ):
                        _yearly_df = _load_state_yearly(str(PROCESSED_ROOT), PILOT_STATE)
                        pdf_path = _make_state_yearly_pdf(
                            _yearly_df,
                            selected_state,
                            sel_scenario,
                            VARIABLES[VARIABLE_SLUG]["label"],
                            PROCESSED_ROOT / "pdf_plots",
                        )
                        if pdf_path is not None and pdf_path.exists():
                            with open(pdf_path, "rb") as fh:
                                st.download_button(
                                    "⬇️ Download state-average time-series (PDF)",
                                    fh.read(),
                                    file_name=pdf_path.name,
                                    mime="application/pdf",
                                )
                        else:
                            st.info(
                                "State-average yearly time-series is not available for this combination."
                            )

    # ----------- DISTRICT DETAILS MODE (enhanced) -----------
    else:
        row = matched_row.iloc[0]
        district_name = row.get("district_name", "Unknown")
        state_to_show = (
            st.session_state.get("selected_state")
            if st.session_state.get("selected_state") != "All"
            else (row.get("state_name") or "Unknown")
        )

        st.subheader(district_name)
        st.markdown(f"**State:** {state_to_show}")

        # If this view was triggered by a point query, show the coordinates used.
        if click_coords is not None:
            st.caption(
                f"Point location used: lat {click_coords[0]:.4f}, "
                f"lon {click_coords[1]:.4f} (assigned to this district)."
            )

        # --- Portfolio add button (for multi-district analysis) ---
        if st.session_state.get("analysis_mode", "Single district focus") == "Multi-district portfolio":
            if st.button(
                "➕ Add this district to portfolio",
                key=f"btn_add_portfolio_single_{state_to_show}_{district_name}",
            ):
                _portfolio_add(state_to_show, district_name)
                st.success(f"Added {district_name}, {state_to_show} to portfolio.")

        # ---- Risk cards (1.1) ----
        current_val = row.get(metric_col)
        current_val_f = float(current_val) if not pd.isna(current_val) else None

        # baseline: same metric, historical, baseline period
        baseline_col = find_baseline_column(df.columns, sel_metric)
        baseline_val = row.get(baseline_col) if baseline_col else np.nan
        baseline_val_f = float(baseline_val) if not pd.isna(baseline_val) else None

        # position within state: rank + percentile
        percentile_state = None
        rank_in_state = None
        n_in_state = None
        try:
            in_state_mask = (
                merged["state_name"].astype(str).str.strip().str.lower()
                == str(state_to_show).strip().lower()
            )
            state_vals = pd.to_numeric(
                merged.loc[in_state_mask, metric_col], errors="coerce"
            ).dropna()

            if current_val_f is not None and not state_vals.empty:
                n_in_state = int(len(state_vals))
                # percentile: fraction of districts with lower value
                percentile_state = float(
                    (state_vals < current_val_f).sum() / n_in_state * 100.0
                )
                # rank: 1 = highest value (most extreme / highest risk)
                rank_in_state = int((state_vals > current_val_f).sum() + 1)
        except Exception:
            pass

        # ---- Wrap risk cards in an expander slab ----
        with st.expander("Risk summary", expanded=True):

            colc1, colc2, colc3 = st.columns(3)
            with colc1:
                st.markdown("**Current value**")
                if current_val_f is not None:
                    st.metric(
                        label="Current Value",
                        label_visibility="collapsed",
                        value=f"{current_val_f:.2f}",
                        help=f"{VARIABLES[VARIABLE_SLUG]['label']} ({sel_scenario}, {sel_period}, {sel_stat})",
                    )
                else:
                    st.write("No data")

            with colc2:
                st.markdown("**Change vs baseline**")
                if current_val_f is not None and baseline_val_f is not None:
                    diff_abs = current_val_f - baseline_val_f
                    diff_pct = (
                        (diff_abs / baseline_val_f * 100.0)
                        if baseline_val_f not in (0.0, None)
                        else None
                    )
                    delta_str = f"{diff_abs:+.2f}"
                    if diff_pct is not None:
                        delta_str += f" ({diff_pct:+.1f}%)"

                    # Pretty baseline descriptor: only scenario, period, stat
                    if baseline_col:
                        parts = str(baseline_col).split("__")
                        if len(parts) == 4:
                            _, base_scenario, base_period, base_stat = parts
                            baseline_desc = f"{base_scenario}, {base_period}, {base_stat}"
                        else:
                            baseline_desc = str(baseline_col)
                    else:
                        baseline_desc = "not found"

                    st.metric(
                        label="Change Vs Baseline",
                        label_visibility="collapsed",
                        value=f"{baseline_val_f:.2f}",
                        delta=delta_str,
                        help=f"Baseline: {baseline_desc}",
                    )
                else:
                    st.write("Baseline not available")

            with colc3:
                st.markdown("**Position in state**")
                if rank_in_state is not None and n_in_state is not None:
                    # Display as "3/33" style rank
                    rank_label = f"{rank_in_state}/{n_in_state}"

                    if percentile_state is not None:
                        help_text = (
                            f"Approximate percentile: {percentile_state:.0f}th\n"
                            f"Computed among {n_in_state} districts in {state_to_show} "
                            f"for this index (higher values = higher rank)."
                        )
                    else:
                        help_text = (
                            f"Computed among {n_in_state} districts in {state_to_show} "
                            f"(higher values = higher rank)."
                        )

                    st.metric(
                        label="Rank in state",
                        label_visibility="collapsed",
                        value=rank_label,
                        help=help_text,
                    )
                else:
                    st.write("Insufficient data")

        # ---- Sparkline + uncertainty band (1.2) & narrative (1.3) ----

        @st.cache_data
        def _read_yearly_csv(fpath: Path) -> pd.DataFrame:
            d = None
            for enc in (None, "ISO-8859-1"):
                try:
                    d = pd.read_csv(fpath, encoding=enc) if enc else pd.read_csv(fpath)
                    break
                except Exception:
                    d = None
            if d is None:
                try:
                    d = pd.read_csv(fpath, encoding="utf-8", errors="replace")
                except Exception:
                    return pd.DataFrame()
            required = {"district", "scenario", "year", "mean"}
            return d if required.issubset(set(map(str, d.columns))) else pd.DataFrame()

        def _slugify_fs(s: str) -> str:
            s = (
                unicodedata.normalize("NFKD", str(s))
                .encode("ascii", "ignore")
                .decode("ascii")
            )
            s = re.sub(r"[^A-Za-z0-9]+", "_", s.strip())
            return re.sub(r"_+", "_", s).strip("_").lower()

        @st.cache_data
        def _load_district_yearly(
            ts_root: Path,
            state_dir: str,
            district_display: str,
            scenario_name: str,
            varcfg: dict,
            aliases: dict | None = None,
        ) -> pd.DataFrame:
            """
            Load the *scenario-specific* yearly ensemble CSV for a district.
            """
            base = Path(ts_root) / state_dir
            if not base.exists():
                return pd.DataFrame()
            try:
                existing_dirs = [p for p in base.iterdir() if p.is_dir()]
            except Exception:
                existing_dirs = []

            disp = str(district_display).strip()
            scenario = str(scenario_name).strip()
            root = str(Path(ts_root))
            district_u = _slugify_fs(disp)
            district_underscored = disp.replace(" ", "_")

            # direct candidates by registry
            cands = []
            for pat in varcfg.get("district_yearly_candidates", []):
                cands.append(
                    pat.format(
                        root=root,
                        state=state_dir,
                        district=disp,
                        district_underscored=district_underscored,
                        scenario=scenario,
                    )
                )
            seen = set()
            cands = [c for c in cands if not (c in seen or seen.add(c))]
            for full in cands:
                f = Path(full)
                if f.exists():
                    df_local = _read_yearly_csv(f)
                    if not df_local.empty:
                        return df_local

            # fallbacks by folder scanning (generic stats file)
            def _norm(s: str) -> str:
                s = (
                    unicodedata.normalize("NFKD", str(s))
                    .encode("ascii", "ignore")
                    .decode("ascii")
                )
                s = s.lower()
                s = re.sub(r"[_\-\W]+", " ", s)
                s = re.sub(r"\s+", " ", s).strip()
                return s

            disp_norm = _norm(disp)
            cand_names = [
                disp,
                disp.replace(" ", "_"),
                disp.replace("_", " "),
                re.sub(r"\s+", "_", disp_norm),
                disp_norm,
            ]
            aliases = aliases or {}
            ali = aliases.get(disp_norm)
            if ali:
                cand_names += [
                    ali,
                    ali.replace(" ", "_"),
                    re.sub(r"\s+", "_", _norm(ali)),
                ]
            seen = set()
            cand_names = [c for c in cand_names if not (c in seen or seen.add(c))]

            for name in cand_names:
                p = base / name
                f = p / "district_yearly_ensemble_stats.csv"
                if f.exists():
                    df_local = _read_yearly_csv(f)
                    if not df_local.empty:
                        # filter by scenario if needed
                        if "scenario" in df_local.columns:
                            df_local = df_local[
                                df_local["scenario"]
                                .astype(str)
                                .str.strip()
                                .str.lower()
                                == scenario.lower()
                            ]
                        return df_local

            contains_hits = [p for p in existing_dirs if disp_norm in _norm(p.name)]
            for p in contains_hits:
                f = p / "district_yearly_ensemble_stats.csv"
                if f.exists():
                    df_local = _read_yearly_csv(f)
                    if not df_local.empty:
                        if "scenario" in df_local.columns:
                            df_local = df_local[
                                df_local["scenario"]
                                .astype(str)
                                .str.strip()
                                .str.lower()
                                == scenario.lower()
                            ]
                        return df_local

            folder_names = [p.name for p in existing_dirs]
            best = difflib.get_close_matches(disp, folder_names, n=1, cutoff=0.72)
            if best:
                p = base / best[0]
                f = p / "district_yearly_ensemble_stats.csv"
                if f.exists():
                    df_local = _read_yearly_csv(f)
                    if not df_local.empty:
                        if "scenario" in df_local.columns:
                            df_local = df_local[
                                df_local["scenario"]
                                .astype(str)
                                .str.strip()
                                .str.lower()
                                == scenario.lower()
                            ]
                        return df_local
            return pd.DataFrame()

        def _filter_series_for_trend(
            df: pd.DataFrame, state_name: str, district_name: str
        ) -> pd.DataFrame:
            """
            Extract a clean time series for a single state+district from a
            scenario-specific yearly dataframe.
            """
            if df is None or df.empty:
                return pd.DataFrame()
            d = df.copy()
            cols = set(map(str, d.columns))
            if not {"district", "year", "mean"}.issubset(cols):
                return pd.DataFrame()
            if "state" not in d.columns:
                d["state"] = state_name

            def _n(s: str) -> str:
                return alias(s)

            d["_state_key"] = d["state"].astype(str).map(_n)
            d["_district_key"] = d["district"].astype(str).map(_n)

            mask = (
                (d["_state_key"] == _n(state_name))
                & (d["_district_key"] == _n(district_name))
            )
            if not mask.any():
                mask = (
                    (d["_state_key"] == _n(state_name))
                    & d["_district_key"].str.contains(_n(district_name), na=False)
                )
            d = d[mask]
            if d.empty:
                return d

            for c in ("year", "mean", "p05", "p95"):
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            d = d.dropna(subset=["year", "mean"]).sort_values("year")
            return d

        def _make_district_yearly_pdf(
            df_yearly: pd.DataFrame,
            state_name: str,
            district_name: str,
            scenario_name: str,
            metric_label: str,
            out_dir: Path,
        ) -> Optional[Path]:
            """
            Make a PDF for a *single scenario* time series (reused for download).
            """
            if df_yearly is None or df_yearly.empty:
                return None
            d = df_yearly.copy()
            cols = set(map(str, d.columns))
            if not {"district", "scenario", "year", "mean"}.issubset(cols):
                return None
            if "state" not in d.columns:
                d["state"] = state_name
            has_p05, has_p95 = ("p05" in d.columns), ("p95" in d.columns)

            def _n(s: str) -> str:
                return alias(s)

            d["_state_key"] = d["state"].astype(str).map(_n)
            d["_district_key"] = d["district"].astype(str).map(_n)
            d["_scen_key"] = d["scenario"].astype(str).str.strip().str.lower()
            mask = (
                (d["_state_key"] == _n(state_name))
                & (d["_district_key"] == _n(district_name))
                & (d["_scen_key"] == scenario_name.strip().lower())
            )
            if not mask.any():
                mask = (
                    (d["_state_key"] == _n(state_name))
                    & d["_district_key"].str.contains(_n(district_name), na=False)
                    & (d["_scen_key"] == scenario_name.strip().lower())
                )
            if not mask.any():
                cand = d.loc[
                    (d["_state_key"] == _n(state_name))
                    & (d["_scen_key"] == scenario_name.strip().lower()),
                    "_district_key",
                ].dropna().unique().tolist()
                best = difflib.get_close_matches(_n(district_name), cand, n=1, cutoff=0.72)
                if best:
                    mask = (
                        (d["_state_key"] == _n(state_name))
                        & (d["_district_key"] == best[0])
                        & (d["_scen_key"] == scenario_name.strip().lower())
                    )

            d = d[mask]
            if d.empty:
                return None

            for c in ("year", "mean"):
                d[c] = pd.to_numeric(d[c], errors="coerce")
            if has_p05:
                d["p05"] = pd.to_numeric(d.get("p05"), errors="coerce")
            if has_p95:
                d["p95"] = pd.to_numeric(d.get("p95"), errors="coerce")
            d = d.dropna(subset=["year"]).sort_values("year")
            if d.empty:
                return None

            fig, ax = plt.subplots(figsize=(7.5, 4.5), dpi=150)
            ax.plot(d["year"], d["mean"], linewidth=3.0, label="Mean")
            if has_p05:
                ax.plot(d["p05"], linewidth=1.5, label="5th percentile")
            if has_p95:
                ax.plot(d["p95"], linewidth=1.5, label="95th percentile")
            ax.set_xlabel("Year")
            ax.set_ylabel(metric_label)
            ax.set_title(
                f"{district_name}, {state_name} • {metric_label} • {scenario_name}"
            )
            ax.grid(True, linestyle="--", alpha=0.35)
            ax.legend(frameon=False, ncol=3, fontsize=9)
            out_dir.mkdir(parents=True, exist_ok=True)
            safe = lambda s: "".join(
                c if c.isalnum() or c in ("-", "_") else "_" for c in str(s)
            )
            pdf_path = (
                out_dir
                / f"{safe(state_name)}__{safe(district_name)}__"
                  f"{safe(metric_label)}__{safe(scenario_name)}__yearly_timeseries.pdf"
            )
            fig.tight_layout()
            fig.savefig(pdf_path, format="pdf")
            plt.close(fig)
            return pdf_path

        def _create_trend_figure_for_index(
            hist_ts: pd.DataFrame,
            scen_ts: pd.DataFrame,
            idx_label: str,
            scenario_name: str,
            ax: "plt.Axes | None" = None,
            figsize: tuple[float, float] = (4.8, 2.4),
        ):
            """
            Create the same 'Trend over time' figure used in the Climate Profile panel.

            If ``ax`` is provided, the plot is drawn into that axis and the parent
            figure is returned. Otherwise, a new figure is created with the given
            ``figsize`` and returned.

            Parameters
            ----------
            hist_ts : pd.DataFrame
                Historical time series with at least 'year' and 'mean' columns.
                Optional columns: 'p05', 'p95'.
            scen_ts : pd.DataFrame
                Scenario time series with the same columns as hist_ts.
            idx_label : str
                Label for the y-axis (index name).
            scenario_name : str
                Scenario slug, e.g., 'ssp245' or 'ssp585'.
            ax : matplotlib.axes.Axes, optional
                If provided, draw into this axis instead of creating a new figure.
            figsize : tuple[float, float]
                Figure size if a new figure is created.

            Returns
            -------
            matplotlib.figure.Figure
                Figure that contains the trend plot.
            """
            if ax is None:
                fig_ts, ax_ts = plt.subplots(figsize=figsize, dpi=150)
            else:
                ax_ts = ax
                fig_ts = ax_ts.figure

            has_any = False

            # Historical: 1990–2010 (or whatever range is in hist_ts) in blue + band
            if hist_ts is not None and not hist_ts.empty:
                ax_ts.plot(
                    hist_ts["year"],
                    hist_ts["mean"],
                    linewidth=2.0,
                    color="tab:blue",
                    label="Historical",
                )
                if {"p05", "p95"}.issubset(hist_ts.columns):
                    ax_ts.fill_between(
                        hist_ts["year"],
                        hist_ts["p05"],
                        hist_ts["p95"],
                        alpha=0.2,
                        color="tab:blue",
                    )
                has_any = True

            # Scenario: 2020–2060 (or whatever range) in red + band
            if scen_ts is not None and not scen_ts.empty:
                scen_label = (scenario_name or "scenario").upper()
                ax_ts.plot(
                    scen_ts["year"],
                    scen_ts["mean"],
                    linewidth=2.0,
                    color="tab:red",
                    label=scen_label,
                )
                if {"p05", "p95"}.issubset(scen_ts.columns):
                    ax_ts.fill_between(
                        scen_ts["year"],
                        scen_ts["p05"],
                        scen_ts["p95"],
                        alpha=0.2,
                        color="tab:red",
                    )
                has_any = True

            # Transition line: last historical → first scenario in grey dashed
            if (
                hist_ts is not None
                and scen_ts is not None
                and not hist_ts.empty
                and not scen_ts.empty
            ):
                try:
                    last_hist_year = int(hist_ts["year"].max())
                    last_hist = hist_ts.loc[hist_ts["year"] == last_hist_year].iloc[-1]

                    target_year = 2020
                    if "year" in scen_ts.columns and target_year in scen_ts["year"].values:
                        first_scen = scen_ts.loc[scen_ts["year"] == target_year].iloc[0]
                    else:
                        first_scen = scen_ts.loc[scen_ts["year"].idxmin()]

                    ax_ts.plot(
                        [last_hist["year"], first_scen["year"]],
                        [last_hist["mean"], first_scen["mean"]],
                        color="grey",
                        linestyle="--",
                        linewidth=1.5,
                    )
                except Exception:
                    # If something odd happens (e.g. missing values), don't kill the plot
                    pass

            ax_ts.set_xlabel("Year")
            ax_ts.set_ylabel(idx_label)

            if has_any:
                ax_ts.grid(True, linestyle="--", alpha=0.25)
                for spine in ax_ts.spines.values():
                    spine.set_visible(False)
                handles, labels = ax_ts.get_legend_handles_labels()
                if handles:
                    ax_ts.legend(frameon=False, fontsize=8, ncol=3)

            if ax is None:
                fig_ts.tight_layout()

            return fig_ts

        def _build_district_case_study_data(
            state_name: str,
            district_name: str,
            index_slugs: list[str],
            sel_scenario: str,
            sel_period: str,
            sel_stat: str,
        ):
            """
            Assemble per-index summary metrics + yearly time series + scenario
            comparison panel for a single (state, district).

            Returns
            -------
            summary_df : pd.DataFrame
                One row per index_slug with current value, baseline, delta,
                ranking & risk class.
            timeseries_by_index : dict[str, dict[str, pd.DataFrame]]
                {"slug": {"historical": df_hist, "scenario": df_scen}}
            scenario_panels : dict[str, pd.DataFrame]
                {"slug": panel_df} from build_scenario_comparison_panel_for_row.
            """
            records: list[dict] = []
            timeseries_by_index: dict[str, dict[str, pd.DataFrame]] = {}
            scenario_panels: dict[str, pd.DataFrame] = {}

            for slug in index_slugs:
                varcfg = VARIABLES.get(slug)
                if not varcfg:
                    continue

                # Determine processed root for this index, similar to PROCESSED_ROOT logic
                env_root = os.getenv("IRT_PROCESSED_ROOT")
                if env_root:
                    base_path = Path(env_root)
                    if base_path.name.lower() == slug.lower():
                        proc_root = base_path
                    else:
                        proc_root = base_path / slug
                else:
                    proc_root = DATA_DIR / "processed" / slug
                proc_root = proc_root.resolve()

                master_path = proc_root / PILOT_STATE / "master_metrics_by_district.csv"
                if not master_path.exists():
                    continue

                try:
                    df_master, schema_items_local, metrics_local, by_metric_local = _load_master_and_schema(
                        master_path, slug
                    )
                except Exception:
                    continue

                if df_master is None or df_master.empty:
                    continue

                # Decide metric name for this slug (align with normalized metrics)
                registry_metric = varcfg.get("periods_metric_col")
                available_metrics = set(metrics_local or [])
                if not available_metrics:
                    continue
                if registry_metric not in available_metrics:
                    m_lower = {m.lower(): m for m in available_metrics}
                    registry_metric = m_lower.get(str(registry_metric).lower(), next(iter(available_metrics)))

                metric_col = f"{registry_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
                if metric_col not in df_master.columns:
                    # skip this index if we don't have the requested scenario/period/stat
                    continue

                # Robust match for a single state+district row
                dm = df_master.copy()
                if "state" not in dm.columns or "district" not in dm.columns:
                    continue

                def _n(s: str) -> str:
                    return alias(s)

                dm["_state_key"] = dm["state"].astype(str).map(_n)
                dm["_district_key"] = dm["district"].astype(str).map(_n)

                target_state = _n(state_name)
                target_dist = _n(district_name)

                mask = (dm["_state_key"] == target_state) & (dm["_district_key"] == target_dist)
                if not mask.any():
                    # fall back to contains on district name within same state
                    mask = (dm["_state_key"] == target_state) & dm["_district_key"].str.contains(
                        target_dist, na=False
                    )
                if not mask.any():
                    continue

                row_local = dm.loc[mask].iloc[0]

                current_val = row_local.get(metric_col)
                current_val_f = pd.to_numeric([current_val], errors="coerce")[0]
                if pd.isna(current_val_f):
                    current_val_f = None

                # Baseline for same metric/stat in historical baseline period
                baseline_col = find_baseline_column_for_stat(dm.columns, registry_metric, sel_stat)
                baseline_val_f = None
                if baseline_col and baseline_col in dm.columns:
                    baseline_val = row_local.get(baseline_col)
                    baseline_val_f = pd.to_numeric([baseline_val], errors="coerce")[0]
                    if pd.isna(baseline_val_f):
                        baseline_val_f = None

                if current_val_f is not None and baseline_val_f is not None:
                    delta_abs = current_val_f - baseline_val_f
                    delta_pct = None
                    if baseline_val_f not in (0.0,):
                        delta_pct = (delta_abs / baseline_val_f) * 100.0
                else:
                    delta_abs = None
                    delta_pct = None

                # Ranking within state
                state_mask = dm["_state_key"] == target_state
                state_vals = pd.to_numeric(dm.loc[state_mask, metric_col], errors="coerce").dropna()
                n_in_state = int(len(state_vals)) if len(state_vals) else None
                rank_in_state = None
                percentile_in_state = None
                if n_in_state and current_val_f is not None:
                    rank_in_state = int((state_vals > current_val_f).sum() + 1)
                    percentile_in_state = float(
                        (state_vals < current_val_f).sum() / n_in_state * 100.0
                    )
                risk_class = (
                    risk_class_from_percentile(percentile_in_state)
                    if percentile_in_state is not None
                    else "Unknown"
                )

                records.append(
                    {
                        "index_slug": slug,
                        "index_label": varcfg.get("label", slug),
                        "group": varcfg.get("group"),
                        "scenario": sel_scenario,
                        "period": sel_period,
                        "stat": sel_stat,
                        "current": current_val_f,
                        "baseline": baseline_val_f,
                        "delta_abs": delta_abs,
                        "delta_pct": delta_pct,
                        "rank_in_state": rank_in_state,
                        "percentile_in_state": percentile_in_state,
                        "n_in_state": n_in_state,
                        "risk_class": risk_class,
                    }
                )

                # Timeseries for this index
                ts_root = proc_root
                hist_df = _load_district_yearly(
                    ts_root=ts_root,
                    state_dir=str(state_name),
                    district_display=str(district_name),
                    scenario_name="historical",
                    varcfg=varcfg,
                    aliases=NAME_ALIASES,
                )
                scen_df = _load_district_yearly(
                    ts_root=ts_root,
                    state_dir=str(state_name),
                    district_display=str(district_name),
                    scenario_name=sel_scenario,
                    varcfg=varcfg,
                    aliases=NAME_ALIASES,
                )
                hist_ts = _filter_series_for_trend(hist_df, state_name, district_name)
                scen_ts = _filter_series_for_trend(scen_df, state_name, district_name)
                timeseries_by_index[slug] = {
                    "historical": hist_ts,
                    "scenario": scen_ts,
                }

                # Scenario comparison panel (period-mean across scenarios)
                try:
                    panel_df = build_scenario_comparison_panel_for_row(
                        row=row_local,
                        schema_items=schema_items_local,
                        metric_name=registry_metric,
                        sel_stat=sel_stat,
                    )
                except Exception:
                    panel_df = None
                if panel_df is not None and not panel_df.empty:
                    scenario_panels[slug] = panel_df

            summary_df = pd.DataFrame.from_records(records) if records else pd.DataFrame()
            return summary_df, timeseries_by_index, scenario_panels

        def _make_case_study_zip(
            state_name: str,
            district_name: str,
            summary_df: pd.DataFrame,
            ts_dict: dict[str, dict[str, pd.DataFrame]],
            panel_dict: dict[str, pd.DataFrame],
            pdf_bytes: bytes,
        ) -> bytes:
            """
            Build a ZIP (as bytes) containing:
              - summary.csv
              - timeseries_<index>_<scenario>.csv
              - scenario_mean_<index>.csv
              - climate_profile_<state>__<district>.pdf
            """
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                if summary_df is not None and not summary_df.empty:
                    zf.writestr(
                        "summary.csv",
                        summary_df.to_csv(index=False).encode("utf-8"),
                    )

                for slug, parts in (ts_dict or {}).items():
                    for scen_key, ts_df in (parts or {}).items():
                        if ts_df is None or ts_df.empty:
                            continue
                        df_out = ts_df.copy()
                        if "scenario" not in df_out.columns:
                            df_out["scenario"] = scen_key
                        df_out["index_slug"] = slug
                        df_out["index_label"] = VARIABLES.get(slug, {}).get("label", slug)
                        name = f"timeseries_{_slugify_fs(slug)}_{scen_key}.csv"
                        zf.writestr(name, df_out.to_csv(index=False).encode("utf-8"))

                for slug, panel_df in (panel_dict or {}).items():
                    if panel_df is None or panel_df.empty:
                        continue
                    df_out = panel_df.copy()
                    df_out["index_slug"] = slug
                    df_out["index_label"] = VARIABLES.get(slug, {}).get("label", slug)
                    name = f"scenario_mean_{_slugify_fs(slug)}.csv"
                    zf.writestr(name, df_out.to_csv(index=False).encode("utf-8"))

                if pdf_bytes:
                    safe_state = _slugify_fs(state_name)
                    safe_dist = _slugify_fs(district_name)
                    zf.writestr(
                        f"climate_profile_{safe_state}__{safe_dist}.pdf",
                        pdf_bytes,
                    )
            buf.seek(0)
            return buf.getvalue()

        def _make_district_case_study_pdf(
            state_name: str,
            district_name: str,
            summary_df: pd.DataFrame,
            ts_dict: dict[str, dict[str, pd.DataFrame]],
            panel_dict: dict[str, pd.DataFrame],
            sel_scenario: str,
            sel_period: str,
            sel_stat: str,
        ) -> bytes:
            """
            Build a multi-page PDF for a single district and multiple indices.

            Page 1  : A4 cover + summary table
            Page 2+ : One full A4 page per index with:

                    Row 1 – yearly trend plot (historical + scenario)
                    Row 2 – period-mean scenario comparison bar chart
                    Row 3 – short narrative + scenario bullets
            """
            if summary_df is None or summary_df.empty:
                return b""

            buf = io.BytesIO()
            with PdfPages(buf) as pdf:
                # ------------------------------------------------------------------
                # Cover / summary page (A4)
                # ------------------------------------------------------------------
                fig = plt.figure(figsize=(8.27, 11.69), dpi=150)
                fig.patch.set_facecolor("white")

                title = f"{district_name}, {state_name} — Climate profile"
                fig.text(
                    0.5,
                    0.92,
                    title,
                    ha="center",
                    va="top",
                    fontsize=16,
                    fontweight="bold",
                )

                subtitle = (
                    f"Scenario: {sel_scenario.upper()}   |   "
                    f"Period: {sel_period}   |   Statistic: {sel_stat}"
                )
                fig.text(0.5, 0.88, subtitle, ha="center", va="top", fontsize=10)

                fig.text(
                    0.5,
                    0.84,
                    f"Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                    ha="center",
                    va="top",
                    fontsize=8,
                )

                summary_to_show = summary_df.copy()

                # 1) Format numeric columns
                num_cols = ["current", "baseline", "delta_abs", "delta_pct", "percentile_in_state"]
                for c in num_cols:
                    if c in summary_to_show.columns:
                        summary_to_show[c] = summary_to_show[c].apply(
                            lambda x: f"{x:.2f}"
                            if isinstance(x, (int, float)) and not pd.isna(x)
                            else ""
                        )

                # 2) Select & order columns
                cols_order = [
                    "index_label",
                    "group",
                    "current",
                    "baseline",
                    "delta_abs",
                    "delta_pct",
                    "rank_in_state",
                    "percentile_in_state",
                    "risk_class",
                ]
                cols_existing = [c for c in cols_order if c in summary_to_show.columns]
                summary_to_show = summary_to_show[cols_existing]

                # 3) Wrap long text so it doesn't overflow table cells
                def _wrap_text(val: object, width: int) -> object:
                    if not isinstance(val, str):
                        return val
                    return "\n".join(textwrap.wrap(val, width=width)) if val else val

                if "index_label" in summary_to_show.columns:
                    summary_to_show["index_label"] = summary_to_show["index_label"].map(
                        lambda x: _wrap_text(x, width=18)
                    )
                if "group" in summary_to_show.columns:
                    summary_to_show["group"] = summary_to_show["group"].map(
                        lambda x: _wrap_text(x, width=10)
                    )
                if "risk_class" in summary_to_show.columns:
                    summary_to_show["risk_class"] = summary_to_show["risk_class"].map(
                        lambda x: _wrap_text(x, width=12)
                    )

                # Draw table in middle of the cover page
                ax_table = fig.add_axes([0.05, 0.10, 0.9, 0.70])
                ax_table.axis("off")
                table = ax_table.table(
                    cellText=summary_to_show.values,
                    colLabels=[c.replace("_", " ").title() for c in summary_to_show.columns],
                    loc="center",
                    cellLoc="center",
                )
                table.auto_set_font_size(False)
                table.set_fontsize(8)
                # Slightly shrink horizontally, stretch vertically to reduce overflow
                table.scale(0.9, 1.2)

                pdf.savefig(fig)
                plt.close(fig)

                # ------------------------------------------------------------------
                # Per-index pages
                # ------------------------------------------------------------------
                for _, row_idx in summary_df.sort_values("index_label").iterrows():
                    slug = row_idx["index_slug"]
                    idx_label = row_idx.get("index_label", slug)

                    ts = ts_dict.get(slug, {}) or {}
                    hist_ts = ts.get("historical", pd.DataFrame())
                    scen_ts = ts.get("scenario", pd.DataFrame())

                    panel_df = panel_dict.get(slug)

                    # Fresh A4 page with 3 vertically stacked rows
                    fig_idx = plt.figure(figsize=(8.27, 11.69), dpi=150)
                    fig_idx.patch.set_facecolor("white")
                    gs = fig_idx.add_gridspec(
                        nrows=3,
                        ncols=1,
                        height_ratios=[3.0, 2.0, 1.3],
                        hspace=0.4,
                    )

                    ax_trend = fig_idx.add_subplot(gs[0, 0])
                    ax_bar = fig_idx.add_subplot(gs[1, 0])
                    ax_text = fig_idx.add_subplot(gs[2, 0])
                    ax_text.axis("off")

                    # Page title
                    fig_idx.suptitle(
                        f"{idx_label} — {district_name}, {state_name}",
                        fontsize=12,
                        y=0.98,
                    )

                    # 1) Trend plot on the top row
                    try:
                        if (hist_ts is not None and not hist_ts.empty) or (
                            scen_ts is not None and not scen_ts.empty
                        ):
                            _create_trend_figure_for_index(
                                hist_ts=hist_ts,
                                scen_ts=scen_ts,
                                idx_label=idx_label,
                                scenario_name=sel_scenario,
                                ax=ax_trend,
                                figsize=(6.0, 3.0),
                            )
                        else:
                            ax_trend.text(
                                0.5,
                                0.5,
                                "No yearly time series available for this index.",
                                ha="center",
                                va="center",
                                fontsize=9,
                            )
                            ax_trend.set_axis_off()
                    except Exception:
                        # Fail softly: don't break PDF generation for one bad index
                        ax_trend.text(
                            0.5,
                            0.5,
                            "Trend plot could not be generated.",
                            ha="center",
                            va="center",
                            fontsize=9,
                        )
                        ax_trend.set_axis_off()

                    # 2) Scenario comparison bar chart in the middle row
                    bullet_lines: list[str] = []
                    try:
                        if panel_df is not None and not panel_df.empty:
                            make_scenario_comparison_figure(
                                panel_df=panel_df,
                                metric_label=idx_label,
                                sel_scenario=sel_scenario,
                                sel_period=sel_period,
                                sel_stat=sel_stat,
                                district_name=district_name,
                                ax=ax_bar,
                                figsize=(6.0, 3.0),
                            )

                            # Build short bullet-style lines from panel values
                            panel_sorted = panel_df.sort_values(["period", "scenario"])
                            for _, r in panel_sorted.iterrows():
                                scen_label = SCENARIO_DISPLAY.get(
                                    r["scenario"],
                                    str(r["scenario"]),
                                )
                                try:
                                    val_str = f"{float(r['value']):.2f}"
                                except Exception:
                                    val_str = str(r["value"])

                                period_label = canonical_period_label(str(r.get("period", "")))
                                bullet_lines.append(
                                    f"• {scen_label} — {period_label}: {val_str}"
                                )
                        else:
                            ax_bar.text(
                                0.5,
                                0.5,
                                "No period-mean scenario data available.",
                                ha="center",
                                va="center",
                                fontsize=9,
                            )
                            ax_bar.set_axis_off()
                    except Exception:
                        ax_bar.text(
                            0.5,
                            0.5,
                            "Scenario comparison chart could not be generated.",
                            ha="center",
                            va="center",
                            fontsize=9,
                        )
                        ax_bar.set_axis_off()

                    # 3) Narrative + bullets in the bottom row
                    narrative_lines: list[str] = []
                    try:
                        parts = []
                        if hist_ts is not None and not hist_ts.empty:
                            parts.append(hist_ts[["year", "mean"]])
                        if scen_ts is not None and not scen_ts.empty:
                            parts.append(scen_ts[["year", "mean"]])

                        if parts:
                            combined = (
                                pd.concat(parts, ignore_index=True)
                                .sort_values("year")
                            )
                            start_year = int(combined["year"].iloc[0])
                            end_year = int(combined["year"].iloc[-1])
                            start_val = float(combined["mean"].iloc[0])
                            end_val = float(combined["mean"].iloc[-1])
                            delta = end_val - start_val

                            if abs(delta) < 0.1:
                                trend_word = "has remained broadly stable"
                            elif delta > 0:
                                trend_word = "has increased"
                            else:
                                trend_word = "has decreased"

                            narrative_lines.append(
                                f"Between {start_year} and {end_year}, "
                                f"{idx_label.lower()} in {district_name} {trend_word}, "
                                f"from about {start_val:.1f} to about {end_val:.1f}."
                            )
                    except Exception:
                        # If something goes wrong, just skip the narrative
                        pass

                    y_text = 0.95
                    if narrative_lines:
                        ax_text.text(
                            0.01,
                            y_text,
                            narrative_lines[0],
                            fontsize=9,
                            va="top",
                            ha="left",
                            wrap=True,
                            transform=ax_text.transAxes,
                        )
                        y_text -= 0.25

                    if bullet_lines:
                        ax_text.text(
                            0.01,
                            y_text,
                            "Scenario / period mean values:",
                            fontsize=9,
                            va="top",
                            ha="left",
                            transform=ax_text.transAxes,
                        )
                        y_text -= 0.08
                        for line in bullet_lines:
                            ax_text.text(
                                0.03,
                                y_text,
                                line,
                                fontsize=8,
                                va="top",
                                ha="left",
                                transform=ax_text.transAxes,
                            )
                            y_text -= 0.06

                    pdf.savefig(fig_idx)
                    plt.close(fig_idx)

            return buf.getvalue()

        # --- Load historical + selected scenario series separately ---
        requested_state_dir = (
            selected_state
            if selected_state != "All"
            else (row.get("state_name") or PILOT_STATE)
        )
        state_dir_for_fs = requested_state_dir
        district_for_fs = row.get("district_name") or selected_district

        # Historical (1990–2010)
        _district_yearly_hist = _load_district_yearly(
            ts_root=PROCESSED_ROOT,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            scenario_name="historical",
            varcfg=VARCFG,
            aliases=NAME_ALIASES,
        )

        # Selected SSP scenario (2020–2060)
        _district_yearly_scen = _load_district_yearly(
            ts_root=PROCESSED_ROOT,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            scenario_name=sel_scenario,
            varcfg=VARCFG,
            aliases=NAME_ALIASES,
        )

        # ---- Trend over time (collapsible) ----
        with st.expander("Trend over time", expanded=False):
            st.caption(
                f"Looking for yearly CSVs under: {state_dir_for_fs} / {district_for_fs} "
                f"(historical + {sel_scenario})"
            )

            # Prepare clean series for plotting
            hist_ts = _filter_series_for_trend(_district_yearly_hist, state_to_show, district_name)
            scen_ts = _filter_series_for_trend(_district_yearly_scen, state_to_show, district_name)

            if not hist_ts.empty or not scen_ts.empty:
                st.markdown("**Trend over time**")

                fig_ts = _create_trend_figure_for_index(
                    hist_ts=hist_ts,
                    scen_ts=scen_ts,
                    idx_label=VARIABLES[VARIABLE_SLUG]["label"],
                    scenario_name=sel_scenario,
                )
                st.pyplot(fig_ts)

                # Narrative: use combined range (historical + scenario if available)
                try:
                    parts = []
                    if not hist_ts.empty:
                        parts.append(hist_ts[["year", "mean"]])
                    if not scen_ts.empty:
                        parts.append(scen_ts[["year", "mean"]])
                    if parts:
                        combined = pd.concat(parts, ignore_index=True).sort_values("year")
                        start_year = int(combined["year"].iloc[0])
                        end_year = int(combined["year"].iloc[-1])
                        start_val = float(combined["mean"].iloc[0])
                        end_val = float(combined["mean"].iloc[-1])
                        delta = end_val - start_val
                        pct = (delta / start_val * 100.0) if start_val not in (0.0, None) else None

                        if abs(delta) < 0.1:
                            trend_word = "has remained broadly stable"
                        elif delta > 0:
                            trend_word = "has increased"
                        else:
                            trend_word = "has decreased"

                        if pct is not None:
                            st.markdown(
                                f"**Narrative:** Between **{start_year}** and **{end_year}**, "
                                f"{VARIABLES[VARIABLE_SLUG]['label'].lower()} in **{district_name}** "
                                f"{trend_word}, from about **{start_val:.1f}** to **{end_val:.1f}** "
                                f"({pct:+.1f}% change)."
                            )
                        else:
                            st.markdown(
                                f"**Narrative:** Between **{start_year}** and **{end_year}**, "
                                f"{VARIABLES[VARIABLE_SLUG]['label'].lower()} in **{district_name}** "
                                f"{trend_word}."
                            )
                except Exception:
                    pass
            else:
                st.caption("No yearly time-series available for this district (historical or scenario).")

        # ---- Scenario comparison mini-panel (period-mean across scenarios) ----
        with st.expander("Scenario comparison (period-mean)", expanded=False):
            panel_df = build_scenario_comparison_panel_for_row(
                row=row,
                schema_items=schema_items,
                metric_name=sel_metric,
                sel_stat=sel_stat,
            )

            if panel_df is not None and not panel_df.empty:

                fig_sc, ax_sc = make_scenario_comparison_figure(
                    panel_df=panel_df,
                    metric_label=VARIABLES[VARIABLE_SLUG]["label"],
                    sel_scenario=sel_scenario,
                    sel_period=sel_period,
                    sel_stat=sel_stat,
                    district_name=district_name,
                )

                if fig_sc is not None:
                    st.pyplot(fig_sc)

                # Optional numeric summary in text, IPCC-style
                # (uses global PERIOD_ORDER to ensure consistent ordering)
                lines = []
                for period in PERIOD_ORDER:
                    sub = panel_df[panel_df["period"] == period]
                    if sub.empty:
                        continue
                    # Collect scenario=value pairs for this period
                    parts = []
                    for scen in ["historical", "ssp245", "ssp585"]:
                        sub_s = sub[sub["scenario"] == scen]
                        if sub_s.empty:
                            continue
                        val = sub_s["value"].iloc[0]
                        parts.append(f"{SCENARIO_DISPLAY.get(scen, scen)} = {val:.1f}")
                    if parts:
                        lines.append(f"- **{period}**: " + ", ".join(parts))

                if lines:
                    st.markdown(
                        "For this district and selected statistic, the **period-average** values are:\n"
                        + "\n".join(lines)
                    )
            else:
                st.caption(
                    "Scenario comparison (period-mean) not available for this district/index combination."
                )

        # st.markdown("---")




        def _make_district_yearly_pdf(
            df_yearly: pd.DataFrame,
            state_name: str,
            district_name: str,
            scenario_name: str,
            metric_label: str,
            out_dir: Path,
        ) -> Optional[Path]:
            if df_yearly is None or df_yearly.empty:
                return None
            d = df_yearly.copy()
            cols = set(map(str, d.columns))
            # We need at least these columns to proceed
            if not {"district", "scenario", "year", "mean"}.issubset(cols):
                return None
            # Ensure state column exists
            if "state" not in d.columns:
                d["state"] = state_name
            has_p05, has_p95 = ("p05" in d.columns), ("p95" in d.columns)

            def _n(s: str) -> str:
                return alias(s)

            # Normalised keys for matching
            d["_state_key"] = d["state"].astype(str).map(_n)
            d["_district_key"] = d["district"].astype(str).map(_n)
            d["_scen_key"] = d["scenario"].astype(str).str.strip().str.lower()

            # First try exact state+district+scenario match
            mask = (
                (d["_state_key"] == _n(state_name))
                & (d["_district_key"] == _n(district_name))
                & (d["_scen_key"] == scenario_name.strip().lower())
            )
            # Fallback: contains match on district
            if not mask.any():
                mask = (
                    (d["_state_key"] == _n(state_name))
                    & d["_district_key"].str.contains(_n(district_name), na=False)
                    & (d["_scen_key"] == scenario_name.strip().lower())
                )
            # Second fallback: fuzzy match on district within state+scenario
            if not mask.any():
                cand = d.loc[
                    (d["_state_key"] == _n(state_name))
                    & (d["_scen_key"] == scenario_name.strip().lower()),
                    "_district_key",
                ].dropna().unique().tolist()
                best = difflib.get_close_matches(_n(district_name), cand, n=1, cutoff=0.72)
                if best:
                    mask = (
                        (d["_state_key"] == _n(state_name))
                        & (d["_district_key"] == best[0])
                        & (d["_scen_key"] == scenario_name.strip().lower())
                    )

            d = d[mask]
            if d.empty:
                return None

            # Clean numeric types
            for c in ("year", "mean"):
                d[c] = pd.to_numeric(d[c], errors="coerce")
            if has_p05:
                d["p05"] = pd.to_numeric(d.get("p05"), errors="coerce")
            if has_p95:
                d["p95"] = pd.to_numeric(d.get("p95"), errors="coerce")

            d = d.dropna(subset=["year"]).sort_values("year")
            if d.empty:
                return None

            fig, ax = plt.subplots(figsize=(7.5, 4.5), dpi=150)
            ax.plot(d["year"], d["mean"], linewidth=3.0, label="Mean")
            if has_p05:
                ax.plot(d["year"], d["p05"], linewidth=1.5, label="5th percentile")
            if has_p95:
                ax.plot(d["year"], d["p95"], linewidth=1.5, label="95th percentile")

            ax.set_xlabel("Year")
            ax.set_ylabel(metric_label)
            ax.set_title(
                f"{district_name}, {state_name} • {metric_label} • {scenario_name}"
            )
            ax.grid(True, linestyle="--", alpha=0.35)
            ax.legend(frameon=False, ncol=3, fontsize=9)

            out_dir.mkdir(parents=True, exist_ok=True)

            safe = lambda s: "".join(
                c if c.isalnum() or c in ("-", "_") else "_" for c in str(s)
            )
            pdf_path = (
                out_dir
                / f"{safe(state_name)}__{safe(district_name)}__"
                  f"{safe(metric_label)}__{safe(scenario_name)}__yearly_timeseries.pdf"
            )
            fig.tight_layout()
            fig.savefig(pdf_path, format="pdf")
            plt.close(fig)
            return pdf_path



        # ---- Detailed statistics (collapsible) ----
        with st.expander("Detailed statistics for selected district", expanded=False):
            # Basic stats table
            stats_list = ["mean", "median", "p05", "p95", "std"]
            rows_stats = []
            for sname in stats_list:
                coln = f"{sel_metric}__{sel_scenario}__{sel_period}__{sname}"
                val = row.get(coln)
                rows_stats.append(
                    {
                        "Statistic": sname,
                        "Value": val,
                    }
                )

            df_stats_state = pd.DataFrame(rows_stats)
            # Make sure Value is numeric where possible; this avoids Arrow complaining
            df_stats_state["Value"] = pd.to_numeric(df_stats_state["Value"], errors="coerce")
            st.table(df_stats_state.set_index("Statistic"))

            # -----------------------------
            # Optional PDF generation + reuse via session_state
            # -----------------------------
            st.caption(
                "You can optionally generate a PDF of the district's yearly "
                "time-series for the selected scenario."
            )

            # Unique key for storing the PDF path for this district/scenario
            pdf_state_key = (
                f"district_pdf_path_"
                f"{VARIABLE_SLUG}_{state_to_show}_{selected_district}_{sel_scenario}"
            )
            pdf_path_d = st.session_state.get(pdf_state_key)

            # Button to (re)generate the PDF
            if st.button(
                "Generate district yearly time-series PDF",
                key=f"btn_district_pdf_{VARIABLE_SLUG}_{state_to_show}_{selected_district}_{sel_scenario}",
            ):
                pdf_path_d = _make_district_yearly_pdf(
                    df_yearly=_district_yearly_scen,
                    state_name=state_to_show,
                    district_name=row.get("district_name", selected_district),
                    scenario_name=sel_scenario,
                    metric_label=VARIABLES[VARIABLE_SLUG]["label"],
                    out_dir=OUTDIR,
                )

                # Store or clear in session_state depending on success
                if pdf_path_d and pdf_path_d.exists():
                    st.session_state[pdf_state_key] = pdf_path_d
                else:
                    st.session_state.pop(pdf_state_key, None)
                    pdf_path_d = None

            # Show download + open-in-new-tab link if we have a valid PDF
            if pdf_path_d and pdf_path_d.exists():
                with open(pdf_path_d, "rb") as fh:
                    st.download_button(
                        "⬇️ Download district yearly time-series (PDF)",
                        fh.read(),
                        file_name=pdf_path_d.name,
                        mime="application/pdf",
                        key="btn_dist_pdf_dl",
                    )

                abs_url_d = pdf_path_d.resolve().as_uri()
                st.markdown(
                    f'<a href="{abs_url_d}" target="_blank" rel="noopener">'
                    f"🗎 Open district yearly figure in a new tab</a>",
                    unsafe_allow_html=True,
                )
            else:
                st.caption(
                    "No yearly time-series PDF is currently available for this "
                    "district/scenario. Click the button above to generate it."
                )

        # st.markdown("---")

        # ---- Case study export: single district, multi-index (MVP) ----
        with st.expander("📄 Case study export (single district, multi-index)", expanded=False):
            st.caption(
                "Build a case-study style report for the selected district across "
                "multiple climate indices (experimental)."
            )

            index_options = list(VARIABLES.keys())
            default_indices = (
                [VARIABLE_SLUG] if VARIABLE_SLUG in index_options else index_options[:1]
            )
            selected_index_slugs = st.multiselect(
                "Indices to include in the report",
                options=index_options,
                default=default_indices,
                format_func=lambda s: VARIABLES[s]["label"],
                key="case_study_indices",
            )

            if not selected_index_slugs:
                st.info("Select at least one index to build the case-study report.")
            else:
                if st.button(
                    "Build case-study data for this district",
                    key="btn_build_case_study",
                ):
                    with st.spinner("Assembling climate profile for selected indices..."):
                        summary_df_cs, ts_dict_cs, panel_dict_cs = _build_district_case_study_data(
                            state_name=state_to_show,
                            district_name=district_name,
                            index_slugs=selected_index_slugs,
                            sel_scenario=sel_scenario,
                            sel_period=sel_period,
                            sel_stat=sel_stat,
                        )
                        if summary_df_cs is None or summary_df_cs.empty:
                            st.warning(
                                "No data found for the selected index/district combination. "
                                "Try including fewer indices or a different scenario/period/statistic."
                            )
                        else:
                            st.session_state["case_study_summary"] = summary_df_cs
                            st.session_state["case_study_ts"] = ts_dict_cs
                            st.session_state["case_study_panels"] = panel_dict_cs

                summary_df_cs = st.session_state.get("case_study_summary")
                ts_dict_cs = st.session_state.get("case_study_ts")
                panel_dict_cs = st.session_state.get("case_study_panels")

                if isinstance(summary_df_cs, pd.DataFrame) and not summary_df_cs.empty:
                    st.markdown("**Preview of case-study summary table**")
                    st.dataframe(summary_df_cs)

                    pdf_bytes = _make_district_case_study_pdf(
                        state_name=state_to_show,
                        district_name=district_name,
                        summary_df=summary_df_cs,
                        ts_dict=ts_dict_cs or {},
                        panel_dict=panel_dict_cs or {},
                        sel_scenario=sel_scenario,
                        sel_period=sel_period,
                        sel_stat=sel_stat,
                    )

                    if pdf_bytes:
                        safe_state = _slugify_fs(state_to_show)
                        safe_dist = _slugify_fs(district_name)
                        pdf_filename = f"climate_profile_{safe_state}__{safe_dist}.pdf"

                        st.download_button(
                            label="⬇️ Download case-study PDF",
                            data=pdf_bytes,
                            file_name=pdf_filename,
                            mime="application/pdf",
                            key="download_case_study_pdf",
                        )

                        zip_bytes = _make_case_study_zip(
                            state_name=state_to_show,
                            district_name=district_name,
                            summary_df=summary_df_cs,
                            ts_dict=ts_dict_cs or {},
                            panel_dict=panel_dict_cs or {},
                            pdf_bytes=pdf_bytes,
                        )
                        st.download_button(
                            label="⬇️ Download PDF + CSVs as ZIP",
                            data=zip_bytes,
                            file_name=f"climate_profile_{safe_state}__{safe_dist}__with_data.zip",
                            mime="application/zip",
                            key="download_case_study_zip",
                        )
                else:
                    st.caption(
                        "Build the case-study data using the button above to enable downloads."
                    )

        # ---- District comparison (1.5) ----
        with st.expander("Compare with another district", expanded=False):
        # st.markdown("### Compare with another district")
            same_state_mask = (
                merged["state_name"].astype(str).str.strip().str.lower()
                == str(state_to_show).strip().lower()
            )
            compare_candidates = (
                merged.loc[same_state_mask, "district_name"]
                .astype(str)
                .sort_values()
                .unique()
                .tolist()
            )
            compare_candidates = [
                d for d in compare_candidates if d != district_name
            ]

            if compare_candidates:
                comp_choice = st.selectbox(
                    "Compare with",
                    options=["(None)"] + compare_candidates,
                    index=0,
                    key="compare_district",
                )

                if comp_choice != "(None)":
                    mask_c = (
                        merged["district_name"].astype(str).str.strip()
                        == str(comp_choice).strip()
                    )
                    comp_row = merged[mask_c].iloc[0] if mask_c.any() else None

                    if comp_row is not None:
                        # <-- these two lines MUST be before the if (val_this...) check
                        val_this = current_val_f
                        val_other = comp_row.get(metric_col)
                        val_other_f = float(val_other) if not pd.isna(val_other) else None

                        if (val_this is not None) and (val_other_f is not None):
                            diff = val_this - val_other_f
                            direction = (
                                "higher than"
                                if diff > 0
                                else "lower than"
                                if diff < 0
                                else "the same as"
                            )
                            st.markdown(
                                f"- **{VARIABLES[VARIABLE_SLUG]['label']}** in **{district_name}** "
                                f"is **{abs(diff):.2f}** {direction} in **{comp_choice}** "
                                f"for the selected scenario and period."
                            )

                            # Small visual comparison: two bars side by side
                            fig_cmp, ax_cmp = plt.subplots(figsize=(3.6, 2.2), dpi=150)
                            labels_cmp = [district_name, comp_choice]
                            values_cmp = [val_this, val_other_f]

                            colors_cmp = ["tab:blue", "tab:grey"]
                            bars = ax_cmp.bar(labels_cmp, values_cmp, color=colors_cmp)

                            ax_cmp.set_ylabel(
                                f"{VARIABLES[VARIABLE_SLUG]['label']} ({sel_stat})"
                            )
                            ax_cmp.set_title("District comparison", fontsize=9)
                            ax_cmp.grid(True, axis="y", linestyle="--", alpha=0.25)

                            # Annotate values on top of bars
                            for b in bars:
                                height = b.get_height()
                                ax_cmp.text(
                                    b.get_x() + b.get_width() / 2,
                                    height,
                                    f"{height:.1f}",
                                    ha="center",
                                    va="bottom",
                                    fontsize=8,
                                )

                            # Clean spines
                            for spine in ax_cmp.spines.values():
                                spine.set_visible(False)

                            fig_cmp.tight_layout()
                            st.pyplot(fig_cmp)
                        else:
                            st.caption(
                                "Comparison data not fully available for the selected index."
                            )
            else:
                st.caption("No other districts found in this state for comparison.")

with col2:
    # -------------------------
    # Portfolio analysis (multi-district)
    # -------------------------
    if st.session_state.get("analysis_mode", "Single district focus") == "Multi-district portfolio":
        with st.expander("Portfolio analysis (multi-district)", expanded=False):

            if st.button(
                "📋 Select districts from table",
                key="btn_portfolio_select_from_rankings",
            ):
                # Set a one-shot flag; the top-level / view hook will honour this
                st.session_state["jump_to_rankings"] = True
                st.rerun()

            portfolio = st.session_state.get("portfolio_districts", [])

            if not portfolio:
                st.info(
                    "No districts in the portfolio yet. "
                    "Use **Select districts from table** above to open the Rankings view, "
                    "then check rows in the 'Add to portfolio' column. "
                    "You can also click districts on the map and use "
                    "**Add this district to portfolio** in the Climate Profile panel."
                )
            else:
                portfolio_df = pd.DataFrame(portfolio)
                st.markdown("**Selected districts**")
                st.dataframe(
                    portfolio_df.rename(
                        columns={"state": "State", "district": "District"}
                    ),
                    use_container_width=True,
                )

                # Use the already-built ranking table (table_df) to show
                # current index values for only the portfolio districts
                if table_df is not None and not table_df.empty:
                    key_set = {
                        _portfolio_key(item["state"], item["district"])
                        for item in portfolio
                    }

                    def _row_in_portfolio(r: pd.Series) -> bool:
                        return _portfolio_key(
                            r.get("state_name"), r.get("district_name")
                        ) in key_set

                    portfolio_metric_df = table_df[
                        table_df.apply(_row_in_portfolio, axis=1)
                    ].copy()

                    if portfolio_metric_df.empty:
                        st.caption(
                            "No data for the selected index for the current portfolio."
                        )
                    else:
                        show_cols = [
                            c
                            for c in [
                                "district_name",
                                "state_name",
                                "value",
                                "baseline",
                                "delta_abs",
                                "delta_pct",
                                "percentile_value",
                                "risk_class",
                            ]
                            if c in portfolio_metric_df.columns
                        ]
                        st.markdown("**Current index values for portfolio**")
                        st.dataframe(
                            portfolio_metric_df[show_cols].rename(
                                columns={
                                    "district_name": "District",
                                    "state_name": "State",
                                    "value": pretty_metric_label,
                                }
                            ),
                            use_container_width=True,
                        )

                        # CSV download for portfolio & current index
                        csv_bytes = portfolio_metric_df.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            "⬇️ Download portfolio data (current index only, CSV)",
                            data=csv_bytes,
                            file_name=(
                                f"portfolio_{VARIABLE_SLUG}_{sel_scenario}_"
                                f"{sel_period}_{sel_stat}.csv"
                            ),
                            mime="text/csv",
                        )

                        # -------------------------------------------------
                        # NEW: Multi-index comparison across portfolio
                        # -------------------------------------------------
                        st.markdown("---")
                        st.markdown("#### Multi-index comparison for portfolio")

                        index_options = list(VARIABLES.keys())
                        default_multi = st.session_state.get(
                            "portfolio_multiindex_selection",
                            [VARIABLE_SLUG]
                            if VARIABLE_SLUG in index_options
                            else index_options[:1],
                        )
                        selected_index_slugs = st.multiselect(
                            "Indices to include",
                            options=index_options,
                            default=default_multi,
                            format_func=lambda s: VARIABLES[s]["label"],
                            key="portfolio_multiindex_selection",
                        )

                        if selected_index_slugs:
                            if st.button(
                                "Build multi-index portfolio table",
                                key=f"btn_build_portfolio_multiindex_{sel_scenario}_{sel_period}_{sel_stat}",
                            ):
                                records: list[dict] = []
                                for item in portfolio:
                                    state_name = item.get("state")
                                    district_name = item.get("district")
                                    if not state_name or not district_name:
                                        continue

                                    for slug in selected_index_slugs:
                                        varcfg = VARIABLES.get(slug)
                                        if not varcfg:
                                            continue

                                        # Determine processed root for this index, similar to PROCESSED_ROOT logic
                                        env_root = os.getenv("IRT_PROCESSED_ROOT")
                                        if env_root:
                                            base_path = Path(env_root)
                                            if base_path.name.lower() == slug.lower():
                                                proc_root = base_path
                                            else:
                                                proc_root = base_path / slug
                                        else:
                                            proc_root = DATA_DIR / "processed" / slug
                                        proc_root = proc_root.resolve()

                                        master_path = proc_root / PILOT_STATE / "master_metrics_by_district.csv"
                                        if not master_path.exists():
                                            continue

                                        try:
                                            df_master, schema_items_local, metrics_local, by_metric_local = _load_master_and_schema(
                                                master_path, slug
                                            )
                                        except Exception:
                                            continue

                                        if df_master is None or df_master.empty:
                                            continue

                                        # Decide metric name for this slug (align with normalized metrics)
                                        registry_metric = varcfg.get("periods_metric_col")
                                        available_metrics = set(metrics_local or [])
                                        if not available_metrics:
                                            continue
                                        if registry_metric not in available_metrics:
                                            m_lower = {m.lower(): m for m in available_metrics}
                                            registry_metric = m_lower.get(
                                                str(registry_metric).lower(), next(iter(available_metrics))
                                            )

                                        metric_col = f"{registry_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
                                        if metric_col not in df_master.columns:
                                            # skip this index if we don't have the requested scenario/period/stat
                                            continue

                                        # Robust match for a single state+district row
                                        dm = df_master.copy()
                                        if "state" not in dm.columns or "district" not in dm.columns:
                                            continue

                                        def _n(s: str) -> str:
                                            return alias(s)

                                        dm["_state_key"] = dm["state"].astype(str).map(_n)
                                        dm["_district_key"] = dm["district"].astype(str).map(_n)

                                        target_state = _n(state_name)
                                        target_dist = _n(district_name)

                                        mask = (dm["_state_key"] == target_state) & (dm["_district_key"] == target_dist)
                                        if not mask.any():
                                            # fall back to contains on district name within same state
                                            mask = (dm["_state_key"] == target_state) & dm["_district_key"].str.contains(
                                                target_dist, na=False
                                            )
                                        if not mask.any():
                                            continue

                                        row_local = dm.loc[mask].iloc[0]

                                        current_val = row_local.get(metric_col)
                                        current_val_f = pd.to_numeric([current_val], errors="coerce")[0]
                                        if pd.isna(current_val_f):
                                            current_val_f = None

                                        # Baseline for same metric/stat in historical baseline period
                                        baseline_col = find_baseline_column_for_stat(
                                            dm.columns, registry_metric, sel_stat
                                        )
                                        baseline_val_f = None
                                        if baseline_col and baseline_col in dm.columns:
                                            baseline_val = row_local.get(baseline_col)
                                            baseline_val_f = pd.to_numeric(
                                                [baseline_val], errors="coerce"
                                            )[0]
                                            if pd.isna(baseline_val_f):
                                                baseline_val_f = None

                                        delta_abs = None
                                        delta_pct = None
                                        if current_val_f is not None and baseline_val_f is not None:
                                            delta_abs = current_val_f - baseline_val_f
                                            if abs(baseline_val_f) > 1e-6:
                                                delta_pct = delta_abs / baseline_val_f * 100.0

                                        # Rank and percentile within state
                                        rank_in_state = None
                                        percentile_in_state = None
                                        n_in_state = None
                                        try:
                                            state_mask = dm["_state_key"] == target_state
                                            state_vals = pd.to_numeric(
                                                dm.loc[state_mask, metric_col], errors="coerce"
                                            ).dropna()
                                            n_in_state = int(state_vals.size) if state_vals.size else None
                                            if (
                                                n_in_state
                                                and current_val_f is not None
                                                and n_in_state > 0
                                            ):
                                                rank_in_state = int((state_vals > current_val_f).sum() + 1)
                                                percentile_in_state = float(
                                                    (state_vals < current_val_f).sum() / n_in_state * 100.0
                                                )
                                        except Exception:
                                            pass

                                        risk_class = (
                                            risk_class_from_percentile(percentile_in_state)
                                            if percentile_in_state is not None
                                            else "Unknown"
                                        )

                                        records.append(
                                            {
                                                "state": state_name,
                                                "district": district_name,
                                                "index_slug": slug,
                                                "index_label": varcfg.get("label", slug),
                                                "group": varcfg.get("group"),
                                                "scenario": sel_scenario,
                                                "period": sel_period,
                                                "stat": sel_stat,
                                                "current": current_val_f,
                                                "baseline": baseline_val_f,
                                                "delta_abs": delta_abs,
                                                "delta_pct": delta_pct,
                                                "rank_in_state": rank_in_state,
                                                "percentile_in_state": percentile_in_state,
                                                "n_in_state": n_in_state,
                                                "risk_class": risk_class,
                                            }
                                        )

                                if records:
                                    portfolio_multiindex_df = pd.DataFrame.from_records(records)
                                    st.session_state["portfolio_multiindex_df"] = portfolio_multiindex_df
                                    st.success(
                                        f"Built multi-index table for {len(portfolio)} district(s) "
                                        f"and {len(selected_index_slugs)} index/indices."
                                    )
                                else:
                                    st.warning(
                                        "No data found for the selected indices and portfolio districts. "
                                        "Try a different combination of indices or scenario/period/statistic."
                                    )

                            portfolio_multiindex_df = st.session_state.get("portfolio_multiindex_df")
                            if (
                                isinstance(portfolio_multiindex_df, pd.DataFrame)
                                and not portfolio_multiindex_df.empty
                            ):
                                cols_display = [
                                    "state",
                                    "district",
                                    "index_label",
                                    "group",
                                    "current",
                                    "baseline",
                                    "delta_abs",
                                    "delta_pct",
                                    "rank_in_state",
                                    "percentile_in_state",
                                    "risk_class",
                                ]
                                cols_display = [
                                    c
                                    for c in cols_display
                                    if c in portfolio_multiindex_df.columns
                                ]

                                st.markdown("**Portfolio – multi-index summary**")
                                st.dataframe(
                                    portfolio_multiindex_df[cols_display].rename(
                                        columns={
                                            "state": "State",
                                            "district": "District",
                                            "index_label": "Index",
                                            "group": "Group",
                                            "current": "Current value",
                                            "baseline": "Baseline",
                                            "delta_abs": "Δ vs baseline",
                                            "delta_pct": "%Δ vs baseline",
                                            "rank_in_state": "Rank in state",
                                            "percentile_in_state": "Percentile in state",
                                            "risk_class": "Risk class",
                                        }
                                    ),
                                    use_container_width=True,
                                )

                                csv_multi = portfolio_multiindex_df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "⬇️ Download portfolio data (multi-index, CSV)",
                                    data=csv_multi,
                                    file_name=(
                                        f"portfolio_multiindex_{sel_scenario}_{sel_period}_{sel_stat}.csv"
                                    ),
                                    mime="text/csv",
                                    key="btn_portfolio_multiindex_csv",
                                )
                        else:
                            st.caption(
                                "Select at least one index to build a multi-index portfolio table."
                            )
                else:
                    st.caption(
                        "Ranking table is not available for the current index selection."
                    )

                # Clear portfolio button (after the table_df if/else)
                if st.button("🧹 Clear portfolio", key="btn_portfolio_clear"):
                    _portfolio_clear()
                    st.success("Cleared portfolio selection.")

st.markdown("---")
st.caption(
    "Notes: first choose an Index group (e.g. Temperature vs Rainfall), then an Index within that group. "
    "Details panel shows risk cards, trends, narrative, and a comparison option."
)