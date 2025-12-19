#!/usr/bin/env python3
from __future__ import annotations
import io, os, re, json, zipfile, shutil, subprocess, unicodedata, difflib, copy
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from contextlib import contextmanager
from functools import lru_cache
import textwrap
import time

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

from india_resilience_tool.data.adm2_loader import (
    build_adm1_from_adm2 as _build_adm1_from_adm2,
    enrich_adm2_with_state_names as _enrich_adm2_with_state_names,
    ensure_key_column as _ensure_key_column,
    featurecollections_by_state as _featurecollections_by_state,
    load_local_adm2 as _load_local_adm2,
)
from india_resilience_tool.data.merge import (
    get_or_build_merged_for_index_cached as _get_or_build_merged_for_index_cached,
)
from india_resilience_tool.analysis.portfolio import (
    build_portfolio_multiindex_df as _build_portfolio_multiindex_df_impl,
    portfolio_add as _portfolio_add_impl,
    portfolio_clear as _portfolio_clear_impl,
    portfolio_contains as _portfolio_contains_impl,
    portfolio_normalize as _portfolio_normalize_impl,
    portfolio_remove as _portfolio_remove_impl,
)

from india_resilience_tool.viz.tables import build_rankings_table_df as _build_rankings_table_df

from india_resilience_tool.utils.naming import alias

from india_resilience_tool.app.sidebar import (
    apply_jump_once_flags,
    render_analysis_mode_selector,
    render_hover_toggle_if_portfolio,
    render_view_selector,
)

from india_resilience_tool.app.views.map_view import render_map_view
from india_resilience_tool.app.views.rankings_view import render_rankings_view
from india_resilience_tool.app.views.details_panel import render_details_panel

from matplotlib.backends.backend_pdf import PdfPages

# -------------------------
# DEBUG
# -------------------------
DEBUG = bool(int(os.getenv("IRT_DEBUG", "0")))

def dbg(*args, **kwargs):
    if DEBUG:
        st.write(*args, **kwargs)

# -------------------------
# PERFORMANCE TIMING (opt-in)
# -------------------------
def _perf_is_enabled() -> bool:
    """Return True if perf timing is enabled for this session."""
    return bool(st.session_state.get("perf_enabled", False))


def perf_reset() -> None:
    """Clear per-rerun performance records (call once near app start)."""
    if _perf_is_enabled():
        st.session_state["_perf_records"] = []


def perf_start(section: str) -> Optional[float]:
    """Start timing and return a token (start time)."""
    if not _perf_is_enabled():
        return None
    return time.perf_counter()


def perf_end(section: str, start: Optional[float]) -> None:
    """Stop timing for `section` using the token from perf_start()."""
    if start is None or not _perf_is_enabled():
        return
    elapsed = time.perf_counter() - start
    st.session_state.setdefault("_perf_records", []).append(
        {"section": section, "seconds": float(elapsed)}
    )


@contextmanager
def perf_section(section: str):
    """Context manager wrapper around perf_start/perf_end."""
    start = perf_start(section)
    try:
        yield
    finally:
        perf_end(section, start)


def render_perf_panel(container) -> None:
    """Render the timing table into a Streamlit container/placeholder."""
    if not _perf_is_enabled():
        return

    records = st.session_state.get("_perf_records", [])
    with container:
        with st.expander("⏱ Performance timings", expanded=False):
            if not records:
                st.caption("No timings recorded for this rerun yet.")
                return

            df_perf = pd.DataFrame(records)
            df_perf["ms"] = (df_perf["seconds"] * 1000.0).round(1)
            df_perf = df_perf.drop(columns=["seconds"])
            st.dataframe(df_perf, hide_index=True, use_container_width=True)
            st.caption(f"Total: {df_perf['ms'].sum():.1f} ms")

def render_perf_panel_safe() -> None:
    """Best-effort performance panel render.

    This makes the perf panel resilient to early `st.stop()` branches by
    rendering into a sidebar placeholder if available.
    """
    if not _perf_is_enabled():
        return

    placeholder = globals().get("perf_panel_placeholder")
    if placeholder is None:
        # Prefer the sidebar so the UI matches the developer control location.
        try:
            placeholder = st.sidebar.empty()
        except Exception:
            placeholder = st.empty()
        globals()["perf_panel_placeholder"] = placeholder

    render_perf_panel(placeholder)

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
from india_resilience_tool.utils.naming import NAME_ALIASES, alias, normalize_name, normalize_compact

# -------------------------
# Geo load / prep
# -------------------------
@st.cache_data
@st.cache_data
def load_local_adm2(path: str, tolerance: float = SIMPLIFY_TOL_ADM2) -> gpd.GeoDataFrame:
    gdf = _load_local_adm2(
        path=path,
        tolerance=float(tolerance),
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
        min_area=0.0003,
    )
    return gdf

if not ADM2_GEOJSON.exists():
    st.set_page_config(page_title="India Resilience Tool", layout="wide")
    st.error(f"ADM2 geojson not found at {ADM2_GEOJSON}. Place your districts_4326.geojson at this path.")
    st.stop()

adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)
adm2["__key"] = adm2["district_name"].map(alias)

@st.cache_data(ttl=3600)
def build_adm2_geojson_by_state(
    path: str,
    tolerance: float,
    mtime: float,
) -> dict[str, dict]:
    """
    Build and cache an ADM2 FeatureCollection per state (geometry + identifiers only).

    Cached by (path, tolerance, mtime) so it invalidates automatically when the
    source GeoJSON changes or simplification tolerance is updated.
    """
    _ = mtime  # mtime is used only to invalidate Streamlit's cache

    gdf = load_local_adm2(path, tolerance=tolerance)
    if "__key" not in gdf.columns:
        gdf = _ensure_key_column(gdf, district_col="district_name", alias_fn=alias, key_col="__key")

    by_state = _featurecollections_by_state(
        gdf,
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["district_name", "state_name", "__key", "geometry"],
    )
    return by_state

@st.cache_data
def build_adm1_from_adm2(_adm2_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return _build_adm1_from_adm2(_adm2_gdf, state_col="state_name")

@st.cache_data
def enrich_adm2_with_state_names(
    _adm2_gdf: gpd.GeoDataFrame,
    _adm1_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return _enrich_adm2_with_state_names(_adm2_gdf, _adm1_gdf, state_col="state_name", adm1_name_col="shapeName")

# -------------------------
# Color helpers (no GeoJSON round-trip)
# -------------------------
from india_resilience_tool.viz.colors import (
    apply_fillcolor,
    build_vertical_gradient_legend_html,
    get_cmap_hex_list as _get_cmap_hex_list,
)



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
    """
    Backward-compatible wrapper: preserves caching semantics and deterministic merge.

    Cached by master mtime in st.session_state["_merged_cache"][slug].
    """
    merged = _get_or_build_merged_for_index_cached(
        adm2,
        df,
        slug=slug,
        master_path=master_path,
        session_state=st.session_state,
        alias_fn=alias,
        adm2_state_col="state_name",
        master_state_col="state",
    )
    # typing: cached function returns DataFrame; in practice this is a GeoDataFrame when adm2 is one
    return merged  # type: ignore[return-value]


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

# @st.cache_data
from india_resilience_tool.data.master_loader import (
    load_master_csv,
    normalize_master_columns,
    parse_master_schema,
)

def resolve_metric_column(
    df_or_cols,
    base_metric: str,
    scenario: str,
    period: str,
    stat: str,
) -> Optional[str]:
    """
    Resolve the actual master CSV column name for a metric/scenario/period/stat.

    Master columns are expected to be normalized to:
        <metric>__<scenario>__<period>__<stat>

    Returns the matching column name (preserving original casing) if found,
    otherwise returns None.
    """
    if not base_metric:
        return None

    # Accept a DataFrame/GeoDataFrame or an iterable of column names.
    try:
        cols = list(df_or_cols.columns)  # type: ignore[attr-defined]
    except Exception:
        try:
            cols = list(df_or_cols)
        except Exception:
            return None

    scen = str(scenario).strip().lower()
    per = str(period).strip().replace("_", "-").replace("–", "-")
    stt = str(stat).strip().lower()

    col_map = {str(c).lower(): str(c) for c in cols}
    candidate = f"{str(base_metric).strip()}__{scen}__{per}__{stt}".lower()

    if candidate in col_map:
        return col_map[candidate]

    # Fallback: match by pieces (handles minor period formatting differences).
    try:
        pat = re.compile(
            rf"^{re.escape(str(base_metric).strip())}__{re.escape(scen)}__.+__{re.escape(stt)}$",
            flags=re.IGNORECASE,
        )
        matches = [str(c) for c in cols if pat.match(str(c))]
        if not matches:
            return None

        per_l = per.lower()
        for c in matches:
            if per_l in c.lower():
                return c
        return matches[0]
    except Exception:
        return None

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
from india_resilience_tool.viz.charts import (
    PERIOD_ORDER,
    SCENARIO_DISPLAY,
    SCENARIO_ORDER,
    build_scenario_comparison_panel_for_row,
    canonical_period_label,
    make_scenario_comparison_figure,
)

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
from india_resilience_tool.analysis.metrics import risk_class_from_percentile



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

# Portfolio-build UX router state (multi-district portfolio mode)
st.session_state.setdefault("portfolio_build_route", None)  # None | "rankings" | "map" | "saved_points"
st.session_state.setdefault("jump_to_rankings", False)
st.session_state.setdefault("jump_to_map", False)
st.session_state.setdefault("_analysis_mode_prev", st.session_state.get("analysis_mode", "Single district focus"))

# Which main view is active in the left column: map vs rankings
if "active_view" not in st.session_state:
    st.session_state["active_view"] = "🗺 Map view"

# Perf timing toggle (developer)
st.session_state.setdefault("perf_enabled", DEBUG)
perf_reset()

# If a downstream control requested to jump to a specific left-panel view,
# honour it BEFORE the main_view_selector radio is created.
apply_jump_once_flags()

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
    # (function also preserves legacy default outside portfolio mode)
    _ = render_hover_toggle_if_portfolio(analysis_mode_current)


    analysis_mode_placeholder = st.empty()  # Single vs multi-district analysis

    state_placeholder = st.empty()
    district_placeholder = st.empty()

    metric_ui_placeholder = st.empty()  # unified "Index" UI
    map_mode_placeholder = st.empty()   # NEW: absolute vs change toggle
    color_slider_placeholder = st.empty()
    st.markdown("---")

    master_controls_placeholder = st.empty()
    st.markdown("---")

    with st.expander("Developer", expanded=False):
        st.checkbox(
            "Show performance timings",
            key="perf_enabled",
            value=st.session_state.get("perf_enabled", DEBUG),
            help="Shows per-section timings for the current rerun.",
        )

    perf_panel_placeholder = st.empty()


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

        # Default registry metric (prevents NameError if downstream master/schema logic short-circuits)
        registry_metric = str(VARCFG.get("periods_metric_col", "")).strip()
        st.session_state["registry_metric"] = registry_metric

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
            render_perf_panel_safe()
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
            with perf_section("master: read csv"):
                with st.spinner("Loading master CSV..."):
                    df_local = load_master_csv(str(master_path))

            with perf_section("master: normalize columns"):
                df_local = normalize_master_columns(df_local)

            with perf_section("master: parse schema"):
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
            st.error(
                "No ensemble statistic columns found in the master CSV. Did the builder run?"
            )
            render_perf_panel_safe()
            st.stop()

        # Choose the internal metric name from the registry (no separate Metric dropdown)
        registry_metric = str(VARCFG.get("periods_metric_col", "")).strip()

        # If normalized columns changed the metric name casing, align it
        available_metrics = set(metrics)
        if registry_metric not in available_metrics and available_metrics:
            m_lower = {str(m).lower(): m for m in available_metrics}
            registry_metric = m_lower.get(
                str(registry_metric).lower(), next(iter(available_metrics))
            )

        # Persist so downstream code can always access it safely
        st.session_state["registry_metric"] = registry_metric

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
            render_perf_panel_safe()
            st.stop()

        sel_scenario = st.selectbox("Scenario", scenarios, index=0, key="sel_scenario")

        periods = sorted(
            {
                i["period"]
                for i in (by_metric.get(registry_metric, []) or schema_items)
                if i["scenario"] == sel_scenario
            }
        )
        if not periods:
            st.error("No periods found for the selected scenario in the master CSV.")
            render_perf_panel_safe()
            st.stop()

        sel_period = st.selectbox("Period", periods, index=0, key="sel_period")
        stats = ["mean", "median", "p05", "p95", "std"]
        sel_stat = st.selectbox("Statistic", stats, index=0, key="sel_stat")

# Column chosen to plot
sel_metric = st.session_state.get("registry_metric", registry_metric)  # internal name
metric_col = f"{sel_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
if metric_col not in df.columns:
    st.error(f"Selected column '{metric_col}' not found in master CSV.")
    render_perf_panel_safe()
    st.stop()
pretty_metric_label = (
    f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario} · {sel_period} · {sel_stat}"
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

# State/district selectors + analysis focus (combined block in sidebar)
with state_placeholder.container():
    with st.expander("Geography & analysis focus", expanded=True):
        # ---- Step 1: State selection ----
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

        # Build per-state district GeoDataFrame
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

        # Ensure we always have a valid district in session state
        if (
            "selected_district" not in st.session_state
            or st.session_state["selected_district"] not in districts
        ):
            st.session_state["selected_district"] = "All"

        from india_resilience_tool.app.sidebar import render_analysis_mode_selector

        # ---- Step 2: Analysis focus (single vs multi-district) ----
        analysis_mode = render_analysis_mode_selector(
            label="Analysis focus",
            options=[
                "Single district focus",
                "Multi-district portfolio",
            ],
            index=0,
            help_text=(
                "Choose “Single district focus” to explore one district at a time, "
                "or “Multi-district portfolio” to build and compare a set of districts."
            ),
            label_visibility="collapsed",
            use_markdown_header=True,
        )

        # Reset portfolio route state when switching analysis focus modes
        prev_mode = st.session_state.get("_analysis_mode_prev", analysis_mode)
        if analysis_mode != prev_mode:
            st.session_state["_analysis_mode_prev"] = analysis_mode
            # Clear any previously selected portfolio-build route and pending view jumps
            st.session_state["portfolio_build_route"] = None
            st.session_state["jump_to_rankings"] = False
            st.session_state["jump_to_map"] = False

        # Brief helper text so the mode explains itself
        if analysis_mode == "Single district focus":
            st.caption(
                "Inspect one district at a time. Use the **District** dropdown below "
                "to pick which district you want to explore in detail."
            )
        else:
            st.markdown(
                "<div style='font-size:0.9rem; margin-top:0.25rem; margin-bottom:0.1rem;'>"
                "In <strong>Multi-district portfolio</strong> mode you build a set of districts "
                "for comparison. Districts are added from the <em>🗺 Map view</em>, the "
                "<em>📊 Rankings table</em>, or from saved point locations. "
                # "The <strong>District</strong> dropdown is fixed to <strong>All</strong> here "
                # "because selection now happens directly from the map and table."
                "</div>",
                unsafe_allow_html=True,
            )

        # ---- Step 3: District selection (only for single-district mode) ----
        if analysis_mode == "Single district focus":
            # Normal behaviour: user chooses the district from the sidebar
            selected_district = st.selectbox(
                "District",
                options=districts,
                index=districts.index(st.session_state["selected_district"]),
                key="selected_district",
            )
        else:
            # Portfolio mode: freeze district selection to "All"
            st.session_state["selected_district"] = "All"
            selected_district = "All"

# -------------------------
# Portfolio selection helpers (multi-district)
# -------------------------

if "portfolio_districts" not in st.session_state:
    # List of {"state": ..., "district": ...}
    st.session_state["portfolio_districts"] = []


def _portfolio_normalize(text: str) -> str:
    """
    Normalize a state/district name for robust comparison across data sources.

    Delegates to india_resilience_tool.analysis.portfolio to keep logic centralized.
    """
    return _portfolio_normalize_impl(text, alias_fn=alias)


def _portfolio_key(state_name: str, district_name: str) -> tuple[str, str]:
    return (_portfolio_normalize(state_name), _portfolio_normalize(district_name))


def _portfolio_add(state_name: str, district_name: str) -> None:
    """Add a (state, district) pair to the portfolio if not already present."""
    _portfolio_add_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        state_key="portfolio_districts",
    )


def _portfolio_remove(state_name: str, district_name: str) -> None:
    """Remove a (state, district) pair from the portfolio."""
    _portfolio_remove_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        state_key="portfolio_districts",
    )

def _portfolio_contains(state_name: str, district_name: str) -> bool:
    """
    Return True if the (state, district) pair is already present
    in the current portfolio_districts list.
    """
    return _portfolio_contains_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        state_key="portfolio_districts",
    )

def _portfolio_clear() -> None:
    """Clear all districts from the portfolio."""
    _portfolio_clear_impl(st.session_state, state_key="portfolio_districts")


def _portfolio_set_flash(message: str, level: str = "success") -> None:
    """Store a one-shot UI message to be rendered at the top of the right panel."""
    st.session_state["_portfolio_flash"] = {
        "message": str(message),
        "level": str(level or "success"),
    }

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
    render_perf_panel_safe()
    st.stop()

with perf_section("merge: build merged gdf"):
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
        with perf_section("map: compute baseline delta"):
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
    render_perf_panel_safe()
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

with perf_section("colors: apply_fillcolor"):
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
_t_rank = perf_start("rank_table: build")

table_df, has_baseline = _build_rankings_table_df(
    merged,
    metric_col=metric_col,
    baseline_col=baseline_col,
    selected_state=selected_state,
    risk_class_from_percentile=risk_class_from_percentile,
    district_col="district_name",
    state_col="state_name",
    aspirational_col="aspirational",
)

perf_end("rank_table: build", _t_rank)

_t_disp = perf_start("map: filter display_gdf")

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

perf_end("map: filter display_gdf", _t_disp)

m = folium.Map(
    location=st.session_state["map_center"],
    zoom_start=st.session_state["map_zoom"],
    tiles="CartoDB positron",
    control_scale=True,
    min_zoom=4,
    max_zoom=12,
    prefer_canvas=True,
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

# -------------------------
# Step 5: GeoJSON-by-state cache (geometry cached; properties patched per rerun)
# -------------------------
adm2_mtime = float(ADM2_GEOJSON.stat().st_mtime)
geojson_by_state = build_adm2_geojson_by_state(
    path=str(ADM2_GEOJSON),
    tolerance=SIMPLIFY_TOL_ADM2,
    mtime=adm2_mtime,
)

state_key = "all" if selected_state == "All" else (normalize_name(selected_state) or "unknown")
fc = copy.deepcopy(geojson_by_state.get(state_key, geojson_by_state["all"]))

# If a single district is selected, keep only that feature
if selected_district != "All":
    dist_key = alias(selected_district)
    fc["features"] = [
        f
        for f in fc.get("features", [])
        if alias(((f.get("properties") or {}).get("district_name", ""))) == dist_key
    ]

# Patch feature properties (fillColor + value columns) from the current display_gdf/merged
prop_gdf = display_gdf if not display_gdf.empty else merged
prop_work = prop_gdf.copy()
if "__key" not in prop_work.columns:
    prop_work["__key"] = prop_work["district_name"].map(alias)

value_cols: list[str] = []
for _c in (metric_col, map_value_col):
    if _c and (_c not in value_cols) and (_c in prop_work.columns):
        value_cols.append(_c)

keep_cols = ["__key", "district_name"]
if "fillColor" in prop_work.columns:
    keep_cols.append("fillColor")
keep_cols.extend(value_cols)

prop_work = prop_work[keep_cols].copy()

props_map: dict[str, dict] = {}
for _, r in prop_work.iterrows():
    k = r.get("__key")
    if not isinstance(k, str) or not k:
        continue

    upd: dict = {"district_name": r.get("district_name")}
    fill = r.get("fillColor")
    upd["fillColor"] = fill if isinstance(fill, str) and fill else "#cccccc"

    for c in value_cols:
        v = r.get(c)
        upd[c] = None if pd.isna(v) else v

    props_map[k] = upd

# --- Reduce GeoJSON to only districts present in the current data ---
valid_keys = set(props_map.keys())
if valid_keys:
    fc["features"] = [
        f
        for f in fc.get("features", [])
        if (
            ((f.get("properties") or {}).get("__key") in valid_keys)
            or (alias(((f.get("properties") or {}).get("district_name", ""))) in valid_keys)
        )
    ]

for feat in fc.get("features", []):
    props = feat.get("properties") or {}

    k = props.get("__key")
    if not isinstance(k, str) or not k:
        k = alias(props.get("district_name", ""))
        props["__key"] = k

    upd = props_map.get(k)
    if upd:
        props.update(upd)
    else:
        props.setdefault("fillColor", "#cccccc")

    # IMPORTANT: Folium tooltip asserts if a listed field key is missing.
    # Ensure these exist on every feature even if values are NaN/missing.
    for c in value_cols:
        props.setdefault(c, None)

    feat["properties"] = props

highlight_fn = None
if hover_enabled:
    highlight_fn = lambda f: {
        "fillColor": "#ffff00",
        "color": "#000",
        "weight": 2,
        "fillOpacity": 0.9,
    }

_t_geojson = perf_start("map: GeoJSON serialize+add layer")
folium.GeoJson(
    data=fc,
    name="Districts",
    style_function=style_fn,
    tooltip=tooltip,
    highlight_function=highlight_fn,
    smooth_factor=0.8,
    zoom_on_click=False,
).add_to(m)
perf_end("map: GeoJSON serialize+add layer", _t_geojson)

MAP_WIDTH, MAP_HEIGHT = 780, 700

legend_html = build_vertical_gradient_legend_html(
    pretty_metric_label=pretty_metric_label,
    vmin=vmin,
    vmax=vmax,
    cmap_name=cmap_name,
    map_width=MAP_WIDTH,
    map_height=MAP_HEIGHT,
)
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

    from india_resilience_tool.app.sidebar import render_view_selector

    # Preserve the exact widget key + option strings; keep horizontal=True like legacy
    view = render_view_selector(label="View", horizontal=True)

# ---------- VIEW 1: MAP ----------
    if view == "🗺 Map view":

        returned, clicked_district, clicked_state = render_map_view(
            m=m,
            variable_slug=VARIABLE_SLUG,
            map_mode=map_mode,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            selected_state=selected_state,
            selected_district=selected_district,
            map_width=MAP_WIDTH,
            map_height=MAP_HEIGHT,
            perf_section=perf_section,
        )

        if clicked_district:
            st.session_state["pending_selected_district"] = clicked_district
            if clicked_state:
                st.session_state["pending_selected_state"] = clicked_state

    elif view == "📊 Rankings table":

        render_rankings_view(
            view=view,
            table_df=table_df,
            has_baseline=has_baseline,
            variables=VARIABLES,
            variable_slug=VARIABLE_SLUG,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            selected_state=selected_state,
            portfolio_add=_portfolio_add,
        )

# -------------------------
# Details panel (portfolio + risk cards, sparkline + comparison)
# -------------------------
with col2:

    # Reserved slot: "Selected district for portfolio" (map route) should appear ABOVE
    # the Portfolio analysis expander even though it's determined later in the script.
    portfolio_selected_slot = st.empty()

    # -------------------------
    # Multi-district portfolio mode: show a clean, guided right-panel flow
    # -------------------------
    analysis_mode_rhs = st.session_state.get("analysis_mode", "Single district focus")
    portfolio_route = st.session_state.get("portfolio_build_route", None)

    if analysis_mode_rhs == "Multi-district portfolio":
        # Ensure saved-points container exists even if the Point selection UI is hidden
        st.session_state.setdefault("point_query_points", [])

        # ---- State summary (shown first in portfolio mode; hide once a build method is chosen) ----
        if portfolio_route is None:
            st.subheader(f"{selected_state} — State summary")
            st.markdown(
                f"**Index:** {VARIABLES[VARIABLE_SLUG]['label']}  \n"
                f"**Scenario:** {sel_scenario}  \n"
                f"**Period:** {sel_period}"
            )

            if selected_state == "All":
                st.info("Select a state in the left panel to see a state summary and build a portfolio.")
            else:
                try:
                    ensemble_port, _, _ = compute_state_metrics_from_merged(
                        merged, adm1, metric_col, selected_state
                    )
                except Exception:
                    ensemble_port = {
                        "mean": None,
                        "median": None,
                        "p05": None,
                        "p95": None,
                        "std": None,
                        "n_districts": 0,
                    }

                def _fmt_metric(v: object) -> str:
                    try:
                        x = float(v)  # type: ignore[arg-type]
                        if np.isnan(x):
                            return "—"
                        return f"{x:.2f}"
                    except Exception:
                        return "—"

                if ensemble_port.get("n_districts", 0) > 0:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Mean", _fmt_metric(ensemble_port.get("mean")))
                    c2.metric("Median", _fmt_metric(ensemble_port.get("median")))
                    c3.metric("P05", _fmt_metric(ensemble_port.get("p05")))
                    c4.metric("P95", _fmt_metric(ensemble_port.get("p95")))
                    st.caption(f"Districts used: {int(ensemble_port.get('n_districts', 0))}")
                else:
                    st.caption("No numeric district values found for this state & selection.")

        # Keep consistent spacing between top section and the Portfolio analysis expander
        st.markdown("---")

        # ---- Portfolio analysis expander (shown second) ----
        with st.expander("Portfolio analysis (multi-district)", expanded=True):

            # ---- STEP 1: Choose how to build the portfolio ----
            st.markdown("### Step 1 – Build your district portfolio")
            st.caption(
                "Choose one method to start adding districts. The dashboard will guide you through the relevant path."
            )

            col_route_1, col_route_2, col_route_3 = st.columns(3)
            with col_route_1:
                if st.button(
                    "📊 From the rankings table",
                    key="btn_portfolio_route_rankings",
                    use_container_width=True,
                ):
                    st.session_state["portfolio_build_route"] = "rankings"
                    st.session_state["jump_to_rankings"] = True
                    st.session_state["jump_to_map"] = False
                    st.rerun()
            with col_route_2:
                if st.button(
                    "🗺 From the map",
                    key="btn_portfolio_route_map",
                    use_container_width=True,
                ):
                    st.session_state["portfolio_build_route"] = "map"
                    st.session_state["jump_to_map"] = True
                    st.session_state["jump_to_rankings"] = False
                    st.rerun()
            with col_route_3:
                if st.button(
                    "📍 From saved points",
                    key="btn_portfolio_route_saved_points",
                    use_container_width=True,
                ):
                    st.session_state["portfolio_build_route"] = "saved_points"
                    st.session_state["jump_to_rankings"] = False
                    st.session_state["jump_to_map"] = False
                    st.rerun()

            route = st.session_state.get("portfolio_build_route", None)

            route_label_map = {
                "rankings": "From the rankings table",
                "map": "From the map",
                "saved_points": "From saved points",
            }

            if route in route_label_map:
                st.caption(f"Selected method: **{route_label_map[route]}**")
                if st.button("↩ Change method", key="btn_portfolio_route_reset"):
                    st.session_state["portfolio_build_route"] = None
                    st.session_state["jump_to_rankings"] = False
                    st.session_state["jump_to_map"] = False
                    st.rerun()

            # Always fetch portfolio (even if route is None)
            portfolio = st.session_state.get("portfolio_districts", [])

            # Route hint (but do NOT gate analysis if portfolio already exists)
            if route is None:
                if portfolio:
                    st.caption(
                        f"Current portfolio: **{len(portfolio)}** district(s). "
                        "You can continue to Step 2 below, or choose a method above to add more."
                    )
                else:
                    st.caption("Choose a method above to start building your portfolio.")
            elif route == "rankings":
                st.caption(
                    "Add districts from the **Rankings table** (left) and come back here to analyse."
                )
            elif route == "map":
                st.caption(
                    "Add districts by selecting them on the **Map** (left) and clicking **Add to portfolio**."
                )
            elif route == "saved_points":
                st.caption("Add districts using the **Saved points** panel below.")

            st.markdown("---")

            # ---- Always show portfolio + analysis steps whenever portfolio is non-empty ----
            if not portfolio:
                st.info(
                    "No districts in portfolio yet. Add districts via **From the rankings table**, "
                    "**From the map**, or **From saved points**."
                )
            else:
                # ---- STEP 2: Select indices for portfolio comparison ----
                st.markdown("### Step 2 – Select indices for portfolio analysis")
                st.caption(
                    "Pick one or more indices to compare across the portfolio. "
                    "This is the main lever for portfolio comparison."
                )

                available_indices = [(slug, meta["label"]) for slug, meta in VARIABLES.items()]
                default_sel = st.session_state.get("portfolio_multiindex_selection", [])
                selected_slugs = st.multiselect(
                    "Select indices",
                    options=[s for s, _ in available_indices],
                    default=default_sel if default_sel else [VARIABLE_SLUG],
                    format_func=lambda s: VARIABLES[s]["label"] if s in VARIABLES else str(s),
                    key="portfolio_multiindex_selection",
                )

                # ---- STEP 3: Multi-index comparison for portfolio (build + results) ----
                if not selected_slugs:
                    st.warning("Select at least one index to build a portfolio comparison.")
                else:
                    st.markdown("### Step 3 – Portfolio comparison (multi-index)")
                    st.caption(
                        "Build a combined table across all selected indices for the districts in your portfolio."
                    )

                    st.markdown("#### Multi-index comparison for portfolio")

                    def _resolve_proc_root_for_slug(slug: str) -> Path:
                        """
                        Resolve processed root for a given index slug.

                        If IRT_PROCESSED_ROOT is set:
                          - if it already points to .../<slug>, use it
                          - else assume it's a base dir and append /<slug>
                        Else default to DATA_DIR/processed/<slug>.
                        """
                        env_root = os.getenv("IRT_PROCESSED_ROOT")
                        if env_root:
                            base_path = Path(env_root)
                            if base_path.name == slug:
                                proc_root = base_path
                            else:
                                proc_root = base_path / slug
                        else:
                            proc_root = DATA_DIR / "processed" / slug
                        return proc_root.resolve()

                    def _load_master_and_schema_for_slug(slug: str) -> tuple[pd.DataFrame, list, list, dict]:
                        """
                        Load master_metrics_by_district.csv for a slug, normalize columns,
                        and parse schema. Cached by (slug, master_path, mtime).
                        """
                        proc_root = _resolve_proc_root_for_slug(slug)
                        master_path = proc_root / PILOT_STATE / "master_metrics_by_district.csv"

                        cache = st.session_state.setdefault("_portfolio_master_cache", {})

                        try:
                            mtime = master_path.stat().st_mtime
                        except Exception:
                            mtime = None

                        cache_key = f"{slug}::{str(master_path)}"
                        entry = cache.get(cache_key)
                        if entry is not None and entry.get("mtime") == mtime:
                            return (
                                entry["df"],
                                entry["schema_items"],
                                entry["metrics"],
                                entry["by_metric"],
                            )

                        if not master_path.exists():
                            empty_df = pd.DataFrame()
                            cache[cache_key] = {
                                "df": empty_df,
                                "schema_items": [],
                                "metrics": [],
                                "by_metric": {},
                                "mtime": mtime,
                            }
                            return empty_df, [], [], {}

                        # Load + normalize + schema
                        df_local = load_master_csv(str(master_path))
                        df_local = normalize_master_columns(df_local)
                        schema_items_local, metrics_local, by_metric_local = parse_master_schema(df_local.columns)

                        cache[cache_key] = {
                            "df": df_local,
                            "schema_items": schema_items_local,
                            "metrics": metrics_local,
                            "by_metric": by_metric_local,
                            "mtime": mtime,
                        }
                        return df_local, schema_items_local, metrics_local, by_metric_local

                    def _match_row_idx(df_local: pd.DataFrame, st_name: str, dist_name: str) -> Optional[int]:
                        """
                        Robustly match (state, district) in a master df that has columns
                        ['state', 'district'] using normalized comparisons + contains fallback.
                        """
                        if df_local is None or df_local.empty:
                            return None
                        if "state" not in df_local.columns or "district" not in df_local.columns:
                            return None

                        st_norm = _portfolio_normalize(st_name)
                        dist_norm = _portfolio_normalize(dist_name)

                        state_norm = df_local["state"].astype(str).map(_portfolio_normalize)
                        dist_norm_series = df_local["district"].astype(str).map(_portfolio_normalize)

                        exact = (state_norm == st_norm) & (dist_norm_series == dist_norm)
                        if exact.any():
                            return int(df_local.index[exact][0])

                        # Fallback: contains (handles minor naming differences)
                        # e.g., "north 24 parganas" vs "24 parganas north" style mismatches (rare but helpful)
                        try:
                            contains_1 = dist_norm_series.str.contains(dist_norm, na=False)
                            contains_2 = pd.Series(
                                [dist_norm in str(x) for x in dist_norm_series.tolist()],
                                index=df_local.index,
                            )
                            fallback = (state_norm == st_norm) & (contains_1 | contains_2)
                            if fallback.any():
                                return int(df_local.index[fallback][0])
                        except Exception:
                            pass

                        return None

                    from india_resilience_tool.analysis.metrics import compute_rank_and_percentile

                    def _compute_rank_and_percentile(
                        df_local: pd.DataFrame,
                        st_name: str,
                        metric_col: str,
                        value: float,
                    ) -> tuple[Optional[int], Optional[float]]:
                        """
                        Rank is 1..N within state (descending; higher value => rank 1).
                        Percentile is 0..100 where higher value => higher percentile.

                        Delegates to shared analysis.metrics to keep behavior consistent.
                        """
                        return compute_rank_and_percentile(
                            df_local,
                            st_name,
                            metric_col,
                            value,
                            state_col="state",
                            normalize_fn=_portfolio_normalize,
                            percentile_method="le",
                        )

                    def _build_portfolio_multiindex_df() -> pd.DataFrame:
                        return _build_portfolio_multiindex_df_impl(
                            portfolio=portfolio,
                            selected_slugs=selected_slugs,
                            variables=VARIABLES,
                            index_group_labels=INDEX_GROUP_LABELS,
                            sel_scenario=sel_scenario,
                            sel_period=sel_period,
                            sel_stat=sel_stat,
                            load_master_and_schema_for_slug=_load_master_and_schema_for_slug,
                            resolve_metric_column=resolve_metric_column,
                            find_baseline_column_for_stat=find_baseline_column_for_stat,
                            match_row_idx=_match_row_idx,
                            compute_rank_and_percentile=_compute_rank_and_percentile,
                            risk_class_from_percentile=risk_class_from_percentile,
                            normalize_fn=_portfolio_normalize,
                        )

                    # If the selection context changed, prompt rebuild (avoid showing stale table)
                    context_now = {
                        "slugs": list(selected_slugs),
                        "scenario": sel_scenario,
                        "period": sel_period,
                        "stat": sel_stat,
                    }
                    prev_context = st.session_state.get("portfolio_multiindex_context")
                    if prev_context != context_now:
                        st.session_state.pop("portfolio_multiindex_df", None)
                        st.session_state["portfolio_multiindex_context"] = context_now

                    if st.button(
                        "Build multi-index portfolio table",
                        key="btn_build_multiindex_portfolio_table",
                        use_container_width=True,
                    ):
                        with st.spinner("Building multi-index portfolio table..."):
                            st.session_state["portfolio_multiindex_df"] = _build_portfolio_multiindex_df()

                    portfolio_multiindex_df = st.session_state.get("portfolio_multiindex_df")
                    if isinstance(portfolio_multiindex_df, pd.DataFrame) and not portfolio_multiindex_df.empty:
                        st.markdown("#### Portfolio – multi-index summary")
                        st.dataframe(portfolio_multiindex_df, hide_index=True, use_container_width=True)

                        st.download_button(
                            "⬇️ Download portfolio data (multi-index, CSV)",
                            data=portfolio_multiindex_df.to_csv(index=False).encode("utf-8"),
                            file_name="portfolio_multiindex_summary.csv",
                            mime="text/csv",
                            use_container_width=True,
                        )

                        if st.button(
                            "📊 Open rankings table (portfolio view)",
                            key="btn_open_rankings_from_summary",
                        ):
                            st.session_state["jump_to_rankings"] = True
                            st.rerun()
                    else:
                        st.info(
                            "Click **Build multi-index portfolio table** to generate the comparison table."
                        )


                # ---- STEP 4: Review / edit portfolio (keep editing controls tucked away) ----
                with st.expander("Step 4 – Review and edit portfolio districts", expanded=False):
                    st.caption("Remove individual districts or remove all.")

                    flash_msg = st.session_state.pop("portfolio_flash", None)
                    if flash_msg:
                        st.success(flash_msg)

                    st.session_state.setdefault("confirm_clear_portfolio", False)

                    top_l, top_r = st.columns([3, 2])
                    with top_l:
                        st.markdown(f"**Portfolio districts ({len(df_summary) if 'df_summary' in locals() else len(portfolio)})**")
                    with top_r:
                        if not st.session_state["confirm_clear_portfolio"]:
                            if st.button("🧹 Remove all", key="btn_portfolio_remove_all"):
                                st.session_state["confirm_clear_portfolio"] = True
                                st.rerun()
                        else:
                            st.warning("Remove all districts from the portfolio?")
                            c_yes, c_no = st.columns(2)
                            with c_yes:
                                if st.button("✅ Confirm", key="btn_portfolio_remove_all_confirm"):
                                    _portfolio_remove_all()
                                    st.session_state["confirm_clear_portfolio"] = False
                                    st.session_state["portfolio_flash"] = "Cleared portfolio selection."
                                    st.rerun()
                            with c_no:
                                if st.button("✖ Cancel", key="btn_portfolio_remove_all_cancel"):
                                    st.session_state["confirm_clear_portfolio"] = False
                                    st.rerun()

                    # Show editable list (robust even if df_summary isn't built yet)
                    try:
                        table_df = df_summary[["District", "State"]].copy() if "df_summary" in locals() else pd.DataFrame(
                            [{"District": (d.get("district") if isinstance(d, dict) else d[1]),
                              "State": (d.get("state") if isinstance(d, dict) else d[0])}
                             for d in portfolio]
                        )
                    except Exception:
                        table_df = pd.DataFrame()

                    if table_df.empty:
                        st.warning("Portfolio exists but could not be displayed in table format.")
                    else:
                        for i, row in table_df.iterrows():
                            district_i = str(row.get("District", "")).strip()
                            state_i = str(row.get("State", "")).strip()
                            c1, c2, c3 = st.columns([4, 4, 2])
                            with c1:
                                st.write(district_i)
                            with c2:
                                st.write(state_i)
                            with c3:
                                if st.button(
                                    "🗑 Remove",
                                    key=f"btn_portfolio_remove_{_portfolio_normalize(state_i)}_{_portfolio_normalize(district_i)}",
                                ):
                                    _portfolio_remove(state_i, district_i)
                                    st.session_state["portfolio_flash"] = (
                                        f"Removed {district_i}, {state_i} from portfolio."
                                    )
                                    st.rerun()



    else:
        # In non-portfolio modes, the right panel content is rendered by the
        # district/state details logic below.
        pass

    # -------------------------
    # Climate profile / point query panel
    # -------------------------
    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    portfolio_route = st.session_state.get("portfolio_build_route", None)
    clear_clicked = False

    # In portfolio mode, we keep the right panel clean by default (no Climate Profile header).
    if analysis_mode != "Multi-district portfolio":
        st.header("Climate Profile")

    # --- Point-level query controls: only in portfolio mode AND only for the "saved points" route ---
    if analysis_mode == "Multi-district portfolio" and portfolio_route == "saved_points":
        # Container for multi-point saved list
        if "point_query_points" not in st.session_state:
            st.session_state["point_query_points"] = []

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

        st.subheader("Saved points")

        with st.expander("📍 Point selection", expanded=True):
            st.caption(
                "Choose locations either by typing coordinates or by clicking on the map, "
                "and optionally save them as candidate points for your portfolio."
            )

            # -------------------------------
            # Option 1 — Type coordinates
            # -------------------------------
            st.markdown("**Option 1 — Type coordinates**")

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

            # Actions for the typed point:
            # - Show on map (set as active point)
            # - Save to list (for portfolio later)
            col_set_active, col_save_point = st.columns(2)

            # 1) Show this point on the map (set active point)
            with col_set_active:
                if st.button("Show on map", key="btn_use_latlon"):
                    try:
                        lat_f = float(lat_input)
                        lon_f = float(lon_input)
                    except (TypeError, ValueError):
                        _portfolio_set_flash("Invalid latitude/longitude.", level="warning")
                        st.rerun()

                    st.session_state["point_query_lat"] = lat_f
                    st.session_state["point_query_lon"] = lon_f
                    st.session_state["point_query_latlon"] = {"lat": lat_f, "lon": lon_f}

                    # Ensure the user is looking at the map; then rerun so the marker renders immediately.
                    st.session_state["jump_to_map"] = True
                    st.session_state["jump_to_rankings"] = False
                    st.session_state["active_view"] = "🗺 Map view"
                    st.rerun()

            # 2) Add current typed point to saved list (multi-point / portfolio)
            with col_save_point:
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

            st.markdown("---")

            # -------------------------------
            # Option 2 — Select from map
            # -------------------------------
            st.markdown("**Option 2 — Select from map**")

            col_pick_on_map, col_clear_point = st.columns(2)

            # 3) Enable one-shot map selection
            with col_pick_on_map:
                if st.button("Click on map to choose", key="btn_select_on_map"):
                    # Next map click will set the point and then turn this flag off
                    st.session_state["point_query_select_on_map"] = True

            # 4) Clear current active point
            with col_clear_point:
                if st.button("Clear active point", key="btn_clear_point"):
                    # Clear any previously stored point selection and marker
                    for _k in (
                        "point_query_lat",
                        "point_query_lon",
                        "point_query_latlon",
                        "point_query_select_on_map",
                    ):
                        st.session_state.pop(_k, None)
                    clear_clicked = True

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
                st.caption(
                    "These points remember locations you care about. "
                    "You can map them to districts and add those districts to the portfolio."
                )
                saved_points_df = pd.DataFrame(saved_points)
                saved_points_df.index = saved_points_df.index + 1
                st.dataframe(
                    saved_points_df.rename(columns={"lat": "Latitude", "lon": "Longitude"}),
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
                        pts = st.session_state.get("point_query_points", [])
                        if not pts:
                            _portfolio_set_flash(
                                "No saved points found. Save at least one point first.",
                                level="warning",
                            )
                            st.rerun()

                        # Track what was already in the portfolio so we can count "new" additions.
                        before_items = st.session_state.get("portfolio_districts", [])
                        before_keys = set()
                        for it in before_items:
                            if isinstance(it, dict):
                                before_keys.add(_portfolio_key(it.get("state"), it.get("district")))

                        added_new = 0

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
                            if not (state_name and district_name):
                                continue

                            k = _portfolio_key(state_name, district_name)
                            if k not in before_keys:
                                before_keys.add(k)
                                added_new += 1

                            _portfolio_add(state_name, district_name)

                        if added_new > 0:
                            _portfolio_set_flash(
                                f"Added {added_new} new district(s) to the portfolio from saved points.",
                                level="success",
                            )
                        else:
                            _portfolio_set_flash(
                                "No new districts were added (they may already be in the portfolio).",
                                level="info",
                            )

                        # Force a rerun so the Portfolio analysis panel (above) re-renders with the updated list.
                        st.rerun()

            else:
                st.caption(
                    "Use **Save point** to build a list of locations and then "
                    "send their districts into the multi-district portfolio."
                )

    clicked_feature = None
    click_coords = None
    if returned:
        for k in ("last_active_drawing", "last_object_clicked", "last_object"):
            if returned.get(k) is not None:
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

    if analysis_mode == "Multi-district portfolio" and portfolio_route == "saved_points":
        # If map selection mode is active, use the next map click as the
        # point-query location and then disable the mode (one-shot behaviour).
        if click_coords is not None and st.session_state.get("point_query_select_on_map", False):
            lat_click, lon_click = click_coords
            st.session_state["point_query_lat"] = lat_click
            st.session_state["point_query_lon"] = lon_click
            st.session_state["point_query_latlon"] = {"lat": lat_click, "lon": lon_click}
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
        sel_district_raw = st.session_state.get("selected_district", "All")
        # Some UI controls store values like "District, State" — match on the district token.
        sel_district_norm = str(sel_district_raw).split(",")[0].strip().lower()

        district_series = merged["district_name"].astype(str).str.strip().str.lower()
        mask = district_series == sel_district_norm
        if (not mask.any()) and sel_district_norm:
            mask = district_series.str.contains(re.escape(sel_district_norm), na=False)

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
    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    if (
        (matched_row is None or matched_row.empty)
        and selected_district == "All"
    ):
        if analysis_mode == "Multi-district portfolio":
            # In portfolio mode, we suppress the large state-summary panel here.
            # Portfolio results should be driven by the Portfolio analysis panel.
            pass
        elif selected_state != "All":
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
                from india_resilience_tool.analysis.timeseries import load_state_yearly

                return load_state_yearly(
                    ts_root=Path(ts_root_str),
                    state_dir=state_dir,
                    varcfg=None,
                )

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
        analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

        if matched_row is None or getattr(matched_row, "empty", True):
            st.warning("No district-level data found for the current selection.")
            if analysis_mode == "Multi-district portfolio":
                st.info(
                    "In portfolio mode, add districts via **From the map**, **From saved points**, "
                    "or **From the rankings table** (Portfolio analysis panel)."
                )
            else:
                st.info(
                    "Please choose a different district from the sidebar, or select **All** "
                    "to view the state summary."
                )
            st.stop()

        row = matched_row.iloc[0]
        district_name = row.get("district_name", "Unknown")
        state_to_show = (
            st.session_state.get("selected_state")
            if st.session_state.get("selected_state") != "All"
            else (row.get("state_name") or "Unknown")
        )

        # --- Compact selection view in Multi-district portfolio mode ---
        if analysis_mode == "Multi-district portfolio":
            portfolio_route = st.session_state.get("portfolio_build_route", None)

            # Only show the "selected district" panel when the user explicitly chose
            # the "From the map" route.
            if portfolio_route == "map":
                with portfolio_selected_slot.container():
                    st.subheader("Selected district for portfolio")
                    st.markdown(f"**District:** {district_name}")
                    st.markdown(f"**State:** {state_to_show}")

                    if click_coords is not None:
                        st.caption(
                            f"Selected via map click at lat {click_coords[0]:.4f}, "
                            f"lon {click_coords[1]:.4f} (assigned to this district)."
                        )

                    already_in = _portfolio_contains(state_to_show, district_name)

                    c_add, c_remove = st.columns(2)
                    with c_add:
                        if not already_in:
                            if st.button(
                                "➕ Add to portfolio",
                                key=f"btn_add_portfolio_maproute_{_portfolio_normalize(state_to_show)}_{_portfolio_normalize(district_name)}",
                                use_container_width=True,
                            ):
                                _portfolio_add(state_to_show, district_name)
                                # Flash message is shown in your Step 2 portfolio panel
                                st.session_state["portfolio_flash"] = (
                                    f"Added {district_name}, {state_to_show} to portfolio."
                                )
                                # Force a fresh rerun so the portfolio panel re-renders with new state
                                st.rerun()
                        else:
                            st.success("Already in portfolio")

                    with c_remove:
                        if already_in:
                            if st.button(
                                "🗑 Remove",
                                key=f"btn_remove_portfolio_maproute_{_portfolio_normalize(state_to_show)}_{_portfolio_normalize(district_name)}",
                                use_container_width=True,
                            ):
                                _portfolio_remove(state_to_show, district_name)
                                st.session_state["portfolio_flash"] = (
                                    f"Removed {district_name}, {state_to_show} from portfolio."
                                )
                                st.rerun()

                    st.caption(f"Portfolio size: {len(st.session_state.get('portfolio_districts', []))} district(s)")

            # In portfolio mode, do NOT render the full climate profile below.
            render_perf_panel_safe()
            st.stop()

        # --- Full district climate profile (single-district focus mode) ---
        st.subheader(district_name)
        st.markdown(f"**State:** {state_to_show}")

        # If this view was triggered by a point query, show the coordinates used.
        if click_coords is not None:
            st.caption(
                f"Point location used: lat {click_coords[0]:.4f}, "
                f"lon {click_coords[1]:.4f} (assigned to this district)."
            )

        # --- Portfolio add button (for multi-district analysis) ---
        if analysis_mode == "Multi-district portfolio":
            if st.button(
                "➕ Add this district to portfolio",
                key=f"btn_add_portfolio_single_{state_to_show}_{district_name}",
            ):
                _portfolio_add(state_to_show, district_name)
                st.success(f"Added {district_name}, {state_to_show} to portfolio.")

            # Always show current portfolio below the button
            portfolio_current = st.session_state.get("portfolio_districts", [])
            if portfolio_current:
                st.markdown("**Current portfolio (districts)**")
                try:
                    # Usual case: list of dicts {"state": . "district": .}
                    if isinstance(portfolio_current[0], dict):
                        port_df = (
                            pd.DataFrame(portfolio_current)
                            .rename(columns={"state": "State", "district": "District"})
                        )
                    else:
                        # Fallback: list of (state, district) tuples/lists
                        port_df = pd.DataFrame(
                            portfolio_current, columns=["State", "District"]
                        )
                except Exception:
                    port_df = pd.DataFrame(columns=["State", "District"])

                st.dataframe(
                    port_df,
                    use_container_width=True,
                )
            else:
                st.caption(
                    "No districts in the portfolio yet. "
                    "Use the button above or the Rankings table to add districts."
                )

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

        # ---- Helper functions for time series and case study ----

        @st.cache_data
        def _read_yearly_csv(fpath: Path) -> pd.DataFrame:
            from india_resilience_tool.analysis.timeseries import read_yearly_csv_robust, prepare_yearly_series

            df = read_yearly_csv_robust(fpath)
            return prepare_yearly_series(df)

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

            Delegates to india_resilience_tool.analysis.timeseries for robust discovery.
            """
            from india_resilience_tool.analysis.timeseries import load_district_yearly

            return load_district_yearly(
                ts_root=ts_root,
                state_dir=state_dir,
                district_display=district_display,
                scenario_name=scenario_name,
                varcfg=varcfg,
                aliases=aliases,
                normalize_fn=alias,  # shared normalization + NAME_ALIASES (Step 9)
            )


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
                available_metrics = list(metrics_local or [])
                if not available_metrics:
                    continue

                def _metric_norm(m: str) -> str:
                    # remove spaces AND underscores so:
                    # "gt_25mm" and "gt25mm" can be matched
                    return _portfolio_normalize(m).replace("_", "")

                if registry_metric not in available_metrics:
                    # Exact lower-case match first
                    m_lower = {str(m).lower(): m for m in available_metrics}
                    registry_metric = m_lower.get(str(registry_metric).lower())

                if registry_metric not in available_metrics:
                    # Normalized equality / contains fallback
                    target_norm = _metric_norm(str(registry_metric))
                    eq_matches = [
                        m for m in available_metrics
                        if _metric_norm(str(m)) == target_norm
                    ]
                    if eq_matches:
                        registry_metric = eq_matches[0]
                    else:
                        contains_matches = [
                            m for m in available_metrics
                            if target_norm and target_norm in _metric_norm(str(m))
                        ]
                        registry_metric = contains_matches[0] if contains_matches else available_metrics[0]

                # Candidate column set for this metric + scenario + period (stat may vary)
                prefix = f"{registry_metric}__{sel_scenario}__{sel_period}__"
                metric_col_candidates = [
                    c for c in df_master.columns
                    if isinstance(c, str) and c.startswith(prefix)
                ]

                desired_col = f"{registry_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
                metric_col_local = desired_col if desired_col in df_master.columns else None

                if metric_col_local is None:
                    if not metric_col_candidates:
                        continue

                    def _stat_norm(s: str) -> str:
                        return _portfolio_normalize(s).replace("_", "")

                    sel_stat_norm = _stat_norm(str(sel_stat))
                    stat_matches = [
                        c for c in metric_col_candidates
                        if _stat_norm(c.split("__")[-1]) == sel_stat_norm
                    ]
                    metric_col_local = stat_matches[0] if stat_matches else metric_col_candidates[0]

                used_stat = str(metric_col_local).split("__")[-1]

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

                # Current value (try fallback columns if the chosen one is NaN)
                current_val_f_local = None

                current_val_local = row_local.get(metric_col_local)
                current_val_try = pd.to_numeric([current_val_local], errors="coerce")[0]
                if not pd.isna(current_val_try):
                    current_val_f_local = float(current_val_try)
                else:
                    # Try alternate stat columns for the same metric/scenario/period
                    for alt_col in metric_col_candidates:
                        if alt_col == metric_col_local:
                            continue
                        alt_val = row_local.get(alt_col)
                        alt_try = pd.to_numeric([alt_val], errors="coerce")[0]
                        if not pd.isna(alt_try):
                            metric_col_local = alt_col
                            used_stat = str(metric_col_local).split("__")[-1]
                            current_val_f_local = float(alt_try)
                            break

                # Baseline for same metric/stat in historical baseline period
                baseline_col_local = find_baseline_column_for_stat(
                    dm.columns, registry_metric, used_stat
                )

                baseline_col_local = find_baseline_column_for_stat(dm.columns, registry_metric, sel_stat)
                baseline_val_f_local = None
                if baseline_col_local and baseline_col_local in dm.columns:
                    baseline_val_local = row_local.get(baseline_col_local)
                    baseline_val_f_local = pd.to_numeric([baseline_val_local], errors="coerce")[0]
                    if pd.isna(baseline_val_f_local):
                        baseline_val_f_local = None

                if current_val_f_local is not None and baseline_val_f_local is not None:
                    delta_abs = current_val_f_local - baseline_val_f_local
                    delta_pct = None
                    if baseline_val_f_local not in (0.0,):
                        delta_pct = (delta_abs / baseline_val_f_local) * 100.0
                else:
                    delta_abs = None
                    delta_pct = None

                # Ranking within state
                state_mask = dm["_state_key"] == target_state
                state_vals_local = pd.to_numeric(dm.loc[state_mask, metric_col_local], errors="coerce").dropna()
                n_in_state_local = int(len(state_vals_local)) if len(state_vals_local) else None
                rank_in_state_local = None
                percentile_in_state = None
                if n_in_state_local and current_val_f_local is not None:
                    rank_in_state_local = int((state_vals_local > current_val_f_local).sum() + 1)
                    from india_resilience_tool.analysis.metrics import compute_percentile_in_state
                    percentile_in_state = compute_percentile_in_state(state_vals_local, current_val_f_local, method="lt")
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
                        "current": current_val_f_local,
                        "baseline": baseline_val_f_local,
                        "delta_abs": delta_abs,
                        "delta_pct": delta_pct,
                        "rank_in_state": rank_in_state_local,
                        "percentile_in_state": percentile_in_state,
                        "n_in_state": n_in_state_local,
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
                hist_ts_local = _filter_series_for_trend(hist_df, state_name, district_name)
                scen_ts_local = _filter_series_for_trend(scen_df, state_name, district_name)
                timeseries_by_index[slug] = {
                    "historical": hist_ts_local,
                    "scenario": scen_ts_local,
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
            from india_resilience_tool.viz.exports import make_case_study_zip

            # Preserve exported CSV labels exactly like the legacy dashboard did
            label_lookup: dict[str, str] = {}
            for slug in set(list((ts_dict or {}).keys()) + list((panel_dict or {}).keys())):
                label_lookup[slug] = VARIABLES.get(slug, {}).get("label", slug)

            return make_case_study_zip(
                state_name=state_name,
                district_name=district_name,
                summary_df=summary_df,
                ts_dict=ts_dict,
                panel_dict=panel_dict,
                pdf_bytes=pdf_bytes,
                index_label_lookup=label_lookup,
            )

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
            from india_resilience_tool.viz.exports import make_district_case_study_pdf

            return make_district_case_study_pdf(
                state_name=state_name,
                district_name=district_name,
                summary_df=summary_df,
                ts_dict=ts_dict,
                panel_dict=panel_dict,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
            )

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

        # Prepare time series for the details panel
        hist_ts = _filter_series_for_trend(_district_yearly_hist, state_to_show, district_name)
        scen_ts = _filter_series_for_trend(_district_yearly_scen, state_to_show, district_name)

        # Import required functions for details panel
        from india_resilience_tool.viz.charts import (
            create_trend_figure_for_index as _create_trend_figure_for_index,
        )
        from india_resilience_tool.viz.exports import (
            make_district_yearly_pdf,
            make_district_case_study_pdf as _make_district_case_study_pdf_impl,
            make_case_study_zip as _make_case_study_zip_impl,
        )
        from india_resilience_tool.data.discovery import slugify_fs

        # ---- SINGLE-DISTRICT DETAILS PANEL (extracted to details_panel.py) ----
        render_details_panel(
            # Core district/state context
            row=row,
            district_name=district_name,
            state_to_show=state_to_show,
            selected_district=selected_district,
            # Metric / variable context
            variables=VARIABLES,
            variable_slug=VARIABLE_SLUG,
            metric_col=metric_col,
            sel_metric=sel_metric,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            # Risk summary data
            current_val_f=current_val_f,
            baseline_val_f=baseline_val_f,
            baseline_col=baseline_col,
            rank_in_state=rank_in_state,
            n_in_state=n_in_state,
            percentile_state=percentile_state,
            # Time series data
            hist_ts=hist_ts,
            scen_ts=scen_ts,
            district_yearly_scen=_district_yearly_scen,
            # Schema for scenario comparison
            schema_items=schema_items,
            # GeoDataFrame for district comparison
            merged=merged,
            # Figure styling
            fig_size_panel=FIG_SIZE_PANEL,
            fig_dpi_panel=FIG_DPI_PANEL,
            font_size_title=FONT_SIZE_TITLE,
            font_size_label=FONT_SIZE_LABEL,
            font_size_ticks=FONT_SIZE_TICKS,
            font_size_legend=FONT_SIZE_LEGEND,
            # Constants
            period_order=PERIOD_ORDER,
            scenario_display=SCENARIO_DISPLAY,
            out_dir=OUTDIR,
            # Callable dependencies
            create_trend_figure_fn=_create_trend_figure_for_index,
            build_scenario_panel_fn=build_scenario_comparison_panel_for_row,
            make_scenario_figure_fn=make_scenario_comparison_figure,
            make_district_yearly_pdf_fn=make_district_yearly_pdf,
            build_case_study_data_fn=_build_district_case_study_data,
            make_case_study_pdf_fn=_make_district_case_study_pdf,
            make_case_study_zip_fn=_make_case_study_zip,
            slugify_fs_fn=slugify_fs,
            # Optional filesystem paths
            state_dir_for_fs=state_dir_for_fs,
            district_for_fs=district_for_fs,
        )

render_perf_panel_safe()
st.markdown("---")
st.caption(
    "Notes: first choose an Index group (e.g. Temperature vs Rainfall), then an Index within that group. "
    "Details panel shows risk cards, trends, narrative, and a comparison option."
)