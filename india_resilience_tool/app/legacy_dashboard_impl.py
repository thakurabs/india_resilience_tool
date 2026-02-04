#!/usr/bin/env python3
"""
Legacy Streamlit dashboard orchestrator for the India Resilience Tool.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""
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

from india_resilience_tool.data.adm3_loader import (
    load_local_adm3 as _load_local_adm3,
    get_blocks_for_district as _get_blocks_for_district,
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
    render_admin_level_selector,
    render_analysis_mode_selector,
    render_block_selector,
    render_hover_toggle_if_portfolio,
    render_view_selector,
)

from india_resilience_tool.app.views.map_view import (
    render_map_view,
    render_unit_add_to_portfolio,
)
from india_resilience_tool.app.views.rankings_view import render_rankings_view
from india_resilience_tool.app.views.details_panel import render_details_panel
from india_resilience_tool.app.views.state_summary_view import render_state_summary_view
from india_resilience_tool.app.portfolio_ui import render_portfolio_panel
from india_resilience_tool.app.point_selection_ui import render_point_selection_panel

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
from paths import DATA_DIR, DISTRICTS_PATH, BLOCKS_PATH, resolve_processed_root

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM1,
    MIN_LON,
    MAX_LON,
    MIN_LAT,
    MAX_LAT,
    FIG_SIZE_PANEL,
    FIG_DPI_PANEL,
    FONT_SIZE_TITLE,
    FONT_SIZE_LABEL,
    FONT_SIZE_TICKS,
    FONT_SIZE_LEGEND,
    LOGO_PATH,
)

from india_resilience_tool.config.variables import (
    VARIABLES,
    INDEX_GROUP_LABELS,
    # Bundle imports (NEW)
    get_bundles,
    get_metrics_for_bundle,
    get_bundle_description,
    get_default_bundle,
)

# Data paths derived from DATA_DIR
ADM2_GEOJSON = DISTRICTS_PATH
ADM3_GEOJSON = BLOCKS_PATH

ATTACH_DISTRICT_GEOJSON = str(ADM2_GEOJSON) if ADM2_GEOJSON.exists() else None
ATTACH_BLOCK_GEOJSON = str(ADM3_GEOJSON) if ADM3_GEOJSON.exists() else None

OUTDIR = DATA_DIR

# ---------- Name normalization / aliases ----------
from india_resilience_tool.utils.naming import NAME_ALIASES, alias, normalize_name, normalize_compact

# -------------------------
# Geo load / prep
# -------------------------
# @st.cache_data
@st.cache_data
def load_local_adm2(path: str, tolerance: float = SIMPLIFY_TOL_ADM2) -> gpd.GeoDataFrame:
    gdf = _load_local_adm2(
        path=path,
        tolerance=float(tolerance),
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
        min_area=0.0003,
    )
    return gdf


@st.cache_data(ttl=3600)
def load_local_adm3(path: str, tolerance: float = SIMPLIFY_TOL_ADM2) -> gpd.GeoDataFrame:
    """
    Load ADM3 (blocks) with the same bbox + simplification strategy as ADM2.

    Notes:
      - tolerant of large files via caching
      - does NOT require a __key column (merge.py builds composite keys for blocks)
    """
    gdf = _load_local_adm3(
        path=path,
        tolerance=float(tolerance),
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
        min_area=0.00005,
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

@st.cache_data(ttl=3600)
def build_adm3_geojson_by_state(
    path: str,
    tolerance: float,
    mtime: float,
) -> dict[str, dict]:
    """
    Build and cache an ADM3 FeatureCollection per state (geometry + identifiers only).

    Cached by (path, tolerance, mtime) so it invalidates automatically when the
    source GeoJSON changes or simplification tolerance is updated.
    """
    _ = mtime  # mtime is used only to invalidate Streamlit's cache

    gdf = load_local_adm3(path, tolerance=tolerance)

    # Tolerate alternate ADM3 naming conventions
    if "block_name" not in gdf.columns:
        for c in ("block", "adm3_name", "subdistrict_name", "name"):
            if c in gdf.columns:
                gdf["block_name"] = gdf[c]
                break
    if "district_name" not in gdf.columns:
        for c in ("district", "adm2_name", "shapeName_2", "shapeName_1"):
            if c in gdf.columns:
                gdf["district_name"] = gdf[c]
                break
    if "state_name" not in gdf.columns:
        for c in ("state", "adm1_name", "shapeName_0", "shapeGroup"):
            if c in gdf.columns:
                gdf["state_name"] = gdf[c]
                break

    # Build a composite key: state|district|block (normalized via alias)
    if "__bkey" not in gdf.columns:
        def _mk_bkey(r) -> str:
            return f"{alias(r.get('state_name', ''))}|{alias(r.get('district_name', ''))}|{alias(r.get('block_name', ''))}"

        gdf["__bkey"] = gdf.apply(_mk_bkey, axis=1)

    by_state = _featurecollections_by_state(
        gdf,
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["block_name", "district_name", "state_name", "__bkey", "geometry"],
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
    *,
    adm2: Optional[gpd.GeoDataFrame],
    adm3: Optional[gpd.GeoDataFrame],
    df: pd.DataFrame,
    slug: str,
    master_path: Path,
    level: str = "district",
) -> gpd.GeoDataFrame:
    """
    Level-aware wrapper around get_or_build_merged_for_index_cached().

    - level="district": merges master (district rows) onto ADM2 boundaries
    - level="block":    merges master (block rows) onto ADM3 boundaries
    """
    level_norm = str(level or "district").strip().lower()
    boundary_gdf = adm3 if level_norm == "block" else adm2

    if boundary_gdf is None:
        raise ValueError(f"Boundary GeoDataFrame is required for level={level_norm!r}")

    merged = _get_or_build_merged_for_index_cached(
        boundary_gdf,
        df,
        slug=slug,
        master_path=master_path,
        session_state=st.session_state,
        alias_fn=alias,
        adm2_state_col="state_name",
        master_state_col="state",
        level=level_norm,
    )
    return merged  # type: ignore[return-value]


# -------------------------
# Master CSV freshness helpers (variable-agnostic)
# -------------------------
@st.cache_data(ttl=300)
def latest_processed_periods_mtime(processed_root_str: str, state: str) -> float:
    base = Path(processed_root_str) / state  # Convert string back to Path here
    if not base.exists():
        return 0.0
    latest = 0.0
    count = 0
    for f in base.rglob("*_periods.csv"):
        try:
            latest = max(latest, f.stat().st_mtime)
            count += 1
            if count >= 50:
                break
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
    return latest_processed_periods_mtime(str(processed_root), state) > (master_mtime + 1.0)

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
    period_display_label,
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

    # Admin level selector (District vs Block)
    admin_level = render_admin_level_selector(label_visibility="collapsed")


    # Read current analysis mode (default depends on admin level)
    default_mode = "Single block focus" if admin_level == "block" else "Single district focus"
    analysis_mode_current = st.session_state.get("analysis_mode", default_mode)

    # Show hover toggle (always visible)
    _ = render_hover_toggle_if_portfolio(analysis_mode_current)

    analysis_mode_placeholder = st.empty()  # Single vs portfolio
    state_placeholder = st.empty()
    district_placeholder = st.empty()
    block_placeholder = st.empty()

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
# Unified Index selection (bundle → metric)
# -------------------------
with metric_ui_placeholder.container():
    with st.expander("Metric selection", expanded=True):
        st.markdown("### Metric selection")

        # --- Bundle selection (replaces old group-based selection) ---
        all_bundles = get_bundles()
        
        # Get default bundle from session state or use system default
        default_bundle = st.session_state.get("selected_bundle")
        if default_bundle not in all_bundles:
            default_bundle = get_default_bundle()
            if default_bundle not in all_bundles:
                default_bundle = all_bundles[0] if all_bundles else None
        
        if not all_bundles:
            st.error("No bundles defined in metrics_registry.py")
            st.stop()
        
        selected_bundle = st.selectbox(
            "Risk domain",
            options=all_bundles,
            index=all_bundles.index(default_bundle) if default_bundle in all_bundles else 0,
            key="selected_bundle",
            help="Select a thematic bundle to filter available metrics",
        )
        
        # Show bundle description as tooltip/caption
        bundle_desc = get_bundle_description(selected_bundle)
        if bundle_desc:
            st.caption(bundle_desc)

        # Filter indices by the chosen bundle
        index_slugs = get_metrics_for_bundle(selected_bundle)

        # Safety fallback: if something goes wrong, show all indices
        if not index_slugs:
            index_slugs = list(VARIABLES.keys())
            st.warning(f"Bundle '{selected_bundle}' has no metrics; showing all indices.")

        # Previously selected index might not be in this bundle; clamp it
        default_slug = st.session_state.get("selected_var", index_slugs[0])
        if default_slug not in index_slugs:
            default_slug = index_slugs[0]

        selected_var = st.selectbox(
            "Metric",
            options=index_slugs,
            index=index_slugs.index(default_slug),
            key="selected_var",
            format_func=lambda k: VARIABLES[k]["label"] if k in VARIABLES else k,
        )

        # Resolve per-index config
        VARIABLE_SLUG = selected_var
        VARCFG = VARIABLES[VARIABLE_SLUG]

        # Default registry metric (prevents NameError if downstream master/schema logic short-circuits)
        registry_metric = str(VARCFG.get("periods_metric_col", "")).strip()
        st.session_state["registry_metric"] = registry_metric

        # --- Metric description ---
        desc = VARCFG.get("description", "").strip()
        if desc:
            st.caption(f"ℹ️ {desc}")

        PROCESSED_ROOT = resolve_processed_root(
            VARIABLE_SLUG, data_dir=DATA_DIR, mode="portfolio"
        )
        (PROCESSED_ROOT / PILOT_STATE).mkdir(parents=True, exist_ok=True)
        _master_name = "master_metrics_by_block.csv" if st.session_state.get("admin_level", "district") == "block" else "master_metrics_by_district.csv"
        MASTER_CSV_PATH = PROCESSED_ROOT / PILOT_STATE / _master_name


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

        periods_found = {
            canonical_period_label(i["period"])
            for i in (by_metric.get(registry_metric, []) or schema_items)
            if i["scenario"] == sel_scenario
        }

        # Keep a stable order across the app (baseline, early, mid, end-century)
        periods = [p for p in PERIOD_ORDER if p in periods_found] + sorted(
            [p for p in periods_found if p not in PERIOD_ORDER]
        )

        if not periods:
            st.error("No periods found for the selected scenario in the master CSV.")
            render_perf_panel_safe()
            st.stop()
        sel_period = st.selectbox("Period", periods, index=0, key="sel_period", format_func=period_display_label,)
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
    f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario} · {period_display_label(sel_period)} · {sel_stat}"
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

        admin_level = st.session_state.get("admin_level", "district")

        # ---- Step 2: District selection (always shown, required before block in block mode) ----
        # Ensure we always have a valid district in session state
        if (
            "selected_district" not in st.session_state
            or st.session_state["selected_district"] not in districts
        ):
            st.session_state["selected_district"] = "All"

        # Portfolio mode behavior for district selection:
        # - In district-level portfolio mode: freeze district to "All"
        # - In block-level portfolio mode: allow district selection (needed to navigate blocks)
        # Check this BEFORE creating the widget to avoid Streamlit session state errors
        _current_analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
        if "Multi" in _current_analysis_mode and admin_level != "block":
            st.session_state["selected_district"] = "All"

        selected_district = st.selectbox(
            "District",
            options=districts,
            index=districts.index(st.session_state["selected_district"]),
            key="selected_district",
        )

        # ---- Step 3: Block selection (only when admin_level == block AND district selected) ----
        selected_block = "All"
        if admin_level == "block":
            if not ADM3_GEOJSON.exists():
                st.error(f"ADM3 geojson not found at {ADM3_GEOJSON}. Please provide block_4326.geojson.")
                st.stop()

            # Load ADM3 boundaries for block selection
            adm3_sidebar = load_local_adm3(str(ADM3_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)

            block_options = ["All"]
            if selected_state != "All" and selected_district != "All":
                try:
                    blocks = _get_blocks_for_district(adm3_sidebar, selected_state, selected_district, normalize_fn=alias)
                    block_options = ["All"] + sorted([str(b).strip() for b in blocks if str(b).strip()])
                except Exception:
                    block_options = ["All"]

                if "selected_block" not in st.session_state or st.session_state["selected_block"] not in block_options:
                    st.session_state["selected_block"] = "All"

                selected_block = st.selectbox(
                    "Block",
                    options=block_options,
                    index=block_options.index(st.session_state.get("selected_block", "All")),
                    key="selected_block",
                )
            else:
                # Show disabled/info when district not selected
                if selected_district == "All":
                    st.caption("ℹ️ Select a district to see blocks")
                st.session_state["selected_block"] = "All"
        else:
            st.session_state.pop("selected_block", None)

        # ---- Step 4: Analysis focus (single vs portfolio; labels depend on admin_level) ----
        analysis_options = (
            ["Single block focus", "Multi-block portfolio"]
            if admin_level == "block"
            else ["Single district focus", "Multi-district portfolio"]
        )

        analysis_mode = render_analysis_mode_selector(
            label="Analysis focus",
            options=analysis_options,
            index=0,
            help_text=(
                "Choose a single-unit focus to explore one unit at a time, "
                "or portfolio mode to build and compare a set of units."
            ),
            label_visibility="collapsed",
            use_markdown_header=True,
            level=admin_level,
        )

        # Reset portfolio route state when switching analysis focus modes
        prev_mode = st.session_state.get("_analysis_mode_prev", analysis_mode)
        if analysis_mode != prev_mode:
            st.session_state["_analysis_mode_prev"] = analysis_mode
            # Clear any previously selected portfolio-build route and pending view jumps
            st.session_state["portfolio_build_route"] = None
            st.session_state["jump_to_rankings"] = False
            st.session_state["jump_to_map"] = False

        # Brief helper text so the mode explains itself (level-aware)
        unit_singular = "block" if admin_level == "block" else "district"
        unit_plural = "blocks" if admin_level == "block" else "districts"
        
        if "Single" in analysis_mode:
            st.caption(
                f"Inspect one {unit_singular} at a time. Use the dropdowns above "
                f"to pick which {unit_singular} you want to explore in detail."
            )
        else:
            st.markdown(
                f"<div style='font-size:0.9rem; margin-top:0.25rem; margin-bottom:0.1rem;'>"
                f"In <strong>Multi-{unit_singular} portfolio</strong> mode you build a set of {unit_plural} "
                f"for comparison. {unit_plural.title()} are added from the <em>🗺 Map view</em>, the "
                f"<em>📊 Rankings table</em>, or from saved point locations. "
                f"</div>",
                unsafe_allow_html=True,
            )

        # Note: Portfolio mode behavior for district selection is now handled BEFORE
        # the selectbox widget is created (around line 1186) to avoid Streamlit
        # session state modification errors.
        # In district-level portfolio mode: district is frozen to "All"
        # In block-level portfolio mode: district selection is allowed (needed to navigate blocks)
        if "Multi" in analysis_mode and admin_level != "block":
            # Just update the local variable; session_state was already set before widget
            selected_district = "All"

# -------------------------
# Portfolio selection helpers (multi-district)
# -------------------------

if "portfolio_districts" not in st.session_state:
    # List of {"state": ..., "district": ...}
    st.session_state["portfolio_districts"] = []

if "portfolio_blocks" not in st.session_state:
    # List of {"state": ..., "district": ..., "block": ...}
    st.session_state["portfolio_blocks"] = []


def _portfolio_normalize(text: str) -> str:
    """
    Normalize a state/district name for robust comparison across data sources.

    Delegates to india_resilience_tool.analysis.portfolio to keep logic centralized.
    """
    return _portfolio_normalize_impl(text, alias_fn=alias)


def _portfolio_state_key() -> str:
    return "portfolio_blocks" if st.session_state.get("admin_level", "district") == "block" else "portfolio_districts"


def _portfolio_key(state_name: str, district_name: str, block_name: Optional[str] = None) -> tuple:
    if st.session_state.get("admin_level", "district") == "block":
        return (
            _portfolio_normalize(state_name),
            _portfolio_normalize(district_name),
            _portfolio_normalize(block_name or ""),
        )
    return (_portfolio_normalize(state_name), _portfolio_normalize(district_name))


def _portfolio_add(state_name: str, district_name: str, block_name: Optional[str] = None) -> None:
    """Add a unit (district or block) to the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()

    _portfolio_add_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        block_name=block_name,
        level=level,
        state_key=state_key,
    )


def _portfolio_remove(state_name: str, district_name: str, block_name: Optional[str] = None) -> None:
    """Remove a unit (district or block) from the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()

    _portfolio_remove_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        block_name=block_name,
        level=level,
        state_key=state_key,
    )


def _portfolio_contains(state_name: str, district_name: str, block_name: Optional[str] = None) -> bool:
    """Return True if the unit is already present in the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()

    return bool(
        _portfolio_contains_impl(
            st.session_state,
            state_name,
            district_name,
            normalize_fn=_portfolio_normalize,
            block_name=block_name,
            level=level,
            state_key=state_key,
        )
    )


def _portfolio_clear() -> None:
    """Clear all units from the active portfolio (districts or blocks)."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()

    _portfolio_clear_impl(
        st.session_state,
        level=level,
        state_key=state_key,
    )


# Alias for backward compatibility with portfolio_ui
_portfolio_remove_all = _portfolio_clear


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

# Map zoom logic: handle district and block selections
_admin_level_for_zoom = st.session_state.get("admin_level", "district")
_admin_level_for_zoom = str(_admin_level_for_zoom or "district").strip().lower()

if _admin_level_for_zoom == "block" and selected_block != "All" and selected_district != "All":
    # Block mode with specific block selected: zoom to that block
    # Need to use adm3 boundaries (loaded later, so we'll set a flag to zoom after merge)
    st.session_state["_pending_block_zoom"] = {
        "state": selected_state,
        "district": selected_district,
        "block": selected_block,
    }
elif selected_district != "All":
    district_row = gdf_state_districts[gdf_state_districts["district_name"] == selected_district]
    if not district_row.empty:
        centroid = district_row.iloc[0].geometry.centroid
        st.session_state["map_center"] = [centroid.y, centroid.x]
        # In block mode with district selected, zoom in more to see blocks
        st.session_state["map_zoom"] = 10 if _admin_level_for_zoom == "block" else 9
    st.session_state.pop("_pending_block_zoom", None)
elif selected_state != "All":
    state_row = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
    if not state_row.empty:
        b = state_row.iloc[0].geometry.bounds
        st.session_state["map_center"] = [(b[1] + b[3]) / 2, (b[0] + b[2]) / 2]
        st.session_state["map_zoom"] = 7
    st.session_state.pop("_pending_block_zoom", None)
else:
    st.session_state["map_center"] = [22.0, 82.5]
    st.session_state["map_zoom"] = 4.8
    st.session_state.pop("_pending_block_zoom", None)

# Merge attributes (district vs block)
_admin_level = st.session_state.get("admin_level", "district")
_admin_level = str(_admin_level or "district").strip().lower()

if "district" not in df.columns:
    st.error("Master CSV must contain a 'district' column.")
    render_perf_panel_safe()
    st.stop()

# In block mode, master must also contain a block column
if _admin_level == "block":
    block_col_candidates = ["block", "block_name"]
    block_col = next((c for c in block_col_candidates if c in df.columns), None)
    if block_col is None:
        st.error("Block mode requires master CSV to contain a 'block' (or 'block_name') column.")
        render_perf_panel_safe()
        st.stop()

    if not ADM3_GEOJSON.exists():
        st.error(f"ADM3 geojson not found at {ADM3_GEOJSON}. Please provide block_4326.geojson.")
        render_perf_panel_safe()
        st.stop()

    adm3 = load_local_adm3(str(ADM3_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)
else:
    adm3 = None

with perf_section("merge: build merged gdf"):
    with st.spinner("Preparing merged geometries with CSV attributes..."):
        merged = get_or_build_merged_for_index(
            adm2=adm2,
            adm3=adm3,
            df=df,
            slug=VARIABLE_SLUG,
            master_path=MASTER_CSV_PATH,
            level=_admin_level,
        )

# Handle pending block zoom (needs merged GeoDataFrame with block geometries)
pending_zoom = st.session_state.pop("_pending_block_zoom", None)
if pending_zoom and "block_name" in merged.columns:
    zoom_state = pending_zoom.get("state", "")
    zoom_district = pending_zoom.get("district", "")
    zoom_block = pending_zoom.get("block", "")
    
    # Find the block row
    block_mask = (
        (merged["state_name"].astype(str).str.strip().str.lower() == zoom_state.strip().lower())
        & (merged["district_name"].astype(str).str.strip().str.lower() == zoom_district.strip().lower())
        & (merged["block_name"].astype(str).str.strip().str.lower() == zoom_block.strip().lower())
    )
    block_rows = merged[block_mask]
    
    if not block_rows.empty:
        block_geom = block_rows.iloc[0].geometry
        if block_geom is not None:
            centroid = block_geom.centroid
            st.session_state["map_center"] = [centroid.y, centroid.x]
            st.session_state["map_zoom"] = 11  # Zoom in closer for block view

# --- Baseline column for this metric + stat (used by map & table) ---
baseline_col = find_baseline_column_for_stat(df.columns, sel_metric, sel_stat)

# Debug: Show merged DataFrame info in block mode
if DEBUG and _admin_level == "block":
    st.sidebar.write(f"**DEBUG: merged has {len(merged)} rows**")
    st.sidebar.write(f"Columns: {list(merged.columns[:10])}...")
    if "block_name" in merged.columns:
        st.sidebar.write(f"block_name column exists ✓")
        st.sidebar.write(f"Sample blocks: {merged['block_name'].head(3).tolist()}")
    else:
        st.sidebar.write("❌ block_name column MISSING!")
    if metric_col in merged.columns:
        non_null = merged[metric_col].notna().sum()
        st.sidebar.write(f"metric_col '{metric_col}' has {non_null} non-null values")

# --- Compute current/baseline/delta columns once (used by map + tooltip) ---
with perf_section("map: compute current/baseline/delta"):
    merged["_current_value"] = pd.to_numeric(merged.get(metric_col), errors="coerce")

    if baseline_col and (baseline_col in merged.columns):
        merged["_baseline_value"] = pd.to_numeric(merged.get(baseline_col), errors="coerce")
        merged["_delta_abs"] = merged["_current_value"] - merged["_baseline_value"]

        # % change only when baseline is non-zero
        denom = merged["_baseline_value"].where(merged["_baseline_value"] != 0)
        merged["_delta_pct"] = (merged["_delta_abs"] / denom) * 100.0
    else:
        merged["_baseline_value"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")
        merged["_delta_abs"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")
        merged["_delta_pct"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")

# --- Decide which column the map will actually show ---
map_mode = st.session_state.get("map_mode", "Absolute value")
map_value_col = metric_col  # default: absolute values

if map_mode == "Change from 1990-2010 baseline":
    if baseline_col and (baseline_col in merged.columns):
        map_value_col = "_delta_abs"
    else:
        st.warning(
            "Baseline (historical 1990-2010) column not found for this metric/stat; "
            "showing absolute values instead."
        )
        map_mode = "Absolute value"
        st.session_state["map_mode"] = map_mode
        map_value_col = metric_col

# --- Compute rank/percentile/risk class per state for tooltip quick-glance ---
with perf_section("map: compute rank + risk class"):
    state_series = merged.get("state_name")
    if state_series is None:
        state_series = pd.Series(["Unknown"] * len(merged), index=merged.index)
    state_series = state_series.astype(str).fillna("Unknown")

    # In block mode, rank/percentile should be computed within the parent district
    # (state|district) so the tooltip + risk quick-glance remain meaningful.
    rank_scope_label = "state"
    if _admin_level == "block" and "district_name" in merged.columns:
        district_series = merged["district_name"].astype(str).fillna("Unknown")
        group_key = state_series.map(alias) + "|" + district_series.map(alias)
        rank_scope_label = "district"
    else:
        group_key = state_series

    # Rank is computed on the *current* value (absolute), regardless of map mode.
    # Rank 1 = highest value within the grouping scope.
    v = merged["_current_value"]
    merged["_rank_in_state"] = v.groupby(group_key).rank(method="min", ascending=False)

    # Percentile: higher values -> higher percentile (0..100)
    merged["_percentile_state"] = v.groupby(group_key).rank(pct=True, ascending=True) * 100.0

    def _risk_label(p: float) -> str:
        try:
            if pd.isna(p):
                return "Unknown"
            return str(risk_class_from_percentile(float(p)))
        except Exception:
            return "Unknown"

    merged["_risk_class"] = merged["_percentile_state"].apply(_risk_label)

# --- Human-friendly tooltip strings (avoid raw NaN/long floats) ---
def _fmt_number(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return "—"
    try:
        xf = float(x)
    except Exception:
        return "—"
    if abs(xf - round(xf)) < 1e-9 and abs(xf) < 1e9:
        return f"{int(round(xf)):,}"
    return f"{xf:,.2f}"

with perf_section("map: build tooltip strings"):
    # Main value shown depends on map mode
    if map_mode == "Change from 1990-2010 baseline":
        merged["_tooltip_value"] = merged["_delta_abs"].apply(_fmt_number)
        merged["_tooltip_value_label"] = "Δ vs 1990–2010"
    else:
        merged["_tooltip_value"] = merged["_current_value"].apply(_fmt_number)
        merged["_tooltip_value_label"] = "Value"

    merged["_tooltip_baseline"] = merged["_baseline_value"].apply(_fmt_number)
    merged["_tooltip_delta"] = merged["_delta_abs"].apply(_fmt_number)

    def _fmt_rank(r) -> str:
        if r is None or pd.isna(r):
            return "—"
        try:
            return str(int(round(float(r))))
        except Exception:
            return "—"

    merged["_tooltip_rank"] = merged["_rank_in_state"].apply(_fmt_rank)

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
    cmap_name = "RdBu_r"  # blue-negative, red-positive
    pretty_metric_label = (
        f"Δ {VARIABLES[VARIABLE_SLUG]['label']} vs 1990–2010 · "
        f"{sel_scenario} · {period_display_label(sel_period)} · {sel_stat}"
    )
else:
    cmap_name = "Reds"
    pretty_metric_label = (
        f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario} · {period_display_label(sel_period)} · {sel_stat}"
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

_admin_level = st.session_state.get("admin_level", "district")
_unit_col = "block_name" if _admin_level == "block" else "district_name"

table_df, has_baseline = _build_rankings_table_df(
    merged,
    metric_col=metric_col,
    baseline_col=baseline_col,
    selected_state=selected_state,
    risk_class_from_percentile=risk_class_from_percentile,
    district_col=_unit_col,
    state_col="state_name",
    aspirational_col="aspirational",
)

perf_end("rank_table: build", _t_rank)

_t_disp = perf_start("map: filter display_gdf")

display_gdf = merged
if selected_state != "All":
    state_mask = display_gdf["state_name"].astype(str).str.strip() == selected_state
    if not state_mask.any():
        # Fallback to case-insensitive contains
        state_mask = (
            display_gdf["state_name"]
            .astype(str)
            .str.contains(selected_state, case=False, na=False)
        )
    display_gdf = display_gdf[state_mask]

if selected_district != "All":
    display_gdf = display_gdf[
        display_gdf["district_name"].astype(str) == selected_district
    ]

perf_end("map: filter display_gdf", _t_disp)

m = folium.Map(
    location=st.session_state["map_center"],
    zoom_start=st.session_state["map_zoom"],
    tiles="CartoDB positron",
    control_scale=False,      # Disable scale control - minor speedup
    min_zoom=4,
    max_zoom=12,
    prefer_canvas=True,       # Already good - uses Canvas instead of SVG
    zoom_control=True,
    dragging=True,
    scrollWheelZoom=True,
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

# Hover settings (tooltip is built AFTER fc properties are finalized)
hover_enabled = st.session_state.get("hover_enabled", True)
tooltip = None

# -------------------------
# Step 5: GeoJSON-by-state cache (geometry cached; properties patched per rerun)
# -------------------------
if _admin_level == "block":
    adm3_mtime = float(ADM3_GEOJSON.stat().st_mtime)
    geojson_by_state = build_adm3_geojson_by_state(
        path=str(ADM3_GEOJSON),
        tolerance=SIMPLIFY_TOL_ADM2,
        mtime=adm3_mtime,
    )
else:
    adm2_mtime = float(ADM2_GEOJSON.stat().st_mtime)
    geojson_by_state = build_adm2_geojson_by_state(
        path=str(ADM2_GEOJSON),
        tolerance=SIMPLIFY_TOL_ADM2,
        mtime=adm2_mtime,
    )

state_key = "all" if selected_state == "All" else (normalize_name(selected_state) or "unknown")

# Legacy contract: callers expect geojson_by_state["all"] to exist as a fallback.
# Some implementations return only per-state FeatureCollections, so we synthesize "all" once.
if "all" not in geojson_by_state:
    all_features: list[dict] = []
    for k in sorted(geojson_by_state.keys()):
        _fc = geojson_by_state.get(k) or {}
        all_features.extend(_fc.get("features", []) or [])
    geojson_by_state = dict(geojson_by_state)
    geojson_by_state["all"] = {"type": "FeatureCollection", "features": all_features}

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

is_block_level = _admin_level == "block"
feature_key_col = "__bkey" if is_block_level else "__key"

# Ensure identifier columns exist
if is_block_level:
    if "block_name" not in prop_work.columns and "block" in prop_work.columns:
        prop_work["block_name"] = prop_work["block"]

    if feature_key_col not in prop_work.columns:
        def _mk_bkey_row(r) -> str:
            return f"{alias(r.get('state_name', ''))}|{alias(r.get('district_name', ''))}|{alias(r.get('block_name', ''))}"

        prop_work[feature_key_col] = prop_work.apply(_mk_bkey_row, axis=1)
else:
    if feature_key_col not in prop_work.columns:
        prop_work[feature_key_col] = prop_work["district_name"].map(alias)

# Numeric columns we want available on every feature (even if None)
value_cols: list[str] = []
for _c in (
    metric_col,
    map_value_col,
    "_baseline_value",
    "_delta_abs",
    "_delta_pct",
    "_rank_in_state",
    "_percentile_state",
):
    if _c and (_c not in value_cols) and (_c in prop_work.columns):
        value_cols.append(_c)

# Text tooltip fields we want available on every feature
text_cols: list[str] = []
for _c in ("_risk_class", "_tooltip_value", "_tooltip_baseline", "_tooltip_delta", "_tooltip_rank"):
    if _c in prop_work.columns:
        text_cols.append(_c)

keep_cols: list[str] = []
if is_block_level:
    if "block_name" in prop_work.columns:
        keep_cols.append("block_name")
keep_cols.append("district_name")
if "state_name" in prop_work.columns:
    keep_cols.append("state_name")
keep_cols.append(feature_key_col)

if "fillColor" in prop_work.columns:
    keep_cols.append("fillColor")
keep_cols.extend(value_cols)
keep_cols.extend(text_cols)

prop_work = prop_work[keep_cols].copy()

props_map: dict[str, dict] = {}
for _, r in prop_work.iterrows():
    k = r.get(feature_key_col)
    if not isinstance(k, str) or not k:
        continue

    upd: dict = {
        "district_name": r.get("district_name"),
        "state_name": r.get("state_name") if "state_name" in prop_work.columns else None,
    }
    if is_block_level and "block_name" in prop_work.columns:
        upd["block_name"] = r.get("block_name")

    fill = r.get("fillColor")
    upd["fillColor"] = fill if isinstance(fill, str) and fill else "#cccccc"

    for c in value_cols:
        v = r.get(c)
        upd[c] = None if pd.isna(v) else v

    for c in text_cols:
        v = r.get(c)
        upd[c] = None if pd.isna(v) else v

    props_map[k] = upd

# Patch feature properties (fillColor + value columns) from the current display_gdf/merged
for feat in fc.get("features", []):
    props = feat.get("properties") or {}

    k = props.get(feature_key_col)
    if not isinstance(k, str) or not k:
        if is_block_level:
            props["block_name"] = props.get("block_name") or props.get("block") or props.get("adm3_name") or props.get("name")
            props["district_name"] = props.get("district_name") or props.get("district") or props.get("adm2_name") or props.get("shapeName_2") or props.get("shapeName_1")
            props["state_name"] = props.get("state_name") or props.get("state") or props.get("adm1_name") or props.get("shapeName_0") or props.get("shapeGroup")
            k = f"{alias(props.get('state_name', ''))}|{alias(props.get('district_name', ''))}|{alias(props.get('block_name', ''))}"
        else:
            k = alias(props.get("district_name", ""))

        props[feature_key_col] = k

    upd = props_map.get(k)
    if upd:
        props.update(upd)
    else:
        props.setdefault("fillColor", "#cccccc")

    # Tooltip text fields
    for c in ("_risk_class", "_tooltip_value", "_tooltip_baseline", "_tooltip_delta", "_tooltip_rank"):
        props.setdefault(c, None)

    feat["properties"] = props

# Build tooltip now that fc properties are finalized (unit/state + selection context)
highlight_fn = None
tooltip = None
layer_name = "Blocks" if is_block_level else "Districts"

if hover_enabled:
    # Main label depends on map mode (absolute vs baseline change)
    main_label = "Δ vs 1990–2010" if map_mode == "Change from 1990-2010 baseline" else "Value"

    if is_block_level:
        tooltip_fields = ["block_name", "district_name", "state_name", "_tooltip_value"]
        tooltip_aliases = ["Block", "District", "State", main_label]
    else:
        tooltip_fields = ["district_name", "state_name", "_tooltip_value"]
        tooltip_aliases = ["District", "State", main_label]

    # Show baseline + delta if baseline exists in this dataset
    if baseline_col and (baseline_col in merged.columns):
        tooltip_fields += ["_tooltip_baseline", "_tooltip_delta"]
        tooltip_aliases += ["Baseline (1990–2010)", "Δ vs baseline"]

    # Risk/rank quick glance (rank scope is state for ADM2; district for ADM3)
    tooltip_fields += ["_risk_class", "_tooltip_rank"]
    tooltip_aliases += ["Risk class", f"Rank in {rank_scope_label}"]

    tooltip = folium.features.GeoJsonTooltip(
        fields=tooltip_fields,
        aliases=tooltip_aliases,
        localize=True,
        sticky=True,
    )

    highlight_fn = lambda f: {
        "fillColor": "#ffff00",
        "color": "#000",
        "weight": 2,
        "fillOpacity": 0.9,
    }

_t_geojson = perf_start("map: GeoJSON serialize+add layer")
folium.GeoJson(
    data=fc,
    name=layer_name,
    style_function=style_fn,
    tooltip=tooltip,
    highlight_function=highlight_fn,
    smooth_factor=1.5,        # Increased from 0.8 - reduces polygon vertices for faster render
    zoom_on_click=False,
    bubblingMouseEvents=False,  # Prevents event propagation overhead
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
            selected_block=selected_block,
            map_width=MAP_WIDTH,
            map_height=MAP_HEIGHT,
            perf_section=perf_section,
            level=_admin_level,
        )

        # Show add-to-portfolio button when a unit is clicked in portfolio mode
        if "Multi" in analysis_mode:
            from india_resilience_tool.app.views.map_view import render_unit_add_to_portfolio
            
            # Get clicked block from session state (set by render_map_view in block mode)
            clicked_block = st.session_state.get("clicked_block") if _admin_level == "block" else None
            
            render_unit_add_to_portfolio(
                clicked_district=clicked_district,
                clicked_state=clicked_state,
                clicked_block=clicked_block,
                selected_state=selected_state,
                portfolio_add_fn=_portfolio_add,
                portfolio_remove_fn=_portfolio_remove,
                portfolio_contains_fn=_portfolio_contains,
                normalize_fn=_portfolio_normalize,
                returned=returned,
                merged=merged,
                level=_admin_level,
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
            portfolio_contains=_portfolio_contains,
            portfolio_remove=_portfolio_remove,
            level=_admin_level,
        )

# -------------------------
# Details panel (portfolio + risk cards, sparkline + comparison)
# -------------------------
with col2:

    # Reserved slot: "Selected district for portfolio" (map route) should appear ABOVE
    # the Portfolio analysis expander even though it's determined later in the script.
    portfolio_selected_slot = st.empty()

    # -------------------------
    # Multi-district/block portfolio mode: show a clean, guided right-panel flow
    # -------------------------
    analysis_mode_rhs = st.session_state.get("analysis_mode", "Single district focus")
    portfolio_route = st.session_state.get("portfolio_build_route", None)

    if "Multi" in analysis_mode_rhs:
        # ---- MULTI-UNIT PORTFOLIO PANEL (extracted to portfolio_ui.py) ----
        render_portfolio_panel(
            # State/selection context
            selected_state=selected_state,
            portfolio_route=portfolio_route,
            level=_admin_level,
            # Variable/metric context
            variables=VARIABLES,
            variable_slug=VARIABLE_SLUG,
            index_group_labels=INDEX_GROUP_LABELS,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            metric_col=metric_col,
            # Data
            merged=merged,
            adm1=adm1,
            # Config
            pilot_state=PILOT_STATE,
            data_dir=DATA_DIR,
            # Callable dependencies
            compute_state_metrics_fn=compute_state_metrics_from_merged,
            load_master_csv_fn=load_master_csv,
            normalize_master_columns_fn=normalize_master_columns,
            parse_master_schema_fn=parse_master_schema,
            resolve_metric_column_fn=resolve_metric_column,
            find_baseline_column_for_stat_fn=find_baseline_column_for_stat,
            risk_class_from_percentile_fn=risk_class_from_percentile,
            portfolio_normalize_fn=_portfolio_normalize,
            portfolio_remove_fn=_portfolio_remove,
            portfolio_remove_all_fn=_portfolio_remove_all,
            build_portfolio_multiindex_df_fn=_build_portfolio_multiindex_df_impl,
        )

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
    if "Multi" in analysis_mode and portfolio_route == "saved_points":
        # ---- POINT SELECTION PANEL (extracted to point_selection_ui.py) ----
        clear_clicked = render_point_selection_panel(
            merged=merged,
            portfolio_add_fn=_portfolio_add,
            portfolio_key_fn=_portfolio_key,
            portfolio_set_flash_fn=_portfolio_set_flash,
            level=_admin_level,
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

    if "Multi" in analysis_mode and portfolio_route == "saved_points":
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

        # In block mode, also filter by selected_block
        if _admin_level == "block" and st.session_state.get("selected_block", "All") != "All":
            sel_block_raw = st.session_state.get("selected_block", "All")
            sel_block_norm = str(sel_block_raw).split(",")[0].strip().lower()
            
            if "block_name" in merged.columns:
                block_series = merged["block_name"].astype(str).str.strip().str.lower()
                block_mask = block_series == sel_block_norm
                if not block_mask.any() and sel_block_norm:
                    block_mask = block_series.str.contains(re.escape(sel_block_norm), na=False)
                mask = mask & block_mask

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

    # ----------- STATE/DISTRICT SUMMARY MODE (no unit selected) -----------
    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    # Determine if we should show state/district summary
    # In block mode: show district summary when block is "All" but district is selected
    # In district mode: show state summary when district is "All"
    show_summary = False
    summary_context = None
    
    if _admin_level == "block":
        # Block mode: show district summary (block distribution) when district selected but block is All
        if selected_district != "All" and selected_block == "All":
            show_summary = True
            summary_context = "district"  # Show district summary with block distribution
        elif selected_district == "All" and selected_state != "All":
            show_summary = True
            summary_context = "state"  # Show state summary with district distribution
    else:
        # District mode: show state summary when district is All
        if selected_district == "All" and selected_state != "All":
            show_summary = True
            summary_context = "state"

    if (matched_row is None or matched_row.empty) and show_summary:
        if "Multi" in analysis_mode:
            # In portfolio mode, we suppress the large summary panel here.
            # Portfolio results should be driven by the Portfolio analysis panel.
            pass
        else:
            ensemble, per_model_df, sel_districts_gdf = compute_state_metrics_from_merged(
                merged, adm1, metric_col, selected_state
            )

            # ---- STATE/DISTRICT SUMMARY VIEW (extracted to state_summary_view.py) ----
            render_state_summary_view(
                # State/selection context
                selected_state=selected_state,
                selected_district=selected_district,
                # Variable/metric context
                variables=VARIABLES,
                variable_slug=VARIABLE_SLUG,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                metric_col=metric_col,
                # Pre-computed metrics
                ensemble=ensemble,
                per_model_df=per_model_df,
                sel_districts_gdf=sel_districts_gdf,
                # Config
                processed_root=PROCESSED_ROOT,
                pilot_state=PILOT_STATE,
                # Callable dependencies
                make_state_boxplot_fn=make_state_boxplot_for_districts,
                # Block-level support
                level=_admin_level,
            )

    # ----------- UNIT DETAILS MODE (district or block) -----------
    else:
        analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
        unit_label = "block" if _admin_level == "block" else "district"

        if matched_row is None or getattr(matched_row, "empty", True):
            st.warning(f"No {unit_label}-level data found for the current selection.")
            if "Multi" in analysis_mode:
                st.info(
                    f"In portfolio mode, add {unit_label}s via **From the map**, **From saved points**, "
                    f"or **From the rankings table** (Portfolio analysis panel)."
                )
            else:
                st.info(
                    f"Please choose a different {unit_label} from the sidebar, or select **All** "
                    f"to view the {'district' if _admin_level == 'block' else 'state'} summary."
                )
            st.stop()

        row = matched_row.iloc[0]
        district_name = row.get("district_name", "Unknown")
        block_name = row.get("block_name", "Unknown") if _admin_level == "block" else None
        state_to_show = (
            st.session_state.get("selected_state")
            if st.session_state.get("selected_state") != "All"
            else (row.get("state_name") or "Unknown")
        )

        # --- Compact selection view in Multi-unit portfolio mode ---
        if "Multi" in analysis_mode:
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

        # --- Full unit climate profile (single-district/block focus mode) ---
        if _admin_level == "block" and block_name:
            # Block mode: show block name as main header
            st.subheader(block_name)
            st.markdown(f"**District:** {district_name}")
            st.markdown(f"**State:** {state_to_show}")
        else:
            # District mode: show district name as main header
            st.subheader(district_name)
            st.markdown(f"**State:** {state_to_show}")

        # If this view was triggered by a point query, show the coordinates used.
        if click_coords is not None:
            unit_label_display = "block" if _admin_level == "block" else "district"
            st.caption(
                f"Point location used: lat {click_coords[0]:.4f}, "
                f"lon {click_coords[1]:.4f} (assigned to this {unit_label_display})."
            )

        # --- Portfolio add button (for multi-unit analysis) ---
        if "Multi" in analysis_mode:
            unit_label_btn = "block" if _admin_level == "block" else "district"
            display_name = block_name if _admin_level == "block" else district_name
            
            if st.button(
                f"➕ Add this {unit_label_btn} to portfolio",
                key=f"btn_add_portfolio_single_{state_to_show}_{district_name}_{block_name or 'na'}",
            ):
                _portfolio_add(state_to_show, district_name, block_name)
                st.success(f"Added {display_name}, {state_to_show} to portfolio.")

            # Always show current portfolio below the button
            portfolio_key = "portfolio_blocks" if _admin_level == "block" else "portfolio_districts"
            portfolio_current = st.session_state.get(portfolio_key, [])
            if portfolio_current:
                unit_label_plural = "blocks" if _admin_level == "block" else "districts"
                st.markdown(f"**Current portfolio ({unit_label_plural})**")
                try:
                    if isinstance(portfolio_current[0], dict):
                        if _admin_level == "block":
                            port_df = (
                                pd.DataFrame(portfolio_current)
                                .rename(columns={"state": "State", "district": "District", "block": "Block"})
                            )
                        else:
                            port_df = (
                                pd.DataFrame(portfolio_current)
                                .rename(columns={"state": "State", "district": "District"})
                            )
                    else:
                        port_df = pd.DataFrame(portfolio_current)
                except Exception:
                    port_df = pd.DataFrame()

                st.dataframe(
                    port_df,
                    use_container_width=True,
                )
            else:
                unit_label_plural = "blocks" if _admin_level == "block" else "districts"
                st.caption(
                    f"No {unit_label_plural} in the portfolio yet. "
                    f"Use the button above or the Rankings table to add {unit_label_plural}."
                )

        # ---- Risk cards (1.1) ----
        current_val = row.get(metric_col)
        current_val_f = float(current_val) if not pd.isna(current_val) else None

        # baseline: same metric, historical, baseline period
        baseline_col = find_baseline_column(df.columns, sel_metric)
        baseline_val = row.get(baseline_col) if baseline_col else np.nan
        baseline_val_f = float(baseline_val) if not pd.isna(baseline_val) else None

        # position within parent unit: rank + percentile
        # For districts: within state
        # For blocks: within district
        percentile_in_parent = None
        rank_in_parent = None
        n_in_parent = None
        parent_label = "district" if _admin_level == "block" else "state"
        
        try:
            if _admin_level == "block":
                # Block mode: rank within district
                in_parent_mask = (
                    (merged["state_name"].astype(str).str.strip().str.lower() == str(state_to_show).strip().lower())
                    & (merged["district_name"].astype(str).str.strip().str.lower() == str(district_name).strip().lower())
                )
            else:
                # District mode: rank within state
                in_parent_mask = (
                    merged["state_name"].astype(str).str.strip().str.lower()
                    == str(state_to_show).strip().lower()
                )
            
            parent_vals = pd.to_numeric(
                merged.loc[in_parent_mask, metric_col], errors="coerce"
            ).dropna()

            if current_val_f is not None and not parent_vals.empty:
                n_in_parent = int(len(parent_vals))
                # percentile: fraction of units with lower value
                percentile_in_parent = float(
                    (parent_vals < current_val_f).sum() / n_in_parent * 100.0
                )
                # rank: 1 = highest value (most extreme / highest risk)
                rank_in_parent = int((parent_vals > current_val_f).sum() + 1)
        except Exception:
            pass
        
        # Backward compatibility aliases
        percentile_state = percentile_in_parent
        rank_in_state = rank_in_parent
        n_in_state = n_in_parent

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

        @st.cache_data
        def _load_block_yearly(
            ts_root: Path,
            state_dir: str,
            district_display: str,
            block_display: str,
            scenario_name: str,
            varcfg: dict,
            aliases: dict | None = None,
        ) -> pd.DataFrame:
            """
            Load the *scenario-specific* yearly ensemble CSV for a block.

            Delegates to india_resilience_tool.analysis.timeseries for robust discovery.
            """
            from india_resilience_tool.analysis.timeseries import load_block_yearly

            return load_block_yearly(
                ts_root=ts_root,
                state_dir=state_dir,
                district_display=district_display,
                block_display=block_display,
                scenario_name=scenario_name,
                varcfg=varcfg,
                aliases=aliases,
                normalize_fn=alias,
            )

        def _filter_series_for_trend(
            df: pd.DataFrame,
            state_name: str,
            district_name: str,
            block_name: Optional[str] = None,
        ) -> pd.DataFrame:
            """
            Extract a clean time series for a single unit from a
            scenario-specific yearly dataframe.

            In district mode: filters to (state, district)
            In block mode:    filters to (state, district, block) when block_name is provided
            """
            if df is None or df.empty:
                return pd.DataFrame()

            d = df.copy()

            # Normalize id columns (tolerate 'block_name' vs 'block')
            if "block_name" in d.columns and "block" not in d.columns:
                d["block"] = d["block_name"]

            def _n(s: str) -> str:
                return alias(s)

            if "state" in d.columns:
                d["_state_key"] = d["state"].astype(str).map(_n)
            else:
                d["_state_key"] = pd.Series([""] * len(d), index=d.index)

            if "district" in d.columns:
                d["_district_key"] = d["district"].astype(str).map(_n)
            else:
                d["_district_key"] = pd.Series([""] * len(d), index=d.index)

            mask = (d["_state_key"] == _n(state_name)) & (d["_district_key"] == _n(district_name))

            # Optional block filter when present + requested
            if block_name and ("block" in d.columns):
                d["_block_key"] = d["block"].astype(str).map(_n)
                mask = mask & (d["_block_key"] == _n(block_name))

            if not mask.any():
                # Soft fallback: allow partial matches on district (and block if provided)
                mask = (d["_state_key"] == _n(state_name)) & d["_district_key"].str.contains(_n(district_name), na=False)
                if block_name and ("block" in d.columns):
                    d["_block_key"] = d["block"].astype(str).map(_n)
                    mask = mask & d["_block_key"].str.contains(_n(block_name), na=False)

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
                logo_path=LOGO_PATH,
            )

        # --- Load historical + selected scenario series separately ---
        requested_state_dir = (
            selected_state
            if selected_state != "All"
            else (row.get("state_name") or PILOT_STATE)
        )
        state_dir_for_fs = requested_state_dir
        district_for_fs = row.get("district_name") or selected_district

        block_for_fs = row.get("block_name") or selected_block

        if _admin_level == "block" and selected_block != "All":
            # Historical (1990–2010) - block level
            _district_yearly_hist = _load_block_yearly(
                ts_root=PROCESSED_ROOT,
                state_dir=str(state_dir_for_fs),
                district_display=str(district_for_fs),
                block_display=str(block_for_fs),
                scenario_name="historical",
                varcfg=VARCFG,
                aliases=NAME_ALIASES,
            )

            # Selected SSP scenario (2020–2060) - block level
            _district_yearly_scen = _load_block_yearly(
                ts_root=PROCESSED_ROOT,
                state_dir=str(state_dir_for_fs),
                district_display=str(district_for_fs),
                block_display=str(block_for_fs),
                scenario_name=sel_scenario,
                varcfg=VARCFG,
                aliases=NAME_ALIASES,
            )

            # Prepare time series for the details panel (block filter)
            hist_ts = _filter_series_for_trend(_district_yearly_hist, state_to_show, district_name, str(block_for_fs))
            scen_ts = _filter_series_for_trend(_district_yearly_scen, state_to_show, district_name, str(block_for_fs))
        else:
            # Historical (1990–2010) - district level
            _district_yearly_hist = _load_district_yearly(
                ts_root=PROCESSED_ROOT,
                state_dir=str(state_dir_for_fs),
                district_display=str(district_for_fs),
                scenario_name="historical",
                varcfg=VARCFG,
                aliases=NAME_ALIASES,
            )

            # Selected SSP scenario (2020–2060) - district level
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

        from india_resilience_tool.viz.style import ensure_16x9_figsize

        _fig_size_panel = ensure_16x9_figsize(FIG_SIZE_PANEL, mode="fit_width")

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
            fig_size_panel=_fig_size_panel,
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
            logo_path=LOGO_PATH,
        )

render_perf_panel_safe()
st.markdown("---")
st.caption(
    "Notes: first choose a Risk domain (e.g. Heat Risk, Drought Risk), then a Metric within that bundle. "
    "Details panel shows risk cards, trends, narrative, and a comparison option."
)