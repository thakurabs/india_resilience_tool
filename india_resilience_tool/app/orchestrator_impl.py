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
from shapely.ops import transform

from india_resilience_tool.data.adm3_loader import get_blocks_for_district as _get_blocks_for_district

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
)

from india_resilience_tool.app.state import VIEW_MAP, VIEW_RANKINGS

from india_resilience_tool.app.geo_cache import (
    build_adm1_from_adm2,
    enrich_adm2_with_state_names,
    list_available_states_from_processed_root_cached,
    load_local_adm2,
    load_local_adm3,
)
from india_resilience_tool.app.sidebar_branding import render_sidebar_branding
from india_resilience_tool.app.ribbon import render_metric_ribbon
from india_resilience_tool.app.geography_controls import render_geography_and_analysis_focus

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
from india_resilience_tool.app.perf import (
    perf_reset,
    perf_start,
    perf_end,
    perf_section,
    render_perf_panel_safe,
)

# -------------------------
# CONFIG
# -------------------------
from paths import DATA_DIR, DISTRICTS_PATH, BLOCKS_PATH, resolve_processed_root

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM3,
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
    SCENARIO_UI_LABEL,
)

from india_resilience_tool.config.variables import (
    VARIABLES,
    INDEX_GROUP_LABELS,
)

# Data paths derived from DATA_DIR
ADM2_GEOJSON = DISTRICTS_PATH
ADM3_GEOJSON = BLOCKS_PATH

ATTACH_DISTRICT_GEOJSON = str(ADM2_GEOJSON) if ADM2_GEOJSON.exists() else None
ATTACH_BLOCK_GEOJSON = str(ADM3_GEOJSON) if ADM3_GEOJSON.exists() else None


# ---------- Name normalization / aliases ----------
from india_resilience_tool.utils.naming import NAME_ALIASES, alias, normalize_name, normalize_compact

# -------------------------
# Geo load / prep
# -------------------------
# Streamlit-cached geo helpers live in india_resilience_tool.app.geo_cache

if not ADM2_GEOJSON.exists():
    st.set_page_config(page_title="India Resilience Tool", layout="wide")
    st.error(f"ADM2 geojson not found at {ADM2_GEOJSON}. Place your districts_4326.geojson at this path.")
    st.stop()

adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)
adm2["__key"] = adm2["district_name"].map(alias)

# -------------------------
# Color helpers (no GeoJSON round-trip)
# -------------------------
from india_resilience_tool.viz.colors import (
    apply_fillcolor_binned,
    build_vertical_binned_legend_block_html,
    compute_robust_range,
    get_cmap_hex_list as _get_cmap_hex_list,
)

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


def state_profile_files_missing(processed_root: Path, state: str, level: str) -> bool:
    """Return True when required level-specific state profile files are missing."""
    level_norm = str(level or "district").strip().lower()
    state_root = Path(processed_root) / str(state)
    required = [
        state_root / f"state_yearly_ensemble_stats_{level_norm}.csv",
        state_root / f"state_ensemble_stats_{level_norm}.csv",
    ]
    return any(not p.exists() for p in required)

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
    make_scenario_comparison_figure_plotly,
)


def _make_scenario_comparison_figure_dashboard(**kwargs):
    """Return Plotly scenario comparison when available; fall back to Matplotlib.

    The dashboard uses Plotly for visual consistency with the Plotly trend figure.
    If Plotly is unavailable (or the Plotly builder fails), we fall back to the
    Matplotlib version to avoid breaking the UI.

    Args:
        **kwargs: Passed through to the underlying figure builders. Expected keys
            match the signature used by `render_scenario_comparison()` in
            `app/views/details_panel.py`.

    Returns:
        - Plotly Figure when possible
        - Otherwise the (fig, ax) tuple returned by the Matplotlib builder
    """
    # Prefer Plotly (cleaner titles, consistent fonts, year-only x-axis labels)
    try:
        fig = make_scenario_comparison_figure_plotly(render_context="dashboard", **kwargs)
        if fig is not None:
            return fig
    except Exception:
        pass

    # Fallback: Matplotlib chart
    return make_scenario_comparison_figure(render_context="dashboard", **kwargs)


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
    scen_key = str(sel_scenario).strip().lower()
    scen_label = SCENARIO_UI_LABEL.get(scen_key, str(sel_scenario))
    ax.set_title(
        f"{sel_state}: {metric_label}\nScenario: {scen_label} · "
        f"Period: {sel_period} · Stat: {sel_stat}"
    )
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()

    return fig

# -------------------------
# Risk class helper (percentile → label)
# -------------------------
from india_resilience_tool.analysis.metrics import risk_class_from_percentile
from india_resilience_tool.analysis.map_enrichment import (
    add_current_baseline_delta,
    add_rank_percentile_risk,
    add_tooltip_strings,
)

# -------------------------
# APP START
# -------------------------
st.set_page_config(page_title="India Resilience Tool", layout="wide")

# Selection placeholders (force deliberate choices)
SEL_PLACEHOLDER = "— Select —"

# Initialise analysis mode and portfolio storage in session state
if "analysis_mode" not in st.session_state:
    st.session_state["analysis_mode"] = SEL_PLACEHOLDER
if "map_mode" not in st.session_state:
    st.session_state["map_mode"] = SEL_PLACEHOLDER

if "portfolio_districts" not in st.session_state:
    # Will store a list of (state_name, district_name) tuples
    st.session_state["portfolio_districts"] = []

# Portfolio-build UX router state (multi-district portfolio mode)
st.session_state.setdefault("portfolio_build_route", None)  # None | "rankings" | "map" | "saved_points"
st.session_state.setdefault("jump_to_rankings", False)
st.session_state.setdefault("jump_to_map", False)
st.session_state.setdefault("_analysis_mode_prev", st.session_state.get("analysis_mode", SEL_PLACEHOLDER))

# Which main view is active in the left column: map vs rankings
if "active_view" not in st.session_state:
    st.session_state["active_view"] = VIEW_MAP

# Perf timing toggle (developer)
st.session_state.setdefault("perf_enabled", DEBUG)
perf_reset()

# If a downstream control requested to jump to a specific left-panel view,
# honour it BEFORE the main_view_selector radio is created.
apply_jump_once_flags()


with st.sidebar:
    render_sidebar_branding(logo_path=LOGO_PATH)

    # Admin level selector (District vs Block)
    admin_level = render_admin_level_selector(
        label_visibility="collapsed",
        centered=True,
        center_layout=(1, 8, 1),
    )

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

# -------------------------
# Main layout: left (map/rankings) + right (details)
# -------------------------
col1, col2 = st.columns([5, 3])

# -------------------------
# Metric selection ribbon (bundle → metric → scenario/period/stat + map mode)
# -------------------------
ribbon_ctx = render_metric_ribbon(
    col=col1,
    sel_placeholder=SEL_PLACEHOLDER,
    data_dir=DATA_DIR,
    pilot_state=PILOT_STATE,
    resolve_processed_root_fn=resolve_processed_root,
    attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON,
    master_needs_rebuild_fn=master_needs_rebuild,
    state_profile_files_missing_fn=state_profile_files_missing,
    perf_section=perf_section,
    render_perf_panel_safe=render_perf_panel_safe,
)

# Unpack context into legacy variable names expected by downstream code.
VARIABLE_SLUG = ribbon_ctx.variable_slug
VARCFG = ribbon_ctx.varcfg
PROCESSED_ROOT = ribbon_ctx.processed_root
MASTER_CSV_PATH = ribbon_ctx.master_csv_path
df = ribbon_ctx.df
schema_items = ribbon_ctx.schema_items
metrics = ribbon_ctx.metrics
by_metric = ribbon_ctx.by_metric
registry_metric = ribbon_ctx.registry_metric
sel_metric = str(st.session_state.get("registry_metric", registry_metric) or "").strip()
sel_scenario = ribbon_ctx.sel_scenario
sel_scenario_display = ribbon_ctx.sel_scenario_display
sel_period = ribbon_ctx.sel_period
sel_stat = ribbon_ctx.sel_stat
map_mode = ribbon_ctx.map_mode
metric_col = ribbon_ctx.metric_col
_ribbon_ready = ribbon_ctx.ribbon_ready
pretty_metric_label = ribbon_ctx.pretty_metric_label
rebuild_master_csv_if_needed = ribbon_ctx.rebuild_master_csv_if_needed
_load_master_and_schema = ribbon_ctx.load_master_and_schema_fn
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

geo_ctx = render_geography_and_analysis_focus(
    state_placeholder=state_placeholder,
    admin_level=admin_level,
    processed_root=PROCESSED_ROOT,
    sel_placeholder=SEL_PLACEHOLDER,
    view_map=VIEW_MAP,
    view_rankings=VIEW_RANKINGS,
    adm1=adm1,
    adm2=adm2,
    adm3_geojson=ADM3_GEOJSON,
    simplify_tol_adm3=SIMPLIFY_TOL_ADM3,
)

analysis_mode = geo_ctx.analysis_mode
analysis_ready = geo_ctx.analysis_ready
selected_state = geo_ctx.selected_state
selected_district = geo_ctx.selected_district
selected_block = geo_ctx.selected_block
gdf_state_districts = geo_ctx.gdf_state_districts

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


# Gate map rendering until both the ribbon selections and Analysis focus are chosen
_ready_for_map = bool(_ribbon_ready) and bool(analysis_ready)
if not _ready_for_map:
    with col1:
        st.info(
            "Complete the selections in the **ribbon above the map** (Risk domain, Metric, Scenario, Period, Statistic, Map mode) "
            "and choose an **Analysis focus** in the sidebar to render the map."
        )
    render_perf_panel_safe()
    st.stop()

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

    adm3 = load_local_adm3(str(ADM3_GEOJSON), tolerance=SIMPLIFY_TOL_ADM3)
else:
    adm3 = None

# Require deliberate Analysis focus + Map mode selection before building the map
_analysis_mode = st.session_state.get("analysis_mode", SEL_PLACEHOLDER)
_map_mode = st.session_state.get("map_mode", SEL_PLACEHOLDER)
modes_ready = (_analysis_mode != SEL_PLACEHOLDER) and (_map_mode != SEL_PLACEHOLDER)
if not modes_ready:
    st.info("Select an **Analysis focus** in the sidebar and complete the **ribbon selections** above the map to render the map.")
    render_perf_panel_safe()
    st.stop()

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
        st.sidebar.write("block_name column exists: OK")
        st.sidebar.write(f"Sample blocks: {merged['block_name'].head(3).tolist()}")
    else:
        st.sidebar.write("block_name column missing")
    if metric_col in merged.columns:
        non_null = merged[metric_col].notna().sum()
        st.sidebar.write(f"metric_col '{metric_col}' has {non_null} non-null values")

# --- Compute current/baseline/delta columns once (used by map + tooltip) ---
with perf_section("map: compute current/baseline/delta"):
    merged = add_current_baseline_delta(
        merged,
        metric_col=metric_col,
        baseline_col=baseline_col,
    )

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
    rank_higher_is_worse = bool(VARCFG.get("rank_higher_is_worse", True))
    merged, rank_scope_label = add_rank_percentile_risk(
        merged,
        admin_level=_admin_level,
        rank_higher_is_worse=rank_higher_is_worse,
        alias_fn=alias,
        risk_class_from_percentile_fn=risk_class_from_percentile,
    )

with perf_section("map: build tooltip strings"):
    merged = add_tooltip_strings(merged, map_mode=map_mode)

# Compute color scale defaults from *visible* units (matches the map filter),
# then default to a robust p2–p98 range so outliers don't collapse the palette.
scale_gdf = merged
if selected_state != "All" and "state_name" in scale_gdf.columns:
    state_mask = scale_gdf["state_name"].astype(str).str.strip() == selected_state
    if not state_mask.any():
        # Fallback to case-insensitive contains (tolerate naming/whitespace differences)
        state_mask = (
            scale_gdf["state_name"]
            .astype(str)
            .str.contains(selected_state, case=False, na=False)
        )
    scale_gdf = scale_gdf[state_mask]

if selected_district != "All" and "district_name" in scale_gdf.columns:
    scale_gdf = scale_gdf[scale_gdf["district_name"].astype(str) == selected_district]

if _admin_level == "block" and selected_block != "All" and "block_name" in scale_gdf.columns:
    scale_gdf = scale_gdf[scale_gdf["block_name"].astype(str) == selected_block]

scale_vals = pd.to_numeric(
    scale_gdf.get(map_value_col, pd.Series([], dtype=float)), errors="coerce"
)
scale_vals = scale_vals.replace([np.inf, -np.inf], np.nan).dropna()

if scale_vals.empty:
    st.error("No numeric values found for the current map selection.")
    render_perf_panel_safe()
    st.stop()

# Slider bounds: full min/max of visible data
data_min, data_max = float(scale_vals.min()), float(scale_vals.max())

# If there is no spread (all values identical), pad the range a bit
if data_min == data_max:
    # Use a small padding relative to the magnitude, with a sensible floor
    padding = max(abs(data_min) * 0.1, 1.0)
    data_min -= padding
    data_max += padding

# Default slider selection: robust p2–p98 of visible data (clipped to slider bounds)
vmin_default, vmax_default = compute_robust_range(scale_vals, low_pct=2.0, high_pct=98.0)
if (not np.isfinite(vmin_default)) or (not np.isfinite(vmax_default)):
    vmin_default, vmax_default = data_min, data_max

vmin_default = max(data_min, min(float(vmin_default), data_max))
vmax_default = max(data_min, min(float(vmax_default), data_max))
if vmin_default > vmax_default:
    vmin_default, vmax_default = vmax_default, vmin_default
if vmin_default == vmax_default:
    vmin_default, vmax_default = data_min, data_max

with st.sidebar:
    vmin_vmax = color_slider_placeholder.slider(
        "Color range (min → max)",
        min_value=float(data_min),
        max_value=float(data_max),
        value=(vmin_default, vmax_default),
        step=max((data_max - data_min) / 200.0, 0.001),
        key="color_range_slider",
    )

vmin, vmax = float(vmin_vmax[0]), float(vmin_vmax[1])

# Choose colormap: sequential for absolute, diverging for change
if map_mode == "Change from 1990-2010 baseline":
    cmap_name = "RdBu_r"  # blue-negative, red-positive
    pretty_metric_label = (
        f"Δ {VARIABLES[VARIABLE_SLUG]['label']} vs 1990–2010 · "
        f"{sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
    )
else:
    cmap_name = "Reds"
    pretty_metric_label = (
        f"{VARIABLES[VARIABLE_SLUG]['label']} · {sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
    )

with perf_section("colors: apply_fillcolor_binned"):
    with st.spinner("Computing colors..."):
        merged = apply_fillcolor_binned(
            merged,
            map_value_col,
            vmin,
            vmax,
            cmap_name=cmap_name,
            nlevels=15,
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

try:
    if selected_state != "All" and selected_district == "All":
        row_state = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
        if not row_state.empty:
            b = row_state.iloc[0].geometry.bounds
            st.session_state["map_center"] = [(b[1] + b[3]) / 2, (b[0] + b[2]) / 2]
            st.session_state["map_zoom"] = 7
except Exception:
    pass

# Hover settings (tooltip is built AFTER fc properties are finalized)
hover_enabled = st.session_state.get("hover_enabled", True)
_t_geojson = perf_start("map: GeoJSON serialize+add layer")
from india_resilience_tool.app.map_layer_runtime import build_folium_map_for_selection

m = build_folium_map_for_selection(
    level=_admin_level,
    merged=merged,
    display_gdf=display_gdf,
    selected_state=selected_state,
    selected_district=selected_district,
    map_mode=map_mode,
    baseline_col=baseline_col,
    rank_scope_label=rank_scope_label,
    metric_col=metric_col,
    map_value_col=map_value_col,
    alias_fn=alias,
    normalize_state_fn=normalize_name,
    adm1=adm1,
    map_center=st.session_state["map_center"],
    map_zoom=st.session_state["map_zoom"],
    bounds_latlon=[[MIN_LAT, MIN_LON], [MAX_LAT, MAX_LON]],
    hover_enabled=bool(hover_enabled),
    adm2_geojson_path=ADM2_GEOJSON,
    adm3_geojson_path=ADM3_GEOJSON,
    simplify_tolerance_adm2=SIMPLIFY_TOL_ADM2,
    simplify_tolerance_adm3=SIMPLIFY_TOL_ADM3,
)
perf_end("map: GeoJSON serialize+add layer", _t_geojson)

MAP_WIDTH, MAP_HEIGHT = 780, 700

# Build a container-relative legend block for Streamlit (stable across devices)
legend_block_html = build_vertical_binned_legend_block_html(
    pretty_metric_label=pretty_metric_label,
    vmin=vmin,
    vmax=vmax,
    cmap_name=cmap_name,
    nlevels=15,
    nticks=5,
    include_zero_tick=True,
    map_height=MAP_HEIGHT,
    bar_width_px=18,
)

from india_resilience_tool.app.left_panel_runtime import render_left_panel

# Ensure `returned` always exists, even if the Rankings view is selected.
returned, _view = render_left_panel(
    col=col1,
    m=m,
    legend_block_html=legend_block_html,
    map_mode=map_mode,
    map_width=MAP_WIDTH,
    map_height=MAP_HEIGHT,
    perf_section=perf_section,
    variable_slug=VARIABLE_SLUG,
    sel_scenario=sel_scenario,
    sel_period=sel_period,
    sel_stat=sel_stat,
    selected_state=selected_state,
    selected_district=selected_district,
    selected_block=selected_block,
    level=_admin_level,
    table_df=table_df,
    has_baseline=has_baseline,
    variables=VARIABLES,
    variable_slug_for_rankings=VARIABLE_SLUG,
    portfolio_add_fn=_portfolio_add,
    portfolio_contains_fn=_portfolio_contains,
    portfolio_remove_fn=_portfolio_remove,
    portfolio_normalize_fn=_portfolio_normalize,
    merged=merged,
)

# -------------------------
# Details panel (portfolio + risk cards, sparkline + comparison)
# -------------------------
with col2:
    from india_resilience_tool.app.details_runtime import render_right_panel

    render_right_panel(
        returned=returned,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_block=selected_block,
        admin_level=_admin_level,
        variables=VARIABLES,
        variable_slug=VARIABLE_SLUG,
        index_group_labels=INDEX_GROUP_LABELS,
        sel_metric=sel_metric,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        metric_col=metric_col,
        merged=merged,
        adm1=adm1,
        df=df,
        schema_items=schema_items,
        processed_root=PROCESSED_ROOT,
        pilot_state=PILOT_STATE,
        data_dir=DATA_DIR,
        logo_path=LOGO_PATH,
        fig_size_panel=FIG_SIZE_PANEL,
        fig_dpi_panel=FIG_DPI_PANEL,
        font_size_title=FONT_SIZE_TITLE,
        font_size_label=FONT_SIZE_LABEL,
        font_size_ticks=FONT_SIZE_TICKS,
        font_size_legend=FONT_SIZE_LEGEND,
        period_order=PERIOD_ORDER,
        scenario_display=SCENARIO_DISPLAY,
        alias_fn=alias,
        name_aliases=NAME_ALIASES,
        varcfg=VARCFG,
        portfolio_add_fn=_portfolio_add,
        portfolio_remove_fn=_portfolio_remove,
        portfolio_contains_fn=_portfolio_contains,
        portfolio_key_fn=_portfolio_key,
        portfolio_set_flash_fn=_portfolio_set_flash,
        portfolio_normalize_fn=_portfolio_normalize,
        portfolio_remove_all_fn=_portfolio_remove_all,
        build_portfolio_multiindex_df_fn=_build_portfolio_multiindex_df_impl,
        load_master_csv_fn=load_master_csv,
        normalize_master_columns_fn=normalize_master_columns,
        parse_master_schema_fn=parse_master_schema,
        resolve_metric_column_fn=resolve_metric_column,
        find_baseline_column_for_stat_fn=find_baseline_column_for_stat,
        risk_class_from_percentile_fn=risk_class_from_percentile,
        load_master_and_schema_fn=_load_master_and_schema,
        build_scenario_comparison_panel_for_row_fn=build_scenario_comparison_panel_for_row,
        make_scenario_comparison_figure_fn=_make_scenario_comparison_figure_dashboard,
    )

render_perf_panel_safe()
st.markdown("---")
st.caption(
    "Notes: first choose a Risk domain (e.g. Heat Risk, Drought Risk), then a Metric within that bundle. "
    "Details panel shows risk cards, trends, narrative, and case-study export."
)
