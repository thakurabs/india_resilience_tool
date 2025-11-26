#!/usr/bin/env python3
from __future__ import annotations
import io, os, re, json, zipfile, shutil, subprocess, unicodedata, difflib
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

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
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(r"D:\projects\irt_data\\")
DATA_DIR.mkdir(parents=True, exist_ok=True)

ADM2_GEOJSON = DATA_DIR / "districts_4326.geojson"
ATTACH_DISTRICT_GEOJSON = str(ADM2_GEOJSON) if ADM2_GEOJSON.exists() else None
OUTDIR = DATA_DIR
LOGO_PATH = "./resilience_actions_logo.png"

SIMPLIFY_TOL_ADM2 = 0.015
SIMPLIFY_TOL_ADM1 = 0.01

MIN_LON, MAX_LON = 68.0, 97.5
MIN_LAT, MAX_LAT = 5, 45.0

# ---- Variable/Index registry ----
# Each entry maps an "index slug" to:
#  - label: what the user sees in the Index dropdown
#  - periods_metric_col: the base metric name in master CSV (<metric>__<scenario>__<period>__<stat>)
#  - file patterns for district/state yearly series discovery
VARIABLES = {
    "tas_gt32": {
        "label": "Summer Days",
        "periods_metric_col": "days_gt_32C",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "rain_gt_2p5mm": {
        "label": "Rainy days (pr > 2.5 mm)",
        "periods_metric_col": "days_rain_gt_2p5mm",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_csd_gt30": {
        "label": "Consecutive Summer Days (tasmax > 30°C)",
        "periods_metric_col": "consec_summer_days_gt_30C",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_tropical_nights_gt20": {
        "label": "Tropical Nights (tasmin > 20°C)",
        "periods_metric_col": "tropical_nights_gt_20C",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwdi_tasmax_plus5C": {
        "label": "Heat Wave Duration Index (HWDI)",
        "periods_metric_col": "hwdi_max_spell_len",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwfi_tmean_90p": {
        "label": "Heat Wave Frequency Index (HWFI)",
        "periods_metric_col": "hwfi_days_in_spells",
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
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

# -------------------------
# Color precompute
# -------------------------
@st.cache_data
def precompute_fillcolor(merged_json: str, metric_col: str, vmin: float, vmax: float, cmap_name: str = "Reds") -> str:
    merged_gdf = gpd.read_file(io.StringIO(merged_json))
    try:
        cmap = mpcm.get_cmap(cmap_name)
    except Exception:
        import matplotlib as mpl
        cmap = mpl.colormaps.get_cmap(cmap_name)
    nsteps = 256
    hex_colors = [mcolors.to_hex(cmap(i / (nsteps - 1))) for i in range(nsteps)]

    def color_for_val(v):
        if pd.isna(v):
            return "#cccccc"
        try:
            t = (float(v) - vmin) / (vmax - vmin) if vmax != vmin else 0.5
            t = max(0.0, min(1.0, t))
            return hex_colors[int(t * (nsteps - 1))]
        except Exception:
            return "#cccccc"

    merged_gdf["fillColor"] = merged_gdf[metric_col].apply(color_for_val)
    merged_gdf["_metric_val"] = pd.to_numeric(merged_gdf[metric_col], errors="coerce")
    return merged_gdf.to_json()

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
def find_chrome_executable() -> Optional[str]:
    for name in ("google-chrome", "chrome", "chromium", "chromium-browser"):
        p = shutil.which(name)
        if p:
            return p
    return None

def chrome_screenshot(html_path: Path, png_out: Path, width=3000, height=2000, timeout=60) -> bool:
    exe = find_chrome_executable()
    if not exe:
        return False
    file_url = f"file://{html_path.resolve()}"
    cmd = [
        exe,
        "--headless",
        "--hide-scrollbars",
        f"--window-size={width},{height}",
        f"--screenshot={png_out}",
        file_url,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=timeout)
        return png_out.exists()
    except Exception:
        return False

def open_in_new_tab_link(file_path: Path, label: str, mime: str = "application/pdf"):
    import base64

    data = Path(file_path).read_bytes()
    b64 = base64.b64encode(data).decode("ascii")
    href = f"data:{mime};base64,{b64}"
    st.markdown(
        f'<a href="{href}" target="_blank" rel="noopener" download="{Path(file_path).name}">{label}</a>',
        unsafe_allow_html=True,
    )

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

# Normalize master columns to <metric>__<scenario>__<period>__<stat>
def normalize_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    pat = re.compile(
        rf"^(.+?)_"
        r"(historical|ssp119|ssp126|ssp245|ssp370|ssp434|ssp460|ssp585)_"
        r"(\d{4})_(\d{4})__(mean|median|std|p05|p95)$",
        re.I,
    )
    for c in df.columns:
        s = str(c)
        m = pat.match(s)
        if m:
            metric, scen, y0, y1, stat = m.groups()
            mapping[c] = f"{metric.strip()}__{scen.lower().strip()}__{y0}-{y1}__{stat.lower().strip()}"
    return df.rename(columns=mapping) if mapping else df

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
):
    """
    Build a compact bar chart:
      - 1 blue bar for Historical 1990-2010 (if available)
      - 2 bars (yellow, red) for SSP2-4.5 / SSP5-8.5 in 2020-2040
      - 2 bars (yellow, red) for SSP2-4.5 / SSP5-8.5 in 2040-2060

    Bars are grouped visually: [Historical] [2020-2040 pair] [2040-2060 pair].
    The bar matching the *current map selection* (sel_scenario, sel_period, sel_stat)
    is slightly emphasized.
    """
    import matplotlib.pyplot as plt

    if panel_df is None or panel_df.empty:
        return None, None

    # Canonicalize selection for matching
    sel_scen_norm = str(sel_scenario).strip().lower()
    sel_period_norm = canonical_period_label(sel_period)

    # Colors per scenario
    scenario_colors = {
        "historical": "tab:blue",
        "ssp245": "gold",    # yellow-like
        "ssp585": "tab:red",
    }

    # Define the exact combinations we care about for the 5-bar layout
    combos = []
    # Historical 1990-2010 as a single bar (if present)
    combos.append(("historical", "1990-2010"))
    # Futures: SSP2-4.5 & SSP5-8.5 in each future period
    for p in ["2020-2040", "2040-2060"]:
        for s in ["ssp245", "ssp585"]:
            combos.append((s, p))

    # Assign x positions with gaps: [0] [2,3] [5,6]
    x_positions = {}
    x = 0.0
    group_spacing = 1.0
    within_spacing = 1.0

    for scen, period in combos:
        if period == "1990-2010":
            x_positions[(scen, period)] = x
            x += group_spacing  # gap to next group
        elif period == "2020-2040":
            x_positions[(scen, period)] = x
            x += within_spacing
        else:  # 2040-2060 group
            # on first bar of this group, ensure we bump a bit for visual gap
            if (scen, period) == ("ssp245", "2040-2060"):
                x += group_spacing
            x_positions[(scen, period)] = x
            x += within_spacing

    # Collect actual data + positions
    xs, ys, colors, edgecolors, labels = [], [], [], [], []
    highlight_idx = []  # indices of bars that match the current selection

    for idx, (scen, period) in enumerate(combos):
        mask = (
            (panel_df["scenario"] == scen)
            & (panel_df["period"] == period)
        )
        if not mask.any():
            continue
        val = float(panel_df.loc[mask, "value"].iloc[0])
        xs.append(x_positions[(scen, period)])
        ys.append(val)
        colors.append(scenario_colors.get(scen, "grey"))
        # Basic label: for tooltip-like use if needed
        labels.append(f"{SCENARIO_DISPLAY.get(scen, scen)} {period}")

        # Emphasize the bar matching current map selection
        if (scen == sel_scen_norm) and (period == sel_period_norm):
            edgecolors.append("black")
            highlight_idx.append(len(xs) - 1)
        else:
            edgecolors.append("none")

    if not xs:
        return None, None

    # Compute x-ticks to "group" periods visually
    # We know positions: 0 ~ historical, around 2-3 ~ 2020-2040, 5-6 ~ 2040-2060
    # We'll derive ticks by the midpoints of each period group actually present.
    period_to_x = {}
    for (scen, period), xp in x_positions.items():
        if period not in period_to_x:
            period_to_x[period] = []
        if (scen, period) in [k for k in zip(panel_df["scenario"], panel_df["period"])]:
            period_to_x[period].append(xp)

    xticks = []
    xticklabels = []
    for p in PERIOD_ORDER:
        xs_p = [x_positions[(s, p)] for s in ["historical", "ssp245", "ssp585"]
                if (s, p) in x_positions and (panel_df["scenario"] == s).any()]
        if not xs_p:
            continue
        xticks.append(sum(xs_p) / len(xs_p))
        if p == "1990-2010":
            xticklabels.append("Historical\n1990–2010")
        else:
            xticklabels.append(p.replace("-", "–"))

    fig, ax = plt.subplots(figsize=(4.8, 2.6), dpi=150)
    bars = ax.bar(xs, ys, color=colors, edgecolor=edgecolors, linewidth=1.2)

    ax.set_ylabel(f"{metric_label} ({sel_stat})")
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels)
    ax.set_title(f"Scenario comparison – {district_name}", fontsize=9)
    ax.grid(True, axis="y", linestyle="--", alpha=0.25)

    # Light legend keyed by scenario
    legend_handles = []
    legend_labels = []
    for scen in SCENARIO_ORDER:
        if (panel_df["scenario"] == scen).any():
            legend_handles.append(
                plt.Line2D([0], [0],
                           marker="s",
                           linestyle="none",
                           markersize=7,
                           markerfacecolor=scenario_colors.get(scen, "grey"),
                           markeredgecolor="none")
            )
            legend_labels.append(SCENARIO_DISPLAY.get(scen, scen))
    if legend_handles:
        ax.legend(legend_handles, legend_labels, frameon=False, fontsize=8, ncol=3)

    # Clean spines
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    return fig, ax

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

with st.sidebar:
    try:
        st.image(LOGO_PATH, width=220)
    except Exception:
        pass
    # st.markdown("---")

    state_placeholder = st.empty()
    district_placeholder = st.empty()

    metric_ui_placeholder = st.empty()  # unified "Index" UI
    map_mode_placeholder = st.empty()   # NEW: absolute vs change toggle
    color_slider_placeholder = st.empty()
    st.markdown("---")

    master_controls_placeholder = st.empty()
    st.markdown("---")

    publish_btn = st.button("Publish (export map + info)", key="btn_publish")

st.title("India Resilience Tool")

# Pilot state default
PILOT_STATE = os.getenv("IRT_PILOT_STATE", "Telangana")

# -------------------------
# Pre-build master CSVs for all indices (on app launch)
# -------------------------
for slug, cfg in VARIABLES.items():
    processed_root = Path(
        os.getenv("IRT_PROCESSED_ROOT", DATA_DIR / "processed" / slug)
    ).resolve()
    (processed_root / PILOT_STATE).mkdir(parents=True, exist_ok=True)
    master_path = processed_root / PILOT_STATE / "master_metrics_by_district.csv"

    try:
        if master_needs_rebuild(master_path, processed_root, PILOT_STATE):
            # Build quietly, without user-facing spinner.
            from build_master_metrics import build_master_metrics

            build_master_metrics(
                str(processed_root),
                PILOT_STATE,
                metric_col_in_periods=cfg["periods_metric_col"],
                out_path=str(master_path),
                attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON,
                verbose=False,
            )
    except Exception as e:
        # Don't break the app if one index fails; the per-index fallback below will handle it.
        print(f"[WARN] Pre-build of master CSV failed for index '{slug}': {e}")

# -------------------------
# Unified Index selection (single dropdown)
# -------------------------
with metric_ui_placeholder.container():
    st.markdown("### Metric selection")

    # Show all available indices in a single dropdown labeled "Index"
    index_slugs = list(VARIABLES.keys())
    default_slug = st.session_state.get("selected_var", index_slugs[0])
    selected_var = st.selectbox(
        "Index",
        options=index_slugs,
        index=index_slugs.index(default_slug) if default_slug in index_slugs else 0,
        key="selected_var",
        format_func=lambda k: VARIABLES[k]["label"],
    )

    # Resolve per-index config
    VARIABLE_SLUG = selected_var
    VARCFG = VARIABLES[VARIABLE_SLUG]

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

    # Load + parse schema (for scenario/period/stat only)
    with st.spinner("Loading master CSV..."):
        df = load_master_csv(str(MASTER_CSV_PATH))
    df = normalize_master_columns(df)
    schema_items, metrics, by_metric = parse_master_schema(df.columns)
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

with map_mode_placeholder.container():
    # Tight "Map mode" label with no extra space before the radio
    st.markdown(
        "<div style='font-weight:600; font-size:1rem; margin-bottom:-0.35rem;'>Map mode</div>",
        unsafe_allow_html=True,
    )

    map_mode = st.radio(
        label="",
        options=[
            "Absolute value",
            "Change from 1990-2010 baseline",
        ],
        index=0,
        key="map_mode",
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
        force=False, attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON
    )
    st.success("Master CSV rebuilt or already up-to-date.") if ok else st.info(
        f"Master CSV status: {msg}"
    )

if force_btn:
    ok, msg = rebuild_master_csv_if_needed(
        force=True, attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON
    )
    st.success("Master CSV force-rebuilt.") if ok else st.error(
        f"Forced rebuild failed: {msg}"
    )

# -------------------------
# Build adm1 & enrich adm2 state names
# -------------------------
adm1 = build_adm1_from_adm2(adm2)
if "state_name" not in adm2.columns:
    adm2["state_name"] = "Unknown"
if "district_name" not in adm2.columns:
    adm2["district_name"] = adm2.index.astype(str)

with st.spinner("Enriching district data with state names..."):
    adm2_pts = adm2.copy()
    adm2_pts["geometry"] = adm2_pts.geometry.representative_point()
    joined = gpd.sjoin(
        adm2_pts[["geometry"]], adm1[["geometry", "shapeName"]], how="left", predicate="within"
    )
    if "shapeName" in joined.columns:
        mapping = joined["shapeName"].to_dict()
        for adm2_idx, state_name_val in mapping.items():
            if pd.notna(state_name_val):
                adm2.at[adm2_idx, "state_name"] = str(state_name_val).strip()
    # robust fallback
    missing = adm2["state_name"].isna() | (adm2["state_name"].astype(str).str.strip() == "")
    if missing.any():
        for idx in adm2[missing].index:
            adm2.at[idx, "state_name"] = (
                adm2.at[idx, "state_name"]
                if pd.notna(adm2.at[idx, "state_name"])
                else "Unknown"
            )

# Sync pending selections
if "pending_selected_state" in st.session_state:
    st.session_state["selected_state"] = st.session_state.pop("pending_selected_state")
if "pending_selected_district" in st.session_state:
    st.session_state["selected_district"] = st.session_state.pop("pending_selected_district")

# State/district selectors
states = ["All"] + sorted(adm1["shapeName"].astype(str).str.strip().unique().tolist())
if "selected_state" not in st.session_state or st.session_state["selected_state"] not in states:
    st.session_state["selected_state"] = "Telangana" if "Telangana" in states else "All"
selected_state = state_placeholder.selectbox(
    "State",
    options=states,
    index=states.index(st.session_state["selected_state"]),
    key="selected_state",
)

if selected_state != "All":
    sel_state_norm = selected_state.strip().lower()
    state_row = adm1[adm1["shapeName"].astype(str).str.strip().str.lower() == sel_state_norm]
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
            gdf_state_districts = adm2[adm2.geometry.within(state_geom.buffer(0.001))].copy()
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

districts = ["All"] + sorted(
    gdf_state_districts["district_name"].astype(str).unique().tolist()
)
if "selected_district" not in st.session_state or st.session_state["selected_district"] not in districts:
    st.session_state["selected_district"] = "All"
selected_district = district_placeholder.selectbox(
    "District",
    options=districts,
    index=districts.index(st.session_state["selected_district"]),
    key="selected_district",
)

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

with st.spinner("Merging geometries with CSV attributes (deterministic join)..."):
    adm2c = adm2.copy()
    dfc = df.copy()
    if "state" in dfc.columns:
        dfc = dfc[
            dfc["state"].astype(str).str.strip().str.lower() == PILOT_STATE.lower()
        ].copy()
    if "__key" not in adm2c.columns:
        adm2c["__key"] = adm2c["district_name"].map(alias)
    dfc["__key"] = dfc["district"].map(alias)
    merged = adm2c.merge(dfc, on="__key", how="left", suffixes=("", "_csv")).drop(
        columns=["__key"]
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
    merged_json = precompute_fillcolor(
        merged.to_json(), map_value_col, vmin, vmax, cmap_name=cmap_name
    )
    merged = gpd.read_file(io.StringIO(merged_json))

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

tooltip = folium.features.GeoJsonTooltip(
    fields=tooltip_fields,
    aliases=tooltip_aliases,
    localize=True,
    sticky=True,
)

folium.GeoJson(
    data=json.loads(merged.to_json()),
    name="Districts",
    style_function=style_fn,
    tooltip=tooltip,
    highlight_function=lambda f: {
        "fillColor": "#ffff00",
        "color": "#000",
        "weight": 2,
        "fillOpacity": 0.9,
    },
).add_to(m)

MAP_WIDTH, MAP_HEIGHT = 780, 700
bar_height_px = int(MAP_HEIGHT * 0.92)
bar_width_px = 28
label_font = "12px"
cmap = mpcm.get_cmap(cmap_name)
legend_colors = [mcolors.to_hex(cmap(i / 255)) for i in range(256)]
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

    # Tabs: Map view + Rankings table
    tab_map, tab_table = st.tabs(["🗺 Map view", "📊 Rankings table"])

    # ---------- TAB 1: MAP VIEW ----------
    with tab_map:
        returned = st_folium(
            m,
            width=MAP_WIDTH,
            height=MAP_HEIGHT,
            returned_objects=[
                "last_object_clicked",
                "last_clicked",
                "center",
                "zoom",
                "last_active_drawing",
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

        clicked_name, clicked_state = extract_district_name_from_returned(returned)
        if clicked_name:
            cur = st.session_state.get("selected_district", "All")
            if clicked_name != cur:
                lc = clicked_name.strip().lower()
                matches = merged[merged["district_name"].astype(str).str.lower() == lc]
                if not matches.empty:
                    canonical = matches.iloc[0]["district_name"]
                    canonical_state = matches.iloc[0].get("state_name")
                    geom_for_lookup = matches.iloc[0].geometry
                else:
                    cont = merged[
                        merged["district_name"]
                        .astype(str)
                        .str.lower()
                        .str.contains(lc)
                    ]
                    if not cont.empty:
                        canonical = cont.iloc[0]["district_name"]
                        canonical_state = cont.iloc[0].get("state_name")
                        geom_for_lookup = cont.iloc[0].geometry
                    else:
                        canonical = clicked_name
                        canonical_state = clicked_state
                        geom_for_lookup = None
                adm1_name_to_set = None
                try:
                    if geom_for_lookup is not None:
                        pt = geom_for_lookup.representative_point()
                        contains = adm1[adm1.geometry.contains(pt)]
                        if not contains.empty:
                            adm1_name_to_set = (
                                contains.iloc[0].get("shapeName")
                                or contains.iloc[0].get("shapeGroup")
                            )
                    if adm1_name_to_set is None and canonical_state:
                        cs = str(canonical_state).strip().lower()
                        exact = adm1[
                            adm1["shapeName"]
                            .astype(str)
                            .str.strip()
                            .str.lower()
                            == cs
                        ]
                        if not exact.empty:
                            adm1_name_to_set = exact.iloc[0]["shapeName"]
                        else:
                            subs = adm1[
                                adm1["shapeName"]
                                .astype(str)
                                .str.strip()
                                .str.lower()
                                .str.contains(cs, na=False)
                            ]
                            if not subs.empty:
                                adm1_name_to_set = subs.iloc[0]["shapeName"]
                            else:
                                candidates = adm1["shapeName"].astype(str).tolist()
                                close = difflib.get_close_matches(
                                    canonical_state, candidates, n=1, cutoff=0.7
                                )
                                if close:
                                    adm1_name_to_set = close[0]
                except Exception:
                    adm1_name_to_set = None

                if adm1_name_to_set is None:
                    adm1_name_to_set = canonical_state or "All"
                st.session_state["pending_selected_district"] = canonical

                states_list = ["All"] + sorted(
                    adm1["shapeName"].astype(str).str.strip().unique().tolist()
                )
                if adm1_name_to_set and adm1_name_to_set in states_list:
                    st.session_state["pending_selected_state"] = adm1_name_to_set
                else:
                    lowered = {s.strip().lower(): s for s in states_list}
                    if (
                        adm1_name_to_set
                        and adm1_name_to_set.strip().lower() in lowered
                    ):
                        st.session_state["pending_selected_state"] = lowered[
                            adm1_name_to_set.strip().lower()
                        ]
                    else:
                        st.session_state["pending_selected_state"] = "All"
                st.rerun()

    # ---------- TAB 2: RANKINGS TABLE ----------
    with tab_table:
        st.subheader("District rankings")

        if table_df is None or table_df.empty:
            st.caption(
                "No ranking data available for this index, scenario, period and selection."
            )
        else:
            # Ranking mode selector
            options = ["Top 20 hottest", "Top 20 biggest increases", "All"]
            rank_mode = st.radio(
                "Show:",
                options=options,
                index=0,
                key="rank_mode",
            )

            df_to_show = table_df.copy()

            if rank_mode == "Top 20 hottest":
                if "rank_value" in df_to_show.columns:
                    df_to_show = df_to_show.sort_values("rank_value").head(20)
                else:
                    df_to_show = df_to_show.sort_values("value", ascending=False).head(20)

            elif rank_mode == "Top 20 biggest increases":
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

            st.dataframe(
                df_display,
                use_container_width=True,
            )

            st.caption(
                f"Ranking based on **{VARIABLES[VARIABLE_SLUG]['label']}**, "
                f"**{sel_scenario}**, **{sel_period}**, **{sel_stat}**. "
                f"Change vs baseline uses historical **1990–2010** where available. "
                + (
                    f"Filtered to state: **{selected_state}**."
                    if selected_state != "All"
                    else "Showing all states."
                )
            )


# -------------------------
# Details panel (with risk cards, sparkline + comparison)
# -------------------------
with col2:
    st.header("Climate Profile")

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
    if matched_row is None or matched_row.empty:
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
            if ensemble.get("n_districts", 0) > 0:
                stat_rows = [
                    {"Statistic": "mean", "Value": f"{ensemble['mean']:.2f}"},
                    {"Statistic": "median", "Value": f"{ensemble['median']:.2f}"},
                    {"Statistic": "p05", "Value": f"{ensemble['p05']:.2f}"},
                    {"Statistic": "p95", "Value": f"{ensemble['p95']:.2f}"},
                    {"Statistic": "std", "Value": f"{ensemble['std']:.2f}"},
                    {"Statistic": "n_districts", "Value": int(ensemble["n_districts"])},
                ]
                st.table(pd.DataFrame(stat_rows).set_index("Statistic"))
            else:
                st.info("No numeric district values found for this state & selection.")
            if not per_model_df.empty:
                st.markdown("**Per-model state averages**")
                st.dataframe(
                    per_model_df.rename(
                        columns={"value": "state_avg", "n_districts": "n_districts_used"}
                    ),
                    use_container_width=True,
                )
            if sel_districts_gdf is not None and not sel_districts_gdf.empty:
                st.caption(f"Districts used: {len(sel_districts_gdf)}")

            # State-level yearly PDF (unchanged, but uses VARCFG label)
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
                d = df_yearly.copy()
                if d.empty:
                    return None
                d = d[
                    (d["state"].astype(str).str.strip().str.lower() == state_name.strip().lower())
                    & (d["scenario"].astype(str).str.strip().str.lower() == scenario_name.strip().lower())
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
                ax.plot(d["year"], d["mean"], linewidth=3.0, label="Mean")
                if "p05" in d.columns:
                    ax.plot(d["year"], d["p05"], linewidth=1.5, label="5th percentile")
                if "p95" in d.columns:
                    ax.plot(d["year"], d["p95"], linewidth=1.5, label="95th percentile")
                ax.set_xlabel("Year")
                ax.set_ylabel(metric_label)
                ax.set_title(f"{state_name} • {metric_label} • {scenario_name}")
                ax.grid(True, linestyle="--", alpha=0.35)
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

            _yearly_df = _load_state_yearly(str(PROCESSED_ROOT), PILOT_STATE)
            pdf_path = _make_state_yearly_pdf(
                _yearly_df,
                selected_state,
                sel_scenario,
                VARIABLES[VARIABLE_SLUG]["label"],
                OUTDIR,
            )
            if pdf_path and pdf_path.exists():
                with open(pdf_path, "rb") as fh:
                    st.download_button(
                        "⬇️ Download yearly time-series (PDF)",
                        data=fh.read(),
                        file_name=pdf_path.name,
                        mime="application/pdf",
                        use_container_width=True,
                        key="btn_state_pdf_dl",
                    )
                open_in_new_tab_link(
                    pdf_path, "🗎 Open yearly figure in a new tab", mime="application/pdf"
                )
            else:
                st.caption("No yearly time-series available for this state/scenario.")
        else:
            st.info(
                "Click a district on the map or pick one from the sidebar to see full metrics here."
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
                        label="",
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
                        label="",
                        value=f"{baseline_val_f:.2f}",
                        delta=delta_str,
                        # add \n if you want a line break after "Baseline:"
                        help=f"Baseline: {baseline_desc}",
                    )
                else:
                    st.write("Baseline not available")

            with colc3:
                st.markdown("**Position in state**")
                if rank_in_state is not None and n_in_state is not None:
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
                        label="",
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
                fig_ts, ax_ts = plt.subplots(figsize=(4.8, 2.4), dpi=150)

                # Historical: 1990–2010 in blue + band
                if not hist_ts.empty:
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

                # Scenario: 2020–2060 in red + band
                if not scen_ts.empty:
                    ax_ts.plot(
                        scen_ts["year"],
                        scen_ts["mean"],
                        linewidth=2.0,
                        color="tab:red",
                        label=sel_scenario.upper(),
                    )
                    if {"p05", "p95"}.issubset(scen_ts.columns):
                        ax_ts.fill_between(
                            scen_ts["year"],
                            scen_ts["p05"],
                            scen_ts["p95"],
                            alpha=0.2,
                            color="tab:red",
                        )

                # Transition line: last historical point (2010) to first scenario point (2020) in grey dashed
                if not hist_ts.empty and not scen_ts.empty:
                    # Last historical year (should be 2010 in your pipeline)
                    last_hist_year = int(hist_ts["year"].max())
                    last_hist = hist_ts.loc[hist_ts["year"] == last_hist_year].iloc[-1]

                    # Prefer to connect to 2020 if present; otherwise fall back to earliest scenario year
                    target_year = 2020
                    if target_year in scen_ts["year"].values:
                        first_scen = scen_ts.loc[scen_ts["year"] == target_year].iloc[0]
                    else:
                        # Fallback: earliest available scenario year
                        first_scen = scen_ts.loc[scen_ts["year"].idxmin()]

                    ax_ts.plot(
                        [last_hist["year"], first_scen["year"]],
                        [last_hist["mean"], first_scen["mean"]],
                        color="grey",
                        linestyle="--",
                        linewidth=1.5,
                        label="Transition",
                    )

                ax_ts.set_xlabel("Year")
                ax_ts.set_ylabel(VARIABLES[VARIABLE_SLUG]["label"])
                ax_ts.grid(True, linestyle="--", alpha=0.25)
                # Clean up spines
                for spine in ax_ts.spines.values():
                    spine.set_visible(False)

                # Only show legend if multiple elements exist
                handles, labels = ax_ts.get_legend_handles_labels()
                if handles:
                    ax_ts.legend(frameon=False, fontsize=8, ncol=3)

                fig_ts.tight_layout()
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
            stats_list = ["mean", "median", "p05", "p95", "std"]
            rows_stats = []
            for sname in stats_list:
                coln = f"{sel_metric}__{sel_scenario}__{sel_period}__{sname}"
                val = row.get(coln)
                rows_stats.append(
                    {
                        "Statistic": sname,
                        "Value": "No data" if pd.isna(val) else f"{float(val):.2f}",
                    }
                )
            st.markdown(
                f"**Index:** {VARIABLES[VARIABLE_SLUG]['label']}  \n"
                f"**Scenario:** {sel_scenario}  \n"
                f"**Period:** {sel_period}"
            )
            st.table(pd.DataFrame(rows_stats).set_index("Statistic"))

            # District-level yearly PDF (scenario-specific)
            pdf_path_d = _make_district_yearly_pdf(
                df_yearly=_district_yearly_scen,
                state_name=state_to_show,
                district_name=row.get("district_name", selected_district),
                scenario_name=sel_scenario,
                metric_label=VARIABLES[VARIABLE_SLUG]["label"],
                out_dir=OUTDIR,
            )

            if pdf_path_d and pdf_path_d.exists():
                with open(pdf_path_d, "rb") as fh:
                    st.download_button(
                        "⬇️ Download district yearly time-series (PDF)",
                        data=fh.read(),
                        file_name=pdf_path_d.name,
                        mime="application/pdf",
                        use_container_width=True,
                        key="btn_dist_pdf_dl",
                    )
                abs_url_d = pdf_path_d.resolve().as_uri()
                st.markdown(
                    f'<a href="{abs_url_d}" target="_blank" rel="noopener">🗎 Open district yearly figure in a new tab</a>',
                    unsafe_allow_html=True,
                )
            else:
                st.caption("No yearly time-series available for this district/scenario.")

        # st.markdown("---")

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

# -------------------------
# Publish: HTML/PNG/TXT ZIP
# -------------------------
if publish_btn:
    with st.spinner("Preparing export (HTML + PNG + summary)..."):
        OUTDIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        map_html = OUTDIR / f"export_map_{ts}.html"

        try:
            if returned:
                center = returned.get("center") or returned.get("map_center")
                zoom = returned.get("zoom") or returned.get("map_zoom")
                if center and isinstance(center, (list, tuple)) and len(center) == 2:
                    m.location = [float(center[0]), float(center[1])]
                if zoom:
                    m.zoom_start = int(zoom)
        except Exception:
            pass

        m.save(str(map_html))
        summary_lines = [
            f"Export generated: {ts}",
            f"Layer: {pretty_metric_label}",
            f"State filter: {st.session_state.get('selected_state','All')}",
            f"District filter: {st.session_state.get('selected_district','All')}",
            "",
        ]
        last_clicked_name = None
        if returned:
            for k in (
                "last_object_clicked",
                "clicked_feature",
                "last_active_drawing",
                "last_object",
            ):
                if returned.get(k):
                    feat = returned.get(k)
                    props = feat.get("properties") or feat
                    last_clicked_name = (
                        props.get("district_name") or props.get("shapeName") or None
                    )
                    break
        if last_clicked_name:
            summary_lines.append(f"Clicked district: {last_clicked_name}")
            rr = merged[
                merged["district_name"].astype(str).str.lower()
                == str(last_clicked_name).lower()
            ]
            if not rr.empty:
                r = rr.iloc[0]
                for sname in ["mean", "median", "p05", "p95", "std"]:
                    ccol = f"{sel_metric}__{sel_scenario}__{sel_period}__{sname}"
                    if ccol in r.index:
                        summary_lines.append(f"{ccol}: {r.get(ccol)}")
        else:
            summary_lines.append("No district clicked; map shows current view.")

        txt_path = OUTDIR / f"export_summary_{ts}.txt"
        txt_path.write_text("\n".join(map(str, summary_lines)), encoding="utf-8")

        png_out = OUTDIR / f"export_map_{ts}.png"
        chrome_ok = chrome_screenshot(
            map_html, png_out, width=3000, height=2000, timeout=60
        )

        zip_path = OUTDIR / f"irt_export_{ts}.zip"
        with zipfile.ZipFile(
            zip_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            zf.write(map_html, arcname="map.html")
            zf.write(txt_path, arcname="summary.txt")
            if chrome_ok and png_out.exists():
                zf.write(png_out, arcname="map.png")

        with open(zip_path, "rb") as fh:
            st.download_button(
                "Download export (ZIP)",
                data=fh,
                file_name=zip_path.name,
                mime="application/zip",
                key="btn_export_zip",
            )
        st.success("Export created (HTML + summary + PNG if headless Chrome is available).")

st.markdown("---")
st.caption(
    "Notes: single ‘Index’ picker in the sidebar now lists all indices. "
    "Details panel shows risk cards, trends, narrative, and a comparison option."
)