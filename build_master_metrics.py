#!/usr/bin/env python3
"""
build_master_metrics.py

Build master CSVs for processed metric outputs.

Supports two modes:

1) Single-metric (legacy/backward compatible):
   python build_master_metrics.py --output-root <processed/<metric_slug>> --state <State> --metric <metric_col>

2) Batch mode (new default):
   python build_master_metrics.py
   - Discovers metrics under the processed root (paths.BASE_OUTPUT_ROOT if available)
   - Filters to metric slugs present in BOTH:
       - variables.py VARIABLES
       - metrics_registry.py METRICS_BY_SLUG (pipeline specs)
     (compute_indices.py uses the same pipeline registry)
   - For each metric + state, writes:
       master_metrics_by_district.csv
       state_model_averages.csv
       state_ensemble_stats.csv
       state_yearly_model_averages.csv
       state_yearly_ensemble_stats.csv

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
import argparse
from collections import defaultdict
import json
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# -----------------------------------------------------------------------------
# Optional imports (repo/package layout tolerant)
# -----------------------------------------------------------------------------
def _try_import_registries() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Return (VARIABLES, METRICS_BY_SLUG) with import fallbacks.

    VARIABLES comes from variables.py.
    METRICS_BY_SLUG comes from metrics_registry.py (pipeline registry).
    """
    variables: Dict[str, Any]
    metrics_by_slug: Dict[str, Any]

    # variables.py
    try:
        from india_resilience_tool.config.variables import VARIABLES as _VARIABLES  # type: ignore
        variables = dict(_VARIABLES)
    except Exception:
        try:
            from variables import VARIABLES as _VARIABLES  # type: ignore
            variables = dict(_VARIABLES)
        except Exception as e:
            raise ImportError(
                "Could not import VARIABLES from india_resilience_tool.config.variables or variables.py"
            ) from e

    # metrics_registry.py
    try:
        from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG as _METRICS_BY_SLUG  # type: ignore
        metrics_by_slug = dict(_METRICS_BY_SLUG)
    except Exception:
        try:
            from metrics_registry import METRICS_BY_SLUG as _METRICS_BY_SLUG  # type: ignore
            metrics_by_slug = dict(_METRICS_BY_SLUG)
        except Exception as e:
            raise ImportError(
                "Could not import METRICS_BY_SLUG from india_resilience_tool.config.metrics_registry or metrics_registry.py"
            ) from e

    return variables, metrics_by_slug


def _try_import_processed_root() -> Optional[Path]:
    """
    Try to import BASE_OUTPUT_ROOT from paths.py (compute_indices.py uses this),
    returning None if unavailable.
    """
    try:
        from paths import BASE_OUTPUT_ROOT  # type: ignore
        return Path(BASE_OUTPUT_ROOT)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def safe_read_csv(path: Path) -> pd.DataFrame:
    """Try robust CSV read with encoding fallbacks."""
    path = Path(path)
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, encoding="ISO-8859-1")
        except Exception:
            return pd.read_csv(path, encoding="utf-8", errors="replace")


def sanitize_colname(s: str) -> str:
    """Return a machine/ArcGIS-friendly column string (ASCII-ish, underscores)."""
    s = str(s).strip()
    s = s.replace(" ", "_").replace("-", "_").replace("/", "_").replace("%", "pct")
    s = s.replace("(", "").replace(")", "").replace(",", "").replace(":", "").replace("'", "")
    while "__" in s:
        s = s.replace("__", "_")
    return s


def compute_ensemble_stats(values_list: Sequence[float]) -> Optional[Dict[str, float]]:
    """Given a list of numeric values (per-model), return dict of stats."""
    arr = np.array(list(values_list), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return None
    stats: Dict[str, float] = {}
    stats["mean"] = float(np.nanmean(arr))
    stats["std"] = float(np.nanstd(arr, ddof=0))
    stats["median"] = float(np.nanmedian(arr))
    stats["p05"] = float(np.percentile(arr, 5))
    stats["p95"] = float(np.percentile(arr, 95))
    return stats


def _first_existing_metric_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """
    Return the first metric column found in df from candidates, using:
      - exact match,
      - case-insensitive match,
      - sanitized-name match.

    Returns None if nothing matches.
    """
    if df is None or df.empty:
        # Still allow column checks if headers exist
        pass

    cols = list(df.columns)
    col_lc = {str(c).strip().lower(): c for c in cols}

    for cand in candidates:
        if not cand:
            continue
        cand_str = str(cand).strip()
        if cand_str in cols:
            return cand_str

        cand_lc = cand_str.lower()
        if cand_lc in col_lc:
            return str(col_lc[cand_lc])

        # sanitized match
        sanitized_target = sanitize_colname(cand_str).lower()
        for c in cols:
            if sanitize_colname(c).lower() == sanitized_target:
                return str(c)

    return None


def _discover_metric_dirs(processed_root: Path) -> List[Path]:
    """Return immediate subdirectories under processed_root (each is a metric slug folder)."""
    if not processed_root.exists():
        return []
    return sorted([p for p in processed_root.iterdir() if p.is_dir()])


def _looks_like_state_dir(state_dir: Path) -> bool:
    """
    Heuristic: a valid state folder should contain at least one
    <district>/<model>/<scenario>/<district>_periods.csv or _yearly.csv
    exactly four levels below the state directory.

    This filters out auxiliary folders like 'pdf_plots', 'plots', etc.
    """
    if not state_dir.is_dir():
        return False

    patterns = ("*/*/*/*_periods.csv", "*/*/*/*_yearly.csv")
    for pat in patterns:
        try:
            # Stop at the first match (fast)
            for _ in state_dir.glob(pat):
                return True
        except Exception:
            continue
    return False


def _discover_states(metric_root: Path) -> List[str]:
    """Return state directories under a metric root (filtered)."""
    states: List[str] = []
    for p in metric_root.iterdir():
        if p.is_dir() and _looks_like_state_dir(p):
            states.append(p.name)
    return sorted(states)


# -----------------------------------------------------------------------------
# Core builder (single metric root + single state)  [keeps legacy signature]
# -----------------------------------------------------------------------------
def build_master_metrics(
    output_root: str,
    state: str,
    metric_col_in_periods: str = "days_gt_32C",
    out_path: str | None = None,
    attach_centroid_geojson: str | None = None,
    verbose: bool = True,
    metric_col_candidates: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Traverse:
      OUTPUT_ROOT/<state>/<district>/<model>/<scenario>/<district>_periods.csv
      OUTPUT_ROOT/<state>/<district>/<model>/<scenario>/<district>_yearly.csv

    Compute:
      - District-wide master (per scenario,period) -> master_metrics_by_district.csv
      - State per-(scenario,period):
           * per-model averages over districts -> state_model_averages.csv
           * ensemble across models          -> state_ensemble_stats.csv
      - State per-(scenario,year):
           * per-model averages over districts -> state_yearly_model_averages.csv
           * ensemble across models            -> state_yearly_ensemble_stats.csv

    Notes:
      - metric_col_in_periods is used to NAME the master columns:
          <metric_col_in_periods>__<scenario>__<period>__<stat>
      - metric_col_candidates controls how we READ values from *_periods.csv/_yearly.csv.
        If None, we default to [metric_col_in_periods, "value"].

    Returns: pandas.DataFrame (the district-wide master)
    """
    out_root = Path(output_root)
    state_dir = out_root / state
    if not state_dir.exists():
        raise FileNotFoundError(f"State directory not found: {state_dir}")

    read_candidates: List[str] = []
    if metric_col_candidates:
        read_candidates.extend([str(x) for x in metric_col_candidates if x])
    # Always include naming col and generic fallback
    if metric_col_in_periods:
        read_candidates.insert(0, metric_col_in_periods)
    if "value" not in [c.lower() for c in read_candidates]:
        read_candidates.append("value")

    # PER-PERIOD accumulation:
    # metrics[district][(scenario,period)] = list of (model, value)
    metrics = defaultdict(lambda: defaultdict(list))
    districts_seen = set()

    # NEW: accumulate district-yearly series across models
    # district_yearly[(district, scenario)][year] = list of model values
    district_yearly = defaultdict(lambda: defaultdict(list))

    # PER-YEAR accumulation:
    # years_acc[(scenario, year)][model] = list of values (one per district with data)
    years_acc = defaultdict(lambda: defaultdict(list))

    # walk district directories
    for district_dir in sorted(state_dir.iterdir()):
        if not district_dir.is_dir():
            continue
        district = district_dir.name
        districts_seen.add(district)

        # inside district folder expect model folders
        for model_dir in sorted(district_dir.iterdir()):
            if not model_dir.is_dir():
                continue
            model_name = model_dir.name

            # inside each model dir expect scenario folders
            for scenario_dir in sorted(model_dir.iterdir()):
                if not scenario_dir.is_dir():
                    continue
                scenario_name = scenario_dir.name

                # ---------- PER-PERIOD FILE ----------
                periods_path = scenario_dir / f"{district}_periods.csv"
                if periods_path.exists():
                    try:
                        dfp = safe_read_csv(periods_path)
                    except Exception as e:
                        if verbose:
                            print(f"Warning: could not read {periods_path}: {e}", file=sys.stderr)
                        dfp = None

                    if dfp is not None and "period" in dfp.columns:
                        metric_col = _first_existing_metric_col(dfp, read_candidates)
                        if metric_col is None:
                            if verbose:
                                print(
                                    f"Warning: {periods_path} missing metric columns. "
                                    f"Tried: {read_candidates}. Have: {list(dfp.columns)}",
                                    file=sys.stderr,
                                )
                        else:
                            for _, row in dfp.iterrows():
                                period = str(row.get("period"))
                                val = row.get(metric_col)
                                if pd.isna(val):
                                    continue
                                try:
                                    valf = float(val)
                                except Exception:
                                    try:
                                        valf = float(str(val).replace(",", ""))
                                    except Exception:
                                        continue
                                key = (scenario_name, period)
                                metrics[district][key].append((model_name, valf))

                # ---------- PER-YEAR FILE ----------
                yearly_path = scenario_dir / f"{district}_yearly.csv"
                if yearly_path.exists():
                    try:
                        dfy = safe_read_csv(yearly_path)
                    except Exception as e:
                        if verbose:
                            print(f"Warning: could not read {yearly_path}: {e}", file=sys.stderr)
                        dfy = None

                    if dfy is not None:
                        # Find year column (case-insensitive)
                        year_col = _first_existing_metric_col(dfy, ["year", "yr"])
                        metric_col = _first_existing_metric_col(dfy, read_candidates)

                        if year_col is None or metric_col is None:
                            if verbose:
                                print(
                                    f"Warning: {yearly_path} missing year/metric columns. "
                                    f"Tried year in ['year','yr'] and metric in {read_candidates}. "
                                    f"Have: {list(dfy.columns)}",
                                    file=sys.stderr,
                                )
                        else:
                            for _, r in dfy.iterrows():
                                y = r.get(year_col)
                                v = r.get(metric_col)
                                if pd.isna(y) or pd.isna(v):
                                    continue
                                try:
                                    y_int = int(str(y).split(".")[0])
                                except Exception:
                                    continue
                                try:
                                    v_f = float(v)
                                except Exception:
                                    try:
                                        v_f = float(str(v).replace(",", ""))
                                    except Exception:
                                        continue

                                # For district-yearly ensemble by scenario
                                district_yearly[(district, scenario_name)][y_int].append(v_f)
                                # For state-yearly model averages
                                years_acc[(scenario_name, y_int)][model_name].append(v_f)

    # ----------------------------
    # Build district-wide master (per-period)
    # ----------------------------
    all_keys = set()
    for d in metrics:
        all_keys.update(metrics[d].keys())
    all_keys = sorted(all_keys)

    rows = []
    for district in sorted(districts_seen):
        row = {"district": district, "state": state}
        for (scenario, period) in all_keys:
            vals_models = metrics[district].get((scenario, period), [])
            models = [m for (m, _) in vals_models]
            vals = [v for (_, v) in vals_models]
            colbase = f"{metric_col_in_periods}__{scenario}__{period}"
            colbase = sanitize_colname(colbase)

            if vals:
                stats = compute_ensemble_stats(vals)
                assert stats is not None
                row[f"{colbase}__mean"] = stats["mean"]
                row[f"{colbase}__std"] = stats["std"]
                row[f"{colbase}__median"] = stats["median"]
                row[f"{colbase}__p05"] = stats["p05"]
                row[f"{colbase}__p95"] = stats["p95"]
                row[f"{colbase}__n_models"] = len(vals)
                row[f"{colbase}__models"] = json.dumps(models, ensure_ascii=False)
                mapping = {m: v for (m, v) in vals_models}
                row[f"{colbase}__values_per_model"] = json.dumps(mapping, ensure_ascii=False)
            else:
                row[f"{colbase}__mean"] = None
                row[f"{colbase}__std"] = None
                row[f"{colbase}__median"] = None
                row[f"{colbase}__p05"] = None
                row[f"{colbase}__p95"] = None
                row[f"{colbase}__n_models"] = 0
                row[f"{colbase}__models"] = json.dumps([])
                row[f"{colbase}__values_per_model"] = json.dumps({})
        rows.append(row)

        if not rows:
            if verbose:
                print(
                    f"Warning: No district rows found under {state_dir}. "
                    "Skipping master build for this state folder.",
                    file=sys.stderr,
                )
            master = pd.DataFrame(columns=["district", "state"])
            # Still write empty outputs if caller provided out_path, to make batch runs robust.
            if out_path:
                outp = Path(out_path)
                outp.parent.mkdir(parents=True, exist_ok=True)
                master.to_csv(outp, index=False)

                # Create empty companion CSVs with expected headers
                (outp.parent / "state_model_averages.csv").write_text("state,scenario,period,model,value,n_districts\n", encoding="utf-8")
                (outp.parent / "state_ensemble_stats.csv").write_text("state,scenario,period,mean,std,median,p05,p95,n_models\n", encoding="utf-8")
                (outp.parent / "state_yearly_model_averages.csv").write_text("state,scenario,year,model,value,n_districts\n", encoding="utf-8")
                (outp.parent / "state_yearly_ensemble_stats.csv").write_text("state,scenario,year,mean,std,median,p05,p95,n_models\n", encoding="utf-8")
            return master

        master = pd.DataFrame(rows).sort_values("district").reset_index(drop=True)


    # ---------- Write per-district yearly ensemble CSVs ----------
    from collections import defaultdict as _dd
    district_yearly_rows_by_d = _dd(list)

    for (district, scenario), year_map in sorted(district_yearly.items()):
        for year, values in sorted(year_map.items()):
            arr = np.array([float(x) for x in values if pd.notna(x)], dtype=float)
            if arr.size > 0:
                row_d = {
                    "district": district,
                    "state": state,
                    "scenario": scenario,
                    "year": int(year),
                    "mean": float(np.nanmean(arr)),
                    "median": float(np.nanmedian(arr)),
                    "p05": float(np.percentile(arr, 5)),
                    "p95": float(np.percentile(arr, 95)),
                    "std": float(np.nanstd(arr, ddof=0)),
                    "n_models": int(arr.size),
                }
            else:
                row_d = {
                    "district": district,
                    "state": state,
                    "scenario": scenario,
                    "year": int(year),
                    "mean": None,
                    "median": None,
                    "p05": None,
                    "p95": None,
                    "std": None,
                    "n_models": 0,
                }
            district_yearly_rows_by_d[district].append(row_d)

    for district, rows_d in district_yearly_rows_by_d.items():
        ddir = Path(output_root) / state / district
        ddir.mkdir(parents=True, exist_ok=True)
        out_csv = ddir / "district_yearly_ensemble_stats.csv"
        cols = ["district", "state", "scenario", "year", "mean", "median", "p05", "p95", "std", "n_models"]
        df_out = pd.DataFrame(rows_d, columns=cols).sort_values(["scenario", "year"]) if rows_d else pd.DataFrame(columns=cols)
        df_out.to_csv(out_csv, index=False)
        if verbose:
            print(f"District yearly ensemble -> {out_csv.resolve()}")

    # ---------------------------------------------------------------
    # State-level per-(scenario, period): per-model averages and ensemble
    # ---------------------------------------------------------------
    model_avgs_acc = defaultdict(lambda: defaultdict(list))
    for district, dmap in metrics.items():
        for (scenario, period), items in dmap.items():
            for m, v in items:
                model_avgs_acc[(scenario, period)][m].append(float(v))

    state_model_rows = []
    state_ensemble_rows = []
    for (scenario, period), per_model_values in sorted(model_avgs_acc.items()):
        model_avgs: Dict[str, float] = {}
        for model, vs in per_model_values.items():
            vs2 = [float(x) for x in vs if pd.notna(x)]
            if vs2:
                model_avgs[model] = float(np.mean(vs2))
                state_model_rows.append(
                    {
                        "state": state,
                        "scenario": scenario,
                        "period": period,
                        "model": model,
                        "value": model_avgs[model],
                        "n_districts": int(len(vs2)),
                    }
                )

        vals = list(model_avgs.values())
        if vals:
            stats = compute_ensemble_stats(vals)
            assert stats is not None
            state_ensemble_rows.append(
                {
                    "state": state,
                    "scenario": scenario,
                    "period": period,
                    "mean": stats["mean"],
                    "std": stats["std"],
                    "median": stats["median"],
                    "p05": stats["p05"],
                    "p95": stats["p95"],
                    "n_models": len(vals),
                }
            )
        else:
            state_ensemble_rows.append(
                {
                    "state": state,
                    "scenario": scenario,
                    "period": period,
                    "mean": None,
                    "std": None,
                    "median": None,
                    "p05": None,
                    "p95": None,
                    "n_models": 0,
                }
            )

    state_model_df = pd.DataFrame(state_model_rows).sort_values(["scenario", "period", "model"]).reset_index(drop=True)
    state_ensemble_df = pd.DataFrame(state_ensemble_rows).sort_values(["scenario", "period"]).reset_index(drop=True)

    # ---------------------------------------------------------------
    # State-level per-(scenario, year): per-model averages & ensemble
    # ---------------------------------------------------------------
    yearly_model_rows = []
    yearly_ensemble_rows = []
    for (scenario, year), per_model in sorted(years_acc.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        model_state_avgs: Dict[str, float] = {}
        for model, vs in per_model.items():
            vs2 = [float(x) for x in vs if pd.notna(x)]
            if vs2:
                val = float(np.mean(vs2))
                model_state_avgs[model] = val
                yearly_model_rows.append(
                    {
                        "state": state,
                        "scenario": scenario,
                        "year": int(year),
                        "model": model,
                        "value": val,
                        "n_districts": int(len(vs2)),
                    }
                )

        vals = list(model_state_avgs.values())
        if vals:
            stats = compute_ensemble_stats(vals)
            assert stats is not None
            yearly_ensemble_rows.append(
                {
                    "state": state,
                    "scenario": scenario,
                    "year": int(year),
                    "mean": stats["mean"],
                    "std": stats["std"],
                    "median": stats["median"],
                    "p05": stats["p05"],
                    "p95": stats["p95"],
                    "n_models": len(vals),
                }
            )
        else:
            yearly_ensemble_rows.append(
                {
                    "state": state,
                    "scenario": scenario,
                    "year": int(year),
                    "mean": None,
                    "std": None,
                    "median": None,
                    "p05": None,
                    "p95": None,
                    "n_models": 0,
                }
            )

    state_yearly_model_df = pd.DataFrame(yearly_model_rows).sort_values(["scenario", "year", "model"]).reset_index(drop=True)
    state_yearly_ensemble_df = pd.DataFrame(yearly_ensemble_rows).sort_values(["scenario", "year"]).reset_index(drop=True)

    # -----------------------------
    # Optionally attach district centroids to master
    # -----------------------------
    if attach_centroid_geojson:
        try:
            import geopandas as gpd  # optional dependency
            gdf = gpd.read_file(attach_centroid_geojson)
            if "DISTRICT" in gdf.columns:
                gdf["district_key"] = gdf["DISTRICT"].astype(str).str.strip()
            else:
                txt_cols = [c for c in gdf.columns if gdf[c].dtype == object and c != "geometry"]
                if txt_cols:
                    gdf["district_key"] = gdf[txt_cols[0]].astype(str).str.strip()
                else:
                    gdf["district_key"] = gdf.index.astype(str)
            gdf = gdf.to_crs("EPSG:4326")
            gdf["centroid"] = gdf.geometry.centroid
            gdf["centroid_lon"] = gdf["centroid"].x
            gdf["centroid_lat"] = gdf["centroid"].y
            cent_map = gdf.set_index("district_key")[["centroid_lon", "centroid_lat"]].to_dict(orient="index")

            def get_centroid(d: str) -> Tuple[Optional[float], Optional[float]]:
                v = cent_map.get(d)
                if v:
                    return float(v["centroid_lon"]), float(v["centroid_lat"])
                return (None, None)

            lon_list, lat_list = [], []
            for d in master["district"].tolist():
                lon, lat = get_centroid(str(d))
                lon_list.append(lon)
                lat_list.append(lat)
            master["centroid_lon"] = lon_list
            master["centroid_lat"] = lat_list
        except Exception as e:
            if verbose:
                print(f"Warning: could not attach centroids from {attach_centroid_geojson}: {e}", file=sys.stderr)

    # -----------------------------
    # Write outputs
    # -----------------------------
    if out_path:
        outp = Path(out_path)
        outp.parent.mkdir(parents=True, exist_ok=True)
        master.to_csv(outp, index=False)

        state_model_out = outp.parent / "state_model_averages.csv"
        state_ensemble_out = outp.parent / "state_ensemble_stats.csv"
        state_yearly_model_out = outp.parent / "state_yearly_model_averages.csv"
        state_yearly_ensemble_out = outp.parent / "state_yearly_ensemble_stats.csv"

        state_model_df.to_csv(state_model_out, index=False)
        state_ensemble_df.to_csv(state_ensemble_out, index=False)
        state_yearly_model_df.to_csv(state_yearly_model_out, index=False)
        state_yearly_ensemble_df.to_csv(state_yearly_ensemble_out, index=False)

        if verbose:
            print(f"Master CSV                   -> {outp.resolve()}")
            print(f"State model averages         -> {state_model_out.resolve()}")
            print(f"State ensemble stats         -> {state_ensemble_out.resolve()}")
            print(f"State yearly model averages  -> {state_yearly_model_out.resolve()}")
            print(f"State yearly ensemble stats  -> {state_yearly_ensemble_out.resolve()}")

    return master


# -----------------------------------------------------------------------------
# Batch mode (new)
# -----------------------------------------------------------------------------
def build_all_master_metrics(
    processed_root: Path,
    *,
    state_filter: Optional[Sequence[str]] = None,
    district_geojson: Optional[str] = None,
    verbose: bool = True,
    skip_existing: bool = False,
) -> None:
    """
    Build master CSVs for all metrics under processed_root that are present in:
      - variables.py VARIABLES
      - metrics_registry.py METRICS_BY_SLUG

    For each metric, for each state directory found (or filtered), writes
    <processed_root>/<metric_slug>/<state>/master_metrics_by_district.csv
    """
    variables, metrics_by_slug = _try_import_registries()

    metric_dirs = _discover_metric_dirs(processed_root)
    on_disk_slugs = {p.name for p in metric_dirs}

    variables_slugs = set(variables.keys())
    registry_slugs = set(metrics_by_slug.keys())

    eligible_slugs = sorted(on_disk_slugs & variables_slugs & registry_slugs)

    if verbose:
        print(f"[BATCH] processed_root = {processed_root}")
        print(f"[BATCH] found_on_disk = {len(on_disk_slugs)} metrics")
        print(f"[BATCH] eligible (disk ∩ variables ∩ registry) = {len(eligible_slugs)} metrics")

    if not eligible_slugs:
        print(
            "[BATCH] No eligible metrics found. Check that your metric folder names match slugs in "
            "variables.py and metrics_registry.py.",
            file=sys.stderr,
        )
        return

    # Normalize state filter
    state_filter_norm: Optional[set[str]] = None
    if state_filter:
        state_filter_norm = {str(s).strip() for s in state_filter if str(s).strip()}

    for slug in eligible_slugs:
        metric_root = processed_root / slug
        states = _discover_states(metric_root)
        if state_filter_norm is not None:
            states = [s for s in states if s in state_filter_norm]

        if not states:
            if verbose:
                print(f"[BATCH] {slug}: no matching states; skipping")
            continue

        vcfg = variables.get(slug, {}) or {}
        periods_metric_col = str(vcfg.get("periods_metric_col") or "").strip()

        reg = metrics_by_slug.get(slug)
        # MetricSpec has .value_col and .periods_metric_col (in your shared registry)
        reg_value_col = ""
        try:
            reg_value_col = str(getattr(reg, "value_col", "") or "").strip()
        except Exception:
            reg_value_col = ""

        # Use dashboard-facing naming first (periods_metric_col), fallback to registry value_col, then "value"
        out_metric_name = periods_metric_col or reg_value_col or "value"

        # Read candidates: allow either naming col, registry value_col, and generic 'value'
        read_candidates = [c for c in [periods_metric_col, reg_value_col, "value"] if c]

        for state in states:
            out_path = metric_root / state / "master_metrics_by_district.csv"
            if skip_existing and out_path.exists():
                if verbose:
                    print(f"[BATCH] {slug}/{state}: exists; skipping ({out_path})")
                continue

            if verbose:
                print(f"[BATCH] Building {slug}/{state} using out_metric_name='{out_metric_name}' read_candidates={read_candidates}")

            build_master_metrics(
                str(metric_root),
                state,
                metric_col_in_periods=out_metric_name,
                out_path=str(out_path),
                attach_centroid_geojson=district_geojson,
                verbose=verbose,
                metric_col_candidates=read_candidates,
            )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build master_metrics CSV(s) from processed outputs.")

    # Batch mode
    p.add_argument(
        "--processed-root",
        "-p",
        default=None,
        help="Processed root directory (e.g., D:/projects/irt_data/processed). "
             "If omitted, tries paths.BASE_OUTPUT_ROOT. If neither, you must provide --output-root for single mode.",
    )
    p.add_argument(
        "--state",
        "-s",
        default=None,
        help="Optional state filter. Comma-separated allowed (e.g., Telangana,Odisha). "
             "If omitted in batch mode, builds for all states found under each metric.",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="In batch mode, skip writing if master_metrics_by_district.csv already exists.",
    )

    # Single-metric mode (legacy)
    p.add_argument(
        "--output-root",
        "-r",
        default=None,
        help="Processed variable root (single-metric), e.g. D:/.../processed/pr_max_5day_precip",
    )
    p.add_argument(
        "--metric",
        "-m",
        default=None,
        help="Metric column name inside *_periods.csv and *_yearly.csv (single-metric mode). "
             "If omitted, batch mode chooses from registries.",
    )

    p.add_argument(
        "--district-geojson",
        "-g",
        default=None,
        help="Optional path to districts_4326.geojson to attach centroid coordinates",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce stdout printing (errors still go to stderr).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    verbose = not bool(args.quiet)

    state_filter: Optional[List[str]] = None
    if args.state:
        state_filter = [s.strip() for s in str(args.state).split(",") if s.strip()]

    # Decide mode:
    # - If --output-root is given -> single metric mode
    # - Else -> batch mode using --processed-root or paths.BASE_OUTPUT_ROOT
    if args.output_root:
        if not args.state:
            raise SystemExit("Single-metric mode requires --state. (You gave --output-root.)")
        metric_col = args.metric or "value"
        default_out = Path(args.output_root) / str(args.state) / "master_metrics_by_district.csv"
        out_path = str(default_out)
        build_master_metrics(
            args.output_root,
            str(args.state),
            metric_col_in_periods=metric_col,
            out_path=out_path,
            attach_centroid_geojson=args.district_geojson,
            verbose=verbose,
            metric_col_candidates=[metric_col, "value"],
        )
        if verbose:
            print("Done.")
        return

    processed_root = Path(args.processed_root) if args.processed_root else (_try_import_processed_root() or None)
    if processed_root is None:
        raise SystemExit(
            "Batch mode needs --processed-root OR paths.BASE_OUTPUT_ROOT. "
            "Neither was available. Provide --processed-root <.../processed>."
        )

    build_all_master_metrics(
        processed_root,
        state_filter=state_filter,
        district_geojson=args.district_geojson,
        verbose=verbose,
        skip_existing=bool(args.skip_existing),
    )


if __name__ == "__main__":
    main()
