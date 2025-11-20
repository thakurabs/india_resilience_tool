#!/usr/bin/env python3
"""
build_master_metrics.py

Scan a processed directory tree like:
  OUTPUT_ROOT/<State>/<District>/<Model>/<Scenario>/<District>_periods.csv
  OUTPUT_ROOT/<State>/<District>/<Model>/<Scenario>/<District>_yearly.csv

and produce:
1) A master CSV with ensemble statistics per (scenario, period) per district (wide).
2) State-level per-(scenario,period) per-model averages & ensemble stats across models.
3) NEW: State-level per-(scenario,year) per-model averages & ensemble stats across models (from *_yearly.csv).

Outputs written next to the master CSV:
  - master_metrics_by_district.csv
  - state_model_averages.csv
  - state_ensemble_stats.csv
  - state_yearly_model_averages.csv
  - state_yearly_ensemble_stats.csv

Optional: attach centroid_lon, centroid_lat by supplying --district-geojson.
"""

from pathlib import Path
import argparse
from collections import defaultdict
import pandas as pd
import numpy as np
import json
import sys

# ---------- helper functions ----------

def safe_read_csv(path):
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


def compute_ensemble_stats(values_list):
    """Given a list of numeric values (per-model), return dict of stats."""
    arr = np.array(values_list, dtype=float)
    if arr.size == 0:
        return None
    stats = {}
    stats["mean"] = float(np.nanmean(arr))
    stats["std"] = float(np.nanstd(arr, ddof=0))
    stats["median"] = float(np.nanmedian(arr))
    stats["p05"] = float(np.percentile(arr, 5))
    stats["p95"] = float(np.percentile(arr, 95))
    return stats


# ---------- core builder ----------

def build_master_metrics(output_root: str,
                         state: str,
                         metric_col_in_periods: str = "days_gt_32C",
                         out_path: str = None,
                         attach_centroid_geojson: str = None,
                         verbose: bool = True):
    """
    Traverse:
      OUTPUT_ROOT/<state>/<district>/<model>/<scenario>/<district>_periods.csv
      OUTPUT_ROOT/<state>/<district>/<model>/<scenario>/<district>_yearly.csv

    Compute:
      - District-wide master (per scenario,period) -> master_metrics_by_district.csv
      - State per-(scenario,period):
           * per-model averages over districts -> state_model_averages.csv
           * ensemble across models          -> state_ensemble_stats.csv
      - NEW: State per-(scenario,year):
           * per-model averages over districts -> state_yearly_model_averages.csv
           * ensemble across models            -> state_yearly_ensemble_stats.csv

    Returns: pandas.DataFrame (the district-wide master)
    """

    out_root = Path(output_root)
    state_dir = out_root / state
    if not state_dir.exists():
        raise FileNotFoundError(f"State directory not found: {state_dir}")

    # PER-PERIOD accumulation:
    # metrics[district][(scenario,period)] = list of (model, value)
    metrics = defaultdict(lambda: defaultdict(list))
    districts_seen = set()

    # NEW: accumulate district-yearly series across models
    # district_yearly[(district, scenario)][year] = list of model values
    district_yearly = defaultdict(lambda: defaultdict(list))

    # PER-YEAR accumulation (NEW):
    # years_acc[(scenario, year)][model] = list of values (one per district with data)
    years_acc = defaultdict(lambda: defaultdict(list))  # nested mapping: (scen,year) -> model -> [vals]

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
                # --- NEW: pull per-year series for this district/model/scenario ---
                yfile = scenario_dir / f"{district}_yearly.csv"
                if yfile.exists():
                    try:
                        dy = safe_read_csv(yfile)
                        # expect columns: year, <metric_col_in_periods>
                        if "year" in dy.columns and metric_col_in_periods in dy.columns:
                            for _, r in dy.iterrows():
                                y = r.get("year")
                                v = r.get(metric_col_in_periods)
                                try:
                                    y = int(str(y))
                                    v = float(str(v).replace(",", ""))
                                except Exception:
                                    continue
                                district_yearly[(district, scenario_name)][y].append(v)
                    except Exception as e:
                        if verbose:
                            print(f"Warning: could not read yearly file {yfile}: {e}", file=sys.stderr)


                if periods_path.exists():
                    try:
                        dfp = safe_read_csv(periods_path)
                    except Exception as e:
                        if verbose:
                            print(f"Warning: could not read {periods_path}: {e}", file=sys.stderr)
                        dfp = None
                    if dfp is not None and "period" in dfp.columns:
                        for _, row in dfp.iterrows():
                            period = str(row.get("period"))
                            val = row.get(metric_col_in_periods)
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

                # ---------- PER-YEAR FILE (NEW) ----------
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
                        year_col = None
                        for cand in dfy.columns:
                            if str(cand).strip().lower() in ("year", "yr"):
                                year_col = cand
                                break
                        # Find metric column (case-insensitive strict match or fallback by sanitized name)
                        metric_col = None
                        col_lc = {str(c).strip().lower(): c for c in dfy.columns}
                        target_lc = metric_col_in_periods.strip().lower()
                        if target_lc in col_lc:
                            metric_col = col_lc[target_lc]
                        else:
                            # try sanitized name matching
                            sanitized_target = sanitize_colname(metric_col_in_periods).lower()
                            for c in dfy.columns:
                                if sanitize_colname(c).lower() == sanitized_target:
                                    metric_col = c
                                    break

                        if year_col is None or metric_col is None:
                            if verbose:
                                print(f"Warning: {yearly_path} missing year/metric columns (have: {list(dfy.columns)})", file=sys.stderr)
                        else:
                            # iterate yearly rows
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
            models = [m for (m, v) in vals_models]
            vals = [v for (m, v) in vals_models]
            colbase = f"{metric_col_in_periods}__{scenario}__{period}"
            colbase = sanitize_colname(colbase)
            if vals:
                stats = compute_ensemble_stats(vals)
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

    master = pd.DataFrame(rows).sort_values("district").reset_index(drop=True)

    # ---------- NEW: write per-district yearly ensemble CSVs (INSIDE the function) ----------
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
                    "district": district, "state": state, "scenario": scenario, "year": int(year),
                    "mean": None, "median": None, "p05": None, "p95": None, "std": None, "n_models": 0
                }
            district_yearly_rows_by_d[district].append(row_d)

    # Write one CSV per district under the district folder (uniform schema, always has 'state')
    for district, rows_d in district_yearly_rows_by_d.items():
        ddir = Path(output_root) / state / district
        ddir.mkdir(parents=True, exist_ok=True)
        out_csv = ddir / "district_yearly_ensemble_stats.csv"

        cols = ["district","state","scenario","year","mean","median","p05","p95","std","n_models"]
        if rows_d:
            df_out = pd.DataFrame(rows_d, columns=cols).sort_values(["scenario","year"])
        else:
            # write empty file WITH header so downstream reads still have columns
            df_out = pd.DataFrame(columns=cols)

        df_out.to_csv(out_csv, index=False)
        if verbose:
            print(f"District yearly ensemble -> {out_csv.resolve()}")



    # ---------------------------------------------------------------
    # State-level per-(scenario, period): per-model averages and ensemble
    # ---------------------------------------------------------------
    # model_avgs_acc[(scenario, period)][model] -> list of district values
    model_avgs_acc = defaultdict(lambda: defaultdict(list))
    for district, dmap in metrics.items():
        for (scenario, period), items in dmap.items():
            for m, v in items:
                model_avgs_acc[(scenario, period)][m].append(float(v))

    state_model_rows = []    # tidy
    state_ensemble_rows = [] # tidy
    for (scenario, period), per_model_values in sorted(model_avgs_acc.items()):
        # per-model district-average for the state
        model_avgs = {}
        for model, vs in per_model_values.items():
            vs = [float(x) for x in vs if pd.notna(x)]
            if vs:
                model_avgs[model] = float(np.mean(vs))
                state_model_rows.append({
                    "state": state,
                    "scenario": scenario,
                    "period": period,
                    "model": model,
                    "value": model_avgs[model],
                    "n_districts": int(len(vs))
                })

        # ensemble across models
        vals = list(model_avgs.values())
        if vals:
            stats = compute_ensemble_stats(vals)
            state_ensemble_rows.append({
                "state": state,
                "scenario": scenario,
                "period": period,
                "mean": stats["mean"],
                "std": stats["std"],
                "median": stats["median"],
                "p05": stats["p05"],
                "p95": stats["p95"],
                "n_models": len(vals),
            })
        else:
            state_ensemble_rows.append({
                "state": state,
                "scenario": scenario,
                "period": period,
                "mean": None, "std": None, "median": None, "p05": None, "p95": None,
                "n_models": 0,
            })

    state_model_df = pd.DataFrame(state_model_rows).sort_values(
        ["scenario","period","model"]
    ).reset_index(drop=True)
    state_ensemble_df = pd.DataFrame(state_ensemble_rows).sort_values(
        ["scenario","period"]
    ).reset_index(drop=True)

    # ---------------------------------------------------------------
    # NEW: State-level per-(scenario, year): per-model averages & ensemble (from *_yearly.csv)
    # ---------------------------------------------------------------
    yearly_model_rows = []
    yearly_ensemble_rows = []
    for (scenario, year), per_model in sorted(years_acc.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        model_state_avgs = {}
        for model, vs in per_model.items():
            vs = [float(x) for x in vs if pd.notna(x)]
            if vs:
                val = float(np.mean(vs))
                model_state_avgs[model] = val
                yearly_model_rows.append({
                    "state": state,
                    "scenario": scenario,
                    "year": int(year),
                    "model": model,
                    "value": val,
                    "n_districts": int(len(vs))
                })

        vals = list(model_state_avgs.values())
        if vals:
            stats = compute_ensemble_stats(vals)
            yearly_ensemble_rows.append({
                "state": state,
                "scenario": scenario,
                "year": int(year),
                "mean": stats["mean"],
                "std": stats["std"],
                "median": stats["median"],
                "p05": stats["p05"],
                "p95": stats["p95"],
                "n_models": len(vals),
            })
        else:
            yearly_ensemble_rows.append({
                "state": state,
                "scenario": scenario,
                "year": int(year),
                "mean": None, "std": None, "median": None, "p05": None, "p95": None,
                "n_models": 0,
            })

    state_yearly_model_df = pd.DataFrame(yearly_model_rows).sort_values(
        ["scenario","year","model"]
    ).reset_index(drop=True)
    state_yearly_ensemble_df = pd.DataFrame(yearly_ensemble_rows).sort_values(
        ["scenario","year"]
    ).reset_index(drop=True)

    # -----------------------------
    # Optionally attach district centroids to master
    # -----------------------------
    if attach_centroid_geojson:
        try:
            import geopandas as gpd
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
            def get_centroid(d):
                v = cent_map.get(d)
                if v:
                    return v["centroid_lon"], v["centroid_lat"]
                return (None, None)
            lon_list, lat_list = [], []
            for d in master["district"].tolist():
                lon, lat = get_centroid(d)
                lon_list.append(lon); lat_list.append(lat)
            master["centroid_lon"] = lon_list
            master["centroid_lat"] = lat_list
        except Exception as e:
            if verbose:
                print(f"Warning: could not attach centroids from {attach_centroid_geojson}: {e}", file=sys.stderr)

    # -----------------------------
    # Write outputs (uniform schemas)
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


# ---------- CLI entrypoint ----------

def parse_args():
    p = argparse.ArgumentParser(description="Build master_metrics CSV from processed outputs.")
    p.add_argument("--output-root", "-r", required=True, help="Processed variable root (e.g. /home/abu/.../processed/tas_gt32)")
    p.add_argument("--state", "-s", required=True, help="State dir name (e.g. Telangana)")
    p.add_argument("--metric", "-m", default="days_gt_32C", help="Metric column name inside *_periods.csv and *_yearly.csv")
    p.add_argument("--district-geojson", "-g", default=None, help="Optional path to districts_4326.geojson to attach centroid coordinates")
    p.add_argument("--out", "-o", default=None, help="Output CSV path. If omitted, writes to <output_root>/<state>/master_metrics_by_district.csv")
    return p.parse_args()


def main():
    args = parse_args()
    default_out = Path(args.output_root) / args.state / "master_metrics_by_district.csv"
    out_path = args.out or str(default_out)
    master = build_master_metrics(args.output_root, args.state, metric_col_in_periods=args.metric,
                                  out_path=out_path, attach_centroid_geojson=args.district_geojson)
    print("Done. Rows:", len(master))


if __name__ == "__main__":
    main()