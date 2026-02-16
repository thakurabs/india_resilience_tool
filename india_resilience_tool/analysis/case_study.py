"""
Case study helpers for single-district, multi-index exports.

Extracted from the legacy dashboard logic.
Provides:
- build_district_case_study_data(...)
- build_scenario_comparison_panel_for_row(...)

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

import pandas as pd


def _default_normalize(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def parse_master_schema(
    columns: list[str] | pd.Index,
) -> tuple[list[dict[str, Any]], list[str], dict[str, list[dict[str, Any]]]]:
    items: list[dict[str, Any]] = []
    metrics: set[str] = set()
    by_metric: dict[str, list[dict[str, Any]]] = {}

    for c in list(columns):
        if not isinstance(c, str) or "__" not in c:
            continue
        parts = c.split("__")
        if len(parts) != 4:
            continue
        metric, scenario, period, stat = parts
        it = {"metric": metric, "scenario": scenario, "period": period, "stat": stat, "col": c}
        items.append(it)
        metrics.add(metric)
        by_metric.setdefault(metric, []).append(it)

    return items, sorted(metrics), by_metric


def find_baseline_column_for_stat(df_cols: list[str] | pd.Index, metric: str, stat: str) -> Optional[str]:
    metric = str(metric)
    stat = str(stat)

    pat = re.compile(rf"^{re.escape(metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__{re.escape(stat)}$")
    candidates: list[tuple[str, str]] = []
    for c in list(df_cols):
        if not isinstance(c, str):
            continue
        m = pat.match(c)
        if not m:
            continue
        if m.group("scenario").strip().lower() == "historical":
            candidates.append((c, m.group("period")))

    if not candidates:
        return None

    pref = {"1990-2010", "1990_2010", "1995-2014", "1995_2014", "1985-2014", "1985_2014"}
    for c, p in candidates:
        if str(p).replace(" ", "") in pref:
            return c

    candidates.sort(key=lambda x: str(x[1]))
    return candidates[0][0]


def build_scenario_comparison_panel_for_row(
    *,
    row: pd.Series,
    schema_items: list[dict[str, Any]],
    metric_name: str,
    sel_stat: str,
) -> pd.DataFrame:
    metric_name = str(metric_name)
    sel_stat = str(sel_stat)

    recs: list[dict[str, Any]] = []
    for it in schema_items:
        if str(it.get("metric")) != metric_name:
            continue
        if str(it.get("stat")) != sel_stat:
            continue
        col = str(it.get("col"))
        v = pd.to_numeric([row.get(col)], errors="coerce")[0]
        if pd.isna(v):
            continue
        recs.append({"scenario": it.get("scenario"), "period": it.get("period"), "value": float(v)})

    df = pd.DataFrame.from_records(recs) if recs else pd.DataFrame()
    return df


def compute_percentile_in_state(state_vals: pd.Series, value: float, *, method: str = "lt") -> Optional[float]:
    try:
        vals = pd.to_numeric(state_vals, errors="coerce").dropna()
        if vals.empty:
            return None
        n = float(len(vals))
        if method == "le":
            return float((vals <= value).sum() / n * 100.0)
        return float((vals < value).sum() / n * 100.0)
    except Exception:
        return None


def risk_class_from_percentile(p: Optional[float]) -> str:
    if p is None:
        return "Unknown"
    try:
        x = float(p)
    except Exception:
        return "Unknown"
    if x >= 80:
        return "Very High"
    if x >= 60:
        return "High"
    if x >= 40:
        return "Medium"
    if x >= 20:
        return "Low"
    return "Very Low"


def resolve_processed_root_for_slug(*, processed_root: Path, slug: str) -> Path:
    slug = str(slug)
    env_root = os.getenv("IRT_PROCESSED_ROOT")
    if env_root:
        base = Path(env_root)
        return (base if base.name.lower() == slug.lower() else base / slug).resolve()
    base2 = Path(processed_root)
    return (base2 if base2.name.lower() == slug.lower() else base2 / slug).resolve()


def build_district_case_study_data(
    *,
    state_name: str,
    district_name: str,
    index_slugs: list[str],
    variables: Mapping[str, Mapping[str, Any]],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    processed_root: Path,
    pilot_state: str,
    normalize_fn: Optional[Callable[[str], str]] = None,
    name_aliases: Optional[Mapping[str, str]] = None,
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]:
    nrm = normalize_fn or _default_normalize

    records: list[dict[str, Any]] = []
    ts_by_index: dict[str, dict[str, pd.DataFrame]] = {}
    panel_by_index: dict[str, pd.DataFrame] = {}

    for slug in index_slugs:
        varcfg = variables.get(slug)
        if not varcfg:
            continue

        proc_root = resolve_processed_root_for_slug(processed_root=processed_root, slug=slug)
        master_path = proc_root / pilot_state / "master_metrics_by_district.csv"
        if not master_path.exists():
            continue

        try:
            df_master = pd.read_csv(master_path)
        except Exception:
            continue

        if "state" not in df_master.columns and "state_name" in df_master.columns:
            df_master = df_master.rename(columns={"state_name": "state"})
        if "district" not in df_master.columns and "district_name" in df_master.columns:
            df_master = df_master.rename(columns={"district_name": "district"})

        if "state" not in df_master.columns or "district" not in df_master.columns:
            continue

        schema_items, _metrics, _by_metric = parse_master_schema(df_master.columns)

        metric_name = varcfg.get("periods_metric_col") or varcfg.get("metric") or None
        if not metric_name:
            continue

        desired_col = f"{metric_name}__{sel_scenario}__{sel_period}__{sel_stat}"
        metric_col = desired_col if desired_col in df_master.columns else None
        if metric_col is None:
            prefix = f"{metric_name}__{sel_scenario}__{sel_period}__"
            candidates = [c for c in df_master.columns if isinstance(c, str) and c.startswith(prefix)]
            metric_col = candidates[0] if candidates else None
        if metric_col is None:
            continue

        used_stat = str(metric_col).split("__")[-1]

        df_master["_state_key"] = df_master["state"].astype(str).map(nrm)
        df_master["_district_key"] = df_master["district"].astype(str).map(nrm)

        target_state = nrm(state_name)
        target_dist = nrm(district_name)

        mask = (df_master["_state_key"] == target_state) & (df_master["_district_key"] == target_dist)
        if not mask.any():
            mask = (df_master["_state_key"] == target_state) & df_master["_district_key"].str.contains(target_dist, na=False)

        if not mask.any():
            continue

        row = df_master.loc[mask].iloc[0]

        current_val = pd.to_numeric([row.get(metric_col)], errors="coerce")[0]
        current_f = None if pd.isna(current_val) else float(current_val)

        baseline_col = find_baseline_column_for_stat(df_master.columns, metric_name, used_stat)
        baseline_val = pd.to_numeric([row.get(baseline_col)], errors="coerce")[0] if baseline_col else pd.NA
        baseline_f = None if pd.isna(baseline_val) else float(baseline_val)

        delta_abs = (current_f - baseline_f) if (current_f is not None and baseline_f is not None) else None
        delta_pct = None
        if delta_abs is not None and baseline_f not in (None, 0.0):
            try:
                delta_pct = delta_abs / float(baseline_f) * 100.0
            except Exception:
                delta_pct = None

        state_vals = pd.to_numeric(
            df_master.loc[df_master["_state_key"] == target_state, metric_col], errors="coerce"
        ).dropna()
        from india_resilience_tool.analysis.metrics import compute_position_stats

        higher_is_worse = bool(varcfg.get("rank_higher_is_worse", True))
        pos = compute_position_stats(state_vals, current_f, higher_is_worse=higher_is_worse)
        n_in_state = pos.n
        rank_in_state = pos.rank
        percentile = pos.percentile

        records.append(
            {
                "index_slug": slug,
                "index_label": varcfg.get("label", slug),
                "units": str(varcfg.get("units") or varcfg.get("unit") or "").strip(),
                "group": varcfg.get("group"),
                "scenario": sel_scenario,
                "period": sel_period,
                "stat": sel_stat,
                "current": current_f,
                "baseline": baseline_f,
                "delta_abs": delta_abs,
                "delta_pct": delta_pct,
                "rank_in_state": rank_in_state,
                "percentile_in_state": percentile,
                "n_in_state": n_in_state,
                "risk_class": risk_class_from_percentile(percentile),
            }
        )

        panel_df = build_scenario_comparison_panel_for_row(
            row=row, schema_items=schema_items, metric_name=metric_name, sel_stat=sel_stat
        )
        if isinstance(panel_df, pd.DataFrame) and not panel_df.empty:
            panel_by_index[slug] = panel_df

        # Optional timeseries (only if your timeseries helpers exist)
        try:
            from india_resilience_tool.analysis.timeseries import load_district_yearly, prepare_yearly_series
        except Exception:
            load_district_yearly = None
            prepare_yearly_series = None

        if load_district_yearly is not None:
            try:
                hist_df = load_district_yearly(
                    ts_root=proc_root,
                    state_dir=str(state_name),
                    district_display=str(district_name),
                    scenario_name="historical",
                    varcfg=dict(varcfg),
                    aliases=dict(name_aliases or {}),
                    normalize_fn=nrm,
                )
                scen_df = load_district_yearly(
                    ts_root=proc_root,
                    state_dir=str(state_name),
                    district_display=str(district_name),
                    scenario_name=str(sel_scenario),
                    varcfg=dict(varcfg),
                    aliases=dict(name_aliases or {}),
                    normalize_fn=nrm,
                )
                if prepare_yearly_series is not None:
                    try:
                        hist_df = prepare_yearly_series(hist_df)
                        scen_df = prepare_yearly_series(scen_df)
                    except Exception:
                        pass
                ts_by_index[slug] = {"historical": hist_df, "scenario": scen_df}
            except Exception:
                pass

    summary_df = pd.DataFrame.from_records(records) if records else pd.DataFrame()
    return summary_df, ts_by_index, panel_by_index
