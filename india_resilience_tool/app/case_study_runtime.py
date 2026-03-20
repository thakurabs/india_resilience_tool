"""
Runtime helpers for case-study exports (single district, multi-index).

This module keeps the dashboard runtime smaller while preserving the legacy
case-study export behavior (column matching, baseline selection, and time-series
discovery) by accepting the orchestrator's existing loader/filter callables.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

import pandas as pd


def make_district_case_study_builder(
    *,
    variables: Mapping[str, Mapping[str, Any]],
    data_dir: Path,
    pilot_state: str,
    load_master_and_schema_fn: Callable[[Path, str], tuple[pd.DataFrame, list[dict], list[str], dict]],
    portfolio_normalize_fn: Callable[[str], str],
    alias_fn: Callable[[str], str],
    name_aliases: Mapping[str, str],
    load_district_yearly_fn: Callable[..., pd.DataFrame],
    filter_series_for_trend_fn: Callable[..., pd.DataFrame],
    find_baseline_column_for_stat_fn: Callable[[list[str] | pd.Index, str, str], Optional[str]],
    build_scenario_comparison_panel_for_row_fn: Callable[..., pd.DataFrame],
    risk_class_from_percentile_fn: Callable[[Optional[float]], str],
) -> Callable[..., tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]]:
    """
    Return a `build_case_study_data_fn` callable compatible with
    `app/views/details_panel.py`.

    Note: this is district-focused (ADM2). Block support is intentionally not
    added here to avoid changing export scope; the UI should gate exports if
    needed.
    """
    data_dir = Path(data_dir)
    pilot_state = str(pilot_state)

    def _build(
        *,
        state_name: str,
        district_name: str,
        index_slugs: list[str],
        sel_scenario: str,
        sel_period: str,
        sel_stat: str,
        progress_cb: Optional[Callable[[float, str], None]] = None,
    ) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]:
        records: list[dict] = []
        timeseries_by_index: dict[str, dict[str, pd.DataFrame]] = {}
        scenario_panels: dict[str, pd.DataFrame] = {}

        slugs = list(index_slugs or [])
        total = max(1, len(slugs))

        if progress_cb is not None:
            progress_cb(0.0, "Starting…")

        for i, slug in enumerate(slugs):
            if progress_cb is not None:
                progress_cb(min(0.95, float(i) / float(total)), f"Loading {slug}…")

            varcfg = variables.get(slug)
            if not varcfg:
                continue

            # Determine processed root for this index, similar to PROCESSED_ROOT logic
            env_root = os.getenv("IRT_PROCESSED_ROOT")
            if env_root:
                base_path = Path(env_root)
                if base_path.name.lower() == str(slug).lower():
                    proc_root = base_path
                else:
                    proc_root = base_path / str(slug)
            else:
                proc_root = data_dir / "processed" / str(slug)
            proc_root = proc_root.resolve()

            master_path = proc_root / pilot_state / "master_metrics_by_district.csv"
            if not master_path.exists():
                continue

            try:
                df_master, schema_items_local, metrics_local, _by_metric_local = load_master_and_schema_fn(
                    master_path, str(slug)
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
                return portfolio_normalize_fn(m).replace("_", "")

            if registry_metric not in available_metrics:
                # Exact lower-case match first
                m_lower = {str(m).lower(): m for m in available_metrics}
                registry_metric = m_lower.get(str(registry_metric).lower())

            if registry_metric not in available_metrics:
                # Normalized equality / contains fallback
                target_norm = _metric_norm(str(registry_metric))
                eq_matches = [m for m in available_metrics if _metric_norm(str(m)) == target_norm]
                if eq_matches:
                    registry_metric = eq_matches[0]
                else:
                    contains_matches = [
                        m for m in available_metrics if target_norm and target_norm in _metric_norm(str(m))
                    ]
                    registry_metric = contains_matches[0] if contains_matches else available_metrics[0]

            # Candidate column set for this metric + scenario + period (stat may vary)
            prefix = f"{registry_metric}__{sel_scenario}__{sel_period}__"
            metric_col_candidates = [c for c in df_master.columns if isinstance(c, str) and c.startswith(prefix)]

            desired_col = f"{registry_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
            metric_col_local = desired_col if desired_col in df_master.columns else None

            if metric_col_local is None:
                if not metric_col_candidates:
                    continue

                def _stat_norm(s: str) -> str:
                    return portfolio_normalize_fn(s).replace("_", "")

                sel_stat_norm = _stat_norm(str(sel_stat))
                stat_matches = [c for c in metric_col_candidates if _stat_norm(c.split("__")[-1]) == sel_stat_norm]
                metric_col_local = stat_matches[0] if stat_matches else metric_col_candidates[0]

            used_stat = str(metric_col_local).split("__")[-1]

            # Robust match for a single state+district row
            dm = df_master.copy()
            if "state" not in dm.columns or "district" not in dm.columns:
                continue

            dm["_state_key"] = dm["state"].astype(str).map(alias_fn)
            dm["_district_key"] = dm["district"].astype(str).map(alias_fn)

            target_state = alias_fn(state_name)
            target_dist = alias_fn(district_name)

            mask = (dm["_state_key"] == target_state) & (dm["_district_key"] == target_dist)
            if not mask.any():
                # fall back to contains on district name within same state
                mask = (dm["_state_key"] == target_state) & dm["_district_key"].str.contains(target_dist, na=False)
            if not mask.any():
                continue

            row_local = dm[mask].iloc[0]

            # Current value (with fallback columns if the chosen one is NaN)
            current_val_f_local: Optional[float] = None
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
            baseline_col_local = find_baseline_column_for_stat_fn(dm.columns, str(registry_metric), used_stat)
            baseline_val_f_local: Optional[float] = None
            if baseline_col_local and (baseline_col_local in dm.columns):
                baseline_val_local = row_local.get(baseline_col_local)
                baseline_val_f_local = pd.to_numeric([baseline_val_local], errors="coerce")[0]
                if pd.isna(baseline_val_f_local):
                    baseline_val_f_local = None
                else:
                    baseline_val_f_local = float(baseline_val_f_local)

            if current_val_f_local is not None and baseline_val_f_local is not None:
                delta_abs = current_val_f_local - baseline_val_f_local
                delta_pct = None
                if baseline_val_f_local not in (0.0,):
                    delta_pct = (delta_abs / baseline_val_f_local) * 100.0
            else:
                delta_abs = None
                delta_pct = None

            # Ranking within state (direction-aware, inclusive percentile)
            from india_resilience_tool.analysis.metrics import compute_position_stats

            state_mask = dm["_state_key"] == target_state
            state_vals_local = pd.to_numeric(dm.loc[state_mask, metric_col_local], errors="coerce").dropna()

            higher_is_worse_local = bool(varcfg.get("rank_higher_is_worse", True))
            pos_local = compute_position_stats(
                state_vals_local, current_val_f_local, higher_is_worse=higher_is_worse_local
            )
            n_in_state_local = pos_local.n
            rank_in_state_local = pos_local.rank
            percentile_in_state = pos_local.percentile
            risk_class = (
                risk_class_from_percentile_fn(percentile_in_state)
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
            hist_df = load_district_yearly_fn(
                ts_root=proc_root,
                state_dir=str(state_name),
                district_display=str(district_name),
                scenario_name="historical",
                varcfg=varcfg,
                aliases=name_aliases,
            )
            scen_df = load_district_yearly_fn(
                ts_root=proc_root,
                state_dir=str(state_name),
                district_display=str(district_name),
                scenario_name=sel_scenario,
                varcfg=varcfg,
                aliases=name_aliases,
            )
            hist_ts_local = filter_series_for_trend_fn(hist_df, state_name, district_name)
            scen_ts_local = filter_series_for_trend_fn(scen_df, state_name, district_name)
            timeseries_by_index[slug] = {"historical": hist_ts_local, "scenario": scen_ts_local}

            # Scenario comparison panel (period-mean across scenarios)
            try:
                metric_name_for_panel = (
                    str(metric_col_local).split("__")[0] if metric_col_local else str(registry_metric)
                )
                panel_df = build_scenario_comparison_panel_for_row_fn(
                    row=row_local,
                    schema_items=schema_items_local,
                    metric_name=metric_name_for_panel,
                    sel_stat=sel_stat,
                )
            except Exception:
                panel_df = None
            if panel_df is not None and not panel_df.empty:
                scenario_panels[slug] = panel_df

        summary_df = pd.DataFrame.from_records(records) if records else pd.DataFrame()
        if progress_cb is not None:
            progress_cb(1.0, "Completed.")

        return summary_df, timeseries_by_index, scenario_panels

    return _build


def make_case_study_zip_with_labels(
    *, variables: Mapping[str, Mapping[str, Any]]
) -> Callable[..., bytes]:
    """
    Wrap `viz.exports.make_case_study_zip` while preserving legacy label lookup.
    """
    from india_resilience_tool.viz.exports import make_case_study_zip

    def _make(
        *,
        state_name: str,
        district_name: str,
        summary_df: pd.DataFrame,
        ts_dict: dict[str, dict[str, pd.DataFrame]],
        panel_dict: dict[str, pd.DataFrame],
        pdf_bytes: bytes,
    ) -> bytes:
        label_lookup: dict[str, str] = {}
        for slug in set(list((ts_dict or {}).keys()) + list((panel_dict or {}).keys())):
            label_lookup[slug] = variables.get(slug, {}).get("label", slug)

        return make_case_study_zip(
            state_name=state_name,
            district_name=district_name,
            summary_df=summary_df,
            ts_dict=ts_dict,
            panel_dict=panel_dict,
            pdf_bytes=pdf_bytes,
            index_label_lookup=label_lookup,
        )

    return _make
