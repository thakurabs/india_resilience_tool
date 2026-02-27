"""State climate profile panel for selected state + all districts/blocks."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional

import pandas as pd
from pyproj import Geod


def _parse_metric_parts(metric_col: str) -> tuple[str, str, str, str]:
    parts = str(metric_col or "").split("__")
    if len(parts) == 4:
        return parts[0], parts[1], parts[2], parts[3]
    return str(metric_col or ""), "", "", ""


def _find_baseline_column(df_cols: list[str], base_metric: str) -> Optional[str]:
    candidates: list[tuple[str, str]] = []
    for c in df_cols:
        p = str(c).split("__")
        if len(p) == 4 and p[0] == base_metric and p[1].lower() == "historical" and p[3] == "mean":
            candidates.append((c, p[2]))
    if not candidates:
        return None
    for c, period in candidates:
        if period.replace(" ", "") in ("1995-2014", "1995_2014", "1985-2014", "1990-2010"):
            return c
    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]


def _with_area_weights(gdf: Any) -> pd.DataFrame:
    geod = Geod(ellps="WGS84")
    out = gdf.copy()
    areas: list[float] = []
    for geom in out.geometry:
        if geom is None or geom.is_empty:
            areas.append(0.0)
            continue
        try:
            a, _ = geod.geometry_area_perimeter(geom)
            areas.append(abs(float(a)))
        except Exception:
            areas.append(0.0)
    out["__area_m2"] = areas
    return out


def _weighted_mean(df: pd.DataFrame, value_col: str) -> Optional[float]:
    if df is None or df.empty or value_col not in df.columns:
        return None
    t = df[[value_col, "__area_m2"]].copy()
    t[value_col] = pd.to_numeric(t[value_col], errors="coerce")
    t["__area_m2"] = pd.to_numeric(t["__area_m2"], errors="coerce")
    t = t.dropna(subset=[value_col, "__area_m2"])
    t = t[t["__area_m2"] > 0]
    if t.empty:
        return None
    return float((t[value_col] * t["__area_m2"]).sum() / t["__area_m2"].sum())


def render_state_summary_view(
    *,
    selected_state: str,
    selected_district: str = "All",
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    sel_districts_gdf: Any,
    merged_gdf: Any,
    processed_root: Path,
    level: str = "district",
) -> None:
    """Render state climate profile (risk summary + trend + scenario comparison)."""
    import streamlit as st
    import plotly.graph_objects as go
    from india_resilience_tool.analysis.metrics import compute_position_stats
    from india_resilience_tool.analysis.timeseries import load_state_yearly
    from india_resilience_tool.data.discovery import (
        discover_state_period_ensemble_file,
        discover_state_yearly_model_file,
    )

    variable_label = variables.get(variable_slug, {}).get("label", variable_slug)
    units = variables.get(variable_slug, {}).get("units") or variables.get(variable_slug, {}).get("unit")
    level_norm = str(level).strip().lower()
    rank_higher_is_worse = bool(variables.get(variable_slug, {}).get("rank_higher_is_worse", True))

    st.subheader(f"{selected_state} — State Climate Profile")
    st.markdown(f"**Index:** {variable_label}  \n**Scenario:** {sel_scenario}  \n**Period:** {sel_period}")

    df_state = _with_area_weights(sel_districts_gdf)
    df_all = _with_area_weights(merged_gdf)

    base_metric, _, _, _ = _parse_metric_parts(metric_col)
    baseline_col = _find_baseline_column(list(df_all.columns), base_metric)

    current_val = _weighted_mean(df_state, metric_col)
    baseline_val = _weighted_mean(df_state, baseline_col) if baseline_col else None

    per_state_vals: list[float] = []
    state_col = "state_name" if "state_name" in df_all.columns else "state"
    for _, grp in df_all.groupby(state_col):
        wv = _weighted_mean(grp, metric_col)
        if wv is not None:
            per_state_vals.append(float(wv))

    pos = compute_position_stats(pd.Series(per_state_vals, dtype=float), current_val, higher_is_worse=rank_higher_is_worse)

    with st.expander("Risk summary", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Historical baseline**")
            st.metric("", f"{baseline_val:.2f} {units or ''}" if baseline_val is not None else "N/A")
        with c2:
            st.markdown("**Current value**")
            delta = (current_val - baseline_val) if (current_val is not None and baseline_val is not None) else None
            st.metric("", f"{current_val:.2f} {units or ''}" if current_val is not None else "N/A", f"{delta:+.2f}" if delta is not None else None)
        with c3:
            st.markdown("**Position in India**")
            if pos.rank is not None and pos.n is not None:
                st.metric("", f"#{pos.rank} / {pos.n}")
            else:
                st.write("N/A")

    with st.expander("Trend over time (state average)", expanded=False):
        yearly_df = load_state_yearly(ts_root=processed_root, state_dir=selected_state, varcfg=None, level=level_norm)
        if yearly_df is None or yearly_df.empty:
            st.info("State yearly ensemble data not available.")
        else:
            d = yearly_df.copy()
            d["scenario"] = d["scenario"].astype(str).str.strip()
            d["year"] = pd.to_numeric(d["year"], errors="coerce")
            d["mean"] = pd.to_numeric(d["mean"], errors="coerce")
            d = d.dropna(subset=["year", "mean"])

            show_models = st.checkbox(
                "Show model members",
                key=f"state_trend_show_models_{variable_slug}_{selected_state}_{level_norm}_{sel_scenario}",
                value=False,
            )

            fig = go.Figure()
            if show_models:
                mf = discover_state_yearly_model_file(ts_root=processed_root, state_dir=selected_state, level=level_norm)
                if mf is None:
                    st.caption("Model members not available for this state/index; showing ensemble only.")
                else:
                    mdf = pd.read_csv(mf)
                    if not mdf.empty and {"scenario", "year", "model", "value"}.issubset(set(mdf.columns)):
                        mdf = mdf[mdf["scenario"].astype(str).str.strip().str.lower().isin(["historical", sel_scenario.lower()])]
                        mdf["year"] = pd.to_numeric(mdf["year"], errors="coerce")
                        mdf["value"] = pd.to_numeric(mdf["value"], errors="coerce")
                        mdf = mdf.dropna(subset=["year", "value"])
                        for model, grp in mdf.groupby("model"):
                            fig.add_trace(go.Scatter(x=grp["year"], y=grp["value"], mode="lines", line={"width": 1}, opacity=0.2, name=str(model), showlegend=False))
                    else:
                        st.caption("Model members not available for this state/index; showing ensemble only.")

            ens = d[d["scenario"].str.lower().isin(["historical", sel_scenario.lower()])].sort_values(["scenario", "year"])
            fig.add_trace(go.Scatter(x=ens["year"], y=ens["mean"], mode="lines", line={"width": 3}, name="Ensemble mean"))
            fig.update_layout(xaxis_title="Year", yaxis_title=variable_label, margin={"l": 10, "r": 10, "t": 10, "b": 10})
            st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False, "responsive": True})

    with st.expander("Scenario comparison (period-mean)", expanded=False):
        f = discover_state_period_ensemble_file(ts_root=processed_root, state_dir=selected_state, level=level_norm)
        if f is None:
            st.info("Scenario comparison not available for this state/index.")
        else:
            sdf = pd.read_csv(f)
            if sdf.empty or not {"scenario", "period", "ensemble_mean"}.issubset(set(sdf.columns)):
                st.info("Scenario comparison not available for this state/index.")
            else:
                force_zero = st.checkbox(
                    "Start y-axis at zero",
                    key=f"state_scenario_y0_{variable_slug}_{selected_state}_{level_norm}_{sel_period}",
                    value=False,
                )
                sdf = sdf.copy()
                sdf["ensemble_mean"] = pd.to_numeric(sdf["ensemble_mean"], errors="coerce")
                sdf = sdf.dropna(subset=["ensemble_mean"])
                fig2 = go.Figure()
                for scen in sorted(sdf["scenario"].astype(str).unique()):
                    g = sdf[sdf["scenario"].astype(str) == scen]
                    fig2.add_trace(go.Bar(x=g["period"], y=g["ensemble_mean"], name=str(scen)))
                if force_zero:
                    fig2.update_yaxes(rangemode="tozero")
                fig2.update_layout(barmode="group", xaxis_title="Period", yaxis_title=variable_label, margin={"l": 10, "r": 10, "t": 10, "b": 10})
                st.plotly_chart(fig2, use_container_width=True, config={"displaylogo": False, "responsive": True})
