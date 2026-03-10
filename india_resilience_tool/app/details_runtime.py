"""
Right-panel runtime (Streamlit UI) for IRT.

This module centralizes the right-column "Climate Profile" routing logic so the
app runtime (`app/runtime.py`) can focus on orchestration.

Design notes
------------
- This is app-layer code (Streamlit allowed).
- It preserves legacy widget keys and session_state behavior.
- Heavy lifting (analysis/data/viz) remains in Streamlit-free modules where possible.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


def render_right_panel(
    *,
    returned: Optional[Mapping[str, Any]],
    selected_state: str,
    selected_district: str,
    selected_block: str,
    selected_basin: str,
    selected_subbasin: str,
    admin_level: str,
    spatial_family: str,
    # Variable/metric context
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    index_group_labels: Mapping[str, str],
    sel_metric: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    # Data
    merged: Any,
    adm1: Any,
    df: pd.DataFrame,
    schema_items: Sequence[Mapping[str, Any]],
    processed_root: Path,
    # Config
    pilot_state: str,
    data_dir: Path,
    logo_path: Optional[Path],
    # Figure styling
    fig_size_panel: tuple[float, float],
    fig_dpi_panel: int,
    font_size_title: int,
    font_size_label: int,
    font_size_ticks: int,
    font_size_legend: int,
    period_order: Sequence[str],
    scenario_display: Mapping[str, str],
    # Naming/config
    alias_fn: Callable[[str], str],
    name_aliases: Mapping[str, str],
    varcfg: Mapping[str, Any],
    # Portfolio state callables
    portfolio_add_fn: Callable[..., None],
    portfolio_remove_fn: Callable[..., None],
    portfolio_contains_fn: Callable[..., bool],
    portfolio_key_fn: Callable[..., Any],
    portfolio_set_flash_fn: Callable[[str, str], None],
    portfolio_normalize_fn: Callable[[str], str],
    portfolio_remove_all_fn: Callable[[], None],
    build_portfolio_multiindex_df_fn: Callable[..., Any],
    # Master/schema callables used by portfolio UI
    load_master_csv_fn: Callable[..., Any],
    normalize_master_columns_fn: Callable[..., Any],
    parse_master_schema_fn: Callable[..., Any],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    load_master_and_schema_fn: Callable[..., Any],
    # Scenario comparison
    build_scenario_comparison_panel_for_row_fn: Callable[..., pd.DataFrame],
    make_scenario_comparison_figure_fn: Callable[..., Any],
) -> None:
    import streamlit as st

    from india_resilience_tool.analysis.metrics import compute_position_stats
    from india_resilience_tool.app.perf import render_perf_panel_safe
    from india_resilience_tool.app.point_selection_ui import render_point_selection_panel
    from india_resilience_tool.app.portfolio_ui import render_portfolio_panel
    from india_resilience_tool.app.views.details_panel import render_details_panel
    from india_resilience_tool.app.views.state_summary_view import render_state_summary_view
    from india_resilience_tool.data.master_columns import find_baseline_column_for_metric
    from india_resilience_tool.data.spatial_match import (
        extract_click_coords,
        extract_clicked_feature,
        resolve_matched_row,
    )

    def _render_climate_profile_header() -> None:
        """Render the right-panel header with an inline collapse control."""
        title_col, btn_col = st.columns([1, 0.2], vertical_alignment="center")
        with title_col:
            st.header("Climate Profile")
        with btn_col:
            if st.button(
                "⟩",
                key="btn_rhs_collapse",
                help="Collapse right panel",
                use_container_width=False,
                type="secondary",
            ):
                st.session_state["right_panel_collapsed"] = True
                st.rerun()

    # Reserved slot: "Selected district for portfolio" (map route) should appear ABOVE
    # the Portfolio analysis expander even though it's determined later in the script.
    portfolio_selected_slot = st.empty()

    # -------------------------
    # Multi-district/block portfolio mode: show a clean, guided right-panel flow
    # -------------------------
    analysis_mode_rhs = st.session_state.get("analysis_mode", "Single district focus")
    portfolio_route = st.session_state.get("portfolio_build_route", None)

    if "Multi" in str(analysis_mode_rhs):
        render_portfolio_panel(
            selected_state=selected_state,
            portfolio_route=portfolio_route,
            level=admin_level,
            variables=variables,
            variable_slug=variable_slug,
            index_group_labels=index_group_labels,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            metric_col=metric_col,
            merged=merged,
            adm1=adm1,
            pilot_state=pilot_state,
            data_dir=data_dir,
            compute_state_metrics_fn=lambda *_args, **_kwargs: ({}, None, None),
            load_master_csv_fn=load_master_csv_fn,
            normalize_master_columns_fn=normalize_master_columns_fn,
            parse_master_schema_fn=parse_master_schema_fn,
            resolve_metric_column_fn=resolve_metric_column_fn,
            find_baseline_column_for_stat_fn=find_baseline_column_for_stat_fn,
            risk_class_from_percentile_fn=risk_class_from_percentile_fn,
            portfolio_normalize_fn=portfolio_normalize_fn,
            portfolio_remove_fn=portfolio_remove_fn,
            portfolio_remove_all_fn=portfolio_remove_all_fn,
            build_portfolio_multiindex_df_fn=build_portfolio_multiindex_df_fn,
        )

    # -------------------------
    # Climate profile / point query panel
    # -------------------------
    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    portfolio_route = st.session_state.get("portfolio_build_route", None)
    clear_clicked = False

    is_portfolio_mode = "Multi" in str(analysis_mode)
    if is_portfolio_mode:
        # In any portfolio mode (multi-district or multi-block), keep the right panel
        # clean and driven by the portfolio panel above.
        render_perf_panel_safe()
        st.stop()

    _render_climate_profile_header()

    # --- Point-level query controls: only in portfolio mode AND only for the "saved points" route ---
    # NOTE: This block is legacy; portfolio mode is stopped above in current behavior.
    if "Multi" in str(analysis_mode) and portfolio_route == "saved_points":
        clear_clicked = render_point_selection_panel(
            portfolio_add_fn=portfolio_add_fn,
            portfolio_key_fn=portfolio_key_fn,
            portfolio_set_flash_fn=portfolio_set_flash_fn,
            level=admin_level,
        )

    clicked_feature = extract_clicked_feature(returned) if returned else None
    click_coords = extract_click_coords(returned) if returned else None

    if "Multi" in str(analysis_mode) and portfolio_route == "saved_points":
        if click_coords is not None and st.session_state.get("point_query_select_on_map", False):
            lat_click, lon_click = click_coords
            st.session_state["point_query_lat"] = lat_click
            st.session_state["point_query_lon"] = lon_click
            st.session_state["point_query_latlon"] = {"lat": lat_click, "lon": lon_click}
            st.session_state["point_query_select_on_map"] = False
            st.rerun()

        if clear_clicked:
            click_coords = None
        elif click_coords is None:
            point_query = st.session_state.get("point_query_latlon")
            if isinstance(point_query, dict):
                try:
                    lat_q = float(point_query.get("lat"))
                    lon_q = float(point_query.get("lon"))
                    click_coords = (lat_q, lon_q)
                except (TypeError, ValueError):
                    click_coords = None

    matched_row = resolve_matched_row(
        merged=merged,
        level=admin_level,
        clicked_feature=clicked_feature,
        click_coords=click_coords,
        selected_district=str(st.session_state.get("selected_district", "All")),
        selected_block=str(st.session_state.get("selected_block", "All")),
        selected_basin=str(st.session_state.get("selected_basin", "All")),
        selected_subbasin=str(st.session_state.get("selected_subbasin", "All")),
    )

    # ----------- STATE/DISTRICT SUMMARY MODE (no unit selected) -----------
    if str(admin_level).strip().lower() == "sub_basin":
        show_summary = selected_basin != "All" and selected_subbasin == "All"
    elif str(admin_level).strip().lower() == "basin":
        show_summary = False
    elif str(admin_level).strip().lower() == "block":
        show_summary = selected_state != "All" and selected_block == "All"
    else:
        show_summary = selected_state != "All" and selected_district == "All"

    if (matched_row is None or getattr(matched_row, "empty", True)) and show_summary:
        if "Multi" in str(analysis_mode):
            return
        render_state_summary_view(
            selected_state=selected_state,
            variables=variables,
            variable_slug=variable_slug,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            metric_col=metric_col,
            merged_gdf=merged,
            processed_root=processed_root,
            level=admin_level,
        )
        return

    # ----------- UNIT DETAILS MODE (district or block) -----------
    level_norm = str(admin_level).strip().lower()
    if level_norm == "sub_basin":
        unit_label = "sub-basin"
    elif level_norm == "basin":
        unit_label = "basin"
    else:
        unit_label = "block" if level_norm == "block" else "district"

    if matched_row is None or getattr(matched_row, "empty", True):
        st.warning(f"No {unit_label}-level data found for the current selection.")
        if "Multi" in str(analysis_mode):
            st.info(
                f"In portfolio mode, add {unit_label}s via **From the map**, **From saved points**, "
                f"or **From the rankings table** (Portfolio analysis panel)."
            )
        else:
            st.info(
                f"Please choose a different {unit_label} from the sidebar, or select **All** "
                f"to view the {'district' if str(admin_level).strip().lower() == 'block' else 'state'} summary."
            )
        st.stop()

    row = matched_row.iloc[0]
    district_name = row.get("district_name", "Unknown")
    block_name = row.get("block_name", "Unknown") if level_norm == "block" else None
    basin_name = row.get("basin_name", "Unknown")
    subbasin_name = row.get("subbasin_name", "Unknown") if level_norm == "sub_basin" else None
    state_to_show = (
        st.session_state.get("selected_state")
        if st.session_state.get("selected_state") != "All"
        else (row.get("state_name") or ("Hydro" if spatial_family == "hydro" else "Unknown"))
    )

    # --- Compact selection view in Multi-unit portfolio mode ---
    if "Multi" in str(analysis_mode):
        portfolio_route = st.session_state.get("portfolio_build_route", None)
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

                already_in = bool(portfolio_contains_fn(state_to_show, district_name))

                c_add, c_remove = st.columns(2)
                with c_add:
                    if not already_in and st.button(
                        "Add to portfolio",
                        key=f"btn_add_portfolio_maproute_{portfolio_normalize_fn(state_to_show)}_{portfolio_normalize_fn(district_name)}",
                        use_container_width=True,
                    ):
                        portfolio_add_fn(state_to_show, district_name)
                        st.session_state["portfolio_flash"] = (
                            f"Added {district_name}, {state_to_show} to portfolio."
                        )
                        st.rerun()
                    elif already_in:
                        st.success("Already in portfolio")
                with c_remove:
                    if already_in and st.button(
                        "Remove",
                        key=f"btn_remove_portfolio_maproute_{portfolio_normalize_fn(state_to_show)}_{portfolio_normalize_fn(district_name)}",
                        use_container_width=True,
                    ):
                        portfolio_remove_fn(state_to_show, district_name)
                        st.session_state["portfolio_flash"] = (
                            f"Removed {district_name}, {state_to_show} from portfolio."
                        )
                        st.rerun()

                st.caption(
                    f"Portfolio size: {len(st.session_state.get('portfolio_districts', []))} district(s)"
                )

        # In portfolio mode, do NOT render the full climate profile below.
        render_perf_panel_safe()
        st.stop()

    # --- Full unit climate profile (single-district/block focus mode) ---
    if click_coords is not None:
        if level_norm == "sub_basin":
            unit_label_display = "sub-basin"
        elif level_norm == "basin":
            unit_label_display = "basin"
        else:
            unit_label_display = "block" if level_norm == "block" else "district"
        st.caption(
            f"Point location used: lat {click_coords[0]:.4f}, "
            f"lon {click_coords[1]:.4f} (assigned to this {unit_label_display})."
        )

    # ---- Risk cards (1.1) ----
    current_val = row.get(metric_col)
    current_val_f = float(current_val) if not pd.isna(current_val) else None

    baseline_col = find_baseline_column_for_metric(df.columns, base_metric=sel_metric)
    baseline_val = row.get(baseline_col) if baseline_col else np.nan
    baseline_val_f = float(baseline_val) if not pd.isna(baseline_val) else None

    rank_higher_is_worse = bool(varcfg.get("rank_higher_is_worse", True))

    # Prefer median for ranking if available (more robust to outliers)
    rank_metric_col = metric_col
    try:
        parts = str(metric_col).split("__")
        if len(parts) == 4 and parts[-1] == "mean":
            cand = "__".join(parts[:-1] + ["median"])
            if cand in df.columns:
                rank_metric_col = cand
    except Exception:
        pass

    # State distribution (districts or blocks)
    rank_in_state = None
    n_in_state = None
    percentile_state = None
    try:
        if "state_name" in merged.columns:
            in_state_mask = (
                merged["state_name"].astype(str).str.strip().str.lower()
                == str(state_to_show).strip().lower()
            )
        else:
            in_state_mask = pd.Series(True, index=merged.index)
        state_vals = pd.to_numeric(merged.loc[in_state_mask, rank_metric_col], errors="coerce").dropna()
        pos_state = compute_position_stats(
            state_vals, current_val_f, higher_is_worse=rank_higher_is_worse
        )
        rank_in_state, n_in_state, percentile_state = (
            pos_state.rank,
            pos_state.n,
            pos_state.percentile,
        )
    except Exception:
        pass

    # District distribution (blocks only)
    rank_in_district = None
    n_in_district = None
    percentile_district = None
    try:
        if str(admin_level).strip().lower() == "block":
            in_dist_mask = (
                (merged["state_name"].astype(str).str.strip().str.lower() == str(state_to_show).strip().lower())
                & (merged["district_name"].astype(str).str.strip().str.lower() == str(district_name).strip().lower())
            )
            dist_vals = pd.to_numeric(merged.loc[in_dist_mask, rank_metric_col], errors="coerce").dropna()
            pos_dist = compute_position_stats(
                dist_vals, current_val_f, higher_is_worse=rank_higher_is_worse
            )
            rank_in_district, n_in_district, percentile_district = (
                pos_dist.rank,
                pos_dist.n,
                pos_dist.percentile,
            )
    except Exception:
        pass

    # ---- Time series + case study dependencies ----
    # These delegates already live in analysis/ and are Streamlit-free.
    @st.cache_data
    def _load_district_yearly(
        ts_root: Path,
        state_dir: str,
        district_display: str,
        scenario_name: str,
        varcfg: Mapping[str, Any],
        aliases: Mapping[str, str] | None = None,
    ) -> pd.DataFrame:
        from india_resilience_tool.analysis.timeseries import load_district_yearly

        return load_district_yearly(
            ts_root=ts_root,
            state_dir=state_dir,
            district_display=district_display,
            scenario_name=scenario_name,
            varcfg=dict(varcfg),
            aliases=dict(aliases) if aliases else None,
            normalize_fn=alias_fn,
        )

    @st.cache_data
    def _load_block_yearly(
        ts_root: Path,
        state_dir: str,
        district_display: str,
        block_display: str,
        scenario_name: str,
        varcfg: Mapping[str, Any],
        aliases: Mapping[str, str] | None = None,
    ) -> pd.DataFrame:
        from india_resilience_tool.analysis.timeseries import load_block_yearly

        return load_block_yearly(
            ts_root=ts_root,
            state_dir=state_dir,
            district_display=district_display,
            block_display=block_display,
            scenario_name=scenario_name,
            varcfg=dict(varcfg),
            aliases=dict(aliases) if aliases else None,
            normalize_fn=alias_fn,
        )

    @st.cache_data
    def _load_hydro_yearly(
        ts_root: Path,
        hydro_level: str,
        basin_display: str,
        subbasin_display: str | None,
        scenario_name: str,
    ) -> pd.DataFrame:
        from india_resilience_tool.analysis.timeseries import load_hydro_yearly

        return load_hydro_yearly(
            ts_root=ts_root,
            level="sub_basin" if hydro_level == "sub_basin" else "basin",
            basin_display=basin_display,
            subbasin_display=subbasin_display,
            scenario_name=scenario_name,
        )

    def _filter_series_for_trend(
        series_df: pd.DataFrame,
        state_name: str,
        district_name_in: str,
        block_name_in: Optional[str] = None,
    ) -> pd.DataFrame:
        if series_df is None or series_df.empty:
            return pd.DataFrame()

        d = series_df.copy()

        if "block_name" in d.columns and "block" not in d.columns:
            d["block"] = d["block_name"]

        def _n(s: str) -> str:
            return alias_fn(s)

        d["_state_key"] = d["state"].astype(str).map(_n) if "state" in d.columns else ""
        d["_district_key"] = d["district"].astype(str).map(_n) if "district" in d.columns else ""

        mask = (d["_state_key"] == _n(state_name)) & (d["_district_key"] == _n(district_name_in))

        if block_name_in and ("block" in d.columns):
            d["_block_key"] = d["block"].astype(str).map(_n)
            mask = mask & (d["_block_key"] == _n(block_name_in))

        if not bool(getattr(mask, "any", lambda: False)()):
            mask = (d["_state_key"] == _n(state_name)) & d["_district_key"].str.contains(
                _n(district_name_in), na=False
            )
            if block_name_in and ("block" in d.columns):
                d["_block_key"] = d["block"].astype(str).map(_n)
                mask = mask & d["_block_key"].str.contains(_n(block_name_in), na=False)

        d = d[mask]
        if d.empty:
            return d

        for c in ("year", "mean", "p05", "p95"):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")
        d = d.dropna(subset=["year", "mean"]).sort_values("year")
        return d

    from india_resilience_tool.app.case_study_runtime import (
        make_case_study_zip_with_labels,
        make_district_case_study_builder,
    )
    from india_resilience_tool.viz.charts import (
        create_trend_figure_for_index_plotly as _create_trend_figure_for_index,
    )
    from india_resilience_tool.viz.exports import make_district_case_study_pdf as _make_district_case_study_pdf
    from india_resilience_tool.viz.style import ensure_16x9_figsize
    from india_resilience_tool.data.discovery import slugify_fs

    _build_district_case_study_data = make_district_case_study_builder(
        variables=variables,
        data_dir=data_dir,
        pilot_state=pilot_state,
        load_master_and_schema_fn=load_master_and_schema_fn,
        portfolio_normalize_fn=portfolio_normalize_fn,
        alias_fn=alias_fn,
        name_aliases=dict(name_aliases),
        load_district_yearly_fn=_load_district_yearly,
        filter_series_for_trend_fn=_filter_series_for_trend,
        find_baseline_column_for_stat_fn=find_baseline_column_for_stat_fn,
        build_scenario_comparison_panel_for_row_fn=build_scenario_comparison_panel_for_row_fn,
        risk_class_from_percentile_fn=risk_class_from_percentile_fn,
    )

    _make_case_study_zip = make_case_study_zip_with_labels(variables=variables)

    requested_state_dir = (
        selected_state if selected_state != "All" else (row.get("state_name") or pilot_state)
    )
    state_dir_for_fs = "hydro" if spatial_family == "hydro" else requested_state_dir
    district_for_fs = row.get("district_name") or selected_district
    block_for_fs = row.get("block_name") or selected_block

    if level_norm == "sub_basin":
        yearly_hist = _load_hydro_yearly(
            ts_root=processed_root,
            hydro_level="sub_basin",
            basin_display=str(basin_name),
            subbasin_display=str(subbasin_name),
            scenario_name="historical",
        )
        yearly_scen = _load_hydro_yearly(
            ts_root=processed_root,
            hydro_level="sub_basin",
            basin_display=str(basin_name),
            subbasin_display=str(subbasin_name),
            scenario_name=sel_scenario,
        )
        hist_ts = yearly_hist
        scen_ts = yearly_scen
    elif level_norm == "basin":
        yearly_hist = _load_hydro_yearly(
            ts_root=processed_root,
            hydro_level="basin",
            basin_display=str(basin_name),
            subbasin_display=None,
            scenario_name="historical",
        )
        yearly_scen = _load_hydro_yearly(
            ts_root=processed_root,
            hydro_level="basin",
            basin_display=str(basin_name),
            subbasin_display=None,
            scenario_name=sel_scenario,
        )
        hist_ts = yearly_hist
        scen_ts = yearly_scen
    elif level_norm == "block" and selected_block != "All":
        yearly_hist = _load_block_yearly(
            ts_root=processed_root,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            block_display=str(block_for_fs),
            scenario_name="historical",
            varcfg=varcfg,
            aliases=name_aliases,
        )
        yearly_scen = _load_block_yearly(
            ts_root=processed_root,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            block_display=str(block_for_fs),
            scenario_name=sel_scenario,
            varcfg=varcfg,
            aliases=name_aliases,
        )
        hist_ts = _filter_series_for_trend(yearly_hist, state_to_show, district_name, str(block_for_fs))
        scen_ts = _filter_series_for_trend(yearly_scen, state_to_show, district_name, str(block_for_fs))
    else:
        yearly_hist = _load_district_yearly(
            ts_root=processed_root,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            scenario_name="historical",
            varcfg=varcfg,
            aliases=name_aliases,
        )
        yearly_scen = _load_district_yearly(
            ts_root=processed_root,
            state_dir=str(state_dir_for_fs),
            district_display=str(district_for_fs),
            scenario_name=sel_scenario,
            varcfg=varcfg,
            aliases=name_aliases,
        )
        hist_ts = _filter_series_for_trend(yearly_hist, state_to_show, district_name)
        scen_ts = _filter_series_for_trend(yearly_scen, state_to_show, district_name)

    _fig_size_panel = ensure_16x9_figsize(fig_size_panel, mode="fit_width")

    render_details_panel(
        row=row,
        district_name=(
            str(subbasin_name)
            if level_norm == "sub_basin"
            else str(basin_name) if level_norm == "basin" else district_name
        ),
        state_to_show=state_to_show,
        selected_district=selected_district,
        variables=variables,
        variable_slug=variable_slug,
        sel_metric=sel_metric,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        current_val_f=current_val_f,
        baseline_val_f=baseline_val_f,
        baseline_col=baseline_col,
        rank_in_state=rank_in_state,
        n_in_state=n_in_state,
        percentile_state=percentile_state,
        rank_higher_is_worse=rank_higher_is_worse,
        hist_ts=hist_ts,
        scen_ts=scen_ts,
        schema_items=schema_items,
        fig_size_panel=_fig_size_panel,
        fig_dpi_panel=fig_dpi_panel,
        font_size_title=font_size_title,
        font_size_label=font_size_label,
        font_size_ticks=font_size_ticks,
        font_size_legend=font_size_legend,
        period_order=period_order,
        scenario_display=scenario_display,
        create_trend_figure_fn=_create_trend_figure_for_index,
        build_scenario_panel_fn=build_scenario_comparison_panel_for_row_fn,
        make_scenario_figure_fn=make_scenario_comparison_figure_fn,
        build_case_study_data_fn=_build_district_case_study_data,
        make_case_study_pdf_fn=_make_district_case_study_pdf,
        make_case_study_zip_fn=_make_case_study_zip,
        slugify_fs_fn=slugify_fs,
        state_dir_for_fs=state_dir_for_fs,
        district_for_fs=district_for_fs,
        ts_root=processed_root,
        logo_path=logo_path,
        level=admin_level,
        block_name=str(block_for_fs) if (level_norm == "block" and selected_block != "All") else None,
        parent_district_name=str(district_name) if level_norm == "block" else None,
        rank_in_district=rank_in_district,
        n_in_district=n_in_district,
        percentile_district=percentile_district,
    )
