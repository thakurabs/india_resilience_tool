"""
Dashboard runtime entry (canonical) for the India Resilience Tool (IRT).

The canonical entrypoint is `run_app()`, which runs on every Streamlit rerun.
"""

from __future__ import annotations


def run_app() -> None:
    """Run the Streamlit dashboard."""
    import os
    from pathlib import Path

    import pandas as pd
    import streamlit as st

    from paths import (
        BASINS_PATH,
        BLOCKS_PATH,
        DATA_DIR,
        DISTRICTS_PATH,
        SUBBASINS_PATH,
        resolve_processed_root,
    )

    from india_resilience_tool.app.geo_cache import (
        build_adm1_from_adm2,
        enrich_adm2_with_state_names,
        load_local_adm2,
        load_local_adm3,
        load_local_basin,
        load_local_subbasin,
    )
    from india_resilience_tool.app.geography_controls import render_geography_and_analysis_focus
    from india_resilience_tool.app.master_freshness import (
        master_needs_rebuild,
        state_profile_files_missing,
    )
    from india_resilience_tool.app.perf import (
        perf_reset,
        perf_section,
        render_perf_panel_safe,
    )
    from india_resilience_tool.app.ribbon import render_metric_ribbon
    from india_resilience_tool.app.sidebar import (
        apply_jump_once_flags,
        render_admin_level_selector,
        render_hover_toggle_if_portfolio,
        render_spatial_family_selector,
    )
    from india_resilience_tool.app.sidebar_branding import render_sidebar_branding
    from india_resilience_tool.app.state import VIEW_MAP, VIEW_RANKINGS
    from india_resilience_tool.config.constants import (
        FIG_DPI_PANEL,
        FIG_SIZE_PANEL,
        FONT_SIZE_LABEL,
        FONT_SIZE_LEGEND,
        FONT_SIZE_TICKS,
        FONT_SIZE_TITLE,
        LOGO_PATH,
        MAX_LAT,
        MAX_LON,
        MIN_LAT,
        MIN_LON,
        SIMPLIFY_TOL_ADM2,
        SIMPLIFY_TOL_ADM3,
    )
    from india_resilience_tool.config.variables import INDEX_GROUP_LABELS, VARIABLES
    from india_resilience_tool.data.master_columns import (
        find_baseline_column_for_stat,
        resolve_metric_column,
    )
    from india_resilience_tool.data.master_loader import (
        load_master_csv,
        normalize_master_columns,
        parse_master_schema,
    )
    from india_resilience_tool.utils.naming import NAME_ALIASES, alias, normalize_name
    from india_resilience_tool.viz.charts import (
        PERIOD_ORDER,
        SCENARIO_DISPLAY,
        build_scenario_comparison_panel_for_row,
        make_scenario_comparison_figure_dashboard,
    )
    from india_resilience_tool.analysis.portfolio import (
        build_portfolio_multiindex_df as _build_portfolio_multiindex_df_impl,
    )

    from india_resilience_tool.app.map_pipeline import build_map_and_rankings

    DEBUG = bool(int(os.getenv("IRT_DEBUG", "0")))

    st.set_page_config(page_title="India Resilience Tool", layout="wide")

    # Selection placeholders (force deliberate choices)
    SEL_PLACEHOLDER = "— Select —"

    # Perf timing toggle (developer)
    st.session_state.setdefault("perf_enabled", DEBUG)
    perf_reset()

    # If a downstream control requested to jump to a specific left-panel view,
    # honour it BEFORE the main_view_selector radio is created.
    apply_jump_once_flags()

    # Data paths
    ADM2_GEOJSON = DISTRICTS_PATH
    ADM3_GEOJSON = BLOCKS_PATH
    BASIN_GEOJSON = BASINS_PATH
    SUBBASIN_GEOJSON = SUBBASINS_PATH

    ATTACH_DISTRICT_GEOJSON = str(ADM2_GEOJSON) if ADM2_GEOJSON.exists() else None

    with st.sidebar:
        render_sidebar_branding(logo_path=LOGO_PATH)

        spatial_family = render_spatial_family_selector(label_visibility="collapsed")

        # Family-aware level selector
        admin_level = render_admin_level_selector(
            label_visibility="collapsed",
            centered=True,
            center_layout=(1, 8, 1),
        )

        # Read current analysis mode (default depends on admin level)
        if admin_level == "sub_basin":
            default_mode = "Single sub-basin focus"
        elif admin_level == "basin":
            default_mode = "Single basin focus"
        elif admin_level == "block":
            default_mode = "Single block focus"
        else:
            default_mode = "Single district focus"
        analysis_mode_current = st.session_state.get("analysis_mode", default_mode)

        # Show hover toggle (always visible)
        _ = render_hover_toggle_if_portfolio(analysis_mode_current)

        state_placeholder = st.empty()
        metric_ui_placeholder = st.empty()
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

    # --- Split-pane layout CSS (left stays visible; right scrolls internally) ---
    def _inject_split_pane_css() -> None:
        st.markdown(
            """
            <style>
            :root {
              /* Tune if the right panel feels too tall/short on your screen. */
              --irt-pane-top-offset: 8.5rem;
              --irt-rhs-collapsed-width: 3.25rem;
              --irt-rhs-transition-ms: 200ms;
            }

            .irt-left-workspace-marker,
            .irt-rhs-scroll-marker,
            .irt-rhs-rail-marker {
              display: none;
            }

            /* Smoothly animate the main two-column layout (left workspace + right panel). */
            [data-testid="stMainBlockContainer"]
              div[data-testid="stHorizontalBlock"]:has(.irt-left-workspace-marker):has(.irt-rhs-rail-marker)
              div[data-testid="column"] {
              transition:
                flex-basis var(--irt-rhs-transition-ms) ease,
                flex-grow var(--irt-rhs-transition-ms) ease,
                width var(--irt-rhs-transition-ms) ease,
                max-width var(--irt-rhs-transition-ms) ease;
              will-change: flex-basis, flex-grow, width, max-width;
            }

            /* Visual separator between workspace and right panel. */
            [data-testid="stMainBlockContainer"] div[data-testid="column"]:has(.irt-rhs-rail-marker) {
              border-left: 1px solid rgba(148, 163, 184, 0.22);
              padding-left: 0.2rem;
            }

            /* Compact, icon-style toggle buttons for collapsing/expanding the right panel. */
            button[title="Collapse right panel"],
            button[title="Expand right panel"] {
              width: 2.2rem !important;
              min-width: 2.2rem !important;
              max-width: 2.2rem !important;
              height: 2.2rem !important;
              padding: 0 !important;
              border-radius: 0.7rem !important;
              border: 1px solid rgba(148, 163, 184, 0.45) !important;
              background: rgba(255, 255, 255, 0.92) !important;
              color: rgba(51, 65, 85, 0.92) !important;
              box-shadow: 0 1px 2px rgba(0, 0, 0, 0.06) !important;
              display: inline-flex !important;
              align-items: center !important;
              justify-content: center !important;
              line-height: 1 !important;
              font-weight: 900 !important;
              font-size: 1.05rem !important;
            }
            button[title="Collapse right panel"]:hover,
            button[title="Expand right panel"]:hover {
              border-color: rgba(148, 163, 184, 0.7) !important;
              background: rgba(248, 250, 252, 0.98) !important;
              color: rgba(30, 41, 59, 0.98) !important;
              box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08) !important;
            }
            button[title="Collapse right panel"]:active,
            button[title="Expand right panel"]:active {
              transform: translateY(1px);
            }

            /* Make the right panel scroll inside its own container (not the page). */
            [data-testid="stMainBlockContainer"] div[data-testid="stVerticalBlock"]:has(.irt-rhs-scroll-marker) {
              height: calc(100vh - var(--irt-pane-top-offset));
              max-height: calc(100vh - var(--irt-pane-top-offset));
              overflow-y: auto;
              overflow-x: hidden;
              padding-right: 0.25rem;
              scrollbar-gutter: stable;
            }

            /* Keep the left workspace visible when the page scrolls for any reason. */
            [data-testid="stMainBlockContainer"] div[data-testid="stVerticalBlock"]:has(.irt-left-workspace-marker) {
              position: sticky;
              top: var(--irt-pane-top-offset);
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

    _inject_split_pane_css()

    # Main layout: left (workspace) + right (panel: scrollable, collapsible)
    rhs_collapsed = bool(st.session_state.get("right_panel_collapsed", False))

    col_weights = [5, 3] if not rhs_collapsed else [9.4, 0.6]
    col1, col2 = st.columns(col_weights)

    with col1:
        left_root = st.container()
    with col2:
        right_header = st.container()
        right_body = st.container()

    with left_root:
        st.markdown('<div class="irt-left-workspace-marker"></div>', unsafe_allow_html=True)
    with right_header:
        st.markdown('<div class="irt-rhs-rail-marker"></div>', unsafe_allow_html=True)

    # -------------------------
    # Load base admin boundaries (ADM2 is always required)
    # -------------------------
    if not ADM2_GEOJSON.exists():
        st.error(
            f"ADM2 geojson not found at {ADM2_GEOJSON}. Place your districts_4326.geojson at this path."
        )
        render_perf_panel_safe()
        st.stop()

    adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)
    if "__key" not in adm2.columns and "district_name" in adm2.columns:
        adm2["__key"] = adm2["district_name"].map(alias)

    # -------------------------
    # Metric selection ribbon (bundle → metric → scenario/period/stat + map mode)
    # -------------------------
    ribbon_ctx = render_metric_ribbon(
        col=left_root,
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
    registry_metric = ribbon_ctx.registry_metric
    sel_metric = str(st.session_state.get("registry_metric", registry_metric) or "").strip()
    sel_scenario = ribbon_ctx.sel_scenario
    sel_scenario_display = ribbon_ctx.sel_scenario_display
    sel_period = ribbon_ctx.sel_period
    sel_stat = ribbon_ctx.sel_stat
    map_mode = ribbon_ctx.map_mode
    metric_col = ribbon_ctx.metric_col
    _ribbon_ready = ribbon_ctx.ribbon_ready
    rebuild_master_csv_if_needed = ribbon_ctx.rebuild_master_csv_if_needed
    _load_master_and_schema = ribbon_ctx.load_master_and_schema_fn

    # -------------------------
    # Master controls (sidebar)
    # -------------------------
    with master_controls_placeholder.container():
        st.markdown("#### Master CSV controls")
        col_a, col_b = st.columns(2)
        with col_a:
            auto_check = st.button("Check / Rebuild master (auto)", key="btn_auto_check")
        with col_b:
            force_btn = st.button("Rebuild now", key="btn_force_rebuild")

    if auto_check:
        ok, msg = rebuild_master_csv_if_needed(force=False, attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON)
        if ok:
            st.success("Master CSV rebuilt or already up-to-date.")
        else:
            st.info(f"Master CSV status: {msg}")

    if force_btn:
        ok, msg = rebuild_master_csv_if_needed(force=True, attach_centroid_geojson=ATTACH_DISTRICT_GEOJSON)
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
        spatial_family=spatial_family,
        admin_level=admin_level,
        processed_root=PROCESSED_ROOT,
        sel_placeholder=SEL_PLACEHOLDER,
        view_map=VIEW_MAP,
        view_rankings=VIEW_RANKINGS,
        adm1=adm1,
        adm2=adm2,
        adm3_geojson=ADM3_GEOJSON,
        basins_geojson=BASIN_GEOJSON,
        subbasins_geojson=SUBBASIN_GEOJSON,
        simplify_tol_adm3=SIMPLIFY_TOL_ADM3,
    )

    analysis_ready = geo_ctx.analysis_ready
    selected_state = geo_ctx.selected_state
    selected_district = geo_ctx.selected_district
    selected_block = geo_ctx.selected_block
    selected_basin = geo_ctx.selected_basin
    selected_subbasin = geo_ctx.selected_subbasin
    gdf_state_districts = geo_ctx.gdf_state_districts

    from india_resilience_tool.app.portfolio_state_runtime import (
        _portfolio_add,
        _portfolio_clear,
        _portfolio_contains,
        _portfolio_key,
        _portfolio_normalize,
        _portfolio_remove,
        _portfolio_set_flash,
    )

    # Alias for backward compatibility with portfolio_ui
    _portfolio_remove_all = _portfolio_clear

    st.session_state.setdefault("map_center", [25.0, 82.5])
    st.session_state.setdefault("map_zoom", 4.0)

    # Map zoom logic: handle district and block selections
    _admin_level_for_zoom = str(st.session_state.get("admin_level", "district") or "district").strip().lower()

    if _admin_level_for_zoom == "block" and selected_block != "All" and selected_district != "All":
        # Need merged block geometries; do it after merge.
        st.session_state["_pending_block_zoom"] = {
            "state": selected_state,
            "district": selected_district,
            "block": selected_block,
        }
    elif _admin_level_for_zoom == "sub_basin" and selected_subbasin != "All":
        st.session_state.pop("_pending_block_zoom", None)
    elif _admin_level_for_zoom == "basin" and selected_basin != "All":
        st.session_state.pop("_pending_block_zoom", None)
    elif selected_district != "All":
        district_row = gdf_state_districts[gdf_state_districts["district_name"] == selected_district]
        if not district_row.empty:
            centroid = district_row.iloc[0].geometry.centroid
            st.session_state["map_center"] = [float(centroid.y), float(centroid.x)]
            st.session_state["map_zoom"] = 10 if _admin_level_for_zoom == "block" else 9
        st.session_state.pop("_pending_block_zoom", None)
    elif selected_state != "All":
        state_row = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
        if not state_row.empty:
            b = state_row.iloc[0].geometry.bounds
            st.session_state["map_center"] = [float((b[1] + b[3]) / 2), float((b[0] + b[2]) / 2)]
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
    _admin_level = str(st.session_state.get("admin_level", "district") or "district").strip().lower()

    # Load ADM3 boundaries only when needed
    if _admin_level == "block":
        if not ADM3_GEOJSON.exists():
            st.error(f"ADM3 geojson not found at {ADM3_GEOJSON}. Please provide block_4326.geojson.")
            render_perf_panel_safe()
            st.stop()
        adm3 = load_local_adm3(str(ADM3_GEOJSON), tolerance=SIMPLIFY_TOL_ADM3)
    elif _admin_level == "basin":
        if not BASIN_GEOJSON.exists():
            st.error(f"Basin geojson not found at {BASIN_GEOJSON}. Please provide basins.geojson.")
            render_perf_panel_safe()
            st.stop()
        adm3 = load_local_basin(str(BASIN_GEOJSON))
    elif _admin_level == "sub_basin":
        if not SUBBASIN_GEOJSON.exists():
            st.error(f"Sub-basin geojson not found at {SUBBASIN_GEOJSON}. Please provide subbasins.geojson.")
            render_perf_panel_safe()
            st.stop()
        adm3 = load_local_subbasin(str(SUBBASIN_GEOJSON))
    else:
        adm3 = None

    # Require deliberate Analysis focus + Map mode selection before building the map
    _analysis_mode = st.session_state.get("analysis_mode", SEL_PLACEHOLDER)
    _map_mode = st.session_state.get("map_mode", SEL_PLACEHOLDER)
    if (_analysis_mode == SEL_PLACEHOLDER) or (_map_mode == SEL_PLACEHOLDER):
        st.info(
            "Select an **Analysis focus** in the sidebar and complete the **ribbon selections** above the map to render the map."
        )
        render_perf_panel_safe()
        st.stop()

    # Build map + rankings artifacts
    MAP_WIDTH, MAP_HEIGHT = 780, 700
    pending_zoom = st.session_state.pop("_pending_block_zoom", None)

    artifacts = build_map_and_rankings(
        adm_level=_admin_level,
        adm1=adm1,
        adm2=adm2,
        adm3=adm3,
        df=df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
        master_csv_path=MASTER_CSV_PATH if MASTER_CSV_PATH is not None else Path("."),
        variable_slug=str(VARIABLE_SLUG or ""),
        varcfg=VARCFG or {},
        sel_metric=sel_metric,
        sel_scenario_display=sel_scenario_display,
        sel_period=sel_period,
        sel_stat=sel_stat,
        metric_col=str(metric_col or ""),
        map_mode=map_mode,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_block=selected_block,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
        spatial_family=spatial_family,
        hover_enabled=bool(st.session_state.get("hover_enabled", True)),
        map_center=list(st.session_state["map_center"]),
        map_zoom=float(st.session_state["map_zoom"]),
        bounds_latlon=[[MIN_LAT, MIN_LON], [MAX_LAT, MAX_LON]],
        pending_block_zoom=pending_zoom,
        normalize_state_fn=normalize_name,
        adm2_geojson_path=ADM2_GEOJSON,
        adm3_geojson_path=ADM3_GEOJSON,
        basin_geojson_path=BASIN_GEOJSON,
        subbasin_geojson_path=SUBBASIN_GEOJSON,
        simplify_tol_adm2=SIMPLIFY_TOL_ADM2,
        simplify_tol_adm3=SIMPLIFY_TOL_ADM3,
        map_height=MAP_HEIGHT,
        color_slider_placeholder=color_slider_placeholder,
        perf_section=perf_section,
        render_perf_panel_safe=render_perf_panel_safe,
    )

    from india_resilience_tool.app.left_panel_runtime import render_left_panel

    returned, _view = render_left_panel(
        col=left_root,
        m=artifacts.folium_map,
        legend_block_html=artifacts.legend_block_html,
        map_mode=artifacts.map_mode,
        map_width=MAP_WIDTH,
        map_height=MAP_HEIGHT,
        perf_section=perf_section,
        variable_slug=str(VARIABLE_SLUG or ""),
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_block=selected_block,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
        level=_admin_level,
        table_df=artifacts.table_df,
        has_baseline=artifacts.has_baseline,
        variables=VARIABLES,
        variable_slug_for_rankings=str(VARIABLE_SLUG or ""),
        portfolio_add_fn=_portfolio_add,
        portfolio_contains_fn=_portfolio_contains,
        portfolio_remove_fn=_portfolio_remove,
        portfolio_normalize_fn=_portfolio_normalize,
        merged=artifacts.merged,
    )

    if rhs_collapsed:
        with right_header:
            _, btn_col = st.columns([1, 0.25])
            with btn_col:
                if st.button(
                    "⟨",
                    key="btn_rhs_expand",
                    help="Expand right panel",
                    use_container_width=False,
                    type="secondary",
                ):
                    st.session_state["right_panel_collapsed"] = False
                    st.rerun()
    else:
        with right_header:
            _, btn_col = st.columns([1, 0.25])
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

        with right_body:
            st.markdown('<div class="irt-rhs-scroll-marker"></div>', unsafe_allow_html=True)

            from india_resilience_tool.app.details_runtime import render_right_panel
            from india_resilience_tool.analysis.metrics import risk_class_from_percentile

            # Adapt the Streamlit-free helper (keyword-only) to the older positional
            # signature expected by the case-study builder.
            def _find_baseline_column_for_stat_compat(cols: object, metric: str, stat: str) -> str | None:
                return find_baseline_column_for_stat(cols, base_metric=metric, stat=stat)

            render_right_panel(
                returned=returned,
                selected_state=selected_state,
                selected_district=selected_district,
                selected_block=selected_block,
                admin_level=_admin_level,
                spatial_family=spatial_family,
                selected_basin=selected_basin,
                selected_subbasin=selected_subbasin,
                variables=VARIABLES,
                variable_slug=str(VARIABLE_SLUG or ""),
                index_group_labels=INDEX_GROUP_LABELS,
                sel_metric=sel_metric,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                metric_col=str(metric_col or ""),
                merged=artifacts.merged,
                adm1=adm1,
                df=df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
                schema_items=schema_items,
                processed_root=PROCESSED_ROOT if PROCESSED_ROOT is not None else Path("."),
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
                varcfg=VARCFG or {},
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
                find_baseline_column_for_stat_fn=_find_baseline_column_for_stat_compat,
                risk_class_from_percentile_fn=risk_class_from_percentile,
                load_master_and_schema_fn=_load_master_and_schema,
                build_scenario_comparison_panel_for_row_fn=build_scenario_comparison_panel_for_row,
                make_scenario_comparison_figure_fn=make_scenario_comparison_figure_dashboard,
            )

    render_perf_panel_safe()
    st.markdown("---")
    st.caption(
        "Notes: first choose a Risk domain (e.g. Heat Risk, Drought Risk), then a Metric within that bundle. "
        "Details panel shows risk cards, trends, narrative, and case-study export."
    )
