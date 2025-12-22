"""
Multi-district portfolio panel (right column) for IRT.

This module extracts the portfolio-mode right-column UI from
dashboard_unfactored_impl.py. It includes:
- State summary (shown when portfolio_route is None)
- Step 1: Route chooser buttons (rankings/map/saved_points)
- Step 2: Index selection for portfolio comparison
- Step 3: Multi-index comparison table builder
- Step 4: Review and edit portfolio districts

Widget keys preserved:
- btn_portfolio_route_rankings
- btn_portfolio_route_map
- btn_portfolio_route_saved_points
- btn_portfolio_route_reset
- portfolio_multiindex_selection
- btn_build_multiindex_portfolio_table
- btn_open_rankings_from_summary
- btn_portfolio_remove_all
- btn_portfolio_remove_all_confirm
- btn_portfolio_remove_all_cancel
- btn_portfolio_remove_{state}_{district}

Session state keys used:
- portfolio_build_route
- portfolio_districts
- portfolio_multiindex_selection
- portfolio_multiindex_df
- portfolio_multiindex_context
- portfolio_flash
- confirm_clear_portfolio
- jump_to_rankings
- jump_to_map
- point_query_points

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence

import numpy as np
import pandas as pd


def render_state_summary(
    *,
    selected_state: str,
    variable_label: str,
    sel_scenario: str,
    sel_period: str,
    merged: Any,  # GeoDataFrame
    adm1: Any,  # GeoDataFrame
    metric_col: str,
    compute_state_metrics_fn: Callable[..., tuple[dict, Any, Any]],
) -> None:
    """Render the state summary section (shown when portfolio_route is None)."""
    import streamlit as st

    st.subheader(f"{selected_state} — State summary")
    st.markdown(
        f"**Index:** {variable_label}  \n"
        f"**Scenario:** {sel_scenario}  \n"
        f"**Period:** {sel_period}"
    )

    if selected_state == "All":
        st.info("Select a state in the left panel to see a state summary and build a portfolio.")
    else:
        try:
            ensemble_port, _, _ = compute_state_metrics_fn(
                merged, adm1, metric_col, selected_state
            )
        except Exception:
            ensemble_port = {
                "mean": None,
                "median": None,
                "p05": None,
                "p95": None,
                "std": None,
                "n_districts": 0,
            }

        def _fmt_metric(v: object) -> str:
            try:
                x = float(v)  # type: ignore[arg-type]
                if np.isnan(x):
                    return "—"
                return f"{x:.2f}"
            except Exception:
                return "—"

        if ensemble_port.get("n_districts", 0) > 0:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Mean", _fmt_metric(ensemble_port.get("mean")))
            c2.metric("Median", _fmt_metric(ensemble_port.get("median")))
            c3.metric("P05", _fmt_metric(ensemble_port.get("p05")))
            c4.metric("P95", _fmt_metric(ensemble_port.get("p95")))
            st.caption(f"Districts used: {int(ensemble_port.get('n_districts', 0))}")
        else:
            st.caption("No numeric district values found for this state & selection.")


def render_route_chooser() -> Optional[str]:
    """
    Render Step 1 - route chooser buttons for portfolio building.
    
    Returns the current route (or None if not selected).
    """
    import streamlit as st

    st.markdown("### Step 1 – Build your district portfolio")
    st.caption(
        "Choose one method to start adding districts. The dashboard will guide you through the relevant path."
    )

    col_route_1, col_route_2, col_route_3 = st.columns(3)
    with col_route_1:
        if st.button(
            "📊 From the rankings table",
            key="btn_portfolio_route_rankings",
            use_container_width=True,
        ):
            st.session_state["portfolio_build_route"] = "rankings"
            st.session_state["jump_to_rankings"] = True
            st.session_state["jump_to_map"] = False
            st.rerun()
    with col_route_2:
        if st.button(
            "🗺 From the map",
            key="btn_portfolio_route_map",
            use_container_width=True,
        ):
            st.session_state["portfolio_build_route"] = "map"
            st.session_state["jump_to_map"] = True
            st.session_state["jump_to_rankings"] = False
            st.rerun()
    with col_route_3:
        if st.button(
            "📍 From saved points",
            key="btn_portfolio_route_saved_points",
            use_container_width=True,
        ):
            st.session_state["portfolio_build_route"] = "saved_points"
            st.session_state["jump_to_rankings"] = False
            st.session_state["jump_to_map"] = False
            st.rerun()

    route = st.session_state.get("portfolio_build_route", None)

    route_label_map = {
        "rankings": "From the rankings table",
        "map": "From the map",
        "saved_points": "From saved points",
    }

    if route in route_label_map:
        st.caption(f"Selected method: **{route_label_map[route]}**")
        if st.button("↩ Change method", key="btn_portfolio_route_reset"):
            st.session_state["portfolio_build_route"] = None
            st.session_state["jump_to_rankings"] = False
            st.session_state["jump_to_map"] = False
            st.rerun()

    return route


def render_route_hints(route: Optional[str], portfolio: Sequence[Any]) -> None:
    """Render hints based on the selected route."""
    import streamlit as st

    if route is None:
        if portfolio:
            st.caption(
                f"Current portfolio: **{len(portfolio)}** district(s). "
                "You can continue to Step 2 below, or choose a method above to add more."
            )
        else:
            st.caption("Choose a method above to start building your portfolio.")
    elif route == "rankings":
        st.caption(
            "Add districts from the **Rankings table** (left) and come back here to analyse."
        )
    elif route == "map":
        st.caption(
            "Add districts by selecting them on the **Map** (left) and clicking **Add to portfolio**."
        )
    elif route == "saved_points":
        st.caption("Add districts using the **Saved points** panel below.")


def render_index_selection(
    *,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
) -> Sequence[str]:
    """
    Render Step 2 - index selection for portfolio comparison.
    
    Returns the list of selected index slugs.
    """
    import streamlit as st

    st.markdown("### Step 2 – Select indices for portfolio analysis")
    st.caption(
        "Pick one or more indices to compare across the portfolio. "
        "This is the main lever for portfolio comparison."
    )

    available_indices = [(slug, meta["label"]) for slug, meta in variables.items()]
    default_sel = st.session_state.get("portfolio_multiindex_selection", [])
    selected_slugs = st.multiselect(
        "Select indices",
        options=[s for s, _ in available_indices],
        default=default_sel if default_sel else [variable_slug],
        format_func=lambda s: variables[s]["label"] if s in variables else str(s),
        key="portfolio_multiindex_selection",
    )

    return selected_slugs


def render_multiindex_comparison(
    *,
    portfolio: Sequence[Any],
    selected_slugs: Sequence[str],
    variables: Mapping[str, Mapping[str, Any]],
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    pilot_state: str,
    data_dir: Path,
    # Callable dependencies
    load_master_csv_fn: Callable[[str], pd.DataFrame],
    normalize_master_columns_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parse_master_schema_fn: Callable[[Any], tuple[list, list, dict]],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    portfolio_normalize_fn: Callable[[str], str],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
) -> None:
    """Render Step 3 - multi-index comparison table builder."""
    import streamlit as st
    
    from india_resilience_tool.analysis.metrics import compute_rank_and_percentile
    import os

    st.markdown("### Step 3 – Portfolio comparison (multi-index)")
    st.caption(
        "Build a combined table across all selected indices for the districts in your portfolio."
    )

    st.markdown("#### Multi-index comparison for portfolio")

    def _resolve_proc_root_for_slug(slug: str) -> Path:
        """
        Resolve processed root for a given index slug.
        """
        env_root = os.getenv("IRT_PROCESSED_ROOT")
        if env_root:
            base_path = Path(env_root)
            if base_path.name == slug:
                proc_root = base_path
            else:
                proc_root = base_path / slug
        else:
            proc_root = data_dir / "processed" / slug
        return proc_root.resolve()

    def _load_master_and_schema_for_slug(slug: str) -> tuple[pd.DataFrame, list, list, dict]:
        """
        Load master_metrics_by_district.csv for a slug, normalize columns,
        and parse schema. Cached by (slug, master_path, mtime).
        """
        proc_root = _resolve_proc_root_for_slug(slug)
        master_path = proc_root / pilot_state / "master_metrics_by_district.csv"

        cache = st.session_state.setdefault("_portfolio_master_cache", {})

        try:
            mtime = master_path.stat().st_mtime
        except Exception:
            mtime = None

        cache_key = f"{slug}::{str(master_path)}"
        entry = cache.get(cache_key)
        if entry is not None and entry.get("mtime") == mtime:
            return (
                entry["df"],
                entry["schema_items"],
                entry["metrics"],
                entry["by_metric"],
            )

        if not master_path.exists():
            empty_df = pd.DataFrame()
            cache[cache_key] = {
                "df": empty_df,
                "schema_items": [],
                "metrics": [],
                "by_metric": {},
                "mtime": mtime,
            }
            return empty_df, [], [], {}

        # Load + normalize + schema
        df_local = load_master_csv_fn(str(master_path))
        df_local = normalize_master_columns_fn(df_local)
        schema_items_local, metrics_local, by_metric_local = parse_master_schema_fn(df_local.columns)

        cache[cache_key] = {
            "df": df_local,
            "schema_items": schema_items_local,
            "metrics": metrics_local,
            "by_metric": by_metric_local,
            "mtime": mtime,
        }
        return df_local, schema_items_local, metrics_local, by_metric_local

    def _match_row_idx(df_local: pd.DataFrame, st_name: str, dist_name: str) -> Optional[int]:
        """
        Robustly match (state, district) in a master df.
        """
        if df_local is None or df_local.empty:
            return None
        if "state" not in df_local.columns or "district" not in df_local.columns:
            return None

        st_norm = portfolio_normalize_fn(st_name)
        dist_norm = portfolio_normalize_fn(dist_name)

        state_norm = df_local["state"].astype(str).map(portfolio_normalize_fn)
        dist_norm_series = df_local["district"].astype(str).map(portfolio_normalize_fn)

        exact = (state_norm == st_norm) & (dist_norm_series == dist_norm)
        if exact.any():
            return int(df_local.index[exact][0])

        # Fallback: contains
        try:
            contains_1 = dist_norm_series.str.contains(dist_norm, na=False)
            contains_2 = pd.Series(
                [dist_norm in str(x) for x in dist_norm_series.tolist()],
                index=df_local.index,
            )
            fallback = (state_norm == st_norm) & (contains_1 | contains_2)
            if fallback.any():
                return int(df_local.index[fallback][0])
        except Exception:
            pass

        return None

    def _compute_rank_and_percentile(
        df_local: pd.DataFrame,
        st_name: str,
        metric_col: str,
        value: float,
    ) -> tuple[Optional[int], Optional[float]]:
        """Compute rank and percentile within state."""
        return compute_rank_and_percentile(
            df_local,
            st_name,
            metric_col,
            value,
            state_col="state",
            normalize_fn=portfolio_normalize_fn,
            percentile_method="le",
        )

    def _build_portfolio_multiindex_df() -> pd.DataFrame:
        return build_portfolio_multiindex_df_fn(
            portfolio=portfolio,
            selected_slugs=selected_slugs,
            variables=variables,
            index_group_labels=index_group_labels,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            load_master_and_schema_for_slug=_load_master_and_schema_for_slug,
            resolve_metric_column=resolve_metric_column_fn,
            find_baseline_column_for_stat=find_baseline_column_for_stat_fn,
            match_row_idx=_match_row_idx,
            compute_rank_and_percentile=_compute_rank_and_percentile,
            risk_class_from_percentile=risk_class_from_percentile_fn,
            normalize_fn=portfolio_normalize_fn,
        )

    # If the selection context changed, prompt rebuild
    context_now = {
        "slugs": list(selected_slugs),
        "scenario": sel_scenario,
        "period": sel_period,
        "stat": sel_stat,
    }
    prev_context = st.session_state.get("portfolio_multiindex_context")
    if prev_context != context_now:
        st.session_state.pop("portfolio_multiindex_df", None)
        st.session_state["portfolio_multiindex_context"] = context_now

    if st.button(
        "Build multi-index portfolio table",
        key="btn_build_multiindex_portfolio_table",
        use_container_width=True,
    ):
        with st.spinner("Building multi-index portfolio table..."):
            st.session_state["portfolio_multiindex_df"] = _build_portfolio_multiindex_df()

    portfolio_multiindex_df = st.session_state.get("portfolio_multiindex_df")
    if isinstance(portfolio_multiindex_df, pd.DataFrame) and not portfolio_multiindex_df.empty:
        st.markdown("#### Portfolio – multi-index summary")
        st.dataframe(portfolio_multiindex_df, hide_index=True, use_container_width=True)

        st.download_button(
            "⬇️ Download portfolio data (multi-index, CSV)",
            data=portfolio_multiindex_df.to_csv(index=False).encode("utf-8"),
            file_name="portfolio_multiindex_summary.csv",
            mime="text/csv",
            use_container_width=True,
        )

        if st.button(
            "📊 Open rankings table (portfolio view)",
            key="btn_open_rankings_from_summary",
        ):
            st.session_state["jump_to_rankings"] = True
            st.rerun()
    else:
        st.info(
            "Click **Build multi-index portfolio table** to generate the comparison table."
        )


def render_portfolio_editor(
    *,
    portfolio: Sequence[Any],
    portfolio_remove_fn: Callable[[str, str], None],
    portfolio_remove_all_fn: Callable[[], None],
    portfolio_normalize_fn: Callable[[str], str],
) -> None:
    """Render Step 4 - review and edit portfolio districts."""
    import streamlit as st

    with st.expander("Step 4 – Review and edit portfolio districts", expanded=False):
        st.caption("Remove individual districts or remove all.")

        flash_msg = st.session_state.pop("portfolio_flash", None)
        if flash_msg:
            st.success(flash_msg)

        st.session_state.setdefault("confirm_clear_portfolio", False)

        top_l, top_r = st.columns([3, 2])
        with top_l:
            st.markdown(f"**Portfolio districts ({len(portfolio)})**")
        with top_r:
            if not st.session_state["confirm_clear_portfolio"]:
                if st.button("🧹 Remove all", key="btn_portfolio_remove_all"):
                    st.session_state["confirm_clear_portfolio"] = True
                    st.rerun()
            else:
                st.warning("Remove all districts from the portfolio?")
                c_yes, c_no = st.columns(2)
                with c_yes:
                    if st.button("✅ Confirm", key="btn_portfolio_remove_all_confirm"):
                        portfolio_remove_all_fn()
                        st.session_state["confirm_clear_portfolio"] = False
                        st.session_state["portfolio_flash"] = "Cleared portfolio selection."
                        st.rerun()
                with c_no:
                    if st.button("✖ Cancel", key="btn_portfolio_remove_all_cancel"):
                        st.session_state["confirm_clear_portfolio"] = False
                        st.rerun()

        # Show editable list
        try:
            table_df = pd.DataFrame(
                [{"District": (d.get("district") if isinstance(d, dict) else d[1]),
                  "State": (d.get("state") if isinstance(d, dict) else d[0])}
                 for d in portfolio]
            )
        except Exception:
            table_df = pd.DataFrame()

        if table_df.empty:
            st.warning("Portfolio exists but could not be displayed in table format.")
        else:
            for i, row in table_df.iterrows():
                district_i = str(row.get("District", "")).strip()
                state_i = str(row.get("State", "")).strip()
                c1, c2, c3 = st.columns([4, 4, 2])
                with c1:
                    st.write(district_i)
                with c2:
                    st.write(state_i)
                with c3:
                    if st.button(
                        "🗑 Remove",
                        key=f"btn_portfolio_remove_{portfolio_normalize_fn(state_i)}_{portfolio_normalize_fn(district_i)}",
                    ):
                        portfolio_remove_fn(state_i, district_i)
                        st.session_state["portfolio_flash"] = (
                            f"Removed {district_i}, {state_i} from portfolio."
                        )
                        st.rerun()


def render_portfolio_panel(
    *,
    # State/selection context
    selected_state: str,
    portfolio_route: Optional[str],
    # Variable/metric context
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    # Data
    merged: Any,  # GeoDataFrame
    adm1: Any,  # GeoDataFrame
    # Config
    pilot_state: str,
    data_dir: Path,
    # Callable dependencies
    compute_state_metrics_fn: Callable[..., tuple[dict, Any, Any]],
    load_master_csv_fn: Callable[[str], pd.DataFrame],
    normalize_master_columns_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parse_master_schema_fn: Callable[[Any], tuple[list, list, dict]],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    portfolio_normalize_fn: Callable[[str], str],
    portfolio_remove_fn: Callable[[str, str], None],
    portfolio_remove_all_fn: Callable[[], None],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
) -> None:
    """
    Render the complete multi-district portfolio panel (right column).

    This is the main entry point for the portfolio panel, composing all sub-renderers.

    Contract:
    - Preserves all widget keys from the legacy dashboard
    - Preserves all session_state keys
    - Takes explicit inputs rather than relying on globals
    """
    import streamlit as st

    # Ensure saved-points container exists
    st.session_state.setdefault("point_query_points", [])

    variable_label = variables.get(variable_slug, {}).get("label", variable_slug)

    # 1. State summary (shown when portfolio_route is None)
    if portfolio_route is None:
        render_state_summary(
            selected_state=selected_state,
            variable_label=variable_label,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            merged=merged,
            adm1=adm1,
            metric_col=metric_col,
            compute_state_metrics_fn=compute_state_metrics_fn,
        )

    # Separator
    st.markdown("---")

    # 2. Portfolio analysis expander
    with st.expander("Portfolio analysis (multi-district)", expanded=True):
        # Step 1: Route chooser
        route = render_route_chooser()

        # Get current portfolio
        portfolio = st.session_state.get("portfolio_districts", [])

        # Route hints
        render_route_hints(route, portfolio)

        st.markdown("---")

        # Check if portfolio is empty
        if not portfolio:
            st.info(
                "No districts in portfolio yet. Add districts via **From the rankings table**, "
                "**From the map**, or **From saved points**."
            )
        else:
            # Step 2: Index selection
            selected_slugs = render_index_selection(
                variables=variables,
                variable_slug=variable_slug,
            )

            # Step 3: Multi-index comparison
            if not selected_slugs:
                st.warning("Select at least one index to build a portfolio comparison.")
            else:
                render_multiindex_comparison(
                    portfolio=portfolio,
                    selected_slugs=selected_slugs,
                    variables=variables,
                    index_group_labels=index_group_labels,
                    sel_scenario=sel_scenario,
                    sel_period=sel_period,
                    sel_stat=sel_stat,
                    pilot_state=pilot_state,
                    data_dir=data_dir,
                    load_master_csv_fn=load_master_csv_fn,
                    normalize_master_columns_fn=normalize_master_columns_fn,
                    parse_master_schema_fn=parse_master_schema_fn,
                    resolve_metric_column_fn=resolve_metric_column_fn,
                    find_baseline_column_for_stat_fn=find_baseline_column_for_stat_fn,
                    risk_class_from_percentile_fn=risk_class_from_percentile_fn,
                    portfolio_normalize_fn=portfolio_normalize_fn,
                    build_portfolio_multiindex_df_fn=build_portfolio_multiindex_df_fn,
                )

            # Step 4: Review and edit portfolio
            render_portfolio_editor(
                portfolio=portfolio,
                portfolio_remove_fn=portfolio_remove_fn,
                portfolio_remove_all_fn=portfolio_remove_all_fn,
                portfolio_normalize_fn=portfolio_normalize_fn,
            )