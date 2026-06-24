"""
Map + rankings pipeline for the IRT Streamlit dashboard.

This module extracts the "merged dataframe → enriched columns → color scale →
folium map → rankings table" block from the legacy monolith so the main
`run_app()` orchestrator can stay small.

Notes:
    - This is app-layer code and may use Streamlit (widgets/warnings).
    - Scientific transforms (baseline/delta, rank/percentile/risk, tooltips)
      live in Streamlit-free modules under `analysis/` and `viz/`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
from typing import Any, Callable, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd
import streamlit as st

from india_resilience_tool.analysis.map_enrichment import (
    add_current_baseline_delta,
    add_rank_percentile_risk,
    add_tooltip_strings,
)
from india_resilience_tool.viz.formatting import (
    get_metric_display_meta,
    get_metric_display_units,
)
from india_resilience_tool.analysis.metrics import risk_class_from_percentile
from india_resilience_tool.app.color_range_controls import compute_color_range_defaults
from india_resilience_tool.app.overlays import (
    OverlayControlState,
    RP100_FLOOD_DEPTH_BINS,
    RP100_FLOOD_OVERLAY_ID,
    RURAL_FACILITIES_DENSITY_OVERLAY_ID,
    build_overlay_render_layers,
)
from india_resilience_tool.app.map_layer_runtime import build_folium_map_for_selection
from india_resilience_tool.data.admin_coverage import CoverageDiagnostics
from india_resilience_tool.data.master_columns import find_baseline_column_for_stat
from india_resilience_tool.data.merge import (
    get_or_build_merged_for_index_cached as _get_or_build_merged_for_index_cached,
)
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.colors import (
    FLOOD_SEVERITY_CLASS_COLORS,
    apply_fillcolor_classed,
    apply_fillcolor_binned,
    build_rp100_flood_depth_legend_html,
    build_vertical_categorical_legend_block_html,
    build_vertical_binned_legend_block_html,
)
from india_resilience_tool.viz.tables import build_rankings_table_df as _build_rankings_table_df
from india_resilience_tool.viz.charts import period_display_label
from india_resilience_tool.config.dashboard_bundles import (
    composite_slug_for_bundle,
    get_dashboard_bundle_spec,
    is_dashboard_bundle_slug,
)
from india_resilience_tool.config.paths import resolve_processed_optimised_root
from india_resilience_tool.data.optimized_bundle import is_optimized_metric_root


FLOOD_SEVERITY_METRIC_SLUG = "jrc_flood_depth_index_rp100"


@dataclass(frozen=True)
class MapArtifacts:
    merged: Any
    table_df: Any
    has_baseline: bool
    folium_map: Any
    legend_block_html: Optional[str]
    baseline_col: Optional[str]
    map_mode: str
    map_value_col: str
    pretty_metric_label: str
    cmap_name: str
    rank_scope_label: str
    overlay_messages: tuple[str, ...]
    blocked_message: Optional[str]


def _boundary_signature(
    boundary_df: pd.DataFrame,
    *,
    boundary_path: Path,
    simplify_tolerance: Optional[float],
) -> tuple[str, Optional[float], Optional[float], int]:
    """Return the explicit boundary signature used to invalidate merged-cache entries."""
    try:
        resolved_path = str(boundary_path.resolve())
    except Exception:
        resolved_path = str(boundary_path)
    try:
        boundary_mtime = float(boundary_path.stat().st_mtime)
    except Exception:
        boundary_mtime = None
    return (
        resolved_path,
        boundary_mtime,
        None if simplify_tolerance is None else float(simplify_tolerance),
        int(len(boundary_df)),
    )


def _build_legend_title(varcfg: Mapping[str, Any]) -> str:
    """Return the minimal legend title text derived from metric units."""
    return get_metric_display_units(
        metric_slug=str(varcfg.get("slug") or ""),
        units=str(varcfg.get("display_units") or varcfg.get("unit") or varcfg.get("units") or "").strip(),
    )


def _uses_fixed_class_scale(variable_slug: str, varcfg: Mapping[str, Any]) -> bool:
    return (
        str(variable_slug or "").strip().lower() == FLOOD_SEVERITY_METRIC_SLUG
        and str(varcfg.get("class_display_mode") or "").strip().lower() == "label_with_score"
    )


def _stack_legend_blocks(primary_html: str, overlay_html: str, *, map_height: int) -> str:
    """Pair the choropleth legend and active overlay legend in one legend column."""
    return f"""
<div style="height:{int(map_height)}px; width:100%; display:flex; flex-direction:row;
            align-items:center; justify-content:center; gap:28px; min-width:0; overflow:visible;">
  <div style="height:100%; min-width:0; overflow:visible;">
    {primary_html}
  </div>
  <div style="height:100%; min-width:0; overflow:visible;">
    {overlay_html}
  </div>
</div>
"""


def details_require_geometry(
    *,
    adm_level: str,
    spatial_family: str,
    selected_state: str,
    selected_district: str,
    selected_block: str,
    selected_basin: str,
    selected_subbasin: str,
) -> bool:
    """Return whether the right-panel flow still needs merged geometries."""
    level_norm = str(adm_level or "district").strip().lower()
    family_norm = str(spatial_family or "admin").strip().lower()
    if family_norm == "hydro" and level_norm == "sub_basin":
        return selected_basin != "All" and selected_subbasin == "All"
    if level_norm == "block":
        return selected_state != "All" and selected_block == "All"
    return level_norm == "district" and selected_state != "All" and selected_district == "All"


def blocked_drilldown_message(
    *,
    adm_level: str,
    spatial_family: str,
    selected_state: str,
    selected_basin: str,
) -> Optional[str]:
    """Return the drill-down prompt for fine-grain nationwide views, if any."""
    level_norm = str(adm_level or "district").strip().lower()
    family_norm = str(spatial_family or "admin").strip().lower()
    if family_norm != "hydro" and level_norm == "block" and selected_state == "All":
        return "Select a state to render block maps and rankings."
    if family_norm == "hydro" and level_norm == "sub_basin" and selected_basin == "All":
        return "Select a basin to render sub-basin maps and rankings."
    return None


def _build_nonspatial_details_source_df(
    df: pd.DataFrame,
    *,
    level: str,
    spatial_family: str,
) -> pd.DataFrame:
    """Return a details/rankings dataframe that does not require geometry."""
    level_norm = str(level or "district").strip().lower()
    family_norm = str(spatial_family or "admin").strip().lower()
    out = df.copy()
    rename_map: dict[str, str] = {}
    if "state" in out.columns and "state_name" not in out.columns:
        rename_map["state"] = "state_name"
    if "district" in out.columns and "district_name" not in out.columns:
        rename_map["district"] = "district_name"
    if level_norm == "block" and "block" in out.columns and "block_name" not in out.columns:
        rename_map["block"] = "block_name"
    out = out.rename(columns=rename_map)
    if family_norm == "hydro" and "state_name" not in out.columns:
        out["state_name"] = "Hydro"
    return out


def _filter_frame_by_selection_value(
    frame: pd.DataFrame,
    *,
    column: str,
    selected_value: str,
) -> pd.DataFrame:
    """Filter a dataframe by a user selection with case/alias fallback."""
    if selected_value == "All" or column not in frame.columns:
        return frame

    series = frame[column].astype(str).str.strip()
    mask = series == str(selected_value).strip()
    if not mask.any():
        selected_key = alias(selected_value)
        mask = series.map(alias) == selected_key
    if not mask.any():
        mask = series.str.contains(re.escape(str(selected_value).strip()), case=False, na=False)
    return frame[mask]


def _level_aware_merge(
    *,
    adm2: Any,
    adm3: Any,
    df: pd.DataFrame,
    variable_slug: str,
    master_csv_path: Path,
    level: str,
    adm2_geojson_path: Path,
    adm3_geojson_path: Path,
    basin_geojson_path: Path,
    subbasin_geojson_path: Path,
    simplify_tol_adm2: float,
    simplify_tol_adm3: float,
) -> Any:
    level_norm = str(level or "district").strip().lower()
    boundary_gdf = adm3 if level_norm in {"block", "basin", "sub_basin"} else adm2
    if boundary_gdf is None:
        raise ValueError(f"Boundary GeoDataFrame is required for level={level_norm!r}")

    if level_norm == "district":
        boundary_sig = _boundary_signature(
            boundary_gdf,
            boundary_path=adm2_geojson_path,
            simplify_tolerance=simplify_tol_adm2,
        )
    elif level_norm == "block":
        boundary_sig = _boundary_signature(
            boundary_gdf,
            boundary_path=adm3_geojson_path,
            simplify_tolerance=simplify_tol_adm3,
        )
    elif level_norm == "basin":
        boundary_sig = _boundary_signature(
            boundary_gdf,
            boundary_path=basin_geojson_path,
            simplify_tolerance=None,
        )
    else:
        boundary_sig = _boundary_signature(
            boundary_gdf,
            boundary_path=subbasin_geojson_path,
            simplify_tolerance=None,
        )

    return _get_or_build_merged_for_index_cached(
        boundary_gdf,
        df,
        slug=variable_slug,
        master_path=master_csv_path,
        boundary_signature=boundary_sig,
        session_state=st.session_state,
        alias_fn=alias,
        adm2_state_col="state_name",
        master_state_col="state",
        level=level_norm,
    )


def _summarize_coverage_buckets(diagnostics: CoverageDiagnostics, *, level: str) -> list[str]:
    """Return short bucket summaries for coverage diagnostics messaging."""
    level_label = "features" if str(level).strip().lower() in {"basin", "sub_basin"} else str(level).strip().lower() + "s"
    parts: list[str] = []
    if diagnostics.missing_master_row_keys:
        parts.append(f"{len(diagnostics.missing_master_row_keys)} {level_label} without master rows")
    if diagnostics.null_value_keys:
        parts.append(f"{len(diagnostics.null_value_keys)} {level_label} with null rendered values")
    if diagnostics.broken_join_keys:
        parts.append(f"{len(diagnostics.broken_join_keys)} {level_label} with non-null values that failed to join")
    return parts


def evaluate_coverage_policy(
    *,
    adm_level: str,
    spatial_family: str,
    selected_state: str,
    diagnostics: Optional[CoverageDiagnostics],
) -> tuple[tuple[str, ...], Optional[str]]:
    """Return overlay warnings and an optional blocking error for render coverage issues."""
    if diagnostics is None:
        return (), None

    family_norm = str(spatial_family or "admin").strip().lower()
    level_norm = str(adm_level or "district").strip().lower()
    if family_norm != "admin" or level_norm not in {"district", "block"}:
        return (), None

    bucket_parts = _summarize_coverage_buckets(diagnostics, level=level_norm)
    if not bucket_parts:
        return (), None

    summary = (
        f"Render coverage matched {diagnostics.matched_feature_keys}/{diagnostics.total_feature_keys} "
        f"{level_norm} features ({diagnostics.coverage_pct:.1f}%). "
        + "; ".join(bucket_parts)
        + "."
    )

    if level_norm == "district" and selected_state == "All" and diagnostics.broken_join_keys:
        return (), (
            "Nationwide district map blocked because at least one district has a non-null rendered value "
            f"but failed the geometry join. {summary}"
        )

    return (summary,), None


MasterSourceLike = Union[Path, Sequence[Path]]


def _bundle_debug(message: str) -> None:
    """Emit a bundle-score diagnostic line when IRT_DEBUG is enabled."""
    if bool(int(os.getenv("IRT_DEBUG", "0") or "0")):
        print(f"[bundle_score] {message}")


def _resolve_composite_master_source(
    composite_slug: str,
    *,
    level: str,
    selected_state: str,
    spatial_family: str,
    data_dir: Path,
) -> MasterSourceLike:
    """
    Resolve the composite master source for ``composite_slug`` through the SAME
    optimized-root machinery the dashboard uses for the metric ``df``.

    Reuses the admin/hydro master-source resolvers from the ribbon module so the
    composite is loaded from the identical bundle layout (optimized-first with
    legacy fallback), rather than a separately-guessed path. The ribbon import is
    performed lazily to keep this module's import graph light.

    Returns a single ``Path`` (hydro) or a tuple of per-state shard ``Path``s
    (admin), suitable to pass straight to the injected master loader.
    """
    from india_resilience_tool.app.ribbon import (
        _resolve_admin_master_source,
        _resolve_hydro_master_source,
    )

    root = resolve_processed_optimised_root(composite_slug, data_dir=data_dir)
    if str(spatial_family or "admin").strip().lower() == "hydro":
        _, source_path, _ = _resolve_hydro_master_source(
            root,
            variable_slug=composite_slug,
            level=level,
            data_dir=data_dir,
        )
        return source_path

    optimized_intent = is_optimized_metric_root(root)
    _, source_tuple, _ = _resolve_admin_master_source(
        root,
        variable_slug=composite_slug,
        level=level,
        selected_state=selected_state,
        data_dir=data_dir,
        optimized_intent=optimized_intent,
    )
    return source_tuple


def _bundle_join_columns(level_norm: str) -> Optional[list[tuple[str, str, bool]]]:
    """
    Return the (ranking_col, composite_col, is_name) join keys for a level.

    Admin levels join on normalized name columns (the only shared admin key
    across metric and composite masters); hydro levels join on stable IDs.
    Returns None for unsupported levels.
    """
    if level_norm == "district":
        return [("state_name", "state", True), ("district_name", "district", True)]
    if level_norm == "block":
        return [
            ("state_name", "state", True),
            ("district_name", "district", True),
            ("block_name", "block", True),
        ]
    if level_norm == "basin":
        return [("basin_id", "basin_id", False)]
    if level_norm == "sub_basin":
        return [("subbasin_id", "subbasin_id", False)]
    return None


def _bundle_join_key_series(
    frame: pd.DataFrame,
    join_cols: list[tuple[str, str, bool]],
    *,
    side: str,
    normalize_fn: Callable[[str], str],
) -> pd.Series:
    """Build a composite join-key Series (name parts aliased, id parts stripped)."""
    parts: list[pd.Series] = []
    for rank_col, comp_col, is_name in join_cols:
        col = rank_col if side == "rank" else comp_col
        series = frame[col].astype(str).str.strip()
        if is_name:
            series = series.map(normalize_fn)
        parts.append(series)
    key = parts[0].astype(str)
    for extra in parts[1:]:
        key = key.str.cat(extra.astype(str), sep="||")
    return key


def _resolve_bundle_score_column(
    *,
    ranking_source: pd.DataFrame,
    selected_bundle: Optional[str],
    variable_slug: str,
    metric_col: str,
    level: str,
    selected_state: str,
    spatial_family: str,
    data_dir: Path,
    load_master_and_schema_fn: Callable[..., tuple],
    resolve_composite_source_fn: Callable[..., MasterSourceLike],
    normalize_fn: Callable[[str], str] = alias,
) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Resolve a per-unit bundle composite (0-100) score column for Method-B labels.

    PURE / Streamlit-free: performs no ``st.*`` calls. Both the master loader and
    the composite-source resolver are injected so this can be unit-tested with
    fakes (no disk, no ribbon).

    Returns ``(frame, bundle_score_col)`` where ``bundle_score_col`` is:
      - ``metric_col`` when the current metric is itself a dashboard composite
        (the value already IS a 0-100 score; no load/merge);
      - ``"bundle_score"`` after merging the bundle composite onto the frame;
      - ``None`` to signal Method-A fallback (non-bundle, unsupported
        level/scenario, malformed metric_col, missing column, or missing join
        key). When ``None``, the original ``ranking_source`` is returned unchanged.
    """
    level_norm = str(level or "district").strip().lower()

    # (0) Non-bundle selection -> Method A.
    bundle_name = str(selected_bundle or "").strip()
    if not bundle_name:
        return ranking_source, None
    composite_slug = composite_slug_for_bundle(bundle_name)
    if not composite_slug:
        return ranking_source, None
    spec = get_dashboard_bundle_spec(bundle_name)
    if spec is None:
        return ranking_source, None

    # (1) Level gate (cheap; no parse yet).
    if level_norm not in {str(v).strip().lower() for v in spec.supported_levels}:
        return ranking_source, None

    # (2) Metric is itself a composite -> classify its own value, no load.
    if is_dashboard_bundle_slug(variable_slug):
        return ranking_source, metric_col

    # (3) Defensive metric_col parse + scenario gate (after parse).
    parts = str(metric_col).split("__")
    if len(parts) != 4:
        return ranking_source, None
    scenario, period = parts[1], parts[2]
    if scenario not in spec.supported_scenarios:
        return ranking_source, None

    # (4) Resolve composite source via the same optimized-root machinery.
    try:
        composite_source = resolve_composite_source_fn(
            composite_slug,
            level=level_norm,
            selected_state=selected_state,
            spatial_family=spatial_family,
            data_dir=data_dir,
        )
    except Exception as exc:  # pragma: no cover - defensive
        _bundle_debug(f"composite source resolution failed for {composite_slug}: {exc}")
        return ranking_source, None

    # (5) Load composite master (injected loader concatenates shards).
    try:
        loaded = load_master_and_schema_fn(composite_source, composite_slug)
        comp_df = loaded[0] if isinstance(loaded, (tuple, list)) else loaded
    except Exception as exc:  # pragma: no cover - defensive
        _bundle_debug(f"composite master load failed for {composite_slug}: {exc}")
        return ranking_source, None
    if comp_df is None or comp_df.empty:
        _bundle_debug(f"composite master empty for {composite_slug}")
        return ranking_source, None

    # (6) Dynamic stat-column resolution (prefer __mean, else any matching stat).
    prefix = f"{composite_slug}__{scenario}__{period}__"
    candidates = [c for c in comp_df.columns if str(c).startswith(prefix)]
    if not candidates:
        _bundle_debug(
            f"composite column absent for prefix {prefix!r} in {composite_slug}"
        )
        return ranking_source, None
    composite_col = next(
        (c for c in candidates if str(c).endswith("__mean")), candidates[0]
    )

    # (7) Merge composite score onto the frame by real, shared join keys.
    join_cols = _bundle_join_columns(level_norm)
    if join_cols is None:
        return ranking_source, None
    for rank_col, comp_col, _is_name in join_cols:
        if rank_col not in ranking_source.columns or comp_col not in comp_df.columns:
            _bundle_debug(
                f"join key missing ({rank_col!r}/{comp_col!r}) for {composite_slug}"
            )
            return ranking_source, None

    rank_key = _bundle_join_key_series(
        ranking_source, join_cols, side="rank", normalize_fn=normalize_fn
    )
    comp_key = _bundle_join_key_series(
        comp_df, join_cols, side="comp", normalize_fn=normalize_fn
    )
    mapping = pd.Series(
        pd.to_numeric(comp_df[composite_col], errors="coerce").values, index=comp_key
    )
    mapping = mapping[~mapping.index.duplicated(keep="first")]

    out = ranking_source.drop(columns=["bundle_score"], errors="ignore").copy()  # N1
    out["bundle_score"] = rank_key.map(mapping)

    n_rows = len(out)
    n_matched = int(out["bundle_score"].notna().sum())
    if n_matched == 0:
        # Most likely cause: composite vs metric masters on different admin-name
        # vintages (LGD boundary rename). Surface loudly so it is not a silent
        # Method-A fallback.
        _bundle_debug(
            f"WARNING 0/{n_rows} {level_norm} units matched composite "
            f"{composite_slug} ({scenario}/{period}); falling back to Method A"
        )
        return ranking_source, None
    _bundle_debug(
        f"matched {n_matched}/{n_rows} {level_norm} units for composite {composite_slug}"
    )
    return out, "bundle_score"


def build_map_and_rankings(
    *,
    adm_level: str,
    adm1: Any,
    adm2: Any,
    adm3: Any,
    df: pd.DataFrame,
    master_csv_path: Path,
    variable_slug: str,
    varcfg: Mapping[str, Any],
    sel_metric: str,
    sel_scenario_display: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    map_mode: str,
    selected_state: str,
    selected_district: str,
    selected_block: str,
    selected_basin: str,
    selected_subbasin: str,
    spatial_family: str,
    include_map: bool,
    crosswalk_overlay: Optional[Mapping[str, Any]],
    overlay_states: Mapping[str, OverlayControlState],
    hover_enabled: bool,
    map_center: list[float],
    map_zoom: float,
    bounds_latlon: list[list[float]],
    pending_block_zoom: Optional[Mapping[str, str]],
    normalize_state_fn: Any,
    adm2_geojson_path: Path,
    adm3_geojson_path: Path,
    basin_geojson_path: Path,
    subbasin_geojson_path: Path,
    river_display_geojson_path: Path,
    river_basin_reconciliation_path: Path,
    river_subbasin_diagnostics_path: Path,
    data_dir: Path,
    selected_bundle: Optional[str] = None,
    load_master_and_schema_fn: Optional[Callable[..., tuple]] = None,
    simplify_tol_adm2: float,
    simplify_tol_adm3: float,
    map_height: int,
    color_slider_placeholder: Any,
    perf_section: Any,
    render_perf_panel_safe: Any,
) -> MapArtifacts:
    """
    Build the full map + rankings artifacts for the current selection.

    This function preserves the legacy behavior: it warns and stops the Streamlit
    run when required inputs are missing.
    """
    level_norm = str(adm_level or "district").strip().lower()
    blocked_message = blocked_drilldown_message(
        adm_level=level_norm,
        spatial_family=spatial_family,
        selected_state=selected_state,
        selected_basin=selected_basin,
    )

    if level_norm in {"district", "block"} and "district" not in df.columns:
        st.error("Master CSV must contain a 'district' column.")
        render_perf_panel_safe()
        st.stop()

    if level_norm == "block":
        block_col_candidates = ["block", "block_name"]
        block_col = next((c for c in block_col_candidates if c in df.columns), None)
        if block_col is None:
            st.error("Block mode requires master CSV to contain a 'block' (or 'block_name') column.")
            render_perf_panel_safe()
            st.stop()

    if level_norm == "basin" and "basin_id" not in df.columns:
        st.error("Basin mode requires master CSV to contain a 'basin_id' column.")
        render_perf_panel_safe()
        st.stop()

    if level_norm == "sub_basin" and "subbasin_id" not in df.columns:
        st.error("Sub-basin mode requires master CSV to contain a 'subbasin_id' column.")
        render_perf_panel_safe()
        st.stop()

        if adm3 is None:
            st.error("Block mode requires ADM3 boundaries to be loaded.")
            render_perf_panel_safe()
            st.stop()

    # --- Baseline column for this metric + stat (used by map & table) ---
    baseline_col = find_baseline_column_for_stat(
        df.columns,
        base_metric=sel_metric,
        stat=sel_stat,
    )
    ranking_source = _build_nonspatial_details_source_df(
        df,
        level=level_norm,
        spatial_family=spatial_family,
    )

    # Optional Method-B risk label: classify the risk class by the bundle
    # composite (0-100) score rather than the metric's ordinal percentile.
    # Falls back to Method A (bundle_score_col=None) whenever a composite cannot
    # be resolved/matched. Requires the injected master loader.
    bundle_score_col: Optional[str] = None
    if load_master_and_schema_fn is not None:
        ranking_source, bundle_score_col = _resolve_bundle_score_column(
            ranking_source=ranking_source,
            selected_bundle=selected_bundle,
            variable_slug=variable_slug,
            metric_col=metric_col,
            level=level_norm,
            selected_state=selected_state,
            spatial_family=spatial_family,
            data_dir=data_dir,
            load_master_and_schema_fn=load_master_and_schema_fn,
            resolve_composite_source_fn=_resolve_composite_master_source,
        )

    # -------------------------
    # Build ranking table
    # -------------------------
    with perf_section("rank_table: build"):
        extra_rank_cols: list[str] = []
        if level_norm == "sub_basin":
            unit_col = "subbasin_name"
            extra_rank_cols = ["basin_name", "basin_id", "subbasin_id"]
        elif level_norm == "basin":
            unit_col = "basin_name"
            extra_rank_cols = ["basin_id"]
        elif level_norm == "block":
            unit_col = "block_name"
        else:
            unit_col = "district_name"
        table_df, has_baseline = _build_rankings_table_df(
            ranking_source,
            metric_col=metric_col,
            baseline_col=baseline_col,
            selected_state=selected_state,
            risk_class_from_percentile=risk_class_from_percentile,
            district_col=unit_col,
            state_col="state_name",
            aspirational_col="aspirational",
            extra_cols=extra_rank_cols,
            higher_is_worse=bool(varcfg.get("rank_higher_is_worse", True)),
            bundle_score_col=bundle_score_col,
        )

    if blocked_message:
        return MapArtifacts(
            merged=pd.DataFrame(),
            table_df=pd.DataFrame(),
            has_baseline=bool(has_baseline),
            folium_map=None,
            legend_block_html=None,
            baseline_col=baseline_col,
            map_mode=map_mode,
            map_value_col=metric_col,
            pretty_metric_label=str(varcfg.get("label") or variable_slug),
            cmap_name="Reds",
            rank_scope_label="",
            overlay_messages=(),
            blocked_message=blocked_message,
        )

    needs_geometry = include_map or details_require_geometry(
        adm_level=level_norm,
        spatial_family=spatial_family,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_block=selected_block,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
    )

    if needs_geometry:
        with perf_section("merge: build merged gdf"):
            with st.spinner("Preparing merged geometries with CSV attributes..."):
                merged = _level_aware_merge(
                    adm2=adm2,
                    adm3=adm3,
                    df=df,
                    variable_slug=variable_slug,
                    master_csv_path=master_csv_path,
                    level=level_norm,
                    adm2_geojson_path=adm2_geojson_path,
                    adm3_geojson_path=adm3_geojson_path,
                    basin_geojson_path=basin_geojson_path,
                    subbasin_geojson_path=subbasin_geojson_path,
                    simplify_tol_adm2=simplify_tol_adm2,
                    simplify_tol_adm3=simplify_tol_adm3,
                )
    else:
        merged = ranking_source

    # Handle pending block zoom (needs merged GeoDataFrame with block geometries)
    if needs_geometry and pending_block_zoom and "block_name" in getattr(merged, "columns", []):
        zoom_state = str(pending_block_zoom.get("state", "")).strip()
        zoom_district = str(pending_block_zoom.get("district", "")).strip()
        zoom_block = str(pending_block_zoom.get("block", "")).strip()

        try:
            block_mask = (
                (merged["state_name"].astype(str).str.strip().str.lower() == zoom_state.lower())
                & (merged["district_name"].astype(str).str.strip().str.lower() == zoom_district.lower())
                & (merged["block_name"].astype(str).str.strip().str.lower() == zoom_block.lower())
            )
            block_rows = merged[block_mask]
            if not block_rows.empty:
                block_geom = block_rows.iloc[0].geometry
                if block_geom is not None:
                    centroid = block_geom.centroid
                    map_center = [float(centroid.y), float(centroid.x)]
                    map_zoom = 11.0
                    st.session_state["map_center"] = map_center
                    st.session_state["map_zoom"] = map_zoom
        except Exception:
            pass

    if not include_map:
        return MapArtifacts(
            merged=merged,
            table_df=table_df,
            has_baseline=bool(has_baseline),
            folium_map=None,
            legend_block_html=None,
            baseline_col=baseline_col,
            map_mode=map_mode,
            map_value_col=metric_col,
            pretty_metric_label=str(varcfg.get("label") or variable_slug),
            cmap_name="Reds",
            rank_scope_label="",
            overlay_messages=(),
            blocked_message=None,
        )

    # --- Compute current/baseline/delta columns once (used by map + tooltip) ---
    with perf_section("map: compute current/baseline/delta"):
        merged = add_current_baseline_delta(
            merged,
            metric_col=metric_col,
            baseline_col=baseline_col,
        )

    # --- Decide which column the map will actually show ---
    map_value_col = metric_col  # default: absolute values
    supports_baseline_comparison = bool(varcfg.get("supports_baseline_comparison", True))
    baseline_map_mode_label = (
        "Change from baseline"
        if str(varcfg.get("source_type") or "").strip().lower() == "external"
        else "Change from 1990-2010 baseline"
    )

    if supports_baseline_comparison and map_mode == baseline_map_mode_label:
        if baseline_col and (baseline_col in merged.columns):
            map_value_col = "_delta_abs"
        else:
            st.warning(
                "Historical baseline column not found for this metric/stat; "
                "showing absolute values instead."
            )
            map_mode = "Absolute value"
            st.session_state["map_mode"] = map_mode
            map_value_col = metric_col

    # --- Compute rank/percentile/risk class per state for tooltip quick-glance ---
    with perf_section("map: compute rank + risk class"):
        rank_higher_is_worse = bool(varcfg.get("rank_higher_is_worse", True))
        merged, rank_scope_label = add_rank_percentile_risk(
            merged,
            admin_level=level_norm,
            rank_higher_is_worse=rank_higher_is_worse,
            alias_fn=alias,
            risk_class_from_percentile_fn=risk_class_from_percentile,
        )

    with perf_section("map: build tooltip strings"):
        merged = add_tooltip_strings(merged, map_mode=map_mode, variable_slug=variable_slug)

    # Compute color scale defaults from *visible* units (matches the map filter),
    # then default to a robust p2–p98 range so outliers don't collapse the palette.
    scale_gdf = merged
    scale_gdf = _filter_frame_by_selection_value(
        scale_gdf,
        column="state_name",
        selected_value=selected_state,
    )
    scale_gdf = _filter_frame_by_selection_value(
        scale_gdf,
        column="district_name",
        selected_value=selected_district,
    )

    if level_norm == "block":
        scale_gdf = _filter_frame_by_selection_value(
            scale_gdf,
            column="block_name",
            selected_value=selected_block,
        )
    if level_norm == "basin":
        scale_gdf = _filter_frame_by_selection_value(
            scale_gdf,
            column="basin_name",
            selected_value=selected_basin,
        )
    if level_norm == "sub_basin":
        scale_gdf = _filter_frame_by_selection_value(
            scale_gdf,
            column="basin_name",
            selected_value=selected_basin,
        )
        scale_gdf = _filter_frame_by_selection_value(
            scale_gdf,
            column="subbasin_name",
            selected_value=selected_subbasin,
        )

    scale_vals = pd.to_numeric(
        scale_gdf.get(map_value_col, pd.Series([], dtype=float)), errors="coerce"
    )
    scale_vals = scale_vals.replace([np.inf, -np.inf], np.nan).dropna()

    if scale_vals.empty:
        st.error("No numeric values found for the current map selection.")
        render_perf_panel_safe()
        st.stop()

    use_fixed_class_scale = _uses_fixed_class_scale(variable_slug, varcfg)
    class_labels = {
        int(key): str(value)
        for key, value in dict(varcfg.get("class_labels") or {}).items()
    } if use_fixed_class_scale else {}
    data_min, data_max, vmin_default, vmax_default = compute_color_range_defaults(scale_vals)
    display_units, display_scale = get_metric_display_meta(
        metric_slug=variable_slug,
        units=str(varcfg.get("unit") or varcfg.get("units") or "").strip(),
    )
    use_composite_bundle_scale = is_dashboard_bundle_slug(variable_slug) and not use_fixed_class_scale

    if use_fixed_class_scale:
        color_slider_placeholder.empty()
        vmin, vmax = 1.0, 5.0
    elif use_composite_bundle_scale:
        color_slider_placeholder.empty()
        vmin, vmax = 0.0, 100.0
    else:
        slider_min = float(data_min * display_scale)
        slider_max = float(data_max * display_scale)
        slider_default = (float(vmin_default * display_scale), float(vmax_default * display_scale))
        slider_step = max((slider_max - slider_min) / 200.0, 0.001)
        slider_label = "Color range (min → max)"
        if display_units:
            slider_label = f"{slider_label} [{display_units}]"

        with st.sidebar:
            vmin_vmax = color_slider_placeholder.slider(
                slider_label,
                min_value=slider_min,
                max_value=slider_max,
                value=slider_default,
                step=slider_step,
                key="color_range_slider",
            )

        vmin, vmax = float(vmin_vmax[0] / display_scale), float(vmin_vmax[1] / display_scale)

    # Choose colormap: diverging for baseline-change; YlOrRd for composite bundle
    # scores (matches Glance 0-100 palette); Reds for all other metrics.
    if supports_baseline_comparison and map_mode == "Change from 1990-2010 baseline":
        cmap_name = "RdBu_r"  # blue-negative, red-positive
        pretty_metric_label = (
            f"Δ {str(varcfg.get('label') or variable_slug)} vs 1990–2010 · "
            f"{sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )
    elif use_composite_bundle_scale:
        cmap_name = "YlOrRd"
        pretty_metric_label = (
            f"{str(varcfg.get('label') or variable_slug)} · {sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )
    else:
        cmap_name = "Reds"
        pretty_metric_label = (
            f"{str(varcfg.get('label') or variable_slug)} · {sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )
    legend_title = _build_legend_title(varcfg)

    with perf_section("colors: apply_fillcolor_binned"):
        with st.spinner("Computing colors..."):
            if use_fixed_class_scale:
                value_to_color = {
                    class_index: FLOOD_SEVERITY_CLASS_COLORS[class_index - 1]
                    for class_index in sorted(class_labels)
                    if 1 <= class_index <= len(FLOOD_SEVERITY_CLASS_COLORS)
                }
                merged = apply_fillcolor_classed(
                    merged,
                    map_value_col,
                    value_to_color=value_to_color,
                )
            else:
                merged = apply_fillcolor_binned(
                    merged,
                    map_value_col,
                    vmin,
                    vmax,
                    cmap_name=cmap_name,
                    nlevels=15,
                )

    # Filter for map display (preserves legacy behavior: block selection does not
    # hide other blocks; it only affects the details panel).
    display_gdf = merged
    display_gdf = _filter_frame_by_selection_value(
        display_gdf,
        column="state_name",
        selected_value=selected_state,
    )
    display_gdf = _filter_frame_by_selection_value(
        display_gdf,
        column="district_name",
        selected_value=selected_district,
    )
    if level_norm == "basin":
        display_gdf = _filter_frame_by_selection_value(
            display_gdf,
            column="basin_name",
            selected_value=selected_basin,
        )
    if level_norm == "sub_basin":
        display_gdf = _filter_frame_by_selection_value(
            display_gdf,
            column="basin_name",
            selected_value=selected_basin,
        )
        display_gdf = _filter_frame_by_selection_value(
            display_gdf,
            column="subbasin_name",
            selected_value=selected_subbasin,
        )

    overlay_layers, overlay_messages, _overlay_cache_sig = build_overlay_render_layers(
        overlay_states=overlay_states,
        spatial_family=spatial_family,
        admin_level=level_norm,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
        data_dir=data_dir,
        river_display_geojson_path=river_display_geojson_path,
        river_basin_reconciliation_path=river_basin_reconciliation_path,
        river_subbasin_diagnostics_path=river_subbasin_diagnostics_path,
        alias_fn=alias,
        selected_district=selected_district,
    )

    map_build = build_folium_map_for_selection(
        level=level_norm,
        master_df=df,
        merged=merged,
        display_gdf=display_gdf,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
        map_mode=map_mode,
        baseline_col=baseline_col,
        rank_scope_label=rank_scope_label,
        metric_col=metric_col,
        map_value_col=map_value_col,
        alias_fn=alias,
        normalize_state_fn=normalize_state_fn,
        adm1=adm1,
        map_center=map_center,
        map_zoom=map_zoom,
        bounds_latlon=bounds_latlon,
        hover_enabled=bool(hover_enabled),
        adm2_geojson_path=adm2_geojson_path,
        adm3_geojson_path=adm3_geojson_path,
        basin_geojson_path=basin_geojson_path,
        subbasin_geojson_path=subbasin_geojson_path,
        river_display_geojson_path=river_display_geojson_path,
        simplify_tolerance_adm2=simplify_tol_adm2,
        simplify_tolerance_adm3=simplify_tol_adm3,
        crosswalk_overlay=crosswalk_overlay,
        overlay_layers=overlay_layers,
        perf_section=perf_section,
    )
    coverage_messages, coverage_block = evaluate_coverage_policy(
        adm_level=level_norm,
        spatial_family=spatial_family,
        selected_state=selected_state,
        diagnostics=map_build.coverage_diagnostics,
    )
    if coverage_block:
        st.error(coverage_block)
        render_perf_panel_safe()
        st.stop()
    overlay_messages = tuple(overlay_messages) + tuple(coverage_messages)

    if use_fixed_class_scale:
        legend_block_html = build_vertical_categorical_legend_block_html(
            legend_title=str(varcfg.get("label") or variable_slug),
            labels=[
                str(class_labels[index])
                for index in sorted(class_labels)
                if index in class_labels
            ],
            colors=list(FLOOD_SEVERITY_CLASS_COLORS[: len(class_labels)]),
            map_height=map_height,
            bar_width_px=18,
        )
    else:
        legend_block_html = build_vertical_binned_legend_block_html(
            legend_title=legend_title,
            vmin=vmin,
            vmax=vmax,
            cmap_name=cmap_name,
            display_scale=display_scale,
            nlevels=15,
            nticks=5,
            include_zero_tick=True,
            map_height=map_height,
            bar_width_px=18,
        )

    overlay_legend_blocks: list[str] = []
    rp100_overlay_legend = next(
        (
            build_rp100_flood_depth_legend_html(
                bins=RP100_FLOOD_DEPTH_BINS,
                map_height=map_height,
            )
            for layer in overlay_layers
            if layer.overlay_id == RP100_FLOOD_OVERLAY_ID and layer.legend_html
        ),
        None,
    )
    if rp100_overlay_legend:
        overlay_legend_blocks.append(rp100_overlay_legend)

    rural_overlay_legend = next(
        (
            layer.legend_html
            for layer in overlay_layers
            if layer.overlay_id == RURAL_FACILITIES_DENSITY_OVERLAY_ID and layer.legend_html
        ),
        None,
    )
    if rural_overlay_legend:
        overlay_legend_blocks.append(rural_overlay_legend)

    for overlay_legend_html in overlay_legend_blocks:
        if legend_block_html:
            legend_block_html = _stack_legend_blocks(
                legend_block_html,
                overlay_legend_html,
                map_height=map_height,
            )
        else:
            legend_block_html = overlay_legend_html

    return MapArtifacts(
        merged=merged,
        table_df=table_df,
        has_baseline=bool(has_baseline),
        folium_map=map_build.folium_map,
        legend_block_html=legend_block_html,
        baseline_col=baseline_col,
        map_mode=map_mode,
        map_value_col=map_value_col,
        pretty_metric_label=pretty_metric_label,
        cmap_name=cmap_name,
        rank_scope_label=rank_scope_label,
        overlay_messages=overlay_messages,
        blocked_message=None,
    )
