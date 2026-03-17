"""
Metric selection ribbon (pillar → domain → metric → scenario/period/stat + map mode).

Extracted from the legacy orchestrator to keep orchestration thin while
preserving widget keys and session_state contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
import streamlit as st

from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.app.help_text import RIBBON_HELP_MD, help_md_to_plain_text
from india_resilience_tool.app.master_cache import make_load_master_and_schema_fn
from india_resilience_tool.config.constants import SCENARIO_HELP_MD, SCENARIO_UI_LABEL
from india_resilience_tool.config.variables import (
    VARIABLES,
    get_domain_description,
    get_domains_for_pillar,
    get_metrics_for_domain,
    normalize_domain_name,
    get_pillar_description,
    get_pillar_for_domain,
    get_pillars,
)
from india_resilience_tool.viz.charts import (
    PERIOD_ORDER,
    canonical_period_label,
    ordered_scenario_keys,
    period_display_label,
)
from paths import get_master_csv_filename


@dataclass(frozen=True)
class RibbonContext:
    variable_slug: Optional[str]
    varcfg: Optional[dict]
    processed_root: Optional[Path]
    master_csv_path: Optional[Path | tuple[Path, ...]]
    df: Optional[pd.DataFrame]
    schema_items: list[dict]
    metrics: list[str]
    by_metric: dict[str, list[dict]]
    registry_metric: str
    sel_scenario: str
    sel_scenario_display: str
    sel_period: str
    sel_stat: str
    map_mode: str
    metric_col: Optional[str]
    ribbon_ready: bool
    pretty_metric_label: str
    rebuild_master_csv_if_needed: Callable[..., tuple[bool, str]]
    load_master_and_schema_fn: Callable[[Path | tuple[Path, ...], str], tuple[pd.DataFrame, list[dict], list[str], dict]]


def _hydro_output_glob(level: str) -> str:
    """Return the relative glob used to detect hydro period outputs for a level."""
    if str(level).strip().lower() == "sub_basin":
        return "hydro/sub_basins/**/*_periods.csv"
    return "hydro/basins/**/*_periods.csv"


def _hydro_outputs_available(processed_root: Path, level: str) -> bool:
    """Return True when hydro processed period CSVs exist for the requested level."""
    try:
        return any(processed_root.glob(_hydro_output_glob(level)))
    except Exception:
        return False


def _hydro_master_contract_ready(master_csv_path: Path, level: str) -> bool:
    """Return True when a hydro master CSV contains the canonical hydro ID columns."""
    if not master_csv_path.exists():
        return False
    try:
        cols = set(pd.read_csv(master_csv_path, nrows=0).columns)
    except Exception:
        return False
    if str(level).strip().lower() == "sub_basin":
        return {"basin_id", "subbasin_id"}.issubset(cols)
    return {"basin_id"}.issubset(cols)


def _is_external_metric(varcfg: Optional[dict]) -> bool:
    """Return True when the selected metric is backed by an external prebuilt source."""
    return str((varcfg or {}).get("source_type") or "").strip().lower() == "external"


def _selection_mode(varcfg: Optional[dict]) -> str:
    """Return the metric selection mode declared in the registry."""
    return str((varcfg or {}).get("selection_mode") or "scenario_period").strip().lower()


def _is_static_snapshot_metric(varcfg: Optional[dict]) -> bool:
    """Return True for fixed-snapshot metrics such as population exposure layers."""
    return _selection_mode(varcfg) == "static_snapshot"


def _fixed_scenario(varcfg: Optional[dict]) -> str:
    return str((varcfg or {}).get("fixed_scenario") or "").strip()


def _fixed_period(varcfg: Optional[dict]) -> str:
    return str((varcfg or {}).get("fixed_period") or "").strip()


def _supported_statistics(varcfg: Optional[dict]) -> list[str]:
    raw = list((varcfg or {}).get("supported_statistics") or ("mean", "median"))
    out = [str(v).strip().lower() for v in raw if str(v).strip()]
    return out or ["mean", "median"]


def _supports_baseline_comparison(varcfg: Optional[dict]) -> bool:
    return bool((varcfg or {}).get("supports_baseline_comparison", True))


def _metric_rebuild_command(varcfg: Optional[dict], *, level: str) -> Optional[str]:
    level_norm = str(level).strip().lower()
    key = "hydro_rebuild_command" if level_norm in {"basin", "sub_basin"} else "admin_rebuild_command"
    cmd = str((varcfg or {}).get(key) or "").strip()
    return cmd or None


def _selected_state_for_admin_master_loading() -> str:
    """Return the best available admin-state selection for master loading."""
    pending = st.session_state.get("pending_selected_state")
    if pending not in (None, ""):
        return str(pending).strip() or "All"
    selected = st.session_state.get("selected_state", "All")
    return str(selected).strip() or "All"


def _resolve_external_admin_master_sources(
    processed_root: Path,
    *,
    level: str,
    selected_state: str,
) -> tuple[Path, ...]:
    """Resolve one or many external admin master CSVs for the current state selection."""
    master_name = get_master_csv_filename(level)
    state_norm = str(selected_state or "All").strip() or "All"
    if state_norm != "All":
        return (processed_root / state_norm / master_name,)

    states = list_available_states_from_processed_root(str(processed_root))
    sources: list[Path] = []
    for state_name in states:
        candidate = processed_root / state_name / master_name
        if candidate.exists():
            sources.append(candidate)
    return tuple(sources)


def _master_source_exists(master_source: Path | tuple[Path, ...]) -> bool:
    """Return True when the resolved master source has at least one readable file."""
    if isinstance(master_source, tuple):
        return bool(master_source) and all(path.exists() for path in master_source)
    return master_source.exists()


def _master_source_label(master_source: Path | tuple[Path, ...]) -> str:
    """Return a compact user-facing label for one or many master CSVs."""
    if isinstance(master_source, tuple):
        if not master_source:
            return "(no state-sliced master CSVs found)"
        if len(master_source) == 1:
            return str(master_source[0])
        root = master_source[0].parents[1] if len(master_source[0].parents) >= 2 else master_source[0].parent
        return f"{root}/*/{master_source[0].name}"
    return str(master_source)


def _coerce_selection(
    *,
    key: str,
    options: list[str],
    placeholder: str,
    default: Optional[str] = None,
) -> str:
    preferred = default if (default is not None and default in options) else None
    fallback = preferred or (options[0] if options else placeholder)
    current = st.session_state.get(key, placeholder)
    if current not in options:
        st.session_state[key] = fallback
    return str(st.session_state.get(key, fallback))


def render_metric_ribbon(
    *,
    col: object,
    sel_placeholder: str,
    data_dir: Path,
    pilot_state: str,
    resolve_processed_root_fn: Callable[..., Path],
    attach_centroid_geojson: str | None,
    master_needs_rebuild_fn: Callable[[Path, Path, str], bool],
    state_profile_files_missing_fn: Callable[[Path, str, str], bool],
    perf_section: Callable[..., object],
    render_perf_panel_safe: Callable[[], None],
) -> RibbonContext:
    """
    Render the ribbon controls and load the master CSV once a metric is selected.

    Contract:
    - Preserves all widget keys from the legacy dashboard:
      `selected_pillar`, `selected_bundle`, `selected_var`, `sel_scenario`, `sel_period`, `sel_stat`, `map_mode`
    - Preserves placeholder-first selection behavior.
    """
    # Force deliberate choices for ribbon controls
    for k in ("selected_pillar", "selected_bundle", "selected_var", "sel_scenario", "sel_period", "sel_stat"):
        st.session_state.setdefault(k, sel_placeholder)

    # Bound once a metric is selected (safe defaults otherwise)
    variable_slug: Optional[str] = None
    varcfg: Optional[dict] = None
    processed_root: Optional[Path] = None
    master_csv_path: Optional[Path] = None

    df: Optional[pd.DataFrame] = None
    schema_items: list[dict] = []
    metrics: list[str] = []
    by_metric: dict[str, list[dict]] = {}
    registry_metric: str = ""
    sel_scenario: str = st.session_state.get("sel_scenario", sel_placeholder)
    sel_period: str = st.session_state.get("sel_period", sel_placeholder)
    sel_stat: str = st.session_state.get("sel_stat", sel_placeholder)
    map_mode: str = st.session_state.get("map_mode", sel_placeholder)

    # Used by details/case-study even outside the ribbon.
    load_master_and_schema_fn = make_load_master_and_schema_fn(perf_section=perf_section)

    def rebuild_master_csv_if_needed(force: bool = False, attach_centroid_geojson: str | None = None) -> tuple[bool, str]:
        _ = force, attach_centroid_geojson
        return False, "disabled (select a metric first)"

    with col:
        row1 = st.columns([2.2, 3.0, 1.8])
        row2 = st.columns([1.8, 2.2, 1.4])
        row3 = st.columns([2.4, 1.2, 1.8])
        spatial_family = str(st.session_state.get("spatial_family", "admin")).strip().lower()
        current_level = str(st.session_state.get("admin_level", "district")).strip().lower()

        # --- Pillar selection ---
        all_pillars = get_pillars(spatial_family=spatial_family, level=current_level)
        if not all_pillars:
            st.error("No assessment pillars defined in metrics_registry.py")
            render_perf_panel_safe()
            st.stop()

        pillar_options = [sel_placeholder] + all_pillars
        cur_pillar = st.session_state.get("selected_pillar", sel_placeholder)
        if cur_pillar not in pillar_options:
            inferred_pillar = get_pillar_for_domain(st.session_state.get("selected_bundle", ""))
            if inferred_pillar in all_pillars:
                cur_pillar = inferred_pillar
                st.session_state["selected_pillar"] = inferred_pillar
            else:
                cur_pillar = sel_placeholder
                st.session_state["selected_pillar"] = sel_placeholder

        with row1[0]:
            pillar_help = help_md_to_plain_text(RIBBON_HELP_MD["assessment_pillar"])
            selected_pillar_preview = st.session_state.get("selected_pillar", sel_placeholder)
            if selected_pillar_preview != sel_placeholder:
                pillar_desc_preview = get_pillar_description(selected_pillar_preview)
                if pillar_desc_preview:
                    pillar_help += f"\n\nThis pillar covers:\n- {pillar_desc_preview}"

            selected_pillar = st.selectbox(
                "Assessment pillar",
                options=pillar_options,
                index=pillar_options.index(cur_pillar),
                key="selected_pillar",
                label_visibility="visible",
                help=pillar_help,
            )

        # --- Domain selection ---
        domain_disabled = selected_pillar == sel_placeholder
        if domain_disabled:
            all_domains: list[str] = []
            domain_options = [sel_placeholder]
        else:
            all_domains = get_domains_for_pillar(
                selected_pillar,
                spatial_family=spatial_family,
                level=current_level,
            )
            domain_options = [sel_placeholder] + all_domains

        cur_bundle = st.session_state.get("selected_bundle", sel_placeholder)
        if cur_bundle != sel_placeholder:
            cur_bundle = normalize_domain_name(cur_bundle)
        if cur_bundle not in domain_options:
            cur_bundle = sel_placeholder
            st.session_state["selected_bundle"] = sel_placeholder
        elif cur_bundle != st.session_state.get("selected_bundle", sel_placeholder):
            st.session_state["selected_bundle"] = cur_bundle

        with row1[1]:
            bundle_help = help_md_to_plain_text(RIBBON_HELP_MD["risk_domain"])
            selected_bundle_preview = st.session_state.get("selected_bundle", sel_placeholder)
            if selected_bundle_preview != sel_placeholder:
                bundle_desc_preview = get_domain_description(selected_bundle_preview)
                if bundle_desc_preview:
                    bundle_help += f"\n\nThis domain covers:\n- {bundle_desc_preview}"

            selected_bundle = st.selectbox(
                "Domain",
                options=domain_options,
                index=domain_options.index(cur_bundle),
                key="selected_bundle",
                label_visibility="visible",
                disabled=domain_disabled,
                help=bundle_help,
            )

        # --- Metric selection (filtered by domain) ---
        metric_disabled = selected_bundle == sel_placeholder
        if metric_disabled:
            index_slugs: list[str] = []
            metric_options = [sel_placeholder]
        else:
            index_slugs = get_metrics_for_domain(
                selected_bundle,
                spatial_family=spatial_family,
                level=current_level,
            ) or []
            if not index_slugs:
                st.warning(f"Domain '{selected_bundle}' has no metrics for the current spatial view.")
            metric_options = [sel_placeholder] + index_slugs

        cur_var = st.session_state.get("selected_var", sel_placeholder)
        if cur_var not in metric_options:
            if cur_var == sel_placeholder or not index_slugs:
                st.session_state["selected_var"] = sel_placeholder
            else:
                st.session_state["selected_var"] = index_slugs[0]
        cur_var = st.session_state.get("selected_var", sel_placeholder)

        with row1[2]:
            metric_help = help_md_to_plain_text(RIBBON_HELP_MD["metric"])
            selected_metric_preview = st.session_state.get("selected_var", sel_placeholder)
            if selected_metric_preview != sel_placeholder and selected_metric_preview in VARIABLES:
                desc_preview = str(VARIABLES[selected_metric_preview].get("description", "")).strip()
                units_preview = str(
                    VARIABLES[selected_metric_preview].get("unit")
                    or VARIABLES[selected_metric_preview].get("units")
                    or ""
                ).strip()
                if desc_preview or units_preview:
                    metric_help += "\n\nAbout this metric:"
                    if desc_preview:
                        metric_help += f"\n- {desc_preview}"
                    if units_preview:
                        metric_help += f"\n- Units: {units_preview}"

            selected_var = st.selectbox(
                "Metric",
                options=metric_options,
                index=metric_options.index(cur_var) if cur_var in metric_options else 0,
                key="selected_var",
                label_visibility="visible",
                format_func=lambda k: VARIABLES[k]["label"] if k in VARIABLES else k,
                disabled=metric_disabled,
                help=metric_help,
            )

        metric_ready = (selected_var != sel_placeholder) and (selected_var in VARIABLES)
        if metric_ready:
            variable_slug = selected_var
            varcfg = VARIABLES[variable_slug]

            registry_metric = str(varcfg.get("periods_metric_col", "")).strip()
            st.session_state["registry_metric"] = registry_metric

            processed_root = resolve_processed_root_fn(variable_slug, data_dir=data_dir, mode="portfolio")
            level = str(st.session_state.get("admin_level", "district")).strip().lower()
            selected_admin_state = _selected_state_for_admin_master_loading()
            master_name = get_master_csv_filename(level)
            if level in {"basin", "sub_basin"}:
                master_root = processed_root / "hydro"
                master_root.mkdir(parents=True, exist_ok=True)
                master_csv_path = master_root / master_name
            elif _is_external_metric(varcfg):
                master_csv_path = _resolve_external_admin_master_sources(
                    processed_root,
                    level=level,
                    selected_state=selected_admin_state,
                )
            else:
                master_root = processed_root / str(pilot_state)
                master_root.mkdir(parents=True, exist_ok=True)
                master_csv_path = master_root / master_name
            master_source_label = _master_source_label(master_csv_path)

            def rebuild_master_csv_if_needed(
                force: bool = False, attach_centroid_geojson: str | None = None
            ) -> tuple[bool, str]:
                level = str(st.session_state.get("admin_level", "district")).strip().lower()
                is_hydro = level in {"basin", "sub_basin"}
                is_external = _is_external_metric(varcfg)
                if is_external:
                    if _master_source_exists(master_csv_path):
                        return False, "up-to-date"
                    rebuild_cmd = _metric_rebuild_command(varcfg, level=level)
                    if level in {"district", "block"}:
                        return (
                            False,
                            (
                                "external admin masters are built by dedicated geodata tooling; "
                                f"run {rebuild_cmd or 'the metric-specific admin master builder'}"
                            ),
                        )
                    if level in {"basin", "sub_basin"}:
                        return (
                            False,
                            (
                                "external hydro masters are built by dedicated geodata tooling; "
                                f"run {rebuild_cmd or 'the metric-specific hydro master builder'}"
                            ),
                        )
                    return False, "external metric master CSV missing"
                needs = (
                    force
                    or (is_hydro and not _hydro_master_contract_ready(master_csv_path, level))
                    or (
                        level in {"district", "block"}
                        and master_needs_rebuild_fn(master_csv_path, processed_root, str(pilot_state))
                    )
                    or (
                        level in {"district", "block"}
                        and state_profile_files_missing_fn(processed_root, str(pilot_state), level)
                    )
                )
                if not needs:
                    return False, "up-to-date"
                if is_hydro and not _hydro_outputs_available(processed_root, level):
                    return (
                        False,
                        (
                            f"no hydro processed outputs found under {processed_root / 'hydro'}; "
                            f"run compute_indices_multiprocess for --level {level} first"
                        ),
                    )
                try:
                    from india_resilience_tool.compute.master_builder import build_master_metrics
                except Exception as e:
                    return False, f"builder import failed: {e}"
                try:
                    master_df = build_master_metrics(
                        str(processed_root),
                        ("hydro" if level in {"basin", "sub_basin"} else str(pilot_state)),
                        metric_col_in_periods=varcfg["periods_metric_col"],
                        out_path=str(master_csv_path),
                        attach_centroid_geojson=attach_centroid_geojson,
                        verbose=True,
                        level=level,
                    )
                    if master_csv_path.exists():
                        return True, "rebuilt"
                    if getattr(master_df, "empty", True):
                        return False, f"builder found no source rows for {level} under {master_root}"
                    return False, f"builder finished but did not create {master_csv_path}"
                except Exception as e:
                    return False, f"rebuild failed: {e}"

            # Ensure master exists/fresh for this metric (only once a metric is chosen)
            try:
                level = str(st.session_state.get("admin_level", "district")).strip().lower()
                needs_rebuild = False
                if _is_external_metric(varcfg):
                    needs_rebuild = not _master_source_exists(master_csv_path)
                elif level in {"district", "block"}:
                    needs_rebuild = master_needs_rebuild_fn(master_csv_path, processed_root, str(pilot_state)) or state_profile_files_missing_fn(
                        processed_root, str(pilot_state), level
                    )
                if level in {"basin", "sub_basin"} and not _hydro_master_contract_ready(master_csv_path, level):
                    needs_rebuild = True

                if needs_rebuild:
                    with st.spinner("Master CSV missing or stale — rebuilding now..."):
                        ok, msg = rebuild_master_csv_if_needed(
                            force=False,
                            attach_centroid_geojson=attach_centroid_geojson,
                        )
                        st.success("Master CSV rebuilt.") if ok else st.error(f"Auto-rebuild failed: {msg}")
            except Exception as e:
                st.warning(f"Could not check master CSV freshness: {e}")

            if not _master_source_exists(master_csv_path):
                if _is_external_metric(varcfg):
                    rebuild_cmd = _metric_rebuild_command(varcfg, level=level)
                    if level in {"district", "block"}:
                        st.error(
                            f"Admin master CSV not found for {VARIABLES[variable_slug]['label']} at {master_source_label}. "
                            f"Run `{rebuild_cmd or 'the metric-specific admin master builder'}` first."
                        )
                    elif level in {"basin", "sub_basin"}:
                        st.error(
                            f"Hydro master CSV not found for {VARIABLES[variable_slug]['label']} at {master_source_label}. "
                            f"Run `{rebuild_cmd or 'the metric-specific hydro master builder'}` first."
                        )
                    else:
                        st.error(
                            f"Master CSV not found for {VARIABLES[variable_slug]['label']} at {master_source_label}."
                        )
                elif level in {"basin", "sub_basin"} and not _hydro_outputs_available(processed_root, level):
                    st.error(
                        f"Hydro boundary files are loaded, but no hydro processed outputs were found for "
                        f"{VARIABLES[variable_slug]['label']} under {processed_root / 'hydro'}. "
                        f"Run the hydro compute pipeline for `--level {level}` first, then rebuild the master CSV."
                    )
                else:
                    st.error(
                        f"Master CSV not found for {VARIABLES[variable_slug]['label']} at {master_source_label}. "
                        f"Click 'Rebuild now' in the sidebar under 'Master dataset'."
                    )
                render_perf_panel_safe()
                st.stop()

            df, schema_items, metrics, by_metric = load_master_and_schema_fn(master_csv_path, variable_slug)

            if not metrics:
                st.error("No ensemble statistic columns found in the master CSV. Did the builder run?")
                render_perf_panel_safe()
                st.stop()

            # Align registry metric if column normalization changed casing
            available_metrics = set(metrics)
            if registry_metric not in available_metrics and available_metrics:
                m_lower = {str(m).lower(): m for m in available_metrics}
                registry_metric = m_lower.get(str(registry_metric).lower(), next(iter(available_metrics)))
            st.session_state["registry_metric"] = registry_metric

            # Scenario list (metric-aware; climate metrics default to SSP245/SSP585)
            items_for_m = by_metric.get(registry_metric, [])
            all_scenarios = (
                sorted(set(i["scenario"] for i in items_for_m)) if items_for_m else sorted(set(i["scenario"] for i in schema_items))
            )
            allowed_list = [
                str(s).strip().lower()
                for s in (varcfg.get("supported_scenarios") or ("ssp245", "ssp585"))
                if str(s).strip()
            ]
            allowed = set(allowed_list)
            scenarios = [s for s in ordered_scenario_keys(all_scenarios) if str(s).strip().lower() in allowed]
            if _is_static_snapshot_metric(varcfg):
                fixed = _fixed_scenario(varcfg) or (scenarios[0] if scenarios else "")
                if fixed and fixed not in scenarios:
                    scenarios = [fixed]
                elif fixed:
                    scenarios = [fixed]
            if not scenarios:
                st.error("No supported scenarios found for this metric in the master CSV.")
                render_perf_panel_safe()
                st.stop()
            scenario_options = scenarios if _is_static_snapshot_metric(varcfg) else [sel_placeholder] + scenarios
        else:
            scenario_options = [sel_placeholder]

        scenario_disabled = (not metric_ready) or (_is_static_snapshot_metric(varcfg) and len(scenario_options) == 1)
        cur_scn = _coerce_selection(
            key="sel_scenario",
            options=scenario_options,
            placeholder=sel_placeholder,
            default=_fixed_scenario(varcfg) if _is_static_snapshot_metric(varcfg) else sel_placeholder,
        )

        with row2[0]:
            scenario_help = help_md_to_plain_text(RIBBON_HELP_MD["scenario"])
            if _is_static_snapshot_metric(varcfg):
                scenario_help = "This metric uses a fixed snapshot selection and does not expose multiple scenarios."
            if cur_scn != sel_placeholder:
                scen_key_preview = str(cur_scn).strip().lower()
                extra = SCENARIO_HELP_MD.get(scen_key_preview)
                if extra:
                    scenario_help += f"\n\n{help_md_to_plain_text(extra)}"

            sel_scenario = st.selectbox(
                "Scenario",
                options=scenario_options,
                index=scenario_options.index(cur_scn),
                key="sel_scenario",
                label_visibility="visible",
                disabled=scenario_disabled,
                format_func=lambda s: (
                    s if s == sel_placeholder else SCENARIO_UI_LABEL.get(str(s).strip().lower(), str(s))
                ),
                help=scenario_help,
            )

        sel_scenario_display = (
            sel_scenario
            if sel_scenario == sel_placeholder
            else SCENARIO_UI_LABEL.get(str(sel_scenario).strip().lower(), str(sel_scenario))
        )

        # --- Period selection (depends on scenario) ---
        period_disabled = (not metric_ready) or (sel_scenario == sel_placeholder)
        if metric_ready and (sel_scenario != sel_placeholder):
            periods_found = {
                canonical_period_label(i["period"])
                for i in (by_metric.get(st.session_state.get("registry_metric", registry_metric), []) or schema_items)
                if i["scenario"] == sel_scenario
            }
            preferred_periods = [
                canonical_period_label(p)
                for p in (varcfg.get("preferred_period_order") or ())
                if str(p).strip()
            ]
            base_period_order = preferred_periods + [p for p in PERIOD_ORDER if p not in preferred_periods]
            periods = [p for p in base_period_order if p in periods_found]
            periods.extend(sorted([p for p in periods_found if p not in set(base_period_order)]))
            if _is_static_snapshot_metric(varcfg):
                fixed_period = canonical_period_label(_fixed_period(varcfg) or (periods[0] if periods else ""))
                period_options = [fixed_period] if fixed_period else [sel_placeholder]
                period_disabled = period_disabled or (fixed_period != sel_placeholder)
            else:
                period_options = [sel_placeholder] + periods if periods else [sel_placeholder]
        else:
            period_options = [sel_placeholder]

        cur_per = _coerce_selection(
            key="sel_period",
            options=period_options,
            placeholder=sel_placeholder,
            default=canonical_period_label(_fixed_period(varcfg)) if _is_static_snapshot_metric(varcfg) else sel_placeholder,
        )

        with row2[1]:
            period_help = help_md_to_plain_text(RIBBON_HELP_MD["period"])
            if _is_static_snapshot_metric(varcfg):
                period_help = "This metric uses a fixed data snapshot year and does not expose multi-period climate windows."
            sel_period = st.selectbox(
                "Period",
                options=period_options,
                index=period_options.index(cur_per),
                key="sel_period",
                label_visibility="visible",
                disabled=period_disabled,
                format_func=lambda p: period_display_label(p) if p != sel_placeholder else p,
                help=period_help,
            )

        # --- Statistic selection (mean/median only, placeholder-first) ---
        stat_disabled = not metric_ready
        stat_values = _supported_statistics(varcfg) if metric_ready else ["mean", "median"]
        stat_options = stat_values if (_is_static_snapshot_metric(varcfg) and len(stat_values) == 1) else [sel_placeholder] + stat_values
        if _is_static_snapshot_metric(varcfg) and len(stat_values) == 1:
            stat_disabled = True
        cur_stat = _coerce_selection(
            key="sel_stat",
            options=stat_options,
            placeholder=sel_placeholder,
            default=(stat_values[0] if (_is_static_snapshot_metric(varcfg) and stat_values) else sel_placeholder),
        )

        with row2[2]:
            statistic_help = help_md_to_plain_text(RIBBON_HELP_MD["statistic"])
            sel_stat = st.selectbox(
                "Statistic",
                options=stat_options,
                index=stat_options.index(cur_stat),
                key="sel_stat",
                label_visibility="visible",
                disabled=stat_disabled,
                help=statistic_help,
            )

        # --- Map mode selection ---
        baseline_map_mode_label = "Change from baseline" if _is_external_metric(varcfg) else "Change from 1990-2010 baseline"
        if metric_ready and not _supports_baseline_comparison(varcfg):
            map_mode_options = ["Absolute value"]
        else:
            map_mode_options = [sel_placeholder, "Absolute value", baseline_map_mode_label]
        cur_map_mode = _coerce_selection(
            key="map_mode",
            options=map_mode_options,
            placeholder=sel_placeholder,
            default=("Absolute value" if map_mode_options == ["Absolute value"] else sel_placeholder),
        )

        with row3[0]:
            map_mode_help = help_md_to_plain_text(RIBBON_HELP_MD["map_mode"])
            if metric_ready and not _supports_baseline_comparison(varcfg):
                map_mode_help = "This metric only supports absolute-value mapping; baseline change mode is not available."
            map_mode = st.selectbox(
                "Map mode",
                options=map_mode_options,
                index=map_mode_options.index(cur_map_mode),
                key="map_mode",
                label_visibility="visible",
                disabled=(metric_ready and map_mode_options == ["Absolute value"]),
                help=map_mode_help,
            )

    sel_metric = str(st.session_state.get("registry_metric", registry_metric) or "").strip()
    metric_col: Optional[str] = None
    pretty_metric_label = "Select a metric to visualize"

    ribbon_ready = (
        (variable_slug is not None)
        and (df is not None)
        and (sel_scenario != sel_placeholder)
        and (sel_period != sel_placeholder)
        and (sel_stat != sel_placeholder)
        and (map_mode != sel_placeholder)
    )

    if ribbon_ready:
        metric_col = f"{sel_metric}__{sel_scenario}__{sel_period}__{sel_stat}"
        if df is None or metric_col not in df.columns:
            st.error(f"Selected column '{metric_col}' not found in master CSV.")
            render_perf_panel_safe()
            st.stop()

        pretty_metric_label = (
            f"{VARIABLES[variable_slug]['label']} · {sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )

    return RibbonContext(
        variable_slug=variable_slug,
        varcfg=varcfg,
        processed_root=processed_root,
        master_csv_path=master_csv_path,
        df=df,
        schema_items=schema_items,
        metrics=metrics,
        by_metric=by_metric,
        registry_metric=registry_metric,
        sel_scenario=sel_scenario,
        sel_scenario_display=sel_scenario_display,
        sel_period=sel_period,
        sel_stat=sel_stat,
        map_mode=map_mode,
        metric_col=metric_col,
        ribbon_ready=ribbon_ready,
        pretty_metric_label=pretty_metric_label,
        rebuild_master_csv_if_needed=rebuild_master_csv_if_needed,
        load_master_and_schema_fn=load_master_and_schema_fn,
    )
