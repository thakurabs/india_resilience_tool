"""Runtime helpers for dashboard bundle visibility and composite-source lookup."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.config.dashboard_bundles import (
    DashboardBundleSpec,
    get_dashboard_bundle_spec,
    get_dashboard_bundle_specs,
    grouped_bundle_label,
)
from india_resilience_tool.data.master_loader import (
    load_master_csvs,
    master_source_signature,
    normalize_master_columns,
    parse_master_schema,
    resolve_preferred_master_path,
)
from india_resilience_tool.data.optimized_bundle import optimized_master_sources_from_metric_root
from paths import get_master_csv_filename, resolve_processed_optimised_root, resolve_processed_root


def _level_norm(level: str) -> str:
    return str(level or "").strip().lower()


def _path_exists(path: Path) -> bool:
    """Return True when a path or its preferred Parquet companion exists."""
    return resolve_preferred_master_path(path).exists()


def _spec_supports_level(spec: DashboardBundleSpec, *, level: str) -> bool:
    supported = {value.lower() for value in spec.supported_levels}
    return _level_norm(level) in supported


def resolve_dashboard_bundle_master_sources(
    bundle_name: str,
    *,
    level: str,
    data_dir: Path,
) -> tuple[Path, ...]:
    """Resolve the composite master sources for one dashboard bundle and admin level."""
    spec = get_dashboard_bundle_spec(bundle_name)
    if spec is None or not _spec_supports_level(spec, level=level):
        return ()

    level_norm = _level_norm(level)
    optimized_root = resolve_processed_optimised_root(spec.composite_slug, data_dir=data_dir, mode="portfolio")
    optimized_sources: tuple[Path, ...] = ()
    if optimized_root.exists():
        optimized_sources = tuple(
            path
            for path in optimized_master_sources_from_metric_root(
                optimized_root,
                level=level_norm,
                selected_state="All",
            )
            if path.exists()
        )
    if optimized_sources:
        return optimized_sources

    legacy_root = resolve_processed_root(spec.composite_slug, data_dir=data_dir, mode="portfolio")
    master_name = get_master_csv_filename(level_norm)
    states = list_available_states_from_processed_root(str(legacy_root.resolve()))
    return tuple(
        candidate
        for state_name in states
        for candidate in [legacy_root / state_name / master_name]
        if _path_exists(candidate)
    )


@st.cache_data(show_spinner=False)
def dashboard_bundle_scenario_period_options(
    bundle_name: str,
    *,
    level: str,
    data_dir: Path,
) -> tuple[tuple[str, str], ...]:
    """Return available mean scenario-period pairs for one dashboard bundle and level."""
    spec = get_dashboard_bundle_spec(bundle_name)
    if spec is None or not _spec_supports_level(spec, level=level):
        return ()

    source_paths = resolve_dashboard_bundle_master_sources(bundle_name, level=level, data_dir=data_dir)
    if not source_paths:
        return ()

    df = normalize_master_columns(load_master_csvs(source_paths))
    _schema_items, _metrics, by_metric = parse_master_schema(df.columns)
    items = [
        item
        for item in by_metric.get(spec.composite_slug, [])
        if str(item.get("stat") or "").strip().lower() == "mean"
    ]
    pairs = {
        (str(item["scenario"]).strip().lower(), str(item["period"]).strip())
        for item in items
        if str(item.get("scenario") or "").strip() and str(item.get("period") or "").strip()
    }
    return tuple(sorted(pairs))


@st.cache_data(show_spinner=False)
def dashboard_bundle_is_valid_for_level(
    bundle_name: str,
    *,
    level: str,
    data_dir: Path,
) -> bool:
    """Return whether a dashboard bundle is trustworthy enough to show for a level."""
    spec = get_dashboard_bundle_spec(bundle_name)
    if spec is None or not _spec_supports_level(spec, level=level):
        return False

    source_paths = resolve_dashboard_bundle_master_sources(bundle_name, level=level, data_dir=data_dir)
    if not source_paths:
        return False

    df = normalize_master_columns(load_master_csvs(source_paths))
    _schema_items, _metrics, by_metric = parse_master_schema(df.columns)
    mean_items = [
        item
        for item in by_metric.get(spec.composite_slug, [])
        if str(item.get("stat") or "").strip().lower() == "mean"
    ]
    if not mean_items:
        return False

    for item in mean_items:
        column = str(item.get("column") or "").strip()
        if column in df.columns and pd.to_numeric(df[column], errors="coerce").notna().any():
            return True
    return False


def available_dashboard_bundle_names(
    *,
    level: str,
    data_dir: Path,
    landing_only: bool = False,
) -> list[str]:
    """Return canonical dashboard bundle names valid for one admin level."""
    out: list[str] = []
    for spec in get_dashboard_bundle_specs():
        if landing_only and not spec.show_in_landing:
            continue
        if not _spec_supports_level(spec, level=level):
            continue
        if dashboard_bundle_is_valid_for_level(spec.canonical_bundle, level=level, data_dir=data_dir):
            out.append(spec.canonical_bundle)
    return out


def dashboard_bundle_display(bundle_name: str) -> str:
    """Return the grouped display label for one bundle/domain name."""
    return grouped_bundle_label(bundle_name)


def dashboard_bundle_source_signature(
    bundle_name: str,
    *,
    level: str,
    data_dir: Path,
) -> tuple[tuple[str, Optional[float]], ...]:
    """Return a cache-friendly source signature for one bundle's composite sources."""
    source_paths = resolve_dashboard_bundle_master_sources(bundle_name, level=level, data_dir=data_dir)
    if not source_paths:
        return ()
    return master_source_signature(source_paths)
