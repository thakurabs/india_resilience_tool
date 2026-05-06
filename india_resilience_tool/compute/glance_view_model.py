"""Offline Glance view-model builder for persisted landing runtime artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.bundle_scores import normalize_metric_series
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.dashboard_bundles import (
    DashboardBundleSpec,
    get_dashboard_bundle_spec_by_slug,
    get_dashboard_bundle_specs,
)
from india_resilience_tool.config.proposal_bundles import (
    get_proposal_bundle_spec_by_slug,
    proposal_rule_score_column,
)
from india_resilience_tool.config.variables import VARIABLES
from india_resilience_tool.data.master_columns import resolve_metric_column
from india_resilience_tool.data.master_loader import (
    load_master_csvs,
    normalize_master_columns,
    parse_master_schema,
    resolve_preferred_master_path,
)
from india_resilience_tool.data.optimized_bundle import (
    optimized_glance_root,
    optimized_master_sources_from_metric_root,
    resolve_optimized_metric_root,
)
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.charts import canonical_period_label, ordered_period_keys, ordered_scenario_keys
from paths import get_master_csv_filename, resolve_processed_root


GLANCE_FILENAMES = ("district.parquet", "state.parquet", "drivers.parquet", "attributes.parquet", "distributions.parquet")
GLANCE_BANDS = ("Low", "Moderate", "High", "Very High")
GLANCE_DRIVER_COLUMNS = [
    "scope_level",
    "state_name",
    "district_name",
    "__state_key",
    "__district_key",
    "driver_rank",
    "driver_slug",
    "driver_label",
    "driver_score",
    "driver_score_display",
    "driver_source",
]
GLANCE_ATTRIBUTE_COLUMNS = [
    "state_name",
    "district_name",
    "__state_key",
    "__district_key",
    "attribute_slug",
    "attribute_label",
    "attribute_value",
    "attribute_display",
    "sort_order",
]
GLANCE_DISTRIBUTION_COLUMNS = [
    "scope_level",
    "state_name",
    "__state_key",
    "band",
    "band_order",
    "count",
]
GLANCE_REQUIRED_COLUMNS: dict[str, set[str]] = {
    "district.parquet": {
        "bundle_slug",
        "bundle_name",
        "group_key",
        "selector_label",
        "scenario",
        "period",
        "state_name",
        "district_name",
        "__state_key",
        "__district_key",
        "bundle_score",
        "bundle_score_display",
        "score_band",
        "district_rank",
        "district_count",
        "state_bundle_score",
        "state_bundle_score_display",
        "state_rank",
        "state_count",
        "state_mean_score",
        "delta_vs_state_mean",
        "delta_vs_state_mean_display",
    },
    "state.parquet": {
        "bundle_slug",
        "bundle_name",
        "group_key",
        "selector_label",
        "scenario",
        "period",
        "state_name",
        "__state_key",
        "bundle_score",
        "bundle_score_display",
        "score_band",
        "state_rank",
        "state_count",
    },
    "drivers.parquet": {
        "scope_level",
        "state_name",
        "district_name",
        "__state_key",
        "__district_key",
        "driver_rank",
        "driver_slug",
        "driver_label",
        "driver_score",
        "driver_score_display",
        "driver_source",
    },
    "attributes.parquet": {
        "state_name",
        "district_name",
        "__state_key",
        "__district_key",
        "attribute_slug",
        "attribute_label",
        "attribute_value",
        "attribute_display",
        "sort_order",
    },
    "distributions.parquet": {
        "scope_level",
        "state_name",
        "__state_key",
        "band",
        "band_order",
        "count",
    },
}


@dataclass(frozen=True)
class GlanceBuildResult:
    """Summary of one written or planned Glance scenario-period artifact set."""

    composite_slug: str
    scenario: str
    period: str
    output_root: Path
    wrote: bool


def score_band(score: object) -> str:
    """Return the persisted Glance score band for a 0-100 score."""
    try:
        value = float(score)
    except (TypeError, ValueError):
        return "Insufficient data"
    if not np.isfinite(value):
        return "Insufficient data"
    if value < 25.0:
        return "Low"
    if value < 50.0:
        return "Moderate"
    if value < 75.0:
        return "High"
    return "Very High"


def format_score(score: object) -> Optional[str]:
    """Return a persisted one-decimal Glance score string, or null for missing values."""
    try:
        value = float(score)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(value):
        return None
    return f"{value:.1f}"


def _format_signed_delta(value: object) -> Optional[str]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(numeric):
        return None
    return f"{numeric:+.1f}"


def _admin_sources_for_slug(slug: str, *, data_dir: Path) -> tuple[Path, ...]:
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    if metric_root.exists():
        optimized = tuple(
            path
            for path in optimized_master_sources_from_metric_root(metric_root, level="district", selected_state="All")
            if path.exists()
        )
        if optimized:
            return optimized

    legacy_root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
    master_name = get_master_csv_filename("district")
    states = sorted(path for path in legacy_root.iterdir() if path.is_dir()) if legacy_root.exists() else []
    return tuple(
        resolved
        for state_root in states
        for resolved in [resolve_preferred_master_path(state_root / master_name)]
        if resolved.exists()
    )


def _standardize_district_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map: dict[str, str] = {}
    if "state_name" not in out.columns:
        for candidate in ("state", "STATE_UT", "shapeName_0"):
            if candidate in out.columns:
                rename_map[candidate] = "state_name"
                break
    if "district_name" not in out.columns:
        for candidate in ("district", "DISTRICT", "shapeName", "shapeName_2"):
            if candidate in out.columns:
                rename_map[candidate] = "district_name"
                break
    if rename_map:
        out = out.rename(columns=rename_map)
    for col in ("state_name", "district_name"):
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].astype("string").fillna("").str.strip()
    out = out[(out["state_name"] != "") & (out["district_name"] != "")]
    out["__state_key"] = out["state_name"].astype(str).map(alias)
    out["__district_key"] = out["__state_key"] + "|" + out["district_name"].astype(str).map(alias)
    return out.reset_index(drop=True)


def _load_metric_values(
    slug: str,
    *,
    scenario: str,
    period: str,
    stat: str,
    data_dir: Path,
) -> pd.DataFrame:
    sources = _admin_sources_for_slug(slug, data_dir=data_dir)
    columns = ["state_name", "district_name", "__state_key", "__district_key", slug]
    if not sources:
        return pd.DataFrame(columns=columns)
    df = _standardize_district_frame(normalize_master_columns(load_master_csvs(sources)))
    metric_base = str(VARIABLES.get(slug, {}).get("periods_metric_col") or slug).strip()
    value_col = resolve_metric_column(df, metric_base, scenario, canonical_period_label(period), stat)
    out = df[["state_name", "district_name", "__state_key", "__district_key"]].copy()
    out[slug] = pd.to_numeric(df[value_col], errors="coerce") if value_col in df.columns else np.nan
    return (
        out.groupby(["state_name", "district_name", "__state_key", "__district_key"], as_index=False, dropna=False)[slug]
        .mean()
        .reset_index(drop=True)
    )


def _available_pairs_for_slug(slug: str, *, data_dir: Path) -> tuple[tuple[str, str], ...]:
    sources = _admin_sources_for_slug(slug, data_dir=data_dir)
    if not sources:
        return ()
    df = normalize_master_columns(load_master_csvs(sources))
    schema_items, _metrics, by_metric = parse_master_schema(df.columns)
    metric_base = str(VARIABLES.get(slug, {}).get("periods_metric_col") or slug).strip()
    items = by_metric.get(metric_base, []) or schema_items
    pairs = {
        (str(item["scenario"]).strip().lower(), canonical_period_label(str(item["period"]).strip()))
        for item in items
        if str(item.get("stat") or "").strip().lower() in {"mean", "score"}
    }
    ordered: list[tuple[str, str]] = []
    by_scenario: dict[str, list[str]] = {}
    for scenario, period in pairs:
        by_scenario.setdefault(scenario, []).append(period)
    for scenario in ordered_scenario_keys(list(by_scenario.keys())):
        for period in ordered_period_keys(by_scenario.get(scenario, [])):
            ordered.append((scenario, period))
    return tuple(ordered)


def _bundle_pairs(spec: DashboardBundleSpec, *, data_dir: Path) -> tuple[tuple[str, str], ...]:
    pairs = set(_available_pairs_for_slug(spec.composite_slug, data_dir=data_dir))
    supported = {value.lower() for value in spec.supported_scenarios}
    pairs = {(scenario, period) for scenario, period in pairs if scenario in supported}
    ordered: list[tuple[str, str]] = []
    by_scenario: dict[str, list[str]] = {}
    for scenario, period in pairs:
        by_scenario.setdefault(scenario, []).append(period)
    for scenario in ordered_scenario_keys(list(by_scenario.keys())):
        for period in ordered_period_keys(by_scenario.get(scenario, [])):
            ordered.append((scenario, period))
    return tuple(ordered)


def _base_score_tables(
    spec: DashboardBundleSpec,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    score_frame = _load_metric_values(
        spec.composite_slug,
        scenario=scenario,
        period=period,
        stat="mean",
        data_dir=data_dir,
    ).rename(columns={spec.composite_slug: "bundle_score"})
    if score_frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    district = score_frame.copy()
    district["bundle_slug"] = spec.composite_slug
    district["bundle_name"] = spec.canonical_bundle
    district["group_key"] = spec.group_key
    district["selector_label"] = spec.selector_label
    district["scenario"] = str(scenario).strip().lower()
    district["period"] = canonical_period_label(period)
    district["bundle_score"] = pd.to_numeric(district["bundle_score"], errors="coerce")
    district["bundle_score_display"] = district["bundle_score"].map(format_score)
    district["score_band"] = district["bundle_score"].map(score_band)
    district["district_rank"] = (
        district.groupby("state_name", dropna=False)["bundle_score"]
        .rank(method="min", ascending=False, na_option="bottom")
        .where(district["bundle_score"].notna())
    )
    district["district_count"] = district.groupby("state_name", dropna=False)["bundle_score"].transform(
        lambda series: int(pd.to_numeric(series, errors="coerce").notna().sum())
    )

    state = (
        district.groupby(["state_name", "__state_key"], as_index=False, dropna=False)["bundle_score"]
        .mean()
        .reset_index(drop=True)
    )
    state["bundle_slug"] = spec.composite_slug
    state["bundle_name"] = spec.canonical_bundle
    state["group_key"] = spec.group_key
    state["selector_label"] = spec.selector_label
    state["scenario"] = str(scenario).strip().lower()
    state["period"] = canonical_period_label(period)
    state["bundle_score_display"] = state["bundle_score"].map(format_score)
    state["score_band"] = state["bundle_score"].map(score_band)
    state["state_rank"] = state["bundle_score"].rank(method="min", ascending=False, na_option="bottom").where(
        state["bundle_score"].notna()
    )
    state_count = int(pd.to_numeric(state["bundle_score"], errors="coerce").notna().sum())
    state["state_count"] = state_count

    district = district.merge(
        state[["state_name", "bundle_score", "bundle_score_display", "state_rank", "state_count"]].rename(
            columns={
                "bundle_score": "state_bundle_score",
                "bundle_score_display": "state_bundle_score_display",
            }
        ),
        on="state_name",
        how="left",
    )
    state_mean = district.groupby("state_name", dropna=False)["bundle_score"].transform(
        lambda series: pd.to_numeric(series, errors="coerce").dropna().mean()
    )
    district["state_mean_score"] = state_mean
    district["delta_vs_state_mean"] = district["bundle_score"] - district["state_mean_score"]
    district.loc[
        district["bundle_score"].isna() | district["state_mean_score"].isna(),
        "delta_vs_state_mean",
    ] = np.nan
    district["delta_vs_state_mean_display"] = district["delta_vs_state_mean"].map(_format_signed_delta)

    district_cols = [
        "bundle_slug",
        "bundle_name",
        "group_key",
        "selector_label",
        "scenario",
        "period",
        "state_name",
        "district_name",
        "__state_key",
        "__district_key",
        "bundle_score",
        "bundle_score_display",
        "score_band",
        "district_rank",
        "district_count",
        "state_bundle_score",
        "state_bundle_score_display",
        "state_rank",
        "state_count",
        "state_mean_score",
        "delta_vs_state_mean",
        "delta_vs_state_mean_display",
    ]
    non_attr = [entry for entry in get_bundle_weights(spec.canonical_bundle) if not entry.is_attribute]
    if len(non_attr) == 1:
        primary_slug = str(non_attr[0].metric_slug).strip()
        if VARIABLES.get(primary_slug, {}).get("class_labels"):
            primary_values = _load_metric_values(
                primary_slug,
                scenario=scenario,
                period=period,
                stat="mean",
                data_dir=data_dir,
            )
            if not primary_values.empty and primary_slug in primary_values.columns:
                district = district.merge(
                    primary_values[["__district_key", primary_slug]],
                    on="__district_key",
                    how="left",
                )
                district[primary_slug] = (
                    pd.to_numeric(district[primary_slug], errors="coerce")
                    .round()
                    .astype("Int64")
                )
                district_cols.append(primary_slug)
    state_cols = [
        "bundle_slug",
        "bundle_name",
        "group_key",
        "selector_label",
        "scenario",
        "period",
        "state_name",
        "__state_key",
        "bundle_score",
        "bundle_score_display",
        "score_band",
        "state_rank",
        "state_count",
    ]
    return district[district_cols].sort_values(["state_name", "district_name"], kind="stable"), state[state_cols].sort_values(
        ["state_name"], kind="stable"
    )


def _driver_rows_for_scope(
    values: pd.DataFrame,
    *,
    driver_specs: Sequence[tuple[str, str, str]],
    scope_level: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    group_cols = ["state_name", "__state_key"] if scope_level == "state" else [
        "state_name",
        "district_name",
        "__state_key",
        "__district_key",
    ]
    for keys, group in values.groupby(group_cols, dropna=False):
        key_values = keys if isinstance(keys, tuple) else (keys,)
        base = dict(zip(group_cols, key_values))
        scored: list[dict[str, object]] = []
        for slug, label, source in driver_specs:
            if slug not in group.columns:
                continue
            score = pd.to_numeric(group[slug], errors="coerce").dropna()
            if score.empty:
                continue
            scored.append(
                {
                    "driver_slug": slug,
                    "driver_label": label,
                    "driver_score": float(score.mean()),
                    "driver_source": source,
                }
            )
        scored = sorted(scored, key=lambda item: float(item["driver_score"]), reverse=True)
        for rank, item in enumerate(scored, start=1):
            rows.append(
                {
                    "scope_level": scope_level,
                    "state_name": base.get("state_name"),
                    "district_name": base.get("district_name"),
                    "__state_key": base.get("__state_key"),
                    "__district_key": base.get("__district_key"),
                    "driver_rank": rank,
                    "driver_slug": item["driver_slug"],
                    "driver_label": item["driver_label"],
                    "driver_score": item["driver_score"],
                    "driver_score_display": format_score(item["driver_score"]),
                    "driver_source": item["driver_source"],
                }
            )
    return pd.DataFrame(rows, columns=GLANCE_DRIVER_COLUMNS)


def _thematic_drivers(
    spec: DashboardBundleSpec,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> pd.DataFrame:
    values: Optional[pd.DataFrame] = None
    driver_specs: list[tuple[str, str, str]] = []
    for entry in get_bundle_weights(spec.canonical_bundle):
        if entry.is_attribute:
            continue
        slug = str(entry.metric_slug).strip()
        frame = _load_metric_values(slug, scenario=scenario, period=period, stat="mean", data_dir=data_dir)
        if frame.empty:
            continue
        raw = frame[slug]
        norm = normalize_metric_series(raw, higher_is_worse=bool(VARIABLES.get(slug, {}).get("rank_higher_is_worse", True)))
        frame = frame[["state_name", "district_name", "__state_key", "__district_key"]].copy()
        frame[slug] = norm
        values = frame if values is None else values.merge(
            frame,
            on=["state_name", "district_name", "__state_key", "__district_key"],
            how="outer",
        )
        driver_specs.append((slug, str(VARIABLES.get(slug, {}).get("label") or slug), "thematic_component_norm"))
    if values is None or not driver_specs:
        return pd.DataFrame(columns=GLANCE_DRIVER_COLUMNS)
    return pd.concat(
        [
            _driver_rows_for_scope(values, driver_specs=driver_specs, scope_level="state"),
            _driver_rows_for_scope(values, driver_specs=driver_specs, scope_level="district"),
        ],
        ignore_index=True,
    )


def _sector_drivers(
    spec: DashboardBundleSpec,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> pd.DataFrame:
    proposal_spec = get_proposal_bundle_spec_by_slug(spec.composite_slug)
    if proposal_spec is None:
        return pd.DataFrame(columns=GLANCE_DRIVER_COLUMNS)
    sources = _admin_sources_for_slug(spec.composite_slug, data_dir=data_dir)
    if not sources:
        return pd.DataFrame(columns=GLANCE_DRIVER_COLUMNS)
    df = _standardize_district_frame(normalize_master_columns(load_master_csvs(sources)))
    values = df[["state_name", "district_name", "__state_key", "__district_key"]].copy()
    driver_specs: list[tuple[str, str, str]] = []
    selected_period = canonical_period_label(period)
    for rule in proposal_spec.rules:
        column = proposal_rule_score_column(rule.rule_slug, scenario, selected_period)
        if column not in df.columns:
            continue
        values[rule.rule_slug] = pd.to_numeric(df[column], errors="coerce")
        driver_specs.append((rule.rule_slug, rule.display_label, "proposal_rule_score"))
    if not driver_specs:
        return pd.DataFrame(columns=GLANCE_DRIVER_COLUMNS)
    return pd.concat(
        [
            _driver_rows_for_scope(values, driver_specs=driver_specs, scope_level="state"),
            _driver_rows_for_scope(values, driver_specs=driver_specs, scope_level="district"),
        ],
        ignore_index=True,
    )


def _attribute_display(slug: str, value: object) -> Optional[str]:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if not np.isfinite(numeric):
        return None
    varcfg = VARIABLES.get(slug, {})
    scale = float(varcfg.get("display_scale") or 1.0)
    units = str(varcfg.get("display_units") or varcfg.get("units") or "").strip()
    return f"{float(numeric) * scale:.1f} {units}".strip()


def _attributes(spec: DashboardBundleSpec, *, data_dir: Path) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for order, entry in enumerate(get_bundle_weights(spec.canonical_bundle), start=1):
        if not entry.is_attribute:
            continue
        slug = str(entry.metric_slug).strip()
        frame = _load_metric_values(slug, scenario="snapshot", period="Current", stat="mean", data_dir=data_dir)
        if frame.empty:
            continue
        out = frame[["state_name", "district_name", "__state_key", "__district_key"]].copy()
        out["attribute_slug"] = slug
        out["attribute_label"] = str(VARIABLES.get(slug, {}).get("label") or slug)
        out["attribute_value"] = pd.to_numeric(frame[slug], errors="coerce")
        out["attribute_display"] = out["attribute_value"].map(lambda value, slug=slug: _attribute_display(slug, value))
        out["sort_order"] = order
        rows.append(out)
    if not rows:
        return pd.DataFrame(columns=GLANCE_ATTRIBUTE_COLUMNS)
    return pd.concat(rows, ignore_index=True)[GLANCE_ATTRIBUTE_COLUMNS]


def _distributions(district: pd.DataFrame, state: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    def add(scope_level: str, scores: pd.Series, *, state_name: Optional[str], state_key: Optional[str]) -> None:
        bands = scores.map(score_band)
        for order, band in enumerate(GLANCE_BANDS, start=1):
            rows.append(
                {
                    "scope_level": scope_level,
                    "state_name": state_name,
                    "__state_key": state_key,
                    "band": band,
                    "band_order": order,
                    "count": int((bands == band).sum()),
                }
            )

    add("national", state["bundle_score"], state_name=None, state_key=None)
    for state_name, group in district.groupby("state_name", dropna=False):
        key = str(group["__state_key"].iloc[0]) if "__state_key" in group.columns and not group.empty else alias(state_name)
        add("state", group["bundle_score"], state_name=str(state_name), state_key=key)
    return pd.DataFrame(rows, columns=GLANCE_DISTRIBUTION_COLUMNS)


def build_glance_view_model_for_bundle(
    composite_slug: str,
    *,
    data_dir: Path,
    overwrite: bool = False,
    dry_run: bool = False,
) -> list[GlanceBuildResult]:
    """Build persisted Glance artifacts for one dashboard composite slug."""
    spec = get_dashboard_bundle_spec_by_slug(composite_slug)
    if spec is None or "district" not in {level.lower() for level in spec.supported_levels}:
        return []
    results: list[GlanceBuildResult] = []
    for scenario, period in _bundle_pairs(spec, data_dir=data_dir):
        out_root = optimized_glance_root(spec.composite_slug, scenario=scenario, period=period, data_dir=data_dir)
        results.append(GlanceBuildResult(spec.composite_slug, scenario, period, out_root, wrote=not dry_run))
        if dry_run:
            continue
        if out_root.exists() and not overwrite and all((out_root / name).exists() for name in GLANCE_FILENAMES):
            continue
        district, state = _base_score_tables(spec, scenario=scenario, period=period, data_dir=data_dir)
        if district.empty and state.empty:
            continue
        drivers = (
            _sector_drivers(spec, scenario=scenario, period=period, data_dir=data_dir)
            if spec.group_key == "sector_wise"
            else _thematic_drivers(spec, scenario=scenario, period=period, data_dir=data_dir)
        )
        attrs = _attributes(spec, data_dir=data_dir)
        distributions = _distributions(district, state)
        out_root.mkdir(parents=True, exist_ok=True)
        district.to_parquet(out_root / "district.parquet", index=False)
        state.to_parquet(out_root / "state.parquet", index=False)
        drivers.to_parquet(out_root / "drivers.parquet", index=False)
        attrs.to_parquet(out_root / "attributes.parquet", index=False)
        distributions.to_parquet(out_root / "distributions.parquet", index=False)
    return results


def build_glance_view_models(
    *,
    data_dir: Path,
    composite_slugs: Optional[Iterable[str]] = None,
    overwrite: bool = False,
    dry_run: bool = False,
) -> list[GlanceBuildResult]:
    """Build persisted Glance artifacts for selected dashboard composite slugs."""
    selected = {str(slug).strip() for slug in composite_slugs or [] if str(slug).strip()}
    specs = [
        spec
        for spec in get_dashboard_bundle_specs()
        if spec.show_in_landing and (not selected or spec.composite_slug in selected)
    ]
    results: list[GlanceBuildResult] = []
    for spec in specs:
        results.extend(
            build_glance_view_model_for_bundle(
                spec.composite_slug,
                data_dir=data_dir,
                overwrite=overwrite,
                dry_run=dry_run,
            )
        )
    return results


def list_glance_available_pairs(*, data_dir: Path, composite_slug: str) -> tuple[tuple[str, str], ...]:
    """List scenario-period pairs with a complete Glance artifact set on disk."""
    root = optimized_glance_root(composite_slug, data_dir=data_dir)
    if not root.exists():
        return ()
    pairs: list[tuple[str, str]] = []
    for scenario_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        for period_dir in sorted(path for path in scenario_dir.iterdir() if path.is_dir()):
            if all((period_dir / name).exists() for name in GLANCE_FILENAMES):
                pairs.append((scenario_dir.name, period_dir.name))
    return tuple(pairs)


def glance_manifest_payload(*, data_dir: Path) -> dict[str, object]:
    """Return the Glance section for the optimized bundle manifest."""
    root = optimized_glance_root(data_dir=data_dir)
    bundles: list[dict[str, object]] = []
    for spec in get_dashboard_bundle_specs():
        pairs = list_glance_available_pairs(data_dir=data_dir, composite_slug=spec.composite_slug)
        if not pairs:
            continue
        bundles.append(
            {
                "bundle_slug": spec.composite_slug,
                "bundle_name": spec.canonical_bundle,
                "group_key": spec.group_key,
                "selector_label": spec.selector_label,
                "available_pairs": [{"scenario": scenario, "period": period} for scenario, period in pairs],
            }
        )
    return {
        "artifact_version": 1,
        "root": str(root),
        "required_filenames": list(GLANCE_FILENAMES),
        "bundles": bundles,
    }
