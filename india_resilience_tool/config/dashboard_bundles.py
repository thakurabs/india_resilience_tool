"""Declarative dashboard-selectable bundle catalog.

This module is the sole source of truth for dashboard bundle ordering,
grouped selector labels, canonical bundle names, and composite-slug mapping.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardBundleSpec:
    """One dashboard-selectable bundle/domain and its composite binding."""

    group_key: str
    group_label: str
    canonical_bundle: str
    selector_label: str
    composite_slug: str
    composite_label: str
    supported_levels: tuple[str, ...]
    show_in_landing: bool


DASHBOARD_BUNDLES: tuple[DashboardBundleSpec, ...] = (
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Heat Risk",
        selector_label="Thematic - Heat Risk",
        composite_slug="composite_heat_risk",
        composite_label="Composite Heat Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Drought Risk",
        selector_label="Thematic - Drought Risk",
        composite_slug="composite_drought_risk",
        composite_label="Composite Drought Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Extreme Rainfall | Flash Flood Risk",
        selector_label="Thematic - Extreme Rainfall | Flash Flood Risk",
        composite_slug="composite_flood_extreme_rainfall_risk",
        composite_label="Composite Flash Flood Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Riverine Flood",
        selector_label="Thematic - Riverine Flood",
        composite_slug="composite_flood_jrc_depth",
        composite_label="RP-100 Flood Severity Index",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Heat Stress",
        selector_label="Thematic - Heat Stress",
        composite_slug="composite_heat_stress",
        composite_label="Composite Heat Stress",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Cold Risk",
        selector_label="Thematic - Cold Risk",
        composite_slug="composite_cold_risk",
        composite_label="Composite Cold Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="thematic",
        group_label="Thematic",
        canonical_bundle="Agriculture & Growing Conditions",
        selector_label="Thematic - Agriculture & Growing Conditions",
        composite_slug="composite_agriculture_growing_conditions",
        composite_label="Composite Agriculture & Growing Conditions",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Agricultural Risk",
        selector_label="Sector-wise - Agricultural Risk",
        composite_slug="composite_agricultural_risk",
        composite_label="Composite Agricultural Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Health Risk",
        selector_label="Sector-wise - Health Risk",
        composite_slug="composite_health_risk",
        composite_label="Composite Health Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Industrial Risk",
        selector_label="Sector-wise - Industrial Risk",
        composite_slug="composite_industrial_risk",
        composite_label="Composite Industrial Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Investment / Financial Risk",
        selector_label="Sector-wise - Investment / Financial Risk",
        composite_slug="composite_investment_financial_risk",
        composite_label="Composite Investment / Financial Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Infrastructure Risk",
        selector_label="Sector-wise - Infrastructure Risk",
        composite_slug="composite_infrastructure_risk",
        composite_label="Composite Infrastructure Risk",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Asset Risk (Thermal Power Plants)",
        selector_label="Sector-wise - Asset Risk (Thermal Power Plants)",
        composite_slug="composite_asset_risk_thermal_power",
        composite_label="Composite Asset Risk (Thermal Power Plants)",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Asset Risk (Hydropower Plants)",
        selector_label="Sector-wise - Asset Risk (Hydropower Plants)",
        composite_slug="composite_asset_risk_hydropower",
        composite_label="Composite Asset Risk (Hydropower Plants)",
        supported_levels=("district", "block"),
        show_in_landing=True,
    ),
    DashboardBundleSpec(
        group_key="sector_wise",
        group_label="Sector-wise",
        canonical_bundle="Life & Livelihood Loss Risk",
        selector_label="Sector-wise - Life & Livelihood Loss Risk",
        composite_slug="composite_life_livelihood_loss_risk",
        composite_label="Composite Life & Livelihood Loss Risk",
        supported_levels=("district",),
        show_in_landing=True,
    ),
)

THEMATIC_DASHBOARD_BUNDLES: tuple[DashboardBundleSpec, ...] = tuple(
    spec for spec in DASHBOARD_BUNDLES if spec.group_key == "thematic"
)
SECTOR_WISE_DASHBOARD_BUNDLES: tuple[DashboardBundleSpec, ...] = tuple(
    spec for spec in DASHBOARD_BUNDLES if spec.group_key == "sector_wise"
)
DASHBOARD_BUNDLES_BY_NAME: dict[str, DashboardBundleSpec] = {
    spec.canonical_bundle: spec for spec in DASHBOARD_BUNDLES
}
DASHBOARD_BUNDLES_BY_SLUG: dict[str, DashboardBundleSpec] = {
    spec.composite_slug: spec for spec in DASHBOARD_BUNDLES
}


def get_dashboard_bundle_specs() -> tuple[DashboardBundleSpec, ...]:
    """Return all dashboard bundle specs in canonical selector order."""
    return DASHBOARD_BUNDLES


def get_dashboard_bundle_spec(bundle_name: str) -> DashboardBundleSpec | None:
    """Return one dashboard bundle spec by canonical bundle/domain name."""
    return DASHBOARD_BUNDLES_BY_NAME.get(str(bundle_name or "").strip())


def get_dashboard_bundle_spec_by_slug(composite_slug: str) -> DashboardBundleSpec | None:
    """Return one dashboard bundle spec by composite slug."""
    return DASHBOARD_BUNDLES_BY_SLUG.get(str(composite_slug or "").strip())


def is_dashboard_bundle(bundle_name: str) -> bool:
    """Return whether a bundle/domain is part of the dashboard bundle catalog."""
    return get_dashboard_bundle_spec(bundle_name) is not None


def is_dashboard_bundle_slug(composite_slug: str) -> bool:
    """Return whether a composite slug belongs to the dashboard bundle catalog."""
    return get_dashboard_bundle_spec_by_slug(composite_slug) is not None


def grouped_bundle_label(bundle_name: str) -> str:
    """Return the grouped selector label for a canonical bundle/domain name."""
    spec = get_dashboard_bundle_spec(bundle_name)
    if spec is None:
        return str(bundle_name or "").strip()
    return spec.selector_label


def composite_slug_for_bundle(bundle_name: str) -> str | None:
    """Return the dashboard composite slug for a canonical bundle/domain name."""
    spec = get_dashboard_bundle_spec(bundle_name)
    return None if spec is None else spec.composite_slug


def dashboard_bundle_names(*, level: str | None = None, landing_only: bool = False) -> tuple[str, ...]:
    """Return canonical bundle names filtered by optional level and landing visibility."""
    level_norm = str(level or "").strip().lower()
    out: list[str] = []
    for spec in DASHBOARD_BUNDLES:
        if landing_only and not spec.show_in_landing:
            continue
        if level_norm and level_norm not in {v.lower() for v in spec.supported_levels}:
            continue
        out.append(spec.canonical_bundle)
    return tuple(out)


def dashboard_composite_slugs() -> tuple[str, ...]:
    """Return all dashboard composite slugs in catalog order."""
    return tuple(spec.composite_slug for spec in DASHBOARD_BUNDLES)


def validate_dashboard_bundle_specs() -> list[str]:
    """Return validation issues for the dashboard bundle catalog."""
    issues: list[str] = []
    seen_names: set[str] = set()
    seen_labels: set[str] = set()
    seen_slugs: set[str] = set()
    for spec in DASHBOARD_BUNDLES:
        if spec.canonical_bundle in seen_names:
            issues.append(f"Duplicate dashboard bundle name: {spec.canonical_bundle!r}")
        seen_names.add(spec.canonical_bundle)

        if spec.selector_label in seen_labels:
            issues.append(f"Duplicate dashboard selector label: {spec.selector_label!r}")
        seen_labels.add(spec.selector_label)

        if spec.composite_slug in seen_slugs:
            issues.append(f"Duplicate dashboard composite slug: {spec.composite_slug!r}")
        seen_slugs.add(spec.composite_slug)

        if not spec.supported_levels:
            issues.append(f"Dashboard bundle {spec.canonical_bundle!r} supports no levels.")

    return issues
