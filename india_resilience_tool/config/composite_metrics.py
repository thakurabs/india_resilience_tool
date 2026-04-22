"""Declarative persisted composite metrics for visible Glance bundles."""

from __future__ import annotations

from dataclasses import dataclass

from india_resilience_tool.config.bundle_weights import get_bundle_weights


@dataclass(frozen=True)
class CompositeMetricSpec:
    """One persisted composite metric mapped to a visible Glance bundle."""

    bundle_domain: str
    composite_slug: str
    composite_label: str
    component_metric_slugs: tuple[str, ...]
    supported_spatial_families: tuple[str, ...] = ("admin",)
    supported_levels: tuple[str, ...] = ("district", "block")


_VISIBLE_GLANCE_COMPOSITE_BASE: tuple[tuple[str, str, str], ...] = (
    ("Heat Risk", "composite_heat_risk", "Composite Heat Risk"),
    ("Drought Risk", "composite_drought_risk", "Composite Drought Risk"),
    (
        "Flood & Extreme Rainfall Risk",
        "composite_flood_extreme_rainfall_risk",
        "Composite Flood & Extreme Rainfall Risk",
    ),
    ("Heat Stress", "composite_heat_stress", "Composite Heat Stress"),
    ("Cold Risk", "composite_cold_risk", "Composite Cold Risk"),
    (
        "Agriculture & Growing Conditions",
        "composite_agriculture_growing_conditions",
        "Composite Agriculture & Growing Conditions",
    ),
)


VISIBLE_GLANCE_COMPOSITES: tuple[CompositeMetricSpec, ...] = tuple(
    CompositeMetricSpec(
        bundle_domain=bundle_domain,
        composite_slug=composite_slug,
        composite_label=composite_label,
        component_metric_slugs=tuple(entry.metric_slug for entry in get_bundle_weights(bundle_domain)),
    )
    for bundle_domain, composite_slug, composite_label in _VISIBLE_GLANCE_COMPOSITE_BASE
)

COMPOSITES_BY_BUNDLE: dict[str, CompositeMetricSpec] = {
    spec.bundle_domain: spec for spec in VISIBLE_GLANCE_COMPOSITES
}
COMPOSITES_BY_SLUG: dict[str, CompositeMetricSpec] = {
    spec.composite_slug: spec for spec in VISIBLE_GLANCE_COMPOSITES
}


def get_composite_metric_for_bundle(bundle_domain: str) -> CompositeMetricSpec | None:
    """Return the persisted composite metric spec for one visible Glance bundle."""
    return COMPOSITES_BY_BUNDLE.get(str(bundle_domain).strip())


def is_composite_metric(metric_slug: str) -> bool:
    """Return whether a metric slug is one of the persisted visible-Glance composites."""
    return str(metric_slug).strip() in COMPOSITES_BY_SLUG


def get_visible_glance_composite_slugs() -> tuple[str, ...]:
    """Return composite metric slugs in visible Glance order."""
    return tuple(spec.composite_slug for spec in VISIBLE_GLANCE_COMPOSITES)


def validate_composite_metric_specs() -> list[str]:
    """Return validation issues for the declarative composite mapping layer."""
    issues: list[str] = []
    seen_bundles: set[str] = set()
    seen_slugs: set[str] = set()
    for spec in VISIBLE_GLANCE_COMPOSITES:
        if not spec.bundle_domain:
            issues.append("Composite metric config contains an empty bundle domain.")
        if spec.bundle_domain in seen_bundles:
            issues.append(f"Composite metric bundle {spec.bundle_domain!r} is declared more than once.")
        seen_bundles.add(spec.bundle_domain)

        if not spec.composite_slug:
            issues.append(f"Composite metric bundle {spec.bundle_domain!r} has an empty composite slug.")
        if spec.composite_slug in seen_slugs:
            issues.append(f"Composite metric slug {spec.composite_slug!r} is declared more than once.")
        seen_slugs.add(spec.composite_slug)

        if not spec.component_metric_slugs:
            issues.append(f"Composite metric bundle {spec.bundle_domain!r} has no component metrics.")

    return issues
