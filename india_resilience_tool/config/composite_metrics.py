"""Compatibility wrapper for dashboard composite bundle specs.

Thematic visible-Glance composites remain available here for existing callers,
but the canonical dashboard bundle catalog now lives in
``india_resilience_tool.config.dashboard_bundles``.
"""

from __future__ import annotations

from dataclasses import dataclass

from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.dashboard_bundles import (
    THEMATIC_DASHBOARD_BUNDLES,
    composite_slug_for_bundle,
    get_dashboard_bundle_spec,
    is_dashboard_bundle_slug,
)


@dataclass(frozen=True)
class CompositeMetricSpec:
    """One persisted thematic composite metric mapped to a visible Glance bundle."""

    bundle_domain: str
    composite_slug: str
    composite_label: str
    component_metric_slugs: tuple[str, ...]
    supported_spatial_families: tuple[str, ...] = ("admin",)
    supported_levels: tuple[str, ...] = ("district", "block")


VISIBLE_GLANCE_COMPOSITES: tuple[CompositeMetricSpec, ...] = tuple(
    CompositeMetricSpec(
        bundle_domain=spec.canonical_bundle,
        composite_slug=spec.composite_slug,
        composite_label=spec.composite_label,
        component_metric_slugs=tuple(entry.metric_slug for entry in get_bundle_weights(spec.canonical_bundle)),
        supported_levels=spec.supported_levels,
    )
    for spec in THEMATIC_DASHBOARD_BUNDLES
)

COMPOSITES_BY_BUNDLE: dict[str, CompositeMetricSpec] = {
    spec.bundle_domain: spec for spec in VISIBLE_GLANCE_COMPOSITES
}
COMPOSITES_BY_SLUG: dict[str, CompositeMetricSpec] = {
    spec.composite_slug: spec for spec in VISIBLE_GLANCE_COMPOSITES
}


def get_composite_metric_for_bundle(bundle_domain: str) -> CompositeMetricSpec | None:
    """Return the persisted thematic composite spec for one visible Glance bundle."""
    return COMPOSITES_BY_BUNDLE.get(str(bundle_domain).strip())


def get_dashboard_composite_slug_for_bundle(bundle_domain: str) -> str | None:
    """Return the dashboard composite slug for any dashboard bundle/domain."""
    return composite_slug_for_bundle(bundle_domain)


def is_composite_metric(metric_slug: str) -> bool:
    """Return whether a metric slug belongs to the dashboard bundle catalog."""
    return is_dashboard_bundle_slug(metric_slug)


def get_visible_glance_composite_slugs() -> tuple[str, ...]:
    """Return thematic visible-Glance composite metric slugs in UX order."""
    return tuple(spec.composite_slug for spec in VISIBLE_GLANCE_COMPOSITES)


def validate_composite_metric_specs() -> list[str]:
    """Return validation issues for the thematic visible-Glance subset."""
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

        dashboard_spec = get_dashboard_bundle_spec(spec.bundle_domain)
        if dashboard_spec is None:
            issues.append(f"Composite metric bundle {spec.bundle_domain!r} is missing from dashboard bundles.")
        elif dashboard_spec.composite_slug != spec.composite_slug:
            issues.append(
                f"Composite metric bundle {spec.bundle_domain!r} does not match dashboard slug "
                f"{dashboard_spec.composite_slug!r}."
            )

    return issues
