"""
Variable/Index registry and configuration for IRT.

This module defines the climate indices available in the dashboard,
their metadata, and display groupings.

IMPORTANT: This module imports from metrics_registry.py to ensure
dashboard and pipeline use identical definitions. Do not define metrics
here - add them to metrics_registry.py instead.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Dict

# Import from the unified registry (single source of truth)
from india_resilience_tool.config.metrics_registry import (
    METRICS_BY_SLUG,
    get_dashboard_variables,
    get_metrics_by_group,
    get_metric_count,
    # Bundle exports (NEW)
    BUNDLES,
    BUNDLE_ORDER,
    BUNDLE_DESCRIPTIONS,
    DEFAULT_BUNDLE,
    get_bundles,
    get_metrics_for_bundle,
    get_bundle_for_metric,
    get_bundle_description,
    get_default_bundle,
    get_metric_options_for_bundle,
    validate_bundles,
)

# ---- Variable/Index registry ----
# Auto-generated from metrics_registry.py
VARIABLES: Dict[str, Dict[str, Any]] = get_dashboard_variables()


INDEX_GROUP_LABELS: Dict[str, str] = {
    "temperature": "Temperature",
    "rain": "Rainfall",
}


def get_index_groups() -> list[str]:
    """Return ordered list of index groups (Temperature first, then Rainfall, then others)."""
    raw_groups = {cfg.get("group", "other") for cfg in VARIABLES.values()}
    preferred_order = ["temperature", "rain"]
    all_groups: list[str] = []
    for g in preferred_order:
        if g in raw_groups:
            all_groups.append(g)
    for g in sorted(raw_groups):
        if g not in all_groups:
            all_groups.append(g)
    return all_groups


def get_indices_for_group(group: str) -> list[str]:
    """Return list of index slugs for a given group."""
    return [slug for slug, cfg in VARIABLES.items() if cfg.get("group", "other") == group]


# -----------------------------------------------------------------------------
# VALIDATION
# -----------------------------------------------------------------------------
def validate_variables() -> list[str]:
    """
    Validate that VARIABLES dict is properly populated.
    Returns list of issues (empty if valid).
    """
    issues = []
    
    required_keys = {"label", "group", "periods_metric_col"}
    
    for slug, cfg in VARIABLES.items():
        missing = required_keys - set(cfg.keys())
        if missing:
            issues.append(f"Slug '{slug}' missing required keys: {missing}")
        
        if not cfg.get("periods_metric_col"):
            issues.append(f"Slug '{slug}' has empty periods_metric_col")
    
    # Also validate bundles
    bundle_issues = validate_bundles()
    issues.extend(bundle_issues)
    
    return issues


def print_index_summary():
    """Print a summary of available indices."""
    counts = get_metric_count()
    total = sum(counts.values())
    
    print(f"\n{'='*60}")
    print(f"India Resilience Tool - Climate Index Summary")
    print(f"{'='*60}")
    print(f"Total indices: {total}")
    print(f"\nBy category:")
    for group, count in sorted(counts.items()):
        label = INDEX_GROUP_LABELS.get(group, group.title())
        print(f"  {label}: {count} indices")
    
    print(f"\n{'='*60}")
    print("Index list by category:")
    print(f"{'='*60}")
    
    groups = get_metrics_by_group()
    for group in ["temperature", "rain"]:
        if group in groups:
            label = INDEX_GROUP_LABELS.get(group, group.title())
            print(f"\n{label}:")
            for slug in groups[group]:
                cfg = VARIABLES[slug]
                print(f"  - {cfg['label']} ({slug})")
    
    # Also print bundle summary
    print(f"\n{'='*60}")
    print("Bundles:")
    print(f"{'='*60}")
    for bundle in get_bundles():
        slugs = get_metrics_for_bundle(bundle)
        print(f"\n{bundle} ({len(slugs)} metrics)")


if __name__ == "__main__":
    # Run validation and print summary when executed directly
    issues = validate_variables()
    if issues:
        print("Validation issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("All variables validated successfully!")
    
    print_index_summary()