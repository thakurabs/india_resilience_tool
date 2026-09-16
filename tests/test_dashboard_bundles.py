from __future__ import annotations

from india_resilience_tool.config.composite_metrics import VISIBLE_GLANCE_COMPOSITES
from india_resilience_tool.config.dashboard_bundles import (
    DASHBOARD_BUNDLES,
    THEMATIC_DASHBOARD_BUNDLES,
    composite_slug_for_bundle,
    grouped_bundle_label,
    validate_dashboard_bundle_specs,
)


def test_dashboard_bundle_catalog_is_exact() -> None:
    observed = [
        (
            spec.group_key,
            spec.group_label,
            spec.canonical_bundle,
            spec.selector_label,
            spec.composite_slug,
            spec.composite_label,
            spec.supported_levels,
            spec.show_in_landing,
        )
        for spec in DASHBOARD_BUNDLES
    ]
    assert observed == [
        (
            "thematic",
            "Thematic",
            "Heat Risk",
            "Thematic - Heat Risk",
            "composite_heat_risk",
            "Composite Heat Risk",
            ("district", "block"),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Drought Risk",
            "Thematic - Drought Risk",
            "composite_drought_risk",
            "Composite Drought Risk",
            ("district", "block"),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Extreme Rainfall | Flash Flood Risk",
            "Thematic - Extreme Rainfall | Flash Flood Risk",
            "composite_flood_extreme_rainfall_risk",
            "Composite Flash Flood Risk",
            ("district", "block"),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Riverine Flood",
            "Thematic - Riverine Flood",
            "composite_flood_jrc_depth",
            "Composite Riverine Flood Risk",
            ("district", "block"),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Water Risk",
            "Thematic - Water Risk",
            "composite_water_risk",
            "Composite Water Risk",
            ("district",),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Heat Stress",
            "Thematic - Heat Stress",
            "composite_heat_stress",
            "Composite Heat Stress",
            ("district", "block"),
            True,
        ),
        (
            "thematic",
            "Thematic",
            "Cold Risk",
            "Thematic - Cold Risk",
            "composite_cold_risk",
            "Composite Cold Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Agricultural Risk",
            "Sector-wise - Agricultural Risk",
            "composite_agricultural_risk",
            "Composite Agricultural Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Health Risk",
            "Sector-wise - Health Risk",
            "composite_health_risk",
            "Composite Health Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Industrial Risk",
            "Sector-wise - Industrial Risk",
            "composite_industrial_risk",
            "Composite Industrial Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Investment / Financial Risk",
            "Sector-wise - Investment / Financial Risk",
            "composite_investment_financial_risk",
            "Composite Investment / Financial Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Infrastructure Risk",
            "Sector-wise - Infrastructure Risk",
            "composite_infrastructure_risk",
            "Composite Infrastructure Risk",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Asset Risk (Thermal Power Plants)",
            "Sector-wise - Asset Risk (Thermal Power Plants)",
            "composite_asset_risk_thermal_power",
            "Composite Asset Risk (Thermal Power Plants)",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Asset Risk (Hydropower Plants)",
            "Sector-wise - Asset Risk (Hydropower Plants)",
            "composite_asset_risk_hydropower",
            "Composite Asset Risk (Hydropower Plants)",
            ("district", "block"),
            True,
        ),
        (
            "sector_wise",
            "Sector-wise",
            "Life & Livelihood Loss Risk",
            "Sector-wise - Life & Livelihood Loss Risk",
            "composite_life_livelihood_loss_risk",
            "Composite Life & Livelihood Loss Risk",
            ("district", "block"),
            True,
        ),
    ]


def test_dashboard_bundle_helpers_are_exact() -> None:
    assert validate_dashboard_bundle_specs() == []
    assert grouped_bundle_label("Extreme Rainfall | Flash Flood Risk") == "Thematic - Extreme Rainfall | Flash Flood Risk"
    assert composite_slug_for_bundle("Health Risk") == "composite_health_risk"
    assert composite_slug_for_bundle("Agriculture & Growing Conditions") is None


def test_thematic_visible_glance_subset_delegates_to_dashboard_catalog() -> None:
    assert [spec.bundle_domain for spec in VISIBLE_GLANCE_COMPOSITES] == [
        spec.canonical_bundle for spec in THEMATIC_DASHBOARD_BUNDLES
    ]
    assert [spec.composite_slug for spec in VISIBLE_GLANCE_COMPOSITES] == [
        spec.composite_slug for spec in THEMATIC_DASHBOARD_BUNDLES
    ]
