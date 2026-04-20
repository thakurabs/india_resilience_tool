"""
Smoke tests for config modules.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations


def test_config_variables_imports() -> None:
    """Test that the variables config module imports correctly."""
    from india_resilience_tool.config import variables

    assert variables is not None
    assert hasattr(variables, "VARIABLES")
    assert hasattr(variables, "INDEX_GROUP_LABELS")
    assert hasattr(variables, "get_pillars")
    assert hasattr(variables, "get_domains_for_pillar")


def test_config_variables_exports() -> None:
    """Test that VARIABLES contains expected structure."""
    from india_resilience_tool.config.variables import VARIABLES, INDEX_GROUP_LABELS

    assert isinstance(VARIABLES, dict)
    assert len(VARIABLES) > 0

    # Check at least one entry has expected keys
    first_key = next(iter(VARIABLES))
    first_entry = VARIABLES[first_key]
    assert "label" in first_entry
    assert "group" in first_entry
    assert "periods_metric_col" in first_entry


def test_config_constants_imports() -> None:
    """Test that the constants config module imports correctly."""
    from india_resilience_tool.config import constants

    assert constants is not None
    assert hasattr(constants, "SIMPLIFY_TOL_ADM2")
    assert hasattr(constants, "FIG_SIZE_PANEL")


def test_config_helper_functions() -> None:
    """Test helper functions in variables module."""
    from india_resilience_tool.config.variables import get_index_groups, get_indices_for_group

    groups = get_index_groups()
    assert isinstance(groups, list)
    assert "temperature" in groups

    temp_indices = get_indices_for_group("temperature")
    assert isinstance(temp_indices, list)
    assert len(temp_indices) > 0


def test_aqueduct_metrics_are_exposed_to_dashboard_variables() -> None:
    """Aqueduct metrics should be available through the dashboard variable registry."""
    from india_resilience_tool.config.variables import VARIABLES

    for slug, label in [
        ("aq_water_stress", "Aqueduct Water Stress"),
        ("aq_interannual_variability", "Aqueduct Interannual Variability"),
        ("aq_seasonal_variability", "Aqueduct Seasonal Variability"),
        ("aq_water_depletion", "Aqueduct Water Depletion"),
    ]:
        cfg = VARIABLES[slug]
        assert cfg["label"] == label
        assert cfg["source_type"] == "external"
        assert cfg["supports_yearly_trend"] is False
        assert cfg["supported_scenarios"] == ["historical", "bau", "opt", "pes"]
        assert cfg["supported_levels"] == ["district", "block", "basin", "sub_basin"]
        assert "Aqueduct Water Risk" in cfg["domains"]
        assert "Bio-physical Hazards" in cfg["pillars"]


def test_population_metrics_are_exposed_as_static_admin_layers() -> None:
    """Population metrics should appear as fixed-snapshot exposure layers."""
    from india_resilience_tool.config.variables import VARIABLES

    for slug, label, units in [
        ("population_total", "Total Population", "people"),
        ("population_density", "Population Density", "people/km2"),
    ]:
        cfg = VARIABLES[slug]
        assert cfg["label"] == label
        assert cfg["source_type"] == "external"
        assert cfg["selection_mode"] == "static_snapshot"
        assert cfg["fixed_scenario"] == "snapshot"
        assert cfg["fixed_period"] == "2025"
        assert cfg["supported_scenarios"] == ["snapshot"]
        assert cfg["supported_levels"] == ["district", "block"]
        assert cfg["supported_spatial_families"] == ["admin"]
        assert cfg["supported_statistics"] == ["mean"]
        assert cfg["supports_baseline_comparison"] is False
        assert cfg["supports_scenario_comparison"] is False
        assert cfg["units"] == units
        assert "Population Exposure" in cfg["domains"]
        assert "Exposure" in cfg["pillars"]


def test_groundwater_metrics_are_exposed_as_static_district_layers() -> None:
    """Groundwater metrics should appear as fixed-snapshot district layers."""
    from india_resilience_tool.config.variables import VARIABLES

    for slug, label, units, worse_high in [
        ("gw_stage_extraction_pct", "Stage of Ground Water Extraction", "%", True),
        (
            "gw_future_availability_ham",
            "Net Annual Ground Water Availability for Future Use",
            "ham",
            False,
        ),
        ("gw_extractable_resource_ham", "Annual Extractable Ground Water Resource", "ham", False),
        ("gw_total_extraction_ham", "Ground Water Extraction for All Uses", "ha.m", True),
    ]:
        cfg = VARIABLES[slug]
        assert cfg["label"] == label
        assert cfg["source_type"] == "external"
        assert cfg["selection_mode"] == "static_snapshot"
        assert cfg["fixed_scenario"] == "snapshot"
        assert cfg["fixed_period"] == "2024-2025"
        assert cfg["supported_scenarios"] == ["snapshot"]
        assert cfg["supported_levels"] == ["district"]
        assert cfg["supported_spatial_families"] == ["admin"]
        assert cfg["supported_statistics"] == ["mean"]
        assert cfg["supports_baseline_comparison"] is False
        assert cfg["supports_scenario_comparison"] is False
        assert cfg["units"] == units
        assert cfg["rank_higher_is_worse"] is worse_high
        assert "Groundwater Status & Availability" in cfg["domains"]
        assert "Bio-physical Hazards" in cfg["pillars"]


def test_jrc_metrics_are_exposed_as_static_telangana_admin_layers() -> None:
    """JRC metrics should appear as static Telangana-only admin layers."""
    from india_resilience_tool.config.variables import VARIABLES

    for slug, label in [
        ("jrc_flood_depth_index_rp100", "Flood Depth Index (RP-100)"),
        ("jrc_flood_extent_rp100", "RP-100 Flood Extent"),
        ("jrc_flood_depth_rp10", "RP-10 Flood Depth"),
        ("jrc_flood_depth_rp50", "RP-50 Flood Depth"),
        ("jrc_flood_depth_rp100", "RP-100 Flood Depth"),
        ("jrc_flood_depth_rp500", "RP-500 Flood Depth"),
    ]:
        cfg = VARIABLES[slug]
        assert cfg["label"] == label
        assert cfg["source_type"] == "external"
        assert cfg["selection_mode"] == "static_snapshot"
        assert cfg["fixed_scenario"] == "snapshot"
        assert cfg["fixed_period"] == "Current"
        assert cfg["supported_scenarios"] == ["snapshot"]
        assert cfg["supported_levels"] == ["district", "block"]
        assert cfg["supported_spatial_families"] == ["admin"]
        assert cfg["supported_statistics"] == ["mean"]
        assert cfg["supports_yearly_trend"] is False
        assert cfg["supports_baseline_comparison"] is False
        assert cfg["supports_scenario_comparison"] is False
        assert cfg["supported_admin_states"] == ["Telangana"]
        if slug == "jrc_flood_depth_index_rp100":
            assert cfg["units"] == "severity class (1-5)"
        elif slug == "jrc_flood_extent_rp100":
            assert cfg["units"] == "fraction"
            assert cfg["display_units"] == "%"
            assert cfg["display_scale"] == 100.0
        else:
            assert cfg["units"] == "m"
        assert "Flood Inundation Depth (JRC)" in cfg["domains"]
        assert "Bio-physical Hazards" in cfg["pillars"]


def test_current_period_display_label_is_stable() -> None:
    from india_resilience_tool.viz.charts import period_display_label

    assert period_display_label("Current") == "Current"
