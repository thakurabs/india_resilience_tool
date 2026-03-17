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
