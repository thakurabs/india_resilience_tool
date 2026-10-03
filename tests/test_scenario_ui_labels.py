from __future__ import annotations


def test_scenario_ui_labels_include_expected_keys() -> None:
    from india_resilience_tool.config.constants import SCENARIO_UI_LABEL

    assert "ssp245" in SCENARIO_UI_LABEL
    assert "ssp585" in SCENARIO_UI_LABEL
    assert "historical" in SCENARIO_UI_LABEL
    assert "snapshot" in SCENARIO_UI_LABEL

    # The bau/opt/pes pathways were Aqueduct-only and were retired with it.
    assert "bau" not in SCENARIO_UI_LABEL
    assert "opt" not in SCENARIO_UI_LABEL
    assert "pes" not in SCENARIO_UI_LABEL


def test_scenario_ui_labels_are_friendly_and_scientific() -> None:
    from india_resilience_tool.config.constants import SCENARIO_UI_LABEL

    s245 = SCENARIO_UI_LABEL["ssp245"]
    assert "Middle-of-the-road" in s245
    assert "SSP2-4.5" in s245

    s585 = SCENARIO_UI_LABEL["ssp585"]
    assert "Fossil-fuelled development" in s585
    assert "SSP5-8.5" in s585

    assert SCENARIO_UI_LABEL["historical"] == "Historical"
