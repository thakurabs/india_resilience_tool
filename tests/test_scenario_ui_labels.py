from india_resilience_tool.config.constants import SCENARIO_UI_LABEL


def test_scenario_ui_label_required_keys_exist() -> None:
    for key in ("ssp245", "ssp585", "historical"):
        assert key in SCENARIO_UI_LABEL


def test_scenario_ui_label_contains_friendly_name_and_ssp_code() -> None:
    assert "Middle-of-the-road" in SCENARIO_UI_LABEL["ssp245"]
    assert "SSP2-4.5" in SCENARIO_UI_LABEL["ssp245"]
    assert "Fossil-fuelled development" in SCENARIO_UI_LABEL["ssp585"]
    assert "SSP5-8.5" in SCENARIO_UI_LABEL["ssp585"]
