import pytest


def test_scenario_y_range_auto_zooms_close_to_max_values() -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [27.40, 27.91, 27.93, 29.02, 30.13]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy="auto")

    assert y_range is not None
    y_low, y_high = y_range
    assert y_low < y_high
    assert zoomed is True
    assert y_low > 0.0


def test_scenario_y_range_auto_zooms_high_baseline_counts() -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [114.66, 127.94, 129.33, 139.13, 146.64, 148.79, 173.02]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy="auto")

    assert y_range is not None
    y_low, y_high = y_range
    assert y_low < y_high
    assert zoomed is True
    assert y_low > 0.0


def test_scenario_y_range_auto_keeps_zero_baseline_near_zero() -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [0.2, 1.0, 4.0]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy="auto")

    assert y_range is not None
    y_low, y_high = y_range
    assert y_low == 0.0
    assert y_high > 0.0
    assert zoomed is False


def test_scenario_y_range_negative_values_use_tight_range() -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [-2.0, 0.5, 1.0]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy="auto")

    assert y_range is not None
    y_low, y_high = y_range
    assert y_low < 0.0
    assert y_high > 0.0
    assert zoomed is False


@pytest.mark.parametrize(
    ("policy", "expected_low"),
    [
        ("zero", 0.0),
        ("ZERO", 0.0),
    ],
)
def test_scenario_y_range_policy_zero_forces_zero_baseline(policy: str, expected_low: float) -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [114.66, 127.94, 129.33]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy=policy)

    assert y_range is not None
    y_low, _ = y_range
    assert y_low == expected_low
    assert zoomed is False


def test_scenario_y_range_policy_tight_zooms_nonnegative_data() -> None:
    from india_resilience_tool.viz.charts import compute_scenario_y_range

    values = [114.66, 127.94, 129.33]
    zoomed, y_range = compute_scenario_y_range(values, y_axis_policy="tight")

    assert y_range is not None
    y_low, _ = y_range
    assert y_low > 0.0
    assert zoomed is True

