import pandas as pd

from india_resilience_tool.analysis.map_enrichment import (
    add_current_baseline_delta,
    add_rank_percentile_risk,
    add_tooltip_strings,
)


def test_add_current_baseline_delta_with_baseline() -> None:
    merged = pd.DataFrame({"m": [10.0, 20.0], "b": [5.0, 10.0]})
    out = add_current_baseline_delta(merged.copy(), metric_col="m", baseline_col="b")

    assert out["_current_value"].tolist() == [10.0, 20.0]
    assert out["_baseline_value"].tolist() == [5.0, 10.0]
    assert out["_delta_abs"].tolist() == [5.0, 10.0]
    assert out["_delta_pct"].tolist() == [100.0, 100.0]


def test_add_current_baseline_delta_without_baseline() -> None:
    merged = pd.DataFrame({"m": [10.0, 20.0]})
    out = add_current_baseline_delta(merged.copy(), metric_col="m", baseline_col="missing")

    assert out["_current_value"].tolist() == [10.0, 20.0]
    assert out["_baseline_value"].isna().all()
    assert out["_delta_abs"].isna().all()
    assert out["_delta_pct"].isna().all()


def test_add_rank_percentile_risk_state_scope_directionality() -> None:
    merged = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "_current_value": [1.0, 2.0, 3.0],
        }
    )
    out, scope = add_rank_percentile_risk(
        merged.copy(),
        admin_level="district",
        rank_higher_is_worse=True,
        alias_fn=lambda s: str(s).strip().lower(),
        risk_class_from_percentile_fn=lambda p: "OK",
    )

    assert scope == "state"
    assert out["_rank_in_state"].tolist() == [2.0, 1.0, 1.0]
    assert out["_percentile_state"].tolist() == [50.0, 100.0, 100.0]
    assert out["_risk_class"].tolist() == ["OK", "OK", "OK"]


def test_add_rank_percentile_risk_block_scope_groups_by_state_and_district() -> None:
    merged = pd.DataFrame(
        {
            "state_name": ["A", "A"],
            "district_name": ["D1", "D2"],
            "_current_value": [1.0, 2.0],
        }
    )
    out, scope = add_rank_percentile_risk(
        merged.copy(),
        admin_level="block",
        rank_higher_is_worse=True,
        alias_fn=lambda s: str(s).strip().lower(),
        risk_class_from_percentile_fn=lambda p: "OK",
    )

    assert scope == "district"
    assert out["_rank_in_state"].tolist() == [1.0, 1.0]
    assert out["_percentile_state"].tolist() == [100.0, 100.0]


def test_add_tooltip_strings_formats_missing_and_integers() -> None:
    merged = pd.DataFrame(
        {
            "_current_value": [1000.0, pd.NA],
            "_baseline_value": [500.0, pd.NA],
            "_delta_abs": [500.0, pd.NA],
            "_rank_in_state": [1.0, pd.NA],
        }
    )
    out = add_tooltip_strings(merged.copy(), map_mode="Absolute value")

    assert out["_tooltip_value"].tolist() == ["1,000", "—"]
    assert out["_tooltip_baseline"].tolist() == ["500", "—"]
    assert out["_tooltip_delta"].tolist() == ["500", "—"]
    assert out["_tooltip_rank"].tolist() == ["1", "—"]

