"""Guards for the Overview prototype's Context and Evidence aggregation (CHG-0400).

The workflow is explicit that a State/UT context statistic is not the average of
its districts' percentages: counts and areas are summed and every share and rate
is recomputed from the State/UT totals. That distinction is invisible in a
rendered card and easy to lose in a later edit, so it is pinned here.
"""

from __future__ import annotations

import pandas as pd

from tools.diagnostics.build_overview_flow_prototype import (
    _parse_also,
    _state_exposure_rows,
)


def _exposure() -> pd.DataFrame:
    """Two districts in one State/UT, with deliberately unequal areas.

    The small district is almost entirely built up and the large one barely at
    all, so an average of the two percentages (25.5%) is far from the correct
    area-weighted share (6.0%).
    """
    return pd.DataFrame(
        [
            {
                "admin_key": "s|small",
                "admin_level": "district",
                "state_name": "Teststan",
                "pop_2020": 100_000.0,
                "rural_facilities_total_count": 100.0,
                "rural_facilities_agro_count": 40.0,
                "rural_facilities_education_count": 30.0,
                "rural_facilities_health_count": 10.0,
                "rural_facilities_service_count": 20.0,
                "built_up_area_km2": 50.0,
                "lulc_agri_area_km2": 10.0,
            },
            {
                "admin_key": "s|large",
                "admin_level": "district",
                "state_name": "Teststan",
                "pop_2020": 900_000.0,
                "rural_facilities_total_count": 300.0,
                "rural_facilities_agro_count": 100.0,
                "rural_facilities_education_count": 100.0,
                "rural_facilities_health_count": 50.0,
                "rural_facilities_service_count": 50.0,
                "built_up_area_km2": 10.0,
                "lulc_agri_area_km2": 800.0,
            },
        ]
    )


#: Areas in m^2, matching the canonical ``area_m2`` the score weighting uses.
_AREAS = {"s|small": 100e6, "s|large": 900e6}


def test_state_counts_are_summed() -> None:
    row = _state_exposure_rows(_exposure(), {"s|small", "s|large"}, _AREAS)["Teststan"]
    assert row["pop"] == 1_000_000
    assert row["rf"] == 400
    assert row["rf_agro"] == 140
    assert row["bu"] == 60.0
    assert row["ag"] == 810.0
    assert row["n"] == 2


def test_state_shares_come_from_totals_not_from_averaged_percentages() -> None:
    row = _state_exposure_rows(_exposure(), {"s|small", "s|large"}, _AREAS)["Teststan"]
    # 60 km^2 built up over 1,000 km^2 of State/UT area.
    assert row["bu_pct"] == 6.0
    assert row["ag_pct"] == 81.0
    # The average of the district percentages would be 25.5 and 40.5.
    assert row["bu_pct"] != 25.5
    assert row["ag_pct"] != 40.5


def test_per_capita_rate_is_recomputed_from_state_totals() -> None:
    row = _state_exposure_rows(_exposure(), {"s|small", "s|large"}, _AREAS)["Teststan"]
    # 400 facilities over 1,000,000 people is 40 per 100k. The district rates are
    # 100 and 33.3, whose mean (66.7) is not the State/UT rate.
    assert row["rf_per100k"] == 40.0


def test_population_share_is_of_the_scored_national_total() -> None:
    exposure = _exposure()
    other = exposure.iloc[[0]].copy()
    other["admin_key"] = "o|only"
    other["state_name"] = "Otherland"
    frame = pd.concat([exposure, other], ignore_index=True)
    areas = dict(_AREAS, **{"o|only": 100e6})

    rows = _state_exposure_rows(frame, set(areas), areas)
    assert rows["Teststan"]["pshare"] + rows["Otherland"]["pshare"] == 100.0
    assert rows["Otherland"]["pshare"] == 9.09


def test_districts_outside_the_page_do_not_enter_the_state_total() -> None:
    """Only units the page can actually select contribute to its State/UT row."""
    rows = _state_exposure_rows(_exposure(), {"s|large"}, _AREAS)
    assert rows["Teststan"]["pop"] == 900_000
    assert rows["Teststan"]["n"] == 1


def test_zero_population_does_not_divide_by_zero() -> None:
    frame = _exposure()
    frame["pop_2020"] = 0.0
    row = _state_exposure_rows(frame, {"s|small", "s|large"}, _AREAS)["Teststan"]
    assert "rf_per100k" not in row
    assert "pshare" not in row
    assert row["bu_pct"] == 6.0


def test_missing_area_leaves_shares_out_rather_than_guessing() -> None:
    row = _state_exposure_rows(_exposure(), {"s|small", "s|large"}, {})["Teststan"]
    assert "bu_pct" not in row
    assert "ag_pct" not in row
    assert row["bu"] == 60.0


def test_parse_also_keeps_the_two_largest_secondary_basins() -> None:
    raw = (
        '[{"basin_name":"A","basin_frac":0.1},'
        '{"basin_name":"B","basin_frac":0.4},'
        '{"basin_name":"C","basin_frac":0.25}]'
    )
    assert _parse_also(raw) == [["B", 0.4], ["C", 0.25]]


def test_parse_also_tolerates_junk() -> None:
    for raw in (None, "", "not json", "{}", '[{"basin_name":"A"}]', float("nan")):
        assert _parse_also(raw) == []
