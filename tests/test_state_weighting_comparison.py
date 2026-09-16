"""Guards for the State weighting comparison (CHG-0374).

`state_means` is the candidate replacement for `Elevated Bundle-Score
Concentration (%)`, so its weighting has to be exactly what it claims: area and
population weights applied to district composite scores, with unusable weights
excluded rather than silently treated as zero-weight members of the mean.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import build_state_weighting_comparison as swc


def _frame(**overrides) -> pd.DataFrame:
    base = pd.DataFrame(
        {
            "state_name": ["S", "S"],
            "district_key": ["s|a", "s|b"],
            "scenario": ["ssp585", "ssp585"],
            "period": ["2040-2060", "2040-2060"],
            "comp": [20.0, 80.0],
            "area_m2": [3.0, 1.0],
            "population": [1.0, 3.0],
        }
    )
    for key, value in overrides.items():
        base[key] = value
    return base


def test_area_and_population_weights_pull_in_opposite_directions():
    out = swc.state_means(_frame())["ssp585|2040-2060"]["S"]

    assert out["area"] == pytest.approx(35.0)   # (20*3 + 80*1) / 4
    assert out["pop"] == pytest.approx(65.0)    # (20*1 + 80*3) / 4
    assert out["delta"] == pytest.approx(-30.0)
    assert out["n"] == 2


def test_a_district_without_population_still_counts_toward_the_area_mean():
    """Mirpur has no population row; it is land, so it belongs in one mean only."""
    frame = _frame(population=[1.0, np.nan])

    out = swc.state_means(frame)["ssp585|2040-2060"]["S"]

    assert out["area"] == pytest.approx(35.0)
    assert out["pop"] == pytest.approx(20.0)    # the NaN-population district drops out
    assert out["n"] == 2


def test_zero_weights_are_excluded_rather_than_averaged_in():
    frame = _frame(area_m2=[3.0, 0.0])

    out = swc.state_means(frame)["ssp585|2040-2060"]["S"]

    assert out["area"] == pytest.approx(20.0)


def test_states_with_no_finite_score_are_omitted():
    frame = _frame(comp=[np.nan, np.nan])

    assert swc.state_means(frame)["ssp585|2040-2060"] == {}


def test_every_slice_present_in_the_input_gets_its_own_block():
    frame = pd.concat(
        [_frame(), _frame(scenario="ssp245", period="2020-2040")], ignore_index=True
    )

    out = swc.state_means(frame)

    assert set(out) == {"ssp585|2040-2060", "ssp245|2020-2040"}
