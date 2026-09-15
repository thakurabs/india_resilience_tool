"""Guards for the district-vs-block resolution comparison page (CHG-0370).

The page re-applies the frozen ruler outside `MetricRuler`, reading its knots
back from `cdf_support.csv`. That is a second implementation of a scoring step,
so the tests below pin it to the pilot's own `MetricRuler.apply` — a divergence
here would be a silent methodology change that no map would reveal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import build_resolution_comparison as cmp_
from tools.diagnostics.heat_risk_national_ruler_pilot import build_ruler


@pytest.fixture()
def pooled() -> np.ndarray:
    rng = np.random.default_rng(11)
    # zero-inflated, like the day-count metrics: ties must survive the round trip
    return np.concatenate([np.zeros(40), rng.gamma(2.0, 9.0, size=260)])


def test_score_against_matches_the_pilot_ruler(pooled):
    ruler = build_ruler("m", pooled, kind="cdf", higher_is_worse=True)
    values = pd.Series(np.concatenate([pooled[:50], [-5.0, 1e4, np.nan]]))

    mine = cmp_.score_against(values, (ruler.knot_values, ruler.knot_scores))

    pd.testing.assert_series_equal(mine, ruler.apply(values), check_names=False)


def test_score_against_clamps_outside_the_district_pool(pooled):
    """Blocks legitimately escape the district-fitted range; they clamp, not NaN."""
    ruler = build_ruler("m", pooled, kind="cdf", higher_is_worse=True)
    knots = (ruler.knot_values, ruler.knot_scores)

    out = cmp_.score_against(pd.Series([-1e6, 1e6, np.nan]), knots)

    assert out.iloc[0] == pytest.approx(float(ruler.knot_scores[0]))
    assert out.iloc[1] == pytest.approx(float(ruler.knot_scores[-1]))
    assert np.isnan(out.iloc[2])


def test_load_frozen_support_reads_only_cdf_rows(tmp_path):
    path = tmp_path / "cdf_support.csv"
    pd.DataFrame(
        {
            "ruler": ["cdf", "cdf", "linear"],
            "metric_slug": ["a", "a", "a"],
            "knot_value": [2.0, 1.0, 99.0],
            "midrank_score": [90.0, 10.0, 50.0],
            "tie_count": [1, 1, 1],
        }
    ).to_csv(path, index=False)

    support = cmp_.load_frozen_support(path)

    assert set(support) == {"a"}
    values, scores = support["a"]
    np.testing.assert_allclose(values, [1.0, 2.0])   # sorted by knot_value
    np.testing.assert_allclose(scores, [10.0, 90.0])


def test_within_district_share_is_zero_when_blocks_match_their_district():
    frame = pd.DataFrame(
        {
            "district_key": ["d1", "d1", "d2", "d2"],
            "scenario": ["ssp585"] * 4,
            "period": ["2040-2060"] * 4,
            "comp": [10.0, 10.0, 90.0, 90.0],
        }
    )

    assert cmp_.within_district_share(frame)["ssp585|2040-2060"] == pytest.approx(0.0)


def test_within_district_share_is_total_when_districts_do_not_separate_blocks():
    frame = pd.DataFrame(
        {
            "district_key": ["d1", "d1", "d2", "d2"],
            "scenario": ["ssp585"] * 4,
            "period": ["2040-2060"] * 4,
            "comp": [10.0, 90.0, 10.0, 90.0],
        }
    )

    assert cmp_.within_district_share(frame)["ssp585|2040-2060"] == pytest.approx(100.0)
