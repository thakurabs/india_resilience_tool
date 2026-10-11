"""Holdout decision tests for the frozen national shade release rule."""
from __future__ import annotations

import numpy as np
import pandas as pd

from tools.diagnostics.wbgt_shade_cp_release_score import (
    METRICS, YEARS, bootstrap_weights, score_model,
)


def test_shared_model_year_draws_and_independent_reference_draws():
    method, reference = bootstrap_weights()
    method_again, reference_again = bootstrap_weights()
    assert np.array_equal(method, method_again)
    assert np.array_equal(reference, reference_again)
    assert not np.array_equal(method, reference)
    assert np.allclose(method.sum(axis=1), 1)
    assert np.allclose(reference.sum(axis=1), 1)


def test_two_thirds_and_positive_interval_block_publication():
    units = pd.DataFrame({"unit_index": [0, 1, 2], "state_name": ["X", "X", "X"]})
    reference = {metric: np.zeros((len(YEARS), 3)) for metric in METRICS}
    deployed = {metric: np.zeros((len(YEARS), 3)) for metric in METRICS}
    holdout = {metric: np.tile([1., 1., 0.], (len(YEARS), 1)) for metric in METRICS}
    result = score_model("ACCESS-CM2", units, holdout, deployed, reference)
    national = result.cuts[(result.cuts.state_name == "national") &
                           (result.cuts.metric == "days_ge_28")].iloc[0]
    assert national.verdict == "worsened"
    assert national.worsened_units == 2
    assert national.share_worsened == 2 / 3
    assert national.bootstrap_p05 > 0
    assert national.reference_zero_units == 3


def test_missing_unit_is_attrition_not_unchanged():
    units = pd.DataFrame({"unit_index": [0, 1, 2], "state_name": ["X", "X", "Y"]})
    reference = {metric: np.zeros((len(YEARS), 3)) for metric in METRICS}
    deployed = {metric: np.zeros((len(YEARS), 3)) for metric in METRICS}
    holdout = {metric: np.zeros((len(YEARS), 3)) for metric in METRICS}
    holdout["days_ge_32"][0, 2] = np.nan
    result = score_model("ACCESS-CM2", units, holdout, deployed, reference)
    national = result.cuts[(result.cuts.state_name == "national") &
                           (result.cuts.metric == "days_ge_32")].iloc[0]
    assert national.units_classified == 2
    assert national.units_missing == 1
    assert national.unchanged_units == 2
    assert result.units[(result.units.unit_index == 2) &
                        (result.units.metric == "days_ge_32")].classification.iloc[0] == "missing"
