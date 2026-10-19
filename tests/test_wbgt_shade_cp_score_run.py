"""Contract checks for the staged shade annual comparison inputs."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics.wbgt_shade_cp_score_run import (
    annual_matrix, deployed_matrices, reference_matrices, unit_keys,
)


def test_unit_keys_and_annual_matrix_preserve_missing_years():
    units = pd.DataFrame({"unit_index": [0, 1], "state_name": ["Kerala", "Kerala"],
                          "level": ["district", "block"],
                          "district_name": ["Wayanad", "Wayanad"],
                          "block_name": ["", "Mananthavady"]})
    assert unit_keys(units).tolist() == ["kerala|wayanad", "kerala|wayanad|mananthavady"]
    frame = pd.DataFrame({"unit_index": [0], "year": [2000], "value": [28.]})
    matrix = annual_matrix(frame, "mean_c", units)
    assert matrix.shape == (11, 2)
    assert matrix[0, 0] == 28.
    assert np.isnan(matrix[1, 0]) and np.isnan(matrix[0, 1])


def test_annual_matrix_rejects_duplicate_unit_year():
    units = pd.DataFrame({"unit_index": [0]})
    frame = pd.DataFrame({"unit_index": [0, 0], "year": [2000, 2000], "value": [1., 2.]})
    with pytest.raises(ValueError, match="Duplicate"):
        annual_matrix(frame, "mean_c", units)


def test_deployed_matrices_match_state_aware_keys(tmp_path):
    units = pd.DataFrame({"unit_index": [0], "state_name": ["Kerala"],
                          "level": ["district"], "district_name": ["Wayanad"],
                          "block_name": [""]})
    metrics = ("wbgt_shade_stull_annual_mean", "wbgt_shade_stull_days_ge_28",
               "wbgt_shade_stull_days_ge_30", "wbgt_shade_stull_days_ge_32")
    for slug in metrics:
        target = (tmp_path / "metrics" / slug / "yearly_models" / "admin" /
                  "district" / "state=Kerala.parquet")
        target.parent.mkdir(parents=True)
        pd.DataFrame({"year": [2000, 2000], "value": [3., 99.],
                      "scenario": ["historical", "ssp245"],
                      "model": ["ACCESS-CM2", "ACCESS-CM2"],
                      "district_key": ["kerala|wayanad", "kerala|wayanad"]}).to_parquet(target)
    arrays, hashes = deployed_matrices(tmp_path, "ACCESS-CM2", units)
    assert len(hashes) == 4
    assert all(array[0, 0] == 3. for array in arrays.values())
    assert all(np.isnan(array[1, 0]) for array in arrays.values())


def test_reference_thresholds_are_cell_first(tmp_path):
    units = pd.DataFrame({"unit_index": [0]})
    cells_ok = np.array([True, True])
    units_ok = np.array([True])
    weights = np.array([[.5, .5]])
    target = tmp_path / "reference"
    target.mkdir()
    for year in range(2000, 2011):
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31")
        values = np.tile([27., 29.], (len(dates), 1))
        np.savez(target / f"{year}.npz", signature=np.array("signed"),
                 dates=dates.strftime("%Y-%m-%d").to_numpy(dtype=str), values=values)
    result = reference_matrices(tmp_path, units, cells_ok, units_ok, weights, "signed")
    assert np.all(result["days_ge_28"] == 182.5)
    assert np.all(result["days_ge_30"] == 0.)
    assert np.all(result["mean_c"] == 28.)
