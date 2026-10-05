"""Reference-QC support and weighting contracts, using small synthetic grids."""
import numpy as np
import pytest
from shapely.geometry import box

from tools.diagnostics.wbgt_route_b_reference import aggregate, polygon_weights, main


def test_half_missing_support_is_not_hidden():
    out = aggregate(np.array([30., np.nan]), np.array([0, 1]), np.array([2., 2.]), 4.)
    assert out["weighted_mean_c"] == 30
    assert out["valid_area_fraction"] == .5
    assert out["grid_area_fraction"] == 1


@pytest.mark.parametrize("values,indices,areas", [
    (np.array([np.nan]), np.array([0]), np.array([1.])),
    (np.array([30.]), np.array([], dtype=int), np.array([])),
])
def test_no_support_is_nan_not_zero(values, indices, areas):
    out = aggregate(values, indices, areas, 1.)
    assert np.isnan(out["weighted_mean_c"])
    assert out["valid_area_fraction"] == 0


def test_population_weight_and_zero_population_fallback():
    values, indices, areas = np.array([20., 40.]), np.array([0, 1]), np.array([1., 3.])
    assert aggregate(values, indices, areas, 4., np.array([3., 1.]))["weighted_mean_c"] == 30
    assert aggregate(values, indices, areas, 4., np.zeros(2))["weighted_mean_c"] == 35
    with pytest.raises(ValueError, match="population"):
        aggregate(values, indices, areas, 4., np.array([1.8e308, 1.]))


def test_native_polygon_support_and_off_grid_area():
    lat, lon = np.array([10.025]), np.array([70.025, 70.075])
    indices, areas, whole = polygon_weights(box(70., 10., 70.2, 10.05), lat, lon, .05)
    assert indices.tolist() == [0, 1]
    assert areas.sum() / whole == pytest.approx(.5, abs=1e-9)
    out = aggregate(np.array([[25., 35.]]), indices, areas, whole)
    assert out["weighted_mean_c"] == pytest.approx(30)


def test_default_plan_does_not_create_outputs(tmp_path):
    assert main(["--out-dir", str(tmp_path / "absent")]) == 0
    assert not (tmp_path / "absent").exists()
