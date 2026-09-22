"""Hargreaves-Samani PET physics for the Drought Risk aridity index.

Tier 1 under ``tests/CLAUDE.md``: scientific compute, so every published
constant is pinned against the FAO-56 worked examples rather than against this
implementation's own output.
"""

import numpy as np
import pytest

from india_resilience_tool.compute.evapotranspiration import (
    MJ_M2_DAY_TO_MM_DAY,
    aridity_class,
    extraterrestrial_radiation_mj,
    extraterrestrial_radiation_mm,
    hargreaves_pet_mm_per_day,
    kelvin_to_celsius,
    sunset_hour_angle_rad,
)


def test_extraterrestrial_radiation_matches_fao56_example_8():
    """FAO-56 Example 8: Ra on 3 September (J=246) at 20 degrees South is 32.2 MJ/m2/day."""
    assert float(extraterrestrial_radiation_mj(-20.0, 246)) == pytest.approx(32.2, abs=0.05)


def test_radiation_in_mm_is_the_energy_value_divided_by_latent_heat():
    """Dropping the 0.408 conversion would inflate every PET by a factor of ~2.45."""
    ra_mj = float(extraterrestrial_radiation_mj(20.0, 180))
    assert float(extraterrestrial_radiation_mm(20.0, 180)) == pytest.approx(
        ra_mj * MJ_M2_DAY_TO_MM_DAY
    )
    assert MJ_M2_DAY_TO_MM_DAY == pytest.approx(0.408, abs=0.001)


def test_hargreaves_reproduces_the_fao56_reference_magnitude_for_lyon():
    """FAO-56 Example 19 (Lyon, 45.72N, July, Tmax 26.6 / Tmin 14.8) gives ETo near 5 mm/day."""
    ra = extraterrestrial_radiation_mm(45.72, 196)
    assert float(hargreaves_pet_mm_per_day(26.6, 14.8, ra)) == pytest.approx(5.0, abs=0.2)


def test_hargreaves_equals_the_published_closed_form():
    ra = 15.0
    expected = 0.0023 * ((35.0 + 20.0) / 2.0 + 17.8) * np.sqrt(35.0 - 20.0) * ra
    assert float(hargreaves_pet_mm_per_day(35.0, 20.0, ra)) == pytest.approx(expected)


def test_zero_diurnal_range_gives_zero_demand():
    assert float(hargreaves_pet_mm_per_day(30.0, 30.0, 15.0)) == 0.0


def test_negative_diurnal_range_is_missing_not_zero_demand():
    """Invalid temperature ordering must not pass downstream coverage gates."""
    assert np.isnan(float(hargreaves_pet_mm_per_day(20.0, 22.0, 15.0)))


def test_pet_preserves_missing_and_valid_elements_in_mixed_array():
    tmax = np.array([20.0, 30.0, 35.0, np.nan, -25.0])
    tmin = np.array([22.0, 30.0, 20.0, 20.0, -35.0])
    pet = hargreaves_pet_mm_per_day(tmax, tmin, 15.0)
    assert np.isnan(pet[[0, 3]]).all()
    assert (pet[[1, 4]] == 0).all()
    assert pet[2] > 0
    assert hargreaves_pet_mm_per_day(np.array([]), np.array([]), 15.0).size == 0


def test_pet_is_floored_at_zero_in_deep_cold():
    """Below -17.8 C the published equation turns negative, which would read as the
    atmosphere handing water back."""
    assert float(hargreaves_pet_mm_per_day(-25.0, -35.0, 10.0)) == 0.0


def test_pet_rises_with_temperature_at_fixed_range_and_radiation():
    """The whole reason aridity was added to Drought Risk: PET must carry a warming signal."""
    ra = 14.0
    cool = float(hargreaves_pet_mm_per_day(28.0, 18.0, ra))
    warm = float(hargreaves_pet_mm_per_day(32.0, 22.0, ra))
    assert warm > cool


def test_radiation_is_symmetric_about_the_equator_at_equinox():
    """J=80 is near the March equinox, where both hemispheres receive the same Ra."""
    north = float(extraterrestrial_radiation_mj(25.0, 80))
    south = float(extraterrestrial_radiation_mj(-25.0, 80))
    assert north == pytest.approx(south, rel=0.02)


def test_sunset_hour_angle_is_clipped_at_the_poles():
    """India never reaches polar day, but an unclipped arccos would return NaN if this
    code were ever pointed at a global grid."""
    assert float(sunset_hour_angle_rad(np.deg2rad(85.0), 0.409)) == pytest.approx(np.pi)
    assert float(sunset_hour_angle_rad(np.deg2rad(85.0), -0.409)) == pytest.approx(0.0)


def test_kelvin_conversion():
    assert float(kelvin_to_celsius(np.float64(300.15))) == pytest.approx(27.0)


@pytest.mark.parametrize(
    "value,expected",
    [
        (0.01, "hyper-arid"),
        (0.049, "hyper-arid"),
        (0.05, "arid"),
        (0.19, "arid"),
        (0.20, "semi-arid"),
        (0.49, "semi-arid"),
        (0.50, "dry sub-humid"),
        (0.64, "dry sub-humid"),
        (0.65, "humid"),
        (3.0, "humid"),
        (float("nan"), "unknown"),
    ],
)
def test_unep_aridity_class_boundaries(value, expected):
    """The class edges are the absolute anchors that make the index poolable nationally."""
    assert aridity_class(value) == expected


def test_radiation_broadcasts_a_latitude_column_against_a_day_row():
    lat = np.array([[8.0], [20.0], [34.0]])
    doy = np.array([[15, 105, 196, 288]])
    ra = extraterrestrial_radiation_mm(lat, doy)
    assert ra.shape == (3, 4)
    # In January the far north receives less than the far south of India; in June
    # the ordering reverses because the sun has crossed the equator.
    assert ra[0, 0] > ra[2, 0]
    assert ra[2, 2] > ra[0, 2]
