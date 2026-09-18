"""Potential evapotranspiration physics for the Drought Risk aridity index.

Pure numpy/xarray arithmetic: no I/O, no config imports, no grid assumptions
beyond broadcastability. The one consumer today is the grid-first aridity index
in :mod:`india_resilience_tool.compute.drought_risk_gridfirst`; a future SPEI
would use the same PET field.

Method is Hargreaves-Samani (1985) as published in FAO-56 equation 52. It is the
FAO-recommended estimator for exactly this data-limited case -- daily maximum
and minimum temperature and latitude, with no wind, humidity or measured
radiation -- which is all the CMIP6 archive behind this tool carries.
"""

from __future__ import annotations

from typing import TypeVar

import numpy as np

# FAO-56 constants.
SOLAR_CONSTANT_MJ_M2_MIN: float = 0.0820
"""Gsc, the solar constant, MJ m-2 min-1 (FAO-56 eq. 21)."""

LATENT_HEAT_MJ_PER_KG: float = 2.45
"""Latent heat of vaporisation at ~20 C, MJ kg-1 (FAO-56 eq. 20)."""

MJ_M2_DAY_TO_MM_DAY: float = 1.0 / LATENT_HEAT_MJ_PER_KG
"""0.408: energy per unit area converted to equivalent depth of evaporated water."""

HARGREAVES_COEFFICIENT: float = 0.0023
"""The empirical coefficient in FAO-56 eq. 52."""

HARGREAVES_TEMPERATURE_OFFSET_C: float = 17.8
"""The additive term in FAO-56 eq. 52, degrees Celsius."""

DAYS_PER_SOLAR_YEAR: float = 365.0
"""FAO-56 uses a fixed 365-day year in the declination and distance terms."""

ArrayLike = TypeVar("ArrayLike")


def kelvin_to_celsius(values: ArrayLike) -> ArrayLike:
    """Convert Kelvin to Celsius, preserving the container type."""
    return values - 273.15


def solar_declination_rad(day_of_year: ArrayLike) -> ArrayLike:
    """Solar declination in radians for a calendar day (FAO-56 eq. 24)."""
    return 0.409 * np.sin(2.0 * np.pi * day_of_year / DAYS_PER_SOLAR_YEAR - 1.39)


def inverse_relative_earth_sun_distance(day_of_year: ArrayLike) -> ArrayLike:
    """Inverse relative Earth-Sun distance ``dr`` (FAO-56 eq. 23)."""
    return 1.0 + 0.033 * np.cos(2.0 * np.pi * day_of_year / DAYS_PER_SOLAR_YEAR)


def sunset_hour_angle_rad(latitude_rad: ArrayLike, declination_rad: ArrayLike) -> ArrayLike:
    """Sunset hour angle in radians (FAO-56 eq. 25).

    The ``arccos`` argument is clipped to [-1, 1] so polar day and polar night
    return 0 and pi rather than NaN. India never reaches either, but the clip
    keeps the function safe if the same code is pointed at a global grid.
    """
    argument = -np.tan(latitude_rad) * np.tan(declination_rad)
    return np.arccos(np.clip(argument, -1.0, 1.0))


def extraterrestrial_radiation_mj(latitude_deg: ArrayLike, day_of_year: ArrayLike) -> ArrayLike:
    """Daily extraterrestrial radiation ``Ra`` in MJ m-2 day-1 (FAO-56 eq. 21).

    ``latitude_deg`` and ``day_of_year`` are broadcast against one another, so a
    (lat,) column and a (time,) row produce the full (time, lat) field without
    an explicit loop.
    """
    latitude_rad = np.deg2rad(latitude_deg)
    declination = solar_declination_rad(day_of_year)
    dr = inverse_relative_earth_sun_distance(day_of_year)
    omega_s = sunset_hour_angle_rad(latitude_rad, declination)
    bracket = omega_s * np.sin(latitude_rad) * np.sin(declination) + np.cos(latitude_rad) * np.cos(
        declination
    ) * np.sin(omega_s)
    return (24.0 * 60.0 / np.pi) * SOLAR_CONSTANT_MJ_M2_MIN * dr * bracket


def extraterrestrial_radiation_mm(latitude_deg: ArrayLike, day_of_year: ArrayLike) -> ArrayLike:
    """``Ra`` expressed as equivalent evaporation, mm day-1.

    FAO-56 eq. 52 expects ``Ra`` in this unit, not in MJ m-2 day-1; dropping the
    0.408 conversion inflates PET by a factor of about 2.45.
    """
    return MJ_M2_DAY_TO_MM_DAY * extraterrestrial_radiation_mj(latitude_deg, day_of_year)


def hargreaves_pet_mm_per_day(
    tmax_c: ArrayLike,
    tmin_c: ArrayLike,
    ra_mm_per_day: ArrayLike,
) -> ArrayLike:
    """Hargreaves-Samani reference evapotranspiration, mm day-1 (FAO-56 eq. 52).

    ``ETo = 0.0023 * (Tmean + 17.8) * sqrt(Tmax - Tmin) * Ra``

    ``Tmean`` is taken as ``(Tmax + Tmin) / 2``, which is the FAO-56 definition
    for this equation rather than a convenience substitution for daily mean
    temperature.

    A negative diurnal range is an invalid temperature pair and returns NaN,
    rather than a valid zero-demand day. Monthly coverage gates must count it
    as missing. An exactly zero range remains valid and returns zero PET.
    PET itself is floored at zero for valid pairs in deep cold.
    """
    # numpy ufuncs preserve xarray coordinates and lazy-array compatibility.
    # Invalid ranges intentionally produce NaN without a sqrt warning.
    with np.errstate(invalid="ignore"):
        sqrt_range = np.sqrt(tmax_c - tmin_c)
    tmean_c = (tmax_c + tmin_c) / 2.0
    pet = (
        HARGREAVES_COEFFICIENT
        * (tmean_c + HARGREAVES_TEMPERATURE_OFFSET_C)
        * sqrt_range
        * ra_mm_per_day
    )
    return np.maximum(pet, 0.0)


# UNEP/FAO aridity classes (World Atlas of Desertification), expressed as upper
# bounds on P/PET. These are the absolute physical anchors that make the index
# poolable on a national ruler -- the same role 20 mm of rain or 35 C plays for
# the other bundles.
ARIDITY_CLASS_UPPER_BOUNDS: tuple[tuple[str, float], ...] = (
    ("hyper-arid", 0.05),
    ("arid", 0.20),
    ("semi-arid", 0.50),
    ("dry sub-humid", 0.65),
    ("humid", float("inf")),
)


def aridity_class(aridity_index: float) -> str:
    """Return the UNEP/FAO drylands class for one P/PET value."""
    if not np.isfinite(aridity_index):
        return "unknown"
    for name, upper in ARIDITY_CLASS_UPPER_BOUNDS:
        if aridity_index < upper:
            return name
    return "humid"
