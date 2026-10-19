"""Outdoor-WBGT reconstruction feasibility, milestone 1 (CHG-0603).

Answers one question, and only that question:

    Can the DAILY climate inputs available to IRT support a sufficiently accurate
    estimate of daily maximum OPEN-SKY WBGT, using a physical WBGT calculation
    (Liljegren et al. 2008) and a defensible within-day reconstruction?

The frozen contract is ``docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md``.  Read it
first: it fixes the target quantity, the sites, the periods, the units, the calendar, the
candidate set and both acceptance gates, and it was written before any candidate score was
computed.

How the experiment removes climate-model error
----------------------------------------------
Both sides are driven from the *same* cached hourly ERA5 point series:

    reference  = actual hourly ERA5  -> hourly Liljegren -> daily maximum
    candidate  = actual hourly ERA5  -> collapse to DAILY inputs
                 -> reconstruct hourly from those daily inputs alone
                 -> hourly Liljegren -> daily maximum

So a candidate score isolates **reconstruction error**.  Because candidate and reference call
the same solver, this is *not* a validation of Liljegren against instruments, and no such
claim is made anywhere in the outputs.

Reference identities
--------------------
``legacy``
    ``tools/diagnostics/wbgt_deployed_vs_reference.py`` reproduced unchanged: ``cossza`` at the
    labelled UTC hour.
``audited``
    ``cossza`` at the midpoint of the radiation averaging interval (``H - 30 min``), because
    Open-Meteo documents ``shortwave_radiation`` as "average of the preceding hour" while the
    thermodynamic drivers are instantaneous.  This is the reference every candidate is scored
    against; the legacy identity is kept so the historical tables stay reproducible and so the
    correction can be priced.

Wind height
-----------
``thermofeel.calculate_wbgt_liljegren`` takes the **10 m** wind and converts it internally
(KNMI 0.62 m/s floor at 10 m, then the Liljegren stability power law to 2 m).  Passing
``wind_speed_10m`` is therefore correct and a log-law pre-conversion would double-count.  See
SPEC.md section 6.2, which cites the installed source.

Usage
-----
    python -m tools.diagnostics.wbgt_outdoor_feasibility --help
    python -m tools.diagnostics.wbgt_outdoor_feasibility --self-test
    python -m tools.diagnostics.wbgt_outdoor_feasibility --dry-run
    python -m tools.diagnostics.wbgt_outdoor_feasibility --stage audit
    python -m tools.diagnostics.wbgt_outdoor_feasibility --stage inventory
    python -m tools.diagnostics.wbgt_outdoor_feasibility --stage experiment
    python -m tools.diagnostics.wbgt_outdoor_feasibility --stage all

Nothing is written outside ``--out-dir`` and ``--work-dir``.  Both default to isolated
locations and neither may resolve inside ``IRT_DATA_DIR`` or inside a shade release stage;
the tool refuses to start if they do.

References
----------
Liljegren et al. (2008), https://doi.org/10.1080/15459620802310770
Parton and Logan (1981), Agricultural Meteorology 23, 205-216
Erbs, Klein and Duffie (1982), Solar Energy 28(4), 293-302
Lemke and Kjellstrom (2012), Industrial Health 50(4), 267-278
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from tools.diagnostics.wbgt_method_validation import SITES, Site, cos_solar_zenith

__all__ = [
    "CANDIDATE_IDS",
    "DailyInputs",
    "ReconstructionParams",
    "SiteStatic",
    "aggregate_daily_inputs",
    "barometric_pressure_hpa",
    "erbs_diffuse_fraction",
    "reconstruct_hourly",
    "reconstruct_radiation_w_m2",
    "reconstruct_temperature_c",
    "score_pair",
    "solar_events_utc",
]


# ==========================================================================
# Frozen contract constants (SPEC.md)
# ==========================================================================

IST_OFFSET = pd.Timedelta(hours=5, minutes=30)

#: Radiation is the mean over [H-1h, H]; its representative instant is the midpoint.
RADIATION_INTERVAL_H = 1.0
RADIATION_MIDPOINT_OFFSET = pd.Timedelta(minutes=-30)

REFERENCE_LEGACY = "legacy"
REFERENCE_AUDITED = "audited"
REFERENCE_SIGNATURES = {
    REFERENCE_LEGACY: "liljegren-legacy-v1:cossza-at-label",
    REFERENCE_AUDITED: "liljegren-audited-v1:cossza-at-radiation-interval-midpoint",
}

#: Day-count thresholds mirroring the shipped ``*_days_ge_*`` slugs.
THRESHOLDS_C: tuple[float, ...] = (28.0, 30.0, 32.0)

#: Daily-error gate, carried over unchanged from wbgt_deployed_vs_reference.compare_series.
GATE_MEDIAN_ABS_ERROR_C = 1.0
GATE_RMSE_C = 1.5

#: Count-acceptance policy (SPEC.md 10.2), predeclared.
COUNT_GATE_MIN_REF_PER_YEAR = 5.0
COUNT_GATE_REL_TOL = 0.20
COUNT_GATE_ABS_FLOOR_PER_YEAR = 2.0

#: Year-block bootstrap.
BOOTSTRAP_DRAWS = 1000
BOOTSTRAP_SEED = 20260929

#: Solar / radiation reconstruction.
SOLAR_CONSTANT_W_M2 = 1361.0
SUNRISE_ZENITH_DEG = 90.833  # standard refraction + solar semidiameter
RADIATION_SUBSTEPS_PER_HOUR = 6  # 10-minute sub-sampling of the interval
RADIATION_ENERGY_RTOL = 1e-9

#: Parton and Logan (1981) diurnal temperature parameters.
PARTON_LOGAN_A_H = 1.86  # lag of Tmax after solar noon [h]
PARTON_LOGAN_B = 2.2  # nocturnal decay coefficient [dimensionless]

#: Mean-preserving diurnal wind amplitude for candidate C2 (assumed, not fitted).
WIND_DIURNAL_AMPLITUDE = 0.4

#: Magnus coefficients as shipped in heat_stress_gridfirst.swbgt_empirical_cell_c.
MAGNUS_A_HPA = 6.112
MAGNUS_B = 17.62
MAGNUS_C_C = 243.12

#: ISA barometric constants.
ISA_P0_HPA = 1013.25
ISA_LAPSE_TERM = 2.25577e-5
ISA_EXPONENT = 5.25588

#: Retired Tier-2 linear sun adjustment, retained only as a labelled baseline (CHG-0567
#: measured that its intercept is not constant; it is never a candidate).
TIER2_ADJ_INTERCEPT = -2.1564
TIER2_ADJ_RSDS = -0.005375
TIER2_ADJ_WIND = 1.0424
TIER2_RSDS_DOMAIN = (300.0, 900.0)
TIER2_WIND_DOMAIN = (0.5, 3.0)
TIER2_PEAK_LAG_FACTOR = 0.75

SEASONS: Mapping[str, tuple[int, ...]] = {
    "DJF": (12, 1, 2),
    "MAM": (3, 4, 5),
    "JJAS": (6, 7, 8, 9),
    "ON": (10, 11),
}

CANDIDATE_IDS: tuple[str, ...] = ("C1", "C2", "C3")
CANDIDATE_NAMES = {
    "C1": "C1 recon-baseline (constant e, constant wind)",
    "C2": "C2 recon-wind-diurnal (mean-preserving wind shape)",
    "C3": "C3 recon-constant-rh (constant RH instead of constant e)",
}
ABLATION_IDS: tuple[str, ...] = ("A1", "A2", "A3", "A4")
ABLATION_NAMES = {
    "A1": "A1 ORACLE recon T+RH, hourly rsds/wind/pressure",
    "A2": "A2 ORACLE recon radiation+fdir, hourly T/RH/wind/pressure",
    "A3": "A3 ORACLE daily-mean wind, hourly T/RH/rsds/pressure",
    "A4": "A4 ORACLE ISA barometric pressure, hourly T/RH/rsds/wind",
}

DEFAULT_PRIMARY_WINDOW = "1990-2004"
DEFAULT_CONTINUITY_WINDOW = "2005-2014"
DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_outdoor_feasibility")
DEFAULT_WORK_DIR = Path("scratch/wbgt_outdoor_feasibility")
DEFAULT_ERA5_CACHE = Path("scratch/wbgt_deployed_vs_reference_cache")
DEFAULT_NEX_MAIN_ROOT = Path("D:/projects/irt_data/r1i1p1f1")
DEFAULT_NEX_WBGT_ROOT = Path("D:/projects/irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1")

#: Variables the physical candidate needs, and where each tree carries them.
REQUIRED_NEX_VARIABLES = ("tas", "tasmin", "tasmax", "hurs", "rsds", "sfcWind")
NEX_WBGT_TREE_VARIABLES = frozenset({"rsds", "sfcWind"})

#: Paths the tool must never write into, however it is invoked.
FORBIDDEN_WRITE_FRAGMENTS = ("processed_optimised", "wbgt_shade_national", "irt_data")


# ==========================================================================
# Static site information available to a reconstruction
# ==========================================================================


@dataclass(frozen=True)
class SiteStatic:
    """The only site information a reconstruction may read (SPEC.md 8)."""

    name: str
    lat: float
    lon: float
    elevation_m: float

    @classmethod
    def from_site(cls, site: Site) -> "SiteStatic":
        return cls(site.name, site.lat, site.lon, site.elevation_m)


@dataclass(frozen=True)
class ReconstructionParams:
    """Declared reconstruction parameters. No member is tuned against any site or period."""

    humidity_invariant: str = "vapour_pressure"  # or "relative_humidity"
    wind_profile: str = "constant"  # or "diurnal"
    wind_amplitude: float = WIND_DIURNAL_AMPLITUDE
    parton_logan_a_h: float = PARTON_LOGAN_A_H
    parton_logan_b: float = PARTON_LOGAN_B

    def signature(self) -> str:
        return (
            f"recon-v1:parton-logan-a{self.parton_logan_a_h}-b{self.parton_logan_b}"
            f":{self.humidity_invariant}"
            f":toa-shape-erbs1982"
            f":wind-{self.wind_profile}"
            + (f"-amp{self.wind_amplitude}" if self.wind_profile == "diurnal" else "")
            + ":pressure-isa-from-site-elevation"
        )


CANDIDATE_PARAMS: Mapping[str, ReconstructionParams] = {
    "C1": ReconstructionParams(),
    "C2": ReconstructionParams(wind_profile="diurnal"),
    "C3": ReconstructionParams(humidity_invariant="relative_humidity"),
}


@dataclass(frozen=True)
class DailyInputs:
    """Daily fields a NEX-equivalent pipeline would have, indexed by IST local day.

    This object is the *only* weather a reconstruction is permitted to read.  It deliberately
    carries no hourly series, so hourly leakage into a deployable candidate is prevented by
    construction rather than by convention.
    """

    frame: pd.DataFrame  # index: DatetimeIndex of local days; columns == REQUIRED_NEX_VARIABLES

    def __post_init__(self) -> None:
        missing = [c for c in REQUIRED_NEX_VARIABLES if c not in self.frame.columns]
        if missing:
            raise ValueError(f"DailyInputs missing required columns: {missing}")
        extra = set(self.frame.columns) - set(REQUIRED_NEX_VARIABLES)
        if extra:
            raise ValueError(
                "DailyInputs may carry only the daily NEX-equivalent variables; "
                f"refusing hourly leakage via {sorted(extra)}"
            )
        if not isinstance(self.frame.index, pd.DatetimeIndex):
            raise ValueError("DailyInputs index must be a DatetimeIndex of local days")


# ==========================================================================
# Thermodynamics
# ==========================================================================


def saturation_pressure_hpa(t_c: np.ndarray | float) -> np.ndarray:
    """Magnus saturation vapour pressure (hPa), IRT's shipped coefficients."""

    t = np.asarray(t_c, dtype=float)
    return MAGNUS_A_HPA * np.exp((MAGNUS_B * t) / (MAGNUS_C_C + t))


def barometric_pressure_hpa(elevation_m: float | np.ndarray) -> np.ndarray:
    """ISA barometric pressure (hPa) from elevation (m).

    Used for the candidate path only: NEX publishes no ``ps`` (SPEC.md 6.4).
    """

    z = np.asarray(elevation_m, dtype=float)
    return ISA_P0_HPA * np.power(1.0 - ISA_LAPSE_TERM * z, ISA_EXPONENT)


def stull_twb_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """Stull (2011) psychrometric wet bulb (degC), as IRT ships it."""

    t = np.asarray(t_c, dtype=float)
    rh = np.clip(np.asarray(rh_pct, dtype=float), 0.0, 100.0)
    return (
        t * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(t + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * np.power(rh, 1.5) * np.arctan(0.023101 * rh)
        - 4.686035
    )


def shade_stull_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """IRT's shaded WBGT: ``0.7 * Twb_Stull + 0.3 * Ta`` (degC)."""

    return 0.7 * stull_twb_c(t_c, rh_pct) + 0.3 * np.asarray(t_c, dtype=float)


def swbgt_empirical_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """IRT's empirical sWBGT: ``0.567*Ta + 0.393*e + 3.94`` (degC)."""

    t = np.asarray(t_c, dtype=float)
    rh = np.clip(np.asarray(rh_pct, dtype=float), 0.0, 100.0)
    e_hpa = (rh / 100.0) * saturation_pressure_hpa(t)
    return 0.567 * t + 0.393 * e_hpa + 3.94


def rh_at_tasmax_pct(
    tas_c: np.ndarray, tasmax_c: np.ndarray, hurs_pct: np.ndarray
) -> np.ndarray:
    """RH at ``tasmax`` holding vapour pressure from (``tas``, ``hurs``) fixed."""

    e = (np.asarray(hurs_pct, dtype=float) / 100.0) * saturation_pressure_hpa(tas_c)
    return np.clip(100.0 * e / saturation_pressure_hpa(tasmax_c), 0.0, 100.0)


def liljegren_wbgt_c(
    t_c: np.ndarray,
    rh_pct: np.ndarray,
    pressure_hpa: np.ndarray,
    wind_10m_ms: np.ndarray,
    ssrd_w_m2: np.ndarray,
    fdir_frac: np.ndarray,
    cossza: np.ndarray,
    *,
    wind_scaling: str = "liljegren",
) -> np.ndarray:
    """Liljegren et al. (2008) outdoor WBGT (degC) via ``thermofeel``.

    ``wind_10m_ms`` is the **10 m** wind: the library applies its own 10 m floor and its own
    10 m -> 2 m stability profile (SPEC.md 6.2).  Units are K / percent / hPa / m s-1 /
    W m-2 / fraction / dimensionless; a wrong unit returns NaN silently rather than raising,
    so callers report the NaN rate.
    """

    from thermofeel import calculate_wbgt_liljegren

    wbgt_k = calculate_wbgt_liljegren(
        np.asarray(t_c, dtype=float) + 273.15,
        np.asarray(rh_pct, dtype=float),
        np.asarray(pressure_hpa, dtype=float),
        np.asarray(wind_10m_ms, dtype=float),
        np.asarray(ssrd_w_m2, dtype=float),
        np.asarray(fdir_frac, dtype=float),
        np.asarray(cossza, dtype=float),
        wind_scaling=wind_scaling,
    )
    return np.asarray(wbgt_k, dtype=float) - 273.15


# ==========================================================================
# Solar geometry
# ==========================================================================


def _solar_gamma_eqtime_decl(
    doy: np.ndarray, hour_utc: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """NOAA fractional-year equation of time (minutes) and declination (radians)."""

    gamma = 2.0 * np.pi / 365.0 * (doy - 1.0 + (hour_utc - 12.0) / 24.0)
    eqtime_min = 229.18 * (
        0.000075
        + 0.001868 * np.cos(gamma)
        - 0.032077 * np.sin(gamma)
        - 0.014615 * np.cos(2.0 * gamma)
        - 0.040849 * np.sin(2.0 * gamma)
    )
    decl = (
        0.006918
        - 0.399912 * np.cos(gamma)
        + 0.070257 * np.sin(gamma)
        - 0.006758 * np.cos(2.0 * gamma)
        + 0.000907 * np.sin(2.0 * gamma)
        - 0.002697 * np.cos(3.0 * gamma)
        + 0.00148 * np.sin(3.0 * gamma)
    )
    return eqtime_min, decl


def solar_events_utc(
    lat_deg: float, lon_deg: float, dates: pd.DatetimeIndex
) -> pd.DataFrame:
    """Sunrise, solar noon and sunset in UTC hours-since-midnight for each UTC date.

    Returns a frame indexed by ``dates`` with columns ``sunrise_h``, ``noon_h``, ``sunset_h``
    and ``polar`` (True where the sun neither rises nor sets, in which case the window is
    degenerate and the caller must fall back).  Declination and the equation of time are taken
    at 12:00 UTC of each date; the residual sunrise error is under a couple of minutes at
    Indian latitudes, which is far finer than an hourly reconstruction resolves.
    """

    dates = pd.DatetimeIndex(dates)
    doy = dates.dayofyear.to_numpy().astype(float)
    eqtime_min, decl = _solar_gamma_eqtime_decl(doy, np.full(doy.shape, 12.0))

    noon_min = 720.0 - 4.0 * lon_deg - eqtime_min
    lat = np.deg2rad(lat_deg)
    cos_h0 = (
        np.cos(np.deg2rad(SUNRISE_ZENITH_DEG)) - np.sin(lat) * np.sin(decl)
    ) / (np.cos(lat) * np.cos(decl))
    polar = np.abs(cos_h0) > 1.0
    h0_deg = np.degrees(np.arccos(np.clip(cos_h0, -1.0, 1.0)))

    sunrise_min = noon_min - 4.0 * h0_deg
    sunset_min = noon_min + 4.0 * h0_deg
    return pd.DataFrame(
        {
            "sunrise_h": sunrise_min / 60.0,
            "noon_h": noon_min / 60.0,
            "sunset_h": sunset_min / 60.0,
            "polar": polar,
        },
        index=dates,
    )


def toa_horizontal_w_m2(cossza: np.ndarray, doy: np.ndarray) -> np.ndarray:
    """Top-of-atmosphere horizontal shortwave flux (W/m2)."""

    e0 = 1.0 + 0.033 * np.cos(2.0 * np.pi * np.asarray(doy, dtype=float) / 365.0)
    return SOLAR_CONSTANT_W_M2 * e0 * np.clip(np.asarray(cossza, dtype=float), 0.0, None)


def toa_interval_mean_w_m2(
    lat_deg: float, lon_deg: float, times: pd.DatetimeIndex
) -> np.ndarray:
    """Mean TOA horizontal flux over ``[t - 1h, t]`` for each ``t``, by sub-sampling.

    Matches the Open-Meteo radiation convention (SPEC.md 6.3) so the reconstructed radiation
    series carries the same interval semantics as the reference it is scored against.
    """

    times = pd.DatetimeIndex(times)
    n_sub = RADIATION_SUBSTEPS_PER_HOUR
    # Midpoints of n_sub equal sub-intervals spanning [t-1h, t].
    offsets_h = -(np.arange(n_sub, dtype=float) + 0.5) / n_sub * RADIATION_INTERVAL_H
    total = np.zeros(len(times), dtype=float)
    for off in offsets_h:
        sub = times + pd.Timedelta(hours=float(off))
        cz = cos_solar_zenith(lat_deg, sub, lon_deg)
        total += toa_horizontal_w_m2(cz, sub.dayofyear.to_numpy().astype(float))
    return total / float(n_sub)


def erbs_diffuse_fraction(kt: np.ndarray) -> np.ndarray:
    """Erbs, Klein and Duffie (1982) hourly diffuse fraction from the clearness index."""

    k = np.asarray(kt, dtype=float)
    low = 1.0 - 0.09 * k
    mid = (
        0.9511
        - 0.1604 * k
        + 4.388 * k**2
        - 16.638 * k**3
        + 12.336 * k**4
    )
    out = np.where(k <= 0.22, low, np.where(k <= 0.80, mid, 0.165))
    return np.clip(out, 0.0, 1.0)


# ==========================================================================
# Reconstruction from daily inputs only
# ==========================================================================


def reconstruct_temperature_c(
    times: pd.DatetimeIndex,
    daily: pd.DataFrame,
    static: SiteStatic,
    params: ReconstructionParams,
) -> np.ndarray:
    """Hourly temperature (degC) from daily ``tasmin``/``tasmax`` (Parton and Logan 1981).

    ``daily`` must be indexed by IST local day and carry ``tasmin`` and ``tasmax``; the value
    for UTC date ``u`` is taken from local day ``u``, which is the local day containing that
    solar day's noon (SPEC.md 8.2).  Hours whose surrounding daily values are unavailable
    return NaN rather than being filled.

    Daylight is a sine from ``tasmin`` at sunrise to ``tasmax`` lagged ``a`` hours after solar
    noon; night is an exponential relaxation from the sunset temperature toward the next day's
    ``tasmin``.  The 24-hour mean of the result is not forced to equal the supplied ``tas``;
    that residual is reported instead.
    """

    times = pd.DatetimeIndex(times)
    utc_date = pd.DatetimeIndex(times.normalize())
    hour = (
        times.hour.to_numpy().astype(float)
        + times.minute.to_numpy().astype(float) / 60.0
    )

    span = pd.date_range(utc_date.min() - pd.Timedelta(days=1), utc_date.max() + pd.Timedelta(days=1), freq="D")
    events = solar_events_utc(static.lat, static.lon, span)

    tmin = daily["tasmin"].reindex(span)
    tmax = daily["tasmax"].reindex(span)

    def by_offset(series: pd.Series, days_ahead: int) -> np.ndarray:
        """Value belonging to ``utc_date + days_ahead``, aligned to the hourly index."""
        return series.shift(-days_ahead).reindex(utc_date).to_numpy(dtype=float)

    def evt(col: str, days_ahead: int) -> np.ndarray:
        return by_offset(events[col], days_ahead)

    sunrise = evt("sunrise_h", 0)
    sunset = evt("sunset_h", 0)
    sunrise_next = evt("sunrise_h", +1) + 24.0  # next UTC date, on this date's clock
    sunrise_prev_own = evt("sunrise_h", -1)  # previous UTC date, on its own clock
    sunset_prev_own = evt("sunset_h", -1)
    sunset_prev = sunset_prev_own - 24.0  # previous sunset, on this date's clock

    tmin_today = by_offset(tmin, 0)
    tmax_today = by_offset(tmax, 0)
    tmin_next = by_offset(tmin, +1)
    tmax_prev = by_offset(tmax, -1)
    tmin_prev = by_offset(tmin, -1)

    a = params.parton_logan_a_h
    b = params.parton_logan_b

    def daylight(t: np.ndarray, rise: np.ndarray, set_: np.ndarray, lo: np.ndarray, hi: np.ndarray) -> np.ndarray:
        day_len = set_ - rise
        denom = day_len + 2.0 * a
        with np.errstate(divide="ignore", invalid="ignore"):
            frac = np.pi * (t - rise) / denom
        return lo + (hi - lo) * np.sin(frac)

    # Temperature at the two sunsets that bound this hour's possible night branches.
    t_at_sunset_today = daylight(sunset, sunrise, sunset, tmin_today, tmax_today)
    t_at_sunset_prev = daylight(
        sunset_prev_own, sunrise_prev_own, sunset_prev_own, tmin_prev, tmax_prev
    )

    def night(t: np.ndarray, set_: np.ndarray, rise_next: np.ndarray, t_set: np.ndarray, floor: np.ndarray) -> np.ndarray:
        z = rise_next - set_
        with np.errstate(divide="ignore", invalid="ignore"):
            decay = np.exp(-b * (t - set_) / z)
        return floor + (t_set - floor) * decay

    before_sunrise = night(hour, sunset_prev, sunrise, t_at_sunset_prev, tmin_today)
    in_daylight = daylight(hour, sunrise, sunset, tmin_today, tmax_today)
    after_sunset = night(hour, sunset, sunrise_next, t_at_sunset_today, tmin_next)

    out = np.where(
        hour < sunrise,
        before_sunrise,
        np.where(hour <= sunset, in_daylight, after_sunset),
    )
    return np.asarray(out, dtype=float)


def reconstruct_radiation_w_m2(
    times: pd.DatetimeIndex,
    daily: pd.DataFrame,
    static: SiteStatic,
    local_day: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Hourly interval-mean shortwave (W/m2), direct fraction and the TOA shape.

    The TOA interval-mean shape is rescaled by one scalar per local day so that the mean of
    the day's 24 hourly interval means equals the daily input ``rsds`` exactly (to within
    ``RADIATION_ENERGY_RTOL``).  Night-time values are exactly zero because the TOA shape is
    zero there.  Days with no insolation shape return all zeros instead of dividing by zero.
    """

    times = pd.DatetimeIndex(times)
    toa = toa_interval_mean_w_m2(static.lat, static.lon, times)

    frame = pd.DataFrame({"toa": toa, "day": local_day})
    shape_mean = frame.groupby("day")["toa"].transform("mean").to_numpy(dtype=float)
    rsds_day = daily["rsds"].reindex(pd.DatetimeIndex(local_day)).to_numpy(dtype=float)

    with np.errstate(divide="ignore", invalid="ignore"):
        scale = np.where(shape_mean > 0.0, rsds_day / shape_mean, 0.0)
    ssrd = np.where(toa > 0.0, np.clip(toa * scale, 0.0, None), 0.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        kt = np.where(toa > 0.0, ssrd / toa, 0.0)
    fdir = np.where(toa > 0.0, np.clip(1.0 - erbs_diffuse_fraction(kt), 0.0, 1.0), 0.0)
    return ssrd, fdir, toa


def reconstruct_humidity_pct(
    t_hourly_c: np.ndarray,
    daily: pd.DataFrame,
    local_day: np.ndarray,
    params: ReconstructionParams,
) -> tuple[np.ndarray, np.ndarray]:
    """Hourly RH (percent) and a boolean clip mask, from daily ``tas``/``hurs``."""

    day_index = pd.DatetimeIndex(local_day)
    hurs = daily["hurs"].reindex(day_index).to_numpy(dtype=float)
    if params.humidity_invariant == "relative_humidity":
        rh_raw = hurs
    elif params.humidity_invariant == "vapour_pressure":
        tas = daily["tas"].reindex(day_index).to_numpy(dtype=float)
        e_day = (hurs / 100.0) * saturation_pressure_hpa(tas)
        with np.errstate(divide="ignore", invalid="ignore"):
            rh_raw = 100.0 * e_day / saturation_pressure_hpa(t_hourly_c)
    else:
        raise ValueError(f"Unknown humidity invariant: {params.humidity_invariant!r}")
    clipped = np.isfinite(rh_raw) & ((rh_raw < 0.0) | (rh_raw > 100.0))
    return np.clip(rh_raw, 0.0, 100.0), clipped


def reconstruct_wind_10m_ms(
    daily: pd.DataFrame,
    local_day: np.ndarray,
    cossza: np.ndarray,
    params: ReconstructionParams,
) -> np.ndarray:
    """Hourly 10 m wind (m/s) from the daily mean.

    ``constant`` holds the daily mean at every hour and is labelled as such: a daily mean does
    not imply a known hourly wind.  ``diurnal`` applies the declared mean-preserving shape of
    SPEC.md 8.3 C2; the shape is assumed, not fitted.
    """

    day_index = pd.DatetimeIndex(local_day)
    wbar = daily["sfcWind"].reindex(day_index).to_numpy(dtype=float)
    if params.wind_profile == "constant":
        return wbar
    if params.wind_profile != "diurnal":
        raise ValueError(f"Unknown wind profile: {params.wind_profile!r}")

    cz = np.clip(np.asarray(cossza, dtype=float), 0.0, None)
    frame = pd.DataFrame({"cz": cz, "day": local_day})
    day_max = frame.groupby("day")["cz"].transform("max").to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        norm = np.where(day_max > 0.0, cz / day_max, 0.0)
    frame["norm"] = norm
    norm_mean = frame.groupby("day")["norm"].transform("mean").to_numpy(dtype=float)
    shape = norm - norm_mean  # zero mean within each local day
    return np.clip(wbar * (1.0 + params.wind_amplitude * shape), 0.0, None)


def reconstruct_hourly(
    times: pd.DatetimeIndex,
    local_day: np.ndarray,
    daily_inputs: DailyInputs,
    static: SiteStatic,
    params: ReconstructionParams,
    *,
    cossza: np.ndarray,
) -> pd.DataFrame:
    """Reconstruct every Liljegren driver from daily inputs and static site data alone.

    ``cossza`` is solar geometry, not weather, and is therefore permitted.  No hourly weather
    is accepted by this function, which is how leakage into a deployable candidate is
    prevented (SPEC.md 8; asserted in ``tests/test_wbgt_outdoor_feasibility.py``).
    """

    daily = daily_inputs.frame
    t_c = reconstruct_temperature_c(times, daily, static, params)
    rh_pct, rh_clipped = reconstruct_humidity_pct(t_c, daily, local_day, params)
    ssrd, fdir, toa = reconstruct_radiation_w_m2(times, daily, static, local_day)
    wind = reconstruct_wind_10m_ms(daily, local_day, cossza, params)
    pressure = np.full(len(times), float(barometric_pressure_hpa(static.elevation_m)))
    return pd.DataFrame(
        {
            "t_c": t_c,
            "rh_pct": rh_pct,
            "rh_clipped": rh_clipped,
            "ssrd_w_m2": ssrd,
            "fdir_frac": fdir,
            "toa_w_m2": toa,
            "wind_10m_ms": wind,
            "pressure_hpa": pressure,
        },
        index=times,
    )


# ==========================================================================
# Hourly ERA5 loading and daily aggregation
# ==========================================================================


def parse_years(spec: str) -> tuple[int, ...]:
    """Parse ``"1990-2004"`` or ``"1990,1995"`` into a sorted tuple of years."""

    years: set[int] = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            if int(hi) < int(lo):
                raise ValueError(f"Descending year range: {part!r}")
            years.update(range(int(lo), int(hi) + 1))
        else:
            years.add(int(part))
    if not years:
        raise ValueError(f"No years parsed from {spec!r}")
    return tuple(sorted(years))


def load_hourly_cache(
    site: Site, years: Sequence[int], cache_dir: Path
) -> pd.DataFrame:
    """Load one site's cached hourly ERA5 series. Missing years raise; nothing is fetched."""

    blocks: list[pd.DataFrame] = []
    missing: list[int] = []
    for year in years:
        path = cache_dir / f"{site.name.lower()}_{year}.parquet"
        if not path.exists():
            missing.append(year)
            continue
        blocks.append(pd.read_parquet(path))
    if missing:
        raise FileNotFoundError(
            f"{site.name}: hourly ERA5 cache missing years {missing} under {cache_dir}. "
            "This tool never downloads; populate the cache with "
            "tools.diagnostics.wbgt_deployed_vs_reference first."
        )
    frame = pd.concat(blocks).sort_index()
    return frame[~frame.index.duplicated(keep="first")]


def attach_geometry(site: Site, raw: pd.DataFrame) -> pd.DataFrame:
    """Add both cossza identities, the observed direct fraction and the IST local day."""

    frame = raw.copy()
    times = pd.DatetimeIndex(frame.index)
    frame["cossza_label"] = cos_solar_zenith(site.lat, times, site.lon)
    frame["cossza_mid"] = cos_solar_zenith(
        site.lat, times + RADIATION_MIDPOINT_OFFSET, site.lon
    )

    ssrd = frame["shortwave_radiation"].to_numpy(dtype=float)
    direct = frame["direct_radiation"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        fdir = np.where(ssrd > 0.0, direct / ssrd, 0.0)
    frame["fdir_frac"] = np.clip(np.nan_to_num(fdir), 0.0, 1.0)
    frame["local_day"] = pd.DatetimeIndex((times + IST_OFFSET).date)
    return frame


def complete_local_days(hourly: pd.DataFrame) -> pd.DatetimeIndex:
    """Local days carrying all 24 hours. Partial boundary days are dropped, never padded."""

    counts = hourly.groupby("local_day").size()
    return pd.DatetimeIndex(counts.index[counts == 24])


def aggregate_daily_inputs(hourly: pd.DataFrame) -> DailyInputs:
    """Collapse hourly ERA5 to the daily NEX-equivalent inputs over complete IST days."""

    keep = hourly[hourly["local_day"].isin(complete_local_days(hourly))]
    grouped = keep.groupby("local_day")
    frame = pd.DataFrame(
        {
            "tas": grouped["temperature_2m"].mean(),
            "tasmin": grouped["temperature_2m"].min(),
            "tasmax": grouped["temperature_2m"].max(),
            "hurs": grouped["relative_humidity_2m"].mean(),
            "rsds": grouped["shortwave_radiation"].mean(),
            "sfcWind": grouped["wind_speed_10m"].mean(),
        }
    )
    frame.index = pd.DatetimeIndex(frame.index, name="local_day")
    return DailyInputs(frame[list(REQUIRED_NEX_VARIABLES)])


def daily_max(values: np.ndarray, local_day: np.ndarray, keep: pd.DatetimeIndex) -> pd.Series:
    """Daily maximum over complete local days, taken AFTER the hourly calculation.

    A local day with any non-finite hour yields NaN: an invalid input must never become a
    zero-exceedance day.
    """

    frame = pd.DataFrame({"v": np.asarray(values, dtype=float), "day": local_day})
    grouped = frame.groupby("day")["v"]
    out = grouped.max()
    out = out.where(grouped.apply(lambda s: bool(np.isfinite(s).all())))
    out.index = pd.DatetimeIndex(out.index)
    return out.reindex(keep)


# ==========================================================================
# Scoring
# ==========================================================================

QUANTILES = (0.50, 0.90, 0.95, 0.99)


def score_pair(
    reference: pd.Series,
    candidate: pd.Series,
    *,
    site: str,
    window: str,
    season: str,
    reference_name: str,
    candidate_name: str,
    kind: str,
) -> dict[str, object]:
    """Matched-day scores. Definitions are those frozen in SPEC.md 10.1 and 10.3."""

    paired = pd.DataFrame({"ref": reference, "cand": candidate})
    n_available = int(len(paired))
    paired = paired.dropna()
    n_valid = int(len(paired))
    row: dict[str, object] = {
        "site": site,
        "window": window,
        "season": season,
        "reference": reference_name,
        "candidate": candidate_name,
        "kind": kind,
        "n_available_days": n_available,
        "n_valid_days": n_valid,
    }
    if n_valid == 0:
        return row

    diff = paired["cand"] - paired["ref"]
    row["bias_c"] = float(diff.mean())
    row["median_abs_error_c"] = float(diff.abs().median())
    row["rmse_c"] = float(np.sqrt((diff**2).mean()))
    row["pearson_r"] = float(paired["ref"].corr(paired["cand"])) if n_valid > 1 else np.nan
    for q in QUANTILES:
        # Quantile DIFFERENCE: candidate's own quantile minus the reference's own quantile.
        row[f"q{int(q * 100)}_difference_c"] = float(
            paired["cand"].quantile(q) - paired["ref"].quantile(q)
        )
    # CONDITIONAL error: mean error on the reference's hottest 1% of days. Not the same
    # statistic as q99_difference_c, and deliberately named differently.
    cut = paired["ref"].quantile(0.99)
    hottest = paired[paired["ref"] >= cut]
    row["n_hottest_1pct_days"] = int(len(hottest))
    row["cond_mean_error_hottest_1pct_c"] = (
        float((hottest["cand"] - hottest["ref"]).mean()) if len(hottest) else np.nan
    )
    if season == "ALL":
        row["daily_gate"] = (
            "PASS"
            if row["median_abs_error_c"] < GATE_MEDIAN_ABS_ERROR_C
            and row["rmse_c"] < GATE_RMSE_C
            else "FAIL"
        )
    return row


def complete_years(series: pd.Series) -> list[int]:
    """Years with 365 valid daily maxima after 29 February is dropped (evaluation policy)."""

    idx = pd.DatetimeIndex(series.index)
    keep = series[~((idx.month == 2) & (idx.day == 29))]
    keep_idx = pd.DatetimeIndex(keep.index)
    good = keep.notna().groupby(keep_idx.year).sum()
    total = keep.groupby(keep_idx.year).size()
    return [int(y) for y in good.index if good[y] == 365 and total[y] == 365]


def annual_counts(series: pd.Series, years: Sequence[int]) -> pd.DataFrame:
    """Per-year annual mean and threshold counts over complete years only."""

    idx = pd.DatetimeIndex(series.index)
    keep = series[~((idx.month == 2) & (idx.day == 29))]
    keep = keep[pd.DatetimeIndex(keep.index).year.isin(list(years))]
    keep_idx = pd.DatetimeIndex(keep.index)
    rows = {"annual_mean_c": keep.groupby(keep_idx.year).mean()}
    for threshold in THRESHOLDS_C:
        rows[f"days_ge_{threshold:g}"] = (
            (keep >= threshold).groupby(keep_idx.year).sum().astype(float)
        )
    out = pd.DataFrame(rows)
    out.index.name = "year"
    return out


def count_gate(ref_per_year: float, cand_per_year: float) -> tuple[str, float]:
    """Apply the predeclared count-acceptance policy (SPEC.md 10.2)."""

    if not np.isfinite(ref_per_year) or ref_per_year < COUNT_GATE_MIN_REF_PER_YEAR:
        return "NOT GATED (rare event)", np.nan
    tol = max(COUNT_GATE_REL_TOL * ref_per_year, COUNT_GATE_ABS_FLOOR_PER_YEAR)
    return ("PASS" if abs(cand_per_year - ref_per_year) <= tol else "FAIL"), tol


def year_block_bootstrap(
    reference: pd.Series, candidate: pd.Series, *, statistic: str, seed: int = BOOTSTRAP_SEED
) -> tuple[float, float]:
    """Percentile interval for a statistic, resampling whole YEARS with replacement.

    Daily samples are never treated as independent.  Returns ``(lo, hi)`` at 2.5 / 97.5 %.
    """

    paired = pd.DataFrame({"ref": reference, "cand": candidate}).dropna()
    if paired.empty:
        return (np.nan, np.nan)
    years = pd.DatetimeIndex(paired.index).year
    blocks = [paired[years == y] for y in sorted(set(years))]
    if len(blocks) < 2:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    draws = np.empty(BOOTSTRAP_DRAWS, dtype=float)
    n = len(blocks)
    for i in range(BOOTSTRAP_DRAWS):
        pick = pd.concat([blocks[j] for j in rng.integers(0, n, n)])
        d = pick["cand"] - pick["ref"]
        if statistic == "bias_c":
            draws[i] = d.mean()
        elif statistic == "rmse_c":
            draws[i] = float(np.sqrt((d**2).mean()))
        elif statistic == "median_abs_error_c":
            draws[i] = float(d.abs().median())
        else:
            raise ValueError(f"Unsupported bootstrap statistic: {statistic!r}")
    return (float(np.percentile(draws, 2.5)), float(np.percentile(draws, 97.5)))


def season_of(index: pd.DatetimeIndex) -> pd.Series:
    """Map dates onto the four reported seasons."""

    month_to_season = {m: name for name, months in SEASONS.items() for m in months}
    return pd.Series(
        [month_to_season[m] for m in pd.DatetimeIndex(index).month], index=index
    )


# ==========================================================================
# Per-site experiment
# ==========================================================================


@dataclass
class SiteResult:
    """Everything one site-window run produces."""

    site: str
    window: str
    daily: pd.DataFrame
    diagnostics: dict[str, object]


def reconstruction_consistency_metrics(
    recon: pd.DataFrame,
    daily: pd.DataFrame,
    keep: pd.DatetimeIndex,
    local_day: np.ndarray,
) -> dict[str, float]:
    """How faithfully the reconstruction reproduces the daily inputs it was built from.

    Nothing here is forced to zero: the Parton-Logan profile is driven by ``tasmin``/``tasmax``,
    so its 24-hour mean need not equal the supplied ``tas``, and constant vapour pressure does
    not preserve daily-mean ``RH``.  Both residuals are measured and reported.

    The two shortwave keys measure deliberately different masks.
    ``fully_dark_hour_max_shortwave_w_m2`` is over hours whose whole ``[H-1h, H]`` interval is
    dark (zero TOA interval mean) and must be exactly 0.
    ``straddling_hour_max_shortwave_w_m2`` is over hours whose midpoint cosine is zero but whose
    interval still catches part of the day; a non-zero value there is correct, not a leak, and
    the earlier key name ``night_shortwave_max_w_m2`` was wrong about which mask it used.
    """

    frame = pd.DataFrame(
        {
            "day": local_day,
            "t_recon": recon["t_c"].to_numpy(),
            "rh_recon": recon["rh_pct"].to_numpy(),
            "ssrd_recon": recon["ssrd_w_m2"].to_numpy(),
            "toa": recon["toa_w_m2"].to_numpy(),
            "clip": recon["rh_clipped"].to_numpy(),
        }
    )
    grouped = frame.groupby("day")
    consistency = pd.DataFrame(
        {
            "recon_tas_minus_input_tas_c": grouped["t_recon"].mean().reindex(keep)
            - daily["tas"].reindex(keep),
            "recon_tasmax_minus_input_tasmax_c": grouped["t_recon"].max().reindex(keep)
            - daily["tasmax"].reindex(keep),
            "recon_tasmin_minus_input_tasmin_c": grouped["t_recon"].min().reindex(keep)
            - daily["tasmin"].reindex(keep),
            "recon_hurs_minus_input_hurs_pct": grouped["rh_recon"].mean().reindex(keep)
            - daily["hurs"].reindex(keep),
            "recon_rsds_rel_error": (
                grouped["ssrd_recon"].mean().reindex(keep) - daily["rsds"].reindex(keep)
            )
            / daily["rsds"].reindex(keep).replace(0.0, np.nan),
        }
    )
    out: dict[str, float] = {}
    for column, quantiles in (
        ("recon_tas_minus_input_tas_c", (0.05, 0.95)),
        ("recon_tasmax_minus_input_tasmax_c", ()),
        ("recon_tasmin_minus_input_tasmin_c", ()),
        ("recon_hurs_minus_input_hurs_pct", (0.05, 0.95)),
    ):
        out[f"{column}_mean"] = float(consistency[column].mean())
        for q in quantiles:
            out[f"{column}_p{int(q * 100):02d}"] = float(consistency[column].quantile(q))
    out["recon_rsds_max_abs_rel_error"] = float(
        consistency["recon_rsds_rel_error"].abs().max()
    )
    out["rh_clipped_hour_fraction"] = float(frame["clip"].sum() / max(len(frame), 1))

    fully_dark = frame["toa"] <= 0.0
    out["fully_dark_hour_max_shortwave_w_m2"] = (
        float(frame.loc[fully_dark, "ssrd_recon"].max()) if bool(fully_dark.any()) else 0.0
    )
    out["n_fully_dark_hours"] = int(fully_dark.sum())
    out["min_shortwave_w_m2"] = float(frame["ssrd_recon"].min())
    return out


def _tier2_outdoor_c(
    daily: pd.DataFrame, static: SiteStatic, days: pd.DatetimeIndex
) -> np.ndarray:
    """The retired Tier-2 chain, reproduced only as a labelled comparison baseline."""

    tasmax = daily["tasmax"].reindex(days).to_numpy(dtype=float)
    tas = daily["tas"].reindex(days).to_numpy(dtype=float)
    hurs = daily["hurs"].reindex(days).to_numpy(dtype=float)
    rsds = daily["rsds"].reindex(days).to_numpy(dtype=float)
    wind = daily["sfcWind"].reindex(days).to_numpy(dtype=float)

    # Daily-mean rsds -> peak hourly rsds via the TOA shape, then the published lag factor.
    peak = np.empty(len(days), dtype=float)
    for i, day in enumerate(days):
        hours = pd.date_range(day, periods=24, freq="h")
        toa = toa_horizontal_w_m2(
            cos_solar_zenith(static.lat, hours, static.lon),
            hours.dayofyear.to_numpy().astype(float),
        )
        mean_toa = float(toa.mean())
        peak[i] = (rsds[i] / mean_toa * float(toa.max())) if mean_toa > 0 else 0.0
    peak = peak * TIER2_PEAK_LAG_FACTOR

    shade = shade_stull_c(tasmax, rh_at_tasmax_pct(tas, tasmax, hurs))
    adj = (
        TIER2_ADJ_INTERCEPT
        + TIER2_ADJ_RSDS * np.clip(peak, *TIER2_RSDS_DOMAIN)
        + TIER2_ADJ_WIND * np.clip(wind, *TIER2_WIND_DOMAIN)
    )
    return shade - adj


def run_site_window(
    site: Site,
    years: Sequence[int],
    *,
    cache_dir: Path,
    candidates: Sequence[str],
    with_ablations: bool,
    with_sensitivity: bool,
    verbose: bool = True,
) -> SiteResult:
    """Build every reference, candidate, ablation and baseline daily series for one site."""

    static = SiteStatic.from_site(site)
    started = time.monotonic()
    raw = load_hourly_cache(site, years, cache_dir)
    hourly = attach_geometry(site, raw)
    keep = complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    local_day = hourly["local_day"].to_numpy()
    times = pd.DatetimeIndex(hourly.index)

    t_c = hourly["temperature_2m"].to_numpy(dtype=float)
    rh = hourly["relative_humidity_2m"].to_numpy(dtype=float)
    p_hpa = hourly["surface_pressure"].to_numpy(dtype=float)
    wind = hourly["wind_speed_10m"].to_numpy(dtype=float)
    ssrd = hourly["shortwave_radiation"].to_numpy(dtype=float)
    fdir = hourly["fdir_frac"].to_numpy(dtype=float)
    cz_label = hourly["cossza_label"].to_numpy(dtype=float)
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)

    series: dict[str, pd.Series] = {}
    diagnostics: dict[str, object] = {
        "site": site.name,
        "regime": site.regime,
        "elevation_m": site.elevation_m,
        "years": [int(y) for y in years],
        "n_hours": int(len(hourly)),
        "n_complete_local_days": int(len(keep)),
        "isa_pressure_hpa": float(barometric_pressure_hpa(site.elevation_m)),
        "era5_mean_surface_pressure_hpa": float(np.nanmean(p_hpa)),
        "nan_rate": {},
    }

    def add(name: str, hourly_values: np.ndarray) -> None:
        series[name] = daily_max(hourly_values, local_day, keep)
        diagnostics["nan_rate"][name] = float(np.mean(~np.isfinite(hourly_values)))

    # ---- References ----
    add(
        "ref_legacy",
        liljegren_wbgt_c(t_c, rh, p_hpa, wind, ssrd, fdir, cz_label),
    )
    add(
        "ref_audited",
        liljegren_wbgt_c(t_c, rh, p_hpa, wind, ssrd, fdir, cz_mid),
    )
    if with_sensitivity:
        add(
            "sens_wind_brode",
            liljegren_wbgt_c(
                t_c, rh, p_hpa, wind, ssrd, fdir, cz_mid, wind_scaling="brode"
            ),
        )

    # ---- Daily inputs (the only weather a candidate may read) ----
    daily_inputs = aggregate_daily_inputs(hourly)
    daily = daily_inputs.frame

    # ---- Candidates ----
    recon_cache: dict[str, pd.DataFrame] = {}
    for cid in candidates:
        params = CANDIDATE_PARAMS[cid]
        recon = reconstruct_hourly(
            times, local_day, daily_inputs, static, params, cossza=cz_mid
        )
        recon_cache[cid] = recon
        add(
            f"cand_{cid}",
            liljegren_wbgt_c(
                recon["t_c"].to_numpy(),
                recon["rh_pct"].to_numpy(),
                recon["pressure_hpa"].to_numpy(),
                recon["wind_10m_ms"].to_numpy(),
                recon["ssrd_w_m2"].to_numpy(),
                recon["fdir_frac"].to_numpy(),
                cz_mid,
            ),
        )
        diagnostics[f"{cid}_signature"] = params.signature()

    # ---- Reconstruction self-consistency diagnostics (baseline candidate) ----
    base = recon_cache.get("C1")
    if base is not None:
        consistency = reconstruction_consistency_metrics(base, daily, keep, local_day)
        straddling = (cz_mid <= 0.0) & (base["toa_w_m2"].to_numpy() > 0.0)
        consistency["straddling_hour_max_shortwave_w_m2"] = (
            float(base.loc[straddling, "ssrd_w_m2"].max()) if bool(straddling.any()) else 0.0
        )
        consistency["n_straddling_hours"] = int(straddling.sum())
        diagnostics["reconstruction_consistency"] = consistency

    # ---- Ablations (oracle: they consume hourly weather) ----
    if with_ablations and base is not None:
        add(
            "abl_A1",
            liljegren_wbgt_c(
                base["t_c"].to_numpy(), base["rh_pct"].to_numpy(), p_hpa, wind, ssrd, fdir, cz_mid
            ),
        )
        add(
            "abl_A2",
            liljegren_wbgt_c(
                t_c,
                rh,
                p_hpa,
                wind,
                base["ssrd_w_m2"].to_numpy(),
                base["fdir_frac"].to_numpy(),
                cz_mid,
            ),
        )
        add(
            "abl_A3",
            liljegren_wbgt_c(
                t_c, rh, p_hpa, base["wind_10m_ms"].to_numpy(), ssrd, fdir, cz_mid
            ),
        )
        add(
            "abl_A4",
            liljegren_wbgt_c(
                t_c, rh, base["pressure_hpa"].to_numpy(), wind, ssrd, fdir, cz_mid
            ),
        )

    # ---- Labelled comparison baselines (daily-input only, not reconstructions) ----
    tas_d = daily["tas"].reindex(keep).to_numpy(dtype=float)
    tasmax_d = daily["tasmax"].reindex(keep).to_numpy(dtype=float)
    hurs_d = daily["hurs"].reindex(keep).to_numpy(dtype=float)
    series["base_swbgt_empirical_as_deployed"] = pd.Series(
        swbgt_empirical_c(tas_d, hurs_d), index=keep
    )
    series["base_shade_stull_at_tasmax"] = pd.Series(
        shade_stull_c(tasmax_d, rh_at_tasmax_pct(tas_d, tasmax_d, hurs_d)), index=keep
    )
    series["base_tier2_linear"] = pd.Series(
        _tier2_outdoor_c(daily, static, keep), index=keep
    )

    diagnostics["wall_seconds"] = round(time.monotonic() - started, 2)
    if verbose:
        print(
            f"    {site.name} {min(years)}-{max(years)}: "
            f"{len(keep)} complete local days, {diagnostics['wall_seconds']:.1f} s"
        )
    out = pd.DataFrame(series)
    out.index = pd.DatetimeIndex(out.index, name="local_day")
    return SiteResult(site.name, f"{min(years)}-{max(years)}", out, diagnostics)


# ==========================================================================
# NEX inventory (read-only)
# ==========================================================================


def nex_variable_root(variable: str, main_root: Path, wbgt_root: Path, scenario: str) -> Path:
    """Locate a variable's directory in whichever of the two NEX trees carries it."""

    root = wbgt_root if variable in NEX_WBGT_TREE_VARIABLES else main_root
    return root / scenario / variable


def nex_file_presence(
    main_root: Path, wbgt_root: Path, scenarios: Sequence[str]
) -> pd.DataFrame:
    """Tier 1: pure filesystem enumeration of variable x model x scenario x year."""

    rows: list[dict[str, object]] = []
    for scenario in scenarios:
        for variable in REQUIRED_NEX_VARIABLES:
            base = nex_variable_root(variable, main_root, wbgt_root, scenario)
            if not base.is_dir():
                rows.append(
                    {
                        "scenario": scenario,
                        "variable": variable,
                        "model": None,
                        "year": None,
                        "present": False,
                        "note": f"variable directory absent: {base}",
                    }
                )
                continue
            for model_dir in sorted(p for p in base.iterdir() if p.is_dir()):
                for path in sorted(model_dir.glob("*.nc")):
                    try:
                        year = int(path.stem)
                    except ValueError:
                        continue
                    rows.append(
                        {
                            "scenario": scenario,
                            "variable": variable,
                            "model": model_dir.name,
                            "year": year,
                            "present": True,
                            "note": "",
                        }
                    )
    return pd.DataFrame(rows)


def nex_required_intersection(presence: pd.DataFrame) -> pd.DataFrame:
    """Model x scenario x year rows where ALL required variables are present."""

    ok = presence[presence["present"] & presence["model"].notna()]
    if ok.empty:
        return pd.DataFrame(columns=["scenario", "model", "year", "n_variables", "complete"])
    grouped = (
        ok.groupby(["scenario", "model", "year"])["variable"].nunique().reset_index()
    )
    grouped = grouped.rename(columns={"variable": "n_variables"})
    grouped["complete"] = grouped["n_variables"] == len(REQUIRED_NEX_VARIABLES)
    return grouped


def nex_header_scan(
    main_root: Path,
    wbgt_root: Path,
    scenario: str,
    years: Sequence[int],
    models: Sequence[str] | None,
) -> pd.DataFrame:
    """Tier 2: metadata/calendar compatibility on a bounded sample. Not a national audit."""

    import xarray as xr

    rows: list[dict[str, object]] = []
    for variable in REQUIRED_NEX_VARIABLES:
        base = nex_variable_root(variable, main_root, wbgt_root, scenario)
        if not base.is_dir():
            continue
        available = sorted(p.name for p in base.iterdir() if p.is_dir())
        for model in (models if models else available):
            if model not in available:
                rows.append(
                    {
                        "scenario": scenario,
                        "variable": variable,
                        "model": model,
                        "year": None,
                        "status": "MODEL ABSENT",
                    }
                )
                continue
            for year in years:
                path = base / model / f"{year}.nc"
                row: dict[str, object] = {
                    "scenario": scenario,
                    "variable": variable,
                    "model": model,
                    "year": year,
                }
                if not path.exists():
                    row["status"] = "FILE ABSENT"
                    rows.append(row)
                    continue
                try:
                    with xr.open_dataset(path, decode_times=True) as ds:
                        index = ds.indexes.get("time")
                        row.update(
                            {
                                "status": "OK",
                                "units": str(ds[variable].attrs.get("units", "")),
                                "n_time": int(ds.sizes.get("time", 0)),
                                "n_lat": int(ds.sizes.get("lat", 0)),
                                "n_lon": int(ds.sizes.get("lon", 0)),
                                "lat_min": float(ds.lat.min()),
                                "lat_max": float(ds.lat.max()),
                                "lon_min": float(ds.lon.min()),
                                "lon_max": float(ds.lon.max()),
                                "calendar": str(
                                    getattr(index, "calendar", None)
                                    or ds.time.dt.calendar
                                ),
                                "time_first": str(index[0]) if index is not None else "",
                                "time_last": str(index[-1]) if index is not None else "",
                                "duplicate_times": int(
                                    len(index) - len(set(index))
                                )
                                if index is not None
                                else -1,
                                "frequency": str(ds.attrs.get("frequency", "")),
                                "source_id": str(ds.attrs.get("cmip6_source_id", "")),
                                "member": str(
                                    ds.attrs.get("irt_acquisition_member", "")
                                ),
                                "has_time_bnds": bool(
                                    "time_bnds" in ds.variables
                                    or "time_bounds" in ds.variables
                                ),
                            }
                        )
                except Exception as exc:  # noqa: BLE001 - inventory must not abort
                    row["status"] = f"OPEN FAILED: {type(exc).__name__}: {exc}"
                rows.append(row)
    return pd.DataFrame(rows)


def nex_site_sample(
    main_root: Path,
    wbgt_root: Path,
    scenario: str,
    years: Sequence[int],
    models: Sequence[str],
    sites: Sequence[Site],
) -> pd.DataFrame:
    """Tier 3: the six grid points, checked for non-finite and physically invalid values."""

    import xarray as xr

    frames: list[pd.DataFrame] = []
    for model in models:
        for year in years:
            block: dict[str, pd.DataFrame] = {}
            ok = True
            for variable in REQUIRED_NEX_VARIABLES:
                path = (
                    nex_variable_root(variable, main_root, wbgt_root, scenario)
                    / model
                    / f"{year}.nc"
                )
                if not path.exists():
                    ok = False
                    break
                with xr.open_dataset(path, decode_times=True) as ds:
                    picked = ds[variable].sel(
                        lat=[s.lat for s in sites],
                        lon=[s.lon for s in sites],
                        method="nearest",
                    )
                    # Diagonal pick: one (lat, lon) pair per site.
                    values = np.stack(
                        [
                            ds[variable]
                            .sel(lat=s.lat, lon=s.lon, method="nearest")
                            .to_numpy()
                            for s in sites
                        ],
                        axis=1,
                    )
                    units = str(ds[variable].attrs.get("units", ""))
                    times = pd.DatetimeIndex(ds.indexes["time"].to_numpy())
                    frame = pd.DataFrame(
                        values, index=times, columns=[s.name for s in sites]
                    )
                    if units.lower() in {"k", "kelvin"}:
                        frame = frame - 273.15
                    block[variable] = frame
                    del picked
            if not ok:
                continue
            for site in sites:
                merged = pd.DataFrame(
                    {v: block[v][site.name] for v in REQUIRED_NEX_VARIABLES}
                )
                merged.insert(0, "model", model)
                merged.insert(1, "scenario", scenario)
                merged.insert(2, "site", site.name)
                frames.append(merged)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames)
    out.index.name = "time"
    return out


def nex_site_validity(sample: pd.DataFrame) -> pd.DataFrame:
    """Summarise Tier-3 validity: non-finite counts and physically invalid values."""

    if sample.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    limits = {
        "tas": (-90.0, 60.0),
        "tasmin": (-90.0, 60.0),
        "tasmax": (-90.0, 60.0),
        "hurs": (0.0, 100.0),
        "rsds": (0.0, 500.0),
        "sfcWind": (0.0, 60.0),
    }
    for (model, scenario, site), block in sample.groupby(["model", "scenario", "site"]):
        row: dict[str, object] = {
            "model": model,
            "scenario": scenario,
            "site": site,
            "n_days": int(len(block)),
        }
        for variable, (lo, hi) in limits.items():
            values = block[variable].to_numpy(dtype=float)
            row[f"{variable}_nonfinite"] = int(np.sum(~np.isfinite(values)))
            row[f"{variable}_out_of_range"] = int(
                np.sum(np.isfinite(values) & ((values < lo) | (values > hi)))
            )
            row[f"{variable}_min"] = float(np.nanmin(values)) if len(values) else np.nan
            row[f"{variable}_max"] = float(np.nanmax(values)) if len(values) else np.nan
        row["tasmax_below_tas"] = int(
            np.sum(
                (block["tasmax"].to_numpy(dtype=float))
                < (block["tas"].to_numpy(dtype=float))
            )
        )
        row["tasmin_above_tas"] = int(
            np.sum(
                (block["tasmin"].to_numpy(dtype=float))
                > (block["tas"].to_numpy(dtype=float))
            )
        )
        rows.append(row)
    return pd.DataFrame(rows)


def nex_candidate_distribution(
    sample: pd.DataFrame, sites: Sequence[Site], candidate: str = "C1"
) -> pd.DataFrame:
    """Run the candidate on NEX daily inputs at the sampled sites.

    This is a DISTRIBUTION comparison only.  A NEX day is not an IST civil day and NEX carries
    no weather forecast for a particular ERA5 date, so same-date differences and correlations
    against ERA5 are meaningless and none are computed here.
    """

    if sample.empty:
        return pd.DataFrame()
    params = CANDIDATE_PARAMS[candidate]
    by_name = {s.name: s for s in sites}
    rows: list[pd.DataFrame] = []
    for (model, scenario, site_name), block in sample.groupby(
        ["model", "scenario", "site"]
    ):
        site = by_name[site_name]
        static = SiteStatic.from_site(site)
        daily = block[list(REQUIRED_NEX_VARIABLES)].copy()
        daily.index = pd.DatetimeIndex(daily.index).normalize()
        daily = daily[~daily.index.duplicated(keep="first")].sort_index()
        inputs = DailyInputs(daily)

        # One synthetic 24-hour UTC grid per NEX day, labelled by that day.
        times = pd.DatetimeIndex(
            np.concatenate(
                [
                    pd.date_range(day, periods=24, freq="h").to_numpy()
                    for day in daily.index
                ]
            )
        )
        local_day = pd.DatetimeIndex(times.normalize())
        cz_mid = cos_solar_zenith(
            site.lat, times + RADIATION_MIDPOINT_OFFSET, site.lon
        )
        recon = reconstruct_hourly(
            times, local_day.to_numpy(), inputs, static, params, cossza=cz_mid
        )
        wbgt = liljegren_wbgt_c(
            recon["t_c"].to_numpy(),
            recon["rh_pct"].to_numpy(),
            recon["pressure_hpa"].to_numpy(),
            recon["wind_10m_ms"].to_numpy(),
            recon["ssrd_w_m2"].to_numpy(),
            recon["fdir_frac"].to_numpy(),
            cz_mid,
        )
        peak = daily_max(wbgt, local_day.to_numpy(), daily.index)
        frame = peak.to_frame("wbgt_daily_max_c")
        frame.insert(0, "model", model)
        frame.insert(1, "scenario", scenario)
        frame.insert(2, "site", site_name)
        rows.append(frame)
    out = pd.concat(rows)
    out.index.name = "nex_day"
    return out


# ==========================================================================
# Reporting helpers
# ==========================================================================


def build_score_table(results: Sequence[SiteResult]) -> pd.DataFrame:
    """Score every candidate/ablation/baseline against the audited reference."""

    reference = "ref_audited"
    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        seasons = season_of(pd.DatetimeIndex(daily.index))
        for column in daily.columns:
            if column == reference:
                continue
            kind = (
                "reference"
                if column.startswith("ref_")
                else "sensitivity"
                if column.startswith("sens_")
                else "oracle ablation"
                if column.startswith("abl_")
                else "deployable candidate"
                if column.startswith("cand_")
                else "comparison baseline"
            )
            for season in ("ALL", *SEASONS):
                mask = slice(None) if season == "ALL" else (seasons == season).to_numpy()
                rows.append(
                    score_pair(
                        daily[reference][mask],
                        daily[column][mask],
                        site=result.site,
                        window=result.window,
                        season=season,
                        reference_name=reference,
                        candidate_name=column,
                        kind=kind,
                    )
                )
    return pd.DataFrame(rows)


def build_reference_audit_table(results: Sequence[SiteResult]) -> pd.DataFrame:
    """Quantify legacy versus audited by site, season, daily maximum and annual counts."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        seasons = season_of(pd.DatetimeIndex(daily.index))
        for season in ("ALL", *SEASONS):
            mask = slice(None) if season == "ALL" else (seasons == season).to_numpy()
            row = score_pair(
                daily["ref_legacy"][mask],
                daily["ref_audited"][mask],
                site=result.site,
                window=result.window,
                season=season,
                reference_name="ref_legacy",
                candidate_name="ref_audited",
                kind="reference audit",
            )
            rows.append(row)
        years = complete_years(daily["ref_legacy"])
        legacy = annual_counts(daily["ref_legacy"], years)
        audited = annual_counts(daily["ref_audited"], years)
        for threshold in THRESHOLDS_C:
            col = f"days_ge_{threshold:g}"
            rows.append(
                {
                    "site": result.site,
                    "window": result.window,
                    "season": "ANNUAL COUNT",
                    "reference": "ref_legacy",
                    "candidate": "ref_audited",
                    "kind": "reference audit",
                    "threshold_c": threshold,
                    "n_complete_years": len(years),
                    "legacy_per_year": float(legacy[col].mean()) if years else np.nan,
                    "audited_per_year": float(audited[col].mean()) if years else np.nan,
                    "count_difference_per_year": (
                        float(audited[col].mean() - legacy[col].mean()) if years else np.nan
                    ),
                    "legacy_annual_mean_c": (
                        float(legacy["annual_mean_c"].mean()) if years else np.nan
                    ),
                    "audited_annual_mean_c": (
                        float(audited["annual_mean_c"].mean()) if years else np.nan
                    ),
                }
            )
    return pd.DataFrame(rows)


def low_sun_amplification_audit(
    site: Site, years: Sequence[int], *, cache_dir: Path, verbose: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Measure the low-sun beam amplification that separates the two reference identities.

    The Liljegren globe balance carries a direct-beam projection ``fdir * (1/(2*cos z) - 1)``.
    Pairing an hour-mean flux with the solar geometry of the *labelled* instant means that the
    hour straddling sunrise or sunset is evaluated at a cosine just above ``CZA_MIN = 0.00873``,
    where that factor reaches ~57, so a small mean flux is amplified into a large radiant load.
    This returns a per-hour table of the worst cases and a per-site summary of how often the
    legacy daily maximum lands in such an hour instead of at the genuine afternoon peak.
    """

    from thermofeel.liljegren import CZA_MIN

    raw = load_hourly_cache(site, years, cache_dir)
    hourly = attach_geometry(site, raw)
    keep = complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    shared = dict(
        t_c=hourly["temperature_2m"].to_numpy(dtype=float),
        rh_pct=hourly["relative_humidity_2m"].to_numpy(dtype=float),
        pressure_hpa=hourly["surface_pressure"].to_numpy(dtype=float),
        wind_10m_ms=hourly["wind_speed_10m"].to_numpy(dtype=float),
        ssrd_w_m2=hourly["shortwave_radiation"].to_numpy(dtype=float),
        fdir_frac=hourly["fdir_frac"].to_numpy(dtype=float),
    )
    legacy = liljegren_wbgt_c(cossza=hourly["cossza_label"].to_numpy(dtype=float), **shared)
    audited = liljegren_wbgt_c(cossza=hourly["cossza_mid"].to_numpy(dtype=float), **shared)

    frame = hourly.assign(legacy_c=legacy, audited_c=audited)
    frame["hour_difference_c"] = frame["legacy_c"] - frame["audited_c"]
    # "Low sun with light" = the sun is nominally up at the label but barely so, while the
    # preceding-hour mean flux is non-zero. This is the straddling hour.
    frame["low_sun_with_flux"] = (
        (frame["cossza_label"] > CZA_MIN)
        & (frame["cossza_label"] < 0.05)
        & (frame["shortwave_radiation"] > 0.0)
    )

    grouped = frame.groupby("local_day")
    legacy_peak = grouped["legacy_c"].max().reindex(keep)
    audited_peak = grouped["audited_c"].max().reindex(keep)
    legacy_argmax_low_sun = grouped.apply(
        lambda block: bool(block.loc[block["legacy_c"].idxmax(), "low_sun_with_flux"]),
        include_groups=False,
    ).reindex(keep)

    window = f"{min(years)}-{max(years)}"
    n_days = int(len(keep))
    summary = pd.DataFrame(
        [
            {
                "site": site.name,
                "window": window,
                "n_days": n_days,
                "cza_min": float(CZA_MIN),
                "max_beam_factor_at_cza_min": float(1.0 / (2.0 * CZA_MIN) - 1.0),
                "n_low_sun_with_flux_hours": int(frame["low_sun_with_flux"].sum()),
                "n_days_legacy_peak_in_low_sun_hour": int(legacy_argmax_low_sun.sum()),
                "pct_days_legacy_peak_in_low_sun_hour": round(
                    100.0 * float(legacy_argmax_low_sun.mean()), 3
                ),
                "mean_peak_inflation_on_those_days_c": float(
                    (legacy_peak - audited_peak)[legacy_argmax_low_sun.fillna(False)].mean()
                )
                if bool(legacy_argmax_low_sun.any())
                else np.nan,
                "max_peak_inflation_c": float((legacy_peak - audited_peak).max()),
                "max_hour_difference_c": float(frame["hour_difference_c"].max()),
            }
        ]
    )
    worst = (
        frame.loc[
            frame["hour_difference_c"].abs().sort_values(ascending=False).index[:10],
            [
                "local_day",
                "temperature_2m",
                "shortwave_radiation",
                "direct_radiation",
                "fdir_frac",
                "cossza_label",
                "cossza_mid",
                "legacy_c",
                "audited_c",
                "hour_difference_c",
            ],
        ]
        .assign(site=site.name, window=window)
        .reset_index()
    )
    if verbose:
        row = summary.iloc[0]
        print(
            f"    {site.name} {window}: legacy peak sits in a low-sun hour on "
            f"{row['n_days_legacy_peak_in_low_sun_hour']}/{n_days} days "
            f"({row['pct_days_legacy_peak_in_low_sun_hour']:.2f} %), "
            f"max inflation {row['max_peak_inflation_c']:.2f} C"
        )
    return summary, worst


def legacy_reproduction_parity(
    results: Sequence[SiteResult], historical: Path
) -> dict[str, object]:
    """Assert this tool's legacy reference reproduces the committed historical series.

    The historical evidence is read, never written.  If the file is absent the result says
    NOT EVALUATED rather than claiming a pass.
    """

    if not historical.exists():
        return {
            "status": "NOT EVALUATED",
            "reason": f"historical series absent: {historical}",
        }
    old = pd.read_parquet(historical)
    if "liljegren_daily_max" not in old.columns or "site" not in old.columns:
        return {"status": "NOT EVALUATED", "reason": "unexpected historical schema"}
    rows: list[dict[str, object]] = []
    for result in results:
        reference = old[old["site"] == result.site]["liljegren_daily_max"]
        reference.index = pd.DatetimeIndex(reference.index)
        paired = pd.DataFrame(
            {"historical": reference, "reproduced": result.daily["ref_legacy"]}
        ).dropna()
        if paired.empty:
            continue
        diff = (paired["reproduced"] - paired["historical"]).abs()
        rows.append(
            {
                "site": result.site,
                "window": result.window,
                "n_compared_days": int(len(paired)),
                "max_abs_difference_c": float(diff.max()),
                "days_ge_32_historical": int((paired["historical"] >= 32.0).sum()),
                "days_ge_32_reproduced": int((paired["reproduced"] >= 32.0).sum()),
            }
        )
    if not rows:
        return {
            "status": "NOT EVALUATED",
            "reason": "no overlapping days; the historical series covers 2005-2014 only",
        }
    worst = max(r["max_abs_difference_c"] for r in rows)
    return {
        "status": "IDENTICAL" if worst == 0.0 else f"DIFFERS (max {worst:.6g} C)",
        "historical_file": str(historical),
        "comparisons": rows,
    }


def build_annual_table(results: Sequence[SiteResult]) -> pd.DataFrame:
    """Annual means, annual counts, count errors and the predeclared count gate."""

    reference = "ref_audited"
    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        years = complete_years(daily[reference])
        ref_annual = annual_counts(daily[reference], years)
        for column in daily.columns:
            if column == reference:
                continue
            cand_annual = annual_counts(daily[column], years)
            base = {
                "site": result.site,
                "window": result.window,
                "candidate": column,
                "n_complete_years": len(years),
                "years": ",".join(str(y) for y in years),
            }
            rows.append(
                {
                    **base,
                    "threshold_c": np.nan,
                    "statistic": "annual_mean_c",
                    "reference_per_year": float(ref_annual["annual_mean_c"].mean())
                    if years
                    else np.nan,
                    "candidate_per_year": float(cand_annual["annual_mean_c"].mean())
                    if years
                    else np.nan,
                    "signed_error": float(
                        cand_annual["annual_mean_c"].mean()
                        - ref_annual["annual_mean_c"].mean()
                    )
                    if years
                    else np.nan,
                    "absolute_error": float(
                        abs(
                            cand_annual["annual_mean_c"].mean()
                            - ref_annual["annual_mean_c"].mean()
                        )
                    )
                    if years
                    else np.nan,
                    "relative_error": np.nan,
                    "count_gate": "",
                    "count_gate_tolerance_per_year": np.nan,
                }
            )
            for threshold in THRESHOLDS_C:
                col = f"days_ge_{threshold:g}"
                ref_per_year = float(ref_annual[col].mean()) if years else np.nan
                cand_per_year = float(cand_annual[col].mean()) if years else np.nan
                verdict, tol = count_gate(ref_per_year, cand_per_year)
                rows.append(
                    {
                        **base,
                        "threshold_c": threshold,
                        "statistic": f"days_ge_{threshold:g}_per_year",
                        "reference_per_year": ref_per_year,
                        "candidate_per_year": cand_per_year,
                        "signed_error": cand_per_year - ref_per_year,
                        "absolute_error": abs(cand_per_year - ref_per_year),
                        "relative_error": (
                            (cand_per_year - ref_per_year) / ref_per_year
                            if np.isfinite(ref_per_year) and ref_per_year > 0
                            else np.nan
                        ),
                        "count_gate": verdict,
                        "count_gate_tolerance_per_year": tol,
                    }
                )
    return pd.DataFrame(rows)


def build_uncertainty_table(
    results: Sequence[SiteResult], candidates: Sequence[str]
) -> pd.DataFrame:
    """Year-block bootstrap intervals for the gated daily statistics."""

    rows: list[dict[str, object]] = []
    wanted = [f"cand_{c}" for c in candidates]
    for result in results:
        daily = result.daily
        for column in wanted:
            if column not in daily:
                continue
            for statistic in ("bias_c", "median_abs_error_c", "rmse_c"):
                lo, hi = year_block_bootstrap(
                    daily["ref_audited"], daily[column], statistic=statistic
                )
                rows.append(
                    {
                        "site": result.site,
                        "window": result.window,
                        "candidate": column,
                        "statistic": statistic,
                        "ci_lo": lo,
                        "ci_hi": hi,
                        "draws": BOOTSTRAP_DRAWS,
                        "seed": BOOTSTRAP_SEED,
                        "method": "year-block bootstrap, whole years with replacement",
                    }
                )
    return pd.DataFrame(rows)


def verdict_from_tables(scores: pd.DataFrame, annual: pd.DataFrame) -> dict[str, object]:
    """Apply both predeclared gates. Absent evidence stays absent."""

    out: dict[str, object] = {}
    for cid in CANDIDATE_IDS:
        column = f"cand_{cid}"
        daily = scores[
            (scores["candidate"] == column) & (scores["season"] == "ALL")
        ]
        counts = annual[
            (annual["candidate"] == column) & annual["threshold_c"].notna()
        ]
        gated = counts[counts["count_gate"].isin(["PASS", "FAIL"])]
        if daily.empty:
            out[cid] = {"daily_gate": "NOT EVALUATED", "count_gate": "NOT EVALUATED"}
            continue
        out[cid] = {
            "daily_gate": "PASS" if (daily["daily_gate"] == "PASS").all() else "FAIL",
            "daily_pairs": int(len(daily)),
            "daily_pairs_passing": int((daily["daily_gate"] == "PASS").sum()),
            "count_gate": (
                "NOT EVALUATED"
                if gated.empty
                else "PASS"
                if (gated["count_gate"] == "PASS").all()
                else "FAIL"
            ),
            "count_pairs_gated": int(len(gated)),
            "count_pairs_passing": int((gated["count_gate"] == "PASS").sum()),
            "count_pairs_not_gated": int(
                (counts["count_gate"] == "NOT GATED (rare event)").sum()
            ),
        }
    return out


def run_manifest(args: argparse.Namespace, extra: Mapping[str, object]) -> dict[str, object]:
    """Git snapshot, package versions, method signatures and the exact command."""

    def git(*cmd: str) -> str:
        try:
            return subprocess.run(
                ["git", *cmd], capture_output=True, text=True, timeout=30, check=False
            ).stdout.strip()
        except OSError:
            return ""

    versions: dict[str, str] = {}
    for module in ("numpy", "pandas", "xarray", "thermofeel", "netCDF4", "pyarrow"):
        try:
            versions[module] = str(
                __import__(module).__version__  # noqa: PLC0415
            )
        except Exception:  # noqa: BLE001
            versions[module] = "NOT INSTALLED"

    return {
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_sha": git("rev-parse", "--short", "HEAD"),
        "git_dirty_tracked": bool(git("status", "--porcelain", "--untracked-files=no")),
        "python": sys.version,
        "platform": platform.platform(),
        "packages": versions,
        "command": " ".join([sys.executable, "-m", __spec__.name if __spec__ else __name__, *sys.argv[1:]]),
        "argv": sys.argv[1:],
        "reference_signatures": REFERENCE_SIGNATURES,
        "candidate_signatures": {
            cid: CANDIDATE_PARAMS[cid].signature() for cid in CANDIDATE_IDS
        },
        "gates": {
            "median_abs_error_c": GATE_MEDIAN_ABS_ERROR_C,
            "rmse_c": GATE_RMSE_C,
            "count_min_reference_per_year": COUNT_GATE_MIN_REF_PER_YEAR,
            "count_relative_tolerance": COUNT_GATE_REL_TOL,
            "count_absolute_floor_per_year": COUNT_GATE_ABS_FLOOR_PER_YEAR,
        },
        "radiation_energy_rtol": RADIATION_ENERGY_RTOL,
        "spec": "docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md",
        **dict(extra),
    }


# ==========================================================================
# Self-test (no network, no NEX, no cache)
# ==========================================================================


def _synthetic_hourly(site: Site, days: int = 40, seed: int = 7) -> pd.DataFrame:
    """A plausible hourly series for exercising the whole chain offline."""

    rng = np.random.default_rng(seed)
    times = pd.date_range("2001-03-01", periods=24 * days, freq="h")
    cz = cos_solar_zenith(site.lat, times, site.lon)
    hour = times.hour.to_numpy().astype(float)
    t_c = 30.0 - 6.0 * np.cos(2.0 * np.pi * (hour - 15.0) / 24.0) + rng.normal(0, 0.4, len(times))
    rh = np.clip(70.0 - 25.0 * np.sin(2.0 * np.pi * (hour - 6.0) / 24.0), 10.0, 98.0)
    frame = pd.DataFrame(
        {
            "temperature_2m": t_c,
            "relative_humidity_2m": rh,
            "dew_point_2m": t_c - 5.0,
            "wind_speed_10m": np.clip(2.0 + rng.normal(0, 0.5, len(times)), 0.2, None),
            "surface_pressure": np.full(len(times), 1005.0),
            "shortwave_radiation": 850.0 * cz,
            "direct_radiation": 600.0 * cz,
        },
        index=times,
    )
    frame.index.name = "time"
    return frame


def self_test() -> int:
    """Exercise every numerical contract that can be checked without external data."""

    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  [{'ok ' if ok else 'FAIL'}] {name}{(': ' + detail) if detail else ''}")
        if not ok:
            failures.append(name)

    site = SITES[0]
    static = SiteStatic.from_site(site)

    check(
        "barometric pressure falls with elevation",
        barometric_pressure_hpa(0.0) > barometric_pressure_hpa(2276.0) > 700.0,
        f"{barometric_pressure_hpa(0.0):.1f} -> {barometric_pressure_hpa(2276.0):.1f} hPa",
    )
    check(
        "Erbs diffuse fraction bounded and monotone-ish",
        bool(
            np.all((erbs_diffuse_fraction(np.linspace(0, 1, 101)) >= 0.0))
            and np.all(erbs_diffuse_fraction(np.linspace(0, 1, 101)) <= 1.0)
            and erbs_diffuse_fraction(np.array([0.1]))[0]
            > erbs_diffuse_fraction(np.array([0.7]))[0]
        ),
    )
    events = solar_events_utc(site.lat, site.lon, pd.date_range("2001-06-21", periods=1))
    check(
        "sunrise before solar noon before sunset",
        bool(
            events["sunrise_h"].iloc[0]
            < events["noon_h"].iloc[0]
            < events["sunset_h"].iloc[0]
        ),
        f"{events['sunrise_h'].iloc[0]:.2f} / {events['noon_h'].iloc[0]:.2f} / {events['sunset_h'].iloc[0]:.2f} UTC h",
    )

    hourly = attach_geometry(site, _synthetic_hourly(site))
    keep = complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    inputs = aggregate_daily_inputs(hourly)
    times = pd.DatetimeIndex(hourly.index)
    local_day = hourly["local_day"].to_numpy()
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)

    recon = reconstruct_hourly(times, local_day, inputs, static, CANDIDATE_PARAMS["C1"], cossza=cz_mid)

    ssrd = recon["ssrd_w_m2"].to_numpy()
    toa = recon["toa_w_m2"].to_numpy()
    night = toa <= 0.0
    check("night shortwave exactly zero", bool(np.all(ssrd[night] == 0.0)))
    check("shortwave non-negative", bool(np.all(ssrd >= 0.0)))
    recon_rsds = pd.Series(ssrd, index=times).groupby(local_day).mean()
    target = inputs.frame["rsds"].reindex(pd.DatetimeIndex(recon_rsds.index))
    rel = ((recon_rsds.to_numpy() - target.to_numpy()) / target.to_numpy())
    check(
        "radiation energy conserved to RADIATION_ENERGY_RTOL",
        bool(np.nanmax(np.abs(rel)) < RADIATION_ENERGY_RTOL),
        f"max |rel| = {np.nanmax(np.abs(rel)):.2e}",
    )
    rh_values = recon["rh_pct"].to_numpy()
    finite_rh = rh_values[np.isfinite(rh_values)]
    check(
        "reconstructed RH within [0, 100] wherever defined",
        bool(np.all(finite_rh >= 0.0) and np.all(finite_rh <= 100.0)),
        f"[{finite_rh.min():.1f}, {finite_rh.max():.1f}] %",
    )
    # Hours needing a neighbouring day outside the record are NaN by design. Assert they are
    # confined to the first and last local day and never appear in the interior.
    t_values = recon["t_c"].to_numpy()
    nan_days = set(pd.DatetimeIndex(local_day[~np.isfinite(t_values)]).unique())
    edges = {keep[0], keep[-1]}
    check(
        "temperature NaN confined to the record's boundary local days",
        nan_days.issubset(edges),
        f"NaN days {sorted(str(d.date()) for d in nan_days)}",
    )
    interior = keep[1:-1]
    recon_peak = (
        pd.Series(t_values, index=times).groupby(local_day).max().reindex(interior)
    )
    peak_gap = (recon_peak - inputs.frame["tasmax"].reindex(interior)).abs().max()
    check(
        "reconstructed hourly peak recovers the daily tasmax on interior days",
        bool(peak_gap < 0.35),
        f"max |recon peak - tasmax| = {peak_gap:.4f} C",
    )
    recon_trough = (
        pd.Series(t_values, index=times).groupby(local_day).min().reindex(interior)
    )
    trough_gap = (recon_trough - inputs.frame["tasmin"].reindex(interior)).abs().max()
    check(
        "reconstructed hourly trough stays near the daily tasmin on interior days",
        bool(trough_gap < 1.5),
        f"max |recon trough - tasmin| = {trough_gap:.4f} C",
    )
    diurnal = reconstruct_wind_10m_ms(
        inputs.frame, local_day, cz_mid, CANDIDATE_PARAMS["C2"]
    )
    per_day = pd.Series(diurnal, index=times).groupby(local_day).mean()
    wbar = inputs.frame["sfcWind"].reindex(pd.DatetimeIndex(per_day.index))
    check(
        "diurnal wind shape preserves the daily mean",
        bool(np.nanmax(np.abs(per_day.to_numpy() - wbar.to_numpy())) < 1e-9),
    )

    try:
        import thermofeel  # noqa: F401

        wbgt = liljegren_wbgt_c(
            recon["t_c"].to_numpy(),
            recon["rh_pct"].to_numpy(),
            recon["pressure_hpa"].to_numpy(),
            recon["wind_10m_ms"].to_numpy(),
            ssrd,
            recon["fdir_frac"].to_numpy(),
            cz_mid,
        )
        interior_mask = np.isin(
            pd.DatetimeIndex(local_day), pd.DatetimeIndex(interior)
        )
        finite_interior = float(np.isfinite(wbgt[interior_mask]).mean())
        check(
            "Liljegren returns finite values on every interior reconstructed hour",
            finite_interior == 1.0,
            f"finite fraction {finite_interior:.6f}",
        )
        nan_in = wbgt.copy()
        bad = liljegren_wbgt_c(
            np.array([np.nan]), np.array([50.0]), np.array([1000.0]),
            np.array([2.0]), np.array([500.0]), np.array([0.7]), np.array([0.8]),
        )
        check("NaN temperature propagates as NaN", bool(not np.isfinite(bad[0])))
        peaks = daily_max(nan_in, local_day, keep)
        check("daily maxima produced after the hourly calculation", bool(peaks.notna().any()))
        marked = nan_in.copy()
        marked[0] = np.nan
        holed = daily_max(marked, local_day, keep)
        check(
            "a day with one invalid hour becomes NaN, not zero",
            bool(pd.isna(holed.iloc[0])),
        )
    except ImportError:
        print("  [skip] thermofeel not installed; solver checks skipped")

    print(f"\n{'self-test PASSED' if not failures else 'self-test FAILED: ' + ', '.join(failures)}")
    return 0 if not failures else 1


# ==========================================================================
# CLI
# ==========================================================================


def _guard_write_target(path: Path, label: str) -> Path:
    """Refuse to write into production data or into the running shade release stage."""

    resolved = path.expanduser().resolve()
    lowered = str(resolved).replace("\\", "/").lower()
    for fragment in FORBIDDEN_WRITE_FRAGMENTS:
        if f"/{fragment}" in lowered or lowered.endswith(f"/{fragment}"):
            raise SystemExit(
                f"Refusing to use {label}={resolved}: it resolves inside '{fragment}', "
                "which this milestone must not write to."
            )
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.diagnostics.wbgt_outdoor_feasibility",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        choices=("audit", "lowsun", "recondiag", "inventory", "experiment", "all"),
        default="all",
        help="which part of the milestone to run (default: all)",
    )
    parser.add_argument("--self-test", action="store_true", help="offline numerical contract checks")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and preflight; write nothing")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--era5-cache", type=Path, default=DEFAULT_ERA5_CACHE)
    parser.add_argument(
        "--legacy-series",
        type=Path,
        default=Path("docs/diagnostics/wbgt_deployed_vs_reference/daily_series.parquet"),
        help="committed historical daily series, read-only, for the legacy reproduction check",
    )
    parser.add_argument("--nex-main-root", type=Path, default=DEFAULT_NEX_MAIN_ROOT)
    parser.add_argument("--nex-wbgt-root", type=Path, default=DEFAULT_NEX_WBGT_ROOT)
    parser.add_argument("--sites", default="", help="comma-separated subset (default: all six)")
    parser.add_argument(
        "--windows",
        default=f"{DEFAULT_PRIMARY_WINDOW},{DEFAULT_CONTINUITY_WINDOW}",
        help="semicolon- or comma-separated year windows; the first is the primary period",
    )
    parser.add_argument(
        "--candidates",
        default=",".join(CANDIDATE_IDS),
        help=f"subset of {CANDIDATE_IDS}",
    )
    parser.add_argument("--no-ablations", action="store_true")
    parser.add_argument("--no-sensitivity", action="store_true")
    parser.add_argument("--no-uncertainty", action="store_true")
    parser.add_argument(
        "--nex-header-years", default="1990", help="years for the bounded header scan"
    )
    parser.add_argument(
        "--nex-sample-models",
        default="ACCESS-CM2",
        help="comma-separated models for the six-site sampled read",
    )
    parser.add_argument("--nex-sample-years", default="1990-1994")
    parser.add_argument("--nex-scenario", default="historical")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="reuse per-site daily results already cached under --work-dir",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="recompute and replace cached per-site daily results",
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="bounded worker count (default 1)"
    )
    parser.add_argument("--quiet", action="store_true")
    return parser


def select_sites(spec: str) -> tuple[Site, ...]:
    if not spec.strip():
        return SITES
    wanted = {s.strip().casefold() for s in spec.split(",") if s.strip()}
    chosen = tuple(s for s in SITES if s.name.casefold() in wanted)
    unknown = wanted - {s.name.casefold() for s in chosen}
    if unknown:
        raise SystemExit(f"Unknown site(s): {sorted(unknown)}. Known: {[s.name for s in SITES]}")
    return chosen


def parse_windows(spec: str) -> tuple[tuple[int, ...], ...]:
    parts = [p for p in spec.replace(";", "|").replace(",", "|").split("|") if p.strip()]
    return tuple(parse_years(p) for p in parts)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()

    if args.resume and args.overwrite:
        raise SystemExit("--resume and --overwrite are mutually exclusive")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.workers > 4:
        raise SystemExit(
            "--workers is bounded at 4 for this milestone: the national shade build owns the disk"
        )

    out_dir = _guard_write_target(args.out_dir, "--out-dir")
    work_dir = _guard_write_target(args.work_dir, "--work-dir")
    sites = select_sites(args.sites)
    windows = parse_windows(args.windows)
    candidates = tuple(c.strip() for c in args.candidates.split(",") if c.strip())
    unknown = set(candidates) - set(CANDIDATE_IDS)
    if unknown:
        raise SystemExit(f"Unknown candidate(s): {sorted(unknown)}. Known: {CANDIDATE_IDS}")
    verbose = not args.quiet

    print("Outdoor-WBGT reconstruction feasibility, milestone 1")
    print("  spec        docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md")
    print(f"  stage       {args.stage}")
    print(f"  out-dir     {out_dir}")
    print(f"  work-dir    {work_dir}")
    print(f"  era5 cache  {args.era5_cache}")
    print(f"  sites       {', '.join(s.name for s in sites)}")
    print(f"  windows     {', '.join(f'{min(w)}-{max(w)}' for w in windows)}")
    print(f"  candidates  {', '.join(candidates)}")
    print(f"  ablations   {'no' if args.no_ablations else 'yes (oracle, not deployable)'}")
    print(f"  workers     {args.workers}")

    # ---- Preflight ----
    problems: list[str] = []
    for site in sites:
        for window in windows:
            for year in window:
                path = args.era5_cache / f"{site.name.lower()}_{year}.parquet"
                if not path.exists():
                    problems.append(f"missing hourly ERA5 cache: {path}")
    try:
        import thermofeel

        print(f"  thermofeel  {getattr(thermofeel, '__version__', '?')}")
    except ImportError:
        problems.append("thermofeel is not installed; the physical solver cannot run")
    for label, root in (
        ("nex-main-root", args.nex_main_root),
        ("nex-wbgt-root", args.nex_wbgt_root),
    ):
        if args.stage in {"inventory", "all"} and not Path(root).is_dir():
            problems.append(f"{label} is not a directory: {root}")

    if problems:
        print("\nPREFLIGHT PROBLEMS")
        for problem in problems:
            print(f"  - {problem}")
    else:
        print("\nPreflight OK.")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 1 if problems else 0
    if problems:
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest_extra: dict[str, object] = {
        "sites": [s.name for s in sites],
        "windows": [f"{min(w)}-{max(w)}" for w in windows],
        "candidates": list(candidates),
        "era5_cache": str(args.era5_cache),
    }

    if args.stage in {"recondiag", "all"}:
        print("\nReconstruction self-consistency (no solver; reconstruction only)")
        rows: list[dict[str, object]] = []
        for window in windows:
            for site in sites:
                static = SiteStatic.from_site(site)
                hourly = attach_geometry(
                    site, load_hourly_cache(site, window, args.era5_cache)
                )
                keep = complete_local_days(hourly)
                hourly = hourly[hourly["local_day"].isin(keep)]
                inputs = aggregate_daily_inputs(hourly)
                times = pd.DatetimeIndex(hourly.index)
                local_day = hourly["local_day"].to_numpy()
                cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)
                for cid in candidates:
                    recon = reconstruct_hourly(
                        times,
                        local_day,
                        inputs,
                        static,
                        CANDIDATE_PARAMS[cid],
                        cossza=cz_mid,
                    )
                    metrics = reconstruction_consistency_metrics(
                        recon, inputs.frame, keep, local_day
                    )
                    straddling = (cz_mid <= 0.0) & (recon["toa_w_m2"].to_numpy() > 0.0)
                    metrics["straddling_hour_max_shortwave_w_m2"] = (
                        float(recon.loc[straddling, "ssrd_w_m2"].max())
                        if bool(straddling.any())
                        else 0.0
                    )
                    metrics["n_straddling_hours"] = int(straddling.sum())
                    rows.append(
                        {
                            "site": site.name,
                            "window": f"{min(window)}-{max(window)}",
                            "candidate": cid,
                            "isa_pressure_hpa": float(
                                barometric_pressure_hpa(site.elevation_m)
                            ),
                            "era5_mean_surface_pressure_hpa": float(
                                hourly["surface_pressure"].mean()
                            ),
                            **metrics,
                        }
                    )
                if verbose:
                    print(f"    {site.name} {min(window)}-{max(window)}: done")
        pd.DataFrame(rows).to_csv(
            out_dir / "reconstruction_consistency.csv", index=False
        )
        print(f"  wrote {out_dir / 'reconstruction_consistency.csv'}")

    if args.stage in {"lowsun", "all"}:
        print("\nLow-sun beam amplification audit (reference identity difference)")
        summaries: list[pd.DataFrame] = []
        worsts: list[pd.DataFrame] = []
        for window in windows:
            for site in sites:
                summary, worst = low_sun_amplification_audit(
                    site, window, cache_dir=args.era5_cache, verbose=verbose
                )
                summaries.append(summary)
                worsts.append(worst)
        pd.concat(summaries).to_csv(out_dir / "reference_low_sun_audit.csv", index=False)
        pd.concat(worsts).to_csv(out_dir / "reference_low_sun_worst_hours.csv", index=False)
        print(f"  wrote {out_dir / 'reference_low_sun_audit.csv'}")

    results: list[SiteResult] = []
    if args.stage in {"audit", "experiment", "all"}:
        print("\nBuilding per-site daily series")
        for window in windows:
            for site in sites:
                tag = f"{site.name.lower()}_{min(window)}-{max(window)}"
                cached = work_dir / f"daily_{tag}.parquet"
                diag_path = work_dir / f"diag_{tag}.json"
                if args.resume and cached.exists() and diag_path.exists():
                    daily = pd.read_parquet(cached)
                    diagnostics = json.loads(diag_path.read_text(encoding="utf-8"))
                    if diagnostics.get("spec_signature") != REFERENCE_SIGNATURES[
                        REFERENCE_AUDITED
                    ]:
                        print(f"    {tag}: cached provenance stale, recomputing")
                    else:
                        print(f"    {tag}: reused from cache")
                        results.append(SiteResult(site.name, f"{min(window)}-{max(window)}", daily, diagnostics))
                        continue
                result = run_site_window(
                    site,
                    window,
                    cache_dir=args.era5_cache,
                    candidates=candidates,
                    with_ablations=not args.no_ablations,
                    with_sensitivity=not args.no_sensitivity,
                    verbose=verbose,
                )
                result.diagnostics["spec_signature"] = REFERENCE_SIGNATURES[REFERENCE_AUDITED]
                result.daily.to_parquet(cached)
                diag_path.write_text(
                    json.dumps(result.diagnostics, indent=2, default=str), encoding="utf-8"
                )
                results.append(result)

        # Dictionary-encode the two label columns and compress: this file is committed
        # evidence, so it should not carry a repeated string per row.
        combined = pd.concat(
            [r.daily.assign(site=r.site, window=r.window) for r in results]
        )
        for column in ("site", "window"):
            combined[column] = combined[column].astype("category")
        combined.to_parquet(
            out_dir / "daily_series.parquet", compression="zstd", index=True
        )

        audit = build_reference_audit_table(results)
        audit.to_csv(out_dir / "reference_audit.csv", index=False)
        print(f"  wrote {out_dir / 'reference_audit.csv'} ({len(audit)} rows)")

        parity = legacy_reproduction_parity(results, args.legacy_series)
        (out_dir / "legacy_reproduction.json").write_text(
            json.dumps(parity, indent=2), encoding="utf-8"
        )
        print(f"  legacy reproduction: {parity['status']}")
        manifest_extra["legacy_reproduction"] = parity["status"]

        (out_dir / "site_diagnostics.json").write_text(
            json.dumps([r.diagnostics for r in results], indent=2, default=str),
            encoding="utf-8",
        )

    if args.stage in {"experiment", "all"} and results:
        scores = build_score_table(results)
        scores.to_csv(out_dir / "candidate_scores.csv", index=False)
        print(f"  wrote {out_dir / 'candidate_scores.csv'} ({len(scores)} rows)")

        annual = build_annual_table(results)
        annual.to_csv(out_dir / "annual_counts.csv", index=False)
        print(f"  wrote {out_dir / 'annual_counts.csv'} ({len(annual)} rows)")

        if not args.no_uncertainty:
            uncertainty = build_uncertainty_table(results, candidates)
            uncertainty.to_csv(out_dir / "uncertainty.csv", index=False)
            print(f"  wrote {out_dir / 'uncertainty.csv'} ({len(uncertainty)} rows)")

        verdicts = verdict_from_tables(scores, annual)
        (out_dir / "gate_verdicts.json").write_text(
            json.dumps(verdicts, indent=2), encoding="utf-8"
        )
        print("\nGate verdicts (predeclared, SPEC.md 10)")
        for cid, verdict in verdicts.items():
            print(f"  {cid}: {verdict}")
        manifest_extra["gate_verdicts"] = verdicts

    if args.stage in {"inventory", "all"}:
        print("\nNEX daily-input inventory (read-only)")
        presence = nex_file_presence(
            args.nex_main_root,
            args.nex_wbgt_root,
            ("historical", "ssp245", "ssp585"),
        )
        presence.to_csv(out_dir / "nex_files_present.csv", index=False)
        print(f"  tier 1: {len(presence)} rows -> nex_files_present.csv")

        intersection = nex_required_intersection(presence)
        intersection.to_csv(out_dir / "nex_required_intersection.csv", index=False)
        complete = intersection[intersection["complete"]]
        rosters = {
            scenario: sorted(block["model"].unique())
            for scenario, block in complete.groupby("scenario")
        }
        (out_dir / "nex_roster.json").write_text(
            json.dumps(
                {
                    "required_variables": list(REQUIRED_NEX_VARIABLES),
                    "rosters_by_scenario": rosters,
                    "model_counts": {k: len(v) for k, v in rosters.items()},
                    "note": (
                        "Computed from disk. Neither the '21 models' figure nor the shade "
                        "release roster of 19 was carried over."
                    ),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        for scenario, models in rosters.items():
            print(f"  tier 1 roster {scenario}: {len(models)} models with all required variables")

        headers = nex_header_scan(
            args.nex_main_root,
            args.nex_wbgt_root,
            args.nex_scenario,
            parse_years(args.nex_header_years),
            None,
        )
        headers.to_csv(out_dir / "nex_header_scan.csv", index=False)
        print(f"  tier 2: {len(headers)} rows -> nex_header_scan.csv (bounded sample, not a national audit)")

        sample_models = [m.strip() for m in args.nex_sample_models.split(",") if m.strip()]
        sample = nex_site_sample(
            args.nex_main_root,
            args.nex_wbgt_root,
            args.nex_scenario,
            parse_years(args.nex_sample_years),
            sample_models,
            sites,
        )
        validity = nex_site_validity(sample)
        validity.to_csv(out_dir / "nex_site_validity.csv", index=False)
        print(f"  tier 3: {len(validity)} rows -> nex_site_validity.csv")

        distribution = nex_candidate_distribution(sample, sites)
        if not distribution.empty:
            distribution.to_parquet(work_dir / "nex_candidate_daily_max.parquet")
            summary_rows: list[dict[str, object]] = []
            for (model, scenario, site_name), block in distribution.groupby(
                ["model", "scenario", "site"]
            ):
                values = block["wbgt_daily_max_c"].dropna()
                idx = pd.DatetimeIndex(block.index)
                row: dict[str, object] = {
                    "model": model,
                    "scenario": scenario,
                    "site": site_name,
                    "n_days": int(len(block)),
                    "n_valid": int(len(values)),
                    "mean_c": float(values.mean()) if len(values) else np.nan,
                }
                for q in QUANTILES:
                    row[f"q{int(q * 100)}_c"] = (
                        float(values.quantile(q)) if len(values) else np.nan
                    )
                for threshold in THRESHOLDS_C:
                    years = len(set(idx.year))
                    row[f"days_ge_{threshold:g}_per_year"] = (
                        float((values >= threshold).sum() / years) if years else np.nan
                    )
                for season, months in SEASONS.items():
                    seasonal = values[pd.DatetimeIndex(values.index).month.isin(months)]
                    row[f"mean_{season}_c"] = (
                        float(seasonal.mean()) if len(seasonal) else np.nan
                    )
                summary_rows.append(row)
            pd.DataFrame(summary_rows).to_csv(
                out_dir / "nex_candidate_distribution.csv", index=False
            )
            print(f"  wrote {out_dir / 'nex_candidate_distribution.csv'}")
        else:
            print("  NEX candidate distribution: NOT EVALUATED (no complete sampled model-year)")

    (out_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest(args, manifest_extra), indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\nwrote {out_dir / 'run_manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
