"""Stage A validation harness for the Tier-2 outdoor-WBGT method (CHG-0538).

Purpose
-------
Decide whether the proposed Tier-2 outdoor-WBGT chain is defensible *before*
any NEX-GDDP-CMIP6 bytes are downloaded. It answers one question:

    If you only have DAILY climate-model fields, how much WBGT error does the
    daily Tier-2 chain incur relative to hourly Liljegren physics?

The trick is that this question does not involve CMIP6 at all. Both sides of
the comparison are driven from the *same* hourly ERA5 reanalysis, so climate
model error cancels and what remains is pure method error:

    truth      = hourly ERA5 -> Liljegren WBGT per hour -> daily maximum
    candidate  = hourly ERA5 -> collapse to daily means  -> Tier-2 chain
                 (Stull shade WBGT + empirical sun adjustment)

This is the protocol Kong & Huber (2022, 2024) use to grade simplified WBGT
forms, and it costs well under a gigabyte of public data.

The Tier-2 chain under test
---------------------------
1. ``wbgt_shade_stull_cell_c(tasmax_c, hurs)`` -- imported from the shipping
   module, not re-transcribed here, so the harness grades production code.
2. Daily-mean ``rsds`` is disaggregated to a peak hourly value using
   top-of-atmosphere solar geometry, then scaled by ``--peak-lag-factor``
   (default 0.75) because WBGT peaks a few hours after solar noon.
3. An additive sun adjustment refit from the CarbonPlan ``extreme-heat``
   training points (R2 0.92, RMSE 0.45 C)::

       adjustment = -2.1564 - 0.005375 * rsds_max + 1.0424 * sfcWind
       WBGT_sun   = WBGT_shade - adjustment

   Inputs are clipped to the fit domain (rsds_max 300-900 W/m2,
   wind 0.5-3.0 m/s) because the fit has no support outside it.

Acceptance criteria, fixed before any result was inspected
----------------------------------------------------------
Per site: median |bias| < 1.0 C AND RMSE < 1.5 C against hourly Liljegren,
restricted to warm days (daily max shade WBGT >= ``--warm-day-threshold``),
since that is the regime the metric is read in. All six sites must pass.

Data
----
ARCO-ERA5 (``gs://gcp-public-data-arco-era5``) -- public analysis-ready zarr,
anonymous access, no CDS registration.

Outputs (written only under ``--out-dir``; nothing else is touched)
-------------------------------------------------------------------
  wbgt_method_validation_daily.csv    per site per day, both methods
  wbgt_method_validation_summary.csv  per site statistics + verdict
  wbgt_method_validation_summary.md   human-readable verdict table

Usage
-----
    python -m tools.diagnostics.wbgt_method_validation --dry-run
    python -m tools.diagnostics.wbgt_method_validation --self-test
    python -m tools.diagnostics.wbgt_method_validation --years 1995-2014

``--dry-run`` exercises the entire chain on synthetic hourly data with no
network access and no optional dependencies, so the code path can be verified
before ``gcsfs``/``zarr``/``thermofeel`` are installed.
"""

from __future__ import annotations

import argparse
import inspect
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.compute.heat_stress_gridfirst import (
    stull_twb_c,
    wbgt_shade_stull_cell_c,
)

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

ARCO_ERA5_URI = "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"

#: ERA5 variable name -> role in this harness.
ERA5_VARIABLES: Mapping[str, str] = {
    "2m_temperature": "t2m_k",
    "2m_dewpoint_temperature": "d2m_k",
    "10m_u_component_of_wind": "u10_ms",
    "10m_v_component_of_wind": "v10_ms",
    "surface_pressure": "sp_pa",
    "surface_solar_radiation_downwards": "ssrd_j",
    "total_sky_direct_solar_radiation_at_surface": "fdir_j",
}

SOLAR_CONSTANT_W_M2 = 1367.0

#: Adjustment model refit from CarbonPlan ``extreme-heat`` training points.
ADJ_INTERCEPT = -2.1564
ADJ_RSDS_COEF = -0.005375
ADJ_WIND_COEF = 1.0424
ADJ_RSDS_CLIP = (300.0, 900.0)
ADJ_WIND_CLIP = (0.5, 3.0)

DEFAULT_PEAK_LAG_FACTOR = 0.75

#: Liljegren returns NaN where its fixed-point iteration does not converge,
#: which is normal for a small fraction of hours but is also exactly how a
#: units mistake presents. Above this rate the run aborts rather than silently
#: taking a daily maximum over a decimated sample.
MAX_LILJEGREN_NAN_RATE = 0.05

#: Acceptance thresholds. Fixed before results were inspected -- do not tune
#: these to make a run pass; a miss is a finding, not a parameter.
ACCEPT_MEDIAN_ABS_BIAS_C = 1.0
ACCEPT_RMSE_C = 1.5
DEFAULT_WARM_DAY_THRESHOLD_C = 25.0

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_method_validation")
DEFAULT_YEARS = "1995-2014"


@dataclass(frozen=True)
class Site:
    """A validation point, chosen to span India's distinct heat regimes."""

    name: str
    lat: float
    lon: float
    elevation_m: float
    regime: str


#: The six approved validation sites.
SITES: tuple[Site, ...] = (
    Site("Kochi", 9.93, 76.27, 3.0, "hot-humid coastal"),
    Site("Kolkata", 22.57, 88.36, 9.0, "monsoon delta"),
    Site("Bikaner", 28.02, 73.31, 242.0, "hot-dry Thar"),
    Site("Lucknow", 26.85, 80.95, 123.0, "Gangetic pre-monsoon"),
    Site("Hyderabad", 17.39, 78.49, 505.0, "semi-arid inland"),
    Site("Shimla", 31.10, 77.17, 2276.0, "elevation"),
)


# --------------------------------------------------------------------------
# Thermodynamic helpers
# --------------------------------------------------------------------------


def relative_humidity_from_dewpoint(t_c: np.ndarray, td_c: np.ndarray) -> np.ndarray:
    """Relative humidity in percent from temperature and dewpoint (deg C).

    Uses the same Magnus coefficients as ``swbgt_empirical_cell_c`` so the
    harness is internally consistent with the shipping code.
    """

    def _es(temp_c: np.ndarray) -> np.ndarray:
        return 6.112 * np.exp((17.62 * temp_c) / (243.12 + temp_c))

    rh = 100.0 * _es(td_c) / _es(t_c)
    return np.clip(rh, 0.0, 100.0)


def cos_solar_zenith(lat_deg: float, times: pd.DatetimeIndex, lon_deg: float) -> np.ndarray:
    """Cosine of the solar zenith angle, clipped at zero (night -> 0).

    Standard NOAA-style solar geometry with the equation of time. Accurate to
    well under a degree, which is far finer than this application needs.
    """

    doy = times.dayofyear.to_numpy().astype(float)
    hour_utc = (
        times.hour.to_numpy().astype(float)
        + times.minute.to_numpy().astype(float) / 60.0
    )

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

    time_offset_min = eqtime_min + 4.0 * lon_deg
    true_solar_min = hour_utc * 60.0 + time_offset_min
    hour_angle = np.deg2rad(true_solar_min / 4.0 - 180.0)

    lat = np.deg2rad(lat_deg)
    cosz = np.sin(lat) * np.sin(decl) + np.cos(lat) * np.cos(decl) * np.cos(hour_angle)
    return np.clip(cosz, 0.0, None)


def daily_peak_rsds_from_mean(
    rsds_daily_mean: np.ndarray,
    *,
    lat_deg: float,
    lon_deg: float,
    days: pd.DatetimeIndex,
    peak_lag_factor: float = DEFAULT_PEAK_LAG_FACTOR,
    steps_per_day: int = 24,
) -> np.ndarray:
    """Daily-mean rsds -> peak hourly rsds, via top-of-atmosphere geometry.

    This is the metsim ``solar_geom`` + ``shortwave`` idea reduced to its
    essential assumption: atmospheric transmissivity is treated as constant
    through the day, so the hourly shortwave profile has the same *shape* as
    the top-of-atmosphere profile and need only be rescaled to reproduce the
    known daily mean. The result is then multiplied by ``peak_lag_factor``
    because WBGT peaks a few hours after solar noon, when irradiance has
    already fallen below its own maximum (Parsons 2021).

    Returns an array aligned with ``days``. Polar-night style days with zero
    insolation return 0.0 rather than dividing by zero.
    """

    peaks = np.empty(len(days), dtype=float)
    for i, day in enumerate(days):
        sub = pd.date_range(day, periods=steps_per_day, freq="h")
        cosz = cos_solar_zenith(lat_deg, sub, lon_deg)
        toa = SOLAR_CONSTANT_W_M2 * cosz
        toa_mean = float(np.mean(toa))
        if toa_mean <= 0.0:
            peaks[i] = 0.0
            continue
        peaks[i] = float(np.max(toa)) / toa_mean
    return np.asarray(rsds_daily_mean, dtype=float) * peaks * peak_lag_factor


def sun_adjustment_c(rsds_max: np.ndarray, wind_ms: np.ndarray) -> np.ndarray:
    """CarbonPlan-style additive sun adjustment, in deg C.

    Returns the *adjustment* term, which is negative over most of its domain;
    the caller subtracts it, so shade WBGT is raised. Inputs are clipped to
    the fit domain because the underlying linear model has no support beyond
    it and extrapolates without bound.
    """

    r = np.clip(np.asarray(rsds_max, dtype=float), *ADJ_RSDS_CLIP)
    w = np.clip(np.asarray(wind_ms, dtype=float), *ADJ_WIND_CLIP)
    return ADJ_INTERCEPT + ADJ_RSDS_COEF * r + ADJ_WIND_COEF * w


def tier2_outdoor_wbgt_c(
    tasmax_c: np.ndarray,
    hurs_pct: np.ndarray,
    rsds_max: np.ndarray,
    wind_ms: np.ndarray,
) -> np.ndarray:
    """The full Tier-2 candidate chain, from daily fields to outdoor WBGT."""

    shade = np.asarray(wbgt_shade_stull_cell_c(tasmax_c, hurs_pct), dtype=float)
    return shade - sun_adjustment_c(rsds_max, wind_ms)


# --------------------------------------------------------------------------
# Truth: hourly Liljegren via thermofeel
# --------------------------------------------------------------------------


def _resolve_liljegren() -> Callable[..., np.ndarray]:
    """Return a keyword-mapped wrapper around thermofeel's Liljegren WBGT.

    thermofeel's argument names have moved between releases, so rather than
    hard-coding a call signature the wrapper inspects the installed function
    and maps the canonical quantities onto whatever it actually accepts. An
    unmapped required parameter is a loud failure, never a silent default.
    """

    try:
        import thermofeel  # noqa: PLC0415 - optional, resolved at call time
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "thermofeel is not installed. It supplies the Liljegren (2008) "
            "reference WBGT that this harness grades against. Install it into "
            "the irt env (pure-python/numpy, no PROJ or GDAL involvement), "
            "then re-run. Use --dry-run to exercise the chain without it."
        ) from exc

    func = getattr(thermofeel, "calculate_wbgt_liljegren", None)
    if func is None:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "thermofeel is installed but exposes no calculate_wbgt_liljegren; "
            f"available names: {sorted(n for n in dir(thermofeel) if 'wbgt' in n.lower())}"
        )

    # Canonical quantity -> the parameter names it has been known to carry.
    # Units verified against thermofeel 2.3.0's own docstring, which differs
    # from several older wrappers in three ways that all fail silently as NaN
    # rather than loudly: pressure is hPa (not Pa), ``fdir`` is the
    # DIMENSIONLESS direct-beam fraction 0-1 (not a flux in W/m2), and ``ssrd``
    # is instantaneous W/m2 (not a J/m2 hourly accumulation). Older builds took
    # dewpoint where 2.3.0 takes relative humidity; both are supplied so either
    # signature binds.
    aliases: dict[str, tuple[str, ...]] = {
        "t_k": ("t_k", "t2m", "t2_k", "ta", "t"),
        "rh_pct": ("rh", "rh_pct", "relative_humidity"),
        "td_k": ("td_k", "td", "d2m", "tdew"),
        "va": ("va", "wind", "u10", "va_ms", "v"),
        "pressure_hpa": ("pressure", "sp", "p"),
        "cossza": ("cossza", "cossza_", "cosz", "cos_sza"),
        "fdir_frac": ("fdir", "fdir_w", "direct"),
        "dsrf": ("dsrf", "ssrd", "ghi", "dsrp"),
    }

    params = inspect.signature(func).parameters
    accepted = [
        name
        for name, p in params.items()
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    ]

    def wrapper(**canonical: np.ndarray) -> np.ndarray:
        kwargs: dict[str, np.ndarray] = {}
        for name in accepted:
            for quantity, names in aliases.items():
                if name in names and quantity in canonical:
                    kwargs[name] = canonical[quantity]
                    break
        missing = [
            name
            for name in accepted
            if name not in kwargs and params[name].default is inspect.Parameter.empty
        ]
        if missing:
            raise RuntimeError(
                "Cannot map Liljegren inputs onto the installed thermofeel "
                f"signature; unmapped required parameters: {missing}. "
                f"Full signature: {inspect.signature(func)}. Extend the alias "
                "table in _resolve_liljegren()."
            )
        return np.asarray(func(**kwargs), dtype=float)

    return wrapper


def hourly_liljegren_wbgt_c(
    hourly: pd.DataFrame,
    *,
    liljegren: Callable[..., np.ndarray] | None = None,
) -> np.ndarray:
    """Hourly Liljegren WBGT in deg C for one site.

    ``hourly`` must carry columns ``t2m_k, td_k, wind_ms, sp_pa, cossza,
    fdir_frac, ssrd_w``. ``ssrd_w`` is instantaneous W/m2 (already
    de-accumulated from ERA5's J/m2) and ``fdir_frac`` is the dimensionless
    direct-beam fraction; pressure is converted to hPa here. Getting any of
    these three conventions wrong makes thermofeel return NaN rather than
    raise, so they are asserted below instead of trusted.

    Wind is passed at its native 10 m height. thermofeel's Liljegren routine
    applies its own ``wind_scaling='liljegren'`` reduction to 2 m internally,
    so the reference side of the comparison handles wind height correctly
    while the Tier-2 candidate does not -- which is exactly the asymmetry the
    validation is meant to expose, not something to paper over here.
    """

    solver = liljegren if liljegren is not None else _resolve_liljegren()
    t_c = hourly["t2m_k"].to_numpy() - 273.15
    td_c = hourly["td_k"].to_numpy() - 273.15
    fdir_frac = np.clip(hourly["fdir_frac"].to_numpy(), 0.0, 1.0)

    wbgt_k = solver(
        t_k=hourly["t2m_k"].to_numpy(),
        rh_pct=relative_humidity_from_dewpoint(t_c, td_c),
        td_k=hourly["td_k"].to_numpy(),
        va=hourly["wind_ms"].to_numpy(),
        pressure_hpa=hourly["sp_pa"].to_numpy() / 100.0,
        cossza=hourly["cossza"].to_numpy(),
        fdir_frac=fdir_frac,
        dsrf=hourly["ssrd_w"].to_numpy(),
    )
    wbgt = np.asarray(wbgt_k, dtype=float)
    # thermofeel returns kelvin; tolerate a build that already returns celsius.
    if np.nanmedian(wbgt) > 100.0:
        wbgt = wbgt - 273.15

    # A wrong unit convention shows up as NaN, never as an exception, so a
    # high non-finite rate is treated as a configuration error rather than
    # quietly shrinking the sample the daily maximum is taken over.
    nan_rate = float(np.mean(~np.isfinite(wbgt)))
    if nan_rate > MAX_LILJEGREN_NAN_RATE:
        raise RuntimeError(
            f"Liljegren returned {nan_rate:.1%} non-finite values, above the "
            f"{MAX_LILJEGREN_NAN_RATE:.0%} tolerance. This is the signature of "
            "a unit mismatch: thermofeel expects pressure in hPa, ssrd as "
            "instantaneous W/m2, and fdir as a 0-1 fraction. Check the inputs "
            "before trusting any daily maximum built on them."
        )
    return wbgt


# --------------------------------------------------------------------------
# ERA5 access
# --------------------------------------------------------------------------


def open_arco_era5(uri: str = ARCO_ERA5_URI) -> xr.Dataset:
    """Open the public ARCO-ERA5 zarr store read-only and anonymously."""

    try:
        import gcsfs  # noqa: F401, PLC0415 - presence check only
        import zarr  # noqa: F401, PLC0415
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "gcsfs and zarr are required to read ARCO-ERA5. Install both into "
            "the irt env, then re-run. Use --dry-run to exercise the chain "
            "without network access."
        ) from exc

    return xr.open_zarr(
        uri,
        chunks=None,
        storage_options={"token": "anon"},
        consolidated=True,
    )


def extract_site_hourly(
    dataset: xr.Dataset,
    site: Site,
    *,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Pull one ERA5 grid cell for one site into an hourly frame.

    Returns columns ``t2m_k, td_k, wind_ms, sp_pa, ssrd_w, fdir_frac`` plus
    the cosine of the solar zenith angle. ERA5 shortwave fields are hourly
    accumulations in J/m2, so they are divided by 3600 to reach W/m2; the
    direct-beam flux is then divided by the global flux to give the
    dimensionless fraction thermofeel actually wants, and is defined as zero
    where there is no incoming shortwave at all.
    """

    missing = [v for v in ERA5_VARIABLES if v not in dataset.variables]
    if missing:
        raise RuntimeError(
            f"ARCO-ERA5 store is missing expected variables: {missing}. "
            "The store layout may have changed; inspect dataset.variables."
        )

    lon = site.lon % 360.0
    point = (
        dataset[list(ERA5_VARIABLES)]
        .sel(time=slice(start, end))
        .sel(latitude=site.lat, longitude=lon, method="nearest")
        .load()
    )

    times = pd.DatetimeIndex(point["time"].to_numpy())
    u = point["10m_u_component_of_wind"].to_numpy()
    v = point["10m_v_component_of_wind"].to_numpy()

    frame = pd.DataFrame(
        {
            "t2m_k": point["2m_temperature"].to_numpy(),
            "td_k": point["2m_dewpoint_temperature"].to_numpy(),
            "wind_ms": np.sqrt(u**2 + v**2),
            "sp_pa": point["surface_pressure"].to_numpy(),
            "ssrd_w": point["surface_solar_radiation_downwards"].to_numpy() / 3600.0,
            "fdir_raw_w": point["total_sky_direct_solar_radiation_at_surface"].to_numpy()
            / 3600.0,
        },
        index=times,
    )
    frame["cossza"] = cos_solar_zenith(site.lat, times, site.lon)
    with np.errstate(divide="ignore", invalid="ignore"):
        frac = np.where(
            frame["ssrd_w"].to_numpy() > 0.0,
            frame["fdir_raw_w"].to_numpy() / frame["ssrd_w"].to_numpy(),
            0.0,
        )
    frame["fdir_frac"] = np.clip(np.nan_to_num(frac, nan=0.0), 0.0, 1.0)
    return frame


# --------------------------------------------------------------------------
# Synthetic data for --dry-run
# --------------------------------------------------------------------------


def synthetic_site_hourly(site: Site, *, days: int = 90, seed: int = 0) -> pd.DataFrame:
    """Plausible hourly forcing for one site, for offline chain exercise.

    This is deliberately crude -- a diurnal cycle with noise. It exists to
    prove the code path runs end to end, never to produce a scientific result,
    and the driver labels any run built on it accordingly.
    """

    rng = np.random.default_rng(seed + int(abs(site.lat) * 100))
    times = pd.date_range("2010-03-01", periods=days * 24, freq="h")
    cossza = cos_solar_zenith(site.lat, times, site.lon)

    base_c = 30.0 - 0.0065 * site.elevation_m
    diurnal = 8.0 * cossza
    t_c = base_c + diurnal + rng.normal(0.0, 1.0, times.size)
    rh = np.clip(65.0 - 25.0 * cossza + rng.normal(0.0, 5.0, times.size), 5.0, 99.0)

    es = 6.112 * np.exp((17.62 * t_c) / (243.12 + t_c))
    e = es * rh / 100.0
    td_c = 243.12 * np.log(e / 6.112) / (17.62 - np.log(e / 6.112))

    ssrd = 0.75 * SOLAR_CONSTANT_W_M2 * cossza
    return pd.DataFrame(
        {
            "t2m_k": t_c + 273.15,
            "td_k": td_c + 273.15,
            "wind_ms": np.clip(rng.gamma(2.0, 1.0, times.size), 0.1, 12.0),
            "sp_pa": 101325.0 * np.exp(-site.elevation_m / 8400.0),
            "ssrd_w": ssrd,
            "fdir_frac": np.where(ssrd > 0.0, 0.7, 0.0),
            "cossza": cossza,
        },
        index=times,
    )


def stub_liljegren(**canonical: np.ndarray) -> np.ndarray:
    """Stand-in reference used only by ``--dry-run``.

    Shade WBGT plus a crude irradiance/wind term. It is NOT Liljegren physics
    and must never be used to judge acceptance; it only gives the comparison
    machinery something numeric to chew on offline.
    """

    t_c = canonical["t_k"] - 273.15
    rh = canonical.get("rh_pct")
    if rh is None:
        rh = relative_humidity_from_dewpoint(t_c, canonical["td_k"] - 273.15)
    shade = np.asarray(wbgt_shade_stull_cell_c(t_c, rh), dtype=float)
    wind = np.clip(canonical["va"], 0.5, None)
    return shade + 0.004 * canonical["dsrf"] / np.sqrt(wind)


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------


def compare_site(
    site: Site,
    hourly: pd.DataFrame,
    *,
    liljegren: Callable[..., np.ndarray] | None = None,
    peak_lag_factor: float = DEFAULT_PEAK_LAG_FACTOR,
) -> pd.DataFrame:
    """Run both methods for one site and return a per-day frame.

    Truth is the daily maximum of hourly Liljegren WBGT. The candidate is the
    Tier-2 chain fed with the daily aggregates a CMIP6 archive would actually
    provide: daily maximum temperature, daily mean relative humidity, daily
    mean wind, and daily mean downwelling shortwave.
    """

    frame = hourly.copy()
    frame["wbgt_truth_c"] = hourly_liljegren_wbgt_c(frame, liljegren=liljegren)
    frame["t_c"] = frame["t2m_k"] - 273.15
    frame["rh_pct"] = relative_humidity_from_dewpoint(
        frame["t_c"].to_numpy(), (frame["td_k"] - 273.15).to_numpy()
    )

    grouped = frame.groupby(frame.index.normalize())
    daily = pd.DataFrame(
        {
            "wbgt_truth_c": grouped["wbgt_truth_c"].max(),
            "tasmax_c": grouped["t_c"].max(),
            "hurs_pct": grouped["rh_pct"].mean(),
            "sfcwind_ms": grouped["wind_ms"].mean(),
            "rsds_mean_w": grouped["ssrd_w"].mean(),
        }
    )
    days = pd.DatetimeIndex(daily.index)

    daily["rsds_max_w"] = daily_peak_rsds_from_mean(
        daily["rsds_mean_w"].to_numpy(),
        lat_deg=site.lat,
        lon_deg=site.lon,
        days=days,
        peak_lag_factor=peak_lag_factor,
    )
    daily["wbgt_shade_c"] = np.asarray(
        wbgt_shade_stull_cell_c(daily["tasmax_c"].to_numpy(), daily["hurs_pct"].to_numpy()),
        dtype=float,
    )
    daily["adjustment_c"] = sun_adjustment_c(
        daily["rsds_max_w"].to_numpy(), daily["sfcwind_ms"].to_numpy()
    )
    daily["wbgt_tier2_c"] = daily["wbgt_shade_c"] - daily["adjustment_c"]
    daily["bias_c"] = daily["wbgt_tier2_c"] - daily["wbgt_truth_c"]

    daily.insert(0, "site", site.name)
    daily.insert(1, "regime", site.regime)
    daily.index.name = "date"
    return daily.reset_index()


def summarise_site(
    daily: pd.DataFrame,
    *,
    warm_day_threshold_c: float = DEFAULT_WARM_DAY_THRESHOLD_C,
) -> dict[str, object]:
    """Reduce one site's daily frame to the acceptance statistics."""

    warm = daily.loc[daily["wbgt_shade_c"] >= warm_day_threshold_c]
    used = warm if len(warm) >= 30 else daily
    bias = used["bias_c"].to_numpy()
    bias = bias[np.isfinite(bias)]

    if bias.size == 0:
        return {
            "site": daily["site"].iloc[0],
            "regime": daily["regime"].iloc[0],
            "n_days": 0,
            "restricted_to_warm_days": bool(len(warm) >= 30),
            "mean_bias_c": np.nan,
            "median_bias_c": np.nan,
            "median_abs_bias_c": np.nan,
            "rmse_c": np.nan,
            "p95_abs_bias_c": np.nan,
            "verdict": "NO DATA",
        }

    median_abs = float(np.median(np.abs(bias)))
    rmse = float(np.sqrt(np.mean(bias**2)))
    passed = median_abs < ACCEPT_MEDIAN_ABS_BIAS_C and rmse < ACCEPT_RMSE_C

    return {
        "site": daily["site"].iloc[0],
        "regime": daily["regime"].iloc[0],
        "n_days": int(bias.size),
        "restricted_to_warm_days": bool(len(warm) >= 30),
        "mean_bias_c": float(np.mean(bias)),
        "median_bias_c": float(np.median(bias)),
        "median_abs_bias_c": median_abs,
        "rmse_c": rmse,
        "p95_abs_bias_c": float(np.percentile(np.abs(bias), 95)),
        "verdict": "PASS" if passed else "FAIL",
    }


def render_summary_markdown(
    summary: pd.DataFrame,
    *,
    years: str,
    peak_lag_factor: float,
    warm_day_threshold_c: float,
    synthetic: bool,
) -> str:
    """Render the verdict table, including what to do if a site fails."""

    lines: list[str] = []
    lines.append("# Outdoor WBGT Tier-2 method validation (Stage A)\n")
    if synthetic:
        lines.append(
            "> **SYNTHETIC DRY RUN -- NOT A RESULT.** Generated with `--dry-run`, "
            "which uses fabricated forcing and a stand-in reference in place of "
            "Liljegren physics. The numbers below exercise the code path only "
            "and carry no scientific meaning.\n"
        )
    lines.append(
        "Truth is the daily maximum of hourly Liljegren (2008) WBGT computed on "
        "ARCO-ERA5. The candidate is the daily Tier-2 chain (Stull shade WBGT "
        "plus an empirical sun adjustment) driven from the same ERA5 hours "
        "collapsed to daily aggregates, so model error cancels and only method "
        "error remains.\n"
    )
    lines.append(
        f"- Period: `{years}`\n"
        f"- Peak-lag factor: `{peak_lag_factor}`\n"
        f"- Warm-day threshold: shade WBGT >= `{warm_day_threshold_c}` C\n"
        f"- Acceptance: median |bias| < `{ACCEPT_MEDIAN_ABS_BIAS_C}` C "
        f"and RMSE < `{ACCEPT_RMSE_C}` C, in every regime\n"
    )

    lines.append(
        "\n| Site | Regime | Days | Mean bias | Median \\|bias\\| | RMSE | p95 \\|bias\\| | Verdict |"
    )
    lines.append("|---|---|---:|---:|---:|---:|---:|---|")
    for _, row in summary.iterrows():
        lines.append(
            f"| {row['site']} | {row['regime']} | {row['n_days']} | "
            f"{row['mean_bias_c']:+.2f} | {row['median_abs_bias_c']:.2f} | "
            f"{row['rmse_c']:.2f} | {row['p95_abs_bias_c']:.2f} | {row['verdict']} |"
        )

    failures = summary.loc[summary["verdict"] != "PASS", "site"].tolist()
    lines.append("\n## Verdict\n")
    if not failures:
        lines.append(
            "All six regimes pass. The Tier-2 chain is defensible on daily "
            "inputs; proceed to Stage B (add `rsds`/`sfcWind` to the "
            "downloader, pilot bbox first).\n"
        )
    elif len(failures) == 1:
        lines.append(
            f"One regime fails: **{failures[0]}**. Per the approved plan this "
            "triggers a refit of the adjustment model against Liljegren over "
            "the real Indian joint distribution of temperature, humidity, "
            "irradiance and wind, rather than a fall-back.\n"
        )
    else:
        lines.append(
            f"{len(failures)} regimes fail ({', '.join(failures)}). Per the "
            "approved plan this is a broad miss: fall back to Tier 3 -- keep "
            "Stull shade WBGT only, renamed honestly, and publish no outdoor "
            "WBGT at all.\n"
        )

    lines.append(
        "\n## Open questions this run does not settle\n\n"
        "1. CMIP6 `sfcWind` is a 10 m wind; the adjustment model was fitted on "
        "2 m values. No height conversion is applied, worth roughly +0.3 to "
        "+0.8 C in the direction of understating outdoor heat.\n"
        "2. The 0.75 peak-lag factor was derived outside India. Re-run with "
        "`--peak-lag-factor` to test its sensitivity before accepting it.\n"
        "3. The adjustment model takes neither temperature nor humidity as an "
        "input, having been fitted at 35 C / 50 % RH only.\n"
    )
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------


def run_self_test() -> int:
    """Check the pieces that have published reference values. Returns exit code."""

    failures: list[str] = []

    # Stull (2011) worked example: 20 C at 50 % RH -> 13.7 C wet-bulb.
    twb = float(np.asarray(stull_twb_c(20.0, 50.0)))
    if abs(twb - 13.70) > 0.05:
        failures.append(f"Stull worked example: expected 13.70 C, got {twb:.3f} C")

    # Shade WBGT must sit between wet-bulb and dry-bulb.
    shade = float(np.asarray(wbgt_shade_stull_cell_c(35.0, 60.0)))
    twb35 = float(np.asarray(stull_twb_c(35.0, 60.0)))
    if not twb35 < shade < 35.0:
        failures.append(f"Shade WBGT {shade:.2f} not bracketed by {twb35:.2f} and 35.0")

    # Solar noon in India must beat midnight, and night must be zero.
    times = pd.date_range("2010-06-21", periods=24, freq="h")
    cosz = cos_solar_zenith(17.39, times, 78.49)
    if cosz.max() <= 0.8 or cosz.min() != 0.0:
        failures.append(f"Solar geometry implausible: max={cosz.max():.3f} min={cosz.min():.3f}")

    # Disaggregation must amplify a daily mean into a larger peak.
    days = pd.DatetimeIndex(["2010-05-15"])
    peak = daily_peak_rsds_from_mean(
        np.array([250.0]), lat_deg=17.39, lon_deg=78.49, days=days
    )[0]
    if not 250.0 < peak < 900.0:
        failures.append(f"Peak rsds {peak:.1f} W/m2 outside a plausible range for 250 W/m2 mean")

    # The adjustment must be clipped, not extrapolated.
    wild = sun_adjustment_c(np.array([5000.0]), np.array([50.0]))[0]
    bounded = sun_adjustment_c(np.array([900.0]), np.array([3.0]))[0]
    if not np.isclose(wild, bounded):
        failures.append("sun_adjustment_c is extrapolating outside its fit domain")

    # thermofeel is optional, but when present its unit convention is checked,
    # because getting it wrong yields NaN rather than an exception. Three cases
    # with a known qualitative ordering: in dry heat evaporation wins and WBGT
    # sits below air temperature; in humid sun radiation wins and it sits
    # above; at night with no sun it sits below again.
    try:
        solver = _resolve_liljegren()
    except RuntimeError as exc:
        print(f"SKIP  thermofeel unit check: {exc.args[0].splitlines()[0]}")
    else:
        def _wbgt_c(t_c, rh, ghi, frac, cosz, wind, p_hpa=1013.0):
            arr = lambda x: np.array([x], dtype=float)
            out = solver(
                t_k=arr(t_c + 273.15),
                rh_pct=arr(rh),
                td_k=arr(t_c + 273.15),
                va=arr(wind),
                pressure_hpa=arr(p_hpa),
                cossza=arr(cosz),
                fdir_frac=arr(frac),
                dsrf=arr(ghi),
            )
            return float(np.asarray(out, dtype=float)[0] - 273.15)

        dry = _wbgt_c(40.0, 20.0, 900.0, 0.78, 0.95, 2.0)
        humid = _wbgt_c(33.0, 75.0, 800.0, 0.70, 0.90, 2.0)
        night = _wbgt_c(28.0, 80.0, 0.0, 0.0, 0.0, 1.5)
        for label, value in (("dry", dry), ("humid", humid), ("night", night)):
            if not np.isfinite(value):
                failures.append(
                    f"Liljegren returned NaN for the {label} reference case -- "
                    "this is the signature of a unit mismatch (hPa / W-m-2 / "
                    "0-1 fraction), not of non-convergence"
                )
        if np.isfinite([dry, humid, night]).all():
            if not dry < 40.0:
                failures.append(f"Dry-heat WBGT {dry:.2f} should sit below Ta 40.0 C")
            if not humid > 33.0:
                failures.append(f"Humid-sun WBGT {humid:.2f} should sit above Ta 33.0 C")
            if not night < 28.0:
                failures.append(f"Night WBGT {night:.2f} should sit below Ta 28.0 C")

    for line in failures:
        print(f"FAIL  {line}")
    if failures:
        print(f"\n{len(failures)} self-test failure(s).")
        return 1
    print("Self-test passed: Stull worked example, WBGT bracketing, solar geometry,")
    print("shortwave disaggregation, adjustment-domain clipping, Liljegren units.")
    return 0


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def parse_years(spec: str) -> tuple[str, str]:
    """Parse ``1995-2014`` or ``2010`` into inclusive ISO start/end bounds."""

    spec = spec.strip()
    if "-" in spec:
        first, last = spec.split("-", 1)
    else:
        first = last = spec
    return f"{int(first):04d}-01-01", f"{int(last):04d}-12-31"


def select_sites(names: Sequence[str] | None) -> tuple[Site, ...]:
    if not names:
        return SITES
    wanted = {n.strip().casefold() for n in names}
    chosen = tuple(s for s in SITES if s.name.casefold() in wanted)
    unknown = wanted - {s.name.casefold() for s in chosen}
    if unknown:
        raise SystemExit(
            f"Unknown site(s): {sorted(unknown)}. Known: {[s.name for s in SITES]}"
        )
    return chosen


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.diagnostics.wbgt_method_validation",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--years",
        default=DEFAULT_YEARS,
        help=f"Inclusive year range, e.g. 1995-2014 (default: {DEFAULT_YEARS}).",
    )
    parser.add_argument(
        "--sites",
        nargs="*",
        default=None,
        help="Subset of site names; default is all six.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Directory for CSV and Markdown outputs (default: {DEFAULT_OUT_DIR}).",
    )
    parser.add_argument(
        "--peak-lag-factor",
        type=float,
        default=DEFAULT_PEAK_LAG_FACTOR,
        help="Scaling from peak irradiance to the irradiance at peak WBGT.",
    )
    parser.add_argument(
        "--warm-day-threshold",
        type=float,
        default=DEFAULT_WARM_DAY_THRESHOLD_C,
        help="Shade WBGT (C) above which a day counts toward acceptance.",
    )
    parser.add_argument(
        "--era5-uri",
        default=ARCO_ERA5_URI,
        help="ARCO-ERA5 zarr store URI.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Synthetic forcing and a stand-in reference; no network, no optional deps.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Check published reference values and internal invariants, then exit.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Print the summary but write no files.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.self_test:
        return run_self_test()

    sites = select_sites(args.sites)
    start, end = parse_years(args.years)

    liljegren: Callable[..., np.ndarray] | None
    if args.dry_run:
        print("DRY RUN: synthetic forcing, stand-in reference. Results are not science.\n")
        liljegren = stub_liljegren
        dataset = None
    else:
        liljegren = _resolve_liljegren()
        print(f"Opening {args.era5_uri} ...")
        dataset = open_arco_era5(args.era5_uri)

    daily_frames: list[pd.DataFrame] = []
    summaries: list[dict[str, object]] = []

    for site in sites:
        print(f"[{site.name:10s}] {site.regime}")
        if args.dry_run:
            hourly = synthetic_site_hourly(site)
        else:
            hourly = extract_site_hourly(dataset, site, start=start, end=end)
            print(f"             {len(hourly):,} hours loaded")

        daily = compare_site(
            site, hourly, liljegren=liljegren, peak_lag_factor=args.peak_lag_factor
        )
        summary = summarise_site(daily, warm_day_threshold_c=args.warm_day_threshold)
        daily_frames.append(daily)
        summaries.append(summary)
        print(
            f"             n={summary['n_days']}  "
            f"mean bias {summary['mean_bias_c']:+.2f} C  "
            f"median |bias| {summary['median_abs_bias_c']:.2f} C  "
            f"RMSE {summary['rmse_c']:.2f} C  -> {summary['verdict']}"
        )

    daily_all = pd.concat(daily_frames, ignore_index=True)
    summary_all = pd.DataFrame(summaries)

    markdown = render_summary_markdown(
        summary_all,
        years=args.years,
        peak_lag_factor=args.peak_lag_factor,
        warm_day_threshold_c=args.warm_day_threshold,
        synthetic=args.dry_run,
    )
    print("\n" + markdown)

    if not args.no_write:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        suffix = "_dryrun" if args.dry_run else ""
        daily_path = out_dir / f"wbgt_method_validation_daily{suffix}.csv"
        summary_path = out_dir / f"wbgt_method_validation_summary{suffix}.csv"
        md_path = out_dir / f"wbgt_method_validation_summary{suffix}.md"
        daily_all.to_csv(daily_path, index=False)
        summary_all.to_csv(summary_path, index=False)
        md_path.write_text(markdown, encoding="utf-8")
        print(f"\nWrote {daily_path}\n      {summary_path}\n      {md_path}")

    if args.dry_run:
        return 0
    return 0 if (summary_all["verdict"] == "PASS").all() else 1


if __name__ == "__main__":
    sys.exit(main())
