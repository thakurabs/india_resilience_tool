"""Outdoor-WBGT method selection: two new wind candidates, oracles, reference sensitivity,
a CarbonPlan-style comparator, a matched-period NEX assessment and a conditional QDM trial.

Frozen contract: ``docs/diagnostics/wbgt_outdoor_selection/SPEC.md`` (CHG-0608). Everything this
module measures was declared there before any score was computed.

This is milestone 2 of the physically reconstructed open-sky WBGT. It **builds on** milestone 1
(``tools/diagnostics/wbgt_outdoor_feasibility.py``) and imports its reference identities,
candidates C1 and C2, gate definitions, scoring and tables rather than restating them, so the
carried-forward gates cannot drift. Milestone 1's artifacts are read-only here.

Nothing in this module touches production computation, metric registration, the national shade
rebuild, its stage, its caches, ``processed``, ``processed_optimised`` or ``irt_data``. It never
downloads and never installs.

Usage::

    python -m tools.diagnostics.wbgt_outdoor_selection --self-test
    python -m tools.diagnostics.wbgt_outdoor_selection --dry-run
    python -m tools.diagnostics.wbgt_outdoor_selection --stage all --workers 1
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in (None, ""):  # pragma: no cover - direct-script fallback
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.diagnostics import wbgt_outdoor_feasibility as m1
from tools.diagnostics.wbgt_method_validation import SITES, Site, cos_solar_zenith

# ==========================================================================
# Frozen contract constants (SPEC.md). Gates and references are IMPORTED from
# milestone 1, never restated, so that they cannot drift.
# ==========================================================================

#: SPEC.md 3.1 -- W1's DTR-to-amplitude relationship. A declared assumption, not a published
#: result: the standard daily-to-subdaily treatments (MTCLIM, metsim, and the CarbonPlan chain
#: built on metsim) hold wind constant through the day. The slope is fixed by requiring W1 to
#: reproduce C2's already declared amplitude of 0.4 at a DTR of 13 1/3 C; no coefficient here
#: was derived from any site's DTR or from any WBGT error.
WIND_DTR_SLOPE_PER_C = 0.03
WIND_DTR_AMP_MIN = 0.10
WIND_DTR_AMP_MAX = 0.80
WIND_DTR_HINGE_C = m1.WIND_DIURNAL_AMPLITUDE / WIND_DTR_SLOPE_PER_C  # 13.333... C

#: thermofeel's internal 10 m floor, reported per candidate (SPEC.md 3.1).
THERMOFEEL_MIN_WIND_10M = 0.62

#: SPEC.md 3.2 -- W2's one declared chronological split.
W2_PRIMARY_CALIBRATION = "1990-2004"
W2_PRIMARY_EVALUATION = "2005-2014"

#: SPEC.md 5 -- the third reference identity.
REFERENCE_MIDPOINT = "midpoint"
REFERENCE_SIGNATURE_MIDPOINT = (
    "liljegren-midpoint-v1:all-drivers-interpolated-to-radiation-interval-midpoint"
)
REFERENCE_SENSITIVITY_SITES = ("Bikaner", "Hyderabad")

#: SPEC.md 6 -- CarbonPlan source identity, retrieved and read verbatim 2026-09-29.
CARBONPLAN_REPO = "github.com/carbonplan/extreme-heat"
CARBONPLAN_REVISION = "f662b37200fe219db912ebd09ceb52fdac979861"
CARBONPLAN_REVISION_DATE = "2024-06-14"
CARBONPLAN_NOTEBOOKS = ("07_solar_radiation_wind.ipynb", "08_shade_sun_adjustment.ipynb")

#: The 16 (rsds_max, sfcWind) -> adjustment points CarbonPlan reads off Kong and Huber (2022)
#: Figure S12 and fits by OLS in notebook 08. Transcribed verbatim from that notebook.
CARBONPLAN_KONG_HUBER_X = (
    (300.0, 0.5), (500.0, 0.5), (700.0, 0.5), (900.0, 0.5),
    (300.0, 1.0), (500.0, 1.0), (700.0, 1.0), (900.0, 1.0),
    (300.0, 2.0), (500.0, 2.0), (700.0, 2.0), (900.0, 2.0),
    (300.0, 3.0), (500.0, 3.0), (700.0, 3.0), (900.0, 3.0),
)
CARBONPLAN_KONG_HUBER_Y = (
    -3.0, -5.0, -6.0, -7.0, -2.0, -3.5, -4.5, -6.0,
    -1.5, -2.5, -3.5, -4.5, -1.5, -2.0, -3.0, -3.5,
)

#: SPEC.md 8 -- matched-period NEX assessment.
NEX_MODELS = ("ACCESS-CM2", "MRI-ESM2-0")
NEX_SCENARIO = "historical"
NEX_MATCHED_YEARS = tuple(range(1990, 2000))

#: SPEC.md 9 -- the bias-correction trigger, frozen before any NEX score was inspected.
QDM_TRIGGER_REL_TOL = 0.25
QDM_TRIGGER_MIN_SITES = 2
QDM_TRIGGER_ABS_DAYS = 15.0
QDM_TRIGGER_THRESHOLD_C = 32.0

#: SPEC.md 9 -- QDM settings, as verified against CarbonPlan notebook 06.
QDM_NQUANTILES = 100
QDM_WINDOW_DAYS = 31
QDM_TRAIN_YEARS = tuple(range(1990, 2000))
QDM_TEST_YEARS = tuple(range(2000, 2011))
QDM_FUTURE_SCENARIO = "ssp585"
QDM_FUTURE_YEARS = tuple(range(2071, 2081))

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_outdoor_selection")
DEFAULT_WORK_DIR = Path("scratch/wbgt_outdoor_selection")

#: Candidates eligible for selection (SPEC.md 3). W2rev and the oracles are not.
DEPLOYABLE_IDS: tuple[str, ...] = ("C1", "C2", "W1", "W2")
NEW_WIND_IDS: tuple[str, ...] = ("W1", "W2", "W2rev")
ORACLE_IDS: tuple[str, ...] = ("A5", "A6")

CANDIDATE_NAMES: Mapping[str, str] = {
    "C1": "C1 recon-baseline (constant daily-mean wind) [milestone 1, unchanged]",
    "C2": "C2 recon-wind-diurnal (fixed amplitude 0.4) [milestone 1, unchanged]",
    "W1": "W1 recon-wind-dtr (DTR-dependent amplitude, declared assumption)",
    "W2": "W2 recon-wind-climatology (site-month hourly profile, forward calibration)",
    "W2rev": "W2rev recon-wind-climatology (REVERSE calibration; diagnostic only)",
    "A5": "A5 ORACLE C1 drivers with ACTUAL hourly wind",
    "A6": "A6 ORACLE C2 drivers with ACTUAL hourly wind",
}

#: Candidates that need no asset beyond what a NEX-driven national pipeline already has.
NEEDS_CLIMATOLOGY_ASSET: Mapping[str, bool] = {
    "C1": False, "C2": False, "W1": False, "W2": True, "W2rev": True,
}


# ==========================================================================
# Wind shapes
# ==========================================================================


def zero_mean_cz_shape(cossza: np.ndarray, local_day: np.ndarray) -> np.ndarray:
    """The mean-preserving within-day shape shared by C2 and W1.

    ``cz`` normalised by its own daily maximum, minus that day's mean, so the shape sums to
    zero over every local day and any amplitude applied to it preserves the daily mean exactly.
    Reproduces milestone 1's C2 construction; asserted equal in the companion test module.
    """

    cz = np.clip(np.asarray(cossza, dtype=float), 0.0, None)
    frame = pd.DataFrame({"cz": cz, "day": local_day})
    day_max = frame.groupby("day")["cz"].transform("max").to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        norm = np.where(day_max > 0.0, cz / day_max, 0.0)
    frame["norm"] = norm
    norm_mean = frame.groupby("day")["norm"].transform("mean").to_numpy(dtype=float)
    return norm - norm_mean


def dtr_amplitude(daily: pd.DataFrame, local_day: np.ndarray) -> np.ndarray:
    """W1's per-day amplitude: ``clip(slope * DTR, min, max)`` (SPEC.md 3.1)."""

    day_index = pd.DatetimeIndex(local_day)
    tasmax = daily["tasmax"].reindex(day_index).to_numpy(dtype=float)
    tasmin = daily["tasmin"].reindex(day_index).to_numpy(dtype=float)
    dtr = tasmax - tasmin
    return np.clip(WIND_DTR_SLOPE_PER_C * dtr, WIND_DTR_AMP_MIN, WIND_DTR_AMP_MAX)


def wind_w1_dtr_ms(
    daily: pd.DataFrame, local_day: np.ndarray, cossza: np.ndarray
) -> np.ndarray:
    """W1: C2's shape with a DTR-dependent amplitude. Non-negative, mean-preserving."""

    wbar = daily["sfcWind"].reindex(pd.DatetimeIndex(local_day)).to_numpy(dtype=float)
    shape = zero_mean_cz_shape(cossza, local_day)
    return np.clip(wbar * (1.0 + dtr_amplitude(daily, local_day) * shape), 0.0, None)


def wind_climatology_profile(hourly: pd.DataFrame) -> pd.DataFrame:
    """Normalised site-month hourly wind profile from the CALIBRATION period only.

    ``ratio = wind / daily-mean wind`` averaged by (calendar month of the local day, UTC hour),
    then rescaled so each month's 24 values average exactly 1.  Returns a frame indexed by
    month with 24 hour columns.  Every value is non-negative because every ratio is.
    """

    frame = hourly[["wind_speed_10m", "local_day"]].copy()
    keep = m1.complete_local_days(hourly)
    frame = frame[frame["local_day"].isin(keep)]
    day_mean = frame.groupby("local_day")["wind_speed_10m"].transform("mean")
    with np.errstate(divide="ignore", invalid="ignore"):
        frame["ratio"] = np.where(
            day_mean.to_numpy(dtype=float) > 0.0,
            frame["wind_speed_10m"].to_numpy(dtype=float) / day_mean.to_numpy(dtype=float),
            np.nan,
        )
    frame["month"] = pd.DatetimeIndex(frame["local_day"]).month
    frame["hour"] = pd.DatetimeIndex(frame.index).hour
    profile = frame.pivot_table(
        index="month", columns="hour", values="ratio", aggfunc="mean"
    )
    profile = profile.reindex(index=range(1, 13), columns=range(24))
    # A month with no calibration data stays NaN: an uncalibrated month is not filled.
    row_mean = profile.mean(axis=1)
    profile = profile.div(row_mean, axis=0)
    profile.index.name = "month"
    profile.columns.name = "utc_hour"
    return profile


def wind_w2_climatology_ms(
    daily: pd.DataFrame,
    local_day: np.ndarray,
    times: pd.DatetimeIndex,
    profile: pd.DataFrame,
) -> np.ndarray:
    """W2: the calibration-period profile applied to the daily mean, mean-preserving.

    Rescaled once per local day, because a local IST day spans two UTC dates and can straddle a
    month boundary; without that step the daily mean would not be preserved exactly.
    """

    day_index = pd.DatetimeIndex(local_day)
    wbar = daily["sfcWind"].reindex(day_index).to_numpy(dtype=float)
    months = day_index.month.to_numpy()
    hours = pd.DatetimeIndex(times).hour.to_numpy()
    values = profile.to_numpy(dtype=float)
    raw = values[months - 1, hours]

    frame = pd.DataFrame({"raw": raw, "day": local_day})
    day_mean = frame.groupby("day")["raw"].transform("mean").to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        scaled = np.where(day_mean > 0.0, raw / day_mean, np.nan)
    return np.clip(wbar * scaled, 0.0, None)


def wind_diagnostics(
    wind: np.ndarray, daily: pd.DataFrame, local_day: np.ndarray
) -> dict[str, float]:
    """Clipping, daily-mean conservation and solver-floor exposure for one wind series."""

    day_index = pd.DatetimeIndex(local_day)
    wbar = daily["sfcWind"].reindex(day_index).to_numpy(dtype=float)
    frame = pd.DataFrame({"w": wind, "target": wbar, "day": local_day})
    got = frame.groupby("day")["w"].mean()
    want = frame.groupby("day")["target"].first()
    err = (got - want).abs()
    finite = np.isfinite(wind)
    return {
        "n_hours": int(len(wind)),
        "n_clipped_at_zero": int(np.sum(finite & (wind <= 0.0))),
        "max_daily_mean_error_ms": float(err.max()) if len(err) else float("nan"),
        "frac_hours_at_or_below_solver_floor": float(
            np.mean(wind[finite] <= THERMOFEEL_MIN_WIND_10M) if finite.any() else np.nan
        ),
        "min_ms": float(np.nanmin(wind)) if finite.any() else float("nan"),
        "max_ms": float(np.nanmax(wind)) if finite.any() else float("nan"),
    }


# ==========================================================================
# CarbonPlan-style comparator (SPEC.md 6)
# ==========================================================================


def carbonplan_adjustment_coefficients() -> np.ndarray:
    """Refit CarbonPlan's notebook-08 OLS locally: ``(const, rsds_max, sfcWind)``.

    Their notebook uses ``statsmodels.OLS`` on the same 16 points; neither ``statsmodels`` nor
    ``sklearn``'s presence changes the fit, so plain least squares reproduces it. Refitting
    rather than hard-coding means the comparator's coefficients are traceable to the published
    source points, not to a transcription.
    """

    x = np.asarray(CARBONPLAN_KONG_HUBER_X, dtype=float)
    y = np.asarray(CARBONPLAN_KONG_HUBER_Y, dtype=float)
    design = np.column_stack([np.ones(len(x)), x])
    beta, *_ = np.linalg.lstsq(design, y, rcond=None)
    return beta


def carbonplan_style_outdoor_c(
    daily: pd.DataFrame, static: m1.SiteStatic, days: pd.DatetimeIndex
) -> tuple[np.ndarray, dict[str, object]]:
    """The CarbonPlan outdoor chain, reproduced as faithfully as local data allows.

    Chain, from notebooks 02, 07 and 08 at revision ``f662b372``:

    1. shade WBGT ``= 0.7*WBT(tasmax, RH-at-tasmax) + 0.2*BGT(tasmax, tasmax, 0.5) + 0.1*tasmax``.
       Milestone-1 evidence (CHG-0554) measured ``BGT(t, t, v) == t`` to 1e-12 K, so this is
       identically ``0.7*Twb_Stull + 0.3*tasmax``, which is what is computed here.
    2. ``rsds_max`` from daily-mean ``rsds`` via a solar-geometry disaggregation, then scaled by
       0.75 (Parsons et al. 2021) for the lag between peak insolation and peak WBGT.
    3. ``rsds_max`` clipped to [300, 900] W/m2 and ``sfcWind`` to [0.5, 3] m/s -- the domain of
       the adjustment model, clipped AFTER the 0.75 factor, in that order, as they do.
    4. ``WBGT_sun = WBGT_shade - (c0 + c1*rsds_max_clipped + c2*sfcWind_clipped)``.

    Deviations, which make this CarbonPlan-**style** and not an exact reproduction, are returned
    alongside the series.
    """

    beta = carbonplan_adjustment_coefficients()
    tasmax = daily["tasmax"].reindex(days).to_numpy(dtype=float)
    tas = daily["tas"].reindex(days).to_numpy(dtype=float)
    hurs = daily["hurs"].reindex(days).to_numpy(dtype=float)
    rsds = daily["rsds"].reindex(days).to_numpy(dtype=float)
    wind = daily["sfcWind"].reindex(days).to_numpy(dtype=float)

    # Step 2: peak hourly rsds from the daily mean, via the TOA interval-mean shape used by the
    # physical candidates, so the two chains share one radiation disaggregation.
    peak = np.empty(len(days), dtype=float)
    for i, day in enumerate(days):
        hours = pd.date_range(day, periods=24, freq="h")
        toa = m1.toa_interval_mean_w_m2(static.lat, static.lon, hours)
        mean_toa = float(toa.mean())
        peak[i] = (rsds[i] / mean_toa * float(toa.max())) if mean_toa > 0.0 else 0.0
    peak = peak * m1.TIER2_PEAK_LAG_FACTOR

    shade = m1.shade_stull_c(tasmax, m1.rh_at_tasmax_pct(tas, tasmax, hurs))
    rsds_clipped = np.clip(peak, *m1.TIER2_RSDS_DOMAIN)
    wind_clipped = np.clip(wind, *m1.TIER2_WIND_DOMAIN)
    adjustment = beta[0] + beta[1] * rsds_clipped + beta[2] * wind_clipped

    provenance = {
        "repo": CARBONPLAN_REPO,
        "revision": CARBONPLAN_REVISION,
        "revision_date": CARBONPLAN_REVISION_DATE,
        "notebooks": list(CARBONPLAN_NOTEBOOKS),
        "coefficients_refit_locally": [float(v) for v in beta],
        "peak_lag_factor": m1.TIER2_PEAK_LAG_FACTOR,
        "rsds_domain_w_m2": list(m1.TIER2_RSDS_DOMAIN),
        "wind_domain_ms": list(m1.TIER2_WIND_DOMAIN),
        "frac_days_rsds_clipped": float(np.mean(peak != rsds_clipped)),
        "frac_days_wind_clipped": float(np.mean(wind != wind_clipped)),
        "mean_adjustment_c": float(np.nanmean(-adjustment)),
        "deviations": [
            "rsds_max disaggregated with the TOA interval-mean shape, not metsim.shortwave "
            "(metsim is not installed and installing it is out of scope)",
            "no QDM bias correction of the shade WBGT against UHE-daily (their notebook 06); "
            "this is the RAW chain, as SPEC.md 6 requires",
            "RH at tasmax derived from hurs directly, not from huss with an "
            "elevation-synthesised surface pressure",
        ],
    }
    return shade - adjustment, provenance


# ==========================================================================
# Alternative reference treatment R3 (SPEC.md 5)
# ==========================================================================

INSTANTANEOUS_DRIVERS = (
    "temperature_2m",
    "relative_humidity_2m",
    "surface_pressure",
    "wind_speed_10m",
)


def interpolate_drivers_to_midpoint(hourly: pd.DataFrame) -> pd.DataFrame:
    """Linearly interpolate the instantaneous drivers to ``H - 30 min``.

    An APPROXIMATION, and labelled as one: the midpoint value of an instantaneous field is not
    the mean of its two neighbours.  Radiation is left untouched because it is already the mean
    over ``[H-1h, H]``, whose representative instant is the midpoint.  The first hour of the
    record has no earlier neighbour and stays NaN rather than being back-filled.
    """

    out = hourly.copy()
    for column in INSTANTANEOUS_DRIVERS:
        current = hourly[column].to_numpy(dtype=float)
        previous = hourly[column].shift(1).to_numpy(dtype=float)
        out[column] = 0.5 * (current + previous)
    return out


def peak_hour_utc(values: np.ndarray, local_day: np.ndarray, times: pd.DatetimeIndex) -> pd.Series:
    """UTC hour at which each local day's maximum occurs; NaN where the day is invalid."""

    frame = pd.DataFrame(
        {"v": np.asarray(values, dtype=float), "day": local_day, "h": pd.DatetimeIndex(times).hour}
    )
    valid = frame.groupby("day")["v"].apply(lambda s: bool(np.isfinite(s).all()))
    picked = frame.loc[frame.groupby("day")["v"].idxmax().dropna()]
    out = pd.Series(picked["h"].to_numpy(dtype=float), index=pd.DatetimeIndex(picked["day"]))
    return out.where(valid.reindex(out.index).to_numpy(dtype=bool))


# ==========================================================================
# Per-site experiment
# ==========================================================================


@dataclass
class SelectionResult:
    """One site-window run: daily maxima per identity plus its diagnostics."""

    site: str
    window: str
    daily: pd.DataFrame
    diagnostics: dict[str, object]


def _calibration_window_for(window: str, windows: Sequence[str]) -> str | None:
    """The other window, which is W2's calibration period for this one (SPEC.md 3.2)."""

    others = [w for w in windows if w != window]
    return others[0] if len(others) == 1 else None


def run_site_window(
    site: Site,
    years: Sequence[int],
    *,
    cache_dir: Path,
    calibration_years: Sequence[int] | None,
    climatology_label: str = "W2",
    day_convention: str = "ist",
    with_reference_sensitivity: bool = False,
    verbose: bool = True,
) -> SelectionResult:
    """Build the audited reference, C1, C2, W1, W2, the two oracles and the comparator."""

    if day_convention not in ("ist", "utc"):
        raise ValueError(f"Unknown day convention: {day_convention!r}")
    static = m1.SiteStatic.from_site(site)
    started = time.monotonic()
    raw = m1.load_hourly_cache(site, years, cache_dir)
    hourly = m1.attach_geometry(site, raw)
    if day_convention == "utc":
        # SPEC.md 8.1: group by the inferred NEX day boundary. No timestamp is rewritten;
        # only the grouping changes, and the tool says so rather than implying interval
        # semantics were established.
        hourly["local_day"] = pd.DatetimeIndex(pd.DatetimeIndex(hourly.index).date)

    keep = m1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    local_day = hourly["local_day"].to_numpy()
    times = pd.DatetimeIndex(hourly.index)

    t_c = hourly["temperature_2m"].to_numpy(dtype=float)
    rh = hourly["relative_humidity_2m"].to_numpy(dtype=float)
    p_hpa = hourly["surface_pressure"].to_numpy(dtype=float)
    wind_actual = hourly["wind_speed_10m"].to_numpy(dtype=float)
    ssrd = hourly["shortwave_radiation"].to_numpy(dtype=float)
    fdir = hourly["fdir_frac"].to_numpy(dtype=float)
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)

    series: dict[str, pd.Series] = {}
    peaks: dict[str, pd.Series] = {}
    diagnostics: dict[str, object] = {
        "site": site.name,
        "regime": site.regime,
        "elevation_m": site.elevation_m,
        "years": [int(y) for y in years],
        "day_convention": day_convention,
        "n_hours": int(len(hourly)),
        "n_complete_days": int(len(keep)),
        "nan_rate": {},
        "wind": {},
    }

    def add(name: str, values: np.ndarray) -> None:
        series[name] = m1.daily_max(values, local_day, keep)
        peaks[name] = peak_hour_utc(values, local_day, times)
        diagnostics["nan_rate"][name] = float(np.mean(~np.isfinite(values)))

    # ---- Reference (carried forward unchanged) ----
    add("ref_audited", m1.liljegren_wbgt_c(t_c, rh, p_hpa, wind_actual, ssrd, fdir, cz_mid))

    # ---- Alternative reference treatment R3 (SPEC.md 5) ----
    if with_reference_sensitivity:
        shifted = interpolate_drivers_to_midpoint(hourly)
        add(
            "ref_R3_midpoint",
            m1.liljegren_wbgt_c(
                shifted["temperature_2m"].to_numpy(dtype=float),
                shifted["relative_humidity_2m"].to_numpy(dtype=float),
                shifted["surface_pressure"].to_numpy(dtype=float),
                shifted["wind_speed_10m"].to_numpy(dtype=float),
                ssrd,
                fdir,
                cz_mid,
            ),
        )
        diagnostics["ref_R3_signature"] = REFERENCE_SIGNATURE_MIDPOINT

    # ---- Daily inputs: the only weather a deployable candidate may read ----
    daily_inputs = m1.aggregate_daily_inputs(hourly)
    daily = daily_inputs.frame

    # ---- W2's calibration profile, from the disjoint period only ----
    profile: pd.DataFrame | None = None
    if calibration_years:
        cal_raw = m1.load_hourly_cache(site, calibration_years, cache_dir)
        cal_hourly = m1.attach_geometry(site, cal_raw)
        overlap = sorted(set(int(y) for y in calibration_years) & set(int(y) for y in years))
        if overlap:
            raise ValueError(
                f"W2 calibration period overlaps the evaluation window at years {overlap}; "
                "SPEC.md 3.2 forbids deriving a test year's profile from that year"
            )
        profile = wind_climatology_profile(cal_hourly)
        diagnostics["w2_label"] = climatology_label
        diagnostics["w2_calibration_years"] = [int(y) for y in calibration_years]
        diagnostics["w2_profile_min"] = float(np.nanmin(profile.to_numpy()))
        diagnostics["w2_profile_max"] = float(np.nanmax(profile.to_numpy()))
        diagnostics["w2_profile_months_uncalibrated"] = int(
            profile.isna().all(axis=1).sum()
        )

    # ---- Candidates: identical to C1 in every driver except the within-day wind ----
    base = m1.reconstruct_hourly(
        times, local_day, daily_inputs, static, m1.CANDIDATE_PARAMS["C1"], cossza=cz_mid
    )
    winds: dict[str, np.ndarray] = {
        "C1": base["wind_10m_ms"].to_numpy(dtype=float),
        "C2": m1.reconstruct_wind_10m_ms(
            daily, local_day, cz_mid, m1.CANDIDATE_PARAMS["C2"]
        ),
        "W1": wind_w1_dtr_ms(daily, local_day, cz_mid),
    }
    if profile is not None:
        # The label distinguishes the forward-calibrated primary run from the
        # reverse-calibrated diagnostic one, which is not eligible for selection (SPEC.md 3.2).
        winds[climatology_label] = wind_w2_climatology_ms(daily, local_day, times, profile)

    for cid, wind in winds.items():
        diagnostics["wind"][cid] = wind_diagnostics(wind, daily, local_day)
        add(
            f"cand_{cid}",
            m1.liljegren_wbgt_c(
                base["t_c"].to_numpy(),
                base["rh_pct"].to_numpy(),
                base["pressure_hpa"].to_numpy(),
                wind,
                base["ssrd_w_m2"].to_numpy(),
                base["fdir_frac"].to_numpy(),
                cz_mid,
            ),
        )
    diagnostics["wind"]["ACTUAL"] = wind_diagnostics(wind_actual, daily, local_day)

    # ---- Oracle A5: reconstructed drivers with ACTUAL hourly wind. Diagnostic only. ----
    add(
        "abl_A5",
        m1.liljegren_wbgt_c(
            base["t_c"].to_numpy(),
            base["rh_pct"].to_numpy(),
            base["pressure_hpa"].to_numpy(),
            wind_actual,
            base["ssrd_w_m2"].to_numpy(),
            base["fdir_frac"].to_numpy(),
            cz_mid,
        ),
    )
    # SPEC.md 4 allows A6 (C2's drivers with actual wind) "only if it provides additional
    # information". It cannot: C1 and C2 differ ONLY in their wind shape, and the oracle
    # replaces exactly that, so A6 is A5 element for element. It is therefore not computed,
    # and the reason is recorded rather than the column silently omitted.
    diagnostics["a6"] = (
        "NOT COMPUTED: identical to A5 by construction, since C1 and C2 differ only in the "
        "within-day wind shape that the oracle replaces with the actual hourly wind"
    )

    # ---- CarbonPlan-style comparator ----
    cp, cp_provenance = carbonplan_style_outdoor_c(daily, static, keep)
    series["cp_style_outdoor"] = pd.Series(cp, index=keep)
    diagnostics["carbonplan"] = cp_provenance

    diagnostics["wall_seconds"] = round(time.monotonic() - started, 2)
    if verbose:
        print(
            f"    {site.name} {min(years)}-{max(years)} [{day_convention}]: "
            f"{len(keep)} complete days, {diagnostics['wall_seconds']:.1f} s"
        )
    frame = pd.DataFrame(series)
    frame.index = pd.DatetimeIndex(frame.index, name="local_day")
    peak_frame = pd.DataFrame(peaks)
    peak_frame.index = pd.DatetimeIndex(peak_frame.index, name="local_day")
    diagnostics["peak_hour_mean_utc"] = {
        c: float(peak_frame[c].mean()) for c in peak_frame.columns
    }
    diagnostics["peak_hour_frame"] = peak_frame
    return SelectionResult(site.name, f"{min(years)}-{max(years)}", frame, diagnostics)


# ==========================================================================
# Per-year count errors (SPEC.md 2.3)
# ==========================================================================


def matched_complete_years(reference: pd.Series, candidate: pd.Series) -> list[int]:
    """Years complete in BOTH series.

    Milestone 1's candidates were valid on exactly the reference's days, so intersecting made no
    difference there. W2 can legitimately be missing an uncalibrated month, and every
    reconstruction is NaN at the record boundary. Counting a candidate over a year it does not
    fully cover would score its missing days as non-exceedances -- turning invalid input into a
    zero-exceedance day, which is precisely what the contract forbids. So the intersection is
    taken, and the per-series year counts are reported so the shrinkage is visible.
    """

    return sorted(set(m1.complete_years(reference)) & set(m1.complete_years(candidate)))


def build_annual_table(results: Sequence[SelectionResult]) -> pd.DataFrame:
    """Annual means, counts, count errors and the carried-forward count gate.

    Same statistics and the same imported gate as milestone 1's ``build_annual_table``, but
    evaluated over years complete in both the reference and the candidate (see
    ``matched_complete_years``) rather than over the reference's complete years alone.
    """

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        reference = daily["ref_audited"]
        ref_years = m1.complete_years(reference)
        for column in daily.columns:
            if column == "ref_audited":
                continue
            years = matched_complete_years(reference, daily[column])
            base = {
                "site": result.site,
                "window": result.window,
                "candidate": column,
                "n_complete_years_reference": len(ref_years),
                "n_complete_years_candidate": len(m1.complete_years(daily[column])),
                "n_complete_years": len(years),
                "years": ",".join(str(y) for y in years),
            }
            if not years:
                rows.append(
                    {
                        **base,
                        "threshold_c": np.nan,
                        "statistic": "annual_mean_c",
                        "reference_per_year": np.nan,
                        "candidate_per_year": np.nan,
                        "signed_error": np.nan,
                        "absolute_error": np.nan,
                        "relative_error": np.nan,
                        "count_gate": "NOT EVALUATED (no matched complete year)",
                        "count_gate_tolerance_per_year": np.nan,
                    }
                )
                continue
            ref_annual = m1.annual_counts(reference, years)
            cand_annual = m1.annual_counts(daily[column], years)
            ref_mean = float(ref_annual["annual_mean_c"].mean())
            cand_mean = float(cand_annual["annual_mean_c"].mean())
            rows.append(
                {
                    **base,
                    "threshold_c": np.nan,
                    "statistic": "annual_mean_c",
                    "reference_per_year": ref_mean,
                    "candidate_per_year": cand_mean,
                    "signed_error": cand_mean - ref_mean,
                    "absolute_error": abs(cand_mean - ref_mean),
                    "relative_error": np.nan,
                    "count_gate": "",
                    "count_gate_tolerance_per_year": np.nan,
                }
            )
            for threshold in m1.THRESHOLDS_C:
                col = f"days_ge_{threshold:g}"
                ref_per_year = float(ref_annual[col].mean())
                cand_per_year = float(cand_annual[col].mean())
                verdict, tol = m1.count_gate(ref_per_year, cand_per_year)
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
                            if ref_per_year > 0
                            else np.nan
                        ),
                        "count_gate": verdict,
                        "count_gate_tolerance_per_year": tol,
                    }
                )
    return pd.DataFrame(rows)


def per_year_count_errors(results: Sequence[SelectionResult]) -> pd.DataFrame:
    """Individual complete-year count errors, so a cancelling mean-level pass is visible."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        for column in daily.columns:
            if column == "ref_audited":
                continue
            years = matched_complete_years(daily["ref_audited"], daily[column])
            if not years:
                continue
            ref = m1.annual_counts(daily["ref_audited"], years)
            cand = m1.annual_counts(daily[column], years)
            for threshold in m1.THRESHOLDS_C:
                col = f"days_ge_{threshold:g}"
                ref_mean = float(ref[col].mean())
                verdict, tol = m1.count_gate(ref_mean, float(cand[col].mean()))
                errors = (cand[col] - ref[col]).to_numpy(dtype=float)
                rows.append(
                    {
                        "site": result.site,
                        "window": result.window,
                        "candidate": column,
                        "threshold_c": threshold,
                        "n_complete_years": len(years),
                        "mean_gate": verdict,
                        "gate_tolerance_per_year": tol,
                        "reference_mean_per_year": ref_mean,
                        "mean_signed_error": float(np.mean(errors)),
                        "mean_absolute_error": float(np.mean(np.abs(errors))),
                        "min_signed_error": float(np.min(errors)),
                        "max_signed_error": float(np.max(errors)),
                        "n_years_outside_tolerance": (
                            int(np.sum(np.abs(errors) > tol)) if np.isfinite(tol) else -1
                        ),
                    }
                )
    return pd.DataFrame(rows)


def wind_attribution_table(results: Sequence[SelectionResult]) -> pd.DataFrame:
    """A5/A6 versus C1/C2: what a perfect wind would and would not fix (SPEC.md 4)."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        for column in ("cand_C1", "cand_C2", "cand_W1", "cand_W2", "cand_W2rev", "abl_A5"):
            if column not in daily:
                continue
            years = matched_complete_years(daily["ref_audited"], daily[column])
            ref = m1.annual_counts(daily["ref_audited"], years) if years else None
            scored = m1.score_pair(
                daily["ref_audited"],
                daily[column],
                site=result.site,
                window=result.window,
                season="ALL",
                reference_name="ref_audited",
                candidate_name=column,
                kind="oracle ablation" if column.startswith("abl_") else "deployable candidate",
            )
            if ref is not None:
                cand = m1.annual_counts(daily[column], years)
                for threshold in m1.THRESHOLDS_C:
                    col = f"days_ge_{threshold:g}"
                    scored[f"count_error_{threshold:g}"] = float(
                        cand[col].mean() - ref[col].mean()
                    )
                    scored[f"count_gate_{threshold:g}"] = m1.count_gate(
                        float(ref[col].mean()), float(cand[col].mean())
                    )[0]
            rows.append(scored)
    return pd.DataFrame(rows)


def _peak_hour_frame(result: SelectionResult) -> pd.DataFrame | None:
    """Peak-hour table for one result, from memory or from a resumed cache.

    A freshly computed result carries the frame directly; a result reloaded from the work-dir
    cache carries the same data as plain lists, because a DataFrame is not JSON.  Peak timing is
    a required output of SPEC.md 5, so it is recovered rather than quietly dropped on resume.
    """

    frame = result.diagnostics.get("peak_hour_frame")
    if isinstance(frame, pd.DataFrame):
        return frame
    records = result.diagnostics.get("peak_hour_records")
    if isinstance(records, Mapping) and records:
        out = pd.DataFrame({k: list(v) for k, v in records.items()})
        out.index = pd.DatetimeIndex(result.daily.index[: len(out)], name="local_day")
        return out
    return None


def reference_sensitivity_table(results: Sequence[SelectionResult]) -> pd.DataFrame:
    """R3 versus ref_audited: daily maxima, peak timing, counts and gate outcomes."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        if "ref_R3_midpoint" not in daily:
            continue
        peaks = _peak_hour_frame(result)
        years = m1.complete_years(daily["ref_audited"])
        row = m1.score_pair(
            daily["ref_audited"],
            daily["ref_R3_midpoint"],
            site=result.site,
            window=result.window,
            season="ALL",
            reference_name="ref_audited",
            candidate_name="ref_R3_midpoint",
            kind="reference sensitivity",
        )
        if isinstance(peaks, pd.DataFrame) and "ref_R3_midpoint" in peaks:
            row["mean_peak_hour_audited_utc"] = float(peaks["ref_audited"].mean())
            row["mean_peak_hour_R3_utc"] = float(peaks["ref_R3_midpoint"].mean())
            row["frac_days_peak_hour_differs"] = float(
                (peaks["ref_audited"] != peaks["ref_R3_midpoint"]).mean()
            )
        rows.append(row)
        if not years:
            continue
        # Gate every candidate against BOTH references and record whether the verdict moves.
        audited = m1.annual_counts(daily["ref_audited"], years)
        r3 = m1.annual_counts(daily["ref_R3_midpoint"], years)
        for column in daily.columns:
            if not column.startswith("cand_"):
                continue
            cand = m1.annual_counts(daily[column], years)
            for threshold in m1.THRESHOLDS_C:
                col = f"days_ge_{threshold:g}"
                v_audited, _ = m1.count_gate(float(audited[col].mean()), float(cand[col].mean()))
                v_r3, _ = m1.count_gate(float(r3[col].mean()), float(cand[col].mean()))
                d_audited = m1.score_pair(
                    daily["ref_audited"], daily[column], site=result.site,
                    window=result.window, season="ALL", reference_name="ref_audited",
                    candidate_name=column, kind="x",
                ).get("daily_gate")
                d_r3 = m1.score_pair(
                    daily["ref_R3_midpoint"], daily[column], site=result.site,
                    window=result.window, season="ALL", reference_name="ref_R3_midpoint",
                    candidate_name=column, kind="x",
                ).get("daily_gate")
                rows.append(
                    {
                        "site": result.site,
                        "window": result.window,
                        "candidate": column,
                        "kind": "gate under both references",
                        "threshold_c": threshold,
                        "reference_per_year_audited": float(audited[col].mean()),
                        "reference_per_year_R3": float(r3[col].mean()),
                        "candidate_per_year": float(cand[col].mean()),
                        "count_gate_audited": v_audited,
                        "count_gate_R3": v_r3,
                        "count_gate_verdict_changed": v_audited != v_r3,
                        "daily_gate_audited": d_audited,
                        "daily_gate_R3": d_r3,
                        "daily_gate_verdict_changed": d_audited != d_r3,
                    }
                )
    return pd.DataFrame(rows)


# ==========================================================================
# Selection rule (SPEC.md 7)
# ==========================================================================


def apply_selection_rule(
    scores: pd.DataFrame, annual: pd.DataFrame, per_year: pd.DataFrame
) -> dict[str, object]:
    """Rank deployable candidates by the predeclared rule. Absent evidence stays absent."""

    ranked: list[dict[str, object]] = []
    for cid in DEPLOYABLE_IDS:
        column = f"cand_{cid}"
        daily = scores[(scores["candidate"] == column) & (scores["season"] == "ALL")]
        counts = annual[(annual["candidate"] == column) & annual["threshold_c"].notna()]
        gated = counts[counts["count_gate"].isin(["PASS", "FAIL"])]
        years_out = per_year[
            (per_year["candidate"] == column) & (per_year["mean_gate"] != "NOT GATED (rare event)")
        ]
        if daily.empty:
            ranked.append({"candidate": cid, "status": "NOT EVALUATED"})
            continue
        failing = gated[gated["count_gate"] == "FAIL"]
        entry = {
            "candidate": cid,
            "name": CANDIDATE_NAMES[cid],
            "needs_climatology_asset": NEEDS_CLIMATOLOGY_ASSET[cid],
            "daily_pairs": int(len(daily)),
            "daily_pairs_passing": int((daily["daily_gate"] == "PASS").sum()),
            "daily_gate": "PASS" if (daily["daily_gate"] == "PASS").all() else "FAIL",
            "worst_median_abs_error_c": float(daily["median_abs_error_c"].max()),
            "worst_rmse_c": float(daily["rmse_c"].max()),
            "count_pairs_gated": int(len(gated)),
            "count_pairs_failing": int(len(failing)),
            "count_gate": (
                "NOT EVALUATED" if gated.empty else "FAIL" if len(failing) else "PASS"
            ),
            "worst_count_error_per_year": (
                float(gated["absolute_error"].max()) if not gated.empty else np.nan
            ),
            "n_individual_years_outside_tolerance": (
                int(years_out["n_years_outside_tolerance"].clip(lower=0).sum())
                if not years_out.empty
                else -1
            ),
        }
        ranked.append(entry)

    evaluated = [r for r in ranked if r.get("status") != "NOT EVALUATED"]
    survivors = [r for r in evaluated if r["daily_gate"] == "PASS"]
    eliminated = [
        {"candidate": r["candidate"], "reason": "failed the daily gate"}
        for r in evaluated
        if r["daily_gate"] != "PASS"
    ]

    def rank_key(entry: Mapping[str, object]) -> tuple:
        return (
            int(entry["count_pairs_failing"]),
            float(entry["worst_count_error_per_year"]),
            int(entry["n_individual_years_outside_tolerance"]),
            bool(entry["needs_climatology_asset"]),
            DEPLOYABLE_IDS.index(str(entry["candidate"])),
        )

    survivors_sorted = sorted(survivors, key=rank_key)
    passing_all = [r for r in survivors_sorted if r["count_gate"] == "PASS"]
    if passing_all:
        # Step 3/4: among full passes, prefer no-asset, then the simpler (earlier) candidate.
        best = sorted(
            passing_all,
            key=lambda r: (bool(r["needs_climatology_asset"]), DEPLOYABLE_IDS.index(str(r["candidate"]))),
        )[0]
        selected, status = best["candidate"], "SELECTED (both gates pass)"
    elif survivors_sorted:
        selected = survivors_sorted[0]["candidate"]
        status = "BEST DEVELOPMENT CANDIDATE (count gate not passed)"
    else:
        selected, status = None, "NONE (no candidate passed the daily gate)"

    return {
        "rule": "SPEC.md 7, predeclared",
        "selected": selected,
        "status": status,
        "ranking": survivors_sorted,
        "eliminated": eliminated,
        "all_candidates": ranked,
    }


# ==========================================================================
# Matched-period NEX comparison (SPEC.md 8)
# ==========================================================================


def nex_candidate_daily_max(
    sample: pd.DataFrame,
    sites: Sequence[Site],
    candidate: str,
    profiles: Mapping[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Run one candidate on NEX daily inputs. A DISTRIBUTION comparison only.

    A NEX day is not an IST civil day and NEX carries no weather for a particular ERA5 date, so
    no same-date difference or correlation against ERA5 is computed anywhere in this function.
    """

    if sample.empty:
        return pd.DataFrame()
    by_name = {s.name: s for s in sites}
    frames: list[pd.DataFrame] = []
    for (model, scenario, site_name), block in sample.groupby(["model", "scenario", "site"]):
        site = by_name[site_name]
        static = m1.SiteStatic.from_site(site)
        daily = block[list(m1.REQUIRED_NEX_VARIABLES)].copy()
        daily.index = pd.DatetimeIndex(daily.index).normalize()
        daily = daily[~daily.index.duplicated(keep="first")].sort_index()
        inputs = m1.DailyInputs(daily)

        times = pd.DatetimeIndex(
            np.concatenate(
                [pd.date_range(day, periods=24, freq="h").to_numpy() for day in daily.index]
            )
        )
        local_day = pd.DatetimeIndex(times.normalize()).to_numpy()
        cz_mid = cos_solar_zenith(site.lat, times + m1.RADIATION_MIDPOINT_OFFSET, site.lon)
        base = m1.reconstruct_hourly(
            times, local_day, inputs, static, m1.CANDIDATE_PARAMS["C1"], cossza=cz_mid
        )
        if candidate == "C1":
            wind = base["wind_10m_ms"].to_numpy(dtype=float)
        elif candidate == "C2":
            wind = m1.reconstruct_wind_10m_ms(
                daily, local_day, cz_mid, m1.CANDIDATE_PARAMS["C2"]
            )
        elif candidate == "W1":
            wind = wind_w1_dtr_ms(daily, local_day, cz_mid)
        elif candidate == "W2":
            if not profiles or site_name not in profiles:
                raise ValueError(f"W2 on NEX needs a calibration profile for {site_name}")
            wind = wind_w2_climatology_ms(daily, local_day, times, profiles[site_name])
        else:
            raise ValueError(f"Unknown candidate for NEX: {candidate!r}")

        wbgt = m1.liljegren_wbgt_c(
            base["t_c"].to_numpy(),
            base["rh_pct"].to_numpy(),
            base["pressure_hpa"].to_numpy(),
            wind,
            base["ssrd_w_m2"].to_numpy(),
            base["fdir_frac"].to_numpy(),
            cz_mid,
        )
        frame = m1.daily_max(wbgt, local_day, daily.index).to_frame("wbgt_daily_max_c")
        frame.insert(0, "model", model)
        frame.insert(1, "scenario", scenario)
        frame.insert(2, "site", site_name)
        frames.append(frame)
    out = pd.concat(frames)
    out.index.name = "nex_day"
    return out


def _distribution_row(series: pd.Series, years: Sequence[int]) -> dict[str, float]:
    counts = m1.annual_counts(series, years)
    row: dict[str, float] = {
        "n_complete_years": float(len(years)),
        "annual_mean_c": float(counts["annual_mean_c"].mean()) if len(years) else np.nan,
    }
    for threshold in m1.THRESHOLDS_C:
        row[f"days_ge_{threshold:g}_per_year"] = (
            float(counts[f"days_ge_{threshold:g}"].mean()) if len(years) else np.nan
        )
    clean = series.dropna()
    for q in (0.50, 0.90, 0.99):
        row[f"q{int(q * 100)}_c"] = float(clean.quantile(q)) if len(clean) else np.nan
    for season, months in m1.SEASONS.items():
        mask = pd.DatetimeIndex(clean.index).month.isin(months)
        row[f"{season}_mean_c"] = float(clean[mask].mean()) if mask.any() else np.nan
    return row


def nex_matched_comparison(
    nex_daily: pd.DataFrame,
    era5_results: Sequence[SelectionResult],
    candidate: str,
    years: Sequence[int],
) -> pd.DataFrame:
    """Per-model distribution and annual-count comparison against the ERA5-driven reference."""

    if nex_daily.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    era5_by_site: dict[str, pd.Series] = {}
    for result in era5_results:
        ref = result.daily["ref_audited"]
        ref = ref[pd.DatetimeIndex(ref.index).year.isin(list(years))]
        era5_by_site[result.site] = (
            pd.concat([era5_by_site[result.site], ref]).sort_index()
            if result.site in era5_by_site
            else ref
        )

    for site, ref in era5_by_site.items():
        ref_years = m1.complete_years(ref)
        base = {"site": site, "candidate": candidate, "years_requested": f"{min(years)}-{max(years)}"}
        reference_row = _distribution_row(ref, ref_years)
        rows.append({**base, "source": "ERA5 ref_audited (IST day)", "model": "", **reference_row})
        for model, block in nex_daily[nex_daily["site"] == site].groupby("model"):
            series = block["wbgt_daily_max_c"]
            nex_years = m1.complete_years(series)
            nex_row = _distribution_row(series, nex_years)
            entry = {**base, "source": f"NEX {candidate} (inferred NEX day)", "model": model, **nex_row}
            for threshold in m1.THRESHOLDS_C:
                key = f"days_ge_{threshold:g}_per_year"
                ref_value, nex_value = reference_row[key], nex_row[key]
                entry[f"signed_error_{threshold:g}"] = nex_value - ref_value
                entry[f"relative_error_{threshold:g}"] = (
                    (nex_value - ref_value) / ref_value if ref_value > 0 else np.nan
                )
            entry["annual_mean_signed_error_c"] = (
                nex_row["annual_mean_c"] - reference_row["annual_mean_c"]
            )
            rows.append(entry)
    return pd.DataFrame(rows)


def qdm_trigger(comparison: pd.DataFrame) -> dict[str, object]:
    """Evaluate the predeclared bias-correction trigger (SPEC.md 9)."""

    if comparison.empty:
        return {"fired": False, "reason": "NOT EVALUATED: no matched-period comparison"}
    nex = comparison[comparison["model"] != ""]
    key_rel = f"relative_error_{QDM_TRIGGER_THRESHOLD_C:g}"
    key_abs = f"signed_error_{QDM_TRIGGER_THRESHOLD_C:g}"
    details: list[dict[str, object]] = []
    fired = False
    for model, block in nex.groupby("model"):
        over_rel = block[block[key_rel].abs() > QDM_TRIGGER_REL_TOL]
        over_abs = block[block[key_abs].abs() > QDM_TRIGGER_ABS_DAYS]
        model_fired = (
            len(over_rel) >= QDM_TRIGGER_MIN_SITES or len(over_abs) >= 1
        )
        fired = fired or model_fired
        details.append(
            {
                "model": model,
                "sites_over_relative_tolerance": sorted(over_rel["site"].tolist()),
                "sites_over_absolute_tolerance": sorted(over_abs["site"].tolist()),
                "fired": bool(model_fired),
            }
        )
    return {
        "fired": bool(fired),
        "threshold_c": QDM_TRIGGER_THRESHOLD_C,
        "relative_tolerance": QDM_TRIGGER_REL_TOL,
        "min_sites": QDM_TRIGGER_MIN_SITES,
        "absolute_tolerance_days_per_year": QDM_TRIGGER_ABS_DAYS,
        "per_model": details,
    }


# ==========================================================================
# Conditional QDM trial (SPEC.md 9)
# ==========================================================================


def qdm_trial(
    nex_daily: pd.DataFrame,
    era5_reference: Mapping[str, pd.Series],
    *,
    train_years: Sequence[int] = QDM_TRAIN_YEARS,
    test_years: Sequence[int] = QDM_TEST_YEARS,
    future: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """One additive-QDM configuration on the derived WBGT, trained and applied disjointly.

    Trained on ``train_years`` only; applied to ``test_years`` and, unchanged, to the future
    sample.  Training years never appear in the evaluation, which is asserted here rather than
    assumed.
    """

    from tools.diagnostics.wbgt_qdm_bias_correction import (
        pseudo_doy,
        qdm_adjust,
        quantile_table,
    )

    overlap = sorted(set(int(y) for y in train_years) & set(int(y) for y in test_years))
    if overlap:
        raise ValueError(f"QDM training and evaluation years overlap at {overlap}")

    def doys(index: pd.DatetimeIndex) -> np.ndarray:
        idx = pd.DatetimeIndex(index)
        return pseudo_doy(idx.month.to_numpy(), idx.day.to_numpy())

    rows: list[dict[str, object]] = []
    for (model, site), block in nex_daily.groupby(["model", "site"]):
        if site not in era5_reference:
            continue
        ref = era5_reference[site].dropna()
        ref_train = ref[pd.DatetimeIndex(ref.index).year.isin(list(train_years))]
        series = block["wbgt_daily_max_c"]
        mod_train = series[pd.DatetimeIndex(series.index).year.isin(list(train_years))].dropna()
        mod_test = series[pd.DatetimeIndex(series.index).year.isin(list(test_years))]
        if ref_train.empty or mod_train.empty or mod_test.dropna().empty:
            rows.append(
                {
                    "model": model, "site": site, "split": "held-out",
                    "status": "NOT EVALUATED: insufficient training or evaluation data",
                }
            )
            continue

        q_ref = quantile_table(
            ref_train.to_numpy(dtype=float), doys(ref_train.index),
            nquantiles=QDM_NQUANTILES, window=QDM_WINDOW_DAYS,
        )
        q_hist = quantile_table(
            mod_train.to_numpy(dtype=float), doys(mod_train.index),
            nquantiles=QDM_NQUANTILES, window=QDM_WINDOW_DAYS,
        )

        for label, target in (("held-out", mod_test), ("future", None if future is None else None)):
            if target is None:
                continue
            values = target.to_numpy(dtype=float)
            corrected = qdm_adjust(
                values, doys(target.index), q_ref=q_ref, q_hist=q_hist,
                nquantiles=QDM_NQUANTILES, window=QDM_WINDOW_DAYS,
            )
            raw_series = pd.Series(values, index=target.index)
            cor_series = pd.Series(corrected, index=target.index)
            ref_eval = ref[pd.DatetimeIndex(ref.index).year.isin(list(test_years))]
            ref_years = m1.complete_years(ref_eval)
            row: dict[str, object] = {
                "model": model, "site": site, "split": label,
                "train_years": f"{min(train_years)}-{max(train_years)}",
                "eval_years": f"{min(test_years)}-{max(test_years)}",
                "n_eval_days": int(len(values)),
                "n_uncorrectable_nan": int(
                    np.sum(np.isfinite(values) & ~np.isfinite(corrected))
                ),
                "raw_min_c": float(np.nanmin(values)),
                "raw_max_c": float(np.nanmax(values)),
                "corrected_min_c": float(np.nanmin(corrected)),
                "corrected_max_c": float(np.nanmax(corrected)),
                "train_range_max_c": float(np.nanmax(mod_train.to_numpy(dtype=float))),
                "n_eval_above_train_range": int(
                    np.sum(values > np.nanmax(mod_train.to_numpy(dtype=float)))
                ),
            }
            ref_dist = _distribution_row(ref_eval, ref_years)
            for tag, s in (("raw", raw_series), ("corrected", cor_series)):
                dist = _distribution_row(s, m1.complete_years(s))
                row[f"{tag}_annual_mean_c"] = dist["annual_mean_c"]
                for threshold in m1.THRESHOLDS_C:
                    key = f"days_ge_{threshold:g}_per_year"
                    row[f"{tag}_{key}"] = dist[key]
                    row[f"{tag}_error_{threshold:g}"] = dist[key] - ref_dist[key]
                for season in m1.SEASONS:
                    row[f"{tag}_{season}_mean_c"] = dist[f"{season}_mean_c"]
            row["reference_annual_mean_c"] = ref_dist["annual_mean_c"]
            for threshold in m1.THRESHOLDS_C:
                row[f"reference_days_ge_{threshold:g}_per_year"] = ref_dist[
                    f"days_ge_{threshold:g}_per_year"
                ]
            rows.append(row)

        if future is not None and not future.empty:
            block_future = future[(future["model"] == model) & (future["site"] == site)]
            if not block_future.empty:
                fut = block_future["wbgt_daily_max_c"]
                fut_corrected = qdm_adjust(
                    fut.to_numpy(dtype=float), doys(fut.index), q_ref=q_ref, q_hist=q_hist,
                    nquantiles=QDM_NQUANTILES, window=QDM_WINDOW_DAYS,
                )
                raw_hist = mod_train.to_numpy(dtype=float)
                cor_hist = qdm_adjust(
                    raw_hist, doys(mod_train.index), q_ref=q_ref, q_hist=q_hist,
                    nquantiles=QDM_NQUANTILES, window=QDM_WINDOW_DAYS,
                )
                entry: dict[str, object] = {
                    "model": model, "site": site, "split": "future signal",
                    "future_years": f"{min(QDM_FUTURE_YEARS)}-{max(QDM_FUTURE_YEARS)}",
                    "scenario": QDM_FUTURE_SCENARIO,
                    "n_future_days": int(len(fut)),
                    "n_uncorrectable_nan": int(
                        np.sum(np.isfinite(fut.to_numpy(dtype=float)) & ~np.isfinite(fut_corrected))
                    ),
                }
                for q in (0.50, 0.90, 0.99):
                    raw_delta = float(
                        np.nanquantile(fut.to_numpy(dtype=float), q) - np.nanquantile(raw_hist, q)
                    )
                    cor_delta = float(
                        np.nanquantile(fut_corrected, q) - np.nanquantile(cor_hist, q)
                    )
                    entry[f"raw_future_minus_hist_q{int(q * 100)}_c"] = raw_delta
                    entry[f"corrected_future_minus_hist_q{int(q * 100)}_c"] = cor_delta
                    entry[f"signal_change_q{int(q * 100)}_c"] = cor_delta - raw_delta
                rows.append(entry)
    return pd.DataFrame(rows)


# ==========================================================================
# Run manifest and write guards
# ==========================================================================


def _guard_write_target(path: Path, label: str) -> Path:
    resolved = path.resolve()
    lowered = str(resolved).replace("\\", "/").lower()
    for fragment in m1.FORBIDDEN_WRITE_FRAGMENTS:
        if fragment in lowered:
            raise SystemExit(
                f"Refusing to use {label} {resolved}: it resolves inside '{fragment}', which "
                "this milestone must not write to (SPEC.md 11)."
            )
    milestone1 = (Path("docs/diagnostics/wbgt_outdoor_feasibility")).resolve()
    if resolved == milestone1 or milestone1 in resolved.parents:
        raise SystemExit(
            f"Refusing to use {label} {resolved}: milestone 1's evidence is immutable "
            "(SPEC.md 11)."
        )
    return resolved


def run_manifest(args: argparse.Namespace, extra: Mapping[str, object]) -> dict[str, object]:
    """Git snapshot, versions, frozen settings and the exact command."""

    import subprocess

    def git(*cmd: str) -> str:
        try:
            return subprocess.run(
                ["git", *cmd], capture_output=True, text=True, check=True, timeout=30
            ).stdout.strip()
        except Exception:  # pragma: no cover - git absent or detached
            return "unavailable"

    import thermofeel

    return {
        "tool": "tools/diagnostics/wbgt_outdoor_selection.py",
        "spec": "docs/diagnostics/wbgt_outdoor_selection/SPEC.md",
        "milestone_1_spec": "docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md",
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_sha": git("rev-parse", "--short", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "command": " ".join(sys.argv),
        "python": sys.version.split()[0],
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "thermofeel": getattr(thermofeel, "__version__", "unknown"),
        "reference_signatures": {
            **m1.REFERENCE_SIGNATURES,
            REFERENCE_MIDPOINT: REFERENCE_SIGNATURE_MIDPOINT,
        },
        "gates": {
            "median_abs_error_c": m1.GATE_MEDIAN_ABS_ERROR_C,
            "rmse_c": m1.GATE_RMSE_C,
            "count_min_ref_per_year": m1.COUNT_GATE_MIN_REF_PER_YEAR,
            "count_rel_tol": m1.COUNT_GATE_REL_TOL,
            "count_abs_floor_per_year": m1.COUNT_GATE_ABS_FLOOR_PER_YEAR,
            "source": "imported from wbgt_outdoor_feasibility, unchanged",
        },
        "w1": {
            "slope_per_c": WIND_DTR_SLOPE_PER_C,
            "amplitude_bounds": [WIND_DTR_AMP_MIN, WIND_DTR_AMP_MAX],
            "hinge_dtr_c": WIND_DTR_HINGE_C,
            "status": "declared assumption, not a published coefficient",
        },
        "w2": {
            "primary_calibration": W2_PRIMARY_CALIBRATION,
            "primary_evaluation": W2_PRIMARY_EVALUATION,
            "needs_spatial_climatology_for_national_use": True,
        },
        "carbonplan": {
            "repo": CARBONPLAN_REPO,
            "revision": CARBONPLAN_REVISION,
            "revision_date": CARBONPLAN_REVISION_DATE,
            "notebooks": list(CARBONPLAN_NOTEBOOKS),
            "coefficients": [float(v) for v in carbonplan_adjustment_coefficients()],
        },
        "qdm": {
            "nquantiles": QDM_NQUANTILES,
            "window_days": QDM_WINDOW_DAYS,
            "train_years": [min(QDM_TRAIN_YEARS), max(QDM_TRAIN_YEARS)],
            "test_years": [min(QDM_TEST_YEARS), max(QDM_TEST_YEARS)],
            "kind": "additive, on the derived WBGT",
            "implementation": "tools/diagnostics/wbgt_qdm_bias_correction.py",
        },
        **dict(extra),
    }


# ==========================================================================
# Self-test
# ==========================================================================


def _synthetic_site() -> Site:
    return SITES[0]


def self_test() -> int:
    """Contract checks that need no cache, no network and no NEX tree."""

    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'ok  ' if ok else 'FAIL'} {name}" + (f" -- {detail}" if detail else ""))
        if not ok:
            failures.append(name)

    site = _synthetic_site()
    hourly = m1.attach_geometry(site, m1._synthetic_hourly(site, days=60, seed=11))
    keep = m1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    local_day = hourly["local_day"].to_numpy()
    times = pd.DatetimeIndex(hourly.index)
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)
    daily = m1.aggregate_daily_inputs(hourly).frame

    # 1. The shared shape reproduces milestone 1's C2 exactly.
    c2_m1 = m1.reconstruct_wind_10m_ms(daily, local_day, cz_mid, m1.CANDIDATE_PARAMS["C2"])
    wbar = daily["sfcWind"].reindex(pd.DatetimeIndex(local_day)).to_numpy(dtype=float)
    c2_here = np.clip(
        wbar * (1.0 + m1.WIND_DIURNAL_AMPLITUDE * zero_mean_cz_shape(cz_mid, local_day)),
        0.0,
        None,
    )
    check(
        "shared shape reproduces milestone 1 C2",
        float(np.nanmax(np.abs(c2_here - c2_m1))) < 1e-12,
        f"max diff {float(np.nanmax(np.abs(c2_here - c2_m1))):.2e}",
    )

    # 2. W1 is non-negative and preserves the daily mean.
    w1 = wind_w1_dtr_ms(daily, local_day, cz_mid)
    diag = wind_diagnostics(w1, daily, local_day)
    check("W1 non-negative", bool(np.nanmin(w1) >= 0.0), f"min {np.nanmin(w1):.4f}")
    check(
        "W1 preserves the daily-mean wind",
        diag["max_daily_mean_error_ms"] < 1e-9,
        f"max error {diag['max_daily_mean_error_ms']:.2e} m/s",
    )

    # 3. W1 collapses onto C2 at the declared hinge DTR.
    hinged = daily.copy()
    hinged["tasmin"] = hinged["tasmax"] - WIND_DTR_HINGE_C
    w1_hinge = wind_w1_dtr_ms(hinged, local_day, cz_mid)
    c2_hinge = m1.reconstruct_wind_10m_ms(hinged, local_day, cz_mid, m1.CANDIDATE_PARAMS["C2"])
    check(
        "W1 equals C2 at the hinge DTR",
        float(np.nanmax(np.abs(w1_hinge - c2_hinge))) < 1e-9,
        f"DTR {WIND_DTR_HINGE_C:.4f} C",
    )

    # 4. W2 profile normalisation and mean preservation.
    profile = wind_climatology_profile(hourly)
    rows = profile.dropna(how="all")
    check(
        "W2 monthly profiles average exactly 1",
        bool(np.allclose(rows.mean(axis=1).to_numpy(), 1.0, atol=1e-12)),
    )
    check("W2 profile is non-negative", bool(np.nanmin(profile.to_numpy()) >= 0.0))
    w2 = wind_w2_climatology_ms(daily, local_day, times, profile)
    diag2 = wind_diagnostics(w2, daily, local_day)
    check(
        "W2 preserves the daily-mean wind",
        diag2["max_daily_mean_error_ms"] < 1e-9,
        f"max error {diag2['max_daily_mean_error_ms']:.2e} m/s",
    )
    check("W2 non-negative", bool(np.nanmin(w2) >= 0.0))

    # 5. CarbonPlan coefficients reproduce their published fit.
    beta = carbonplan_adjustment_coefficients()
    published = np.array([m1.TIER2_ADJ_INTERCEPT, m1.TIER2_ADJ_RSDS, m1.TIER2_ADJ_WIND])
    check(
        "CarbonPlan OLS refit matches the shipped Tier-2 coefficients",
        float(np.max(np.abs(beta - published))) < 1e-4,
        f"max diff {float(np.max(np.abs(beta - published))):.2e}",
    )

    # 6. R3 interpolation leaves radiation alone and NaNs only the first hour.
    shifted = interpolate_drivers_to_midpoint(hourly)
    check(
        "R3 leaves radiation untouched",
        bool(
            np.array_equal(
                shifted["shortwave_radiation"].to_numpy(),
                hourly["shortwave_radiation"].to_numpy(),
            )
        ),
    )
    nan_positions = np.flatnonzero(~np.isfinite(shifted["temperature_2m"].to_numpy(dtype=float)))
    check(
        "R3 NaN confined to the first hour",
        nan_positions.tolist() == [0],
        f"positions {nan_positions[:5].tolist()}",
    )

    # 7. Write guard.
    try:
        _guard_write_target(Path("docs/diagnostics/wbgt_outdoor_feasibility"), "out-dir")
        check("write guard protects milestone 1", False, "no SystemExit raised")
    except SystemExit:
        check("write guard protects milestone 1", True)

    # 8. QDM trigger is inert on an empty comparison.
    check(
        "QDM trigger reports NOT EVALUATED on empty input",
        qdm_trigger(pd.DataFrame())["fired"] is False,
    )

    print(f"\n{len(failures)} failure(s)" if failures else "\nall contract checks passed")
    return 1 if failures else 0


# ==========================================================================
# CLI
# ==========================================================================

STAGES = ("windows", "reference", "nex", "qdm", "all")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.diagnostics.wbgt_outdoor_selection",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--stage", choices=STAGES, default="all")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--era5-cache", type=Path, default=m1.DEFAULT_ERA5_CACHE)
    parser.add_argument("--nex-main-root", type=Path, default=m1.DEFAULT_NEX_MAIN_ROOT)
    parser.add_argument("--nex-wbgt-root", type=Path, default=m1.DEFAULT_NEX_WBGT_ROOT)
    parser.add_argument("--sites", default="all")
    parser.add_argument(
        "--windows", default=f"{m1.DEFAULT_PRIMARY_WINDOW};{m1.DEFAULT_CONTINUITY_WINDOW}"
    )
    parser.add_argument("--nex-models", default=",".join(NEX_MODELS))
    parser.add_argument("--nex-years", default=f"{min(NEX_MATCHED_YEARS)}-{max(NEX_MATCHED_YEARS)}")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="bounded at 4; default 1 because the national shade rebuild owns the disk",
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--resume", action="store_true", help="reuse cached per-site parquets")
    group.add_argument("--overwrite", action="store_true", help="recompute every per-site parquet")
    return parser


def select_sites(spec: str) -> tuple[Site, ...]:
    if spec.strip().lower() == "all":
        return tuple(SITES)
    wanted = [s.strip().lower() for s in spec.split(",") if s.strip()]
    chosen = tuple(s for s in SITES if s.name.lower() in wanted)
    if len(chosen) != len(wanted):
        raise SystemExit(f"Unknown site(s) in {spec!r}; known: {[s.name for s in SITES]}")
    return chosen


def parse_windows(spec: str) -> tuple[tuple[int, ...], ...]:
    return tuple(m1.parse_years(part) for part in spec.split(";") if part.strip())


def _window_label(years: Sequence[int]) -> str:
    return f"{min(years)}-{max(years)}"


def _cache_path(work_dir: Path, site: str, window: str, convention: str) -> Path:
    tag = "" if convention == "ist" else f"_{convention}"
    return work_dir / f"{site.lower()}_{window}{tag}.parquet"


def _run_one(payload: tuple) -> tuple[str, str, pd.DataFrame, dict[str, object]]:
    site, years, cache_dir, calibration, label, convention, sensitivity = payload
    result = run_site_window(
        site,
        years,
        cache_dir=cache_dir,
        calibration_years=calibration,
        climatology_label=label,
        day_convention=convention,
        with_reference_sensitivity=sensitivity,
        verbose=True,
    )
    diagnostics = dict(result.diagnostics)
    peaks = diagnostics.pop("peak_hour_frame", None)
    if isinstance(peaks, pd.DataFrame):
        diagnostics["peak_hour_records"] = peaks.to_dict(orient="list")
    return result.site, result.window, result.daily, diagnostics


def _json_default(value: object) -> object:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (pd.DataFrame, pd.Series)):
        return "omitted"
    return str(value)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()

    out_dir = _guard_write_target(args.out_dir, "out-dir")
    work_dir = _guard_write_target(args.work_dir, "work-dir")
    sites = select_sites(args.sites)
    windows = parse_windows(args.windows)
    window_labels = [_window_label(w) for w in windows]
    workers = max(1, min(4, int(args.workers)))

    print("Outdoor-WBGT method selection, milestone 2")
    print("  spec        docs/diagnostics/wbgt_outdoor_selection/SPEC.md")
    print(f"  stage       {args.stage}")
    print(f"  out-dir     {out_dir}")
    print(f"  work-dir    {work_dir}")
    print(f"  era5 cache  {args.era5_cache}")
    print(f"  sites       {', '.join(s.name for s in sites)}")
    print(f"  windows     {', '.join(window_labels)}")
    print("  candidates  C1, C2, W1, W2 (+ W2rev diagnostic), oracles A5/A6")
    print(f"  workers     {workers}")

    missing = [
        (s.name, y)
        for s in sites
        for w in windows
        for y in w
        if not (args.era5_cache / f"{s.name.lower()}_{y}.parquet").exists()
    ]
    if missing:
        raise SystemExit(
            f"Hourly ERA5 cache incomplete under {args.era5_cache}: {missing[:6]}"
            f"{' ...' if len(missing) > 6 else ''}. This tool never downloads."
        )
    if args.dry_run:
        print("\nPreflight OK (dry run). Nothing computed, nothing written.")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    print("\nPreflight OK.\n")

    extra: dict[str, object] = {"stages_run": []}
    results: list[SelectionResult] = []

    # ---------------- Stage: windows ----------------
    if args.stage in ("windows", "reference", "nex", "qdm", "all"):
        print("Building per-site daily series")
        jobs: list[tuple] = []
        for years in windows:
            label = _window_label(years)
            other = _calibration_window_for(label, window_labels)
            calibration = m1.parse_years(other) if other is not None else None
            # SPEC.md 3.2: forward calibration (earlier period -> later window) is the primary
            # W2; the reverse direction is diagnostic only and carries a distinct label so the
            # selection rule cannot pick it up.
            climatology_label = (
                "W2"
                if other is not None and min(m1.parse_years(other)) < min(years)
                else "W2rev"
            )
            sensitivity = args.stage in ("reference", "all")
            for site in sites:
                path = _cache_path(work_dir, site.name, label, "ist")
                if args.resume and path.exists():
                    daily = pd.read_parquet(path)
                    meta_path = path.with_suffix(".json")
                    diagnostics = (
                        json.loads(meta_path.read_text()) if meta_path.exists() else {}
                    )
                    results.append(SelectionResult(site.name, label, daily, diagnostics))
                    print(f"    {site.name} {label}: reused cache ({len(daily)} days)")
                    continue
                jobs.append(
                    (
                        site,
                        years,
                        args.era5_cache,
                        calibration,
                        climatology_label,
                        "ist",
                        sensitivity and site.name in REFERENCE_SENSITIVITY_SITES,
                    )
                )

        def store(site: str, window: str, daily: pd.DataFrame, diagnostics: dict) -> None:
            path = _cache_path(work_dir, site, window, "ist")
            daily.to_parquet(path)
            path.with_suffix(".json").write_text(
                json.dumps(diagnostics, indent=2, default=_json_default)
            )
            results.append(SelectionResult(site, window, daily, diagnostics))

        if jobs:
            if workers == 1:
                for payload in jobs:
                    store(*_run_one(payload))
            else:
                with ProcessPoolExecutor(max_workers=workers) as pool:
                    for outcome in pool.map(_run_one, jobs):
                        store(*outcome)
        extra["stages_run"].append("windows")

        scores = m1.build_score_table(results)
        annual = build_annual_table(results)
        per_year = per_year_count_errors(results)
        uncertainty = m1.build_uncertainty_table(
            results, [c for c in DEPLOYABLE_IDS]
        )
        attribution = wind_attribution_table(results)

        for name, table in (
            ("candidate_scores.csv", scores),
            ("annual_counts.csv", annual),
            ("per_year_count_errors.csv", per_year),
            ("uncertainty.csv", uncertainty),
            ("wind_attribution.csv", attribution),
        ):
            table.to_csv(out_dir / name, index=False)
            print(f"  wrote {out_dir / name} ({len(table)} rows)")

        selection = apply_selection_rule(scores, annual, per_year)
        (out_dir / "selection.json").write_text(
            json.dumps(selection, indent=2, default=_json_default)
        )
        print(f"\nSelection rule (SPEC.md 7): {selection['status']}")
        print(f"  selected: {selection['selected']}")
        for entry in selection["ranking"]:
            print(
                f"    {entry['candidate']}: daily {entry['daily_gate']} "
                f"({entry['daily_pairs_passing']}/{entry['daily_pairs']}), "
                f"count {entry['count_gate']} "
                f"({entry['count_pairs_gated'] - entry['count_pairs_failing']}"
                f"/{entry['count_pairs_gated']}), "
                f"worst count error {entry['worst_count_error_per_year']:.1f} d/yr"
            )
        extra["selection"] = selection

        wind_rows = []
        for result in results:
            for cid, diag in result.diagnostics.get("wind", {}).items():
                wind_rows.append(
                    {"site": result.site, "window": result.window, "wind": cid, **diag}
                )
        pd.DataFrame(wind_rows).to_csv(out_dir / "wind_diagnostics.csv", index=False)
        print(f"  wrote {out_dir / 'wind_diagnostics.csv'} ({len(wind_rows)} rows)")

        combined = pd.concat(
            [r.daily.assign(site=r.site, window=r.window) for r in results]
        )
        for column in ("site", "window"):
            combined[column] = combined[column].astype("category")
        combined.to_parquet(
            out_dir / "daily_series.parquet", compression="zstd", index=True
        )

    # ---------------- Stage: reference sensitivity ----------------
    if args.stage in ("reference", "all"):
        table = reference_sensitivity_table(results)
        table.to_csv(out_dir / "reference_sensitivity.csv", index=False)
        changed = table[table.get("count_gate_verdict_changed", pd.Series(dtype=bool)) == True]  # noqa: E712
        daily_changed = table[
            table.get("daily_gate_verdict_changed", pd.Series(dtype=bool)) == True  # noqa: E712
        ]
        verdict = (
            "CLOSED: no gate verdict changes under R3"
            if table.empty is False and changed.empty and daily_changed.empty
            else "REFERENCE UNCERTAINTY: a gate verdict changes under R3"
            if not table.empty
            else "NOT EVALUATED"
        )
        extra["reference_sensitivity"] = {
            "verdict": verdict,
            "sites": list(REFERENCE_SENSITIVITY_SITES),
            "n_count_gate_verdicts_changed": int(len(changed)),
            "n_daily_gate_verdicts_changed": int(len(daily_changed)),
            "signature": REFERENCE_SIGNATURE_MIDPOINT,
        }
        extra["stages_run"].append("reference")
        print(f"\nReference sensitivity (SPEC.md 5): {verdict}")
        print(f"  wrote {out_dir / 'reference_sensitivity.csv'} ({len(table)} rows)")

    # ---------------- Stage: matched-period NEX ----------------
    if args.stage in ("nex", "qdm", "all"):
        models = tuple(m.strip() for m in args.nex_models.split(",") if m.strip())
        nex_years = m1.parse_years(args.nex_years)
        selected = str(extra.get("selection", {}).get("selected") or "C1")
        print(f"\nMatched-period NEX comparison (SPEC.md 8): {selected} on {', '.join(models)}")
        sample_path = work_dir / f"nex_sample_{NEX_SCENARIO}_{_window_label(nex_years)}.parquet"
        if args.resume and sample_path.exists():
            sample = pd.read_parquet(sample_path)
            print(f"  reused NEX sample ({len(sample)} rows)")
        else:
            sample = m1.nex_site_sample(
                args.nex_main_root, args.nex_wbgt_root, NEX_SCENARIO, nex_years, models, sites
            )
            if not sample.empty:
                sample.to_parquet(sample_path)
            print(f"  read NEX sample ({len(sample)} rows)")

        if sample.empty:
            extra["nex"] = {"status": "NOT EVALUATED: no NEX sample could be read"}
            print("  NOT EVALUATED: no NEX sample could be read")
        else:
            profiles = None
            if selected in ("W2", "W2rev"):
                profiles = {}
                for site in sites:
                    cal = m1.parse_years(W2_PRIMARY_CALIBRATION)
                    profiles[site.name] = wind_climatology_profile(
                        m1.attach_geometry(
                            site, m1.load_hourly_cache(site, cal, args.era5_cache)
                        )
                    )
            nex_path = work_dir / f"nex_{selected}_daily_max.parquet"
            if args.resume and nex_path.exists():
                nex_daily = pd.read_parquet(nex_path)
                print(f"  reused NEX {selected} daily maxima ({len(nex_daily)} rows)")
            else:
                nex_daily = nex_candidate_daily_max(sample, sites, selected, profiles)
                nex_daily.to_parquet(nex_path)

            era5_subset = [r for r in results if r.window == m1.DEFAULT_PRIMARY_WINDOW]
            comparison = nex_matched_comparison(nex_daily, era5_subset, selected, nex_years)
            comparison.to_csv(out_dir / "nex_matched_comparison.csv", index=False)
            print(f"  wrote {out_dir / 'nex_matched_comparison.csv'} ({len(comparison)} rows)")

            validity = m1.nex_site_validity(sample)
            validity.to_csv(out_dir / "nex_sample_validity.csv", index=False)

            # ---- Source-day sensitivity (SPEC.md 8.1) ----
            utc_rows: list[dict[str, object]] = []
            for site in sites:
                path = _cache_path(work_dir, site.name, _window_label(nex_years), "utc")
                if args.resume and path.exists():
                    daily_utc = pd.read_parquet(path)
                else:
                    r = run_site_window(
                        site, nex_years, cache_dir=args.era5_cache,
                        calibration_years=None, day_convention="utc", verbose=True,
                    )
                    daily_utc = r.daily
                    daily_utc.to_parquet(path)
                ist = next(
                    (
                        r.daily["ref_audited"][
                            pd.DatetimeIndex(r.daily.index).year.isin(list(nex_years))
                        ]
                        for r in results
                        if r.site == site.name and r.window == m1.DEFAULT_PRIMARY_WINDOW
                    ),
                    None,
                )
                column = f"cand_{selected}" if f"cand_{selected}" in daily_utc else "cand_C1"
                for label, series in (
                    ("UTC day, ref_audited", daily_utc["ref_audited"]),
                    (f"UTC day, {selected}", daily_utc[column]),
                    ("IST day, ref_audited", ist),
                ):
                    if series is None:
                        continue
                    utc_rows.append(
                        {
                            "site": site.name,
                            "years": _window_label(nex_years),
                            "series": label,
                            **_distribution_row(series, m1.complete_years(series)),
                        }
                    )
            pd.DataFrame(utc_rows).to_csv(out_dir / "source_day_sensitivity.csv", index=False)
            print(f"  wrote {out_dir / 'source_day_sensitivity.csv'} ({len(utc_rows)} rows)")

            trigger = qdm_trigger(comparison)
            extra["nex"] = {
                "models": list(models),
                "scenario": NEX_SCENARIO,
                "years": _window_label(nex_years),
                "candidate": selected,
                "day_boundary": "INFERRED from the CMIP6 daily convention; no file publishes "
                "time_bnds, so this is not verified locally",
                "qdm_trigger": trigger,
            }
            extra["stages_run"].append("nex")
            print(
                f"\nQDM trigger (SPEC.md 9, predeclared): "
                f"{'FIRED' if trigger['fired'] else 'did not fire'}"
            )

            # ---------------- Stage: conditional QDM ----------------
            if args.stage in ("qdm", "all") and trigger["fired"]:
                era5_reference = {}
                for site in sites:
                    parts = [
                        r.daily["ref_audited"] for r in results if r.site == site.name
                    ]
                    if parts:
                        era5_reference[site.name] = pd.concat(parts).sort_index()
                future = pd.DataFrame()
                future_path = (
                    work_dir
                    / f"nex_{selected}_daily_max_{QDM_FUTURE_SCENARIO}.parquet"
                )
                if args.resume and future_path.exists():
                    future = pd.read_parquet(future_path)
                    print(f"  reused NEX future daily maxima ({len(future)} rows)")
                else:
                    try:
                        future_sample = m1.nex_site_sample(
                            args.nex_main_root, args.nex_wbgt_root, QDM_FUTURE_SCENARIO,
                            QDM_FUTURE_YEARS, models, sites,
                        )
                        if not future_sample.empty:
                            future = nex_candidate_daily_max(
                                future_sample, sites, selected, profiles
                            )
                            future.to_parquet(future_path)
                    except Exception as exc:  # pragma: no cover - reported, never silent
                        print(f"  future sample unavailable: {exc}")
                full_path = work_dir / f"nex_{selected}_daily_max_qdm_window.parquet"
                if args.resume and full_path.exists():
                    nex_full = pd.read_parquet(full_path)
                    print(f"  reused NEX QDM-window daily maxima ({len(nex_full)} rows)")
                else:
                    full = m1.nex_site_sample(
                        args.nex_main_root, args.nex_wbgt_root, NEX_SCENARIO,
                        tuple(sorted(set(QDM_TRAIN_YEARS) | set(QDM_TEST_YEARS))), models, sites,
                    )
                    nex_full = (
                        nex_candidate_daily_max(full, sites, selected, profiles)
                        if not full.empty
                        else nex_daily
                    )
                    nex_full.to_parquet(full_path)
                qdm = qdm_trial(
                    nex_full, era5_reference, future=future if not future.empty else None
                )
                qdm.to_csv(out_dir / "qdm_trial.csv", index=False)
                extra["qdm"] = {
                    "status": "RUN (trigger fired)",
                    "future_sample": "available" if not future.empty else "NOT AVAILABLE",
                    "rows": int(len(qdm)),
                }
                extra["stages_run"].append("qdm")
                print(f"  wrote {out_dir / 'qdm_trial.csv'} ({len(qdm)} rows)")
            elif args.stage in ("qdm", "all"):
                extra["qdm"] = {
                    "status": "NOT RUN: the predeclared trigger did not fire",
                }

    manifest = run_manifest(args, extra)
    (out_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, default=_json_default)
    )
    print(f"\nwrote {out_dir / 'run_manifest.json'}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
