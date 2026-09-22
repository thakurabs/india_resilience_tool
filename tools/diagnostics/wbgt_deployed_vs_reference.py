"""Compare IRT's deployed WBGT formulations against the Lemke & Kjellstrom references.

Lemke and Kjellstrom (2012), *Calculating workplace WBGT from meteorological data:
a tool for climate change assessment*, Industrial Health 50(4), 267-278, conclude:

    "We recommend the method of Liljegren et al. (2008) for calculating outdoor
    WBGT and the method by Bernard et al. (1999) for indoor WBGT when estimating
    climate change impacts on occupational heat stress at a population level."

IRT ships neither. It ships two approximations:

    wbgt_shade_stull  = 0.7 * Twb_Stull(Ta, RH) + 0.3 * Ta
    swbgt_empirical   = 0.567 * Ta + 0.393 * e + 3.94          (e in hPa)

and it evaluates both on **daily-mean** ``tas`` and ``hurs``, not on the daily
maximum and not hourly.  This script quantifies the resulting disagreement by
driving every formulation from one common hourly ERA5 point series, so that
climate-model error cancels and only method error remains.

Four quantities are produced per site-day:

===========================  =================================================
``liljegren_daily_max``      Reference OUTDOOR WBGT: hourly Liljegren (1) -> daily max
``bernard_daily_max``        Reference INDOOR WBGT: hourly Bernard (2) -> daily max
``<metric>_hourly_max``      IRT formula applied hourly, then daily max
``<metric>_daily_mean_in``   IRT formula applied to daily-mean inputs (AS DEPLOYED)
===========================  =================================================

The pair ``_hourly_max`` / ``_daily_mean_in`` decomposes the total error into a
**formula** term and an **aggregation** term.  That decomposition is the point of
the script: a metric can be defensible in form and still be wrong in practice
because of when it is evaluated.

ERA5 comes from the Open-Meteo archive API (no key, no quota registration). The
ARCO-ERA5 Zarr store was measured at roughly 448 GB for an equivalent extract
because its chunks are whole global fields; Open-Meteo returns a single site's
twenty-year hourly series in about twenty seconds.

Usage
-----
    python -m tools.diagnostics.wbgt_deployed_vs_reference --self-test
    python -m tools.diagnostics.wbgt_deployed_vs_reference --years 2005-2014
    python -m tools.diagnostics.wbgt_deployed_vs_reference \
        --sites Kochi,Bikaner --years 2010-2014 --months 3-9

References
----------
(1) Liljegren et al. (2008), https://doi.org/10.1080/15459620802310770
(2) Bernard and Pourmoghani (1999); implemented in the form given by Lemke and
    Kjellstrom (2012) and cross-checked against the reference R implementation
    in ``anacv/HeatStress`` (``R/wbgt.Bernard.R``).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

# Reuse the approved site list and solar geometry from the Stage A harness
# rather than duplicating validated code.
from tools.diagnostics.wbgt_method_validation import (  # noqa: E402
    SITES,
    Site,
    cos_solar_zenith,
)

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

#: Hourly fields requested from Open-Meteo. ``wind_speed_unit=ms`` is mandatory:
#: the API defaults to km/h, which would silently inflate the Liljegren wind.
OPEN_METEO_HOURLY = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "surface_pressure",
    "shortwave_radiation",
    "direct_radiation",
)

#: India Standard Time offset, used only to group hourly values into local days.
#: Solar geometry is always computed on the UTC timestamps.
IST_OFFSET = pd.Timedelta(hours=5, minutes=30)

# Bernard/Lemke psychrometric constants (Lemke and Kjellstrom 2012).
BERNARD_C1 = 6.106  # hPa
BERNARD_C2 = 17.27
BERNARD_C3 = 237.3  # degC
BERNARD_C4 = 1556.0
BERNARD_C5 = 1.484
BERNARD_C6 = 1010.0

#: Bisection iterations for the psychrometric wet bulb. The root is bracketed by
#: [Td, Ta], so 60 halvings take any plausible bracket below float resolution.
BERNARD_BISECT_ITERS = 60

#: Day-count thresholds mirroring the shipped ``*_days_ge_*`` slugs.
DAY_COUNT_THRESHOLDS_C = (28.0, 30.0, 32.0)

DEFAULT_YEARS = "2005-2014"
DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_deployed_vs_reference")
DEFAULT_CACHE_DIR = Path("scratch/wbgt_deployed_vs_reference_cache")

#: Politeness delay between Open-Meteo requests.
REQUEST_SPACING_S = 1.0


# --------------------------------------------------------------------------
# Thermodynamics
# --------------------------------------------------------------------------


def saturation_vapour_pressure_hpa(t_c: np.ndarray) -> np.ndarray:
    """Saturation vapour pressure (hPa) in the Magnus form used by Bernard.

    The coefficients are those of Lemke and Kjellstrom (2012) so that the
    psychrometric solver below stays internally consistent with its reference.
    """

    t_c = np.asarray(t_c, dtype=float)
    return BERNARD_C1 * np.exp((BERNARD_C2 * t_c) / (BERNARD_C3 + t_c))


def stull_twb_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """Stull (2011) psychrometric wet-bulb temperature (degC).

    This is the exact expression IRT ships in
    ``india_resilience_tool.compute.heat_stress_gridfirst.stull_twb_c``; it is
    restated here so the comparison is driven by one hourly series without
    importing runtime code into a diagnostic.
    """

    t_c = np.asarray(t_c, dtype=float)
    rh = np.clip(np.asarray(rh_pct, dtype=float), 0.0, 100.0)
    return (
        t_c * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(t_c + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * np.power(rh, 1.5) * np.arctan(0.023101 * rh)
        - 4.686035
    )


def bernard_psychrometric_wet_bulb_c(
    t_c: np.ndarray,
    td_c: np.ndarray,
) -> np.ndarray:
    """Psychrometric wet-bulb temperature (degC) by Bernard's energy balance.

    Solves ``f(Tpwb) = 0`` where, with ``ed`` the ambient vapour pressure and
    ``es(T)`` the saturation vapour pressure::

        f(T) = c4*ed - c5*ed*T - c4*es(T) + c5*es(T)*T + c6*(Ta - T)

    The reference R implementation minimises ``|f|`` with Brent's method. Here
    the root is bracketed instead: ``f(Td) = c6*(Ta - Td) >= 0`` and
    ``f(Ta) = (c4 - c5*Ta)*(ed - es(Ta)) <= 0``, so plain bisection is both
    vectorised and unconditionally convergent, which matters when the input is
    a multi-year hourly series.

    Dew points above the air temperature are clipped to it (RH capped at 100%),
    matching the reference implementation's ``noNAs`` branch.
    """

    t_c = np.asarray(t_c, dtype=float)
    td_c = np.minimum(np.asarray(td_c, dtype=float), t_c)

    ed = saturation_vapour_pressure_hpa(td_c)

    def residual(tt: np.ndarray) -> np.ndarray:
        es = saturation_vapour_pressure_hpa(tt)
        return (
            BERNARD_C4 * ed
            - BERNARD_C5 * ed * tt
            - BERNARD_C4 * es
            + BERNARD_C5 * es * tt
            + BERNARD_C6 * (t_c - tt)
        )

    lo = td_c.copy()
    hi = t_c.copy()
    for _ in range(BERNARD_BISECT_ITERS):
        mid = 0.5 * (lo + hi)
        f_mid = residual(mid)
        # f decreases across the bracket: keep the half that still spans zero.
        go_up = f_mid > 0.0
        lo = np.where(go_up, mid, lo)
        hi = np.where(go_up, hi, mid)

    tpwb = 0.5 * (lo + hi)
    return np.where(np.isfinite(t_c) & np.isfinite(td_c), tpwb, np.nan)


def bernard_indoor_wbgt_c(t_c: np.ndarray, td_c: np.ndarray) -> np.ndarray:
    """Bernard et al. (1999) indoor/shade WBGT (degC).

    ``WBGT_indoor = 0.67 * Tpwb + 0.33 * Ta``. There is no wind term and no
    solar term: the method is defined for no-solar-load conditions, which is
    precisely the regime IRT's ``wbgt_shade_stull`` claims to describe.
    """

    tpwb = bernard_psychrometric_wet_bulb_c(t_c, td_c)
    return 0.67 * tpwb + 0.33 * np.asarray(t_c, dtype=float)


def irt_wbgt_shade_stull_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """IRT's deployed shaded WBGT: ``0.7 * Twb_Stull + 0.3 * Ta`` (degC)."""

    return 0.7 * stull_twb_c(t_c, rh_pct) + 0.3 * np.asarray(t_c, dtype=float)


def irt_swbgt_empirical_c(t_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """IRT's deployed outdoor sWBGT proxy: ``0.567*Ta + 0.393*e + 3.94`` (degC).

    The Magnus coefficients (17.62 / 243.12) are those of the shipped
    ``swbgt_empirical_cell_c``, deliberately kept distinct from the Bernard
    constants above so this reproduces what IRT actually computes.
    """

    t_c = np.asarray(t_c, dtype=float)
    rh = np.clip(np.asarray(rh_pct, dtype=float), 0.0, 100.0)
    es_hpa = 6.112 * np.exp((17.62 * t_c) / (243.12 + t_c))
    e_hpa = (rh / 100.0) * es_hpa
    return 0.567 * t_c + 0.393 * e_hpa + 3.94


def liljegren_wbgt_c(
    t_c: np.ndarray,
    rh_pct: np.ndarray,
    pressure_hpa: np.ndarray,
    wind_ms: np.ndarray,
    ssrd_w_m2: np.ndarray,
    fdir_frac: np.ndarray,
    cossza: np.ndarray,
) -> np.ndarray:
    """Liljegren et al. (2008) outdoor WBGT (degC) via thermofeel.

    Units follow thermofeel exactly: temperature in K, RH in percent, pressure
    in hPa, ``fdir`` a 0-1 fraction of ``ssrd``. Passing any of these in the
    wrong unit returns NaN silently rather than raising, so the caller checks
    the NaN rate.
    """

    from thermofeel import calculate_wbgt_liljegren

    wbgt_k = calculate_wbgt_liljegren(
        np.asarray(t_c, dtype=float) + 273.15,
        np.asarray(rh_pct, dtype=float),
        np.asarray(pressure_hpa, dtype=float),
        np.asarray(wind_ms, dtype=float),
        np.asarray(ssrd_w_m2, dtype=float),
        np.asarray(fdir_frac, dtype=float),
        np.asarray(cossza, dtype=float),
    )
    return np.asarray(wbgt_k, dtype=float) - 273.15


# --------------------------------------------------------------------------
# ERA5 retrieval
# --------------------------------------------------------------------------


def fetch_open_meteo_hourly(
    site: Site,
    start: str,
    end: str,
    *,
    timeout_s: float = 120.0,
) -> pd.DataFrame:
    """Fetch one site's hourly ERA5 series from the Open-Meteo archive.

    Returns a frame indexed by UTC timestamp. Raises ``RuntimeError`` if the
    API reports an error or returns no rows.
    """

    import requests

    params = {
        "latitude": site.lat,
        "longitude": site.lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(OPEN_METEO_HOURLY),
        "models": "era5",
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }
    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_s)
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(f"Open-Meteo error for {site.name}: {payload.get('reason')}")

    hourly = payload.get("hourly") or {}
    if not hourly.get("time"):
        raise RuntimeError(f"Open-Meteo returned no hours for {site.name}")

    frame = pd.DataFrame(hourly)
    frame["time"] = pd.to_datetime(frame["time"])
    frame = frame.set_index("time").sort_index()
    frame.attrs["grid_latitude"] = payload.get("latitude")
    frame.attrs["grid_longitude"] = payload.get("longitude")
    frame.attrs["grid_elevation_m"] = payload.get("elevation")
    return frame


def load_site_hourly(
    site: Site,
    years: Sequence[int],
    *,
    cache_dir: Path | None,
    verbose: bool = True,
) -> pd.DataFrame:
    """Return a site's hourly ERA5 series for ``years``, one API call per year.

    Per-year parquet caching makes reruns free and makes an interrupted run
    resumable. Set ``cache_dir`` to ``None`` to disable.
    """

    blocks: list[pd.DataFrame] = []
    attrs: dict[str, object] = {}

    for year in years:
        cache_path = (
            None
            if cache_dir is None
            else cache_dir / f"{site.name.lower()}_{year}.parquet"
        )
        if cache_path is not None and cache_path.exists():
            block = pd.read_parquet(cache_path)
            if verbose:
                print(f"    {site.name} {year}: {len(block):>5d} h (cached)")
        else:
            started = time.monotonic()
            block = fetch_open_meteo_hourly(site, f"{year}-01-01", f"{year}-12-31")
            attrs.update(
                {
                    key: block.attrs[key]
                    for key in ("grid_latitude", "grid_longitude", "grid_elevation_m")
                    if key in block.attrs
                }
            )
            if verbose:
                elapsed = time.monotonic() - started
                print(f"    {site.name} {year}: {len(block):>5d} h ({elapsed:.1f} s)")
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                block.to_parquet(cache_path)
            time.sleep(REQUEST_SPACING_S)
        blocks.append(block)

    frame = pd.concat(blocks).sort_index()
    frame = frame[~frame.index.duplicated(keep="first")]
    frame.attrs.update(attrs)
    return frame


# --------------------------------------------------------------------------
# Per-site computation
# --------------------------------------------------------------------------


def build_hourly_frame(site: Site, raw: pd.DataFrame) -> pd.DataFrame:
    """Attach solar geometry and every WBGT formulation to the hourly series."""

    frame = raw.copy()
    times = pd.DatetimeIndex(frame.index)

    frame["cossza"] = cos_solar_zenith(site.lat, times, site.lon)

    ssrd = frame["shortwave_radiation"].to_numpy(dtype=float)
    direct = frame["direct_radiation"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        fdir = np.where(ssrd > 0.0, direct / ssrd, 0.0)
    frame["fdir_frac"] = np.clip(np.nan_to_num(fdir), 0.0, 1.0)

    t_c = frame["temperature_2m"].to_numpy(dtype=float)
    rh = frame["relative_humidity_2m"].to_numpy(dtype=float)
    td_c = frame["dew_point_2m"].to_numpy(dtype=float)

    frame["liljegren_c"] = liljegren_wbgt_c(
        t_c,
        rh,
        frame["surface_pressure"].to_numpy(dtype=float),
        frame["wind_speed_10m"].to_numpy(dtype=float),
        ssrd,
        frame["fdir_frac"].to_numpy(dtype=float),
        frame["cossza"].to_numpy(dtype=float),
    )
    frame["bernard_c"] = bernard_indoor_wbgt_c(t_c, td_c)
    frame["irt_shade_c"] = irt_wbgt_shade_stull_c(t_c, rh)
    frame["irt_swbgt_c"] = irt_swbgt_empirical_c(t_c, rh)

    # Local civil day, used only for grouping; solar geometry stayed on UTC.
    frame["local_day"] = (times + IST_OFFSET).date
    return frame


def build_daily_frame(hourly: pd.DataFrame) -> pd.DataFrame:
    """Collapse the hourly frame to the four daily quantities under comparison.

    Days with fewer than 24 hours present (the UTC/IST boundary days at each end
    of the record) are dropped so that no daily maximum is taken over a partial
    day.
    """

    grouped = hourly.groupby("local_day")
    counts = grouped.size()

    daily = pd.DataFrame(
        {
            # References: hourly physics, then the daily peak.
            "liljegren_daily_max": grouped["liljegren_c"].max(),
            "bernard_daily_max": grouped["bernard_c"].max(),
            # IRT formulas evaluated hourly, then the daily peak (formula error only).
            "irt_shade_hourly_max": grouped["irt_shade_c"].max(),
            "irt_swbgt_hourly_max": grouped["irt_swbgt_c"].max(),
            # Daily-mean drivers, which is what the pipeline actually receives.
            "tas_daily_mean_c": grouped["temperature_2m"].mean(),
            "hurs_daily_mean_pct": grouped["relative_humidity_2m"].mean(),
            "tasmax_c": grouped["temperature_2m"].max(),
        }
    )
    daily = daily.loc[counts == 24]

    # IRT as deployed: the formula applied to daily-mean inputs.
    tas_mean = daily["tas_daily_mean_c"].to_numpy(dtype=float)
    hurs_mean = daily["hurs_daily_mean_pct"].to_numpy(dtype=float)
    daily["irt_shade_daily_mean_in"] = irt_wbgt_shade_stull_c(tas_mean, hurs_mean)
    daily["irt_swbgt_daily_mean_in"] = irt_swbgt_empirical_c(tas_mean, hurs_mean)

    daily.index = pd.to_datetime(daily.index)
    daily.index.name = "local_day"
    return daily


# --------------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Comparison:
    """One candidate scored against one reference."""

    site: str
    reference: str
    candidate: str
    n_days: int
    bias_c: float
    median_abs_bias_c: float
    rmse_c: float
    pearson_r: float
    p99_bias_c: float
    days_ref: dict[float, int]
    days_cand: dict[float, int]

    def to_row(self) -> dict[str, object]:
        row: dict[str, object] = {
            "site": self.site,
            "reference": self.reference,
            "candidate": self.candidate,
            "n_days": self.n_days,
            "bias_c": self.bias_c,
            "median_abs_bias_c": self.median_abs_bias_c,
            "rmse_c": self.rmse_c,
            "pearson_r": self.pearson_r,
            "p99_bias_c": self.p99_bias_c,
        }
        for threshold in DAY_COUNT_THRESHOLDS_C:
            row[f"days_ge_{threshold:g}_ref"] = self.days_ref[threshold]
            row[f"days_ge_{threshold:g}_cand"] = self.days_cand[threshold]
        return row


def compare_series(
    site_name: str,
    reference: pd.Series,
    candidate: pd.Series,
    *,
    reference_name: str,
    candidate_name: str,
) -> Comparison:
    """Score ``candidate`` against ``reference`` over their common valid days."""

    paired = pd.DataFrame({"ref": reference, "cand": candidate}).dropna()
    if paired.empty:
        raise ValueError(f"No overlapping valid days for {site_name}/{candidate_name}")

    diff = paired["cand"] - paired["ref"]
    correlation = (
        float(paired["ref"].corr(paired["cand"])) if len(paired) > 1 else float("nan")
    )

    # Tail behaviour matters more than the mean for threshold-count metrics, so
    # the bias is also reported at the 99th percentile of the reference.
    tail = paired[paired["ref"] >= paired["ref"].quantile(0.99)]
    p99_bias = float((tail["cand"] - tail["ref"]).mean()) if not tail.empty else np.nan

    return Comparison(
        site=site_name,
        reference=reference_name,
        candidate=candidate_name,
        n_days=int(len(paired)),
        bias_c=float(diff.mean()),
        median_abs_bias_c=float(diff.abs().median()),
        rmse_c=float(np.sqrt((diff**2).mean())),
        pearson_r=correlation,
        p99_bias_c=p99_bias,
        days_ref={
            t: int((paired["ref"] >= t).sum()) for t in DAY_COUNT_THRESHOLDS_C
        },
        days_cand={
            t: int((paired["cand"] >= t).sum()) for t in DAY_COUNT_THRESHOLDS_C
        },
    )


#: Which candidate is judged against which reference, and why.
COMPARISON_PLAN: tuple[tuple[str, str, str], ...] = (
    # Outdoor claim: swbgt_empirical is shipped as "Outdoor WBGT".
    ("liljegren_daily_max", "irt_swbgt_hourly_max", "outdoor, formula only"),
    ("liljegren_daily_max", "irt_swbgt_daily_mean_in", "outdoor, AS DEPLOYED"),
    # Shade claim: wbgt_shade_stull is shipped as "Shaded WBGT".
    ("bernard_daily_max", "irt_shade_hourly_max", "shade, formula only"),
    ("bernard_daily_max", "irt_shade_daily_mean_in", "shade, AS DEPLOYED"),
    # Does the shipped shade metric track the outdoor reference at all?
    ("liljegren_daily_max", "irt_shade_daily_mean_in", "shade vs outdoor reference"),
)


def compare_site(site_name: str, daily: pd.DataFrame) -> list[Comparison]:
    """Run every comparison in ``COMPARISON_PLAN`` for one site."""

    results: list[Comparison] = []
    for reference_col, candidate_col, label in COMPARISON_PLAN:
        results.append(
            compare_series(
                site_name,
                daily[reference_col],
                daily[candidate_col],
                reference_name=reference_col,
                candidate_name=f"{candidate_col} [{label}]",
            )
        )
    return results


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def render_markdown(
    rows: pd.DataFrame,
    *,
    years: Sequence[int],
    months: frozenset[int] | None,
    nan_rates: dict[str, float],
) -> str:
    """Render the findings document."""

    month_note = (
        "all months"
        if months is None
        else "months " + ",".join(str(m) for m in sorted(months))
    )
    lines = [
        "# Deployed IRT WBGT vs the Lemke & Kjellstrom reference methods",
        "",
        f"Generated {pd.Timestamp.utcnow():%Y-%m-%d %H:%M} UTC.",
        "",
        f"**Window:** {min(years)}-{max(years)}, {month_note}.  ",
        "**Driver:** ERA5 hourly, Open-Meteo archive, nearest 0.25 deg cell.  ",
        "**References:** Liljegren et al. (2008) outdoor; Bernard et al. (1999) indoor "
        "-- the two methods recommended by Lemke & Kjellstrom (2012).",
        "",
        "Every column below is driven by the *same* hourly series, so climate-model "
        "error cancels and what remains is method error.",
        "",
        "## Liljegren NaN rate by site",
        "",
        "| site | NaN rate |",
        "| --- | --- |",
    ]
    for site_name, rate in nan_rates.items():
        lines.append(f"| {site_name} | {rate:.4f} |")

    lines += [
        "",
        "## Scores",
        "",
        "`bias` is candidate minus reference. `p99 bias` is the same difference "
        "restricted to the hottest 1% of reference days, where the shipped "
        "threshold-count metrics are decided.",
        "",
        "| site | reference | candidate | n | bias C | med abs C | RMSE C | r | p99 bias C |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows.itertuples(index=False):
        lines.append(
            f"| {row.site} | {row.reference} | {row.candidate} | {row.n_days} | "
            f"{row.bias_c:+.2f} | {row.median_abs_bias_c:.2f} | {row.rmse_c:.2f} | "
            f"{row.pearson_r:.3f} | {row.p99_bias_c:+.2f} |"
        )

    lines += [
        "",
        "## Day counts at the shipped thresholds",
        "",
        "| site | candidate | "
        + " | ".join(
            f">={t:g} ref | >={t:g} cand" for t in DAY_COUNT_THRESHOLDS_C
        )
        + " |",
        "| --- | --- | " + " | ".join(["---:"] * (2 * len(DAY_COUNT_THRESHOLDS_C))) + " |",
    ]
    for row in rows.itertuples(index=False):
        counts = " | ".join(
            f"{getattr(row, f'days_ge_{t:g}_ref')} | {getattr(row, f'days_ge_{t:g}_cand')}"
            for t in DAY_COUNT_THRESHOLDS_C
        )
        lines.append(f"| {row.site} | {row.candidate} | {counts} |")

    lines += [
        "",
        "## Reading this table",
        "",
        "- A large gap between the `formula only` and `AS DEPLOYED` rows for the same "
        "metric is **aggregation** error: it is caused by evaluating the formula on "
        "daily-mean inputs instead of hourly, not by the formula itself.",
        "- A large `formula only` bias is **formula** error and cannot be fixed by "
        "changing the aggregation.",
        "- A high `r` with a large bias means the metric ranks places correctly but "
        "reports the wrong absolute level -- which is benign for a frozen-CDF ruler "
        "and serious for an absolute threshold count.",
        "",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------


def run_self_test() -> int:
    """Validate the Bernard solver and the formulations without any network I/O."""

    failures: list[str] = []

    def check(label: str, condition: bool, detail: str = "") -> None:
        status = "PASS" if condition else "FAIL"
        print(f"  [{status}] {label}{(' -- ' + detail) if detail else ''}")
        if not condition:
            failures.append(label)

    print("Bernard psychrometric wet bulb")

    # Saturated air: Tpwb must equal Ta when Td == Ta.
    t_sat = np.array([20.0, 30.0, 40.0])
    tpwb_sat = bernard_psychrometric_wet_bulb_c(t_sat, t_sat)
    check(
        "Tpwb == Ta at saturation",
        bool(np.allclose(tpwb_sat, t_sat, atol=1e-6)),
        f"max dev {np.max(np.abs(tpwb_sat - t_sat)):.2e}",
    )

    # The root must be bracketed by the dew point and the air temperature.
    t_c = np.array([25.0, 30.0, 35.0, 40.0, 45.0])
    td_c = np.array([5.0, 15.0, 20.0, 25.0, 30.0])
    tpwb = bernard_psychrometric_wet_bulb_c(t_c, td_c)
    check(
        "Td <= Tpwb <= Ta",
        bool(np.all(tpwb >= td_c - 1e-6) and np.all(tpwb <= t_c + 1e-6)),
        f"Tpwb = {np.round(tpwb, 3).tolist()}",
    )

    # The residual must actually vanish at the returned root.
    ed = saturation_vapour_pressure_hpa(td_c)
    es = saturation_vapour_pressure_hpa(tpwb)
    residual = (
        BERNARD_C4 * ed
        - BERNARD_C5 * ed * tpwb
        - BERNARD_C4 * es
        + BERNARD_C5 * es * tpwb
        + BERNARD_C6 * (t_c - tpwb)
    )
    check(
        "residual ~ 0 at the root",
        bool(np.max(np.abs(residual)) < 1e-6),
        f"max |f| = {np.max(np.abs(residual)):.2e}",
    )

    # Bernard's Tpwb should sit close to Stull's, since both are psychrometric.
    from_rh = 100.0 * (
        saturation_vapour_pressure_hpa(td_c) / saturation_vapour_pressure_hpa(t_c)
    )
    stull = stull_twb_c(t_c, from_rh)
    check(
        "Bernard Tpwb within 1 C of Stull Twb",
        bool(np.max(np.abs(tpwb - stull)) < 1.0),
        f"max dev {np.max(np.abs(tpwb - stull)):.3f} C",
    )

    print("Formulation sanity")

    # IRT's shade form and Bernard's indoor form differ only in their weights
    # (0.70/0.30 vs 0.67/0.33) and in how the wet bulb is obtained.
    irt_shade = irt_wbgt_shade_stull_c(t_c, from_rh)
    bernard = bernard_indoor_wbgt_c(t_c, td_c)
    check(
        "IRT shade within 1 C of Bernard indoor",
        bool(np.max(np.abs(irt_shade - bernard)) < 1.0),
        f"max dev {np.max(np.abs(irt_shade - bernard)):.3f} C",
    )

    # sWBGT has no radiation term, so it must be insensitive to the sun by
    # construction; this is the mechanism objection, stated as a test.
    swbgt = irt_swbgt_empirical_c(t_c, from_rh)
    check(
        "sWBGT exceeds the shade form in humid heat",
        bool(np.all(swbgt[from_rh > 30.0] > irt_shade[from_rh > 30.0])),
        f"sWBGT = {np.round(swbgt, 2).tolist()}",
    )

    print("Liljegren wiring")
    try:
        cossza = np.array([0.0, 0.5, 0.9])
        lilj = liljegren_wbgt_c(
            np.array([35.0, 35.0, 35.0]),
            np.array([50.0, 50.0, 50.0]),
            np.array([1000.0, 1000.0, 1000.0]),
            np.array([2.0, 2.0, 2.0]),
            np.array([0.0, 500.0, 900.0]),
            np.array([0.0, 0.6, 0.8]),
            cossza,
        )
        check(
            "Liljegren returns finite values",
            bool(np.all(np.isfinite(lilj))),
            f"WBGT = {np.round(lilj, 2).tolist()} C",
        )
        check(
            "Liljegren increases with solar load",
            bool(lilj[2] > lilj[0]),
            f"{lilj[0]:.2f} -> {lilj[2]:.2f} C",
        )
    except Exception as exc:  # pragma: no cover - environment dependent
        check("Liljegren callable", False, str(exc))

    print()
    if failures:
        print(f"{len(failures)} check(s) FAILED: {', '.join(failures)}")
        return 1
    print("All self-test checks passed.")
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def parse_years(spec: str) -> list[int]:
    """Parse ``"2005-2014"`` or ``"2005,2008,2011"`` into a sorted year list."""

    years: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            years.update(range(int(start), int(end) + 1))
        else:
            years.add(int(part))
    if not years:
        raise ValueError(f"No years parsed from {spec!r}")
    return sorted(years)


def parse_months(spec: str | None) -> frozenset[int] | None:
    """Parse ``"3-9"`` into a month set, or ``None`` for all months."""

    if not spec:
        return None
    months: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            months.update(range(int(start), int(end) + 1))
        else:
            months.add(int(part))
    bad = {m for m in months if not 1 <= m <= 12}
    if bad:
        raise ValueError(f"Months out of range: {sorted(bad)}")
    return frozenset(months)


def select_sites(spec: str | None) -> list[Site]:
    """Resolve a comma-separated site-name spec against ``SITES``."""

    if not spec:
        return list(SITES)
    by_name = {site.name.lower(): site for site in SITES}
    chosen: list[Site] = []
    for raw in spec.split(","):
        name = raw.strip().lower()
        if not name:
            continue
        if name not in by_name:
            raise ValueError(
                f"Unknown site {raw.strip()!r}; known: {', '.join(s.name for s in SITES)}"
            )
        chosen.append(by_name[name])
    if not chosen:
        raise ValueError(f"No sites parsed from {spec!r}")
    return chosen


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare IRT's deployed WBGT formulations against the Liljegren "
            "(outdoor) and Bernard (indoor) references recommended by Lemke & "
            "Kjellstrom (2012), driven by one common hourly ERA5 series."
        )
    )
    parser.add_argument(
        "--sites",
        default=None,
        help=f"Comma-separated subset of: {', '.join(s.name for s in SITES)}",
    )
    parser.add_argument("--years", default=DEFAULT_YEARS, help="e.g. 2005-2014")
    parser.add_argument(
        "--months",
        default=None,
        help="Restrict the comparison to these months, e.g. 3-9. Default: all.",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument(
        "--no-cache", action="store_true", help="Bypass the per-year parquet cache."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan and exit without any network or disk writes.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline checks of the solvers and formulations, then exit.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    if args.self_test:
        return run_self_test()

    sites = select_sites(args.sites)
    years = parse_years(args.years)
    months = parse_months(args.months)
    cache_dir = None if args.no_cache else args.cache_dir

    if args.dry_run:
        print("DRY RUN -- nothing fetched, nothing written.")
        print(f"  sites      : {', '.join(s.name for s in sites)}")
        print(f"  years      : {years[0]}-{years[-1]} ({len(years)} years)")
        print(f"  months     : {'all' if months is None else sorted(months)}")
        print(f"  requests   : {len(sites) * len(years)} (1 per site-year)")
        print(f"  cache dir  : {cache_dir if cache_dir else '(disabled)'}")
        print(f"  out dir    : {args.out_dir}")
        print(f"  comparisons: {len(COMPARISON_PLAN)} per site")
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, object]] = []
    nan_rates: dict[str, float] = {}
    daily_frames: list[pd.DataFrame] = []

    for site in sites:
        print(f"[{site.name}] {site.regime}")
        raw = load_site_hourly(site, years, cache_dir=cache_dir)
        hourly = build_hourly_frame(site, raw)

        nan_rate = float(hourly["liljegren_c"].isna().mean())
        nan_rates[site.name] = nan_rate
        print(f"    Liljegren NaN rate: {nan_rate:.4f}")

        daily = build_daily_frame(hourly)
        if months is not None:
            daily = daily[daily.index.month.isin(sorted(months))]
        print(f"    {len(daily)} complete days after filtering")

        for comparison in compare_site(site.name, daily):
            all_rows.append(comparison.to_row())

        frame = daily.copy()
        frame.insert(0, "site", site.name)
        daily_frames.append(frame)

    rows = pd.DataFrame(all_rows)

    daily_path = args.out_dir / "daily_series.parquet"
    pd.concat(daily_frames).to_parquet(daily_path)

    scores_path = args.out_dir / "scores.csv"
    rows.to_csv(scores_path, index=False)

    report_path = args.out_dir / "README.md"
    report_path.write_text(
        render_markdown(rows, years=years, months=months, nan_rates=nan_rates),
        encoding="utf-8",
    )

    meta_path = args.out_dir / "run_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "generated_utc": pd.Timestamp.utcnow().isoformat(),
                "sites": [s.name for s in sites],
                "years": years,
                "months": sorted(months) if months else None,
                "source": "Open-Meteo ERA5 archive",
                "references": {
                    "outdoor": "Liljegren et al. (2008) via thermofeel",
                    "indoor": "Bernard et al. (1999) via Lemke & Kjellstrom (2012)",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(f"Wrote {report_path}")
    print(f"      {scores_path}")
    print(f"      {daily_path}")
    print(f"      {meta_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
