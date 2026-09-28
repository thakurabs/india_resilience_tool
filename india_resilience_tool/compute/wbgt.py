"""Sheltered shade daily-peak estimate and complete-calendar annual reductions.

Temperature contracts default to NEX Kelvin and humidity to percent only when
metadata is absent. No inference from observed extrema is performed.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from india_resilience_tool.data.wbgt_contract import (  # re-export public contract
    SHADE_SLUGS,
    SHADE_METHOD_SIGNATURE,
    require_shade_signature,
    shade_artifact_current,
)


__all__ = [
    "SHADE_SLUGS",
    "SHADE_METHOD_SIGNATURE",
    "require_shade_signature",
    "shade_artifact_current",
    "shade_daily",
    "shade_annual",
    "shade_value_key",
    "saturation_pressure",
]


def saturation_pressure(
    t_c: xr.DataArray | np.ndarray | float,
) -> xr.DataArray | np.ndarray | float:
    """Magnus saturation vapour pressure in hPa from degrees Celsius."""
    return 6.112 * np.exp(17.62 * t_c / (243.12 + t_c))


def _temperature(da: xr.DataArray) -> xr.DataArray:
    unit = str(da.attrs.get("units", "K")).lower().replace(" ", "")
    if unit in {"k", "kelvin"}:
        return da - 273.15
    if unit in {"c", "degc", "celsius", "degree_celsius", "degrees_celsius", "°c"}:
        return da
    raise ValueError(f"Unsupported shade temperature units: {unit!r}")


def shade_daily(
    tas: xr.DataArray, tasmax: xr.DataArray, hurs: xr.DataArray
) -> xr.Dataset:
    """Return daily shade peaks (C), RH and explicit input/domain diagnostics.

    Invalid humidity, nonfinite values, temperatures below absolute zero or
    tasmax below tas invalidate a day. Stull domain excursions are reported,
    not silently floored or discarded. Coordinates and identity must agree.
    """
    if tasmax is None:
        raise ValueError(
            "Shade daily maxima require tasmax alongside tas and hurs; rebuild the input inventory."
        )
    for key in ("source_id", "model", "experiment_id", "scenario", "variant_label"):
        identities = {str(a.attrs[key]) for a in (tas, tasmax, hurs) if key in a.attrs}
        if len(identities) > 1:
            raise ValueError(f"Shade input identity mismatch: {key}={identities}")
    if any(a.dims != tas.dims for a in (tasmax, hurs)):
        raise ValueError("Shade inputs require identical dimensions and coordinates")
    tas, tasmax, hurs = xr.align(tas, tasmax, hurs, join="exact")
    for coord in tas.coords:
        if any(
            coord not in a.coords or not tas[coord].equals(a[coord])
            for a in (tasmax, hurs)
        ):
            raise ValueError(f"Shade coordinate mismatch: {coord}")
    t, tx = _temperature(tas), _temperature(tasmax)
    unit = str(hurs.attrs.get("units", "%")).lower().strip()
    if unit in {"%", "percent", "percentage"}:
        rh = hurs
    elif unit in {"1", "fraction"}:
        rh = hurs * 100
    else:
        raise ValueError(f"Unsupported shade humidity units: {unit!r}")
    missing = ~(np.isfinite(t) & np.isfinite(tx) & np.isfinite(rh))
    physical = (tx < t) | (t <= -273.15) | (rh < 0) | (rh > 100)
    valid = ~missing & ~physical
    r = (
        rh * saturation_pressure(t.where(valid)) / saturation_pressure(tx.where(valid))
    ).where(valid)
    clipped = valid & ((r < 0) | (r > 100))
    r = r.clip(0, 100)
    tw = (
        tx * np.arctan(0.151977 * np.sqrt(r + 8.313659))
        + np.arctan(tx + r)
        - np.arctan(r - 1.676331)
        + 0.00391838 * r**1.5 * np.arctan(0.023101 * r)
        - 4.686035
    )
    peak = (0.7 * tw + 0.3 * tx).where(valid)
    peak = peak.where(np.isfinite(peak))
    return xr.Dataset(
        {
            "peak": peak,
            "rh_at_tasmax": r,
            "missing_input": missing,
            "physical_invalid": physical & ~missing,
            "rh_clipped": clipped,
            "stull_excursion": valid & ((tx < -20) | (tx > 50) | (r < 5) | (r > 99)),
        }
    )


def shade_annual(daily: xr.Dataset, year: int) -> xr.Dataset:
    """Reduce one source year, excluding Feb 29; incomplete cells remain NaN.

    Supports Gregorian/proleptic Gregorian/standard and 365-day calendars.
    Duplicate dates, subdaily timestamps and other calendars raise errors.
    Diagnostic counts and native support survive incomplete years.
    """
    time = daily.time
    calendar = str(time.dt.calendar)
    if calendar not in {
        "standard",
        "gregorian",
        "proleptic_gregorian",
        "noleap",
        "365_day",
    }:
        raise ValueError(f"Unsupported shade calendar: {calendar}")
    if bool((time.dt.year != year).any()):
        raise ValueError("Shade annual reduction requires a single requested year")
    dates = list(zip(time.dt.month.values.tolist(), time.dt.day.values.tolist()))
    if len(set(dates)) != len(dates):
        raise ValueError("Shade inputs contain duplicate dates or subdaily samples")
    d = daily.sel(time=~((time.dt.month == 2) & (time.dt.day == 29)))
    valid = np.isfinite(d.peak)
    count = valid.sum("time")
    complete = (count == 365) & (d.sizes["time"] == 365)
    out = xr.Dataset(
        {
            "annual_mean": d.peak.mean("time").where(complete),
            "valid_days": count,
            "incomplete_year": ~complete,
            "native_support": valid.any("time"),
            "missing_dates": xr.full_like(count, 365 - d.sizes["time"]),
        }
    )
    for threshold in (28, 30, 32):
        out[f"days_ge_{threshold}"] = (
            (d.peak >= threshold).sum("time").astype(float).where(complete)
        )
    for key in ("missing_input", "physical_invalid", "rh_clipped", "stull_excursion"):
        out[f"{key}_days"] = d[key].sum("time")
    out.attrs["shade_method_signature"] = SHADE_METHOD_SIGNATURE
    return out


def shade_value_key(slug: str) -> str:
    """Return the annual dataset field for a retained public shade slug."""
    if slug not in SHADE_SLUGS:
        raise ValueError(f"Not a shade metric: {slug}")
    return slug.removeprefix("wbgt_shade_stull_")
