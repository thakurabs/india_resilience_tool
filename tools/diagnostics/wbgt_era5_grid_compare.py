"""Compare IRT's deployed WBGT formulas against the Lemke & Kjellstrom reference
methods on a gridded ERA5 day.

Four methods, all driven by the *same* hourly ERA5 fields so that driver error
cancels and what remains is method error:

    irt_shade    IRT deployed shade WBGT   0.7*Twb_Stull + 0.3*Ta
    irt_swbgt    IRT deployed sWBGT        0.567*Ta + 0.393*e + 3.94
    bernard      reference INDOOR/shade    0.67*Tpwb + 0.33*Ta   (Bernard 1999)
    liljegren    reference OUTDOOR         energy balance        (Liljegren 2008)

Each is reduced over the local (IST) day three ways:

    _day_mean   mean of the hourly WBGT series
    _day_peak   max  of the hourly WBGT series
    _deployed   the IRT formula evaluated on daily-MEAN Ta and RH -- which is
                what ``heat_stress_gridfirst`` actually ships. Only defined for
                the two IRT methods; the references have no deployed analogue.

The gap between ``_day_peak`` and ``_deployed`` for the same IRT method is
aggregation error. The gap between ``irt_*_day_peak`` and the matching
reference ``_day_peak`` is formula error. Reporting them separately is the
whole point -- see CHG-0546.

Reads the two NetCDF files CDS returns for a mixed instantaneous/accumulated
request -- a request spanning both step types is split server-side and arrives
as a zip, so the archive must be extracted before this runs. Reads nothing
under ``IRT_DATA_DIR`` and touches no config, master or bundle.

    python -m tools.diagnostics.wbgt_era5_grid_compare --dry-run
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from tools.diagnostics.wbgt_method_validation import cos_solar_zenith
from tools.diagnostics.wbgt_deployed_vs_reference import (
    IST_OFFSET,
    bernard_indoor_wbgt_c,
    irt_swbgt_empirical_c,
    irt_wbgt_shade_stull_c,
    liljegren_wbgt_c,
    saturation_vapour_pressure_hpa,
)

DEFAULT_IN_DIR = Path("scratch/wbgt_era5_grid")
DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_era5_grid")

INSTANT_FILE = "data_stream-oper_stepType-instant.nc"
ACCUM_FILE = "data_stream-oper_stepType-accum.nc"

#: candidate, reference, label
COMPARISON_PLAN = (
    ("irt_shade_day_peak", "bernard_day_peak", "shade, peak vs peak (formula only)"),
    ("irt_shade_day_mean", "bernard_day_mean", "shade, mean vs mean (formula only)"),
    ("irt_shade_deployed", "bernard_day_peak", "shade, AS DEPLOYED vs reference peak"),
    ("irt_swbgt_day_peak", "liljegren_day_peak", "outdoor, peak vs peak (formula only)"),
    ("irt_swbgt_day_mean", "liljegren_day_mean", "outdoor, mean vs mean (formula only)"),
    ("irt_swbgt_deployed", "liljegren_day_peak", "outdoor, AS DEPLOYED vs reference peak"),
    ("irt_shade_deployed", "liljegren_day_peak", "shipped shade vs outdoor reference"),
)


def load_hourly(in_dir: Path) -> xr.Dataset:
    """Merge the instantaneous and accumulated CDS files onto one hourly grid."""
    inst = xr.open_dataset(in_dir / INSTANT_FILE)
    accum = xr.open_dataset(in_dir / ACCUM_FILE)
    merged = xr.merge([inst, accum], join="inner")
    for name in ("number", "expver"):
        if name in merged.coords:
            merged = merged.drop_vars(name)
    return merged


def derive_fields(ds: xr.Dataset) -> xr.Dataset:
    """Convert raw ERA5 to the units each WBGT method expects.

    ERA5 traps handled here:
      * ``sp`` is Pa; thermofeel wants hPa.
      * ``ssrd``/``fdir`` are J m-2 accumulated over the PRECEDING hour, not
        W m-2. Dividing by 3600 gives the mean flux over that hour, which is
        paired with the instantaneous fields at the same stamp -- a <=30 min
        phase error, negligible against a daily max near solar noon.
      * thermofeel wants ``fdir`` as a 0-1 FRACTION of ``ssrd``, not a flux.
      * ERA5 has no 2 m RH, so it is derived from Ta and Td with the same
        Magnus form Bernard uses, keeping the humidity definition identical
        across all four methods.
    """
    t_c = ds["t2m"] - 273.15
    td_c = ds["d2m"] - 273.15
    rh = 100.0 * (
        saturation_vapour_pressure_hpa(td_c.to_numpy())
        / saturation_vapour_pressure_hpa(t_c.to_numpy())
    )
    ssrd_w = ds["ssrd"] / 3600.0
    fdir_w = ds["fdir"] / 3600.0
    with np.errstate(invalid="ignore", divide="ignore"):
        fdir_frac = xr.where(ssrd_w > 0.0, fdir_w / ssrd_w, 0.0)

    out = xr.Dataset(
        {
            "t_c": t_c,
            "td_c": td_c,
            "rh": (t_c.dims, np.clip(rh, 0.0, 100.0)),
            "sp_hpa": ds["sp"] / 100.0,
            "wind_ms": np.sqrt(ds["u10"] ** 2 + ds["v10"] ** 2),
            "ssrd_w": ssrd_w,
            "fdir_frac": fdir_frac.clip(0.0, 1.0),
        }
    )
    return out


def grid_cossza(ds: xr.Dataset) -> np.ndarray:
    """Cosine of the solar zenith angle on the (time, lat, lon) grid.

    ``cos_solar_zenith`` is written for one site, so it is called once per cell
    against the shared UTC time index. 289 calls over 48 steps is negligible,
    and it keeps the solar geometry identical to the six-site harness rather
    than re-deriving it.
    """
    times = pd.DatetimeIndex(ds["valid_time"].to_numpy())
    lats = ds["latitude"].to_numpy()
    lons = ds["longitude"].to_numpy()
    out = np.empty((times.size, lats.size, lons.size), dtype=float)
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            out[:, i, j] = cos_solar_zenith(float(lat), times, float(lon))
    return out


def add_hourly_wbgt(ds: xr.Dataset) -> xr.Dataset:
    """Evaluate all four methods on every (time, lat, lon) cell."""
    shape = ds["t_c"].shape
    cossza = grid_cossza(ds).ravel()
    t_c = ds["t_c"].to_numpy().ravel()
    td_c = ds["td_c"].to_numpy().ravel()
    rh = ds["rh"].to_numpy().ravel()

    ds = ds.assign(
        irt_shade=(ds["t_c"].dims, irt_wbgt_shade_stull_c(t_c, rh).reshape(shape)),
        irt_swbgt=(ds["t_c"].dims, irt_swbgt_empirical_c(t_c, rh).reshape(shape)),
        bernard=(ds["t_c"].dims, bernard_indoor_wbgt_c(t_c, td_c).reshape(shape)),
        liljegren=(
            ds["t_c"].dims,
            liljegren_wbgt_c(
                t_c,
                rh,
                ds["sp_hpa"].to_numpy().ravel(),
                ds["wind_ms"].to_numpy().ravel(),
                ds["ssrd_w"].to_numpy().ravel(),
                ds["fdir_frac"].to_numpy().ravel(),
                cossza,
            ).reshape(shape),
        ),
    )
    return ds


def reduce_to_local_day(ds: xr.Dataset) -> pd.DataFrame:
    """Reduce to one row per (local day, lat, lon), keeping only complete days.

    IST is UTC+05:30, so a UTC day boundary cuts through the local afternoon.
    Partial local days are dropped rather than reduced, because a daily max
    over a truncated day is exactly the artefact under investigation.
    """
    local = pd.to_datetime(ds["valid_time"].to_numpy()) + IST_OFFSET
    ds = ds.assign_coords(local_day=("valid_time", local.normalize()))

    frame = ds.to_dataframe().reset_index()
    keys = ["local_day", "latitude", "longitude"]
    counts = frame.groupby(keys)["t_c"].transform("size")
    frame = frame[counts == 24]
    if frame.empty:
        raise SystemExit(
            "No complete local day in the input. Fetch two consecutive UTC days."
        )

    grouped = frame.groupby(keys)
    daily = pd.DataFrame(
        {
            "irt_shade_day_mean": grouped["irt_shade"].mean(),
            "irt_shade_day_peak": grouped["irt_shade"].max(),
            "irt_swbgt_day_mean": grouped["irt_swbgt"].mean(),
            "irt_swbgt_day_peak": grouped["irt_swbgt"].max(),
            "bernard_day_mean": grouped["bernard"].mean(),
            "bernard_day_peak": grouped["bernard"].max(),
            "liljegren_day_mean": grouped["liljegren"].mean(),
            "liljegren_day_peak": grouped["liljegren"].max(),
            "t_c_day_mean": grouped["t_c"].mean(),
            "t_c_day_peak": grouped["t_c"].max(),
            "rh_day_mean": grouped["rh"].mean(),
        }
    ).reset_index()

    # What heat_stress_gridfirst actually ships: the formula on daily means.
    daily["irt_shade_deployed"] = irt_wbgt_shade_stull_c(
        daily["t_c_day_mean"].to_numpy(), daily["rh_day_mean"].to_numpy()
    )
    daily["irt_swbgt_deployed"] = irt_swbgt_empirical_c(
        daily["t_c_day_mean"].to_numpy(), daily["rh_day_mean"].to_numpy()
    )
    return daily


def score(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for candidate, reference, label in COMPARISON_PLAN:
        pair = daily[[candidate, reference]].dropna()
        diff = pair[candidate] - pair[reference]
        rows.append(
            {
                "comparison": label,
                "candidate": candidate,
                "reference": reference,
                "n_cells": len(pair),
                "ref_mean_C": pair[reference].mean(),
                "cand_mean_C": pair[candidate].mean(),
                "bias_C": diff.mean(),
                "min_bias_C": diff.min(),
                "max_bias_C": diff.max(),
                "rmse_C": float(np.sqrt((diff**2).mean())),
                "r": pair[candidate].corr(pair[reference]),
            }
        )
    return pd.DataFrame(rows)


def render_markdown(daily: pd.DataFrame, scores: pd.DataFrame, in_dir: Path) -> str:
    days = sorted(daily["local_day"].dt.date.unique())
    lines = [
        "# Gridded ERA5 WBGT: IRT deployed vs Lemke & Kjellstrom reference methods",
        "",
        f"**Local days (IST):** {', '.join(str(d) for d in days)}  ",
        f"**Cells:** {daily[['latitude', 'longitude']].drop_duplicates().shape[0]} "
        f"at 0.25 deg  ",
        f"**Driver:** ERA5 hourly single levels, CDS (`{in_dir}`)  ",
        "**References:** Liljegren et al. (2008) outdoor; Bernard et al. (1999) indoor.",
        "",
        "All four methods share one hourly driver, so driver error cancels and what",
        "remains is method error.",
        "",
        "## Scores",
        "",
        "`bias` is candidate minus reference, averaged over cells.",
        "",
        "| comparison | n | ref mean C | cand mean C | bias C | min bias | max bias | RMSE C | r |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in scores.itertuples(index=False):
        lines.append(
            f"| {row.comparison} | {row.n_cells} | {row.ref_mean_C:.2f} | "
            f"{row.cand_mean_C:.2f} | {row.bias_C:+.2f} | {row.min_bias_C:+.2f} | "
            f"{row.max_bias_C:+.2f} | {row.rmse_C:.2f} | {row.r:.3f} |"
        )
    lines += [
        "",
        "## Reading this table",
        "",
        "- `peak vs peak` isolates **formula** error: both sides see the same hourly",
        "  series and the same reduction.",
        "- `AS DEPLOYED vs reference peak` adds **aggregation** error, because the IRT",
        "  side is the formula evaluated on daily-mean Ta and RH, which is what the",
        "  pipeline ships.",
        "- The spread between `min bias` and `max bias` is the part that matters for a",
        "  frozen national CDF ruler: a uniform offset barely moves ranks, a spatially",
        "  varying one reorders districts.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in-dir", type=Path, default=DEFAULT_IN_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.dry_run:
        print(f"would read  {args.in_dir / INSTANT_FILE}")
        print(f"would read  {args.in_dir / ACCUM_FILE}")
        print(f"would write {args.out_dir / 'per_cell.csv'}")
        print(f"would write {args.out_dir / 'README.md'}")
        print(f"{len(COMPARISON_PLAN)} scored comparisons")
        return 0

    ds = add_hourly_wbgt(derive_fields(load_hourly(args.in_dir)))
    nan_rate = float(np.isnan(ds["liljegren"].to_numpy()).mean())
    print(f"Liljegren NaN rate (hourly, all cells): {nan_rate:.4f}")

    daily = reduce_to_local_day(ds)
    scores = score(daily)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    daily.to_csv(args.out_dir / "per_cell.csv", index=False)
    (args.out_dir / "README.md").write_text(
        render_markdown(daily, scores, args.in_dir), encoding="utf-8"
    )

    print(scores.to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    print(f"\nwrote {args.out_dir / 'per_cell.csv'}")
    print(f"wrote {args.out_dir / 'README.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
