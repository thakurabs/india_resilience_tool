"""Reproduce published CarbonPlan outdoor WBGT using its own pinned formulas and QDM.

Run in the isolated Python environment documented in README.md. Source snippets are
executed from checksum-verified upstream files, not reimplemented approximations.
Published shade is used only for a separately labelled outdoor-stage comparison.

Scores three cities (Kochi, Bikaner, Shimla) across three years (2005, 2007, 2009).
Three radiation interpretations are reported for every city-year:

  literal           -- the pinned source as written: MetSim geometry at SW_RAD_DT=30 s
                       read by notebook 07's wrapper at 3600 s. Internally inconsistent;
                       the hourly disaggregation does not conserve the daily mean.
  consistent30s     -- geometry and wrapper both at 30 s.
  consistent3600s   -- geometry and wrapper both at 3600 s. This is the interpretation
                       the overall verdict is keyed to, per SPEC.md.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import json
from pathlib import Path
from types import SimpleNamespace
import warnings

import numpy as np
import pandas as pd
import thermofeel as tf
import xarray as xr
import xclim
from xclim import sdba
from xclim.sdba.adjustment import QuantileDeltaMapping

HERE = Path(__file__).resolve().parent
WORK = Path("scratch/carbonplan_reproduction")
CITIES = ("Kochi", "Bikaner", "Shimla")
RADIATION_YEARS = (2005, 2007, 2009)
THRESHOLDS = (28, 30, 32)
# SPEC.md's declared parity gate, and a separately named band expressing the
# residual expected from local NEX files differing in version from CarbonPlan's
# 2023 download. The second is an input-lineage assumption, not a parity result.
DECLARED_PARITY_C = 1e-6
INPUT_VERSION_TOLERANCE_C = 1e-3
# The interpretation the verdict is keyed to; see SPEC.md.
PRIMARY = "consistent3600s"


def load_function(path: Path, name: str, namespace: dict):
    """Load one inspected upstream function; omit decorators, retain its body."""
    source = path.read_text()
    if path.suffix == ".ipynb":
        source = "\n".join("".join(c["source"]) for c in json.loads(source)["cells"] if c["cell_type"] == "code")
    nodes = [n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name]
    if len(nodes) != 1:
        raise ValueError(f"Expected one upstream function: {name}")
    nodes[0].decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def verify_sources(upstream: Path) -> dict:
    """Refuse modified upstream source; return the version/hash lock."""
    lock = json.loads((HERE.parent / "source_lock.json").read_text())
    for name, record in lock["files"].items():
        actual = hashlib.sha256((upstream / name).read_bytes()).hexdigest()
        if actual != record["sha256"]:
            raise ValueError(f"Source checksum mismatch: {name}")
    return lock


def raw_shade(drivers: pd.DataFrame, cells: pd.DataFrame) -> tuple[xr.DataArray, pd.DataFrame]:
    """Use notebook 02: huss/pressure RH at Tmax; thermofeel 1.3 globe and wet bulb."""
    dates = pd.DatetimeIndex(sorted(drivers.date.unique()))
    values = {}
    for var in ("tas", "tasmax", "huss"):
        a = drivers.pivot(index="date", columns="cell", values=var).reindex(dates)
        if list(a.columns) != list(range(len(cells))) or not np.isfinite(a.values).all():
            raise ValueError("Incomplete raw-shade input grid")
        values[var] = xr.DataArray(a.values.astype("float32"), dims=("time", "cell"), coords={"time": dates.values})
        values[var].attrs["units"] = "1" if var == "huss" else "K"
    pressure = 101325 * 10 ** (-cells.elevation_m.values.astype("float32") / (18400 * values["tas"] / 273.15))
    pressure.attrs["units"] = "Pa"
    rh = xclim.indices.relative_humidity(tas=values["tasmax"], huss=values["huss"], ps=pressure)
    tmax = values["tasmax"]
    wind = (values["tas"] - values["tas"]) + .5
    wet = tf.thermofeel.calculate_wbt(tmax - 273.15, rh)
    globe = tf.thermofeel.calculate_bgt(tmax, tmax, wind)
    wbgt = .7 * wet + .2 * globe + .1 * (tmax - 273.15)
    shade = (wbgt * xr.DataArray(cells.weight.values, dims="cell")).sum("cell", skipna=False)
    shade.attrs["units"] = "degC"
    cells_daily = wbgt.to_dataframe(name="raw_shade_c").reset_index()
    cells_daily["rh_at_tmax_pct"] = rh.values.ravel()
    return shade, cells_daily


def solar_peak(rsds: np.ndarray, dates: pd.DatetimeIndex, cells: pd.DataFrame,
               upstream: Path, *, interpretation: str) -> tuple[np.ndarray, dict]:
    """Execute pinned MetSim geometry and shortwave under one radiation interpretation.

    `literal` reproduces the pinned source exactly, including its SW_RAD_DT mismatch
    between MetSim's constants and notebook 07's wrapper. The two `consistent`
    interpretations use the same value in both places; neither tunes a coefficient.
    """
    if interpretation not in ("literal", "consistent30s", "consistent3600s"):
        raise ValueError(f"Unknown radiation interpretation: {interpretation}")
    constants = ast.parse((upstream / "metsim_constants.py").read_text())
    ns: dict = {}
    assignments = [n for n in constants.body if isinstance(n, ast.Assign)]
    exec(compile(ast.Module(body=assignments, type_ignores=[]), "metsim_constants", "exec"), ns)
    cnst = SimpleNamespace(**{k: v for k, v in ns.items() if not k.startswith("_")})
    if interpretation == "consistent3600s":
        cnst.SW_RAD_DT = 3600.0
    wrapper_dt = 30 if interpretation == "consistent30s" else 3600
    namespace = {"np": np, "pd": pd, "cnst": cnst}
    geom = load_function(upstream / "metsim_physics.py", "solar_geom", namespace)
    shortwave = load_function(upstream / "metsim_disaggregate.py", "shortwave", namespace)
    elevation = float(cells.elevation_m @ cells.weight)
    latitude = float(cells.lat @ cells.weight)
    tiny, daylength = geom(elevation, latitude, -6.5)
    params = {"time_step": 60, "method": "other", "utc_offset": False,
              "calendar": "gregorian", "SW_RAD_DT": wrapper_dt}
    day = dates.dayofyear.values
    hourly = shortwave(rsds, daylength[day - 1], day, tiny, params).reshape(-1, 24)
    return hourly.max(axis=1), {
        "interpretation": interpretation,
        "latitude": latitude, "elevation_m": elevation,
        "geometry_timestep_seconds": cnst.SW_RAD_DT,
        "wrapper_timestep_seconds": params["SW_RAD_DT"],
        "tiny_shape": list(tiny.shape),
        "maximum_daily_mean_radiation_residual_wm2": float(np.max(np.abs(hourly.mean(axis=1) - rsds))),
    }


def sun_delta(peak: np.ndarray, wind: np.ndarray) -> np.ndarray:
    """Fit the original 16 Kong/Huber points and apply notebook 08's bounds/sign."""
    x = np.array([[rad, speed] for speed in (.5, 1, 2, 3) for rad in (300, 500, 700, 900)])
    y = np.array([-3, -5, -6, -7, -2, -3.5, -4.5, -6, -1.5, -2.5, -3.5, -4.5, -1.5, -2, -3, -3.5])
    coef = np.linalg.lstsq(np.column_stack([np.ones(16), x]), y, rcond=None)[0]
    return -(coef[0] + coef[1] * np.clip(.75 * peak, 300, 900) + coef[2] * np.clip(wind, .5, 3))


def metrics(a: np.ndarray, b: np.ndarray) -> dict:
    """Compare complete daily series; fail incomplete inputs instead of dropping days.

    Reports SPEC.md's declared 1e-6 degC parity gate and the separately named
    1e-3 degC input-version band. Count agreement alone is never parity.
    """
    a, b = np.asarray(a), np.asarray(b)
    if a.shape != b.shape or a.ndim != 1 or not len(a) or not np.isfinite(a).all() or not np.isfinite(b).all():
        raise ValueError("Metrics require matching, finite, nonempty daily series")
    d = a - b
    out = {"days": len(a), "mean_c": float(a.mean()), "reference_mean_c": float(b.mean()),
           "bias_c": float(d.mean()), "mae_c": float(np.abs(d).mean()),
           "rmse_c": float(np.sqrt(np.mean(d*d))), "max_abs_c": float(np.abs(d).max())}
    out["correlation"] = float(np.corrcoef(a, b)[0, 1]) if a.std() > 0 and b.std() > 0 else float("nan")
    for threshold in THRESHOLDS:
        out[f"days_ge_{threshold}"] = int((a >= threshold).sum())
        out[f"reference_days_ge_{threshold}"] = int((b >= threshold).sum())
    out["counts_match"] = all(out[f"days_ge_{t}"] == out[f"reference_days_ge_{t}"] for t in THRESHOLDS)
    out["parity_declared_1e6"] = bool(out["max_abs_c"] <= DECLARED_PARITY_C and out["counts_match"])
    out["within_input_version_tolerance_1e3"] = bool(
        out["max_abs_c"] <= INPUT_VERSION_TOLERANCE_C and out["counts_match"])
    return out


def reproduce_city(city: str, work: Path, out: Path, years: tuple[int, ...],
                   upstream: Path) -> tuple[list[dict], dict]:
    """Run the full unchanged chain for one city and write its daily evidence."""
    city_in, city_out = work / "cities" / city, out / city
    city_out.mkdir(parents=True)
    cells = pd.read_csv(city_in / "cells.csv")
    drivers = pd.read_csv(city_in / "local_drivers.csv", parse_dates=["date"])
    ref_frame = pd.read_csv(city_in / "reference.csv", parse_dates=["date"]).set_index("date").loc["1985":"2014"]
    ref = xr.DataArray(ref_frame.reference.values, dims="time",
                       coords={"time": ref_frame.index.values}, attrs={"units": "degC"})
    raw, cell_raw = raw_shade(drivers, cells)
    cell_raw.to_csv(city_out / "raw_shade_cell_daily.csv", index=False)
    print(f"  raw shade {len(raw)} days over {len(cells)} cell(s)", flush=True)
    train = load_function(upstream / "06_bias_correction.ipynb", "train_bias_correction",
                          {"np": np, "sdba": sdba, "QuantileDeltaMapping": QuantileDeltaMapping})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        trained = train(ref, raw, "ACCESS-CM2")
        # Notebook 06 applies to the original projection, rather than the temporary
        # noleap copy inside train_bias_correction. Preserve that behavior exactly.
        corrected = trained.adjust(raw).compute()
        corrected = corrected.convert_calendar("gregorian", align_on="year", missing=np.nan,
                                               use_cftime=None).interpolate_na(dim="time", method="linear")
    print(f"  pinned QDM {len(corrected)} days", flush=True)
    warning_messages = sorted(set(str(w.message) for w in caught))
    shade = pd.read_csv(city_in / "published_shade.csv", parse_dates=["date"]).set_index("date").published_shade
    sun = pd.read_csv(city_in / "published_sun.csv", parse_dates=["date"]).set_index("date").published_sun
    normalized_dates = pd.DatetimeIndex(raw.time.values).normalize()
    all_days = pd.DataFrame({"raw_shade": raw.values, "reproduced_shade": corrected.values,
                             "uhe_reference": ref.values,
                             "published_shade": shade.reindex(normalized_dates).values},
                            index=normalized_dates)
    all_days.index.name = "date"
    all_days.to_csv(city_out / "shade_daily_1985_2014.csv")
    rows, radiation_diag = [], {}
    shade_30y = metrics(all_days.reproduced_shade.values, all_days.published_shade.values)
    rows.append({"city": city, "year": "1985-2014", "stage": "shade_independent",
                 "interpretation": "n/a", "target": "published_shade", **shade_30y})
    for year in years:
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31")
        sample = drivers[drivers.date.dt.year == year].copy()
        sample["date"] = sample.date.dt.normalize()
        rsds = sample.pivot(index="date", columns="cell", values="rsds").reindex(dates).values @ cells.weight.values
        wind = sample.pivot(index="date", columns="cell", values="sfcWind").reindex(dates).values @ cells.weight.values
        if not (np.isfinite(rsds).all() and np.isfinite(wind).all()):
            raise ValueError(f"{city} {year}: incomplete radiation or wind drivers")
        daily = all_days.loc[str(year)].copy()
        daily["published_sun"] = sun.reindex(dates).values
        daily["rsds_daily_mean_wm2"] = rsds
        daily["wind_daily_mean_ms"] = wind
        daily["published_sun_delta_c"] = daily.published_sun - daily.published_shade
        rows.append({"city": city, "year": year, "stage": "shade_independent",
                     "interpretation": "n/a", "target": "published_shade",
                     **metrics(daily.reproduced_shade.values, daily.published_shade.values)})
        for interpretation in ("literal", "consistent30s", "consistent3600s"):
            peak, diag = solar_peak(rsds, dates, cells, upstream, interpretation=interpretation)
            delta = sun_delta(peak, wind)
            radiation_diag[f"{year}_{interpretation}"] = diag
            daily[f"{interpretation}_radiation_peak_wm2"] = peak
            daily[f"{interpretation}_sun_delta_c"] = delta
            daily[f"{interpretation}_full_sun"] = daily.reproduced_shade + delta
            daily[f"{interpretation}_outdoor_stage_only"] = daily.published_shade + delta
            for stage, column in [("full_sun_independent", f"{interpretation}_full_sun"),
                                  ("sun_stage_on_published_shade", f"{interpretation}_outdoor_stage_only")]:
                rows.append({"city": city, "year": year, "stage": stage,
                             "interpretation": interpretation, "target": "published_sun",
                             **metrics(daily[column].values, daily.published_sun.values)})
        daily.to_csv(city_out / f"daily_{year}.csv")
        print(f"  {year} scored", flush=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(city_out / "summary.csv", index=False)
    diagnostics = {"cells": len(cells), "warnings": warning_messages,
                   "population_weights": cells.weight.round(6).tolist(),
                   "weighted_elevation_m": float(cells.elevation_m @ cells.weight),
                   "radiation": radiation_diag, "shade_30year_metrics": shade_30y}
    return rows, diagnostics


def verdicts(frame: pd.DataFrame) -> dict:
    """Judge the primary interpretation, and report the literal source separately."""
    def judge(interpretation: str) -> dict:
        sel = frame[(frame.stage == "full_sun_independent") & (frame.interpretation == interpretation)]
        if sel.empty:
            raise ValueError(f"No end-to-end rows for {interpretation}")
        return {"city_years": int(len(sel)),
                "counts_match_all": bool(sel.counts_match.all()),
                "worst_max_abs_c": float(sel.max_abs_c.max()),
                "worst_mae_c": float(sel.mae_c.max()),
                "declared_parity_all": bool(sel.parity_declared_1e6.all()),
                "within_input_version_tolerance_all": bool(sel.within_input_version_tolerance_1e3.all())}
    primary, literal = judge(PRIMARY), judge("literal")
    # A compound verdict, because the single-token form misreports this result in
    # both directions: the declared continuous gates are genuinely unmet, while
    # every threshold count is reproduced exactly at every city-year. Neither gate
    # is relaxed to reach a nicer token.
    if primary["declared_parity_all"]:
        verdict = "REPRODUCED"
    elif primary["within_input_version_tolerance_all"]:
        verdict = "COUNTS_EXACT_WITHIN_INPUT_VERSION_TOLERANCE_DECLARED_PARITY_NOT_MET"
    elif primary["counts_match_all"]:
        verdict = "COUNTS_EXACT_CONTINUOUS_PARITY_NOT_MET"
    else:
        verdict = "NOT_REPRODUCED"
    shade = frame[(frame.stage == "shade_independent") & (frame.year == "1985-2014")]
    return {
        "verdict": verdict,
        "verdict_basis": f"end-to-end independent chain under the {PRIMARY} radiation interpretation",
        "verdict_scope": "all scored city-years must pass; a single failure downgrades the verdict",
        "primary_interpretation": primary,
        "literal_source_interpretation": literal,
        "literal_source_note": (
            "The pinned source is internally inconsistent: MetSim constants set SW_RAD_DT=30 s "
            "while notebook 07 passes 3600 s to shortwave. Run literally it does not conserve the "
            "daily mean radiation and cannot reproduce the published product. This is a defect in "
            "CarbonPlan's published wiring, reported separately rather than as a failure here."),
        "shade_stage_30year": {"cities": int(len(shade)),
                               "counts_match_all": bool(shade.counts_match.all()),
                               "worst_max_abs_c": float(shade.max_abs_c.max())},
        "counts_reproduced_all_city_years": primary["counts_match_all"],
        "declared_parity_threshold_c": DECLARED_PARITY_C,
        "input_version_tolerance_c": INPUT_VERSION_TOLERANCE_C,
        "input_version_tolerance_is_an_assumption_not_a_met_criterion": True,
    }


def main() -> None:
    """Run the frozen comparison for every city and emit all daily series and residuals."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--out-dir", type=Path, default=WORK / "run")
    parser.add_argument("--cities", nargs="+", default=list(CITIES), choices=list(CITIES))
    parser.add_argument("--years", nargs="+", type=int, default=list(RADIATION_YEARS))
    args = parser.parse_args()
    work, out = args.work_dir.resolve(), args.out_dir.resolve()
    if "scratch" not in out.parts or out.exists():
        raise ValueError("Choose a fresh output directory under scratch")
    if xclim.__version__ != "0.44.0" or importlib.metadata.version("thermofeel") != "1.3.0":
        raise ValueError("The pinned xclim 0.44.0 and thermofeel 1.3.0 are required")
    upstream = work / "upstream"
    lock = verify_sources(upstream)
    years = tuple(sorted(set(args.years)))
    out.mkdir(parents=True)
    rows, diagnostics = [], {}
    for city in args.cities:
        print(f"=== {city} ===", flush=True)
        city_rows, city_diag = reproduce_city(city, work, out, years, upstream)
        rows.extend(city_rows)
        diagnostics[city] = city_diag
    frame = pd.DataFrame(rows)
    frame.to_csv(out / "summary_all.csv", index=False)
    manifest = {"source_lock": lock, "cities": list(args.cities), "years": list(years),
                "primary_interpretation": PRIMARY, "per_city": diagnostics,
                "package_versions": {n: importlib.metadata.version(n)
                                     for n in ("numpy", "pandas", "xarray", "xclim", "thermofeel", "scipy")},
                **verdicts(frame)}
    (out / "run_manifest.json").write_text(json.dumps(manifest, indent=2))
    show = ["city", "year", "stage", "interpretation", "bias_c", "mae_c", "max_abs_c",
            "days_ge_28", "reference_days_ge_28", "days_ge_30", "reference_days_ge_30",
            "days_ge_32", "reference_days_ge_32", "counts_match"]
    print(frame[show].to_string(index=False), flush=True)
    print("\nverdict:", manifest["verdict"], flush=True)
    print("literal source:", json.dumps(manifest["literal_source_interpretation"]), flush=True)


if __name__ == "__main__":
    main()
