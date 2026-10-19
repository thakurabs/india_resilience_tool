"""Run CarbonPlan shade WBGT, area-weighted, for Kerala units (SPEC.md steps 1-5).

Run in the pinned py3.10 environment (xclim 0.44.0, thermofeel 1.3.0) documented
in ../wbgt_outdoor_carbonplan_reproduction/README.md, from the repository root,
after prepare_inputs.py. The formula code and QDM come from the predecessor
reproduction and CarbonPlan's checksum-locked notebook 06; nothing is re-fitted.

The population-weighted correctness check runs first. If it fails, the run stops
before any pilot unit value is written.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import thermofeel as tf
import xarray as xr
import xclim
from xclim import sdba
from xclim.sdba.adjustment import QuantileDeltaMapping

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts"))
from reproduce import load_function, raw_shade, verify_sources  # noqa: E402

WORK = Path("scratch/carbonplan_kerala")
UPSTREAM = Path("scratch/carbonplan_reproduction/upstream")
MODEL = "ACCESS-CM2"
YEARS = (1990, 2010)
THRESHOLDS = (28, 30, 32)
SLUGS = ("annual_mean",) + tuple(f"days_ge_{t}" for t in THRESHOLDS)
CHECK_REGIONS = 3
COVERAGE_FLAG = .95
INPUT_VERSION_TOLERANCE_C = 1e-3


def cell_shade(drivers: dict, elevation: np.ndarray) -> xr.DataArray:
    """Notebook 02 raw shade per cell, the same operations and dtypes as reproduce.raw_shade."""
    time = pd.DatetimeIndex(drivers["time"])
    values = {}
    for var in ("tas", "tasmax", "huss"):
        values[var] = xr.DataArray(drivers[var].astype("float32"), dims=("time", "cell"),
                                   coords={"time": time.values})
        values[var].attrs["units"] = "1" if var == "huss" else "K"
    pressure = 101325 * 10 ** (-elevation.astype("float32") / (18400 * values["tas"] / 273.15))
    pressure.attrs["units"] = "Pa"
    rh = xclim.indices.relative_humidity(tas=values["tasmax"], huss=values["huss"], ps=pressure)
    tmax = values["tasmax"]
    wind = (values["tas"] - values["tas"]) + .5
    wet = tf.thermofeel.calculate_wbt(tmax - 273.15, rh)
    globe = tf.thermofeel.calculate_bgt(tmax, tmax, wind)
    return .7 * wet + .2 * globe + .1 * (tmax - 273.15)


def weighted(wbgt: xr.DataArray, cells: pd.DataFrame, cell_index: dict, column: str) -> xr.DataArray:
    """Region series as the weighted sum over its cells, skipna=False like reproduce.raw_shade."""
    idx = [cell_index[(a, b)] for a, b in zip(cells.lat, cells.lon)]
    out = (wbgt.isel(cell=idx) * xr.DataArray(cells[column].values, dims="cell")).sum("cell", skipna=False)
    out.attrs["units"] = "degC"
    return out


def qdm(train, raw: xr.DataArray, reference: xr.DataArray) -> xr.DataArray:
    """Notebook 06: train on 1985-2014, adjust the original series, back to Gregorian."""
    trained = train(reference, raw, MODEL)
    corrected = trained.adjust(raw).compute()
    return corrected.convert_calendar("gregorian", align_on="year", missing=np.nan,
                                      use_cftime=None).interpolate_na(dim="time", method="linear")


def counts(values: np.ndarray) -> dict:
    """Inclusive threshold counts."""
    return {f"days_ge_{t}": int((values >= t).sum()) for t in THRESHOLDS}


def irt_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    """IRT conventions: drop Feb 29, complete 365-day years, mean and inclusive counts."""
    d = daily[~((daily.index.month == 2) & (daily.index.day == 29))]
    d = d.loc[f"{YEARS[0]}":f"{YEARS[1]}"]
    sizes = d.groupby(d.index.year).size()
    if not (sizes == 365).all() or len(sizes) != YEARS[1] - YEARS[0] + 1:
        raise ValueError("Incomplete 365-day years")
    if not np.isfinite(d.values).all():
        raise ValueError("Non-finite corrected values")
    g = d.groupby(d.index.year)
    out = {"annual_mean": g.mean()}
    for t in THRESHOLDS:
        out[f"days_ge_{t}"] = (d >= t).groupby(d.index.year).sum().astype(float)
    frames = []
    for slug, table in out.items():
        long = table.stack().rename("value").reset_index()
        long.columns = ["year", "key", "value"]
        long["slug"] = slug
        frames.append(long)
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    """Check wiring with population weights, then produce area-weighted unit values."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--out-dir", type=Path, default=WORK / "run")
    parser.add_argument("--weights", choices=("area", "population"), default="area",
                        help="cell -> CarbonPlan-unit weights (SPEC step 2); unit -> IRT unit stays area")
    args = parser.parse_args()
    weight_column = {"area": "area_weight", "population": "population_weight"}[args.weights]
    work, out = args.work_dir.resolve(), args.out_dir.resolve()
    if "scratch" not in out.parts or out.exists():
        raise ValueError("Choose a fresh output directory under scratch")
    if xclim.__version__ != "0.44.0" or importlib.metadata.version("thermofeel") != "1.3.0":
        raise ValueError("The pinned xclim 0.44.0 and thermofeel 1.3.0 are required")
    lock = verify_sources(UPSTREAM.resolve())
    out.mkdir(parents=True)

    cells = pd.read_csv(work / "cells.csv")
    crosswalk = pd.read_csv(work / "crosswalk.csv")
    drivers = dict(np.load(work / "drivers.npz"))
    series = np.load(work / "region_series.npz")
    sdates = pd.DatetimeIndex(series["series_dates"])
    pids = list(series["series_processing_ids"])
    uhe = pd.DataFrame(series["uhe"], index=sdates, columns=pids)
    published = pd.DataFrame(series["published_shade"], index=sdates, columns=pids)

    cell_index = {(a, b): i for i, (a, b) in enumerate(zip(drivers["lat"], drivers["lon"]))}
    elevation = (cells.drop_duplicates(["lat", "lon"]).set_index(["lat", "lon"]).elevation_m
                 .reindex(list(zip(drivers["lat"], drivers["lon"]))).values)
    if not np.isfinite(elevation).all():
        raise ValueError("Cell elevation missing for a driver cell")
    wbgt = cell_shade(drivers, elevation)
    time = pd.DatetimeIndex(drivers["time"])
    print(f"Raw shade: {wbgt.sizes['time']} days x {wbgt.sizes['cell']} cells", flush=True)

    train = load_function(UPSTREAM / "06_bias_correction.ipynb", "train_bias_correction",
                          {"np": np, "sdba": sdba, "QuantileDeltaMapping": QuantileDeltaMapping})

    def reference(pid: int) -> xr.DataArray:
        return xr.DataArray(uhe[pid].values, dims="time", coords={"time": sdates.values},
                            attrs={"units": "degC"})

    # --- Correctness check: CarbonPlan's own population weights must reproduce published shade.
    kerala_area = crosswalk[crosswalk.level == "district"].groupby("processing_id").intersection_m2.sum()
    check_ids = list(kerala_area.drop(-1, errors="ignore").nlargest(CHECK_REGIONS).index)
    check_rows, caught_all = [], set()
    for pid in check_ids:
        rc = cells[cells.processing_id == pid]
        raw = weighted(wbgt, rc, cell_index, "population_weight")
        # Identity guard: the vectorised path equals the predecessor's raw_shade exactly.
        long = pd.DataFrame({"date": np.repeat(time.values, len(rc)),
                             "cell": np.tile(np.arange(len(rc)), len(time))})
        idx = [cell_index[(a, b)] for a, b in zip(rc.lat, rc.lon)]
        for var in ("tas", "tasmax", "huss"):
            long[var] = drivers[var][:, idx].ravel()
        legacy, _ = raw_shade(long, rc.assign(weight=rc.population_weight.values).reset_index(drop=True))
        identity = float(np.abs(legacy.values - raw.values).max())
        if identity > 1e-9:
            raise ValueError(f"Vectorised raw shade differs from reproduce.raw_shade by {identity}")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            corrected = qdm(train, raw, reference(pid))
        caught_all |= {str(w.message) for w in caught}
        mine = pd.Series(corrected.values, index=pd.DatetimeIndex(corrected.time.values).normalize())
        target = published[pid].reindex(mine.index)
        diff = (mine - target).abs()
        row = {"processing_id": pid, "hierid": rc.hierid.iloc[0], "cells": len(rc), "days": len(mine),
               "max_abs_c": float(diff.max()), "days_over_1e3": int((diff > INPUT_VERSION_TOLERANCE_C).sum()),
               "raw_identity_c": identity}
        for k, v in counts(mine.values).items():
            row[k], row["published_" + k] = v, counts(target.values)[k]
        row["counts_match"] = all(row[k] == row["published_" + k] for k in counts(mine.values))
        check_rows.append(row)
        print("  check", json.dumps(row), flush=True)
    check = pd.DataFrame(check_rows)
    check.to_csv(out / "correctness_check.csv", index=False)
    if not check.counts_match.all():
        raise SystemExit("Correctness check FAILED: population-weighted counts differ from published; stopping")
    print("Correctness check PASSED", flush=True)

    # --- Pilot: area-weighted regions, QDM per region, IRT metrics.
    corrected_regions = {}
    for pid in pids:
        rc = cells[cells.processing_id == pid]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            corrected = qdm(train, weighted(wbgt, rc, cell_index, weight_column), reference(pid))
        caught_all |= {str(w.message) for w in caught}
        corrected_regions[pid] = pd.Series(corrected.values,
                                           index=pd.DatetimeIndex(corrected.time.values).normalize())
    region_daily = pd.DataFrame(corrected_regions)
    region_daily.index.name = "date"
    region_daily.to_csv(out / "region_daily_corrected.csv")
    print(f"QDM done for {len(pids)} regions", flush=True)

    region_metrics = irt_metrics(region_daily).rename(columns={"key": "processing_id"})
    region_metrics.to_csv(out / "region_yearly.csv", index=False)

    cw = crosswalk[crosswalk.processing_id >= 0].copy()
    missing = set(cw.processing_id) - set(pids)
    if missing:
        raise ValueError(f"Crosswalk regions without series: {sorted(missing)}")
    keys = ["level", "district", "block"]
    cw["block"] = cw.block.fillna("")
    cw["w"] = cw.intersection_m2 / cw.groupby(keys).intersection_m2.transform("sum")

    # Primary: region-first (metric per region-year, then area-weighted to the unit).
    merged = cw.merge(region_metrics, on="processing_id")
    merged["wv"] = merged.w * merged.value
    primary = merged.groupby(keys + ["slug", "year"]).wv.sum().rename("value_region_first")

    # Secondary: metric on the area-weighted unit daily series.
    rows = []
    for key, group in cw.groupby(keys):
        unit = (region_daily[group.processing_id.values] * group.w.values).sum(axis=1, skipna=False)
        m = irt_metrics(unit.to_frame("unit"))
        for column, value in zip(keys, key):
            m[column] = value
        rows.append(m.drop(columns="key"))
    secondary = pd.concat(rows).set_index(keys + ["slug", "year"]).value.rename("value_unit_series")

    yearly = pd.concat([primary, secondary], axis=1).reset_index()
    cover = cw.groupby(keys).coverage.first().rename("coverage").reset_index()
    nreg = cw.groupby(keys).processing_id.nunique().rename("regions").reset_index()
    yearly = yearly.merge(cover, on=keys).merge(nreg, on=keys)
    yearly["coverage_flag"] = yearly.coverage < COVERAGE_FLAG
    yearly["model"], yearly["scenario"] = MODEL, "historical"
    yearly["method"] = f"carbonplan-shade-{args.weights}-weighted-pilot"
    yearly.to_csv(out / "unit_yearly.csv", index=False)
    periods = (yearly.groupby(keys + ["slug"])[["value_region_first", "value_unit_series"]].mean()
               .reset_index().merge(cover, on=keys).merge(nreg, on=keys))
    periods["period"] = f"{YEARS[0]}-{YEARS[1]}"
    periods.to_csv(out / "unit_periods.csv", index=False)

    manifest = {"source_lock": lock, "model": MODEL, "years": list(YEARS), "regions": len(pids),
                "cell_to_unit_weights": weight_column, "unit_to_irt_weights": "intersection area",
                "check_processing_ids": [int(p) for p in check_ids],
                "correctness_check": check.to_dict(orient="records"),
                "units": {lvl: int(periods[periods.level == lvl][keys].drop_duplicates().shape[0])
                          for lvl in ("district", "block")},
                "units_below_coverage_flag": cover[cover.coverage < COVERAGE_FLAG].to_dict(orient="records"),
                "qdm_warnings": sorted(caught_all),
                "package_versions": {n: importlib.metadata.version(n)
                                     for n in ("numpy", "pandas", "xarray", "xclim", "thermofeel", "scipy")}}
    (out / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    print(periods[periods.level == "district"].pivot(index="district", columns="slug",
                                                     values="value_region_first").round(2).to_string(),
          flush=True)


if __name__ == "__main__":
    main()
