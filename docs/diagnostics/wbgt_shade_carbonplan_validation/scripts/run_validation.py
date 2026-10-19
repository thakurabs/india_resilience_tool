"""Run the CarbonPlan shade-WBGT validation experiments (SPEC.md, CHG-0646).

Run in the pinned py3.10 environment (xclim 0.44.0, thermofeel 1.3.0) from the
repository root, after prepare.py. Single worker. Subcommands:

  fidelity   G1 raw parity, G2 historical and G3 future published-product
             reproduction, AW application-window diagnostic (Kerala, ACCESS-CM2)
  holdout    G1 + H1/W1/M1 for one state x model x weighting: train 1985-1999,
             apply 2000-2014 once; Kerala ACCESS-CM2 also trains 1985-2014 and
             applies the two published future windows (W1 future comparison)

Upstream notebook-06 code is AST-loaded from checksum-locked sources; nothing is
re-fitted beyond what SPEC.md declares. Outputs go to a fresh scratch directory.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import resource
import sys
import time
import warnings
from pathlib import Path

import cftime
import numpy as np
import pandas as pd
import thermofeel as tf
import xarray as xr
import xclim
from xclim import sdba
from xclim.sdba.adjustment import QuantileDeltaMapping

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts"))
from reproduce import load_function, raw_shade, verify_sources  # noqa: E402
from tools.diagnostics import wbgt_shade_cp_validation as V  # noqa: E402

WORK = Path("scratch/carbonplan_shade_validation")
UPSTREAM = Path("scratch/carbonplan_reproduction/upstream")
TRAIN, TEST = V.Window(1985, 1999), V.Window(2000, 2014)
FULL = V.Window(1985, 2014)
FUTURE = {"ssp245-2030": V.Window(2020, 2039), "ssp245-2050": V.Window(2040, 2059)}
AW_ALTERNATIVES = {"2020-2059": V.Window(2020, 2059), "2020-2080": V.Window(2020, 2080)}
AW_HIST_SPLIT = (V.Window(2000, 2007), V.Window(2008, 2014))
AW_UNITS = (19567, 20346, 20372)
G1_TOL = 1e-9
WEIGHT_COLUMN = {"population": "population_weight", "area": "area_weight"}


# --------------------------------------------------------------------------- inputs

def load_frame(path: Path) -> pd.DataFrame:
    z = np.load(path)
    return pd.DataFrame(z["values"], index=pd.DatetimeIndex(z["dates"]), columns=list(z["processing_id"]))


def driver_time(stamps: np.ndarray, calendar: str):
    """Rebuild the model's own time axis: datetime64 for Gregorian-type, cftime otherwise."""
    if calendar in ("proleptic_gregorian", "standard", "gregorian"):
        return pd.DatetimeIndex(stamps).values
    # The calendar-specific subclasses are what xarray's NEX decoding (and so CarbonPlan) yields;
    # generic cftime.datetime objects fail inside convert_calendar + interpolate_na.
    cls = {"365_day": cftime.DatetimeNoLeap, "noleap": cftime.DatetimeNoLeap,
           "360_day": cftime.Datetime360Day, "366_day": cftime.DatetimeAllLeap,
           "all_leap": cftime.DatetimeAllLeap}[calendar]
    parsed = pd.Series(stamps).str.extract(r"(\d+)-(\d+)-(\d+) (\d+)").astype(int).to_numpy()
    return xr.CFTimeIndex([cls(y, m, d, h) for y, m, d, h in parsed])


def load_drivers(work: Path, model: str, scenario: str, years: tuple[int, int], suffix: str = "") -> dict:
    path = work / f"drivers_{model}_{scenario}_{years[0]}_{years[1]}{suffix}.npz"
    if not path.exists():
        raise FileNotFoundError(f"{path} (model ineligible or drivers stage not run)")
    d = dict(np.load(path))
    d["calendar"] = str(d["calendar"])
    d["time"] = driver_time(d["time"], d["calendar"])
    return d


def cell_shade(drivers: dict, elevation: np.ndarray) -> xr.DataArray:
    """Notebook 02 raw shade per cell; same operations and dtypes as reproduce.raw_shade."""
    values = {}
    for var in ("tas", "tasmax", "huss"):
        values[var] = xr.DataArray(drivers[var].astype("float32"), dims=("time", "cell"),
                                   coords={"time": drivers["time"]})
        values[var].attrs["units"] = "1" if var == "huss" else "K"
    pressure = 101325 * 10 ** (-elevation.astype("float32") / (18400 * values["tas"] / 273.15))
    pressure.attrs["units"] = "Pa"
    rh = xclim.indices.relative_humidity(tas=values["tasmax"], huss=values["huss"], ps=pressure)
    tmax = values["tasmax"]
    wind = (values["tas"] - values["tas"]) + .5
    wet = tf.thermofeel.calculate_wbt(tmax - 273.15, rh)
    globe = tf.thermofeel.calculate_bgt(tmax, tmax, wind)
    return .7 * wet + .2 * globe + .1 * (tmax - 273.15)


class Shade:
    """Per-cell raw shade for one model/period and per-unit weighted series."""

    def __init__(self, drivers: dict, cells: pd.DataFrame):
        self.drivers = drivers
        self.index = {(a, b): i for i, (a, b) in enumerate(zip(drivers["lat"], drivers["lon"]))}
        elev = (cells.drop_duplicates(["lat", "lon"]).set_index(["lat", "lon"]).elevation_m
                .reindex(list(zip(drivers["lat"], drivers["lon"]))).to_numpy())
        if not np.isfinite(elev).all():
            raise ValueError("Missing elevation for a driver cell")
        self.wbgt = cell_shade(drivers, elev)

    def unit(self, rc: pd.DataFrame, column: str) -> xr.DataArray:
        idx = [self.index[(a, b)] for a, b in zip(rc.lat, rc.lon)]
        out = (self.wbgt.isel(cell=idx) * xr.DataArray(rc[column].to_numpy(), dims="cell")).sum(
            "cell", skipna=False)
        out.attrs["units"] = "degC"
        return out

    def legacy_identity(self, rc: pd.DataFrame, column: str, vectorised: xr.DataArray) -> float:
        """G1: max |vectorised - reproduce.raw_shade| for one unit (Gregorian-type calendars)."""
        idx = [self.index[(a, b)] for a, b in zip(rc.lat, rc.lon)]
        time_values = pd.DatetimeIndex(self.drivers["time"]) if not isinstance(
            self.drivers["time"], xr.CFTimeIndex) else None
        if time_values is None:
            # reproduce.raw_shade pivots on pandas dates, which cannot hold a cftime noleap axis;
            # compare against the same elementwise code path on an ordinal index instead.
            time_values = pd.date_range("2001-01-01", periods=len(self.drivers["time"]), freq="D")
        long = pd.DataFrame({"date": np.repeat(time_values.values, len(rc)),
                             "cell": np.tile(np.arange(len(rc)), len(time_values))})
        for var in ("tas", "tasmax", "huss"):
            long[var] = self.drivers[var][:, idx].ravel()
        legacy, _ = raw_shade(long, rc.assign(weight=rc[column].to_numpy()).reset_index(drop=True))
        return float(np.abs(legacy.to_numpy() - vectorised.to_numpy()).max())


def reference(uhe: pd.DataFrame, pid: int) -> xr.DataArray:
    return xr.DataArray(uhe[pid].to_numpy(), dims="time", coords={"time": uhe.index.values},
                        attrs={"units": "degC"})


def make_qdm(train_fn):
    """Notebook-06 train and apply (adjust in the original calendar, then back to Gregorian)."""
    def train(ref_ts, model_ts):
        return train_fn(ref_ts, model_ts, "model")

    def apply(model, ts):
        adjusted = model.adjust(ts).compute()
        converted = adjusted.convert_calendar("gregorian", align_on="year", missing=np.nan, use_cftime=None)
        gaps = pd.DatetimeIndex(converted.time.values)[np.isnan(converted.to_numpy())]
        filled = converted.interpolate_na(dim="time", method="linear")
        return filled, gaps
    return train, apply


def as_series(da: xr.DataArray) -> pd.Series:
    t = da.indexes["time"]
    if isinstance(t, xr.CFTimeIndex):
        t = pd.DatetimeIndex([pd.Timestamp(x.year, x.month, x.day) for x in t])
    return pd.Series(da.to_numpy(), index=pd.DatetimeIndex(t).normalize())


def peak_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


# --------------------------------------------------------------------------- fidelity

def fidelity(work: Path, out: Path, train_fn) -> dict:
    """G1 (Kerala ACCESS-CM2, both weightings), G2, G3 and the AW diagnostic."""
    units = json.loads((work / "units.geojson").read_text())["features"]
    kerala = sorted(int(f["properties"]["processing_id"]) for f in units if "Kerala" in f["properties"]["states"])
    cells = pd.read_csv(work / "cells.csv")
    uhe = load_frame(work / "uhe_1985_2014.npz")
    published = {k: load_frame(work / f"published_{k}.npz") for k in ("historical", *FUTURE)}
    hist = Shade(load_drivers(work, "ACCESS-CM2", "historical", (1985, 2014)), cells)
    fut = Shade(load_drivers(work, "ACCESS-CM2", "ssp245", (2020, 2059)), cells)
    aw_drivers = load_drivers(work, "ACCESS-CM2", "ssp245", (2060, 2080), "_aw")
    train, apply = make_qdm(train_fn)

    g1, g2_rows, flips, daily_hist, future_rows, future_flips = [], [], [], {}, [], []
    warn = set()
    for pid in kerala:
        rc = cells[cells.processing_id == pid]
        raws = {}
        for weighting, column in WEIGHT_COLUMN.items():
            raws[weighting] = hist.unit(rc, column)
            g1.append({"processing_id": pid, "model": "ACCESS-CM2", "weights": weighting,
                       "max_abs_c": hist.legacy_identity(rc, column, raws[weighting])})
        raw = raws["population"]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            model = train(reference(uhe, pid), raw)
            corrected, gaps = apply(model, raw)
            fut_raw = fut.unit(rc, "population_weight")
            fut_corr = {name: apply(model, V.select_window(fut_raw, w))[0] for name, w in FUTURE.items()}
        warn |= {str(w.message) for w in caught}
        mine = as_series(corrected)
        raw_s = as_series(raw).groupby(level=0).first()
        pub = published["historical"][pid].reindex(mine.index)
        daily_hist[pid] = pd.DataFrame({"raw": raw_s.reindex(mine.index), "reproduced": mine, "published": pub,
                                        "uhe": uhe[pid].reindex(mine.index)})
        g2_rows.append(compare_row(pid, "historical", mine, pub))
        f = V.threshold_flips(mine, pub, raw_s.reindex(mine.index))
        f["processing_id"], f["window"] = pid, "historical"
        flips.append(f)
        fr = as_series(fut_raw)
        for name, series in fut_corr.items():
            m = as_series(series)
            p = published[name][pid].reindex(m.index)
            future_rows.append(compare_row(pid, name, m, p))
            ff = V.threshold_flips(m, p, fr.reindex(m.index))
            ff["processing_id"], ff["window"] = pid, name
            future_flips.append(ff)
    g1 = pd.DataFrame(g1)
    g2 = pd.DataFrame(g2_rows)
    g3 = pd.DataFrame(future_rows)
    flips = pd.concat(flips + future_flips, ignore_index=True)
    g1.to_csv(out / "g1_raw_parity_kerala_access.csv", index=False)
    pd.concat([g2, g3]).to_csv(out / "g2_g3_published_reproduction.csv", index=False)
    flips.to_csv(out / "g2_g3_threshold_flips.csv", index=False)
    residual_quantiles(daily_hist).to_csv(out / "g2_residual_distribution.csv", index=False)
    pd.concat(daily_hist, names=["processing_id", "date"]).to_csv(out / "g2_daily_kerala_access_pop.csv.gz")
    tie_cases(daily_hist).to_csv(out / "g2_tie_pair_days.csv", index=False)
    aw = application_windows(cells, uhe, hist, fut, aw_drivers, train, apply)
    aw.to_csv(out / "aw_window_sensitivity.csv", index=False)
    gate = {
        "G1": {"units": int(g1.processing_id.nunique()), "max_abs_c": float(g1.max_abs_c.max()),
               "pass": bool((g1.max_abs_c <= G1_TOL).all())},
        "G2": gate_summary(g2, flips[flips.window == "historical"]),
        "G3": {name: gate_summary(g3[g3.window == name], flips[flips.window == name]) for name in FUTURE},
        "qdm_warnings": sorted(warn)}
    return gate


def compare_row(pid: int, window: str, mine: pd.Series, pub: pd.Series) -> dict:
    both = pd.concat({"m": mine, "p": pub}, axis=1)
    finite = both.dropna()
    d = (finite.m - finite.p).abs()
    row = {"processing_id": pid, "window": window, "days": len(both), "overlap_days": len(finite),
           "mean_abs_c": float(d.mean()), "p50_abs_c": float(d.quantile(.5)),
           "p99_abs_c": float(d.quantile(.99)), "max_abs_c": float(d.max()),
           "days_over_1e3": int((d > V.INPUT_VERSION_BAND_C).sum())}
    for t in V.THRESHOLDS:
        row[f"days_ge_{t}"] = int((finite.m >= t).sum())
        row[f"published_days_ge_{t}"] = int((finite.p >= t).sum())
    row["counts_match"] = all(row[f"days_ge_{t}"] == row[f"published_days_ge_{t}"] for t in V.THRESHOLDS)
    return row


def gate_summary(rows: pd.DataFrame, flips: pd.DataFrame) -> dict:
    """G2/G3 rule: every count discrepancy must be an explained flip (SPEC section 9)."""
    net = {}
    for t in V.THRESHOLDS:
        diff = rows[f"days_ge_{t}"] - rows[f"published_days_ge_{t}"]
        f = flips[flips.threshold == t].groupby("processing_id").direction.sum()
        net[t] = bool((diff.to_numpy() == f.reindex(rows.processing_id).fillna(0).to_numpy()).all())
    unexplained = int((flips.explanation == "unexplained").sum())
    return {"units": int(len(rows)), "exact_count_units": int(rows.counts_match.sum()),
            "flip_days": int(len(flips)), "unexplained_flips": unexplained,
            "flip_accounting_consistent": all(net.values()),
            "overlap_days_min": int(rows.overlap_days.min()), "days_min": int(rows.days.min()),
            "max_abs_c": float(rows.max_abs_c.max()), "median_unit_max_abs_c": float(rows.max_abs_c.median()),
            "pass": bool(unexplained == 0 and all(net.values()))}


def residual_quantiles(daily: dict) -> pd.DataFrame:
    d = pd.concat({k: (v.reproduced - v.published).abs() for k, v in daily.items()})
    qs = [.5, .9, .99, .999, 1.0]
    return pd.DataFrame({"quantile": qs, "abs_residual_c": [float(d.quantile(q)) for q in qs]})


def tie_cases(daily: dict) -> pd.DataFrame:
    rows = []
    for pid, v in daily.items():
        resid = v.reproduced - v.published
        for day, other in V.tie_pair_days(resid.dropna(), v.raw).items():
            rows.append({"processing_id": pid, "date": day.date(), "partner": other.date(),
                         "residual_c": float(resid[day]), "partner_residual_c": float(resid[other]),
                         "raw_c": float(v.raw[day]), "partner_raw_c": float(v.raw[other])})
    return pd.DataFrame(rows)


def application_windows(cells, uhe, hist, fut, aw_drivers, train, apply) -> pd.DataFrame:
    """AW: same trained model, different application windows, compared on overlapping dates."""
    aw_fut = Shade(aw_drivers, cells)
    rows = []
    for pid in AW_UNITS:
        rc = cells[cells.processing_id == pid]
        raw_hist = hist.unit(rc, "population_weight")
        full_model = train(reference(uhe, pid), raw_hist)
        f1 = fut.unit(rc, "population_weight")
        f2 = aw_fut.unit(rc, "population_weight")
        long = xr.concat([f1, f2], dim="time")
        base = {name: as_series(apply(full_model, V.select_window(f1, w))[0]) for name, w in FUTURE.items()}
        for alt, w in AW_ALTERNATIVES.items():
            series = as_series(apply(full_model, V.select_window(long, w))[0])
            for name, b in base.items():
                rows.append(window_diff(pid, f"train 1985-2014; {name} vs one call {alt}", b, series))
        held = V.fit_and_apply(train, lambda m, ts: apply(m, ts)[0], reference(uhe, pid), raw_hist,
                               TRAIN, [TEST, *AW_HIST_SPLIT])
        one = as_series(held[TEST.label])
        split = pd.concat([as_series(held[w.label]) for w in AW_HIST_SPLIT])
        rows.append(window_diff(pid, "train 1985-1999; 2000-2014 one call vs 2000-2007 + 2008-2014", one, split))
    return pd.DataFrame(rows)


def window_diff(pid: int, label: str, a: pd.Series, b: pd.Series) -> dict:
    both = pd.concat({"a": a, "b": b}, axis=1).dropna()
    d = (both.a - both.b).abs()
    row = {"processing_id": pid, "comparison": label, "overlap_days": len(both),
           "max_abs_c": float(d.max()), "mean_abs_c": float(d.mean()),
           "days_changed_over_1e6": int((d > 1e-6).sum())}
    for t in V.THRESHOLDS:
        row[f"count_change_ge_{t}"] = int((both.b >= t).sum() - (both.a >= t).sum())
    return row


# --------------------------------------------------------------------------- holdout

def holdout(work: Path, out: Path, train_fn, state: str, model: str, weighting: str) -> dict:
    """G1 + held-out QDM (train 1985-1999, apply 2000-2014 once) for every eligible unit."""
    units = json.loads((work / "units.geojson").read_text())["features"]
    pids = sorted(int(f["properties"]["processing_id"]) for f in units if state in f["properties"]["states"])
    cells = pd.read_csv(work / "cells.csv")
    uhe = load_frame(work / "uhe_1985_2014.npz")
    column = WEIGHT_COLUMN[weighting]
    drivers = load_drivers(work, model, "historical", (1985, 2014))
    shade = Shade(drivers, cells)
    with_future = state == "Kerala" and model == "ACCESS-CM2"
    fut = Shade(load_drivers(work, model, "ssp245", (2020, 2059)), cells) if with_future else None
    train, apply = make_qdm(train_fn)
    raw_cols, qdm_cols, fut_cols, g1, excluded, gaps_all, warn = {}, {}, {}, [], [], {}, set()
    for pid in pids:
        rc = cells[cells.processing_id == pid]
        if rc.empty:
            excluded.append({"processing_id": pid, "reason": "no non-null-elevation cell"})
            continue
        if rc[column].isna().any():
            excluded.append({"processing_id": pid, "reason": f"undefined {column} (zero population)"})
            continue
        ref = uhe[pid] if pid in uhe else None
        if ref is None or not np.isfinite(ref.to_numpy()).all():
            excluded.append({"processing_id": pid, "reason": "UHE not finite on every day 1985-2014"})
            continue
        raw = shade.unit(rc, column)
        if not np.isfinite(raw.to_numpy()).all():
            excluded.append({"processing_id": pid, "reason": "non-finite raw series"})
            continue
        g1.append({"processing_id": pid, "max_abs_c": shade.legacy_identity(rc, column, raw)})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            res = V.fit_and_apply(train, apply, reference(uhe, pid), raw, TRAIN, [TEST])
            corrected, gaps = res[TEST.label]
            if with_future:
                full = train(reference(uhe, pid), raw)
                fr = fut.unit(rc, column)
                for name, w in FUTURE.items():
                    fut_cols.setdefault(name, {})[pid] = as_series(apply(full, V.select_window(fr, w))[0])
        warn |= {str(w.message) for w in caught}
        raw_cols[pid] = as_series(raw)
        qdm_cols[pid] = as_series(corrected)
        gaps_all[pid] = len(gaps)
    tag = f"{state.replace(' ', '_')}__{model}__{weighting}"
    pd.DataFrame(raw_cols).to_csv(out / f"raw_daily__{tag}.csv.gz")
    pd.DataFrame(qdm_cols).to_csv(out / f"qdm_test_daily__{tag}.csv.gz")
    for name, cols in fut_cols.items():
        pd.DataFrame(cols).to_csv(out / f"qdm_future_{name}__{tag}.csv.gz")
    pd.DataFrame(excluded, columns=["processing_id", "reason"]).to_csv(out / f"excluded__{tag}.csv", index=False)
    g1 = pd.DataFrame(g1)
    g1.to_csv(out / f"g1__{tag}.csv", index=False)
    return {"state": state, "model": model, "weights": weighting, "calendar": drivers["calendar"],
            "units_total": len(pids), "units_run": len(qdm_cols), "excluded": len(excluded),
            "G1_max_abs_c": float(g1.max_abs_c.max()) if len(g1) else None,
            "G1_pass": bool(len(g1) and (g1.max_abs_c <= G1_TOL).all()),
            "calendar_conversion_gap_days_per_unit": sorted(set(gaps_all.values())),
            "qdm_warnings": sorted(warn)}


# --------------------------------------------------------------------------- main

def main() -> None:
    """Dispatch a subcommand; record versions, wall time and peak RSS."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("fidelity", "holdout"))
    parser.add_argument("--work-dir", type=Path, default=WORK)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--state", default="Kerala")
    parser.add_argument("--model", default="ACCESS-CM2")
    parser.add_argument("--weights", choices=tuple(WEIGHT_COLUMN), default="population")
    args = parser.parse_args()
    work, out = args.work_dir.resolve(), args.out_dir.resolve()
    if "scratch" not in out.parts or out.exists():
        raise ValueError("Choose a fresh output directory under scratch")
    if xclim.__version__ != "0.44.0" or importlib.metadata.version("thermofeel") != "1.3.0":
        raise ValueError("The pinned xclim 0.44.0 and thermofeel 1.3.0 are required")
    lock = verify_sources(UPSTREAM.resolve())
    out.mkdir(parents=True)
    train_fn = load_function(UPSTREAM / "06_bias_correction.ipynb", "train_bias_correction",
                             {"np": np, "sdba": sdba, "QuantileDeltaMapping": QuantileDeltaMapping})
    started = time.time()
    if args.command == "fidelity":
        result = fidelity(work, out, train_fn)
    else:
        result = holdout(work, out, train_fn, args.state, args.model, args.weights)
    result.update({"command": args.command, "seconds": round(time.time() - started, 1),
                   "peak_rss_mb": round(peak_rss_mb(), 1), "source_lock_commit": lock["carbonplan_commit"],
                   "package_versions": {n: importlib.metadata.version(n) for n in
                                        ("numpy", "pandas", "xarray", "xclim", "thermofeel", "scipy")},
                   "python": sys.version.split()[0]})
    (out / "manifest.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=1, default=str), flush=True)


if __name__ == "__main__":
    main()
