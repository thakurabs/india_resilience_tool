"""Run the diagnostic Route B paired pilot in the pinned CarbonPlan environment.

CHG-0650. Input preparation uses IRT's geo environment; this runner uses the
existing isolated Python 3.10 / xclim 0.44 environment. --smoke is synthetic;
--probe measures six real series; --run completes one model. No publication.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import time
import warnings

import numpy as np
import pandas as pd
from scipy import sparse

from tools.diagnostics.wbgt_route_b import fixed_aggregate, annual_metrics, paired_outputs
from tools.diagnostics.wbgt_route_b_prepare import atomic_npz, verify_support, PERIODS, MODEL_DEFAULTS

REPO = Path(__file__).resolve().parents[2]
FUTURE_WINDOWS = ((2020, 2039), (2040, 2059), (2060, 2080))
METHOD = "route-b-native-reference-paired-v1"


def peak_rss_mb() -> float | None:
    """Report Linux peak resident memory; leave unavailable platforms explicit."""
    try:
        import resource
    except ImportError:
        return None
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def upstream_functions(upstream: Path):
    """Verify source checksums and package versions, then load pinned functions."""
    import xclim
    from xclim import sdba
    from xclim.sdba.adjustment import QuantileDeltaMapping

    for package, expected in (("xclim", "0.44.0"), ("thermofeel", "1.3.0")):
        if importlib.metadata.version(package) != expected:
            raise ValueError(f"Use pinned {package} {expected} environment")
    path = REPO / "docs/diagnostics/wbgt_outdoor_carbonplan_reproduction/scripts/reproduce.py"
    spec = importlib.util.spec_from_file_location("route_b_pinned_reproduction", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    lock = mod.verify_sources(upstream)
    train = mod.load_function(upstream / "06_bias_correction.ipynb", "train_bias_correction",
                              {"np": np, "sdba": sdba, "QuantileDeltaMapping": QuantileDeltaMapping})
    return mod, train, lock


def data_array(values: np.ndarray, dates, calendar: str = "proleptic_gregorian"):
    """Build a finite shade array in the source calendar, preserving date identity."""
    import cftime
    import xarray as xr

    dates = pd.DatetimeIndex(dates)
    if calendar in ("noleap", "365_day"):
        axis = xr.CFTimeIndex([cftime.DatetimeNoLeap(t.year, t.month, t.day) for t in dates])
    else:
        axis = dates
    return xr.DataArray(values, dims="time", coords={"time": axis}, attrs={"units": "degC"})


def adjust(model, values: np.ndarray, dates, calendar: str) -> np.ndarray:
    """Apply pinned QDM to finite inputs; interpolate only introduced leap dates."""
    if not np.isfinite(values).all():
        return np.full(len(values), np.nan)
    corrected = model.adjust(data_array(values, dates, calendar)).compute()
    converted = corrected.convert_calendar("gregorian", align_on="year", missing=np.nan, use_cftime=None)
    times = pd.DatetimeIndex(converted.time.values)
    missing = ~np.isfinite(converted.values)
    leap = (times.month == 2) & (times.day == 29)
    if np.any(missing & ~leap):
        raise ValueError("QDM produced missing values outside introduced leap dates")
    converted = converted.interpolate_na(dim="time", method="linear")
    series = pd.Series(converted.to_numpy(), index=times)
    return series.reindex(pd.DatetimeIndex(dates)).to_numpy()


def raw_shade(arrays: dict, cells: pd.DataFrame) -> np.ndarray:
    """Vectorize the pinned notebook-02 formula, retaining float32 driver arithmetic."""
    import thermofeel as tf
    import xarray as xr
    import xclim

    values = {}
    for name in ("tas", "tasmax", "huss"):
        values[name] = xr.DataArray(arrays[name].astype("float32"), dims=("time", "cell"),
                                    attrs={"units": "1" if name == "huss" else "K"})
    pressure = 101325 * 10 ** (-cells.elevation_m.to_numpy().astype("float32") / (18400 * values["tas"] / 273.15))
    pressure.attrs["units"] = "Pa"
    rh = xclim.indices.relative_humidity(tas=values["tasmax"], huss=values["huss"], ps=pressure)
    tmax = values["tasmax"]
    wind = values["tas"] - values["tas"] + .5
    result = (.7 * tf.thermofeel.calculate_wbt(tmax - 273.15, rh)
              + .2 * tf.thermofeel.calculate_bgt(tmax, tmax, wind)
              + .1 * (tmax - 273.15)).to_numpy()
    invalid = (~np.isfinite(arrays["tas"]) | ~np.isfinite(arrays["tasmax"]) | ~np.isfinite(arrays["huss"])
               | (arrays["tas"] <= 0) | (arrays["tasmax"] <= 0)
               | (arrays["huss"] < 0) | (arrays["huss"] >= 1))
    result[invalid] = np.nan
    return result


def load_climate(root: Path, model: str, scenario: str, cells: pd.DataFrame) -> dict:
    """Read signature-matched year shards and calculate raw shade without filling."""
    expected_signature = json.loads((root / "support_manifest.json").read_text())["signature_sha256"]
    pieces, dates, calendars, source_hashes = [], [], set(), []
    for year in range(PERIODS[scenario][0], PERIODS[scenario][1] + 1):
        path = root / "drivers" / model / scenario / f"{year}.npz"
        with np.load(path, allow_pickle=False) as z:
            if str(z["signature"]) != expected_signature:
                raise ValueError(f"Driver/support mismatch: {path}")
            calendars.add(str(z["calendar"]))
            dates.extend(z["dates"].tolist())
            arrays = {v: z[v] for v in ("tas", "tasmax", "huss")}
            shade = raw_shade(arrays, cells)
            pieces.append({"shade": shade, "rsds": z["rsds"] if "rsds" in z else np.full(shade.shape, np.nan),
                           "sfcWind": z["sfcWind"] if "sfcWind" in z else np.full(shade.shape, np.nan)})
            source_hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
    if len(calendars) != 1:
        raise ValueError("Model calendar changes between years")
    result = {v: np.concatenate([p[v] for p in pieces]) for v in pieces[0]}
    result.update({"dates": pd.DatetimeIndex(dates), "calendar": calendars.pop(),
                   "source_hash": hashlib.sha256("".join(source_hashes).encode()).hexdigest()})
    return result


def load_reference(root: Path) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, str]:
    """Read all thirty prepared years, retaining the minimum cell support fraction."""
    signature = json.loads((root / "support_manifest.json").read_text())["signature_sha256"]
    values, fractions, dates, hashes = [], [], [], []
    for year in range(1985, 2015):
        path = root / "reference" / f"{year}.npz"
        with np.load(path, allow_pickle=False) as z:
            if str(z["signature"]) != signature:
                raise ValueError("Reference/support signature mismatch")
            values.append(z["values"])
            fractions.append(z["fractions"].min(axis=0))
            dates.extend(z["dates"].tolist())
            hashes.append(hashlib.sha256(path.read_bytes()).hexdigest())
    return np.concatenate(values), np.min(fractions, axis=0), pd.DatetimeIndex(dates), hashlib.sha256("".join(hashes).encode()).hexdigest()


def fit_series(train_fn, reference: np.ndarray, ref_dates, raw: dict, calendar: str,
               signature: str, target: Path, window_sensitivity: bool = False) -> tuple[dict, float]:
    """Fit full/held-out QDM once per support, then apply declared future windows."""
    if target.exists():
        with np.load(target, allow_pickle=False) as z:
            if str(z["signature"]) != signature:
                raise ValueError(f"Stale QDM cache: {target}")
            required = {"historical", "ssp245", "ssp585", "holdout"}
            if window_sensitivity:
                required.update({"ssp245_whole_window", "ssp585_whole_window"})
            if not required.issubset(z.files):
                raise ValueError(f"Incomplete QDM cache: {target}")
            return {k: z[k] for k in z.files if k != "signature"}, 0.
    start = time.monotonic()
    hist, dates = raw["historical"]["values"], raw["historical"]["dates"]
    if not np.isfinite(reference).all() or not np.isfinite(hist).all():
        raise ValueError("Incomplete calibration series")
    full = train_fn(data_array(reference, ref_dates), data_array(hist, dates, calendar), "model")
    result = {"historical": adjust(full, hist, dates, calendar)}
    for scenario in ("ssp245", "ssp585"):
        future = raw[scenario]
        corrected = np.full(len(future["dates"]), np.nan)
        for first, last in FUTURE_WINDOWS:
            mask = (future["dates"].year >= first) & (future["dates"].year <= last)
            corrected[mask] = adjust(full, future["values"][mask], future["dates"][mask], calendar)
        result[scenario] = corrected
        if window_sensitivity:
            result[scenario + "_whole_window"] = adjust(full, future["values"], future["dates"], calendar)
    train_ref = pd.DatetimeIndex(ref_dates).year <= 1999
    train_hist, test_hist = dates.year <= 1999, dates.year >= 2000
    holdout = train_fn(data_array(reference[train_ref], ref_dates[train_ref]),
                      data_array(hist[train_hist], dates[train_hist], calendar), "model")
    result["holdout"] = adjust(holdout, hist[test_hist], dates[test_hist], calendar)
    seconds = time.monotonic() - start
    atomic_npz(target, **result, signature=np.array(signature), training_seconds=np.array(seconds))
    return result, seconds


def sun_adjustments(mod, rsds: np.ndarray, wind: np.ndarray, dates, calendar: str,
                    supports: pd.DataFrame, upstream: Path) -> tuple[np.ndarray, list[dict]]:
    """Compute pinned per-support solar adjustment and radiation/clipping diagnostics."""
    dates = pd.DatetimeIndex(dates)
    solar_dates = dates
    if calendar in ("noleap", "365_day"):
        # MetSim's annual geometry is indexed with the source calendar's dayofyear.
        solar_dates = pd.DatetimeIndex([pd.Timestamp(2001, t.month, t.day) for t in dates])
    result = np.full_like(rsds, np.nan, dtype=float)
    records = []
    for i, row in supports.iterrows():
        # MetSim multiplies daily radiation by 24 before its float64 output.
        # Promote first so float32 rounding cannot violate energy conservation.
        radiation, speed = np.asarray(rsds[:, i], dtype=np.float64), wind[:, i]
        valid = np.isfinite(radiation) & np.isfinite(speed) & (radiation >= 0) & (speed >= 0)
        if not valid.any():
            records.append({"support": i, "valid_days": 0})
            continue
        geometry = pd.DataFrame({"lat": [row.lat], "elevation_m": [row.elevation_m], "weight": [1.]})
        peak, diagnostic = mod.solar_peak(radiation[valid], solar_dates[valid], geometry, upstream,
                                          interpretation="consistent3600s")
        residual = diagnostic["maximum_daily_mean_radiation_residual_wm2"]
        if not np.isfinite(residual) or residual > 1e-8:
            raise ValueError(f"Radiation conservation gate failed at support {i}: {residual} W/m2")
        result[valid, i] = mod.sun_delta(peak, speed[valid])
        records.append({"support": i, "valid_days": int(valid.sum()), "invalid_days": int((~valid).sum()),
                        "conservation_max_wm2": residual,
                        "radiation_clip_fraction": float(((.75 * peak < 300) | (.75 * peak > 900)).mean()),
                        "wind_clip_fraction": float(((speed[valid] < .5) | (speed[valid] > 3)).mean())})
    return result, records


def distribution_errors(dates, model: np.ndarray, ref_dates, reference: np.ndarray) -> dict:
    """Score distributions, not synchronized model-versus-observation weather."""
    dates, ref_dates = pd.DatetimeIndex(dates), pd.DatetimeIndex(ref_dates)
    keep = ~((dates.month == 2) & (dates.day == 29))
    rkeep = ~((ref_dates.month == 2) & (ref_dates.day == 29))
    dates, model, ref_dates, reference = dates[keep], model[keep], ref_dates[rkeep], reference[rkeep]
    if not np.isfinite(model).all() or not np.isfinite(reference).all():
        return {"complete": False}
    result = {"complete": True, "annual_mean_bias_c": float(model.mean() - reference.mean()),
              "monthly_climatology_mae_c": float(np.mean([abs(model[dates.month == m].mean() - reference[ref_dates.month == m].mean()) for m in range(1, 13)]))}
    for q in (.9, .95, .99):
        result[f"p{int(q * 100)}_bias_c"] = float(np.quantile(model, q) - np.quantile(reference, q))
    a, b = annual_metrics(dates, model), annual_metrics(ref_dates, reference)
    for threshold in (28, 30, 32):
        key = f"days_ge_{threshold}"
        result[key + "_bias"] = float(a[key].mean() - b[key].mean())
        result[key + "_reference"] = float(b[key].mean())
    return result


def smoke(upstream: Path, out_dir: Path) -> dict:
    """Exercise real pinned QDM, formula parity and radiation with synthetic inputs."""
    mod, train, lock = upstream_functions(upstream)
    days = pd.date_range("1985-01-01", "2014-12-31")
    baseline = 25 + 5 * np.sin(2 * np.pi * (days.dayofyear.to_numpy() - 1) / 365.25) + np.arange(len(days)) * 1e-5
    train_mask, test = days.year <= 1999, days.year >= 2000
    fitted = train(data_array(baseline[train_mask] + 1, days[train_mask]), data_array(baseline[train_mask], days[train_mask]), "model")
    corrected = adjust(fitted, baseline[test], days[test], "proleptic_gregorian")
    error = float(np.max(np.abs(corrected - baseline[test] - 1)))
    if error > 1e-5:
        raise ValueError(f"Constant-additive QDM smoke failed: {error}")
    noleap = ~((days.month == 2) & (days.day == 29))
    ndays, nbase = days[noleap], baseline[noleap]
    ntrain, ntest = ndays.year <= 1999, ndays.year >= 2000
    nfitted = train(data_array(nbase[ntrain] + 1, ndays[ntrain]),
                    data_array(nbase[ntrain], ndays[ntrain], "noleap"), "model")
    ncorrected = adjust(nfitted, nbase[ntest], ndays[ntest], "noleap")
    nerror = float(np.max(np.abs(ncorrected - nbase[ntest] - 1)))
    if nerror > 1e-5 or len(ncorrected) != 15 * 365:
        raise ValueError(f"No-leap QDM/calendar smoke failed: {nerror}")
    cells = pd.DataFrame({"lat": [10.125], "elevation_m": [10.], "weight": [1.]})
    radiation = np.full((365, 1), 200.)
    delta, record = sun_adjustments(mod, radiation, np.ones((365, 1)), pd.date_range("2005-01-01", "2005-12-31"), "proleptic_gregorian", cells, upstream)
    dates = pd.date_range("2005-01-01", periods=12)
    drivers = {"tas": np.full((12, 1), 298., dtype="float32"), "tasmax": np.full((12, 1), 305., dtype="float32"), "huss": np.full((12, 1), .012, dtype="float32")}
    direct = raw_shade(drivers, cells)[:, 0]
    frame = pd.DataFrame({"date": dates, "cell": 0, **{k: v[:, 0] for k, v in drivers.items()}})
    legacy, _ = mod.raw_shade(frame, cells)
    parity = float(np.max(np.abs(direct - legacy.to_numpy())))
    if parity > 1e-9 or not np.isfinite(delta).all():
        raise ValueError("Formula/solar smoke failed")
    result = {"status": "PASS", "additive_qdm_max_abs_c": error, "noleap_additive_qdm_max_abs_c": nerror,
              "raw_formula_max_abs_c": parity,
              "radiation": record, "source_commit": lock["carbonplan_commit"]}
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "integration_smoke.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return result


def run(args: argparse.Namespace) -> None:
    """Run/cache both aggregation paths, write explicit support and scored outputs."""
    started = time.monotonic()
    mod, train, lock = upstream_functions(args.upstream)
    root, out = args.work_dir, args.out_dir / args.model
    out.mkdir(parents=True, exist_ok=True)
    verify_support(root)
    cells = pd.read_csv(root / "cells.csv")
    units = pd.read_csv(root / "units.csv").fillna({"block_name": ""})
    areas = sparse.load_npz(root / "admin_areas.npz").toarray()
    reference, support, ref_dates, reference_hash = load_reference(root)
    climate = {s: load_climate(root, args.model, s, cells) for s in PERIODS}
    calendar = climate["historical"]["calendar"]
    if any(v["calendar"] != calendar for v in climate.values()):
        raise ValueError("Scenario calendars disagree")
    hist = climate["historical"]
    valid = (support >= .50) & np.isfinite(reference).all(axis=0) & np.isfinite(hist["shade"]).all(axis=0)
    coverage95 = areas[:, valid & (support >= .95)].sum(axis=1) / units.whole_area_m2.to_numpy()
    coverage = areas[:, valid].sum(axis=1) / units.whole_area_m2.to_numpy()
    support_report = cells.copy()
    support_report["minimum_reference_fraction"], support_report["eligible"] = support, valid
    support_report.to_csv(out / "cell_support.csv", index=False)
    units["retained_fraction"], units["retained_fraction_strict_reference"] = coverage, coverage95
    units["eligible"] = coverage >= .95
    units.to_csv(out / "unit_support.csv", index=False)
    active_units = units[units.eligible].copy()
    if active_units.empty or not valid.any():
        raise ValueError("No eligible common support")
    uid = active_units.index.to_numpy()
    weights = areas[uid][:, valid]
    weights /= weights.sum(axis=1, keepdims=True)
    active_cells = cells[valid].reset_index(drop=True)
    reference_cells = reference[:, valid]
    reference_units = fixed_aggregate(reference_cells, weights)
    cell_raw = {s: {"values": c["shade"][:, valid], "dates": c["dates"]} for s, c in climate.items()}
    unit_raw = {s: {"values": fixed_aggregate(c["shade"][:, valid], weights), "dates": c["dates"]} for s, c in climate.items()}
    district_choices = active_units[active_units.level == "district"].groupby("state_name").whole_area_m2.idxmax().to_numpy()
    unit_positions = [int(np.flatnonzero(uid == i)[0]) for i in district_choices]
    probe_cells = sorted({int(np.argmax(weights[i])) for i in unit_positions})
    base_signature = {"method": METHOD, "source_lock": lock, "model": args.model,
                      "code_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in
                                      (Path(__file__), Path(__file__).with_name("wbgt_route_b.py"))},
                      "support": json.loads((root / "support_manifest.json").read_text()),
                      "reference": reference_hash, "drivers": {s: c["source_hash"] for s, c in climate.items()}}
    signature = hashlib.sha256(json.dumps(base_signature, sort_keys=True).encode()).hexdigest()
    corrections, timings, caught_messages = {}, [], set()
    for kind, refs, raw in (("cell", reference_cells, cell_raw), ("unit", reference_units, unit_raw)):
        nseries = refs.shape[1]
        indices = (probe_cells if kind == "cell" else unit_positions) if args.probe else range(nseries)
        results = {s: np.full_like(r["values"], np.nan, dtype=float) for s, r in raw.items()}
        results["holdout"] = np.full((int((hist["dates"].year >= 2000).sum()), nseries), np.nan)
        for i in indices:
            target = root / "qdm" / args.model / kind / f"{i}.npz"
            selected = {s: {"values": r["values"][:, i], "dates": r["dates"]} for s, r in raw.items()}
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                fitted, seconds = fit_series(train, refs[:, i], ref_dates, selected, calendar,
                                             signature, target, kind == "unit" and i in unit_positions)
            caught_messages.update(str(w.message) for w in caught)
            if args.probe and seconds == 0:
                with np.load(target, allow_pickle=False) as cached:
                    seconds = float(cached["training_seconds"])
            for s in results:
                results[s][:, i] = fitted[s]
            timings.append({"kind": kind, "series": i, "seconds": seconds})
            print(f"{args.model} {kind} {i + 1}/{nseries}: {seconds:.2f}s", flush=True)
        corrections[kind] = results
    pd.DataFrame(timings).to_csv(out / ("probe_timings.csv" if args.probe else "timings.csv"), index=False)
    if args.probe:
        probe = {"status": "PROBE_COMPLETE", "series": len(timings), "timings": timings,
                 "total_cell_series": len(active_cells), "total_unit_series": len(active_units),
                 "elapsed_seconds": time.monotonic() - started,
                 "peak_rss_mb": peak_rss_mb()}
        (out / "probe.json").write_text(json.dumps(probe, indent=2))
        print(json.dumps(probe, indent=2))
        return
    unit_geometry = pd.DataFrame({"lat": weights @ active_cells.lat.to_numpy(),
                                  "elevation_m": weights @ active_cells.elevation_m.to_numpy()})
    yearly, radiation_rows, solar_probes = [], [], {}
    for scenario, source in climate.items():
        dates = source["dates"]
        keep = (dates.year >= 1990) & (dates.year <= 2010) if scenario == "historical" else np.ones(len(dates), dtype=bool)
        rsds, wind = source["rsds"][keep][:, valid], source["sfcWind"][keep][:, valid]
        delta_cells, diagnostic_cells = sun_adjustments(mod, rsds, wind, dates[keep], calendar, active_cells, args.upstream)
        delta_units, diagnostic_units = sun_adjustments(mod, fixed_aggregate(rsds, weights), fixed_aggregate(wind, weights), dates[keep], calendar, unit_geometry, args.upstream)
        if scenario != "historical":
            solar_probes[scenario] = delta_units[:, unit_positions]
        else:
            cell_years = annual_metrics(dates[keep], corrections["cell"][scenario][keep] + delta_cells)
            yi = int(np.flatnonzero(cell_years["years"] == 2005)[0])
            cell_table = active_cells.copy()
            for name, values in cell_years.items():
                if name != "years":
                    cell_table[name] = values[yi]
            cell_table.to_csv(out / "cell_metrics_2005.csv", index=False)
        radiation_rows.extend({"scenario": scenario, "kind": kind, **r} for kind, rows in (("cell", diagnostic_cells), ("unit", diagnostic_units)) for r in rows)
        results = paired_outputs(dates[keep], corrections["cell"][scenario][keep] + delta_cells,
                                 corrections["unit"][scenario][keep] + delta_units, weights)
        for method, metrics in results.items():
            for yi, year in enumerate(metrics["years"]):
                for ui, unit in enumerate(uid):
                    yearly.append({"unit_index": int(unit), "scenario": scenario, "year": int(year), "method": method,
                                   **{name: float(v[yi, ui]) for name, v in metrics.items() if name != "years"}})
    frame = pd.DataFrame(yearly)
    # Preserve the complete roster: excluded units are missing, never absent or zero.
    roster = pd.MultiIndex.from_tuples(
        [(int(u), s, y, m) for u in units.index for s in PERIODS
         for y in range(1990 if s == "historical" else 2020, 2011 if s == "historical" else 2081)
         for m in ("region_first_series", "grid_first_series", "grid_first_cell_metrics")],
        names=["unit_index", "scenario", "year", "method"])
    frame.set_index(roster.names).reindex(roster).reset_index().to_csv(out / "yearly_metrics.csv", index=False)
    pd.DataFrame(radiation_rows).to_csv(out / "radiation_diagnostics.csv", index=False)
    test_dates = hist["dates"][hist["dates"].year >= 2000]
    ref_test = ref_dates.year >= 2000
    scored, windows = [], []
    holdout_grid = fixed_aggregate(corrections["cell"]["holdout"], weights)
    for i, unit in enumerate(uid):
        for kind, values in (("raw", unit_raw["historical"]["values"][hist["dates"].year >= 2000, i]),
                             ("region_first", corrections["unit"]["holdout"][:, i]),
                             ("grid_first", holdout_grid[:, i])):
            scored.append({"unit_index": int(unit), "kind": kind, **distribution_errors(test_dates, values, ref_dates[ref_test], reference_units[ref_test, i])})
    for probe_position, i in enumerate(unit_positions):
        with np.load(root / "qdm" / args.model / "unit" / f"{i}.npz", allow_pickle=False) as z:
            for scenario in ("ssp245", "ssp585"):
                delta = z[scenario + "_whole_window"] - z[scenario]
                solar = solar_probes[scenario][:, probe_position]
                primary = annual_metrics(climate[scenario]["dates"], z[scenario] + solar)
                whole = annual_metrics(climate[scenario]["dates"], z[scenario + "_whole_window"] + solar)
                window_row = {"unit_index": int(uid[i]), "scenario": scenario,
                              "mean_delta_c": float(np.nanmean(delta)), "max_abs_delta_c": float(np.nanmax(np.abs(delta)))}
                for threshold in (28, 30, 32):
                    change = whole[f"days_ge_{threshold}"] - primary[f"days_ge_{threshold}"]
                    window_row[f"days_ge_{threshold}_mean_delta"] = float(np.nanmean(change))
                    window_row[f"days_ge_{threshold}_maximum_abs_delta"] = float(np.nanmax(np.abs(change)))
                windows.append(window_row)
    pd.DataFrame(scored).to_csv(out / "holdout_scores.csv", index=False)
    pd.DataFrame(windows).to_csv(out / "application_window_sensitivity.csv", index=False)
    manifest = {**base_signature, "signature_sha256": signature, "status": "PILOT_COMPLETE_DIAGNOSTIC_ONLY",
                "units_eligible": len(uid), "units_total": len(units), "cells_eligible": int(valid.sum()),
                "warnings": sorted(caught_messages), "elapsed_seconds": time.monotonic() - started,
                "peak_rss_mb": peak_rss_mb()}
    artifact_names = ("yearly_metrics.csv", "radiation_diagnostics.csv", "holdout_scores.csv",
                      "application_window_sensitivity.csv", "cell_support.csv", "unit_support.csv", "timings.csv",
                      "cell_metrics_2005.csv")
    manifest["artifact_sha256"] = {p: hashlib.sha256((out / p).read_bytes()).hexdigest() for p in artifact_names}
    (out / "run_manifest.json").write_text(json.dumps(manifest, indent=2))


def main(argv: list[str] | None = None) -> int:
    """Plan, smoke-test, cost-probe or execute one diagnostic model."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--run", action="store_true")
    mode.add_argument("--probe", action="store_true")
    mode.add_argument("--smoke", action="store_true")
    parser.add_argument("--model", choices=MODEL_DEFAULTS, default="ACCESS-CM2")
    parser.add_argument("--work-dir", type=Path, default=Path("scratch/wbgt_route_b"))
    parser.add_argument("--upstream", type=Path, default=Path("scratch/carbonplan_reproduction/upstream"))
    parser.add_argument("--out-dir", type=Path, default=Path("docs/diagnostics/wbgt_outdoor_route_b_pilot"))
    args = parser.parse_args(argv)
    if args.smoke:
        smoke(args.upstream, args.out_dir)
    elif args.run or args.probe:
        run(args)
    else:
        print(json.dumps({"action": "plan_only", "model": args.model, "method": METHOD,
                          "future_windows": FUTURE_WINDOWS, "output": str(args.out_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
