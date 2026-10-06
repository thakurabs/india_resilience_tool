"""Bounded CarbonPlan shade-WBGT pilot: Kerala, Rajasthan, Himachal Pradesh (CHG-0653).

Frozen method: ``docs/diagnostics/wbgt_shade_carbonplan_pilot/METHOD.md``. Diagnostic
only -- nothing here is imported by production code, nothing is published, and
outputs go to an isolated scratch tree under a distinct method signature.

Stages (run from the repository root; the environment each needs is fixed):

  coverage      [irt env]    union-based admin coverage + overlap exclusions
  fetch-parity  [irt env]    CMCC-CM2-SR5 Kerala published historical (ledger-capped)
  calibrate     [pinned env] raw shade + notebook-06 QDM per CarbonPlan unit, one model
  parity        [pinned env] G1 summary, QDM parity vs published, determinism
  write         [irt env]    unit-year metrics -> count-first admin transfer -> IRT CSVs
  verify        [irt env]    read the CSVs through master_builder; build pilot masters
  compare       [irt env]    descriptive comparison with shade-peak-v1 (read-only)

The pinned env is Python 3.10 + xclim 0.44.0 + thermofeel 1.3.0; the irt env is
the repository's conda env. Every stage appends wall time and peak RSS to
``<results>/stage_log.jsonl``.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from tools.diagnostics import wbgt_shade_cp_validation as V  # noqa: E402

VALIDATION_WORK = Path("scratch/carbonplan_shade_validation")
WORK = Path("scratch/carbonplan_shade_pilot")
PROCESSED = WORK / "processed"
RESULTS = Path("docs/diagnostics/wbgt_shade_carbonplan_pilot/results")
VALIDATION_SCRIPTS = Path("docs/diagnostics/wbgt_shade_carbonplan_validation/scripts")
STAGING = Path("scratch/wbgt_shade_national/processed")

STATES = ("Kerala", "Rajasthan", "Himachal Pradesh")
MODELS = ("ACCESS-CM2", "CMCC-CM2-SR5")
WEIGHT_COLUMN = "population_weight"
HIST = V.Window(1985, 2014)
FUTURE = {"ssp245-2030": V.Window(2020, 2039), "ssp245-2050": V.Window(2040, 2059)}
FUTURE_SPAN = V.Window(2020, 2059)
PERIODS = {"historical": {"1990-2010": V.Window(1990, 2010)},
           "ssp245": {"2020-2039": FUTURE["ssp245-2030"], "2040-2059": FUTURE["ssp245-2050"]}}
KEYS = ["level", "admin_key"]
G1_TOL = 1e-9

SIGNATURE = (
    "carbonplan-shade-pilot-v1:tasmax,tas,huss+elevation-pressure:rh-at-tasmax(xclim-0.44):"
    "thermofeel-1.3-wbt+bgt-iso3-wind0.5:population-weighted-cells:"
    "qdm-additive-100q-doy31-vs-uhe-daily-train-1985-2014:apply-1985-2014|2020-2039|2040-2059:"
    "noleap-train-gregorian-out-drop-feb29:complete-365:inclusive-ge:count-first-area-transfer:"
    "union-coverage:overlap-excluded")
METHOD_VERSION = "carbonplan-shade-pilot-v1"
AGGREGATION = "count_first_area_weighted_from_carbonplan_units"
SLUG_COLUMN = {"annual_mean": "wbgt_shade_stull_annual_mean_C",
               **{f"days_ge_{t}": f"wbgt_shade_stull_days_ge_{t}_days" for t in V.THRESHOLDS}}


def peak_rss_mb() -> float:
    """Peak resident memory of this process in MB (Linux ru_maxrss or Windows peak_wset)."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        import psutil
        info = psutil.Process().memory_info()
        return getattr(info, "peak_wset", info.rss) / 1e6


def log_stage(name: str, started: float, extra: dict) -> None:
    """Append one stage record (time, memory, results) to the stage log and print it."""
    RESULTS.mkdir(parents=True, exist_ok=True)
    rec = {"stage": name, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "seconds": round(time.time() - started, 1), "peak_rss_mb": round(peak_rss_mb(), 1), **extra}
    with open(RESULTS / "stage_log.jsonl", "a") as fh:
        fh.write(json.dumps(rec, default=str) + "\n")
    print(json.dumps(rec, indent=1, default=str), flush=True)


def units_by_state(work: Path = VALIDATION_WORK) -> dict[str, list[int]]:
    feats = json.loads((work / "units.geojson").read_text())["features"]
    return {s: sorted(int(f["properties"]["processing_id"]) for f in feats if s in f["properties"]["states"])
            for s in STATES}


def save_frame(path: Path, frame: pd.DataFrame) -> None:
    np.savez_compressed(path, values=frame.to_numpy(float), dates=frame.index.values.astype("datetime64[ns]"),
                        processing_id=np.asarray(frame.columns, dtype=int))


def load_frame(path: Path) -> pd.DataFrame:
    z = np.load(path)
    return pd.DataFrame(z["values"], index=pd.DatetimeIndex(z["dates"]), columns=[int(c) for c in z["processing_id"]])


def read_admin() -> pd.DataFrame:
    """The written admin-year table; empty strings survive the CSV round trip as ''."""
    admin = pd.read_csv(WORK / "admin_yearly_all.csv.gz",
                        dtype={"admin_key": str, "block": str, "district": str, "exclusion_reason": str})
    for c in ("block", "exclusion_reason"):
        admin[c] = admin[c].fillna("")
    return admin


# --------------------------------------------------------------------------- coverage [irt]

def stage_coverage() -> dict:
    """Recompute every admin x unit intersection and the union of each admin's intersections."""
    import geopandas as gpd
    from shapely.geometry import shape

    sys.path.insert(0, str(VALIDATION_SCRIPTS))
    import prepare as P

    feats = json.loads((VALIDATION_WORK / "units.geojson").read_text())["features"]
    reg = gpd.GeoDataFrame([{"processing_id": int(f["properties"]["processing_id"])} for f in feats],
                           geometry=[shape(f["geometry"]).buffer(0) for f in feats],
                           crs="EPSG:4326").to_crs(P.EQUAL_AREA)
    sindex = reg.sindex
    old = pd.read_csv(VALIDATION_WORK / "crosswalk.csv", dtype={"admin_key": str})
    old["block"] = old.block.fillna("")
    rows = []
    for state, (districts, blocks) in P.state_units().items():
        districts = districts.assign(district_key=state + "::" + districts.district_name)
        for level, frame, key in (("district", districts, "district_key"), ("block", blocks, "block_key")):
            for unit in frame.to_crs(P.EQUAL_AREA).itertuples():
                g = unit.geometry
                hits = reg.iloc[sindex.query(g, predicate="intersects")]
                total, union = V.union_intersection_area(g, list(hits.geometry))
                rows.append({"state": state, "level": level, "admin_key": str(getattr(unit, key)),
                             "summed_m2": total, "union_m2": union})
    u = pd.DataFrame(rows)
    cw = old.merge(u, on=["state", "level", "admin_key"], how="left", validate="many_to_one")
    if cw.union_m2.isna().any():
        raise ValueError("Admin units in crosswalk.csv without a recomputed union")
    summed_old = cw.groupby(["state", "level", "admin_key"]).intersection_m2.transform("sum")
    drift = float(((summed_old - cw.summed_m2).abs() / cw.unit_area_m2).max())
    if drift > 1e-9:
        raise ValueError(f"Recomputed intersections drift from crosswalk.csv by {drift}")
    WORK.mkdir(parents=True, exist_ok=True)
    cw.drop(columns="summed_m2").to_csv(WORK / "crosswalk_union.csv", index=False)
    tables = []
    for state in STATES:
        w = V.crosswalk_weights(cw[cw.state == state], KEYS)
        st = V.overlap_status(w, KEYS)
        names = w.groupby(KEYS)[["district", "block"]].first().reset_index()
        st = st.merge(names, on=KEYS)
        st["state"] = state
        st["summed_coverage_retired"] = (w.groupby(KEYS).intersection_m2.sum() / w.groupby(KEYS).unit_area_m2.first()
                                         ).reindex(pd.MultiIndex.from_frame(st[KEYS])).to_numpy()
        tables.append(st)
    st = pd.concat(tables, ignore_index=True)
    RESULTS.mkdir(parents=True, exist_ok=True)
    cols = ["state", "level", "admin_key", "district", "block", "coverage", "summed_coverage_retired",
            "overlap_m2", "overlap_fraction", "source_units", "excluded", "exclusion_reason"]
    st[cols].to_csv(RESULTS / "coverage_and_overlap.csv", index=False)
    st[st.overlap_m2 > 0][cols].to_csv(RESULTS / "overlap_all_nonzero.csv", index=False)
    st[st.excluded][cols].to_csv(RESULTS / "excluded_admins.csv", index=False)
    return {"intersection_drift_vs_crosswalk": drift, "admin_units": len(st),
            "max_union_coverage": float(st.coverage.max()), "max_summed_coverage_retired":
            float(st.summed_coverage_retired.max()), "nonzero_overlap_units": int((st.overlap_m2 > 0).sum()),
            "excluded": st[st.excluded][["state", "level", "admin_key", "overlap_fraction"]].to_dict("records"),
            "below_0.95": int((st.coverage < 0.95).sum())}


# --------------------------------------------------------------------------- fetch-parity [irt]

def stage_fetch_parity() -> dict:
    """CMCC-CM2-SR5 published historical shade for Kerala units, booked on the task ledger."""
    sys.path.insert(0, str(VALIDATION_SCRIPTS))
    import prepare as P

    ledger = P.Ledger(VALIDATION_WORK / "transfer_ledger.json")
    before = ledger.total
    archive = P.CappedArchive(VALIDATION_WORK.resolve(), ledger, allow_download=True)
    kerala = units_by_state()["Kerala"]
    pub, missing = P.batch_series(archive, P.SHADE["historical"], "wbgt-shade", kerala,
                                  P.SHADE_WINDOWS["historical"], "CMCC-CM2-SR5")
    WORK.mkdir(parents=True, exist_ok=True)
    P.save_frame(WORK / "published_CMCC-CM2-SR5_historical.npz", pub)
    return {"units": int(pub.shape[1]), "days": int(pub.shape[0]), "missing": missing,
            "new_bytes": ledger.total - before, "ledger_total_bytes": ledger.total, "cap": P.CAP_BYTES}


# --------------------------------------------------------------------------- calibrate [pinned]

def stage_calibrate(model: str) -> dict:
    """Train notebook-06 QDM on 1985-2014 per unit; adjust each published window in its own call."""
    import importlib.metadata
    import warnings

    import xclim
    from xclim import sdba
    from xclim.sdba.adjustment import QuantileDeltaMapping

    if xclim.__version__ != "0.44.0" or importlib.metadata.version("thermofeel") != "1.3.0":
        raise ValueError("The pinned xclim 0.44.0 and thermofeel 1.3.0 are required")
    sys.path.insert(0, str(VALIDATION_SCRIPTS))
    import run_validation as RV

    lock = RV.verify_sources(RV.UPSTREAM.resolve())
    train_fn = RV.load_function(RV.UPSTREAM / "06_bias_correction.ipynb", "train_bias_correction",
                                {"np": np, "sdba": sdba, "QuantileDeltaMapping": QuantileDeltaMapping})
    train, apply = RV.make_qdm(train_fn)
    cells = pd.read_csv(VALIDATION_WORK / "cells.csv")
    uhe = RV.load_frame(VALIDATION_WORK / "uhe_1985_2014.npz")
    drivers = RV.load_drivers(VALIDATION_WORK, model, "historical", (HIST.start, HIST.end))
    hist = RV.Shade(drivers, cells)
    fut = RV.Shade(RV.load_drivers(VALIDATION_WORK, model, "ssp245", (FUTURE_SPAN.start, FUTURE_SPAN.end)), cells)
    pids = sorted({p for v in units_by_state().values() for p in v})
    out = {k: {} for k in ("raw_historical", "raw_ssp245", "historical", *FUTURE)}
    g1, excluded, gaps, warn = [], [], {}, set()
    for i, pid in enumerate(pids):
        rc = cells[cells.processing_id == pid]
        reason = None
        if rc.empty:
            reason = "no non-null-elevation cell"
        elif rc[WEIGHT_COLUMN].isna().any():
            reason = "undefined population_weight (zero population)"
        elif pid not in uhe or not np.isfinite(uhe[pid].to_numpy()).all():
            reason = "UHE not finite on every day 1985-2014"
        if reason:
            excluded.append({"processing_id": pid, "reason": reason})
            continue
        raw = hist.unit(rc, WEIGHT_COLUMN)
        fr = fut.unit(rc, WEIGHT_COLUMN)
        if not (np.isfinite(raw.to_numpy()).all() and np.isfinite(fr.to_numpy()).all()):
            excluded.append({"processing_id": pid, "reason": "non-finite raw series"})
            continue
        g1.append({"processing_id": pid, "historical_max_abs_c": hist.legacy_identity(rc, WEIGHT_COLUMN, raw),
                   "ssp245_max_abs_c": fut.legacy_identity(rc, WEIGHT_COLUMN, fr)})
        V.assert_within(raw, HIST, "training raw")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            trained = train(RV.reference(uhe, pid), raw)
            series, g = apply(trained, raw)
            out["historical"][pid] = RV.as_series(series)
            gaps[pid] = [len(g)]
            for name, w in FUTURE.items():
                series, g = apply(trained, V.select_window(fr, w))
                out[name][pid] = RV.as_series(series)
                gaps[pid].append(len(g))
        warn |= {str(w.message) for w in caught}
        out["raw_historical"][pid] = RV.as_series(raw).groupby(level=0).first()
        out["raw_ssp245"][pid] = RV.as_series(fr).groupby(level=0).first()
        if i % 50 == 0:
            print(f"{model} {i + 1}/{len(pids)} units, {peak_rss_mb():.0f} MB", flush=True)
    dest = WORK / "calibrated" / model
    dest.mkdir(parents=True, exist_ok=True)
    for name, cols in out.items():
        save_frame(dest / f"{name}.npz", pd.DataFrame(cols))
    g1 = pd.DataFrame(g1)
    g1.to_csv(dest / "g1_raw_parity.csv", index=False)
    pd.DataFrame(excluded, columns=["processing_id", "reason"]).to_csv(dest / "excluded_units.csv", index=False)
    return {"model": model, "calendar": drivers["calendar"], "units": len(pids), "calibrated": len(out["historical"]),
            "excluded_units": len(excluded),
            "G1_max_abs_c": float(g1[["historical_max_abs_c", "ssp245_max_abs_c"]].max().max()),
            "G1_pass": bool((g1[["historical_max_abs_c", "ssp245_max_abs_c"]] <= G1_TOL).all().all()),
            "calendar_gap_days_per_unit_window": sorted({tuple(v) for v in gaps.values()}),
            "qdm_warnings": sorted(warn), "source_lock_commit": lock["carbonplan_commit"],
            "package_versions": {n: importlib.metadata.version(n) for n in
                                 ("numpy", "pandas", "xarray", "xclim", "thermofeel", "scipy")},
            "python": sys.version.split()[0]}


# --------------------------------------------------------------------------- parity [pinned]

def _published(name: str) -> pd.DataFrame:
    if name.startswith("CMCC"):
        return load_frame(WORK / "published_CMCC-CM2-SR5_historical.npz")
    return load_frame(VALIDATION_WORK / f"published_{name}.npz")


def stage_parity() -> dict:
    """QDM parity of the pilot against CarbonPlan's published product, plus determinism."""
    sys.path.insert(0, str(VALIDATION_SCRIPTS))
    import run_validation as RV

    kerala = units_by_state()["Kerala"]
    cases = [("ACCESS-CM2", "historical", "historical", "raw_historical"),
             ("ACCESS-CM2", "ssp245-2030", "ssp245-2030", "raw_ssp245"),
             ("ACCESS-CM2", "ssp245-2050", "ssp245-2050", "raw_ssp245"),
             ("CMCC-CM2-SR5", "historical", "CMCC-historical", "raw_historical")]
    rows, flips, result = [], [], {}
    for model, window, pub_name, raw_name in cases:
        mine_all = load_frame(WORK / "calibrated" / model / f"{window}.npz")
        raw_all = load_frame(WORK / "calibrated" / model / f"{raw_name}.npz")
        pub_all = _published(pub_name)
        for pid in kerala:
            if pid not in mine_all or pid not in pub_all:
                continue
            mine = mine_all[pid].dropna()
            pub = pub_all[pid].reindex(mine.index)
            row = RV.compare_row(pid, window, mine, pub)
            row["model"] = model
            rows.append(row)
            f = V.threshold_flips(mine, pub, raw_all[pid].reindex(mine.index))
            f["processing_id"], f["window"], f["model"] = pid, window, model
            flips.append(f)
    rows, flips = pd.DataFrame(rows), pd.concat(flips, ignore_index=True)
    out = RESULTS / "parity"
    out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out / "qdm_parity_vs_published.csv", index=False)
    flips.to_csv(out / "qdm_parity_threshold_flips.csv", index=False)
    for (model, window), g in rows.groupby(["model", "window"]):
        f = flips[(flips.model == model) & (flips.window == window)]
        summary = RV.gate_summary(g, f)
        summary["flip_explanations"] = f.explanation.value_counts().to_dict()
        result[f"{model} {window}"] = summary
    g1 = {m: pd.read_csv(WORK / "calibrated" / m / "g1_raw_parity.csv") for m in MODELS}
    result["G1"] = {m: {"units": len(t), "max_abs_c": float(t.iloc[:, 1:].max().max()),
                        "pass": bool((t.iloc[:, 1:] <= G1_TOL).all().all())} for m, t in g1.items()}
    result["determinism"] = determinism(kerala)
    (out / "parity_summary.json").write_text(json.dumps(result, indent=2, default=str))
    return result


def determinism(kerala: list[int]) -> dict:
    """Pilot ACCESS-CM2 Kerala vs the validation's own full-window fidelity and future runs."""
    fid = pd.read_csv(VALIDATION_WORK / "fidelity" / "g2_daily_kerala_access_pop.csv.gz", parse_dates=["date"])
    fid = fid.pivot(index="date", columns="processing_id", values="reproduced")
    mine = load_frame(WORK / "calibrated" / "ACCESS-CM2" / "historical.npz")
    out = {"historical": float((mine[fid.columns].reindex(fid.index) - fid).abs().max().max())}
    run = VALIDATION_WORK / "runs" / "Kerala__ACCESS-CM2__population"
    for name in FUTURE:
        prev = pd.read_csv(run / f"qdm_future_{name}__Kerala__ACCESS-CM2__population.csv.gz", index_col=0,
                           parse_dates=True)
        prev.columns = [int(c) for c in prev.columns]
        m = load_frame(WORK / "calibrated" / "ACCESS-CM2" / f"{name}.npz")
        out[name] = float((m[prev.columns].reindex(prev.index) - prev).abs().max().max())
    out["bit_identical"] = all(v == 0.0 for v in out.values())
    return out


# --------------------------------------------------------------------------- write [irt]

def unit_years(daily: pd.DataFrame, window: V.Window) -> pd.DataFrame:
    """Per CarbonPlan unit-year: annual mean and inclusive counts on the IRT calendar."""
    d, valid = V.irt_calendar(daily, window)
    tables = {"annual_mean": d.groupby(d.index.year).mean().reindex(valid.index).where(valid)}
    for t, c in V.annual_counts(d, valid).items():
        tables[f"days_ge_{t}"] = c
    frames = []
    for slug, table in tables.items():
        long = table.stack(dropna=False).rename("value").reset_index()
        long.columns = ["year", "processing_id", "value"]
        long["slug"] = slug
        frames.append(long)
    return pd.concat(frames, ignore_index=True)


def stage_write(out_root: Path) -> dict:
    """Transfer unit-year metrics to admin units and write the IRT shade CSV layout."""
    from india_resilience_tool.utils.naming import hydro_fs_token

    root = out_root.resolve()
    if "carbonplan_shade_pilot" not in root.parts or STAGING.resolve() in root.parents:
        raise ValueError("The pilot writes only under a carbonplan_shade_pilot scratch tree")
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"{root} is not empty; choose a fresh output root")
    cw = pd.read_csv(WORK / "crosswalk_union.csv", dtype={"admin_key": str})
    cw["block"] = cw.block.fillna("")
    files, admin_rows = 0, []
    for model in MODELS:
        cal = WORK / "calibrated" / model
        hist = load_frame(cal / "historical.npz")
        fut = pd.concat([load_frame(cal / f"{n}.npz") for n in FUTURE]).sort_index()
        values = {"historical": unit_years(hist, HIST), "ssp245": unit_years(fut, FUTURE_SPAN)}
        for state in STATES:
            w = V.crosswalk_weights(cw[cw.state == state], KEYS)
            status = V.overlap_status(w, KEYS)
            names = w.groupby(KEYS)[["district", "block"]].first().reset_index()
            cov = status.merge(names, on=KEYS)
            for scenario, vals in values.items():
                admin = V.count_first_transfer(vals, w, KEYS, excluded=status)
                admin = admin.merge(cov[KEYS + ["district", "block", "coverage", "overlap_fraction"]], on=KEYS)
                admin["state"], admin["model"], admin["scenario"] = state, model, scenario
                admin_rows.append(admin)
                files += write_admin_csvs(root, admin, state, model, scenario, hydro_fs_token)
    admin = pd.concat(admin_rows, ignore_index=True)
    admin.to_csv(WORK / "admin_yearly_all.csv.gz", index=False)
    excl = admin[admin.exclusion_reason != ""]
    return {"out_root": str(root), "files": files, "admin_year_rows": len(admin),
            "excluded_admin_units": int(excl.groupby(["state"] + KEYS).ngroups),
            "nan_values_not_excluded": int(admin[(admin.exclusion_reason == "") & admin.value.isna()].shape[0]),
            "signature": SIGNATURE}


def write_admin_csvs(root: Path, admin: pd.DataFrame, state: str, model: str, scenario: str,
                     token) -> int:
    """One yearly + one periods CSV per admin unit, slug, model and scenario."""
    n = 0
    sources = (f"NEX-GDDP-CMIP6 {model} r1i1p1f1 {scenario} tas,tasmax,huss; UHE-Daily 1985-2014; "
               "CarbonPlan extreme-heat v1.0 @ f662b372 (cells, weights, elevation, notebook 06)")
    for (slug, level, district, block), g in admin.groupby(["slug", "level", "district", "block"]):
        col = SLUG_COLUMN[slug]
        g = g.sort_values("year")
        y = pd.DataFrame({"year": g.year.astype(int).to_numpy(), "value": g.value.to_numpy()})
        y[col] = y["value"]
        y["source_file"] = sources
        y["method_version"], y["aggregation_method"] = METHOD_VERSION, AGGREGATION
        y["climate_fill_method"], y["shade_method_signature"] = "native", SIGNATURE
        y["valid_area_fraction"] = g.series_coverage.to_numpy()
        y["coverage_union_fraction"] = g.coverage.to_numpy()
        y["overlap_fraction"] = g.overlap_fraction.to_numpy()
        y["contributing_units"] = g.contributing_units.to_numpy()
        y["pilot_exclusion_reason"] = g.exclusion_reason.to_numpy()
        y["district"] = district
        if level == "block":
            y["block"] = block
        y["scenario"], y["model"] = scenario, model
        prow = []
        for label, w in PERIODS[scenario].items():
            sel = y[(y.year >= w.start) & (y.year <= w.end)]
            used = int(sel.value.notna().sum())
            requested = w.end - w.start + 1
            value = float(sel.value.mean()) if used == requested else np.nan
            r = {"district": district, **({"block": block} if level == "block" else {}), "value": value,
                 "period": label, "years_used_count": used, "years_requested": requested, col: value,
                 "method_version": METHOD_VERSION, "aggregation_method": AGGREGATION,
                 "shade_method_signature": SIGNATURE, "climate_fill_method": "native",
                 "pilot_exclusion_reason": str(g.exclusion_reason.iloc[0]), "model": model, "scenario": scenario}
            prow.append(r)
        if level == "block":
            d = root / f"wbgt_shade_stull_{slug}" / state / "blocks" / token(district) / token(block) / model / scenario
            stem = token(block)
        else:
            d = root / f"wbgt_shade_stull_{slug}" / state / "districts" / token(district) / model / scenario
            stem = token(district)
        d.mkdir(parents=True, exist_ok=True)
        y.to_csv(d / f"{stem}_yearly.csv", index=False)
        pd.DataFrame(prow).to_csv(d / f"{stem}_periods.csv", index=False)
        n += 2
    return n


# --------------------------------------------------------------------------- verify [irt]

def stage_verify(out_root: Path) -> dict:
    """Read the pilot through master_builder: stock guard must refuse; patched read must round-trip."""
    from unittest import mock

    from india_resilience_tool.compute import master_builder as MB
    from india_resilience_tool.data import wbgt_contract as WC
    from india_resilience_tool.utils.naming import hydro_fs_token

    written = read_admin()
    # the reader rebuilds names from folder tokens ('_' -> ' '); compare on that form
    for c in ("district", "block"):
        written[c] = [hydro_fs_token(x).replace("_", " ") if x else "" for x in written[c]]
    res: dict = {"refused_by_stock_guard": {}, "roundtrip": {}, "masters": {}}
    root = out_root.resolve()
    for slug, col in SLUG_COLUMN.items():
        metric_root = root / f"wbgt_shade_stull_{slug}"
        state = "Kerala"
        try:
            MB._collect_district_data(metric_root / state, state, [col, "value"], verbose=False)
            res["refused_by_stock_guard"][slug] = False
        except ValueError as exc:
            res["refused_by_stock_guard"][slug] = "stale shade signature" in str(exc)
        with mock.patch.object(WC, "SHADE_METHOD_SIGNATURE", SIGNATURE), \
                mock.patch.object(MB, "SHADE_METHOD_SIGNATURE", SIGNATURE):
            for state in STATES:
                for level in ("district", "block"):
                    collect = MB._collect_block_data if level == "block" else MB._collect_district_data
                    periods, yearly = collect(metric_root / state, state, [col, "value"], verbose=False)
                    w = written[(written.state == state) & (written.level == level) & (written.slug == slug)]
                    key = ["district", "model", "scenario", "year"] + (["block"] if level == "block" else [])
                    yearly = yearly.assign(year=yearly.year.astype(int))
                    if level == "block":
                        yearly["block"] = yearly.block.astype(str)
                    j = w.merge(yearly, on=key, how="outer", suffixes=("_w", "_r"), indicator=True)
                    both = j[j._merge == "both"]
                    same = np.allclose(both.value_w.to_numpy(float), both.value_r.to_numpy(float),
                                       rtol=0, atol=0, equal_nan=True)
                    res["roundtrip"][f"{slug}|{state}|{level}"] = {
                        "written": len(w), "read": len(yearly), "unmatched": int((j._merge != "both").sum()),
                        "values_identical": bool(same), "period_rows": len(periods)}
                    master_path = root / f"wbgt_shade_stull_{slug}" / state / f"master_metrics_by_{level}.csv"
                    master = MB.build_master_metrics(str(metric_root), state, metric_col_in_periods=col,
                                                     out_path=str(master_path), verbose=False, level=level,
                                                     metric_col_candidates=[col, "value"])
                    n_models = [c for c in master.columns if c.endswith("__n_models")]
                    res["masters"][f"{slug}|{state}|{level}"] = {
                        "rows": len(master), "n_models_values": sorted({int(v) for c in n_models
                                                                         for v in master[c].dropna()})}
    res["all_refused"] = all(res["refused_by_stock_guard"].values())
    res["all_roundtrip"] = all(v["values_identical"] and v["unmatched"] == 0 for v in res["roundtrip"].values())
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "reader_verification.json").write_text(json.dumps(res, indent=2, default=str))
    return {"all_refused": res["all_refused"], "all_roundtrip": res["all_roundtrip"],
            "checks": len(res["roundtrip"])}


# --------------------------------------------------------------------------- compare [irt]

def load_v1_yearly(state: str, model: str, slug: str) -> pd.DataFrame:
    """shade-peak-v1 yearly values from the staging tree, read-only; signature checked."""
    frames = []
    root = STAGING / f"wbgt_shade_stull_{slug}" / state
    for level, pattern in (("district", f"districts/*/{model}/*/*_yearly.csv"),
                           ("block", f"blocks/*/*/{model}/*/*_yearly.csv")):
        for path in sorted(root.glob(pattern)):
            if path.parent.name not in PERIODS:
                continue
            f = pd.read_csv(path)
            if not f.shade_method_signature.str.startswith("shade-peak-v1:").all():
                raise ValueError(f"Unexpected method signature in {path}")
            f["level"] = level
            if "block" not in f:
                f["block"] = ""
            frames.append(f[["level", "district", "block", "scenario", "year", "value"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def stage_compare() -> dict:
    """Pilot vs shade-peak-v1 on the same years, per state, level, slug, model and period."""
    from scipy.stats import spearmanr

    admin = read_admin()
    rows, joined = [], []
    for state in STATES:
        for model in MODELS:
            for slug in SLUG_COLUMN:
                v1 = load_v1_yearly(state, model, slug)
                mine = admin[(admin.state == state) & (admin.model == model) & (admin.slug == slug)]
                for scenario, periods in PERIODS.items():
                    for label, w in periods.items():
                        def per(frame):
                            f = frame[(frame.scenario == scenario) & (frame.year >= w.start) & (frame.year <= w.end)]
                            return f.groupby(["level", "district", "block"]).value.agg(["mean", "count"])
                        a, b = per(mine), per(v1) if len(v1) else None
                        if b is None:
                            continue
                        excl = (mine[mine.exclusion_reason != ""].groupby(["level", "district", "block"]).size()
                                .index)
                        j = a.join(b, lsuffix="_pilot", rsuffix="_v1", how="outer")
                        j["excluded"] = j.index.isin(excl)
                        j = j.reset_index().assign(state=state, model=model, slug=slug, scenario=scenario, period=label)
                        joined.append(j)
                        for level, g in j.groupby("level"):
                            ok = g[(g.count_pilot == w.end - w.start + 1) & (g.count_v1 == w.end - w.start + 1)
                                   & ~g.excluded].dropna(subset=["mean_pilot", "mean_v1"])
                            d = ok.mean_pilot - ok.mean_v1
                            rho = (spearmanr(ok.mean_pilot, ok.mean_v1).correlation
                                   if len(ok) > 2 and ok.mean_pilot.nunique() > 1 and ok.mean_v1.nunique() > 1
                                   else np.nan)
                            rows.append({"state": state, "level": level, "model": model, "slug": slug,
                                         "scenario": scenario, "period": label, "units_compared": len(ok),
                                         "units_excluded": int(g.excluded.sum()),
                                         "units_unmatched": int(g[["mean_pilot", "mean_v1"]].isna().any(axis=1).sum()),
                                         "units_v1_incomplete_years": int((g.count_v1 < w.end - w.start + 1).sum()),
                                         "units_pilot_incomplete_years": int(
                                             ((g.count_pilot < w.end - w.start + 1) & ~g.excluded).sum()),
                                         "pilot_median": ok.mean_pilot.median(), "v1_median": ok.mean_v1.median(),
                                         "median_pilot_minus_v1": d.median(), "median_abs_diff": d.abs().median(),
                                         "max_abs_diff": d.abs().max(), "spearman": rho})
    summary = pd.DataFrame(rows)
    out = RESULTS / "comparison"
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "pilot_vs_shade_peak_v1_summary.csv", index=False)
    pd.concat(joined, ignore_index=True).to_csv(out / "pilot_vs_shade_peak_v1_units.csv.gz", index=False)
    return {"summary_rows": len(summary), "max_unmatched": int(summary.units_unmatched.max())}


# --------------------------------------------------------------------------- main

def main() -> None:
    """Dispatch one stage and record its wall time and peak memory."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stage", choices=("coverage", "fetch-parity", "calibrate", "parity", "write", "verify",
                                          "compare"))
    parser.add_argument("--model", choices=MODELS, help="calibrate: the one model to run")
    parser.add_argument("--out-root", type=Path, default=PROCESSED, help="write/verify: pilot processed root")
    parser.add_argument("--dry-run", action="store_true", help="print the plan for the stage and exit")
    args = parser.parse_args()
    if args.dry_run:
        print(json.dumps({"stage": args.stage, "model": args.model, "out_root": str(args.out_root),
                          "work": str(WORK), "results": str(RESULTS), "signature": SIGNATURE}, indent=1))
        return
    started = time.time()
    if args.stage == "coverage":
        extra = stage_coverage()
    elif args.stage == "fetch-parity":
        extra = stage_fetch_parity()
    elif args.stage == "calibrate":
        if not args.model:
            parser.error("calibrate needs --model")
        extra = stage_calibrate(args.model)
    elif args.stage == "parity":
        extra = stage_parity()
    elif args.stage == "write":
        extra = stage_write(args.out_root)
    elif args.stage == "verify":
        extra = stage_verify(args.out_root)
    else:
        extra = stage_compare()
    log_stage(args.stage + (f":{args.model}" if args.model else ""), started, extra)


if __name__ == "__main__":
    main()
