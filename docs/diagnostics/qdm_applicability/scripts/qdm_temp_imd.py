"""tasmax/tasmin metrics and the aridity index: QDM applicability against IMD 1-degree gridded temperature.

Read-only diagnostic. Same holdout/future design, metric definitions, floors and verdict rule as
qdm_pr_imd.py. Units are IMD 1-degree boxes; NEX is the mean of the 16 NEX cells in each box
(extract_temp_national.py). The rainfall reference for the aridity index is IMD 0.25-degree rain
area-averaged onto the same box (edge rows/columns half weight, since IMD rain centres sit on
whole and quarter degrees).

QDM: additive, 31-day window for tasmax/tasmin (as for all temperature metrics); multiplicative,
91-day window for pr (the window §5 showed is needed for rainfall). tasmax and tasmin are
corrected independently, as production QDM would; inverted days (tasmin > tasmax) are counted
because Hargreaves PET is undefined on them.
"""
from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import imdlib as imd

sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts")
sys.path.insert(0, ".")
import qdm_applicability as Q  # noqa: E402
from qdm_pr_imd import classify  # noqa: E402
from india_resilience_tool.compute.evapotranspiration import (  # noqa: E402
    extraterrestrial_radiation_mm, hargreaves_pet_mm_per_day,
)

SER = Path(".scratch/qdm_applicability/series_temp_national")
OUT = Path(".scratch/qdm_applicability/out_imd_temp"); OUT.mkdir(parents=True, exist_ok=True)
IMD = "D:/projects/irt_data/imd"
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
TEMP_METRICS = [
    "txx_annual_max", "tnx_annual_max", "tnn_annual_min", "txge30_hot_days", "txge35_extreme_heat_days",
    "tasmin_tropical_nights_gt25", "tasmin_tropical_nights_gt28", "tnle10_cold_nights", "tnle5_severe_cold_nights",
    "txle15_cold_days", "tnle10_consecutive_cold_nights", "tasmax_summer_mean", "tasmin_winter_mean",
    "tasmin_winter_min", "tx90p_hot_days_pct", "tn90p_warm_nights_pct", "tx10p_cool_days_pct",
    "tn10p_cool_nights_pct", "wsdi_warm_spell_days", "csdi_cold_spell_days", "hwfi_events_tmean_90p",
    "hwa_heatwave_amplitude",
]
W_TEMP, W_PR = 31, 91


def aridity_metric(lat):
    """Mean over years of annual P / annual Hargreaves PET (production formula, Ra from latitude + doy)."""
    def f(e, b):
        ra = extraterrestrial_radiation_mm(np.full(len(e), lat), e.doy.values)
        pet = hargreaves_pet_mm_per_day(e.tasmax.values, e.tasmin.values, ra)   # NaN on inverted days
        d = pd.DataFrame({"year": e.year.values, "pr": e.pr.values, "pet": pet})
        ann = d.groupby("year").agg(pr=("pr", "sum"), pet=("pet", "sum"), bad=("pet", lambda s: s.isna().mean()))
        ann = ann[ann.bad <= 0.10]                                              # ~ production 90 % daily coverage
        return float((ann.pr / ann.pet).mean())
    return f


def corrector(ref, hist, target):
    out = target.copy()
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=W_TEMP)
    for c in ("tasmax", "tasmin"):
        out[c] = Q.qdm_add(ref.rename(columns={c: "v"}), hist.rename(columns={c: "v"}), target.rename(columns={c: "v"}))
    Q.KW = dict(nquantiles=Q.NQUANTILES, window=W_PR)
    out["pr"] = Q.qdm_mult(ref.rename(columns={"pr": "v"}), hist.rename(columns={"pr": "v"}), target.rename(columns={"pr": "v"}))
    return out


def imd_refs(boxes):
    tx = imd.open_data("tmax", 1990, 2010, "yearwise", IMD).get_xarray()["tmax"]
    tn = imd.open_data("tmin", 1990, 2010, "yearwise", IMD).get_xarray()["tmin"]
    rn = imd.open_data("rain", 1990, 2010, "yearwise", IMD).get_xarray()["rain"]
    rn = rn.where(rn > -998)
    t = pd.to_datetime(tx.time.values)
    assert (pd.to_datetime(rn.time.values) == t).all()
    w1 = np.array([0.5, 1, 1, 1, 0.5]); w2 = np.outer(w1, w1); w2 /= w2.sum()
    refs = {}
    for la, lo in boxes:
        sub = rn.sel(lat=slice(la - 0.5 - 1e-6, la + 0.5 + 1e-6), lon=slice(lo - 0.5 - 1e-6, lo + 0.5 + 1e-6)).values
        assert sub.shape[1:] == (5, 5), (la, lo, sub.shape)
        pr = np.einsum("tij,ij->t", np.nan_to_num(sub), w2)
        wsum = np.einsum("tij,ij->t", np.isfinite(sub).astype(float), w2)
        pr = np.where(wsum >= 0.75, pr / np.maximum(wsum, 1e-9), np.nan)      # sea-edge boxes: land-weighted mean
        refs[(la, lo)] = Q._finish(pd.DataFrame({
            "year": t.year, "month": t.month, "day": t.day,
            "tasmax": tx.sel(lat=la, lon=lo).values, "tasmin": tn.sel(lat=la, lon=lo).values, "pr": pr}))
    return refs


def run_model(args):
    model, boxes, refs = args
    def load(exp):
        fr = {v: pd.read_parquet(SER / f"{model}_{exp}_{v}.parquet") for v in ("tasmax", "tasmin", "pr")}
        return fr
    H, F = load("historical"), load("ssp245")
    base = Q.make_metrics()
    rows, inv = [], []
    for la, lo in boxes:
        key = f"{la:.1f}_{lo:.1f}"
        ref = refs[(la, lo)]
        if ref[["tasmax", "tasmin", "pr"]].isna().mean().max() > 0.01:
            continue
        def frame(D):
            return Q._finish(pd.DataFrame({"year": D["tasmax"].year, "month": D["tasmax"].month, "day": D["tasmax"].day,
                                           **{v: D[v][key].values for v in ("tasmax", "tasmin", "pr")}}))
        hist, fut = frame(H), frame(F)
        if hist[["tasmax", "tasmin"]].isna().any().any():
            continue
        metrics = {m: base[m] for m in TEMP_METRICS}
        metrics["aridity_index_p_over_pet"] = ("ratio", aridity_metric(la))
        for r in Q.score_unit(metrics, ref, hist, fut, corrector):
            rows.append(dict(unit=key, lat=la, lon=lo, model=model, **r))
        qf = corrector(ref, hist, fut)
        inv.append(dict(unit=key, model=model, raw_fut=float((fut.tasmin > fut.tasmax).mean()),
                        qdm_fut=float((qf.tasmin > qf.tasmax).mean()), ref=float((ref.tasmin > ref.tasmax).mean())))
    return rows, inv


def main():
    boxes = [tuple(x) for x in pd.read_csv(SER / "boxes.csv").itertuples(index=False)]
    refs = imd_refs(boxes)
    rows, inv = [], []
    with ProcessPoolExecutor(5) as ex:
        for m, (r, i) in zip(MODELS, ex.map(run_model, [(m, boxes, refs) for m in MODELS])):
            rows += r; inv += i
            print("done", m, len(r), flush=True)
    d = pd.DataFrame(rows)
    d.to_csv(OUT / "temperature_imd_unit_rows.csv", index=False)
    pd.DataFrame(inv).to_csv(OUT / "inversions.csv", index=False)

    nat = d.groupby("metric").apply(classify)
    sb = d.assign(raw=d.raw_test - d.ref_test, qdm=d.qdm_test - d.ref_test).groupby("metric")[["raw", "qdm"]].mean()
    nat = nat.join(sb.rename(columns={"raw": "bias_raw", "qdm": "bias_qdm"}))
    era = pd.read_csv("docs/diagnostics/qdm_applicability/data/summary_final.csv", index_col=0)
    vcol = "verdict" if "verdict" in era.columns else era.columns[-1]
    nat["verdict_era5_sites"] = era[vcol].reindex(nat.index)
    nat.round(3).to_csv(OUT / "summary_temp_imd.csv")
    pd.set_option("display.width", 250, "display.max_rows", 300)
    print(nat[["ref", "bias_raw", "bias_qdm", "err_raw", "err_qdm", "raw_2050", "qdm_2050", "chg_raw", "chg_qdm",
               "n_units", "verdict", "verdict_era5_sites"]].round(2).sort_values("verdict").to_string())
    iv = pd.DataFrame(inv)
    print("\ninverted days (tasmin > tasmax), ssp245 2040-60: raw %.4f%%  QDM %.4f%%  IMD %.4f%%; max per unit QDM %.3f%%"
          % (100 * iv.raw_fut.mean(), 100 * iv.qdm_fut.mean(), 100 * iv.ref.mean(), 100 * iv.qdm_fut.max()))


if __name__ == "__main__":
    main()
