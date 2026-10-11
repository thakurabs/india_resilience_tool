"""Does QDM against a reference produce a measurable improvement, metric by metric?

Read-only diagnostic. For every deployed NEX-derived metric that a local reference can score:

  holdout : QDM trained on 1990-2002, scored on 2003-2010 against the reference
            (ERA5 at six sites for temperature/humidity; ERA5 Telangana grid for
            precipitation and aridity).
  future  : QDM trained on 1990-2010, applied to ssp245 2040-2060; how much the
            mapped mid-century value and the change signal move.

Metric definitions are site-level re-implementations of the registry parameters
(not the production code path); raw, QDM and reference all use the identical
implementation, so the comparison isolates the correction. Baseline-relative
metrics use a common 1990-2010 baseline for all sources.

Materiality floors are declared up front (FLOORS) and applied mechanically.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from scipy import stats

sys.path.insert(0, ".")
from tools.diagnostics.wbgt_qdm_bias_correction import (  # noqa: E402
    GROUP_WINDOW_DAYS, NQUANTILES, pseudo_doy, qdm_adjust, quantile_table,
)
from india_resilience_tool.compute.evapotranspiration import (  # noqa: E402
    extraterrestrial_radiation_mm, hargreaves_pet_mm_per_day,
)

SERIES = Path(".scratch/qdm_applicability/series")
ERA5_SITE = Path("scratch/wbgt_deployed_vs_reference_cache")
ERA5_GRID = Path("D:/projects/irt_data/era5/nc")
OUT = Path(".scratch/qdm_applicability/out")
OUT.mkdir(parents=True, exist_ok=True)

SITES = {"kochi": 9.93, "kolkata": 22.57, "bikaner": 28.02, "lucknow": 26.85, "hyderabad": 17.39, "shimla": 31.10}
HIST = list(range(1990, 2011)); TRAIN = list(range(1990, 2003)); TEST = list(range(2003, 2011)); FUT = list(range(2040, 2061))
KW = dict(nquantiles=NQUANTILES, window=GROUP_WINDOW_DAYS)
TRACE_MM = 0.1
RNG = np.random.default_rng(20261011)

# Materiality floor per metric family, in the metric's own units. Declared before results.
FLOORS = {"count": 5.0, "degC": 0.5, "pct": 1.0, "spell": 1.0, "mm_rel": 0.10, "events": 0.5, "ratio": 0.05}


def stull(t, rh):
    rh = np.clip(rh, 0, 100)
    return (t * np.arctan(0.151977 * np.sqrt(rh + 8.313659)) + np.arctan(t + rh) - np.arctan(rh - 1.676331)
            + 0.00391838 * rh ** 1.5 * np.arctan(0.023101 * rh) - 4.686035)


def swbgt(t, rh):
    rh = np.clip(rh, 0, 100)
    e = 6.112 * np.exp(17.62 * t / (243.12 + t)) * rh / 100.0
    return 0.567 * t + 0.393 * e + 3.94


# ----------------------------------------------------------------------------- QDM
def qdm_add(ref, hist, target):
    """Additive QDM of target['v'] trained on ref/hist ('v', 'doy' columns)."""
    qr = quantile_table(ref.v.values, ref.doy.values, **KW)
    qh = quantile_table(hist.v.values, hist.doy.values, **KW)
    return qdm_adjust(target.v.values, target.doy.values, q_ref=qr, q_hist=qh, **KW)


def _jitter(x):
    x = np.asarray(x, float).copy()
    z = np.isfinite(x) & (x < TRACE_MM)
    x[z] = RNG.uniform(TRACE_MM / 10, TRACE_MM, z.sum())
    return x


def qdm_mult(ref, hist, target):
    """Multiplicative QDM for precipitation with trace-value jitter (Cannon et al. 2015)."""
    levels = (np.arange(NQUANTILES) + 0.5) / NQUANTILES
    r, h, s = _jitter(ref.v.values), _jitter(hist.v.values), _jitter(target.v.values)
    qr = quantile_table(r, ref.doy.values, **KW)
    qh = quantile_table(h, hist.doy.values, **KW)
    qs = quantile_table(s, target.doy.values, **KW)
    out = np.full(s.shape, np.nan)
    doys = target.doy.values
    for d in range(1, 366):
        m = doys == d
        if not m.any() or not (np.isfinite(qr[d - 1]).all() and np.isfinite(qh[d - 1]).all() and np.isfinite(qs[d - 1]).all()):
            continue
        tau = np.interp(s[m], qs[d - 1], levels)
        out[m] = np.interp(tau, levels, qr[d - 1]) * s[m] / np.maximum(np.interp(tau, levels, qh[d - 1]), 1e-6)
    out[out < TRACE_MM] = 0.0
    return out


# ----------------------------------------------------------------------------- metric helpers
def _runs(flag):
    """Lengths and start indices of True runs."""
    f = np.r_[False, np.asarray(flag, bool), False]
    d = np.diff(f.astype(int))
    st, en = np.where(d == 1)[0], np.where(d == -1)[0]
    return en - st, st


def doy_threshold(base, col, pct, window=5):
    """Per-pseudo-doy percentile of base[col] in a centred window (ETCCDI style)."""
    half = window // 2
    v, d = base[col].values, base.doy.values
    thr = np.full(366, np.nan)
    for doy in range(1, 366):
        mem = ((np.arange(doy - half, doy + half + 1) - 1) % 365) + 1
        s = v[np.isin(d, mem)]
        s = s[np.isfinite(s)]
        if s.size:
            thr[doy] = np.percentile(s, pct)
    return thr


def spi_monthly(df, base_years, scale):
    """SPI on monthly totals (gamma, method of moments, with zero probability), per calendar month."""
    m = df.groupby(["year", "month"]).pr.sum().reset_index()
    m["acc"] = m.pr.rolling(scale, min_periods=scale).sum()
    out = np.full(len(m), np.nan)
    for mo in range(1, 13):
        sel = m.month == mo
        b = m[sel & m.year.isin(base_years)].acc.dropna().values
        if b.size < 10:
            continue
        p0 = np.mean(b <= 0)
        pos = b[b > 0]
        mu, var = pos.mean(), pos.var(ddof=1)
        k, th = mu * mu / var, var / mu
        x = m[sel].acc.values
        cdf = p0 + (1 - p0) * stats.gamma.cdf(np.where(x > 0, x, 0), k, scale=th)
        cdf = np.clip(cdf, 1e-6, 1 - 1e-6)
        out[sel.values] = np.where(np.isfinite(x), stats.norm.ppf(cdf), np.nan)
    m["spi"] = out
    return m


# Each metric: (family, function(eval_df, base_df) -> mean over eval years)
def _yearly(df, fn):
    return float(np.nanmean([fn(g) for _, g in df.groupby("year")]))


def make_metrics():
    M = {}
    T = lambda c, op, t: (lambda e, b: _yearly(e, lambda g: np.nansum(op(g[c].values, t))))  # noqa: E731
    ge, gt, le = np.greater_equal, np.greater, np.less_equal
    M["txx_annual_max"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmax(g.tasmax)))
    M["tnx_annual_max"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmax(g.tasmin)))
    M["tnn_annual_min"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmin(g.tasmin)))
    M["txge30_hot_days"] = ("count", T("tasmax", ge, 30.0))
    M["txge35_extreme_heat_days"] = ("count", T("tasmax", ge, 35.0))
    M["tasmin_tropical_nights_gt25"] = ("count", T("tasmin", gt, 25.0))
    M["tasmin_tropical_nights_gt28"] = ("count", T("tasmin", gt, 28.0))
    M["tnle10_cold_nights"] = ("count", T("tasmin", le, 10.0))
    M["tnle5_severe_cold_nights"] = ("count", T("tasmin", le, 5.0))
    M["txle15_cold_days"] = ("count", T("tasmax", le, 15.0))
    M["tnle10_consecutive_cold_nights"] = ("spell", lambda e, b: _yearly(e, lambda g: max(_runs(g.tasmin.values <= 10.0)[0], default=0)))
    seas = lambda c, mon, f: (lambda e, b: _yearly(e, lambda g: f(g[g.month.isin(mon)][c])))  # noqa: E731
    M["tas_annual_mean"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmean(g.tas)))
    M["tas_summer_mean"] = ("degC", seas("tas", [3, 4, 5], np.nanmean))
    M["tas_winter_mean"] = ("degC", seas("tas", [12, 1, 2], np.nanmean))
    M["tasmax_summer_mean"] = ("degC", seas("tasmax", [3, 4, 5], np.nanmean))
    M["tasmin_winter_mean"] = ("degC", seas("tasmin", [12, 1, 2], np.nanmean))
    M["tasmin_winter_min"] = ("degC", seas("tasmin", [12, 1, 2], np.nanmin))

    def pctdays(col, pct, below):
        def f(e, b):
            thr = doy_threshold(b, col, pct)[e.doy.values]
            x = e[col].values
            flag = (x < thr) if below else (x > thr)
            return float(100.0 * np.nanmean(pd.Series(flag).groupby(e.year.values).mean()))
        return f
    M["tx90p_hot_days_pct"] = ("pct", pctdays("tasmax", 90, False))
    M["tn90p_warm_nights_pct"] = ("pct", pctdays("tasmin", 90, False))
    M["tx10p_cool_days_pct"] = ("pct", pctdays("tasmax", 10, True))
    M["tn10p_cool_nights_pct"] = ("pct", pctdays("tasmin", 10, True))

    def spell_days(col, pct, below, minlen):
        def f(e, b):
            thr = doy_threshold(b, col, pct)
            def yr(g):
                x, t = g[col].values, thr[g.doy.values]
                L, _ = _runs((x < t) if below else (x > t))
                return float(L[L >= minlen].sum())
            return _yearly(e, yr)
        return f
    M["wsdi_warm_spell_days"] = ("count", spell_days("tasmax", 90, False, 6))
    M["csdi_cold_spell_days"] = ("count", spell_days("tasmin", 10, True, 6))
    M["hwfi_tmean_90p"] = ("count", spell_days("tas", 90, False, 5))

    def hw_events(e, b):
        thr = doy_threshold(b, "tasmax", 90)
        return _yearly(e, lambda g: float((_runs(g.tasmax.values > thr[g.doy.values])[0] >= 5).sum()))
    M["hwfi_events_tmean_90p"] = ("events", hw_events)

    def hwa(e, b):
        thr = doy_threshold(b, "tasmax", 90)
        def yr(g):
            x = g.tasmax.values
            L, S = _runs(x > thr[g.doy.values])
            best = [(x[s:s + n].mean(), x[s:s + n].max()) for n, s in zip(L, S) if n >= 5]
            return max(best)[1] if best else np.nan
        return _yearly(e, yr)
    M["hwa_heatwave_amplitude"] = ("degC", hwa)

    M["twb_annual_mean"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmean(g.twb)))
    M["twb_summer_mean"] = ("degC", seas("twb", [3, 4, 5], np.nanmean))
    M["twb_annual_max"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmax(g.twb)))
    M["twb_days_ge_28"] = ("count", T("twb", ge, 28.0))
    M["twb_days_ge_30"] = ("count", T("twb", ge, 30.0))
    M["swbgt_empirical_annual_mean"] = ("degC", lambda e, b: _yearly(e, lambda g: np.nanmean(g.swbgt)))
    for t in (28, 30, 32):
        M[f"swbgt_empirical_days_ge_{t}"] = ("count", T("swbgt", ge, float(t)))
    return M


def make_pr_metrics():
    M = {}
    M["pr_max_1day_precip"] = ("mm_rel", lambda e, b: _yearly(e, lambda g: np.nanmax(g.pr)))
    M["pr_max_5day_precip"] = ("mm_rel", lambda e, b: _yearly(e, lambda g: np.nanmax(g.pr.rolling(5).sum())))
    M["r20mm_very_heavy_precip_days"] = ("count", lambda e, b: _yearly(e, lambda g: float((g.pr >= 20).sum())))
    M["cwd_consecutive_wet_days"] = ("spell", lambda e, b: _yearly(e, lambda g: max(_runs(g.pr.values >= 1)[0], default=0)))
    M["pr_consecutive_dry_days_lt1mm"] = ("spell", lambda e, b: _yearly(e, lambda g: max(_runs(g.pr.values < 1)[0], default=0)))

    def rnp(p, contrib):
        def f(e, b):
            wet = b.pr[b.pr >= 1.0]
            thr = np.percentile(wet, p) if len(wet) else np.nan
            def yr(g):
                w = g.pr[g.pr >= 1.0]
                ex = w[w >= thr].sum()
                return 100.0 * ex / w.sum() if contrib else ex
            return _yearly(e, yr)
        return f
    M["r95p_very_wet_precip"] = ("mm_rel", rnp(95, False))
    M["r99p_extreme_wet_precip"] = ("mm_rel", rnp(99, False))
    M["r95ptot_contribution_pct"] = ("pct", rnp(95, True))

    def spi_metric(scale, kind):
        def f(e, b):
            both = pd.concat([b, e]).drop_duplicates(["year", "month", "day"]).sort_values(["year", "month", "day"])
            s = spi_monthly(both, sorted(b.year.unique()), scale)
            s = s[s.year.isin(e.year.unique())]
            def yr(g):
                L, _ = _runs(g.spi.values < -1.0)
                return {"months": float((g.spi < -1).sum()), "events": float(len(L)), "spell": float(max(L, default=0))}[kind]
            return float(np.nanmean([yr(g) for _, g in s.groupby("year")]))
        return f
    M["spi3_count_months_lt_minus1"] = ("count", spi_metric(3, "months"))
    M["spi3_count_events_lt_minus1"] = ("events", spi_metric(3, "events"))
    M["spi3_max_spell_lt_minus1"] = ("spell", spi_metric(3, "spell"))
    M["spi6_count_events_lt_minus1"] = ("events", spi_metric(6, "events"))
    M["spi6_max_spell_lt_minus1"] = ("spell", spi_metric(6, "spell"))
    M["spi12_count_events_lt_minus1"] = ("events", spi_metric(12, "events"))
    M["spi12_max_spell_lt_minus1"] = ("spell", spi_metric(12, "spell"))
    return M


# ----------------------------------------------------------------------------- data
def era5_sites():
    out = []
    for s in SITES:
        h = pd.concat([pd.read_parquet(ERA5_SITE / f"{s}_{y}.parquet") for y in HIST])
        h.index = pd.to_datetime(h.index) + pd.Timedelta(hours=5.5)
        h = h[(h.index.year >= HIST[0]) & (h.index.year <= HIST[-1])]
        d = h.resample("D").agg({"temperature_2m": ["mean", "max", "min"], "relative_humidity_2m": "mean"})
        d.columns = ["tas", "tasmax", "tasmin", "hurs"]
        out.append(pd.DataFrame({"site": s, "year": d.index.year, "month": d.index.month, "day": d.index.day, **{c: d[c].values for c in d}}))
    return _finish(pd.concat(out, ignore_index=True))


def _finish(df):
    df = df[~((df.month == 2) & (df.day == 29))].copy()
    df["doy"] = pseudo_doy(df.month.values, df.day.values)
    return df.sort_values(["year", "month", "day"]).reset_index(drop=True)


def nex_sites():
    df = pd.concat([pd.read_parquet(f) for f in glob.glob(str(SERIES / "*_*.parquet"))], ignore_index=True)
    for c in ("tas", "tasmax", "tasmin"):
        df[c] = df[c] - 273.15
    df["pr"] = df["pr"] * 86400.0
    return _finish(df)


def era5_grid(var, years):
    files = [ERA5_GRID / var / f for y in years for f in sorted(p.name for p in (ERA5_GRID / var).glob(f"*_{y}??_tel.nc"))]
    ds = xr.open_mfdataset(files, combine="by_coords")
    v = list(ds.data_vars)[0]
    return ds[v].rename({"valid_time": "time", "latitude": "lat", "longitude": "lon"}).sortby("lat").load()


def to_nex_cells(da, lats, lons):
    """Mean of the four ERA5 points surrounding each NEX cell centre."""
    acc = 0
    for dl in (-0.125, 0.125):
        for dn in (-0.125, 0.125):
            acc = acc + da.sel(lat=xr.DataArray(lats + dl, dims="cell"), lon=xr.DataArray(lons + dn, dims="cell"), method="nearest").values
    return acc / 4.0


# ----------------------------------------------------------------------------- scoring
def score_unit(metrics, ref, hist, fut, correct):
    """Return rows for one unit/model. correct(ref_tr, hist_tr, target) -> corrected frame."""
    rows = []
    tr_ref, tr_hist = ref[ref.year.isin(TRAIN)], hist[hist.year.isin(TRAIN)]
    hold = correct(tr_ref, tr_hist, hist)          # 2003-2010 out of sample
    qh = correct(ref, hist, hist)                  # full-baseline fit
    qf = correct(ref, hist, fut)
    for slug, (fam, fn) in metrics.items():
        try:
            r_t = fn(ref[ref.year.isin(TEST)], ref)
            raw_t = fn(hist[hist.year.isin(TEST)], hist)
            q_t = fn(hold[hold.year.isin(TEST)], hold)
            raw_h, raw_f = fn(hist, hist), fn(fut, hist)
            q_h, q_f = fn(qh, qh), fn(qf, qh)
        except Exception as exc:  # pragma: no cover - diagnostic
            print("  fail", slug, exc, flush=True)
            continue
        rows.append(dict(metric=slug, family=fam, ref_test=r_t, raw_test=raw_t, qdm_test=q_t,
                         raw_hist=raw_h, raw_fut=raw_f, qdm_hist=q_h, qdm_fut=q_f))
    return rows


def corrector(cols_add, derived):
    """Build a frame-level corrector: additive QDM per column, then derived quantities corrected directly."""
    def run(ref, hist, target):
        out = target.copy()
        for c in cols_add:
            out[c] = qdm_add(ref.rename(columns={c: "v"}), hist.rename(columns={c: "v"}), target.rename(columns={c: "v"}))
        for c in derived:
            out[c] = qdm_add(ref.rename(columns={c: "v"}), hist.rename(columns={c: "v"}), target.rename(columns={c: "v"}))
        return out
    return run


def pr_corrector(ref, hist, target):
    out = target.copy()
    out["pr"] = qdm_mult(ref.rename(columns={"pr": "v"}), hist.rename(columns={"pr": "v"}), target.rename(columns={"pr": "v"}))
    return out


def classify(g):
    fam = g.family.iloc[0]
    if fam == "mm_rel":
        floor = FLOORS["mm_rel"] * np.nanmean(np.abs(g.ref_test))
    else:
        floor = FLOORS[fam]
    mae_raw = np.nanmean(np.abs(g.raw_test - g.ref_test))
    mae_q = np.nanmean(np.abs(g.qdm_test - g.ref_test))
    fut_shift = np.nanmean(np.abs(g.qdm_fut - g.raw_fut))
    sig_raw = np.nanmean(g.raw_fut - g.raw_hist)
    sig_q = np.nanmean(g.qdm_fut - g.qdm_hist)
    if mae_raw - mae_q >= floor and mae_q <= 0.5 * mae_raw and fut_shift >= floor:
        verdict = "MATERIAL"
    elif mae_q < mae_raw and (mae_raw - mae_q) >= 0.5 * floor:
        verdict = "MARGINAL"
    elif mae_q > mae_raw + 0.5 * floor:
        verdict = "WORSE"
    else:
        verdict = "NO EFFECT"
    return pd.Series(dict(family=fam, floor=round(floor, 2), ref_mean=np.nanmean(g.ref_test), mae_raw=mae_raw, mae_qdm=mae_q,
                          err_cut_pct=100 * (1 - mae_q / mae_raw) if mae_raw > 0 else np.nan,
                          raw_2050=np.nanmean(g.raw_fut), qdm_2050=np.nanmean(g.qdm_fut), mean_abs_2050_shift=fut_shift,
                          signal_raw=sig_raw, signal_qdm=sig_q, verdict=verdict))


def main():
    era = era5_sites()
    era["twb"], era["swbgt"] = stull(era.tas, era.hurs), swbgt(era.tas, era.hurs)
    nex = nex_sites()
    nex["twb"], nex["swbgt"] = stull(nex.tas, nex.hurs), swbgt(nex.tas, nex.hurs)

    tmet = make_metrics()
    corr = corrector(["tas", "tasmax", "tasmin"], ["twb", "swbgt"])
    rows = []
    for site in SITES:
        ref = era[era.site == site]
        for model in sorted(nex.model.unique()):
            m = nex[(nex.site == site) & (nex.model == model)]
            hist, fut = m[m.experiment == "historical"], m[m.experiment == "ssp245"]
            for r in score_unit(tmet, ref, hist, fut, corr):
                rows.append(dict(unit=site, model=model, **r))
            print("done", site, model, flush=True)
    t = pd.DataFrame(rows)
    t["inversions_note"] = ""
    t.to_csv(OUT / "temperature_humidity_unit_rows.csv", index=False)

    # Precipitation: Telangana grid against ERA5 (weak reference; flagged)
    pmet = make_pr_metrics()
    tp = era5_grid("pr", HIST)
    scale = 1000.0 if float(tp.max()) < 1.0 else 1.0     # ERA5 tp in metres
    prow = []
    for model in sorted(nex.model.unique()):
        h = xr.open_dataarray(SERIES / f"telpr_{model}_historical.nc")
        f = xr.open_dataarray(SERIES / f"telpr_{model}_ssp245.nc")
        lats, lons = np.meshgrid(h.lat.values, h.lon.values, indexing="ij")
        lats, lons = lats.ravel(), lons.ravel()
        # every 3rd cell keeps runtime bounded; cells are 0.25 deg apart so this is a regular subsample
        pick = np.arange(0, lats.size, 3)
        ref_cells = to_nex_cells(tp, lats[pick], lons[pick]) * scale  # (time, cell)
        rt = pd.to_datetime(tp.time.values)
        for j, k in enumerate(pick):
            def frame(vals, times):
                tt = pd.to_datetime(times)
                return _finish(pd.DataFrame({"year": tt.year, "month": tt.month, "day": tt.day, "pr": vals}))
            ref = frame(ref_cells[:, j], rt)
            yi, xi = divmod(k, h.sizes["lon"])
            hist = frame(h.isel(lat=yi, lon=xi).values * 86400.0, h.time.values)
            fut = frame(f.isel(lat=yi, lon=xi).values * 86400.0, f.time.values)
            if not (np.isfinite(ref.pr).mean() > 0.9 and np.isfinite(hist.pr).mean() > 0.9):
                continue
            for r in score_unit(pmet, ref, hist, fut, pr_corrector):
                prow.append(dict(unit=f"tel_{lats[k]:.3f}_{lons[k]:.3f}", model=model, **r))
        print("done pr", model, flush=True)
    p = pd.DataFrame(prow)
    p.to_csv(OUT / "precipitation_unit_rows.csv", index=False)

    summary = pd.concat([
        t.groupby("metric").apply(classify).assign(reference="ERA5 six sites"),
        p.groupby("metric").apply(classify).assign(reference="ERA5 Telangana grid"),
    ])
    summary.round(2).to_csv(OUT / "summary.csv")
    pd.set_option("display.width", 250, "display.max_rows", 200)
    print(summary.round(2).sort_values(["verdict", "reference"]))


if __name__ == "__main__":
    main()
