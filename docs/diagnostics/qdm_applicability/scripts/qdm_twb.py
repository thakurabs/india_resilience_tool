"""Small check: does QDM against ERA5 fix the Twb>=28 day-count inflation? (read-only diagnostic)"""
import glob, numpy as np, pandas as pd
import sys; sys.path.insert(0, ".")
from tools.diagnostics.wbgt_qdm_bias_correction import pseudo_doy, quantile_table, qdm_adjust, NQUANTILES, GROUP_WINDOW_DAYS

def stull(t, rh):
    rh = np.clip(rh, 0, 100)
    return (t*np.arctan(0.151977*np.sqrt(rh+8.313659)) + np.arctan(t+rh) - np.arctan(rh-1.676331)
            + 0.00391838*rh**1.5*np.arctan(0.023101*rh) - 4.686035)

# ERA5 reference: Stull on daily-mean T and RH (IST days) -- same method as deployed
ref = []
for s in ["bikaner", "hyderabad", "kochi", "kolkata", "lucknow", "shimla"]:
    h = pd.concat([pd.read_parquet(f"scratch/wbgt_deployed_vs_reference_cache/{s}_{y}.parquet") for y in range(1990, 2011)])
    h.index = pd.to_datetime(h.index) + pd.Timedelta(hours=5.5)
    d = h.resample("D").mean()[["temperature_2m", "relative_humidity_2m"]]
    d = d[(d.index.year >= 1990) & (d.index.year <= 2010) & ~((d.index.month == 2) & (d.index.day == 29))]
    ref.append(pd.DataFrame({"site": s, "year": d.index.year, "month": d.index.month, "day": d.index.day,
                             "twb": stull(d.temperature_2m.values, d.relative_humidity_2m.values)}))
ref = pd.concat(ref)
ref["doy"] = pseudo_doy(ref.month.values, ref.day.values)

nex = pd.concat([pd.read_parquet(f) for f in glob.glob("scratch/wbgt_qdm_site_series/*.parquet")])
nex["site"] = nex.site.str.lower()
nex = nex[~((nex.month == 2) & (nex.day == 29))]
nex["twb"] = stull(nex.tas.values - 273.15, nex.hurs.values)
nex["doy"] = pseudo_doy(nex.month.values, nex.day.values)

def qdm(train_ref, train_hist, target):
    kw = dict(nquantiles=NQUANTILES, window=GROUP_WINDOW_DAYS)
    qr = quantile_table(train_ref.twb.values, train_ref.doy.values, **kw)
    qh = quantile_table(train_hist.twb.values, train_hist.doy.values, **kw)
    return qdm_adjust(target.twb.values, target.doy.values, q_ref=qr, q_hist=qh, **kw)

def days(v, years):  # days/yr >= 28
    return np.nansum(v >= 28.0) / len(set(years))

rows, fut = [], []
for site in sorted(ref.site.unique()):
    r = ref[ref.site == site]
    yrs_by_heat = r.groupby("year").twb.mean().sort_values()
    splits = {"holdout 2003-10": (list(range(1990, 2003)), list(range(2003, 2011))),
              "warm transfer": (list(yrs_by_heat.index[:12]), list(yrs_by_heat.index[12:]))}
    for model in sorted(nex.model.unique()):
        m = nex[(nex.site == site) & (nex.model == model)]
        hist = m[m.experiment == "historical"]
        for name, (tr, te) in splits.items():
            test = hist[hist.year.isin(te)]
            cor = qdm(r[r.year.isin(tr)], hist[hist.year.isin(tr)], test)
            rows.append(dict(site=site, model=model, split=name, era5=days(r[r.year.isin(te)].twb.values, te),
                             raw=days(test.twb.values, te), qdm=days(cor, te)))
        # future: train on all 1990-2010, apply to ssp585 2071-2080
        f = m[m.experiment == "ssp585"]
        hc = qdm(r, hist, hist); fc = qdm(r, hist, f)
        fut.append(dict(site=site, model=model,
                        era5_hist=days(r.twb.values, r.year), raw_hist=days(hist.twb.values, hist.year),
                        raw_fut=days(f.twb.values, f.year), qdm_fut=days(fc, f.year),
                        raw_dmean=np.nanmean(f.twb) - np.nanmean(hist.twb), qdm_dmean=np.nanmean(fc) - np.nanmean(hc)))

pd.set_option("display.width", 200)
t = pd.DataFrame(rows).groupby(["split", "site"])[["era5", "raw", "qdm"]].mean().round(1)
print("Days/yr with daily-mean Twb >= 28C, scored on years NOT used to fit (5-model mean)\n", t, "\n")
F = pd.DataFrame(fut).groupby("site").mean(numeric_only=True).round(2)
print("Future ssp585 2071-2080 (5-model mean): counts and mean-Twb warming signal\n", F)
spread = pd.DataFrame(fut).groupby("site")[["raw_fut", "qdm_fut"]].agg(["min", "max"]).round(0)
print("\nInter-model range of future counts\n", spread)
