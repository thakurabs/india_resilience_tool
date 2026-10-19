"""QDM effect on Twb>=28 counts for ssp245 2040-2060 (the map default). Read-only diagnostic."""
import sys, glob; sys.path.insert(0, ".")
import numpy as np, pandas as pd
exec(open(".scratch/twb_check/qdm_twb.py").read().split("rows, fut = [], []")[0])  # reuse ref, nex(hist), qdm(), days()

f245 = pd.concat([pd.read_parquet(f) for f in glob.glob(".scratch/twb_check/ssp245_series/*.parquet")])
f245 = f245[~((f245.month == 2) & (f245.day == 29))]
f245["twb"] = stull(f245.tas.values - 273.15, f245.hurs.values)
f245["doy"] = pseudo_doy(f245.month.values, f245.day.values)

out = []
for site in sorted(ref.site.unique()):
    r = ref[ref.site == site]
    for model in sorted(f245.model.unique()):
        hist = nex[(nex.site == site) & (nex.model == model) & (nex.experiment == "historical")]
        f = f245[(f245.site == site) & (f245.model == model)]
        hc, fc = qdm(r, hist, hist), qdm(r, hist, f)
        out.append(dict(site=site, model=model, era5_hist=days(r.twb.values, r.year),
                        raw_hist=days(hist.twb.values, hist.year), qdm_hist=days(hc, hist.year),
                        raw_2050=days(f.twb.values, f.year), qdm_2050=days(fc, f.year),
                        dTwb_raw=np.nanmean(f.twb) - np.nanmean(hist.twb), dTwb_qdm=np.nanmean(fc) - np.nanmean(hc)))
o = pd.DataFrame(out)
pd.set_option("display.width", 200)
print("ssp245 2040-2060, days/yr daily-mean Twb>=28C, 5-model mean (QDM fitted on all 1990-2010)")
print(o.groupby("site").mean(numeric_only=True).round(1))
print("\nInter-model range 2040-2060")
print(o.groupby("site")[["raw_2050", "qdm_2050"]].agg(["min", "max"]).round(0))

# Sanity: published district values (same 5 models) vs raw site cell
pub = {"kolkata": ("West Bengal", "kolkata"), "lucknow": ("Uttar Pradesh", "lucknow"), "bikaner": ("Rajasthan", "bikaner"),
       "hyderabad": ("Telangana", "hyderabad"), "kochi": ("Kerala", "ernakulam")}
B = "D:/projects/irt_data/processed_optimised/metrics/twb_days_ge_28/yearly_models/admin/district/"
rows = []
for s, (st, key) in pub.items():
    y = pd.read_parquet(B + f"state={st}.parquet")
    y = y[y.district_key.str.lower().str.contains(key) & y.model.isin(f245.model.unique())]
    h = y[(y.scenario == "historical") & y.year.between(1990, 2010)].value.mean()
    f = y[(y.scenario == "ssp245") & y.year.between(2040, 2060)].value.mean()
    rows.append(dict(site=s, district_keys=",".join(sorted(y.district_key.unique()))[:40], published_hist=h, published_2050=f))
print("\nPublished district values, same 5 models (compare with raw_hist / raw_2050 above)")
print(pd.DataFrame(rows).round(1).to_string(index=False))
