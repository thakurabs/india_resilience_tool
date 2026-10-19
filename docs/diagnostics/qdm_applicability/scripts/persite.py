import pandas as pd, numpy as np
t=pd.read_csv(".scratch/qdm_applicability/out/temperature_humidity_unit_rows.csv")
t["e_raw"]=(t.raw_test-t.ref_test).abs(); t["e_qdm"]=(t.qdm_test-t.ref_test).abs()
mets=["tas_annual_mean","txx_annual_max","tasmin_tropical_nights_gt25","tnle10_cold_nights","twb_days_ge_28","twb_annual_mean","swbgt_empirical_days_ge_28","txge35_extreme_heat_days","txge30_hot_days"]
pd.set_option("display.width",250)
print("Per-site mean |error| on holdout years (raw -> qdm), and signed raw bias")
x=t[t.metric.isin(mets)].groupby(["metric","unit"]).agg(raw=("e_raw","mean"),qdm=("e_qdm","mean"),bias=("raw_test",lambda s: np.nan)).drop(columns="bias")
x["raw_bias"]=t[t.metric.isin(mets)].assign(b=lambda d:d.raw_test-d.ref_test).groupby(["metric","unit"]).b.mean()
print(x.round(2).unstack("unit").to_string())
print("\nMAE without Shimla:")
y=t[(t.unit!="shimla")&t.metric.isin(mets)].groupby("metric")[["e_raw","e_qdm"]].mean().round(2); print(y)
p=pd.read_csv(".scratch/qdm_applicability/out/precipitation_unit_rows.csv")
print("\nTelangana precip, holdout means: ERA5 vs raw NEX")
print(p.groupby("metric")[["ref_test","raw_test","qdm_test"]].mean().round(1))
