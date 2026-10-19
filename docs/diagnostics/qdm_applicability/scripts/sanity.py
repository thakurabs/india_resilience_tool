import pandas as pd, numpy as np
t=pd.read_csv(".scratch/qdm_applicability/out/temperature_humidity_unit_rows.csv")
B="D:/projects/irt_data/processed_optimised/metrics/{m}/yearly_models/admin/district/state={s}.parquet"
pub={"kolkata":("West Bengal","kolkata"),"lucknow":("Uttar Pradesh","lucknow"),"bikaner":("Rajasthan","bikaner"),"hyderabad":("Telangana","hyderabad"),"kochi":("Kerala","ernakulam")}
mets=["txx_annual_max","txge35_extreme_heat_days","tasmin_tropical_nights_gt25","tnle10_cold_nights","tx90p_hot_days_pct","wsdi_warm_spell_days","twb_days_ge_28","swbgt_empirical_days_ge_28","tas_annual_mean"]
models=sorted(t.model.unique()); rows=[]
for m in mets:
    for s,(st,k) in pub.items():
        try: y=pd.read_parquet(B.format(m=m,s=st))
        except Exception as e: continue
        y=y[y.district_key.str.lower().str.contains(k)&y.model.isin(models)]
        ph=y[(y.scenario=="historical")&y.year.between(1990,2010)].value.mean()
        pf=y[(y.scenario=="ssp245")&y.year.between(2040,2060)].value.mean()
        mine=t[(t.metric==m)&(t.unit==s)]
        rows.append(dict(metric=m,site=s,pub_hist=ph,site_hist=mine.raw_hist.mean(),pub_2050=pf,site_2050=mine.raw_fut.mean()))
pd.set_option("display.width",200); print(pd.DataFrame(rows).round(1).to_string(index=False))
