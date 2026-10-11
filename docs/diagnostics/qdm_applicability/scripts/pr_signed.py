import pandas as pd
d=pd.read_csv(".scratch/qdm_applicability/out_imd/precipitation_imd_unit_rows.csv")
d["bias_raw"]=d.raw_test-d.ref_test; d["bias_qdm"]=d.qdm_test-d.ref_test
pd.set_option("display.width",200)
print(d.groupby("metric")[["ref_test","raw_test","qdm_test","bias_raw","bias_qdm"]].mean().round(1))
x=d[d.metric=="pr_max_1day_precip"]; print("\nrx1day per-cell qdm/ref ratio quantiles:", (x.qdm_test/x.ref_test).quantile([.1,.5,.9]).round(2).to_dict(), " raw/ref:", (x.raw_test/x.ref_test).quantile([.1,.5,.9]).round(2).to_dict())
