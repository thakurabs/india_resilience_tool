import sys; sys.path.insert(0,"."); sys.path.insert(0,".scratch/qdm_applicability")
import pandas as pd, numpy as np
from qdm_applicability import FLOORS
def classify(g):
    fam=g.family.iloc[0]
    floor=FLOORS["mm_rel"]*np.nanmean(np.abs(g.ref_test)) if fam=="mm_rel" else FLOORS[fam]
    er=np.nanmean(np.abs(g.raw_test-g.ref_test)); eq=np.nanmean(np.abs(g.qdm_test-g.ref_test))
    sh=np.nanmean(np.abs(g.qdm_fut-g.raw_fut))
    if er-eq>=floor and eq<=0.5*er and sh>=floor: v="MATERIAL"
    elif eq>er+0.5*floor: v="WORSE"
    elif er<floor and sh>=floor: v="FUTURE-ONLY (unverifiable)"
    elif eq<er and (er-eq)>=0.5*floor: v="MARGINAL"
    else: v="NO EFFECT"
    return pd.Series(dict(family=fam,floor=floor,ref=np.nanmean(g.ref_test),err_raw=er,err_qdm=eq,raw_2050=np.nanmean(g.raw_fut),qdm_2050=np.nanmean(g.qdm_fut),shift_2050=sh,chg_raw=np.nanmean(g.raw_fut-g.raw_hist),chg_qdm=np.nanmean(g.qdm_fut-g.qdm_hist),verdict=v))
t=pd.read_csv(".scratch/qdm_applicability/out/temperature_humidity_unit_rows.csv")
p=pd.read_csv(".scratch/qdm_applicability/out/precipitation_unit_rows.csv")
a=t.groupby("metric").apply(classify).assign(scope="6 sites")
b=t[t.unit!="shimla"].groupby("metric").apply(classify).assign(scope="5 sites (no Shimla)")
c=p.groupby("metric").apply(classify).assign(scope="Telangana grid (ERA5 pr - UNFIT)")
out=pd.concat([b,c]); out["verdict_6sites"]=a.verdict
out.round(2).to_csv(".scratch/qdm_applicability/out/summary_final.csv")
pd.set_option("display.width",250,"display.max_rows",200)
print(out.drop(columns=["family"]).round(2).sort_values(["scope","verdict"]).to_string())
