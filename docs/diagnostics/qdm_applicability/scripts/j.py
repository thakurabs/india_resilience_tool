import pandas as pd, numpy as np
def stull(t,rh):
    rh=np.clip(rh,0,100)
    return t*np.arctan(0.151977*np.sqrt(rh+8.313659))+np.arctan(t+rh)-np.arctan(rh-1.676331)+0.00391838*rh**1.5*np.arctan(0.023101*rh)-4.686035
# NEX-minus-ERA5 JJAS offsets measured this session (hurs pts, tas C)
cases={"kolkata":(108.1,[0,3,5,7],0.6),"lucknow":(59.9,[0,2,4,5],0.2)}
for s,(pub,drh,dt) in cases.items():
    h=pd.concat([pd.read_parquet(f"scratch/wbgt_deployed_vs_reference_cache/{s}_{y}.parquet") for y in range(1990,2011)])
    h.index=pd.to_datetime(h.index)+pd.Timedelta(hours=5.5)
    d=h.resample("D").mean()[["temperature_2m","relative_humidity_2m"]]
    d=d[~((d.index.month==2)&(d.index.day==29))]
    n=d.index.year.nunique(); jj=d.index.month.isin([6,7,8,9])
    print(f"{s}: published={pub}  ERA5 JJAS mean Twb={stull(d.temperature_2m[jj],d.relative_humidity_2m[jj]).mean():.2f}C")
    for r in drh:
        for t in (0,dt):
            c=(stull(d.temperature_2m+t,d.relative_humidity_2m+r)>=28).sum()/n
            print(f"   +{r:>2} RH pts, +{t} C  ->  {c:6.1f} days/yr")
