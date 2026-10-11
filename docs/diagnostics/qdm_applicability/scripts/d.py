import pandas as pd, numpy as np
def stull(t,rh):
    rh=np.clip(rh,0,100)
    return t*np.arctan(0.151977*np.sqrt(rh+8.313659))+np.arctan(t+rh)-np.arctan(rh-1.676331)+0.00391838*rh**1.5*np.arctan(0.023101*rh)-4.686035
irt={"kolkata":108.1,"lucknow":59.9,"hyderabad":1.2,"bikaner":8.1,"kochi":8.3,"shimla":None}
rows=[]
for s in irt:
    h=pd.concat([pd.read_parquet(f"scratch/wbgt_deployed_vs_reference_cache/{s}_{y}.parquet") for y in range(1990,2011)])
    h.index=pd.to_datetime(h.index)+pd.Timedelta(hours=5.5)  # assume UTC -> IST
    h["twb"]=stull(h.temperature_2m,h.relative_humidity_2m)
    d=h.resample("D").agg({"temperature_2m":"mean","relative_humidity_2m":"mean","twb":["mean","max"]})
    d.columns=["t","rh","twb_mean_h","twb_max_h"]
    d=d[~((d.index.month==2)&(d.index.day==29))]
    d["twb_irt"]=stull(d.t,d.rh)
    n=d.index.year.nunique()
    rows.append(dict(site=s,IRT_NEX_published=irt[s],
      ERA5_irt_method=(d.twb_irt>=28).sum()/n,
      ERA5_dailymean_of_hourly=(d.twb_mean_h>=28).sum()/n,
      ERA5_daily_max_hourly=(d.twb_max_h>=28).sum()/n,
      ERA5_annmax_irt=d.twb_irt.groupby(d.index.year).max().mean(),
      JJAS_t=d.t[d.index.month.isin([6,7,8,9])].mean(), JJAS_rh=d.rh[d.index.month.isin([6,7,8,9])].mean()))
print(pd.DataFrame(rows).round(1).to_string(index=False))
