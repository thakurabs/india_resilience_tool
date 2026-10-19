import xarray as xr, numpy as np, pandas as pd, os, sys
B="D:/projects/irt_data/r1i1p1f1/historical/"
models=sorted(os.listdir(B+"hurs"))
sites={"kolkata":(22.57,88.36),"lucknow":(26.85,80.95),"bikaner":(28.02,73.31),"hyderabad":(17.39,78.49)}
YRS=range(2000,2005)
rows=[]
for m in models:
    try:
        h=xr.open_mfdataset([f"{B}hurs/{m}/{y}.nc" for y in YRS],combine="by_coords")
    except Exception as e:
        print(m,"ERR",e,flush=True); continue
    pts=h["hurs"].sel(lat=xr.DataArray([v[0] for v in sites.values()],dims="s"),lon=xr.DataArray([v[1] for v in sites.values()],dims="s"),method="nearest").load()
    mon=pts["time"].dt.month.values
    jjas=np.isin(mon,[6,7,8,9])
    r=dict(model=m, ver=h.attrs.get("version", h.attrs.get("product_version","?")))
    for i,s in enumerate(sites): r[s]=round(float(pts.isel(s=i).values[jjas].mean()),1)
    rows.append(r); print(r,flush=True)
print(pd.DataFrame(rows).to_string(index=False))
