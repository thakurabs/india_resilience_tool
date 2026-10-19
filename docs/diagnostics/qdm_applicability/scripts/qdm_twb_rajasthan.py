"""Read-only check (CHG-0711): why corrected twb_days_ge_28 in western Rajasthan (Jaisalmer, Barmer) is ~5x raw.
Uses test B's cached Twb series (series_twb_national_s1), the ERA5 daily reference (tas, hurs)
and raw NEX tas/hurs for the same cells. Same QDM settings as test B (additive, 31-day window)."""
import sys
sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts")
import warnings; warnings.simplefilter("ignore")
import numpy as np, pandas as pd, xarray as xr, geopandas as gpd
import qdm_applicability as Q

SER = "D:/projects/india_resilience_tool/.scratch/qdm_applicability/series_twb_national_s1"
MODELS = ["ACCESS-CM2", "EC-Earth3", "GFDL-ESM4", "MIROC6", "MPI-ESM1-2-LR"]
ROOT = "D:/projects/irt_data/r1i1p1f1"
Q.KW = dict(nquantiles=Q.NQUANTILES, window=31)
pd.set_option("display.width", 250, "display.max_rows", 200)

d = gpd.read_file("D:/projects/irt_data/districts_4326.geojson")
poly = d[d.district_name.isin(["Jaisalmer", "Barmer"]) & (d.state_name == "Rajasthan")].union_all()
cells = pd.read_csv(f"{SER}/cells.csv")
inside = cells[gpd.points_from_xy(cells.lon, cells.lat).within(poly)]
keys = [f"{a:.3f}_{b:.3f}" for a, b in zip(inside.lat, inside.lon)]
print("cells in Jaisalmer+Barmer:", len(keys))
cols = ["year", "month", "day"] + keys
ref = pd.read_parquet(f"{SER}/era5_ref.parquet", columns=cols)


def frame(D, k):
    return Q._finish(pd.DataFrame({"year": D.year.values, "month": D.month.values, "day": D.day.values, "twb": D[k].values}))


def stats(f, lab):
    t = f.twb.values; n = f.year.nunique()
    return dict(source=lab, mean=t.mean(), p99=np.percentile(t, 99), max=t.max(),
                d26_28=((t >= 26) & (t < 28)).sum() / n, d27_28=((t >= 27) & (t < 28)).sum() / n, d_ge28=(t >= 28).sum() / n)


rows, months = [], []
for m in MODELS:
    H = pd.read_parquet(f"{SER}/{m}_historical.parquet", columns=cols)
    F = pd.read_parquet(f"{SER}/{m}_ssp245.parquet", columns=cols)
    corr = Q.corrector([], ["twb"])
    for k in keys:
        r, h, f = frame(ref, k), frame(H, k), frame(F, k)
        qf = corr(r, h, f)
        for lab, x in [("ERA5 1990-2010", r), ("NEX raw 1990-2010", h), ("NEX raw 2040-2060", f), ("QDM 2040-2060", qf)]:
            rows.append(dict(model=m, cell=k, **stats(x, lab)))
            months.append(x[x.twb >= 28].groupby("month").size().div(x.year.nunique()).rename(lab).to_frame().assign(model=m))
        # quantile change the model itself projects, May-Sep, in the upper tail vs median
        hs, fs = h[h.month.isin(range(5, 10))].twb, f[f.month.isin(range(5, 10))].twb
        rows.append(dict(model=m, cell=k, source="NEX change May-Sep", mean=fs.mean() - hs.mean(),
                         p99=fs.quantile(.99) - hs.quantile(.99), max=np.nan,
                         d26_28=fs.quantile(.9) - hs.quantile(.9), d27_28=fs.quantile(.5) - hs.quantile(.5), d_ge28=np.nan))
s = pd.DataFrame(rows)
print("\nMean over cells and models (days/yr; C). For 'NEX change' rows: mean=mean change, p99=p99 change, d26_28=p90 change, d27_28=median change")
print(s.groupby("source")[["mean", "p99", "max", "d26_28", "d27_28", "d_ge28"]].mean().round(2).to_string())
print("\nBy model: days >= 28 per year")
print(s[s.source != "NEX change May-Sep"].pivot_table(index="model", columns="source", values="d_ge28").round(1).to_string())
mo = pd.concat(months).reset_index().groupby("month").sum(numeric_only=True) / (len(MODELS) * len(keys))
print("\nDays >= 28 per year by month (mean over cells; ERA5 counted once per model so same scale)")
print(mo.round(2).to_string())

# Components: ERA5 vs raw NEX tas and hurs in the season of the >=28 days (Jun-Sep), cell mean, 1990-2010
la = xr.DataArray(inside.lat.values, dims="c"); lo = xr.DataArray(inside.lon.values, dims="c")
e = xr.open_mfdataset([f"D:/projects/irt_data/era5_daily_ref_nexgrid/{y}.nc" for y in range(1990, 2011)],
                      engine="h5netcdf", combine="by_coords").sel(lat=la, lon=lo).load()
jj = e.time.dt.month.isin([6, 7, 8, 9])
comp = [dict(source="ERA5", tas=float(e.tas[jj].mean()), hurs=float(e.hurs[jj].mean()),
             hurs_p90=float(e.hurs[jj].quantile(.9)), tas_on_humid=float(e.tas[jj].where(e.hurs[jj] > 60).mean()))]
for m in MODELS:
    t, h = [], []
    for y in range(1990, 2011):
        for var, L in (("tas", t), ("hurs", h)):
            with xr.open_dataset(f"{ROOT}/historical/{var}/{m}/{y}.nc", use_cftime=True) as ds:
                v = ds[var].sel(lat=la, lon=lo, method="nearest")
                L.append(v.sel(time=[tt for tt in v.time.values if tt.month in (6, 7, 8, 9)]).values)
    t = np.concatenate(t) - 273.15; h = np.concatenate(h)
    comp.append(dict(source=f"NEX {m}", tas=t.mean(), hurs=h.mean(), hurs_p90=np.quantile(h, .9),
                     tas_on_humid=t[h > 60].mean() if (h > 60).any() else np.nan))
print("\nJun-Sep daily means 1990-2010, cells in Jaisalmer+Barmer (tas C, hurs %; tas_on_humid = mean tas on days hurs>60)")
print(pd.DataFrame(comp).round(2).to_string(index=False))
