"""Wet-bulb QDM test B averaged up to districts (CHG-0713).

Read-only diagnostic. Inputs: test B's per-cell, per-model results
(.scratch/qdm_applicability/out_twb_national_s1/twb_national_unit_rows.csv, from
qdm_twb_national.py --step 1) and districts_4326.geojson.

Each NEX 0.25-degree cell is a box; its weight in a district is the area of the box inside the
district (equal-area CRS EPSG:6933). District values are area-weighted means over the scored cells,
taken per model before any error is computed, so errors are district-level errors. Coverage is the
share of the district's area covered by scored cells; districts with no scored cell are listed
and left out. Districts are keyed by their row in the boundary file (district_id), not the LGD code:
17 carved-out districts share their parent's LGD code (e.g. Balotra and Barmer). Verdicts use the same rule as the cell tests (qdm_pr_imd.classify).

Map impact uses the 5-model mean in 2040-2060: rank correlation of raw vs corrected district
values, district rank shifts, and national quintile changes. Quintiles are a stand-in for colour
bands, not the dashboard's frozen ruler.

Writes .scratch/qdm_applicability/out_twb_districts_s1/.
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box

sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts")
sys.path.insert(0, ".")
from qdm_pr_imd import classify  # noqa: E402
from qdm_twb_national import METRICS, region  # noqa: E402

CELLS = Path(".scratch/qdm_applicability/out_twb_national_s1/twb_national_unit_rows.csv")
DISTRICTS = "D:/projects/irt_data/districts_4326.geojson"
OUT = Path(".scratch/qdm_applicability/out_twb_districts_s1")
VALS = ["ref_test", "raw_test", "qdm_test", "raw_hist", "raw_fut", "qdm_hist", "qdm_fut"]
EA = "EPSG:6933"


def weights(units: pd.DataFrame, dist: gpd.GeoDataFrame) -> pd.DataFrame:
    """Area of each scored cell box inside each district (km2), plus district coverage."""
    g = gpd.GeoDataFrame(units, geometry=[box(lo - 0.125, la - 0.125, lo + 0.125, la + 0.125)
                                          for la, lo in zip(units.lat, units.lon)], crs="EPSG:4326")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        x = gpd.overlay(g.to_crs(EA), dist.to_crs(EA), how="intersection", keep_geom_type=True)
    x["w_km2"] = x.area / 1e6
    return x[["unit", "district_id", "w_km2"]]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_csv(CELLS)
    dist = gpd.read_file(DISTRICTS)[["state_name", "district_name", "district_lgd_code", "geometry"]]
    dist.insert(0, "district_id", np.arange(len(dist)))
    dist["dist_km2"] = dist.to_crs(EA).area / 1e6
    c = dist.to_crs(EA).centroid.to_crs("EPSG:4326")
    dist["clat"], dist["clon"] = c.y, c.x

    w = weights(d[["unit", "lat", "lon"]].drop_duplicates(), dist)
    cov = w.groupby("district_id").w_km2.sum().rename("covered_km2")
    info = dist.drop(columns="geometry").set_index("district_id").join(cov)
    info["coverage"] = (info.covered_km2 / info.dist_km2).fillna(0)
    missing = info[info.coverage == 0]
    print(f"districts {len(info)}; with scored cells {int((info.coverage > 0).sum())}; "
          f"none {len(missing)}; coverage <50% {int(((info.coverage > 0) & (info.coverage < 0.5)).sum())}", flush=True)
    print("no scored cell:", ", ".join(f"{r.district_name} ({r.state_name})" for r in missing.itertuples()), flush=True)

    m = d.merge(w, on="unit")
    for v in VALS:
        m[v] = m[v] * m.w_km2
    a = m.groupby(["district_id", "model", "metric", "family"])[VALS + ["w_km2"]].sum().reset_index()
    for v in VALS:
        a[v] = a[v] / a.w_km2
    a = a.drop(columns="w_km2").join(info[["state_name", "district_name", "district_lgd_code", "clat", "clon", "coverage"]],
                                     on="district_id")
    a["unit"] = a.district_id
    a["region"] = [region(la, lo) for la, lo in zip(a.clat, a.clon)]
    a.to_csv(OUT / "twb_district_rows.csv", index=False)
    info.to_csv(OUT / "district_coverage.csv")

    pd.set_option("display.width", 250, "display.max_rows", 400)
    cols = ["ref", "err_raw", "err_qdm", "raw_2050", "qdm_2050", "chg_raw", "chg_qdm", "n_units", "verdict"]
    for lab, sub, fn in [("ALL DISTRICTS", a, "all"), ("DISTRICTS EXCLUDING HIMALAYA (centroid < 30N)", a[a.clat < 30], "no_himalaya")]:
        s = sub.groupby("metric").apply(classify)
        s.round(3).to_csv(OUT / f"summary_{fn}.csv")
        print(f"\n{lab}\n" + s[cols].round(2).reindex(METRICS).to_string(), flush=True)
    s = a[a.clat < 30].groupby(["metric", "state_name"]).apply(classify)
    s.round(3).to_csv(OUT / "summary_by_state_no_himalaya.csv")

    # Map impact: 5-model mean per district in 2040-2060, raw vs corrected
    e = a.groupby(["metric", "district_id", "state_name", "district_name", "clat", "coverage"])[
        ["ref_test", "raw_test", "qdm_test", "raw_fut", "qdm_fut"]].mean().reset_index()
    rows = []
    for met, g in e.groupby("metric"):
        g = g.copy()
        g["rank_raw"] = g.raw_fut.rank(ascending=False, method="min")
        g["rank_qdm"] = g.qdm_fut.rank(ascending=False, method="min")
        g["q_raw"] = pd.qcut(g.raw_fut.rank(method="first"), 5, labels=False) + 1
        g["q_qdm"] = pd.qcut(g.qdm_fut.rank(method="first"), 5, labels=False) + 1
        e.loc[g.index, ["rank_raw", "rank_qdm", "q_raw", "q_qdm"]] = g[["rank_raw", "rank_qdm", "q_raw", "q_qdm"]]
        sh = (g.rank_qdm - g.rank_raw).abs()
        rows.append(dict(metric=met, districts=len(g), spearman=g.raw_fut.corr(g.qdm_fut, method="spearman"),
                         median_rank_shift=sh.median(), p90_rank_shift=sh.quantile(0.9), max_rank_shift=sh.max(),
                         quintile_changed=int((g.q_raw != g.q_qdm).sum()),
                         quintile_moved_2plus=int(((g.q_raw - g.q_qdm).abs() >= 2).sum()),
                         top50_overlap=len(set(g.nsmallest(50, "rank_raw").district_id)
                                           & set(g.nsmallest(50, "rank_qdm").district_id))))
    imp = pd.DataFrame(rows).set_index("metric").reindex(METRICS)
    imp.round(3).to_csv(OUT / "map_impact.csv")
    e.to_csv(OUT / "district_ensemble_2050.csv", index=False)
    print("\nMAP IMPACT (5-model mean, 2040-2060, raw vs corrected)\n" + imp.round(2).to_string(), flush=True)

    t = e[e.metric == "twb_days_ge_28"]
    show = ["district_name", "state_name", "ref_test", "raw_test", "qdm_test", "raw_fut", "qdm_fut", "rank_raw", "rank_qdm"]
    print("\ntwb_days_ge_28: top 15 districts, raw 2040-2060\n" + t.nsmallest(15, "rank_raw")[show].round(1).to_string(index=False))
    print("\ntwb_days_ge_28: top 15 districts, corrected 2040-2060\n" + t.nsmallest(15, "rank_qdm")[show].round(1).to_string(index=False))
    t = t.assign(shift=t.rank_raw - t.rank_qdm)
    print("\ntwb_days_ge_28: biggest rises in rank after correction\n" + t.nlargest(10, "shift")[show + ["shift"]].round(1).to_string(index=False))


if __name__ == "__main__":
    main()
