"""Reconstruct the five Kochi pilot cells daily for comparable city daily counts."""

import argparse
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from india_resilience_tool.compute.gridfirst_spatial import normalize_lat_lon
from tools.diagnostics import wbgt_outdoor_pilot as pilot


OUT = Path("docs/diagnostics/wbgt_outdoor_carbonplan_compare")
CELLS = OUT / "w1_intersecting_cells_2005.csv"
PILOT = Path("scratch/wbgt_outdoor_pilot_qc/cells_Kerala_ACCESS-CM2_2005_combined.nc")


def main() -> None:
    global OUT, CELLS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT,
                        help="Directory produced by extract_and_compare.py")
    args = parser.parse_args()
    OUT = args.out_dir
    CELLS = OUT / "w1_intersecting_cells_2005.csv"
    cells = pd.read_csv(CELLS)
    target_path = pilot.variable_path(
        "tas", source_root=pilot.DEFAULT_SOURCE_ROOT,
        wbgt_root=pilot.DEFAULT_WBGT_ROOT, model="ACCESS-CM2",
        scenario="historical", year=2005)
    with xr.open_dataset(target_path) as raw:
        reference = normalize_lat_lon(raw)
        lat = np.asarray(reference.lat.values)
        lon = np.asarray(reference.lon.values)
        li = [int(np.where(np.isclose(lat, v, atol=1e-10))[0][0]) for v in cells.lat]
        lj = [int(np.where(np.isclose(lon, v, atol=1e-10))[0][0]) for v in cells.lon]
    lat0, lat1 = min(li), max(li) + 1
    lon0, lon1 = min(lj), max(lj) + 1
    cube, days = pilot.load_daily_cube(
        source_root=pilot.DEFAULT_SOURCE_ROOT, wbgt_root=pilot.DEFAULT_WBGT_ROOT,
        model="ACCESS-CM2", scenario="historical", year=2005,
        index_range=(lat0, lat1, lon0, lon1))
    cube, flags, _ = pilot.apply_rh_policy(cube, rh_policy="clip100")
    valid_mask, _ = pilot.input_validity_mask(cube)
    target = pilot.drop_feb29(days[days.year == 2005])
    series = []
    with xr.open_dataset(PILOT) as ds:
        for cell, i, j in zip(cells.itertuples(), li, lj):
            if not cell.complete_year:
                raise ValueError("Incomplete city cell in existing pilot")
            frame = pd.DataFrame(
                {v: cube[v][:, i-lat0, j-lon0] for v in pilot.m1.REQUIRED_NEX_VARIABLES},
                index=days)
            frame.loc[~valid_mask[:, i-lat0, j-lon0], :] = np.nan
            elev = float(ds.cell_elevation_m.sel(lat=cell.lat, lon=cell.lon).item())
            static = pilot.m1.SiteStatic(
                f"kochi_{i}_{j}", float(cell.lat), float(cell.lon), elev)
            daily = pilot.cell_daily_max_c(frame, static, target_days=target)
            if not np.isfinite(daily).all():
                raise ValueError(f"Daily series incomplete for cell {cell.lat}/{cell.lon}")
            for key, computed in (("wbgt_annual_mean_c", float(daily.mean())),
                                  ("days_ge_28", int((daily >= 28).sum())),
                                  ("days_ge_30", int((daily >= 30).sum())),
                                  ("days_ge_32", int((daily >= 32).sum()))):
                if abs(computed - getattr(cell, key)) > 1e-10:
                    raise AssertionError(f"W1 daily parity failed for {key} at {cell.lat}/{cell.lon}")
            series.append(daily.to_numpy(dtype=float))
    array = np.column_stack(series)
    weights = cells.overlap_m2.to_numpy() / cells.overlap_m2.sum()
    regional = array @ weights
    frame = pd.DataFrame({"date": target.date.astype(str), "w1_kochi_city_area_mean_daily_max_c": regional})
    published = pd.read_csv(OUT / "carbonplan_kochi_daily.csv")
    match = frame.merge(published.rename(columns={"wbgt_c": "carbonplan_kochi_city_daily_c"}), on="date")
    match = match[match.date.str.startswith("2005")]
    if len(match) != 365:
        raise ValueError("Daily city join is incomplete")
    match.to_csv(OUT / "paired_kochi_daily_2005.csv", index=False)
    results = []
    for name, column in (("CarbonPlan", "carbonplan_kochi_city_daily_c"),
                         ("W1 city area mean", "w1_kochi_city_area_mean_daily_max_c")):
        values = match[column].to_numpy()
        results.append({"product": name, "annual_mean_daily_max_c": values.mean(),
                        **{f"days_ge_{t}": int((values >= t).sum()) for t in (28, 30, 32)}})
    comparison = pd.DataFrame(results)
    comparison.to_csv(OUT / "paired_city_series_summary_2005.csv", index=False)
    print(comparison.to_string(index=False))
    print("daily MAE", np.mean(np.abs(match.w1_kochi_city_area_mean_daily_max_c - match.carbonplan_kochi_city_daily_c)))
    print("RH clipped city cell days", sum(int(flags[:, i-lat0, j-lon0].sum()) for i,j in zip(li,lj)))


if __name__ == "__main__":
    main()
