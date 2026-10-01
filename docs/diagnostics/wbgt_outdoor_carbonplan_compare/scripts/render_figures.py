"""Render compact evidence from the completed Kochi product comparison."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle
from shapely.geometry import shape


OUT = Path("docs/diagnostics/wbgt_outdoor_carbonplan_compare")


def main() -> None:
    global OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=OUT,
                        help="Directory produced by the comparison scripts")
    OUT = parser.parse_args().out_dir
    paired = pd.read_csv(OUT / "paired_kochi_daily_2005.csv")
    paired["month"] = pd.to_datetime(paired.date).dt.month
    monthly = paired.groupby("month").agg(
        carbonplan_mean_c=("carbonplan_kochi_city_daily_c", "mean"),
        w1_area_mean_c=("w1_kochi_city_area_mean_daily_max_c", "mean"),
    ).reset_index()
    for threshold in (28, 30, 32):
        monthly[f"carbonplan_days_ge_{threshold}"] = paired.groupby("month")[
            "carbonplan_kochi_city_daily_c"].apply(lambda values: int((values >= threshold).sum())).values
        monthly[f"w1_days_ge_{threshold}"] = paired.groupby("month")[
            "w1_kochi_city_area_mean_daily_max_c"].apply(lambda values: int((values >= threshold).sum())).values
    monthly.to_csv(OUT / "paired_monthly_2005.csv", index=False)

    fig, ax = plt.subplots(figsize=(8, 3.5), constrained_layout=True)
    ax.plot(monthly.month, monthly.carbonplan_mean_c, marker="o", label="CarbonPlan published Kochi")
    ax.plot(monthly.month, monthly.w1_area_mean_c, marker="o", label="W1 area mean over city")
    ax.set(xlabel="Month of 2005", ylabel="Mean daily maximum outdoor WBGT (°C)",
           title="Kochi · ACCESS-CM2 historical 2005", xticks=range(1, 13))
    ax.grid(alpha=0.25)
    ax.legend(fontsize=8)
    fig.savefig(OUT / "monthly_mean_2005.png", dpi=170)
    plt.close(fig)

    feature = json.loads((OUT / "carbonplan_kochi_geometry.geojson").read_text())
    city = shape(feature["geometry"])
    cells = pd.read_csv(OUT / "w1_intersecting_cells_2005.csv")
    fig, ax = plt.subplots(figsize=(6, 5), constrained_layout=True)
    for polygon in city.geoms if hasattr(city, "geoms") else [city]:
        x, y = polygon.exterior.xy
        ax.fill(x, y, color="#f2ba69", alpha=0.5, label="CarbonPlan city polygon" if not ax.patches else None)
        ax.plot(x, y, color="#a34c1e", linewidth=1.3)
    for cell in cells.itertuples():
        ax.add_patch(Rectangle((cell.lon - 0.125, cell.lat - 0.125), 0.25, 0.25,
                               fill=False, edgecolor="#24689b", linewidth=1))
        ax.plot(cell.lon, cell.lat, "o", color="#24689b", markersize=3)
    ax.set(xlabel="Longitude (°E)", ylabel="Latitude (°N)",
           title="Kochi published city footprint and intersecting W1 cells", aspect="equal")
    fig.savefig(OUT / "kochi_city_cells.png", dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    main()
