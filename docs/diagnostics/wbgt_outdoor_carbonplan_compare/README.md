# Published CarbonPlan outdoor WBGT versus IRT W1: Kochi, 2005

**Diagnostic comparison only.** This uses CarbonPlan's *released* daily sun-WBGT dataset, not the earlier IRT reproduction of a CarbonPlan-like raw calculation. It compares one city, one GCM, and one historical year. CarbonPlan is a published product, not observed WBGT or a ground-truth accuracy target.

## Data availability and match

CarbonPlan explicitly publishes [historical daily outdoor WBGT](https://github.com/carbonplan/extreme-heat/blob/main/data/zarr_daily_locations.md) as a [Zarr store](https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0/outputs/zarr/daily/historical-WBGT-sun.zarr/.zmetadata). Its [released geography](https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0/inputs/all_regions_and_cities.json) has a Kochi city polygon: `ID_HDC_G0=7887`, `processing_id=7886`. The Zarr coordinate for that ID is at position **7883**; it is unsafe to use the ID as an array position. The selected GCM is `ACCESS-CM2`, index 0. The published variable is `wbgt-sun`, units `degC`, with QDM bias adjustment recorded in its metadata. The daily record covers 1985–2014; only **2005** is paired here with the existing IRT pilot.

CarbonPlan's published dataset is [licensed CC BY 4.0](https://github.com/carbonplan/extreme-heat#data). This report credits CarbonPlan for the released WBGT and city geometry.

IRT's W1 source is the existing `ACCESS-CM2` historical 2005 Kerala pilot, with the declared RH physical-bound treatment and GMTED cell elevations. Five valid 0.25° NEX cells intersect the CarbonPlan Kochi polygon. Their intersections account for **100.0%** of its area (721.4 km²), measured in EPSG:6933. The city polygon and intersecting cells are shown in [the footprint image](kochi_city_cells.png); exact cell overlaps are in [`w1_intersecting_cells_2005.csv`](w1_intersecting_cells_2005.csv).

CarbonPlan applies **population-informed spatial weights** to gridded values for its regions, including cities, in [notebook 05](https://github.com/carbonplan/extreme-heat/blob/main/notebooks/05_aggregate.ipynb) via [`calc_sparse_weights`](https://github.com/carbonplan/extreme-heat/blob/main/notebooks/utils.py). Here W1 is **area-weighted over the same published city polygon** because the existing pilot does not contain CarbonPlan's population grid. The physical footprint is matched; the effective spatial weights and underlying grids are not. CarbonPlan also [bias-corrects shade WBGT and adds a radiation/wind adjustment](https://github.com/carbonplan/extreme-heat/blob/main/notebooks/08_shade_sun_adjustment.ipynb), while W1 reconstructs hourly drivers and solves Liljegren WBGT. The published data do not establish identical NEX input versions or member identities; only the GCM name and year were matched.

## Like-defined city daily-series comparison

To make threshold definitions comparable, W1 was reconstructed daily for the five intersecting cells; on each day their maxima were area-averaged over the city; then the city series was counted at each threshold. The recomputed cell annual values match the frozen pilot cache within `1e-10` °C/day. Every city cell and every 2005 day is valid. The daily `hurs` clipping flag is zero for these five cells. CarbonPlan's released daily **city** series is counted at the same `>=` thresholds. Each count below is therefore an integer number of days when the respective *regional daily series* crosses the threshold.

| 2005 Kochi | CarbonPlan published city | IRT W1 city area mean | W1 − CarbonPlan |
|---|---:|---:|---:|
| Annual mean of daily maximum WBGT | 31.07 °C | 31.30 °C | +0.23 °C |
| Days ≥28 °C | 361 | 363 | +2 |
| Days ≥30 °C | 266 | 318 | **+52** |
| Days ≥32 °C | 110 | 116 | +6 |

Source rows: [`paired_city_series_summary_2005.csv`](paired_city_series_summary_2005.csv); all 365 paired days: [`paired_kochi_daily_2005.csv`](paired_kochi_daily_2005.csv). The daily product difference has a mean absolute value of **0.80 °C**, RMSE **0.95 °C**, and correlation **0.793**. These describe disagreement between two model products; they are **not weather forecast errors against observations**.

The close annual means hide a seasonal shift: CarbonPlan is warmer in February–March, while W1 is warmer during June–October. March means are **32.82 versus 31.58 °C**, and August means **29.21 versus 30.33 °C** (CarbonPlan then W1). [Monthly plot](monthly_mean_2005.png) and [`paired_monthly_2005.csv`](paired_monthly_2005.csv) show the full pattern. The large difference at ≥30 °C arises from the daily distributions near that threshold; the much smaller ≥32 °C difference should not be generalized beyond this single city-year.

## Alternative count meaning in IRT

The engineering pilot's district/block count metric is an **area-weighted mean of cell annual exceedance days**, not a count of days when the area-mean daily series exceeds the threshold. Applying that metric to the Kochi city polygon gives **363.34 / 315.91 / 117.01** days at ≥28/30/32 °C, respectively, in [`comparison_2005.csv`](comparison_2005.csv). These fractional values are valid area-weighted exposure summaries, but they are not the same statistic as CarbonPlan's integer city-series count; the table above uses like-defined regional series counts.

## Interpretation and limits

This confirms that published CarbonPlan outdoor WBGT **is available for Kochi** and supports a numeric comparison at a mapped city footprint. In this one model-year, W1's annual level is close to CarbonPlan's, while the ≥30 °C count and seasonal timing differ materially. The mismatch can reflect spatial weights, input/downscaling lineage, QDM correction, and the outdoor method; this experiment does not separate their contributions. It cannot establish national accuracy, district/block ranking quality, or long-run count bias. The 1985–2014 CarbonPlan annual series is provided in [`carbonplan_kochi_annual.csv`](carbonplan_kochi_annual.csv) for context; it is not used as a mismatched-period comparator to W1 2005.

The exact remote source URLs, Zarr chunk checksum, geography feature checksum, and frozen W1 cache checksum are in [`manifest.json`](manifest.json). The extracted released city daily data are in [`carbonplan_kochi_daily.csv`](carbonplan_kochi_daily.csv). The comparison read only the single necessary Zarr chunk for ACCESS-CM2 and Kochi's location chunk (61.6 MB compressed); it did not download the global archive or run a new state build. CarbonPlan's historical series includes leap days, while the paired 2005 target is an ordinary 365-day year. No production metric or shade artifact was changed.

To reproduce in a **new** output directory, first verify that the frozen pilot cache exists, then run these from the repository root in the project's Python environment. The first command downloads a 16 MB range of CarbonPlan geography and one 61.6 MB daily Zarr chunk; the later commands read local pilot inputs and write only the chosen output directory.

```bash
python -c "from pathlib import Path; p=Path('scratch/wbgt_outdoor_pilot_qc/cells_Kerala_ACCESS-CM2_2005_combined.nc'); assert p.is_file(), f'Missing frozen pilot cache: {p}'"
python docs/diagnostics/wbgt_outdoor_carbonplan_compare/scripts/extract_and_compare.py --out-dir scratch/wbgt_carbonplan_reproduce
python docs/diagnostics/wbgt_outdoor_carbonplan_compare/scripts/compute_city_series.py --out-dir scratch/wbgt_carbonplan_reproduce
python docs/diagnostics/wbgt_outdoor_carbonplan_compare/scripts/render_figures.py --out-dir scratch/wbgt_carbonplan_reproduce
```

The extraction script refuses an existing output directory, protecting the evidence above. The three scripts are retained in [`scripts/`](scripts/) with the report.
