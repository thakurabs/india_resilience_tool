# Published CarbonPlan versus W1: five Indian cities, three years

Completed diagnostic product comparison. Luna began the extension but reached its usage limit; the parent completed, reviewed, executed and verified the scripts. No production or shade files were changed.

## Fixed scope and result

ACCESS-CM2 historical, **2005, 2007 and 2009**; Kochi, Bikaner, Shimla, Hyderabad (India), Kolkata. These years were chosen for available local inputs before scoring. This is a 15-city-year comparison, not a continuous climatological baseline or nationally representative validation.

The table shows **W1 minus CarbonPlan**, averaged over the three complete years. Counts are days per year, not percentages. Mean temperature is the annual mean of each product's city daily maximum series.

| City | Mean difference (°C) | ≥28°C days/year difference | ≥30°C days/year difference | ≥32°C days/year difference |
|---|---:|---:|---:|---:|
| Kochi | +0.46 | +23.33 | +59.33 | +4.67 |
| Bikaner | -1.01 | -18.67 | -21.67 | -21.33 |
| Shimla | +2.42 | -2.67 | -0.67 | +0.00 |
| Hyderabad | +0.18 | +47.33 | +41.00 | -15.67 |
| Kolkata | +0.81 | +3.33 | +18.67 | +42.67 |

The Kochi ≥30°C difference persists: **+52, +65, +61 days** in 2005/2007/2009. Its previously close ≥28°C result does not generalize: the differences are +2, +45, +23 days. Hyderabad shows +61/+13/+49 days at ≥30°C despite a mean-level difference of only +0.18°C across the three years. Bikaner has fewer W1 exceedance days at all three thresholds in every year. Kolkata has +28/+46/+54 days at ≥32°C. Shimla has a +2.42°C annual mean difference despite few threshold events; zero ≥32°C counts in both products offer little evidence of agreement.

These results establish persistent, location-dependent product differences. They do not establish which product is more accurate, nor identify a single physical cause. A uniform national offset would not reconcile differences with opposing signs. Small annual-average differences or net count differences must not be interpreted as agreement in seasonal timing.

- [Seasonal means](results/seasonal_comparison.png): each month averaged over the same three years; each panel has its own temperature axis.
- [Threshold differences](results/threshold_differences.png): three-year means with observed year ranges, **not confidence intervals**.
- [City summary](results/city_summary.csv), [all city-years](results/annual_summary.csv), [threshold overlap/disagreement](results/threshold_summary.csv), [monthly means and counts](results/monthly_summary.csv).
- [All 5,475 paired daily values](results/paired_daily.csv).

## Published data, locations and matching

CarbonPlan releases [historical daily outdoor WBGT](https://github.com/carbonplan/extreme-heat/blob/main/data/zarr_daily_locations.md) in its [historical sun-WBGT Zarr store](https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0/outputs/zarr/daily/historical-WBGT-sun.zarr/.zmetadata). Its data are **CC BY 4.0**; credit CarbonPlan. We downloaded a bounded 16 MiB portion of the released geography plus four necessary ACCESS-CM2 daily location chunks, cached locally for reuse. No global archive was downloaded.

All five selected published city polygons and their daily series were verified. Names alone are insufficient: the geography includes Hyderabad in Pakistan as well as India. The script selects the intended city within one degree of its declared Indian coordinates, rejects duplicate geographically matched records, and resolves the published processing ID through the complete Zarr coordinate axis. Feature JSONs are retained beside the results. Exact IDs, axis positions, metadata/chunk hashes, GMTED hashes and input-array slice hashes are in [the manifest](results/manifest.json) and [coverage diagnostics](results/coverage_diagnostics.json).

W1 uses unchanged pilot Liljegren reconstruction, W1 wind, constant-vapour-pressure humidity, flagged hurs>100 clipping and GMTED2010 cell-mean elevation. All six variables are checked for units/calendar/day coverage across 2004–2010, including padding years. Daily cubes must have identical variable grids and dates. There are 28 distinct intersecting climate cells: Kochi 5, Bikaner 2, Shimla 1, Hyderabad 5, Kolkata 15. Every footprint has 100% represented area and every city-year has 365 jointly finite days. Kolkata 2009 has two target cell-days with RH clipping; all others have none. No missing days were filled, annualized or counted as zero.

W1 cell daily maxima are area-averaged over each exact CarbonPlan city polygon, **then** the regional daily series is thresholded with >=28/30/32°C. This differs from area-averaging cell annual exceedance counts. Threshold tables include both-exceed, neither-exceed, W1-only and CarbonPlan-only days, exposing cancellation behind net totals. Annual metrics require complete years; monthly counts require complete months. On incomplete years only explicitly matched-date diagnostic disagreements are retained, not full-year counts. This run has no incomplete years.

## Limits and decision

CarbonPlan uses population-informed spatial aggregation and a corrected shade-WBGT chain followed by a sun adjustment; W1 uses area-weighted cell-level physical reconstruction without accepted derived-WBGT bias correction. Aggregation order, spatial weights/grids and correction differ. Matching GCM name and year does not establish identical ensemble member or NEX input version. This is a matched-footprint **product comparison**, not a controlled comparison of formulas or an observational accuracy test. The 1-cell Shimla footprint particularly limits spatial inference. Three selected years of one model do not quantify long-run or ensemble uncertainty.

**Keep outdoor counts diagnostic-only.** This larger comparison strengthens the evidence of regional/seasonal disagreement, rather than demonstrating alignment with CarbonPlan. Do not tune a global offset or another wind shape to match this table. A follow-up, if commissioned, should separate spatial weighting/aggregation and correction-chain effects at the already selected cities before drawing scientific conclusions. No new experiment or deployment is included here.

## Validation and reproduction

Two complete reconstruction runs produced identical daily output values. The original Kochi 2005 W1 and CarbonPlan daily series reproduce within 1e-10°C, with exactly matching threshold counts. Independent summary validation checks all 5,475 unique dates, 15 annual summaries, 45 threshold summaries, 180 monthly summaries, monthly/annual count identities and four-way threshold partitions. Five focused contract tests cover inclusive thresholds, missing days, all-missing, single-point, empty input and single-cell geometry/extreme values.

Run in the existing IRT conda environment from the repository root. No dependencies are added. Commands are chronological:

1. Verify the local elevation/cache inputs (read-only):

```bash
python -c "from pathlib import Path; b=Path('scratch/wbgt_outdoor_pilot_qc'); assert (b/'cells_Kerala_ACCESS-CM2_2005_combined.nc').is_file(); assert all((b/'elevation'/f'{t}_20101117_gmted_mea300.tif').is_file() for t in ('10S060E','10N060E','30N060E'))"
```

2. Run contract checks (no downloads):

```bash
python docs/diagnostics/wbgt_outdoor_carbonplan_multicity/scripts/test_contracts.py
```

3. Validate source inputs, acquire/reuse bounded published CarbonPlan chunks, reconstruct city cells and write a **fresh** output directory. Existing output directories are refused; all source and predecessor evidence remains read-only:

```bash
python docs/diagnostics/wbgt_outdoor_carbonplan_multicity/scripts/compare_multicity.py --out-dir scratch/wbgt_multicity_reproduce --work-dir scratch/wbgt_carbonplan_multicity
```

4. Validate all numerical identities and write summaries/figures inside that output directory:

```bash
python docs/diagnostics/wbgt_outdoor_carbonplan_multicity/scripts/summarize_and_plot.py --out-dir scratch/wbgt_multicity_reproduce
```

The default source roots are the same local six-variable NEX roots as the frozen pilot. Missing/invalid files fail preflight explicitly. README.md at the repository root needs no change because this adds isolated diagnostic evidence, not production behavior. MANIFEST.md indexes this comparison. No HANDOFF/BACKLOG update or commit was performed.

Maintenance: `graphify update .` was attempted with a 50-second bound and timed out without tracked graph changes. The diagnostic evidence and scripts are complete; the knowledge graph refresh remains outstanding. `git diff --check` passed.
