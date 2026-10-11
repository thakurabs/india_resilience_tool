# Does QDM bias correction measurably improve IRT's deployed metrics?

**Status:** diagnostic only — nothing in the pipeline, the processed outputs or the dashboard was changed.
**Date:** 2026-10-11 · **Snapshot:** `add_flood_depth@a87b239`
**Trigger:** the user observed that on the Heat Stress map, `twb_days_ge_28` paints central India almost white.

## 0. Answer in one screen

1. **`twb_days_ge_28` is inflated about 10–17× in the humid east, and the cause is understood.**
   - In 1990–2010, Kolkata publishes 108 days/yr and Lucknow 60. ERA5, computed the same way, gives 9.8 and 3.5.
   - In all 22 models, NEX monsoon `hurs` is 5–7 RH points more humid than ERA5. Kolkata JJAS is 90.3–92.5 % against ERA5's 85 %.
   - The 28 °C threshold sits only about 1.2 °C above the mean monsoon-day wet-bulb, so this small offset tips most of the season over the line.
   - Central India being near-white is **physically correct** (dry heat). It looks extreme because the east stretches the colour scale.
2. **QDM against ERA5 fixes the baseline out of sample and roughly halves the mapped mid-century value.**
   - Kolkata, ssp245 2040–60: 132 → 60 days.
   - The mean warming signal is preserved exactly.
3. **Across all deployed metrics, QDM gives a material, verifiable improvement for 13 metrics.** They form one coherent class: **absolute-threshold counts and levels built on humidity or night-time temperature.** A 14th, `twb_days_ge_30`, follows the same mechanism, but its baseline is too rare to verify.
4. **QDM does nothing measurable for any metric defined against its own baseline.** That covers tx90p/tn90p/tx10p/tn10p, WSDI/CSDI, HWFI and SPI; those definitions already cancel bias.
5. **For two tasmax metrics QDM makes things worse** (`txge35_extreme_heat_days`, `hwa_heatwave_amplitude`). ERA5 is a doubtful reference for afternoon maxima.
6. **Precipitation is UNRESOLVED.**
   - The only rainfall reference covering 1990–2010 on disk is ERA5 over Telangana, and ERA5 has the known drizzle bias (longest dry spell 64 days vs NEX 102).
   - QDM toward it would degrade dry-spell metrics.
   - A valid rainfall verdict needs IMD 0.25° gridded rainfall for 1990–2010. Only 1980–1985 is on disk.

## 1. Inventory of deployed metrics (105 in `processed_optimised/metrics`)

| Group | Count | QDM relevance |
|---|---|---|
| NEX-derived temperature / humidity metrics, measured here | 35 | §4 verdicts |
| NEX-derived precipitation metrics, measured here | 15 | §5 — UNRESOLVED (reference unfit) |
| NEX-derived, not measured: `aridity_index_p_over_pet`, `r95p_interannual_variability` | 2 | open (§7) |
| Shade WBGT (`wbgt_shade_stull_*`) | 4 | **already QDM-corrected** (CarbonPlan shade, CHG-0700..0703) |
| Outdoor WBGT Route B (`wbgt_outdoor_route_b_*`) | 5 | **already QDM-corrected** (CHG-0676..0683) |
| Composites (`composite_*`) | 15 | inherit their inputs; `composite_heat_stress` weights `twb_days_ge_28` |
| Non-climate context / exposure / hazard layers (population, built-up, LULC, facilities, groundwater, water scarcity, JRC flood) | 29 | not applicable — no NEX daily input |

## 2. Method

**Data.** No new downloads were made; every read is read-only.

- **NEX-GDDP-CMIP6 v1.0:** daily tas, tasmax, tasmin, hurs and pr from `irt_data/r1i1p1f1`.
  - 5 models: ACCESS-CM2, EC-Earth3, GFDL-ESM4, MIROC6, MPI-ESM1-2-LR.
  - Taken at the nearest cell to 6 sites, for historical 1990–2010 and ssp245 2040–2060.
  - Precipitation additionally on every 3rd cell of the Telangana grid (~100 cells).
- **Temperature/humidity reference:** ERA5 hourly at the 6 sites (`scratch/wbgt_deployed_vs_reference_cache`), aggregated to IST days.
  - Daily mean T and RH; tasmax and tasmin are the max and min of the hourly values.
- **Precipitation reference:** ERA5 daily precipitation over Telangana (`irt_data/era5/nc/pr/*_tel.nc`).
  - Each NEX cell takes the mean of the 4 surrounding ERA5 points.

**Correction.**

- **Settings:** QDM exactly as in `tools/diagnostics/wbgt_qdm_bias_correction.py`. That means Cannon et al. (2015), 100 quantiles, and day-of-year groups with a 31-day window, the CarbonPlan settings.
- **Temperature:** additive QDM per variable.
- **Wet-bulb and sWBGT:** corrected directly as derived daily quantities, as CarbonPlan does for WBGT.
- **Precipitation:** multiplicative QDM with 0.1 mm trace jitter.

**Two tests per metric:**

- **holdout:** train on 1990–2002, score on 2003–2010 against the reference. These years were never seen by the fit.
- **future:** train on 1990–2010 and apply to ssp245 2040–2060, the map's default view. Measure how far the mapped value and the change signal move.

**Metric definitions.**

- Each metric is a site-level re-implementation of the registry parameters. Raw, QDM and reference all go through the identical code, so the comparison isolates the correction.
- Baseline-relative metrics use a common 1990–2010 baseline. The registry uses 1981–2010 for some; this is a deliberate simplification.
- Reproduction check (`scripts/sanity.py`): the raw site values reproduce the **published district values** for the same 5 models.
  - Examples: tx90p 10.3 vs 10.3, wsdi 11.5 vs 11.6, txx 41.6 vs 41.3, `twb_days_ge_28` 101.5 vs 99.2 (all Kolkata).
  - Kochi differs because the coastal point is not the Ernakulam district mean.

**Materiality rule, declared before results were seen** (`FLOORS` in `scripts/qdm_applicability.py`).

Floors by unit:

| Unit | Floor |
|---|---|
| Day counts | 5 days/yr |
| °C | 0.5 °C |
| % of days | 1 pp |
| Spell length | 1 day |
| Event counts | 0.5 /yr |
| Rainfall amounts | 10 % of the reference |

Verdicts:

- **MATERIAL** requires all three of:
  - the holdout error falls by at least the floor;
  - the error at least halves;
  - the mid-century value moves by at least the floor.
- **MARGINAL:** the error falls by at least half the floor.
- **WORSE:** the error rises by more than half the floor.
- **FUTURE-ONLY:** the historical error is below the floor, but the future value moves by at least the floor. The baseline cannot verify these.
- **NO EFFECT:** none of the above.

**Shimla is excluded from the verdicts.**

- Its raw errors of 3–4 °C are an elevation mismatch between the ERA5 point and the NEX cell, not a bias that QDM should be credited for fixing.
- With Shimla included, `tas_annual_mean`, `txx_annual_max`, `tnn_annual_min`, `tasmin_winter_min`, `tnle5_severe_cold_nights` and `swbgt_empirical_annual_mean` flip to MATERIAL. Both verdicts are kept in `data/summary_final.csv` (`verdict_6sites`).

## 3. The triggering case: `twb_days_ge_28`

| | Kolkata | Lucknow | Bikaner | Kochi | Hyderabad |
|---|---|---|---|---|---|
| Published district value 1990–2010 (all-model ensemble) | 108 | 60 | 8.1 | 8.3 | 1.2 |
| ERA5, same method (Stull on daily means) | 9.8 | 3.5 | 0.4 | 0.0 | 0.0 |
| ERA5, daily **max**-hour Twb ≥ 28 | 76.5 | 50.7 | 11.3 | 9.4 | 0.0 |
| NEX JJAS `hurs`, range across 22 models (%) | 90.3–92.5 | 74.7–82.4 | 48.5–56.5 | — | 74.9–80.8 |
| ERA5 JJAS RH (%) | 85.1 | 76.1 | 53.9 | 90.1 | 74.4 |

- **Mechanism test (`scripts/j.py`).** Adding the measured NEX-minus-ERA5 offsets (+7 RH points, +0.6 °C) to ERA5's own Kolkata series gives **117 days**, against the published 108. So the offset alone explains Kolkata.
  - At Lucknow the offsets explain only about half (32 vs 60). The remainder is unexplained, probably NEX's daily variance or the point-versus-district-mean difference.
- **CMCC-CM2-SR5 and TaiESM1** give about 172 days at Kolkata even historically. Their `hurs` is normal, so the excess comes from temperature (CMCC JJAS tas is +1.8 °C against the other models). This is a separate open item.
- **NEX version.** The local tas/tasmax/hurs files are NEX v1.0.
  - NASA's v1.1 `hurs` fix bounded unrealistically high values. Our monsoon values never exceed 100 %, so that fix would not change this result.
  - The 2026-09-24 ruling not to re-fetch v2.x stands. A direct v1.0-versus-v2.0 humidity comparison has never been measured. Three NCSS point probes on 2026-10-11 all timed out.

QDM (`scripts/qdm_twb.py`, `scripts/qdm_twb_245.py`), 5-model mean, days/yr:

| Site | ERA5 1990–2010 | Raw 1990–2010 | QDM holdout 2003–10 (ERA5 14.5 / 4.5) | Raw ssp245 2040–60 | QDM ssp245 2040–60 | Raw ssp585 2071–80 | QDM ssp585 2071–80 |
|---|---|---|---|---|---|---|---|
| Kolkata | 10.2 | 101.5 | 8.6 | 131.7 | **60.3** | 165.7 | 161.8 |
| Lucknow | 3.7 | 50.0 | 8.6 | 79.8 | **39.3** | 110.3 | 112.1 |
| Kochi | 0.0 | 3.1 | 0.1 | 22.7 | **4.6** | 117.6 | 70.9 |
| Hyderabad | 0.0 | 0.2 | 0.0 | 2.7 | **0.2** | 27.8 | 14.7 |
| Bikaner | 0.4 | 5.7 | 1.2 | 25.1 | 20.1 | 63.9 | 76.7 |

- The mean-Twb warming signal is identical with and without QDM at every site.
- The effect is largest where the baseline sits just below the threshold: the baseline and mid-century.
- By ssp585 2071–80 the threshold saturates and QDM barely changes Kolkata or Lucknow.
- After correction the models disagree more. Kolkata's 2040–60 range is 29–116 days after QDM, against 106–146 raw: the raw data was more certain than it should have been.

## 4. Verdicts: temperature and humidity metrics (5 sites, Shimla excluded)

Values are 5-model means over 5 sites. "Holdout err" is the mean absolute error against ERA5 on 2003–2010.

| Verdict | Metric | Holdout err raw → QDM | Raw → QDM ssp245 2040–60 |
|---|---|---|---|
| **MATERIAL** | `twb_days_ge_28` | 31.2 → 2.0 d | 52.4 → 24.9 d |
| | `twb_annual_mean` | 0.83 → 0.15 °C | 21.9 → 22.7 °C |
| | `twb_summer_mean` | 2.10 → 0.32 °C | 21.5 → 23.4 °C |
| | `twb_annual_max` | 1.27 → 0.34 °C | 29.9 → 28.8 °C |
| | `swbgt_empirical_days_ge_28` | 20.6 → 4.8 d | 229 → 246 d |
| | `swbgt_empirical_days_ge_30` | 20.2 → 8.1 d | 197 → 211 d |
| | `swbgt_empirical_days_ge_32` | 18.7 → 9.0 d | 148 → 141 d |
| | `tasmin_tropical_nights_gt25` | 26.1 → 6.5 d | 159 → 166 d |
| | `tasmin_tropical_nights_gt28` | 11.7 → 4.3 d | 43 → 61 d |
| | `tnle10_cold_nights` | 10.7 → 2.7 d | 18.6 → 9.7 d |
| | `tnle10_consecutive_cold_nights` | 3.9 → 1.5 d | 7.4 → 4.1 d |
| | `tasmin_winter_mean` | 1.26 → 0.47 °C | 15.5 → 16.5 °C |
| | `tnx_annual_max` | 1.01 → 0.47 °C | 31.4 → 31.7 °C |
| **MARGINAL** | `tas_annual_mean`, `tas_summer_mean`, `txx_annual_max`, `tnn_annual_min`, `tasmin_winter_min`, `tnle5_severe_cold_nights`, `txge30_hot_days`, `swbgt_empirical_annual_mean` | gains below the floor | |
| **FUTURE-ONLY** | `twb_days_ge_30` | 0.96 → 0.06 d (event absent in baseline) | 8.6 → 1.8 d |
| | `wsdi_warm_spell_days` | no historical change | 62.0 → 56.9 d |
| | `tas_winter_mean` | 0.46 → 0.42 °C | 22.1 → 21.9 °C |
| **NO EFFECT** | `tx90p_hot_days_pct`, `tn90p_warm_nights_pct`, `tx10p_cool_days_pct`, `tn10p_cool_nights_pct`, `csdi_cold_spell_days`, `hwfi_tmean_90p`, `hwfi_events_tmean_90p`, `tasmax_summer_mean`, `txle15_cold_days` | ≤ 0.15 change | |
| **WORSE** | `txge35_extreme_heat_days` | 8.9 → 13.1 d | 113 → 108 d |
| | `hwa_heatwave_amplitude` | 1.50 → 1.86 °C | 42.1 → 41.6 °C |

**Reading the pattern:**

- **Humidity-built metrics gain most.**
  - BCSD corrects `tas` and `hurs` as separate marginals.
  - Nothing constrains the derived wet-bulb, so its bias goes uncorrected.
  - That is exactly what correcting the derived quantity repairs.
- **Night-time (tasmin) counts gain clearly.** Their raw bias has mixed signs by site (Kochi +36 tropical nights, Bikaner −33).
- **Baseline-relative metrics don't move,** as expected: the percentile threshold travels with the bias.
- **tasmax extremes get worse.**
  - ERA5's hourly-maximum tasmax under-represents the dry-season afternoon peak.
  - Pulling NEX toward ERA5 there imports ERA5's error.
  - Do not apply ERA5-QDM to tasmax-threshold metrics without a station-grade reference (e.g. IMD gridded Tmax).

## 5. Precipitation: UNRESOLVED (reference unfit)

ERA5 over Telangana, holdout means:

| Metric | ERA5 | Raw NEX | QDM toward ERA5 |
|---|---|---|---|
| CDD (longest run < 1 mm) | 64 | 102 | 58 |
| CWD (longest run ≥ 1 mm) | 23.5 | 16.8 | 21.7 |
| Rx1day (mm) | 85 | 48 | 88 |
| R99p (mm) | 122 | 49 | 140 |

- Telangana has a long dry season. An ERA5 CDD of 64 days and a CWD of 23.5 days reflect ERA5's light-rain (drizzle) excess, not reality.
- QDM therefore "improves" agreement with a reference that is itself wrong for dry/wet spells.
- For the heavy-rain metrics (Rx1day, R95p, R99p), ERA5's larger extremes may be closer to the truth than BCSD's. Under multiplicative QDM they also inflate the projected change: R99p goes from +31 mm to +161 mm by 2040–60.
- **None of the precipitation verdicts in `data/summary_final.csv` should be acted on.**
- SPI metrics showed no effect, consistent with SPI's own baseline gamma fit cancelling the bias.

## 6. Threats to validity

- **ERA5 is treated as the truth.** For single-variable metrics, "MATERIAL" means QDM consistently moves NEX from its own BCSD target (GMFD) toward ERA5. That is an improvement only if ERA5 is the better reference at those sites.
  - Neither humidity nor tasmin has been checked against station observations here. An IMD station RH comparison at Kolkata/Lucknow is the cheapest independent check.
- **Small sample.** 5 sites, 5 of 22 models, 8 holdout years.
- **Points, not districts.** These are nearest-cell points, not district means. The district reproduction check passes, but a national gridded test has not been done.
- **Simplified definitions.** Metrics are re-implemented with a common 1990–2010 baseline, and SPI uses method-of-moments gamma per calendar month.
- **No physical-consistency step.** Additive QDM per variable can produce tasmin > tasmax. That was not checked or repaired here, and a production design would need it.

## 7. Open items for verification

1. **Precipitation:** acquire IMD 0.25° gridded daily rainfall for 1990–2010 (download — needs approval), then re-run §5 against it.
2. **tasmax extremes:** test against IMD gridded Tmax before any decision on `txge35`, `txx` or `hwa`.
3. **Scale-up check:** run the 15 MATERIAL metrics against a **gridded** ERA5 reference on a subset of states, with district aggregation, to confirm the site results hold at district level. National gridded ERA5 does not exist locally; only Telangana is on disk.
4. **Not measured yet:** `aridity_index_p_over_pet` (multivariate: pr + Hargreaves PET from tasmax/tasmin) and `r95p_interannual_variability`.
5. **Outlier models:** investigate CMCC-CM2-SR5 and TaiESM1 historical tas at Kolkata (+1.8 °C).

## 8. Reproduction

Run from the repo root with the Windows `irt` interpreter (pandas, xarray, scipy):

```bash
python docs/diagnostics/qdm_applicability/scripts/extract.py            # NEX site + Telangana series -> .scratch/qdm_applicability/series (~1 min/model-period, 5 workers)
python docs/diagnostics/qdm_applicability/scripts/qdm_applicability.py  # all metrics -> .scratch/qdm_applicability/out
python docs/diagnostics/qdm_applicability/scripts/final.py              # verdict table (Shimla excluded) -> summary_final.csv
python docs/diagnostics/qdm_applicability/scripts/sanity.py             # reproduction check vs published district values
```

- **Wet-bulb deep-dive scripts:**
  - `scripts/d.py`: ERA5 vs published counts.
  - `scripts/h.py`: monsoon `hurs`, all 22 models.
  - `scripts/j.py`: offset-mechanism test.
  - `scripts/qdm_twb.py` and `scripts/qdm_twb_245.py`: QDM at ssp585 2071–80 and ssp245 2040–60.
  - `scripts/extract_ssp245.py`: series extraction.
- **Path assumptions:** the scripts were run from `.scratch/` and still read their caches from `.scratch/...` and `scratch/...`.
- **Unit-level outputs:** `data/temperature_humidity_unit_rows.csv` and `data/precipitation_unit_rows.csv`.
