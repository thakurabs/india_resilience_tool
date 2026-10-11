# Does QDM bias correction measurably improve IRT's deployed metrics?

**Status:** diagnostic only — nothing in the pipeline, the processed outputs or the dashboard was changed.
**Date:** 2026-10-11 · **Snapshot:** `add_flood_depth@a87b239`. Precipitation section added at `b18a751` after the IMD 1990–2010 download.
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
6. **Precipitation, scored nationally against IMD 0.25° gridded rainfall (74 cells, §5).** NEX underestimates heavy-rain extremes by about 40 %: Rx1day −31 mm, R99p −45 mm, R95p −110 mm. It overestimates the longest dry spell by 22 days.
   - **The QDM configuration decides the outcome.** With the 31-day window used for temperature, multiplicative QDM overshoots the rain tail (Rx1day bias −31 → +17.5 mm), and its verdict is WORSE for Rx5day, R99p and R95p variability.
   - With a **91-day window** it removes the systematic bias almost exactly (Rx1day +0.1, R99p +2.4, R95pTOT 0.0, CDD −1.4).
   - Per-cell error stays larger because of 8-year sampling noise, so the mechanical halving rule rates these MARGINAL.
   - The mapped 2040–60 values move a lot: Rx1day 55 → 87 mm, R95p 227 → 352 mm, CDD 102 → 80 days.
   - **No effect:** SPI (all 7), CWD and `r95p_interannual_variability`. These are sequence or self-normalised properties that marginal QDM cannot change.
   - **Correction to the first version of this report:** "ERA5 has a drizzle bias that makes CDD wrong" was half wrong. IMD gives a Telangana CDD of 68 days, close to ERA5's 64; NEX (102) is the outlier. ERA5's drizzle bias shows in wet spells instead (CWD 23.5 vs IMD 12.0).

## 1. Inventory of deployed metrics (105 in `processed_optimised/metrics`)

| Group | Count | QDM relevance |
|---|---|---|
| NEX-derived temperature / humidity metrics, measured here | 35 | §4 verdicts |
| NEX-derived precipitation metrics, measured against IMD (incl. `r95p_interannual_variability`) | 16 | §5 |
| NEX-derived, not measured: `aridity_index_p_over_pet` | 1 | open (§7) |
| Shade WBGT (`wbgt_shade_stull_*`) | 4 | **already QDM-corrected** (CarbonPlan shade, CHG-0700..0703) |
| Outdoor WBGT Route B (`wbgt_outdoor_route_b_*`) | 5 | **already QDM-corrected** (CHG-0676..0683) |
| Composites (`composite_*`) | 15 | inherit their inputs; `composite_heat_stress` weights `twb_days_ge_28` |
| Non-climate context / exposure / hazard layers (population, built-up, LULC, facilities, groundwater, water scarcity, JRC flood) | 29 | not applicable — no NEX daily input |

## 2. Method

**Data.** No new downloads were made; every read is read-only.

- **NEX-GDDP-CMIP6 v1.0:** daily tas, tasmax, tasmin, hurs and pr from `irt_data/r1i1p1f1`.
  - 5 models: ACCESS-CM2, EC-Earth3, GFDL-ESM4, MIROC6, MPI-ESM1-2-LR.
  - Taken at the nearest cell to 6 sites, for historical 1990–2010 and ssp245 2040–2060.
  - Precipitation additionally on every 3rd cell of the Telangana grid (~100 cells), scored against ERA5. **Superseded by the national IMD test in §5.** The ERA5 precipitation rows in `data/summary_final.csv` and `data/precipitation_unit_rows.csv` are kept only as a record.
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

## 5. Precipitation, against IMD gridded rainfall

**Reference and design.**

- **Reference:** IMD 0.25° gridded daily rainfall, 1990–2010, downloaded 2026-10-11 to `irt_data/imd/rain/` via `imdlib`.
  - All 21 files were checked against the exact byte size of 135 × 129 × days × 4.
  - The IMD server dropped connections repeatedly; `scripts/imd_get.py` retries with backoff. 7 of the 21 years needed more than one attempt (1998 needed 5). No year failed, and no existing file was overwritten.
  - The files for 1980–1985 were already on disk and were not used.
- **Sampling:** 74 NEX cells on a national 2° lattice, each with all four overlapping IMD boxes on land. The reference for each cell is the mean of those 4 IMD boxes (conservative regrid; NEX cells are offset 0.125° from IMD).
- **Same as §2:** holdout/future design, metric definitions, floors and 5 models. Multiplicative QDM with 0.1 mm trace jitter.
- **Two QDM configurations were run:**
  - 31-day day-of-year window, the CarbonPlan/temperature setting.
  - 91-day window.

**Systematic error (mean signed bias against IMD, holdout 2003–2010):**

| Metric | IMD | Raw NEX bias | QDM 31-d bias | **QDM 91-d bias** | Raw → QDM-91 ssp245 2040–60 |
|---|---|---|---|---|---|
| Rx1day (mm) | 81.6 | −30.9 | +17.5 | **+0.1** | 54.7 → 87.4 |
| Rx5day (mm) | 173.7 | −27.2 | +53.4 | +22.7 | 154 → 205 |
| R95p (mm) | 295.9 | −110.1 | +60.0 | **+11.0** | 227 → 352 |
| R99p (mm) | 98.9 | −44.6 | +34.8 | **+2.4** | 79 → 134 |
| R95pTOT (%) | 24.2 | −7.3 | +1.4 | **0.0** | 19.5 → 26.3 |
| R20mm (days) | 15.5 | +2.5 | +3.0 | +1.3 | 19.0 → 17.5 |
| CDD (days) | 79.7 | +22.5 | −4.3 | **−1.4** | 102 → 80 |
| CWD (days) | 14.7 | +4.5 | +5.8 | +6.0 | 19.5 → 20.0 |

**Mechanical verdicts** (mean absolute per-cell error, §2 rule; `data/imd_w31/`, `data/imd_w91/`):

| Metric | 31-day window | 91-day window |
|---|---|---|
| CDD `pr_consecutive_dry_days_lt1mm` | MATERIAL (25.8 → 11.9) | MARGINAL (25.8 → 13.0; misses halving by 0.1) |
| Rx1day | NO EFFECT | MARGINAL (34.7 → 22.9) |
| R95p | NO EFFECT | MARGINAL (135 → 97) |
| R99p | WORSE | MARGINAL (58.5 → 53.3) |
| R95pTOT | MARGINAL | MARGINAL (8.0 → 4.9) |
| R20mm | MARGINAL | MARGINAL (8.0 → 3.6) |
| Rx5day | WORSE | NO EFFECT |
| `r95p_interannual_variability` | WORSE | NO EFFECT |
| CWD | NO EFFECT | NO EFFECT |
| SPI 3/6/12, all 7 metrics | NO EFFECT | NO EFFECT |

**Reading:**

- **Extremes.**
  - NEX BCSD underestimates heavy-rain extremes nationally by about 40 % and overestimates dry spells.
  - With a 91-day window, QDM removes that systematic bias almost exactly.
  - The per-cell error that remains (Rx1day about 23 mm) is mostly 8-year sampling noise in single-cell annual maxima, which no correction can remove. That is why the halving rule, built for the temperature metrics, rates these MARGINAL even though the bias has gone.
- **The window choice is decisive for rainfall.**
  - With 13 training years, a 31-day group has about 400 days per quantile set but only a handful of heavy-rain days in the top quantiles.
  - The multiplicative ratio then amplifies that noise, so the tail overshoots: the per-cell QDM/IMD ratio for Rx1day spans 0.73–1.71.
  - Any production design for rainfall should use a wider window, or monthly/seasonal grouping, and test it the same way.
- **Multiplicative QDM preserves relative change, so absolute projected changes grow with the corrected baseline.** Rx1day's 2040–60 change goes from +4.9 mm raw to +9.7 mm. This is intended QDM behaviour, but it is a methodology change to the projected signal and must be called out if adopted.
- **Some metrics can't be corrected this way.**
  - CWD is a sequence property: wet-spell persistence is set by the model's daily ordering, not its marginal distribution.
  - SPI is self-normalised against its own baseline gamma fit.
  - R95p variability is a ratio.
  - Marginal QDM changes none of them, as expected.
- **ERA5 claim corrected.** On the same Telangana cells (holdout means):

  | Metric | ERA5 | IMD | Raw NEX |
  |---|---|---|---|
  | CDD (days) | 64.0 | 68.4 | 101.6 |
  | CWD (days) | 23.5 | 12.0 | 16.8 |
  | Rx1day (mm) | 85.0 | 91.5 | 47.7 |
  | R99p (mm) | 121.9 | 115.1 | 49.4 |

  ERA5 matches IMD for CDD and extremes. Its drizzle bias shows only in wet-spell length. The first version of this report wrongly called ERA5's CDD unrealistic.

## 6. Threats to validity

- **ERA5 is treated as the truth.** For single-variable metrics, "MATERIAL" means QDM consistently moves NEX from its own BCSD target (GMFD) toward ERA5. That is an improvement only if ERA5 is the better reference at those sites.
  - Neither humidity nor tasmin has been checked against station observations here. An IMD station RH comparison at Kolkata/Lucknow is the cheapest independent check.
- **Small sample.** 5 sites, 5 of 22 models, 8 holdout years.
- **Points, not districts.** These are nearest-cell points, not district means. The district reproduction check passes, but a national gridded test has not been done.
- **Simplified definitions.** Metrics are re-implemented with a common 1990–2010 baseline, and SPI uses method-of-moments gamma per calendar month.
- **Precipitation reference and sample (§5).**
  - IMD is interpolated from rain gauges. Gauges are sparse in the north-east and the Himalaya, so the reference itself is weaker there.
  - The 74 cells sit on a 2° lattice; they are not district means.
  - Only 5 of the 22 models were used, and the holdout is 8 years. Single-cell annual maxima are noisy over 8 years, which is why the bias disappears while the per-cell error does not.
- **No physical-consistency step.** Additive QDM per variable can produce tasmin > tasmax. That was not checked or repaired here, and a production design would need it.

## 7. Open items for verification

1. **Precipitation:** done against IMD (§5). Remaining questions:
   - Confirm that the 91-day (or monthly) grouping holds with more models and at district level.
   - Decide whether a larger projected change in rainfall extremes is acceptable as a methodology change.
2. **tasmax extremes:** test against IMD gridded Tmax before any decision on `txge35`, `txx` or `hwa`.
3. **Scale-up check:** run the 13 MATERIAL metrics against a **gridded** ERA5 reference on a subset of states, with district aggregation, to confirm the site results hold at district level. National gridded ERA5 does not exist locally; only Telangana is on disk.
4. **Not measured yet:** `aridity_index_p_over_pet` (multivariate: pr + Hargreaves PET from tasmax/tasmin). It needs IMD Tmax/Tmin, which also serves item 2.
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
- **Precipitation against IMD:**
  - `scripts/imd_get.py 1990 2010`: download, new files only, retries, size check.
  - `scripts/extract_pr_national.py`: 74-cell NEX lattice.
  - `scripts/qdm_pr_imd.py`: 31-day window.
  - `scripts/qdm_pr_imd_w91.py`: 91-day window.
  - `scripts/pr_signed.py`: signed bias.
  - Outputs are in `data/imd_w31/` and `data/imd_w91/`; the cell list is `data/imd_cells.csv`.
- **Path assumptions:** the scripts were run from `.scratch/` and still read their caches from `.scratch/...` and `scratch/...`.
- **Unit-level outputs:** `data/temperature_humidity_unit_rows.csv` and `data/precipitation_unit_rows.csv`.
