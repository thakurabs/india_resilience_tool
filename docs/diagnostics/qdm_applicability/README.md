# Does QDM bias correction measurably improve IRT's deployed metrics?

**Status:** diagnostic only — nothing in the pipeline, the processed outputs or the dashboard was changed.
**Date:** 2026-10-11 · **Snapshot:** `add_flood_depth@a87b239`. Precipitation section (§5) added at `b18a751` after the IMD 1990–2010 rain download. Tmax/Tmin and aridity section (§6) added 2026-10-12 at `1131afe` after the IMD temperature download. National wet-bulb section (§7) added 2026-10-18 at `b4dc757`, after the hourly ERA5 India download (CHG-0708) and the daily reference built from it (CHG-0709).
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
3. **Across all deployed metrics, QDM gives a material, verifiable improvement for 13 metrics.** They form one coherent class: **absolute-threshold counts and levels built on humidity or night-time temperature.** A 14th, `twb_days_ge_30`, follows the same mechanism, but its baseline is too rare to verify. **The IMD tests in §6 revise this list:** `txx_annual_max` and `txge30_hot_days` join it. `tnle10_cold_nights` and `tnle10_consecutive_cold_nights` hold only at two sites and in the Himalaya, so they are doubtful nationally.
4. **QDM does nothing measurable for any metric defined against its own baseline.** That covers tx90p/tn90p/tx10p/tn10p, WSDI/CSDI, HWFI and SPI; those definitions already cancel bias.
5. **Against ERA5, QDM made two tasmax metrics worse** (`txge35_extreme_heat_days`, `hwa_heatwave_amplitude`). **IMD shows this was ERA5's fault (§6):** against IMD gridded Tmax, QDM improves every tasmax metric. `txx_annual_max` and `txge30_hot_days` are MATERIAL; `txge35` and `hwa` are MARGINAL.
6. **Precipitation, scored nationally against IMD 0.25° gridded rainfall (74 cells, §5).** NEX underestimates heavy-rain extremes by about 40 %: Rx1day −31 mm, R99p −45 mm, R95p −110 mm. It overestimates the longest dry spell by 22 days.
   - **The QDM configuration decides the outcome.** With the 31-day window used for temperature, multiplicative QDM overshoots the rain tail (Rx1day bias −31 → +17.5 mm), and its verdict is WORSE for Rx5day, R99p and R95p variability.
   - With a **91-day window** it removes the systematic bias almost exactly (Rx1day +0.1, R99p +2.4, R95pTOT 0.0, CDD −1.4).
   - Per-cell error stays larger because of 8-year sampling noise, so the mechanical halving rule rates these MARGINAL.
   - The mapped 2040–60 values move a lot: Rx1day 55 → 87 mm, R95p 227 → 352 mm, CDD 102 → 80 days.
   - **No effect:** SPI (all 7), CWD and `r95p_interannual_variability`. These are sequence or self-normalised properties that marginal QDM cannot change.
   - **Correction to the first version of this report:** "ERA5 has a drizzle bias that makes CDD wrong" was half wrong. IMD gives a Telangana CDD of 68 days, close to ERA5's 64; NEX (102) is the outlier. ERA5's drizzle bias shows in wet spells instead (CWD 23.5 vs IMD 12.0).

7. **Daily max/min temperature and aridity, scored nationally against IMD 1° gridded Tmax/Tmin (59 boxes, §6).**
   - **Confirmed independently of ERA5:** tropical nights > 25 and > 28 °C, and `tnx_annual_max`, are MATERIAL on both references.
   - **Newly MATERIAL:** `txx_annual_max` and `txge30_hot_days`.
   - **Weakened:** `tnle10_cold_nights` and `tnle10_consecutive_cold_nights` were MATERIAL against ERA5, but nearly all of their IMD improvement comes from 5 Himalayan boxes. Outside the Himalaya they are NO EFFECT and MARGINAL. `tasmin_winter_mean` drops to MARGINAL outside the Himalaya.
   - **Aridity index: NO EFFECT.** Raw NEX already matches IMD annual rainfall within 1.4 %. Multiplicative QDM *adds* 8 % to annual rainfall, an artefact of the correction that IMD itself does not explain, and raises the projected aridity change from +0.05 to +0.09.
   - Independent tasmax/tasmin correction does not create inverted days (0.003 %, same as raw).

8. **Wet-bulb metrics, scored on every Indian land cell and every district against a national ERA5 reference (§7).**
   - All four checkable `twb_*` metrics are MATERIAL nationally, on cells (4,625) and on districts (778). Error cut 80–95 %.
   - `twb_days_ge_28` district error falls from 17.9 to 0.8 days/yr outside the Himalaya. Its mapped 2040–60 value falls from 38 to 14 days/yr.
   - **The correction redraws the map, not just the scale.** Rank agreement raw vs corrected is 0.85–0.90, about 40 % of districts change quintile, and only 26 of the top 50 `twb_days_ge_28` districts stay in the top 50.
     - Assam and Arunachal fall out of the top (Biswanath #1 → #387): the raw model is far too humid there.
     - The Odisha and Andhra coasts rise (Puri #158 → #8): the raw model is too dry there.
     - Western Rajasthan rises 5-fold (Jaisalmer 6 → 28 days). The mechanism is understood (raw model 8 RH points too dry), but ERA5's desert humidity is unchecked.
   - `twb_days_ge_30` is too rare in ERA5 to verify.
   - The empirical sWBGT metrics in §4 were removed from the dashboard (`2fcce38`), so §7 does not cover them.

## 1. Inventory of deployed metrics (105 in `processed_optimised/metrics`)

| Group | Count | QDM relevance |
|---|---|---|
| NEX-derived temperature / humidity metrics, measured here | 35 | §4 verdicts |
| NEX-derived precipitation metrics, measured against IMD (incl. `r95p_interannual_variability`) | 16 | §5 |
| NEX-derived aridity index `aridity_index_p_over_pet` (pr + Hargreaves PET), measured against IMD | 1 | §6 |
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
  - **Superseded by §6:** against IMD gridded Tmax, QDM improves every tasmax metric. The WORSE verdicts above describe ERA5, not QDM.

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

## 6. Daily maximum/minimum temperature and the aridity index, against IMD gridded temperature

**Reference and design.**

- **Reference:** IMD 1° gridded daily Tmax and Tmin, 1990–2010, downloaded 2026-10-12 to `irt_data/imd/tmax/` and `irt_data/imd/tmin/`. All 42 files match the expected size of 31 × 31 × days × 4 bytes, and none needed a retry.
- **Units are IMD 1° boxes, not 0.25° cells.**
  - Each IMD box holds exactly 4 × 4 NEX cells. The NEX value is the plain mean of those 16 cells; a box with any missing cell is dropped.
  - For the aridity index, IMD 0.25° rain is area-averaged onto the same box. The edge rows and columns get half weight, because IMD rain centres sit on whole and quarter degrees.
- **Sample:** every other IMD box (a 2° lattice) that has complete Tmax and Tmin. That gives 75 boxes; 59 survive the NEX and IMD rain completeness checks.
  - 5 of them are Himalayan (latitude ≥ 30°). They are reported separately below, for the same reason Shimla was excluded in §4: a 1° box in the mountains mixes very different elevations.
- **Same as §2/§5:** holdout and future design, metric definitions, floors, verdict rule and the 5 models.
- **QDM settings:**
  - Additive QDM with a 31-day window for tasmax and tasmin, corrected independently.
  - Multiplicative QDM with a 91-day window for rainfall, the setting §5 showed is needed.
- **Scope:** all 22 deployed metrics built only on tasmax and/or tasmin, plus `aridity_index_p_over_pet` (annual P ÷ annual Hargreaves PET, the production formula from `evapotranspiration.py`). Metrics on daily-mean `tas` or humidity can't be scored, because IMD has neither.

**Verdicts** (holdout 2003–2010, mean absolute per-box error; `data/imd_temp/`):

| Metric | All 59 boxes | Excluding Himalaya (54) | ERA5 sites (§4) |
|---|---|---|---|
| `tasmin_tropical_nights_gt25` | MATERIAL (18.1 → 6.5 d) | **MATERIAL** (17.6 → 6.9) | MATERIAL |
| `tasmin_tropical_nights_gt28` | MATERIAL (8.9 → 3.0 d) | **MATERIAL** (8.7 → 3.2) | MATERIAL |
| `tnx_annual_max` | MATERIAL (2.11 → 0.54 °C) | **MATERIAL** (1.73 → 0.54) | MATERIAL |
| `txx_annual_max` | MATERIAL (1.66 → 0.54 °C) | **MATERIAL** (1.20 → 0.53) | MARGINAL |
| `txge30_hot_days` | MATERIAL (31.5 → 12.4 d) | **MATERIAL** (30.3 → 12.7) | MARGINAL |
| `txge35_extreme_heat_days` | MARGINAL (15.6 → 8.7 d) | MARGINAL (15.8 → 9.1) | WORSE |
| `hwa_heatwave_amplitude` | MARGINAL (2.27 → 1.57 °C) | MARGINAL (1.89 → 1.56) | WORSE |
| `tasmax_summer_mean` | MATERIAL (1.65 → 0.60 °C) | MARGINAL (1.00 → 0.57) | NO EFFECT |
| `tasmin_winter_mean` | MATERIAL (1.36 → 0.36 °C) | MARGINAL (0.81 → 0.37) | MATERIAL |
| `tasmin_winter_min`, `tnn_annual_min` | MATERIAL | MARGINAL | MARGINAL |
| `tnle10_consecutive_cold_nights` | MATERIAL (9.5 → 2.3 d) | MARGINAL (2.7 → 2.1) | MATERIAL |
| `tnle10_cold_nights` | MATERIAL (13.3 → 4.8 d) | NO EFFECT (6.6 → 4.6) | MATERIAL |
| `tnle5_severe_cold_nights`, `txle15_cold_days` | MATERIAL | NO EFFECT (event rare outside the Himalaya) | MARGINAL / NO EFFECT |
| `aridity_index_p_over_pet` | NO EFFECT (0.11 → 0.11) | NO EFFECT | not measured |
| tx90p, tn90p, tx10p, tn10p, WSDI, CSDI, `hwfi_events_tmean_90p` | NO EFFECT | NO EFFECT | NO EFFECT (WSDI future-only) |

**Reading:**

- **The §4 "tasmax extremes get worse" verdict was a reference artefact.** Against IMD, QDM improves every tasmax metric. `txx` and `txge30` are MATERIAL. `txge35` and `hwa` improve without reaching the halving rule, and `txge35` misses it by about 1 day. ERA5's hourly-maximum tasmax was the problem, as suspected.
- **Why `txge35` stays about 7 days low after QDM.** IMD's own days ≥ 35 °C rose from 91.0 (1990–2002) to 98.5 (2003–2010) on these boxes. A correction trained on 1990–2002 can only carry the models' own warming into the test years, so the extra observed rise remains as a −7 d signed bias even though the per-box spread halves. This is a limit of the holdout design. The production correction would be trained on all of 1990–2010, which includes that rise.
- **Tropical nights and `tnx` are confirmed independently.** ERA5 at sites and IMD on a national grid agree that these are MATERIAL.
- **The cold-night verdicts are Himalaya-driven.** The 5 Himalayan boxes supply over 90 % of the improvement for `txle15`, `tnle5` and `tnle10_consecutive_cold_nights`, and 79 % for `tnle10` (`data/imd_temp/himalaya_share_of_gain.csv`). Outside the Himalaya, `tnle10` is NO EFFECT and its consecutive-night variant is MARGINAL. The §4 MATERIAL verdicts for these two also rest on two sites: Bikaner (28.3 d of gain for `tnle10`) and Lucknow (11.2 d), with about 0 at the other three. So they are not established nationally.
- **Aridity index: no effect, and QDM moves the mean in the wrong direction.**

  | Holdout annual totals (mm) | IMD | Raw NEX | QDM |
  |---|---|---|---|
  | Precipitation | 988 | 1001 | 1081 |
  | Hargreaves PET | 1733 | 1668 | 1703 |

  - Raw NEX already matches IMD's annual rainfall within 1.4 %, because BCSD corrects monthly means.
  - Multiplicative QDM adds 8 % to annual rainfall. IMD itself doesn't explain this: its annual rainfall is 982 mm in 1990–2002 and 988 mm in 2003–2010. So the inflation is an artefact of the rainfall QDM; the likely cause is the tail correction adding rain to heavy days without taking it from moderate ones. The cause has not been isolated.
  - PET moves the right way (−4 % → −2 %), but the aridity bias grows from +0.03 to +0.07, and the per-box error does not change.
  - QDM also raises the projected 2040–60 aridity change from +0.05 to +0.09, almost all through precipitation (+11.6 % → +17.8 %). PET change is unaffected (+1.0 % vs +0.9 %).
  - **This also bears on §5:** any multiplicative-QDM rollout for rainfall needs a check that annual and seasonal totals are preserved.
- **Physical consistency holds.** Independent tasmax and tasmin correction produced inverted days (tasmin > tasmax) on 0.0027 % of 2040–60 days, the same as raw (0.0029 %). The worst box had 0.09 %. It doesn't affect Hargreaves PET in practice.

## 7. Wet-bulb metrics nationally, against a gridded ERA5 humidity reference

**Reference.**

- **Hourly ERA5** 2 m temperature and dewpoint over 6–38 N, 68–98 E, 1990–2010 (CHG-0708, `irt_data/era5_hourly_india/`, 253 files, 9 GB).
- **Daily reference on the NEX grid** (CHG-0709, `scripts/era5_daily_ref_build.py`, `irt_data/era5_daily_ref_nexgrid/`, 21 files, 753 MB). It matches how NEX defines a day:
  - IST calendar days (UTC+5:30);
  - tas = mean of the hourly temperatures; hurs = mean of the hourly RH (Magnus over water);
  - each NEX cell is the mean of the 4 surrounding ERA5 points.
- **Verification** (`.scratch/era5_national_ref/ref_check.py`):
  - all 7,670 days present, no NaN;
  - an independent pandas re-derivation at 6 sites matches to ≤ 2.3e-5;
  - `twb_days_ge_28` at the site cells reproduces the §3 site reference: Kolkata 10.05 vs 10.24, Lucknow 3.52 vs 3.71.
  - **The reference is unreliable in the mountains.** The Shimla cell reads 16.3 °C against 13.5 °C at the site, because a 4-point mean mixes valley and ridge. Cells at latitude ≥ 30° are reported separately.

**Design.** Same holdout/future design, verdict rule, floors and 5 models as §2. Wet-bulb is computed with Stull from daily-mean tas and hurs, the deployed method, and corrected directly with additive QDM (31-day window).

| Test | Units | Script | Run time |
|---|---|---|---|
| A | 129 cells on a 1.5° lattice | `scripts/qdm_twb_national.py` | 3 min |
| B | all 4,625 land cells (all 4 surrounding IMD rain points on land) | `scripts/qdm_twb_national.py --step 1 --workers 40` | 12 min |
| Districts | test B's cells, area-weighted to 778 of 784 districts | `scripts/qdm_twb_districts.py` | 35 s |

Test A was the first, sparse sample. B superseded it and agrees with it within about 5 %, so only B is reported below (A is kept in `data/twb_national/testA_*`).

**Verdicts, excluding the Himalaya** (holdout 2003–2010 mean absolute error; 2040–60 is the 5-model mean):

| Metric | Cells (4,045): error raw → QDM | Districts (709): error raw → QDM | Districts 2040–60 raw → QDM | Verdict (both) |
|---|---|---|---|---|
| `twb_days_ge_28` | 14.0 → 0.7 d | 17.9 → 0.8 d | 37.9 → 14.2 d | MATERIAL |
| `twb_summer_mean` | 2.20 → 0.27 °C | 2.08 → 0.27 °C | 20.1 → 21.8 °C | MATERIAL |
| `twb_annual_max` | 1.81 → 0.36 °C | 1.61 → 0.33 °C | 29.0 → 27.8 °C | MATERIAL |
| `twb_annual_mean` | 1.18 → 0.13 °C | 1.07 → 0.13 °C | 21.0 → 21.4 °C | MATERIAL |
| `twb_days_ge_30` | 0.73 → 0.02 d | 0.78 → 0.02 d | 6.1 → 1.1 d | NO EFFECT / FUTURE-ONLY (ERA5 has almost no such days) |

- **Signed bias:** raw NEX is too cool in the mean (summer −1.9 °C) but too hot at the annual maximum (+1.6 °C). Its daily wet-bulb spread is too wide, which is why the ≥ 28 °C count is inflated (+13.7 d) even though the mean is cool. QDM leaves biases of −0.1, +0.25 and +0.1 d respectively.
- **The mean warming signal is preserved** (+1.29 °C raw and QDM, districts). The projected rise in `twb_days_ge_28` shrinks from +21.0 to +13.3 d/yr, a threshold effect of the lower corrected baseline.
- **By State** (`data/twb_national/testB_summary_by_state_no_himalaya.csv`), `twb_days_ge_28` is MATERIAL in 11 States. The largest raw errors are Bihar (80 → 3 d), West Bengal (73 → 3), Uttar Pradesh (50 → 3) and Assam (43 → 0.4). Thirteen States, including Maharashtra, Madhya Pradesh, Rajasthan and the south, are FUTURE-ONLY because ERA5 has almost no such days today. No State is WORSE for any metric.

**Map impact** (districts, 5-model mean, 2040–60, `data/twb_national/district_map_impact.csv`):

| Metric | Spearman raw vs QDM | Districts changing quintile | Moving ≥ 2 quintiles | Top-50 kept |
|---|---|---|---|---|
| `twb_days_ge_28` | 0.89 | 318 / 778 | 43 | 26 |
| `twb_summer_mean` | 0.85 | 363 | 67 | 27 |
| `twb_annual_max` | 0.86 | 363 | 54 | 28 |
| `twb_annual_mean` | 0.90 | 303 | 30 | 32 |

Quintiles stand in for colour bands; the dashboard's frozen ruler was not applied.

For `twb_days_ge_28` (2040–60 days/yr, raw → QDM; rank raw → QDM; ERA5 1990–2010 vs raw NEX 2003–10 in brackets):

- **Falls:** Assam and Arunachal. Biswanath 138 → 7 (#1 → #387; ERA5 0.3 vs NEX 106), Sonitpur 133 → 12, Namsai 132 → 7. The raw model is far too humid there.
- **Stays on top, at about half the value:** coastal Bengal and central Bihar. Medinipur East 124 → 65 (#21 → #1), Howrah 132 → 63 (#7 → #2), Kolkata 131 → 60 (#8 → #4), Patna 120 → 51.
- **Rises:** the Odisha and Andhra coasts, where the raw model is too dry. Puri 62 → 57 (#158 → #8; ERA5 14.5 vs NEX 6.9), Jagatsinghapur #130 → #5, Krishna #340 → #57, Guntur #368 → #73.
- **Rises sharply:** western Rajasthan. Jaisalmer 6 → 28 (#607 → #180), Barmer 7 → 20.

**Western Rajasthan** (`scripts/qdm_twb_rajasthan.py`, 79 cells in Jaisalmer and Barmer, `data/twb_national/rajasthan_check.txt`). This is the only place where corrected exceeds raw several-fold.

| 79 cells, 5 models | ERA5 1990–2010 | Raw NEX 1990–2010 | Raw NEX 2040–60 | QDM 2040–60 |
|---|---|---|---|---|
| Mean Twb (°C) | 19.1 | 16.0 | 17.5 | 20.5 |
| Days/yr with Twb 26–28 °C | 48.8 | 7.6 | 24.1 | 84.1 |
| Days/yr with Twb 27–28 °C | 12.5 | 1.9 | 7.6 | 42.2 |
| Days/yr ≥ 28 °C | 1.1 | 1.0 | 6.2 | 26.2 |
| Jun–Sep mean hurs (%) | 58.2 | 48.9–50.3 | | |

- **Mechanism:** the raw model is about 8 RH points drier than ERA5 in the monsoon and 3 °C cooler in mean wet-bulb. ERA5 has a dense band of days just under the threshold (12.5 d/yr at 27–28 °C); the raw model almost none. The model's projected warming (+1.3–1.5 °C across the distribution) lifts ERA5's band over 28 °C. The days fall in Jun–Aug, as in ERA5 today.
- **Why the holdout cannot test it:** today's ≥ 28 °C count agrees by coincidence (1.1 vs 1.0 d/yr). The disagreement is just below the threshold, so the verdict is FUTURE-ONLY.
- **Model spread is wide:** 9–58 d/yr after QDM (ACCESS-CM2 highest, MIROC6 lowest), against 0.5–21 raw.
- **Open:** the result is as good as ERA5's desert monsoon humidity, which has not been checked against stations. This is the first place a station check should go.

## 8. Threats to validity

- **ERA5 is treated as the truth.** For single-variable metrics, "MATERIAL" means QDM consistently moves NEX from its own BCSD target (GMFD) toward ERA5. That is an improvement only if ERA5 is the better reference at those sites.
  - Neither humidity nor tasmin has been checked against station observations here. An IMD station RH comparison at Kolkata/Lucknow is the cheapest independent check.
  - §7 extends this to every district. ERA5 understates humid-heat extremes in some studies, so corrected wet-bulb counts may be low. In western Rajasthan the corrected count rests entirely on ERA5's monsoon humidity (§7).
- **Small sample.** 5 sites, 5 of 22 models, 8 holdout years.
- **Points, not districts.** §3–§4 use nearest-cell points, not district means. For the wet-bulb metrics this is resolved: §7 scores every land cell and 778 districts.
- **Wet-bulb sample (§7).** 5 of 22 models; neighbouring cells share weather, so 4,625 cells are not 4,625 independent checks. The 6 districts without a scored cell (Andaman & Nicobar ×3, Lakshadweep, Diu, Chennai) and 20 coastal districts under 50 % covered rest on few or no cells.
- **Boundary file.** `districts_4326.geojson` gives 17 carved-out districts their parent's LGD code (e.g. Balotra and Barmer). §7 keys districts by row; any production join on LGD code alone would merge them.
- **Simplified definitions.** Metrics are re-implemented with a common 1990–2010 baseline, and SPI uses method-of-moments gamma per calendar month.
- **Precipitation reference and sample (§5).**
  - IMD is interpolated from rain gauges. Gauges are sparse in the north-east and the Himalaya, so the reference itself is weaker there.
  - The 74 cells sit on a 2° lattice; they are not district means.
  - Only 5 of the 22 models were used, and the holdout is 8 years. Single-cell annual maxima are noisy over 8 years, which is why the bias disappears while the per-cell error does not.
- **Temperature reference and scale (§6).**
  - IMD 1° temperature is interpolated from a few hundred stations, which is sparse in the Himalaya and the north-east.
  - The test is run at 1°, coarser than the 0.25° cells the dashboard aggregates. Production QDM would need the IMD reference interpolated to 0.25° or the correction trained at 1°, and neither was tested.
- **No physical-consistency step.** Additive QDM per variable can produce tasmin > tasmax. That was not checked or repaired here, and a production design would need it.

## 9. Open items for verification

1. **Precipitation:** done against IMD (§5). Remaining questions:
   - Confirm that the 91-day (or monthly) grouping holds with more models and at district level.
   - Decide whether a larger projected change in rainfall extremes is acceptable as a methodology change.
2. **tasmax extremes:** done against IMD (§6). `txx` and `txge30` are MATERIAL; `txge35` and `hwa` are MARGINAL, and `txge35` misses only because of the holdout design. Remaining question: retest `txge35` with a design robust to observed train→test warming, e.g. alternating years.
3. **Scale-up check:** done for the wet-bulb metrics (§7): every land cell and 778 districts, against a national ERA5 daily reference built for it. The tasmin metrics (tropical nights, `tnx`) are covered nationally by IMD (§6).
4. **Aridity index:** done against IMD (§6), NO EFFECT. The open question is the **8 % annual-rainfall inflation from multiplicative QDM.** Isolate its cause (tail extrapolation vs trace jitter vs window) before any rainfall QDM is adopted, and add an annual and seasonal total-preservation check to any rollout.
5. **Outlier models:** investigate CMCC-CM2-SR5 and TaiESM1 historical tas at Kolkata (+1.8 °C).
6. **Cold-night metrics:** `tnle10_cold_nights` and `tnle10_consecutive_cold_nights` are MATERIAL only at Bikaner/Lucknow (ERA5) and in the Himalaya (IMD). Decide whether they belong on the QDM list.
7. **Western Rajasthan wet-bulb (§7):** check ERA5 monsoon humidity against station observations at Jaisalmer and Barmer (and Kolkata/Lucknow, first bullet of §8) before publishing a 5-fold rise there. Needs a data source and a download decision.
8. **Map disclosure:** a corrected wet-bulb map reorders districts (about 40 % change quintile) and shrinks the projected rise in `twb_days_ge_28`. Both are methodology changes and need to be called out if adopted. `composite_heat_stress` weights these metrics and would shift with them.

## 10. Reproduction

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
- **Daily max/min temperature and aridity against IMD (§6):**
  - `scripts/imd_get.py 1990 2010 tmax` and `... tmin`: download.
  - `scripts/extract_temp_national.py`: NEX tasmax/tasmin/pr averaged onto IMD 1° boxes (about 2 min).
  - `scripts/qdm_temp_imd.py`: verdicts, signed bias and inverted-day counts (about 7 min, 5 workers).
  - `scripts/temp_imd_followup.py`: aridity P/PET split and Himalayan share of gain (about 20 min, serial).
  - `scripts/temp_imd_nohimalaya.py`: verdicts excluding latitude ≥ 30°, and the IMD train-vs-test shift.
  - Outputs are in `data/imd_temp/`; the box list is `data/imd_temp_boxes.csv`.
- **Wet-bulb nationally (§7):**
  - `scripts/era5_hourly_get.py 198912 201012`: hourly ERA5 download (CDS; new files only).
  - `scripts/era5_daily_ref_build.py 1990 2010`: daily reference on the NEX grid (about 17 s/year).
  - `scripts/qdm_twb_national.py`: test A (129 cells). `--step 1 --workers 40`: test B (all land cells; caches 3.4 GB in `.scratch/qdm_applicability/series_twb_national_s1/`). `--limit K --models M`: timing probe.
  - `scripts/qdm_twb_districts.py`: test B aggregated to districts, with map impact.
  - `scripts/qdm_twb_rajasthan.py`: the western Rajasthan mechanism check (about 2 min).
  - Outputs are in `data/twb_national/`; the per-cell and per-district unit rows (22 MB, 5 MB) stay in `.scratch/`.
- **Path assumptions:** the scripts were run from `.scratch/` and still read their caches from `.scratch/...` and `scratch/...`.
- **Unit-level outputs:** `data/temperature_humidity_unit_rows.csv` and `data/precipitation_unit_rows.csv`.
