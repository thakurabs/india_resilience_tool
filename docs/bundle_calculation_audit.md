# Dashboard Bundle Calculation Audit

This working note reviews dashboard bundle calculations in the order shown in
the dashboard Domain selector. Each bundle dossier follows the same structure:
bundle definition, metric-by-metric index calculation, period and ensemble
aggregation, normalization and risk interpretation, bundle score calculation,
UI presentation, and validation checks / open methodology comments.

Status:
- Sections 1 through 6 have been reviewed in chat and remediated against the
  open methodology items noted at the time of review. Section 5 (Heat Stress)
  was updated in-place to document the v2 grid-first bundle.
- Section 7 is retained as retired history after absorption into Section 8.
- Section 8 documents the active `Sector-wise - Agricultural Risk` methodology.
- Sector-wise sections 9 onward are placeholders until reviewed in chat.

## Cross-Cutting Methodology Notes

### Lens-Based Scoring (sectoral bundles)

The sectoral bundles (Sections 8 onward) score each metric through up to three
**lenses** — `absolute` (value vs peer cohort), `change` (anomaly vs the
`1990-2010` baseline), and `impact` (position within a cited physical danger
band). The framework, its scientific basis and references, the impact-band
provenance policy, and the per-metric lens reasoning are defined in
`docs/lens_scoring_methodology.md`. That document is the methodological
source-of-truth for sectoral scoring; the dossiers below record what each bundle
computes today and cross-reference it.

### Spatial Aggregation Recommendation

Recommended future method for gridded climate-to-polygon aggregation:
- Use area-weighted polygon overlap between each climate grid cell and the
  target geography polygon.
- Compute grid-cell overlap areas in an equal-area CRS, then use those
  overlap areas as spatial weights.
- For `aggregate_then_compute` semantics, first build the polygon daily time
  series as an overlap-area-weighted mean and then compute thresholds,
  spell lengths, annual maxima, seasonal totals, and other metrics.
- For `compute_then_aggregate` semantics, first compute the metric at each
  grid cell and then aggregate the resulting metric field using the same
  overlap-area weights.

Rationale:
- Current boolean-mask approaches are all-or-nothing at polygon boundaries
  and can over- or under-represent partial cells, especially for coarse
  climate grids, small polygons, and comparisons across datasets with
  different resolutions.
- Area-weighted polygon overlap is more defensible for district/block and
  hydro-unit summaries because each grid cell contributes in proportion to
  the area that actually lies inside the geography.
- Any migration from the current method should be treated as a methodology
  change and accompanied by parity diagnostics against existing processed
  metrics.

## Dashboard Bundle Order

1. Thematic - Heat Risk
2. Thematic - Drought Risk
3. Thematic - Extreme Rainfall | Flash Flood Risk
4. Thematic - Riverine Flood
5. Thematic - Heat Stress
6. Thematic - Cold Risk
7. Retired / absorbed - Thematic - Agriculture & Growing Conditions
8. Sector-wise - Agricultural Risk
9. Sector-wise - Health Risk
10. Sector-wise - Industrial Risk
11. Sector-wise - Investment / Financial Risk
12. Sector-wise - Infrastructure Risk
13. Sector-wise - Asset Risk (Thermal Power Plants)
14. Sector-wise - Asset Risk (Hydropower Plants)
15. Sector-wise - Life & Livelihood Loss Risk

## 1. Thematic - Heat Risk

### 1.1 Bundle Definition

Dashboard selector label: `Thematic - Heat Risk`

Canonical bundle name: `Heat Risk`

Composite metric slug: `composite_heat_risk`

Composite display label: `Composite Heat Risk`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

The active Heat Risk bundle uses fourteen temperature, warm-night, threshold,
percentile, and heatwave-persistence metrics. The configured weights sum to
1.0. All component metrics are currently interpreted as higher-is-worse.

| Component group | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Mean & Background Heat | `tas_annual_mean` | Annual Mean Temperature (TM Mean) | 0.0667 |
| Mean & Background Heat | `tasmax_summer_mean` | Summer Max Temperature (MAM Mean) | 0.0667 |
| Mean & Background Heat | `tas_summer_mean` | Summer Mean Temperature (TM; MAM Mean) | 0.0667 |
| Extremes | `txx_annual_max` | Annual Maximum Temperature (TXx) | 0.0833 |
| Extremes | `tn90p_warm_nights_pct` | Warm Nights (TN90p) | 0.0833 |
| Extremes | `hwa_heatwave_amplitude` | Heatwave Amplitude (peak day) | 0.0833 |
| Threshold-based Frequency | `txge30_hot_days` | Hot Days (TX >= 30 deg C) | 0.0667 |
| Threshold-based Frequency | `txge35_extreme_heat_days` | Extreme Heat Days (TX >= 35 deg C) | 0.0667 |
| Threshold-based Frequency | `tasmin_tropical_nights_gt25` | Tropical Nights (TR, TN > 25 deg C) | 0.0667 |
| Percentile Extremes | `hwfi_tmean_90p` | Heat Wave Frequency Index (HWFI, days) | 0.0750 |
| Percentile Extremes | `hwfi_events_tmean_90p` | Heat Wave Frequency (events) | 0.0750 |
| Heatwave Characteristics | `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | 0.0667 |
| Heatwave Characteristics | `tnx_annual_max` | Warmest Night | 0.0667 |
| Heatwave Characteristics | `tx90p_hot_days_pct` | Hot Days (TX90p) | 0.0667 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Composite builder: `india_resilience_tool/compute/composite_metrics.py`
- Core score helpers: `india_resilience_tool/analysis/bundle_scores.py`

### 1.2 Metric-by-Metric Index Calculation

Heat Risk uses temperature variables:
- `tas`: daily mean near-surface air temperature
- `tasmax`: daily maximum near-surface air temperature
- `tasmin`: daily minimum near-surface air temperature

Temperature unit handling:
- Source model temperature is expected in Kelvin.
- Reported degree-temperature metrics subtract `273.15` and are stored in
  degrees Celsius.
- Temperature differences, where used, are numerically identical in K and
  degrees Celsius.

Current spatial aggregation:
- For each geography polygon, the pipeline masks grid cells to the unit and
  computes a daily spatial mean over `lat` and `lon`.
- The index functions then operate on that polygon-average daily time series.
- This means localized hot pockets can be diluted before threshold and spell
  calculations are performed.

#### Annual Mean Temperature: `tas_annual_mean`

Output base column: `annual_tas_mean_C`

Unit: `deg C`

Formula:
- Compute polygon daily mean `tas`.
- Average across all days in the year.
- Convert Kelvin to degrees Celsius.

Interpretation:
- Background annual heat load.
- Higher values imply warmer overall climate conditions.

#### Summer Max Temperature: `tasmax_summer_mean`

Output base column: `summer_tasmax_mean_C`

Unit: `deg C`

Formula:
- Select March, April, and May.
- Compute polygon daily mean `tasmax`.
- Average selected daily maximum temperatures.
- Convert Kelvin to degrees Celsius.

Interpretation:
- Pre-monsoon / summer daytime heat burden.
- Higher values imply hotter daytime conditions during the main hot season.

#### Summer Mean Temperature: `tas_summer_mean`

Output base column: `summer_tas_mean_C`

Unit: `deg C`

Formula:
- Select March, April, and May.
- Compute polygon daily mean `tas`.
- Average selected daily mean temperatures.
- Convert Kelvin to degrees Celsius.

Interpretation:
- Seasonal mean thermal load, including both day and night conditions.
- Higher values imply hotter average summer conditions.

#### Annual Maximum Temperature: `txx_annual_max`

Output base column: `txx_annual_max_C`

Unit: `deg C`

Formula:
- Compute polygon daily mean `tasmax`.
- Return the maximum daily value in the year.
- Convert Kelvin to degrees Celsius.

Interpretation:
- Hottest daytime extreme in the year.
- Higher values imply greater acute extreme-heat hazard.

#### Warmest Night: `tnx_annual_max`

Output base column: `tnx_annual_max_C`

Unit: `deg C`

Formula:
- Compute polygon daily mean `tasmin`.
- Return the maximum daily minimum temperature in the year.
- Convert Kelvin to degrees Celsius.

Interpretation:
- Warmest night of the year.
- Higher values imply reduced night-time cooling and greater accumulated heat
  burden.

#### Hot Days: `txge30_hot_days`

Output base column: `days_tx_ge_30C`

Unit: `days`

Formula:
- Count days where polygon daily mean `tasmax >= 30 deg C`.

Interpretation:
- Annual frequency of hot days.
- Higher values imply more frequent daytime heat exposure.

#### Extreme Heat Days: `txge35_extreme_heat_days`

Output base column: `days_tx_ge_35C`

Unit: `days`

Formula:
- Count days where polygon daily mean `tasmax >= 35 deg C`.

Interpretation:
- Annual frequency of stronger heat days.
- Higher values imply more frequent severe daytime heat exposure.

#### Tropical Nights: `tasmin_tropical_nights_gt25`

Output base column: `tropical_nights_gt_25C`

Unit: `days`

Formula:
- Count nights where polygon daily mean `tasmin > 25 deg C`.

Current configuration:
- The bundle intentionally uses `TN > 25 deg C` for the Indian Heat Risk
  context instead of the legacy tropical-night threshold of `TN > 20 deg C`.

Interpretation:
- Frequency of warm nights.
- Higher values imply less night-time cooling and greater cumulative heat
  stress potential.

#### Hot Days Percentile: `tx90p_hot_days_pct`

Output base column: `tx90p_pct`

Unit: `%`

Formula:
- Use `tasmax`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Drop February 29 and use a 365-day no-leap day-of-year basis.
- Count the share of evaluation-year days meeting the threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.

Interpretation:
- Percentage of days that are unusually hot relative to the local historical
  calendar-day distribution.
- Higher values imply more frequent relative hot extremes.

#### Warm Nights Percentile: `tn90p_warm_nights_pct`

Output base column: `tn90p_pct`

Unit: `%`

Formula:
- Use `tasmin`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Count the share of evaluation-year nights meeting the threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.

Interpretation:
- Percentage of nights that are unusually warm relative to the local historical
  calendar-day distribution.
- Higher values imply more frequent relative night-time heat.

#### Warm Spell Duration Index: `wsdi_warm_spell_days`

Output base column: `wsdi_days`

Unit: `days`

Formula:
- Use `tasmax`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Count days that belong to warm spells of at least 6 consecutive days meeting
  the threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.
- Minimum spell length: 6 days.

Interpretation:
- Persistence of anomalously hot daytime conditions.
- Higher values imply more sustained warm-spell exposure.

#### Heat Wave Frequency Index: `hwfi_tmean_90p`

Output base column: `hwfi_days_in_spells`

Unit: `days`

Formula:
- Use `tas`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Count days belonging to heatwave spells of at least 5 consecutive days
  meeting the threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.
- Minimum spell length: 5 days.

Interpretation:
- Persistent heatwave exposure using daily mean temperature.
- Higher values imply more days inside sustained mean-temperature heatwave
  spells.

#### Heat Wave Frequency Events: `hwfi_events_tmean_90p`

Output base column: `hwfi_events_count`

Unit: `events`

Formula:
- Registry variable is currently `tasmax`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Count distinct heatwave events of at least 5 consecutive days meeting the
  threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.
- Minimum spell length: 5 days.

Interpretation:
- Number of distinct heatwave episodes.
- Higher values imply more frequent heatwave events.

Review note:
- The slug says `tmean`, but the registry currently uses `tasmax`. This should
  either be documented as intentional or corrected for name / variable
  consistency.

#### Heatwave Amplitude: `hwa_heatwave_amplitude`

Output base column: `hwa_peak_temp_C`

Unit: `deg C`

Formula:
- Use `tasmax`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Identify heatwave spells of at least 5 consecutive days meeting the threshold.
- For each spell, compute mean exceedance above the threshold.
- Select the hottest spell by mean exceedance.
- Return the peak daily maximum temperature inside that spell.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.
- Minimum spell length: 5 days.

Interpretation:
- Intensity of the worst heatwave spell.
- Higher values imply more severe peak heatwave conditions.

### 1.3 Period and Ensemble Aggregation

Per model and geography:
1. Compute annual metric values.
2. Aggregate annual values into configured periods by taking the mean across
   available years.
3. Historical baseline period: `1990-2010`.
4. Future periods: `2020-2040`, `2040-2060`, `2060-2080`.

Across models:
- The master builder computes ensemble `mean`, `std`, `median`, `p05`, `p95`,
  `n_models`, and `values_per_model`.
- Dashboard views commonly use statistic `mean`, so they read the ensemble mean
  columns.

Example selected table column:
- `annual_tas_mean_C__ssp585__2060-2080__mean`

Baseline comparison column:
- `annual_tas_mean_C__historical__1990-2010__mean`

### 1.4 Normalization and Risk Interpretation

For each component metric:
- Raw values are normalized across the active comparison frame to a 0-100
  higher-worse scale.
- Since all Heat Risk metrics are currently higher-is-worse, the lowest finite
  value receives the lowest normalized score and the highest finite value
  receives the highest normalized score.
- If all finite values are identical, all finite rows receive `50.0`.
- Missing values remain missing for that component.

Composite score:

```text
Heat Risk score =
  weighted mean(normalized component metrics)
```

Missing-data behavior:
- Metrics missing from the source frame are skipped.
- For a row with some valid component metrics, weights are renormalized across
  available metrics.
- Rows with no valid component metrics receive `NaN`.

Risk meaning:
- Higher composite score means higher relative Heat Risk within the comparison
  frame.
- The score is a relative screening index, not an absolute physical probability
  of heat damage.

### 1.5 Rankings, Percentiles, and UI Presentation

For dashboard rankings:
- `Rank (value)` ranks the active metric or composite value.
- Percentiles are calculated within the active geographic comparison scope.
- For district views, the comparison scope is the selected state.
- For block views, the comparison scope is the selected district when a district
  scope is active.

Risk classes:
- Risk class is assigned from the percentile value.
- Higher percentiles indicate higher relative risk for this bundle.

Heat Risk vs Heat Stress:
- Heat Risk is temperature-only and hazard-oriented.
- Heat Stress includes humidity and wet-bulb physiology.
- The two bundles should not be interpreted as identical even though they share
  some heat-related inputs.

### 1.6 Validation Checks and Open Methodology Comments

1. Baseline consistency:
   - Percentile and heatwave metrics currently use `(1981, 2010)`.
   - Dashboard delta baselines use `1990-2010` historical period columns.
   - This should be resolved consistently across bundles if the mismatch was
     not intentional.

2. Inclusive vs strict percentile thresholds:
   - Current config uses inclusive `>=` for TX90p, TN90p, WSDI, HWFI, and HWA.
   - ETCCDI convention is generally strict `>`.
   - The numerical difference is likely small for continuous model temperature,
     but the chosen convention should be deliberate and documented.

3. Spatial aggregation:
   - Current implementation computes polygon-average daily temperature first
     and then evaluates thresholds and spells.
   - Computing indices at grid-cell level first and then aggregating to
     polygons would better preserve localized extremes.
   - The difference may be meaningful for large or heterogeneous districts /
     blocks, especially where elevation, urbanization, or coastal gradients are
     strong.

4. Metric naming / variable consistency:
   - `hwfi_events_tmean_90p` currently uses `tasmax` even though the slug says
     `tmean`.
   - Confirm whether this is intentional. If intentional, document the label as
     tasmax-based. If not, align the variable and regenerated outputs.

5. Correlated components:
   - The bundle combines background heat, absolute thresholds, relative
     thresholds, warm nights, and heatwave persistence.
   - These are correlated but not redundant; together they form a broad
     heat-hazard screening score rather than an independent-factor causal model.

6. Threshold provenance:
   - The `TN > 25 deg C` tropical-night threshold is an India-context
     substitution from the workbook-aligned bundle.
   - This should remain documented because it differs from the classic
     tropical-night `TN > 20 deg C` definition.

## 2. Thematic - Drought Risk

### 2.1 Bundle Definition

Dashboard selector label: `Thematic - Drought Risk`

Canonical bundle name: `Drought Risk`

Composite metric slug: `composite_drought_risk`

Composite display label: `Composite Drought Risk`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

The active Drought Risk bundle uses six Standardised Precipitation Index (SPI)
metrics: three drought-event counts and three maximum drought-spell durations.
The duration gap and grid-first spatial aggregation gap are addressed in Drought
Risk v2. The configured weights sum to 1.0. All component metrics are
interpreted as higher-is-worse.

| Component group | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Seasonal Drought | `spi3_count_events_lt_minus1` | SPI3: Count of drought events with SPI < -1 | 0.08 |
| Meteorological Drought | `spi6_count_events_lt_minus1` | SPI6: Count of drought events with SPI < -1 | 0.12 |
| Long-term Drought | `spi12_count_events_lt_minus1` | SPI12: Count of drought events with SPI < -1 | 0.20 |
| Seasonal Drought | `spi3_max_spell_lt_minus1` | SPI3: Maximum drought spell length with SPI < -1 | 0.12 |
| Meteorological Drought | `spi6_max_spell_lt_minus1` | SPI6: Maximum drought spell length with SPI < -1 | 0.18 |
| Long-term Drought | `spi12_max_spell_lt_minus1` | SPI12: Maximum drought spell length with SPI < -1 | 0.30 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- SPI workflow: `tools/pipeline/compute_indices_multiprocess.py`
- Core score helpers: `india_resilience_tool/analysis/bundle_scores.py`

### 2.2 Metric-by-Metric Index Calculation

Drought Risk uses precipitation variable `pr`.

Precipitation unit handling:
- Daily precipitation is converted to `mm/day` where needed.
- Daily precipitation is aggregated to monthly precipitation totals before SPI
  calculation.

Current spatial aggregation:
- For each geography polygon, the pipeline masks grid cells to the unit and
  computes a daily spatial mean over `lat` and `lon`.
- Monthly totals and SPI are then computed from that polygon-average daily time
  series.

SPI workflow:
1. Aggregate daily precipitation to monthly totals.
2. Compute rolling accumulated precipitation over the configured SPI scale.
3. Fit a Gamma distribution per calendar month on the configured baseline
   period, with zero-precipitation handling.
4. Transform monthly accumulated precipitation to a standard normal SPI value.
5. Annualize monthly SPI values using the configured annual aggregation.

For future scenarios:
- SPI parameters are fitted from the same model's historical baseline period.
- Those fitted historical parameters are then applied to the future scenario
  monthly precipitation series.

Annual aggregation:
- The three event-count Drought Risk metrics use `annual_aggregation: count_events_lt`.
- The three duration Drought Risk metrics use `annual_aggregation: max_spell_lt`.
- The threshold is `-1.0`.
- A drought event is a contiguous run of monthly SPI values below `-1.0`.
- Event-count annual values are the number of contiguous drought-event runs in
  that year; duration annual values are the longest within-year run in months.
- A minimum valid-months check is applied; the default is 9 valid months per
  year.

#### SPI3 Drought Events: `spi3_count_events_lt_minus1`

Output base column: `spi3_events_lt_minus1`

Unit: `events`

Formula:
- Compute 3-month rolling precipitation accumulations.
- Convert to SPI3 using Gamma parameters fitted per calendar month from the
  configured baseline.
- Within each year, count contiguous monthly runs where `SPI3 < -1.0`.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Scale: 3 months.
- Threshold: `SPI < -1.0`.

Interpretation:
- Seasonal / short-term drought episode count.
- Higher values imply more frequent moderate-or-worse short-term drought
  episodes.

#### SPI6 Drought Events: `spi6_count_events_lt_minus1`

Output base column: `spi6_events_lt_minus1`

Unit: `events`

Formula:
- Compute 6-month rolling precipitation accumulations.
- Convert to SPI6 using Gamma parameters fitted per calendar month from the
  configured baseline.
- Within each year, count contiguous monthly runs where `SPI6 < -1.0`.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Scale: 6 months.
- Threshold: `SPI < -1.0`.

Interpretation:
- Medium-term meteorological drought episode count.
- Higher values imply more frequent half-year rainfall-deficit episodes.

#### SPI12 Drought Events: `spi12_count_events_lt_minus1`

Output base column: `spi12_events_lt_minus1`

Unit: `events`

Formula:
- Compute 12-month rolling precipitation accumulations.
- Convert to SPI12 using Gamma parameters fitted per calendar month from the
  configured baseline.
- Within each year, count contiguous monthly runs where `SPI12 < -1.0`.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Scale: 12 months.
- Threshold: `SPI < -1.0`.

Interpretation:
- Long-term drought episode count.
- Higher values imply more frequent persistent annual-scale rainfall-deficit
  episodes.
- This metric receives the largest bundle weight, so the composite is tilted
  toward long-term drought persistence.

### 2.3 Period and Ensemble Aggregation

Per model and geography:
1. Compute annual drought-event counts and maximum spell lengths.
2. Aggregate event-count annual values into configured periods by taking the
   mean across available years.
3. Aggregate duration annual values into configured periods by taking the
   maximum across available years.
4. Historical baseline period: `1990-2010`.
5. Future periods: `2020-2040`, `2040-2060`, `2060-2080`.

Across models:
- The master builder computes ensemble `mean`, `std`, `median`, `p05`, `p95`,
  `n_models`, and `values_per_model`.
- Dashboard views commonly use statistic `mean`, so they read the ensemble mean
  columns.

Example selected table column:
- `spi12_events_lt_minus1__ssp585__2060-2080__mean`

Baseline comparison column:
- `spi12_events_lt_minus1__historical__1990-2010__mean`

### 2.4 Normalization and Risk Interpretation

For each component metric:
- Raw component values are normalized to a 0-100 higher-worse scale using the
  same-state, same-level historical `1990-2010` anchor cohort.
- Higher event counts and longer maximum spells receive higher normalized
  scores.
- If all finite values are identical, all finite rows receive `50.0`.
- Missing values remain missing for that component.

Composite score:

```text
Drought Risk score =
  0.08 * norm(spi3_count_events_lt_minus1)
+ 0.12 * norm(spi6_count_events_lt_minus1)
+ 0.20 * norm(spi12_count_events_lt_minus1)
+ 0.12 * norm(spi3_max_spell_lt_minus1)
+ 0.18 * norm(spi6_max_spell_lt_minus1)
+ 0.30 * norm(spi12_max_spell_lt_minus1)
```

Missing-data behavior:
- Metrics missing from the source frame are skipped.
- For a row with some valid component metrics, weights are renormalized across
  available metrics.
- Rows with fewer than 4 anchored component metrics receive `NaN`.

Risk meaning:
- Higher composite score means higher relative meteorological Drought Risk
  within the comparison frame.
- The score is a relative screening index, not an absolute probability of
  drought damage.

### 2.5 Rankings, Percentiles, and UI Presentation

For dashboard rankings:
- `Rank (value)` ranks the active metric or composite value.
- Percentiles are calculated within the active geographic comparison scope.
- For district views, the comparison scope is the selected state.
- For block views, the comparison scope is the selected district when a district
  scope is active.

Risk classes:
- Risk class is assigned from the percentile value.
- Higher percentiles indicate higher relative risk for this bundle.

### 2.6 Validation Checks and Open Methodology Comments

1. Baseline consistency:
   - SPI metrics currently use `(1981, 2010)` for calibration.
   - Dashboard delta baselines use `1990-2010` historical period columns.
   - This should be resolved consistently across bundles if the mismatch was
     not intentional.

2. Event count does not encode duration or severity:
   - A one-month SPI drought event and a six-month SPI drought event both count
     as one event.
   - The bundle does not currently encode event duration, accumulated SPI
     deficit, minimum SPI value, or drought-area persistence.

3. Long-term drought emphasis:
   - SPI12 receives 50% of the composite weight.
   - This makes the bundle more sensitive to persistent annual-scale deficits
     than to short seasonal failures.
   - This is defensible, but should remain explicit in the methodology.

4. Spatial aggregation:
   - Current implementation computes polygon-average precipitation first and
     then calculates SPI.
   - Computing SPI at grid-cell level first and then aggregating to polygons
     would better preserve localized rainfall deficits.

5. Meteorological drought only:
   - SPI uses precipitation only.
   - It does not account for temperature-driven evaporative demand.
   - The bundle should be interpreted as meteorological drought risk, not full
     agricultural, ecological, or hydrological drought risk.

6. Future enhancement:
   - SPEI or PET-aware drought indicators could be added if the goal is to
     capture warming-driven atmospheric demand.

## 3. Thematic - Extreme Rainfall | Flash Flood Risk

### 3.1 Bundle Definition

Dashboard selector label: `Thematic - Extreme Rainfall | Flash Flood Risk`

Canonical bundle name: `Extreme Rainfall | Flash Flood Risk`

Composite metric slug: `composite_flood_extreme_rainfall_risk`

Composite display label: `Composite Flash Flood Risk`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

The active bundle uses six precipitation / wet-spell metrics. The configured
weights sum to 1.0.

| Component group | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Peak Intensity | `pr_max_1day_precip` | Maximum 1-day Precipitation (Rx1day) | 0.125 |
| Peak Intensity | `pr_max_5day_precip` | Maximum 5-day Precipitation (Rx5day) | 0.125 |
| Heavy Rain Frequency | `r20mm_very_heavy_precip_days` | Very Heavy Precipitation Days (R20mm) | 0.250 |
| Very Wet Contribution | `r95p_very_wet_precip` | Very Wet Day Precipitation (R95p) | 0.125 |
| Very Wet Contribution | `r95ptot_contribution_pct` | Very Wet Day Contribution (R95pTOT) | 0.125 |
| Wet-spell Persistence | `cwd_consecutive_wet_days` | Consecutive Wet Days (CWD) | 0.250 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Composite builder: `india_resilience_tool/compute/composite_metrics.py`
- Core score helpers: `india_resilience_tool/analysis/bundle_scores.py`

### 3.2 Metric-by-Metric Index Calculation

All six component metrics use precipitation variable `pr`.

Precipitation unit handling:
- If source units are `kg m-2 s-1`, values are multiplied by `86400` to become
  `mm/day`.
- If source values are already in daily depth units, they are used directly.

Current spatial aggregation:
- Admin district/block outputs use the Extreme Rainfall v2 grid-first path:
  annual cell-level indices are computed first, then area-weighted to polygons
  with a 50% retained finite-cell weight floor.
- Hydro basin/sub-basin outputs remain on the legacy path: the pipeline masks
  grid cells to the unit, computes a daily spatial mean over `lat` and `lon`,
  and then operates on that polygon-average daily time series.

#### Rx1day: `pr_max_1day_precip`

Output base column: `max_1day_precip_mm`

Unit: `mm`

Formula:
- Convert daily precipitation to `mm/day`.
- Compute the polygon daily mean.
- Return the maximum daily value in the year.

Interpretation:
- Annual wettest single-day rainfall amount.
- Higher means greater short-duration rainfall intensity and higher flash-flood
  pressure.

#### Rx5day: `pr_max_5day_precip`

Output base column: `max_5day_precip_mm`

Unit: `mm`

Formula:
- Convert precipitation to `mm/day`.
- Compute polygon daily mean.
- Compute rolling 5-day precipitation totals.
- Return the maximum 5-day total in the year.

Interpretation:
- Largest accumulated rainfall over any consecutive 5-day window.
- Higher values imply stronger multi-day saturation / flood pressure.

#### R20mm: `r20mm_very_heavy_precip_days`

Output base column: `r20mm_days`

Unit: `days`

Formula:
- Count days where polygon daily mean precipitation is greater than `20 mm/day`.

Note:
- Admin v2 and the shared registry now use inclusive `>= 20 mm/day`, matching
  the ETCCDI R20mm label. The separate `rain_gt_2p5mm` metric remains strict
  `> 2.5 mm/day`.

Interpretation:
- Annual number of very heavy precipitation days.
- Higher values imply more frequent intense rainfall days.

#### R95p: `r95p_very_wet_precip`

Output base column: `r95p_mm`

Unit: `mm`

Formula:
- Consider wet days with precipitation `>= 1 mm/day`.
- For admin v2, compute the 95th percentile threshold from `1990-2010`
  historical wet days using linear quantile.
- Sum precipitation on days strictly greater than that threshold.

Current configuration:
- The registry keeps the legacy `(1981, 2010)` / nearest / inclusive settings
  for hydro compatibility.
- The admin v2 compute module owns the `1990-2010` / linear / strict semantics.

Interpretation:
- Total rainfall contributed by very wet days.
- Higher values imply more rainfall concentrated in extreme events.

#### R95pTOT: `r95ptot_contribution_pct`

Output base column: `r95ptot_pct`

Unit: `%`

Formula:
- Compute the same baseline wet-day 95th percentile threshold as R95p.
- Sum precipitation on days strictly greater than the threshold.
- Divide by total wet-day precipitation.
- Multiply by 100.

Current configuration:
- The registry keeps the legacy `(1981, 2010)` / nearest / inclusive settings
  for hydro compatibility.
- The admin v2 compute module owns the `1990-2010` / linear / strict semantics.

Interpretation:
- Share of wet-day rainfall coming from very wet days.
- Higher values imply rainfall is more concentrated in extremes.

#### CWD: `cwd_consecutive_wet_days`

Output base column: `cwd_max_spell_len`

Unit: `days`

Formula:
- Count the maximum run length of consecutive days with precipitation
  `>= 1 mm/day`.

Interpretation:
- Longest wet spell in the year.
- Higher values imply longer persistent wet periods and greater saturation
  pressure.

### 3.3 Period and Ensemble Aggregation

Per model and geography:
1. Compute annual metric values.
2. Aggregate annual values into configured periods by taking the mean across
   available years.
3. Historical baseline period: `1990-2010`.
4. Future periods: `2020-2040`, `2040-2060`, `2060-2080`.

Across models:
- The master builder computes ensemble `mean`, `std`, `median`, `p05`, `p95`,
  `n_models`, and `values_per_model`.
- The current dashboard screenshots use statistic `mean`, so they read the
  ensemble mean columns.

Example selected table column:
- `max_1day_precip_mm__ssp585__2060-2080__mean`

Baseline comparison column:
- `max_1day_precip_mm__historical__1990-2010__mean`

### 3.4 Normalization and Risk Interpretation

For individual metric deep-dive views:
- Rankings use the selected raw metric value.
- Percentiles are computed over the current comparison set.
- For block rankings, the table is state-filtered, so Telangana block
  percentiles compare against Telangana blocks.
- Risk class is derived from percentile:
  - `>=80`: Very High
  - `>=60`: High
  - `>=40`: Medium
  - `>=20`: Low
  - otherwise: Very Low

For this bundle, all six component metrics are treated as higher-is-worse.

### 3.5 Bundle Score Calculation

For composite bundle scoring:
1. Resolve each component metric column for the selected scenario and period.
2. Normalize each metric across the available geography frame to a `0-100`
   scale.
3. Higher values map to higher risk scores for all six metrics in this bundle.
4. Apply configured weights.
5. If a row has missing component metrics, weights are renormalized across
   available metrics.
6. If all component metrics are missing, the bundle score is `NaN`.

Formula:

```text
bundle_score =
  sum(normalized_metric_i * weight_i for available metrics)
  / sum(weight_i for available metrics)
```

Important distinction:
- Composite score is min-max normalized metric aggregation.
- Deep-dive metric rankings and portfolio heatmaps use metric percentiles, not
  the composite formula unless the selected metric itself is the composite.

### 3.6 UI Presentation

Deep Dive metric view:
- `Index value` = selected raw metric/statistic value.
- `Delta vs baseline` = selected value minus historical baseline.
- `%Delta vs baseline` = `100 * Delta / baseline`.
- `Rank (value)` = rank by selected raw value, where 1 is highest risk for
  these rainfall metrics.
- `Top 20 biggest increases` sorts rows by largest `Delta vs baseline`, even
  though the displayed rank remains `Rank (value)`.

Portfolio heatmap:
- Shows `Percentile`.
- Cell colors are risk classes derived from percentile.
- For block portfolio views, percentiles are computed against all blocks in the
  same state, not only against selected portfolio blocks.

### 3.7 Validation Checks and Open Methodology Comments

Recommended validation checks:
1. Pick one Telangana block, for example `BANSWADA`, and verify:
   - `Index value`
   - baseline value
   - `Delta vs baseline`
   - `%Delta vs baseline`
   - percentile and risk class
2. Reproduce the top-10 list for:
   - `All`, sorted by `Rank (value)`
   - `Top 20 biggest increases`, sorted by `rank_delta`
3. For the composite:
   - Pull all six component values for one block.
   - Recompute min-max normalized scores.
   - Apply weights and compare to `composite_flood_extreme_rainfall_risk`.

Methodology status:
- Addressed for admin district/block: grid-first annual index calculation is
  now used for all six active metrics in this bundle.
- Addressed globally for R20mm: the threshold is inclusive `>=20 mm/day`.
- Addressed for admin R95p/R95pTOT: v2 uses `1990-2010`, linear quantile, wet
  days `>=1 mm/day`, and strict `>` threshold exceedance.
- Preserved for hydro basin/sub-basin: legacy polygon-average-first
  methodology and registry percentile settings remain until a hydro migration
  is explicitly scoped.
- Flood depth is not part of this bundle. Riverine/JRC flood depth is handled
  by the separate Riverine Flood bundle.

## 4. Thematic - Riverine Flood

### 4.1 Bundle Definition

Dashboard selector label: `Thematic - Riverine Flood`

Canonical bundle name: `Riverine Flood`

Composite metric slug: `composite_flood_jrc_depth`

Composite display label: `Composite Riverine Flood Risk`

Supported levels:
- Admin district
- Admin block

Supported state:
- Telangana only

Supported scenario / period:
- Scenario: `snapshot`
- Period: `Current`
- Statistic: `mean`

This is an external hazard snapshot bundle, not a climate-model SSP projection.

The bundle has one scored component and two inline attributes.

| Role | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Scored component | `jrc_flood_depth_index_rp100` | Flood Severity Index (RP-100) | 1.0 |
| Attribute | `jrc_flood_depth_rp100` | RP-100 Flood Depth | 0.0 |
| Attribute | `jrc_flood_extent_rp100` | RP-100 Flood Extent | 0.0 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Builder: `tools/geodata/build_jrc_flood_depth_admin_masters.py`

### 4.2 Metric-by-Metric Index Calculation

Inputs:
- `RP10_depth.tif`
- `RP50_depth.tif`
- `RP100_depth.tif`
- `RP500_depth.tif`

The Riverine Flood bundle itself uses RP-100 derived outputs:
- RP-100 depth
- RP-100 extent
- RP-100 severity index

Raster contract:
- Single-band numeric rasters.
- Shared CRS, shape, transform, nodata, and mask semantics across return
  periods.
- Units are attested as meters via `--assume-units m`.
- Telangana district and block boundaries are used.

#### RP-100 Flood Depth: `jrc_flood_depth_rp100`

Output base column: `jrc_flood_depth_rp100`

Unit: `m`

Block formula:
- Clip RP-100 depth raster to the block polygon.
- Use `all_touched=False`.
- Treat valid zero cells as dry / non-flooded support.
- Identify positive valid depth cells.
- If there is valid raster support but no positive depth, publish `0.0`.
- Otherwise publish the linear p95 of positive flooded-cell depths.

Interpretation:
- A high-end flooded-cell depth statistic for the block.
- It does not average all cells; it summarizes the deeper part of flooded cells
  only.

District formula:
- Build district values bottom-up from child blocks.
- Use each child block's flooded-cell p95 depth.
- Weight by each block's flooded supported area.
- If valid support exists but no flooded child blocks exist, publish `0.0`.
- If no valid support exists, publish `NaN`.

Interpretation:
- Flooded-area-weighted mean of block-level p95 depths.
- This avoids district-wide direct p95 behavior overwhelming smaller local flood
  pockets.

#### RP-100 Flood Extent: `jrc_flood_extent_rp100`

Output base column: `jrc_flood_extent_rp100`

Storage unit:
- Fraction from `0` to `1`.

Display unit:
- Percent via display scale `100`.

Block formula:
- Compute positive valid cell support inside the block.
- Convert this to flooded supported area.
- Divide by total block polygon area.
- If there is no valid raster support, publish `NaN`.

District formula:
- Sum flooded supported area across child blocks.
- Divide by total district polygon area.
- If there is no valid raster support, publish `NaN`.

Interpretation:
- Share of the full polygon area covered by positive modeled RP-100 flood depth.
- A value of `0.34` is displayed as `34%`.

#### Flood Severity Index (RP-100): `jrc_flood_depth_index_rp100`

Output base column: `jrc_flood_depth_index_rp100`

Unit:
- Severity class `1-5`.

Class labels:
- `1`: VeryLow
- `2`: Low
- `3`: Moderate
- `4`: High
- `5`: Extreme

Block formula:
1. Classify RP-100 depth.
2. Classify RP-100 extent.
3. Look up final severity from a fixed 5x5 extent-by-depth expert-rule matrix.

Depth classes:

| Depth class | RP-100 depth |
|---:|---|
| 1 | `<= 0.2 m` |
| 2 | `> 0.2 m` and `<= 0.5 m` |
| 3 | `> 0.5 m` and `<= 1.0 m` |
| 4 | `> 1.0 m` and `<= 2.5 m` |
| 5 | `> 2.5 m` |

Extent classes:

| Extent class | RP-100 flooded extent |
|---:|---|
| 1 | `<= 0.01` of polygon area (`<= 1%`) |
| 2 | `> 0.01` and `<= 0.05` (`> 1%` to `<= 5%`) |
| 3 | `> 0.05` and `<= 0.15` (`> 5%` to `<= 15%`) |
| 4 | `> 0.15` and `<= 0.25` (`> 15%` to `<= 25%`) |
| 5 | `> 0.25` (`> 25%`) |

Expert-rule severity lookup:

| Extent class \ Depth class | 1 | 2 | 3 | 4 | 5 |
|---|---:|---:|---:|---:|---:|
| 1 | 1 | 2 | 2 | 3 | 4 |
| 2 | 2 | 2 | 3 | 4 | 4 |
| 3 | 2 | 3 | 4 | 4 | 5 |
| 4 | 3 | 4 | 4 | 5 | 5 |
| 5 | 4 | 5 | 5 | 5 | 5 |

How to read the matrix:
- Rows are extent classes.
- Columns are depth classes.
- The cell value is the final severity class.
- Example: depth class `3` and extent class `3` gives severity `4`.
- Example: depth class `5` and extent class `1` gives severity `4`, meaning
  very deep but highly localized flooding is serious but not maximum severity.
- Example: depth class `1` and extent class `5` gives severity `4`, meaning
  shallow flooding over a large area is also serious.
- Example: depth class `5` and extent class `5` gives severity `5`, meaning
  deep flooding over a large area is extreme.

The matrix is not estimated from the raster values. The raster values determine
the depth and extent classes; the matrix encodes an expert-rule judgement for
combining those two classes into one severity score.

District formula:
- Compute block severity classes first.
- Aggregate to district level using flooded-area weighting.
- Only blocks with `flooded_supported_area_km2 > 0` contribute.
- Districts with valid raster coverage but no flooded child blocks receive
  `1.0`.
- Districts with all child block severity missing receive `NaN`.

Interpretation:
- A combined depth x extent severity score for RP-100 inundation.
- Block values are integer classes.
- District values may be non-integer weighted means, for example `3.7 / 5`.

### 4.3 Period and Ensemble Aggregation

This bundle does not use climate-model period or ensemble aggregation.

There are:
- No SSP scenarios.
- No yearly time series.
- No model ensemble statistics.
- No historical baseline delta.

All output columns use the static snapshot schema:

```text
<metric_slug>__snapshot__Current__mean
```

Examples:
- `jrc_flood_depth_index_rp100__snapshot__Current__mean`
- `jrc_flood_depth_rp100__snapshot__Current__mean`
- `jrc_flood_extent_rp100__snapshot__Current__mean`

### 4.4 Normalization and Risk Interpretation

For direct metric views:
- Higher depth is worse.
- Higher extent is worse.
- Higher severity index is worse.
- Baseline comparison is disabled.
- Scenario comparison is disabled.
- Yearly trend is disabled.

For risk class:
- `jrc_flood_depth_index_rp100` is already a classed severity metric.
- UI formatting displays exact integer classes as labels, for example
  `High (4)` or `Extreme (5)`.
- Non-integer district aggregates display as numeric score over 5, for example
  `3.8 / 5`.

For map/ranking interpretation:
- Rank 1 means highest severity / depth / extent.
- For severity, percentile-style risk labels are less meaningful than the 1-5
  severity classes. Tooltip logic has special handling to avoid redundant
  percentile/risk-class display for JRC severity.

### 4.5 Bundle Score Calculation

The bundle score is effectively based only on the severity index:

```text
composite_flood_jrc_depth ~= normalized/scored jrc_flood_depth_index_rp100
```

Configured bundle weights:
- `jrc_flood_depth_index_rp100`: scored weight `1.0`
- `jrc_flood_depth_rp100`: attribute only
- `jrc_flood_extent_rp100`: attribute only

Important distinction:
- Depth and extent are not separately weighted in the bundle score because they
  are already embedded in the severity index.
- They are carried as explanatory attributes for interpretation.

### 4.6 UI Presentation

Dashboard behavior:
- Appears as `Thematic - Riverine Flood`.
- Uses `snapshot / Current / mean`.
- Available for Telangana district and block views.
- It is conceptually separate from `Extreme Rainfall | Flash Flood Risk`.

Map and ranking:
- `Flood Severity Index (RP-100)` displays class labels for integer values.
- `RP-100 Flood Depth` displays meters.
- `RP-100 Flood Extent` is stored as fraction and displayed as percent.
- No baseline delta columns should be shown for this bundle.
- No SSP scenario comparison should be shown.

Reference overlay:
- The RP-100 flood-depth raster overlay is display-only.
- It uses exported PNG/metadata artifacts, not raw raster reads at dashboard
  runtime.
- Overlay colors represent RP-100 depth bins and should be interpreted visually,
  not as the primary tabular calculation source.

### 4.7 Validation Checks and Open Methodology Comments

Recommended validation checks:
1. Pick one block and verify:
   - RP-100 p95 positive depth.
   - RP-100 flooded extent fraction.
   - depth class.
   - extent class.
   - severity matrix lookup.
2. Pick one district and verify:
   - child block flooded areas.
   - child block severity classes.
   - flooded-area-weighted district severity.
   - district RP-100 depth weighted from child block p95 values.
   - district RP-100 extent as total flooded supported area / district polygon
     area.
3. Verify formatting:
   - severity integer class displays as label plus score.
   - extent fraction displays as percent.
4. Verify disabled controls:
   - no baseline comparison.
   - no yearly trend.
   - no scenario comparison.

Open methodology comments:
- The severity matrix is a fixed expert-rule matrix. We should document its
  provenance or approval basis clearly: who approved the depth/extent thresholds
  and why those cut points are appropriate for Telangana.
- Block depth uses p95 of positive flooded cells, not mean depth. This is
  conservative for depth severity and should be explicitly justified.
- District depth is not direct raster p95 over district pixels. It is a
  flooded-area-weighted rollup of child block p95 values. This preserves local
  block hazard signal, but users may expect direct district zonal statistics;
  documentation should state the bottom-up method.
- Extent denominator is full polygon area, not raster-supported area. This is
  good for exposure-style interpretation, but low raster support cases need QA
  review.
- The district severity can be non-integer because it is a weighted mean of
  block severity classes. This is analytically useful, but it slightly differs
  from a pure ordinal class. The UI should continue showing non-integer values
  as numeric `/ 5`, not as a categorical label.
- This bundle is an external current/snapshot hazard layer. It should not be
  compared directly with SSP climate bundles as if it represented the same
  scenario semantics.

## 5. Thematic - Heat Stress

### 5.1 Bundle Definition

Dashboard selector label: `Thematic - Heat Stress`

Canonical bundle name: `Heat Stress`

Composite metric slug: `composite_heat_stress`

Composite display label: `Composite Heat Stress`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

Heat Stress v2 drops WBD from the scored bundle because local diagnostics showed WBD-only metrics can identify humid/low-depression days that are not high Twb heat-stress days. WBD remains a registered legacy diagnostic metric but is no longer part of Heat Stress scoring or domain membership.

All retained Heat Stress v2 inputs are computed grid-first and then area-weighted to district/block polygons. Twb is computed per grid cell/day before annual summaries or threshold counts.

The bundle keeps the `composite_heat_stress` slug, so this is an in-place methodology update rather than a new dashboard product.

The active weighted composite uses 8 scored metrics.

| Component group | Group weight | Metric slug | Metric label | Code weight |
|---|---:|---|---|---:|
| Background humid heat | 0.20 | `twb_annual_mean` | Wet-Bulb Temperature (Annual Mean) | `0.20 / 2.0` |
| Background humid heat | 0.20 | `twb_summer_mean` | Wet-Bulb Temperature (Summer Mean; MAM Mean) | `0.20 / 2.0` |
| Extreme / threshold humid heat | 0.40 | `twb_annual_max` | Wet-Bulb Temperature (Annual Max) | `0.40 / 3.0` |
| Extreme / threshold humid heat | 0.40 | `twb_days_ge_28` | Heat Stress Days (Twb >= 28C) | `0.40 / 3.0` |
| Extreme / threshold humid heat | 0.40 | `twb_days_ge_30` | Wet-Bulb Days (Twb >= 30C) | `0.40 / 3.0` |
| Night-time recovery stress | 0.20 | `tasmin_tropical_nights_gt28` | Tropical Nights (TR, TN > 28C) | `0.20 / 2.0` |
| Night-time recovery stress | 0.20 | `tn90p_warm_nights_pct` | Warm Nights (TN90p) | `0.20 / 2.0` |
| Persistence | 0.20 | `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | `0.20 / 1.0` |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Heat Stress v2 grid-first compute: `india_resilience_tool/compute/heat_stress_gridfirst.py`
- Shared Heat Risk v2 percentile compute for TN90p/WSDI: `india_resilience_tool/compute/heat_risk_gridfirst.py`
- Pipeline dispatch and public CSV writer: `tools/pipeline/compute_indices_multiprocess.py`
- Composite scoring: `india_resilience_tool/analysis/bundle_scores.py`

### 5.2 Metric-by-Metric Index Calculation

The bundle mixes:
- wet-bulb temperature metrics derived from `tas` and `hurs`;
- night-time minimum-temperature threshold and percentile metrics;
- warm-spell persistence metrics from `tasmax`.

Grid-first spatial aggregation:
- For the six Heat Stress-only metrics, the pipeline computes the annual cell field first and then applies polygon area-weighted means.
- `twb_annual_mean`, `twb_summer_mean`, `twb_annual_max`, `twb_days_ge_28`, and `twb_days_ge_30` compute Stull Twb per grid cell/day before annual summaries or threshold counts.
- `tasmin_tropical_nights_gt28` counts daily grid-cell `tasmin > 28C`; exactly `28.0C` is not counted, Feb 29 is dropped, and NaN days are non-events.
- Day-count metrics remain fractional after polygon aggregation and are not rounded.
- `twb_annual_max` is the area-weighted mean of per-cell annual maxima.

Private cache artifacts:
- Shared spatial weights: `processed/_internal/spatial_weights/`
- Heat Stress annual cell fields: `processed/_internal/heat_stress/grid_metrics/<slug>/<model>/<grid_id>/<scenario>/<year>.nc`
- Cache sidecars include method version, slug, model, scenario, year, grid id, sorted params, input file hashes, value column, and baseline metadata when relevant.
- Cache reads ignore artifacts whose `method_version` differs from `heat-stress-v2-gridfirst-1`.

Public CSV metadata for the six Heat Stress-only metrics:
- `method_version = heat-stress-v2-gridfirst-1`
- `aggregation_method = gridfirst_area_weighted_mean`

### 5.3 Baselines and Screening Caveats

TN90p and WSDI reuse Heat Risk v2 percentile-baseline machinery unchanged: baseline `1990-2010`, linear quantile, 5-day window, strict `>`, and WSDI minimum spell length `6`. Heat Stress does not duplicate that implementation.

Stull Twb is an approximation. It is suitable for climate-screening and relative prioritization, but it is not a full psychrometric wet-bulb calculation and should not be interpreted as an occupational WBGT standard.

WBD, WBGT, and simplified WBGT metrics remain registered for backward compatibility and diagnostics, but they are not Heat Stress domain members and are not scored in `composite_heat_stress` v2.

### 5.4 Normalization and Risk Interpretation

All retained Heat Stress v2 bundle components are treated as higher-is-worse:
- Higher Twb is worse.
- More Twb threshold days are worse.
- More hot nights are worse.
- More warm-spell days are worse.

For individual metric deep-dive views:
- `Index value` is the selected raw metric/statistic.
- `Delta vs baseline` is selected value minus historical baseline where available.
- Percentile/risk class is computed over the active comparison set.
- For block views, percentiles are state-scoped unless otherwise filtered by the runtime path.

Risk class from percentile:
- `>=80`: Very High
- `>=60`: High
- `>=40`: Medium
- `>=20`: Low
- otherwise: Very Low

### 5.5 Bundle Score Calculation

For composite scoring:
1. Resolve each of the 8 retained component metric columns for the selected scenario and period.
2. Normalize each component across the available geography frame to a `0-100` higher-is-worse scale.
3. Apply configured weights.
4. Renormalize weights row-wise across available component metrics.
5. Set score to `NaN` if no component is available.

Formula:

```text
bundle_score =
  sum(normalized_metric_i * weight_i for available metrics)
  / sum(weight_i for available metrics)
```

Important distinction:
- The composite is a relative normalized score.
- It is not a physical heat-stress unit.
- Because components are correlated, especially the Twb metrics, the score should be interpreted as a screening index rather than an independent-effects risk model.

### 5.6 UI Presentation

Deep Dive metric view:
- Shows raw metric values for the selected component.
- Baseline deltas are meaningful for climate metrics with historical columns.
- Scenario comparison is available for SSP climate metrics.
- Trends may be available where yearly outputs exist.

Portfolio heatmap:
- Displays percentiles, not raw values.
- Colors are risk-class bins derived from percentile.
- Percentiles are metric-specific, so a `90` for Twb annual max and a `90` for tropical nights mean "high relative position" within each metric, not equivalent physical magnitude.

Glance / bundle view:
- Displays persisted `composite_heat_stress` bundle scores.
- Component drivers may show the metrics contributing most strongly to the score.

### 5.7 Validation Checklist

Recommended validation checks:
1. Recompute the six Heat Stress-only grid-first metrics for a pilot state and inspect yearly rows for `method_version` and `aggregation_method`.
2. Verify one district/block Twb metric from source grids: per-cell daily `tas`, per-cell daily `hurs`, per-cell Stull Twb, annual cell summary, area-weighted polygon value.
3. Verify `tasmin_tropical_nights_gt28`: strict `> 28C`, Feb 29 dropped, NaNs as non-events, fractional polygon result allowed after area weighting.
4. Verify TN90p/WSDI are routed through Heat Risk v2 and use the documented `1990-2010` strict-exceedance baseline settings.
5. Pull all 8 component columns for one geography, min-max normalize each component, apply weights, and compare to `composite_heat_stress`.
6. Run `tools.diagnostics.heat_stress_gridfirst_parity` on pilot legacy/new extracts and review per-metric deltas, rank-shift summary, and top movers.

## 6. Thematic - Cold Risk

### 6.1 Bundle Definition

Dashboard selector label: `Thematic - Cold Risk`

Canonical bundle name: `Cold Risk`

Composite metric slug: `composite_cold_risk`

Composite display label: `Composite Cold Risk`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

The active weighted composite uses 11 metrics.

| Component group | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Background Cold | `tas_winter_mean` | Winter Mean Temperature (TM; DJF Mean) | 0.100 |
| Background Cold | `tasmin_winter_mean` | Winter Min Temperature (DJF Mean) | 0.100 |
| Absolute Extremes | `tnn_annual_min` | Annual Minimum of Daily Minimum Temperature (TNn) | 0.100 |
| Absolute Extremes | `tasmin_winter_min` | Winter Minimum Tmin (DJF Min TN) | 0.100 |
| Threshold-based Cold Days | `tnle10_cold_nights` | Cold Nights (TN <= 10C) | 0.0833 |
| Threshold-based Cold Days | `tnle5_severe_cold_nights` | Severe Cold Nights (TN <= 5C) | 0.0833 |
| Threshold-based Cold Days | `txle15_cold_days` | Cold Days (TX <= 15C) | 0.0833 |
| Relative Cold | `tx10p_cool_days_pct` | Cool Days (TX10p) | 0.075 |
| Relative Cold | `tn10p_cool_nights_pct` | Cool Nights (TN10p) | 0.075 |
| Cold Spell Characteristics | `csdi_cold_spell_days` | Cold Spell Duration Index (CSDI) | 0.100 |
| Cold Spell Characteristics | `tnle10_consecutive_cold_nights` | Consecutive Cold Nights (TN <= 10C) | 0.100 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Temperature compute functions: `tools/pipeline/compute_indices_multiprocess.py`
- Composite scoring: `india_resilience_tool/analysis/bundle_scores.py`

### 6.2 Metric-by-Metric Index Calculation

The bundle mixes:
- absolute winter temperature level metrics;
- absolute annual/winter minimum temperature metrics;
- threshold cold-night/day counts;
- relative percentile cold metrics;
- cold-spell persistence metrics.

Current spatial aggregation:
- For each polygon, the pipeline masks grid cells to the unit and computes a
  daily spatial mean over `lat` and `lon`.
- Temperature index functions operate on that polygon-average daily time series.
- Values are converted from Kelvin to Celsius for output where applicable.

#### `tas_winter_mean`

Unit: `C`

Formula:
- Use daily mean temperature `tas`.
- Select DJF months: December, January, February.
- Compute daily polygon-average temperature.
- Return seasonal mean in Celsius.

Interpretation:
- Background winter mean thermal condition.
- For cold risk, lower values should imply higher risk.

#### `tasmin_winter_mean`

Unit: `C`

Formula:
- Use daily minimum temperature `tasmin`.
- Select DJF months.
- Compute daily polygon-average Tmin.
- Return seasonal mean Tmin in Celsius.

Interpretation:
- Background winter night-time cold condition.
- Lower values should imply higher risk.

#### `tnn_annual_min`

Unit: `C`

Formula:
- Use daily minimum temperature `tasmin`.
- Compute daily polygon-average Tmin.
- Return annual minimum daily Tmin in Celsius.

Interpretation:
- Coldest night of the year.
- Lower values should imply higher risk.

#### `tasmin_winter_min`

Unit: `C`

Formula:
- Use daily minimum temperature `tasmin`.
- Select DJF months.
- Compute daily polygon-average Tmin.
- Return winter minimum daily Tmin in Celsius.

Interpretation:
- Coldest winter night.
- Lower values should imply higher risk.

#### `tnle10_cold_nights`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Count days/nights where polygon-average Tmin is `<= 10C`.

Interpretation:
- Number of cold nights relevant to plains and central India.
- Higher count means higher cold exposure.

#### `tnle5_severe_cold_nights`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Count days/nights where polygon-average Tmin is `<= 5C`.

Interpretation:
- Number of more severe cold nights.
- Higher count means higher cold exposure.

#### `txle15_cold_days`

Unit: `days`

Formula:
- Use daily maximum temperature `tasmax`.
- Count days where polygon-average Tmax is `<= 15C`.

Interpretation:
- Number of cold daytime conditions.
- Higher count means higher cold exposure.

#### `tx10p_cool_days_pct`

Unit: `%`

Formula:
- Use daily maximum temperature `tasmax`.
- Compute ETCCDI-style day-of-year 10th percentile thresholds from baseline
  years.
- Baseline currently configured as `(1981, 2010)`.
- Uses a 5-day moving window and nearest quantile.
- Count/evaluate days below the threshold as a percentage.

Interpretation:
- Relative frequency of unusually cool days compared with historical local
  climate.
- Higher percentage means more relative cold exposure.

#### `tn10p_cool_nights_pct`

Unit: `%`

Formula:
- Use daily minimum temperature `tasmin`.
- Compute ETCCDI-style day-of-year 10th percentile thresholds from baseline
  years.
- Baseline currently configured as `(1981, 2010)`.
- Uses a 5-day moving window and nearest quantile.
- Count/evaluate nights below the threshold as a percentage.

Interpretation:
- Relative frequency of unusually cool nights compared with historical local
  climate.
- Higher percentage means more relative night-time cold exposure.

#### `csdi_cold_spell_days`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Compute ETCCDI-style day-of-year 10th percentile thresholds from baseline
  years.
- Baseline currently configured as `(1981, 2010)`.
- Uses a 5-day moving window, nearest quantile, and minimum spell length of 6
  days.
- Count days contributing to qualifying cold spells.

Interpretation:
- Persistence of relative cold spells.
- Higher values mean longer/more frequent cold-spell exposure.

#### `tnle10_consecutive_cold_nights`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Flag days/nights where Tmin is `<= 10C`.
- Return the longest consecutive run.

Interpretation:
- Persistence of absolute cold-night conditions.
- Higher values mean longer cold-night spells.

### 6.3 Period and Ensemble Aggregation

Per model and geography:
1. Compute annual metric values.
2. Aggregate annual values into configured periods by taking the mean across
   available years.
3. Historical baseline period: `1990-2010`.
4. Future periods: `2020-2040`, `2040-2060`, `2060-2080`.

Across models:
- The master builder computes ensemble `mean`, `std`, `median`, `p05`, `p95`,
  `n_models`, and `values_per_model`.
- Dashboard statistic `mean` reads the ensemble mean period column.

Example selected columns:
- `winter_tas_mean_C__ssp585__2060-2080__mean`
- `days_tn_le_10C__ssp585__2060-2080__mean`
- `csdi_days__ssp585__2060-2080__mean`

Baseline comparison columns use historical `1990-2010` where available.

### 6.4 Normalization and Risk Interpretation

Cold Risk is direction-sensitive.

Expected direction:
- For temperature magnitude metrics, lower is worse:
  - `tas_winter_mean`
  - `tasmin_winter_mean`
  - `tnn_annual_min`
  - `tasmin_winter_min`
- For count/spell/percentile metrics, higher is worse:
  - `tnle10_cold_nights`
  - `tnle5_severe_cold_nights`
  - `txle15_cold_days`
  - `tx10p_cool_days_pct`
  - `tn10p_cool_nights_pct`
  - `csdi_cold_spell_days`
  - `tnle10_consecutive_cold_nights`

High-priority implementation review:
- The registry default is `rank_higher_is_worse=True`.
- The cold temperature magnitude metrics listed above must explicitly set
  `rank_higher_is_worse=False`.
- If they do not, composite normalization can treat warmer winter temperatures
  as higher cold risk, which would invert part of the Cold Risk bundle.
- This affects not only the composite score, but also rankings, percentiles,
  risk classes, driver selection, and portfolio heatmaps for those metrics.

For individual metric deep-dive views:
- `Index value` is the selected raw metric/statistic.
- `Delta vs baseline` is selected value minus historical baseline.
- For cold temperature magnitude metrics, positive deltas usually mean warming /
  reduced cold exposure, not increased cold risk. UI interpretation needs
  direction-aware language.

Risk class from percentile:
- The generic percentile class system assumes higher percentile is worse after
  direction handling.
- This is only scientifically correct if `rank_higher_is_worse` is set
  correctly for each metric.

### 6.5 Bundle Score Calculation

For composite scoring:
1. Resolve each of the 11 component metric columns for the selected scenario and
   period.
2. Normalize each component across the available geography frame to a `0-100`
   higher-is-worse risk scale.
3. Apply configured weights.
4. Renormalize weights row-wise across available component metrics.
5. Set score to `NaN` if no component is available.

Formula:

```text
bundle_score =
  sum(normalized_metric_i * weight_i for available metrics)
  / sum(weight_i for available metrics)
```

Direction handling is essential:
- For lower-is-worse temperature metrics, normalization should invert the scale.
- For higher-is-worse count/spell metrics, normalization should not invert the
  scale.

### 6.6 UI Presentation

Deep Dive metric view:
- Raw metric values should be interpreted carefully:
  - lower temperatures can indicate higher cold hazard;
  - higher cold-day/cold-night counts indicate higher cold hazard.
- Baseline deltas should be interpreted directionally:
  - warming deltas for Tmin/Tas winter metrics can reduce cold risk;
  - increasing cold-day counts can increase cold risk.
- Scenario comparison is available for climate metrics.
- Trends may be available where yearly outputs exist.

Portfolio heatmap:
- Displays percentiles, not raw values.
- Percentiles must be direction-aware to be meaningful for lower-is-worse
  temperature metrics.
- A high percentile should always mean higher cold risk; this depends on correct
  registry direction flags.

Glance / bundle view:
- Displays persisted `composite_cold_risk` bundle scores.
- Component drivers should be checked after direction correction, because
  inverted metrics could produce misleading drivers.

### 6.7 Validation Checks and Open Methodology Comments

Recommended validation checks:
1. Pick one district/block and verify one lower-is-worse metric:
   - `tasmin_winter_min` raw value.
   - expected cold-risk direction.
   - normalized score direction.
   - ranking and percentile direction.
2. Verify threshold count metrics:
   - `tnle10_cold_nights`
   - `tnle5_severe_cold_nights`
   - `txle15_cold_days`
3. Verify percentile cold metrics:
   - `tx10p_cool_days_pct`
   - `tn10p_cool_nights_pct`
   - `csdi_cold_spell_days`
4. Verify composite:
   - pull all 11 component columns for one geography;
   - normalize with correct direction for each component;
   - apply weights;
   - compare to `composite_cold_risk`.

Resolved methodology items (previously open):
- Directionality (resolved, 34efcea): the four cold-magnitude metrics
  `tas_winter_mean`, `tasmin_winter_mean`, `tnn_annual_min`, and
  `tasmin_winter_min` now carry `rank_higher_is_worse=False` in the registry,
  with a regression test guarding the configuration.
- DJF cross-year season (resolved, 30b889b + d307c6f): seasonal cold metrics
  now use a meteorological-winter window spanning Dec of the prior year plus
  Jan-Feb of the current winter year (`season_djf_cross_year`), with a
  historical-December fallback for SSP first years. Applies to
  `tas_winter_mean`, `tasmin_winter_mean`, and `tasmin_winter_min`.
- Baseline harmonization (resolved, 4f1c9e8): TX10p, TN10p, and CSDI now use
  the `1990-2010` baseline, matching dashboard historical deltas and removing
  the prior 1981-2010 vs 1990-2010 mismatch.
- ETCCDI strict-< convention (resolved, 4f1c9e8): TX10p, TN10p, and CSDI are
  configured with `exceed_ge=False`, aligning the percentile workflow with the
  strict `<` convention documented in the registry, with boundary tests.
- Grid-first spatial aggregation (resolved, d307c6f): cold-risk indices are
  computed per grid cell and then zonally aggregated, preserving localized
  cold pockets in heterogeneous terrain rather than averaging daily fields to
  polygons first.
- Registry descriptions and UI/tooltip wording (resolved, 4c1c7c3 + d307c6f):
  CSDI description and DJF tooltips now reflect the strict-< convention and
  cross-year DJF semantics actually computed.
- Threshold provenance (resolved, 4c1c7c3): registry descriptions document
  `TN <= 10C`, `TN <= 5C`, and `TX <= 15C` with workbook alignment and
  India/Telangana applicability notes.

Remaining open items:
- None at this time. Re-open if downstream review surfaces new concerns.

## 7. Retired / absorbed - Thematic Agriculture & Growing Conditions

This former thematic bundle has been retired as an active dashboard option and
absorbed into the active `Sector-wise - Agricultural Risk` dossier in Section 8.
The legacy canonical name `Agriculture & Growing Conditions` is retained only as
a domain alias to `Agricultural Risk`, and the retired processed slug
`composite_agriculture_growing_conditions` is pruned only when the explicit
`--prune-retired` flag is used.

The historical notes below remain for audit traceability only. They no longer
describe an active visible Glance bundle.

### 7.1 Retired Bundle Definition

Retired dashboard selector label: `Thematic - Agriculture & Growing Conditions`

Retired canonical bundle name: `Agriculture & Growing Conditions`

Retired composite metric slug: `composite_agriculture_growing_conditions`

Retired composite display label: `Composite Agriculture & Growing Conditions`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

The retired Agriculture & Growing Conditions bundle used nine metrics spanning
phenology, heat burden, cold burden, rainfall / drought, and temperature
variability. The configured weights sum to 1.0.

This bundle is direction-sensitive because some metrics are hazards where
higher is worse, while others are resource / suitability metrics where lower is
worse.

| Component group | Metric slug | Metric label | Weight | Expected direction |
|---|---|---|---:|---|
| Growing Season / Phenology | `gsl_growing_season` | Growing Season Length (GSL) | 0.2000 | Lower is worse |
| Heat Burden | `tasmax_summer_mean` | Summer Max Temperature (MAM Mean) | 0.0667 | Higher is worse |
| Heat Burden | `txge35_extreme_heat_days` | Extreme Heat Days (TX >= 35 deg C) | 0.0667 | Higher is worse |
| Heat Burden | `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | 0.0667 | Higher is worse |
| Cold Burden | `tasmin_winter_mean` | Winter Min Temperature (DJF Mean) | 0.1000 | Lower should be worse |
| Cold Burden | `tnle10_cold_nights` | Cold Nights (TN <= 10 deg C) | 0.1000 | Higher is worse |
| Water Availability / Drought | `spi3_drought_index` | Standardised Precipitation Index 3-month (SPI3) | 0.1000 | Lower is worse |
| Water Availability / Drought | `prcptot_annual_total` | Total Wet-Day Precipitation (PRCPTOT) | 0.1000 | Lower is worse |
| Temperature Variability / Suitability | `dtr_daily_temp_range` | Daily Temperature Range (DTR) | 0.2000 | Higher currently treated as worse |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Temperature and precipitation compute functions:
  `tools/pipeline/compute_indices_multiprocess.py`
- Core score helpers: `india_resilience_tool/analysis/bundle_scores.py`

### 7.2 Conceptual Interpretation

This bundle is best interpreted as an agricultural growing-condition stress
screening index.

It is not a crop-yield model. It does not currently account for:
- crop-specific calendars;
- irrigation status;
- soil type or soil moisture storage;
- crop variety or phenological stage;
- pest / disease pressure;
- management practices;
- flood or waterlogging impacts beyond the rainfall proxy selected here.

The bundle combines:
- length of the potential growing season;
- high-temperature burden;
- cold-night burden;
- short-term meteorological drought;
- annual rainfall availability;
- diurnal temperature variability.

Because these drivers can affect different crops differently, the composite is
most appropriate for relative screening across geographies, not for estimating
absolute crop loss.

### 7.3 Metric-by-Metric Index Calculation

Retired Agriculture & Growing Conditions used:
- `tas`: daily mean near-surface air temperature;
- `tasmax`: daily maximum near-surface air temperature;
- `tasmin`: daily minimum near-surface air temperature;
- `pr`: precipitation.

Temperature unit handling:
- Source temperature is expected in Kelvin.
- Temperature outputs subtract `273.15` where stored as degrees Celsius.
- DTR is a temperature difference, so the numeric difference is identical in K
  and degrees Celsius.

Precipitation unit handling:
- Precipitation is converted to `mm/day` where needed.
- PRCPTOT sums wet-day precipitation.
- SPI uses monthly precipitation totals derived from daily precipitation.

Current spatial aggregation:
- For each geography polygon, the pipeline masks grid cells to the unit and
  computes a daily spatial mean over `lat` and `lon`.
- The index functions then operate on that polygon-average daily time series.
- This can smooth localized heat, cold, and rainfall-deficit signals before
  threshold, spell, and SPI calculations.

#### Growing Season Length: `gsl_growing_season`

Output base column: `gsl_days`

Unit: `days`

Formula:
- Use daily mean temperature `tas`.
- Drop February 29 to keep a 365-day no-leap basis.
- Start of season: first occurrence of at least 6 consecutive days with
  polygon-average `TM > 5 deg C`.
- End of season: first occurrence after July 1 of at least 6 consecutive days
  with polygon-average `TM < 5 deg C`.
- Growing season length is the number of days between start and end.
- If no end is found, the season continues to the end of the year.
- If no start is found, output is `0`.

Current direction:
- Registry explicitly sets `rank_higher_is_worse=False`.

Interpretation:
- Shorter potential growing season increases agricultural suitability stress.
- In many warm Indian locations, this metric may saturate at long values and
  may contribute less differentiation than heat, drought, or DTR metrics.

#### Summer Max Temperature: `tasmax_summer_mean`

Output base column: `summer_tasmax_mean_C`

Unit: `deg C`

Formula:
- Select March, April, and May.
- Compute polygon daily mean `tasmax`.
- Average selected daily maximum temperatures.
- Convert Kelvin to degrees Celsius.

Current direction:
- Higher is worse.

Interpretation:
- Pre-monsoon / summer daytime heat burden relevant to crop heat stress.
- Higher values imply stronger high-temperature stress potential.

#### Extreme Heat Days: `txge35_extreme_heat_days`

Output base column: `days_tx_ge_35C`

Unit: `days`

Formula:
- Use daily maximum temperature `tasmax`.
- Count days where polygon-average `TX >= 35 deg C`.

Current direction:
- Higher is worse.

Interpretation:
- Annual frequency of high-temperature days that can affect flowering, grain
  filling, labor conditions, livestock, and irrigation demand.

#### Warm Spell Duration Index: `wsdi_warm_spell_days`

Output base column: `wsdi_days`

Unit: `days`

Formula:
- Use daily maximum temperature `tasmax`.
- Compute day-of-year 90th percentile thresholds from the configured baseline
  period using a 5-day moving window.
- Drop February 29 and use a 365-day no-leap basis.
- Count days that belong to warm spells of at least 6 consecutive days meeting
  the threshold.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Quantile method: `nearest`.
- Threshold comparison: inclusive `>=` because `exceed_ge=True`.
- Minimum spell length: 6 days.

Current direction:
- Higher is worse.

Interpretation:
- Persistent anomalous heat exposure.
- More warm-spell days imply longer periods of crop heat stress and potentially
  greater irrigation demand.

#### Winter Minimum Temperature Mean: `tasmin_winter_mean`

Output base column: `winter_tasmin_mean_C`

Unit: `deg C`

Formula:
- Use daily minimum temperature `tasmin`.
- Select months `[12, 1, 2]`.
- Compute polygon daily mean Tmin.
- Average selected daily minimum temperatures.
- Convert Kelvin to degrees Celsius.

Expected direction:
- If the metric is intended as cold burden, lower should be worse.

Implementation review note:
- The inspected registry entry does not explicitly show
  `rank_higher_is_worse=False`.
- Since the registry default is higher-is-worse, this metric may currently be
  inverted in composite scoring, rankings, percentiles, drivers, and heatmaps.
- This is the same directionality risk identified in the Cold Risk bundle.

Interpretation:
- Background winter night-time cold condition.
- Lower values imply greater cold-night stress potential for sensitive crops,
  horticulture, livestock, and cold-wave exposure.

#### Cold Nights: `tnle10_cold_nights`

Output base column: `days_tn_le_10C`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Count days/nights where polygon-average `TN <= 10 deg C`.

Current direction:
- Higher is worse.

Interpretation:
- Frequency of cold nights relevant to plains and central India.
- Higher values imply more cold exposure days.

#### SPI3 Drought Index: `spi3_drought_index`

Output base column: `spi3_index`

Unit: `index`

Formula:
- Convert daily precipitation to monthly precipitation totals.
- Compute 3-month rolling precipitation accumulations.
- Fit Gamma parameters per calendar month using the configured baseline period.
- Transform monthly accumulated precipitation to a standard normal SPI3 value.
- Annualize using the configured mean SPI aggregation for this index metric.

Current configuration:
- Baseline years: `(1981, 2010)`.
- Scale: 3 months.

Current direction:
- Registry explicitly sets `rank_higher_is_worse=False`.

Interpretation:
- Short-term meteorological wetness / dryness condition.
- Lower SPI3 values indicate drier conditions and higher agricultural drought
  stress.
- Positive SPI3 values indicate wetter-than-baseline conditions.

#### Total Wet-Day Precipitation: `prcptot_annual_total`

Output base column: `prcptot_mm`

Unit: `mm`

Formula:
- Convert daily precipitation to `mm/day`.
- Identify wet days where polygon-average precipitation is `>= 1 mm/day`.
- Sum precipitation on those wet days over the year.

Current direction:
- Registry explicitly sets `rank_higher_is_worse=False`.

Interpretation:
- Annual wet-day rainfall availability proxy.
- Lower totals imply greater water-availability stress in this bundle.

Review note:
- This metric is interpreted as water-availability stress only.
- Very high rainfall can also harm agriculture through flooding,
  waterlogging, disease, or delayed operations, but those effects are not
  captured by the current lower-is-worse PRCPTOT direction.

#### Daily Temperature Range: `dtr_daily_temp_range`

Output base column: `dtr_mean_C`

Unit: `deg C`

Formula:
- Use daily maximum temperature `tasmax` and daily minimum temperature `tasmin`.
- Compute polygon daily mean `tasmax`.
- Compute polygon daily mean `tasmin`.
- Align the two daily time series.
- Compute `tasmax - tasmin` for each day.
- Average daily differences across the year.

Current direction:
- The inspected registry entry does not explicitly set
  `rank_higher_is_worse=False`, so the default higher-is-worse direction likely
  applies.

Interpretation:
- Mean diurnal temperature range.
- Larger DTR can be interpreted as thermal variability / suitability stress,
  but agronomic response is crop- and season-specific.

Review note:
- The higher-is-worse direction is defensible if the bundle treats high daily
  thermal amplitude as a stressor.
- The rationale should be documented because DTR is not universally harmful in
  all cropping systems.

### 7.4 Period and Ensemble Aggregation

Per model and geography:
1. Compute annual metric values.
2. Aggregate annual values into configured periods by taking the mean across
   available years.
3. Historical baseline period: `1990-2010`.
4. Future periods: `2020-2040`, `2040-2060`, `2060-2080`.

Across models:
- The master builder computes ensemble `mean`, `std`, `median`, `p05`, `p95`,
  `n_models`, and `values_per_model`.
- Dashboard views commonly use statistic `mean`, so they read ensemble mean
  columns.

Example selected columns:
- `gsl_days__ssp585__2060-2080__mean`
- `days_tx_ge_35C__ssp585__2060-2080__mean`
- `spi3_index__ssp585__2060-2080__mean`
- `prcptot_mm__ssp585__2060-2080__mean`

Baseline comparison columns use historical `1990-2010` where available.

### 7.5 Normalization and Risk Interpretation

Retired Agriculture & Growing Conditions was direction-sensitive.

Expected direction handling:
- Lower is worse:
  - `gsl_growing_season`
  - `tasmin_winter_mean` if used as cold burden
  - `spi3_drought_index`
  - `prcptot_annual_total`
- Higher is worse:
  - `tasmax_summer_mean`
  - `txge35_extreme_heat_days`
  - `wsdi_warm_spell_days`
  - `tnle10_cold_nights`
  - `dtr_daily_temp_range` if high DTR is accepted as a stressor

Known explicit registry direction flags:
- `gsl_growing_season`: `rank_higher_is_worse=False`
- `spi3_drought_index`: `rank_higher_is_worse=False`
- `prcptot_annual_total`: `rank_higher_is_worse=False`

High-priority directionality review:
- `tasmin_winter_mean` should likely set `rank_higher_is_worse=False`.
- Without that flag, warmer winter Tmin could be scored as higher agricultural
  cold burden, which would invert that component.
- `dtr_daily_temp_range` should be explicitly accepted as higher-is-worse or
  assigned a different direction / transformation.

Composite score:

```text
Retired Agriculture & Growing Conditions score =
  0.20   * norm(gsl_growing_season)
+ 0.0667 * norm(tasmax_summer_mean)
+ 0.0667 * norm(txge35_extreme_heat_days)
+ 0.0667 * norm(wsdi_warm_spell_days)
+ 0.10   * norm(tasmin_winter_mean)
+ 0.10   * norm(tnle10_cold_nights)
+ 0.10   * norm(spi3_drought_index)
+ 0.10   * norm(prcptot_annual_total)
+ 0.20   * norm(dtr_daily_temp_range)
```

Missing-data behavior:
- Metrics missing from the source frame are skipped.
- For a row with some valid component metrics, weights are renormalized across
  available metrics.
- Rows with no valid component metrics receive `NaN`.

Risk meaning:
- Higher composite score means more adverse relative growing conditions within
  the comparison frame.
- The score is a screening index for combined growing-condition stress, not a
  crop-specific loss estimate.

### 7.6 Rankings, Percentiles, and UI Presentation

For dashboard rankings:
- `Rank (value)` ranks the active metric or composite value after applying the
  metric's configured risk direction.
- Percentiles are calculated within the active geographic comparison scope.
- For district views, the comparison scope is the selected state.
- For block views, the comparison scope is the selected district when a district
  scope is active.

For direct metric interpretation:
- `gsl_growing_season`: lower raw values are worse.
- `tasmax_summer_mean`, `txge35_extreme_heat_days`, `wsdi_warm_spell_days`,
  and `tnle10_cold_nights`: higher raw values are worse.
- `spi3_drought_index`: lower raw values are worse.
- `prcptot_annual_total`: lower raw values are worse in the current
  water-availability interpretation.
- `dtr_daily_temp_range`: currently higher raw values are likely worse, pending
  methodology confirmation.
- `tasmin_winter_mean`: lower raw values should likely be worse, pending
  directionality correction.

Portfolio heatmap:
- Displays percentiles.
- Percentiles must be direction-aware to be meaningful for mixed-direction
  metrics.
- A high percentile should always mean higher growing-condition stress.

### 7.7 Validation Checks and Open Methodology Comments

Recommended validation checks:
1. Directionality spot checks:
   - Confirm `gsl_growing_season` ranks shorter seasons as higher risk.
   - Confirm `spi3_drought_index` ranks lower SPI as higher risk.
   - Confirm `prcptot_annual_total` ranks lower rainfall as higher risk.
   - Confirm `tasmin_winter_mean` is corrected or explicitly documented.
   - Confirm the selected DTR direction.
2. Metric recomputation for one block:
   - `gsl_days`
   - `summer_tasmax_mean_C`
   - `days_tx_ge_35C`
   - `wsdi_days`
   - `winter_tasmin_mean_C`
   - `days_tn_le_10C`
   - `spi3_index`
   - `prcptot_mm`
   - `dtr_mean_C`
3. Composite recomputation:
   - Pull all nine component columns for one geography.
   - Normalize each component with the correct direction.
   - Apply configured weights.
   - Compare to retired `composite_agriculture_growing_conditions` artifacts.
4. UI checks:
   - Verify direct metric rank and percentile direction for lower-is-worse
     metrics.
   - Verify portfolio heatmap percentiles for mixed-direction metrics.
   - Verify top-change views are interpreted carefully for lower-is-worse
     metrics.

Open methodology comments:
- `tasmin_winter_mean` directionality is the highest-priority issue for this
  bundle. If retained as a cold-burden metric, it should likely be flagged
  `rank_higher_is_worse=False`.
- DJF handling recurs. The current seasonal function selects months
  `[12, 1, 2]` within each processed calendar year. This may not represent
  cross-year meteorological DJF. If retained, documentation should describe it
  as a calendar-year winter-month subset rather than a true DJF season.
- Baseline consistency recurs. WSDI and SPI3 use `(1981, 2010)`, while
  dashboard historical deltas use `1990-2010`. This should be consolidated if
  the mismatch was not intentional.
- Spatial aggregation should be reviewed. Grid-cell index calculation followed
  by zonal aggregation may better preserve local agricultural stress signals,
  especially for heat thresholds, cold nights, SPI, PRCPTOT, and GSL in large
  or heterogeneous polygons.
- The PRCPTOT direction assumes rainfall shortage is the relevant agricultural
  stress. It does not capture excess rainfall, flood, waterlogging, or crop
  disease risk from very wet conditions.
- DTR direction and transformation should be confirmed. A simple higher-is-worse
  DTR treatment may be reasonable as a thermal variability stress proxy, but
  crop response is nonlinear and crop-specific.
- The bundle should be labeled and documented as a broad screening index for
  growing-condition stress, not a replacement for crop-specific agronomic or
  yield modeling.

## 8. Sector-wise - Agricultural Risk

### 8.1 Bundle Definition

Dashboard selector label: `Sector-wise - Agricultural Risk`

Canonical bundle name: `Agricultural Risk`

Composite metric slug: `composite_agricultural_risk`

Composite display label: `Composite Agricultural Risk`

Supported levels:
- Admin district
- Admin block

Supported scenarios:
- `ssp245`
- `ssp585`

Agricultural Risk is now the survivor agriculture bundle. It is a pure
current/future absolute-pressure composite: all retained rules are scored from
the selected scenario and period only, using robust p10-p90 normalization within
the state / level / scenario / period comparison frame. Higher scores always
mean higher agricultural climate pressure.

| Rule slug | Label | Source metric | Weight | Direction |
|---|---|---|---:|---|
| `txx_peak_crop_heat` | Peak crop heat | `txx_annual_max` | 0.10 | Higher is worse |
| `txge35_damaging_heat_days` | Damaging heat days | `txge35_extreme_heat_days` | 0.10 | Higher is worse |
| `wsdi_persistent_heat` | Persistent heat | `wsdi_warm_spell_days` | 0.10 | Higher is worse |
| `spi3_drought_episodes` | Drought episodes | `spi3_count_events_lt_minus1` | 0.15 | Higher is worse |
| `spi3_longest_drought_spell` | Longest drought spell | `spi3_max_spell_lt_minus1` | 0.15 | Higher is worse |
| `rx5day_heavy_rainfall` | 5-day heavy rainfall | `pr_max_5day_precip` | 0.20 | Higher is worse |
| `tnle10_cold_nights` | Cold nights | `tnle10_cold_nights` | 0.20 | Higher is worse |

### 8.2 Methodology Change

This is an intentional methodology change from the old proposal agriculture
score. The earlier within-rule absolute/change/impact blends are removed for
Agricultural Risk. The old TXx 40-45 deg C soft impact band and the R95p
relative-change signal are retired from this score.

The following metrics are intentionally dropped from the agriculture composite:
- `dtr_daily_temp_range`: retired because the direction and crop response are
  nonlinear and require crop-specific justification.
- `prcptot_annual_total`: retired because lower-is-worse water-availability
  framing does not capture harmful excess rainfall in this sector score.
- `spi3_drought_index`: replaced by event-count and max-spell drought pressure
  metrics so all retained rules are higher-is-worse.
- `gsl_growing_season`: retired because broad growing-season length is less
  directly comparable across irrigated/crop-calendar contexts.
- `tasmin_winter_mean`: retired because cold burden is represented by explicit
  cold-night threshold frequency.
- `pr_max_1day_precip`: retired from this sector score in favor of 5-day heavy
  rainfall pressure.
- `r95p_very_wet_precip`: retired with the old relative-change rule.
- `r95ptot_contribution_pct`: not retained because the final score uses direct
  5-day heavy rainfall pressure rather than wet-precipitation contribution.

### 8.3 Scoring

In lens terms (see `docs/lens_scoring_methodology.md`), every Agricultural Risk
rule uses the **absolute lens only** (`absolute_weight = 1.0`,
`change_weight = 0.0`, `impact_weight = 0.0`) — a pure projected-level pressure
score with no change or impact lens. Each rule is normalized independently:

```text
rule_score = clip((value - p10) / (p90 - p10), 0, 1) * 100
```

If p10 and p90 are equal, valid rows receive the flat score used by the proposal
builder. Missing source values produce `NaN` for that rule.

Composite score:

```text
Agricultural Risk =
  0.10 * norm(txx_annual_max)
+ 0.10 * norm(txge35_extreme_heat_days)
+ 0.10 * norm(wsdi_warm_spell_days)
+ 0.15 * norm(spi3_count_events_lt_minus1)
+ 0.15 * norm(spi3_max_spell_lt_minus1)
+ 0.20 * norm(pr_max_5day_precip)
+ 0.20 * norm(tnle10_cold_nights)
```

The builder persists both `available_rule_count` and
`available_rule_weight_fraction`. The minimum coverage gate is 0.70 by weight:
missing only `rx5day_heavy_rainfall` leaves 0.80 and the score is allowed;
missing both rainfall and cold leaves 0.60 and the score is set to `NaN`.

### 8.4 Implementation Notes

Implementation references:
- Rule catalog: `india_resilience_tool/config/proposal_bundles.py`
- Proposal builder: `india_resilience_tool/compute/proposal_bundles.py`
- Dashboard catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Registry resolver: `india_resilience_tool/config/metrics_registry.py`
- Grid-first heat support: `india_resilience_tool/compute/heat_risk_gridfirst.py`
- Pipeline dispatch: `tools/pipeline/compute_indices_multiprocess.py`

Agriculture source metrics must use grid-first district/block masters. Current
grid-first paths cover TXx, TX >= 35 days, WSDI, SPI3 event/spell metrics,
5-day rainfall, and TN <= 10 cold nights. No agriculture-only aggregation method
is introduced.

### 8.5 Validation Checks

Recommended validation checks:
1. Confirm `get_metrics_for_bundle("Agricultural Risk", admin/district)` returns
   `composite_agricultural_risk` plus the seven source metrics in rule order.
2. Recompute one district and one block manually from retained rule scores and
   explicit weights.
3. Verify that increasing any retained source metric increases its rule score
   because all retained rules are higher-is-worse.
4. Confirm the 0.70 available-weight gate by dropping only rainfall, then both
   rainfall and cold.
5. Compare old and new agriculture outputs and record major rank movers as a
   methodology-change audit, not as a regression failure.

Validation status as of 2026-05-22:
- Focused proposal/registry/dashboard tests passed:
  `python -m pytest -q tests/test_proposal_bundle_config.py tests/test_proposal_bundle_builder.py tests/test_metrics_registry.py tests/test_dashboard_bundles.py`
  returned `63 passed`.
- Telangana pilot artifacts were refreshed with:
  `python -m tools.pipeline.build_proposal_bundles --bundle composite_agricultural_risk --level admin --state Telangana --overwrite`
- The proposal-bundle builder uses `--level admin` for district + block output;
  it does not accept the climate-index pipeline's `--level both` selector.
- The refresh wrote both admin masters:
  `processed/composite_agricultural_risk/Telangana/master_metrics_by_district.csv`
  and
  `processed/composite_agricultural_risk/Telangana/master_metrics_by_block.csv`.
- District sanity check: 33 rows, all 33 rows had non-null
  `composite_agricultural_risk__ssp245__2020-2040__mean`, all rows had 7 of 7
  rules available, and `available_rule_weight_fraction` was 1.0.
- Block sanity check: 620 rows, all 620 rows had non-null
  `composite_agricultural_risk__ssp245__2020-2040__mean`, all rows had 7 of 7
  rules available, and `available_rule_weight_fraction` was 1.0.
- Build-time geopandas geographic-CRS area warnings were observed from admin
  geometry loaders; they did not block output generation.

## 9. Sector-wise - Health Risk

The Health Risk lens methodology is documented in
`docs/lens_scoring_methodology.md`, Section 6 (the worked template for the
sectoral lens dossiers). It covers the per-metric lens decisions (TXx, WSDI,
TNx, Rx1day, CWD), the impact-band provenance (IMD heatwave 40-45 deg C; IMD
very-heavy-to-extremely-heavy rainfall 115.6-204.5 mm; expert night-time band),
and the bundle assembly notes. The remaining bundle-dossier subsections (9.1
onward, in the structure used by Sections 1-8) are pending review in chat.

## 10. Sector-wise - Industrial Risk

Pending review.

## 11. Sector-wise - Investment / Financial Risk

Pending review.

## 12. Sector-wise - Infrastructure Risk

Pending review.

## 13. Sector-wise - Asset Risk (Thermal Power Plants)

Pending review.

## 14. Sector-wise - Asset Risk (Hydropower Plants)

Pending review.

## 15. Sector-wise - Life & Livelihood Loss Risk

Pending review.
