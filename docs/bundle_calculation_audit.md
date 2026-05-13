# Dashboard Bundle Calculation Audit

This working note reviews dashboard bundle calculations in the order shown in
the dashboard Domain selector. Each bundle dossier follows the same structure:
bundle definition, metric-by-metric index calculation, period and ensemble
aggregation, normalization and risk interpretation, bundle score calculation,
UI presentation, and validation checks / open methodology comments.

Status:
- Sections 1, 2, and 4 onward are placeholders until reviewed in chat.
- Section 3 has been reviewed and should be treated as the first draft dossier.

## Dashboard Bundle Order

1. Thematic - Heat Risk
2. Thematic - Drought Risk
3. Thematic - Extreme Rainfall | Flash Flood Risk
4. Thematic - Riverine Flood
5. Thematic - Heat Stress
6. Thematic - Cold Risk
7. Thematic - Agriculture & Growing Conditions
8. Sector-wise - Agricultural Risk
9. Sector-wise - Health Risk
10. Sector-wise - Industrial Risk
11. Sector-wise - Investment / Financial Risk
12. Sector-wise - Infrastructure Risk
13. Sector-wise - Asset Risk (Thermal Power Plants)
14. Sector-wise - Asset Risk (Hydropower Plants)
15. Sector-wise - Life & Livelihood Loss Risk

## 1. Thematic - Heat Risk

Pending review.

## 2. Thematic - Drought Risk

Pending review.

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
- For each geography polygon, the pipeline masks grid cells to the unit and
  computes a daily spatial mean over `lat` and `lon`.
- The index functions then operate on that polygon-average daily time series.

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
- The registry description says `>= 20mm`, while the current count helper uses
  `> thresh_mm`. This is a methodology consistency item to resolve.

Interpretation:
- Annual number of very heavy precipitation days.
- Higher values imply more frequent intense rainfall days.

#### R95p: `r95p_very_wet_precip`

Output base column: `r95p_mm`

Unit: `mm`

Formula:
- Consider wet days with precipitation `>= 1 mm/day`.
- Compute the 95th percentile threshold from baseline wet days using nearest
  quantile.
- Sum precipitation on days meeting or exceeding that threshold.

Current configuration:
- The registry config uses `baseline_years: (1981, 2010)`.
- The dashboard historical delta baseline uses `1990-2010` historical period
  columns.

Interpretation:
- Total rainfall contributed by very wet days.
- Higher values imply more rainfall concentrated in extreme events.

#### R95pTOT: `r95ptot_contribution_pct`

Output base column: `r95ptot_pct`

Unit: `%`

Formula:
- Compute the same baseline wet-day 95th percentile threshold as R95p.
- Sum precipitation on days meeting or exceeding the threshold.
- Divide by total wet-day precipitation.
- Multiply by 100.

Current configuration:
- The registry config uses `baseline_years: (1981, 2010)`.
- The dashboard historical delta baseline uses `1990-2010` historical period
  columns.

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

Open methodology comments:
- Spatial aggregation method should be reviewed. The current method computes
  indices from polygon-average daily rainfall. For extreme rainfall hazard,
  grid-cell index calculation followed by zonal aggregation may better preserve
  local extremes. The current method answers "what is the extreme of the
  polygon-average rainfall time series"; the grid-first method answers "what is
  the average local extreme inside this polygon." These are not equivalent, and
  the current method will often smooth local extremes.
- Expected impact of switching to grid-first aggregation is likely modest for
  small blocks near climate-grid resolution, but could be material for larger
  districts, heterogeneous terrain, and convective rainfall. Rx1day/Rx5day may
  differ by several percent to tens of percent in some places; threshold and
  spell metrics such as R20mm, R95p/R95pTOT, and CWD may be more sensitive near
  thresholds or where wet-spell continuity varies spatially.
- R20mm should be made inclusive (`>=20mm`) or the label should be changed.
  Best practice for ETCCDI R20mm is generally `>=20mm`. Expected numerical
  impact is probably small for floating model data after polygon averaging, but
  implementation, label, and documentation should agree.
- R95p/R95pTOT baseline years should be aligned to the project historical
  baseline (`1990-2010`) unless a different climatological reference period is
  intentionally adopted and documented. The current `(1981, 2010)` registry
  setting appears unintentional relative to the dashboard delta baseline.
- Flood-depth wording should be updated. Riverine Flood / JRC is now a separate
  dashboard bundle, while this bundle covers climate-model extreme rainfall /
  flash-flood pressure only. Any note saying "Flood Depth Index remains
  deferred" should be replaced with clearer wording such as: "Riverine
  flood-depth metrics are handled in the separate Riverine Flood bundle; this
  bundle covers climate-model extreme rainfall / flash-flood pressure only."

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

The active weighted composite uses 11 metrics.

| Component group | Metric slug | Metric label | Weight |
|---|---|---|---:|
| Background Heat Stress | `twb_annual_mean` | Wet-Bulb Temperature (Annual Mean) | 0.100 |
| Background Heat Stress | `twb_summer_mean` | Wet-Bulb Temperature (Summer Mean; MAM Mean) | 0.100 |
| Extreme Heat Stress | `twb_annual_max` | Wet-Bulb Temperature (Annual Max) | 0.125 |
| Extreme Heat Stress | `twb_days_ge_30` | Wet-Bulb Days (Twb >= 30C) | 0.125 |
| Humidity Constraint | `wbd_le_3` | Severe Humid-Heat Days (WBD <= 3C) | 0.075 |
| Humidity Constraint | `wbd_gt3_le6` | Moderate Humid-Heat Days (3C < WBD <= 6C) | 0.075 |
| Night-time Heat Stress | `tasmin_tropical_nights_gt28` | Tropical Nights (TR, TN > 28C) | 0.075 |
| Night-time Heat Stress | `tn90p_warm_nights_pct` | Warm Nights (TN90p) | 0.075 |
| Persistence / Duration | `wbd_le_3_consecutive_days` | Consecutive Wet-Bulb Stress Days (WBD <= 3C) | 0.0833 |
| Persistence / Duration | `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | 0.0833 |
| Persistence / Duration | `twb_days_ge_28` | Heat Stress Days (Twb >= 28C) | 0.0833 |

Implementation references:
- Bundle catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Bundle weights: `india_resilience_tool/config/bundle_weights.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`
- Wet-bulb compute functions: `tools/pipeline/compute_indices_multiprocess.py`
- Composite scoring: `india_resilience_tool/analysis/bundle_scores.py`

Note:
- The Heat Stress domain also exposes some WBGT-style screening metrics in the
  registry, but those are not part of the current weighted
  `composite_heat_stress` bundle unless included in `bundle_weights.py`.

### 5.2 Metric-by-Metric Index Calculation

The bundle mixes:
- wet-bulb temperature metrics derived from `tas` and `hurs`;
- wet-bulb depression metrics derived from `tas - Twb`;
- night-time minimum-temperature threshold/percentile metrics;
- warm-spell persistence metrics from `tasmax`.

Current spatial aggregation:
- For each polygon, the pipeline first computes daily spatial mean `tas`,
  `tasmax`, `tasmin`, and/or `hurs`.
- Wet-bulb and wet-bulb depression metrics are then computed from these
  polygon-average daily series.

#### Wet-Bulb Temperature Method

Wet-bulb temperature is computed using the Stull (2011) approximation:

```text
Twb = f(Ta_C, RH_pct)
```

Inputs:
- `tas`: near-surface air temperature, converted from K to C.
- `hurs`: relative humidity, clipped to `0-100%`.
- If humidity appears as a `0-1` fraction, it is multiplied by 100.

Interpretation:
- Wet-bulb temperature combines heat and humidity.
- Higher Twb means greater physiological heat stress because evaporative cooling
  is less effective.

#### `twb_annual_mean`

Unit: `C`

Formula:
- Compute daily polygon-average Twb.
- Return annual mean Twb.

Interpretation:
- Background annual humid-heat burden.

#### `twb_summer_mean`

Unit: `C`

Formula:
- Compute daily polygon-average Twb.
- Select March-April-May.
- Return seasonal mean Twb.

Interpretation:
- Pre-monsoon / summer humid-heat burden.

#### `twb_annual_max`

Unit: `C`

Formula:
- Compute daily polygon-average Twb.
- Return annual maximum Twb.

Interpretation:
- Peak annual humid-heat intensity.

#### `twb_days_ge_30`

Unit: `days`

Formula:
- Count days where daily polygon-average Twb is `>= 30C`.

Interpretation:
- Number of severe wet-bulb heat-stress days.

#### `twb_days_ge_28`

Unit: `days`

Formula:
- Count days where daily polygon-average Twb is `>= 28C`.

Interpretation:
- Number of broader heat-stress days. This is less extreme than the `>=30C`
  threshold and contributes to persistence/duration weighting.

#### Wet-Bulb Depression Metrics

Wet-bulb depression is:

```text
WBD = Ta_C - Twb_C
```

Low WBD means the air is humid and near saturation, so sweat evaporation is
constrained.

#### `wbd_le_3`

Unit: `days`

Formula:
- Compute daily WBD.
- Count days where WBD is `<= 3C`.

Interpretation:
- Severe humidity-constrained heat-stress days.

#### `wbd_gt3_le6`

Unit: `days`

Formula:
- Compute daily WBD.
- Count days where `3C < WBD <= 6C`.

Interpretation:
- Moderate humidity-constrained heat-stress days.

#### `wbd_le_3_consecutive_days`

Unit: `days`

Formula:
- Compute daily WBD.
- Flag days where WBD is `<= 3C`.
- Return the longest run meeting the configured minimum spell length of 3 days.

Interpretation:
- Persistence of severe humidity-constrained heat stress.

#### `tasmin_tropical_nights_gt28`

Unit: `days`

Formula:
- Use daily minimum temperature `tasmin`.
- Count nights where `tasmin > 28C`.

Interpretation:
- Night-time heat stress and limited overnight recovery.

#### `tn90p_warm_nights_pct`

Unit: `%`

Formula:
- Use `tasmin`.
- Compute ETCCDI-style day-of-year 90th percentile thresholds from baseline
  years.
- Baseline currently configured as `(1981, 2010)`.
- Uses a 5-day moving window and nearest quantile.
- Count/evaluate nights above the threshold as percentage of days.

Interpretation:
- Relative warm-night frequency compared with historical baseline night-time
  climate.

#### `wsdi_warm_spell_days`

Unit: `days`

Formula:
- Use `tasmax`.
- Compute ETCCDI-style day-of-year 90th percentile thresholds from baseline
  years.
- Baseline currently configured as `(1981, 2010)`.
- Uses a 5-day moving window, nearest quantile, minimum spell length of 6 days.
- Count days contributing to qualifying warm spells.

Interpretation:
- Persistence of daytime warm spells relative to historical local climate.

### 5.3 Period and Ensemble Aggregation

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
- `twb_annual_mean_C__ssp585__2060-2080__mean`
- `twb_days_ge_30_days__ssp585__2060-2080__mean`
- `wbd_le_3_days__ssp585__2060-2080__mean`

Baseline comparison columns use historical `1990-2010` where available.

### 5.4 Normalization and Risk Interpretation

All current Heat Stress bundle components are treated as higher-is-worse:
- Higher Twb is worse.
- More Twb threshold days are worse.
- More low-WBD days are worse.
- More hot nights are worse.
- More warm-spell days are worse.

For individual metric deep-dive views:
- `Index value` is the selected raw metric/statistic.
- `Delta vs baseline` is selected value minus historical baseline.
- Percentile/risk class is computed over the active comparison set.
- For block views, percentiles are state-scoped unless otherwise filtered by
  the runtime path.

Risk class from percentile:
- `>=80`: Very High
- `>=60`: High
- `>=40`: Medium
- `>=20`: Low
- otherwise: Very Low

### 5.5 Bundle Score Calculation

For composite scoring:
1. Resolve each of the 11 component metric columns for the selected scenario and
   period.
2. Normalize each component across the available geography frame to a `0-100`
   higher-is-worse scale.
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
- Because components are correlated, especially Twb metrics and WBD metrics, the
  score should be interpreted as a screening index rather than an
  independent-effects risk model.

### 5.6 UI Presentation

Deep Dive metric view:
- Shows raw metric values for the selected component.
- Baseline deltas are meaningful for climate metrics with historical columns.
- Scenario comparison is available for SSP climate metrics.
- Trends may be available where yearly outputs exist.

Portfolio heatmap:
- Displays percentiles, not raw values.
- Colors are risk-class bins derived from percentile.
- Percentiles are metric-specific, so a `90` for Twb annual max and a `90` for
  tropical nights mean "high relative position" within each metric, not
  equivalent physical magnitude.

Glance / bundle view:
- Displays persisted `composite_heat_stress` bundle scores.
- Component drivers may show the metrics contributing most strongly to the
  score.

### 5.7 Validation Checks and Open Methodology Comments

Recommended validation checks:
1. Pick one district/block and verify one wet-bulb metric:
   - daily polygon-average `tas`
   - daily polygon-average `hurs`
   - Stull Twb
   - annual mean/max or threshold-day count
2. Verify WBD logic:
   - `WBD = tas_C - Twb_C`
   - `wbd_le_3` days
   - `wbd_gt3_le6` days
   - longest `wbd_le_3` spell
3. Verify night-time heat metrics:
   - `tasmin > 28C` count
   - TN90p baseline threshold and percentage
4. Verify composite:
   - pull all 11 component columns for one geography;
   - min-max normalize each component;
   - apply weights;
   - compare to `composite_heat_stress`.

Open methodology comments:
- Spatial aggregation should be reviewed. Current wet-bulb metrics compute
  polygon-average `tas` and `hurs` first, then compute Twb. Since Twb is
  nonlinear in temperature and humidity, grid-cell Twb followed by zonal
  aggregation may better preserve local humid-heat conditions.
- Stull wet-bulb is an approximation. It is useful for screening, but should be
  documented as approximate and not equivalent to a full psychrometric wet-bulb
  calculation.
- TN90p and WSDI use `baseline_years: (1981, 2010)` while dashboard historical
  deltas use `1990-2010`. As with R95p/R95pTOT, this should be aligned or
  explicitly justified.
- Registry comments say ETCCDI convention is strictly `>` for TN90p/WSDI, but
  the current params set `exceed_ge=True`, making exceedance inclusive.
  Numerical impact may be small, but method text and implementation should
  agree.
- The bundle contains potentially correlated components: Twb annual mean, Twb
  summer mean, Twb annual max, Twb threshold days, WBD threshold days, and WBD
  spell length are not independent. This is acceptable for a screening bundle if
  intentional, but should be documented as a weighted hazard-pressure index.
- Threshold choices need provenance: Twb `28C`, Twb `30C`, WBD `3C`, WBD `6C`,
  and tropical nights `28C` should have a cited or approved basis.

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

Open methodology comments:
- Directionality is the biggest issue for this bundle. Lower winter temperature
  and lower Tmin metrics should be flagged `rank_higher_is_worse=False`;
  otherwise the composite may invert part of the cold-risk signal.
- DJF handling needs explicit review. The current seasonal functions select
  months `[12, 1, 2]` within each processed year. That may not represent
  meteorological winter as a cross-year season spanning December of one year and
  January-February of the next. This affects `tas_winter_mean`,
  `tasmin_winter_mean`, and `tasmin_winter_min`.
- The right DJF convention should be chosen and documented:
  - calendar-year DJF subset: Jan-Feb and Dec from the same calendar year;
  - meteorological winter season: Dec of previous year plus Jan-Feb of current
    winter year;
  - water/climate-year convention if the input pipeline uses one.
- If the current implementation is retained, UI/docs should avoid implying
  cross-year meteorological DJF unless that is actually computed.
- TN10p/TX10p/CSDI use `baseline_years: (1981, 2010)` while dashboard
  historical deltas use `1990-2010`. This recurring baseline mismatch should be
  consolidated and resolved across percentile metrics.
- Registry descriptions for CSDI say cold spell uses `TN < 10th percentile`,
  while the configured `exceed_ge=True` with `direction="below"` makes the
  special-case workflow inclusive (`<= threshold`). This may be small numerically
  but should be aligned with the intended ETCCDI convention.
- Spatial aggregation should be reviewed. Current method computes indices from
  polygon-average daily temperature. For local cold extremes, grid-cell index
  calculation followed by zonal aggregation may better preserve localized cold
  pockets, especially in heterogeneous terrain.
- Threshold choices need provenance: `TN <= 10C`, `TN <= 5C`, and `TX <= 15C`
  are workbook-aligned, but should be documented with rationale for
  India/Telangana use.

## 7. Thematic - Agriculture & Growing Conditions

Pending review.

## 8. Sector-wise - Agricultural Risk

Pending review.

## 9. Sector-wise - Health Risk

Pending review.

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
