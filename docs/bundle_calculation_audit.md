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

Pending review.

## 5. Thematic - Heat Stress

Pending review.

## 6. Thematic - Cold Risk

Pending review.

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
