# processed_optimised Vendor Data Contract

This document explains how the India Resilience Tool dashboard is fed by the
`processed_optimised/` runtime bundle. It is written for a JavaScript web
application vendor who will consume the optimized data directly.

The optimized bundle is the deployable data product. The web application should
not depend on the legacy `processed/` tree for normal runtime rendering.

## Current Bundle Snapshot

The examples below refer to the local bundle inspected at:

```text
D:\projects\irt_data\processed_optimised
```

Observed bundle facts:

| Item | Observed value |
|---|---:|
| `parity_report.json` issue count | 0 |
| Metric directories under `metrics/` | 123 |
| Glance bundles in `bundle_manifest.json` | 15 |
| Admin block index rows | 6,300 |
| Exposure summary rows | 7,091 |
| Hydrology context rows | 7,090 |

Treat `bundle_manifest.json` and `parity_report.json` as the first files to
read. A clean handover must have `parity_report.json` with `issue_count: 0`.

## Directory Contract

The bundle has four runtime surfaces:

```text
processed_optimised/
  bundle_manifest.json
  parity_report.json
  metrics/
  geometry/
  context/
```

Use `metrics/` for Deep Dive values, rankings, map colors, and trend charts.
Use `context/glance/` for the precomputed Glance landing experience. Use
`geometry/` for map shapes. Use the remaining `context/` files for selectors,
crosswalks, exposure summaries, hydrology summaries, and reference overlays.

## Stable Identifiers

The data uses normalized keys so tables can be joined without fuzzy matching.

| Level | Key column | Shape |
|---|---|---|
| District | `district_key` or `__district_key` | `state_alias\|district_alias` |
| Block | `block_key` or `__block_key` | `state_alias\|district_alias\|block_alias` |
| Basin | `basin_id` | SOI basin identifier |
| Sub-basin | `subbasin_id` | SOI sub-basin identifier |

Examples:

```text
district_key = telangana|adilabad
block_key    = telangana|adilabad|adilabad rural
basin_id     = 03
subbasin_id  = F03GOD
```

The Glance tables prefix key columns with double underscores:
`__state_key`, `__district_key`, and `__block_key`. Master metric tables use
plain key columns: `district_key` and `block_key`.

## Metric Column Naming

Metric values in master tables use this pattern:

```text
<metric_value_column>__<scenario>__<period>__<stat>
```

Examples:

```text
max_1day_precip_mm__historical__1990-2010__mean
max_1day_precip_mm__ssp585__2040-2060__median
population_total__snapshot__2025__mean
composite_life_livelihood_loss_risk__ssp585__2040-2060__mean
aq_water_stress__bau__2050__mean
```

Important details:

- Climate metrics usually have `mean` and `median`.
- Static snapshot metrics usually have only `mean`.
- Proposal sector composites may include `mean`, rule-level `score`, and
  `available_rule_count`.
- The optimized bundle intentionally removes heavy or duplicate runtime fields:
  `std`, `p05`, `p95`, `n_models`, `values_per_model`, and `models`.

## Glance View

Glance is precomputed. The JavaScript app should not recompute Glance scores,
ranks, score bands, drivers, attributes, or distributions.

Read available Glance bundles from:

```text
bundle_manifest.json
```

Each bundle entry gives:

```json
{
  "bundle_slug": "composite_heat_risk",
  "bundle_name": "Heat Risk",
  "group_key": "thematic",
  "selector_label": "Thematic - Heat Risk",
  "available_pairs": [
    {"scenario": "ssp245", "period": "2020-2040"}
  ]
}
```

For a selected bundle, scenario, and period, load:

```text
context/glance/<composite_slug>/<scenario>/<period>/
  district.parquet
  state.parquet
  block.parquet
  drivers.parquet
  attributes.parquet
  distributions.parquet
```

`block.parquet` is present for block-supported bundles. In the current contract,
`Life & Livelihood Loss Risk` is district and block supported.

### Glance State Map

Use:

```text
context/glance/<slug>/<scenario>/<period>/state.parquet
geometry/admin/district/state=<STATE>.geojson
```

The state table is used for state-level scores and rankings. The current local
bundle has one state, Telangana, so `state_rank` and `state_count` are not a
national multi-state ranking yet.

Core columns:

| Column | Meaning |
|---|---|
| `bundle_slug` | Composite slug |
| `bundle_name` | Human bundle name |
| `group_key` | `thematic` or `sector_wise` |
| `selector_label` | UI selector label |
| `scenario`, `period` | Active pair |
| `state_name`, `__state_key` | State identity |
| `bundle_score` | 0-100 score |
| `bundle_score_display` | Preformatted one-decimal score |
| `score_band` | `Low`, `Moderate`, `High`, `Very High`, or insufficient-data text |
| `state_rank`, `state_count` | Precomputed rank and denominator |

### Glance District Map And Rankings

Use:

```text
context/glance/<slug>/<scenario>/<period>/district.parquet
geometry/admin/district/state=<STATE>.geojson
```

Join `district.parquet.__district_key` to GeoJSON `district_key`.

Core columns:

| Column | Meaning |
|---|---|
| `district_name`, `state_name` | Display names |
| `__district_key`, `__state_key` | Join keys |
| `bundle_score` | District bundle score |
| `score_band` | Score band |
| `district_rank`, `district_count` | Rank within state |
| `state_bundle_score` | State average/aggregate score |
| `state_mean_score` | Mean score across districts in state |
| `delta_vs_state_mean` | District score minus state mean |
| `delta_vs_state_mean_display` | Preformatted delta |

Example row, shortened:

```json
{
  "bundle_slug": "composite_life_livelihood_loss_risk",
  "bundle_name": "Life & Livelihood Loss Risk",
  "scenario": "ssp585",
  "period": "2040-2060",
  "state_name": "TELANGANA",
  "district_name": "ADILABAD",
  "__district_key": "telangana|adilabad",
  "bundle_score": 47.0853,
  "bundle_score_display": "47.1",
  "score_band": "Moderate",
  "district_rank": 14,
  "district_count": 33,
  "delta_vs_state_mean_display": "-0.1"
}
```

### Glance Block Drilldown

Use:

```text
context/glance/<slug>/<scenario>/<period>/block.parquet
geometry/admin/block/state=<STATE>.geojson
```

Join `block.parquet.__block_key` to GeoJSON `block_key`.

Core columns:

| Column | Meaning |
|---|---|
| `block_name`, `district_name`, `state_name` | Display names |
| `__block_key`, `__district_key`, `__state_key` | Join keys |
| `bundle_score` | Block bundle score |
| `score_band` | Score band |
| `block_rank_within_district` | Block rank among blocks in its district |
| `block_count_within_district` | District denominator |
| `block_percentile_within_district` | Percentile in district |
| `risk_class_within_district` | Risk class in district context |
| `block_rank_within_state` | Block rank among blocks in state |
| `block_count_within_state` | State denominator |
| `block_percentile_within_state` | Percentile in state |
| `risk_class_within_state` | Risk class in state context |
| `district_bundle_score` | Parent district score |
| `state_bundle_score` | Parent state score |

Example row, shortened:

```json
{
  "bundle_slug": "composite_life_livelihood_loss_risk",
  "scenario": "ssp585",
  "period": "2040-2060",
  "block_name": "Adilabad Rural",
  "__block_key": "telangana|adilabad|adilabad rural",
  "bundle_score": 42.1559,
  "score_band": "Moderate",
  "block_rank_within_district": 14,
  "block_count_within_district": 21,
  "block_rank_within_state": 376,
  "block_count_within_state": 620,
  "district_bundle_score": 47.0853,
  "state_bundle_score": 47.2086
}
```

### Glance Drivers, Attributes, And Distributions

Use:

```text
drivers.parquet
attributes.parquet
distributions.parquet
```

`drivers.parquet` contains the ranked input drivers for state, district, and
block drawer/card views. The main columns are:

```text
scope_level
state_name
district_name
block_name
__state_key
__district_key
__block_key
driver_rank
driver_slug
driver_label
driver_score
driver_score_display
driver_source
```

For thematic bundles, drivers are component metrics. For sector-wise proposal
bundles, drivers are proposal rules such as `rx1day_ge_200` or
`wsdi_ge_5`.

`attributes.parquet` contains optional display attributes for district cards.
`distributions.parquet` contains score-band counts for national and state
histograms.

## Deep Dive View

Deep Dive uses the optimized metric masters under:

```text
metrics/<slug>/masters/<family>/<level>/...
```

Paths:

```text
metrics/<slug>/masters/admin/district/state=<STATE>.parquet
metrics/<slug>/masters/admin/block/state=<STATE>.parquet
metrics/<slug>/masters/hydro/basin/master.parquet
metrics/<slug>/masters/hydro/sub_basin/master.parquet
```

Admin levels are sharded by state. Hydro levels use one national master file.

### Deep Dive Value Selection

For the selected metric, scenario, period, statistic, and level:

1. Load the master table for that level.
2. Select the value column matching:

   ```text
   <metric_value_column>__<scenario>__<period>__<stat>
   ```

3. Filter to the selected geography using the relevant key.
4. Use the selected value for the details card, map color, and rankings table.

For example, the selected Deep Dive value for district Rx1day under SSP5-8.5,
2040-2060, mean is:

```text
max_1day_precip_mm__ssp585__2040-2060__mean
```

### Historical Baseline

For climate metrics, a historical baseline is available in master columns:

```text
<metric_value_column>__historical__1990-2010__mean
<metric_value_column>__historical__1990-2010__median
```

The dashboard compares the selected future value against this historical
baseline and displays:

```text
absolute_delta = selected_value - historical_baseline
percent_delta  = absolute_delta / historical_baseline * 100
```

If the baseline value is missing or zero, suppress the percent delta.

Aqueduct metrics have historical baseline columns with period `1979-2019`, but
they are external snapshot-style scenario-period masters and do not have yearly
trend files.

Static snapshot metrics and dashboard composites do not generally have
historical baseline comparison. Show their selected value without a historical
delta unless a matching historical column actually exists.

### Present Or Selected Value

The dashboard's "Current value" label means the value selected by the UI. For
future climate metrics, it is the selected future scenario-period value. For
static snapshot metrics, it is the snapshot value.

Examples:

```text
population_total__snapshot__2025__mean
built_up_area_km2__snapshot__Current__mean
jrc_flood_depth_index_rp100__snapshot__Current__mean
composite_heat_risk__ssp585__2040-2060__mean
```

### Rank In State And Rank In District

Deep Dive ranks are computed from the loaded master table at runtime.

District mode:

1. Filter the district master table to the selected state.
2. Drop rows where the selected value is null.
3. Sort values according to metric direction.
4. Rank the selected district among all districts in the state.

Block mode:

1. Filter the block master table to the selected state for "rank in state".
2. Filter the block master table to the selected district for "rank in district".
3. Drop null selected values.
4. Sort values according to metric direction.

Most risk metrics use "higher value = worse", so rank 1 is the highest value.
Some indicators are not risk-directional. The JavaScript application should
carry an explicit metric metadata table for rank direction. If no metadata is
available, default to higher-is-worse only for risk/composite screens and label
other ranks as value ranks rather than risk ranks.

Glance ranks are different: they are already precomputed in the Glance Parquet
files and should be consumed directly.

### Trend Over Time

Trend charts use yearly fact tables when they exist:

```text
metrics/<slug>/yearly_ensemble/admin/district/state=<STATE>.parquet
metrics/<slug>/yearly_ensemble/admin/block/state=<STATE>.parquet
metrics/<slug>/yearly_ensemble/hydro/basin/master.parquet
metrics/<slug>/yearly_ensemble/hydro/sub_basin/master.parquet
```

Yearly ensemble schema:

| Column | Meaning |
|---|---|
| `year` | Calendar year |
| `scenario` | `historical`, `ssp245`, or `ssp585` for climate metrics |
| `mean` | Ensemble mean for that year |
| `median` | Ensemble median for that year, when retained |
| key column | `district_key`, `block_key`, `basin_id`, or `subbasin_id` |

Example:

```json
{
  "year": 1951,
  "scenario": "historical",
  "district_key": "telangana|adilabad",
  "mean": 45.3852,
  "median": 41.9568
}
```

The dashboard loads two series for a trend chart:

```text
historical + selected future scenario
```

For example, under `ssp585`, load historical rows and `ssp585` rows for the
selected geography. If no yearly files exist for a metric or level, hide the
trend chart or show "trend not available".

### Model-Member Trend Overlay

Some climate metrics also retain district-level per-model yearly facts:

```text
metrics/<slug>/yearly_models/admin/district/state=<STATE>.parquet
```

Schema:

| Column | Meaning |
|---|---|
| `year` | Calendar year |
| `scenario` | Climate scenario |
| `model` | Climate model name |
| `value` | Model-member yearly value |
| `district_key` | District key |

In the inspected bundle, yearly model files are available for district-level
climate metrics only. Block, basin, and sub-basin trend charts should use
yearly ensemble facts only unless a `yearly_models` file exists for that level.

## Map Geometry

Geometry is simplified for display and stored separately from metric values.
For the JavaScript dashboard runtime, prefer the optimized geometry shards in
`processed_optimised/geometry/`. These files are already split by state or
basin, simplified for map rendering, and contain the join keys expected by the
optimized metric and Glance tables.

Paths:

```text
geometry/admin/district/state=<STATE>.geojson
geometry/admin/block/state=<STATE>.geojson
geometry/hydro/basin.geojson
geometry/hydro/sub_basin/basin_id=<ID>.geojson
```

Join fields:

| Geometry | Join field |
|---|---|
| District GeoJSON | `district_key` |
| Block GeoJSON | `block_key` |
| Basin GeoJSON | `basin_id` |
| Sub-basin GeoJSON | `subbasin_id` |

Use `context/admin_block_index.parquet` for state -> district -> block selector
metadata. Use `context/hydro_subbasin_index.parquet` for basin -> sub-basin
selector metadata.

### Canonical Source GeoJSONs

The source data directory may also contain canonical full-boundary files:

```text
districts_4326.geojson
blocks_4326.geojson
basins.geojson
subbasins.geojson
river_network_display.geojson
```

These are useful to provide to the vendor as reference data, QA aids, and a
fallback source for geometry questions. They are not the preferred production
runtime geometry for the JavaScript app.

Recommended handover:

| File family | Provide? | Use in JS app |
|---|---|---|
| `processed_optimised/geometry/...` | Yes, required | Primary map rendering geometry |
| `processed_optimised/context/river_network_display.geojson` | Yes, if river overlay is in scope | Optional river display overlay |
| Root `districts_4326.geojson` | Optional reference | Do not use by default for runtime maps |
| Root `blocks_4326.geojson` | Optional reference | Do not use by default for runtime maps |
| Root `basins.geojson` | Optional reference | Do not use by default for runtime maps |
| Root `subbasins.geojson` | Optional reference | Do not use by default for runtime maps |

Only ask the JavaScript app to read the root canonical GeoJSONs if the vendor
needs unsimplified geometry, full national single-file layers, or a QA
comparison against the optimized shards. If they do use canonical root geometry,
they must confirm the same join keys exist or create them consistently:
`district_key`, `block_key`, `basin_id`, and `subbasin_id`.

### River Network Overlay

The river network is an optional display overlay, not a metric layer. The
runtime artifact is:

```text
context/river_network_display.geojson
```

Use this optimized copy for the JavaScript dashboard. The root
`river_network_display.geojson` can be shared as reference, but the optimized
copy is the runtime file that should travel with `processed_optimised/`.

The river overlay is filtered differently by app context:

| View context | Filter behavior | Required support artifacts/properties |
|---|---|---|
| Admin district/block | show rivers intersecting the selected district | `district_names_clean` property in `river_network_display.geojson` |
| Hydro basin | show rivers for the reconciled river basin name | `context/river_basin_name_reconciliation.parquet` |
| Hydro sub-basin | show rivers for the selected sub-basin when diagnostics match | `context/river_subbasin_diagnostics.parquet` |

For admin district and block views, the dashboard expects the river display
GeoJSON to be enriched with a comma-separated `district_names_clean` property on
each river feature. If that property is missing, the current Streamlit dashboard
reports that the artifact has not been enriched and asks the operator to run:

```bash
python -m tools.pipeline.enrich_river_network_districts
```

That enrichment command intersects the river display lines with
`districts_4326.geojson`, writes `district_names_clean` back to
`river_network_display.geojson`, preserves a one-time `.bak` backup, and refreshes
`processed_optimised/context/river_network_display.geojson` when that optimized
copy already exists. After enrichment, the vendor can filter admin river overlay
features by matching the selected district alias against `district_names_clean`.

River features may include useful display properties such as:

```text
river_feature_id
source_uid_river
river_name_clean
basin_name_clean
subbasin_name_clean
state_names_clean
district_names_clean
length_km_source
```

When no feature matches the selected district, basin, or sub-basin, render the
map without the river overlay and show a non-blocking "not available" message if
the UI has a diagnostics area.

## Exposure Snapshot Summary

The optional exposure summary file is:

```text
context/admin_exposure_summary.parquet
```

It feeds compact admin context cards in district and block details views. It is
not the full exposure metric table. Full exposure metrics still live under
`metrics/<exposure_slug>/masters/...`.

Schema:

```text
admin_key
admin_level
state_name
district_name
block_name
pop_2020
parent_pop_2020
parent_level
parent_name
population_share_parent_pct
rural_facilities_total_count
rural_facilities_agro_count
rural_facilities_education_count
rural_facilities_health_count
rural_facilities_service_count
rural_facilities_total_count_per_100k
built_up_area_km2
built_up_area_share_pct
```

Important naming note: the current summary columns are named `pop_2020` and
`parent_pop_2020`, but they are built from the population metric column
`population_total__snapshot__2025__mean`. Treat the value as the current
population exposure snapshot carried by the source data, not as an independently
computed 2020 product.

How to consume:

1. Build the selected admin key:
   - district: `alias(state)|alias(district)`
   - block: `alias(state)|alias(district)|alias(block)`
2. Filter where `admin_key` matches and `admin_level` is `district` or `block`.
3. Display population share, facility counts, facility rate, and built-up area
   values if present.

Example row, shortened:

```json
{
  "admin_key": "andaman and nicobar islands|nicobars|car nicobar",
  "admin_level": "block",
  "state_name": "ANDAMAN AND NICOBAR ISLANDS",
  "district_name": "Nicobars",
  "block_name": "Car Nicobar",
  "pop_2020": 25730.1367,
  "parent_level": "district",
  "parent_name": "Nicobars",
  "population_share_parent_pct": 63.0382,
  "rural_facilities_total_count": 0,
  "built_up_area_km2": 0.979592,
  "built_up_area_share_pct": 0.726871
}
```

How it is created:

- Population rows come from population district/block QA outputs.
- Rural facility columns are merged from rural facility metric masters.
- Built-up columns are merged from built-up metric masters.
- One row is produced per district and per block when source rows exist.

## Hydrology Context Summary

The optional admin hydrology context file is:

```text
context/admin_hydro_summary.parquet
```

It feeds compact hydrological context cards in admin district/block details
views. It answers: "which basin/sub-basin does this admin unit mostly fall in?"

Schema:

```text
admin_key
admin_level
state_name
district_name
block_name
basin_id
basin_name
basin_frac
subbasin_id
subbasin_name
subbasin_frac
also_intersects_basin_json
drainage_area_km2
primary_river
runoff_coeff
hydro_type
hydro_summary_status
```

How to consume:

1. Build the selected admin key.
2. Filter by `admin_key` and `admin_level`.
3. Display `basin_name`, `basin_frac`, `subbasin_name`, `subbasin_frac`, and
   `hydro_type`.
4. Parse `also_intersects_basin_json` as a JSON list for secondary basin
   intersections.

Example row, shortened:

```json
{
  "admin_key": "andaman and nicobar islands|nicobars",
  "admin_level": "district",
  "basin_id": "24",
  "basin_name": "Drainage Area of Andaman and Nicobar Islands Basin",
  "basin_frac": 0.8508,
  "subbasin_id": "F24DAN",
  "subbasin_name": "Drainage Area of Andaman and Nicobar Islands",
  "subbasin_frac": 0.2020,
  "also_intersects_basin_json": "[]",
  "hydro_type": "Single-basin, mixed sub-basin",
  "hydro_summary_status": "available"
}
```

How it is created:

- District rows are derived from `district_basin.parquet` and
  `district_subbasin.parquet`.
- Block rows are derived from `block_basin.parquet` and
  `block_subbasin.parquet`.
- The dominant basin is the basin with the largest admin-area fraction.
- The dominant sub-basin is the sub-basin with the largest reciprocal
  counterpart fraction available for that admin unit.
- `also_intersects_basin_json` stores up to two secondary basins whose
  intersection fraction is at least 0.05.

## Crosswalk Context

The optimized bundle includes crosswalks between administrative and hydrological
geographies:

```text
context/district_basin.parquet
context/district_subbasin.parquet
context/block_basin.parquet
context/block_subbasin.parquet
```

These files support related-unit overlays and navigation between admin and hydro
views. They contain area intersection columns such as:

```text
district_area_fraction_in_basin
basin_area_fraction_in_district
block_area_fraction_in_subbasin
subbasin_area_fraction_in_block
intersection_area_km2
```

Use these files when the user clicks "show related basin/sub-basin" or moves
between admin and hydro contexts. Do not use them to transfer metric values
unless a separate weighted-transfer methodology is explicitly supplied.

## Reference Overlays

Reference overlays live in `context/` as PNG plus metadata JSON pairs. Current
families include:

```text
context/population/overlay/
context/rural_facilities/overlay/
context/built_up_area/overlay/
context/jrc_flood_depth/overlay/
context/river_network_display.geojson
```

These overlays are display-only context layers. They are not metric master
tables and should not be used as the source of rankings or details values. The
river overlay additionally depends on the reconciliation/diagnostics files noted
in the Map Geometry section when filtering by hydro basin or sub-basin.

## Field Availability Rules

Not every metric has every field family. The application must branch by
available files and available columns, not by assumption.

| Metric family | Masters | Historical baseline | Future scenarios | Snapshot | Yearly trend | Model-member overlay | Glance |
|---|---|---|---|---|---|---|---|
| Climate indicators | district, block, basin, sub_basin for most metrics | `historical/1990-2010` | `ssp245`, `ssp585` | no | yes for available levels | district only in current bundle | no |
| Thematic composites | district, block | no | `ssp245`, `ssp585` | no | no | no | yes |
| Sector-wise proposal composites | district, block | no | `ssp245`, `ssp585` | no | no | no | yes |
| `composite_flood_jrc_depth` | district, block | no | no | `snapshot/Current` | no | no | yes |
| JRC flood-depth metrics | district, block | no | no | `snapshot/Current` | no | no | no |
| Population exposure | district, block | no | no | `snapshot/2025` | no | no | no |
| Rural facilities exposure | district, block | no | no | `snapshot/2019-2021` | no | no | no |
| Built-up area exposure | district, block | no | no | `snapshot/Current` | no | no | no |
| Groundwater | district only | no | no | `snapshot/2024-2025` | no | no | no |
| Aqueduct | district, block, basin, sub_basin | `historical/1979-2019` | `bau`, `opt`, `pes` for 2030/2050/2080 | no | no | no | no |

## Current On-Disk Availability Appendix

This compact appendix lists all metric slugs observed in the inspected
`processed_optimised/metrics/` bundle, grouped by identical availability
patterns. The `Levels` column is based on actual master files on disk.

| Domains | Selection | Levels | Yearly ensemble | Yearly models | Metric slugs |
|---|---|---|---|---|---|
| Aqueduct Water Risk | scenario_period | district, block, basin, sub_basin | - | - | `aq_interannual_variability`, `aq_seasonal_variability`, `aq_water_depletion`, `aq_water_stress` |
| Built-up Area Exposure | static_snapshot | district, block | - | - | `built_up_area_km2`, `built_up_area_share_pct` |
| Population Exposure | static_snapshot | district, block | - | - | `population_density`, `population_total` |
| Rural Facilities Exposure | static_snapshot | district, block | - | - | `rural_facilities_agro_count`, `rural_facilities_agro_count_per_100k`, `rural_facilities_education_count`, `rural_facilities_education_count_per_100k`, `rural_facilities_health_count`, `rural_facilities_health_count_per_100k`, `rural_facilities_service_count`, `rural_facilities_service_count_per_100k`, `rural_facilities_total_count`, `rural_facilities_total_count_per_100k` |
| Groundwater Status & Availability | static_snapshot | district | - | - | `gw_extractable_resource_ham`, `gw_future_availability_ham`, `gw_stage_extraction_pct`, `gw_total_extraction_ham` |
| Riverine Flood | static_snapshot | district, block | - | - | `composite_flood_jrc_depth`, `jrc_flood_depth_index_rp100`, `jrc_flood_depth_rp100`, `jrc_flood_extent_rp100` |
| Unassigned JRC flood-depth return periods | static_snapshot | district, block | - | - | `jrc_flood_depth_rp10`, `jrc_flood_depth_rp50`, `jrc_flood_depth_rp500` |
| Dashboard composites | scenario_period | district, block | - | - | `composite_agricultural_risk`, `composite_agriculture_growing_conditions`, `composite_asset_risk_hydropower`, `composite_asset_risk_thermal_power`, `composite_cold_risk`, `composite_drought_risk`, `composite_flood_extreme_rainfall_risk`, `composite_health_risk`, `composite_heat_risk`, `composite_heat_stress`, `composite_industrial_risk`, `composite_infrastructure_risk`, `composite_investment_financial_risk`, `composite_life_livelihood_loss_risk` |
| Hydropower helper | scenario_period | district, block | - | - | `r95p_interannual_variability` |
| Cold Risk climate indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `csdi_cold_spell_days`, `tas_winter_mean`, `tasmin_winter_min`, `tn10p_cool_nights_pct`, `tnle10_consecutive_cold_nights`, `tnle5_severe_cold_nights`, `tnn_annual_min`, `tx10p_cool_days_pct`, `txle15_cold_days` |
| Heat Risk climate indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `hwa_heatwave_amplitude`, `hwfi_events_tmean_90p`, `tas_annual_mean`, `tas_summer_mean`, `tasmin_tropical_nights_gt25`, `tx90p_hot_days_pct`, `txge30_hot_days`, `hwfi_tmean_90p` |
| Heat Risk and Agriculture climate indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `tasmax_summer_mean`, `txge35_extreme_heat_days` |
| Heat Risk, Heat Stress, and health climate indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `tn90p_warm_nights_pct`, `tnx_annual_max`, `txx_annual_max`, `wsdi_warm_spell_days` |
| Heat Stress climate indicators | scenario_period | district, block | district, block | district | `swbgt_empirical_annual_mean`, `swbgt_empirical_days_ge_28`, `swbgt_empirical_days_ge_30`, `swbgt_empirical_days_ge_32`, `wbgt_shade_stull_annual_mean`, `wbgt_shade_stull_days_ge_28`, `wbgt_shade_stull_days_ge_30`, `wbgt_shade_stull_days_ge_32` |
| Heat Stress wet-bulb indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `tasmin_tropical_nights_gt28`, `twb_annual_max`, `twb_annual_mean`, `twb_days_ge_28`, `twb_days_ge_30`, `twb_summer_mean`, `wbd_gt3_le6`, `wbd_le_3`, `wbd_le_3_consecutive_days` |
| Rainfall and flood climate indicators, including Extreme Rainfall \| Flash Flood Risk | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `cwd_consecutive_wet_days`, `pr_max_1day_precip`, `pr_max_5day_precip`, `r20mm_very_heavy_precip_days`, `r95p_very_wet_precip`, `r95ptot_contribution_pct`, `pr_simple_daily_intensity`, `rain_gt_2p5mm`, `prcptot_annual_total` |
| Admin-only rainfall climate indicators | scenario_period | district, block | district, block | district | `pr_2day_heavy_rainfall_events_ge150mm`, `r99p_extreme_wet_precip` |
| Drought climate indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `pr_consecutive_dry_days_lt1mm`, `spi12_count_events_lt_minus1`, `spi3_count_events_lt_minus1`, `spi6_count_events_lt_minus1`, `spi12_count_months_lt_minus1`, `spi12_count_months_lt_minus2`, `spi12_drought_index`, `spi3_count_months_lt_minus1`, `spi3_count_months_lt_minus2`, `spi3_drought_index`, `spi6_count_months_lt_minus1`, `spi6_count_months_lt_minus2`, `spi6_drought_index` |
| Temperature variability and growing-season indicators | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `dtr_daily_temp_range`, `etr_extreme_temp_range`, `gsl_growing_season`, `tasmin_winter_mean`, `tnle10_cold_nights` |
| Additional climate indicators not currently assigned to a dashboard domain | scenario_period | district, block, basin, sub_basin | district, block, basin, sub_basin | district | `fd_frost_days`, `spi12_count_months_gt_plus1`, `spi12_count_months_gt_plus2`, `spi3_count_months_gt_plus1`, `spi3_count_months_gt_plus2`, `spi6_count_months_gt_plus1`, `spi6_count_months_gt_plus2`, `tasmin_tropical_nights_gt20`, `tnlt2_cold_nights`, `wbd_le_6` |

## Implementation Checklist

For a dashboard page load:

1. Read `bundle_manifest.json`.
2. Read `parity_report.json` and warn internally if `issue_count` is not zero.
3. Populate Glance selectors from `glance_view_model.bundles`.
4. For a Glance selection, load the five required Glance tables plus
   `block.parquet` when present.
5. Load display geometry for the current focus level and join on stable keys.
6. For Deep Dive, load the selected metric master for the selected level and
   choose the matching metric column.
7. Compute Deep Dive ranks from the selected master table.
8. Load yearly ensemble files only when present.
9. Load `admin_exposure_summary.parquet` and `admin_hydro_summary.parquet` only
   for admin district/block context cards.
10. If river overlay is enabled, load `context/river_network_display.geojson`.
    For admin district/block filtering, require `district_names_clean`; for
    hydro filtering, use river reconciliation/diagnostics context files.
11. Treat missing optional files or columns as "not available", not as zero.

## How IRT Builds This Bundle

The normal optimized refresh command is:

```bash
python -m tools.optimized.build_processed_optimised --overwrite
python -m tools.optimized.audit_processed_optimised_parity
```

For one bundle, for example Life & Livelihood:

```bash
python -m tools.pipeline.build_proposal_bundles --bundle composite_life_livelihood_loss_risk --level block --overwrite
python -m tools.optimized.build_processed_optimised --metric composite_life_livelihood_loss_risk --level admin --overwrite
python -m tools.optimized.audit_processed_optimised_parity --metric composite_life_livelihood_loss_risk --level admin
```

The optimized builder:

- copies compact Parquet masters from legacy processed outputs,
- copies or derives yearly ensemble facts,
- copies district-level yearly model facts where available,
- writes simplified GeoJSON geometry shards,
- writes selector indexes,
- copies crosswalk and overlay context artifacts,
- builds persisted Glance view-model Parquet files,
- writes `bundle_manifest.json`,
- writes `parity_report.json`.

The admin exposure and hydrology context summaries are lightweight context
artifacts built from existing metric masters and crosswalks:

```bash
python -m tools.pipeline.build_admin_exposure_summary --data-dir D:\projects\irt_data
python -m tools.pipeline.build_admin_hydro_summary --data-dir D:\projects\irt_data
```

The river district enrichment command is:

```bash
python -m tools.pipeline.enrich_river_network_districts --dry-run
python -m tools.pipeline.enrich_river_network_districts
```

Run it when `river_network_display.geojson` lacks `district_names_clean` or
after regenerating the river display artifact. If the optimized river copy
already exists, the command refreshes it automatically.
