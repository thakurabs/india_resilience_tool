# Climate Risk Indicator Inventory

This note is a working inventory for aligning the current dashboard taxonomy with the indicators proposed in `D:\projects\irt_data\Climate Risk Indicators.docx`.

Sources used:
- Current dashboard/configuration: [india_resilience_tool/config/metrics_registry.py](/mnt/d/projects/india_resilience_tool/india_resilience_tool/config/metrics_registry.py)
- Current Glance bundle composites: [india_resilience_tool/config/composite_metrics.py](/mnt/d/projects/india_resilience_tool/india_resilience_tool/config/composite_metrics.py)
- Proposal document: `D:\projects\irt_data\Climate Risk Indicators.docx`

## 1. Metrics already present in the dashboard

This section lists the metrics currently encoded in the dashboard registry and available through the dashboard taxonomy. Where a metric has an explicit threshold or baseline rule, it is stated below.

### 1.1 Heat Risk

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_heat_risk` | Composite Heat Risk | No direct threshold; persisted weighted composite score for the bundle |
| `tas_annual_mean` | Annual Mean Temperature (TM Mean) | No explicit threshold |
| `txx_annual_max` | Annual Maximum Temperature (TXx) | No explicit threshold |
| `txge30_hot_days` | Hot Days (TX ≥ 30°C) | Count of days with maximum temperature `>= 30°C` |
| `txge35_extreme_heat_days` | Extreme Heat Days (TX ≥ 35°C) | Count of days with maximum temperature `>= 35°C` |
| `tasmin_tropical_nights_gt25` | Tropical Nights (TR, TN > 25°C) | Count of days with minimum temperature `> 25°C` |
| `tx90p_hot_days_pct` | Hot Days (TX90p) | Percent of days above the rolling 90th percentile baseline; baseline years `1981-2010`, `5`-day window |
| `tn90p_warm_nights_pct` | Warm Nights (TN90p) | Percent of nights above the rolling 90th percentile baseline; baseline years `1981-2010`, `5`-day window |
| `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | Warm-spell days above the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `6` days |
| `hwfi_tmean_90p` | Heat Wave Frequency Index (HWFI, #Days) | Count of heatwave days above the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `5` days |
| `hwfi_events_tmean_90p` | Heat Wave Frequency (tasmax 90p, #Events) | Count of heatwave events above the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `5` days |
| `hwa_heatwave_amplitude` | Heatwave Amplitude (peak day) | Amplitude of peak heatwave day within events defined using the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `5` days |
| `tnx_annual_max` | Warmest Night | No explicit threshold |
| `tasmax_summer_mean` | Summer Max Temperature (MAM Mean) | March-May seasonal mean |
| `tas_summer_mean` | Summer Mean Temperature (TM; MAM Mean) | March-May seasonal mean |

### 1.2 Heat Stress

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_heat_stress` | Composite Heat Stress | No direct threshold; persisted weighted composite score for the bundle |
| `twb_annual_mean` | Wet-Bulb Temperature (Annual Mean) | No explicit threshold |
| `twb_summer_mean` | Wet-Bulb Temperature (Summer Mean; MAM Mean) | March-May seasonal mean |
| `twb_annual_max` | Wet-Bulb Temperature (Annual Max) | No explicit threshold |
| `twb_days_ge_30` | Wet-Bulb Days (Twb ≥ 30°C) | Count of days with wet-bulb temperature `>= 30°C` |
| `wbd_le_3` | Severe Humid-Heat Days (WBD ≤ 3°C) | Count of days with wet-bulb depression `<= 3°C` |
| `wbd_gt3_le6` | Moderate Humid-Heat Days (3°C < WBD ≤ 6°C) | Count of days with wet-bulb depression `> 3°C` and `<= 6°C` |
| `tasmin_tropical_nights_gt28` | Tropical Nights (TR, TN > 28°C) | Count of days with minimum temperature `> 28°C` |
| `tn90p_warm_nights_pct` | Warm Nights (TN90p) | Percent of nights above the rolling 90th percentile baseline; baseline years `1981-2010`, `5`-day window |
| `wbd_le_3_consecutive_days` | Consecutive Wet-Bulb Stress Days (WBD ≤ 3°C) | Spell metric using wet-bulb depression `<= 3°C`; minimum spell length `3` days |
| `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | Warm-spell days above the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `6` days |
| `twb_days_ge_28` | Heat Stress Days (Twb ≥ 28°C) | Count of days with wet-bulb temperature `>= 28°C` |

### 1.3 Cold Risk

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_cold_risk` | Composite Cold Risk | No direct threshold; persisted weighted composite score for the bundle |
| `tas_winter_mean` | Winter Mean Temperature (TM; DJF Mean) | December-February seasonal mean |
| `tasmin_winter_mean` | Winter Min Temperature (DJF Mean) | December-February seasonal mean |
| `tnn_annual_min` | Annual Minimum of Daily Minimum Temperature (TNn) | No explicit threshold |
| `tasmin_winter_min` | Winter Minimum Tmin (DJF Min TN) | December-February seasonal minimum |
| `tnle10_cold_nights` | Cold Nights (TN <= 10°C) | Count of nights with minimum temperature `<= 10°C` |
| `tnle5_severe_cold_nights` | Severe Cold Nights (TN <= 5°C) | Count of nights with minimum temperature `<= 5°C` |
| `txle15_cold_days` | Cold Days (TX <= 15°C) | Count of days with maximum temperature `<= 15°C` |
| `tx10p_cool_days_pct` | Cool Days (TX10p) | Percent of days below the rolling 10th percentile baseline; baseline years `1981-2010`, `5`-day window |
| `tn10p_cool_nights_pct` | Cool Nights (TN10p) | Percent of nights below the rolling 10th percentile baseline; baseline years `1981-2010`, `5`-day window |
| `csdi_cold_spell_days` | Cold Spell Duration Index (CSDI) | Cold-spell days below the 10th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `6` days |
| `tnle10_consecutive_cold_nights` | Consecutive Cold Nights (TN <= 10°C) | Spell metric using nights with minimum temperature `<= 10°C` |

### 1.4 Agriculture & Growing Conditions

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_agriculture_growing_conditions` | Composite Agriculture & Growing Conditions | No direct threshold; persisted weighted composite score for the bundle |
| `gsl_growing_season` | Growing Season Length (GSL) | Growing-season metric using threshold `5°C` (`278.15 K`) and minimum spell length `6` days |
| `tasmax_summer_mean` | Summer Max Temperature (MAM Mean) | March-May seasonal mean |
| `tasmin_winter_mean` | Winter Min Temperature (DJF Mean) | December-February seasonal mean |
| `dtr_daily_temp_range` | Daily Temperature Range (DTR) | No explicit threshold |
| `txge35_extreme_heat_days` | Extreme Heat Days (TX ≥ 35°C) | Count of days with maximum temperature `>= 35°C` |
| `tnle10_cold_nights` | Cold Nights (TN <= 10°C) | Count of nights with minimum temperature `<= 10°C` |
| `wsdi_warm_spell_days` | Warm Spell Duration Index (WSDI) | Warm-spell days above the 90th percentile baseline; baseline years `1981-2010`, `5`-day window, minimum spell length `6` days |
| `spi3_drought_index` | Standardised Precipitation Index 3-month (SPI3) | SPI at `3`-month scale using baseline years `1981-2010` |
| `prcptot_annual_total` | Total Wet-Day Precipitation (PRCPTOT) | Annual total over wet days defined as precipitation `>= 1 mm` |

### 1.5 Flood & Extreme Rainfall Risk

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_flood_extreme_rainfall_risk` | Composite Flood & Extreme Rainfall Risk | No direct threshold; persisted weighted composite score for the bundle |
| `pr_max_1day_precip` | Maximum 1-day Precipitation (Rx1day) | No fixed threshold in code; annual maximum 1-day precipitation |
| `pr_max_5day_precip` | Maximum 5-day Precipitation (Rx5day) | No fixed threshold in code; annual maximum 5-day precipitation |
| `r20mm_very_heavy_precip_days` | Very Heavy Precipitation Days (R20mm) | Count of days with precipitation `>= 20 mm` |
| `r95p_very_wet_precip` | Very Wet Day Precipitation (R95p) | Total precipitation from days above the `95th` percentile of wet-day precipitation; baseline years `1981-2010`, wet-day threshold `>= 1 mm` |
| `r95ptot_contribution_pct` | Very Wet Day Contribution (R95pTOT) | Percent contribution from days above the `95th` percentile of wet-day precipitation; baseline years `1981-2010`, wet-day threshold `>= 1 mm` |
| `cwd_consecutive_wet_days` | Consecutive Wet Days (CWD) | Maximum spell length of consecutive wet days with precipitation `>= 1 mm` |

### 1.6 Rainfall Totals & Typical Wetness

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `prcptot_annual_total` | Total Wet-Day Precipitation (PRCPTOT) | Annual total over wet days defined as precipitation `>= 1 mm` |
| `pr_simple_daily_intensity` | Simple Daily Intensity Index (SDII) | Mean precipitation on wet days defined as precipitation `>= 1 mm` |
| `rain_gt_2p5mm` | Rainy Days (PR > 2.5mm) | Count of days with precipitation `> 2.5 mm` |

### 1.7 Drought Risk

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `composite_drought_risk` | Composite Drought Risk | No direct threshold; persisted weighted composite score for the bundle |
| `spi3_count_events_lt_minus1` | SPI3: Count of drought events with SPI < -1 | Event count at `3`-month SPI scale using threshold `< -1.0`; baseline years `1981-2010` |
| `spi6_count_events_lt_minus1` | SPI6: Count of drought events with SPI < -1 | Event count at `6`-month SPI scale using threshold `< -1.0`; baseline years `1981-2010` |
| `spi12_count_events_lt_minus1` | SPI12: Count of drought events with SPI < -1 | Event count at `12`-month SPI scale using threshold `< -1.0`; baseline years `1981-2010` |

### 1.8 Drought Risk (Advanced)

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `spi3_drought_index` | Standardised Precipitation Index 3-month (SPI3) | SPI at `3`-month scale using baseline years `1981-2010` |
| `spi3_count_months_lt_minus1` | SPI3: Count of months with SPI < -1 (moderate drought) | Monthly count threshold `< -1.0`; baseline years `1981-2010` |
| `spi3_count_months_lt_minus2` | SPI3: Count of months with SPI < -2 (severe drought) | Monthly count threshold `< -2.0`; baseline years `1981-2010` |
| `spi6_drought_index` | Standardised Precipitation Index 6-month (SPI6) | SPI at `6`-month scale using baseline years `1981-2010` |
| `spi6_count_months_lt_minus1` | SPI6: Count of months with SPI < -1 (moderate drought) | Monthly count threshold `< -1.0`; baseline years `1981-2010` |
| `spi6_count_months_lt_minus2` | SPI6: Count of months with SPI < -2 (severe drought) | Monthly count threshold `< -2.0`; baseline years `1981-2010` |
| `spi12_drought_index` | Standardised Precipitation Index 12-month (SPI12) | SPI at `12`-month scale using baseline years `1981-2010` |
| `spi12_count_months_lt_minus1` | SPI12: Count of months with SPI < -1 (moderate drought) | Monthly count threshold `< -1.0`; baseline years `1981-2010` |
| `spi12_count_months_lt_minus2` | SPI12: Count of months with SPI < -2 (severe drought) | Monthly count threshold `< -2.0`; baseline years `1981-2010` |

### 1.9 Temperature Variability

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `dtr_daily_temp_range` | Daily Temperature Range (DTR) | No explicit threshold |
| `etr_extreme_temp_range` | Extreme Temperature Range (ETR) | No explicit threshold |

### 1.10 Population Exposure

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `population_total` | Total Population | No explicit threshold |
| `population_density` | Population Density | No explicit threshold |

### 1.11 Aqueduct Water Risk

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `aq_water_stress` | Aqueduct Water Stress | No explicit threshold in dashboard registry |
| `aq_interannual_variability` | Aqueduct Interannual Variability | No explicit threshold in dashboard registry |
| `aq_seasonal_variability` | Aqueduct Seasonal Variability | No explicit threshold in dashboard registry |
| `aq_water_depletion` | Aqueduct Water Depletion | No explicit threshold in dashboard registry |

### 1.12 Groundwater Status & Availability

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `gw_stage_extraction_pct` | Stage of Ground Water Extraction | No explicit threshold in dashboard registry |
| `gw_future_availability_ham` | Net Annual Ground Water Availability for Future Use | No explicit threshold in dashboard registry |
| `gw_extractable_resource_ham` | Annual Extractable Ground Water Resource | No explicit threshold in dashboard registry |
| `gw_total_extraction_ham` | Ground Water Extraction for All Uses | No explicit threshold in dashboard registry |

### 1.13 Flood Inundation Depth (JRC)

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `jrc_flood_depth_index_rp100` | Flood Severity Index (RP-100) | Derived severity class metric; no threshold exposed in registry table |
| `jrc_flood_extent_rp100` | RP-100 Flood Extent | No explicit threshold in registry; stored as area fraction |
| `jrc_flood_depth_rp10` | RP-10 Flood Depth | No explicit threshold |
| `jrc_flood_depth_rp50` | RP-50 Flood Depth | No explicit threshold |
| `jrc_flood_depth_rp100` | RP-100 Flood Depth | No explicit threshold |
| `jrc_flood_depth_rp500` | RP-500 Flood Depth | No explicit threshold |

### 1.14 Present in code but not currently assigned to a dashboard domain

These metrics already exist in the registry and may be useful later, but they are not currently attached to a displayed dashboard domain/bundle.

| Slug | Dashboard label | Threshold / rule currently used |
|---|---|---|
| `pr_consecutive_dry_days_lt1mm` | Consecutive Dry Days (CDD) | Maximum spell length of dry days with precipitation `< 1 mm` |

## 2. Metrics and thresholds prescribed in the proposal document

This section rewrites the document's prescribed indicators as an inventory. The wording here follows the proposal document, including the thresholds that are explicitly stated there.

### 2.1 Master list of all proposed indicators

This is a consolidated list of indicators that appear in the document's `Sector-specific Climate Risk Indicators` section. Where the same indicator is used with different thresholds or rule forms across sectors, those are listed below as separate entries.

| Proposed indicator | Threshold / rule prescribed in the document | Context in the document |
|---|---|---|
| Rx1day | `>= 150 mm` | Health Risk; Industrial Risk |
| Rx1day | `>= 200 mm` | Agricultural Risk; Health Risk; Infrastructure Risk; Life & Livelihood Loss Risk |
| Rx5day | `>= 250 mm` | Industrial Risk |
| Rx5day | `>= 300 mm` | Agricultural Risk; Infrastructure Risk; Asset Risk (Hydropower Plants) |
| Rx5day | `>= 400 mm` | Infrastructure Risk |
| Rx5day | `>= 500 mm` | Asset Risk (Hydropower Plants) |
| CDD | `>= 20 days` | Agricultural Risk |
| CDD | `>= 30 days` | Industrial Risk; Asset Risk (Thermal Power Plants) |
| CDD | `>= 40 days` | Life & Livelihood Loss Risk |
| CDD | `>= 60 days` | Asset Risk (Hydropower Plants) |
| CDD | Increasing trend, `> 20% over baseline` | Investment / Financial Risk |
| TXx | `>= 40°C` in plains | Agricultural Risk |
| TXx | `>= 45°C` | Health Risk; Industrial Risk; Infrastructure Risk; Asset Risk (Thermal Power Plants) |
| R95p change | `> 20% from baseline` | Agricultural Risk |
| TNx | `>= 30°C` | Health Risk |
| CWD | `>= 5 days` | Health Risk |
| Rx1day / Rx5day frequency | Increasing trend | Investment / Financial Risk |
| R99p | Increasing trend | Investment / Financial Risk |
| Heatwave days | Increasing IMD-defined trend | Investment / Financial Risk |
| Hourly rainfall | `>= 50 mm/hr` | Infrastructure Risk |
| River flow reduction linked to drought indices | No numeric threshold given | Asset Risk (Thermal Power Plants) |
| R95p variability | No numeric threshold given | Asset Risk (Hydropower Plants) |
| Multi-day heavy rainfall | `>= 2 consecutive days >= 150 mm` | Life & Livelihood Loss Risk |
| Heatwave duration | `>= 5 consecutive days >= 40°C` | Health Risk |
| Heatwave duration | `>= 5 days` | Life & Livelihood Loss Risk |

### 2.2 Bundle-specific metrics and thresholds from the proposal document

#### Agricultural Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx1day | `>= 200 mm` |
| Rx5day | `>= 300 mm` |
| CDD | `>= 20 days` |
| TXx | `>= 40°C` in plains |
| R95p increase | `> 20% from baseline` |

#### Health Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| TXx / heatwave | `>= 45°C` |
| Heatwave duration / heatwave | `>= 5 consecutive days >= 40°C` |
| TNx | `>= 30°C` |
| Rx1day | `>= 200 mm` |
| CWD | `>= 5 days` |

#### Industrial Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx1day | `>= 150 mm` |
| Rx5day | `>= 250 mm` |
| CDD | `>= 30 days` |
| TXx | `>= 45°C` |

#### Investment / Financial Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx1day / Rx5day frequency | Increasing trend |
| R99p | Increasing trend |
| CDD | Increasing trend, `> 20% over baseline` |
| Heatwave days | Increasing IMD-defined trend |

#### Infrastructure Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx1day | `>= 200 mm` |
| Rx5day | `>= 400 mm` |
| Hourly rainfall | `>= 50 mm/hr` |
| TXx | `>= 45°C` |

#### Asset Risk (Thermal Power Plants)

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| CDD | `>= 30 days` |
| TXx | `>= 45°C` |
| River flow reduction linked to drought indices | No numeric threshold given |

#### Asset Risk (Hydropower Plants)

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx5day | `>= 500 mm` |
| CDD | `>= 60 days` |
| R95p variability | No numeric threshold given |

#### Life & Livelihood Loss Risk

| Proposed metric | Threshold / rule prescribed in the document |
|---|---|
| Rx1day | `>= 200 mm` |
| Multi-day heavy rainfall | `>= 2 consecutive days >= 150 mm` |
| CDD | `>= 40 days` |
| Heatwave duration | `>= 5 days` |

## 3. Proposed indicators from section 2.1 that are absent from section 1

This section consolidates the indicators in section `2.1` that are not presently represented in section `1` as current dashboard metrics. Some of these are completely missing base metrics. Others are thresholded or rule-based derived indicators that are not currently encoded, even where a related raw metric already exists.

| Proposed indicator from section 2.1 | Why it is absent from section 1 | Closest current dashboard coverage |
|---|---|---|
| Rx1day | Section `1` has raw `Rx1day`, but not the thresholded sector-specific indicators at `>= 150 mm` and `>= 200 mm` | `pr_max_1day_precip` |
| Rx5day | Section `1` has raw `Rx5day`, but not the thresholded sector-specific indicators at `>= 250 mm`, `>= 300 mm`, `>= 400 mm`, and `>= 500 mm` | `pr_max_5day_precip` |
| CDD | Section `1` includes raw `CDD` in `1.14`, but not the sector-specific thresholded or trend-based versions | `pr_consecutive_dry_days_lt1mm` |
| TXx | Section `1` has raw `TXx`, but not the sector-specific thresholded indicators at `>= 40°C` in plains and `>= 45°C` | `txx_annual_max` |
| R95p change | Section `1` has raw `R95p`, but not the sector-specific change indicator `> 20% from baseline` | `r95p_very_wet_precip`, `r95ptot_contribution_pct` |
| TNx | Section `1` has raw `TNx`, but not the thresholded sector-specific indicator at `>= 30°C` | `tnx_annual_max` |
| CWD | Section `1` has raw `CWD`, but not the thresholded sector-specific indicator at `>= 5 days` | `cwd_consecutive_wet_days` |
| Rx1day / Rx5day frequency | No trend-based frequency indicator appears in section `1` | `pr_max_1day_precip`, `pr_max_5day_precip` |
| R99p | No `R99p` metric appears in section `1` | `r95p_very_wet_precip`, `r95ptot_contribution_pct` |
| Heatwave days | Section `1` has heatwave-related metrics, but not the sector-specific trend indicator named in the proposal document | `hwfi_tmean_90p`, `hwfi_events_tmean_90p`, `wsdi_warm_spell_days` |
| Hourly rainfall | No hourly rainfall metric appears in section `1` | No direct current dashboard equivalent |
| Low River Flow Months (SPI3 proxy) | No explicit low-river-flow proxy metric appears in section `1`; proposed definition is count of months in a year where `SPI3 < -1`, with higher values treated as worse | SPI drought metrics; Aqueduct water-risk metrics |
| R95p variability | Section `1` has raw `R95p`, but not the variability indicator named for hydropower risk | `r95p_very_wet_precip`, `r95ptot_contribution_pct` |
| Multi-day heavy rainfall | No dedicated multi-day heavy-rainfall trigger exists in section `1` | `pr_max_5day_precip`, `cwd_consecutive_wet_days` |
| Heatwave duration | Section `1` has heatwave-related duration metrics, but not the thresholded sector-specific versions used in section `2.1` | `wsdi_warm_spell_days`, `hwfi_tmean_90p` |

## 4. Implementation-ready metric reference

This section is the implementation-ready reference for incorporating the proposal metrics into the dashboard. It is populated from section `3`, but adds the implementation shape needed for code work:

- whether the proposal item is trend-based
- the threshold or rule that must be implemented
- whether the work is a threshold layer on an existing metric, a trend layer on existing yearly outputs, or a net-new compute primitive
- the closest functional reference in `tools/pipeline/compute_indices_multiprocess.py` when one exists

Implementation contract used in this section:

- Reuse existing percentile-based heat metrics instead of introducing new geography-specific absolute heatwave thresholds.
- Persist reusable new metrics and reusable derived metrics.
- Keep simple threshold checks as bundle-level derivations from persisted continuous metrics wherever practical.
- Exclude hourly-rainfall implementation because the current workflow only has daily raw inputs.

| Proposed metric to implement | Trend-based | Threshold / rule to implement | Implementation status | Implementation shape | Existing metric / closest dashboard coverage | Functional reference(s) |
|---|---|---|---|---|---|---|
| Rx1day | No | Thresholded sector-specific indicators at `>= 150 mm` and `>= 200 mm` | need only thresholding / derivation | Derive threshold flags / counts from existing raw `Rx1day` outputs; no new base climate metric required | `pr_max_1day_precip` | Registry slug `pr_max_1day_precip`; compute name `rx1day`; function `rx1day()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1523) |
| Rx5day | No | Thresholded sector-specific indicators at `>= 250 mm`, `>= 300 mm`, `>= 400 mm`, `>= 500 mm` | need only thresholding / derivation | Derive threshold flags / counts from existing raw `Rx5day` outputs; no new base climate metric required | `pr_max_5day_precip` | Registry slug `pr_max_5day_precip`; compute name `rx5day`; function `rx5day()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1527) |
| CDD | Mixed | Thresholded sector-specific indicators at `>= 20 days`, `>= 30 days`, `>= 40 days`, `>= 60 days`; trend rule `> 20% over baseline` | need only thresholding / derivation | Reuse raw `CDD` for thresholded variants; add trend/change logic for the investment-style rule | `pr_consecutive_dry_days_lt1mm` | Registry slug `pr_consecutive_dry_days_lt1mm`; compute name `consecutive_dry_days`; function `consecutive_dry_days()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1555) |
| TXx | No | Thresholded sector-specific indicators at `>= 40°C` in plains and `>= 45°C` | need only thresholding / derivation | Derive threshold flags / counts from existing raw `TXx` outputs; no new base climate metric required | `txx_annual_max` | Registry slug `txx_annual_max`; compute name `annual_max_temperature`; function `annual_max_temperature()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1125) |
| R95p change | Yes | `> 20% from baseline` | need only thresholding / derivation | Add change-vs-baseline logic on top of existing R95p outputs; likely a derived comparison metric rather than a new climate primitive | `r95p_very_wet_precip`, `r95ptot_contribution_pct` | Registry slug `r95p_very_wet_precip`; compute name `percentile_precipitation_total`; function `percentile_precipitation_total()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1566) |
| TNx | No | Thresholded sector-specific indicator at `>= 30°C` | need only thresholding / derivation | Derive threshold flags / counts from existing raw `TNx` outputs; no new base climate metric required | `tnx_annual_max` | Registry slug `tnx_annual_max`; compute name `annual_max_temperature`; function `annual_max_temperature()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1125) |
| CWD | No | Thresholded sector-specific indicator at `>= 5 days` | need only thresholding / derivation | Derive threshold flag from existing raw `CWD` outputs | `cwd_consecutive_wet_days` | Registry slug `cwd_consecutive_wet_days`; compute name `consecutive_wet_days`; function `consecutive_wet_days()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1549) |
| Rx1day / Rx5day frequency | Yes | Increasing trend | need only thresholding / derivation | Add trend computation over yearly `Rx1day` / `Rx5day` outputs; likely details-layer or derived-master logic rather than a new raw climate primitive | `pr_max_1day_precip`, `pr_max_5day_precip` | Functions `rx1day()` and `rx5day()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1523) and [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1527) |
| R99p | Yes | Increasing trend | need new compute | Net-new base metric plus trend logic if the proposal is to be implemented literally; current codebase has commented-out R99p scaffolding | Closest current coverage: `r95p_very_wet_precip`, `r95ptot_contribution_pct` | Commented registry stub `r99p_extreme_wet_precip` in [india_resilience_tool/config/metrics_registry.py](/mnt/d/projects/india_resilience_tool/india_resilience_tool/config/metrics_registry.py:1221); base primitive would align with `percentile_precipitation_total()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1566) |
| Heatwave days | Yes | Increasing trend; reuse `hwfi_tmean_90p` | need only thresholding / derivation | Reuse existing percentile-based heatwave-days metric and compute trend over yearly outputs instead of introducing literal geography-specific threshold logic | `hwfi_tmean_90p` | Existing function `heatwave_frequency_percentile()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1273); current registry slug `hwfi_tmean_90p` in [india_resilience_tool/config/metrics_registry.py](/mnt/d/projects/india_resilience_tool/india_resilience_tool/config/metrics_registry.py:704) |
| Hourly rainfall | No | `>= 50 mm/hr` | need new data source | Explicitly excluded from implementation with current daily-input workflow; do not implement unless hourly source data are onboarded later | No direct current dashboard equivalent | No current daily-pipeline function; `compute_indices_multiprocess.py` currently operates on daily data inputs |
| Low River Flow Months (SPI3 proxy) | No | Count of months in a year where `SPI3 < -1`; higher is worse | need only thresholding / derivation | Persist a reusable SPI3-derived low-flow proxy metric using the existing SPI3 threshold-count pattern | Closest current coverage: SPI drought metrics; Aqueduct water-risk metrics | Closest existing registry pattern: SPI3 threshold-count metrics in [india_resilience_tool/config/metrics_registry.py](/mnt/d/projects/india_resilience_tool/india_resilience_tool/config/metrics_registry.py:1360) |
| R95p variability | No | Interannual CV of yearly `R95p`; if mean `R95p` is too small for stable CV, fall back to interannual standard deviation | need new compute | New derived variability metric on top of existing yearly `R95p` outputs using CV as the default hydrology-oriented variability measure, with SD fallback for low-mean cases | `r95p_very_wet_precip`, `r95ptot_contribution_pct` | Base function `percentile_precipitation_total()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1566) |
| Multi-day heavy rainfall | No | `>= 2 consecutive days >= 150 mm` | need new compute | Net-new event-style metric; related to Rx / wet-spell logic but not currently encoded as written | Closest current coverage: `pr_max_5day_precip`, `cwd_consecutive_wet_days` | No direct current function; adjacent functions are `rx5day()` and `consecutive_wet_days()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1527) and [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1549) |
| Heatwave duration | No | Reuse `wsdi_warm_spell_days` for proposal-aligned heatwave duration handling | need only thresholding / derivation | Reuse existing percentile-based spell-duration metric instead of introducing literal geography-specific threshold logic | `wsdi_warm_spell_days` | Existing heatwave helpers include `warm_spell_duration_index()` in [tools/pipeline/compute_indices_multiprocess.py](/mnt/d/projects/india_resilience_tool/tools/pipeline/compute_indices_multiprocess.py:1197) |

### 4.1 Persistence contract

Persist these as reusable metrics:

- `R99p`
- `Low River Flow Months (SPI3 proxy)`
- `R95p variability`

Keep these as bundle-level derivations from persisted continuous metrics:

- thresholded `Rx1day` rules
- thresholded `Rx5day` rules
- thresholded `CDD` rules
- thresholded `TXx` rules
- thresholded `TNx` rules
- thresholded `CWD` rules
- `R95p change > 20% from baseline`
- trend interpretations such as increasing `Rx1day / Rx5day frequency`

Rationale:

- Persist continuous or reusable derived metrics when they have standalone analytical value or are likely to be reused across bundles and views.
- Avoid persisting every thresholded bundle rule, because that would create unnecessary metric sprawl when the dashboard can derive those rules from already-persisted continuous metrics.

### 4.2 Explicit exclusions

- `Hourly rainfall >= 50 mm/hr` is excluded from implementation in the current workflow because only daily raw inputs are available.

## 5. Notes for the next comparison step

- The dashboard already contains strong coverage for `Rx1day`, `Rx5day`, `R20mm`, `R95p`, `R95pTOT`, `CWD`, `TXx`, `TNx`, `TN90p`, wet-bulb heat metrics, SPI drought metrics, and several exposure / water-risk metrics.
- The document introduces several thresholded sector bundles that do not yet exist as dashboard bundles.
- The main immediately relevant gap is `CDD`: it already exists in code as `pr_consecutive_dry_days_lt1mm`, but it is not yet attached to a current dashboard domain.
- Additional possible gaps relative to the sector-specific proposal list include `R99p`, hourly rainfall thresholds, `R95p` variability, and explicit thresholded heatwave-duration logic.
