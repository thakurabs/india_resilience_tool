# Aqueduct Field Contract

This note records the Aqueduct source fields currently used for the onboarded
Aqueduct district and hydro metrics in the India Resilience Tool.

It is meant to answer a narrow but important question:

- which Aqueduct fields are currently being used
- where they come from
- how they are interpreted in the dashboard

## Current onboarded metrics

- `aq_water_stress`
- `aq_interannual_variability`
- `aq_seasonal_variability`
- `aq_water_depletion`

All four metrics currently follow the same dashboard contract:

- historical baseline from `baseline_clean_india.geojson`
- future scenario values from `future_annual_india.geojson`
- scenarios: `historical`, `bau`, `opt`, `pes`
- periods: `1979-2019`, `2030`, `2050`, `2080`
- rendering on:
  - SOI `basin`
  - SOI `sub-basin`
  - district admin units via direct `pfaf_id -> district` transfer

## Metric mappings

### Aqueduct Water Stress

| Dashboard column | Aqueduct source column | Source dataset | Scenario | Period | Interpretation |
|---|---|---|---|---|---|
| `aq_water_stress__historical__1979-2019__mean` | `bws_raw` | `baseline_clean_india.geojson` | `historical` | `1979-2019` | Baseline annual water stress screening indicator |
| `aq_water_stress__bau__2030__mean` | `bau30_ws_x_r` | `future_annual_india.geojson` | `bau` | `2030` | Business-as-usual future annual water stress screening indicator |
| `aq_water_stress__bau__2050__mean` | `bau50_ws_x_r` | `future_annual_india.geojson` | `bau` | `2050` | Business-as-usual future annual water stress screening indicator |
| `aq_water_stress__bau__2080__mean` | `bau80_ws_x_r` | `future_annual_india.geojson` | `bau` | `2080` | Business-as-usual future annual water stress screening indicator |
| `aq_water_stress__opt__2030__mean` | `opt30_ws_x_r` | `future_annual_india.geojson` | `opt` | `2030` | Optimistic future annual water stress screening indicator |
| `aq_water_stress__opt__2050__mean` | `opt50_ws_x_r` | `future_annual_india.geojson` | `opt` | `2050` | Optimistic future annual water stress screening indicator |
| `aq_water_stress__opt__2080__mean` | `opt80_ws_x_r` | `future_annual_india.geojson` | `opt` | `2080` | Optimistic future annual water stress screening indicator |
| `aq_water_stress__pes__2030__mean` | `pes30_ws_x_r` | `future_annual_india.geojson` | `pes` | `2030` | Pessimistic future annual water stress screening indicator |
| `aq_water_stress__pes__2050__mean` | `pes50_ws_x_r` | `future_annual_india.geojson` | `pes` | `2050` | Pessimistic future annual water stress screening indicator |
| `aq_water_stress__pes__2080__mean` | `pes80_ws_x_r` | `future_annual_india.geojson` | `pes` | `2080` | Pessimistic future annual water stress screening indicator |

### Aqueduct Interannual Variability

| Dashboard column | Aqueduct source column | Source dataset | Scenario | Period | Interpretation |
|---|---|---|---|---|---|
| `aq_interannual_variability__historical__1979-2019__mean` | `iav_raw` | `baseline_clean_india.geojson` | `historical` | `1979-2019` | Baseline interannual variability screening indicator |
| `aq_interannual_variability__bau__2030__mean` | `bau30_iv_x_r` | `future_annual_india.geojson` | `bau` | `2030` | Business-as-usual future interannual variability screening indicator |
| `aq_interannual_variability__bau__2050__mean` | `bau50_iv_x_r` | `future_annual_india.geojson` | `bau` | `2050` | Business-as-usual future interannual variability screening indicator |
| `aq_interannual_variability__bau__2080__mean` | `bau80_iv_x_r` | `future_annual_india.geojson` | `bau` | `2080` | Business-as-usual future interannual variability screening indicator |
| `aq_interannual_variability__opt__2030__mean` | `opt30_iv_x_r` | `future_annual_india.geojson` | `opt` | `2030` | Optimistic future interannual variability screening indicator |
| `aq_interannual_variability__opt__2050__mean` | `opt50_iv_x_r` | `future_annual_india.geojson` | `opt` | `2050` | Optimistic future interannual variability screening indicator |
| `aq_interannual_variability__opt__2080__mean` | `opt80_iv_x_r` | `future_annual_india.geojson` | `opt` | `2080` | Optimistic future interannual variability screening indicator |
| `aq_interannual_variability__pes__2030__mean` | `pes30_iv_x_r` | `future_annual_india.geojson` | `pes` | `2030` | Pessimistic future interannual variability screening indicator |
| `aq_interannual_variability__pes__2050__mean` | `pes50_iv_x_r` | `future_annual_india.geojson` | `pes` | `2050` | Pessimistic future interannual variability screening indicator |
| `aq_interannual_variability__pes__2080__mean` | `pes80_iv_x_r` | `future_annual_india.geojson` | `pes` | `2080` | Pessimistic future interannual variability screening indicator |

### Aqueduct Seasonal Variability

| Dashboard column | Aqueduct source column | Source dataset | Scenario | Period | Interpretation |
|---|---|---|---|---|---|
| `aq_seasonal_variability__historical__1979-2019__mean` | `sev_raw` | `baseline_clean_india.geojson` | `historical` | `1979-2019` | Baseline seasonal variability screening indicator |
| `aq_seasonal_variability__bau__2030__mean` | `bau30_sv_x_r` | `future_annual_india.geojson` | `bau` | `2030` | Business-as-usual future seasonal variability screening indicator |
| `aq_seasonal_variability__bau__2050__mean` | `bau50_sv_x_r` | `future_annual_india.geojson` | `bau` | `2050` | Business-as-usual future seasonal variability screening indicator |
| `aq_seasonal_variability__bau__2080__mean` | `bau80_sv_x_r` | `future_annual_india.geojson` | `bau` | `2080` | Business-as-usual future seasonal variability screening indicator |
| `aq_seasonal_variability__opt__2030__mean` | `opt30_sv_x_r` | `future_annual_india.geojson` | `opt` | `2030` | Optimistic future seasonal variability screening indicator |
| `aq_seasonal_variability__opt__2050__mean` | `opt50_sv_x_r` | `future_annual_india.geojson` | `opt` | `2050` | Optimistic future seasonal variability screening indicator |
| `aq_seasonal_variability__opt__2080__mean` | `opt80_sv_x_r` | `future_annual_india.geojson` | `opt` | `2080` | Optimistic future seasonal variability screening indicator |
| `aq_seasonal_variability__pes__2030__mean` | `pes30_sv_x_r` | `future_annual_india.geojson` | `pes` | `2030` | Pessimistic future seasonal variability screening indicator |
| `aq_seasonal_variability__pes__2050__mean` | `pes50_sv_x_r` | `future_annual_india.geojson` | `pes` | `2050` | Pessimistic future seasonal variability screening indicator |
| `aq_seasonal_variability__pes__2080__mean` | `pes80_sv_x_r` | `future_annual_india.geojson` | `pes` | `2080` | Pessimistic future seasonal variability screening indicator |

### Aqueduct Water Depletion

| Dashboard column | Aqueduct source column | Source dataset | Scenario | Period | Interpretation |
|---|---|---|---|---|---|
| `aq_water_depletion__historical__1979-2019__mean` | `bwd_raw` | `baseline_clean_india.geojson` | `historical` | `1979-2019` | Baseline water depletion screening indicator |
| `aq_water_depletion__bau__2030__mean` | `bau30_wd_x_r` | `future_annual_india.geojson` | `bau` | `2030` | Business-as-usual future water depletion screening indicator |
| `aq_water_depletion__bau__2050__mean` | `bau50_wd_x_r` | `future_annual_india.geojson` | `bau` | `2050` | Business-as-usual future water depletion screening indicator |
| `aq_water_depletion__bau__2080__mean` | `bau80_wd_x_r` | `future_annual_india.geojson` | `bau` | `2080` | Business-as-usual future water depletion screening indicator |
| `aq_water_depletion__opt__2030__mean` | `opt30_wd_x_r` | `future_annual_india.geojson` | `opt` | `2030` | Optimistic future water depletion screening indicator |
| `aq_water_depletion__opt__2050__mean` | `opt50_wd_x_r` | `future_annual_india.geojson` | `opt` | `2050` | Optimistic future water depletion screening indicator |
| `aq_water_depletion__opt__2080__mean` | `opt80_wd_x_r` | `future_annual_india.geojson` | `opt` | `2080` | Optimistic future water depletion screening indicator |
| `aq_water_depletion__pes__2030__mean` | `pes30_wd_x_r` | `future_annual_india.geojson` | `pes` | `2030` | Pessimistic future water depletion screening indicator |
| `aq_water_depletion__pes__2050__mean` | `pes50_wd_x_r` | `future_annual_india.geojson` | `pes` | `2050` | Pessimistic future water depletion screening indicator |
| `aq_water_depletion__pes__2080__mean` | `pes80_wd_x_r` | `future_annual_india.geojson` | `pes` | `2080` | Pessimistic future water depletion screening indicator |

## Interpretation notes

- The dashboard currently compares the selected Aqueduct scenario value against the historical Aqueduct baseline for each onboarded metric.
- That means the displayed change is a **scenario-versus-baseline Aqueduct change**, not a pure climate-only delta.
- This tranche treats the baseline and future annual Aqueduct fields above as comparable screening indicators for the purpose of spatial transfer and dashboard comparison.
- Aqueduct remains a screening product. High-stakes interpretation should be paired with local review and, where possible, source documentation checks.

## Official references

- [Aqueduct 4.0: Updated decision-relevant global water risk indicators](https://www.wri.org/research/aqueduct-40-updated-decision-relevant-global-water-risk-indicators)
- [Aqueduct 4.0 current and future global maps data](https://www.wri.org/data/aqueduct-global-maps-40-data)
- [Aqueduct FAQ](https://www.wri.org/aqueduct/faq)
