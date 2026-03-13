# Aqueduct Field Contract

This note records the Aqueduct source fields currently used for the `aq_water_stress` onboarding tranche in the India Resilience Tool.

It is meant to answer a narrow but important question:

- which Aqueduct fields are currently being used
- where they come from
- how they are interpreted in the dashboard

## Current mapping

| Dashboard column | Aqueduct source column | Source dataset | Scenario | Period | Interpretation |
|---|---|---|---|---|---|
| `aq_water_stress__historical__1979-2019__mean` | `bws_raw` | `baseline_clean_india.geojson` | `historical` | `1979-2019` | Baseline annual water stress screening indicator |
| `aq_water_stress__bau__2030__mean` | `bau30_ws_x_r` | `future_annual_india.geojson` | `bau` | `2030` | Business-as-usual future water stress |
| `aq_water_stress__bau__2050__mean` | `bau50_ws_x_r` | `future_annual_india.geojson` | `bau` | `2050` | Business-as-usual future water stress |
| `aq_water_stress__bau__2080__mean` | `bau80_ws_x_r` | `future_annual_india.geojson` | `bau` | `2080` | Business-as-usual future water stress |
| `aq_water_stress__opt__2030__mean` | `opt30_ws_x_r` | `future_annual_india.geojson` | `opt` | `2030` | Optimistic future water stress |
| `aq_water_stress__opt__2050__mean` | `opt50_ws_x_r` | `future_annual_india.geojson` | `opt` | `2050` | Optimistic future water stress |
| `aq_water_stress__opt__2080__mean` | `opt80_ws_x_r` | `future_annual_india.geojson` | `opt` | `2080` | Optimistic future water stress |
| `aq_water_stress__pes__2030__mean` | `pes30_ws_x_r` | `future_annual_india.geojson` | `pes` | `2030` | Pessimistic future water stress |
| `aq_water_stress__pes__2050__mean` | `pes50_ws_x_r` | `future_annual_india.geojson` | `pes` | `2050` | Pessimistic future water stress |
| `aq_water_stress__pes__2080__mean` | `pes80_ws_x_r` | `future_annual_india.geojson` | `pes` | `2080` | Pessimistic future water stress |

## Interpretation notes

- The dashboard currently compares the selected Aqueduct scenario value against the historical Aqueduct baseline.
- That means the displayed change is a **scenario-versus-baseline Aqueduct water-stress change**, not a pure climate-only delta.
- This onboarding tranche treats the baseline and future annual water-stress fields as comparable screening indicators for the purpose of spatial transfer and dashboard comparison.
- Aqueduct remains a screening product. High-stakes interpretation should be paired with local review and, where possible, source documentation checks.

## Official references

- [Aqueduct 4.0: Updated decision-relevant global water risk indicators](https://www.wri.org/research/aqueduct-40-updated-decision-relevant-global-water-risk-indicators)
- [Aqueduct 4.0 current and future global maps data](https://www.wri.org/data/aqueduct-global-maps-40-data)
- [Aqueduct FAQ](https://www.wri.org/aqueduct/faq)
