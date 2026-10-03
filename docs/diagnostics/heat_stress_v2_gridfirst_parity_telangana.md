# Heat Stress v2 Grid-First Parity Diagnostic: Telangana

Status: diagnostic utility added; pilot artifact comparison not run in this change because processed climate rebuilds require separate approval.

## Scope

Required metrics:

| Metric |
|---|
| `twb_annual_mean` |
| `twb_summer_mean` |
| `twb_annual_max` |
| `twb_days_ge_28` |
| `twb_days_ge_30` |
| `tasmin_tropical_nights_gt28` |

The diagnostic compares legacy polygon-mean-first extracts against Heat Stress v2 grid-first extracts for Telangana and reports summary deltas, rank-shift summary, and top movers.

## Command Template

```bash
python -m tools.diagnostics.heat_stress_gridfirst_parity --state Telangana --legacy twb_annual_mean=<legacy_twb_annual_mean.csv> --gridfirst twb_annual_mean=<gridfirst_twb_annual_mean.csv> --legacy twb_summer_mean=<legacy_twb_summer_mean.csv> --gridfirst twb_summer_mean=<gridfirst_twb_summer_mean.csv> --legacy twb_annual_max=<legacy_twb_annual_max.csv> --gridfirst twb_annual_max=<gridfirst_twb_annual_max.csv> --legacy twb_days_ge_28=<legacy_twb_days_ge_28.csv> --gridfirst twb_days_ge_28=<gridfirst_twb_days_ge_28.csv> --legacy twb_days_ge_30=<legacy_twb_days_ge_30.csv> --gridfirst twb_days_ge_30=<gridfirst_twb_days_ge_30.csv> --legacy tasmin_tropical_nights_gt28=<legacy_tasmin_tropical_nights_gt28.csv> --gridfirst tasmin_tropical_nights_gt28=<gridfirst_tasmin_tropical_nights_gt28.csv> --report-out docs/diagnostics/heat_stress_v2_gridfirst_parity_telangana.md
```

## Reviewed Evidence

Pending pilot rebuild and extract comparison.

## Validation Notes

- The tool is non-destructive and reads caller-supplied CSV extracts.
- It does not rebuild processed artifacts.
- It should be run after the six Heat Stress-only grid-first metrics have been recomputed for Telangana.
