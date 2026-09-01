# Proposal Bundle Methodology: Sector Climate Hazard Pressure v2

Author: Abu Bakar Siddiqui Thakur  
Email: absthakur@resilience.org.in

## Purpose

Proposal / sector-wise bundles are Phase-1 **sector climate hazard-pressure scores**. They are intended to screen where climate hazards create sector-relevant pressure for agriculture, health, industry, infrastructure, investments, assets, and life/livelihoods.

They are **not** full sectoral risk scores yet. Exposure, vulnerability, and adaptive capacity are not included in this compute path. Later risk modules can use these scores as the sector-specific hazard component in a wider risk equation.

## Score interpretation

Each proposal-bundle rule is scored from `0` to `100`.

- `0` means low relative sector hazard pressure for the selected state, level, scenario, and period.
- `100` means high relative sector hazard pressure for the selected state, level, scenario, and period.
- Higher is always worse.
- Missing or invalid data returns `NaN` for the affected component or rule.
- Bundle means are computed from available rule scores. Legacy equal-weight
  fallback remains available in code, but all active sector-wise proposal
  bundles now use explicit normalized rule weights.
- `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Investment / Financial Risk`,
  `Infrastructure Risk`, `Asset Risk (Thermal Power Plants)`,
  `Asset Risk (Hydropower Plants)`, and
  `Life & Livelihood Loss Risk` use explicit
  normalized rule weights and a 0.70 minimum `available_rule_weight_fraction`;
  rows below that coverage gate are set to `NaN`. This covers all 8
  sector-wise bundles.

## Rule score components

Each rule can combine three scientifically distinct components.

Sectoral bundles (`Agricultural Risk`, `Health Risk`, `Industrial Risk`,
`Investment / Financial Risk`, `Infrastructure Risk`,
`Asset Risk (Thermal Power Plants)`,
`Asset Risk (Hydropower Plants)`, and
`Life & Livelihood Loss Risk`) use lens-decomposed scoring per
`docs/lens_scoring_methodology.md`. Agricultural Risk reinstates the impact
lens with a self-derived TXx 35-45 deg C agronomic band (§12.1) replacing
the previously-retired 40-45 deg C IMD heatwave band, plus self-derived
bands on the six other rules (§12.2-§12.7). Infrastructure Risk now follows
§9 with explicit rule weights across `rx1day_ge_200`, `rx5day_ge_400`, and
`txx_ge_45`, keeping the `rx5day_ge_400` slug stable until a separate
data-contract rename is approved. Investment / Financial Risk adds five
dossier-§8 lens rules with explicit bundle weights; its R99p rule is the
current exception that intentionally omits the impact component because no
defensible danger band exists for that regime metric. Life & Livelihood
Loss Risk follows §13 with HIGH-confidence Rx1day (IMD daily-rainfall
categories) and MEDIUM-confidence CDD and WSDI bands anchored to IMD
agro-met / ICAR-CRIDA / NDMA evidence and to Indian heat-mortality
literature (Azhar 2014; Mazdiyasni 2017) respectively; the `cdd_ge_40` and
`wsdi_ge_5` slugs are stable until the same later data-contract rename.
Asset Risk (Thermal Power Plants) follows §10 (CHG-0057/0058) with CDD
cooling-water pressure (`cdd_ge_30`, lens 0.40/0.30/0.30, band 30-90 days,
weight 0.35), TXx cooling-efficiency pressure (`txx_ge_45`, lens
0.40/0.25/0.35, band 40-45 deg C, weight 0.35), and SPI-3 low-flow proxy
pressure (`spi3_low_flow_proxy_norm`, lens 0.70/0.30/0.00, no impact band,
weight 0.30). Its SPI change lens is operational against the currently
available historical baseline column; public slugs remain stable until the
separate data-contract rename track lands.
Asset Risk (Hydropower Plants) follows §11 (CHG-0036) with a flood/storm
cluster of `rx5day_ge_500` (lens 0.45/0.40/0.15, band 250-500 mm/5 days,
weight 0.45) and the helper-derived `r95p_interannual_variability_norm`
regime rule (lens 0.70/0.30/0.00, no impact band, absolute-dominant, weight
0.20), plus the drought-cluster `cdd_ge_60` (lens 0.40/0.30/0.30, band 30-90
days, weight 0.35). The `r95p_interannual_variability` helper now emits a
canonical `r95p_interannual_variability__historical__1990-2010__mean`
baseline column, with legacy Rx5day/CDD source-master epochs tolerated only as
fallback, so its change lens is operational rather than cosmetic. The code
slugs (`rx5day_ge_500`, `cdd_ge_60`, `r95p_interannual_variability_norm`) and
the helper grid-first / CV-vs-sigma provenance caveat remain deferred under
CHG-0024.

### 1. Future absolute severity

This component answers: **How severe is the future condition in this geography?**

The builder scores the selected future period value against the relevant state/level/scenario/period distribution using robust p10-p90 scaling. This preserves relative hotspot information while reducing sensitivity to outliers.

### 2. Change vs historical baseline

This component answers: **How much is this geography changing relative to its own historical baseline?**

The builder aligns the future value to the available historical baseline column and computes either:

- absolute delta, usually preferred for temperature-like metrics; or
- relative percent change, usually preferred for precipitation/count-like metrics when the baseline denominator is safely non-zero.

If no baseline column is available, the change component is returned as `NaN` and a non-fatal build warning is emitted.

### 3. Impact-threshold pressure

This component answers: **Is the metric approaching or crossing a sector-relevant impact band?**

Impact thresholds are used only where the configuration declares a low/high impact band. The score ramps continuously from `0` at the lower concern threshold to `100` at the severe threshold. The current Phase-1 configuration mixes external and self-derived bands across heat, rainfall, and spell metrics where a defensible onset/severe range exists; rules without a defensible danger band omit the impact component.

## Why binary thresholds were replaced

The previous proposal-bundle rules used brittle binary scoring such as `0` below a threshold and `100` above it. Under the current persisted master-data contract, several rules saturated: some rainfall rules often collapsed to `0`, while several heat/dry/wet-spell rules often collapsed to `100`. This made whole proposal bundles flat and reduced their value for rankings and hotspot screening.

The v2 method replaces these binary rules with continuous pressure scores.

## Trend rules

Legacy trend-rule machinery remains in `india_resilience_tool/compute/proposal_bundles.py` for compatibility and regression coverage, but no active proposal bundle currently uses it. Active sector bundles score future levels, change vs historical baseline, and where defensible impact-threshold pressure from period means rather than within-horizon yearly slopes.

## Quality diagnostics

The builder emits non-fatal warnings when persisted score columns appear flat or saturated:

- flat score columns with only one unique valid value;
- saturated score columns where nearly all valid units are close to `0` or `100`;
- missing historical baseline columns for active change components.

These diagnostics are intended to prevent silent reintroduction of collapsed proposal-bundle outputs.

## Future risk integration

This Phase-1 output should feed future risk work as the sector hazard term:

```text
Sectoral risk = sector hazard pressure × exposure × vulnerability × lack-of-adaptive-capacity modifier
```

Until exposure, vulnerability, and adaptive capacity are included, UI and documentation should describe these outputs as **sector climate hazard pressure** or **sector risk-screening scores based on climate hazard pressure**.
