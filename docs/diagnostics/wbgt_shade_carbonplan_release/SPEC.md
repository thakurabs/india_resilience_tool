# CarbonPlan shade WBGT national release specification (CHG-0700)

Frozen before national holdout scoring. This release supersedes the unpublished
`shade-peak-v1` stage. The four public slugs remain
`wbgt_shade_stull_annual_mean` and `wbgt_shade_stull_days_ge_28/30/32`;
the artifact identity, rather than the historical slug, names the method.

## Inputs and estimand

- Source: the signed Route B grid-cell QDM cache, not a new shade calculation.
  Route B's underlying CarbonPlan shade calculation uses `tas`, `tasmax`,
  `huss`, and pressure derived from cell elevation. The shade release must
  record all three climate variables and the elevation source.
- Roster: ACCESS-CM2, ACCESS-ESM1-5, CMCC-CM2-SR5, CMCC-ESM2, CanESM5,
  EC-Earth3, EC-Earth3-Veg-LR, GFDL-CM4, GFDL-ESM4, INM-CM4-8, INM-CM5-0,
  KIOST-ESM, MPI-ESM1-2-HR, MPI-ESM1-2-LR, MRI-ESM2-0, NorESM2-LM,
  NorESM2-MM, TaiESM1. IPSL-CM6A-LR, MIROC6 and KACE-1-0-G are excluded.
- Support: Route B's signed `cell_support.csv`, `unit_support.csv`, and
  `admin_areas.npz`; unit area coverage must be at least 0.95. Weights are
  normalized once over eligible contributing cells. A missing contributing
  cell-year makes the unit-year missing. Excluded units remain in the roster
  with a reason and missing values.
- Calendar: reconstruct dates from signed driver-year shards, verify cache
  lengths, drop 29 February, and require complete 365-day cell-years.
- Cell annual metrics: mean °C and inclusive counts of days at or above
  28, 30, and 32 °C. Area-weight these **cell-year metrics** to admin units.
  Fractional unit counts are expected. State and national count headlines use
  covered-area weights and report covered area and contributing-unit count.
  Only `n_models` uses the minimum across units.
- Release years: historical 1985–2014 and both SSPs 2020–2080. The historical
  reporting baseline remains 1990–2010. Reporting periods retain
  2020–2040, 2040–2060 and 2060–2080 (inclusive). Report unit-level steps
  across the 2039/2040 and 2059/2060 QDM adjustment seams.

## Holdout acceptance rule

The new method is the cache's `holdout` shade series, trained only on
1985–1999 and applied to 2000–2014. The incumbent is the currently deployed,
original-formula yearly shade data. Score **2000–2010** only, using common
models, years, and supported admin units against the same UHE reference. Never
score the released QDM historical series here: its 1985–2014 fit makes these
years in-sample. Report model and unit attrition.

Calculate UHE annual threshold counts per cell before area transfer. Preflight
and record whether the deployed legacy counts also thresholded per cell before
aggregation. Report any aggregation-order difference without adjusting scores.
The four acceptance metrics are annual mean and days at or above 28/30/32 °C.
For each unit, `delta = abs(error_holdout) - abs(error_deployed)`; indifference
bands are 0.05 °C for mean and 0.5 days/year for counts. A state × model × metric
or national cut is worsened only if at least two-thirds of classified units
worsen **and** the 90% bootstrap interval for the change in median absolute
error lies above zero. Use 1,000 fixed-seed whole-year draws from the 11-year
overlap, shared between methods; draw reference years independently. Report
zero-event and sparse-event support separately. Any worsened class blocks
automatic publication pending an explicit user ruling. QDM versus uncorrected
CarbonPlan shade is a separate diagnostic.

For this gate, "classified" means a common-supported unit with all 11 annual
values in each of the three methods, including units inside the indifference
band (`unchanged`). Missing units are excluded and counted as attrition. The
bootstrap statistic is `median(abs(error_holdout)) -
median(abs(error_deployed))` across those classified units; each error compares
the resampled method mean with the independently resampled reference mean.
The same model-year draw is applied to holdout and deployed; all units in one
draw share its years so spatial dependence is preserved. Use seed `20261010`.

## End-to-end control and publication

For at least one complete model, add Route B's sun adjustment to the same cached
shade and reproduce its validated historical and both-SSP outdoor annual CSVs
at CSV round-trip precision for every eligible unit-year. Stage and fingerprint
all four shade processed and optimised trees before production replacement.
The cutover must preserve and journal the shared manifest, parity report and
identity sidecar with the directories. The seven-day rollback window begins at
verified publication; it closes only after seven consecutive days **and**
explicit user sign-off.
