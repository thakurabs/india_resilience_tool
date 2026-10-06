# CarbonPlan shade-WBGT pilot: frozen method (CHG-0653)

Status: FROZEN 2026-10-05, before any pilot run. Any later change is an
amendment recorded at the end of this file with its reason; nothing is tuned to
results.

This is a bounded **pilot**, not a release. It is not the national method, it is
not selected, and it does not replace `shade-peak-v1`. Two models are **not
enough for a release ensemble**.

## Scope

| Item | Value |
|---|---|
| States | Kerala, Rajasthan, Himachal Pradesh |
| Model roster (explicit, closed) | ACCESS-CM2, CMCC-CM2-SR5 (r1i1p1f1, NEX-GDDP-CMIP6 v1.0, local files) |
| Scenarios | historical, ssp245 |
| Admin levels | district, block (IRT staging boundaries `districts_4326.geojson`, `blocks_4326.geojson`) |
| Metrics | `wbgt_shade_stull_annual_mean`, `wbgt_shade_stull_days_ge_28`, `_30`, `_32` (the four IRT shade slugs) |
| Output root | `scratch/carbonplan_shade_pilot/processed/` (isolated; never the staging tree or `IRT_DATA_DIR`) |

## Method signature

Every row written carries

```
carbonplan-shade-pilot-v1:tasmax,tas,huss+elevation-pressure:rh-at-tasmax(xclim-0.44):thermofeel-1.3-wbt+bgt-iso3-wind0.5:population-weighted-cells:qdm-additive-100q-doy31-vs-uhe-daily-train-1985-2014:apply-1985-2014|2020-2039|2040-2059:noleap-train-gregorian-out-drop-feb29:complete-365:inclusive-ge:count-first-area-transfer:union-coverage:overlap-excluded
```

It deliberately differs from `shade-peak-v1:...`. The IRT reader
(`wbgt_contract.require_shade_signature`) therefore **refuses** these files, so
they cannot be mixed into a shade master by accident.

## Frozen settings

1. **Raw shade WBGT per 0.25° cell**, CarbonPlan notebook 02, run unchanged in the
   pinned env (Python 3.10, xclim 0.44.0, thermofeel 1.3.0):
   - pressure from CarbonPlan elevation, `101325·10^(−z/(18400·tas/273.15))`;
   - RH at `tasmax` from `huss` and that pressure;
   - `0.7·WBT + 0.2·BGT + 0.1·Tmax`, with wind fixed at 0.5 m/s.
2. **Cells to CarbonPlan units**: CarbonPlan's own intersection-area × GHS-POP
   **population weights** (`cells.csv`, `population_weight`). Null-elevation cells
   are dropped before normalising, as in notebook 05. City polygons are units.
3. **QDM** is CarbonPlan notebook 06's `train_bias_correction`, AST-loaded from the
   checksum-locked notebook (commit f662b372):
   - additive, 100 quantiles, `time.dayofyear` grouping with a 31-day window;
   - trained once per unit and model on **1985–2014** against UHE-Daily.
   This is production-style calibration. The holdout experiment
   (train 1985–1999 / test 2000–2014) is **not** reused for any pilot value.
4. **Adjustment windows** are CarbonPlan's published windows. Each is adjusted in
   **its own call** with the same trained model:
   - historical 1985–2014;
   - ssp245 2020–2039;
   - ssp245 2040–2059.
   QDM output depends on the application window (validation A3: up to 3.4 °C).
   Changing any window is therefore a method change.
5. **Calendar**:
   - training converts the reference and the model to no-leap, then interpolates
     the model's gaps;
   - adjusted output is converted to Gregorian, and the Feb 29 gaps are linearly
     interpolated (notebook 06's `apply_bias_correction`);
   - IRT reporting then **drops Feb 29**;
   - a unit-year counts only with all 365 days finite. Otherwise it is missing,
     never zero.
6. **Metrics per CarbonPlan unit-year, computed before any spatial transfer**:
   - the annual mean of the daily series;
   - `days_ge_T` = count of days with WBGT ≥ T, for T = 28, 30, 32 °C, inclusive
     and unrounded.
   Thresholds are not changed, tails are not clipped, and no other correction
   method is substituted. ≥32 °C is retained even though it deteriorated in the
   Rajasthan holdout (validation README).
7. **Count-first transfer to admin units**:
   - each admin value is the intersection-area-weighted mean of its units' metric
     values (equal-area CRS ESRI:53034), giving "area-weighted regional exceedance
     days (fractional)";
   - a unit with a missing unit-year contributes nothing, and its area counts as
     uncovered.
8. **Coverage** is the area of the **union** of the admin unit's intersections
   divided by the admin area. The validation's summed-intersection coverage
   (>100% at Basantpur, Mashobra, Kothamangalam and Shimla district) is retired
   (CHG-0652).
9. **Overlap**:
   - an admin unit whose source polygons overlap by more than 1e-6 of its area
     (the declared vertex-noise floor) is **excluded**: its metrics are written
     as missing, with `pilot_exclusion_reason = unresolved_source_polygon_overlap`;
   - no ownership rule is chosen, and weights are not renormalised to hide the
     overlap;
   - every overlap, including those below the floor, is reported.
10. **Reporting periods**:
    - historical: 1990–2010, IRT's baseline, within the 1985–2014 adjusted window;
    - ssp245: **2020–2039 and 2040–2059**, the frozen adjustment windows.
    IRT's labelled 2020–2040 / 2040–2060 periods would need 2060, which no
    published window adjusts, so they are not produced. Yearly files carry every
    adjusted year.

## Verification required before results are read

- **Raw formula (G1)**: for every unit and model, the vectorised raw series must
  equal the reproduction's `raw_shade` to ≤1e-9 °C.
- **QDM parity against CarbonPlan's published product**:
  - **ACCESS-CM2, Kerala**: historical, ssp245-2030 and ssp245-2050, from the
    cache.
  - **CMCC-CM2-SR5, Kerala**: historical, to test the no-leap calendar path.
    This is a new ≈320 MB fetch booked on the task's 2 GB ledger, after
    614.8 MB already used.
  - The rule is the validation's G2/G3 rule: every count discrepancy must be an
    explained flip (threshold proximity ≤1e-3 °C, or a QDM tie pair).
  - The claim is limited to the units and windows tested.
- **Determinism**: pilot ACCESS-CM2 Kerala series must equal the validation's
  earlier full-window fidelity and future outputs bit-for-bit, or the difference
  is reported.
- **Reader**:
  - the files are read through `master_builder`'s collectors;
  - the stock signature guard must refuse them;
  - with the pilot signature substituted in-process only, the collected values
    must round-trip the written values exactly.

## Amendments

None.
