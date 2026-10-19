# CarbonPlan shade WBGT pilot in IRT — Kerala contract

Written 2026-10-03, before any pilot score. Scope approved as option (a).

## Purpose

Run CarbonPlan's shade WBGT method inside IRT for one state, one model and the
historical period. Replace population weights with area weights, then compare
the result with IRT's deployed `shade-peak-v1` for the same units. This is an
implementation pilot, not a decomposition: it does not try to attribute the
difference to individual method steps (user ruling 2026-10-03).

Scope: Kerala districts (14) and blocks; ACCESS-CM2 r1i1p1f1; historical;
reporting years 1990-2010; QDM calibration 1985-2014.

## Method (fixed before scoring)

Source: CarbonPlan extreme-heat commit
`f662b37200fe219db912ebd09ceb52fdac979861`, verified by
`../wbgt_outdoor_carbonplan_reproduction/source_lock.json`. Pinned xclim 0.44.0
and thermofeel 1.3.0. Local NEX-GDDP-CMIP6 ACCESS-CM2 `tas`, `tasmax` and `huss`.

1. **Raw shade per 0.25° cell** (notebook 02). Surface pressure comes from
   CarbonPlan's elevation raster: `101325 * 10**(-elev / (18400 * tas / 273.15))`.
   RH at tasmax is `xclim.indices.relative_humidity(tasmax, huss, ps)`. WBT is
   thermofeel `calculate_wbt` and BGT is thermofeel `calculate_bgt` with wind
   fixed at 0.5 m/s. Shade = `0.7*WBT + 0.2*BGT + 0.1*Tmax_C`. The code is the
   same `raw_shade` that reproduced every published threshold count at Kochi,
   Bikaner and Shimla.
2. **Cells → CarbonPlan regions by area only.** Regions are the CarbonPlan
   `hierid` regions (Indian `hierid` starting `IND`) whose polygons intersect
   Kerala. Each region uses its **whole** polygon, including any part outside
   Kerala, because that is the unit CarbonPlan corrects. A cell's weight is its
   intersection area with the region in ESRI:53034, normalised to sum to 1.
   **Correction recorded before any score:** an earlier draft excluded CarbonPlan
   city polygons (`UC_NM_MN` features) on the assumption that they overlap the
   regions. They do not. The first preparation measured 51 interior rings
   across the 86 kept regions, and zero overlap between the regions and Kochi's
   city polygon. So cities and regions together tile the land, and the draft
   left urban blocks almost uncovered (Kozhikode block 2.6%). City features
   intersecting Kerala are therefore CarbonPlan units like any region: each has
   its own UHE reference, its own QDM and its own published series. As in notebook 05
   (`utils.calc_sparse_weights` with `mask_nulls`), cells whose WBGT is null
   are dropped **before** the area fractions are normalised. Here that means
   cells with no value in CarbonPlan's elevation raster, because notebook 02
   then has no pressure. (Added before any score, after preparation hit
   null-elevation coastal cells; this follows the source and does not tune
   anything.)
3. **QDM per region.** Use notebook 06's own `train_bias_correction`, AST-loaded
   from the checksum-locked notebook: 100 quantiles, additive, `time.dayofyear`
   grouping with a 31-day window. Train on 1985-2014 against UHE-Daily for the
   same `processing_id`, keeping NEX noon timestamps. Apply to the same raw
   series and convert back to the Gregorian calendar exactly as notebook 06 does.
4. **Regions → IRT units by area.** Each district or block gets weights equal to
   its intersection area with each region in ESRI:53034, normalised over the
   area that regions cover. Coverage (covered area / unit area) is reported per
   unit. A unit below 0.95 coverage is flagged, not filled.
5. **IRT metrics.** Use IRT's own conventions: complete 365-day years with
   Feb 29 dropped; `annual_mean`; `days_ge_28/30/32` with inclusive `>=`; a
   period value equal to the mean of the 21 yearly values for 1990-2010.
   - **Primary aggregation is region-first.** Each metric is computed per
     region-year and then area-weighted onto the unit. This mirrors IRT's
     cell-first convention, with the region in the place of the cell, so the
     pilot and `shade-peak-v1` share their aggregation rule and differ only in
     method. This refines approved step 5 ("metrics from the unit daily series")
     and is recorded here before scoring.
   - **Secondary, also reported:** counts taken on the area-weighted unit daily
     series. `annual_mean` is identical under both rules because it is linear.

## Comparison

Compare per district and per block, period and yearly, with `shade-peak-v1`
read-only from `scratch/wbgt_shade_national/processed/wbgt_shade_stull_*/Kerala`
(ACCESS-CM2, historical). Report the pilot value, the IRT value and their
difference, plus the distribution of differences across units: mean, median,
min, max and IQR. Also report Spearman rank agreement across units within
districts and within blocks. No verdict on which method is better; the user
decides the course after the comparison.

## Correctness check (wiring proof, not analysis)

For up to 3 Kerala regions, recompute step 2 with CarbonPlan's own weights
(intersection area × resampled GHS-POP, as notebook 05 does), run step 3, and
compare with CarbonPlan's published `historical-WBGT-shade` ACCESS-CM2 series
for 1985-2014.

- **Gate:** the `days_ge_28/30/32` counts over the full 1985-2014 window
  (Gregorian, as published) are identical.
- The maximum absolute daily difference is reported against the 1e-3 °C
  input-version band. A known tie-break artefact (an exchanged QDM factor
  between two equal raw days) may produce isolated larger single-day residuals.
  Those are reported, not corrected.
- If the gate fails, the pilot stops and reports. No pilot score is presented
  as a CarbonPlan implementation unless this check passes.

Regions are chosen before scoring as the three Kerala-intersecting regions
with the largest intersection area with Kerala.

## Constraints

- Single worker. Intermediates live only under `scratch/carbonplan_kerala/`.
  Durable evidence lives in this directory.
- Downloads are capped at 2 GB total and 160 MB per stored object.
  - **Declared exception, pre-score:**
    `v1.0/inputs/all_regions_and_cities.json` is 190,178,657 bytes. It is
    streamed once; only the Indian `hierid` features intersecting Kerala are
    stored, and the full stream's SHA-256 and byte count are recorded. Nothing
    larger than 160 MB is written to disk.
- No production code, national staging output, dependency or source climate
  file is modified. National staging (PID 60808) is read only after Kerala's
  outputs are validated, which they are; nothing is written there.
- Local NEX files may differ in version from CarbonPlan's 2023 download, so
  provenance hashes are retained.
- No coefficient, weight rule, calendar or calibration window is tuned after
  scoring. Any post-score change is recorded below as a dated addendum, with
  its reason.

## Addendum 2026-10-03: population-weighted variant (CHG-0643, written before its score)

At the user's request, after the area-weighted result, the pilot is re-run with
CarbonPlan's own step-2 weights (intersection area × resampled GHS-POP 2030,
the weights the correctness check already validated). Everything else is
unchanged: QDM per unit, units → IRT districts and blocks by intersection
area, and IRT metric conventions. With these weights each CarbonPlan unit's
corrected series is CarbonPlan's published ACCESS-CM2 series, up to input
version and tie-break pairs. The variant is selected by
`run_pilot.py --weights population` and written to
`scratch/carbonplan_kerala/run_population`; its comparison goes to
`results_population/`. It is compared with `shade-peak-v1` and with the
area-weighted pilot. No other setting changes.
