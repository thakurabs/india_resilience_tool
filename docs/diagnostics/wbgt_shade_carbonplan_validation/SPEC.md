# CarbonPlan shade WBGT: held-out validation contract (CHG-0645)

Written 2026-10-03, **before any validation score was computed**. These are
prospective validation rules. They are not adjusted after scores are seen. Any
later change is a dated amendment at the end of this file, and results it
affects are labelled *exploratory*.

Starting snapshot: `GIT:add_flood_depth@7bd2ddb`, dirty (Route B CHG-0644 edits
to README.md, MANIFEST.md and tools/README.md, plus untracked WBGT diagnostics;
all preserved, none touched by this work).

Change-ID note: the review that requested this work called it CHG-0644. That ID
is already used by the applied Route B programme (`docs/wbgt_outdoor_route_b.md`).
This validation therefore uses **CHG-0645 onwards**.

## 0. What this is, and is not

The question is whether to replace IRT's `shade-peak-v1` with CarbonPlan's
shade-WBGT pipeline (notebooks 02 → 05 → 06 at commit
`f662b37200fe219db912ebd09ceb52fdac979861`). This contract answers three
separate questions:

1. **Implementation fidelity.** Does IRT's code reproduce CarbonPlan's
   published method?
2. **Temporal transfer.** Does a WBGT-level QDM trained on earlier years
   improve later-year shade estimates against UHE-Daily?
3. **Product suitability.** What happens to geographic detail, coverage,
   threshold counts and rankings when the results are transferred to IRT
   districts and blocks?

Reproducing the published product is **not** independent scientific
validation. A successful historical holdout is **not** proof of future
accuracy. This work authorises no production change, publication or
national computation, and selects no national method.

### Evidence framing carried into every result

- The earlier Kochi result (about 57 raw, 208 corrected and 212 reference days
  ≥32 °C) concerned **outdoor** WBGT against ERA5-derived Liljegren. It was
  already a temporal holdout. It is **not** a shade/UHE validation result.
- The Kerala population-weighted reproduction (CHG-0643) matched threshold
  counts exactly in 141 of 149 units. Each of the other eight differed by one
  day at one threshold over 30 years.
- Some daily residuals are larger because of documented QDM rank/tie
  sensitivity.
- UHE-Daily (Tuholske et al. 2021, as aggregated by CarbonPlan per
  `processing_id`) is an **observation-informed derived WBGT reference**, not
  direct WBGT instrument measurements.
- Population/area equivalence has so far been shown only for the historical
  Kerala calibration experiment (trained and scored on 1985–2014).
- "Uncorrected" or "raw" here means **no additional WBGT-level QDM**. The NEX
  climate inputs are themselves already BCSD bias-adjusted.

## 1. Reference implementation and environment

- Pinned environment: `/tmp/irt-carbonplan-py310` (Python 3.10, xclim 0.44.0,
  thermofeel 1.3.0, xarray 2023.8.0, numpy 1.26.4, pandas 2.0.3, scipy 1.11.4).
  Every QDM, raw-shade and scoring step runs here.
- Geometry, NetCDF and network steps run in IRT's `irt` environment, which has
  no xclim. They produce only inputs, never WBGT values.
- Upstream code is executed from the checksum-locked files in
  `../wbgt_outdoor_carbonplan_reproduction/source_lock.json`. A checksum
  mismatch stops every run.
- Raw shade (notebook 02): `ps = 101325·10^(−elev/(18400·tas/273.15))`;
  `rh = xclim.indices.relative_humidity(tasmax, huss, ps)`; thermofeel
  `calculate_wbt(Tmax_C, rh)`, `calculate_bgt(tasmax, tasmax, wind=0.5)`;
  `shade = 0.7·WBT + 0.2·BGT + 0.1·Tmax_C`, float32 inputs, same operation order.
  Elevation is CarbonPlan's `inputs/elevation.zarr`.
- Cells → units (notebook 05): weights are intersection area in ESRI:53034,
  times resampled GHS-POP 2030 for the population variant. Null-elevation cells
  are dropped **before** normalising, as `calc_sparse_weights(mask_nulls=…)`
  does. Each unit uses its whole CarbonPlan polygon and all its contributing
  cells, including any part outside the selected state.
- QDM (notebook 06): `train_bias_correction` is AST-loaded unchanged. It is
  additive (`kind="+"`), with 100 quantiles, `Grouper("time.dayofyear",
  window=31)`, and converts both series to `noleap` with linear gap-filling of
  the model series. Adjustment applies `trained.adjust(ts)` to the series in its
  **original** calendar, then `convert_calendar("gregorian", align_on="year",
  missing=nan)` and `interpolate_na(time, linear)`, as `apply_bias_correction`
  does.

## 2. Experiment matrix

| ID | Question | State(s) | Model(s) | Weights | Train | Apply / score |
|---|---|---|---|---|---|---|
| G1 | fidelity: raw parity | all run states | all run models | pop + area | — | 1985–2014 raw |
| G2 | fidelity: historical product | Kerala | ACCESS-CM2 | pop | 1985–2014 | 1985–2014 vs published |
| G3 | fidelity: future product | Kerala | ACCESS-CM2 | pop | 1985–2014 | 2020–2039 and 2040–2059 **separately** vs published |
| H1 | transfer: **primary holdout** | Kerala | ACCESS-CM2 | pop | 1985–1999 | 2000–2014 (one window) vs UHE |
| H0 | context only, **not validation** | Kerala | ACCESS-CM2 | pop | 1985–2014 | 2000–2014 vs UHE (in-sample) |
| W1 | weighting | Kerala | ACCESS-CM2 | area | 1985–1999 | 2000–2014 vs UHE; future 2020–2039 / 2040–2059 vs pop |
| AW | application windows (diagnostic) | Kerala, 3 fixed units | ACCESS-CM2 | pop | 1985–2014 and 1985–1999 | see §6 |
| M1 | multi-regime | Kerala, Rajasthan, Himachal Pradesh | ACCESS-CM2, CMCC-CM2-SR5 | pop (primary), area | 1985–1999 | 2000–2014 vs UHE |
| T1 | product transfer | each M1 state | ACCESS-CM2 (primary) | pop | from H1/M1 | units → IRT districts/blocks |

### Selection rules (fixed now)

- **Ensemble member:** `r1i1p1f1`, the only member in IRT's local NEX archive.
- **ACCESS-CM2** is first because CarbonPlan's correctness evidence exists for it.
- **Second model rule:** the first model, in Python sort order, of
  `../wbgt_shade_release/release_roster.json` that (a) uses a different calendar
  from ACCESS-CM2 (`proleptic_gregorian`), (b) is in CarbonPlan's `gcm_list`,
  (c) has local `tas`, `tasmax` and `huss`, and (d) passes the §3 inventory.
  On 2026-10-03, before scoring, the local calendars are: 365_day →
  CMCC-CM2-SR5, CMCC-ESM2, CanESM5, GFDL-CM4, GFDL-ESM4, INM-CM4-8, INM-CM5-0,
  NorESM2-LM, NorESM2-MM, TaiESM1; standard → IPSL-CM6A-LR, MIROC6 (neither
  has local `huss` nor is in `gcm_list`); 360_day → KACE-1-0-G (not in the
  release roster). The rule selects **CMCC-CM2-SR5**. If it fails the
  inventory, the next qualifying 365_day model is taken and the reason
  recorded.
- **No third model.** No other calendar class qualifies under the same rule,
  and choosing a model for its physics or its results is outside this
  contract's selection basis. Only one extra model is added; the brief allowed
  up to two.
- **Units:** every CarbonPlan feature (Indian `hierid` region or city polygon,
  `hierid: null`) whose polygon intersects the union of the state's IRT
  district polygons. A unit touching two states appears in both per-state
  tables. Pooled summaries count each unit **once**, under the state with the
  largest intersection.
- **AW subset:** processing_ids 19567, 20346 and 20372. These are the three
  Kerala units with the largest Kerala intersection, already used by the
  CHG-0639 correctness check.
- **Multi-regime extension condition:** M1 beyond Kerala runs only if (a) G1
  and G2 pass, and (b) the Kerala H1 run completed for every eligible unit with
  at least half the eligible units having a supported threshold (§5) at 28,
  30 or 32 °C. Condition (b) concerns event support, not the direction of the
  result.

## 3. Inputs, calendar and missing data

### Inventory (before use)

For each selected model, inspect `tas`, `tasmax` and `huss` for 1985–2014
(historical) and 2020–2059 (SSP2-4.5). ACCESS-CM2 also gets 2060–2080 for the
AW subset only. For every file, check: the date set equals the declared
calendar's full year (no gaps, no duplicates); units (`K`, `K`, `kg kg-1`/`1`);
lat/lon equal ACCESS-CM2's grid; variant label and version attributes; and
finite values at every cell used. A filename or model directory alone is not
evidence. The 19-model release roster is **not** claimed verified unless its
full required-year intersection is actually checked.

For UHE, inventory finite-day coverage per `processing_id` over 1985–2014.
Record the geometry stream SHA-256 and the population and elevation stores.

### Eligibility (holdout units)

A unit is eligible for H1/W1/M1 iff:
1. it has at least one non-null-elevation cell;
2. its UHE series is finite on **every** day 1985-01-01..2014-12-31 (no
   interpolation of UHE, ever); and
3. its raw series is finite on every day (inputs pass the inventory).

Every excluded unit/year is listed with its reason. Nothing is partially used.

### Two calendars

- **Reproduction calendar (CarbonPlan):** QDM internals and the published
  comparison follow notebook 06 exactly. For 365_day models that includes
  converting to Gregorian and linearly interpolating the new Feb 29. Such
  dates are *calendar-conversion gaps*, counted and reported separately from
  missing source data.
- **IRT reporting calendar:** every scored series (raw, QDM, UHE) drops Feb 29
  and requires complete 365-day years. So no interpolated Feb 29 enters an IRT
  metric. G1–G3 compare on the full Gregorian calendar, as published. H/W/M
  scores use the IRT calendar.

Before QDM, the raw model series must be finite. `interpolate_na` inside
`train_bias_correction` can then fill only calendar-conversion gaps, never
missing climate data. A unit-year with any non-finite scored value gets NaN
metrics, is listed, and is **never** counted as zero exceedance days.

## 4. Train/test separation (H1, W1, M1)

- Training inputs: UHE and the raw model series restricted to 1985-01-01..
  1999-12-31. Code asserts both date ranges before calling `train`.
- Application: the trained model adjusts the raw series for 2000-01-01..
  2014-12-31 in one call (one declared 15-year window). Ranking the held-out
  **model** values inside `adjust` is part of QDM. It is not leakage of
  held-out **reference** data.
- Held-out UHE values are used only in scoring. They are never used in
  training, tuning, missing-data repair or unit selection. Eligibility (§3)
  uses UHE finiteness over 1985–2014 as a data-availability rule, not its
  values.

## 5. Metrics (test period 2000–2014, IRT calendar, 5,475 days)

For each eligible unit and each series X ∈ {raw, qdm}, error = X − UHE.

**Primary**
- **P-seasonal:** monthly-climatology bias for each calendar month, summarised
  as the mean over the 12 months of |monthly bias| (°C).
- **P-tail:** quantile bias at p90, p95 and p99 of all test days (numpy linear
  quantiles), signed and absolute (°C).
- **P-count:** mean annual days ≥28, ≥30 and ≥32 °C (inclusive, unrounded
  values), signed and absolute error (d/yr).

**Secondary**: annual-mean bias; each monthly bias; IMD-season (JF, MAM, JJAS,
OND) p50/p90 bias; interannual SD of annual counts, and the mean absolute
difference between the sorted annual counts of X and UHE (the 1-D Wasserstein
distance over 15 years); sample size; reference exceedance frequency. Daily
paired RMSE and correlation are **diagnostic only**: historical simulations
are not synchronised with observed weather, so these are never an acceptance
criterion.

**Event support** per unit and threshold, from the reference test period:
*zero* (no reference day), *sparse* (fewer than 15 days, i.e. below 1 d/yr) or
*supported* (at least 1 d/yr). No percentage errors are used. A zero-support
threshold where raw and QDM also have zero days is classed `zero/zero`. It is
excluded from improvement fractions and tallied separately.

**Per-unit change** Δ = |err_qdm| − |err_raw|. Indifference bands: 0.05 °C for
temperature metrics, 0.5 d/yr for counts. *Improved* if Δ < −band, *worsened*
if Δ > band, otherwise *unchanged*. These bands are reporting resolution
chosen before scoring, not accuracy standards.

**Uncertainty.** Bootstrap over whole years: B = 1000, seed 20261003. Each
replicate draws 15 test years with replacement, independently for the model
side (raw and qdm share one draw) and the reference side, because the model
years and observed years are not paired weather. The same draw applies to
every unit, which preserves spatial dependence. Neighbouring units are not
treated as independent replicates. Intervals are **conditional on the fitted
correction**: they do not include refitting uncertainty, which would need
bootstrap refits and is outside this run's compute budget. The statistic is
the change in the across-unit median |error| (qdm − raw), with a 90%
percentile interval. No single significance test is presented as proof of
validity.

**Reporting cuts:** each state × model × weighting separately before any
pooling; cities and non-city regions separately; per-month and per-season
regressions; the worst 10 per-unit regressions per primary metric, with
centroids and dominant IRT district; and worsened-unit counts per district as
a spatial-cluster summary.

## 6. Application-window diagnostic (AW)

- **Baseline (reproduction):** a model trained on 1985–2014 is applied to
  2020–2039 and 2040–2059 as two separate calls, exactly as notebook 06 does.
- **Alternatives (diagnostic only):** the same trained model applied to
  2020–2059 in one call, and to 2020–2080 in one call. Report the differences on
  overlapping dates (max/mean absolute, and count changes).
- **Historical analogue:** the H1 model (1985–1999) applied to 2000–2014 in one
  call, compared with separate calls for 2000–2007 and 2008–2014.
- No window is selected by held-out score. The output is a statement of what a
  national SPEC must fix so that extending the horizon cannot silently rewrite
  values already generated.

## 7. Weighting (W1)

Area-weighted raw series are trained on their own 1985–1999 raw against the
same UHE. Population and area are compared per unit: before QDM (test period),
after held-out QDM, and in the future windows (both trained 1985–2014, applied
per §6 baseline). The full-calibration fit alone is not evidence of
equivalence. Units whose pop–area difference exceeds 0.05 °C (annual mean,
tail quantiles) or 0.5 d/yr (counts) are flagged. These flags are descriptive.

## 8. IRT transfer (T1)

- Crosswalk: intersection area in ESRI:53034 between each IRT district/block
  (`districts_4326.geojson`, `blocks_4326.geojson`) and each unit.
- Weights are normalised over the **covered** area. Coverage is reported
  against the **original** admin area. Coverage below 0.95 is flagged, never
  filled. Zero coverage gives NaN with a reason. A unit with missing UHE or an
  ineligible series contributes nothing; its area counts as uncovered for that
  series.
- **Count-first:** counts are computed per unit-year, then area-weighted, and
  labelled "area-weighted regional exceedance days (fractional)". Counts are
  never taken from an averaged admin daily series.
- Checks: unique `processing_id`; stable admin keys (`district_lgd_code`,
  `block_key`); city/region overlap measured (double counting must be ≤ 0.1% of
  admin area); weight sums equal 1 within 1e-9; coastal/water coverage flags.
- Block detail: number of contributing units; dominant unit and its weight;
  blocks sharing an identical weight vector (hence identical series); where
  low coverage concentrates; Spearman rank changes (raw → qdm → UHE), with tie
  counts reported for near-zero metrics.
- `shade-peak-v1`, where staged for the state, is compared **descriptively**
  on 2000–2010, alongside the UHE values transferred through the same
  crosswalk. The UHE-by-unit reference is coarse, so this is not a
  common-reference validation of either method.

## 9. Gates, screening criteria and decision rules

**Implementation gates (fidelity, pass/fail)**
- **G1:** the vectorised raw path equals the predecessor `reproduce.raw_shade`
  to ≤ 1e-9 °C for every unit, model and weighting run.
- **G2:** published-product reproduction, 1985–2014, 149 units. PASS iff every
  threshold-count discrepancy is explained by a flip day where either (a) both
  the reproduced and the published value lie within 1e-3 °C of the threshold
  (input-version noise), or (b) the day belongs to a QDM tie pair. A tie pair
  is two days of one unit whose raw values differ by ≤ 1e-4 °C, are within ±15
  day-of-year, and whose residuals are equal and opposite within 1e-3 °C. Any
  other discrepancy is a FAIL. Tolerances only explain residuals; the count
  definition stays inclusive and unrounded. The CHG-0643 result (141 exact,
  eight off by one day) is a reference outcome, not a waiver.
- **G3:** as G2, per future window. If not runnable within inputs or the cap,
  G3 is **OUTSTANDING**, not passed.

**Screening criteria (temporal transfer, per state × model × weighting × primary metric)**
- *Improves*: the across-unit median |error| falls; at least 2/3 of
  classified units improve; and the 90% bootstrap interval of the change in
  median |error| lies entirely below zero.
- *Worsens*: the mirror image.
- *Mixed/inconclusive*: anything else.
These are predeclared screening conventions, not scientific accuracy
thresholds. Their limits: they treat units equally regardless of population;
the interval ignores refitting uncertainty; and 15 test years are few for
rare thresholds.

**Recommendation rule**
- **Do not proceed** if G1 or G2 fails, or if any primary metric family
  (P-seasonal, P-tail at p95/p99, P-count at supported thresholds)
  *worsens* for ACCESS-CM2 in Kerala (H1).
- **Proceed to national SPEC** only if G1, G2 and G3 pass; M1 ran for all three
  states and both models; no primary family *worsens* in any state × model; and
  P-tail and P-count *improve* in a majority of state × model cases.
- **Proceed conditionally** otherwise. The conditions list every outstanding
  gate, incomplete regime, and worsening or inconclusive case.

## 10. Outputs and resource limits

- Durable evidence goes to `docs/diagnostics/wbgt_shade_carbonplan_validation/`
  (`SPEC.md`, `README.md`, `scripts/`, `results/`). Intermediates go to
  `scratch/carbonplan_shade_validation/`. Pure, testable logic goes to
  `tools/diagnostics/wbgt_shade_cp_validation.py`, with tests in
  `tests/test_wbgt_shade_cp_validation.py`.
- Single worker. Every script records wall time and peak RSS.
- **Download cap: 2 GB cumulative for this task, repeated transfers
  included.** Every network byte is counted, including the geometry stream.
  Objects over 160 MB are refused, except the 190,178,657-byte geometry file,
  which is streamed (not stored) as declared in CHG-0639. Remote objects are
  inventoried (HEAD) and the transfer estimated before any download. If the
  plan exceeds the cap, cached/local work is completed and the remaining
  requirement reported precisely.
- Read-only: CHG-0639/0643 scratch and caches, the national `shade-peak-v1`
  staging tree, NEX sources, boundaries. Nothing is written outside the two
  directories above and the tools/tests files named here.
- No production file, published output, dependency, `environment.yml`,
  `docs/HANDOFF.md` or `docs/BACKLOG.md` is modified.

## Amendments

None of these changes a scoring rule, threshold, window, weighting, band or
decision rule. Each was made before the scores it affects were read.

- **A1, 2026-10-03, before any transfer result: district admin key.** §8 named
  `district_lgd_code` as the stable district key. The crosswalk stage found
  that 29 of Rajasthan's 49 districts share an LGD code with another district:
  districts created in 2023 carry their parent's code, e.g. Balotra/Barmer = 90
  and Jaipur/Jaipur Gramin/Dudu/Kotputli-Behror = 102. Districts are therefore
  keyed `state::district_name`, which is unique and matches the staging tree.
  Blocks keep `block_key`, which is unique in all three states. The duplication
  is reported as a data-contract finding.
- **A2, 2026-10-03, before any score was read: bootstrap draws shared across
  subsets.** The statistic, B = 1000 and the seed are unchanged. The all /
  cities / regions subsets now reuse one set of year draws per replicate,
  instead of drawing separately, to cut runtime threefold. A first Kerala
  scoring process using the old scheme was stopped before it wrote any output.
- **A3, 2026-10-03, before any CMCC-CM2-SR5 score: 365_day time axis.** The
  first three CMCC runs failed inside notebook 06's `interpolate_na`. The time
  axis rebuilt from the driver archive had used generic
  `cftime.datetime(calendar="noleap")` objects. The fix uses
  `cftime.DatetimeNoLeap`, the class xarray produces when it decodes NEX, and
  so the class CarbonPlan's notebooks see. The failed run directories are kept
  under `scratch/.../runs/_failed_cftime/`. For 365_day models, G1 compares
  against `reproduce.raw_shade` on a synthetic Gregorian ordinal index, because
  that function pivots on pandas dates. The formulas are elementwise in time,
  so the comparison is unaffected.
- **A4, 2026-10-03, scope note on G3.** Tie-pair attribution in G3 is applied
  to flip days, as the gate requires. The other future-window days with
  residuals above 1e-3 °C (118 in 2020–2039 and 116 in 2040–2059, over 149
  units) are not individually attributed. In G2 every such day (216 of 216)
  was a tie pair.
- **A5, 2026-10-03, after H1/M1 scores: one exploratory diagnostic.** The
  README adds a test-minus-training change comparison (model vs UHE: mean, p99
  and days ≥32) to help interpret tail regressions. It is post hoc and
  labelled exploratory. It does not alter any verdict.
