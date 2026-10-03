# Outdoor WBGT pilot — input-quality and elevation follow-up, frozen specification

**Status: FROZEN before any WBGT score of this follow-up was computed (CHG-0623).**

Milestone 4b. Predecessor: `docs/diagnostics/wbgt_outdoor_pilot/` (milestone 4,
commit `459d5c7`, verdict `ENGINEERING CONDITIONAL`). That milestone's
specification and evidence are **read-only** to this one.

Milestone 4 closed with two named conditions:

1. `hurs` above 100 % in the NEX inputs invalidated whole cell-years under the
   frozen complete-365 rule (11 of 116 Himachal cells, 1 of 86 Kerala cells to
   `tasmin > tasmax`).
2. No elevation field existed locally, so every cell ran at sea level under the
   declared `elev-sea-level-constant-no-dem` convention.

This document is the contract for closing those two conditions. The source
investigation of §2 was performed **before** this document was written, because
the policy must be chosen from the source records. Everything in §3 onward was
frozen before any WBGT value of this follow-up existed.

Scope is unchanged from milestone 4 and is **not** widened: Kerala, Rajasthan,
Himachal Pradesh; ACCESS-CM2 `r1i1p1f1` historical **2005**; district and
block; MRI-ESM2-0 on Himachal Pradesh only. No new candidate, no new year, no
new state, no bias correction, no QDM, no gate change.

All outputs remain **DIAGNOSTIC-ONLY**, including temperature levels, threshold
counts and every ranking.

---

## 1. Verdict vocabulary

Three conclusions are issued **separately**:

| Conclusion | Question |
|---|---|
| Engineering implementation | Is the pipeline correct, reproducible, isolated and affordable? |
| Input / coverage readiness | Is the adopted input policy justified, and does coverage meet the declared screen? |
| Scientific readiness | Unchanged: diagnostic-only. This milestone resolves no NEX bias and validates no W1 ranking. |

An engineering `PASS` does **not** require that no invalid source cell remains.
It requires correct handling of what is invalid and satisfaction of the declared
engineering criteria. Where input policy or coverage remains unresolved the
verdict stays `CONDITIONAL` and names the precise outstanding requirement.

## 2. Source investigation and the input-quality policy

### 2.1 What the source records show (`hurs > 100 %`)

Measured read-only from the actual pilot inputs, ACCESS-CM2 and MRI-ESM2-0,
`historical/hurs/.../2005.nc`, India subset:

| Property | Finding |
|---|---|
| Variable identity | `hurs`, `standard_name relative_humidity`, `units %`, `cell_methods "area: time: mean"` |
| Storage | `float32`, **no** `scale_factor`, **no** `add_offset` |
| Fill | `_FillValue = 1e+20`, `missing_value = 1e+20`; decoded to NaN correctly |
| Valid-range metadata | **absent** — no `valid_range`, `valid_min` or `valid_max` is published |
| Raw vs decoded | identical at the offending positions; **decoding is not the cause** |
| Coordinates / time | all six variables share one 0.25° grid, 365 `proleptic_gregorian` timestamps at 12:00, `variant_label r1i1p1f1`; **alignment is not the cause** |
| ACCESS-CM2 extent | 1663 of 3 721 905 finite cell-days exceed 100 % (0.045 %); max **103.623 %**; 724 above 101 %, 254 above 102 % |
| MRI-ESM2-0 extent | 974 cell-days exceed 100 %; max **106.756 %**; 234 above 101 %, 88 above 102 %, 5 above 105 % |
| Negative values | none in either model |

Dataset documentation (the file's own `comment` attribute) states RH is defined
with respect to liquid water above 0 °C and with respect to ice below it, which
permits values above 100 % at sub-freezing temperatures. That explains only a
minority of cases: nationally only 1.9 % (ACCESS-CM2) and 2.6 % (MRI-ESM2-0) of
the offending cell-days have a daily-mean `tas` below 0 °C, and the median is
+14.1 °C / +12.7 °C. The Himachal pilot cells are in that cold minority
(daily-mean `tas` 3.3–5.0 °C, `tasmin` below 0 °C on three of four days).

The dominant cause is therefore the product's own method: NEX-GDDP-CMIP6 is
BCSD (`source = "BCSD"`, `activity = "NEX-GDDP-CMIP6"`), and quantile mapping of
a bounded variable is not bound-enforced. **No source-documented tolerance
exists.**

### 2.2 The adopted policy

Because no documented tolerance exists, §4 of the request admits only an
**explicit sensitivity treatment**. Adopted, and frozen here:

> **`rh_policy = "clip100"`** — any **finite** `hurs` value strictly above
> 100 % is replaced by exactly 100.0 % for computation. This is an
> **experimental physical-bound treatment, not accepted source repair.**

Deliberate properties of this policy:

- **No numeric allowance ceiling is adopted.** An upper allowance of 102 % was
  explicitly rejected: the observed maxima are 103.623 % and 106.756 %, so a
  102 % ceiling would have been fitted to one cell's 101.208 % and falsified by
  the wider data. Every finite excess is clipped and its magnitude is reported
  instead.
- **Monotone and conservative.** Clipping lowers vapour pressure, so it can only
  lower reconstructed humidity and therefore WBGT. It cannot manufacture an
  exceedance.
- **Source files are never modified.** Clipping happens in memory.
- **Raw values are preserved and flagged.** Per cell-year the runner records the
  number of clipped days and the maximum correction in percentage points.
- **Missing stays missing.** Non-finite `hurs` is never clipped into a value.
- **Negative `hurs` stays invalid.** No negative value exists in these inputs;
  the rule is retained unconditionally.
- **A flagged corrected day and an unresolved invalid day remain distinguishable**
  in every output: `rh_clipped_days` versus `input_invalid_days`.

**Temporal completeness is not relaxed.** The complete-365 / drop-Feb-29 rule
remains in force *after* the declared treatment. A cell-year that still holds one
unresolved invalid day is still wholly NaN.

Because the treatment is experimental rather than source-supported, the
**input/coverage readiness conclusion cannot exceed `CONDITIONAL`** however the
numbers fall.

### 2.3 The inverted temperature range (`tasmin > tasmax`)

Measured read-only on the single affected Kerala cell, 12.375 N / 74.875 E,
2005-11-03:

| Property | Finding |
|---|---|
| Dates, calendar, coordinates | identical across all six variables; nearest-neighbour selection returns the exact cell centre |
| Units | `tasmin`/`tasmax`/`tas` all `K`; decoding correct |
| Provenance | all files `ACCESS-CM2`, `historical`, `variant_label r1i1p1f1`; per-variable `tracking_id` differs, which is normal for one-file-per-variable |
| Values | `tasmin` 25.924 °C, `tasmax` 25.167 °C, `tas` 25.545 °C — `tas` lies **between** them |
| Extent | 1 day at this cell; 1065 of 3 721 905 national finite cell-days (0.029 %); worst national inversion −1.567 °C |

There is no ingestion or alignment error to correct: the inversion is present in
the delivered values. BCSD bias-corrects `tasmin` and `tasmax` independently and
does not enforce their ordering. This is an **unresolved source defect, not a
processing defect.**

Policy, frozen: the day stays invalid, therefore the cell-year stays NaN. The
values are **not** swapped, averaged, interpolated or filled from neighbours. A
regression test asserts the ordering check still invalidates rather than repairs.

## 3. Elevation

### 3.1 Availability check

Checked first, read-only: no `orog` field exists in either NEX tree (NEX-GDDP-CMIP6
publishes none), and no DEM, SRTM, GMTED, ETOPO or GEBCO asset exists anywhere
under `IRT_DATA_DIR`. Native-grid CMIP6 `orog` for ACCESS-CM2 would be ~1.25°
and is not compatible with the 0.25° NEX target grid. An external product is
therefore required.

### 3.2 Adopted source

| Property | Value |
|---|---|
| Product | **GMTED2010**, Global Multi-resolution Terrain Elevation Data 2010 |
| Publisher | USGS / NGA |
| Layer | `mea` — **mean** elevation aggregate (the correct aggregate for area-averaging) |
| Resolution | **30 arc-second** (`300darcsec`, ≈ 0.93 km at the equator) |
| Tiles | `10S060E`, `10N060E`, `30N060E`, version stamp `20101117` |
| URL root | `https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/topo/downloads/GMTED/Global_tiles_GMTED/300darcsec/mea/E060/` |
| Licence | Public domain (USGS); unrestricted use with citation |
| Horizontal reference | WGS84 geographic, decimal degrees |
| Vertical reference | **metres above the EGM96 geoid** |
| Download size | ≈ 17.3 MB per tile, ≈ 52 MB total |
| Identity | SHA-256 of each downloaded tile, recorded in the run manifest |

30 arc-second was chosen deliberately over the 7.5 and 15 arc-second GMTED
variants: a 0.25° NEX cell contains ≈ 28 × 28 = 784 GMTED samples at 30 arc-sec,
which is ample for a cell mean, while the finer variants would be a 276 MB and
69 MB per-tile download for no gain in a cell average.

### 3.3 Sampling

Per climate cell, the representative elevation is the **cos(latitude)-weighted
mean** of every GMTED pixel whose centre falls inside that cell's 0.25° box.
This is an area average on the sphere; cos-weighting is retained even though its
effect at 0.25° is small, so the method needs no caveat.

Frozen rules:

- Nodata pixels are excluded from the mean, never treated as zero.
- A cell with **no** valid GMTED pixel yields NaN, which is an **explicit
  failure**: the run stops unless the fallback flag is passed, and if it is, the
  affected cells are flagged `elevation_fallback = sea_level` in the output.
  There is **no silent sea-level substitution**.
- **Negative elevations are retained**, not clipped. Count and minimum are
  reported. GMTED carries genuine below-geoid land, and clipping would be a
  silent edit.
- Cell elevations are reused for administrative aggregation. **No district is
  assigned one elevation**; every climate cell keeps its own.
- The elevation-to-pressure formulation is **unchanged**:
  `m1.barometric_pressure_hpa`, ISA. Only its elevation input changes.

Plausibility checks, declared before inspection: finite fraction, units in
metres, minimum/median/maximum per state, and the expected regional ordering
Kerala < Rajasthan < Himachal Pradesh on the state median.

## 4. Comparison runs

Primary model ACCESS-CM2, all three states, W1 only (C1 is not re-run; §9):

| Run | `rh_policy` | Elevation |
|---|---|---|
| `baseline` | `strict` | sea-level constant |
| `rh` | `clip100` | sea-level constant |
| `elev` | `strict` | GMTED2010 cell mean |
| `combined` | `clip100` | GMTED2010 cell mean |

MRI-ESM2-0, Himachal Pradesh only: `baseline` and `combined`.

Reported for every run, and for every pairwise difference:

- valid cell-years and valid area;
- annual mean of daily maximum outdoor WBGT;
- days at or above 28, 30 and 32 °C;
- district and block values;
- coverage classifications.

**Value changes are separated from coverage changes.** Every pair is compared
twice: once on each run's own available support, and once on the **common valid
cells** of the two runs. An administrative change is never attributed wholly to
humidity or elevation when its contributing area also changed; the per-unit table
carries both the value delta and the valid-area delta.

Same-date weather accuracy against ERA5 is **not** computed. These are freely
evolving climate-model simulations and such a comparison would be invalid.

## 5. Acceptance criteria, frozen before scoring

| ID | Criterion | Tolerance |
|---|---|---|
| A1 | `baseline` reproduces the frozen milestone-4 district and block values | `<= 1e-9 °C` on the annual mean; **exactly equal** threshold counts and valid-area fractions |
| A2 | Original per-cell parity checks still hold (frozen path, hour span, batch/chunk/resume) | `<= 1e-9 °C`; bit-identical for batch, chunk and resume |
| A3 | `rh` restores exactly the cell-years whose only defect was `hurs > 100 %` | the 11 Himachal cells become complete; the Kerala `tasmin > tasmax` cell stays NaN |
| A4 | `elev` changes **no** validity | valid cell-year count identical to `baseline` |
| A5 | Every admin unit is classified, and no unit with no valid support carries a number | 0 violations; NaN never 0 |
| A6 | All milestone-4 admin-correctness checks still pass | 0 failures |
| A7 | Geometry discrepancies lie within the §7 tolerances, or are reported with area and percentage | reported either way; production boundaries are **not** repaired |
| A8 | A cache written under one RH policy or elevation identity is refused under another | refusal proven by test |
| A9 | Budget respected | §8 |

**Input/coverage readiness `CONDITIONAL` → the outstanding requirement must be
named.** Readiness is judged against this declared screen: under the adopted
policy, at both levels and in all three states, every unit either meets the
coverage screen of §6 or is explicitly classified `partial_coverage` or
`no_valid_coverage`. Because the RH treatment is experimental (§2.2), readiness
cannot be better than `CONDITIONAL` in this milestone regardless of the counts.

## 6. Coverage classification

For every administrative unit, at both levels, the runner reports:

| Field | Definition |
|---|---|
| `full_polygon_area_m2` | the unit's own polygon area |
| `represented_area_m2` | area of the intersection of the polygon with the **eligible** climate grid (the cells the pilot computed) |
| `valid_area_m2` | represented area whose cell-year annual WBGT is finite |
| `represented_fraction` | `represented_area_m2 / full_polygon_area_m2` |
| `valid_fraction_of_represented` | `valid_area_m2 / represented_area_m2` |
| `valid_fraction_of_full` | `valid_area_m2 / full_polygon_area_m2` |
| `n_cells_valid`, `n_cells_invalid` | contributing cell counts |

All areas are computed in **EPSG:6933** (equal-area), the same projection the
production weight builder uses, so the three areas are mutually consistent.

### 6.1 The 0.99 screen and its denominator

Milestone 4 reported `valid_area_fraction = valid_intersected_area /
total_intersected_area` and screened it at 0.99. That denominator is the
**represented** area, not the full polygon area. Milestone 4's specification did
not say so explicitly, so it is defined here rather than silently reinterpreted:

> **The 0.99 coverage screen is applied to `valid_fraction_of_represented`.**
> This is exactly the quantity milestone 4 screened, so its published numbers
> keep their meaning. `valid_fraction_of_full` is reported alongside it as a
> second, stricter view and is **not** used as the screen.

Classes, assigned from the screen:

| Class | Condition |
|---|---|
| `meets_screen` | `valid_fraction_of_represented >= 0.99` |
| `partial_coverage` | `0 < valid_fraction_of_represented < 0.99` |
| `no_valid_coverage` | no valid contributing cell |

`no_valid_coverage` units carry **NaN**, never 0. `partial_coverage` units keep
their diagnostic value but are **excluded from every unqualified ranking
summary**, which is stated on the table and in the report. An estimate formed
over 55 % of a block is never presented as a block value without that label.

The metric name travels with the number: the exceedance fields are the
**area-weighted mean annual cell exceedance days**, not the number of days on
which an entire district exceeded a threshold.

## 7. Independent geometry checks

Run directly on the pilot boundary files, read-only, in EPSG:6933:

1. canonical identifier uniqueness at both levels, and every block's parent
   district present in the district layer;
2. duplicate geometries and invalid geometries (`is_valid`, including empty);
3. **gap**: parent district area not covered by the union of its child blocks;
4. **child outside parent**: child-block area falling outside its parent district;
5. **child overlap**: pairwise intersection area between blocks of one district.

Tolerances, declared before evaluating:

| Quantity | Tolerance |
|---|---|
| gap, as a fraction of parent area | `<= 0.001` (0.1 %) |
| child area outside parent, as a fraction of child area | `<= 0.001` |
| pairwise block overlap, as a fraction of the smaller child | `<= 0.001` |
| absolute sliver floor, ignored at any fraction | `10 000 m²` (1 ha) |

Discrepancies are reported in both absolute area and percentage. **Production
boundaries are not repaired.**

The milestone-4 numerical block-to-district reconciliation is retained, but
described correctly: **numerical agreement verifies aggregation consistency under
the tested support. It does not independently prove that blocks tile
districts.** The tiling question is answered only by §7.3–7.5 above.

## 8. Resources, isolation and reproducibility

| Limit | Value |
|---|---|
| Wall clock, per compute invocation | 3600 s (unchanged from milestone 4) |
| Peak process RSS | 4 GB (unchanged) |
| Artifacts | 5 GB (unchanged) |
| Workers | **1**, while the shade rebuild runs |
| Elevation acquisition, measured **separately** | 600 s, 250 MB download |

Output locations:

- bulk and cache: `scratch/wbgt_outdoor_pilot_qc/`
- compact evidence: `docs/diagnostics/wbgt_outdoor_pilot_qc/`
- elevation tiles and derived cell grids: `scratch/wbgt_outdoor_pilot_qc/elevation/`

The resolved-path write guard is extended. In addition to milestone 4's
protected roots it refuses, on the **resolved** path including symlink
destinations:

- `docs/diagnostics/wbgt_outdoor_pilot` — milestone 4's own evidence;
- `scratch/wbgt_outdoor_pilot` — milestone 4's cache;
- every predecessor evidence directory already protected by milestone 4;
- `processed`, `processed_optimised`, `wbgt_shade_national` and `IRT_DATA_DIR`.

No isolated output path is ever handed to a tool that independently resolves
production paths. No existing process is stopped, restarted or signalled.

### 8.1 Cache signature

The method signature is bumped to `outdoor-pilot-v2` and now carries, in
addition to every milestone-4 element:

- `rh-<policy>` and the policy version;
- the elevation identity: convention name plus, for GMTED, the sampling method
  and the SHA-256 digest of the tile set;
- the pressure convention (unchanged formulation, declared explicitly);
- the calendar and completeness settings (unchanged).

A `v2` signature therefore never equals a `v1` signature, so **no milestone-4
cache can be reused here**. Milestone 4's strict results are preserved on disk
and are reproduced by direct numerical comparison (A1) rather than by cache
reuse. An incompatible resume artifact is refused, not repaired.

## 9. What is reused and what is re-run

- The pilot runner is **revised, not copied**: `tools/diagnostics/wbgt_outdoor_pilot.py`
  gains the RH policy and the elevation field as parameters whose defaults are
  milestone 4's behaviour. The frozen W1 physics is untouched.
- The follow-up stages live in a new diagnostic module,
  `tools/diagnostics/wbgt_outdoor_pilot_qc.py`.
- **C1 is not re-run.** Milestone 4's method-sensitivity artifacts stand as
  published. This milestone changes inputs and elevation, not the candidate, so
  a fresh W1-vs-C1 comparison would answer a question this milestone did not ask.
- No production module imports either diagnostic tool, and no production helper
  is modified to accommodate them.

## 10. Corrections to milestone 4's wording

Carried into the new report, and the reason each is wrong:

1. "The blocker is input data, not code" — true only of the **observed coverage
   failures**. It is not true of the outdoor limitations in general, which
   include the uncorrected NEX distribution, the unvalidated W1 wind shape and
   the inferred day boundary.
2. "0.250 °C between 0 m and 2276 m" was a **measured single example at fixed
   drivers**, not a national upper bound on the elevation effect. This milestone
   replaces it with a measured distribution over real cells.
3. The method-versus-model sensitivity comparison applied to **different scopes**
   (W1-vs-C1 over three states and 581 blocks; ACCESS-vs-MRI over Himachal and
   81 blocks). Their rank-shift magnitudes are not comparable, and the report
   must not place them side by side as if they were.
4. "Blocks tile districts exactly" does not follow from aggregation equality.
   Aggregation equality proves **aggregation consistency under the tested
   support**. Tiling is tested independently in §7.

## 11. Tests

New focused tests, `tests/test_wbgt_outdoor_pilot_qc.py`, covering:

- `hurs` below, at and above 100 %; negative values; NaN; the 1e20 fill value;
- the adjustment flags, and that the raw input array is left unmodified;
- inverted `tasmin`/`tasmax` invalidated rather than repaired, under both policies;
- elevation units, missing values, negative values, the explicit-failure path and
  the flagged fallback, and the ISA pressure conversion at a known elevation;
- the coverage denominators, the three classes and the all-invalid unit;
- common-support comparison;
- geometry gaps, overlaps and child-outside-parent on small synthetic polygons;
- cache invalidation when the RH policy or the elevation identity changes.

Milestone 4's parity and aggregation tests are retained unchanged and must still
pass. The affected outdoor suites and the shade release regression suite are run;
unrelated broad suites are not.

Manual inspection, required before the verdict: one corrected Himachal humidity
record, one unaffected cell, the invalid Kerala temperature record, Lahul and
Spiti district with Spiti block, and one lowland plus one mountain elevation
sample.

## 12. Stop conditions

- A1 or A2 fails → stop before the sensitivity runs and report an engineering
  regression.
- A write guard refuses a path → stop; do not relax the guard.
- The budget of §8 is exceeded → checkpoint, report the completed scope and give
  the exact continuation command.
- A rule frozen in this document is **not** changed in response to a result it
  produces.
