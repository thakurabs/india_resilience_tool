# Outdoor WBGT pilot — input-quality and elevation follow-up

**Milestone 4b. CHG-0623 … CHG-0627.** Specification frozen first in [`SPEC.md`](SPEC.md).
Predecessor: [`../wbgt_outdoor_pilot/`](../wbgt_outdoor_pilot/) (milestone 4, commit `459d5c7`,
verdict `ENGINEERING CONDITIONAL`), whose evidence this milestone reads but never writes.

Scope unchanged and not widened: **Kerala, Rajasthan, Himachal Pradesh; ACCESS-CM2 `r1i1p1f1`
historical 2005; district and block; MRI-ESM2-0 on Himachal Pradesh only.** 784 climate cells,
75 districts, 581 blocks. No new candidate, no new year, no bias correction, no gate change.

> **Every number here is DIAGNOSTIC-ONLY** — including temperature levels, threshold counts and
> every ranking. Nothing enters the dashboard, the production metric registry, composites, frozen
> rulers or published maps.

---

## The three conclusions

| # | Conclusion | Verdict |
|---|---|---|
| 1 | **Engineering implementation** — correctness, reproducibility, runtime, isolation | **`ENGINEERING PASS`** |
| 2 | **Input / coverage readiness** | **`CONDITIONAL`** — the RH treatment is an *experimental physical-bound* treatment, not source-supported repair |
| 3 | **Scientific readiness** | **unchanged: diagnostic-only.** This milestone resolves no NEX bias and validates no W1 ranking |

All **eight** frozen acceptance criteria pass ([`acceptance.csv`](acceptance.csv)):

| ID | Criterion | Result |
|---|---|---|
| A1 | `baseline` reproduces milestone 4's published district and block tables | **exact** — 0.0 °C, 0 count mismatches, 0 area-fraction mismatches, 75 + 581 units |
| A2 | Per-cell parity holds under the *treated* path | **7 of 7**, `0.000e+00 °C`, batch/chunk/resume bit-identical |
| A3 | The RH treatment restores exactly the cell-years whose only defect was `hurs > 100 %` | **11 restored**; the Kerala `tasmin > tasmax` cell stays NaN |
| A4 | Elevation changes no validity | valid cell-year count identical to `baseline` |
| A5 | A unit with no valid support never carries a number | 0 violations |
| A6 | Milestone 4's admin-correctness checks still pass | **168 of 168** |
| A7 | Geometry within the declared tolerances | **243 of 243**, discrepancy **exactly 0 m²** |
| A9 | Budget respected | §7 |

**One concrete next action** is at §9.

---

## 1. Why `hurs` exceeds 100 %, and what was done about it

### 1.1 The source investigation ([`input_quality_hurs.csv`](input_quality_hurs.csv))

Read-only, from the actual pilot inputs:

| Property | ACCESS-CM2 | MRI-ESM2-0 |
|---|---|---|
| `units` / `cell_methods` | `%` / `area: time: mean` | `%` / `area: time: mean` |
| `scale_factor` / `add_offset` | **none** | **none** |
| `valid_range` / `valid_min` / `valid_max` | **not published** | **not published** |
| `_FillValue`, `missing_value` | `1e+20`, decoded to NaN correctly | same |
| Raw vs decoded at the offending positions | **identical** | **identical** |
| Observed maximum | **103.623 %** | **106.756 %** |
| Cell-days above 100 % (India, 2005) | 1663 of 3 721 905 finite (0.045 %) | 974 (0.026 %) |
| above 101 % / 102 % / 105 % | 724 / 254 / 0 | 234 / 88 / 5 |
| Negative values | none | none |

**Decoding is not the cause. Coordinate and time alignment are not the cause.** All six variables
share one 0.25° grid, 365 `proleptic_gregorian` timestamps and `variant_label r1i1p1f1`.

The file's own `comment` attribute states RH is defined with respect to liquid water above 0 °C
and with respect to ice below it, which permits values above 100 % at sub-freezing temperatures.
That explains only a minority nationally — 1.9 % (ACCESS-CM2) and 2.6 % (MRI-ESM2-0) of the
offending cell-days have a daily-mean `tas` below 0 °C, and the median is +14.1 °C / +12.7 °C.
The dominant cause is the product's own method: NEX-GDDP-CMIP6 is BCSD, and quantile mapping of a
bounded variable is not bound-enforced. **No source-documented tolerance exists.**

Notably, the Himachal pilot cells *are* in the cold minority. All eleven sit at
**4050–5308 m** ([`rh_adjustments.csv`](rh_adjustments.csv)), with daily-mean `tas` of 3.3–5.0 °C
and `tasmin` below 0 °C on most offending days.

### 1.2 The adopted policy

Because no documented tolerance exists, the only defensible option is an **explicit sensitivity
treatment**: any **finite** `hurs` above 100 % is replaced by exactly 100.0 % for computation.

**This is an experimental physical-bound treatment, not accepted source repair.** It is labelled
that way in the method signature, the cache key, every manifest and every limitation list.

- **No numeric allowance ceiling was adopted.** A 102 % allowance would have been fitted to one
  cell's 101.208 % and immediately falsified: the observed maxima are 103.623 % and 106.756 %.
  Every finite excess is clipped and its magnitude reported instead.
- Clipping is **monotone and conservative** — it lowers vapour pressure, so it can only lower
  WBGT. It cannot manufacture an exceedance.
- **Source files are never modified**; the clip happens in memory, and a regression test asserts
  the caller's array is left byte-identical.
- **Missing stays missing**; **negative `hurs` stays invalid**.
- A **flagged corrected day** and an **unresolved invalid day** remain distinguishable in every
  output: `rh_clipped_days` versus `input_invalid_days`.
- **Temporal completeness was not relaxed.** The complete-365 rule still applies *after* the
  treatment.

### 1.3 What the treatment actually corrected, per cell-year

Inside the pilot footprint the corrections are much smaller than the national maxima
([`rh_adjustments.csv`](rh_adjustments.csv), [`input_validity_by_run.csv`](input_validity_by_run.csv)):

| Scope | Clipped cell-days | Max correction | Cells affected |
|---|---|---|---|
| Himachal Pradesh, ACCESS-CM2 | 21 | **1.208 pp** | 11 of 116 |
| Himachal Pradesh, MRI-ESM2-0 | 54 | **1.402 pp** | 11 of 116 |
| Rajasthan, ACCESS-CM2 | 3 | 1.463 pp | 0 of 582 intersecting (the three days fall in non-intersecting grid cells) |
| Kerala, ACCESS-CM2 | 0 | — | 0 of 86 |

**No value above 102 % occurs anywhere in this footprint.** The 103.6 % and 106.8 % extremes are
elsewhere in India and are *not* evidence about these cells — they are the reason a fixed
allowance was refused.

**Manual inspection, one corrected record** — cell 32.625 N / 77.625 E, 4887 m… 5208 m
(GMTED 5208.5 m):

| Date | `hurs` raw | clipped to | correction | `tas` | `tasmin` | `tasmax` |
|---|---|---|---|---|---|---|
| 2005-07-08 | 100.0919 % | 100.0 % | 0.092 pp | 4.21 °C | −0.56 °C | 8.98 °C |
| 2005-07-09 | 101.0554 % | 100.0 % | 1.055 pp | 4.00 °C | 0.46 °C | 7.55 °C |
| 2005-07-18 | 100.3091 % | 100.0 % | 0.309 pp | 5.00 °C | 0.63 °C | 9.36 °C |
| 2005-08-15 | 101.0684 % | 100.0 % | **1.068 pp** | 3.30 °C | −0.41 °C | 7.01 °C |

**Manual inspection, one unaffected cell** — 32.125 N / 77.625 E: annual maximum `hurs`
**99.726 %**, zero days above 100 %, two days at or above 99 %. Nothing was flagged and nothing
was changed there, which is the point: the treatment reaches only the cells that need it.

## 2. The inverted temperature range is a source defect, not ours

[`input_quality_temperature.csv`](input_quality_temperature.csv), cell 12.375 N / 74.875 E
(Kasaragod, Kerala), 2005-11-03:

| Property | Finding |
|---|---|
| Dates, calendar, coordinates | identical across all six variables; nearest-neighbour hits the exact cell centre |
| Units | `tasmin`/`tasmax`/`tas` all `K`; decoding correct |
| Provenance | all `ACCESS-CM2`, `historical`, `variant_label r1i1p1f1` (per-variable `tracking_id` differs, which is normal for one file per variable) |
| Values | `tasmin` **25.924 °C**, `tasmax` **25.167 °C**, `tas` 25.545 °C — `tas` lies *between* them |
| Extent | 1 day at this cell; 1065 of 3 721 905 national finite cell-days (0.029 %); worst national inversion **−1.567 °C** |

**Classification: unresolved source defect.** There is no ingestion or alignment error to correct
— BCSD bias-corrects `tasmin` and `tasmax` independently and does not enforce their ordering.

The day therefore stays invalid and the cell-year stays NaN. The values were **not** swapped,
averaged, interpolated or filled from neighbours, and a regression test asserts that the ordering
check invalidates rather than repairs, **under both RH policies** — so the humidity treatment can
never rescue a cell whose defect is the temperature ordering.

Consequence, carried in every table: **`Kasaragod||Kasargod` remains `partial_coverage` at
0.9476 valid area in all four runs.** It is the only unit in the footprint that the combined
treatment does not resolve.

## 3. Elevation

### 3.1 The source ([`elevation_source.csv`](elevation_source.csv))

No `orog` exists in either NEX tree and no DEM asset exists anywhere under `IRT_DATA_DIR`, so an
external product was acquired.

| Property | Value |
|---|---|
| Product | **GMTED2010**, `mea` (mean) aggregate, **30 arc-second** (≈ 0.93 km) |
| Publisher / licence | USGS / NGA — **public domain** |
| Tiles | `10S060E`, `10N060E`, `30N060E`, version stamp `20101117` |
| URL root | `https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/topo/downloads/GMTED/Global_tiles_GMTED/300darcsec/mea/E060/` |
| Identity | SHA-256 per tile; tile-set digest `elev-gmted2010-mea300-cellmean-7dcdaeb7a588c910` |
| Horizontal / vertical reference | WGS84 geographic / **metres above the EGM96 geoid** |
| Download | **49.6 MB in 21.2 s**, measured separately from computation |

30 arc-second was chosen deliberately: a 0.25° cell holds ≈ 784 GMTED samples, which is ample for
a cell mean, while the 15 and 7.5 arc-second variants would be 69 MB and 276 MB *per tile* for no
gain in an average.

### 3.2 Sampling and checks ([`elevation_cells.csv`](elevation_cells.csv))

Each cell's elevation is the **cos(latitude)-weighted mean of every GMTED pixel whose centre
falls inside its 0.25° box** — an area mean on the sphere. Nodata is excluded from the mean, never
counted as zero. **A missing elevation is an explicit failure**, not a silent sea-level
substitution; the fallback exists but must be requested and is then flagged per cell. Negative
elevations are retained, not clipped.

| State | Cells | Finite | Missing | Negative | min | median | max | GMTED pixels | nodata |
|---|---|---|---|---|---|---|---|---|---|
| Kerala | 330 | 330 | **0** | 0 | 0.0 m | 23.1 m | 2010.7 m | 297 000 | 0 |
| Rajasthan | 1287 | 1287 | **0** | 0 | 0.0 m | 220.5 m | 2301.3 m | 1 158 300 | 0 |
| Himachal Pradesh | 270 | 270 | **0** | 0 | 211.6 m | **2622.9 m** | **5563.2 m** | 243 000 | 0 |

The regional ordering declared before inspection — Kerala < Rajasthan < Himachal Pradesh on the
median — holds. **No cell fell back to sea level in any run.**

The elevation-to-pressure formulation is **unchanged** (`m1.barometric_pressure_hpa`, ISA); only
its input changed.

**Manual inspection, one lowland and one mountain sample:**

| Sample | Elevation | ISA pressure |
|---|---|---|
| Kerala coastal cell, lowest in the footprint | 0.03 m | 1013.25 hPa |
| Himachal cell 32.625 N / 78.125 E, highest intersecting | **5308.5 m** | **518.30 hPa** |

### 3.3 The measured elevation effect — and the milestone-4 claim it replaces

Milestone 4 reported *"0.250 °C between 0 m and 2276 m at fixed drivers."* That was a **single
measured example**, not a national upper bound, and this milestone states it as such. Measured
over real cells instead ([`cell_comparisons.csv`](cell_comparisons.csv), `baseline` → `elev`,
common support, every cell changed):

| State | mean Δ annual mean | range | mean Δ `days_ge_32` | range |
|---|---|---|---|---|
| Kerala | −0.019 °C | −0.129 … −0.000002 | −0.22 d | −5 … +1 |
| Rajasthan | −0.066 °C | −0.187 … −0.002 | **−1.81 d** | **−7 … +1** |
| Himachal Pradesh | −0.123 °C | **−0.261** … **+0.025** | −0.13 d | −4 … +1 |

Three things worth naming:

1. The **level** effect is small — at most 0.261 °C even at 5308 m, because Liljegren is weakly
   pressure-sensitive. Milestone 4's conclusion that the missing DEM was a bounded limitation
   rather than a blocker survives.
2. The **sign is not uniform**: Himachal reaches **+0.025 °C**. A single-point example could not
   have shown that.
3. The **count** effect is largest where it is least expected — **Rajasthan**, at 200–770 m, loses
   up to **7 exceedance days** at a cell and 3.6 at a district (Phalodi 86.69 → 83.08). That is
   because ≥32 °C is actively crossed there, so a small level shift moves many days across the
   threshold. Himachal's far larger pressure change moves almost no counts, because its WBGT
   never approaches 32 °C.

## 4. Separating the two effects

[`cell_comparisons.csv`](cell_comparisons.csv), [`unit_comparisons.csv`](unit_comparisons.csv).
Each pair is compared on **each run's own support** and on the **common valid cells**, and every
per-unit row carries both the value delta and the valid-area delta.

| Pair | Support change | Value change on common cells | Attribution |
|---|---|---|---|
| `baseline` → `rh` | **+11 cells** (Himachal) | **exactly 0.000** on every field | **pure coverage effect** |
| `baseline` → `elev` | **none** (0 of 75 districts, 0 of 581 blocks) | −0.192 … −0.0004 °C per district | **pure value effect, fully attributable** |
| `rh` → `combined` | none | elevation effect on the restored footprint | attributable to elevation |
| `baseline` → `combined` | +11 cells | both | **not attributable to either treatment alone** |

The RH treatment changes **no value at all** on a cell that was already valid, because the clipped
days had previously destroyed the whole cell-year. That is a clean separation: **the humidity
treatment buys coverage, the elevation field changes values.**

Where both moved, the runner says so. All three districts whose value changed between `baseline`
and `rh` also gained area, and each row is marked *"value and support both changed; not
attributable to the treatment alone"*:

| District | `baseline` | `rh` | Δ | valid-area Δ | class |
|---|---|---|---|---|---|
| Lahul and Spiti | 8.250 °C | 7.674 °C | −0.576 | +5.51 × 10⁹ m² | partial → **meets screen** |
| Kullu | 13.665 | 13.428 | −0.238 | +3.88 × 10⁸ m² | partial → meets screen |
| Kangra | 22.054 | 21.869 | −0.184 | +9.47 × 10⁷ m² | partial → meets screen |

Lahul and Spiti's mean falls because the cells the treatment restored are the coldest and highest
in the district — a coverage artefact, not a cooling.

Same-date weather accuracy against ERA5 was **not** computed: these are freely evolving
climate-model simulations and such a comparison would be invalid.

## 5. Coverage, with the denominator defined

[`coverage_classification.csv`](coverage_classification.csv). Every unit reports full polygon
area, represented area, valid area, all three fractions and its contributing cell counts, all in
**EPSG:6933** so the areas are mutually consistent.

### 5.1 The 0.99 screen

Milestone 4 screened `valid_intersected_area / total_intersected_area` at 0.99 but its
specification did not say so. **Defined here rather than silently reinterpreted: the screen is
applied to `valid_fraction_of_represented`** — exactly the quantity milestone 4 screened, so its
published numbers keep their meaning. `valid_fraction_of_full` is reported alongside as a
stricter second view and is **not** the screen.

In this footprint the choice changes nothing: **`represented_fraction` is 1.000 for every unit at
both levels**, so the two denominators coincide. The climate grid fully represents every polygon;
there is no coastal or small-block support loss.

### 5.2 Classification

| Model | Run | Districts `meets_screen` / `partial` | Blocks `meets_screen` / `partial` |
|---|---|---|---|
| ACCESS-CM2 | `baseline` | 72 / **3** | 576 / **5** |
| ACCESS-CM2 | `rh` | **75 / 0** | **580 / 1** |
| ACCESS-CM2 | `elev` | 72 / 3 | 576 / 5 |
| ACCESS-CM2 | `combined` | **75 / 0** | **580 / 1** |
| MRI-ESM2-0 | `baseline` | 10 / 2 | 78 / 3 |
| MRI-ESM2-0 | `combined` | **12 / 0** | **81 / 0** |

**No unit is `no_valid_coverage` in any run.** Nothing was filled spatially, and no NaN became a
zero.

**Manual inspection, Lahul and Spiti and its Spiti block:**

| Unit | Run | valid area fraction | valid cells | annual mean | class |
|---|---|---|---|---|---|
| Lahul and Spiti (district) | `baseline` | **0.605** | 27 of 38 | 8.250 °C | partial |
| Lahul and Spiti (district) | `combined` | **1.000** | 38 of 38 | 7.579 °C | meets screen |
| `Lahul and Spiti‖Spiti` | `baseline` | **0.550** | 13 of 21 | 8.238 °C | partial |
| `Lahul and Spiti‖Spiti` | `combined` | **1.000** | 21 of 21 | 7.307 °C | meets screen |
| `Kasaragod‖Kasargod` | all four | 0.948 | 3 of 4 | 30.02–30.03 °C | **partial in every run** |

Under MRI-ESM2-0 the Spiti block starts even lower, at 0.493, and also reaches 1.000.

### 5.3 Partial units are excluded from ranking

[`ranking_summary.csv`](ranking_summary.csv) ranks only units meeting the declared screen and says
so on every row. In `baseline` that excludes 3 districts and 5 blocks; in `combined`, 0 and 1.
A partial unit keeps its diagnostic value in the value tables but is never ranked against whole
ones, and **no estimate formed over 55 % of a block is presented as a block value.**

The metric name travels with the number: the exceedance fields are the **area-weighted mean
annual cell exceedance days**, not the number of days on which an entire district exceeded a
threshold. The level field is the **annual mean of daily maximum outdoor WBGT** — never "daily
mean WBGT".

## 6. Geometry, tested independently

[`geometry_checks.csv`](geometry_checks.csv) — 243 checks, run directly on the pilot boundaries in
EPSG:6933 against tolerances declared before evaluation (0.1 % of the relevant area, with a
10 000 m² sliver floor).

| Check | n | Failed | Max discrepancy |
|---|---|---|---|
| District names unique | 3 | 0 | — |
| Block names unique within district | 3 | 0 | — |
| Every block's parent district present | 3 | 0 | 0 orphans |
| District geometries valid and non-empty | 3 | 0 | 0 invalid |
| Block geometries valid and non-empty | 3 | 0 | 0 invalid |
| Block geometries distinct | 3 | 0 | 0 duplicates |
| **Gap: blocks cover their district** | 75 | 0 | **0.0 m²** |
| **Child area outside its parent** | 75 | 0 | **0.0 m²** |
| **Pairwise block overlap** | 75 | 0 | **0.0 m²** |

The gap, outside-parent and overlap areas are **exactly zero**, not merely inside tolerance. So the
blocks do tile their districts — and that is now established **geometrically**.

**Correcting milestone 4:** it inferred tiling from the block-to-district aggregation matching to
1.4 × 10⁻¹⁴ °C. That inference was invalid. Aggregation equality verifies **aggregation
consistency under the tested support**, nothing more. The reconciliation is retained
([`district_vs_block_rollup.csv`](district_vs_block_rollup.csv), max |difference| **1.42 × 10⁻¹⁴ °C**
over all 75 districts in every run) and is now labelled with what it actually proves.

## 7. Cost ([`timings.csv`](timings.csv))

| Quantity | Measured | Declared ceiling |
|---|---|---|
| Elevation acquisition, measured separately | **21.2 s, 49.6 MB** | 600 s, 250 MB |
| Compute wall, cold (all 14 state × run jobs) | **749.8 s** | 3600 s |
| Compute wall, warm (every cell grid cached) | 197.1 s | 3600 s |
| Peak process RSS | **0.78 GB** | 4 GB |
| Artifacts | **54.2 MB** (49.6 MB of it the GMTED tiles) | 5 GB |
| Workers | **1**, throughout | 1 |

Per-cell cost 0.13–0.16 s, except Himachal under `strict` at 0.25 s/cell — an invalid cell-day
still costs a full year of solver work before the year is discarded, so treating the inputs makes
Himachal **faster** (0.25 → 0.15 s/cell). Per-state setup is 42–47 s and is dominated by reading
the national block GeoJSON; it is paid once per state and shared across all four runs.

**No national extrapolation is offered.** Setup cost is boundary-dominated and per-state, and
three states are not a basis for projecting 36.

## 8. What this milestone does *not* establish

Carried on every manifest and every table-bearing artifact:

- **DIAGNOSTIC-ONLY.** No output enters the dashboard, metric registry, composites, frozen rulers
  or published maps. All thresholds, including ≥28 and ≥30 °C, remain diagnostic-only on NEX.
- The RH clip is an **experimental physical-bound treatment, not accepted source repair.**
- Uncorrected NEX inputs; no bias correction is applied; remaining NEX distribution and count
  errors are unresolved.
- Six-site reconstruction evidence does not establish national accuracy.
- W1 may benefit from compensating reconstruction errors; **its wind shape is a declared
  assumption**, not an established physical reconstruction.
- ≥32 °C outcomes include reference-sensitive results; rare-event regimes remain insufficiently
  evaluated.
- **Spatial ranking accuracy is not established.**
- Daily time-boundary semantics remain **inferred**; no NEX file publishes `time_bnds`.

### 8.1 Four milestone-4 overstatements, corrected

1. *"The blocker is input data, not code."* True **only of the observed coverage failures.** It
   does not extend to the outdoor limitations in general — the uncorrected NEX distribution, the
   unvalidated W1 wind shape and the inferred day boundary are all unresolved and none is a data
   availability problem.
2. *"0.250 °C between 0 m and 2276 m."* A **measured single example at fixed drivers, not a
   national upper bound.** Replaced by the measured distribution of §3.3, which also shows the
   sign is not uniform and that the count effect is largest in Rajasthan, not Himachal.
3. **Method-versus-model sensitivity was compared across different scopes.** Milestone 4 placed a
   226-place W1-vs-C1 block shift (three states, 581 blocks) beside a 26.5-place ACCESS-vs-MRI
   shift (Himachal, 81 blocks). Those magnitudes are **not comparable** and should not have been
   shown side by side. This milestone did not re-run C1 — it changed inputs and elevation, not the
   candidate — so milestone 4's method-sensitivity artifacts stand as published, with that caveat
   attached.
4. *"Blocks tile districts exactly"* did **not** follow from aggregation equality. Tiling is now
   tested directly (§6) and does hold, exactly — but on geometric evidence, not on the earlier
   invalid inference.

## 9. The one outstanding requirement, and the next action

**Input/coverage readiness is `CONDITIONAL` on exactly one thing: the RH clip is not
source-supported.** NEX-GDDP-CMIP6 publishes no `valid_range` for `hurs` and documents no
tolerance, so clipping to the physical bound is our judgement, not the provider's. Everything else
is resolved: coverage reaches the declared screen for every district and every block but one, that
one exception is a documented unresolved source defect, and no unit lacks valid support.

**Next action — ask NASA NEX whether `hurs` above 100 % is expected and how they recommend
handling it** (contacts are in the files' own `contact` attribute). Their answer converts this
`CONDITIONAL` into either a documented treatment or a reason to retain invalidity; it is the only
input that can, and it costs no compute. Attach the measured evidence of §1.1: no `valid_range`,
correct decoding, 0.045 % of finite cell-days, maxima 103.6 % and 106.8 %, concentrated at cold
high-elevation cells.

Not next: more states, years, candidates or models. Nothing measured here is blocked by any of
them, and the pilot's scope is deliberately closed.

## 10. Isolation

- Tracked changes: `MANIFEST.md`, `tools/README.md`, `tools/diagnostics/wbgt_outdoor_pilot.py`
  (new parameters only, defaults are milestone 4's behaviour), plus three new files.
- `git status` over `india_resilience_tool/`, the three shade tools and **all** predecessor
  evidence directories — including milestone 4's own — returns **0 lines**.
- The write guard resolves symlinks and refuses, in addition to milestone 4's protected roots,
  `docs/diagnostics/wbgt_outdoor_pilot` and `scratch/wbgt_outdoor_pilot`. Asserted by test over
  nested paths as well as exact ones.
- `outdoor-pilot-v2` signatures can never equal `outdoor-pilot-v1`, so **no milestone-4 cache is
  reachable from here** — which is why A1 is an explicit numerical comparison of the two published
  tables rather than a cache hit.
- The shade release build (pid 60808) and the Maharashtra indices run (pid 28124) were running
  throughout and were never stopped, restarted or signalled. One worker was used throughout.
- No production module imports either diagnostic tool (asserted by test).

## 11. Tests

`tests/test_wbgt_outdoor_pilot_qc.py` — **72 tests**, covering RH below/at/above 100 %, negatives,
NaN and the 1e20 fill value, the adjustment flags and the untouched caller array, inverted
temperature ordering under both policies, elevation units, missing values, negatives, the explicit
failure and the flagged fallback, the ISA conversion, the coverage denominators and all three
classes, common-support comparison, geometry gaps/overlaps/outside-parent on synthetic polygons
including a sub-floor sliver, cache invalidation on RH policy and elevation identity, and the
wording corrections of §8.1.

Affected suites together: **337 passed, 1 skipped** (`tests/test_wbgt_outdoor_pilot_qc.py`,
`test_wbgt_outdoor_pilot.py`, `test_wbgt_outdoor_feasibility.py`, `test_wbgt_outdoor_selection.py`,
`test_wbgt_outdoor_humidity.py`, `test_wbgt_shade_release.py`). The one skip is milestone 4's
symlink-destination case, which needs a Windows privilege unavailable here; the same refusal is
covered non-symbolically by the nested-path cases, so the guard itself is not untested. No
unrelated broad suite was run.

## 12. Reproducing this

```bash
# Elevation only, measured separately (~21 s, ~50 MB)
python -m tools.diagnostics.wbgt_outdoor_pilot_qc --acquire-only

# Contracts and isolation, no compute
python -m tools.diagnostics.wbgt_outdoor_pilot_qc --dry-run

# The full follow-up: four runs on three states plus MRI-ESM2-0 on Himachal (~750 s cold)
python -m tools.diagnostics.wbgt_outdoor_pilot_qc

# Milestone 4's own configuration through the revised runner (must reproduce it exactly)
python -m tools.diagnostics.wbgt_outdoor_pilot_qc --runs baseline --skip-second-model

python -m pytest tests/test_wbgt_outdoor_pilot_qc.py tests/test_wbgt_outdoor_pilot.py -q
```

## 13. Files

| File | Rows | Contents |
|---|---|---|
| [`SPEC.md`](SPEC.md) | — | the contract, frozen before any WBGT score of this follow-up existed |
| [`acceptance.csv`](acceptance.csv) | 8 | the frozen acceptance criteria and their results |
| [`input_quality_hurs.csv`](input_quality_hurs.csv) | 2 | `hurs` identity, decoding, fill, valid-range absence and extent, per model |
| [`input_quality_temperature.csv`](input_quality_temperature.csv) | 1 | the Kerala inversion, its provenance and its classification |
| [`rh_adjustments.csv`](rh_adjustments.csv) | 33 | per cell-year corrected-day count and maximum correction |
| [`input_validity_by_run.csv`](input_validity_by_run.csv) | 14 | validity before and after treatment, per state and run |
| [`elevation_source.csv`](elevation_source.csv) | 3 | tile URL, bytes, SHA-256, acquisition time |
| [`elevation_cells.csv`](elevation_cells.csv) | 4 | per-grid sampling diagnostics and plausibility statistics |
| [`cell_summary_by_run.csv`](cell_summary_by_run.csv) | 14 | cells computed, complete cell-years, clipped days, fallback count |
| [`district_values.csv`](district_values.csv) | 324 | district values for every run, with coverage class |
| [`block_values.csv`](block_values.csv) | 2486 | block values for every run, with coverage class |
| [`coverage_classification.csv`](coverage_classification.csv) | 2810 | all three areas, all three fractions, the class and the screen denominator |
| [`cell_comparisons.csv`](cell_comparisons.csv) | 64 | per-field cell deltas on common support, with gained/lost counts |
| [`unit_comparisons.csv`](unit_comparisons.csv) | 3373 | per-unit value delta beside valid-area delta, with attribution |
| [`district_vs_block_rollup.csv`](district_vs_block_rollup.csv) | 324 | block-to-district reconciliation, labelled with what it proves |
| [`ranking_summary.csv`](ranking_summary.csv) | 48 | rankings restricted to units meeting the coverage screen |
| [`geometry_checks.csv`](geometry_checks.csv) | 243 | independent geometry tests with absolute and fractional discrepancies |
| [`admin_checks.csv`](admin_checks.csv) | 168 | milestone 4's admin-correctness checks, re-run for every run |
| [`parity_checks.csv`](parity_checks.csv) | 7 | per-cell parity under the treated path |
| [`timings.csv`](timings.csv) | 14 | setup, load and compute seconds per state and run |
| [`run_manifest.json`](run_manifest.json) | — | git snapshot, package versions, per-run signatures, elevation identity, budgets, limitations |

**Root `README.md` needs no update**: no metric slug is registered, no deployed methodology
changes, and no new operator command enters the documented dashboard workflow. `tools/README.md`
and `MANIFEST.md` are updated for the new runner.
