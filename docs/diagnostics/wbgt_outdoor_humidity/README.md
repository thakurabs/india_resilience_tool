# Outdoor-WBGT humidity consistency — decision report

**Milestone 3.** Contract: [`SPEC.md`](SPEC.md), frozen 2026-09-29 before any candidate score was
computed (CHG-0614). Tool: `tools/diagnostics/wbgt_outdoor_humidity.py` (CHG-0615).
Runs: `GIT:add_flood_depth@99e478a` (dirty), `--workers 1`, six sites, two windows —
`--stage all` (3,032 s) and then `--stage residuals` (69 s), the latter completing SPEC.md 9.1's
per-candidate daily-RH residual for the existing humidity as well as the new one.
Predecessors, unmodified and unrewritten: [`../wbgt_outdoor_feasibility/`](../wbgt_outdoor_feasibility/)
and [`../wbgt_outdoor_selection/`](../wbgt_outdoor_selection/).

---

## Decision

> **Selected method: B — existing humidity (constant daily vapour pressure) + W1 DTR-dependent
> wind.** Unchanged from milestone 2's `W1`. The input-consistent humidity formulation is
> **not adopted.**
>
> **Verdict: `CONDITIONAL`.** B is the best development candidate for a bounded staged
> validation pilot, on one named condition (§9). It is **not** production-ready and **not**
> ready for national publication.

The experiment did what it was designed to do: it tested a candidate and the candidate failed
the predeclared screen. Nothing here was added or relaxed after the numbers were seen.

### What the new formulation achieved

1. **It solves the stated problem exactly.** Over all 54,774 site-window days, the reconstructed
   hourly RH reproduces the supplied daily `hurs` to a maximum absolute residual of
   **9.81 × 10⁻⁷ percentage points** — inside the predeclared 10⁻⁶ pp tolerance — with **zero
   solver failures** and a maximum of **27 bisection iterations** against the 200-iteration
   guard. **The existing method misses the same target on the same days by a mean absolute
   1.17–2.29 pp, a 95th percentile of 2.9–4.9 pp and a maximum of 10.95 pp**, exceeding 1 pp on
   50–79 % of days and 5 pp on 0.3–4.6 % (`humidity_residual_comparison.csv`). So the defect the
   new formulation targets is real and quantified, not notional.
2. **It reduces the all-hours humidity error against observations.** Reconstructed hourly RH
   mean absolute error falls at **12 of 12** site-windows (e.g. Kochi 2005–2014: 5.46 → 4.89 pp;
   RMSE 6.99 → 6.16 pp), and the whole-day RH bias collapses to ~0 by construction.
3. **It passes the daily gate everywhere**, as do all four candidates: 12/12 site-window pairs,
   worst median absolute error 0.87 °C (C) and 0.76 °C (D) against the 1.0 °C bar.

### What it worsened — and why it was rejected

**The all-hours improvement does not reach the hour that sets the daily maximum.** At the
**reference WBGT peak hour** the new formulation is *further* from the observed humidity at five
of six sites:

| site (2005–2014) | RH bias at reference WBGT peak, existing → new | effective vapour-pressure bias, existing → new |
|---|---|---|
| Kochi | +1.06 → **+3.23 pp** | +0.77 → **+1.73 hPa** |
| Kolkata | +2.51 → **+3.65 pp** | +1.20 → **+1.75 hPa** |
| Hyderabad | +1.22 → **+1.84 pp** | +0.78 → +1.04 hPa |
| Shimla | +1.88 → **+2.10 pp** | −0.06 → −0.01 hPa |
| Lucknow | 0.00 → **+0.35 pp** | +0.36 → +0.47 hPa |
| **Bikaner** | +0.76 → **+0.41 pp** *(improves)* | +1.00 → +0.84 hPa *(improves)* |

The mechanism is measured, not inferred. The constraint is satisfied by choosing a single daily
vapour-pressure parameter `e*`, and at the five moist-to-moderate sites `e* > e_old` on 46–91 %
of days (Kochi +0.96 hPa mean, Kolkata +0.55, Hyderabad +0.26). The existing method was too
**dry on the day average** but already too **moist at the afternoon peak**; adding moisture to
fix the average therefore makes the peak worse. Bikaner is the exception — there `e* < e_old`
(−0.16 to −0.19 hPa mean, only 23–25 % of days moister) and the peak improves.

**This is a per-site measurement, not a universal afternoon-drying mechanism.** The direction
reverses at Bikaner, and no single-site ordering is offered as an explanation.

Consequently the extreme-tail counts deteriorate. `q99_difference_c` rises at **all 12**
site-windows under both wind treatments, and the annual ≥ 32 °C count error grows at the moist
sites:

| ≥ 32 °C, error vs `ref_audited` (days/yr) | A (old, C1) | C (new, C1) | B (old, W1) | D (new, W1) |
|---|---|---|---|---|
| Kochi 2005–2014 (ref 206.3) | +38.25 `ROBUST PASS` | **+51.00 `ROBUST FAIL`** | +15.63 `ROBUST PASS` | +29.25 `ROBUST PASS` |
| Kochi 1990–2004 (ref 192.7) | +22.15 | +37.69 | −4.15 | +13.54 |
| Kolkata 1990–2004 (ref 126.4) | +10.08 `ROBUST PASS` | **+17.31 `REFERENCE-SENSITIVE`** | −0.92 | +7.92 |
| Kolkata 2005–2014 (ref 138.1) | +13.75 | +20.75 | +4.50 | +11.50 |
| Bikaner 1990–2004 (ref 72.6) | +18.23 `ROBUST FAIL` | +18.00 `ROBUST FAIL` | +10.38 | +9.38 |
| Bikaner 2005–2014 (ref 86.4) | +21.25 `ROBUST FAIL` | +21.13 `ROBUST FAIL` | +12.13 | +11.00 |

Hyderabad ≥ 30 °C 2005–2014 also turns `ROBUST PASS` → `ROBUST FAIL` under C (+18.38 → +24.00
against a 23.23 d/yr tolerance).

**The predeclared no-material-deterioration screen (SPEC.md 11 step 4) fired:** Kochi
2005–2014 ≥ 32 °C at C1 wind is `ROBUST PASS` under the existing humidity and `ROBUST FAIL`
under the new one. One pair lost, none gained. That blocked the new-humidity preference, and the
rule fell back to the old-humidity counterpart at the same wind treatment — B.

### The one place D looked better, and why it is not enough

D outranked B on the second ranking key (3 reference-sensitive pairs versus 4) because
Hyderabad 2005–2014 ≥ 32 °C moved from −9.50 to −7.38 d/yr against an 8.98 d/yr tolerance,
crossing into the band under both references. **That is a single pass count changing near a
boundary**, which SPEC.md 9.4 forbids treating as an overall improvement. The paired year-block
resample contradicts the reading: D − B worsens daily RMSE significantly at **10 of 12**
site-windows and improves it at **0**, and the change in mean annual ≥ 32 °C count error is
significantly worse at Kochi 2005–2014 (**+13.63 d/yr**, CI [+11.62, +15.50]) and Kolkata
2005–2014 (**+7.00**, CI [+5.62, +8.38]), significantly better only at Hyderabad 2005–2014
(**−2.13**, CI [−4.13, −0.50]). At Bikaner ≥ 32 °C — the site that actually fails — the point
estimate improves by only 1.0–1.1 d/yr and **the interval includes zero**. The one place the
humidity fix was expected to help is not a statistically resolvable improvement.

---

## 1. Remaining count errors under B (the selected method)

Nothing in this milestone reduces them. They are milestone 2's, re-measured against both
references at all six sites:

- **`ROBUST FAIL` pairs: 0.** **`REFERENCE-SENSITIVE`: 4.** **`ROBUST PASS`: 26.**
  **`NOT EVALUATED` (rare event): 6** — all six Shimla threshold pairs, where the reference
  carries < 5 d/yr. They are **not passes.**
- The four reference-sensitive pairs are the whole remaining count problem:

| site | window | threshold | B per year | ref audited | ref R3 | error audited | error R3 | tolerance | audited | R3 |
|---|---|---|---|---|---|---|---|---|---|---|
| Bikaner | 1990–2004 | ≥ 32 | 83.00 | 72.62 | 65.08 | +10.38 | +17.92 | 14.52 | PASS | FAIL |
| Bikaner | 2005–2014 | ≥ 32 | 98.50 | 86.38 | 77.63 | +12.13 | +20.88 | 17.28 | PASS | FAIL |
| Hyderabad | 1990–2004 | ≥ 32 | 30.31 | 39.38 | 33.77 | −9.08 | −3.46 | 7.88 | FAIL | PASS |
| Hyderabad | 2005–2014 | ≥ 32 | 35.38 | 44.88 | 37.25 | −9.50 | −1.88 | 8.98 | FAIL | PASS |

- Worst absolute annual count error over gated pairs: **15.63 d/yr** (Kochi 2005–2014 ≥ 32 °C, a
  `ROBUST PASS` inside its 41.25 d/yr tolerance). **19 individual complete years** sit outside the
  gate tolerance, so the window-mean picture is not built purely on cancelling years but is not
  free of them either: see `per_year_count_errors.csv`.
- Daily gate under `ref_audited`: worst median absolute error **0.728 °C**, worst RMSE
  **1.240 °C**, against 1.0 / 1.5 °C. Conditional mean error on the reference's hottest 1 % of
  days ranges −2.57 to −0.51 °C — B is systematically *cool* on the hottest days, a statistic
  reported separately from `q99_difference_c` and not interchangeable with it.

**All four ≥ 32 °C failures are reference-sensitive, and the sensitivity has a consistent
structure.** R3 lowers the reference's ≥ 32 °C count at every failing pair (Bikaner 72.6 → 65.1,
Hyderabad 44.9 → 37.3), which turns Bikaner's over-count into a failure and Hyderabad's
under-count into a pass. A reference-sensitive pair is **neither a robust pass nor a robust
fail**, and R3 is not adopted because it helps: it remains a labelled approximation (linear
interpolation of an instantaneous field is not its midpoint value, and the
sunrise/sunset-straddling hour is unresolved in both treatments). **Reference sensitivity does
not erase the count evidence** — the 26 robust passes and the direction and size of the Bikaner
and Hyderabad errors stand regardless of which reference is used.

---

## 2. Humidity-consistency diagnostics (`humidity_consistency.csv`, `humidity_per_day.parquet`)

| quantity | result |
|---|---|
| days solved | 54,750 valid of 54,774 |
| invalid days | **24** (2 per site-window), all `non-finite or non-positive saturation vapour pressure` — the record-boundary hours where the reconstructed temperature is NaN. The same days are already NaN for A and B, so no candidate loses a day the others keep. |
| solver failures | **0** |
| max abs daily-RH residual | **9.81 × 10⁻⁷ pp** (tolerance 10⁻⁶) |
| mean abs daily-RH residual | 3.8 × 10⁻⁸ – 1.4 × 10⁻⁷ pp |
| **existing method, same days** | mean abs **1.17 – 2.29 pp**, p95 **2.9 – 4.9 pp**, max **10.95 pp**; > 1 pp on 50 – 79 % of days, > 5 pp on 0.3 – 4.6 % |
| existing method, signed mean | −2.16 pp (Kochi 2005–2014, too dry) to +0.70 pp (Bikaner 1990–2004, too moist) |
| max iterations | **27** (limit 200) |
| analytic branch | 1,500 – 5,351 days per site-window |
| bisection branch | 92 – 3,436 days per site-window |
| `e* − e_old` mean | −0.194 hPa (Bikaner 1990–2004) to **+0.957 hPa** (Kochi 2005–2014) |
| `e* − e_old` range | −2.13 to +5.15 hPa |
| days with `e* > e_old` | 23 % (Bikaner) to **91 %** (Kochi 2005–2014) |
| saturated-hour fraction, new | 0.20 % (Bikaner) to **15.7 %** (Kochi 1990–2004) |
| clipped-hour fraction, old | 0.20 % to 7.3 % |

Both solve branches are exercised at every site, so neither is untested in production use. An
invalid day yields NaN at all 24 hours and therefore a NaN daily maximum: **missing data never
becomes a zero exceedance**, asserted end to end in `tests/test_wbgt_outdoor_humidity.py`.

**Terminology.** `e*` is the fitted daily humidity parameter, not an observed vapour pressure.
Where RH clips at 100 % the **effective** hourly vapour pressure is `min(e*, es(T_h))`, so the
effective vapour pressure is **not** constant across a saturated day — a test pins this. And
matching the daily RH is an **input-consistency constraint**: the hourly-humidity diagnostics
above show directly that it does not recover the actual hourly humidity at the hours that matter.

---

## 3. Old-candidate reproduction (`old_candidate_reproduction.json`)

Run **before** the full comparison, on Kochi 1990–2004:

| candidate | milestone 2 column | common days | max abs difference | days differing |
|---|---|---|---|---|
| A | `cand_C1` | 5,476 | **0.000 °C** | 0 |
| B | `cand_W1` | 5,476 | **0.000 °C** | 0 |

**`REPRODUCED EXACTLY`.** A and B are milestone 2's C1 and W1, not approximations of them, so
every A→C and B→D difference in this report is attributable to the humidity formulation alone.
`tests/test_wbgt_outdoor_humidity.py` additionally asserts that no gate constant and not even
W1's slope is re-declared in this module — they are imported.

---

## 4. Four-candidate summary under both references (`robustness_classification.csv`)

| candidate | daily gate | worst med. abs err | worst RMSE | robust pass | robust fail | ref-sensitive | not evaluated | worst \|count err\| | years outside tol. | input consistency |
|---|---|---|---|---|---|---|---|---|---|---|
| **A** old + C1 | PASS 12/12 | 0.903 | 1.160 | 24 | **2** | 4 | 6 | 38.25 | 29 | n/a by design |
| **B** old + W1 ← **selected** | PASS 12/12 | 0.728 | 1.240 | 26 | **0** | 4 | 6 | 15.63 | 19 | n/a by design |
| **C** new + C1 | PASS 12/12 | 0.867 | 1.190 | 22 | **4** | 4 | 6 | 51.00 | 48 | **PASS** |
| **D** new + W1 | PASS 12/12 | 0.763 | 1.285 | 27 | **0** | 3 | 6 | 29.25 | 22 | **PASS** |

30 classified count pairs each; 6 `NOT EVALUATED` (Shimla, rare event). Robustness is computed on
**common valid dates and common complete years** across the two references, and no year was
excluded by one reference and not the other at any site — `years_excluded_audited_only` and
`years_excluded_R3_only` are empty throughout, so the cross-reference comparison is like for
like. Per-reference absolute errors, tolerances and signed gate margins sit beside every class in
the CSV.

**W1 remains the better wind treatment.** Correcting the humidity did not remove its advantage:
B beats A on robust failures (0 vs 2), worst count error (15.6 vs 38.3 d/yr) and years outside
tolerance (19 vs 29), and D beats C on the same three. The rule did not retain W1 automatically —
it was re-earned.

---

## 5. Limited NEX transfer check (`nex_transfer.csv`)

Reused milestone 2's cached `ACCESS-CM2` and `MRI-ESM2-0` `historical` 1990–1999 six-site sample;
nothing downloaded, no new reader, no new inventory, no QDM, no national scan, and **no same-date
NEX-versus-ERA5 RMSE or correlation** — NEX carries no forecast for a particular ERA5 date. Eight
common complete years per site. The NEX day boundary is **inferred** from the CMIP6 daily
convention; no file publishes `time_bnds`.

**Answer: the humidity formulation neither helps nor harms the NEX discrepancies materially,
because those discrepancies are an order of magnitude larger.**

| site | ERA5 ref ≥ 32 °C d/yr | NEX B error, ACCESS-CM2 / MRI-ESM2-0 | change from B to D |
|---|---|---|---|
| Kochi | 189.0 | −109.8 / −94.1 | −5.9 / −5.6 (worse, already under-counting) |
| Kolkata | 124.2 | +60.5 / +62.5 | −2.1 / −2.3 (better) |
| Bikaner | 70.8 | +30.5 / +38.5 | −3.5 / −5.5 (better) |
| Lucknow | 108.0 | +20.4 / +28.1 | −2.3 / −2.8 (better) |
| Hyderabad | 39.3 | −21.7 / −10.7 | −0.3 / −2.4 (worse) |
| Shimla | 0.0 | 0.0 / 0.0 | 0.0 (no exceedances either side) |

**The humidity change moves NEX in the opposite direction from ERA5.** On NEX inputs it *lowers*
the annual mean at every site (−0.006 to −0.156 °C) and *lowers* ≥ 32 °C counts almost
everywhere, whereas on ERA5 reconstruction it *raised* the moist-site tail. The cause is not
established in this milestone and is not guessed at: NEX's own `hurs`–`tasmin`/`tasmax`
relationship differs from ERA5's, and grid-point sampling, elevation, the reconstruction, the
reference treatment and the inferred day convention all remain in the interpretation.

**The consequence for the decision is a caution, not a reversal:** a humidity formulation
validated on ERA5 reconstruction cannot be assumed to behave the same way on NEX, so the ERA5
finding above does not transfer, and neither does any favourable NEX reading. This check does
**not** validate national applicability.

---

## 6. Qualification of prior claims

Nothing below rewrites a committed result; both predecessor directories are untouched.

- **W1 was the best *development* candidate** under milestone 2's rule and remains so under this
  milestone's. It is **not** an established physically correct wind reconstruction. Its slope
  (0.03 /°C) and bounds (0.10, 0.80) are a **declared assumption**: MTCLIM, `metsim` and the
  CarbonPlan chain built on `metsim` all hold wind **constant** through the day.
- **Reference sensitivity applies to particular outcomes.** All four of B's remaining ≥ 32 °C
  failures are reference-sensitive, but the 26 robust passes, the direction of the Bikaner
  over-count and the Hyderabad under-count hold under both treatments. Reference sensitivity does
  not erase the count evidence.
- **The prior QDM trial is not accepted.** In its largest example (ACCESS-CM2, Bikaner, `ssp585`
  2071–2080) it changed the q99 future-minus-historical signal from **4.00 °C to 4.83 °C**, about
  **+21 %**, and it worsened several lower-threshold held-out results (e.g. ACCESS-CM2 Bikaner
  ≥ 30: raw error +12.5 → corrected −4.8 d/yr but ≥ 28 +0.3 → −2.4; MRI-ESM2-0 Lucknow ≥ 32
  +8.2 → −16.8). That configuration is **not** generally signal-preserving and **not**
  production-ready. No QDM was run in this milestone.
- **The CarbonPlan-style raw comparator** of milestone 2 is a *raw* outdoor calculation scored
  against the same reference. It is **not** the full CarbonPlan corrected product, and no
  national distribution similarity was treated as parity.
- **"Bias correction unnecessary for level" is not established.** There is still no
  product-level accuracy requirement for outdoor WBGT against which to judge it, and §5 shows
  NEX level errors of −0.89 to +1.41 °C in the annual mean under B, and up to 110 d/yr at ≥ 32 °C.
- **Shade retains its separately evaluated release contract**
  (`shade-peak-v1:tas,tasmax,hurs:magnus-17.62-243.12:rh-percent-no-floor:complete-365:noleap-or-gregorian-drop-feb29:cell-first-area-weighted:structural-idw`).
  This diagnostic did not modify, reopen or invalidate it (§8).

---

## 7. What is validated, provisional and diagnostic-only

| output | status |
|---|---|
| The input-consistency solve itself (residual, validity, endpoints, branches) | **validated** — exact, tested, 0 failures |
| A and B as milestone 2's C1 and W1 | **validated** — reproduced exactly |
| B's daily WBGT accuracy at six points, 1990–2014, against `ref_audited` | **provisional** — passes the daily gate, but both windows are follow-up evidence, not independent confirmation |
| B's annual ≥ 28 / ≥ 30 °C counts at five non-Shimla sites | **provisional** — robust passes under both references |
| B's annual ≥ 32 °C counts at Bikaner and Hyderabad | **unresolved** — reference-sensitive, not passes |
| Any ≥ 30 / ≥ 32 °C statistic at Shimla | **`NOT EVALUATED`** — rare event, never a pass |
| The input-consistent humidity formulation (C, D) | **rejected for deployment**, retained as diagnostic evidence |
| Everything on NEX inputs | **diagnostic-only** — distributions and counts, two models, six sites, inferred day boundary |
| Spatial ranking of districts or blocks | **not assessed at all** — six-site temporal agreement is not evidence of spatial ranking accuracy |

**The six sites' observed annual-mean range (19.5 – 31.9 °C) is not a universal physical validity
bound** for Indian grid cells or administrative units, and must not be used as one.

---

## 8. Isolation — the national shade rebuild was untouched

- No production computation, metric registration or method signature was modified. The only
  non-diagnostic files changed in this milestone are `tools/README.md` and `MANIFEST.md`.
- Nothing was written under `irt_data`, `processed`, `processed_optimised`,
  `scratch/wbgt_shade_national`, any shade stage or release configuration, or either
  predecessor's evidence directory. The tool's write guard refuses all of them and
  `tests/test_wbgt_outdoor_humidity.py` asserts the refusals.
- No shade process, cache or staged output was read for writing or interfered with. Ran at
  `--workers 1` throughout while the national shade rebuild owned the disk.
- The outdoor reference-timing problem does **not** affect the Bernard/Stull shade reference.
  The humidity approximation is shared *conceptually* with shade, but shade has its own
  validation evidence and its own accepted contract, and this milestone changes neither.

---

## 9. The condition, and the unexecuted pilot plan

**`CONDITIONAL` on one thing:** B's four remaining ≥ 32 °C failures are all
`REFERENCE-SENSITIVE`, i.e. the decision at the highest threshold currently depends on which of
two labelled reference approximations is used. **The condition is that the ≥ 32 °C outputs are
carried through the pilot as explicitly diagnostic and are not published, ranked or composited,
while ≥ 28 and ≥ 30 °C and the daily mean are treated as provisional.** No new reference
development project is opened, and no gate is relaxed.

The following plan is **written, not executed.** Running it needs separate approval.

**Scope**
- States: **Kerala, Rajasthan, Himachal Pradesh** — one humid-tropical, one arid, one montane, so
  the three regimes that behaved differently above are all represented.
- Levels: **district and block**, both, from the existing boundary layer; the pilot reports the
  unit counts it actually resolves rather than assuming them.
- Models / years / scenario, from verified local availability
  (`docs/diagnostics/wbgt_outdoor_feasibility/nex_required_intersection.csv`): the six required
  variables `tas, tasmin, tasmax, hurs, rsds, sfcWind` are complete for **21 models**, but only
  for **historical 1990–2010** and **ssp245/ssp585 2020–2080**. Pilot scope: **`ACCESS-CM2` and
  `MRI-ESM2-0`**, **`historical` 2005** and **`ssp585` 2071**, four model-years total.
- Method signature: `outdoor-liljegren-v1:B` — the candidate signature this tool already emits,
  carrying the humidity formulation, W1's coefficients, the Magnus constants, the `thermofeel`
  version and the calendar/completeness rules.

**Isolation**
- A **new, isolated outdoor stage**, mirroring `wbgt_shade_pilot.py`'s use of
  `compute_heat_stress_rows_for_metric`'s grid-first, per-state-bbox, area-weighted path — but as
  a separate code path writing to `scratch/`, registering **no** metric slug and touching **no**
  shade slug, contract or artifact.
- Required inputs the production heat-stress path does **not** currently load: **`rsds` and
  `sfcWind`**, from `irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1/<scenario>/`. Confirming that these
  load and align on the same subset grid as `tas`/`tasmax`/`hurs` is the pilot's first checkpoint.

**Checks the pilot must perform**
1. **Missing-data and completeness:** a cell-day with any non-finite driver yields NaN, never a
   zero exceedance; per-cell valid-day counts and the complete-365 rule reported per state.
2. **Runtime measured before any scaling.** The outdoor path evaluates 24 reconstructed hours per
   cell-day through the Liljegren solver, where shade evaluates one daily peak. Measure one
   state-model-year first and report cells, cell-days, wall time and peak memory; **do not**
   extrapolate a national budget from a single state.
3. **District and block ranking stability:** rank correlation and rank-shift distribution between
   the two models, and between district- and block-level aggregation, per state. Six-site
   temporal correlation is **not** evidence of spatial ranking accuracy, which is why this is the
   pilot's primary new question.
4. **Sub-cell and small-unit behaviour:** which blocks fall below one grid cell, and what the
   existing IDW fill does to them.
5. **Output labelling:** ≥ 32 °C diagnostic-only (§9 condition); ≥ 28 / ≥ 30 °C and annual mean
   provisional; nothing entering a composite, a ruler or a published map.

**Explicitly out of the pilot:** national rebuild, bias correction, metric registration,
composite weights, any write under `processed`/`processed_optimised`, and anything touching shade.

---

## 10. Documentation

`tools/README.md` and `MANIFEST.md` carry the new tool (CHG-0617). **Root `README.md` needs no
update:** this milestone registers no metric slug, changes no deployed methodology, adds no
operator command anyone must run, and leaves the shade sections it documents exactly as they were.

---

## 11. Files

| file | contents |
|---|---|
| `SPEC.md` | the frozen contract (CHG-0614) |
| `selection.json` | the predeclared rule applied, ranking, deterioration screen |
| `old_candidate_reproduction.json` | A/B vs milestone 2, exact-equality check |
| `candidate_scores.csv` | daily scores, 4 candidates × 2 references × 5 seasons × 12 site-windows |
| `annual_counts.csv` | annual means, counts, count errors and the count gate, per reference |
| `per_year_count_errors.csv` / `per_year_count_summary.csv` | every complete year's signed and absolute count error |
| `robustness_classification.csv` | both-reference classification with per-reference errors, tolerances and margins |
| `humidity_consistency.csv` | residuals, validity, solver paths, `e*` shift, saturation |
| `humidity_residual_comparison.csv` | both methods' daily-RH residual per site-window (`--stage residuals`) |
| `humidity_per_day.parquet` | per-day `e*`, `e_old`, residual, iterations, reason, solve path |
| `humidity_hourly_diagnostics.csv` | RH and effective-vapour-pressure bias vs observations, by season and at both peak hours |
| `uncertainty_paired.csv` | paired year-block intervals on new − old, daily statistics and count errors |
| `nex_transfer.csv` | four candidates on the cached NEX sample, per model, plus change vs old humidity |
| `nex_sample_validity.csv` | the cached sample's per-site validity |
| `wind_diagnostics.csv` | clipping, daily-mean conservation, solver-floor exposure |
| `daily_series.parquet` | every daily maximum series, all identities, all site-windows |
| `run_manifest.json` | `--stage all`: command, git snapshot, package versions, signatures, gates, input identities |
| `run_manifest_residuals.json` | `--stage residuals`: the same provenance for the residual measurement |
