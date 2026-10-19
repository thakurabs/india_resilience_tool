# Outdoor-WBGT humidity consistency — frozen follow-up specification

**Status:** frozen 2026-09-29, **before any candidate score was computed or inspected**.
**Snapshot:** `GIT:add_flood_depth@99e478a`.
**Tool:** `tools/diagnostics/wbgt_outdoor_humidity.py` (CHG-0615).
**Predecessors, both immutable here:**

- `docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md` — milestone 1 (CHG-0602).
- `docs/diagnostics/wbgt_outdoor_selection/SPEC.md` — milestone 2 (CHG-0608).

Neither predecessor directory is edited, overwritten or re-run by this milestone. Their
contracts, evidence and reports stand exactly as committed at `99e478a`.

This document exists so that every candidate identity, algorithm, tolerance, reference, gate,
classification and stop condition is on record *before* the numbers are known. Anything not
written here was not predeclared, and the report must say so.

---

## 1. The one question this milestone answers

> Does preserving the supplied daily-mean relative humidity — rather than the daily vapour
> pressure derived from it — improve outdoor WBGT reconstruction enough to select a method for a
> bounded staged validation pilot?

One humidity formulation, run under two already-defined wind treatments. **A candidate
experiment, not an assumed fix.** The deliverable is a decision, not a further search.

---

## 2. Candidates — exactly four, frozen

| id | humidity | wind | provenance |
|---|---|---|---|
| **A** | existing (constant daily vapour pressure) | C1 — constant at the daily mean | milestone 2 `cand_C1`, **unchanged** |
| **B** | existing (constant daily vapour pressure) | W1 — DTR-dependent mean-preserving shape | milestone 2 `cand_W1`, **unchanged** |
| **C** | **input-consistent daily RH** (§4) | C1 | new |
| **D** | **input-consistent daily RH** (§4) | W1 | new |

A and B are **imported**, not re-declared: their temperature, radiation, pressure, humidity and
wind code paths are milestone 1's and milestone 2's functions called unmodified. §5 requires a
numerical reproduction check against milestone 2's committed series before the full run.

No other candidate may be added, whatever the scores show (§12). In particular: no new wind
shape, no afternoon drying curve, no fitted or site-specific coefficient, no new QDM
configuration, no acceptance-threshold change.

### 2.1 What is held fixed across all four

Identical, from the same function calls, for every candidate:

- temperature reconstruction (Parton & Logan 1981, `a = 1.86 h`, `b = 2.2`);
- radiation reconstruction (TOA interval-mean shape rescaled to conserve daily `rsds`,
  Erbs et al. 1982 diffuse split) and its direct/diffuse treatment;
- pressure (ISA from site elevation; NEX publishes no `ps`);
- the Liljegren solver and its internal 10 m wind handling (`thermofeel` 2.3.0);
- daily grouping (IST civil day), the complete-day rule (24 hours) and the calendar policy;
- the missing-data rule: any non-finite hour invalidates that local day's daily maximum.

Only the **humidity formulation** and the **already-defined C1/W1 wind choice** differ.
`rsds` and `sfcWind` continue to be consumed by the outdoor calculation.

### 2.2 Wind definitions — inherited verbatim

- **C1**: `wind(h) = w̄_day` at every hour. A daily mean does not imply a known hourly wind;
  this is labelled as an assumption, not a reconstruction.
- **W1**: `s(h) = cz_norm(h) − mean_over_local_day(cz_norm)`;
  `a_day = clip(0.03 · (tasmax − tasmin), 0.10, 0.80)`;
  `wind(h) = clip(w̄_day · (1 + a_day · s(h)), 0, None)`.
  Imported from `wbgt_outdoor_selection.wind_w1_dtr_ms`; the slope and bounds are **not**
  re-derived here and **not** re-tuned. W1 remains a **declared assumption, not a published
  wind reconstruction** (§11).

---

## 3. Sites, periods and completeness — reused, and labelled as follow-up

Six established sites: **Kochi, Kolkata, Bikaner, Lucknow, Hyderabad, Shimla**.
Two established windows: **1990–2004** and **2005–2014**, from the existing hourly ERA5 point
cache `scratch/wbgt_deployed_vs_reference_cache`. **Nothing is downloaded.**

**Both windows have already informed development** across milestones 1 and 2. Results here are
**follow-up evaluation evidence**, not untouched independent confirmation. An untouched
independent period would need hourly forcing outside 1990–2014, which is not on disk. The
report must not describe any pass on these windows as independent confirmation.

Completeness policy, inherited unchanged:

- a local day is used only if all 24 hours are present;
- a daily maximum is NaN if any hour of that day is non-finite;
- a **complete year** carries 365 valid daily maxima after 29 February is dropped;
- annual statistics are computed over years complete in **both** the reference and the
  candidate (`matched_complete_years`), and the per-series year counts are reported so any
  shrinkage is visible. Missing data never becomes a zero-exceedance day.

---

## 4. The input-consistent humidity method

### 4.1 Existing method (A and B), for contrast

```
e_old       = (daily_hurs / 100) · es(daily_tas)
RH_hour     = clip(100 · e_old / es(T_hour), 0, 100)
```

`es` is IRT's shipped Magnus function, `saturation_pressure_hpa`
(`6.112 · exp(17.62 T / (243.12 + T))`, hPa, T in °C). This generally does **not** preserve
`daily_hurs` when the reconstructed hourly RH is averaged over the reconstructed hourly
temperatures, because `mean_h[1/es(T_h)] ≠ 1/es(mean_h T_h)`.

### 4.2 New method (C and D)

For each valid complete day, choose one non-negative daily vapour pressure `e*` such that

```
mean_h[ clip(100 · e* / es(T_h), 0, 100) ] = daily_hurs
```

where `T_h` is **that candidate's existing reconstructed hourly temperature** (unchanged — the
temperature reconstruction is never altered to make this work), `es` is the same Magnus
function and the same units, `daily_hurs` is in percent, and the mean runs over the **same
hourly sampling convention** as the daily-input reconstruction (the 24 hours of the IST civil
day, the same set whose mean defines `daily_hurs` from the cache).

The constraint function `f(e) = mean_h[clip(100 e / es(T_h), 0, 100)] − daily_hurs` is
non-decreasing in `e`, continuous and piecewise linear, so the root is found without any fit.

**Solution path:**

1. **Analytic, where no hour saturates:** `e_a = (daily_hurs / 100) / mean_h[1 / es(T_h)]`.
   Accepted iff `e_a ≤ min_h es(T_h)`, in which case no clip binds and `f(e_a) = 0` exactly.
2. **Otherwise a monotone bounded root solve including the clipping**, by bisection on
   `[0, max_h es(T_h)]`. That bracket is valid for finite positive `es`: `f(0) = −daily_hurs ≤ 0`
   and `f(max_h es) = 100 − daily_hurs ≥ 0`.

**Endpoints, handled explicitly:**

- `daily_hurs == 0` → `e* = 0`.
- `daily_hurs == 100` → `e* = max_h es(T_h)`, the minimum value saturating every sampled hour.
- `daily_hurs` outside `[0, 100]` → **invalid input**, recorded with a reason. Never coerced.

**Numerical tolerances, declared before scoring:**

| quantity | value |
|---|---|
| accepted absolute daily-RH residual | `≤ 1e-6` percentage points |
| bisection iteration limit | `200` |
| `es` positivity requirement | `es(T_h) > 0` and finite at every sampled hour |

Bisection on a bracket of width `≤ ~100 hPa` reaches `|f| ≤ 1e-6` pp in well under 60
iterations at the sensitivities involved, so the limit is a guard, not a budget.

**Validity rules:**

- a missing required hour invalidates the whole day — the solve is **never** performed over a
  shortened set of valid hours;
- non-finite or invalid temperatures, or non-finite/non-positive `es`, invalidate the day;
- non-convergence produces an **explicit invalid result with a diagnostic reason**, not a
  fallback value;
- an invalid day yields NaN at every one of its hours, so its daily maximum is NaN.
  **Missing data must never become zero exceedances.**

**Terminology the report must respect:**

- `e*` is the **fitted daily humidity parameter**, not an observed vapour pressure.
- Where RH is capped at 100 %, the **effective hourly vapour pressure** is
  `min(e*, es(T_h))`. The report must therefore **not** claim the effective vapour pressure
  stays constant at saturated hours.
- Matching the daily RH is an **input-consistency constraint**. It is **not** evidence that the
  actual hourly humidity has been recovered.

**No leakage.** The candidate reads only the daily NEX-equivalent variables
(`tas, tasmin, tasmax, hurs, rsds, sfcWind`) plus static site data and solar geometry. Actual
hourly humidity, dew point, hourly wind and hourly WBGT are **never** inputs to a candidate;
they are used only for scoring and diagnosis. The guard is structural: candidates are built
from `wbgt_outdoor_feasibility.DailyInputs`, which refuses any extra column.

**Isolation.** The new formulation is **diagnostic-only** and lives in this milestone's module
as an explicit method option. No existing function's behaviour is changed, and no production
computation, metric registration or method signature is touched.

---

## 5. Cache, signature and reproduction requirements

Each candidate carries an immutable signature distinguishing

- humidity formulation (`vapour_pressure` vs `input_consistent_daily_rh`, with its tolerance and
  iteration limit),
- wind treatment (`C1` vs `W1`, with W1's coefficients),
- the solver/`thermofeel` version, and
- the calendar, day-convention and completeness rules.

A cached per-site parquet is reused **only** if its recorded signature bundle and input identity
match the current run in **every** relevant parameter. Matching the reference signature alone is
never sufficient. This milestone writes its own work directory and never reuses milestone 2's
per-site caches as candidate results.

**Old-candidate reproduction, before the full run.** A and B are recomputed here and compared,
day for day, against milestone 2's committed `daily_series.parquet` columns `cand_C1` and
`cand_W1` over the overlapping site-windows. The expectation is **exact equality** (identical
code path, identical inputs). Any non-zero difference is **investigated and reported as a
defect**, never described as an improvement. The check is written to
`old_candidate_reproduction.json`.

---

## 6. References — both, unchanged

Both existing model-based reference identities are used, read from their implemented
definitions and **preserved exactly**:

| id | signature |
|---|---|
| `ref_audited` | `liljegren-audited-v1:cossza-at-radiation-interval-midpoint` |
| `ref_R3_midpoint` | `liljegren-midpoint-v1:all-drivers-interpolated-to-radiation-interval-midpoint` |

`ref_legacy` is **not** used: milestone 1 established that it lands its daily maximum in a
spurious low-sun hour on 2.7–12.2 % of days, so its tail statistics are contaminated.

**R3 is extended to all six sites** in this milestone, computed from the existing cached hourly
drivers. Milestone 2 evaluated R3 at Bikaner and Hyderabad only; six-site robustness is **not**
claimed from that two-site coverage, and the new computation is done here rather than inferred.

R3 remains **an approximation, labelled as one**: linear interpolation of an instantaneous field
is not its true midpoint value, and no treatment resolves the sunrise/sunset-straddling hour, in
which an hour-mean flux is paired with a geometry that changes sign within the hour. R3 is
internally consistent, not correct.

**A candidate series is computed once and scored against each reference.** Candidate timing never
depends on which reference is used.

---

## 7. Gates — imported, not re-declared, not widened

Both gates are milestone 1's, imported from `wbgt_outdoor_feasibility` so they cannot drift:

- **Daily gate** (per site-window pair, all seasons pooled, candidate minus reference, matched
  days only): `median_abs_error_c < 1.0` **and** `rmse_c < 1.5`.
- **Count gate** (per site-window-threshold, complete years only, gated only where the reference
  carries ≥ 5.0 days/year): pass requires
  `|cand_per_year − ref_per_year| ≤ max(0.20 · ref_per_year, 2.0 days/year)`.
  Below the floor the pair is `NOT GATED (rare event)` — **neither pass nor fail**.

The two gates stay separate. A daily-gate pass is not a count-gate pass. Thresholds are
**28, 30 and 32 °C**.

---

## 8. Robustness classification across the two references

For every eligible site / window / statistic and candidate:

| class | meaning |
|---|---|
| `ROBUST PASS` | passes the gate against **both** references |
| `ROBUST FAIL` | fails against **both** |
| `REFERENCE-SENSITIVE` | passes against one and fails against the other |
| `NOT EVALUATED` | insufficient, rare-event or missing evidence under the existing policy |

A `REFERENCE-SENSITIVE` result is **neither** a robust pass **nor** a robust fail, and is never
converted into one. `NOT EVALUATED` is reported as `NOT EVALUATED`.

Comparisons between the two reference treatments use **common valid dates** and **common
complete years**, and every exclusion is listed explicitly. Classifications never stand alone:
the original absolute errors and gate margins under each reference remain visible in the tables.

---

## 9. Diagnostics this milestone must report

### 9.1 Humidity consistency, per candidate / site / window

- mean and maximum absolute daily-RH reconstruction residual;
- invalid days and root-solver failures, with reasons;
- saturated-hour fraction (`e* ≥ es(T_h)`, and the old method's clip fraction);
- change in `e*` relative to `e_old` (mean, and signed distribution);
- **diagnostic only:** bias of the reconstructed hourly RH and of the effective vapour pressure
  against the cached hourly observations;
- humidity error near the **actual temperature peak** hour;
- humidity error near the **reference WBGT peak** hour — a distinct hour, which need not
  coincide with the temperature maximum, and is reported separately;
- daily and seasonal (DJF / MAM / JJAS / ON) summaries.

Hourly reference data is used **only** for scoring and diagnosis, never for constructing a
deployable candidate. **No universal afternoon drying mechanism may be inferred from a site
ordering alone.**

### 9.2 WBGT and counts, per candidate / reference / site / window

Valid days and complete years; mean bias; median absolute daily error; RMSE; annual mean of
daily maxima; seasonal errors; quantile differences (q50/q90/q95/q99); conditional mean error on
the reference's hottest 1 % of days; annual counts ≥ 28, ≥ 30, ≥ 32 °C; signed and absolute count
error for **each** complete year; window-mean annual count errors; and the unchanged gate
outcomes.

**Two statistics are kept separate and named differently:** the **error in the 99th percentile**
(`q99_difference_c`, each series' own quantile) and the **mean error on the reference's hottest
1 % of days** (`cond_mean_error_hottest_1pct_c`).

### 9.3 Uncertainty

Where uncertainty is calculated for an old-versus-new comparison, it is a **paired year-block
resample** — whole years drawn with replacement, both candidates evaluated on the same drawn
years, and the statistic reported is the **difference** (new − old). Daily samples are never
treated as independent. **Reference-method uncertainty (§8) is reported separately from temporal
sampling uncertainty and the two are never combined into one interval.**

### 9.4 Prohibited summarisations

- No pooling of sites that would conceal a regime failure.
- Rare-event `NOT EVALUATED` pairs are never counted as passes.
- No per-site or per-threshold method selection.
- No claim of overall improvement resting solely on a pass count changing near a boundary.

---

## 10. Limited NEX transfer check

Run **after** the four-way ERA5 comparison, and **only** if it needs no new reader, download or
inventory work:

- the existing sampled **ACCESS-CM2** and **MRI-ESM2-0** series, experiment `historical`,
  **1990–1999**, six sites, from milestone 2's cached sample;
- the same historical period on the ERA5 side, over **explicitly common complete years** for
  each comparison;
- reported: raw distribution and annual-count changes for C and D **relative to** the old A and
  B versions, per model before any ensemble summary.

**Not done:** new QDM fitting or correction, new model inventory, national scan, and any
same-date NEX-versus-ERA5 weather RMSE or correlation — NEX carries no forecast for a particular
ERA5 date.

This check answers whether the humidity change **helps or harms** the already-observed NEX
discrepancies. It does **not** validate national applicability. If the cached sample cannot be
reused without substantial additional work, the exact limitation is stated and the primary
experiment finishes without it.

---

## 11. Candidate-selection rule — predeclared

Applied in this order to the four candidates:

1. **Daily gate** against `ref_audited` must pass on every evaluated site-window pair. A
   candidate failing any pair is eliminated.
2. **Input-consistency constraint** (new-humidity candidates only): maximum absolute daily-RH
   residual `≤ 1e-6` pp with zero solver failures on every evaluated site-window. A
   new-humidity candidate not meeting this is not selectable *as* input-consistent.
3. **Count performance**, ranked on: (a) fewest `ROBUST FAIL` pairs; then (b) fewest
   `ROBUST FAIL` + `REFERENCE-SENSITIVE` pairs; then (c) smallest worst-case absolute count
   error per year over gated pairs; then (d) fewest individual complete years outside the gate
   tolerance.
4. **No-material-deterioration screen.** The new humidity is preferred only if **no** site-window-
   threshold pair that is `ROBUST PASS` under the old humidity becomes `ROBUST FAIL` under the
   new one. Any such pair is named in the report and blocks the preference.
5. **Understandability.** The new humidity's advantage must hold under **both** references. An
   advantage visible under only one reference is recorded as reference-sensitive, not as an
   improvement.
6. **Availability.** A candidate depending on an input not available nationally loses to one
   that does not. (All four candidates here read only daily NEX-equivalent variables, so this
   step is expected to be inert; it is declared so that it cannot be invented later.)
7. **Tie-break.** Among scientifically comparable candidates, the simpler and already-implemented
   formulation wins — i.e. the old humidity, unless the new one is preferred under 1–5.

**W1 is not automatically retained.** If W1's advantage over C1 disappears once humidity
consistency is corrected, the rule follows the numbers.

### 11.1 Verdict vocabulary

| verdict | meaning |
|---|---|
| `READY FOR BOUNDED VALIDATION PILOT` | one candidate is sufficiently supported to test operational behaviour and spatial aggregation, with unresolved outputs explicitly diagnostic |
| `CONDITIONAL` | a named candidate, plus the exact remaining condition — never a vague "more research needed" |
| `NOT READY` | a named precise failure; stop, and add no further candidate |

**Pilot readiness is not readiness for national publication.**

---

## 12. Stop conditions

This milestone contains exactly: one new humidity formulation, run under the two existing wind
treatments; scored against the two existing references at six sites over two windows; one
limited NEX transfer check; and the decision.

**No variant may be added after any score is inspected.** If nothing meets the criteria, the
deliverable is the named failure and the smallest unresolved requirement. The gates are not
relaxed. A failed round does not prove that daily inputs can never support threshold counts.

---

## 13. Isolation — the national shade rebuild is protected

Outputs: `docs/diagnostics/wbgt_outdoor_humidity/` (committed evidence) and
`scratch/wbgt_outdoor_humidity/` (bulky intermediates, git-ignored). The tool refuses to run if
either target resolves inside `irt_data`, `processed`, `processed_optimised`, any shade stage, or
either predecessor's docs directory.

**The national shade rebuild is left entirely untouched:** its processes, inputs, caches
(`scratch/wbgt_shade_national`), staged outputs, release configuration, production shade
calculations, their method signature and the shared production helpers they use. The outdoor
reference-timing problem does **not** affect the Bernard shade reference, and shade retains its
separately evaluated release contract (§14). Default `--workers 1` while it runs.

Out of scope: modifying production computation or metric registration; national outdoor
rebuilds; launching the pilot; writing under `processed`/`processed_optimised`; installing
dependencies; downloads; changing composite weights; committing or pushing; and updating
`docs/HANDOFF.md` or `docs/BACKLOG.md`.

---

## 14. Prior claims the report must qualify

The report carries a short clarification section stating, without rewriting any prior result:

- **W1** was the best development candidate under milestone 2's selection rule — **not** an
  established physically correct wind reconstruction. Its slope and bounds are a declared
  assumption; the standard daily-to-subdaily treatments hold wind constant.
- **Reference sensitivity** applies to particular outcomes. It does not erase all count evidence.
- The **prior QDM trial** changed q99 warming from 4.00 to 4.83 °C in its largest example — about
  21 % — and worsened some lower-threshold results. That configuration is **not** accepted as
  generally signal-preserving or production-ready.
- The **CarbonPlan-style raw comparator** of milestone 2 is not the full CarbonPlan corrected
  product.
- **"Bias correction unnecessary for level" is not established** without a product-level accuracy
  requirement, which does not yet exist.
- **Shade** retains its separately evaluated release contract; this diagnostic does not modify,
  reopen or invalidate it.

Historical reports are preserved. The clarification is visible from this milestone's README.
