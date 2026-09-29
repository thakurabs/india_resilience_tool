# Outdoor-WBGT method selection — frozen follow-up specification

**Status:** frozen 2026-09-29, before any new candidate score was computed or inspected.
**Snapshot:** `GIT:add_flood_depth@afe6cb8`.
**Predecessor:** `docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md` (milestone 1, CHG-0602),
whose contract, evidence and report are **immutable**. Nothing in this directory rewrites them.
**Tool:** `tools/diagnostics/wbgt_outdoor_selection.py` (CHG-0609).

This document exists so that every candidate definition, coefficient, split, gate, selection
rule and stop condition is on record *before* the numbers are known. Anything that is not
written here was not predeclared, and the report must say so.

---

## 1. Decision this milestone must deliver

> Selected outdoor method (or best development candidate), remaining threshold-count error,
> and readiness for a three-state staged validation pilot.

One selection round. Not a further diagnostic programme.

---

## 2. Target quantity — unchanged from milestone 1

- Daily **maximum** open-sky WBGT, °C, taken **after** the hourly physical calculation.
- Reported as the annual mean of daily maxima, and annual counts ≥ 28, ≥ 30, ≥ 32 °C.
- Six established sites: Kochi, Kolkata, Bikaner, Lucknow, Hyderabad, Shimla.
- Two established windows: **1990–2004** and **2005–2014**, from the existing hourly ERA5
  cache `scratch/wbgt_deployed_vs_reference_cache`. Nothing is downloaded.

### 2.1 Both windows are now follow-up evidence, not independent confirmation

Milestone 1 scored candidates on both windows, and its results informed the design of the two
new wind candidates below. **Neither window is untouched independent data any more.** Results
on them are development/follow-up evidence. The report must not describe a pass on either
window as independent confirmation. An untouched independent period would require hourly
forcing outside 1990–2014, which is not on disk.

### 2.2 Carried-forward gates — verbatim, not re-tuned

Both gates are those of milestone 1 §10, carried forward **unchanged**. They are imported from
`wbgt_outdoor_feasibility`, not re-declared, so they cannot drift:

- **Daily gate** (per site-window pair, all seasons pooled):
  `median_abs_error_c < 1.0` **and** `rmse_c < 1.5`, candidate minus `ref_audited`, matched
  days only. `median_abs_error_c` is `(cand − ref).abs().median()`, the definition verified in
  milestone 1 against `wbgt_deployed_vs_reference.compare_series`.
- **Count gate** (per site-window-threshold, over complete years only):
  gated only where the reference carries ≥ 5.0 days/year. Pass requires
  `|cand_per_year − ref_per_year| ≤ max(0.20 · ref_per_year, 2.0 days/year)`.
  Below the floor the pair is `NOT GATED (rare event)` — neither pass nor fail.

The two gates stay **separate**. A daily-gate pass is not a count-gate pass.

### 2.3 Qualifications the report must carry

- The count gate assesses the **mean annual count over the evaluation window**. This milestone
  additionally reports the per-complete-year count error distribution (min, max, mean absolute,
  and the number of individual years outside the gate tolerance), so that a mean-level pass
  built on cancelling years is visible.
- Rare-event pairs (reference < 5 days/year) remain **unvalidated**. They are not passes.
- Passing a matched-weather reconstruction gate says nothing about **NEX distributional
  accuracy**. §6 is a separate question with a separate answer.
- `ref_legacy`'s tail statistics are contaminated by the low-sun beam-amplification defect found
  in milestone 1. `p99_bias_c`-style statistics from `ref_legacy` are never quoted here.

---

## 3. Candidates — frozen, with coefficients

`C1` and `C2` are retained **unchanged** from milestone 1 (imported, not redefined).

| id | name | wind treatment | deployable |
|---|---|---|---|
| C1 | recon-baseline | constant at the daily mean | yes |
| C2 | recon-wind-diurnal | mean-preserving cos-zenith shape, fixed amplitude 0.4 | yes |
| W1 | recon-wind-dtr | mean-preserving cos-zenith shape, **DTR-dependent** amplitude | yes |
| W2 | recon-wind-climatology | site-month hourly climatological profile | **only with a climatology asset** |

Every candidate reconstructs temperature (Parton & Logan 1981, a = 1.86 h, b = 2.2), humidity
(constant daily vapour pressure), radiation (TOA interval-mean shape rescaled to conserve daily
`rsds`, Erbs et al. 1982 diffuse split) and pressure (ISA from site elevation) exactly as C1
does. **Only the within-day wind shape differs.** All four consume `rsds` and `sfcWind`.

### 3.1 W1 — DTR-dependent diurnal wind shape

```
s(h)      = cz_norm(h) − mean_over_local_day(cz_norm)          # identical to C2's shape
a_day     = clip(WIND_DTR_SLOPE · (tasmax − tasmin), 0.10, 0.80)
wind(h)   = clip(w̄_day · (1 + a_day · s(h)), 0, None)
WIND_DTR_SLOPE     = 0.03 per °C
WIND_DTR_AMP_MIN   = 0.10
WIND_DTR_AMP_MAX   = 0.80
```

**Rationale.** The shape is C2's, unchanged. The only new idea is that the amplitude of the
daytime wind maximum should scale with the strength of surface heating, for which the diurnal
temperature range is the standard daily proxy: convective downward momentum transport is what
lifts 10 m wind above its nocturnal value, and it is driven by the same surface heating that
opens the DTR.

**Honest labelling.** No published formulation gives a DTR-to-wind-amplitude coefficient. The
standard treatments in the daily-to-subdaily disaggregation literature (MTCLIM, `metsim`, and
the CarbonPlan chain that builds on `metsim`) hold wind **constant** through the day — which is
C1, not a shape. **W1's slope and bounds are therefore a declared assumption, not a published
result, and the report must say so.**

**Not tuned.** `WIND_DTR_SLOPE = 0.03 /°C` is fixed by requiring W1 to reproduce C2's already
declared amplitude of 0.4 at a DTR of 13⅓ °C — a round hinge chosen from the two numbers
already on record, not from any site's observed DTR, and not from any WBGT error. The bounds
0.10 and 0.80 are round limits keeping the profile positive and physically moderate. No
coefficient here was, or may be, adjusted after any score is seen.

**Clipping and floors, accounted for explicitly.** `mean_over_local_day(s) = 0` exactly, so the
supplied daily mean is preserved exactly unless the lower clip at 0 m/s binds. The tool reports
the count of clipped hours and the maximum per-day daily-mean error introduced; a non-zero
count is a reportable finding, not a silent correction. Separately, `thermofeel` applies its own
`MIN_WIND_10M = 0.62 m/s` floor inside the solver, so at low wind the shape is partly absorbed
by that floor; the tool reports the fraction of hours at or below the floor per candidate.

### 3.2 W2 — climatological hourly wind shape

```
ratio(t)    = wind(t) / w̄_local_day(t)                    over calibration years only
p[m, h]     = mean of ratio(t) over calibration years, for calendar month m and UTC hour h
p[m, ·]    /= mean_over_h(p[m, ·])                        so each month's profile averages 1
wind(h)     = w̄_day · p[month(h), hour(h)], then rescaled per local day so that
              mean_over_local_day(wind) = w̄_day exactly
```

Non-negative by construction (every ratio is non-negative). The per-local-day rescaling is
required because a local IST day spans two UTC dates and can span a month boundary; without it
the daily mean would not be preserved exactly.

**Calibration/evaluation separation — declared, one chronological split:**

| run | calibration period | evaluation window | status |
|---|---|---|---|
| `W2` | **1990–2004** | **2005–2014** | **primary**: forward in time, operationally realizable |
| `W2rev` | 2005–2014 | 1990–2004 | diagnostic only: reverse-chronological, not realizable |

A test year never contributes to the profile applied to it; the two periods are strictly
disjoint. `W2rev` exists only so that W2 can be seen on the same six sites in the other window,
and it is **not eligible for selection**. Calibration and evaluation periods are reported
separately for every W2 row.

**Deployability, stated up front.** W2 needs a per-location hourly wind climatology. At six
sites that is the cached ERA5 point series. For a district/block pilot it would need a
**spatial** hourly wind climatology over India — an asset that does not exist in this repo or in
`irt_data`, and whose acquisition (ERA5 hourly `10m wind` over the India domain) is a
substantial download not authorized in this milestone. **Site-specific success for W2 is not
national deployability**, and the selection rule below penalizes it accordingly.

### 3.3 No other variants

No additional wind shape, amplitude, profile grouping or parameter search is admissible in this
milestone, whatever the scores show (§9).

---

## 4. Diagnostic oracles — never eligible for selection

| id | definition |
|---|---|
| A5 | C1's reconstructed drivers, **actual hourly wind** supplied |
| A6 | C2's reconstructed drivers, **actual hourly wind** supplied |

A5/A6 are the **complement** of milestone 1's A3, which reconstructed wind alone and left every
other driver actual. A3 answers "what does reconstructing wind cost against a perfect
background"; A5 answers "how much of the reconstruction's remaining error would a perfect wind
remove". Both are needed before wind may be called the sole cause. The report must not assert
wind as the sole cause unless A5 supports it.

Reported for A5/A6: the daily gate, the count gate including whether Bikaner's and Hyderabad's
≥ 32 °C failures resolve, the conditional error on the reference's hottest 1 % of days, and
whether any site-window-threshold pair that passed for C1/C2 **fails** under the oracle.

---

## 5. Alternative reference treatment — targeted sensitivity

Milestone 1 established two reference identities and showed the legacy one lands its daily
maximum in a spurious low-sun hour on 2.7–12.2 % of days. `ref_audited`
(`cossza` at the radiation interval midpoint) is the reference for every gate here, unchanged.

This milestone asks one bounded question: **does the remaining reference approximation change
the decision?**

**R3 — `liljegren-midpoint-v1:all-drivers-interpolated-to-radiation-interval-midpoint`.**
Instead of moving solar geometry back to the interval midpoint and leaving the instantaneous
drivers on the label, move **everything** onto one consistent half-hour grid:

- `temperature_2m`, `relative_humidity_2m`, `surface_pressure`, `wind_speed_10m`: linearly
  interpolated from their labelled instants to `H − 30 min`.
- `shortwave_radiation`, `direct_radiation`: unchanged — they are already the mean over
  `[H − 1h, H]`, whose representative instant is `H − 30 min`.
- `cossza` at `H − 30 min`, as in `ref_audited`.

**This is still an approximation, and is labelled as one.** Linear interpolation of an
instantaneous field is not the field's true value at the midpoint, and no treatment resolves the
sunrise/sunset-straddling hour exactly, in which an hour-mean flux is paired with a geometry
that changes sign within the hour. R3 is internally consistent, not correct.

**Scope:** Bikaner and Hyderabad only — the sites carrying the count-gate failures — on both
windows, over their complete years. Compared: daily maxima, the **hour of the daily maximum**
(peak timing), annual threshold counts, and gate outcomes for C1, C2, W1.

**Decision rule, predeclared.** If the gate verdict for every candidate is unchanged under R3,
the reference question is **closed for this milestone** and `ref_audited` stands. If any gate
verdict changes, that is recorded as a **reference uncertainty blocking a firm pass** — the
report says so and does not open a general reference-development project. **A reference
treatment is never chosen because it makes a candidate pass.**

---

## 6. CarbonPlan comparator — one verification, one scoring

Source of truth: `github.com/carbonplan/extreme-heat`, revision
**`f662b37200fe219db912ebd09ceb52fdac979861`** (2024-06-14), notebooks `07_solar_radiation_wind`
and `08_shade_sun_adjustment`, retrieved 2026-09-29 and read verbatim.

The comparator is the **CarbonPlan-style raw outdoor calculation**, scored against the **same**
`ref_audited`, the same dates, the same seasons and the same two gates as the physical
candidates. It is a comparator, not a candidate, and adoption is not forced if it performs
worse. A linear shade-to-sun adjustment is **not** equated with the complete CarbonPlan
workflow, national distribution similarity is **not** treated as parity, and no unmatched
geography, period or ensemble is used as paired validation.

An alignment table (their treatment vs IRT's, per driver) and an explicit deviation list go in
the report. Where local reproduction is impossible, the exact difference is stated and the
result is called **CarbonPlan-style**, never an exact reproduction.

---

## 7. Method selection rule — predeclared

Applied in this order, to deployable candidates only (C1, C2, W1, W2-primary):

1. **Daily gate must pass** on every evaluated site-window pair. A candidate failing any pair is
   eliminated.
2. **Count gate.** Among survivors, prefer candidates passing every gated site-window-threshold
   pair. If none does, rank by (a) fewest failing gated pairs, then (b) smallest worst-case
   absolute count error per year across gated pairs, then (c) smallest number of individual
   complete years outside the gate tolerance.
3. **Deployability.** A candidate requiring an asset that does not exist and is not authorized
   to be acquired loses to one that does not, at equal or near-equal accuracy.
4. **Simplicity.** Among scientifically comparable candidates, the simpler implementation wins.
5. **No per-site or per-threshold selection.** One method for all sites and all thresholds.

If no candidate passes every required gate: name the **best development candidate**, state the
exact outstanding failures, and do **not** call it production-ready. The NEX assessment of §6
continues regardless, since it clarifies readiness without further candidate tuning.

---

## 8. Matched-period NEX comparison

- Models: **ACCESS-CM2** and **MRI-ESM2-0**, the two already sampled in milestone 1.
- Experiment `historical`, years **1990–1999**, six sites.
- The **selected** candidate is run on NEX daily inputs; compared against the ERA5-driven
  `ref_audited` for **the same 1990–1999 years**, under the same complete-year policy.
- **Per-model results are reported before any ensemble summary.**

This is a **distribution and annual-count** comparison. Same-date NEX-versus-ERA5 RMSE and
correlation are **not** computed and are not skill measures: NEX carries no forecast for a
particular ERA5 date.

Reported: seasonal and annual distributions, annual mean of daily maxima, annual ≥ 28/30/32 °C
counts, absolute and relative count errors, and year-block uncertainty where the number of
complete years supports it. Documented: grid-point sampling and its elevation difference from
the site, calendar handling per model, the **inferred** NEX day boundary, and available versus
valid years.

### 8.1 Source-day sensitivity

The cached ERA5 is additionally aggregated into the **inferred NEX UTC-day** convention and the
reconstruction re-run, then compared with the IST-civil-day result. Timestamps are **not**
silently relabelled: no NEX file publishes `time_bnds`, so the UTC-day convention is an
inference from the CMIP6 daily convention, and this sensitivity measures how much the day
boundary is worth — it does **not** prove interval semantics.

### 8.2 Interpretation constraint

Remaining NEX-vs-ERA5 differences are **not** all attributable to marginal bias or
joint-variable dependence. Grid-point sampling, elevation, the reconstruction itself, the
reference treatment and the day convention all remain in the interpretation.

---

## 9. Conditional bias-correction trial — trigger frozen

**Trigger (predeclared, evaluated before any QDM code runs).** The trial runs **iff**, in §8's
matched-period comparison for the selected candidate, either:

- the relative error in `days_ge_32` per year exceeds ±25 % at **two or more** of the six sites
  for **either** model, or
- the absolute error in `days_ge_32` per year exceeds **15 days/year** at any site for either
  model.

If the trigger does not fire, the trial does not run and the report says the trigger did not
fire — not that bias correction was found unnecessary.

**Scope, if triggered:** the selected physical candidate (plus the CarbonPlan-style comparator
only if its raw performance made it a credible alternative); the two existing NEX models; the six
sites; **one** chronological training/held-out split over complete years; **one** additive
correction of the **derived** WBGT; and one small future sample, `ssp585` **2071–2080**, only if
every required local input exists for it.

**Settings, frozen before scoring**, reusing `tools/diagnostics/wbgt_qdm_bias_correction.py`
after checking its algorithm and its training/application separation: Quantile Delta Mapping,
`nquantiles = 100`, additive (`kind = "+"`), grouped by day-of-year with a 31-day window —
the settings verified against CarbonPlan notebook 06. **No parameter search.**
Train on **1990–1999**, apply to held-out **2000–2010** (and to the future sample unchanged).

Checked and reported: held-out seasonal distributions and annual count errors; raw versus
corrected; rare-event behaviour; NaN and extrapolation behaviour at and beyond the training
range; and preservation of the model's future-minus-historical quantile changes.

**Constraints.** Never fit on evaluation years. Daily paired NEX weather errors are not
validation. Two models and six sites are not national applicability. The older Tier-2 QDM
verdict is **not** borrowed for the new physical candidate.

---

## 10. Stop conditions

This milestone contains exactly:

- C1 and C2, unchanged.
- Two new wind candidates, W1 and W2 (W2 in one primary and one diagnostic calibration direction).
- Two diagnostic oracles, A5 and A6.
- One alternative reference treatment, R3, on two sites.
- One CarbonPlan-style comparator.
- One matched-period NEX assessment over two models, plus one source-day sensitivity.
- At most one QDM configuration, conditional on §9's trigger.

**No additional variant may be added after any score is inspected.** If nothing meets the
criteria, the deliverable is the best candidate, the precise remaining errors and the smallest
unresolved requirement. The gates are not relaxed. A failed candidate round does **not** prove
that daily inputs can never support threshold counts.

---

## 11. Isolation and out of scope

Outputs: `docs/diagnostics/wbgt_outdoor_selection/` (committed evidence) and
`scratch/wbgt_outdoor_selection/` (bulky intermediates, git-ignored). Milestone 1's directory is
read-only here. The tool refuses to run if either target resolves inside `irt_data`,
`processed_optimised`, or any shade stage.

**The national shade rebuild is running** (`scratch/wbgt_shade_national`, PID 60808). Its
execution, inputs, caches, staged outputs and release configuration are untouched. The outdoor
radiation-reference issue does not affect the Bernard/Stull shade reference, and no shade
methodology, code or artifact is reopened. Default `--workers 1` while it runs.

Out of scope: modifying production computations or metric registration; national outdoor
rebuilds; launching the three-state pilot; publishing outputs or writing under
`processed`/`processed_optimised`; installing dependencies; substantial downloads; changing
composite weights; committing or pushing; and updating `docs/HANDOFF.md` or `docs/BACKLOG.md`.

---

## 12. Verdict vocabulary

- `READY` — ready for a **bounded staged validation pilot**, not national publication.
- `CONDITIONAL` — ready only once named, specific conditions are met.
- `NOT READY` — a named blocker prevents even a staged pilot.

`NOT EVALUATED` is reported as `NOT EVALUATED`. It is never converted into a pass or a fail.
Readiness is not claimed if the chosen method requires a national climatology or correction
asset that has not been defined.
