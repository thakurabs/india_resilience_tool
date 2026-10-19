# WBGT reconciliation — divergence from CarbonPlan, the physics, and the order of work

Rewritten 2026-09-26 against `GIT:add_flood_depth@269be91`. Supersedes the 2026-09-24 / 2026-09-25
version of this file, which was organised as a history rather than as a plan; §6 preserves what
that version got wrong so the record is not lost.

**Shade-only implementation update (2026-09-28):** source code and diagnostics for
CHG-0572/0583–0586 have now been applied under the user-approved shade-only plan.
National artifacts have not been replaced. The current authority is
[the shade release runbook](../../wbgt_shade_release.md) and its measured evidence.
The broader outdoor/QDM programme below is historical context, not approved implementation scope.
The Tier-2 probe tests shade plus a linear adjustment; it does not establish the ceiling of
reconstructed-hourly Liljegren. Radiation conservation was not previously verified.
CHG-0576–0582 are superseded by CHG-0583–0587; the Twb false-zero sibling defect remains open.

This document is written to be read cold, after a context compaction, with no conversation
history. Start at §1, then §2, then §4. §3 says what to stop investigating.

---

## 1. The divergence, as measured against CarbonPlan

CarbonPlan's `extreme-heat` v1.0 is the right yardstick: it is built on **the same NEX-GDDP-CMIP6
archive** IRT uses, so a disagreement is a methodology disagreement, not a data disagreement.

Days ≥ 32 °C, historical, 2,257 Indian regions (CHG-0543 / CHG-0544):

| | CarbonPlan | IRT | verdict |
|---|---|---|---|
| **shade** | 4.27 mean, 0.00 median, 55.2 max | `wbgt_shade_stull` 0.669 / 0.302 / 4.357 | **−84.3 % on the mean**, KS D = 0.477 |
| **sun** | 78.21 / 81.75 / 178.8 | `swbgt_empirical` 80.92 / 85.94 / 209.75 | +3.5 % on the mean, but KS D = 0.101, p = 1.4e-05; quantile ratio fans q0.50 1.05 → q0.95 1.11 → q0.99 1.18 |

Two different failures wearing one label:

- **Shade is 6× too low.** Cause known and quantified — see §2 P1.
- **Sun agrees on the mean by accident and diverges in the tail.** A mean agreement is not a
  pass. The tail is where the ≥30 / ≥32 counts live, and r against a real outdoor reference is
  only 0.45. See §2 P2.

And internally, on identical published thresholds, the two IRT families disagree with each other
by ~120×: `swbgt_empirical_days_ge_32` = 80.9 days/yr against `wbgt_shade_stull_days_ge_32` =
0.67 days/yr (national district means, historical 1990–2010). A reader takes that difference for a
sun effect. It is an artefact.

**Window caveat on every number above.** CarbonPlan's historical window is pinned
`slice("1985","2014")`; IRT's is 1990–2010. Any direct comparison carries that mismatch. Align
before quoting a residual gap as final.

### 1.1 The shipped threshold slugs are near-constant zero

Against `bernard_daily_max` at six ERA5 sites, 2005–2014, n = 3651 days per site
(`docs/diagnostics/wbgt_deployed_vs_reference/scores.csv`, re-verified 2026-09-25):

| site | ≥28 ref / shipped | ≥30 ref / shipped | ≥32 ref / shipped |
|---|---|---|---|
| Kochi | 1275 / **11** | 52 / **0** | 0 / 0 |
| Kolkata | 1801 / **614** | 552 / **39** | 39 / **0** |
| Bikaner | 1296 / **548** | 512 / **21** | 18 / **0** |
| Lucknow | 1441 / **468** | 509 / **12** | 30 / **0** |
| Hyderabad | 499 / **2** | 14 / **0** | 0 / 0 |
| Shimla | 0 / 0 | 0 / 0 | 0 / 0 |

`wbgt_shade_stull_days_ge_32` is **zero at every site**. `days_ge_30` returns 0–39 against a
reference of 14–552. This is not "biased low" — three of the four shade slugs carry almost no
spatial information, so their maps are uninformative rather than merely shifted.

Same runs, level and correlation:

| site | bias °C | RMSE °C | r |
|---|---|---|---|
| Kochi | −2.100 | 2.152 | 0.888 |
| Kolkata | −2.219 | 2.374 | 0.990 |
| Bikaner | −2.952 | 3.152 | 0.992 |
| Lucknow | −3.068 | 3.272 | 0.990 |
| Hyderabad | −2.712 | 2.839 | 0.961 |
| Shimla | −3.641 | 3.824 | 0.988 |

Note what `r` = 0.96–0.99 at five of six sites means: **within-site temporal ordering is largely
intact.** The defect is a level offset that varies across sites — survivable for an annual mean,
fatal for a threshold count.

---

## 2. The physics, in order of magnitude

### P1 — WBGT is evaluated at the wrong time of day (dominant; explains the shade gap)

IRT computes WBGT from **daily-mean** `tas` and **daily-mean** `hurs`. Two errors compound:

1. **Convexity (Jensen).** WBGT is convex in temperature through the saturation vapour pressure
   `es(T)`, so `WBGT(mean T) < mean(WBGT) < max(WBGT)`. Averaging first loses heat even before
   humidity is considered.
2. **Diurnal phase error.** `T` and `RH` are anti-correlated within the day. Daily-mean RH is
   dominated by the humid night; pairing it with a temperature that is neither the peak nor the
   trough describes a thermodynamic state that **occurs at no hour of the day**.

Heat stress is a peak-hour quantity. We publish a fictional average-hour one.

Measured magnitude: **−2.1 °C on average, ranging −1.2 to −4.2 °C, varying with diurnal
temperature range (r = −0.63).** That dependence is why this is a *ranking* error and not a
constant offset: high-DTR arid interiors are penalised harder than humid coasts, so the spatial
pattern is wrong, not just the level.

**Fix — the same choice CarbonPlan made, and it costs zero download.** `tas` (24 models),
`tasmax` (23) and `hurs` (22) are all already on disk:

```
e              = (hurs/100) * es(tas)      # es = Magnus, already in heat_stress_gridfirst.py
hurs_at_tasmax = 100 * e / es(tasmax)
WBGT           = f(tasmax, hurs_at_tasmax)
```

Vapour pressure `e` is the correct invariant to conserve across the day: `e` is near-constant
between sunrise and mid-afternoon while `RH` swings widely. Do **not** follow CarbonPlan's
`huss` + `ps` route — NEX publishes no `ps` (0 of the 22 models carrying `hurs`), which is why
they synthesise pressure from elevation, and IRT has no DEM. Holding `hurs` directly makes
pressure unnecessary here.

**Caveat, unresolved:** `tasmax` + RH-at-`tasmax` is a *daily proxy* for the hourly maximum. Its
ceiling is the hourly path (bias −0.04..−0.26 °C, r 0.998–1.000), which NEX cannot supply. The
proxy has **never been scored for shade against a shade reference** — it will land somewhere
between −2.1..−3.6 °C and ~0, and nobody has measured where. Earlier versions of this document
quoted the hourly ceiling as if it were the proxy's expected result. It is not. See §4 Step 1's
gate.

### P2 — "Outdoor WBGT" contains no radiation physics; its agreement is two errors cancelling

`swbgt_empirical` is the ACSM/BoM simplified WBGT: a fitted humidity term applied to dry-bulb
temperature, with **no radiant term and no wind term at all**. The literature treats sWBGT as
representing "average daytime shady conditions outdoors" — it is itself a *shade* estimate.
Both IRT families are therefore shade quantities computed two different ways, and neither is an
outdoor number.

Evidence it is not merely mislabelled but unphysical:

- Runs **+4.1 °C minimum / +7.4 °C mean** above shaded WBGT over T 15–50 °C × RH 5–100 %.
- **Exceeds dry-bulb air temperature on 54 %** of that domain, which a shade index cannot do.
- r = 0.45 against a real outdoor (hourly Liljegren) reference — the +3.5 % mean agreement with
  CarbonPlan's sun product in §1 is coincidence, not skill.
- Kong & Huber (2022) show sWBGT produces up to 30 % labour error and **the bias grows under
  warming**; `thermofeel`'s own documentation calls it "not an adequate approximation".

**What a real outdoor WBGT requires, and why `rsds` + `sfcWind` do not immediately deliver it.**
The ISO 7243 outdoor form is

```
WBGT_outdoor = 0.7*Tnw + 0.2*Tg + 0.1*Ta
```

`Tg` (globe temperature) is set by absorbed radiant load balanced against convective cooling, so
it needs irradiance **and** wind. `Tnw` (natural wet bulb) also depends on both. Both are
**instantaneous** quantities. The specific obstacles:

- **`rsds` is a daily *mean* flux.** A globe responds to instantaneous irradiance, and WBGT peaks
  a few hours *after* solar noon. Disaggregation by solar geometry is mandatory, not optional.
- **Buying hourly solar does not rescue this.** Already measured: true `rsds_max` scored *worse*
  than the disaggregated proxy. Do not spend money on sub-daily radiation on this argument.
- **`sfcWind` is a 10 m value; ISO/Liljegren want ~2 m.** Worth +0.3..0.8 °C, in the direction of
  *understating* outdoor heat. A log-law conversion is needed. CarbonPlan applies none.
- **The linear shade→sun adjustment is dead.** The Kong & Huber / CarbonPlan model
  (`adjustment = -2.1564 - 0.005375*rsds_max + 1.0424*sfcWind`) was fitted at Ta = 35 °C,
  RH = 50 % only and takes neither T nor RH as input. CHG-0567 measured that its intercept is
  **not constant**: it drifts **+0.109 °C per °C of warming**, sign-flips at Kochi, and spans
  2.43 °C warm-vs-cool at Bikaner. A full three-parameter refit beat the intercept-only refit at
  all six sites. **No version of that linear model transfers to SSP5-8.5.** This conclusion is
  firm and should not be re-litigated.

The remaining honest route is Liljegren on disaggregated hourly fields (§4 Step 3), whose
validated ceiling currently passes 4 of 6 sites.

### P3 — No bias correction, and correcting the marginals cannot fix a joint quantity

CarbonPlan bias-corrects against ERA5 (their notebook 06: QDM, 100 quantiles, day-of-year ±31-day
window, UHE-daily reference) **before** aggregation. IRT has no equivalent step.

The subtle part: NEX-GDDP-CMIP6 is already BCSD'd, which is often read as "so bias correction is
done". It is not, for this purpose. BCSD corrects `tas` and `hurs` **marginally, one variable at a
time**. WBGT depends on their *simultaneous joint* state, and a marginal correction does not
correct a derived index built on the joint distribution. The residual shows up exactly where it
hurts most:

- p99 bias **−5.60 °C** at Kochi.
- Days ≥ 32 °C = **57** against a reference of **212**; QDM brings it to **208** (CHG-0568).

Threshold counts are pure tail statistics, so for the ≥30 / ≥32 slugs this is a first-order
effect, not a refinement.

**CHG-0568's evidence has a scoping problem.** It corrects `nex_outdoor_wbgt()` — the Tier-2
*outdoor* chain — scored against `liljegren_daily_max`. If the outdoor metric is retired or
demoted, that evidence describes a metric that will no longer exist and must be **re-run against
`bernard_daily_max`**, which is a full re-run, not a refresh.

### P4 — Two smaller but real gaps

- **Psychrometric vs natural wet bulb.** Stull returns the *psychrometric* wet bulb `Tw`; ISO 7243
  wants the *natural* wet bulb `Tnw`, which is 0.5–1.5 °C higher at low wind. A systematic low
  bias. **We share this with CarbonPlan** (their `thermofeel.calculate_wbt` is also Stull), so it
  does not explain the §1 divergence — but it caps absolute accuracy for both.
- **No pressure correction**, which biases high-elevation Himalayan cells. CarbonPlan synthesises
  `ps` from elevation; IRT has no DEM.
- **RH is clipped at 0** although Stull is valid only over 5–99 %.
- **NaN zero-fill (W-04 / F-05).** `heat_stress_gridfirst.py:240` is
  `(field >= threshold).fillna(False).sum(dim="time").astype(float)` — a NaN day is silently
  scored as a non-exceedance, so an all-NaN cell returns **0**, indistinguishable from a genuine
  zero. This matters for diagnosis as well as correctness: a published zero could be P1's bias
  *or* this defect. The discriminator is cheap — `annual_mean` uses `skipna=True` and therefore
  stays NaN where all inputs are NaN, so reporting the count's zero-fraction **beside** the
  `annual_mean` NaN-fraction separates the two causes. Same Lakshadweep atolls as F-05.
- **Ensemble composition differs by variable**, and the new variables are the narrowest:
  `tas` 24 models, `tasmax` 23, `hurs` 22, **`rsds` 21, `sfcWind` 21** (verified on disk
  2026-09-26). Any outdoor chain is capped at 21 and the roster must be explicitly intersected,
  not assumed.

---

## 3. What is NOT the problem — stop investigating these

- **The shade formula.** IRT's two-term `0.7*Twb + 0.3*Ta` and CarbonPlan's full three-term ISO
  form agree to **2.9e-13 °C**, because they set `tmrt = tas` so their `BGT ≈ Ta` and
  `0.2*BGT + 0.1*Ta ≈ 0.3*Ta`. Verified by reading their source, not inferred.
- **Formula transcription.** Stull reproduces its own worked example (20 °C / 50 % → 13.70 °C).
  The sWBGT coefficients are the correct ACSM/BoM ones. The code's Magnus variant
  (6.112/17.62/243.12) vs BoM's (6.105/17.27/237.7) differs by ≤ 0.03 °C — immaterial, leave it.
- **Urban-vs-district sampling.** Dead. All 2,257 Indian CarbonPlan rows have an empty
  `ID_HDC_G0`; they are climatically-similar *regions*, not urban centres, so the comparison is
  not urban-biased.
- **The frozen CDF ruler.** There is none for WBGT. `config/frozen_rulers/` holds exactly six
  entries, all `composite_*`; `bundle_weights.py` contains no WBGT slug; and
  `metrics_registry.py:2800-2812` declares all eight WBGT/SWBGT slugs display-only diagnostics,
  not scored in `composite_heat_stress`. **Fixing these metrics moves no composite score and
  re-anchors no ruler.** A "ruler refit cost" was asserted in earlier versions of this document
  and in memory; it was phantom.

---

## 4. The order of work

Each step is gated. Step *n* is uninterpretable until step *n*−1 lands, for reasons stated.

### Step 1 — Fix the aggregation (P1). Free, dominant, do first.

Replace the daily-mean chain with `tasmax` + RH-at-`tasmax`, in **both** implementations:

| Path | Lines |
|---|---|
| `india_resilience_tool/compute/heat_stress_gridfirst.py` | `85-97` shade formula, `99-118` sWBGT formula, `221-238` branch |
| `tools/pipeline/compute_indices_multiprocess.py` | `892-934` `_wbgt_shade_stull_daily_mean_c`, `936-965` `_swbgt_empirical_daily_mean_c`, `967-1034` the four public entry points |

Patching one and not the other creates a silent divergence between the grid-first and
multiprocess paths. Ship a parity test asserting they agree on a fixture.

**Gate, fixed before results are seen.** Two measurements, both required:

1. Six-site score of the shade-at-`tasmax` candidate against `bernard_daily_max`, at the tool's
   default window 2005–2014 so it is comparable to §1.1. Bar: median |bias| < 1.0 °C and
   RMSE < 1.5 °C at all six sites; **and** day counts within ±20 % of reference for every
   site-threshold pair whose reference is ≥ 50 days.
2. Re-run the CarbonPlan shade crosscheck. Expect KS D to fall well below 0.477 and the −84.3 %
   mean gap to close to single digits.

**The bar cannot judge `days_ge_32`, and this is structural.** With the ≥ 50-day floor the
qualifying pairs are: five at ≥28 (Kochi 1275, Kolkata 1801, Bikaner 1296, Lucknow 1441,
Hyderabad 499), four at ≥30 (Kolkata 552, Bikaner 512, Lucknow 509, Kochi 52), and **zero at ≥32**
(Kolkata 39, Bikaner 18, Lucknow 30 all fall below the floor; the other three sites are 0). Nine
pairs total. So the slug that is most broken — `days_ge_32`, zero everywhere — **cannot be
validated at these six sites at any outcome of Step 1.** Its disposition (retire, or wait for a
different evidence base) is therefore an independent decision, not a consequence of this gate.

A floor is necessary because ±20 % on a tiny base is noise: Hyderabad's ≥30 reference is 14 days
in 3651, where ±20 % is ±2.8 days.

**Companion measurement, no interpreter dependency on ERA5:** read the eight published slugs out
of `processed_optimised` nationally and report per-state min / median / max, **zero-fraction beside
`annual_mean` NaN-fraction** (the P4 discriminator). This confirms or kills §1.1 against the full
population of 36 states rather than six points, and separates P1's bias from the NaN zero-fill.

### Step 2 — Bias-correct the derived WBGT, not the inputs (P3). Strictly after Step 1.

QDM the WBGT field itself against an ERA5 WBGT, as CarbonPlan does — because P3 is a joint-
distribution problem, correcting `tas` and `hurs` separately will not fix it.

**Order matters for a substantive reason, not tidiness:** run before Step 1 and the quantile map
absorbs Step 1's aggregation error into itself, making the defect invisible rather than fixed.

Cost is real and should be priced before committing: a gridded ERA5 WBGT reference for all India
plus a stored per-cell, per-day-of-year quantile table — a large new data dependency and a new
pipeline artifact. Weigh that against these slugs being display-only diagnostics.

### Step 3 — Build a physical outdoor WBGT, or drop the outdoor claim (P2).

Retire the linear adjustment permanently. Build instead:

1. Disaggregate daily-mean `rsds` to hourly via solar geometry (metsim `solar_geom` + `shortwave`,
   or an equivalent clear-sky shape with a diffuse-fraction split).
2. Disaggregate `tas` across `tasmin`/`tasmax` with a diurnal shape.
3. Convert `sfcWind` from 10 m to 2 m by log law.
4. Run Liljegren hourly; take the daily maximum.
5. Intersect the model roster down to the 21 models carrying `rsds` and `sfcWind`.

Validate against the six-site hourly Liljegren reference. **Honest constraint: that ceiling
currently passes 4 of 6 sites.** If it does not clear, the defensible outcome is to publish
**shade only** and drop the outdoor claim entirely — which is precisely what CarbonPlan's shade
product is.

Until Step 3 lands or is abandoned, `swbgt_empirical`'s display label "Outdoor WBGT" should be
renamed to match the description already shipped at `metrics_registry.py:681-698` (which already
discloses that it "does not directly model solar radiation, wind speed, or black-globe
temperature"). A rename is independent of every other step and can ship at any time.

Note that **rename and retire are orthogonal axes**, and earlier discussion conflated them:
family *labelling* (is "Outdoor WBGT" honest?) is a separate question from slug *viability* (can
`days_ge_32` be validated at all?).

### Step 4 — Residuals.

Natural-vs-psychrometric wet bulb (correct it or document it explicitly); pressure/elevation
correction; RH clipping bounds; the W-04/F-05 NaN zero-fill; and align the analysis window to
1985–2014 for any CarbonPlan comparison.

---

## 5. What is on disk (for Step 3 planning)

Verified 2026-09-26.

| Variable | Models | Location |
|---|---|---|
| `tas` | 24 | main NEX tree |
| `tasmax` | 23 | main NEX tree |
| `hurs` | 22 | main NEX tree |
| `huss` | 21 | main NEX tree |
| `ps` | **0** | not published by NEX — do not plan around it |
| `rsds` | 21 | `irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1/{historical,ssp245,ssp585}/rsds/<MODEL>/<YEAR>.nc` |
| `sfcWind` | 21 | same tree, `/sfcWind/` |

The v2.0 `rsds` + `sfcWind` acquisition completed 2026-09-24 (CHG-0555, commit `742e60d`), 67.05 GB,
full independent verification passed 6006/6006. It lives in a **separate tree** from the main NEX
variables, so Step 3 must join two trees.

ERA5 six-site validation cache: `scratch/wbgt_deployed_vs_reference_cache`, complete **1990–2014**
at all six sites (25 files each, verified). The scoring tool's `DEFAULT_YEARS = "2005-2014"`, which
matches every reference number in §1.1 — **do not override it to 1990–2014 when reproducing those
tables**, or the results will not be comparable. The 1990–2004 backfill buys Step 1's gate nothing
at default settings.

Reference products: CarbonPlan CSVs at
`https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/extreme-heat/v1.0/outputs/csv/carbonplan-extreme-heat-<stat>-WBGT-<shade|sun>.csv`
(public, no auth; Indian rows are `hierid` starting `IND`).

---

## 6. Corrections applied to earlier versions of this document

Kept so the same errors are not reintroduced. The 2026-09-24 version asserted several things that
were checked against disk on 2026-09-25 and 2026-09-26 and found wrong.

| Claim in an earlier version | Status |
|---|---|
| "The frozen CDF ruler for `wbgt_shade_stull_*` must be refit and canaries re-shot — that is the real cost of Step 1." | **FALSE.** No WBGT ruler exists. See §3. This framing had also been written into memory and propagated across several sessions. |
| Patch scope named only `heat_stress_gridfirst.py`. | **INCOMPLETE.** There are two implementations. See §4 Step 1. |
| "`tasmax` + RH-at-`tasmax` raised day-to-day `r` at 6/6 sites" offered as proof the shade fix works. | **MISATTRIBUTED.** Every row of `wbgt_tier2_probe/per_site.csv` carries `reference = liljegren_daily_max` — that is *outdoor*-chain evidence. The shade proxy has never been scored against a shade reference. |
| "Hold QDM, then re-measure" while also retiring the outdoor metric. | **CONTRADICTORY**, and understated the work — a full re-run against a different reference. See §2 P3. |
| `swbgt_empirical` is "mislabelled". | **TOO STRONG.** The methodology is already disclosed at `metrics_registry.py:681-698`. The defect is the *display name*. Rename was never offered as an option; it should have been. |
| Acceptance bar: "the four sites where the reference is non-zero". | **UNEXECUTABLE.** Verified non-zero-reference site counts are **five** at ≥28, **five** at ≥30, **three** at ≥32. No threshold has four. Replaced by the ≥ 50-day floor in §4 Step 1. |
| Bar had no minimum-count floor. | **FIXED.** ±20 % of 14 days is ±2.8 days. |
| "`CHG-0566`, `CHG-0567` and `CHG-0568` exist only in uncommitted work and memory." | **WRONG for `CHG-0566`** — it is in commit `269be91`'s message (`CHG-0563..CHG-0566`), i.e. committed NEX verification work. The id-hygiene section was itself misallocating an id. |
| `CHG-0573` marked `APPLIED (user-confirmed)` in the session handoff. | **FALSE at the time.** The same handoff's closing line said no `Applied CHG-xxxx` had been given, and this file's own ledger said `SUGGESTED`. Three statements, two wrong. |
| Step 0 described the cache as "1990-2014", implying that window. | **TRAP.** See §5 — the default and correct window is 2005–2014. |
| Handoff named `--dry-run` as CHG-0569's validation gate. | **WRONG FLAG.** Both exist; `--dry-run` (line 862) only prints a plan and never evaluates the new humidity inversion. `--self-test` (line 867) is the gate, and must be extended with a round-trip assertion on `e → hurs_at_tasmax`. |
| A national read of the published slugs would prove the aggregation story. | **CONFOUNDED** by the P4 NaN zero-fill until it also reports the `annual_mean` NaN mask. |

**CHG id hygiene.** Highest id in **tracked** files is `CHG-0565`; commit messages additionally
consume up to `CHG-0566`. `CHG-0567` and `CHG-0568` are asserted **only in this file and in
memory** — `tools/diagnostics/wbgt_intercept_refit.py` and
`tools/diagnostics/wbgt_qdm_bias_correction.py` cite only `CHG-0546`/`0554`/`0555` and carry no
`0567`/`0568` marker, so if this untracked file were lost those ids would detach from their code.
Ids are **not** sequential — always `grep -o "CHG-05[0-9][0-9]"` across tracked files *and* commit
messages before allocating.

---

## 7. Ledger

| Change ID | File(s) | Summary | Status |
|---|---|---|---|
| CHG-0569 | `tools/diagnostics/wbgt_deployed_vs_reference.py` | Add shade-at-`tasmax` candidate vs `bernard_daily_max` at default 2005–2014; extend `--self-test` with the humidity round-trip | `SUGGESTED` |
| CHG-0570 | `heat_stress_gridfirst.py` + `tools/pipeline/compute_indices_multiprocess.py` | Aggregation fix (P1), **both** paths | `SUGGESTED` (blocked on CHG-0569) |
| CHG-0571 | `tests/` | Parity guard across the two paths + RH-at-`tasmax` round-trip | `SUGGESTED` (ships with CHG-0570) |
| CHG-0572 | `india_resilience_tool/config/metrics_registry.py` | Rename the "Outdoor WBGT" display label | `SUGGESTED` (independent) |
| CHG-0573 | `docs/diagnostics/wbgt_reconciliation/README.md` | This document | `SUGGESTED` |
| CHG-0574 | `tools/diagnostics/wbgt_published_distribution.py` (new) | National read of the eight published slugs; zero-fraction beside `annual_mean` NaN-fraction | `SUGGESTED` |
| — | — | Frozen-ruler refit — **withdrawn, no such ruler exists** | `SUPERSEDED` |

Superseded id assignments from the 2026-09-24 draft — **do not use these meanings**:
`CHG-0569` = aggregation fix, `CHG-0570` = tests, `CHG-0571` = retire `swbgt_empirical_*`,
`CHG-0572` = ruler refit. All four ids were reassigned in §7 above.

Carried, still open, from earlier sessions: `CHG-0532` (wet-bulb / pressure caveats need
rewriting — P4 is where that text belongs), `CHG-0540` (a stronger argument is now available),
W-04/F-05 zero-fill at `heat_stress_gridfirst.py:240`.

Documentation debt: `MANIFEST.md` and `tools/README.md` owe lines for
`tools/diagnostics/wbgt_intercept_refit.py`, `tools/diagnostics/wbgt_qdm_bias_correction.py`, and
this document.

---

## 8. Open decisions

1. **`days_ge_32` (both families).** Unvalidatable at the six-site evidence base at any outcome of
   Step 1 (§4), and currently publishing ~0 nationally. Retire now, or hold visible pending a
   gridded ERA5 reference? **Awaiting the user's call.**
2. **QDM into the pipeline (Step 2)?** Unmoved since 2026-09-24. Its evidence is tied to the
   outdoor chain, so if outdoor is retired or demoted it must be re-run against
   `bernard_daily_max` first.
3. **Retire vs rename `swbgt_empirical_*`.** §4 Step 3 recommends rename now, retire only if
   Step 3's Liljegren build fails its gate. Retirement remains available.
4. **Was the daily-peak aggregation fix ever scoped as its own CHG?** Asked 2026-09-21, never
   ruled. §7 answers it as CHG-0570; the user has not confirmed.

## 9. Environment blocker

This WSL session has **no interpreter with pandas** — WSL `python3` lacks it, there is no WSL
conda, and `/mnt/c/Users/*/envs/irt` does not resolve. That blocks CHG-0569 **and** CHG-0574
equally. Every numeric step above needs either the Windows `irt` interpreter path or a
Windows-side run.

## 10. Provenance

- Shade-vs-Bernard scores: `docs/diagnostics/wbgt_deployed_vs_reference/scores.csv`
- CarbonPlan crosscheck: `docs/diagnostics/wbgt_carbonplan_crosscheck/` (CHG-0543, CHG-0544)
- Tier-2 variants (all vs Liljegren): `docs/diagnostics/wbgt_tier2_probe/per_site.csv`
- Intercept non-constancy: `docs/diagnostics/wbgt_intercept_refit/README.md` (CHG-0567)
- QDM measurement: `docs/diagnostics/wbgt_qdm_bias_correction/README.md` (CHG-0568)
- CarbonPlan source: `github.com/carbonplan/extreme-heat`, notebooks 02 (shade), 06 (ERA5 QDM),
  07 (solar + wind), 08 (shade→sun)
- Display-only declaration: `india_resilience_tool/config/metrics_registry.py:2800-2812`
- Frozen rulers: `india_resilience_tool/config/frozen_rulers/` (six composites, no WBGT)
