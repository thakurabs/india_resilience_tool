# Outdoor-WBGT method selection — milestone 2

Generated 2026-09-29 from `GIT:add_flood_depth@afe6cb8`.
Frozen contract: [`SPEC.md`](SPEC.md) (CHG-0608), written before any score below existed.
Tool: `tools/diagnostics/wbgt_outdoor_selection.py` (CHG-0609).
Predecessor, immutable: [`../wbgt_outdoor_feasibility/`](../wbgt_outdoor_feasibility/README.md).

Nothing in production changed. The national shade rebuild ran throughout, untouched.

---

## 1. Decision

| | |
|---|---|
| **Selected method** | **W1** — physical Liljegren on a daily-input reconstruction with a **DTR-dependent within-day wind shape**. Named the **best development candidate**, not a production method. |
| **Uses `rsds` and `sfcWind`** | **Yes**, both, and both matter: `rsds` drives the radiant load through an Erbs beam/diffuse split, `sfcWind` sets the convective term the shape redistributes. |
| **Daily gate** | **PASS 12/12** site-window pairs. Median absolute daily error 0.49–0.73 °C (bar 1.0), RMSE 0.85–1.24 °C (bar 1.5), r 0.865–0.979. Year-block bootstrap upper bounds 0.757 and 1.280 — both clear of the bars. **Insensitive to the reference treatment** (0 of 16 verdicts move under R3). |
| **Threshold-count gate** | **FAIL — 28/30 gated pairs pass.** Both failures are Hyderabad ≥ 32 °C: −9.1 d/yr (tolerance 7.9) and −9.5 d/yr (tolerance 9.0). W1 is nonetheless the best of the four: worst-case count error 15.6 d/yr against C1's 38.2, C2's 21.2 and W2's 23.8. |
| **Remaining errors by site/threshold** | See §7. At ≥ 32 °C: Hyderabad −9.1/−9.5 (FAIL), Bikaner +10.4/+12.1 (marginal pass), Kochi −4.2/+15.6, Kolkata −0.9/+4.5, Lucknow −9.8/−3.5, Shimla 0.0 (not gated — reference is 0 days/yr). Annual mean of daily maxima −0.30…+0.18 °C, the tightest of every candidate and comparator. |
| **Matched-period NEX (1990–1999)** | Annual mean transfers to within **−0.88…+1.41 °C**. Annual ≥ 32 °C counts do **not**: −58 %/−50 % at Kochi, +49 %/+50 % at Kolkata, +43 %/+54 % at Bikaner, −55 %/−27 % at Hyderabad, +19 %/+26 % at Lucknow. **The sign varies by site**, so this is not a uniform offset. |
| **Bias correction required** | **Yes for threshold counts; no for the level product.** The predeclared trigger **fired**. QDM on the derived WBGT cuts held-out ≥ 32 °C error from −111.8…+58.9 to −16.8…+10.2 d/yr, preserves the ssp585 warming signal (|Δ| ≤ 0.27 °C at q50, ≤ 0.83 °C at q99) and produced **zero** uncorrectable days — but it **degrades** ≥ 28/30 °C at several sites, and it needs a national reference asset that does not exist. |
| **CarbonPlan alignment** | **Retain as a comparator only.** Their raw chain **fails the daily gate at 9/12 pairs** (median abs 0.80–1.72 °C, RMSE 1.22–2.30 °C, bias −1.84…−0.79 °C) and 14/30 count pairs, worst −96.5 d/yr. The physical reconstruction beats it at every site. Selected workflow elements to adopt: their QDM configuration, and their radiation-peak treatment, which IRT already matches. |
| **Readiness for a three-state staged pilot** | **CONDITIONAL.** Ready for a bounded staged pilot of the **level** product (annual mean of daily maxima, and ranking). **Not** ready for **threshold counts**. |
| **Exact outstanding conditions** | (1) The count-gate result is **not resolvable at the present reference precision** — a 0.20 °C reference ambiguity flips 13 gate verdicts (§4). (2) The count blocker is the **temperature/humidity reconstruction, not wind** (§3), and W1's count advantage is partly error cancellation. (3) National threshold counts need a QDM reference asset that has not been defined (§8). |

### 1.1 What changed relative to milestone 1

| Milestone 1 said | Milestone 2 measured |
|---|---|
| "The blocker is the within-day wind shape, nothing else." | **Refuted.** Supplying the *actual* hourly wind makes counts **worse** (oracle A5: 4 failing pairs, worst 32.9 d/yr, against W1's 2 and 15.6). The blocker is the T/RH reconstruction. |
| Wind shape choice was worth pursuing | **Partly confirmed:** W1 halves the worst count error (38.2 → 15.6 d/yr) and tightens the annual mean. But it does so partly by cancelling a warm bias, not by getting the wind right. |
| Two candidate wind shapes remained to try | **Both tried, neither passes.** The smallest remaining requirement has moved to the moisture invariant (§9). |

---

## 2. What was demonstrated

**W1 is the best daily-accuracy outdoor estimator IRT has produced.** All four physical candidates clear the daily gate on all twelve pairs, W1 and C2 by the widest margin, and W1 has the tightest annual mean of daily maxima of anything measured — better than C1, C2, W2, the CarbonPlan-style comparator, and — on that statistic at **all twelve** site-window pairs — better than the **oracle** that is handed the true hourly wind. That inversion is not a fluke: the oracle inherits the reconstruction's warm temperature/humidity bias without the wind-shape cancellation that offsets it (§3).

| candidate | daily gate | median abs error °C | RMSE °C | annual mean error °C | count pairs failing |
|---|---|---|---|---|---|
| **W1** DTR wind | **PASS 12/12** | 0.49 – 0.73 | 0.85 – 1.24 | **−0.30 … +0.18** | **2 / 30** |
| C2 fixed-amplitude wind | PASS 12/12 | 0.46 – 0.71 | 0.87 – 1.23 | −0.32 … +0.08 | 2 / 30 |
| C1 constant wind | PASS 12/12 | 0.63 – 0.90 | 0.90 – 1.16 | +0.02 … +0.44 | 2 / 30 |
| W2 climatological wind | PASS 6/6 | 0.60 – 0.85 | 0.87 – 1.20 | −0.61 … +0.31 | 2 / 15 |
| A5 **ORACLE** (true hourly wind) | PASS 12/12 | 0.41 – 0.72 | 0.62 – 0.97 | −0.31 … +0.71 | 4 / 30 |
| CarbonPlan-style | **FAIL 9/12** | 0.80 – 1.72 | 1.22 – 2.30 | −1.84 … −0.80 | 14 / 30 |

Both new wind shapes are **exactly mean-preserving and never clip**: `n_clipped_at_zero = 0` and `max_daily_mean_error = 0.0 m/s` for every candidate at every site-window (`wind_diagnostics.csv`). Solver-floor exposure is 0–2.7 % of hours for the candidates against **1.1–14.4 %** for the actual wind — the reconstructions sit above `thermofeel`'s 0.62 m/s floor far more often than reality does, which is itself part of why a perfect wind changes the answer.

W2's evidence base is **half** the others' (6 pairs, not 12), because SPEC.md 3.2 confines it to the window its calibration period does not touch.

---

## 3. Wind attribution — the milestone-1 conclusion is refuted

SPEC.md 4 required a complementary oracle before wind could be called the cause. It was run, and it does not support that conclusion.

**Oracle A5** gives C1's reconstructed temperature, humidity, radiation and pressure the **actual hourly wind**. If wind were the blocker, A5 should pass the count gate. It fails *more*:

| site | ref ≥32 d/yr | C1 | C2 | W1 | W2 | **A5 (true wind)** |
|---|---|---|---|---|---|---|
| Bikaner 1990-2004 | 72.6 | +18.2 **FAIL** | +7.8 | +10.4 | — | **+29.1** |
| Bikaner 2005-2014 | 86.4 | +21.2 **FAIL** | +9.9 | +12.1 | +22.8 **FAIL** | **+32.9** |
| Hyderabad 1990-2004 | 39.4 | +2.0 | −11.0 **FAIL** | −9.1 **FAIL** | — | **+17.2** |
| Hyderabad 2005-2014 | 44.9 | +2.1 | −11.0 **FAIL** | −9.5 **FAIL** | +9.9 **FAIL** | **+19.1** |
| Kochi 2005-2014 | 206.2 | +38.2 | +2.5 | +15.6 | +6.5 | +18.8 |
| Kolkata 2005-2014 | 138.1 | +13.8 | −3.1 | +4.5 | +5.6 | +13.4 |

Signed error, days/year. Every A5 entry is **positive and larger than C1's**, at every site.

**Why.** Milestone 1's own ablations already contained the answer, read here for the first time against counts rather than against daily error. `abl_A1` (reconstructed temperature and humidity, everything else actual) carries **+31.1/+33.7 d/yr at Bikaner** and **+12.9/+15.8 at Hyderabad** — larger than any candidate's total error. `abl_A2` (radiation) is −5.4…+4.3 and `abl_A4` (ISA pressure) is ≈ 0. `abl_A3` (wind alone) is *negative*, −19.9…+4.3. The two dominant terms have **opposite signs and partially cancel**, and the wind shape is the knob that tunes the cancellation.

**The assumed shape is empirically wrong.** Measured from the cached ERA5, the annual-mean hourly 10 m wind profile (ratio to the daily mean):

| site | actual peak hour (UTC) | actual range | assumed peak hour | correlation actual vs assumed |
|---|---|---|---|---|
| Kochi | 10 | 0.70 – 1.67 | 7 | +0.63 |
| Kolkata | 9 | 0.90 – 1.14 | 7 | +0.79 |
| **Bikaner** | **20** | **0.93 – 1.06** | 8 | **−0.47** |
| Lucknow | 10 | 0.92 – 1.16 | 7 | +0.80 |
| Hyderabad | 4 | 0.83 – 1.20 | 7 | +0.66 |
| Shimla | 9 | 0.72 – 1.60 | 7 | +0.83 |

At **Bikaner** — the site C1 fails — the real diurnal wind cycle is **anti-correlated** with the assumed shape and spans only ±7 %, i.e. the real wind there is nearly constant and **C1 is the physically correct choice**. Yet C1 over-counts ≥ 32 °C there by +18/+21 d/yr. Bikaner's count error is therefore **not** a wind error, and C2/W1 "fix" it by imposing a large midday wind increase that does not exist.

**W2 corroborates this.** W2 uses the *real* climatological shape and behaves like the oracle, not like C2/W1: +22.8 at Bikaner and +9.9 at Hyderabad, both failures, in the same direction as A5. A candidate built on the true wind shape performs *worse* on counts than one built on a false shape. That is the signature of error cancellation, and it is why W2's need for a climatology asset is moot — the asset buys nothing.

**Answering SPEC.md 4's three questions directly.** Replacing wind does **not** resolve the Bikaner/Hyderabad failures — it reverses Hyderabad's sign and roughly doubles Bikaner's error. On the daily statistic it helps only patchily: the median absolute error changes by **−0.32 to +0.07 °C**, improving 8 of 12 pairs (most at Shimla, −0.32, and Lucknow, −0.17) and **worsening 4** (Kolkata and Bikaner 2005-2014, Hyderabad 2005-2014). On the annual mean of daily maxima the oracle is worse than W1 at **all twelve** pairs. It **does** create failures elsewhere: two gated pairs that pass for C1/C2/W1 fail under the oracle.

---

## 4. Reference sensitivity — this is what blocks a firm count-gate verdict

R3 puts every instantaneous driver on the same half-hour grid as the radiation interval mean, by linear interpolation to `H − 30 min`. **It is an approximation, not a correction**: interpolating an instantaneous field is not that field's midpoint value, and no treatment resolves the sunrise/sunset-straddling hour, in which an hour-mean flux is paired with a geometry that changes sign inside the hour.

**On daily maxima R3 is a rounding error.** Bias −0.20…−0.23 °C, median absolute difference 0.18–0.21 °C, RMSE 0.28–0.35 °C. The **daily gate does not move for any candidate: 0 of 16 verdicts change.** The daily result is robust to the reference treatment.

**On counts it is decisive.** The same 0.2 °C shifts the *reference's own* ≥ 32 °C count by −7.7 d/yr at Bikaner and −5.8 d/yr at Hyderabad, and the peak hour moves on **39–44 %** of days (mean +0.27 h). **13 of the count-gate verdicts flip**, including every failure and every marginal pass that matters:

| site | window | candidate | ref ≥32 audited | ref ≥32 under R3 | candidate | gate audited | gate R3 |
|---|---|---|---|---|---|---|---|
| Bikaner | 1990-2004 | **W1** | 73.1 | 65.4 | 83.9 | **PASS** | **FAIL** |
| Bikaner | 2005-2014 | **W1** | 87.2 | 78.1 | 98.6 | **PASS** | **FAIL** |
| Hyderabad | 1990-2004 | **W1** | 39.5 | 33.7 | 30.9 | **FAIL** | **PASS** |
| Hyderabad | 1990-2004 | C1 | 39.5 | 33.7 | 41.6 | PASS | FAIL |
| Hyderabad | 2005-2014 | C2 | 45.0 | 37.9 | 35.2 | FAIL | PASS |

**Per the rule frozen in SPEC.md 5, this is recorded as a reference uncertainty blocking a firm pass.** W1's two failures become passes and its two marginal passes become failures under a reference treatment that differs by a fifth of a degree — less than any candidate's error. Neither reference is adopted because it favours a candidate: `ref_audited` remains the reference, and the honest statement is that **the count gate cannot be settled at this reference precision**, not that W1 passes or fails it.

A general reference-development project is **not** opened. The bounded implication for the pilot is in §10.

---

## 5. CarbonPlan alignment

Read verbatim at revision **`f662b37200fe219db912ebd09ceb52fdac979861`** (2024-06-14), notebooks `07_solar_radiation_wind.ipynb` and `08_shade_sun_adjustment.ipynb`. Full table: [`carbonplan_alignment.csv`](carbonplan_alignment.csv).

| element | CarbonPlan | IRT W1 | material? |
|---|---|---|---|
| Temperature | daily `tasmax` only | hourly Parton & Logan curve | **yes** |
| Humidity | RH at `tasmax` from `huss` + synthesised `ps` | hourly RH at constant daily vapour pressure from `hurs` | no (same invariant) |
| Radiation | daily-mean `rsds` → `metsim.shortwave` → daily max → ×0.75 | daily-mean `rsds` → TOA interval-mean shape → hourly, + Erbs beam split | partial |
| Wind | shade fixed 0.5 m/s (inert); adjustment uses daily-mean, clipped [0.5, 3] | hourly 10 m from daily mean via a within-day shape | **yes** |
| Pressure | synthesised from elevation | ISA from elevation | no |
| Mechanism | **empirical**: 2-predictor OLS on 16 points from Kong & Huber (2022) Fig S12, R² = 0.922 | **physical**: Liljegren globe and natural-wet-bulb balances, solved hourly | **yes** |
| Shade formula | three-term ISO | two-term (comparator only) | no — identical to 2.9e-13 °C |
| Bias correction | QDM on derived WBGT vs UHE-daily, 100 quantiles, additive, dayofyear/31 d, trained once 1985–2014 | none in the comparator | **yes, deliberate** |
| Thresholds | 29 / 30.5 / 32 / 35 °C | 28 / 30 / 32 °C | partial (32 shared) |
| Aggregation | population-weighted, ~39 k regions, 1985–2014 | six sites here; district/block area-weighted in production, 1990–2010 | **yes** |

**A finding worth recording: IRT's shipped Tier-2 coefficients *are* CarbonPlan's.** Refitting their 16 published points locally gives `(−2.15635593, −0.005375, +1.04237288)` against IRT's shipped `(−2.1564, −0.005375, 1.0424)` — agreement to **4.4e-5**. The retired "Tier-2 linear sun adjustment" was never an IRT invention; it is the CarbonPlan chain, and scoring it *is* scoring them.

**Deviations that make this CarbonPlan-*style*, not a reproduction:** `metsim` is not installed, so radiation is disaggregated with the TOA interval-mean shape instead of `metsim.shortwave`; their notebook-06 QDM correction of the shade WBGT against UHE-daily is excluded, because SPEC.md 6 requires the **raw** chain; and RH at `tasmax` comes from `hurs` directly rather than from `huss` with a synthesised pressure.

**Recommendation: retain only as a comparator.** Their raw chain fails the daily gate at 9 of 12 pairs and is cold by 0.80–1.84 °C everywhere, with a systematic ≥ 32 °C under-count reaching −96.5 d/yr at Kochi. It is not adopted, and it is not forced. Two elements *are* worth taking: their **QDM configuration**, reproduced and used in §8, and their **0.75 radiation peak-lag factor**, which IRT already applies. Their population weighting and their 1985–2014 window are noted as reasons the earlier national distribution comparison (KS D = 0.101, p = 1.4e-5) was never paired validation.

---

## 6. Selection

Rule applied exactly as frozen in SPEC.md 7. Full record: [`selection.json`](selection.json).

1. **Daily gate.** C1, C2, W1, W2 all pass. Nothing eliminated.
2. **Count gate.** No candidate passes every gated pair. Ranked by fewest failing pairs, then worst absolute count error, then individual years outside tolerance: **W1 (2 failing, 15.6 d/yr) → C2 (2, 21.2) → W2 (2 of 15, 23.8) → C1 (2, 38.2)**.
3. **Deployability.** W2 would need a spatial hourly wind climatology that does not exist; W1 needs nothing beyond the six NEX variables, latitude, longitude and a coarse elevation. W2 also performs worse. The asset buys nothing.
4. **Simplicity.** W1 is C2 with one extra daily predictor and no new data.

**Status: BEST DEVELOPMENT CANDIDATE — the count gate is not passed.** W1 is not production-ready.

**Two honest weaknesses in the gate itself**, carried forward from milestone 1 and now quantified for W1:

- **The relative tolerance is permissive where counts are large.** At Kochi 2005-2014 the tolerance is 41.2 d/yr, so W1's +15.6 d/yr passes comfortably and even C1's +38.2 d/yr passes. A pass at a high-count site is a weak statement.
- **Shimla contributes no count evidence at all.** Its reference carries 0.62/0.88 days ≥ 28 °C and exactly 0 at ≥ 30 and ≥ 32, so all six of its pairs are `NOT GATED (rare event)`. The count gate rests on **five** sites, and the cool-mountain regime is untested for counts. That is reported as not evaluated, not as a pass.

**The mean-level gate can also hide cancelling years** ([`per_year_count_errors.csv`](per_year_count_errors.csv)). W1's Bikaner 1990-2004 pass (+10.4 d/yr mean) has **3 of 13** individual years outside tolerance; its Hyderabad failures have **8 of 13** and **5 of 8** — a consistent failure, not a fluke. C2's Kochi 1990-2004 pass hides one year at −39 d/yr.

---

## 7. Remaining errors

### 7.1 W1 daily statistics, per site-window

| site | window | n days | bias °C | median abs °C | RMSE °C | r |
|---|---|---|---|---|---|---|
| Kochi | 1990-2004 | 5476 | −0.074 | 0.666 | 0.932 | 0.880 |
| Kolkata | 1990-2004 | 5476 | +0.080 | 0.510 | 0.848 | 0.974 |
| Bikaner | 1990-2004 | 5476 | −0.281 | 0.639 | 1.240 | 0.972 |
| Lucknow | 1990-2004 | 5476 | −0.279 | 0.589 | 1.072 | 0.979 |
| Hyderabad | 1990-2004 | 5476 | −0.018 | 0.493 | 0.873 | 0.944 |
| Shimla | 1990-2004 | 5476 | −0.169 | 0.725 | 1.104 | 0.978 |
| Kochi | 2005-2014 | 3649 | +0.052 | 0.719 | 0.983 | 0.865 |
| Kolkata | 2005-2014 | 3649 | +0.193 | 0.582 | 0.901 | 0.974 |
| Bikaner | 2005-2014 | 3649 | −0.117 | 0.687 | 1.171 | 0.976 |
| Lucknow | 2005-2014 | 3649 | −0.206 | 0.601 | 1.053 | 0.979 |
| Hyderabad | 2005-2014 | 3649 | −0.035 | 0.491 | 0.871 | 0.945 |
| Shimla | 2005-2014 | 3649 | −0.091 | 0.728 | 1.094 | 0.978 |

### 7.2 W1 count errors, days/year

| site | ≥28 | ≥30 | ≥32 | gate ≥32 |
|---|---|---|---|---|
| Kochi 1990-2004 / 2005-2014 | within tolerance | within tolerance | −4.2 / +15.6 | PASS (permissive) |
| Kolkata | within | within | −0.9 / +4.5 | PASS |
| Bikaner | within | within | +10.4 / +12.1 | PASS (marginal; 3/13 and 2/8 years out) |
| Lucknow | within | within | −9.8 / −3.5 | PASS |
| **Hyderabad** | within | within | **−9.1 / −9.5** | **FAIL** (tol 7.9 / 9.0) |
| Shimla | not gated | not gated | not gated | — |

### 7.3 Error attribution

| source | evidence | daily cost | ≥32 °C count cost | status |
|---|---|---|---|---|
| **Temperature + humidity reconstruction** | `abl_A1` | 0.35 – 0.79 °C | **+6.4 … +33.7 d/yr** at the five gated sites, 0.0 at Shimla | **dominant, unresolved** |
| Within-day wind shape | `abl_A3`, A5, W2 | 0.27 – 0.87 °C | −19.9 … +4.3 d/yr, **opposite sign** | partly cancels the above; W1 is the best compromise found |
| Reference treatment | R3 | 0.18 – 0.21 °C | **−5.8 … −9.1 d/yr** | **blocks a firm verdict** |
| Radiation reconstruction | `abl_A2` | 0.11 – 0.23 °C | −5.4 … +4.3 d/yr | acceptable |
| Pressure / elevation | `abl_A4`, §10.4 | ≤ 0.02 °C; ≤ 0.15 °C for a 2276 m elevation error | ≤ 1.0 d/yr | **retired** |
| NEX day boundary | §8.2 | ≤ 0.07 °C | −2.7 … +0.2 d/yr | **retired** |
| **NEX joint distribution** | §8 | ≤ 1.41 °C on the annual mean | **−111.8 … +62.5 d/yr** | **largest term of all** |

---

## 8. Matched-period NEX comparison

W1 on **ACCESS-CM2** and **MRI-ESM2-0**, `historical` **1990–1999**, six sites, against the ERA5-driven `ref_audited` for the same years and the same complete-year policy. Distribution and annual counts only — **no same-date RMSE or correlation is computed**, because NEX carries no weather for a particular ERA5 date. Per-model results before any ensemble summary. Full table: [`nex_matched_comparison.csv`](nex_matched_comparison.csv).

| site | annual mean error °C (ACCESS / MRI) | ≥28 rel. | ≥30 rel. | **≥32 rel.** |
|---|---|---|---|---|
| Kochi | −0.88 / −0.80 | +2 % / +2 % | −10 % / −8 % | **−58 % / −50 %** |
| Kolkata | +0.94 / +0.91 | +2 % / +2 % | +10 % / +7 % | **+49 % / +50 %** |
| Bikaner | −0.00 / +0.02 | +1 % / +3 % | +8 % / +14 % | **+43 % / +54 %** |
| Lucknow | +0.05 / +0.10 | −2 % / +1 % | +3 % / +5 % | **+19 % / +26 %** |
| Hyderabad | −0.07 / +0.08 | +9 % / +9 % | +27 % / +33 % | **−55 % / −27 %** |
| Shimla | +1.41 / +1.33 | +170 % / +328 % (on 0.6 d/yr — rare event) | 0 / 0 | 0 / 0 |

**The level transfers; the tail does not, and the sign is site-dependent.** Kochi and Hyderabad under-count by half while Kolkata and Bikaner over-count by half, from the same two models in the same years. Both models agree closely with each other at every site, so this is a property of the NEX joint distribution and not of model spread.

**Available versus valid years:** all 10 years are present for both models and both scenarios (six variables each, verified from milestone 1's inventory). The ERA5 reference yields **9** complete years and the NEX series **8**, the difference being the record-boundary days that the complete-year policy discards. Calendars: both models are `noleap`-compatible in the required range and neither is the `360_day` outlier; the day boundary is **inferred** from the CMIP6 daily convention, since no NEX file publishes `time_bnds`. Grid points are nearest-neighbour picks; Shimla's +1.3…+1.4 °C annual-mean warm bias is consistent with a ~0.25° cell that cannot resolve a 2276 m ridge, which is a **sampling** difference, not a model error.

### 8.1 Interpretation constraint

These differences are **not** all marginal bias or joint-variable dependence. Grid sampling and elevation (demonstrably so at Shimla), the reconstruction itself (§7.3), the reference treatment (§4) and the inferred day convention (§8.2) all remain in the interpretation.

### 8.2 Source-day sensitivity

The cached ERA5 was re-grouped into the inferred NEX UTC day and the reconstruction re-run. **No timestamp was relabelled — only the grouping changed**, and this measures what the boundary is worth; it does not establish interval semantics.

Effect on ≥ 32 °C: **−2.7 to +0.2 d/yr**. On the annual mean: **≤ 0.07 °C**. The day convention is therefore **not** an explanation for the −112…+63 d/yr NEX gap, and it is retired as a candidate cause.

---

## 9. Conditional bias-correction trial

The trigger frozen in SPEC.md 9 **fired** (four sites beyond ±25 % relative and absolute errors past 15 d/yr, for both models). One configuration ran: additive QDM on the **derived** WBGT, 100 quantiles, day-of-year with a 31-day window — the configuration verified against CarbonPlan notebook 06 — trained on **1990–1999** and applied to held-out **2000–2010** and, unchanged, to `ssp585` **2071–2080**. Training and evaluation years are disjoint and the tool refuses an overlap. Full table: [`qdm_trial.csv`](qdm_trial.csv).

**It works at the gated threshold.** Held-out ≥ 32 °C error, days/year:

| site | ACCESS raw → corrected | MRI raw → corrected |
|---|---|---|
| Kochi | **−111.8 → −2.1** | **−84.4 → +2.1** |
| Kolkata | +58.9 → −5.8 | +52.0 → −8.9 |
| Bikaner | +39.9 → +10.2 | +36.4 → −1.6 |
| Lucknow | +25.2 → −8.8 | +8.2 → −16.8 |
| Hyderabad | −8.3 → +0.1 | −12.8 → −0.5 |
| Shimla | 0.0 → 0.0 | 0.0 → 0.0 |

**It is not uniformly good.** At ≥ 30 and ≥ 28 °C it **degrades** several pairs: Bikaner/MRI ≥30 goes +2.9 → −12.9, Lucknow/MRI ≥30 −8.9 → −18.3, Bikaner/MRI ≥28 −6.5 → −13.7. Correcting the whole distribution to fix the tail moves the middle. A correction tuned on nothing and evaluated on three thresholds improves one and worsens two at some sites.

**Numerical behaviour is clean.** Zero uncorrectable days in all 24 rows. Between 0 and 14 held-out days lie above the training range per site-model; the additive delta extends past it, and the corrected maximum can exceed the raw maximum (Kochi/MRI 39.32 → 40.86 °C) — expected for additive QDM, and reported rather than clipped.

**The warming signal survives.** Future-minus-historical quantile change, corrected versus raw: |Δ| ≤ **0.27 °C** at q50 and ≤ **0.83 °C** at q99 (the largest being Bikaner/ACCESS at q99, +4.00 → +4.83 °C). The delta construction does what it is supposed to.

**Rare events remain unvalidated.** Shimla is 0 → 0 at ≥ 30/32 both raw and corrected: the trial says nothing about whether correction would work where the reference count is near zero.

**Constraints observed.** No fit on evaluation years. No daily paired NEX weather error is treated as validation. **Two models and six sites are not national applicability**, and the older Tier-2 QDM verdict is not borrowed — this is a fresh measurement on the physical candidate.

---

## 10. Pilot specification — not launched

A bounded **staged validation** pilot, not national publication and not a production rebuild. Nothing below has been run.

### 10.1 Scope

| | |
|---|---|
| States | **Kerala**, **Rajasthan**, **Himachal Pradesh** — one humid-coastal, one hot-arid, one cool-mountain, matching the three regimes the six sites cover |
| Levels | **district** and **block** |
| Models | **ACCESS-CM2** and **MRI-ESM2-0** — the only two with matched-period evidence in §8 |
| Scenario/years | `historical` **1990–1999** (against which §8 measured the transfer) and `ssp585` **2071–2080** (verified present: 10/10 years, all six variables, both models) |
| Variables | `tas`, `tasmin`, `tasmax`, `hurs`, `rsds`, `sfcWind` — all six required, all on local disk |
| Output paths | `scratch/wbgt_outdoor_pilot/{state}/` for intermediates; `docs/diagnostics/wbgt_outdoor_pilot/` for evidence. **No write under `IRT_DATA_DIR`, `processed`, `processed_optimised`, or any shade stage.** No metric registration. |

### 10.2 Method signature

```
reference: liljegren-audited-v1:cossza-at-radiation-interval-midpoint
candidate: recon-v1:parton-logan-a1.86-b2.2:vapour_pressure:toa-shape-erbs1982
           :wind-dtr-slope0.03-min0.1-max0.8:pressure-isa-from-site-elevation
solver:    thermofeel 2.3.0 calculate_wbgt_liljegren, wind_scaling="liljegren", 10 m wind
target:    daily maximum, taken AFTER the hourly calculation; annual mean of daily maxima
```

### 10.3 What the pilot may and may not publish

| product | status | why |
|---|---|---|
| **Annual mean of daily maximum outdoor WBGT** | **pilot-ready** | daily gate 12/12 with bootstrap margin, reference-insensitive, NEX transfer within ±1.0 °C at five of six sites |
| District/block **ranking** on that mean | **pilot-ready** | r 0.865–0.979 against the reference |
| **`days_ge_28/30/32`** | **diagnostic only, must be labelled unvalidated** | count gate not passed; not resolvable at the present reference precision; NEX tail error −58 %…+54 % with site-dependent sign |
| Anything for the **cool-mountain** regime at a threshold | **not evaluated** | Shimla's count pairs are all `NOT GATED`; Himachal is in the pilot precisely to generate that missing evidence |

### 10.4 Required assets — all available

- **Elevation per admin unit.** Required, but the precision demanded is very low: at Shimla, assuming **0 m instead of the true 2276 m** moves the annual mean by only **+0.145 °C** and q99 by **+0.014 °C** (measured). A coarse per-district elevation is sufficient; a high-resolution DEM is **not** needed, and `ps` is not needed (milestone 1: ISA costs ≤ 0.022 °C).
- **Latitude and longitude** per admin unit — already in the boundary layers.
- **No wind climatology.** W1 was selected partly so that none is required. W2 would have needed a spatial hourly wind climatology over India; it is not needed and would not have helped (§3).
- **No bias-correction asset** for the level product. For threshold counts a national ERA5-derived reference on the model grid would be required, and **it has not been defined** — which is why counts are excluded from the pilot's publishable products.

### 10.5 Missing-data and calendar policy

Carried from the frozen contract, unchanged: a day containing any invalid hour becomes **NaN**, never a zero exceedance; 29 February is dropped and a year needs **365** valid daily maxima to count; annual statistics use years complete in **both** the series being compared; `360_day` models are excluded (neither pilot model is one); the NEX day boundary is treated as **inferred**, with §8.2's ≤ 2.7 d/yr sensitivity carried as a stated uncertainty.

### 10.6 Checks and acceptance conditions

1. Reproduce the six-site daily gate with the pilot's code path — **must** stay at 12/12.
2. Per state, per level: zero unexpected NaN admin units; every unit's annual mean inside the physical range spanned by the six sites' reference series (19.6–32.1 °C), with any excursion explained by elevation or coastline, not accepted silently.
3. Kerala versus Rajasthan versus Himachal ordering on the annual mean must match the Kochi/Bikaner/Shimla ordering.
4. Threshold counts computed and stored, but **not** gated and **not** published as validated.
5. Himachal's count distribution reported as the new evidence it is — the first measurement in the cool-mountain regime.
6. The 2071–2080 minus 1990–1999 change reported per model separately, never as a two-model ensemble mean.

### 10.7 Runtime basis — measured, not extrapolated

Milestone 2's own six-site run, at `--workers 1` while the national shade build owned the disk and three of its processes competed for it:

- 12 site-windows, IST day: **2,696 s** total — 328–432 s per 15-year site, 75–95 s per 10-year site.
- 6 site-windows, inferred UTC day, 10 years: 62–68 s each.
- NEX read, matched comparison, source-day sensitivity and the QDM trial: ~25 min.
- **Measured cold total ≈ 1 h 30 m.** A re-run reusing the cached per-site series took **23 min**.

That is **6 points**. A block-level pilot over three states is of order 10⁴ grid cells, so the per-cell cost — not this total — is the basis, and a pilot budget must be measured on one state before the other two are started, exactly as the shade release requires. **No national extrapolation is offered here.**

---

## 11. Artifacts, runtime and reproduction

| file | contents |
|---|---|
| `SPEC.md` | the frozen contract, written before any score |
| `candidate_scores.csv` | every candidate, oracle and comparator against `ref_audited`, by site, window and season |
| `annual_counts.csv` | annual means, counts, count errors and the count gate, over years complete in **both** series |
| `per_year_count_errors.csv` | individual complete-year count errors, min/max/mean and years outside tolerance |
| `uncertainty.csv` | year-block bootstrap, 1000 draws, seed 20260929 |
| `wind_attribution.csv` | C1/C2/W1/W2 beside oracle A5 |
| `wind_diagnostics.csv` | clipping, daily-mean conservation and solver-floor exposure per wind series |
| `reference_sensitivity.csv` | R3 versus `ref_audited`, peak timing, and both gates under both references |
| `carbonplan_alignment.csv` | the 12-element alignment table with material-difference verdicts |
| `nex_matched_comparison.csv`, `nex_sample_validity.csv` | matched-period distributions and input validity |
| `source_day_sensitivity.csv` | IST civil day versus inferred NEX UTC day |
| `qdm_trial.csv` | held-out and future-signal results for the one QDM configuration |
| `selection.json`, `run_manifest.json` | the selection record and full provenance |
| `daily_series.parquet` | every daily-maximum series, so every number above is recomputable |

Committed evidence 4.0 MB; git-ignored intermediates 14 MB under `scratch/wbgt_outdoor_selection/`.

```bash
# contract checks, offline, no cache and no NEX access
python -m tools.diagnostics.wbgt_outdoor_selection --self-test

# preflight only
python -m tools.diagnostics.wbgt_outdoor_selection --dry-run

# the full run reproduced here (~1 h 50 m cold, --workers 1)
python -m tools.diagnostics.wbgt_outdoor_selection --stage all --workers 1

# reuse the cached per-site series and recompute only the tables
python -m tools.diagnostics.wbgt_outdoor_selection --stage all --workers 1 --resume

# targeted tests
python -m pytest tests/test_wbgt_outdoor_selection.py -q
```

---

## 12. The smallest unresolved requirement

**Not another wind shape.** §3 closes that: two more shapes were tried, the true hourly wind was supplied, and the empirical climatology was applied — none passes, and the true wind is worse than the assumed one because it removes a cancellation.

The single largest reconstruction term is the **temperature/humidity** pair (`abl_A1`, +6.4…+33.7 d/yr at ≥ 32 °C), and its mechanism is measurable and specific. The candidates hold **vapour pressure constant through the day**. Measured at the hour of each day's actual temperature maximum, 1990–2004:

| site | actual e (hPa) | reconstructed e (hPa) | bias (hPa) | RH bias at the peak | `abl_A1` ≥32 error |
|---|---|---|---|---|---|
| **Bikaner** | 14.12 | 16.36 | **+2.24** | **+4.14 %** | **+31.1 d/yr** |
| Kolkata | 25.33 | 26.91 | +1.59 | +3.47 % | +8.4 |
| Hyderabad | 19.63 | 21.08 | +1.45 | +2.85 % | +12.9 |
| Lucknow | 20.32 | 21.47 | +1.16 | +2.22 % | +6.4 |
| Kochi | 27.77 | 28.92 | +1.15 | +1.82 % | +6.6 |
| **Shimla** | 11.89 | 11.71 | **−0.18** | **−0.96 %** | **0.0** |

The constant-vapour-pressure invariant is **too moist at the hot hour at every site except Shimla**, and the ordering of that bias reproduces the ordering of the count error — including Shimla, the only site with a negative moisture bias and the only site with no count error. Afternoon boundary-layer growth dries the surface layer, most strongly where it is driest to begin with.

**The smallest next experiment:** replace the constant-vapour-pressure invariant with a within-day vapour-pressure profile that declines through the afternoon, predeclared and fitted to nothing in the evaluation set — for instance a fixed fractional decline between sunrise and the temperature peak, or Kimball et al.'s dewpoint-at-`tasmin` closure, which is a published formulation. This is a **one-function change** to `reconstruct_humidity_pct`, costs no new data, and is scored on the **unchanged** frozen gates. On the cache already on disk it is roughly the cost of one window, ~35 minutes at `--workers 1`.

It should be run **together with** a decision on the reference: §4 shows that even a successful moisture fix cannot be confirmed against a reference whose own ≥ 32 °C count is uncertain by 6–9 d/yr. Either the count gate is widened to acknowledge that reference uncertainty — a change that must be declared before scoring, not after — or the count product is deferred and IRT ships the **level** product, which passes cleanly today.

**A failed candidate round does not prove daily inputs can never support threshold counts.** It shows that the four wind treatments tried do not, that the moisture invariant is the next term to fix, and that the reference must be pinned down before any count claim can be settled.
