# National Absolute Scale — Pitfalls Register

Status: **open register**, maintained alongside the national-absolute-scale work
(CHG-0330..0348). Nothing in this document is a decision; it is the list of ways
the change can go wrong, what each one would look like if it happened, and what
resolves it.

Context: today bundle composites are normalized by **per-(state, level, scenario,
period) min–max** (`india_resilience_tool/compute/composite_metrics.py:466-483`
→ `analysis/bundle_scores.py:49`). That guarantees a 0 and a 100 inside every
state *and* inside every scenario-period slice, so scores are comparable neither
across space nor across time. The proposal replaces it with one frozen national
ruler per (metric, quantity).

Severity key:
- **BLOCKER** — can invalidate the result; must be resolved before any ruler is frozen.
- **MAJOR** — will bite during implementation; needs a designed answer, not a patch.
- **NOTE** — interpretation, consistency, or communication risk.

---

## Tier 1 — BLOCKER

### P-01 A CDF ruler discards magnitude and manufactures contrast

Min–max preserves value ratios. An empirical-CDF ruler preserves only rank and
then *uniformizes* it. If 90% of India sits inside a 1.5 °C band on
`tas_annual_mean` and 10% is genuinely extreme, the CDF stretches that tight 90%
across scores 0–90.

**Failure signature:** the national map looks beautifully differentiated while the
underlying physical spread is negligible. Invisible on a choropleth.

**Resolution:** a CDF is not the only frozen ruler. A frozen **linear p1→p99**
scale (pooled full-span, clipped) is equally state-independent and
time-independent but keeps magnitude. Both are built in the Heat Risk pilot and
compared before anything is frozen.

### P-02 Averaging uniformized marginals compresses the composite toward 50

Heat Risk is a weighted mean of 14 metric scores. If each marginal is uniform on
[0,100] and the metrics are imperfectly correlated, the mean concentrates. Heat
metrics are strongly correlated so it will not fully collapse, but the realized
composite range may be far narrower than 0–100.

**Failure signature:** with CHG-0341's fixed 0–100 map domain, a washed-out
national map — the original complaint, relocated.

**Resolution:** the pilot reports realized composite min/IQR/max per slice. If
narrow, the options are (a) set the map domain to the ruler's realized composite
range, or (b) apply a final composite-level ruler — which re-imports relative
scaling at bundle level and must be an explicit documented decision, never a
rendering tweak.

### P-03 Spatial contrast and temporal contrast trade off

Pooling baseline + 6 future slices puts baseline districts in the bottom of the
ruler by construction. Warming reads clearly across periods; the baseline map
flattens.

**Failure signature:** users read a flat baseline map as "no data" or "broken".

**Resolution:** product decision, not a bug. Decide and document whether the
baseline view keeps its own presentation treatment.

### P-04 The pool is not a sample; its composition is load-bearing

The 7-slice pool is 1 baseline slice vs 6 future slices (86% future-weighted),
and `ssp245`/`ssp585` are two renderings of the same districts, not independent
draws — every district is counted twice per period.

**Failure signature:** adding 2081-2100 or SSP1-2.6 later silently changes every
previously published score.

**Resolution:** freeze the scenario/period grid **as part of the scale
definition**, with its own version. The `config/absolute_scales.py` validator
(CHG-0348) must refuse to score a slice that is not in the grid the ruler was
fitted on.

---

## Tier 2 — MAJOR

### P-05 Zero-inflation and tied quantile knots

`txge35_extreme_heat_days`, `txge30_hot_days`, `tasmin_tropical_nights_gt25/28`,
`csdi_cold_spell_days`, `cwd_consecutive_wet_days`, `tn10p` all carry a large
mass at exactly 0 across hill and northern districts. With 21 knots this produces
identical knot values → zero-width interpolation segments → divide-by-zero or an
arbitrary tie-break, and a large block of districts on one score.
`normalize_metric_series` only handles the fully degenerate `hi == lo` case.

**Resolution:** the builder must collapse duplicate knots explicitly and assign
the **mid-rank** score to a tied block. The pilot reports duplicate-knot count and
modal mass per metric.

### P-06 p100 knots are single-outlier hostages

Anchoring the ruler top on the pooled max lets one aberrant district — or one bad
IDW-filled cell from the sub-cell climate fill — set the national scale
permanently.

**Resolution:** anchor on p1/p99 with clamping; log clamped counts per slice as a
standing QA signal.

### P-07 Physically bounded metrics get a fake ceiling

`tx90p_hot_days_pct` / `tn90p_warm_nights_pct` are percentages bounded at 100;
day counts bound at 365. Under SSP5-8.5 late century these saturate near their
physical bound across much of India. The ruler's 100 and the metric's bound
coincide only by accident.

**Failure signature:** the map stops moving between 2040-2060 and 2060-2080 and
looks like a bug. (`_normalize_with_anchor:280` already clips to [0,100]; the
frozen ruler will clip far more often.)

**Resolution:** track fraction of districts at score ≥99 and ≤1 per slice as a
first-class diagnostic, not an afterthought.

### P-08 Per-row weight renormalization turns coverage gaps into bias

`compute_bundle_score_frame:141-144` renormalizes weights over whatever metrics
are present, with **no minimum-count gate** (only `baseline_anchored` has
`min_anchored_components`). Per-state min–max masks this today: a district scored
on 3 of 14 metrics still lands somewhere plausible inside its state's range.
Under a frozen national ruler, a district missing Heat Risk's 0.633
absolute-threshold half is scored purely on its 0.367 baseline-referenced half —
a different quantity, presented identically.

**Resolution:** a **minimum-weight-coverage gate** (fraction of weight, not metric
count) is a prerequisite to CHG-0332/0333, not a follow-up. See CHG-0347. The
pilot reports coverage per district and a gated-vs-ungated composite.

### P-09 The slice grid varies by state

`_intersect_available_pairs` (`composite_metrics.py:171`) intersects
scenario-period availability *within a state*, so the panel is uneven: a state
missing a pair contributes fewer rows to the pool and has no score for that
slice.

A first pass measured this against "what exists": states were discovered from the
component roots, and coverage was aggregated nationally with no state dimension,
so a state or district absent from every master was invisible and a national
percentage could not say *which* state lacked a slice.

**Resolution:** the expected universe is the **canonical district roster**, read
from `processed_optimised/geometry/admin/district/` — every roster district x all
7 slices, so absence appears as an NaN row rather than a missing one. Coverage is
reported at state x metric x slice with national totals summed from those same
rows, alongside a roster reconciliation naming roster districts with no master
row, roster districts whose master rows carry no finite value in any slice, and
master keys absent from the roster. Delivered by the pilot (CHG-0354). The first
two are separated by CHG-0358 because they are different faults: no master row is
a roster/boundary gap, while master rows with no finite value is a regeneration
gap (P-10) and sends the reader somewhere else entirely.
Still open, and not a pilot question: the explicit rule for a state whose grid is
a strict subset.

### P-10 Freezing a ruler over data that is still being regenerated

The AP republish (CHG-0304), the Lakshadweep sub-cell IDW fill (CHG-0305..0309)
and the groundwater CGWB rewire all change underlying values. A ruler frozen
before them silently mis-scores after them.

**Resolution:** the scale artifact carries a **data-snapshot hash**; the parity
audit fails when the data moves without a ruler version bump (CHG-0348).
Otherwise this is exactly the silent methodology drift the guardrails exist to
prevent.

### P-11 A ruler change is a full national recompute

Composites persist per state and roll into `processed_optimised`. Changing the
ruler invalidates every composite master for every state and bundle. The two
coexisting score fields (`{slug}__…` and `{slug}_abs__…`) also roughly double
composite column count across 15 bundles × slices — bundle size, the parity
audit, and the vendor UI (whose ranking endpoint already returns HTTP 500) all
feel it.

**Resolution:** decide up front whether `_abs` is a permanent parallel field or a
migration with a deletion date.

---

## Tier 3 — NOTE

### P-12 Two different weightings in one pipeline

The ruler treats every district as one sample regardless of size; state
aggregation is area-weighted (decided 2026-09-08). Not wrong, but area-weighting
a **hazard** score means uninhabited terrain drives the state headline (Ladakh,
Kutch, Barmer). Defensible for hazard; indefensible if the state number is read
as risk-to-people. Document beside the cross-bundle incomparability caveat.

### P-13 Five-band cuts become near-tautological under a CDF

With a CDF ruler, "Extreme = ≥80" means "worse than 80% of pooled observations".
Because the pool is 86% future-weighted this is not literally "top 20% of
districts", but close enough that readers will treat the bands as definitional
rather than physical. A linear ruler does not have this problem.

### P-14 Score semantics change and nobody is told

Today 75 means "three quarters across this state's spread, in this slice".
Afterwards it means "worse than 75% of all district-scenario-period observations
nationally" (CDF) or "75% of the way up India's full-span physical range"
(linear). Same widget, same ramp, different noun. Needs UI copy, not only a docs
change.

### P-15 Baseline rows sit at zero on the change ruler

(extends CHG-0345) Deltas are zero for every baseline row and the pooled delta
distribution is mostly positive, so baseline scores near 0 on the sector `change`
lens by construction. Correct, but expect it and state it.

### P-16 The ruler permanently bakes in freeze-day inconsistencies

The unresolved 1990–2010 vs 1981–2010 code-baseline gap, and the
`hwa_heatwave_amplitude` hybrid (spell selection baseline-relative, value emitted
as absolute peak °C). Freezing does not cause these, but it makes them expensive
to correct later — a fix then requires a ruler version bump and a full recompute
(P-11).

---

## Explicitly not a risk

**District roster churn.** A value-based ruler is robust to LGD splits, merges and
renames in a way a rank-based national ranking would not be. New districts are
simply scored.

---

## Pilot coverage

`tools/diagnostics/heat_risk_national_ruler_pilot.py` (CHG-0346) is scoped to
answer P-01, P-02, P-03, P-05, P-06, P-07, P-08, P-09 and P-12 with data before
any ruler is frozen.

P-09 moved into scope with CHG-0354: the pilot now measures coverage against the
canonical roster rather than against whatever the masters contain, which is the
only way absence can be observed at all. What remains outside the pilot is the
*policy* for a state whose slice grid is a strict subset.

P-12's evidence is only as good as the area column behind it. CHG-0357 requires a
finite `area_m2 > 0` for **every** retained roster district: a district with a
missing, zero or non-finite area is dropped from the weighted mean without
appearing in any output, so a partial area set produces a wrong number rather
than a visibly degraded one.

P-05 likewise moved from "reported" to "resolved" with CHG-0353: the `cdf` ruler
is the exact pooled mid-rank CDF, and the compact 21-knot grid it replaces is
retained only as an approximation whose max/mean score error is measured per
metric — which is also the input to P-11's artifact-size question.

P-04, P-10 and P-11 remain design/process items resolved in
`config/absolute_scales.py` and the audit, not by the pilot.
