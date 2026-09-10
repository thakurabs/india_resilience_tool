# Composite Scale Decisions and Workflow Amendment

Consolidated record of the national-absolute-scale work, 2026-09-08 to 2026-09-09.

**Status.** Part A is decided. Part C was **applied to**
[`recommended_target_workflow.md`](../recommended_target_workflow.md) on 2026-09-09; that file now
carries a revision header pointing back here. Part E lists what is still open.

**Scope.** Everything here was piloted on **Heat Risk at district and block level**. The
decisions are intended to generalize to all 13 eligible bundles, but only Heat Risk has been
measured. Sector bundles carry a change lens and will need a second ruler; that is out of scope
for this record.

---

## Part A — The scoring contract (decided)

### A1. One frozen national ruler, per metric, `cdf`

Per-(state, level, scenario, period) min–max normalization is replaced by one frozen national
ruler per metric: the **exact pooled mid-rank empirical CDF**. A value observed `c` times with
`b` observations strictly below it scores

```text
100 x (b + c/2) / n
```

The pool is every district × every slice for that metric — 784 × 7 = **5,488 values**. One ruler
serves every state, every level and every slice.

Consequences that are properties, not defects:

- The range is **open**: the lowest attainable score is `100 x (c/2)/n > 0` and the highest is
  below 100. No unit is scored a hard 0 or 100 by rank alone. Observed span 0.06 – 98.87.
- Knots span the pooled min to max by construction, so `clamped_metric_count` is structurally 0
  for districts.
- Ties take the mid-rank of the whole tied block. This matters for zero-inflated day counts —
  `txge35_extreme_heat_days` has modal value exactly 0.0.
- A score answers **"how unusual is this unit within the pooled national sample?"**, not "how far
  up India's physical range is it?".

Decided against a linear p1–p99 ruler, by the user, after the evidence in Part B was presented.
Do not re-open.

### A2. Headline = the absolute-threshold half only

The Heat Risk headline is `composite_absolute_threshold`: the 9 metrics scored against absolute
physical thresholds or levels, with weights renormalized **0.633 → 1.000** within the half.

| metric | configured weight | share of headline |
|---|---:|---:|
| `txx_annual_max` | 0.0833 | 13.16% |
| `hwa_heatwave_amplitude` | 0.0833 | 13.16% |
| `tas_annual_mean` | 0.0667 | 10.53% |
| `tasmax_summer_mean` | 0.0667 | 10.53% |
| `tas_summer_mean` | 0.0667 | 10.53% |
| `txge30_hot_days` | 0.0667 | 10.53% |
| `txge35_extreme_heat_days` | 0.0667 | 10.53% |
| `tasmin_tropical_nights_gt25` | 0.0667 | 10.53% |
| `tnx_annual_max` | 0.0667 | 10.53% |

The 5 baseline-referenced metrics — `tn90p_warm_nights_pct`, `hwfi_tmean_90p`,
`hwfi_events_tmean_90p`, `wsdi_warm_spell_days`, `tx90p_hot_days_pct` — are excluded from the
headline and become a separately named lens. They remain computed.

The composite is a weighted mean over available columns, renormalized per row; the coverage
denominator is the **configured** bundle weight, never the weight that happened to fit.

### A3. Fixed 0–100 domain, one colourbar, everywhere

The display domain is fixed 0–100 across every slice, every bundle and every administrative
level. It never rescales to the observed range of the current selection. Identical colours mean
identical scores at national zoom, at state zoom, and between one selection and the next.

There is **one colourbar**, titled level-neutrally (`Heat Risk score`, not `District Bundle
Score` / `Block Bundle Score`). Under one ruler the three legend titles in the current document
describe a distinction the mathematics no longer makes.

### A4. Ramp

NCL `WhiteBlueGreenYellowRed`, vendored at `tools/diagnostics/colormaps/`. Sampled at 101 stops,
**starting at fraction 0.045** rather than 0.0 so score 0 is a faint blue-white (`#e2f4fd`) and
never pure white. Missing data is `#d5d8dc` with its own legend swatch.

Anchors: `0 #e2f4fd · 25 #4b90cb · 50 #bcd654 · 75 #eb5329 · 100 #921519`.

Colour is a pure function of the score: `index = round(score)`, continuous, linear in score. No
binning, no smoothing, no neighbour or parent effects.

### A5. Blocks use the district-fitted ruler, unchanged

One ruler, fitted on the **district** pool, applied to blocks without refitting.

Two reasons. First, cross-level comparability: a block and its parent district holding the same
physical value get the same score and therefore the same colour. With separate rulers they would
not, and a zoom-in would show a colour jump with no physical cause. Second, pool composition is
load-bearing under a CDF — a block-pool ruler would be implicitly weighted by **administrative
subdivision density** (Uttar Pradesh has 75 districts and 822 blocks; Ladakh has 2 and 20),
which is a fact about state administration, not climate.

Cost: 0.52% of block rows clamp on at least one metric. Report it; do not redesign around it.

### A6. Never aggregate scores across levels

Because the CDF is nonlinear, `mean(CDF(x)) != CDF(mean(x))`. Therefore:

- Score each level directly from **its own physical values**.
- Do not disaggregate district scores down to blocks.
- Do not roll block scores up to districts.
- Expect the block layer to read hotter than its district in high-relief terrain. That is Jensen,
  it is real, and it belongs in the interface rather than being reconciled away.

Recommended regression guard: `|district_direct − area_weighted_block_rollup| < 10` points per
district-slice. It holds today with headroom (max observed 7.11) and will catch a future data or
ruler change that breaks nesting.

### A7. State statistic = area-weighted mean of district composite scores

`Elevated Bundle-Score Concentration (%)` is **retired completely**. The State/UT headline is the
area-weighted mean of its district composite scores.

- Weight is **area**. It is the operator already embedded in the pipeline — a district's physical
  value *is* the area-weighted mean of its blocks, exact to machine precision.
- It is computable from the shipped artifact alone (`score` + `area_m2`), which matters given D1.
- A **population-weighted** mean is retained as a second column, not a co-headline.
- Aggregate scores, do not score aggregated values. Both give nearly the same answer (median
  difference +0.00) but the value-first construction would require shipping raw metric values,
  reopening the door D1 closes.

**The State mean is a statistic, never a map encoding.** It appears in the ranking shortlist,
the answer card, the pan-India tooltip and the export. It is never painted: no State/UT polygon
is ever filled from it, at any zoom, in any view. The national map paints districts and the State
view paints blocks — those are the only two fills in the product. A State choropleth would
reintroduce a second spatial encoding competing with the district fill underneath it, which is
precisely the layered construction (`State hue + district tint`) that this amendment removes.

Label it accurately: a state at 57.8 is *the area-weighted average of its districts' national
percentiles*, not "the 58th percentile among states".

### A8. Coverage is a build gate, not a shipped field

`coverage_fraction` is not shipped. It is computed during the artifact build, and the build
**fails** if a state falls below the floor. This preserves the protection without exporting a
field that is currently constant 1.0, and it keeps the wrong computation unconstructible
downstream.

Re-verify per bundle before reusing this schema; only Heat Risk has been measured.

### A9. Map encoding and boundary grammar

```text
National view: districts painted from district composite scores
               thick state boundary, thin district boundary
    -> click a state
State view:    blocks painted from block composite scores
               thick district boundary, thin block boundary
```

Thick stroke = the unit the previous view was painting. Thin stroke = the unit this view is
painting. The same grammar repeats at both levels, which is why the legend title never changes.

These are the **only two fills in the product**. States and union territories are drawn as a
boundary at national zoom and are never filled; see A7.

Strokes are **one hue, two weights**:

```text
light: #7a8794      dark: #8e9ba8
fine   stroke-opacity .45, ~0.45px
coarse stroke-opacity .95, ~1.9px
both:  vector-effect: non-scaling-stroke
```

Not the paper colour (pale districts would lose their boundaries), not black (it reads as dirt
over the ramp's hot end). Drop the fine layer entirely below ~1 screen pixel. Selection is a
third stroke in the **accent hue**, not a fourth grey weight.

Block geometry loads only after a State/UT is selected. Median state is 144 blocks / 0.56 MB;
worst is Uttar Pradesh at 822 / 3.9 MB; 31.6 MB nationally, which is why the national view stays
at district resolution.

### A10. Range bracket, and the local-contrast view

The fixed domain means some states render nearly monochrome, correctly — Ladakh's 20 blocks span
1.8 points. Two mitigations, neither of which touches the fill:

1. A **bracket on the fixed colourbar** marking the range present in the current view, with a
   numeric readout ("observed 49.5 – 66.2, 16.7 wide").
2. A **local-contrast view**: off by default, explicitly labelled non-comparable, never the
   landing state.

Rescaling the default colourbar to the on-screen range was considered and rejected: it
reintroduces per-state min–max at the legend instead of the score. At SSP5-8.5 2040-2060 a score
of 66.2 would be deep red in Goa and yellow in Uttar Pradesh, and Ladakh's 1.8-point spread would
be painted across the full ramp.

The local-contrast view computes its domain client-side from the visible scores. That is a
**view extent**, not a stored normalization parameter — the distinction matters for D1 and must
be written down, or someone will later precompute per-state ranges into the artifact.

---

## Part B — Evidence

All figures from the national pilot run
(`tools/diagnostics/heat_risk_national_ruler_pilot.py`, outputs under
`docs/diagnostics/heat_risk_pilot/`).

### B1. Data completeness

- 36 states, 784 districts × 7 slices = 5,488 rows, 14/14 metrics fitted.
- **Zero data gaps.** 784/784 roster districts finite in every slice; 0 missing master rows, 0
  all-NaN, 0 orphans. Coverage report: 3,626 rows, min 100.0, max 100.0, standard deviation 0.0.
- 7,137 blocks across all 36 states; all 784 districts covered; median 8 blocks per district,
  range 1 to 38.
- Nothing saturates: `pct_score_ge_99 = 0.000` in every slice for both candidate rulers.

### B2. Ruler choice

- Rank agreement between `cdf` and linear is high but set membership is not: Spearman 0.893 –
  0.989, while worst-decile overlap falls 78% → 50% from baseline to SSP5-8.5 2060-2080.
- Mean |cdf − linear| is 17.5 points on the absolute half against 8.7 on the baseline half. The
  full composite's 8.8 is partial cancellation, so composite figures understate how far the new
  headline moves.
- Worked example — **Bijnor, Uttar Pradesh, SSP2-4.5 2020-2040**: `tas_annual_mean` 25.10 °C
  scores **75.6 linear / 21.0 CDF**. Largest single disagreement nationally. Both true, different
  questions.
- The composite there is 35.42 → ramp stop 35 → `#49a773`.

### B3. Blocks nest exactly at the physical layer

District value **is** the area-weighted mean of its blocks — verified on `tas_annual_mean` and
`txge35_extreme_heat_days`: p95 |difference| 0.0000, max 0.087 °C and 0.0001 days.

### B4. Score-layer nesting (Jensen)

Area-weighted mean of block composites vs the district composite computed directly:

```text
median gap  -0.02      p95 |gap|  1.01      max |gap|  7.11
|gap| > 5 pts:  5 of 5,488 (0.09%)      |gap| > 10 pts:  0
mean -0.094  (one-signed: the block roll-up reads hotter)
```

Worst cases are all high-relief: Nainital (29.22 vs 36.32), Kangra (17.35 vs 22.80), Alluri
Sitharama Raju (36.34 vs 41.66).

### B5. What block resolution buys

Within-district spread of the **block composite**: median 7.2 points, p90 21.6, max 69.2.

Share of within-state block variance lying *inside* districts — the part a district-resolution
map cannot show — at SSP5-8.5 2040-2060. National median **18.3%**:

| state | blocks | districts | within-district share |
|---|---:|---:|---:|
| Lakshadweep | 10 | 1 | 100.0% |
| Ladakh | 20 | 2 | 90.8% |
| Goa | 12 | 2 | 74.5% |
| Andaman & Nicobar | 9 | 3 | 68.6% |
| Sikkim | 26 | 6 | 48.2% |
| Kerala | 152 | 14 | 47.5% |
| … | | | |
| West Bengal | 344 | 23 | 5.6% |
| Bihar | 534 | 38 | 4.3% |
| Uttar Pradesh | 822 | 75 | 3.8% |

The argument for block fill is **not** resolution for its own sake. Blocks buy the most where the
district roster is coarse, not where the terrain is rough — Uttarakhand is only 10.4% because its
13 districts already separate hill from Terai, while Kerala and Goa are high because 14 and 2
districts cannot resolve a coast-to-Ghats gradient. Block fill **equalizes effective resolution
across states**, which a district-resolution state map does not.

### B6. Why concentration was retired

The statistic saturates at both ends of the slice grid:

| slice | concentration median | area-weighted mean (min – max) |
|---|---:|---|
| historical 1990-2010 | **0.0** | 0.4 – 58.0 |
| ssp245 2020-2040 | 8.3 | 0.5 – 68.8 |
| ssp245 2040-2060 | 52.1 | 0.7 – 77.8 |
| ssp585 2040-2060 | 68.1 | 0.9 – 83.4 |
| ssp585 2060-2080 | **83.9** | 1.3 – 93.9 |

At baseline more than half of all States/UTs sit at exactly 0.0% — the slice users open first
tells them nothing about most of India.

The tie structure is worse. At SSP5-8.5 2040-2060, **14 states share concentration 0.0 and
therefore rank 23**, while Goa, Delhi and Puducherry all tie at rank 1 with 100.0%:

| state | concentration | area-wtd mean | rank old → new |
|---|---:|---:|---|
| Goa | 100.0 | 57.8 | 1 → 18 |
| Delhi | 100.0 | 62.8 | 1 → 12 |
| Puducherry | 100.0 | 66.5 | 1 → 9 |
| Ladakh | 0.0 | 0.9 | 23 → 36 |
| Sikkim | 0.0 | 1.6 | 23 → 35 |
| Himachal Pradesh | 0.0 | 6.9 | 23 → 34 |

Goa ranking first nationally on the strength of two districts is exactly the artifact the
small-cohort rule was written to suppress; the mean fixes it at source. Spearman between the two
statistics is 0.876, so this is a real change to the shortlist, not a relabelling.

### B7. Area vs population weighting

Measured across all 252 state-slices:

- **Ranking barely moves.** Maximum rank change 5 places in every slice; the top-10 shortlist is
  identical in 6 of 7 slices (8/10 at end-century).
- **Bands disagree in 19 of 252 state-slices (7.5%)**, and only one is substantive:
  **Uttarakhand**, Very Low area-weighted against Low population-weighted, in 4 of 7 slices
  (12.61 vs 25.58 at the default). Every other disagreement is a state sitting within a few
  points of a band cut — Andhra Pradesh 59.1/60.1, Jharkhand 59.1/60.0, Tamil Nadu 58.6/60.2 —
  which is a fact about the band cuts, not the weighting.
- Largest deltas at the default slice: Uttarakhand −12.96, Himachal Pradesh −8.03, Arunachal
  Pradesh −5.61, Maharashtra +5.76, Karnataka +4.19, Rajasthan +3.43. The people-hotter side
  reaches −13 while the land-hotter side tops out near +6.
- `jammu kashmir|mirpur` has no population row: it contributes to J&K's area-weighted mean and
  drops out of its population-weighted one. The two J&K numbers are computed over different
  district sets.

### B8. A methodological correction, recorded deliberately

Scoring a state's area-weighted **physical** values against the district-fitted CDF was described
in discussion as a category error that would compress states toward the middle. Measured, it does
not: median difference from the mean-of-scores is **+0.00**, range −3.65 to +1.60. It is a viable
construction. The mean of district scores is preferred for the artifact reason in A7, not because
the alternative is wrong.

### B9. The colour ramp was faking a problem

`YlOrRd` starts near-white and squashed the linear ruler's real 40–90 range into a narrow orange
band, which read as "washed out" and was briefly taken as evidence about the ruler. On
`WhiteBlueGreenYellowRed` the same data resolves cleanly. **Never read "washed out" as evidence
about a ruler without first checking the ramp.**

---

## Part C — Amendment to `recommended_target_workflow.md`

**Applied 2026-09-09** (257 insertions, 175 deletions). Line references below are against the
**pre-amendment** file and are historical: the numbering has since shifted. Read this part as the
migration record for what changed and why, not as an index into the current document. The
District-view contract (then lines 466–504) was deliberately left in place and is now marked
`Unresolved` there.

### C1. §1 opening list and the concentration definition — lines 23–120

Replace the `Elevated Bundle-Score Concentration (%)` legend bullet (line 31) with
`a District Bundle Score legend on a fixed 0-100 scale`, and the hotspot bullet (line 32) with
`the States and union territories with the highest area-weighted mean district scores`.

Delete the concentration formula and its surrounding rationale (lines 35–58) and replace with:

> The national map paints **districts** directly from their bundle composite scores on the frozen
> `0-100` national scale. Because every score comes from one frozen national ruler rather than a
> per-State normalization, district scores are directly comparable between States, and a
> pan-India district map is a valid absolute comparison rather than a screening proxy.
>
> The State/UT headline statistic is the **area-weighted mean of its valid district composite
> scores**. A population-weighted mean is retained as a secondary field. Neither is a percentile
> among States: the number is the area-weighted average of that State's districts' national
> percentiles, and must be labelled as such.
>
> The State statistic is used for ranking, the answer card, the tooltip and exports. It is **not
> a map encoding**: no State/UT polygon is filled from it at any zoom. The national map paints
> districts; the State view paints blocks.
>
> `Elevated Bundle-Score Concentration (%)`, the fixed `>= 50` elevated-score threshold, and the
> prohibition on a State mean as the national value are all withdrawn. The threshold saturated at
> both ends of the slice grid and tied large groups of States at identical values.

Replace lines 106–115 (the "national statistic answers" block and the interstate caveat). The
statistic now *does* answer an absolute comparison question; the caveat that survives is about
hazard scope, not normalization:

> The national statistic answers: within this State/UT, what is the area-weighted average bundle
> composite score of its districts, on the frozen national scale?
>
> It is hazard-only. It does not include exposure, vulnerability, or resilience, and it is not
> comparable across bundles.

Replace lines 117–120. District and block results are now explicitly on one scale:

> District and block scores are produced on the same frozen national ruler and are directly
> comparable. The national overview uses districts; the State view uses blocks. Neither is
> derived from the other — each level is scored from its own physical values.

### C2. Coverage and ranking eligibility — lines 122–186

Keep the section. Three edits:

- Delete the three-branch contract at lines 130–145 that gates *the concentration percentage*,
  and re-express the same rules as gating **the State mean**.
- `coverage_fraction` moves from a shipped field to a **build gate** (A8). The build fails below
  the floor rather than the interface suppressing a value.
- Delete the `District view / Rank individual Block Bundle Scores` tier at lines 172–175 and its
  denominator sentence. Ranking is two-tier: States nationally, districts within a State. Block
  cohorts run 1 to 38 per district, far below the ten-unit minimum. Blocks are painted, not
  ranked.

Keep line 180's prohibition on State-wide and national block ranks, but restate the reason: block
scores *are* now nationally comparable; the objection is cohort size and administrative
unevenness, not comparability.

Line 202's `Coverage percentages: 1 decimal place` is retired with the field.

### C3. Denominator and provenance — lines 208–281

Keep in full. Two additions to the provenance block at lines 237–247:

```text
ruler_id            which frozen CDF support produced these scores
colour_scale_id     which palette and domain the scores are rendered against
data_snapshot_hash  the input snapshot the ruler was fitted over
```

A score without a `ruler_id` is unfalsifiable — a correctly rendered new ruler cannot be
distinguished from a wrongly rendered old one.

Line 262 (`threshold`), 265 (`coverage_fraction`), 266–267 (`n_ge_threshold`,
`pct_ge_threshold`) leave the artifact field list. Add `state_mean_area_weighted` and
`state_mean_population_weighted`.

### C4. National map visual encoding — lines 308–367

This section is replaced wholesale. The five-anchor green→red palette, the fixed `0-100%`
concentration scale, and the `district tint strength = 30% + 70% x (score/100)` construction all
go.

> The pan-India map paints each district directly from its bundle composite score on the frozen
> `0-100` domain, through the vendored `WhiteBlueGreenYellowRed` table sampled at 101 stops
> starting at fraction 0.045 so no valid score renders as pure white. There is one continuous
> colourbar with fixed numeric ticks, titled `<Bundle> score`. The domain never rescales — not
> when the bundle, scenario or period changes, and not when a State is selected.
>
> Boundary weight carries the administrative level. At national zoom the State/UT boundary is the
> coarse stroke and the district boundary the fine stroke; at State zoom the district boundary is
> coarse and the block boundary fine. Both strokes use one hue — `#7a8794` light, `#8e9ba8` dark
> — differing only in width and opacity, with `vector-effect: non-scaling-stroke`. The fine layer
> is dropped below approximately one screen pixel. Selection is a third stroke in the accent hue.
>
> A bracket on the colourbar marks the score range present in the current view, with a numeric
> readout beside it. A `Local contrast` view may rescale the domain to the visible extent; it is
> off by default, explicitly labelled as not comparable across selections, and is never the
> landing state.
>
> State and union-territory polygons are **never filled**. At national zoom they contribute the
> coarse boundary stroke only; their headline statistic lives in the ranking, tooltip and answer
> card. There is one fill per view and one colourbar for both.
>
> `#d5d8dc` identifies missing composite data and carries its own legend swatch.

Lines 355–367 (no-data treatment, hover/selection precedence, accessibility, colour-not-sole-
carrier) are retained unchanged.

### C5. National map interaction — lines 369–390

The pan-India tooltip (lines 371–382) shows: State/UT name, area-weighted mean district score,
population-weighted mean, national rank and the number of rank-eligible States when eligible,
and any quality flag. Remove the concentration, threshold-count and coverage lines. Line 381's
prohibition on district-level tooltip content at national zoom is **retained** — districts are
painted but not individually inspectable until a State is selected.

Replace lines 384–390:

> Clicking anywhere within a State selects and zooms to it, loads that State's block geometry and
> attributes, and repaints the map from **block** composite scores on the same colourbar. The
> district boundary becomes the coarse stroke and the block boundary the fine stroke. The
> colourbar, its title and its domain do not change.
>
> A `District fill` toggle is available at State view for users who want the map to match the
> district ranking exactly; block fill is the default.

Expect the visual change on click to vary enormously by State — imperceptible in Uttar Pradesh
(3.8% within-district variance), transformative in Goa (74.5%). Interface copy must promise the
same *scale*, never the same *map*.

### C6. Visual hierarchy and layout contracts — lines 402–504

Replace the hierarchy block at lines 402–411:

```text
National view: continuous district composite-score colour
               thick State boundary, thin district boundary
    -> select state
State view:    continuous block composite-score colour
               thick district boundary, thin block boundary
               + five-band district distribution and district-level interaction
```

Line 415's `district-view legend title should be Block Bundle Score` is deleted — there is one
level-neutral title.

Lines 419–437 (the five-band distribution and its interaction) are **retained**, with the
`Elevated Bundle-Score Concentration / 11 of 33 valid districts >= 50` headline example replaced
by the State mean, and line 436's instruction about the Moderate bar and the `>= 50` threshold
deleted along with the threshold.

State-view layout contract (lines 439–460): the Headline block becomes

```text
Headline
    Area-weighted mean district score + five-band classification
    Population-weighted mean (secondary)
    National rank or quality explanation
```

and the Map block becomes `Continuous block composite-score colour + district interaction`.

The District-view layout contract (lines 466–491) and the block-inspection paragraph (lines
497–504) are **withdrawn — decided 2026-09-09**. With blocks painted at State view there is no
finer map to introduce: a District view would repaint the same blocks, on the same ruler, against
the same colourbar, at a smaller extent. Its `top-10 Blocks ranked within the District` clause also
contradicted the two-tier ranking rule, and a five-band block histogram is not viable over a median
of 8 blocks. A district click is an inspection state within State view — breadcrumb
`India > State/UT`, panel showing the district score, band, rank within the State/UT, drivers, and
its blocks' min–max range and count. District and block are independent inspection targets with an
explicit precedence rule; neither adds a breadcrumb level.

### C7. Production migration contract — lines 772–811

Line 775's "must not silently preserve State mean as the national value" is **withdrawn** — the
State mean is now the national value. What must not be preserved is per-State min–max
normalization.

The artifact field list at lines 780–799 drops `threshold`, `coverage_fraction`,
`n_ge_threshold`, `pct_ge_threshold`; adds `state_mean_area_weighted`,
`state_mean_population_weighted`, `ruler_id`, `colour_scale_id`, `data_snapshot_hash`.

Lines 802–805 are replaced by the two-level encoding in C4/C6.

### C8. Acceptance-test contract — lines 813–859

- Line 819 inverts: national State ranking now uses the **area-weighted State mean**, not
  concentration.
- Line 822–823, 827: coverage assertions move to build-gate assertions.
- Line 831: the `>= 50` threshold assertion is deleted; five-band assignment from full precision
  is retained.
- Line 846–847: legend-title assertions become "one level-neutral title at both views" and "the
  same score maps to the same colour at national and State zoom, and at district and block
  level".

Add:

- a district and a block holding the same physical value receive the same score;
- no State/UT polygon carries a score-derived fill at any zoom;
- the domain never rescales outside the labelled local-contrast view;
- `|district_direct − area_weighted_block_rollup| < 10` per district-slice;
- the canary geographies in D3 render as expected.

### C9. Core workflow principles — lines 861–884

- Line 872 loses "or concentration" and "with view-specific legend titles".
- Line 878 ("National screening results are never described as absolute interstate climate-risk
  scores") is **withdrawn** — under a frozen national ruler they are absolute interstate
  comparisons. Replace with: *national results are absolute interstate hazard comparisons on one
  frozen scale, and remain hazard-only and not comparable across bundles.*
- Line 879 ("Coverage validity and denominator-based ranking eligibility remain explicit and
  separate") becomes a build-gate principle.
- Line 883 gains: block **attributes** load with State selection, not after a further click.
- Add a principle: there is exactly one score-derived fill per view — districts nationally,
  blocks within a State — and aggregate statistics are never rendered as a choropleth.

---

## Part D — Vendor handoff contract

The UI/UX vendor building `irt.resilience.org.in` receives computed values, not methodology.

### D1. Ship only the right number, so the wrong one is unconstructible

Pre-computing the score is necessary but not sufficient. If the artifact also carries raw metric
values, per-State minima and maxima, or the ruler's knots, someone downstream will eventually
normalize with them — not maliciously, but because a map looks flat in Ladakh and they "fix" it.
The defence is that the inputs to that computation are simply absent from the file.

**Per-row artifact carries:** stable admin ID, `district_key` / `block_key`, `bundle_id`,
`scenario`, `period`, `admin_level`, `score`, quality flags.

**It does not carry:** raw metric values, normalization parameters, CDF support, per-State
extents. Those live in the build repo.

Store full `float64` and round only at display. Do not round in the artifact — the ranking
contract gives tied ranks to exact full-precision equality, and rounding manufactures ties.

### D2. Ship the ramp as data, not as prose

A score with no palette leaves a "pick a colour scale" step, and that step is where per-State
rescaling gets reinvented. One small versioned `colour_scale.json`:

```json
{
  "colour_scale_id": "whbgyr-101-floor045-v1",
  "stops": ["#e2f4fd", "...101 hex values..."],
  "domain_min": 0,
  "domain_max": 100,
  "missing": "#d5d8dc",
  "rescale": "forbidden"
}
```

Do **not** store a hex colour per row instead of the score. The vendor needs the number for
tooltips, sortable tables, the band distribution, ranking and accessibility — colour cannot be
the sole carrier of score. Storing colour would force them to reverse-engineer the number they
were meant to be given, and would make a palette tweak a full data republish.

### D3. Canaries, because the failure mode is silent

A vendor who rescales per State produces a map that looks entirely plausible. What catches it is
a geography whose *correct* rendering is nearly monochrome:

> **Ladakh.** Its 20 blocks span under 2 points at SSP5-8.5 2040-2060. Rendered correctly, Ladakh
> is a single flat pale-blue field. **If the deployed map shows Ladakh in more than one visually
> distinct colour, the ruler has been rescaled.**

Second canary in a different part of the country: **Delhi**, 2.6 points across 12 blocks.

Pair these with a golden file — a few dozen named units × slice with expected score and expected
hex — as the machine-checkable version.

### D4. Timing

**Nothing may be handed over until the data is frozen.** P-10 is unresolved: the Andhra Pradesh
republish, the Lakshadweep sub-cell fill and the groundwater rewire are all in flight. Scores
shipped now are scores reissued later, against which the vendor will have built caching, golden
tests and screenshots. The ruler must be frozen over a stable snapshot first, and the artifact
must carry `data_snapshot_hash` so a mismatch is loud rather than silent.

---

## Part D2 — Shipped 2026-09-10 (CHG-0367a..f, CHG-0385a, CHG-0388)

Section A of this document is implemented and published. `composite_heat_risk` in
`processed_optimised/` is scored against `composite_heat_risk_cdf_v1`: the exact pooled mid-rank
CDF over 784 districts x 7 slices, headline = the 9 absolute-threshold metrics renormalized
0.6333 -> 1.0, coverage gate 0.70, blocks scored against the district-fitted ruler unchanged (A5).

Verification of the published bundle:

| check | result |
|---|---|
| Published district scores vs `docs/diagnostics/heat_risk_pilot/district_scores.csv` (`ruler == cdf`, `composite_absolute_threshold`) | max abs diff **0.0** over 4,704 rows |
| Districts / blocks published | 784 / 7,137, **zero nulls** across all 6 published slices |
| Score dtype in the published master | **float64** (CHG-0385a; was float32) |
| Jensen guard, area-weighted block rollup vs district's own score | max **4.76**, mean 0.23, none above 10 |
| Ladakh canary (20 blocks, SSP5-8.5 2040-2060) | span **1.79** points |
| Delhi canary (12 blocks) | span **2.56** points |
| `parity_report.json` issue count | **0** |
| Roster reconciliation at fit | clean — no district missing a master, no master row off-roster |

The State headline is now nationally comparable: at SSP5-8.5 2040-2060 the area-weighted State means
run Telangana 83.4 / Gujarat 76.6 / Rajasthan 76.1 at the top and Ladakh 0.9 / Sikkim 1.6 /
Himachal 6.9 at the bottom. Under per-state min-max the same file made West Bengal rank first.

Note the published grid is **6 slices, not 7**: the production composite path has never emitted
`historical/1990-2010` for this bundle (`SUPPORTED_SCENARIOS` carries no `historical`). The ruler is
still fitted over all 7 — the baseline slice is in the pool, it is simply not published. That is
pre-existing behaviour, unchanged here, and it is why P-03's "the baseline map will look benign"
cannot yet be seen in the bundle at all.

---

## Part E — Open

| ref | item | why it matters |
|---|---|---|
| P-13 | Five-band cuts are near-tautological under a CDF: `>= 80` reads as "worse than 80% of pooled observations". Either document that plainly or set band cuts from physical values. | Undecided. B7 shows band cuts already dominating the area-vs-population comparison. |
| P-14 | `75` has changed meaning without the widget changing. Needs UI copy, not a docs change. | Undecided. |
| P-03 | The pool is 86% future-weighted; the absolute-half baseline median is 38.8 against 78.8 at SSP5-8.5 2060-2080. The baseline map will look benign. | Presentation treatment undecided. |
| — | District view: inspection state within State view, or a level of its own. | **Resolved 2026-09-09** — inspection state. Workflow doc amended (CHG-0379). |
| P-04 | Freeze the 7-slice grid with its own version; the validator must refuse off-grid slices. | **Closed 2026-09-10** (CHG-0367c/d). The grid is written into `ruler.json`; `FrozenRulerSet.validate_slice` raises on an off-grid `(scenario, period)` before any row is scored. |
| P-10 | Data still regenerating. Blocks D4. | Blocking. |
| P-11 | A frozen CDF must carry the full 5,488-value support per metric, or accept the 21-knot grid's up-to-4.2-point error. | **Closed 2026-09-10** — full support. The runtime ships only scores, so support size costs the vendor nothing; the committed artifact is 538 KB (`cdf_support.parquet`, 49,363 knots over 9 metrics). |
| P-16 | Freezing bakes in the 1990–2010 vs 1981–2010 code-baseline gap and the `hwa_heatwave_amplitude` hybrid. | Known, accepted for now. |
| — | `admin_roster_version` has not been assigned. Counts alone are not a version. | Blocks release. |
| — | The 12 non-Heat-Risk bundles are unmeasured. Sector bundles need a second ruler for the change lens. | Out of pilot scope. |

---

## Provenance

| tool | produces |
|---|---|
| `tools/diagnostics/heat_risk_national_ruler_pilot.py` | the national run, the frozen support, all evidence tables |
| `tools/pipeline/fit_frozen_ruler.py` | the **published** frozen ruler artifact (`config/frozen_rulers/composite_heat_risk/cdf_v1/`) and `colour_scale.json` |
| `india_resilience_tool/analysis/frozen_rulers.py` | the production ruler core: exact mid-rank fit, apply/clamp, save/load |
| `tools/diagnostics/build_heat_risk_frozen_map.py` | national district map, frozen settings |
| `tools/diagnostics/build_resolution_comparison.py` | district-fill vs block-fill, UP / Kerala / Goa |
| `tools/diagnostics/build_state_weighting_comparison.py` | area vs population State means |

The State-weighting page paints State/UT polygons because that was the fastest way to compare two
candidate weightings side by side. It is a **decision aid, not a product mock** — per A7 the
product never fills a State polygon.

Pitfall references `P-xx` are to [`docs/national_absolute_scale_pitfalls.md`](national_absolute_scale_pitfalls.md).
