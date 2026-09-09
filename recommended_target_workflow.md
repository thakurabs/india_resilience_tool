# Recommended Target Workflow

> **Revision — 2026-09-09.** Section 1 has been amended for the frozen national scoring scale.
> `Elevated Bundle-Score Concentration (%)` and its `>= 50` threshold are withdrawn; the national
> map paints districts directly from their composite scores, the State view paints blocks, and
> the State/UT headline is the area-weighted mean of its district scores. Three earlier rules
> reverse: a State mean is now permitted as the national value, national results *are* absolute
> interstate comparisons, and `coverage_fraction` moves from a shipped field to a build gate.
> The District-view contract is unresolved and is marked in place.
>
> Rationale, evidence and the vendor handoff contract:
> [`docs/composite_scale_decisions.md`](docs/composite_scale_decisions.md).

## Working direction

The recommended direction is a single progressive-disclosure workflow:

```text
Overview by default → Understand the result → Compare if needed → Open Detailed Analysis
```

The Overview provides a low-burden path to a useful answer. Detailed Analysis extends the same
analysis for users who want to inspect metrics, scenarios, trends, methods, or other advanced
controls. They should behave as two levels of one continuous workflow, not as independent
products with duplicated state.

In user-facing language, the preferred labels are:

- `Overview`, rather than `Glance View`; and
- `Detailed Analysis`, rather than `Deep Dive`.

The Glance objective is retained even if Glance is not presented as a separate named mode.

## 1. Open with an immediate national overview

The first screen should already contain a useful result:

- an India map;
- a clearly labelled default bundle;
- a clearly labelled default scenario;
- a clearly labelled default period;
- a continuous bundle-score legend on the fixed `0-100` national scale;
- the eligible states and union territories with the highest area-weighted mean district
  scores; and
- a concise explanation of what the score represents and what it excludes.

The national map paints **districts** directly from their bundle composite scores on the frozen
`0-100` national scale. Because every score comes from one frozen national ruler rather than a
per-State normalization, district scores are directly comparable between States, and a pan-India
district map is a valid absolute comparison rather than a screening proxy.

The State/UT headline statistic is the **area-weighted mean of its valid district composite
scores**. A population-weighted mean is retained as a secondary field. Neither is a percentile
among States: the number is the area-weighted average of that State's districts' national
percentiles, and must be labelled as such.

The State statistic is used for ranking, the answer card, the tooltip and exports. It is **not a
map encoding**: no State/UT polygon is filled from it at any zoom. The national map paints
districts; the State view paints blocks. Those are the only two score-derived fills in the
product.

`Elevated Bundle-Score Concentration (%)`, the fixed `>= 50` elevated-score threshold, and the
prohibition on a State mean as the national value are all withdrawn. The threshold saturated at
both ends of the slice grid — at the historical baseline more than half of all State/UTs sat at
exactly 0%, and at end-century the median exceeded 80% — and it tied large groups of State/UTs at
identical values. The five-band distribution remains the supporting view of the complete score
distribution.

The national screening surface is limited to the 13 scenario-based thematic and sector-wise
bundles:

```text
Thematic
    Heat Risk
    Drought Risk
    Extreme Rainfall | Flash Flood Risk
    Heat Stress
    Cold Risk

Sector-wise
    Agricultural Risk
    Health Risk
    Industrial Risk
    Investment / Financial Risk
    Infrastructure Risk
    Asset Risk (Thermal Power Plants)
    Asset Risk (Hydropower Plants)
    Life & Livelihood Loss Risk
```

`Water Risk`, `Riverine Flood`, and other snapshot, standalone, or non-bundle products are outside
the scope of this scenario-and-period screening surface. Their different temporal or normalization
contracts should not be silently mixed with the 13 eligible bundles.

The public defaults are fixed as:

```text
Bundle:   Heat Risk
Scenario: SSP5-8.5
Period:   2040-2060
```

The Overview uses the following user-facing labels while retaining the SSP and period identifiers
in the artifact and URL state:

```text
Middle-of-the-road (SSP2-4.5)
Fossil-fuelled development (SSP5-8.5)

Early century (2020–2040)
Mid-century (2040–2060)
End century (2060–2080)
```

The national statistic answers:

> Within this State/UT, what is the area-weighted average bundle composite score of its
> districts, on the frozen national scale?

Because the scale is frozen nationally, this **is** an absolute interstate comparison and may be
described as one. The caveat that survives is about scope, not normalization: the score is
hazard-only, it does not include exposure, vulnerability or resilience, and it is not comparable
across bundles. Avoid `national climate-risk score`, which implies the excluded dimensions.

District and block scores are produced on the same frozen national ruler and are therefore
directly comparable: a district and a block holding the same physical value receive the same
score and the same colour. Neither level is derived from the other — each is scored from its own
physical values, and because the ruler is non-linear the area-weighted mean of a district's block
scores does not equal that district's own score. The national overview uses districts across all
State/UTs; the State view uses blocks.

### Ranking eligibility and data quality

Coverage validity and ranking stability remain separate checks:

```text
coverage_fraction = n_valid / n_expected
```

`coverage_fraction` is **not a shipped artifact field**. It is computed during the artifact build,
and the build fails when a State/UT falls below the floor, so a published artifact is complete by
construction. The rules below therefore govern the build and the interface's no-data handling,
not a runtime suppression path over shipped values:

```text
coverage < 90%
    -> the artifact build fails for that bundle x scenario x period
    -> nothing is published for the affected State/UT

coverage >= 90% and n_valid < 10
    -> publish the State mean
    -> suppress the rank
    -> flag Small cohort

coverage >= 90% and n_valid >= 10
    -> publish the State mean
    -> eligible for ranking
```

When a cohort is small, show the calculable State mean and suppress only the rank. If
`n_valid = 0`, show `No valid data` and no score, band, rank, or drivers.

All cohorts with `n_valid < 10` use the single label `Small cohort` and remain unranked. Units
with no valid composite data should show `No valid data`. A geography may remain available for
drill-down when usable lower-level data exists even if its parent-level rank is suppressed.

Ranking is hierarchical and comparison-cohort-specific:

```text
National view
    Rank State/UTs by the area-weighted mean district composite score.
    Publish rank only when the State/UT has district coverage >= 90%
    and at least 10 valid districts.

State view
    Rank individual district composite scores within the selected State/UT.
    Publish district ranks only when the parent State/UT district cohort has
    coverage >= 90% and at least 10 valid districts.
```

Ranking is two-tier. Blocks are painted but not ranked: block cohorts run from 1 to 38 per
district, far below the ten-unit minimum, and the roster's subdivision density reflects State
administration rather than geography.

Rank eligibility controls whether a rank may be shown; it does not determine whether a valid
individual district or block score may be shown. Do not expose State-wide or national block ranks
in Overview. Note that the reason has changed: block scores *are* nationally comparable under the
frozen ruler, and the objection is cohort size and administrative unevenness, not comparability.

The national rank denominator is the number of rank-eligible State/UTs, and the District rank
denominator is `n_valid` districts in the selected State/UT. Missing or invalid units do not
participate in ranking, and `n_expected` must not be presented as the rank denominator when some
units are invalid. No separate `n_ranked` field or public term is required.

Rankings should use competition ranks, so identical values receive the same rank and the following
rank reflects the number of preceding entries. Alphabetical or stable administrative-code sorting
may order tied rows visually but must not break the statistical tie. Show a top-10 shortlist by
default with a `View all` action. Calculate band assignment, eligibility, and ranks from
full-precision stored values; round only for display. Exact full-precision equality receives a
tied rank. If two unequal values appear identical at the default display precision, the tooltip or
expanded ranking should expose sufficient additional decimal precision to explain their order.
When a genuine tie exists, user-facing text may say `Rank N (tied) of M valid units`.

Use the following display precision:

```text
Bundle scores:             1 decimal place
State mean scores:         1 decimal place
Counts and ranks:          integers
```

Scores are stored at full `float64` precision and rounded only for display. Artifacts must not be
rounded: the ranking contract awards tied ranks on exact full-precision equality, and rounding
manufactures ties that do not exist.

A redundant `.0` may be suppressed where useful, for example `100%`.

### Expected denominator and boundary-vintage contract

The canonical Phase 1 administrative roster contains:

```text
784 districts
7,137 blocks
```

Before production release, the roster build must assign and persist a concrete immutable
`admin_roster_version`; the counts alone are not a version identifier.

`n_expected` must come from this fixed, versioned canonical administrative roster, never from the
set of rows that happen to contain scores for the active bundle. District expectations should use
the canonical State/UT-district roster, while block expectations should use the canonical
State/UT-district-block roster. Missing bundle scores reduce `n_valid`; they must not reduce
`n_expected`.

District scores, block scores, map geometry, and denominator artifacts must reference the same
administrative-roster version. The offline build should reject duplicate geographic keys,
unexpected units, missing parent keys, or a mismatch between score and boundary-roster versions.
Stable official geographic identifiers should be used for joins, cohort membership, and ranking,
and retained alongside display names wherever the source provides them. Names are presentation
fields and must not be the primary production join keys. Moving the current name-derived Glance
keys to stable administrative identifiers is explicit production migration work.

Each published screening artifact or its accompanying manifest must record enough provenance to
reproduce the denominator contract, including:

```text
admin_roster_version
boundary_source
boundary_source_date_or_version
boundary_build_date
district_boundary_hash
block_boundary_hash
expected_district_count
expected_block_count
expected counts by State/UT
ruler_id
colour_scale_id
data_snapshot_hash
```

`ruler_id` names the frozen ruler support that produced the scores, `colour_scale_id` the palette
and domain they are rendered against, and `data_snapshot_hash` the input snapshot the ruler was
fitted over. A score without a `ruler_id` is unfalsifiable: a correctly rendered new ruler cannot
be distinguished from a wrongly rendered old one.

Any change to the canonical roster, including the addition, removal, merger, split, or renaming of
an administrative unit, requires an explicit boundary-scope decision, a new roster version, updated
State/UT denominator counts, and revalidation of scores, coverage, ranks, geometry joins, and map
labels. It must not enter production merely because a newer boundary file is present.

Every national screening artifact should retain at least:

```text
state_name
bundle_id
scenario
period
admin_level
n_expected
n_valid
state_mean_area_weighted
state_mean_population_weighted
rank_eligible
national_rank
quality_flag
admin_roster_version
boundary_source_date_or_version
ruler_id
colour_scale_id
data_snapshot_hash
```

Each released analytical artifact set must also have a unique reproducible build identity that
records the build timestamp, exact source-code commit SHA, artifact manifest, manifest
hash/checksum, and `admin_roster_version`. The manifest is the canonical inventory of supported
Bundle x Scenario x Period x administrative-level artifacts and must identify each artifact path or
identifier and version/checksum. Validation baselines must reference this same build identity.
Analytically meaningful changes to code, roster, inputs, or artifacts create a new identity rather
than mutating a validated release.

### Current case-study validation baseline

The refreshed national-screening case study successfully rebuilt the 13 eligible composite and
Glance artifact families against the canonical roster. The accepted validation baseline is:

```text
Eligible bundles:             13
Scenarios:                     2
Periods:                       3
Administrative levels:        2
Threshold diagnostic rows:   28,080
Canonical districts:          784
Canonical blocks:            7,137
Optimized parity issues:       0
```

The refreshed artifacts also contain valid Heat Risk scores for all three districts in `Dadra,
Nagar Haveli, Daman & Diu`. Under the default selection its State mean is visible but its national
rank is suppressed because `n_valid = 3` is below the ranking minimum of 10. This is the intended
small-cohort treatment, not a missing-data case.

This baseline predates the frozen-scale amendment and its threshold-diagnostic row count refers to
the withdrawn statistic. It must be rebuilt against the frozen ruler before it is cited again.

This case-study baseline is methodology evidence rather than a released provenance identity. The
production acceptance rebuild must record its build timestamp, source commit, manifest checksum,
and concrete `admin_roster_version` before these values become a release baseline.

### National map visual encoding

The pan-India map paints each district directly from its bundle composite score on the frozen
`0-100` domain, through the vendored `WhiteBlueGreenYellowRed` colour table sampled at 101 stops
starting at fraction `0.045` so that no valid score renders as pure white. Colour is a pure
function of the score — `index = round(score)` — with no binning, smoothing, or parent-geography
effect.

There is **one** continuous colourbar with fixed numeric ticks, titled level-neutrally
(`<Bundle> score`, not `District Bundle Score` or `Block Bundle Score`). One frozen ruler means one
legend; separate per-level titles would describe a distinction the methodology no longer makes.

The domain never rescales. Not when the bundle, scenario, or period changes, and not when a
State/UT is selected. Identical colours represent identical scores across every selection and both
administrative levels.

State and union-territory polygons are **never filled**. At national zoom they contribute the
coarse boundary stroke only; their headline statistic lives in the ranking, the tooltip and the
answer card. There is one score-derived fill per view.

Boundary weight carries the administrative level:

```text
National view: thick State/UT boundary, thin district boundary
State view:    thick district boundary, thin block boundary
```

The thick stroke is always the unit the previous view was painting; the thin stroke is the unit
this view is painting. Both use one hue differing only in width and opacity:

```text
light #7a8794      dark #8e9ba8
fine    stroke-opacity 0.45, ~0.45px
coarse  stroke-opacity 0.95, ~1.9px
both    vector-effect: non-scaling-stroke
```

Strokes must not use the page background colour, which would dissolve boundaries between pale
low-scoring districts, nor black, which reads as dirt over the hot end of the ramp. The fine layer
is dropped entirely below approximately one screen pixel. Selection is a third stroke in the accent
hue rather than a fourth grey weight, so it cannot be mistaken for another administrative level.

A bracket on the colourbar marks the score range present in the current view, with a numeric
readout beside it — a State/UT whose blocks genuinely span two points should read as narrow, not
as broken. A `Local contrast` view may rescale the domain to the visible extent; it is off by
default, explicitly labelled as not comparable across selections, and is never the landing state.
Its extent is computed from the visible scores at render time and is never a stored normalization
parameter.

`#d5d8dc` identifies missing composite data and carries its own legend swatch. A distinct dashed
State/UT outline may identify a small cohort whose State mean remains visible but is not rank
eligible. A compact persistent method note should explain the frozen scale, the boundary grammar,
quality flags, and the hazard-only interpretation boundary.

Colour interpolation should use a perceptual colour space such as OKLCH or CIELAB where the
implementation resamples the table.

No-data units must use one consistent neutral treatment and must never be mapped onto the valid
low-score end of the palette. Insufficient coverage and small cohort are quality flags rather than
score values: preserve a calculable quantitative fill where permitted and add a secondary pattern
or outline instead of replacing the score colour.

Hover should provide temporary modest emphasis; selection should provide stronger persistent
emphasis. Selection takes precedence over hover and filter styling, while quality-state styling
must remain visible. Colour must not be the sole carrier of score, band, selection, or quality.
Equivalent text and accessible tables, keyboard-operable geography and filter controls, visible
focus, adequate contrast, and colour-vision-deficiency testing are required. Tiny geographies must
remain selectable through usable map targets where feasible and an equivalent non-map control.
Exact palette hex values and responsive stroke widths belong in the design-system implementation
specification, subject to these requirements.

### National map interaction

In the pan-India view, hovering anywhere within a state should highlight the whole state and show
state-level information only:

- state or union-territory name;
- the area-weighted mean district bundle score and its five-band classification;
- the population-weighted mean, as a secondary line;
- national rank and the number of rank-eligible State/UTs, when eligible; and
- a quality flag and an explanation that rank is suppressed, when not eligible.

District names, district composite scores, and district score bands should not appear in the
pan-India tooltip even though districts are individually painted. Districts become inspectable
only after a State/UT is selected.

Clicking anywhere within a state should select and zoom to that state, load that State/UT's block
geometry and attributes, and repaint the map from **block** composite scores. The district boundary
becomes the coarse stroke and the block boundary the fine stroke. The colourbar, its title and its
domain do not change.

A `District fill` toggle is available in the State view for users who want the map to match the
district ranking exactly; block fill is the default.

Expect the amount of visual change on selection to vary greatly by State/UT, because the district
roster is unevenly coarse. In Uttar Pradesh only 3.8% of within-State block variance lies inside
districts, so the block map looks almost identical to the district map; in Kerala it is 47.5% and
in Goa 74.5%, where the map transforms. Interface copy must promise the same *scale*, never the
same *map*.

Block geometry and attributes load together, only after a State/UT is selected. Payload is not a
constraint: the median State/UT is 144 blocks (~0.56 MB) and the largest, Uttar Pradesh, is 822
blocks (~3.9 MB), against 31.6 MB nationally — which is why the national view stays at district
resolution.

The five interpretive score bands are:

```text
Very Low: 0 <= score < 20
Low:      20 <= score < 40
Moderate: 40 <= score < 60
High:     60 <= score < 80
Extreme:  80 <= score <= 100
```

The visual hierarchy is therefore:

```text
National view: continuous district composite-score colour
               thick State/UT boundary, thin district boundary
    → select state
State view:    continuous block composite-score colour
               thick district boundary, thin block boundary
               + five-band district distribution and district-level interaction
```

Both views preserve the same fixed `0-100` colour domain and the same level-neutral legend title,
so identical colours retain identical score meanings across geography, administrative level,
bundle, scenario, and period. The continuous map colourbar and the five-band distribution serve
different purposes: the colourbar encodes exact mapped scores, while the bands provide a compact
interpretive and filtering aid.

The State mean is not the complete evidence surface. The selected State/UT overview should retain
the full district bundle-score distribution alongside the headline, for example:

```text
Telangana — Heat Risk
77.8 area-weighted mean district score · High · 33 valid districts

District bundle-score distribution
[five-band interactive bar chart]
```

The chart should show all five bands in the fixed order even when a band has zero units. Counts are
the default display; percentages and exact score ranges may appear in tooltips. One band may be
selected at a time. Selecting a bar should highlight matching districts, mute rather than hide the
remaining districts, and filter the ranking shortlist. Selecting the active bar again or using
`Clear filter` should restore all districts. Zero-count bars should remain visible but disabled.
All five bars are ordinary bands; none is split or given special treatment, and no secondary cut
point is introduced anywhere in the Overview.

Note that the band cuts become close to definitional under a rank-based ruler, where `>= 80` reads
as "worse than 80% of pooled national observations". Whether to state that plainly or to set the
cuts from physical values is an open methodological question; see
[`docs/composite_scale_decisions.md`](docs/composite_scale_decisions.md), Part E.

The State-view layout contract is:

```text
Header
    State/UT name + persistent Bundle, Scenario, and Period selectors

Headline
    Area-weighted mean district bundle score + five-band classification
    Population-weighted mean (secondary)
    n_valid + eligible national rank or quality explanation

Map
    Continuous block composite-score colour + district interaction
    Thick district boundary, thin block boundary

Supporting evidence
    Five-band interactive distribution + top-10 district shortlist + metric/rule signals

Context and Evidence
    Collapsed exposure, hydrology, data-quality, and optional-overlay content

Navigation
    India > State/UT breadcrumb
```

The district ranking denominator is the number of valid districts in the selected State/UT, not
the number expected when some scores are invalid. A valid district score remains visible when the
parent State/UT cohort is ineligible for ranking.

> **Unresolved — 2026-09-09.** With blocks painted in the State view there is no finer map for a
> District view to introduce, so a district selection is most likely an inspection state inside the
> State view: block fill retained, the selected district's outline promoted to the selection
> stroke, breadcrumb stopping at `India > State/UT`, and the panel showing the district score,
> band, rank within the State/UT, drivers, and the range of its blocks. That has not been decided.
> The contract below is retained verbatim pending that decision and must not be implemented as-is
> without re-reading it against the frozen-scale amendment.

The District-view layout contract is:

```text
Header
    District name + parent State/UT
    Persistent Bundle, Scenario, and Period selectors

District answer
    District Bundle Score + five-band classification
    District rank within State/UT when the parent cohort is eligible
    Relevant coverage or quality status

Drivers
    Up to three valid District-scoped metric drivers or rule signals

Map and within-District variation
    Continuous Block Bundle Score on a fixed 0–100 display scale
    Interactive Block inspection
    Five-band Block distribution + top-10 Blocks ranked within the District

Context and Evidence
    Collapsed exposure, hydrology, data-quality, and optional-overlay content

Navigation
    India > State/UT > District breadcrumb
```

Do not add an Elevated Block Concentration headline or rank districts by block concentration in
Phase 1. Block information explains within-District heterogeneity; it does not redefine the
District's primary score.

A selected Block is an inspection state within District view, not a fourth full Overview
navigation level. Keep the District map visible and show the Block name, parent District and
State/UT, Block Bundle Score, five-band classification, eligible rank among valid Blocks within the
selected District, relevant data-quality state, and valid Block-scoped driver/rule signals. A new
Block selection replaces the previous one and may be cleared without leaving District view. The
breadcrumb remains `India > State/UT > District`. Do not show Elevated Block Concentration,
State-wide or national Block rank, another nested distribution, or a duplicated full Context and
Evidence hierarchy. Show `View Detailed Analysis` only when a registered valid route exists.

The application should avoid an empty first screen that requires the user to complete a series
of controls before seeing any information. Defaults must be visible and clearly identified so
the user understands what is being shown without mistaking them for personal selections.

The public defaults are `Heat Risk`, `SSP5-8.5`, and `2040-2060`. They should remain visibly
identified as defaults rather than being mistaken for user-selected values.

## 2. Limit the primary controls to three

The Overview should expose only three primary analysis selectors:

1. Bundle
2. Scenario
3. Period

Constituent metric, statistic, map mode, model controls, and detailed chart options belong in
Detailed Analysis.

Administrative level should appear contextually through the geographic drill-down:

```text
India → States → Districts → Blocks
```

Geography should be navigated through search or direct map interaction rather than treated as a
fourth analysis selector. This is easier to understand than requiring every geographic dimension
to be configured before the map becomes useful.

Bundle, Scenario, and Period selections should persist throughout drill-down. Breadcrumbs should
provide the reversible geographic path, for example `India > Uttar Pradesh > Kaushambi`. Browser
Back is outside the Overview analytical-state model; breadcrumbs and in-application navigation are
authoritative for moving through analytical states. Existing application-shell Browser Back
behavior is not redefined by this workflow.

When Bundle, Scenario, or Period changes, preserve the current geography where it remains
supported; clear band filters, hover, and temporary emphasis; and recompute scores, bands, ranks,
drivers, and distributions. If a geography still exists in the canonical roster but has no valid
score under the new selection, remain at that geography and show `No valid data`. Fall back through
`Block -> District -> State/UT -> India` only when the selected geography or level is genuinely
obsolete or unsupported, and explain what could not be restored.

Band filters are view-local transient state. Clear them when Bundle, Scenario, Period, or
administrative level changes. Applying a filter must not clear an already selected geography that
falls outside the band; selection takes precedence over filter emphasis. Breadcrumb navigation
preserves Bundle, Scenario, and Period, clears selections below the destination level, and clears
the previous view's filters.

Coordinate analysis should remain available as an alternate location-entry path, but it should
not compete visually with the default geography-first workflow. A clear action such as
`Analyse a custom location` can reveal manual coordinate and file-upload controls when needed.

Supported Bundle x Scenario x Period combinations come from the deployed artifact manifest.
Normal selectors must not offer unsupported combinations. An obsolete or invalid deep-linked
combination should fall back to a valid configured selection with a concise explanation. By
contrast, when an expected artifact is missing or unloadable, retain the requested selectors and
show an unavailable state; never silently substitute another scenario, period, or stale artifact.

During loading, preserve the page structure and selected geography, show an explicit loading
state, and disable interactions that depend on the incoming artifact. Never display old analytical
values under newly selected labels. A valid artifact with no valid data should show `No valid data`
for the affected geography while independently available Context and Evidence may remain visible.
An artifact-version mismatch must never render mixed-version analytical outputs: show a generic
unavailable state and retain technical details in logs and diagnostics.

## 3. Present a direct answer, not merely visualizations

After a geography is selected, the first summary should answer the user's likely question in
plain language. For example:

> Warangal has a Heat Risk bundle score of 72, ranking 4 of 33 districts in Telangana. Its assigned
> five-band classification and strongest drivers are shown below.

The answer card should contain:

- bundle score;
- score band;
- rank within the declared State/cohort comparison group;
- the declared State/cohort comparison scope;
- up to three strongest valid metric drivers or rule signals; and
- a concise interpretation boundary, such as `Hazard-only; does not include exposure,
  vulnerability, or resilience`.

The score, rank, and comparison scope must be understandable without requiring the user to
interpret several charts independently.

## 4. Make deeper Overview information optional

Supporting information should be available through secondary or expandable sections:

- `Where are the hotspots?`
- `How is risk distributed?`
- `Compare locations`
- `View rankings`
- `Context layers`
- `Download answer`

These sections should not all be expanded on first load.

The hotspot list should provide direct navigation to a selected geography. The bundle-score
distribution chart should use the single-band interaction defined in the State-view contract and
filter the ranking table to the corresponding locations. State-view distributions, filters,
rankings, and answer cards should use the same band order: `Very Low`, `Low`, `Moderate`, `High`,
`Extreme`. Filtering must retain each location's original rank rather than recalculating rank
within the filtered subset. The active filter and comparison scope should remain visible.

Comparison should be a deliberate follow-up action. It should not add controls to the initial
path before the user has understood the first result.

Overview exports should focus on the current answer and visible evidence:

- copyable answer text;
- the visible ranking rows; and
- an answer pack containing the current context, drivers, metadata, and method note.

With an active band filter, export the currently filtered rows only, retain their original
unfiltered ranks, record the active filter and selection metadata, and never recompute ranks inside
the exported subset.

## 5. Treat exposure and hydrology as context, not primary filters

Exposure and hydrological information should support interpretation without competing with the
main bundle-score or hazard-pressure question.

In Overview, a compact `Context and Evidence` section should be available from the State view
onward and collapsed by default:

- place exposure overlays under a collapsed `Context layers` control;
- allow a map layer to be selected independently of the risk analysis;
- show a State/UT-level Exposure Summary and Hydrological Context using appropriately aggregated
  context artifacts;
- show progressively more local context after a district, block, or coordinate is selected;
- keep basin, sub-basin, and river-network overlays optional; and
- do not blend exposure or hydrological context into the displayed hazard score unless the
  methodology explicitly defines that relationship.

Context and Evidence is supplementary. Its absence must not invalidate a bundle score, band, rank,
or drivers. Show only fields supported at the current administrative level; do not infer,
interpolate, or substitute missing context from another geography level. Omit an unavailable field,
and show a concise unavailable-state message when an entire subsection is absent. Omit unavailable
overlays from the selector rather than showing disabled controls.

Do not impose one universal minimum-coverage threshold across all context datasets. Any necessary
coverage rule belongs to that dataset or metric's own scientific contract. Where relevant, context
artifacts should expose source, unit, source date/version, validity or availability, and coverage.

The established IRT context fields should guide this section. Exposure may include population,
rural-facility counts and rates, built-up area and share, and agricultural LULC area and share.
Hydrological context may include the dominant basin and sub-basin, other intersecting basins,
overlap shares, hydrological type, primary river, drainage area, and available boundary or river
overlays.

State/UT context must follow scientifically appropriate aggregation rules:

- sum population, facility counts, built-up area, and agricultural LULC area;
- recalculate shares from State/UT totals rather than averaging district percentages;
- recalculate per-capita rates from State/UT totals;
- calculate basin shares from State/UT-to-basin geometry intersections rather than counting the
  dominant basin assigned to individual districts;
- exclude missing values transparently and display context coverage, units, source dates, and
  provenance; and
- keep exposure and hydrology contextual rather than silently incorporating them into the bundle
  score.

A compact context summary could read:

> Population: 1.2 million · Built-up share: 18%
>
> Dominant basin: Krishna, 82% · River network available

The user can expand the summary for category breakdowns, basin or sub-basin details, and overlay
controls.

For coordinate inputs, location-level context should be derived from the block containing the
coordinate, with that geographic basis stated explicitly.

## 6. Use one primary transition to Detailed Analysis

The selected result should expose one prominent action:

`Explore in Detailed Analysis`

The transition should preserve:

- geography and administrative level;
- the selected unit;
- bundle;
- scenario;
- period;
- selected driver or rule when the action originated from one; and
- the current comparison context where compatible.

Do not carry hover, band filter, tooltip, or temporary map emphasis into Detailed Analysis.

Use canonical Bundle-to-Detailed-Analysis and driver/rule-to-Detailed-Analysis route registries.
Do not infer destinations from labels or names. Preserve geography exactly where supported and do
not substitute another administrative level unless the registry explicitly defines that fallback.
If no valid route exists, do not show an active Detailed Analysis action.

Detailed Analysis should open on the composite metric corresponding to the selected bundle. It
should not open with `Metric = All`, because `All` does not clearly communicate whether the user
is still viewing the same score.

The first Detailed Analysis state should reproduce the result the user selected in Overview.
This continuity allows the user to recognise the analysis before deciding whether to refine it.

## 7. Reveal advanced controls inside Detailed Analysis

Detailed Analysis should initially show the same selected result, followed by a collapsed
`Refine analysis` area containing advanced controls such as:

- constituent metric;
- statistic;
- map mode;
- alternative scenario;
- alternative period;
- model-member controls;
- trend and scenario-comparison controls; and
- methodological details.

Advanced controls should be disclosed in response to user intent instead of being prerequisites
for the first useful result.

Overview should reuse the existing persisted Glance driver contract rather than introduce a new
weighted-contribution calculation. For thematic bundles, show `Metric Drivers`; for sectoral
bundles, show `Top Rule Signals`. At State/UT scope, rank each available metric or rule using its
existing mean normalized score across valid districts. District and Block views must use valid
persisted rows scoped to that exact administrative level. Do not infer, interpolate, or borrow
driver signals from another level.

These values should be described as normalized metric drivers or rule signals, not as percentage
shares of the composite. The interface must not claim, for example, that a metric `contributed 34%
of the composite`, and no additional weighted-contribution calculation is required for this
workflow. Show no more than three valid drivers/rules, ordered by full-precision signal strength,
without displaying their numeric signal values in Overview. If fewer than three exist, show only
those available. If none exist, show `Driver information is not available for this geography.` No
secondary tie-breaking rule is required for driver ordering at this stage.

Where a driver has a one-to-one underlying metric or rule route, selecting it should open Detailed
Analysis with the current geography, administrative level, bundle, scenario, and period preserved
and the corresponding metric or rule selected. A sectoral rule without a one-to-one Detailed
Analysis target should be displayed as informative text and remain unclickable; it should not be
routed to an approximate or unrelated metric. Driver validity and route availability are separate:
a valid unroutable driver remains normally styled rather than being greyed out. A canonical
driver/rule-to-Detailed-Analysis route registry controls clickability.

## 8. Preserve a reversible return path

`Back to Overview` should restore the previous Overview context, including:

- geography;
- bundle;
- scenario;
- period;
- selected Block inspection state, where applicable;
- map extent; and
- major panel expansion state where technically supported.

Do not restore hover, tooltip, band-filter, or other temporary emphasis state.

If a Detailed Analysis selection cannot map directly to an Overview bundle, returning should
restore the last valid Overview context rather than clearing or partially reconstructing the
analysis.

Browser controls are outside the dashboard analytical-state model. Breadcrumbs and in-application
navigation must provide the complete reversible path without depending on Browser Back.

## 9. Production migration contract

The current Glance implementation is legacy input to a migration, not authority for the new
Overview. It must not silently preserve per-State min-max normalization, four-band artifacts, old
ranking/filter semantics, name-derived administrative joins, or inferred navigation routes.

The earlier prohibition on a State mean as the national value is withdrawn. It was correct while
scores were normalized within each State, and is void under one frozen national ruler: the State
mean is now the national value. What must not survive is the normalization, not the mean.

The national State/UT artifact must provide at least:

```text
state_id
state_name
bundle_id
scenario
period
admin_level
n_expected
n_valid
state_mean_area_weighted
state_mean_population_weighted
rank_eligible
national_rank
eligible_state_count
quality_flag
admin_roster_version
artifact_build_id
ruler_id
colour_scale_id
data_snapshot_hash
```

Per-unit district and block artifacts carry the stable administrative identifier, the geographic
key, `bundle_id`, `scenario`, `period`, `admin_level`, `score`, and quality flags — and nothing
else. They must not carry raw metric values, normalization parameters, ruler support, or per-State
extents. Omitting those inputs is what makes a per-State renormalization downstream
unconstructible rather than merely discouraged. The colour table and its domain ship alongside as a
versioned scale definition rather than as prose, so that rendering is a lookup with no judgement in
it.

The five-band migration must replace the current four-band fields and distributions throughout the
Overview; all assignment uses full-precision scores. National maps paint districts from their own
composite scores on the frozen `0-100` scale, with State/UT polygons unfilled. State maps paint
blocks on that same scale, with no inherited parent hue and no change of colourbar. There is one
score-derived fill per view and one colourbar for both.

Production work must also implement the canonical route registries, selector/filter/geography
state transitions, quality states, stable administrative-ID joins, immutable roster/build identity,
progressive geometry loading, and accessibility requirements defined above. Block geometry and
Block attributes load together, only after State selection.

## 10. Minimum acceptance-test contract

At minimum, synthetic and artifact-contract tests must prove:

### Ranking and coverage

- national State ranking uses the area-weighted mean district composite score;
- District ranking is within the selected State/UT only;
- blocks are painted but never ranked, at any scope;
- rank is suppressed when `n_valid < 10`;
- the artifact build fails when coverage is below 90%, rather than publishing a suppressed value;
- valid individual scores remain visible when parent-cohort rank is suppressed;
- competition-ranking ties and rank denominators are correct;
- `n_expected` comes from the versioned roster rather than score rows; and
- missing score rows never participate in ranking.

### Precision and bands

- five-band assignment, eligibility, and ranking use full precision;
- artifacts store unrounded scores, and display rounding cannot change a band, rank, or tie; and
- the legacy four-band labels cannot enter new Overview artifacts.

### Interaction and navigation

- filtering never recomputes rank or clears a selected geography outside the active band;
- Bundle, Scenario, Period, and administrative-level changes clear the local band filter;
- a roster-valid no-data geography remains selected with a no-data state;
- an obsolete or unsupported Block, District, or State falls back only to its nearest valid parent;
- Block inspection does not create a fourth breadcrumb level; and
- Detailed Analysis routing and return preserve only the declared durable state.

### Maps, routing, and data states

- the India and State views use one level-neutral legend title and the frozen `0-100` domain;
- the same score maps to the same colour at both zooms and at both administrative levels,
  regardless of parent geography;
- a district and a block holding the same physical value receive the same score;
- no State/UT polygon carries a score-derived fill at any zoom;
- the colour domain never rescales outside the labelled local-contrast view;
- for every district-slice, the district's own score and the area-weighted mean of its block
  scores differ by less than 10 points, guarding the nesting of the two levels;
- the canary geographies render as expected: a State/UT whose blocks span under two points, such
  as Ladakh at SSP5-8.5 2040-2060, must render as one visually uniform fill, since more than one
  distinct colour there proves the domain has been rescaled;
- clickable drivers have registered routes and valid unroutable drivers remain visible;
- unsupported combinations, missing expected artifacts, version mismatch, and valid no-data
  artifacts have distinct behaviors; and
- loading never displays stale analytical values under new selector labels.

### Accessibility and provenance

- geography and band filtering have keyboard-operable non-map alternatives;
- score, selection, and quality are communicated without relying on colour alone;
- small geographies remain selectable outside the map; and
- artifacts reject missing/mismatched roster versions, build identities, and stable administrative
  keys.

## Core workflow principles

The workflow should be evaluated against the following principles during each section-level
refinement:

- The first screen provides information rather than setup work.
- No more than three primary analysis selectors are needed for a quick analysis.
- Each screen has one visually dominant next action.
- Advanced capability is discoverable without being compulsory.
- Overview and Detailed Analysis use one canonical analysis context.
- Scores, bands, ranks, legends, and comparison scopes remain consistent between levels.
- Maps use one continuous fixed `0-100` score colour domain and one level-neutral legend title at
  every level; discrete bands are interpretive and interactive supporting evidence.
- There is exactly one score-derived fill per view — districts nationally, blocks within a
  State/UT — and aggregate statistics are never rendered as a choropleth.
- Switching levels does not clear or silently reinterpret the user's selections.
- Exposure and hydrology remain clearly identified as contextual information unless they are
  explicitly included in a score.
- Missing or partial data is visible and does not silently become a valid-looking score.
- National results are absolute interstate hazard comparisons on one frozen scale; they remain
  hazard-only and are not comparable across bundles.
- Coverage is enforced at the artifact build, and denominator-based ranking eligibility remains
  explicit and separate from it.
- Rankings use top-10 shortlists, competition ranks, stable alphabetical or administrative-code
  display order within ties, and original ranks under filtering.
- Geometry is loaded progressively: national district context first, then State-scoped block
  geometry and attributes together on State selection.
- The interface answers a user question before offering additional analytical controls.
