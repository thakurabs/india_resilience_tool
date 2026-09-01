# Recommended Target Workflow

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
- an `Elevated Bundle-Score Concentration (%)` legend;
- the eligible states and union territories with the highest concentrations; and
- a concise explanation of what the screening statistic represents.

The national map should not average State/cohort-normalized district or block composite scores
and present the result as an absolute pan-India climate-risk comparison. Instead, the Phase 1
district overview should use `Elevated Bundle-Score Concentration (%)`:

```text
Elevated Bundle-Score Concentration (%)
=
100 x
(number of valid districts with bundle composite score >= 50)
/
(number of valid districts)
```

The elevated-score threshold is fixed at `50`. The concentration statistic ranges from 0%, when
no valid district reaches the threshold, to 100%, when every valid district reaches it. This
threshold is independent of the five interpretive score bands: it includes the upper half of the
`Moderate` band together with the complete `High` and `Extreme` bands. The interface should state
the `>= 50` rule in the headline and method note without subdividing the Moderate band in the
distribution chart.

`Elevated Bundle-Score Concentration (%)` is the single headline national screening statistic.
Do not add a State/UT median bundle score as a second headline or parallel national ranking
measure. The five-band distribution provides the supporting view of the complete score
distribution without introducing a competing summary statistic.

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

> Within this State/UT, what proportion of valid districts meet or exceed the defined
> elevated-score threshold under the selected IRT bundle methodology?

It does not answer which State/UT has greater absolute physical climate hazard or climate risk.
The statistic summarizes the concentration of State/cohort-normalized bundle scores and must be
described as a national screening view. For Phase 1 climate bundles, use `bundle-score
concentration` or `hazard-pressure screening` language rather than `highest-risk state`,
`national climate-risk score`, or `absolute interstate risk`.

District and block results must be calculated separately and must never be mixed in one
concentration statistic. The Phase 1 national overview uses districts consistently across all
State/UTs. A block-level overview may be introduced only as a separately declared view after its
data and denominator contracts have been validated.

### Coverage and ranking eligibility

Coverage validity and ranking stability are separate quality checks:

```text
coverage_fraction = n_valid / n_expected
```

The initial screening contract is:

```text
coverage < 90%
    -> show Insufficient coverage
    -> suppress the concentration percentage and rank

coverage >= 90% and n_valid < 10
    -> show the concentration percentage
    -> suppress the rank
    -> flag Small cohort

coverage >= 90% and n_valid >= 10
    -> show the concentration percentage
    -> eligible for ranking
```

If coverage and cohort-size requirements both fail, show both `Insufficient coverage` and `Small
cohort`; do not invent another combined category. When coverage is below 90%, show `n_valid`,
`n_expected`, and coverage but suppress the concentration percentage and rank. When coverage is at
least 90% but the cohort is small, show the calculable concentration percentage and suppress only
the rank. If `n_valid = 0`, show `No valid data` and no score or concentration, band, rank, or
drivers.

All cohorts with `n_valid < 10` use the single label `Small cohort` and remain unranked. Units with
no valid composite data should show `No valid data`. A geography may remain available for
drill-down when usable lower-level data exists even if its parent-level concentration or rank is
suppressed.

Ranking is hierarchical and comparison-cohort-specific:

```text
National view
    Rank State/UTs by Elevated Bundle-Score Concentration.
    Publish rank only when the State/UT has district coverage >= 90%
    and at least 10 valid districts.

State view
    Rank individual District Bundle Scores within the selected State/UT.
    Publish district ranks only when the parent State/UT district cohort has
    coverage >= 90% and at least 10 valid districts.

District view
    Rank individual Block Bundle Scores within the selected district.
    Publish block ranks only when the parent district block cohort has
    coverage >= 90% and at least 10 valid blocks.
```

Rank eligibility controls whether a rank may be shown; it does not determine whether a valid
individual district or block score may be shown. Do not rank districts by block concentration in
Phase 1, and do not expose State-wide or national block ranks in Overview.

The national rank denominator is the number of rank-eligible State/UTs. The District rank
denominator is `n_valid` districts in the selected State/UT, and the Block rank denominator is
`n_valid` blocks in the selected District. Missing or invalid units do not participate in ranking,
and `n_expected` must not be presented as the rank denominator when some units are invalid. No
separate `n_ranked` field or public term is required.

Rankings should use competition ranks, so identical values receive the same rank and the following
rank reflects the number of preceding entries. Alphabetical or stable administrative-code sorting
may order tied rows visually but must not break the statistical tie. Show a top-10 shortlist by
default with a `View all` action. Calculate thresholds, band assignment, eligibility, and ranks
from full-precision stored values; round only for display. Exact full-precision equality receives a
tied rank. If two unequal values appear identical at the default display precision, the tooltip or
expanded ranking should expose sufficient additional decimal precision to explain their order.
When a genuine tie exists, user-facing text may say `Rank N (tied) of M valid units`.

Use the following display precision:

```text
Bundle scores:             1 decimal place
Concentration percentages: 1 decimal place
Coverage percentages:      1 decimal place
Counts and ranks:           integers
```

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
```

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
threshold
n_expected
n_valid
coverage_fraction
n_ge_threshold
pct_ge_threshold
rank_eligible
national_rank
quality_flag
admin_roster_version
boundary_source_date_or_version
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
Nagar Haveli, Daman & Diu`. Under the default selection and threshold, its value is visible but its
national rank is suppressed because `n_valid = 3` is below the ranking minimum of 10. This is the
intended small-cohort treatment, not a missing-data case.

This case-study baseline is methodology evidence rather than a released provenance identity. The
production acceptance rebuild must record its build timestamp, source commit, manifest checksum,
and concrete `admin_roster_version` before these values become a release baseline.

### National map visual encoding

The pan-India map should use a continuous, hierarchical colour treatment rather than assigning
State/UTs to discrete colour classes.

For the active bundle, scenario, and period, each valid State/UT concentration should be positioned
on a fixed `0-100%` colour scale. Values should be interpolated through a five-anchor palette:

```text
0%   -> green
25%  -> light green
50%  -> yellow
75%  -> orange
100% -> red
```

The five palette colours are anchors in one continuous scale, not class bins. The map should show
one continuous `Elevated Bundle-Score Concentration (%)` colourbar with fixed numeric ticks. The scale
must not rescale to the observed State/UT range when the bundle, scenario, or period changes;
identical colours should continue to represent identical percentages across selections.

State and union-territory interiors should retain visible district boundaries. Within each state,
district composite scores should control the strength of the state's assigned colour:

```text
district tint strength = 30% + 70% x (district composite score / 100)
```

A district score of 0 therefore receives a light 30% tint of its state's colour rather than
white, while a score of 100 receives the full state colour. Intermediate scores receive
progressively stronger tints. The 30% floor is the initial visual specification and may be tuned
through accessibility and visual-regression testing without changing the analytical method.
Colour interpolation should use a perceptual colour space such as OKLCH or CIELAB so the gradient
appears visually even and avoids muddy intermediate colours.

At national zoom, district tint is supporting within-State variation only. It must not imply that
district scores are absolute interstate comparisons. When a State/UT is selected, the national
concentration encoding ends: districts are recoloured directly from their own scores on the fixed
0–100 District Bundle Score display scale, and the parent State's national hue is not inherited.

State boundaries should use a strong neutral stroke, while district boundaries should use a thin,
subtle neutral stroke. Grey should identify missing composite data. A distinct dashed State/UT
outline may identify a small cohort whose percentage remains visible but is not rank eligible.
The map should not add a separate district-level colourbar because district tint is supporting
context rather than the national headline statistic. A compact persistent method note should
explain the State/UT statistic, district tint, quality flags, and interstate interpretation caveat.

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
- Elevated Bundle-Score Concentration (%);
- districts meeting the threshold over valid districts;
- valid districts over expected districts and the coverage percentage;
- national rank and the number of rank-eligible State/UTs, when eligible; and
- a quality flag and an explanation that rank is suppressed, when not eligible.

District names, district composite scores, and district score bands should not appear in the
pan-India tooltip even though district boundaries and tint variation remain visible on the map.

Clicking anywhere within a state should select and zoom to that state, transition to the district
analysis view, replace the national concentration colourbar with a continuous fixed `0-100`
colourbar titled `District Bundle Score`, and enable district-level hover and selection. Districts
should use their composite scores directly on this scale. Block boundaries should be shown as thin,
subtle context outlines only; block scores, tint, hover, and selection should not be introduced
until the user enters the district view. Block geometry should be loaded only after a State/UT is
selected.

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
National view: continuous State/UT concentration colour + district composite-score tint
    → select state
State view: continuous district composite-score colour + block outlines
            + five-band distribution and district-level interaction
    → select district
District view: continuous block composite-score colour + block-level interaction
```

The State and district views should preserve the fixed `0-100` colour domain so identical colours
retain identical score meanings across geography, bundle, scenario, and period selections. The
district-view legend title should be `Block Bundle Score`. The continuous map colourbar and the
five-band distribution serve different purposes: the colourbar encodes exact mapped scores, while
the bands provide a compact interpretive and filtering aid.

The threshold statistic is not the complete evidence surface. The selected State/UT overview
should retain the full district bundle-score distribution alongside the headline count, for
example:

```text
Elevated Bundle-Score Concentration
11 of 33 valid districts >= 50

District bundle-score distribution
[five-band interactive bar chart]
```

The chart should show all five bands in the fixed order even when a band has zero units. Counts are
the default display; percentages and exact score ranges may appear in tooltips. One band may be
selected at a time. Selecting a bar should highlight matching districts, mute rather than hide the
remaining districts, and filter the ranking shortlist. Selecting the active bar again or using
`Clear filter` should restore all districts. Zero-count bars should remain visible but disabled.
The Moderate bar should remain a single ordinary band; it should not be split or given a special
two-tone treatment around the `>= 50` concentration threshold.

The State-view layout contract is:

```text
Header
    State/UT name + persistent Bundle, Scenario, and Period selectors

Headline
    Elevated District Bundle-Score Concentration (%)
    n >= 50 / n_valid + coverage + eligible national rank or quality explanation

Map
    Continuous District Bundle Score colour + district interaction + block outlines

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
Overview. It must not silently preserve State mean as the national value, four-band artifacts, old
ranking/filter semantics, name-derived administrative joins, or inferred navigation routes.

The national State/UT artifact must provide at least:

```text
state_id
state_name
bundle_id
scenario
period
admin_level
threshold
n_expected
n_valid
coverage_fraction
n_ge_threshold
pct_ge_threshold
rank_eligible
national_rank
eligible_state_count
quality_flag
admin_roster_version
artifact_build_id
```

The five-band migration must replace the current four-band fields and distributions throughout the
Overview; all assignment uses full-precision scores. National maps use State/UT concentration
colour with district tint as supporting within-State variation. State maps end that encoding and
use District Bundle Score directly on the fixed 0–100 display scale. District maps use Block Bundle
Score on that same fixed display scale, without inherited parent hue.

Production work must also implement the canonical route registries, selector/filter/geography
state transitions, quality states, stable administrative-ID joins, immutable roster/build identity,
progressive geometry loading, and accessibility requirements defined above. Block geometry should
load only after State selection; Block attributes and interaction should activate only in District
view.

## 10. Minimum acceptance-test contract

At minimum, synthetic and artifact-contract tests must prove:

### Ranking and coverage

- national State ranking uses concentration rather than State mean;
- District ranking is within the selected State/UT only;
- Block ranking is within the selected District only;
- rank is suppressed when coverage is below 90% or `n_valid < 10`;
- concentration is also suppressed when coverage is below 90%;
- valid individual scores remain visible when parent-cohort rank is suppressed;
- competition-ranking ties and rank denominators are correct;
- `n_expected` comes from the versioned roster rather than score rows; and
- missing score rows reduce coverage and never participate in ranking.

### Precision and bands

- threshold `>= 50`, five-band assignment, eligibility, and ranking use full precision;
- display rounding cannot change a threshold result, band, rank, or tie; and
- the legacy four-band labels cannot enter new Overview artifacts.

### Interaction and navigation

- filtering never recomputes rank or clears a selected geography outside the active band;
- Bundle, Scenario, Period, and administrative-level changes clear the local band filter;
- a roster-valid no-data geography remains selected with a no-data state;
- an obsolete or unsupported Block, District, or State falls back only to its nearest valid parent;
- Block inspection does not create a fourth breadcrumb level; and
- Detailed Analysis routing and return preserve only the declared durable state.

### Maps, routing, and data states

- India, State, and District views use their declared legend titles and encodings;
- the same lower-level score maps to the same colour regardless of parent geography;
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
- Maps use continuous fixed `0-100` score or concentration colour domains with view-specific legend
  titles; discrete bands are interpretive and interactive supporting evidence.
- Switching levels does not clear or silently reinterpret the user's selections.
- Exposure and hydrology remain clearly identified as contextual information unless they are
  explicitly included in a score.
- Missing or partial data is visible and does not silently become a valid-looking score.
- National screening results are never described as absolute interstate climate-risk scores.
- Coverage validity and denominator-based ranking eligibility remain explicit and separate.
- Rankings use top-10 shortlists, competition ranks, stable alphabetical or administrative-code
  display order within ties, and original ranks under filtering.
- Geometry is loaded progressively: national district context first, State-scoped block outlines
  after State selection, and block attributes/interactions only when needed.
- The interface answers a user question before offering additional analytical controls.
