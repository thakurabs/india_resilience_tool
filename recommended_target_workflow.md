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

For interpretation, `n_valid < 5` may be labelled `Very small cohort` and `n_valid = 5-9` may be
labelled `Small cohort`; both remain unranked. Units with no valid composite data should show
`Data not available`. The same coverage and denominator logic should apply consistently to each
parent-child ranking cohort: State/UTs across India, districts within a selected State/UT, and
blocks within a selected district. A geography may remain available for drill-down when usable
lower-level data exists even if its parent-level concentration or rank is suppressed.

Rankings should use competition ranks, so identical values receive the same rank and the following
rank reflects the number of preceding entries. Population descending should determine display
order within a tie without changing the shared rank. If population is missing or also tied, use
alphabetical order as the deterministic fallback. Show a top-10 shortlist by default with a
`View all` action. Calculate ranks from the full unrounded concentration values; round only for
display. Exact unrounded equality receives a tied rank. If two unequal values appear identical at
the default display precision, the tooltip or expanded ranking should expose sufficient additional
decimal precision to explain their order.

### Expected denominator and boundary-vintage contract

The canonical Phase 1 administrative roster contains:

```text
784 districts
7,137 blocks
```

`n_expected` must come from this fixed, versioned canonical administrative roster, never from the
set of rows that happen to contain scores for the active bundle. District expectations should use
the canonical State/UT-district roster, while block expectations should use the canonical
State/UT-district-block roster. Missing bundle scores reduce `n_valid`; they must not reduce
`n_expected`.

District scores, block scores, map geometry, and denominator artifacts must reference the same
administrative-roster version. The offline build should reject duplicate geographic keys,
unexpected units, missing parent keys, or a mismatch between score and boundary-roster versions.
Stable official geographic identifiers should be retained alongside display names wherever the
source provides them.

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

State boundaries should use a strong neutral stroke, while district boundaries should use a thin,
subtle neutral stroke. Grey should identify missing composite data. A distinct dashed State/UT
outline may identify a small cohort whose percentage remains visible but is not rank eligible.
The map should not add a separate district-level colourbar because district tint is supporting
context rather than the national headline statistic. A compact persistent method note should
explain the State/UT statistic, district tint, quality flags, and interstate interpretation caveat.

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
Back and URL-compatible state should restore the previous valid geography, selectors, filters,
map extent, and Overview section where practical.

Coordinate analysis should remain available as an alternate location-entry path, but it should
not compete visually with the default geography-first workflow. A clear action such as
`Analyse a custom location` can reveal manual coordinate and file-upload controls when needed.

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
- up to five strongest metric drivers or rule signals; and
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
- period; and
- the current comparison context where compatible.

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
existing mean normalized score across valid districts. District and block views should use their
persisted unit-scoped rows, including the documented parent-district fallback when block-scoped
drivers are unavailable.

These values should be described as normalized metric drivers or rule signals, not as percentage
shares of the composite. The interface must not claim, for example, that a metric `contributed 34%
of the composite`, and no additional weighted-contribution calculation is required for this
workflow.

Where a driver has a one-to-one underlying metric or rule route, selecting it should open Detailed
Analysis with the current geography, administrative level, bundle, scenario, and period preserved
and the corresponding metric or rule selected. A sectoral rule without a one-to-one Detailed
Analysis target should be displayed as informative text and remain unclickable; it should not be
routed to an approximate or unrelated metric.

## 8. Preserve a reversible return path

`Back to Overview` should restore the previous Overview context, including:

- geography;
- bundle;
- scenario;
- period;
- selected score-band filter; and
- the last Overview section or comparison state where practical.

If a Detailed Analysis selection cannot map directly to an Overview bundle, returning should
restore the last valid Overview context rather than clearing or partially reconstructing the
analysis.

The browser Back button should also behave predictably. Navigation should not depend exclusively
on an in-page mode switch.

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
- Rankings use top-10 shortlists, competition ranks, population-descending display order within
  ties, and original ranks under filtering.
- Geometry is loaded progressively: national district context first, State-scoped block outlines
  after State selection, and block attributes/interactions only when needed.
- The interface answers a user question before offering additional analytical controls.
