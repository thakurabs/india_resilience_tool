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
- a `High Bundle-Score Concentration (%)` legend;
- the eligible states and union territories with the highest concentrations; and
- a concise explanation of what the screening statistic represents.

The national map should not average State/cohort-normalized district or block composite scores
and present the result as an absolute pan-India climate-risk comparison. Instead, the Phase 1
district overview should use `High Bundle-Score Concentration (%)`:

```text
High Bundle-Score Concentration (%)
=
100 x
(number of valid districts with bundle composite score >= 50)
/
(number of valid districts)
```

The fixed threshold of `50` corresponds to the persisted Glance `High` and `Very High` score
bands. The Glance score-band contract is `Low` for scores below 25, `Moderate` for scores from 25
to below 50, `High` for scores from 50 to below 75, and `Very High` for scores of at least 75.
The concentration statistic ranges from 0%, when no valid district reaches the threshold, to
100%, when every valid district reaches it.

The national statistic answers:

> Within this State/UT, what proportion of valid districts receive a High or Very High score under
> the selected IRT bundle methodology?

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

The initial national screening contract is:

```text
coverage < 90%
    -> show Insufficient coverage
    -> suppress the concentration percentage and national rank

coverage >= 90% and n_valid < 10
    -> show the concentration percentage
    -> suppress the national rank
    -> flag Small cohort

coverage >= 90% and n_valid >= 10
    -> show the concentration percentage
    -> eligible for national ranking
```

For interpretation, `n_valid < 5` may be labelled `Very small cohort` and `n_valid = 5-9` may be
labelled `Small cohort`; both remain unranked. State/UTs with no valid composite data should show
`Data not available`. National rankings should use competition ranks, so identical percentages
receive the same rank and the following rank reflects the number of preceding entries.

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
```

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
one continuous `High Bundle-Score Concentration (%)` colourbar with fixed numeric ticks. The scale
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
- High Bundle-Score Concentration (%);
- districts meeting the threshold over valid districts;
- valid districts over expected districts and the coverage percentage;
- national rank and the number of rank-eligible State/UTs, when eligible; and
- a quality flag and an explanation that rank is suppressed, when not eligible.

District names, district composite scores, and district score bands should not appear in the
pan-India tooltip even though district boundaries and tint variation remain visible on the map.

Clicking anywhere within a state should select and zoom to that state, transition to the district
analysis view, replace the national concentration legend with the four persisted Glance score
bands, and enable district-level hover and selection. The visual hierarchy is therefore:

```text
National view: continuous State/UT concentration colour + district composite-score tint
    → select state
State view: four district score bands + district-level interaction
```

The threshold statistic is not the complete evidence surface. The selected State/UT overview
should retain the full district bundle-score distribution alongside the headline count, for
example:

```text
High Bundle-Score Concentration
11 of 33 valid districts >= 50

Full district bundle-score distribution
[distribution or score-band visualization]
```

The full distribution is necessary because values immediately below and far below the threshold
otherwise count identically, as do values immediately above and far above it.

The application should avoid an empty first screen that requires the user to complete a series
of controls before seeing any information. Defaults must be visible and clearly identified so
the user understands what is being shown without mistaking them for personal selections.

The suitability of the specific public bundle, scenario, and period defaults should be reviewed
separately from the workflow design.

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

Coordinate analysis should remain available as an alternate location-entry path, but it should
not compete visually with the default geography-first workflow. A clear action such as
`Analyse a custom location` can reveal manual coordinate and file-upload controls when needed.

## 3. Present a direct answer, not merely visualizations

After a geography is selected, the first summary should answer the user's likely question in
plain language. For example:

> Warangal has a Heat Risk bundle score of 72, in the Very High score band and ranking 4 of 33
> districts in Telangana. Its strongest drivers are X, Y, and Z.

The answer card should contain:

- bundle score;
- score band;
- rank within the declared State/cohort comparison group;
- difference from the State/cohort mean or median, when useful;
- two or three strongest drivers; and
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
distribution chart should allow a user to select a score band and filter the ranking table to the
corresponding locations. The active filter and comparison scope should remain visible.

Comparison should be a deliberate follow-up action. It should not add controls to the initial
path before the user has understood the first result.

Overview exports should focus on the current answer and visible evidence:

- copyable answer text;
- the visible ranking rows; and
- an answer pack containing the current context, drivers, metadata, and method note.

## 5. Treat exposure and hydrology as context, not primary filters

Exposure and hydrological information should support interpretation without competing with the
main bundle-score or hazard-pressure question.

In Overview:

- place exposure overlays under a collapsed `Context layers` control;
- allow a map layer to be selected independently of the risk analysis;
- show an Exposure Summary only after a single district, block, or coordinate is selected;
- show Hydrological Context only after a single location is selected;
- keep basin, sub-basin, and river-network overlays optional; and
- do not blend exposure or hydrological context into the displayed hazard score unless the
  methodology explicitly defines that relationship.

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

Bundle-level drivers should provide a path to their underlying metrics or rules. Where the data
supports it, the user should be able to move from the bundle score to the contributing metric,
its raw value, its normalized contribution, and the method used to include it in the composite.

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
- Switching levels does not clear or silently reinterpret the user's selections.
- Exposure and hydrology remain clearly identified as contextual information unless they are
  explicitly included in a score.
- Missing or partial data is visible and does not silently become a valid-looking score.
- National screening results are never described as absolute interstate climate-risk scores.
- Coverage validity and denominator-based ranking eligibility remain explicit and separate.
- The interface answers a user question before offering additional analytical controls.
