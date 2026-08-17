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
- a clearly labelled default risk bundle;
- a clearly labelled default scenario-period;
- a risk-band legend;
- the top hotspot states; and
- one sentence explaining what the score represents.

The application should avoid an empty first screen that requires the user to complete a series
of controls before seeing any information. Defaults must be visible and clearly identified so
the user understands what is being shown without mistaking them for personal selections.

The current Streamlit implementation demonstrates this pattern with a default bundle and
scenario-period. The suitability of the specific public defaults should be reviewed separately
from the workflow design.

## 2. Limit the primary controls to three

The Overview should expose only three primary inputs:

1. Geography
2. Risk Bundle
3. Scenario–Period

Constituent metric, statistic, map mode, model controls, and detailed chart options belong in
Detailed Analysis.

Administrative level should appear contextually through the geographic drill-down:

```text
India → States → Districts → Blocks
```

The user should be able to navigate geography through search or direct map interaction. This is
easier to understand than requiring every geographic dimension to be configured before the map
becomes useful.

Coordinate analysis should remain available as an alternate location-entry path, but it should
not compete visually with the default geography-first workflow. A clear action such as
`Analyse a custom location` can reveal manual coordinate and file-upload controls when needed.

## 3. Present a direct answer, not merely visualizations

After a geography is selected, the first summary should answer the user's likely question in
plain language. For example:

> Warangal has a High Heat Risk score of 72, ranking 4 of 33 districts in Telangana. Its
> strongest drivers are X, Y, and Z.

The answer card should contain:

- bundle score;
- risk band;
- rank within the relevant comparison group;
- difference from the state or national average;
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

The hotspot list should provide direct navigation to a selected geography. The risk-distribution
chart should allow a user to select a risk band and filter the ranking table to the corresponding
locations. The active filter and comparison scope should remain visible.

Comparison should be a deliberate follow-up action. It should not add controls to the initial
path before the user has understood the first result.

Overview exports should focus on the current answer and visible evidence:

- copyable answer text;
- the visible ranking rows; and
- an answer pack containing the current context, drivers, metadata, and method note.

## 5. Treat exposure and hydrology as context, not primary filters

Exposure and hydrological information should support interpretation without competing with the
main risk question.

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
- risk bundle;
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
- scenario-period;
- selected risk-band filter; and
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
- No more than three primary inputs are needed for a quick analysis.
- Each screen has one visually dominant next action.
- Advanced capability is discoverable without being compulsory.
- Overview and Detailed Analysis use one canonical analysis context.
- Scores, bands, ranks, legends, and comparison scopes remain consistent between levels.
- Switching levels does not clear or silently reinterpret the user's selections.
- Exposure and hydrology remain clearly identified as contextual information unless they are
  explicitly included in a score.
- Missing or partial data is visible and does not silently become a valid-looking score.
- The interface answers a user question before offering additional analytical controls.
