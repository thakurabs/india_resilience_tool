# Recommended Target Workflow

> **Revision — 2026-09-09.** Section 1 has been amended for the frozen national scoring scale.
> `Elevated Bundle-Score Concentration (%)` and its `>= 50` threshold are withdrawn; the national
> map paints districts directly from their composite scores, the State view paints blocks, and
> the State/UT headline is the area-weighted mean of its district scores. Three earlier rules
> reverse: a State mean is now permitted as the national value, national results *are* absolute
> interstate comparisons, and `coverage_fraction` moves from a shipped field to a build gate.
> A district selection is an inspection state inside the State view; the District-view layout
> contract and the third breadcrumb level are withdrawn. **Both were reversed on 2026-09-16; see
> the revision below and section 1.**
>
> Rationale, evidence and the vendor handoff contract:
> [`docs/composite_scale_decisions.md`](docs/composite_scale_decisions.md).
>
> **Revision — 2026-09-16 (CHG-0489).** A new section 0 states the finalised Overview journey and
> governs the clause-level sections beneath it. Section 11 records the settled user-facing naming.
> Three contracts from the 2026-09-09 amendment above are **reversed**, each for a reason recorded
> at its site: District is a navigation level again, with its own breadcrumb entry; blocks are
> ranked within their own district, though nowhere above it; and the top score band is `Very High`
> rather than `Extreme`. A block remains an inspection state and still adds no breadcrumb level.
> Interface copy throughout now follows section 11; identifiers and methodology prose are
> unchanged.
>
> Demonstrated in
> [`docs/diagnostics/heat_risk_pilot/irt_target_prototype.html`](docs/diagnostics/heat_risk_pilot/irt_target_prototype.html).

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

## 0. The Overview journey

This section is the finalised flow definition. Sections 1-10 below hold the clause-level detail
it refers down into; where the two disagree, this section governs and the clause is a defect.

### The job

> A user who must decide **where to act** opens IRT, sees the national picture for a default
> hazard, identifies which places carry the most, drills into one, understands why, and either
> builds a shortlist or hands off to Detailed Analysis.

IRT stops at diagnosis. Intervention, costing and prioritisation are explicitly not in this
journey; they hand off to a separate future capability.

### The audience

Four kinds of user enter through the same front door: an investor, a policy maker, a researcher,
and an informed citizen. None of them is offered a different entry point, and no step assumes
climate-science vocabulary. This is the constraint that drives section 11: a term that only the
researcher understands is a defect even when it is technically the most precise term available.

### The spine

The journey is a spine of seven steps with three doorways set perpendicular to it. It is not a
nine-step line. The distinction matters: under a linear reading, coordinate entry, Detailed
Analysis and the report have to be squeezed in as steps 8, 9 and 10, and each then appears to
belong at one point in the flow. They do not. Each is reachable wherever a unit is named, and
each returns the user to where they left.

```text
1 arrive → 2 orient → 3 scan → 4 narrow → 5 understand → 6 localise → 7 shortlist

        doorways, reachable wherever a unit is named:
            · coordinate entry     "I know my place, not a region"
            · Detailed Analysis    "interrogate this"
            · Download report      "leave with it"
```

**1. Arrive.** The first screen already carries a result: the India map painted from district
hazard scores, the default Risk Domain, scenario and period named as defaults, the frozen
`0-100` colourbar, the ranked State/UTs, the distribution, and the scope caveat. There is no
configuration step before information. Section 1 specifies this screen.

**2. Orient.** The user learns what the number is before using it: that it is a hazard score on
one national ruler, that it measures how severe the climate conditions are, and that it does not
account for how many people or assets are exposed or how well they can cope. Stated positively,
per section 11 — a low score means less hazard, never "safe" and never "good".

**3. Scan.** The user reads the national picture two ways at once: colour on the map, and the
ranked list of State/UTs beside it. The ranked list shows five highest and five lowest hazard,
with the full set one click away in the Ranking Table. The ten-bin distribution says how the
country is spread and can be pinned to filter the list.

**4. Narrow.** The user picks a State/UT — from the map, the ranked list, the distribution, or
geography search. All four routes do the same thing. The map repaints from block hazard scores,
the ranked list switches to that State/UT's districts, and the colourbar does not move.

**5. Understand.** The user reads why this place scores as it does: the score, its band, its rank
with the comparison set named in words, the top metrics behind it, and the block range. This is
the step the coordinate doorway rejoins, because a coordinate resolves to a block and the
question it answers is "why here", not "where".

**6. Localise.** The user opens a district and sees its own blocks painted and ranked within it.
District is a navigation level with its own breadcrumb entry; a block below it is an inspection
state and adds no level. This is where the journey reaches its finest grain.

**7. Shortlist.** The user adds places to `My Portfolio` with `Add to Analysis`, available
wherever a unit is named, and compares up to four of them on one axis at a time — either four
places at one future, or one place across four futures. The portfolio survives navigation and
Risk Domain changes. Beyond four, the question has become a ranking, which step 3 already
answers.

### The three doorways

Each doorway is reachable from any step where a unit is named, carries the analytical context
with it, and returns the user to the step they left with their selection intact.

**Coordinate entry.** For a user who knows their site but not its administrative region. Reached
through `Coordinate Analysis`. A coordinate resolves to the block containing it, and that
geographic basis is stated on screen. It rejoins the spine at step 5 — the user arrives holding a
place and needs to understand it, not to find it.

**Detailed Analysis.** For a user who wants to interrogate the result. One prominent action per
result, opening on the composite metric for the active Risk Domain with geography, level,
scenario, period and the selected metric preserved. `Back to Overview` restores the Overview
context, including the District and Block selections. Sections 6, 7 and 8 specify this.

**Download report.** For a user who needs to leave with the answer. A PDF carrying the current
answer, the visible evidence, the metadata and the method note, with a live link to the same
state on its cover. Its contents are specified at journey level only; the tier and exclusion
detail belongs to the vendor specification rather than here.

### Cross-journey rules

These hold at every step and inside every doorway.

- **One frozen ruler.** One national `0-100` domain, fitted once, never rescaled per selection.
  It ships baked inside the score, so the vendor performs no normalization and cannot
  reconstruct a per-State one.
- **The number is a hazard score.** Not a risk score. The Risk Domain names the subject; the
  score names what was measured. See section 11.
- **Geography is navigation, not a selector.** India → State/UT → District are breadcrumb levels;
  a block is an inspection state. Administrative level is never a fourth analysis control.
- **Three primary controls only.** Risk Domain, scenario, period. Everything else is in Detailed
  Analysis. The one control that is neither is `Local contrast`, and it does not sit with the
  three: it lives with the colourbar, because the colourbar is the only thing it changes.
- **The ranked list ranks what you would click next.** India ranks State/UTs, a State/UT ranks its
  districts, a district ranks its blocks. Blocks are ranked only within their own district, where
  one administration drew them all.
- **One statistic.** The State/UT figure is the area-weighted mean of its district scores. The
  control reads `Mean`; the weighting is in its tooltip. There is no second statistic.
- **`Add to Analysis` everywhere.** Wherever a unit is named, it can enter the portfolio. No
  separate picker exists.
- **Stops at diagnosis.** No step recommends an intervention, prices one, or ranks one.

### Deliberately out

Named here so later stages do not re-open them:

- A population-weighted State statistic. Not produced, not shipped, not displayed.
- A second distribution chart. The five-band bar chart is withdrawn; the bands survive as labels.
- A State/UT score-derived map fill. There is exactly one score-derived fill per view.
- Cross-Risk-Domain comparison. Scores are not comparable across Risk Domains, and the portfolio
  holds one Risk Domain at a time.
- A minimum size for a comparison set, and the `Small cohort` flag. Withdrawn; the consequence
  is recorded in section 1 instead.
- Block ranks at State or national scope, and blocks in an orderable rank column in the
  portfolio. A block's rank is meaningful only inside its own district.
- Intervention, cost and prioritisation. Handed off, not built here.

## 1. Open with an immediate national overview

The first screen should already contain a useful result:

- an India map;
- a clearly labelled default Risk Domain;
- a clearly labelled default scenario;
- a clearly labelled default period;
- a continuous hazard-score legend on the fixed `0-100` national scale;
- the states and union territories with the highest area-weighted mean district scores;
- a distribution of State/UT mean scores; and
- a concise explanation of what the score represents and what it excludes.

The national map paints **districts** directly from their hazard scores on the frozen
`0-100` national scale. Because every score comes from one frozen national ruler rather than a
per-State normalization, district scores are directly comparable between States, and a pan-India
district map is a valid absolute comparison rather than a screening proxy.

The State/UT headline statistic is the **area-weighted mean of its districts' hazard scores**.
The control that names it reads `Mean`; the area weighting belongs in that control's tooltip, not
in the label. There is no second State statistic: a population-weighted mean is not produced, not
shipped and not displayed. The area-weighted mean is not a percentile among States — it is the
area-weighted average of that State's districts' positions among India's districts. It must be
labelled as an average of its districts, never as a rank among States.

The State statistic is used for ranking, the answer card, the tooltip and exports. It is **not a
map encoding**: no State/UT polygon is filled from it at any zoom. The national map paints
districts; the State view paints blocks. Those are the only two score-derived fills in the
product.

`Elevated Bundle-Score Concentration (%)`, the fixed `>= 50` elevated-score threshold, and the
prohibition on a State mean as the national value are all withdrawn. The threshold saturated at
both ends of the slice grid — at the historical baseline more than half of all State/UTs sat at
exactly 0%, and at end-century the median exceeded 80% — and it tied large groups of State/UTs at
identical values. A ten-bin distribution of State/UT mean scores replaces it as the supporting
view of the complete distribution.

The national screening surface is limited to the 13 scenario-based thematic and sector-wise Risk
Domains. `Risk Domain` is IRT's own name for this selector and is what the user reads; the data
contract keeps `bundle_id` unchanged.

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

`Water Risk`, `Riverine Flood`, and other snapshot or standalone products are outside the scope of
this scenario-and-period screening surface. Their different temporal or normalization contracts
should not be silently mixed with the 13 eligible Risk Domains.

The public defaults are fixed as:

```text
Risk Domain: Heat Risk
Scenario: SSP5-8.5
Period:   2040-2060
```

The Overview uses the following user-facing labels while retaining the SSP and period identifiers
in the artifact and URL state:

```text
Middle of the road (SSP2-4.5)
Fossil-fuelled development (SSP5-8.5)

Early century (2020–2040)
Mid century (2040–2060)
End century (2060–2080)
```

These override IRT's existing `Business as usual` and `Pessimistic`; section 11 records why.

The national statistic answers:

> Within this State/UT, what is the area-weighted average hazard score of its districts, on the
> frozen national scale?

Because the scale is frozen nationally, this **is** an absolute interstate comparison and may be
described as one. The caveat that survives is about scope, not normalization: the score is
hazard-only, it does not include exposure, vulnerability or resilience, and it is not comparable
across Risk Domains. Avoid `national climate-risk score` and `risk score`, both of which imply the
excluded dimensions; the number is the `hazard score` everywhere. Section 11 fixes the wording of
the caveat itself.

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

coverage >= 90%
    -> publish the State mean
    -> rank it
```

Every State/UT that survives the build gate is ranked, every district with data is ranked within
its State/UT, and every block with data is ranked within its district. There is no minimum size
for a comparison set and no `Small cohort` flag: the earlier ten-unit rule is withdrawn as
complexity that cost more to communicate than it bought. One consequence is recorded rather than
hidden — a State/UT holding one valid district ranks on that district's score against one holding
seventy-five, so the ranking column carries units of very uneven statistical weight. This is a
display rule over unchanged scores and can be reinstated without touching an artifact.

If `n_valid = 0`, show `No data` and no score, band, rank, or metrics. A geography may remain
available for drill-down when usable lower-level data exists.

Ranking is hierarchical and comparison-set-specific, and each view ranks the units the user
would click next:

```text
National view
    Rank every State/UT by the area-weighted mean of its districts' hazard scores.

State view
    Rank every district with data in the selected State/UT, by hazard score.

District view
    Rank every block with data in the selected district, by hazard score.
```

The third tier is deliberately narrow, and it reverses the 2026-09-09 prohibition. The original
objection is what defines the scope of that reversal. The objection was never comparability —
block scores *are* nationally comparable under the frozen ruler. It was administrative unevenness:
subdivision density reflects State administration rather than geography, so a block ranking
spanning States compares units drawn by different administrations. **Inside one district that
objection does not arise**, because one administration drew every block in it. So a block carries
a rank within its own district and nowhere else.

Do not expose State-wide or national block ranks in Overview, and do not render block ranks as an
orderable column in `Compare Portfolio`, where members may come from different districts.

The national rank denominator is the number of State/UTs with a valid mean, the district rank
denominator the number of districts with data in the selected State/UT, and the block rank
denominator the number of blocks with data in the selected district. Missing or invalid units do
not participate in ranking, and `n_expected` must not be presented as the rank denominator when some
units are invalid. No separate `n_ranked` field or public term is required.

Rankings should use competition ranks, so identical values receive the same rank and the following
rank reflects the number of preceding entries. Alphabetical or stable administrative-code sorting
may order tied rows visually but must not break the statistical tie. Show five highest-hazard and
five lowest-hazard units by default, headed `Highest hazard` and `Lowest hazard` — never `worst`
and `best`, since a lowest-hazard place is not a good place — with the full set one click away in
the `Ranking Table`. Calculate band assignment and ranks from full-precision stored values; round
only for display. Exact full-precision equality receives a tied rank. If two unequal values appear
identical at the default display precision, the tooltip or expanded ranking should expose
sufficient additional decimal precision to explain their order. When a genuine tie exists,
user-facing text may say `Rank N (tied) of M units with data`, naming the set outright — `rank 4
of 33 districts in Telangana` — in preference to any word for the set.

Use the following display precision:

```text
Hazard scores:             1 decimal place
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
State/UT-district-block roster. Missing hazard scores reduce `n_valid`; they must not reduce
`n_expected`.

District scores, block scores, map geometry, and denominator artifacts must reference the same
administrative-roster version. The offline build should reject duplicate geographic keys,
unexpected units, missing parent keys, or a mismatch between score and boundary-roster versions.
Stable official geographic identifiers should be used for joins, comparison-set membership and
ranking, and retained alongside display names wherever the source provides them. Names are
presentation fields and must not be the primary production join keys. Moving the current
name-derived Glance keys to stable administrative identifiers is explicit production migration
work.

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
Risk Domain x Scenario x Period x administrative-level artifacts and must identify each artifact
path or identifier and version/checksum. Validation baselines must reference this same build
identity. Analytically meaningful changes to code, roster, inputs, or artifacts create a new
identity rather than mutating a validated release.

### Current case-study validation baseline

The refreshed national-screening case study successfully rebuilt the 13 eligible composite and
Glance artifact families against the canonical roster. The accepted validation baseline is:

```text
Eligible Risk Domains:        13
Scenarios:                     2
Periods:                       3
Administrative levels:        2
Threshold diagnostic rows:   28,080
Canonical districts:          784
Canonical blocks:            7,137
Optimized parity issues:       0
```

The refreshed artifacts also contain valid Heat Risk scores for all three districts in `Dadra,
Nagar Haveli, Daman & Diu`. Under the withdrawn ten-unit rule its national rank was suppressed; it
is now ranked on its three districts like any other State/UT.

This baseline predates the frozen-scale amendment and its threshold-diagnostic row count refers to
the withdrawn statistic. It must be rebuilt against the frozen ruler before it is cited again.

This case-study baseline is methodology evidence rather than a released provenance identity. The
production acceptance rebuild must record its build timestamp, source commit, manifest checksum,
and concrete `admin_roster_version` before these values become a release baseline.

### National map visual encoding

The pan-India map paints each district directly from its hazard score on the frozen
`0-100` domain, through the vendored `WhiteBlueGreenYellowRed` colour table sampled at 101 stops
starting at fraction `0.045` so that no valid score renders as pure white. Colour is a pure
function of the score — `index = round(score)` — with no binning, smoothing, or parent-geography
effect.

There is **one** continuous colourbar with fixed numeric ticks, titled level-neutrally (`<Risk
Domain> · hazard score`, not `District Bundle Score` or `Block Bundle Score`). One frozen ruler
means one legend; separate per-level titles would describe a distinction the methodology no longer
makes.

The domain never rescales. Not when the Risk Domain, scenario, or period changes, and not when a
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
as broken. `Local contrast` is an opt-in toggle **placed with the colourbar**, not among the
analysis selectors. It stretches the ramp to the extent of the units in view; the scores
themselves never change. It is never the landing state, says on the legend itself that the colours
have stopped being comparable, and is offered only when the view has a spread to stretch. Its
extent is computed from the visible scores at render time and is never a stored normalization
parameter.

The placement is a contract, not a layout preference. A control that changes what the colours mean
belongs against the legend that declares their meaning, where it cannot be hidden while the legend
it governs stays on screen. It was briefly folded into IRT's `Map Mode` selector and moved back for
exactly that reason.

`Map Mode` itself stays as IRT ships it — auto-filled to `Absolute value` and disabled on the
composite path — because the frozen ruler leaves it nothing to switch to. Its other as-built
option, a change-from-baseline view, is **not** adopted here: `baseline` and `delta_vs_baseline`
are null on the composite path in the deployed API, no historical composite slice is published, and
a signed change needs its own diverging ruler and legend rather than the `0-100` hazard ramp. It is
deferred, not declined.

`#d5d8dc` identifies missing composite data and carries its own legend swatch. A compact
persistent method note should explain the frozen scale, the boundary grammar,
quality flags, and the hazard-only interpretation boundary.

Colour interpolation should use a perceptual colour space such as OKLCH or CIELAB where the
implementation resamples the table.

No-data units must use one consistent neutral treatment and must never be mapped onto the valid
low-score end of the palette. Insufficient coverage is a quality flag rather than a
score value: preserve a calculable quantitative fill where permitted and add a secondary pattern
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
- the area-weighted mean of its districts' hazard scores, and its five-band classification;
- national rank and the number of ranked State/UTs; and
- a quality flag, when one applies.

District names, district hazard scores, and district score bands should not appear in the
pan-India tooltip even though districts are individually painted. Districts become inspectable
only after a State/UT is selected.

Clicking anywhere within a state should select and zoom to that state, load that State/UT's block
geometry and attributes, and repaint the map from **block** hazard scores. The district boundary
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
Very High: 80 <= score <= 100
```

The visual hierarchy is therefore:

```text
National view: continuous district hazard-score colour
               thick State/UT boundary, thin district boundary
    → select state
State view:    continuous block hazard-score colour
               thick district boundary, thin block boundary
               + ten-bin district score distribution, district and block inspection
```

Both views preserve the same fixed `0-100` colour domain and the same level-neutral legend title,
so identical colours retain identical score meanings across geography, administrative level,
Risk Domain, scenario, and period. The continuous map colourbar and the score distribution serve
different purposes: the colourbar encodes exact mapped scores, while the histogram shows how the
ranked units are spread and provides the filtering aid.

The State mean is not the complete evidence surface. The selected State/UT overview should retain
the full district score distribution alongside the headline, for example:

```text
Telangana — Heat Risk
77.8 mean district hazard score · High · 33 districts with data

Distribution of district scores · 33 districts
[ten-bin interactive histogram]
```

### Score distribution histogram

One distribution widget serves both views, and it follows one rule:

```text
The histogram bins the units the current view ranks,
never the units it paints.
```

```text
National view   36 State/UT area-weighted mean scores
                hover or click a bin -> emphasise those State/UTs' districts
                filters the State/UT ranking shortlist

State view      the selected State/UT's district hazard scores
                hover or click a bin -> emphasise those districts' blocks
                filters the district ranking shortlist
```

This keeps the histogram, the ranking column and the click target addressing one object: a bar
counts State/UTs, the ranking ranks State/UTs, and clicking one selects it. A histogram over the
painted units would break that chain — a bar would count districts while the only clickable thing
beneath it is a State/UT.

The widget uses **ten fixed bins of width 10 across the full `0-100` domain**, all ten always
drawn even when empty, on the same frozen scale as the map. Width 10 rather than the five bands'
20 is a resolution decision taken against the data: at width 20 the Telangana district
distribution collapses to `60-80: 17, 80-100: 16`, which reads as a coin flip, while at width 10
it climbs to a clear peak in `80-90`; Uttar Pradesh's peak bin is `70-80`, which at width 20
disappears inside a single `60-80: 47` bar. Empty bins carry information — six empty low bins over
Telangana say that nothing in Telangana is cool — and a fixed axis with all ten bars always drawn
lets two State/UTs be compared by shape.

The five-band bar chart is withdrawn. Two distribution widgets over the same units at different
resolutions is one too many. The five bands survive everywhere they do real work — the ranking
column, the answer card, the tooltip and the method note — and the histogram carries light band
ticks at the `20 / 40 / 60 / 80` bin edges so the verbal vocabulary stays anchored to the chart
without a second chart being drawn. Bin edges are arithmetic divisions of the fixed domain, not a
methodological cut point, and introduce no new threshold.

The widget must be titled by the units it counts, never generically:

```text
National view   Distribution of State/UT mean scores · 36 units
State view      Distribution of district scores · <n> districts with data
```

A mean is a summary, so a unit's bin does not constrain its children. At SSP2-4.5 mid-century
Haryana's mean of 56.5 sits in `50-60` while its districts run 31.2 to 65.1, so hovering that bin
emphasises districts painted in four different bin colours. That is correct behaviour, and the
title is what stops it being read as a claim about the districts. Alongside the national histogram,
show the painted units as plain figures rather than as a second chart:

```text
784 districts painted · median 46.5 · min–max 0.2–90.7
```

The interaction contract is:

```text
hover a bin     emphasise that bin's units on the map, muting rather than hiding
                the rest; the tooltip gives the bin range, the count and the names

click a bin     pin that emphasis and filter the ranking shortlist to those units;
                ranks keep their original values and are never recomputed

click again     clear the pin; `Clear filter` clears it equally

select a unit   proceeds normally whether or not a bin is pinned; selection
                outranks filter emphasis, so a selected unit outside the pinned
                bin stays visible and selected

Risk Domain, scenario, period or view change the pinned bin clears
```

One bin is pinned at a time. Zero-count bins remain drawn and are not clickable. Counts are the
default display; percentages and the exact bin range belong in the tooltip.

Note that the five-band cuts become close to definitional under a rank-based ruler, where `>= 80`
reads as "worse than 80% of pooled national observations". Whether to state that plainly or to set
the cuts from physical values is an open methodological question; see
[`docs/composite_scale_decisions.md`](docs/composite_scale_decisions.md), Part E. The histogram's
bin edges are unaffected either way.

The State-view layout contract is:

```text
Header
    State/UT name + persistent Risk Domain, Scenario, and Period selectors

Headline
    Mean district hazard score + five-band classification
    Districts with data + national rank

Map
    Continuous block composite-score colour + district interaction
    Thick district boundary, thin block boundary

Supporting evidence
    Ten-bin district score distribution + ranked district list + top metrics
    District inspection panel when a district is selected; block panel beneath it when a
    block within that district is selected

Context and Evidence
    Collapsed exposure, hydrology, data-quality, and optional-overlay content

Navigation
    India > State/UT breadcrumb; a selected district extends it to
    India > State/UT > District
```

The district ranking denominator is the number of districts with data in the selected State/UT,
not the number expected when some scores are invalid.

A district selection is a **navigation level**, and the breadcrumb reads
`India > State/UT > District`. This reverses the 2026-09-09 withdrawal. The withdrawal reasoned
from the map alone — a District view repaints the same blocks, on the same ruler, against the same
colourbar, at a smaller extent — and that reasoning still holds for the map. It was the wrong test.
What makes the District a level is not a new fill but a new **ranked set**: the District view
ranks that district's blocks, which no other view does, and a level that ranks something of its own
is a place the user navigates to rather than a state they inspect from elsewhere. The map encoding
is unchanged on entry: block fill is retained, and the colourbar, its title and its domain do not
change.

A **block** remains an inspection state and adds no breadcrumb level, because it ranks nothing and
paints nothing of its own.

Selecting a district promotes its outline to the accent selection stroke and shows:

```text
District inspection panel
    District name + parent State/UT
    District hazard score + five-band classification
    District rank within the selected State/UT
    Relevant coverage or quality status
    Up to three valid District-scoped metrics or rule signals
    The score range of its blocks, with the block count
```

A district may be selected from the map, from the district distribution, or from the ranked
district list. Selecting it opens the District view, which ranks its blocks. The block range shown
in the inspection panel is a minimum, maximum and count on the frozen scale — a summary, not the
ranking itself.

The District view **inherits the parent State/UT's district distribution** rather than binning its
own blocks, and its histogram title says so. This is the one place the "bin what the view ranks"
rule of the histogram contract is deliberately relaxed, and the reason is the same one that
retired the five-band chart at this scope: the median district holds 8 blocks and the range runs 1
to 38, so ten bins over 8 units is noise rather than a distribution.

Districts and blocks are two independent inspection targets within one view, and their precedence
is explicit. Selecting a district clears any selected block. Selecting a block inside the selected
district retains the district selection and adds the block panel beneath it. Selecting a block
outside it replaces the district selection with that block's parent district, so the block panel
always sits under its own district. At most one district and one block are selected at a time,
either may be cleared without leaving the State view, and neither adds a breadcrumb level.

A selected block shows its name, parent district and State/UT, its hazard score, five-band
classification, its rank within its own district, relevant data-quality state, and valid
Block-scoped metrics or rule signals. Do not show a State-wide or national block rank, a nested
distribution, or a duplicated Context and Evidence hierarchy. A block's rank within its own
district is shown; see the District view above. Show `View Detailed Analysis` only when a
registered valid route exists.

Do not rank districts by any block-derived statistic in Phase 1. Block information explains
within-district heterogeneity; it does not redefine the district's primary score.

The application should avoid an empty first screen that requires the user to complete a series
of controls before seeing any information. Defaults must be visible and clearly identified so
the user understands what is being shown without mistaking them for personal selections.

The public defaults are `Heat Risk`, `SSP5-8.5`, and `2040-2060`. They should remain visibly
identified as defaults rather than being mistaken for user-selected values.

## 2. Limit the primary controls to three

The Overview should expose only three primary analysis selectors:

1. Risk Domain
2. Scenario
3. Period

Constituent metric, statistic, map mode, model controls, and detailed chart options belong in
Detailed Analysis. `Statistic` is fixed at `Mean` and `Map Mode` at `Absolute value`, both shown
preset rather than removed, so the vendor can see they did not silently disappear. `Local
contrast` is not one of these selectors; it sits with the colourbar, per section 1.

The `Select Resilience Filters` panel retains the same editable Risk Domain, Scenario and
Period dropdowns in both expanded and collapsed states. The collapsed panel is a compact strip
with labelled controls and an `Expand` button; expanding reveals secondary presets (Composite
hazard score, Mean and Absolute value). Collapsing or expanding changes presentation only and
never changes a selection. Default-value identification remains visible in either state.
Controls wrap onto additional rows when space is limited rather than disappearing; full scenario
names and period ranges remain accessible through the dropdowns without expanding the panel.

Changing a primary selector immediately updates Overview while preserving supported geography
and portfolio membership. In places comparison, the header Scenario and Period govern every
displayed place. In futures comparison, each column retains its own scenario–period pair; the
comparison explains that header Scenario and Period update the main Overview result without
rewriting its columns. Risk Domain continues to govern both modes.

Administrative level should appear contextually through the geographic drill-down:

```text
India → State/UT → District      (three navigation levels)
    ↳ Block                      (an inspection state within the District view)
```

Geography should be navigated through search or direct map interaction rather than treated as a
fourth analysis selector. This is easier to understand than requiring every geographic dimension
to be configured before the map becomes useful.

Risk Domain, Scenario, and Period selections should persist throughout drill-down. Breadcrumbs
should provide the reversible geographic path, for example `India > Uttar Pradesh > Meerut`. A
block selection is an inspection state and does not extend the breadcrumb. Browser Back is outside
the Overview analytical-state model; breadcrumbs and in-application navigation are authoritative
for moving through analytical states. Existing application-shell Browser Back behavior is not
redefined by this workflow.

When Risk Domain, Scenario, or Period changes, preserve the current geography where it remains
supported; clear bin filters, hover, and temporary emphasis; and recompute scores, bands, ranks,
metrics, and distributions. If a geography still exists in the canonical roster but has no valid
score under the new selection, remain at that geography and show `No data`. Fall back through
`Block -> District -> State/UT -> India` only when the selected geography or level is genuinely
obsolete or unsupported, and explain what could not be restored.

Distribution bin filters are view-local transient state. Clear them when Risk Domain, Scenario,
Period, or view changes. Applying a filter must not clear an already selected geography that falls
outside the bin; selection takes precedence over filter emphasis. Breadcrumb navigation preserves
Risk Domain, Scenario, and Period, clears selections below the destination level, and clears the
previous view's filters. Portfolio membership is not transient state and is exempt from these
rules; see `Compare Portfolio` in section 4.

`Coordinate Analysis` — IRT's own name for it — remains available as an alternate location-entry
path, but must not compete visually with the default geography-first workflow. It reveals manual
coordinate and file-upload controls when needed, resolves an input to the block containing it, and
states that geographic basis on screen. It is a doorway rather than a step: it rejoins the spine at
step 5, because a user arriving with a coordinate already knows where and is asking why.

### Coordinate sites and portfolio identity

The portfolio accepts **administrative areas and named sites**. A site has an immutable
`site_id`, user name, entered WGS84 latitude/longitude (decimal degrees), and a separately
resolved block identifier with the boundary/roster release. Its identity is never the block ID.
An administrative block and two sites inside it are three distinct members. Names may be edited
without changing identity; editing coordinates requires a new resolution and explicit confirmation
before replacing the working site's location. Named analyses change only on explicit save/update.

Site scores, metrics, bands, context and ranks use the containing block's published assessment;
there is no point-level interpolation or additional site ranking. Show the site name, coordinates
and containing block together. Where two columns share a block, explain that their block-level
results are identical for the same domain and future. Each site occupies its own Overview column
and can be the futures subject or an advanced-comparison member. No score is invented for a
resolved block lacking data; it remains collectable with unavailable analytical results.

**Import is review, then collect, then choose comparison columns.** Manual entry and upload use
the same validation and membership rules. A review lists each input row, its name, resolution and
ready/duplicate/error status. Only an explicit `Add ready sites` action changes the portfolio;
it never changes the displayed comparison, advanced matrix, current geography or named analyses.
Errors remain visible after valid rows are added and can be corrected and resubmitted.

- Require a nonblank name and finite numeric latitude in [−90, 90] and longitude in [−180, 180].
  Blank, NaN and infinite values are invalid, never zero. Use WGS84 longitude/latitude for spatial
  resolution; label entry fields to avoid axis ambiguity.
- On import without an existing site ID, a repeated name (trimmed, case-insensitive, collapsed
  whitespace) plus exactly equal numeric coordinates is a duplicate, within the file or against
  collected sites. Skip it with the matching site's name. Never deduplicate by block or proximity.
  Different names at identical coordinates remain distinct, with a shared-location notice. To
  distinguish otherwise identical sites, the user supplies distinct names before import.
- Saved definitions restore by immutable site ID. A merge must not overwrite different coordinates
  under an existing site ID: show a conflict and require an explicit choice. Legacy coordinate
  members get stable migrated site identities; retain their original name and coordinates.
- Outside-boundary and ambiguous boundary intersections are unresolved; never choose a nearby
  block. A changed block assignment on reopening is also unresolved until explicitly accepted.
  Show saved and proposed geography where available. The valid remainder may open; unresolved
  sites stay listed, excluded from results until resolved.

The prototype supports CSV (`name,latitude,longitude`, including quoted fields) and manual entry,
using exact, labelled coordinate fixtures rather than pretending to perform a national spatial
lookup. Other deployed upload formats retain this review contract; production parsing and boundary
resolution use the existing web implementation. The prototype's named analyses last for the page
session, consistent with its existing persistence demonstration.

**Vendor acceptance journey:** load two differently named sites at the same worked coordinate,
one at a second coordinate, one repeated name/coordinate and one invalid coordinate. Review shows
three ready sites, one duplicate and one error. Adding collects three without displaying columns.
Select the two co-located sites and verify identical block assessments with distinct names; enter
futures for either and return; open advanced comparison and verify distinct selectable identities.
Save, remove one working site, and reopen using Replace: both identities, coordinates and the saved
Overview configuration return. Reimport skips the three existing sites. Also test an empty file,
all-invalid rows, one valid row, quoted names, out-of-range coordinates, an unresolved coordinate,
and a saved site whose boundary assignment changed. A saved-ID merge conflict must never overwrite
working coordinates silently.

Supported Risk Domain x Scenario x Period combinations come from the deployed artifact manifest.
Normal selectors must not offer unsupported combinations. An obsolete or invalid deep-linked
combination should fall back to a valid configured selection with a concise explanation. By
contrast, when an expected artifact is missing or unloadable, retain the requested selectors and
show an unavailable state; never silently substitute another scenario, period, or stale artifact.

During loading, preserve the page structure and selected geography, show an explicit loading
state, and disable interactions that depend on the incoming artifact. Never display old analytical
values under newly selected labels. A valid artifact with no valid data should show `No data`
for the affected geography while independently available Context and Evidence may remain visible.
An artifact-version mismatch must never render mixed-version analytical outputs: show a generic
unavailable state and retain technical details in logs and diagnostics.

## 3. Present a direct answer, not merely visualizations

After a geography is selected, the first summary should answer the user's likely question in
plain language. For example:

> Warangal has a Heat Risk hazard score of 72, ranking 4 of 33 districts in Telangana. Its
> five-band classification and strongest metrics are shown below.

The answer card should contain:

- hazard score;
- score band;
- rank within the comparison set, with that set named outright;
- the comparison scope;
- up to three strongest valid metrics or rule signals; and
- a concise interpretation boundary, such as `Hazard-only; does not include exposure,
  vulnerability, or resilience`.

The score, rank, and comparison scope must be understandable without requiring the user to
interpret several charts independently.

## 4. Make deeper Overview information optional

Supporting information should be available through secondary or expandable sections:

- `Where are the hotspots?`
- `Compare Portfolio`
- `Ranking Table`
- `Context layers`
- `Download report`

These sections should not all be expanded on first load.

The hotspot list should provide direct navigation to a selected geography. The score distribution
is not one of these collapsed sections: it is visible on the first screen and in the State view,
under the interaction contract above. Filtering must retain each location's original rank rather
than recalculating rank within the filtered subset, and the active filter and comparison scope
should remain visible. Rankings, answer cards and method copy should use the same band order
throughout: `Very Low`, `Low`, `Moderate`, `High`, `Very High`. `Extreme` is withdrawn as the top
band label: it reads as a physical claim, while every band edge on this ruler is positional.

### Compare Portfolio

Comparison should be a deliberate follow-up action. It should not add controls to the initial
path before the user has understood the first result: the tray is absent until the user puts
something in it.

The frozen national ruler is what makes this feature possible. Under per-State normalization two
districts in different States shared no scale and could not be compared; under one ruler fitted
once over the national district pool across every declared scenario and period, they can — and
so can one place against its own futures. Both comparisons are valid, and the tray supports both.

**One axis varies at a time.** The tray is in one of two modes, and the mode fixes what is held
constant:

```text
Places mode     slots differ by place
                scenario and period are locked to the header selectors and
                move every slot together

Futures mode    slots differ by future — one scenario and period pair per slot
                the place is fixed to one selected unit
```

A tray that allowed place and future to vary at once would produce confounded comparisons —
`Warangal at SSP2-4.5 early century` beside `Kozhikode at SSP5-8.5 end century` differ in two ways
and support no inference. The mode switch is what prevents that, and it is not optional.

**The portfolio and the comparison are two different things, and they are stored separately.**
The portfolio is the set of places the user has collected. The comparison configuration is a
choice of what to put side by side, made over that set. Conflating them is what makes a comparison
control destroy collected work: the user's shortlist is the expensive thing to rebuild, and a mode
switch is a cheap and frequently reversed act.

```text
portfolio                    the shared collection; one per session
  members                    stable identifier, display name, administrative level, and for a
                             coordinate member the entered coordinates and the user's site name
  no display cap             the portfolio is not limited to four

comparison configuration     a selection over the portfolio; does not own membership
  mode                       places | futures
  displayed                  up to 4 members, in places mode
  subject                    the one member held fixed, in futures mode only
  slices                     up to 4 scenario-and-period pairs, in futures mode only
```

Four is the **display** cap, not the portfolio cap. Beyond four columns, side-by-side reading
fails and the question has become a ranking, which the Overview already answers. A portfolio
larger than four is normal and is listed in full, with the displayed four chosen explicitly; the
Overview must never silently pick them, and must never drop the rest to fit.

Entering and leaving futures mode is reversible and **non-destructive**. From places mode the user
names one member as the subject; the tray seeds with the current scenario across all three periods.
Every other portfolio member is retained, and is simply not displayed while one place is held
fixed. Returning to places mode restores the previously displayed selection, not the subject
alone. Neither transition leaves the Overview, extends the breadcrumb, or removes a member.

Detailed Analysis reads the same portfolio and keeps its own, broader comparison configuration —
several metrics, several futures and several places at once. Returning to the Overview restores its previous
one-axis display and explicitly selected columns; it does not narrow the portfolio, and it does not
discard the Detailed Analysis configuration, which is restored on the next crossing. The
Overview's one-axis rule governs what may be shown side by side on the screening surface, never
what the user is allowed to have collected.

#### One feature, three selection shapes

IRT's deployed `Compare Portfolio` and the Overview's comparison are **the same feature**, not two.
Both are fed by the same act, `Add to Analysis` into `My Portfolio`, and both read that one
collection. What differs is only how many axes are allowed to vary at once:

```text
Overview, places     place varies (up to 4 columns); scenario and period pinned to the header
Overview, futures    scenario-and-period varies (up to 4 columns); the place pinned to one member
Detailed Analysis    place, scenario and period all vary — IRT's existing flat table, unchanged
```

The Overview form is IRT's table with two of its three selectors pinned. It is the same data path
and the same request, over a narrower choice. The vendor does not build a second comparison
screen; the existing one gains a constrained front. Columns rather than rows follow from the cap,
not from a design disagreement: at four or fewer, columns read side by side; past four, a flat
table is right, and that is where Detailed Analysis takes over.

The one-axis rule binds the **screening surface only**. The wide form, in which a row can differ
from its neighbour in two ways at once, stays available in Detailed Analysis, where the user has
chosen multivariate work and the surface can label it.

Three things the deployed feature must change to meet this contract:

- **A selection change never removes a member, on any surface.** Today the deployed portfolio is
  discarded on an Administrative-to-Coordinate switch (QA finding B4) — the same fault this
  section's separation of portfolio from comparison configuration removes. The stable identifier
  is what de-duplicates a member; the site name is a label, which settles QA finding C4 (one
  coordinate under two names became two rows).
- **Every column carries the band and the scoped rank string** (`18 of 33 districts in
  Telangana`), never a bare or sortable rank number. Neither the band nor the rank denominator is
  in the table endpoint's response today; either the table gains both, or the vendor derives the
  band from the shipped ruler artifact. This must be stated to the vendor explicitly.
- **Scores come from the frozen national ruler**, so the same place is the same number on every
  surface. Under the deployed per-State normalization it is not.

Adding a member is one control, `Add to Analysis`, wherever a unit is already named — the State/UT
headline, the district and block panels, a ranked row, the Ranking Table, and geography search. No
separate picker is introduced. The set it adds to is `My Portfolio`. That `Add to Analysis` feeds
`My Portfolio` is a mismatch, and it is IRT's own; inheriting it beats introducing a third name.

The tray survives geography navigation, view changes, and selector changes. Members are recomputed
in place, never discarded:

```text
scenario or period changes (places mode)  every slot recomputes on the new future
Risk Domain changes                       members are retained and repopulated
                                          with the new Risk Domain's scores,
                                          bands, ranks and metrics
view or geography changes                 the portfolio is unaffected
```

Retaining members across a Risk Domain change is deliberate. The units stay valid and the user's
shortlist is the expensive thing to rebuild; only the figures are replaced. Because the previous
Risk Domain's figures are gone and were never comparable to the new ones, the panel states the
active Risk Domain on its face and carries no residue of the previous one. Bin filters, hover and
temporary emphasis are transient and clear as they do elsewhere; tray membership is not transient
state.

The comparison panel presents one column per slot and one row per attribute: hazard score, band,
rank with its scope, metrics, and the block range with its count. Column headers carry the place
and its parent in places mode, and the scenario and period labels in futures mode.

Ranks compare only within one comparison set, so in places mode a rank must render as a scoped
string — `4 of 33 districts in Telangana` — on its own line, never as a sortable numeric column.
Two districts from different States hold ranks that cannot be ordered against each other, and a
numeric column is precisely the affordance that invites that false ordering. In futures mode the
comparison set is identical across slots and ranks *are* comparable, because the same units are
ranked on a different future; the panel may show rank movement directly. Where the number of units
with data differs between futures the denominators differ, and the panel says so in words rather
than presenting the movement as clean.

Differences are expressed in scale points. The ruler is rank-based, so a gap of 12.3 is a
difference in **position among India's districts** and must be labelled in those terms on screen —
never as a physical difference, a percentage, or a multiple, and not as a "percentile" in
user-facing copy.

Mixed administrative levels are permitted and must be labelled. A district and a block may share a
tray, because both are scored on the same ruler and a district and a block holding the same
physical value receive the same score. Each column states its level, since the rank and block-range
rows differ in kind between them. A block's rank is scoped to its own district, so two blocks from
different districts hold ranks that must not be ordered against each other.

Shared metrics are the analytical output. Metrics common to several columns should be visually
distinguished from those unique to one: a shared metric means one intervention addresses several
places, and a unique metric means it does not. Display follows the existing contract — at most
three, ordered by full-precision signal strength, with no numeric signal values shown.

Tray members visible in the current view carry a persistent outline in a hue distinct from the
accent selection stroke, and never as a fourth grey boundary weight. Members outside the current
view are simply not outlined; the panel is authoritative and the map emphasis is a convenience.
Precedence runs selection, then compare membership, then bin-filter emphasis.

One Risk Domain at a time. The portfolio holds locations and futures, never Risk Domain pairs.
Scores are not comparable across Risk Domains, and a comparison table is the single most likely
place for that prohibition to be breached.

The tray is the comparison context carried into Detailed Analysis where a route supports it, and
dropped cleanly where none does.

### Saved analyses

A saved analysis stores a **question, not an answer**. The question is durable: it is a set of
names and choices, and it stays meaningful across a rebuild. The answer is not: every score was
measured against a ruler, a roster and an artifact release, and each of those can be superseded.
Storing the answer and redisplaying it is how a stale number acquires the authority of a current
one.

The stored definition must be complete enough to reopen the analysis exactly as configured:

```text
analysis_id              stable, assigned on save
name                     the user's own, renameable
saved_at

geography                per member: stable administrative identifier, display name as saved,
                         administrative level
coordinate members       the entered latitude and longitude, and the user's site name,
                         in addition to the resolved block identifier
bundle_id                the Risk Domain
metric / rule ids        every constituent metric or rule signal the user had selected
slices                   every scenario-and-period pair the user had selected
statistic                where the surface permits a choice of statistic
comparison config        mode, displayed members, subject, slices, column order

release identity at save artifact release / build id, roster version, ruler id, scoring-method id
```

The metric and rule identifiers are not optional. An advanced comparison is defined largely by the
metrics the user chose; a definition that stores only places would silently discard that work on
reopening, while the interface claimed the capability was retained.

**Opening a saved analysis retrieves its results from the current compatible published artifact
release.** The surface states the release and scoring-method identity it resolved against, and
explains what has materially changed since the analysis was saved. The release identity recorded
at save time is what makes that explanation possible: a ruler identifier alone is not sufficient,
because data can be revised under an unchanged ruler, and places, metrics, supported combinations
and administrative boundaries can all change while the scale stays fixed.

Opening is an explicit choice between **replacing** the current working set and **merging** into
it. Neither is a default, because both destroy something: replacing discards an unsaved working
portfolio, merging changes the analysis the user just asked to see.

**Roster change is resolved per member, never in bulk.**

```text
renamed, same stable identifier    restores normally, under its current name; the saved display
                                   name may be shown as the name it was saved under
split, merged, retired identifier  unresolved: the member stays listed with the reason, and the
                                   surface offers an explicit resolution
ambiguous legacy name              unresolved for the same reason; a name is never resolved to a
                                   geography on the interface's own judgement
```

A partly resolvable analysis opens its valid remainder, with every unresolved member visible and
labelled. Geography is never silently substituted, and unresolved members are never quietly
dropped — an analysis that opens with four of six places and no notice is indistinguishable from
one that was saved with four.

### State lifetimes

Four lifetimes, and the trigger for each:

```text
hover and tooltip              ends when the interaction ends
pinned bin filter              until explicitly cleared, or invalidated by a declared
                               geography, view, Risk Domain, scenario, period or analysis
                               transition
working context and portfolio  survives navigation and refresh; cleared only by the declared
                               Reset and logout actions
named analyses                 persist until explicitly deleted
```

A pinned bin is not a gesture. It is a stated filter and it survives ordinary interaction; what
ends it is either the user clearing it or one of the transitions named above, and those
transitions are enumerated rather than left to implementation.

`Reset` returns the Overview to **India on the public defaults** — the same state a first arrival
produces — and clears the working context and the portfolio. It is not an empty application, and
it never touches a named analysis. `Clear Portfolio` empties the portfolio only, leaving geography,
selectors and filters exactly as they are; it is not a reset with a narrower name.

### Overview exports

Overview exports should focus on the current answer and visible evidence:

- copyable answer text;
- the visible ranking rows; and
- a `Download report` PDF carrying the current answer, the visible evidence, the metrics,
  the metadata and the method note, with a live link to the same state on its cover.

"Answer pack" was internal shorthand and does not appear on screen. The report's tier and
exclusion detail belongs to the vendor specification rather than to this document.

With an active bin filter, export the currently filtered rows only, retain their original
unfiltered ranks, record the active filter and selection metadata, and never recompute ranks inside
the exported subset.

## 5. Treat exposure and hydrology as context, not primary filters

Exposure and hydrological information should support interpretation without competing with the
main hazard-score question.

In Overview, a compact `Context and Evidence` section should be available from the State view
onward and collapsed by default:

- place exposure overlays under a collapsed `Context layers` control;
- allow a map layer to be selected independently of the risk analysis;
- show a State/UT-level `Exposure context` and `Water context` using appropriately aggregated
  context artifacts;
- show progressively more local context after a district, block, or coordinate is selected;
- keep basin, sub-basin, and river-network overlays optional; and
- do not blend exposure or hydrological context into the displayed hazard score unless the
  methodology explicitly defines that relationship.

Context and Evidence is supplementary. Its absence must not invalidate a hazard score, band, rank,
or metrics. Show only fields supported at the current administrative level; do not infer,
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
- keep exposure and hydrology contextual rather than silently incorporating them into the hazard
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

Entry intent is explicit:

- A place action opens that place's domain composite, even when a portfolio exists.
- A metric action opens that metric and initializes its refinement selector accordingly. Composite
  score and rank may accompany it only as labelled context, never as the metric result.
- A comparison action opens the existing advanced matrix, seeded with Overview's displayed places
  and current future, or its fixed subject and selected future pairs. Other portfolio members remain
  available to select. Once edited, `Resume advanced comparison` restores that separate configuration;
  `Start new advanced comparison from Overview` explicitly replaces it with the current Overview
  selection. Opening a single result must never overwrite the advanced configuration.

The transition should preserve:

- geography and administrative level;
- the selected unit;
- Risk Domain;
- scenario;
- period;
- the selected metric or rule when the action originated from one; and
- the current comparison context where compatible.

Do not carry hover, bin filter, tooltip, or temporary map emphasis into Detailed Analysis.

Use canonical Risk-Domain-to-Detailed-Analysis and metric/rule-to-Detailed-Analysis route
registries.
Do not infer destinations from labels or names. Preserve geography exactly where supported and do
not substitute another administrative level unless the registry explicitly defines that fallback.
If no valid route exists, do not show an active Detailed Analysis action.

A place-originated Detailed Analysis action should open on the composite metric corresponding to the selected Risk Domain. It
should not open with `Metric = All`, because `All` does not clearly communicate whether the user
is still viewing the same score.

The first Detailed Analysis state should reproduce the result the user selected in Overview.
This continuity allows the user to recognise the analysis before deciding whether to refine it.

## 7. Reveal advanced controls inside Detailed Analysis

Detailed Analysis should initially show the same selected result, followed by a collapsed
`Refine analysis` area containing advanced controls such as:

- constituent metric;
- alternative scenario;
- alternative period;
- model-member controls;
- trend and scenario-comparison controls; and
- methodological details.

Advanced controls should be disclosed in response to user intent instead of being prerequisites
for the first useful result.

Overview should reuse the existing persisted Glance driver contract rather than introduce a new
weighted-contribution calculation. For thematic Risk Domains, show `Top metrics`; for sectoral
Risk Domains, show `Top Rule Signals`. `Metric` is IRT's own word; the underlying `driver` field
names are unchanged. At State/UT scope, rank each available metric or rule using its existing mean
normalized score across districts with data. District and block inspection must use valid
persisted rows scoped to that exact administrative level. Do not infer, interpolate, or borrow
driver signals from another level.

These values should be described as normalized metrics or rule signals, not as percentage
shares of the composite. The interface must not claim, for example, that a metric `contributed 34%
of the composite`, and no additional weighted-contribution calculation is required for this
workflow. Show no more than three valid metrics or rules, ordered by full-precision signal strength,
without displaying their numeric signal values in Overview. If fewer than three exist, show only
those available. If none exist, show `Metric information is not available for this geography.` No
secondary tie-breaking rule is required at this stage.

Where a metric has a one-to-one underlying metric or rule route, selecting it should open Detailed
Analysis with the current geography, administrative level, Risk Domain, scenario and period
preserved, and the corresponding metric or rule selected. A sectoral rule without a one-to-one
Detailed Analysis target should be displayed as informative text and remain unclickable; it should
not be routed to an approximate or unrelated metric. Driver validity and route availability are
separate: a valid unroutable driver remains normally styled rather than being greyed out. A
canonical metric/rule-to-Detailed-Analysis route registry controls clickability.

## 8. Preserve a reversible return path

`Back to Overview` should restore the previous Overview context, including:

- geography;
- Risk Domain;
- scenario;
- period;
- the selected District navigation level and Block inspection state, where applicable;
- map/table mode and the Overview comparison mode, displayed places, fixed subject, future pairs,
  and the previous places selection held during futures mode;
- map extent; and
- major panel expansion state where technically supported.

Back is restoration, not an implicit apply action. Changes to geography, domain, scenario, period,
metric or statistic inside Detailed Analysis do not overwrite the saved Overview context. No
`Show this selection in Overview` action is required for this prototype; a future implementation
must make such a transfer explicit and validate the complete selection before applying any part.

The advanced matrix retains its independently editable places, metrics and future pairs, including
more than four places and multiple varying axes. Back never chooses four columns, simplifies the
matrix, or blocks return because the matrix cannot fit Overview. Incomplete configurations remain
editable and show which axis needs a selection; missing published values show `No data`.

Portfolio membership is shared, with these explicit exceptions to exact restoration:

- Adding a place in Detailed Analysis adds it to the portfolio without adding an Overview column.
- Unchecking a matrix place changes only the matrix selection. Removing it from the portfolio also
  removes it from both comparison configurations.
- A removed displayed place leaves its column absent on return, with a notice and no replacement.
- Removing the futures subject exits futures mode, restores the remaining previous place columns,
  and explains why futures comparison closed.
- Named analysis definitions change only through an explicit save/update action. Working edits
  must not mutate the definition from which a comparison was opened.

Do not restore hover, tooltip, bin-filter, or other temporary emphasis state.

If a Detailed Analysis selection cannot map directly to an Overview Risk Domain, returning should
restore the last valid Overview context rather than clearing or partially reconstructing the
analysis.

Vendor acceptance walkthrough for this transition:

1. Open the six-place saved example, retain its advanced definition, and note Overview's displayed
   columns, geography, future, map/table mode, extent and panel state.
2. Resume advanced comparison. Add a place, another metric and another future; Back must restore
   the original Overview with the new place collected but not displayed. Resume must retain all edits.
3. Remove a displayed member inside Detailed Analysis and return: its column is absent, with a
   notice and no substitute. Repeat with the futures subject: futures mode closes with an explanation.
4. Open a block metric: the requested metric is selected and composite facts are labelled context.
   Change the future and choose an unsupported geography; Back still restores the complete origin.
5. Clear an advanced axis, return and resume: the incomplete selection is retained with a useful
   message. Repeat with a single place and with all places removed. Named definitions remain intact.

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
national_rank
ranked_state_count
quality_flag
admin_roster_version
artifact_build_id
ruler_id
colour_scale_id
data_snapshot_hash
```

Per-unit district and block artifacts carry the stable administrative identifier, the geographic
key, `bundle_id`, `scenario`, `period`, `admin_level`, `score`, and quality flags — and nothing
else. `bundle_id` keeps its spelling: the user reads `Risk Domain`, the contract keeps the field.
They must not carry raw metric values, normalization parameters, ruler support, or per-State
extents. Omitting those inputs is what makes a per-State renormalization downstream
unconstructible rather than merely discouraged. The colour table and its domain ship alongside as
a versioned scale definition rather than as prose, so that rendering is a lookup with no judgement
in it.

The five-band migration must replace the current four-band fields and distributions throughout the
Overview; all assignment uses full-precision scores. National maps paint districts from their own
hazard scores on the frozen `0-100` scale, with State/UT polygons unfilled. State maps paint
blocks on that same scale, with no inherited parent hue and no change of colourbar. There is one
score-derived fill per view and one colourbar for both.

### Saved analyses and prior results

Saved analyses are configuration to be migrated, not results to be re-rendered. The migration
requirement is one sentence: **legacy results must never be presented as current-release
results.** Reopening a migrated analysis resolves it against the current compatible release and
shows an explicit migration notice.

That requirement does not license destroying anything. Legacy records may be retained internally,
including any stored results, without building a user-facing archive; their reproducibility value
is real, and whether it warrants an interface is deferred rather than answered here. What is
prohibited is a legacy score entering a current map, colourbar, band, rank, comparison column or
export.

Each migrated definition carries the artifact release, roster version, ruler and scoring-method
identities it was saved against. Without them a change notice can only say that something changed;
with them it can say what.

Production work must also implement the canonical route registries, selector/filter/geography
state transitions, quality states, stable administrative-ID joins, immutable roster/build identity,
progressive geometry loading, and accessibility requirements defined above. Block geometry and
Block attributes load together, only after State selection.

## 10. Minimum acceptance-test contract

At minimum, synthetic and artifact-contract tests must prove:

### Ranking and coverage

- national State ranking uses the area-weighted mean of its districts' hazard scores;
- District ranking is within the selected State/UT only, and block ranking within the selected
  district only;
- blocks are ranked within their own district and at no wider scope: no State-wide or national
  block rank exists, and no block rank is orderable in `Compare Portfolio`;
- every State/UT surviving the build gate carries a national rank, with no comparison-set-size
  minimum suppressing one, and a State/UT holding a single district with data still ranks;
- the artifact build fails when coverage is below 90%, rather than publishing a suppressed value;
- competition-ranking ties and rank denominators are correct at all three ranked levels;
- `n_expected` comes from the versioned roster rather than score rows; and
- missing score rows never participate in ranking.

### Precision and bands

- five-band assignment, eligibility, and ranking use full precision;
- artifacts store unrounded scores, and display rounding cannot change a band, rank, or tie; and
- the legacy four-band labels cannot enter new Overview artifacts.

### Interaction and navigation

- the histogram bins the ranked units, not the painted ones: 36 State/UT means nationally and the
  selected State/UT's districts in the State view, in ten fixed bins of width 10 over `0-100`;
- filtering never recomputes rank or clears a selected geography outside the active bin;
- Risk Domain, Scenario, Period, and view changes clear the pinned bin;
- a roster-valid no-data geography remains selected with a no-data state;
- an obsolete or unsupported Block, District, or State falls back only to its nearest valid parent;
- a district selection extends the breadcrumb to `India > State/UT > District`, and a block
  selection extends it no further;
- selecting a district clears a selected block; selecting a block outside the selected district
  reselects that block's parent district; at most one district and one block are selected at once;
- a block is ranked within its own district and at no wider scope, and no district is ranked by a
  block-derived statistic; and
- Detailed Analysis routing and return preserve only the declared durable state.

### Comparison

- the comparison displays at most four columns, and place and future never vary at once; the
  portfolio itself carries no such cap and a portfolio larger than four is listed in full;
- entering futures mode fixes one subject and displays it alone, retaining every other portfolio
  member; leaving futures mode restores the previously displayed selection rather than the subject
  alone, and no mode transition removes a member;
- returning from a Detailed Analysis comparison narrows the display without narrowing the
  portfolio or discarding the Detailed Analysis configuration;
- scenario or period changes recompute every places-mode slot on the new slice;
- a Risk Domain change retains membership and replaces every figure, with no previous Risk
  Domain's value surviving in the panel;
- a cross-State places-mode comparison exposes no orderable rank column, while a futures-mode
  comparison may compare ranks, and names a differing units-with-data denominator in words;
- no block carries an orderable rank column in either mode;
- differences are reported in scale points and never as physical or percentage differences;
- compare emphasis never overrides a selection, and never renders as an administrative boundary
  weight; and
- portfolio membership survives geography and view changes, and is not cleared by the
  transient-state rules that clear bin filters and hover;
- a saved analysis stores geography identifiers, display names, `bundle_id`, metric and rule
  identifiers, slices, statistic and comparison configuration, and reopening restores every one of
  them;
- opening a saved analysis resolves against the current compatible release, states that release
  and scoring-method identity, and requires an explicit replace-or-merge choice;
- a saved member whose identifier was renamed restores normally, while a split, merged, retired or
  ambiguous member stays listed as unresolved with its reason and is never substituted or dropped;
- no legacy result is rendered in a current map, band, rank, comparison column or export; and
- `Reset` returns to India on public defaults without touching a named analysis, and
  `Clear Portfolio` leaves geography, selectors and filters intact.

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
- clickable metrics have registered routes and valid unroutable metrics remain visible;
- unsupported combinations, missing expected artifacts, version mismatch, and valid no-data
  artifacts have distinct behaviors; and
- loading never displays stale analytical values under new selector labels.

### Accessibility and provenance

- geography and histogram-bin filtering have keyboard-operable non-map alternatives;
- score, selection, and quality are communicated without relying on colour alone;
- small geographies remain selectable outside the map; and
- artifacts reject missing/mismatched roster versions, build identities, and stable administrative
  keys.

## 11. Settled user-facing naming

Ruled 2026-09-16 and demonstrated in
[`docs/diagnostics/heat_risk_pilot/irt_target_prototype.html`](docs/diagnostics/heat_risk_pilot/irt_target_prototype.html).
These are the words that appear on screen. They are settled; a later stage that wants to change
one is changing a decision, not filling a gap.

Two rules govern the table. **Identifiers are not renamed** — `bundle_id`, `n_valid`,
`coverage_fraction`, `n_expected`, `state_mean_area_weighted`, `ruler_id`, the API paths and the
13-row manifest keep their existing spellings, because renaming a field costs the vendor a
migration and buys the user nothing. **Methodology prose keeps its precision** — where a section
below is specifying how a number is computed, it may and should say "area-weighted mean" or
"national percentile". The table governs what the interface displays, not how the specification
reasons.

### What the number is called

| On screen | Not | Why |
|---|---|---|
| `hazard score` | risk score, national score, composite, index | The score carries no exposure and no vulnerability. One name, with `0-100` adjacent on first appearance. |
| `Heat Risk · hazard score` | Heat Risk score | The Risk Domain names the subject; the score names what was measured. |
| `Very Low` `Low` `Moderate` `High` `Very High` | `Extreme` for the top band | Symmetric, and positional rather than a physical claim on a percentile-derived ruler. |
| "higher than 84% of India's districts" | "84th percentile" | The comparison is the point; the statistical term is not. |
| "position among India's districts" | "national percentile position" | Same, for the axis label and the difference between two scores. |

The scope caveat is phrased positively, never as a disclaimer:

> The score measures the hazard itself: how severe the climate conditions are. It does not
> account for how many people or assets are exposed, or how well they can cope.

A low score means **less hazard**. It never means "safe" and never means "good".

### Controls and surfaces — IRT's own words win

Where IRT already has a name for a thing, IRT's name is used. Introducing a third name for a
control the vendor already ships costs them a rename and costs the user a relearn.

| Concept | Ships as | Previously called |
|---|---|---|
| The thematic or sector selector | `Risk Domain` | Bundle |
| The user's saved set of places | `My Portfolio` | My Analysis |
| The comparison surface | `Compare Portfolio` | Compare locations |
| The control that adds a place to it | `Add to Analysis` | — |
| The full ranked table | `Ranking Table` | View all N |
| The statistic control | `Mean` | Area-weighted mean |
| The view-extent toggle | `Local contrast`, a checkbox at the colourbar | — |
| The map colour selector | `Map Mode`, preset to `Absolute value` | — |
| The panels and filter groups | `Spatial Panel`, `Administrative Analysis`, `Coordinate Analysis`, `Select your views`, `Select Resilience Filters` | — |

`Add to Analysis` adding to `My Portfolio` is a mismatch, and it is IRT's own. Inheriting it beats
introducing a third name for the same act.

`Overview` and `Detailed Analysis` are ours, because IRT has no word for the Overview. `Download
report` ships; "answer pack" was internal shorthand and does not appear on screen.

### Jargon removed

| Ships as | Previously | Note |
|---|---|---|
| `Metric`, `Metrics`, `Top metrics` | driver, Drivers | IRT's word. "What drives this score" survives only as a heading verb. |
| "comparison set", or the set named outright | cohort | Naming the actual set — "of 33 districts in Telangana" — is better than either. |
| "districts with data", "blocks with data", "No data" | valid districts, No valid data | |
| "scenario and period", or "future" in the portfolio | slice | |
| `block` | — | **Kept.** It is the LGD level name and the data key. A State's own term — tehsil, taluk, mandal, circle — appears only in that unit's own label. |

### Scenario and period labels

| Ships as | Identifier retained in state and URL |
|---|---|
| `Middle of the road (SSP2-4.5)` | `ssp245` |
| `Fossil-fuelled development (SSP5-8.5)` | `ssp585` |
| `Early century (2020–2040)` | `2020-2040` |
| `Mid century (2040–2060)` | `2040-2060` |
| `End century (2060–2080)` | `2060-2080` |

The scenario labels are the one place this specification deliberately overrides IRT's existing
wording. IRT calls SSP2-4.5 `Business as usual`, which is indefensible — in the literature
business-as-usual is the high-emissions path, so the label attaches to the wrong scenario. IRT
calls SSP5-8.5 `Pessimistic`, which is a judgement about an outcome rather than a name for a
pathway. The SSP identifier is shown alongside the narrative name in both cases.

Ranked-list headings read `Highest hazard` and `Lowest hazard`, five and five. Never "worst" and
"best": the lowest-hazard places are not good places, they are places with less of this hazard.

## Core workflow principles

The workflow should be evaluated against the following principles during each section-level
refinement:

- The first screen provides information rather than setup work.
- No more than three primary analysis selectors — Risk Domain, scenario, period — are needed for
  a quick analysis.
- Each screen has one visually dominant next action.
- Advanced capability is discoverable without being compulsory.
- Overview and Detailed Analysis use one canonical analysis context.
- Scores, bands, ranks, legends, and comparison scopes remain consistent between levels.
- Maps use one continuous fixed `0-100` score colour domain and one level-neutral legend title at
  every level; one ten-bin histogram over the ranked units is the interactive supporting evidence,
  and the five bands are interpretive labels rather than a second chart.
- There is exactly one score-derived fill per view — districts nationally, blocks within a
  State/UT — and aggregate statistics are never rendered as a choropleth.
- Switching levels does not clear or silently reinterpret the user's selections.
- Exposure and hydrology remain clearly identified as contextual information unless they are
  explicitly included in a score.
- Missing or partial data is visible and does not silently become a valid-looking score.
- National results are absolute interstate hazard comparisons on one frozen scale; they remain
  hazard-only and are not comparable across Risk Domains.
- Coverage is enforced at the artifact build; every unit that survives the gate is ranked.
- Rankings use five-highest and five-lowest shortlists over the units the view would open next,
  competition ranks, stable alphabetical or administrative-code display order within ties, and
  original ranks under filtering.
- Geometry is loaded progressively: national district context first, then State-scoped block
  geometry and attributes together on State selection.
- The interface answers a user question before offering additional analytical controls.
