# Figure Generation Instructions for Technical Guidance Note

Source note: `docs/technical_guidance_note.md`  
Source inventory: Appendix F in `docs/technical_guidance_note_review.md`

This document gives production instructions for the proposed new figures in the technical guidance note. It covers `FIG-01` through `FIG-23` only. The already reserved validation figures `FIG-V1` through `FIG-V5` are intentionally excluded because they are tied to the existing validation notebooks named in section 3.4 of the guidance note.

## General Production Rules

### Purpose

The figures should make the methodology easier to audit, not add new methodology. Every figure must be traceable to the prose, equations, or tables already present in `docs/technical_guidance_note.md`.

### Output Conventions

- Preferred editable source: SVG, PDF, or a notebook/script that can reproduce the figure.
- Preferred publication export: high-resolution PNG at 300 dpi and/or vector PDF.
- Suggested file naming:
  - `fig_01_pipeline_flow.svg`
  - `fig_01_pipeline_flow.png`
  - continue the same pattern through `fig_23_district_block_resolution.*`
- Suggested output directory: `docs/figures/technical_guidance_note/`.
- Keep figure IDs visible in source filenames, but captions in the note should use plain labels such as "Figure 1" once final numbering is chosen.

### Visual Style

- Use a restrained technical-report style: clean lines, light gridlines, clear labels, and high contrast.
- Avoid decorative gradients, drop shadows, or icon-heavy illustration unless they clarify the flow.
- Use a consistent palette across all figures:
  - Data/source inputs: neutral blue.
  - Processing steps: muted teal.
  - Climate metrics or hazards: amber/orange.
  - Composite scores or final outputs: red or dark rose.
  - Exposure/vulnerability terms, where shown as out of scope: grey.
- Use colorblind-safe choices; do not encode critical distinctions by hue alone.
- Prefer short labels inside figures and longer interpretation in captions.
- Use units directly on axes or legends.
- Use "higher = worse" consistently where scores are shown.

### Methodological Guardrails

- Do not introduce new thresholds, periods, scenarios, weights, or geography counts unless they already appear in `technical_guidance_note.md`.
- Use the exact time windows in the note:
  - historical raw span: 1950-2014
  - SSP raw span: 2015-2100
  - baseline/reference: 1990-2010
  - future windows: 2020-2040, 2040-2060, 2060-2080
  - static riverine flood period: `Current` under `Snapshot`
- Use the exact spatial domain in the note:
  - 68.0E-97.5E, 5.0N-45.0N
  - 0.25 deg x 0.25 deg grid
  - 118 x 160 cells
- Use the exact administrative counts stated in section 3.3:
  - 784 districts
  - 7,137 blocks
- Where a figure is synthetic, label it as "illustrative" in the caption or figure note.
- Where a figure uses real pipeline output, record the input artifact path, scenario, period, metric, geography level, state, and date generated in a small metadata note beside the source file or in the generating script.

### Caption Template

Use this caption structure unless the note's final style says otherwise:

`FIG-nn. Short title. One-sentence description of what is shown. Method note: [data source or "illustrative schematic"]; [scenario/period/state if data-backed]; units where relevant.`

### Quality Checklist

Before inserting any figure into `technical_guidance_note.md`, verify:

- The figure answers the section's main reader question.
- The figure does not contradict the prose, formulas, or tables.
- All units are present.
- All acronyms are either defined in the figure or already defined nearby in the note.
- Text remains legible when the figure is placed at expected report width.
- Synthetic values are visually plausible but not easily mistaken for observed or model-derived values.
- Maps include enough spatial context to interpret the geography.
- Any real-data figure can be regenerated from an explicit notebook, script, or documented command.

## FIG-01: End-to-End Pipeline Flow

Section anchor: section 1, especially the table "The note proceeds in the order a score is built".

Priority: Essential  
Type: Schematic  
Source/effort: Author-created schematic

### Purpose

Show the full methodological chain from source data to the published 0-100 composite score. This is the first reader orientation figure and should let a reviewer understand the note's structure at a glance.

### Core Message

IRT converts downscaled CMIP6 and static flood inputs into grid-first climate metrics, then into thematic and sectoral hazard-pressure bundles, and finally into a 0-100 higher-is-worse composite score.

### Content

Use a left-to-right pipeline with six main stages:

1. Climate and flood inputs
   - NASA-NEX GDDP-CMIP6
   - CEMS-GloFAS/JRC RP-100 flood layers
2. Downscaling and source preparation
   - BCSD already applied by NASA-NEX
   - India domain clipping
   - unit conversion
3. Grid-first index computation
   - annual per-cell climate indices
   - static flood raster metrics separately
4. Spatial and temporal aggregation
   - 0.25 deg grid -> district/block polygons
   - daily -> annual -> period mean -> ensemble mean
5. Bundle construction
   - thematic bundles: co-normalized hazard-family metrics
   - sectoral bundles: absolute/change/impact rule lenses
6. Output
   - 0-100 higher-is-worse composite
   - district and block views
   - scenario/period outputs

### Visual Layout

- Use a horizontal process flow.
- Put thematic and sectoral bundle construction as two parallel branches that rejoin at "Composite output".
- Show "hazard pressure only" as a small note beside the final output.
- Add a thin reference strip under the stages with section links: section 2 -> 3 -> 4 -> 5 -> 6/7 -> 8.

### Data Requirements

No real data required. This should be a hand-authored schematic based on the note.

### Avoid

- Do not show exposure or vulnerability as inputs to the composite. They are explicitly out of scope.
- Do not imply the dashboard recomputes scores at runtime.
- Do not mix static JRC flood layers into the SSP future chain.

### Acceptance Criteria

- A reader can follow the method sequence without reading the section table.
- The split between thematic and sectoral construction is visually obvious.
- The final output is clearly a hazard-pressure score, not realized impact or full risk.

## FIG-02: Hazard x Exposure x Vulnerability Decomposition

Section anchor: section 1, paragraph beginning "In the standard hazard x exposure x vulnerability decomposition".

Priority: Recommended  
Type: Schematic  
Source/effort: Author-created schematic using IPCC 2022 framing

### Purpose

Clarify that IRT supplies the hazard term only. This prevents readers from over-interpreting "Risk" bundle names as full risk scores.

### Core Message

Full climate risk requires hazard, exposure, and vulnerability. IRT produces the climate hazard-pressure layer and must be combined with exposure and vulnerability before being used as a full risk estimate.

### Content

Use a three-part interaction graphic, functional relationship, or Venn-style decomposition:

`Risk = f(Hazard, Exposure, Vulnerability)`

Mark:

- Hazard: highlighted and labelled "IRT output: climate hazard pressure"
- Exposure: greyed and labelled "Out of scope: people, assets, systems exposed"
- Vulnerability: greyed and labelled "Out of scope: sensitivity and adaptive capacity"

Add small examples below each term:

- Hazard: heat, drought, extreme rainfall, riverine flood
- Exposure: population, assets, crops, facilities
- Vulnerability: income, age, infrastructure condition, coping capacity

### Data Requirements

No data required.

### Avoid

- Do not show IRT output as the final risk result.
- Do not use multiplication graphics in a way that implies precise probabilistic risk modeling; if using the note's hazard x exposure x vulnerability shorthand, treat it as interaction framing rather than arithmetic.

### Acceptance Criteria

- The figure makes the scope caveat visually unmissable.
- The word "hazard-pressure" appears in or near the highlighted term.

## FIG-03: India Physiographic Zones and Hazard Portfolio

Section anchor: section 1, paragraph describing regional hazard portfolios from RBI 2023.

Priority: Optional  
Type: Map  
Source/effort: Author-created from RBI 2023 framing; needs base map

### Purpose

Show why a single national number is insufficient for India by mapping major physiographic regions to different hazard portfolios.

### Core Message

India's climate hazards are spatially heterogeneous: different regions face different dominant hazards, so district/block-resolved hazard-pressure outputs are necessary.

### Content

Create a simplified India map with five broad regions and representative hazard labels:

- Arid northwest: heatwaves
- Himalaya: landslides and cloudbursts
- Indo-Gangetic plains: river floods and heat
- Coasts and Ghats: cyclones and urban flooding
- Central peninsular plateau: drought, heat, and forest fire

### Visual Layout

- Use broad transparent overlays or callout regions rather than precise hazard boundaries.
- Add a small note: "Illustrative regional framing from RBI 2023; not an IRT output map."
- Use one or two short hazard labels per region; do not overcrowd the map.

### Data Requirements

- India state or national boundary base map.
- Optional simplified physiographic region polygons, if available.
- If no defensible region polygons are available, use callout arrows to approximate regions rather than drawing authoritative-looking boundaries.

### Avoid

- Do not imply these are computed hazard classes.
- Do not present region boundaries as exact.
- Do not add hazard types beyond those named in the note unless separately sourced and cited.

### Acceptance Criteria

- The map visually supports the argument for spatially differentiated hazard assessment.
- It is clearly labelled as an illustrative framing map, not a model output.

## FIG-04: India NEX-GDDP Domain and 0.25 Degree Grid

Section anchor: sections 2.3 and 3.3.

Priority: Recommended  
Type: Map  
Source/effort: Notebook/pipeline

### Purpose

Show the spatial domain and grid resolution used for IRT climate inputs.

### Core Message

IRT clips NEX-GDDP-CMIP6 to the India domain 68.0E-97.5E and 5.0N-45.0N, yielding a 118 x 160 grid at 0.25 deg resolution.

### Content

Map elements:

- India boundary.
- Domain rectangle: 68.0E-97.5E, 5.0N-45.0N.
- 0.25 deg grid overlay. For legibility, show full grid lightly or show a reduced subset/zoom plus domain annotation.
- Label: "118 columns x 160 rows".

### Data Requirements

- India boundary from the repo's canonical boundary artifacts or a trusted base map.
- Synthetic grid generated from the domain bounds and 0.25 deg spacing.

### Implementation Notes

- If the full 118 x 160 grid is too dense at page width, show:
  - the full domain rectangle on the main India map, and
  - an inset zoom with several 0.25 deg cells.
- Use longitude/latitude axes or corner labels.
- Keep the grid visually secondary to the India/domain outline.

### Avoid

- Do not call the 0.25 deg cells exactly 25 km everywhere. The note explains that physical size varies by latitude.
- Do not alter the domain bounds.

### Acceptance Criteria

- The reader can see both the national domain and the gridded nature of the data.
- The 118 x 160 count and 0.25 deg resolution appear explicitly.

## FIG-05: Temporal Coverage and Analysis Windows

Section anchor: section 2.2.

Priority: Recommended  
Type: Schematic timeline  
Source/effort: Author-created schematic

### Purpose

Clarify the relationship between raw data spans, the 1990-2010 baseline/reference period, and the future analysis windows.

### Core Message

Historical data span 1950-2014, SSP projections span 2015-2100, and IRT reports multi-year windows using 1990-2010 as the baseline/reference.

### Content

Create a horizontal timeline from 1950 to 2100.

Required bands:

- Historical raw span: 1950-2014.
- SSP2-4.5 and SSP5-8.5 raw span: 2015-2100.
- Baseline/reference: 1990-2010, highlighted within the historical span.
- Future analysis windows:
  - 2020-2040
  - 2040-2060
  - 2060-2080
- Static `Current`/`Snapshot` marker for Riverine Flood, separate from the SSP timeline.

### Visual Layout

- Use stacked timeline rows:
  - raw climate runs
  - analysis windows
  - static snapshot
- Show that 2040 and 2060 endpoints are shared by adjacent windows.
- Add a short note: "Future windows are inclusive 21-year means."

### Data Requirements

No data required.

### Avoid

- Do not show `Current` as a modeled near-present climate period.
- Do not imply historical 1990-2010 is a published sectoral output period; in sectoral bundles it is only the change-lens baseline.

### Acceptance Criteria

- The distinction between raw spans, analysis windows, baseline, and static snapshot is visually clear.

## FIG-06: BCSD Schematic

Section anchor: section 3.2.

Priority: Essential  
Type: Schematic  
Source/effort: Author-created schematic

### Purpose

Explain Bias Correction and Spatial Disaggregation (BCSD), the NASA-NEX downscaling method.

### Core Message

BCSD maps coarse GCM monthly distributions to observational distributions, then spatially disaggregates corrected fields to the 0.25 deg grid. It corrects marginal distributions but does not create new atmospheric dynamics.

### Content

Show a two-step flow:

1. Bias correction
   - coarse GCM monthly distribution
   - observed/reference distribution
   - quantile mapping
   - corrected GCM distribution
2. Spatial disaggregation
   - corrected coarse grid
   - bilinear interpolation/anomaly disaggregation
   - 0.25 deg target grid

Include the equation from section 3.2:

`x' = F_obs^-1(F_mod(x))`

### Visual Layout

- Left: coarse grid cells.
- Middle: CDF transfer diagram or quantile-mapping icon.
- Right: finer 0.25 deg grid.
- Add a caution strip: "Preserves GCM trend and variability; does not fix large-scale monsoon dynamics."

### Data Requirements

No real data required.

### Avoid

- Do not imply IRT performs the BCSD itself; the note says NASA applied it before release.
- Do not imply BCSD resolves convective storms or Himalayan terrain perfectly.

### Acceptance Criteria

- The two BCSD steps are visually distinct.
- The limitation of BCSD is present but not visually dominant.

## FIG-07: Quantile-Mapping Transfer Function

Section anchor: section 3.2, equation for `x' = F_obs^-1(F_mod(x))`.

Priority: Recommended  
Type: Synthetic plot  
Source/effort: Author-created or synthetic

### Purpose

Make the quantile-mapping equation intuitive.

### Core Message

A raw modeled value is converted to its model percentile, then mapped to the observed value at the same percentile.

### Content

Use a two-panel or three-panel plot:

- Panel A: model CDF `F_mod`, marking raw value `x` and percentile `p`.
- Panel B: observed/reference CDF `F_obs`, marking the same percentile `p` and corrected value `x'`.
- Optional Panel C: transfer curve from raw model value `x` to corrected value `x'`.

### Data Requirements

Synthetic distributions are acceptable and preferred for clarity. Use clearly labelled illustrative distributions, not real model output, unless a reproducible data example already exists.

### Visual Details

- Label axes generically as "Monthly value" and "Cumulative probability".
- Use one color for model and one for observed/reference.
- Show arrows: `x -> p -> x'`.

### Avoid

- Do not present synthetic values as measured values.
- Do not overfit the plot to daily extremes; section 3.2 describes monthly CDF bias correction.

### Acceptance Criteria

- The reader can understand the equation without following the algebra.

## FIG-08: District/Block Resolution Zoom with 0.25 Degree Cells

Section anchor: section 3.3 and section 8.2.

Priority: Essential  
Type: Map  
Source/effort: Notebook/pipeline

### Purpose

Show the practical resolution implication of the 0.25 deg grid for districts versus blocks.

### Core Message

Districts generally intersect multiple grid cells, while small blocks may intersect only one or a few cells. Block-level outputs are valid but carry higher spatial uncertainty.

### Content

Create a paired or inset map showing:

- One district boundary.
- Its constituent block boundaries.
- 0.25 deg grid-cell tiles overlaid.
- A district-scale view with several cells.
- A block-scale zoom showing one or a few cells crossing small blocks.

### Data Requirements

- Canonical district/block boundaries.
- Synthetic or actual 0.25 deg grid tile geometry.
- Prefer a representative state/district where block geometry and grid cells are easy to see. Avoid a tiny coastal/island region unless discussing coastal artifacts.

### Implementation Notes

- Choose a district with visually legible block boundaries and 4-20 intersecting cells.
- Label approximate cell size as "0.25 deg grid cell (~25 km scale; physical size varies by latitude)".
- If using real boundaries, record state, district, and boundary artifact path.

### Avoid

- Do not imply block scores are derived from district scores.
- Do not imply blocks between grid points receive no value; section 8.2 says each grid point is treated as a tile covering the full map.

### Acceptance Criteria

- The district and block resolution contrast is immediately visible.
- The figure can be reused near both section 3.3 and section 8.2.

## FIG-09: Admin-First vs Grid-First Worked Example

Section anchor: section 4.1.

Priority: Essential  
Type: Schematic  
Source/effort: Author-created schematic

### Purpose

Visually prove why grid-first computation is necessary for nonlinear indices.

### Core Message

Averaging raw daily values before thresholding can erase an extreme event. Computing the thresholded index per cell first preserves the event before area aggregation.

### Content

Use the exact conceptual example in section 4.1:

- City cell: five consecutive days at 36-38 deg C.
- Valley cell: same days at 28-30 deg C.
- Threshold: 35 deg C.
- Admin-first:
  - average first -> 32-34 deg C
  - hot days -> 0
- Grid-first:
  - city cell -> 5 hot days
  - valley cell -> 0 hot days
  - area-weighted mean -> 2.5 hot days

### Visual Layout

Make a split diagram:

- Left branch: "Admin-first"
  - two cells averaged into one time series
  - threshold line at 35 deg C
  - result: 0 hot days
- Right branch: "Grid-first"
  - threshold each cell separately
  - area-weighted aggregation
  - result: 2.5 hot days

### Data Requirements

No real data required.

### Avoid

- Do not use a mean daily temperature label if the example is about daily maximum-like threshold crossing; label the synthetic values simply as "daily cell temperature".
- Do not add extra days or thresholds.

### Acceptance Criteria

- The numerical contrast "0 vs 2.5 hot days" is prominent.
- The figure clearly shows the order-of-operations difference.

## FIG-10: Fractional-Area Overlap Weights

Section anchor: section 4.2.

Priority: Recommended  
Type: Schematic or buildable geometry plot  
Source/effort: Author-created or buildable

### Purpose

Explain how grid-cell values become district/block values.

### Core Message

IRT aggregates cell values to administrative polygons using intersection areas as weights, computed in an equal-area CRS.

### Content

Show an irregular administrative polygon over several square grid tiles.

Required annotations:

- Grid-cell values `v_j`.
- Intersection areas `a_ij`.
- Weighted average formula:
  `vbar_i = sum_j a_ij v_j / sum_j a_ij`
- Note: "Intersection areas computed after reprojection to EPSG:6933."

### Data Requirements

Synthetic geometry is acceptable. A real polygon/grid example is better if it remains legible.

### Visual Layout

- Shade the part of each grid cell intersecting the polygon.
- Use varying shade intensity or labels to show different overlap weights.
- Add a small side table listing `cell`, `value`, `overlap area`, and `contribution`.

### Avoid

- Do not show centroid-in-polygon as the method except perhaps as a crossed-out contrast.
- Do not compute areas in degrees.

### Acceptance Criteria

- The role of fractional areas is clear without reading the equation.
- EPSG:6933 equal-area calculation is explicitly noted.

## FIG-11: Temporal Aggregation and Ensemble Chain

Section anchor: section 4.3.

Priority: Recommended  
Type: Schematic  
Source/effort: Author-created schematic

### Purpose

Explain how daily model data are reduced to period and ensemble outputs.

### Core Message

IRT computes daily-to-annual indices per model, averages annual index fields over each multi-year period, then averages 24 model period means into the ensemble mean used by composites.

### Content

Show three stages:

1. Daily -> annual index
2. Annual -> period mean
3. Period mean -> 24-model ensemble mean

Include the retained but not composite-used uncertainty fields:

- standard deviation
- median
- 5th and 95th percentiles

### Visual Layout

- Use a "model fan" of 24 thin lines or stacked mini-bars converging to an ensemble mean.
- Show one period window, e.g. 2040-2060, as a highlighted band.
- Add a note: "Composite scores use the ensemble mean; spread statistics are retained for diagnostics."

### Data Requirements

Synthetic schematic is sufficient.

### Avoid

- Do not imply ensemble spread is currently surfaced in the composite output.
- Do not show a trend fit; the method uses period means, not trend slopes, for published composites.

### Acceptance Criteria

- The order time-average first, ensemble-average second is visually clear.

## FIG-12: DOY Percentile Threshold Curve

Section anchor: section 5.1, "DOY percentile threshold framework" and TX90p worked example.

Priority: Essential  
Type: Plot  
Source/effort: Notebook/pipeline preferred; synthetic acceptable only as a placeholder

### Purpose

Explain how TX90p/TN90p/WSDI/hwfi/hwa percentile thresholds are calibrated.

### Core Message

For each day of year, IRT pools baseline values within a +/-2-day window across 1990-2010, computes the relevant percentile threshold, and applies that fixed threshold curve to evaluation years.

### Content

Plot:

- x-axis: day of year, 1-365.
- y-axis: temperature, deg C.
- Smooth threshold curve `tau_d`, preferably 90th percentile for `tasmax`.
- Highlight day 121 / 1 May.
- Show +/-2-day pooling window around 1 May.
- Mark a few evaluation-year daily values above the threshold as exceedances.

### Data Requirements

Preferred:

- One real 0.25 deg grid cell from baseline years 1990-2010.
- Variable: `tasmax`.
- Percentile: 90th.

Fallback:

- Synthetic seasonal temperature curve with explicit "illustrative" label.

### Implementation Notes

- If using real data, the generating notebook/script should record:
  - model
  - grid-cell lat/lon
  - baseline years
  - quantile method
  - no-leap handling
- February 29 should be excluded in line with the note.
- Strict exceedance should be shown as `x_t > tau_d`.

### Avoid

- Do not use a single annual scalar percentile; this figure is for DOY-specific thresholds.
- Do not imply the threshold is recomputed for future periods.

### Acceptance Criteria

- The +/-2-day pooling concept is visible.
- The reader can see why the threshold is seasonal rather than flat.

## FIG-13: Heatwave Amplitude (`hwa`) Example

Section anchor: section 5.1, "Heatwave amplitude (hwa)".

Priority: Recommended  
Type: Plot  
Source/effort: Notebook/pipeline preferred

### Purpose

Show how the IRT-specific `hwa` metric is selected from heatwave spells.

### Core Message

The worst annual heatwave spell is the qualifying spell with the largest mean exceedance above the DOY threshold; `hwa` is the peak daily maximum temperature within that selected spell.

### Content

Use a time-series plot for one illustrative warm season or year:

- Daily `tasmax` line.
- DOY 90th percentile threshold curve `tau_d`.
- Highlight all days in qualifying spells of at least 5 consecutive exceedance days.
- Mark the selected worst spell.
- Mark:
  - mean exceedance for the spell
  - peak daily temperature within the spell = `hwa`

### Data Requirements

Preferred:

- Real grid-cell or admin-aggregated example from one model/year.

Fallback:

- Synthetic daily series labelled illustrative.

### Implementation Notes

- The metric uses minimum spell length 5 consecutive days.
- Spell evaluation is within a calendar year.
- `hwa` is the peak absolute Celsius value inside the worst spell, not the exceedance anomaly.

### Avoid

- Do not label the 5-day spell minimum as an IMD criterion. The note says it is an IRT/ETCCDI-style design choice.
- Do not carry spells across year boundaries.

### Acceptance Criteria

- The difference between "mean exceedance selects the spell" and "`hwa` is the peak temperature inside it" is clear.

## FIG-14: SPI Derivation

Section anchor: section 5.3.

Priority: Essential  
Type: Three-panel plot  
Source/effort: Notebook/pipeline preferred

### Purpose

Make the Standardised Precipitation Index derivation understandable.

### Core Message

Monthly precipitation is accumulated over a timescale, fitted to a Gamma distribution over 1990-2010, adjusted for zero-precipitation probability, and transformed to standard-normal SPI.

### Content

Use three panels:

1. Monthly precipitation accumulation
   - monthly totals or rolling 3-month/6-month/12-month accumulation
2. Gamma/mixed CDF fit
   - positive precipitation Gamma distribution
   - zero-month probability `q`
   - mixed CDF `H(x) = q + (1 - q)G(x)`
3. Normal-quantile transform
   - map `H(x)` to `SPI = Phi^-1(H(x))`
   - shade SPI < -1 as moderate drought threshold

### Data Requirements

Preferred:

- One representative grid cell and SPI-3 example from baseline calibration.

Fallback:

- Synthetic monthly precipitation distribution labelled illustrative.

### Implementation Notes

- The note says IRT uses the `climate_indices` Python package and Method of Moments Gamma fitting over 1990-2010.
- Month totals require at least 90 percent finite daily values.
- If showing drought events, keep the focus on SPI < -1 episodes.

### Avoid

- Do not use a normal fit to precipitation directly.
- Do not imply the Gamma parameters are refit for future SSP periods; the note says baseline parameters are applied unchanged.
- Do not imply monthly SPI values feed composites directly; composites consume the derived SPI < -1 event-count and maximum-spell metrics described in section 5.3.

### Acceptance Criteria

- The transformation from precipitation to dimensionless SPI is visually comprehensible.
- The SPI < -1 threshold is present.

## FIG-15: JRC RP-100 Severity Lookup Matrix

Section anchor: section 5.5.

Priority: Optional  
Type: Data visualization heatmap  
Source/effort: Author-created from table

### Purpose

Visualize the 5 x 5 matrix that combines RP-100 depth class and extent class into flood severity.

### Core Message

Flood severity rises with both depth and flooded-area extent, using a fixed lookup table rather than a continuous formula.

### Content

Create a 5 x 5 heatmap:

- x-axis: depth class 1-5.
- y-axis: extent class 1-5.
- cell value: severity class 1-5.
- Use labels:
  - depth classes: <=0.2 m, <=0.5 m, <=1.0 m, <=2.5 m, >2.5 m
  - extent classes: <=1%, <=5%, <=15%, <=25%, >25%

### Data Requirements

Use the exact lookup matrix from section 5.5:

```text
Extent 1: 1 2 2 3 4
Extent 2: 2 2 3 4 4
Extent 3: 2 3 4 4 5
Extent 4: 3 4 4 5 5
Extent 5: 4 5 5 5 5
```

### Visual Layout

- Use a sequential severity color ramp from very low to extreme.
- Put numeric severity values inside cells.
- Add a note: "Rows = extent class; columns = depth class."

### Avoid

- Do not convert severity into a probabilistic risk value.
- Do not show SSP periods; JRC flood is static `Snapshot`/`Current`.

### Acceptance Criteria

- The heatmap exactly matches the table in section 5.5.

## FIG-16: Per-Period Min-Max Normalization

Section anchor: section 6.2.

Priority: Recommended  
Type: Synthetic plot  
Source/effort: Author-created or synthetic

### Purpose

Explain thematic bundle component normalization.

### Core Message

Each component metric is scaled within a scenario-period geography cohort from raw values to a 0-100 higher-is-worse score. A flat finite field maps to 50 for every unit.

### Content

Use a two-part plot:

1. Main panel:
   - raw metric values for several districts/blocks
   - min and max labelled
   - transformed 0-100 scores shown on a secondary axis or adjacent panel
2. Degenerate flat-field inset:
   - all raw values equal
   - all scores = 50

Include formula:

`S_i = clip((v_i - v_min)/(v_max - v_min), 0, 1) x 100`

Mention lower-is-worse inversion in a note, not in the main visual unless it remains clear.

### Data Requirements

Synthetic values are sufficient.

### Avoid

- Do not use p10-p90 scaling here; that belongs to sectoral lenses in section 7.
- Do not imply scores are normalized against the 1990-2010 baseline.

### Acceptance Criteria

- The reader can distinguish thematic per-period min-max normalization from sectoral robust p10-p90 normalization.
- The flat-field -> 50 behavior is explicit.

## FIG-17: Example Thematic Output Map

Section anchor: section 6, especially sections 6.1-6.4.

Priority: Recommended  
Type: Map  
Source/effort: Notebook/pipeline

### Purpose

Show what a thematic composite output looks like spatially.

### Core Message

A thematic bundle combines several same-family hazard metrics into a 0-100 higher-is-worse score for each district or block within a scenario and period.

### Content

Create a single-state choropleth map for one thematic bundle. Recommended default:

- Bundle: Heat Risk
- Level: district
- Scenario: SSP5-8.5
- Period: 2040-2060
- Geography: one state with complete artifacts and legible district geometry

An India-wide rendering is acceptable only if it is backed by a genuinely pan-India-normalized artifact. Do not mosaic state-cohort-normalized scores into an India-wide map.

Required figure elements:

- Map polygons colored by composite score.
- Legend 0-100, higher-is-worse.
- Optional tier labels: low, moderate, high.
- Caption must state bundle, scenario, period, level, and geography.

### Data Requirements

- Persisted composite outputs or optimized dashboard artifacts.
- Canonical district boundaries matching the scores.

### Implementation Notes

- Use the same cohort semantics as the data artifact. If the score is state-cohort normalized, say so in the caption.
- If a pan-India artifact exists, document its normalization cohort explicitly and confirm that the 0-100 values are comparable across states.

### Avoid

- Do not compare scores across states unless the underlying artifact supports that interpretation.
- Do not show raw metric values as if they are composite scores.

### Acceptance Criteria

- The output visually resembles an actual IRT thematic composite map.
- Scenario, period, bundle, level, and normalization cohort are documented.

## FIG-18: Three-Lens Blended Rule Schematic

Section anchor: section 7.2.

Priority: Essential  
Type: Schematic  
Source/effort: Author-created schematic

### Purpose

Explain the centerpiece of the sectoral scoring method.

### Core Message

Each sectoral rule can blend three lens scores: absolute pressure, change from 1990-2010 baseline, and impact-band position. Available lens scores are combined with rule-specific weights, then rule scores are combined into a bundle composite.

### Content

Show a single rule pipeline:

Input:

- source metric value for one geography/scenario/period
- baseline value where needed
- impact band `[a, b]` where declared

Lens outputs:

- `S_abs`: robust p10-p90 position among state/level cohort
- `S_chg`: robust p10-p90 position of change from 1990-2010 baseline
- `S_imp`: fixed-band interpolation from onset `a` to saturation `b`

Combine:

- weighted rule score using lens weights
- bundle composite using rule weights

### Visual Layout

- Three parallel lens lanes converge into a rule-score node.
- Several rule-score nodes converge into a bundle-composite node.
- Show missing lens handling with a small note: "Unavailable lenses are omitted and weights renormalized."

### Data Requirements

No data required.

### Avoid

- Do not show all rules sharing the same lens weights. The note states lens weights are declared per rule.
- Do not imply the impact lens exists for regime/proxy metrics without defensible thresholds.

### Acceptance Criteria

- The three lens meanings are distinguishable.
- The two weight layers, lens weights and rule weights, are visually separate.

## FIG-19: Impact-Band Ramp

Section anchor: section 7.2, impact lens formula.

Priority: Recommended  
Type: Synthetic plot  
Source/effort: Author-created

### Purpose

Show how a raw metric value becomes an impact score.

### Core Message

The impact lens maps values below onset to 0, values above saturation to 100, and linearly interpolates between onset and saturation.

### Content

Plot:

- x-axis: raw metric value `v`.
- y-axis: impact score `S_imp`.
- Horizontal segment at 0 for `v <= a`.
- Linear ramp from `a` to `b`.
- Horizontal segment at 100 for `v >= b`.
- Label onset `a` and saturation `b`.

Use one concrete example in annotation, preferably:

- TXx 40-45 deg C for Health/Industrial/Infrastructure/Thermal heat rules, or
- Rx1day 115.6-204.5 mm for one-day rainfall rules.

### Data Requirements

Synthetic.

### Avoid

- Do not use percentile or anomaly values for the impact band; section 7.2 says the impact lens is absolute and non-spatial.
- Do not imply the ramp is nonlinear unless a future methodology change introduces that.

### Acceptance Criteria

- The formula `clip((v-a)/(b-a)) x 100` is visually represented.
- Onset and saturation are clearly labelled.

## FIG-20: District A vs B Lens Worked Example

Section anchor: section 7.3.

Priority: Essential  
Type: Bar/panel plot  
Source/effort: Author-created from section 7.3 table

### Purpose

Show why the sectoral blended score is more informative than pure absolute ranking.

### Core Message

District B has low current relative heat but high warming and has crossed the danger onset; the blended score raises it above what pure absolute ranking would show.

### Content

Use the exact example values in section 7.3:

| District | TXx 2060-80 | Anomaly | S_abs | S_chg | S_imp | Blended | Pure absolute |
|---|---:|---:|---:|---:|---:|---:|---:|
| A - already hot | 45.5 deg C | +1.5 deg C | 90 | 20 | 100 | 76 | 90 |
| B - fast-warming | 42.0 deg C | +3.5 deg C | 20 | 100 | 40 | 47 | 20 |

Context:

- Scenario: SSP5-8.5
- Period: 2060-2080
- Metric: TXx
- Health Risk TXx rule
- Lens weights: 0.40 / 0.25 / 0.35
- Impact band: 40-45 deg C
- Cohort ranges:
  - projected TXx q10=41 deg C, q90=46 deg C
  - anomaly q10=+1.0 deg C, q90=+3.5 deg C

### Visual Layout

Recommended:

- Panel A: grouped bars for `S_abs`, `S_chg`, `S_imp`.
- Panel B: blended vs pure-absolute bars for District A and District B.
- Add a callout on District B: "Fast-warming and newly above onset."

### Data Requirements

No real data required; use the worked example exactly as stated.

### Avoid

- Do not modify values or weights.
- Do not imply District A/B are real districts.

### Acceptance Criteria

- The figure visually demonstrates why District B is "rescued" by the blended method.
- The blended score can be recalculated from the shown lens values and weights.

## FIG-21: Robust p10-p90 vs Min-Max Normalization

Section anchor: section 7.2 and contrast with section 6.2.

Priority: Recommended  
Type: Synthetic plot  
Source/effort: Author-created or synthetic

### Purpose

Clarify the main normalization difference between thematic and sectoral methods.

### Core Message

Thematic component metrics use full min-max scaling, while sectoral absolute/change lenses use robust p10-p90 scaling to damp single outliers.

### Content

Use the same synthetic distribution with one or two outliers and show two score mappings:

- Min-max scaling:
  - min -> 0
  - max -> 100
  - outlier stretches scale
- Robust p10-p90 scaling:
  - p10 -> 0
  - p90 -> 100
  - values outside clipped
  - central distribution gets more usable spread

### Visual Layout

Recommended:

- Panel A: raw values sorted by geography, with min/max and p10/p90 markers.
- Panel B: resulting scores under min-max and p10-p90.

### Data Requirements

Synthetic.

### Avoid

- Do not suggest p10-p90 is used for thematic bundles.
- Do not imply p10-p90 makes scores comparable across states or periods; the cohort is still state x level x scenario x period.

### Acceptance Criteria

- The outlier-damping effect is clear.
- The figure explicitly labels which method belongs to thematic and which belongs to sectoral lenses.

## FIG-22: Three-Tier Composite Score Legend

Section anchor: section 8.

Priority: Optional  
Type: Legend  
Source/effort: Author-created

### Purpose

Document the low/moderate/high score bands used to classify the 0-100 composite output.

### Core Message

Composite scores are grouped into low, moderate, and high bands using fixed cut points: 0-33.3, 33.3-66.6, and 66.6-100.

### Content

Create a horizontal legend:

- 0-33.3: Low
- 33.3-66.6: Moderate
- 66.6-100: High

Include:

- Axis from 0 to 100.
- Label "higher = worse hazard pressure".
- Small note: "Bands classify composite score, not probability or percent impact."

### Data Requirements

No data required.

### Avoid

- Do not use unequal visual widths for the three bands.
- Do not label scores as percent risk.

### Acceptance Criteria

- The tier thresholds are readable at report width.

## FIG-23: District vs Block Resolution Side-by-Side

Section anchor: section 8.2, paired with FIG-08.

Priority: Recommended  
Type: Map  
Source/effort: Notebook/pipeline

### Purpose

Show how district and block views are parallel outputs from the same grid-first pipeline but normalized against separate cohorts.

### Core Message

District and block composites are computed independently from the same grid fields. A district score and a block score on the same 0-100 scale should not be compared unit-to-unit because their normalization cohorts differ.

### Content

Create a side-by-side map:

- Left panel: district-level composite for a chosen state/bundle/scenario/period.
- Right panel: block-level composite for the same state/bundle/scenario/period.
- Optional overlay or inset: 0.25 deg grid to remind readers of native resolution.

Recommended default:

- Bundle: Heat Risk or another stable thematic bundle.
- Scenario: SSP5-8.5.
- Period: 2040-2060.
- State: choose one with complete district and block outputs and legible block geometry.

Required labels:

- "District cohort normalization"
- "Block cohort normalization"
- "Scores are internally comparable within each panel, not across levels."

### Data Requirements

- District composite artifact.
- Block composite artifact.
- Matching district and block boundaries.

### Implementation Notes

- Use the same color ramp and 0-100 legend in both panels, but include a strong caption caveat about cohort separation.
- If using state-level maps, ensure both district and block data were generated for the same scenario and period.
- If maps are too dense, use one district focus area for block detail and a state context inset.

### Avoid

- Do not imply block scores nest or average up to district scores.
- Do not imply a district score of 80 equals a block score of 80 in absolute terms.
- Do not use different scenario/period combinations between panels.

### Acceptance Criteria

- The side-by-side view supports the section 8.2 warnings on grid coverage and cohort separation.
- The figure is clearly distinguishable from FIG-08: FIG-08 explains grid coverage; FIG-23 explains output interpretation across levels.

## Suggested Production Order

1. Produce the 8 Essential figures first: `FIG-01`, `FIG-06`, `FIG-08`, `FIG-09`, `FIG-12`, `FIG-14`, `FIG-18`, `FIG-20`.
2. Produce the Recommended figures second: `FIG-02`, `FIG-04`, `FIG-05`, `FIG-07`, `FIG-10`, `FIG-11`, `FIG-13`, `FIG-16`, `FIG-17`, `FIG-19`, `FIG-21`, `FIG-23`.
3. Produce the Optional figures only if the final note has enough space: `FIG-03`, `FIG-15`, `FIG-22`.

Note: The Appendix F table and this production order resolve to 12 Recommended figures and 3 Optional figures. If the Appendix F roll-up still says 11 Recommended and 4 Optional in a future draft, treat that as a roll-up typo and correct it to 12/3.

## Insertion Guidance

When figures are ready, insert placeholder callouts or final image links into `docs/technical_guidance_note.md` near the relevant section anchors. Recommended callout format:

```markdown
![FIG-nn: Short figure title](figures/technical_guidance_note/fig_nn_short_name.png)
```

Do not insert placeholders into the note until the figure shortlist is agreed. This instruction document is meant to support figure generation first; note edits should be made as a separate applied change.

Section anchors are current for the draft reviewed when this file was written; re-verify each anchor against `docs/technical_guidance_note.md` at insertion time.
