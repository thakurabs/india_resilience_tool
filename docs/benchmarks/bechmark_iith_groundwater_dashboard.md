# Benchmark Review: IITH Groundwater Dashboard vs India Resilience Tool

- **Prepared:** March 17, 2026
- **Benchmark target:** IITH groundwater dashboard
- **Compared product:** India Resilience Tool (IRT)
- **Reviewed URL:** `https://ingres.iith.ac.in/gecdataonline/gis/INDIA;parentLocName=INDIA;locname=INDIA;loctype=COUNTRY;view=ADMIN;locuuid=ffce954d-24e1-494b-ba7e-0931d8ad6085;year=2024-2025;computationType=normal;component=recharge;period=annual;category=safe;mapOnClickParams=false`

## 1. Executive Summary

The IITH groundwater dashboard is a tighter, more operational map product than IRT. It is optimized for administrative groundwater status review: open the map, see a clearly bounded geography, inspect a single groundwater component, drill down by administrative or hydrological hierarchy, and pull a comparable table or map export. IRT is strategically broader and analytically deeper, but it asks the user to configure much more before it gives them orientation.

The biggest lesson for IRT is not feature accumulation. It is time-to-first-insight. The groundwater dashboard appears to start with a meaningful national default, explicit legends, visible layer controls, map-history navigation, and a simpler geography-first mental model. IRT is stronger once a user is inside the workflow, but weaker at immediate orientation, public-facing discoverability, and lightweight reporting.

The highest-value changes for IRT are:

1. provide a stronger default landing state,
2. make geographic drill-down and breadcrumbing more explicit,
3. surface persistent map-level legend, layer, and export actions,
4. simplify the overview workflow before asking users to enter deep analytical configuration.

## 2. Detailed Overview of the Groundwater Dashboard

### 2.1 Purpose and Likely Use Case

The dashboard appears built for groundwater assessment review and operational monitoring. The route state, legends, and table structures indicate that it supports annual and seasonal groundwater recharge, extraction, rainfall, stage of extraction, and unit categorization such as Safe, Semi Critical, Critical, and Over Exploited.

It seems designed to help users answer questions like:

- where groundwater stress is concentrated,
- how one assessment unit compares with another,
- how status changes across assessment years,
- how conditions differ by administrative and hydrological geography.

This makes it primarily a monitoring and administrative review tool rather than a broad exploratory resilience platform.

### 2.2 Likely Target Users

The dashboard appears aimed first at administrative and technical groundwater users rather than the general public.

Evidence from the interface and bundle structure suggests intended users include:

- state and district groundwater officials,
- basin-level reviewers,
- technical analysts,
- approval and reporting authorities,
- institutional stakeholders working inside an assessment workflow.

The code exposes role-aware structures such as `GEC_STATE_ADMIN`, `GEC_DISTRICT_ADMIN`, `GEC_BASIN_ADMIN`, `GEC_CGWB_ADMIN`, `GEC_SLC_ADMIN`, and approval/history tables. That indicates a system designed around formal reporting, assessment, and review processes.

### 2.3 Information Architecture

The product is map-first and workflow-led.

Primary layers of information appear to be:

1. map canvas,
2. geography context,
3. thematic selection state,
4. legend,
5. tabular companion,
6. comparison mode.

Main elements inferred from the route state, templates, and controls:

- map canvas with Leaflet controls,
- side filter form,
- year selectors,
- view selector for `Administrative` or `Hydrological`,
- component, period, category, and computation-type state,
- legend control,
- layer toggles,
- extent history back/forward control,
- map image download,
- table-oriented data access,
- dual-year compare mode with synchronized maps.

The hierarchy is straightforward:

- primary: map and filter state,
- secondary: legend, layer selection, drill-down,
- tertiary: tables, compare view, reporting actions.

### 2.4 User Workflow

A likely user flow is:

1. land on India with a meaningful default configuration,
2. inspect the current choropleth and legend,
3. switch between administrative and hydrological views if needed,
4. adjust component, year, period, category, or computation type,
5. click a geography to drill down,
6. open the related table or compare view,
7. export or report from the selected state.

The default route itself is revealing: it already includes a specific year, component, period, category, and computation type, which means the dashboard is designed to open directly into an interpretable thematic state rather than a blank shell.

### 2.5 Map Design and Spatial Interaction

The map is an analytical interaction surface, not just a display canvas.

Observed or strongly implied map behaviors include:

- vector-tile thematic overlays,
- hover popups showing location names,
- click-driven drill-down,
- active boundary overlays,
- extent-history back/forward navigation,
- persistent legend control,
- base-layer and overlay controls,
- mask layers in some contexts,
- synchronized maps in compare mode.

The geography stack supports both:

- country -> state -> district -> block or assessment unit flows,
- country -> basin -> sub-basin flows.

This gives the dashboard a strong spatial-navigation posture. Its map supports practical review tasks well, even if the analytical depth behind the map is narrower than IRT.

### 2.6 Filters, Controls, and State Management

The dashboard’s control scheme is compact and domain-native.

Confirmed route or state parameters include:

- `year`,
- `year1`,
- `year2`,
- `component`,
- `period`,
- `category`,
- `computationType`,
- `view`,
- `locname`,
- `locuuid`,
- `loctype`,
- `mapOnClickParams`.

Confirmed or strongly evidenced control values include:

- views: `ADMIN`, `BASIN`,
- periods: `annual`, `monsoon`, `non_monsoon`,
- components: `recharge`, `draft`, `extraction`, `stage`,
- categories: `safe`, `semi_critical`, `critical`, `over_exploited`,
- computation type: `normal`.

This is a relatively intuitive setup for trained domain users because it aligns directly with the groundwater assessment model. The main downside is that some terms, especially `computationType` and category semantics, depend on prior domain understanding.

### 2.7 Visual Design and Presentation

The visual design appears utilitarian, institutional, and task-oriented rather than polished in a consumer-product sense.

Strengths of the visual presentation:

- stable choropleth legend structure,
- direct use of units in table labels and map titles,
- visible map controls,
- low ambiguity in thematic naming,
- dense but purposeful interface composition.

Weaknesses of the visual presentation:

- likely legacy Bootstrap density,
- terminology-heavy labels,
- limited visual refinement,
- category color choices that may not be accessibility-optimized,
- possible reliance on training rather than interface explanation.

This is not elegant product design, but it is coherent operational design.

### 2.8 Data Communication Style

The dashboard communicates through:

- choropleths,
- legends,
- hover labels,
- drill-down navigation,
- tables,
- compare views,
- downloadable map output.

Its table schema is especially effective. At broader levels, it emphasizes:

- location name,
- rainfall,
- recharge,
- draft.

At more detailed levels, it adds:

- category,
- stage of extraction.

That alignment between map and table makes the product useful for reporting and administrative interpretation.

### 2.9 Strengths

The strongest aspects of the groundwater dashboard are:

- immediate orientation from a meaningful default map state,
- a geography-first analytical workflow,
- clear domain-native control structure,
- strong map-based drill-down,
- explicit legends and units,
- lightweight reporting and extractability,
- practical compare mode,
- closer coupling between map and table views.

### 2.10 Weaknesses and Limitations

The main weaknesses are:

- narrower analytical scope than IRT,
- weaker explanatory framing for non-experts,
- more institutional than user-friendly terminology,
- probable dependence on user familiarity with groundwater-assessment categories,
- limited multi-factor interpretation,
- weaker resilience planning support outside its groundwater domain.

It is strong as a monitoring and administrative review tool, but not as strong as IRT for multi-domain resilience analysis or strategic planning.

## 3. Point-by-Point Comparison with the India Resilience Tool

### 3.1 Comparison Matrix

| Dimension | Groundwater dashboard | India Resilience Tool |
|---|---|---|
| Strategic purpose | Operational groundwater monitoring and administrative assessment | Multi-hazard resilience exploration and planning across admin and hydro units |
| Target user | Groundwater officials, analysts, reviewers, approval users | Climate-risk analysts, planners, program teams, technically capable users |
| Breadth of analytics | Narrower, domain-specific | Much broader across climate hazards, bio-physical hazards, exposure, and hydro/admin context |
| Depth of analytics | Strong within groundwater operations | Stronger overall due to scenarios, periods, statistics, trends, portfolio, and crosswalk context |
| Geographic navigation | More immediate geography-led workflow | Richer but more conditional and configuration-heavy |
| Map role | Primary workflow driver | Strong analytical surface, but more gated by setup requirements |
| Filtering flow | Simpler and lower cognitive load | More powerful but heavier and slower to first insight |
| Information hierarchy | Better at showing what matters first | Better at enabling deep analysis after setup |
| Reporting usefulness | Better lightweight map and table reporting | Better long-form details and case-study exports |
| Decision-support model | Monitoring and administrative review | Planning, comparison, scenario interpretation, and resilience insight |

### 3.2 Strategic Purpose

The groundwater dashboard is fundamentally a monitoring and review system. It answers targeted questions in a known domain. It is optimized for groundwater status interpretation and administrative workflows.

IRT is fundamentally a broader resilience-analysis platform. It supports:

- admin and hydro views,
- map/rankings/details flows,
- scenario and period exploration,
- risk summaries and trend views,
- crosswalk-based contextual interpretation,
- portfolio workflows.

Where groundwater is stronger:

- immediate operational clarity,
- monitoring-oriented workflow discipline.

Where IRT is stronger:

- multi-domain resilience planning,
- interpretive breadth,
- analytical extensibility.

### 3.3 Target User Profile

The groundwater dashboard is better suited to:

- institutional groundwater users,
- reporting-oriented analysts,
- administrative reviewers,
- users who already know the domain categories.

IRT is better suited to:

- climate-risk analysts,
- resilience planners,
- strategy teams,
- users comparing multiple hazard or exposure dimensions.

The difference is not just feature scope. It is product posture. Groundwater is more operational and administrative. IRT is more strategic and analytical.

### 3.4 Breadth and Depth of Analytics

Groundwater has narrower breadth but stronger thematic focus. Its components and category framework are clearly bounded.

IRT has much greater breadth and deeper comparative capacity through:

- assessment pillar -> domain -> metric selection,
- scenario and period selection,
- statistic selection,
- map mode selection,
- right-side details with summary, trend, and scenario comparison,
- hydro/admin crosswalk context,
- portfolio workflows.

Groundwater is better when the user needs a clean single-domain review. IRT is better when the user needs broader analytical power.

### 3.5 Geographic Navigation and Drill-Down

This is one of the groundwater dashboard’s strongest advantages.

Its geography model appears easier to understand at first glance:

- country to state/district/block in administrative mode,
- country to basin/sub-basin in hydrological mode,
- map as the primary entry point,
- simpler drill logic.

IRT supports strong geography switching as well, including:

- admin family: district/block,
- hydro family: basin/sub-basin,
- family-aware geography selectors,
- crosswalk-driven admin-hydro navigation.

However, IRT’s geography logic is more hidden behind configuration and analysis-mode gating. The groundwater product is stronger for discoverability. IRT is stronger for contextual richness once the user reaches a selected unit.

### 3.6 Map Functionality

The groundwater map is more visibly operational. It includes clear control affordances such as:

- layer controls,
- legend,
- history control,
- map download,
- synchronized compare maps.

IRT’s map is analytically strong, with enriched tooltips, map modes, ranking integration, and overlays, but it is less obviously “self-steering” on first entry. The map becomes useful only after the ribbon and analysis-focus selections are completed.

Groundwater is better at visible map ergonomics. IRT is better at tying the map into a larger analytical system.

### 3.7 Filtering and Exploration Flow

Groundwater uses a smaller, more domain-native filter stack. That reduces cognitive load and helps users move quickly.

IRT’s ribbon is more powerful, but heavier:

- assessment pillar,
- domain,
- metric,
- scenario,
- period,
- statistic,
- map mode.

That structure is analytically appropriate for IRT, but it makes first-run exploration slower. The groundwater dashboard is stronger at speed and directness. IRT is stronger at flexible analysis once configured.

### 3.8 Information Hierarchy

Groundwater is better at “what matters first.”

Its likely first screen gives users:

- a themed map,
- visible legend,
- clear location context,
- obvious geography and component state.

IRT is better at “what else can I do after I understand the initial state,” but weaker at initial framing. Its workflow puts more emphasis on control completion than on immediate orientation.

### 3.9 UI and UX Quality

Groundwater has better first-run usability for trained users. It feels more direct and lower-friction, even if visually older.

IRT has stronger deep-work UX:

- split-pane layout,
- dedicated right-side details panel,
- rankings view,
- portfolio workflows,
- crosswalk context.

The UX tradeoff is clear:

- groundwater: simpler and faster,
- IRT: richer and more complex.

### 3.10 Visual Communication

Groundwater’s visual communication is simpler and more administratively legible:

- stable category colors,
- direct unit labeling,
- straightforward map titles,
- fixed table schemas.

IRT’s visual communication is more variable because it spans many metrics and data types. It can communicate more nuance, but consistency depends more on the selected metric and the current view.

Groundwater is stronger in visual consistency. IRT is stronger in expressive range.

### 3.11 Decision-Support Usefulness

Groundwater is better for:

- operational review,
- status communication,
- administrative prioritization,
- quick district or basin assessment.

IRT is better for:

- resilience planning,
- scenario interpretation,
- comparing hazards and metrics,
- understanding cross-system context,
- portfolio analysis.

They are not substitutes. Groundwater is a more focused reference product. IRT is a broader strategic system.

### 3.12 Reporting and Extractability

Groundwater seems to support reporting more directly through:

- immediate map downloads,
- tightly coupled tables,
- simpler naming and metric structure,
- compare-ready presentation.

IRT supports stronger detailed outputs in the form of:

- case-study export,
- trend and scenario detail,
- rankings,
- context panels.

Groundwater is better at lightweight reporting from the map. IRT is better at deeper analytical reporting once a unit has been selected.

### 3.13 Overall Strengths and Weaknesses

Where the groundwater dashboard is stronger than IRT:

- first-run clarity,
- geography-first workflow,
- immediate interpretability,
- visible map controls,
- lightweight extractability,
- monitoring-oriented discipline.

Where IRT is stronger than the groundwater dashboard:

- breadth of metrics,
- scenario and period logic,
- deeper details panels,
- crosswalk context,
- hydro/admin integration,
- resilience-planning usefulness.

Where they are different rather than better or worse:

- groundwater is more administrative and monitoring-oriented,
- IRT is more analytical and planning-oriented.

## 4. Features and Ideas from the Groundwater Dashboard That Could Strengthen IRT

### 4.1 Worth Adopting Directly

#### A. Map Extent History

1. **Feature / pattern name**: map extent back-forward control  
2. **What it does in the groundwater dashboard**: lets users move backward and forward through prior map extents during drill-down and pan/zoom exploration  
3. **Why it is useful**: supports safe exploration without losing context  
4. **Why it is relevant to IRT**: IRT’s geography flow is richer and more complex, so recovery from navigation mistakes matters even more  
5. **Adoption decision**: adopt directly  
6. **How it could be implemented in IRT**: store a bounded stack of view extents in session state and expose back/forward controls near the map header  
7. **Expected user benefit**: lower navigation friction and easier geographic exploration  
8. **Implementation complexity**: Low  
9. **Priority**: High  
10. **Dependencies**: stable map extent capture on pan, zoom, and drill interactions

#### B. Map Image Export

1. **Feature / pattern name**: direct map image download  
2. **What it does in the groundwater dashboard**: provides a visible map-level download affordance  
3. **Why it is useful**: makes the product immediately useful for slideware, reporting, and email circulation  
4. **Why it is relevant to IRT**: IRT has strong analytical context, but map-level lightweight reporting is less obvious  
5. **Adoption decision**: adopt directly  
6. **How it could be implemented in IRT**: add a map header action to export the current map and legend state as PNG  
7. **Expected user benefit**: faster communication and lower reporting overhead  
8. **Implementation complexity**: Medium  
9. **Priority**: High  
10. **Dependencies**: reproducible map render capture and legend inclusion

#### C. Persistent Legend and Layer Affordance

1. **Feature / pattern name**: always-visible legend/layer tray  
2. **What it does in the groundwater dashboard**: keeps symbology and overlay controls close to the analytical surface  
3. **Why it is useful**: reinforces trust and reduces ambiguity  
4. **Why it is relevant to IRT**: IRT’s map can carry more semantic complexity than the groundwater dashboard  
5. **Adoption decision**: adopt directly  
6. **How it could be implemented in IRT**: pin legend and overlay controls in a stable map-side affordance rather than relying on transient interpretation  
7. **Expected user benefit**: better interpretability and faster orientation  
8. **Implementation complexity**: Medium  
9. **Priority**: High  
10. **Dependencies**: a cleaner legend and overlay-control presentation layer

#### D. Data Table Near the Map

1. **Feature / pattern name**: map-adjacent data table toggle  
2. **What it does in the groundwater dashboard**: links the mapped state to a consistent tabular view  
3. **Why it is useful**: reinforces trust and supports extractability  
4. **Why it is relevant to IRT**: IRT has rankings, but a more obvious map/data pairing would reduce mode-switching friction  
5. **Adoption decision**: adopt directly  
6. **How it could be implemented in IRT**: add a top-level `Map / Data table / Rankings` switch or a docked table mode sharing the same filtered state  
7. **Expected user benefit**: easier validation, comparison, and reporting  
8. **Implementation complexity**: Medium  
9. **Priority**: High  
10. **Dependencies**: consistent shared map-filter state across left-panel modes

### 4.2 Worth Adapting with Redesign

#### E. Geography-First Default Landing

1. **Feature / pattern name**: meaningful default thematic landing state  
2. **What it does in the groundwater dashboard**: opens directly into a populated map state rather than waiting for many user choices  
3. **Why it is useful**: users see value immediately  
4. **Why it is relevant to IRT**: IRT currently gates the map behind ribbon completion and analysis-focus selection  
5. **Adoption decision**: adapt with modification  
6. **How it could be implemented in IRT**: define a curated default metric, period, and view for each spatial family and load it on first entry, while preserving the full ribbon for advanced exploration  
7. **Expected user benefit**: dramatically better first-run usability  
8. **Implementation complexity**: Medium  
9. **Priority**: High  
10. **Dependencies**: agreement on default metrics and reliable processed-data availability

#### F. Dual-Year Compare Mode

1. **Feature / pattern name**: side-by-side synchronized comparison maps  
2. **What it does in the groundwater dashboard**: compares two assessment years with a summary/detailed companion view  
3. **Why it is useful**: gives users a concrete before-versus-after lens  
4. **Why it is relevant to IRT**: IRT already has strong scenario and trend logic but lacks a similarly obvious dedicated compare experience  
5. **Adoption decision**: adapt with modification  
6. **How it could be implemented in IRT**: support side-by-side compare for selected scenario-period combinations or baseline-versus-current combinations, with synchronized pan/zoom and matched legends  
7. **Expected user benefit**: clearer comparisons and stronger planning communication  
8. **Implementation complexity**: High  
9. **Priority**: Medium  
10. **Dependencies**: comparable color scaling, synchronized map state, and simplified comparison UX

#### G. Explicit Geographic Breadcrumbs

1. **Feature / pattern name**: geographic breadcrumb and level-jump pattern  
2. **What it does in the groundwater dashboard**: makes the current geographic context and drill path legible  
3. **Why it is useful**: users know where they are and how to move back  
4. **Why it is relevant to IRT**: IRT’s geography is richer but more hidden once users begin drilling or switching families  
5. **Adoption decision**: adapt with modification  
6. **How it could be implemented in IRT**: show persistent chips or breadcrumbs such as `Admin > Telangana > District > ...` or `Hydro > Basin > ...`, with clickable back-navigation  
7. **Expected user benefit**: better discoverability and lower cognitive load  
8. **Implementation complexity**: Medium  
9. **Priority**: High  
10. **Dependencies**: canonical routing and selection-state normalization

#### H. Stable Unit-First Labeling

1. **Feature / pattern name**: fixed, reporting-friendly metric labels  
2. **What it does in the groundwater dashboard**: uses simple titles, unit labels, and recurring schemas across map and table  
3. **Why it is useful**: improves trust and reduces translation overhead  
4. **Why it is relevant to IRT**: IRT’s breadth sometimes makes labels feel system-derived rather than report-ready  
5. **Adoption decision**: adapt with modification  
6. **How it could be implemented in IRT**: standardize display names, units, and field ordering at the metric-family level, especially across map, rankings, and details  
7. **Expected user benefit**: clearer communication and easier reporting  
8. **Implementation complexity**: Medium  
9. **Priority**: Medium  
10. **Dependencies**: metric label contract cleanup in the registry and display helpers

### 4.3 Not Suitable for IRT

#### I. Groundwater Category-First Framing as a General UX Model

1. **Feature / pattern name**: primary reliance on Safe/Critical-style categorical framing  
2. **What it does in the groundwater dashboard**: puts groundwater assessment categories at the center of interpretation  
3. **Why it is useful there**: the domain is narrow and the categories are institutionally meaningful  
4. **Why it is relevant to IRT**: it is tempting because it simplifies interpretation  
5. **Adoption decision**: reject as unsuitable  
6. **Why not**: IRT spans multiple hazards and exposure metrics that should not be collapsed into a single borrowed category framework  
7. **Expected user benefit if rejected**: preserves methodological honesty  
8. **Implementation complexity**: None  
9. **Priority**: Low  
10. **Dependencies**: none

#### J. Approval-Workflow Surfaces

1. **Feature / pattern name**: approval and submission workflow integration  
2. **What it does in the groundwater dashboard**: supports role-based institutional review and workflow progression  
3. **Why it is useful there**: the product appears embedded in a formal reporting process  
4. **Why it is relevant to IRT**: it shows a different product posture  
5. **Adoption decision**: reject as unsuitable  
6. **Why not**: it does not match IRT’s current purpose as an analytical dashboard  
7. **Expected user benefit if rejected**: avoids product sprawl  
8. **Implementation complexity**: None  
9. **Priority**: Low  
10. **Dependencies**: none

## 5. Gaps in IRT Revealed by This Comparison

The comparison reveals that IRT’s biggest gap is not analytical capability. It is product framing.

The groundwater dashboard highlights several structural weaknesses in IRT’s current UX:

### 5.1 Slow Time-to-First-Insight

IRT currently requires users to complete more setup before the map becomes useful. This creates friction for:

- first-time users,
- occasional users,
- stakeholders looking for quick orientation,
- reviewers who want a defensible default view.

### 5.2 Under-Signaled Spatial Workflow

IRT supports rich admin and hydro geography, but its drill-down model is less immediately legible. Users can navigate it, but the product does less to advertise:

- where they are,
- what the next lower level is,
- how to return upward,
- how the map supports spatial exploration.

### 5.3 Information Hierarchy Favors Power Users

IRT leads with configurability rather than interpretive framing. That is good for advanced analysis but weaker for:

- onboarding,
- executive review,
- public demonstrations,
- rapid situational understanding.

### 5.4 Map-Level Reporting Is Underdeveloped

IRT offers strong details and exports, but map-level extractability is less visible than it should be. The benchmark suggests that:

- maps should be exportable,
- legends should stay near the export path,
- users should be able to move quickly from map to data view without losing context.

### 5.5 Analytical Depth Is Not Always Framed for Trust

IRT often has more analytical power than the groundwater dashboard, but not always more immediate trust. Trust is built not only by depth, but by:

- stable labels,
- obvious legends,
- unit clarity,
- predictable tables,
- easy map-state recall.

### 5.6 Overview Flow Is Weaker Than Detail Flow

IRT’s right-side detail model is strong. Its left-side overview posture is weaker. The benchmark makes clear that overview quality is a product capability in its own right, not just a prelude to details.

## 6. Prioritized Recommendations for IRT

### 6.1 High-Priority, High-Value Improvements

#### 1. Ship a Meaningful Default Map State

Why it matters:

- reduces onboarding friction,
- improves demo readiness,
- shortens time-to-first-insight.

Problem solved:

- blank or over-configured first-run experience.

Why high priority:

- it improves every session, not just advanced ones.

#### 2. Add Persistent Legend, Layer, and Export Actions at the Map Level

Why it matters:

- improves interpretability and reporting,
- keeps essential context close to the analytical surface.

Problem solved:

- weak map-level trust and extractability.

Why high priority:

- directly improves usability, communication, and stakeholder adoption.

#### 3. Introduce Geographic Breadcrumbs and Extent History

Why it matters:

- makes exploration safer and easier,
- reduces disorientation during drill-down.

Problem solved:

- hidden navigation state and poor recovery from map movement.

Why high priority:

- high usability return with moderate implementation cost.

#### 4. Add a Stronger Overview-First Mode

Why it matters:

- makes IRT friendlier for non-power users,
- helps leadership and review audiences understand the dashboard faster.

Problem solved:

- current interface emphasizes setup before interpretation.

Why high priority:

- this is the most important structural lesson from the benchmark.

### 6.2 Medium-Priority Improvements

#### 5. Create a Dedicated Compare Experience

Why it matters:

- compare flows are central to planning and communication.

Problem solved:

- comparisons currently exist, but not as a simple first-class workflow.

Why medium priority:

- valuable, but depends on a cleaner baseline map workflow.

#### 6. Standardize Display Labels, Units, and Table Schemas

Why it matters:

- improves trust, reporting quality, and cross-view consistency.

Problem solved:

- some IRT outputs feel registry-driven rather than report-ready.

Why medium priority:

- important for polish and communication, but less urgent than workflow clarity.

#### 7. Tighten Map-to-Data-to-Rankings Continuity

Why it matters:

- helps users validate what they see,
- makes outputs more extractable.

Problem solved:

- more separation than necessary between map interpretation and data inspection.

Why medium priority:

- useful once the default map and navigation experience are improved.

### 6.3 Low-Priority or Optional Improvements

#### 8. Add a Curated Public or Lightweight Viewing Mode

Why it matters:

- could broaden the audience for IRT.

Problem solved:

- advanced interface may overwhelm occasional users.

Why low priority:

- beneficial, but not essential to current analytical users.

#### 9. Consider More Explicit Unit-Family Templates

Why it matters:

- could further align hydro and admin experiences.

Problem solved:

- slight inconsistency in how different metric families are framed.

Why low priority:

- incremental improvement compared with the more urgent workflow fixes.

## 7. Final Takeaway

The IITH groundwater dashboard should not be treated as a richer product than IRT. It should be treated as a sharper one.

Its strongest lesson is that a narrower, operationally coherent dashboard can feel more usable than a broader analytical platform if it:

- shows a meaningful default immediately,
- makes geography primary,
- keeps legends and map controls close at hand,
- couples map and table tightly,
- supports lightweight reporting without friction.

IRT is already stronger in analytical depth, contextual interpretation, and resilience-planning potential. The benchmark reveals that the next major improvement opportunity is not expanding capability, but packaging capability into a clearer, faster, more trust-building user experience.
