# Web Application Workflow — Observed Live Behavior

## Purpose and evidence boundary

This document describes behavior observed on the live vendor web application at
`https://dev.resilience.org.in`. It is a description of the application as exercised during
QA, not a restatement of the user-story specification and not a proposed future workflow.

The latest full regression sweep and manual follow-up were completed on 27 July 2026. Where
older QA observations conflict with that sweep, the later observation is used here.

Authentication, registration, two-factor authentication, password-reset delivery, feedback
submission, profile mutation, and logout were not exercised end to end because they require a
real inbox, change shared account data, send external messages, or terminate the reusable QA
session. Those flows are therefore not described beyond the authenticated surfaces that were
directly observed.

## 1. Authenticated application shell

After authentication, the user reaches the dashboard at the application root.

The header contains:

- the application logo and title;
- a `Welcome, <name>` account control;
- a `Share Feedback` control; and
- the main dashboard beneath the header.

Opening the welcome menu exposes:

- `User Profile`;
- `My Analysis`; and
- `Logout`.

`User Profile` routes to `/profile`. `My Analysis` routes to `/my-analysis`. Logout was visible
but was not activated during QA.

## 2. User profile

The profile page displays the signed-in user's account details. The observed fields were:

- Name;
- Email;
- Organization;
- Designation;
- Purpose of Use;
- Thematic Activity; and
- Country.

Country displayed `India` and was locked. Email was also visibly locked and could not be
edited. No State field was present in the observed profile.

The page exposed an `Update` action and a reset-password area with a `Send OTP` action. Neither
action was submitted during QA, so the post-submit behavior is outside this document's evidence
boundary.

## 3. Feedback popup

`Share Feedback` opens a feedback modal in the working manual flow. The observed modal includes
experience choices, a free-text area, a star-rating control, a submit action, and a close action.

The modal can be dismissed without submission. A feedback survey was also observed appearing
automatically during a longer dashboard session. QA dismissed these prompts without submitting
them.

## 4. Starting a dashboard analysis

The main dashboard analysis is assembled from two inputs:

1. one or more locations; and
2. a resilience-filter selection.

Location selection is available through the Administrative Panel, the Coordinate Panel, and
the map. The `Add to Analysis` action does not become usable from a location alone. It becomes
available after a valid location and a sufficiently complete resilience-filter selection have
produced map data.

## 5. Administrative geography workflow

### 5.1 Opening the panel

The `Administrative Panel` can be expanded and collapsed. In its initial state, district
selection is disabled until a state is selected, and `Add to Analysis` is disabled.

The application also exposes dedicated controls to reset and expand the panel.

### 5.2 Selecting a state and district

The observed district workflow is:

1. Open the Administrative Panel.
2. Keep the administrative level on `District`.
3. Select a State.
4. Wait for the district selector to populate.
5. Select one district for a single-site analysis, select several districts, or use
   `All Districts` for a multi-site selection.

For Telangana, the latest regression run loaded 34 district options. Selecting Adilabad
created a single-site geography state. Selecting `All Districts` created a multi-site state.

Collapsing and reopening the panel retained the selected state. The `Reset` action cleared the
state selection and returned the geography controls to their initial state.

### 5.3 Switching to Block

The panel exposes `District` and `Block` administrative levels. Switching to Block changes the
active geography level and resets the prior district-focused selection state. Block selection
then proceeds through the geography controls available for that level.

### 5.4 Switching between administrative and coordinate modes

Administrative and coordinate selection are separate modes. A mode switch can present a
confirmation explaining that the current geography selection will be cleared.

The latest cross-flow run still observed that a portfolio did not reliably accumulate across
Administrative, Coordinate/Upload, and Map contexts. Switching context could replace or clear
items added in the previous context. `Manage Portfolio` reflected the surviving items after the
mode change rather than an additive combination of every prior location-entry context.

## 6. Coordinate Panel workflow

The Coordinate Panel supports manual coordinate entry and file upload.

### 6.1 Manual coordinate entry

The observed manual flow is:

1. Open the Coordinate Panel.
2. Enter latitude and longitude.
3. Optionally provide a custom site name.
4. Select `Show on Map`.
5. Review the resolved location shown by the application.
6. Use `Add Coordinate` to stage the location when that action is present.
7. Complete the resilience filters.
8. Select `Add to Analysis`.

A valid test coordinate resolved to `GHANPUR(STATION), TELANGANA` and appeared on the map
without a contradictory error toast.

`Show on Map` remained disabled when required coordinate values were missing or invalid. The
`Clear` action removed the latitude and longitude values and returned the entry form to its
empty state.

Showing a coordinate on the map and adding it to an analysis are distinct actions. In the
observed portfolio flow, the coordinate first had to be staged with `Add Coordinate`; showing
the point alone did not add it to the portfolio.

### 6.2 Uploading coordinates

The upload sub-flow exposes sample downloads for CSV, XLSX, and shapefile ZIP inputs, plus an
upload action.

The application accepted the observed sample-style tabular schema:

```text
id,custom_name,lat,long
```

An input using `Latitude,Longitude,Label` was rejected with:

> Invalid file format. Please use the provided sample

After a valid upload, the parsed coordinate records appeared in the uploaded-coordinate list.
An upload can contain multiple sites, and those sites can be added together after the resilience
filters are complete.

Observed input handling in the latest follow-up included:

- malformed, empty, header-only, and structurally invalid files were rejected;
- unsupported file types were rejected;
- files larger than 1 MB were rejected;
- a file whose content did not match its extension was rejected with a specific content/extension
  mismatch message;
- out-of-range, non-numeric, empty, out-of-India, formula-like, and excessively long values were
  rejected for the tested CSV and XLSX cases;
- an out-of-India shapefile was rejected;
- Unicode names were accepted;
- a valid shapefile containing extra unrelated files was accepted; and
- a shapefile with a very long name was accepted in the observed build.

For shapefile uploads, observed DBF custom names were not retained; uploaded features appeared
with generated labels such as `Point N`.

Duplicate handling depended on the path:

- adding the same administrative district again was rejected with an `already in your portfolio`
  message;
- identical uploaded coordinates were deduplicated silently; and
- the latest rerun also silently deduplicated the same coordinates when their supplied names
  differed.

## 7. Resilience-filter workflow

The `Select Resilience Filters` panel exposes six filter groups:

1. Risk Domain;
2. Metric;
3. Scenario;
4. Period;
5. Statistic; and
6. Map Mode.

The controls are cascading. Downstream choices remain unavailable until the required upstream
choice has been made.

An observed Heat Risk analysis followed this sequence:

1. Set Risk Domain to `Thematic - Heat Risk`.
2. Select `Heat Risk Composite (score)` as the Metric.
3. Select `Middle-of-the-road (SSP2-4.5)` as the Scenario.
4. Select `Early century` as the Period.
5. Allow the map data to load.

Statistic and Map Mode can be displayed as later filter groups while the application supplies
an available/default state appropriate to the selected metric. Once the valid cascade has loaded
data for the selected geography, `Add to Analysis` becomes enabled.

The Risk Domain control includes a help icon with a hover tooltip.

## 8. Map View

`Map View` is the default result view after geography and filters are selected.

The observed map workflow includes:

- rendering the selected geography with metric-dependent color fills;
- displaying a legend for the active metric and filter context;
- hovering or clicking mapped regions to expose contextual information;
- an `Add to Analysis` action in the map interaction popup; and
- zoom-in, zoom-out, and reset controls.

In the latest map-interactivity rerun, selecting a district through the dropdown changed the map
view/drill-down but did not lock all other map interactions. Clearing the selection restored the
broader state view. Adding locations map-first or dropdown-first produced the same final
portfolio count in that rerun.

## 9. Ranking Table

The `Select your views` control offers `Map View` and `Ranking Table`.

The views are mutually exclusive:

- selecting Ranking Table hides the map; and
- returning to Map View hides the ranking table and restores the map.

The selected state, district, and resilience filters remain intact while switching between the
two views.

In the latest Telangana regression run, Ranking Table loaded 34 rows successfully for the
selected Heat Risk filters. This supersedes the earlier QA observation in which the ranking
request returned an HTTP 500.

## 10. Adding locations to My Analysis

`Add to Analysis` is gated by both location and analysis context. It becomes enabled after:

1. a valid administrative or coordinate location is present;
2. the required resilience filters are selected; and
3. the associated data has loaded.

Observed add paths include:

- one or more districts selected in the Administrative Panel;
- a manually entered and staged coordinate;
- multiple uploaded coordinates; and
- a district selected through a map interaction popup.

After a successful add, the application updates the portfolio and exposes or opens the My
Analysis panel. After switching among geography-entry modes, the latest cross-flow run displayed
context replacement rather than reliable cross-mode accumulation.

## 11. Single-site Resilience Profile

The Resilience Profile is the single-site analysis surface.

For a regular climate metric, the observed profile included:

- a profile overview with the selected geography, index, scenario, and period;
- a Risk Summary with historical baseline, projected value, absolute change, a change indicator,
  and position within the state;
- a `Trend Over Time` line chart with historical and scenario series;
- a `Show model members` option that revealed a `Max models to draw` control;
- a `Scenario Comparison` grouped bar chart; and
- an expandable/full-screen presentation.

The profile was observed working at mobile width as well as desktop width. The latest manual
spot-check confirmed that visible Resilience Profile content rendered correctly after the
automated selector run had failed to locate parts of the panel.

Background requests to trend and scenario-comparison endpoints returned HTTP 500 during one
multi-site automated run, but the same rerun did not establish a visible failure in the active
portfolio workflow. This remains an observed technical background condition rather than a
confirmed user-visible workflow break.

## 12. Saving an analysis

The `Save Analysis` action is disabled before a usable analysis exists and becomes enabled after
a location and analysis context have been built.

The observed save flow is:

1. Build an analysis from geography and resilience filters.
2. Select `Save Analysis`.
3. Enter an Analysis Name in the save modal, or leave it blank to use the generated default.
4. Confirm the save.
5. Open `Welcome` and select `My Analysis`, or navigate directly to `/my-analysis`.

A unique name saved successfully and appeared in the saved-analysis list. A duplicate name was
rejected without replacing the existing item. A blank name produced an application-generated
label derived from the analysis context.

The saved-analysis page provides:

- a list of saved analyses;
- a `Search Analysis` control;
- a per-row three-dot action menu;
- `Rename`; and
- `Delete`.

Selecting a saved analysis reloads it into the dashboard. In the observed reload flow, the
application restored the state, district, risk domain, metric, scenario, and period associated
with the saved item.

## 13. Multi-site My Analysis workflow

The My Analysis surface is divided into three functional areas:

- `Saved Analysis`;
- `Manage Portfolio`; and
- `Compare Portfolio`.

### 13.1 Manage Portfolio

After multiple sites are added, Manage Portfolio lists the selected site names. In the latest
observed district flow it contained Warangal and Karimnagar.

Each site has a remove control, and removing one site updates the list. `Clear Portfolio` removes
the complete current portfolio after confirmation.

The latest rerun did not display a separate numeric count banner; the authoritative visible count
was the number of entries in Manage Portfolio.

### 13.2 Saved Analysis within My Analysis

The Saved Analysis area lists previously saved analyses and provides a three-dot menu for each
row. The observed per-item actions were Rename and Delete.

### 13.3 Compare Portfolio

The observed comparison flow is:

1. Open `Compare Portfolio`.
2. Select a Risk Domain.
3. Select one or more metrics from the metric multi-select.
4. Choose the Scenario options.
5. Choose the Period.
6. Open the comparison Table or Visualizations area.

Scenarios are presented as checkboxes, allowing selections such as SSP2-4.5 and SSP5-8.5. Metric
selection is manual; the panel does not first show a separate advanced-mode switch.

When data loaded in the working comparison run, the table displayed one row per site and included:

- District Name;
- State Name;
- Scenario;
- Period;
- Index Value;
- Absolute Change;
- Change Percentile; and
- Level of Change.

The visualization view rendered a portfolio comparison heatmap with its legend.

The full-screen My Analysis presentation displayed a left/right layout: Saved Analysis and Manage
Portfolio on the left, with Compare Portfolio on the right. Manual follow-up confirmed the visible
full-screen flow after an automated rerun selected a hidden full-screen control belonging to the
adjacent Resilience Profile.

The My Analysis panel was also observed at a 375-pixel viewport without horizontal page overflow.

### 13.4 Downloads

The comparison surface exposed `Download Reports` and `Download heatmap` controls. Within the
download area, selecting the desired scenario and period enabled table/report downloads.

Observed outputs included:

- a portfolio comparison XLSX table; and
- a portfolio comparison heatmap image.

The generated XLSX used resolved district names rather than user-supplied coordinate labels.
During one mixed-state upload test, an Andhra Pradesh coordinate remained visible in the upload
list and portfolio but was omitted from a comparison generated in the Telangana analysis context;
the report contained three of the four portfolio sites and did not display a warning.

## 14. Responsive behavior observed

The QA evidence includes desktop, tablet, and mobile captures of the major authenticated
surfaces. The Administrative Panel, map/ranking workflow, Resilience Profile, and My Analysis
panel all rendered at the tested mobile width. The latest My Analysis run reported no horizontal
page overflow at 375 pixels.

## 15. Current observed workflow constraints

The following behaviors materially affect how a user moves through the current application:

- `Add to Analysis` requires both a location and completed resilience filters.
- Manual coordinates must be shown/resolved and staged before they are added to the portfolio.
- Administrative, coordinate, upload, and map entry modes do not yet provide reliable additive
  portfolio accumulation across every mode switch.
- The current accepted tabular upload schema uses `id,custom_name,lat,long`.
- Ranking Table is working in the latest observed vendor build and preserves dashboard context
  when toggled with Map View.
- Administrative duplicates are rejected with feedback; uploaded-coordinate duplicates are
  removed silently.
- Saved analyses can restore their geography and resilience-filter context.
- Multi-site comparisons support tables, heatmaps, and downloadable outputs.

## Evidence sources

This workflow is synthesized from live-observation records under:

- `qa/reports/VENDOR_REPORT.md`;
- `qa/reports/us06-header-nav.md` through `qa/reports/us17-analysis-profile.md`;
- `qa/reports/us-crossflow-add-to-analysis.md`;
- `qa/reports/us-map-interactivity.md`;
- `qa/reports/UPLOAD_VALIDATION_HANDOFF.md`; and
- the latest run artifacts under `qa/runs/2026-07-27*`.
