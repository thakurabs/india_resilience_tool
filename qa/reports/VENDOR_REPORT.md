# Vendor Report — Running Bug Log (India Resilience Tool, UAT)

**App:** dev.resilience.org.in · **Basis:** Resilience Actions User Stories v1.3
**Status:** *Running log — not yet sent.* We add findings as charters run and decide
what to send at the end.
**Method:** Playwright harness → Haiku evidence review → Opus triage. Every finding
below is verified against raw evidence (axe JSON / network results / screenshots);
none are model-inferred.

**Send priority (agreed):** B1 (blocker) → **M3** → M1 → M2 → minors.
**Send column:** `SEND` = queued to send · `HOLD` = keep, decide later ·
`ASK-PO` = confirm intent before filing · `INFO` = informational, not a defect.

---

## Findings (severity-ordered)

| # | Sev | Area | Story | Send | Finding |
|---|-----|------|-------|------|---------|
| B1 | **Blocker** | functional | US 12/14 | **SEND** *(sent)* | Ranking Table can't load — `GET /api/api/parquet/ranking` → HTTP 500; doubled `/api/api/` path. |
| M3 | Major | a11y | US 09/11 | **SEND** | Multiple text elements below WCAG AA contrast; error-red `#E75252` worst. |
| M1 | Major | a11y | US 09 | HOLD→keep | Collapsed-panel reopen affordance / toggle labelling for AT users. |
| M2 | Major | a11y | US 09/11 | HOLD→keep | Header brand link + icon has no accessible name (critical `image-alt`). |
| M4 | Major | functional | US 10 | **SEND** | "Show on Map" resolves a valid coordinate correctly **but fires a contradictory "Location could not be resolved" error toast** at the same time. |
| N1 | Minor | a11y | US 11 | HOLD | MapLibre attribution link not distinguishable without color (third-party control). |
| N2 | Minor | a11y | US 09 | HOLD | No `<main>` landmark; 9 blocks outside landmarks. |
| N3 | Minor | data | US 13 | ASK-PO | Map tooltip omits "Baseline (1990–2010)" and "Δ vs baseline / Level of Change". |
| N4 | Cosmetic | data | US 14 | HOLD | Internal slugs leak in ranking caption. |
| N5 | Minor | functional/a11y | US 10 | **SEND** | Missing/invalid coordinates turn the fields' **borders red only** — no error text (spec strings never appear) and no `aria-invalid`; color-only signal invisible to AT/colorblind users. |
| N6 | Minor | data/doc | US 10 | ASK-PO | CSV sample schema mismatch — app requires `id,custom_name,lat,long`; spec documents `Latitude,Longitude,Label`. |
| N7 | Cosmetic | data | US 10 | HOLD | Uploaded-coordinates list omits **District** (shows Custom/Point + Block only). |
| N8 | Minor | functional | US 15 | ASK-PO | Auto-trigger "Save / Don't Save" popup on context change (spec 794–812) **not observed** — unverified whether missing or gated on a stricter dirty-state precondition. |
| N9 | Minor | data/doc | US 15 | ASK-PO | Saved-item **tag taxonomy** drift — app emits "Single District" (outside spec's Multi-* set); older items carry **no tag**. |
| M5 | Major | functional/backend | US 16 | **SEND** | Resilience Profile with a **composite metric** fires `POST /api/api/parquet/trend` + `/scenario-comparison` → **HTTP 500** (×2). UI degrades to "No data available" (no crash), but it's the **same 500 + doubled `/api/api/` family as B1**. |
| N10 | Minor | a11y | US 16 | HOLD | Profile panel has **`nested-interactive`** controls (axe *serious*) — new vs prior charters; interactive elements nested inside a clickable accordion header. |
| N11 | Minor | functional | US 16 | ASK-PO | Trend chart offers **"Show model members" + "Max models to draw" slider** but **no "Show percentile band (p05–p95)"** control (spec 872). |
| N12 | Minor | data | US 16 | ASK-PO | Risk Summary shows **"Position in State"**, spec 861 says **"Position in India"** (scope/label drift; same family as the US 13 tooltip "Rank in state"). |

**US 15 headline (positive):** the full **Save → My Analysis list → Reload** loop
**works end-to-end** — save `201`, graceful duplicate-name guard `409`, blank⇒default
label, `/my-analysis` list with search + Rename/Delete, and **reload faithfully
restores** state + district + all filters (verified visually). No new blocker/major.

**US 16 headline (positive):** the **single-site Resilience Profile works end-to-end**
for a climate metric (10 steps, 0 failures, 0 errors on the climate-metric path).
Overview (geography / index / scenario / period), **Risk Summary** (Historical
Baseline · Projected Value · Δ-with-indicator · Position), a **Trend Over Time**
line chart (Historical + SSP series, *Show model members* → *Max models* slider),
a **Scenario Comparison** grouped bar chart (Historical/SSP2-4.5/SSP5-8.5 in the
spec's blue/orange/red, *Start y-axis at zero*), panel expand/collapse, and a
**full-screen modal** all render — including a clean **375px mobile** layout. The
one new backend defect (**M5**) is isolated to **composite** metrics.

---

## B1 — Ranking Table fails to load (HTTP 500)  ⛔ Blocker

- **What:** Switching to the Ranking Table view shows *"We couldn't load the ranking
  data for the selected filters. Please try again."* and 0 rows.
- **Root cause signal:** `GET https://dev.resilience.org.in/api/api/parquet/ranking`
  returns **HTTP 500**. Note the **doubled `/api/api/`** path segment — likely a
  base-URL/route-construction bug; check that first.
- **Impact:** Ranking Table (US 14) and view-mode switching (US 12) are fully
  unusable. Row selection, ranking order, colour coding, and legend can't be reached.
- **Discriminator:** Reproduces with state-only (all districts) **and** single-district
  (Adilabad). The **Map view renders the same filters correctly**, so the filter
  payload is valid — the failure is specific to the ranking endpoint.
- **Reproduce:**
  1. Log in; State = Telangana (District view).
  2. Resilience Filters: Risk Domain (Heat Risk) → Metric → Scenario → Period.
  3. Switch "Select your views" → **Ranking Table**.
  4. Error banner appears; Network shows the ranking request → 500.
- **Evidence:** `runs/…_us14-ranking/results.json` (rankingResponses 500), `s1-ranking.png`.
- **Verify fix:** ranking endpoint returns 200 + rows; re-run `us14` scenario (passes when data loads).

---

## M5 — Resilience Profile fires HTTP 500s on composite metrics

- **What:** In the Resilience Profile (US 16), selecting a **composite** metric
  (e.g. *Heat Risk Composite (score)*) and opening **Trend Over Time** +
  **Scenario Comparison** fires two backend calls that both return **HTTP 500**:
  - `POST https://dev.resilience.org.in/api/api/parquet/trend` → **500**
  - `POST https://dev.resilience.org.in/api/api/parquet/scenario-comparison` → **500**
- **Same family as B1:** identical **doubled `/api/api/`** path and a **500** on a
  `/api/api/parquet/*` endpoint — likely one shared root cause (base-URL/route
  construction + missing-data error handling). Fixing B1 should be checked against these.
- **Impact / severity nuance:** *non-blocking* — the UI degrades gracefully to
  **"No data available"** / **"No data available for the selected filters."** and
  does not crash. But a 500 is a server error: missing time-series for a composite
  should return **200 + empty** or **404**, not 500. Rated **Major** for the
  reproducible server error; downgrade to Minor if the graceful UI is deemed sufficient.
- **Discriminator:** the **climate-variable** metrics (Annual Mean Temperature, etc.)
  hit the **same two endpoints and return 200 with full charts** — 0 errors across
  S1–S9. The 500 is **specific to composite-score metrics**, which have no temporal
  series. So either the frontend should not request trend/scenario for composites,
  or the backend should answer empty instead of 500.
- **Related data behaviour (composite):** Risk Summary also **omits the Historical
  Baseline** card and shows **Δ = +0.00** for composites (see Informational below).
- **Reproduce:**
  1. Log in; State = Telangana, District = Warangal.
  2. Filters: Risk Domain = Heat Risk, Metric = **Heat Risk Composite (score)**, Scenario, Period, Statistic.
  3. Open the Resilience Profile → expand **Trend Over Time** and **Scenario Comparison**.
  4. Both show "No data available"; Network shows the two POSTs → 500.
- **Evidence:** `runs/…_us16-resilience-profile/results.json` (2× `httperror` 500),
  `s10-composite-nodata.png`. Contrast with `s5-trend.png` / `s7-scenario-comparison.png`
  (climate metric, 200 + charts).
- **Verify fix:** composite metric returns 200 (empty or populated) on
  `/api/api/parquet/trend` + `/scenario-comparison`; no 500 in Network; re-run `us16`
  scenario S10 (still shows "No data available" but with 0 real error events).

---

## M3 — Text below WCAG AA colour-contrast minimum  ★ top a11y priority

- **What:** Several text elements fall below the WCAG AA ratio (≥ 4.5:1 for normal
  text). axe `color-contrast` **serious** — 4 nodes on US 09, growing to 8 on US 11.
  This is a CSS/colour issue, not a labelling issue.
- **Confirmed offenders (worst-first):**
  1. **Error-red `#E75252`** (`.text-[14px].text-[#E75252]…`) — the colour of error
     messages incl. the B1 banner. `#E75252` on white ≈ **3.67:1**, fails AA for 14px
     normal text. *Highest impact — failure messages are the hardest to read.*
  2. **Placeholder text** (`.text-resilience-text-placeholder`) — dropdown/input hints.
  3. **Reset button** (`.dashboard-sidebar__reset-btn`) — muted grey label (correctly
     *labelled* for AT; this is a contrast-only issue).
- **Impact:** Low-vision, older, and colour-blind users, plus anyone on a poor display
  or in bright light (a field tool), may miss errors, field hints, and the Reset action.
- **Reproduce:**
  - Error text: run the B1 path → red `#E75252` banner → check contrast (DevTools /
    WebAIM) against its background → below 4.5:1.
  - Placeholder + Reset: open the Administrative Panel; check the two selectors above.
- **Evidence:** `runs/…_us11-filters/us11__axe.json` (`color-contrast`, serious, 8 nodes).
- **Verify fix:** each element ≥ 4.5:1 (darken `#E75252` toward e.g. `#C0392B`/`#B71C1C`).

---

## M1 — Panel collapse/expand affordance for AT users

- **What:** The sidebar toggle is icon-only. The DOM shows the "hide" control *does*
  carry `aria="Hide sidebar"`, so the original "no accessible name" call was too
  strong — kept for the **collapsed-state reopen** affordance, which should be
  re-checked: confirm an AT/keyboard user can rediscover and reopen the panel once
  collapsed, and that the reopen control is labelled ("Expand panel").
- **Impact:** If the collapsed-state reopen control is unlabelled or non-obvious,
  AT/keyboard users can get stuck with the panel hidden.
- **Reproduce:** Collapse the panel (`s12a-collapsed.png`); with a screen reader /
  keyboard only, attempt to reopen it and inspect the reopen control's accessible name.
- **Evidence:** `runs/…_us09-geography/s12a-collapsed.png`, `s12b-expanded.png`, `us09-final__dom.json`.
- **Verify fix:** collapsed-state reopen control has a clear `aria-label` and is
  reachable/operable by keyboard.

---

## M2 — Header brand link + icon has no accessible name (critical `image-alt`)

- **What:** The header logo / "back to resilience.org.in" link is
  `<a href="https://resilience.org.in/">` with **empty text and no `aria-label`**,
  wrapping `<img class="w-6 h-6">` with **no `alt`**. axe `image-alt` **critical**
  (selector `.hover:text-[#1a9ab8] > .w-6.h-6`), present on every screen (header).
- **Impact:** Screen readers announce a nameless link — users can't tell it's the
  logo or that it navigates **out** to the public `resilience.org.in` site.
  Fails WCAG 1.1.1 (non-text content) and 2.4.4 (link purpose).
- **Reproduce:** Inspect the top-left header logo icon → `<img>` has no `alt`, anchor
  has no text/`aria-label`. Screen-reader tab-through announces no meaningful name.
- **Evidence:** `runs/…_us09-geography/us09__axe.json`, `…_us11-filters/us11__axe.json` (`image-alt`, critical).
- **Verify fix:** add `alt`/`aria-label` naming the destination, e.g.
  `aria-label="India Resilience Tool home (resilience.org.in)"`.

---

## M4 — Contradictory "could not be resolved" toast on a *successful* coordinate lookup  ★ US 10

- **What:** In the Coordinate Panel → *Add Coordinates*, entering a valid, in-coverage
  point (Lat `17.8766`, Long `79.2792`) and clicking **Show on Map** resolves the
  location **correctly** — the panel shows *"This location is GHANPUR(STATION),
  TELANGANA"* and a pin is plotted — **yet a red error toast "Location could not be
  resolved for these coordinates." fires at the same moment.** Success and failure are
  reported simultaneously.
- **Reproducibility:** Confirmed on **two independent runs** (not a race/flake).
- **Impact:** Actively misleading. Users are told their action failed when it
  succeeded; many will re-enter, abandon, or distrust the plotted result. This is the
  primary "add a single site" path (US 10) and the first thing a user does.
- **Secondary a11y note:** the toast is a **class-less `<div>` appended to `<body>`
  with no `role="alert"`/`aria-live`** — screen-reader users get *neither* the success
  detail nor the (wrong) error announced.
- **Reproduce:**
  1. Open **Coordinate Panel** → **Add Coordinates**.
  2. Enter Lat `17.8766`, Long `79.2792` (any valid in-coverage point).
  3. Click **Show on Map** → inline "This location is GHANPUR(STATION), TELANGANA"
     appears **and** a red "Location could not be resolved…" toast pops top-right.
- **Evidence:** `runs/2026-07-08T09-22-12-815Z_us10-coordinates/s2-show-on-map.png`
  (both states visible together), `results.json` step **S2** (`CONTRADICTION: inline
  resolved … but error toast …`).
- **Verify fix:** on a successful resolve, no error toast fires; on a genuine failure
  (uncovered/ocean point), the error toast fires and the inline detail is not shown.
  Give the toast `role="alert"` / `aria-live="assertive"`.

---

## Minor / Cosmetic

- **N1 (Minor, a11y, US 11):** MapLibre attribution link not distinguishable without
  colour (axe `link-in-text-block`, `a[href$="maplibre.org/"]`). Third-party map
  control — vendor may be unable to fix directly. *HOLD / likely omit.*
- **N2 (Minor, a11y, US 09):** No `<main>` landmark; 9 content blocks outside any
  landmark (axe `landmark-one-main` + `region`). Structural markup; small fix.
- **N3 (Minor, data, US 13):** Map click-tooltip omits spec fields
  "Baseline (1990–2010)" and "Δ vs baseline / Level of Change" (shows District, State,
  Composite Score, Rank in state). *ASK-PO — may be an intended simplification.*
- **N4 (Cosmetic, data, US 14):** Internal slugs leak in the ranking caption
  ("composite_heat_risk • ssp245 • 2020-2040 • Mean") instead of friendly labels.
- **N5 (Minor, functional/a11y, US 10):** In *Add Coordinates*, clicking **Show on Map**
  with **empty** or **invalid** fields (e.g. Lat `999`, Long `abc`) turns the Latitude
  and Longitude **input borders red** (computed `oklch(0.637 0.237 25.331)` vs grey
  baseline `rgb(208,208,208)`) — so there *is* a visual signal — but: (a) **no error
  message text** ever appears; the spec's *"Latitude and Longitude are required"* and
  *"Enter valid coordinates, Decimal Degrees Only"* are absent; and (b) the inputs carry
  **no `aria-invalid`** and there is no text alternative, so the red border is a
  **colour-only** signal — invisible to screen-reader and colour-blind users (WCAG 1.4.1
  Use of Color, 3.3.1 Error Identification). Verified by DOM probe of computed
  `border-color` + `aria-invalid` on empty and `999`/`abc` submits.
  Evidence: `runs/…09-22-12…_us10-coordinates/{s3-missing-validation,s4-invalid-validation}.png`
  + border/aria DOM probe. *SEND — add error text + `aria-invalid`.*
  *(Independent Haiku review rated this Major; kept Minor as a visible red-border signal
  exists, but the missing text + colour-only + no-ARIA combination is a real gap.)*
- **N6 (Minor, data/doc, US 10):** The downloadable CSV/XLSX **sample** uses columns
  `id, custom_name, lat, long`, but the v1.3 spec documents `Latitude, Longitude,
  Label`. A file built to the **documented** schema is rejected *"Invalid file format.
  Please use the provided sample"*; the app's own sample uploads fine. Fix is either
  side (doc or accepted headers) — *ASK-PO*. Evidence: downloaded `sample_coordinates.csv`
  header vs `s7a-upload-documented-schema.png` (rejected) vs `s7b-upload-app-schema.png` (accepted).
- **N7 (Cosmetic, data, US 10):** The *My Uploaded Coordinates* list shows
  *Custom/Point + Block* only (e.g. "Site A, Nampally"; "Point 3, Vijayawada Urban");
  spec lists *Custom Name / Point, Block Name, **District Name*** (e.g. "ABC,
  Hanamkonda, Warangal"). **District** appears omitted. Evidence: `s7b-upload-app-schema.png`.
- **N8 (Minor, functional, US 15):** The spec (§794–812) describes an **auto-trigger
  save**: with an unsaved analysis (location + filters), attempting to *change State /
  change View Zone / switch Geography↔Coordinates* should raise a **"Save / Don't Save"**
  popup. In testing, changing State on a freshly-built (unsaved) analysis produced **no
  such prompt**. This could be (a) not implemented, or (b) gated on a stricter
  "dirty-state" precondition (e.g. requires *Add to Analysis* first) that the test did
  not satisfy — the evidence can't distinguish these. **Manual Save is unaffected and
  works fully.** *ASK-PO — confirm whether auto-save is in scope and what triggers it.*
  Evidence: `runs/…_us15-my-analysis/results.json` step S12, `s12-context-change.png`.
- **N9 (Minor, data/doc, US 15):** Saved-analysis **tag** taxonomy drifts from spec.
  The app decorates the stored name as `<label> - <Tag> - <Date>` and emits
  **"Single District"** for a single-district save (e.g. *"QA US15 … - Single District -
  08 Jul, 2026"*) — a tag **not** in the spec's enumerated set (Multi-District /
  Multi-Block / Multi-Coordinates). Older saved items (`14 sites_July5`, `My Analysis`,
  `test1/test2`) carry **no tag at all**, so the tag is inconsistent across items.
  *ASK-PO — align the tag set (doc vs app) and backfill/normalise older items.*
  Evidence: `runs/…_us15-my-analysis/s7-my-analysis-route.png`, POST body `name` field.

**US 15 corroborates existing a11y findings (no new item):** the axe scan on the
save/list flow re-surfaces **M2** (`image-alt` critical, header brand), **M3**
(`color-contrast` serious incl. `#E75252`), and **N1** (`link-in-text-block`, MapLibre)
— all app-chrome issues already filed. This strengthens M2/M3; it is not a separate US 15 defect.

---

## US 15 — behaviours VERIFIED matching spec (no defect)

The full **Save List & Reload** loop was exercised end-to-end (12 steps, 0 failures):

- **Save gating:** *Save Analysis* disabled with no location/filters; enabled once an
  analysis (State + District + filters) is built.
- **Save modal:** title "Save Analysis", "Analysis Name" input (placeholder "My
  Analysis"), Cancel + Save Analysis. Blank name ⇒ **default label** (spec's two options
  collapse into one blank-or-filled input).
- **Save API:** unique name → `POST /api/api/saved-analyses` **201** + toast *"Analysis
  saved successfully."*; **duplicate name → 409** with a correct red toast *"An analysis
  with this name already exists"* (graceful guard, no crash); blank/default when the
  default already exists → 409 (confirms blank⇒default).
- **Listing:** *Welcome > My Analysis* → route **`/my-analysis`** (breadcrumb
  "Dashboard / My Analysis", heading, **Search Analysis** box). Each row: label + date
  (+ tag when set) + a **⋮ Actions** menu (`aria-label="Actions for …"`, `aria-haspopup=menu`).
- **Row actions:** ⋮ menu offers **Rename** and **Delete** (matches US 17 §922–924).
- **Reload:** clicking a saved row restores the dashboard — State=Telangana,
  District=Warangal, filters (Heat Risk Composite / SSP2-4.5 / 2020-2040 / Mean), map
  legend reflects them; `composite-map-data` returns 200. **Verified visually** (`s9-reloaded.png`).
- **Search:** typing in *Search Analysis* filters the list (`GET …?search=…`).

*Note:* the doubled **`/api/api/`** path (seen on the B1 ranking blocker) is also present
on `saved-analyses` and `composite-map-data`, but here every call returns 2xx — so the
doubled segment itself is not fatal; B1's 500 is a ranking-endpoint-specific failure.

---

## US 16 — behaviours VERIFIED matching spec (no defect)

Single-site Resilience Profile exercised end-to-end (10 steps, 0 failures; 0 real
error events on the climate-metric path, `Annual Mean Temperature`):

- **Empty state:** with no location, the panel body reads **"No location(s)
  selected"** (spec 848's "select a location/coordinate to view Insights" — wording
  drift only).
- **Profile Overview:** heading **"Warangal — District Climate Profile"** +
  **Index / Scenario / Period** lines, all populated from the active filters.
- **Risk Summary:** **Historical Baseline** (28.27 °C) · **Projected Value**
  (28.69 °C) · **Δ vs baseline** with up/down indicator (**↑ +0.42**) · **Position**
  (7) — all present, no NaN/blank. (Label drift → **N12**.)
- **Trend Over Time (State Average):** SVG line chart, **Historical + SSP245**
  series, historical/projection divider ~2020, **Year** X-axis, metric Y-axis;
  **"Show model members"** → **"Max models to draw" slider** (default 5) appears
  (spec 871 ✓). (Percentile-band control absent → **N11**.)
- **Scenario Comparison (Period-Mean):** SVG grouped bar chart —
  **Historical (blue) / SSP2-4.5 (orange) / SSP5-8.5 (red)** matching the spec's
  colour codes — grouped by period; **"Start y-axis at zero"** control/note present.
- **Panel interaction:** expand/collapse toggle + **full-screen modal**
  (`role=dialog`, ~1354×792) with a **"Close expanded Resilience Profile view"**
  control; the modal shows the same profile (spec 896 "same functionalities").
- **Dynamic update + data hygiene:** charts re-render on filter change; 0 NaN/blank
  in the DOM scan; **375px mobile** layout renders the full panel cleanly.

**US 16 corroborates existing a11y findings (+1 new candidate):** the axe scan
re-surfaces **M2** (`image-alt` critical), **M3** (`color-contrast` serious),
**N1** (`link-in-text-block`), **N2** (`landmark-one-main`/`region`) — all
app-chrome, already filed. **New:** **N10** `nested-interactive` (*serious*) —
not seen on prior charters; the profile-panel accordion header nests interactive
controls.

---

## Informational — spec-drifts (NOT defects)

- "Geography Selection" panel is named **"Administrative Panel"**.
- US 10 panel is named **"Coordinate Panel"** (spec: "Coordinates Panel"); the sample
  formats are **.csv / .xlsx / .zip** (spec's "Zipped Shape File" = "Zipped shapefile (.zip)").
- **10 Risk Domains** offered vs spec's 8 (adds "Drought Risk (Advanced)", "Population Exposure").
- Scenario + Period are manual; **Statistic ("Mean") + Map Mode auto-default**.
- Map legend is a **continuous numeric scale**, not categorical "Very Low..Extreme".
- Map Mode shows "Absolute value" (greyed) vs spec's Predicted/Historical/Change modes.
- **US 15:** the saved-analysis list is reached **top-right** via *"Welcome, <name>" →
  My Analysis* (route `/my-analysis`), **not** the top-left nav dropdown the spec
  (line 815) describes. The feature works fully; only the entry-point position differs.
- **US 16 empty-state wording:** panel says **"No location(s) selected"** vs spec 848's
  "select a location or coordinate to view Insights" — functionally equivalent.
- **US 16 composite Risk Summary:** for a **composite** metric the Risk Summary
  **omits the Historical Baseline** card and shows **Δ = +0.00** (composites have no
  historical value). Confirm this is intended and that +0.00 is not read as "no change".
- **US 16 full-screen modal (single-site):** the modal shows the profile only; the
  spec 897–899 **left/right split** (Left: Saved Analysis / Manage Portfolio / Refine
  your filters; Right: Compare Portfolio) is **not present for a single site with an
  empty portfolio** — this appears to be the **US 17 (multi-site)** surface. Not a
  single-site defect; will verify under US 17.
- **US 16 Recharts warning (INFO):** two console warnings *"width(-1)/height(-1) of
  chart should be greater than 0"* — a chart mounts into a zero-size container
  (collapsed accordion / modal transition). Cosmetic; no user-visible break.

**US 10 behaviours VERIFIED matching spec (no defect):** Add/Upload mode toggle;
Lat/Long/Custom-Name inputs; **Add to Analysis** and **Save Analysis** disabled by
default (no location + no filters); three sample-download links; unnamed upload row →
"Point N" numbering; upload validation — wrong structure → *"Invalid file format.
Please use the provided sample"*, unsupported type (`.txt`) → *"Unsupported file format"*.

---

## Open questions for PO / vendor (confirm before filing as defects)

- **N3:** Should the map tooltip include Baseline (1990–2010) and Δ / Level of Change?
- **Map Mode:** Is "Absolute value" (greyed, only option) intended vs the spec's
  Predicted / Historical / Change modes?
- **N6 (US 10):** Fix the CSV/XLSX schema mismatch doc-side (update the spec/sample
  columns) or app-side (also accept `Latitude,Longitude,Label`)?
- **US 10 mode-switch note:** Spec 446–447 says switching Geography ↔ Coordinates
  should raise a "save or proceed without saving" note. Not observed with an empty
  analysis — is it gated on unsaved work, or missing? (`s10-mode-switch.png`.)
- **N8 (US 15) auto-save:** Is the auto-trigger "Save / Don't Save" popup on
  State/View-Zone/panel change (spec 794–812) in scope? If yes, what exactly marks an
  analysis "unsaved/dirty" — building filters, or only after *Add to Analysis*? (This
  is the same mutual-exclusivity behaviour as the US 10 mode-switch note above.)
- **N9 (US 15) tags:** Which tag set is authoritative — should single-site saves be
  tagged "Single District/Block" (app) or is the spec's Multi-* set the intended
  taxonomy? Should older untagged items be backfilled?
- **M5 (US 16) composite charts:** Should **composite-score** metrics show Trend +
  Scenario Comparison at all (they have no time series)? If not, the frontend should
  skip those requests; either way the backend should not return **500** for missing data.
- **N11 (US 16) percentile band:** Is the **"Show percentile band (p05–p95)"** control
  (spec 872) in scope? Only "Show model members" + a "Max models to draw" slider exist today.
- **N12 (US 16) position scope:** Is the Risk Summary rank **state-relative
  ("Position in State")** as implemented, or **India-wide ("Position in India")** per
  spec 861? (Same question as the US 13 tooltip "Rank in state".)
- **US 16 modal split:** Confirm the left/right **Saved Analysis / Manage Portfolio /
  Refine filters / Compare Portfolio** split is intended **only** for multi-site
  (US 17) with a non-empty portfolio, not for a single-site profile.

---

## Coverage

- **Done:** US 09 (Geography), US 10 (Coordinates), US 11 (Filters), US 13 (Map),
  US 14 (Ranking — blocked), US 15 (My Analysis — Save/reload — passing),
  **US 16 (Resilience Profile / single-site — passing; M5 composite-500)**.
- **Not yet covered:** US 17 (My Analysis Profile / multi-site portfolio — the
  Manage Portfolio / Compare Portfolio / left-right-split surface), US 01 (landing),
  US 05–08 (nav/profile/feedback).
- **US 15 mobile caveat — partially closed:** the 375px shot for US 16 confirms the
  right-hand **profile panel** renders cleanly on mobile (same panel family). The
  `/my-analysis` **saved-list** route specifically is still unverified at 375px —
  capture it under US 17.
- **Blocked on tooling:** US 02–04 (auth/2FA/reset) need a test email inbox — out of
  current autonomous scope; decide whether to cover.
