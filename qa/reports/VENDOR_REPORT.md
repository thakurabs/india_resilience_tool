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

---

## Informational — spec-drifts (NOT defects)

- "Geography Selection" panel is named **"Administrative Panel"**.
- US 10 panel is named **"Coordinate Panel"** (spec: "Coordinates Panel"); the sample
  formats are **.csv / .xlsx / .zip** (spec's "Zipped Shape File" = "Zipped shapefile (.zip)").
- **10 Risk Domains** offered vs spec's 8 (adds "Drought Risk (Advanced)", "Population Exposure").
- Scenario + Period are manual; **Statistic ("Mean") + Map Mode auto-default**.
- Map legend is a **continuous numeric scale**, not categorical "Very Low..Extreme".
- Map Mode shows "Absolute value" (greyed) vs spec's Predicted/Historical/Change modes.

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

---

## Coverage

- **Done:** US 09 (Geography), US 10 (Coordinates), US 11 (Filters), US 13 (Map),
  US 14 (Ranking — blocked).
- **Not yet covered:** US 15–17 (Save/reload, Profiles),
  US 01 (landing), US 05–08 (nav/profile/feedback).
- **Blocked on tooling:** US 02–04 (auth/2FA/reset) need a test email inbox — out of
  current autonomous scope; decide whether to cover.
