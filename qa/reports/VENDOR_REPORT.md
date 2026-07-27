# Vendor Report — Running Bug Log (India Resilience Tool, UAT)

**App:** dev.resilience.org.in · **Basis:** Resilience Actions User Stories v1.3
**Status:** *Running log — not yet sent.* We add findings as charters run and decide
what to send at the end.
**Method:** Playwright harness → Haiku evidence review → Opus triage. Every finding
below is verified against raw evidence (axe JSON / network results / screenshots);
none are model-inferred.

**Send priority (agreed):** B1 (blocker) → **M3** → M1 → M2 → minors.
**Send column:** `SEND` = queued to send · `SENT` = reported to vendor ·
`HOLD` = keep, decide later · `NEEDS-REPRO` = verified in evidence but not yet
reproduced manually · `DEPRIORITISED` = parked, revisit later ·
`ASK-PO` = confirm intent before filing · `INFO` = informational, not a defect.

**Reported to vendor so far:** B1, M3, M6, N5.

---

## Re-run update - 2026-07-27 vendor build

**Run scope:** priority batch first, then full US06-US17 sweep, plus adversarial upload,
cross-flow, and map-interactivity probes. Session smoke passed:
`qa/runs/2026-07-27T05-24-23-708Z_dashboard-root`.

**Harness note:** the build now exposes three controls matching `/Administrative Panel/i`
(`Administrative Panel`, `Reset administrative panel`, `Expand administrative panel`).
The QA harness was updated to use the exact expand/header controls before rerunning
affected charters.

### Fixed / not reproduced

| Prior # | Rerun verdict | Evidence |
|---|---|---|
| **B1** | **FIXED.** Ranking Table loads with 34 rows; no load-failure banner and no ranking 5xx observed. | `qa/runs/2026-07-27T06-39-41-576Z_us14-ranking/results.json` |
| **M3** | **FIXED in axe rerun.** Latest US09/US11 axe reports have no `color-contrast` violations. | `qa/runs/2026-07-27T06-34-27-546Z_us09-geography/us09__axe.json`; `qa/runs/2026-07-27T06-38-09-252Z_us11-filters/us11__axe.json` |
| **M1** | **NOT REPRODUCED.** Collapse/expand path retained state, and latest axe did not flag the sidebar controls. | `qa/runs/2026-07-27T06-34-27-546Z_us09-geography/results.json` |
| **M2** | **FIXED in axe rerun.** Latest US09/US11 axe reports have no `image-alt` violations. | same US09/US11 axe files above |
| **M4** | **STILL FIXED.** Valid Show-on-Map resolve returned Ghanpur Station and no error toast. | `qa/runs/2026-07-27T06-34-59-550Z_us10-coordinates/results.json` |
| **C4** | **FIXED functionally.** Same coordinate with different name no longer creates an extra row; dedup is silent. | `qa/runs/2026-07-27T06-08-58-632Z_us-crossflow-add-to-analysis/results.json` |
| **Map Claim 1** | **NOT REPRODUCED.** Probe saw view-change/drill-down behavior, not dropdown interactivity lock. | `qa/runs/2026-07-27T06-18-33-056Z_us-map-interactivity/results.json` |
| **Map Claim 2** | **FIXED.** Map-first and dropdown-first both ended with 4 sites; commutative. | same map-interactivity run |

### Changed / still needs triage

| Prior # | Rerun verdict | Evidence / note |
|---|---|---|
| **M5** | **STILL OPEN, shifted context.** US16 no longer records real error events, but US17 comparison still records `POST /api/api/parquet/trend` and `/scenario-comparison` 500s. | `qa/runs/2026-07-27T06-44-59-423Z_us17-analysis-profile/results.json` |
| **M6** | **CHANGED.** The wrong "1 district" banner is gone, but no count banner is shown while Manage Portfolio lists Warangal + Karimnagar. | same US17 run |
| **M7** | **MOSTLY FIXED.** Invalid/out-of-range/non-numeric/empty/out-of-India/formula/long-name CSV and XLSX fixtures now reject; out-of-India shapefile rejects. Remaining quirks: Unicode CSV and CSV-renamed-XLSX show no visible feedback; baseline/long-name/junk shapefile zips are accepted. | `qa/runs/2026-07-27T06-05-40-557Z_us10-adversarial-upload/adversarial-results.json`; `qa/runs/2026-07-27T06-07-30-289Z_us10-adversarial-formats/adversarial-results.json` |
| **N5** | **CHANGED.** Missing/non-numeric manual coordinates now keep Show on Map disabled; old post-click red-border-only path is not reachable in the harness. Needs manual a11y wording check if disabled-only gating is accepted. | `qa/runs/2026-07-27T06-34-59-550Z_us10-coordinates/results.json` |
| **B4** | **STILL OPEN / ASK-PO.** Portfolio still does not accumulate across Administrative/Coordinate/Upload/Map modes; final count 1 vs expected approximately 6. | `qa/runs/2026-07-27T06-08-58-632Z_us-crossflow-add-to-analysis/results.json` |

### New or regressed candidates from sweep

| Candidate | Severity draft | Evidence |
|---|---|---|
| **R1 - US15 Save Analysis blocked** | Major / regression | After building an analysis, Save Analysis remains disabled; save modal/reload path could not be reached. The interrupted run did not finalize `results.json`, but screenshots `s2-analysis-built.png`, `s7-my-analysis-route.png`, `s10-row-menu.png`, and `s11-search.png` were captured under `qa/runs/2026-07-27T06-40-09-343Z_us15-my-analysis/`. |
| **R2 - US16 Resilience Profile content missing** | Major / regression candidate | Climate metric path builds, but overview/risk summary/trend/scenario/full-screen content are missing or empty with no real error events. | `qa/runs/2026-07-27T06-42-45-078Z_us16-resilience-profile/results.json` |
| **R3 - US08 Share Feedback popup incomplete/not opening** | Minor / needs repro | Full sweep S1 failed to open popup; S2 saw only 2 radio inputs, one option label, no Tell Us More, no Submit. | `qa/runs/2026-07-27T06-34-12-279Z_us08-feedback/results.json` |
| **R4 - US17 modal selector/content drift** | Minor / harness-or-product triage | Analysis full-screen step resolves a hidden Resilience Profile button and times out; panel otherwise renders at 375px. Needs manual/selector verification before filing. | `qa/runs/2026-07-27T06-44-59-423Z_us17-analysis-profile/results.json` |

### Clean regression-watch paths

US06, US07, US09, US11, US12, US13, and US14 completed with zero hard failures in
the final sweep. US07 still carries the prior State-field and Update-vs-Save spec
drifts. US10 valid coordinate resolve remains clean; upload schema drift remains
(`Latitude/Longitude/Label` rejected, `id/custom_name/lat/long` accepted).

---

## Findings (severity-ordered)

| # | Sev | Area | Story | Send | Finding |
|---|-----|------|-------|------|---------|
| B1 | **Blocker** | functional | US 12/14 | **SENT** | Ranking Table can't load — `GET /api/api/parquet/ranking` → HTTP 500; doubled `/api/api/` path. |
| M3 | Major | a11y | US 09/11 | **SENT** | Multiple text elements below WCAG AA contrast; error-red `#E75252` worst. Includes internal slugs bleeding into the ranking-table view. |
| M1 | Major | a11y | US 09 | HOLD→keep | Collapsed-panel reopen affordance / toggle labelling for AT users. |
| M2 | Major | a11y | US 09/11 | HOLD→keep | Header brand link + icon has no accessible name (critical `image-alt`). |
| M4 | Major | functional | US 10 | **RESOLVED** (re-verified 2026-07-13) | "Show on Map" fired a contradictory "Location could not be resolved" error toast on a successful resolve. **No longer reproducible:** across 15 fresh cold-load attempts the toast is never shown and, per a `MutationObserver`, the toast node is **never inserted** (0/6 decisive); resolves return **200** with correct inline text + pin (15/15). The earlier "3/3" was a false positive from an `innerText` text-scrape heuristic, not a visible toast. See M4 detail for verification method. |
| M7 | Major | functional/data | US 10 | HOLD | **No value or geographic validation on coordinate upload.** Out-of-range (lat 999 / long −500 / 1e9), non-numeric (`abc`), empty, unbounded-name (5000 chars), and out-of-India (London/Pacific/Null Island) coordinates are all **accepted, resolved to blocks, and plotted** — identically across **CSV, XLSX, and shapefile (.zip)**. All uploads return HTTP 200 → the only rejection is client-side structural (wrong shape / unsupported type / >1 MB); cell values are never validated. |
| N1 | Minor | a11y | US 11 | HOLD | MapLibre attribution link not distinguishable without color (third-party control). |
| N2 | Minor | a11y | US 09 | HOLD | No `<main>` landmark; 9 blocks outside landmarks. |
| N3 | Minor | data | US 13 | ASK-PO | Map tooltip omits "Baseline (1990–2010)" and "Δ vs baseline / Level of Change". |
| N4 | Cosmetic | data | US 14 | HOLD | Internal slugs leak in ranking caption. |
| N5 | Minor | functional/a11y | US 10 | **SENT** | Missing/invalid coordinates turn the fields' **borders red only** — no error text (spec strings never appear) and no `aria-invalid`; color-only signal invisible to AT/colorblind users. |
| N6 | Minor | data/doc | US 10 | ASK-PO | CSV sample schema mismatch — app requires `id,custom_name,lat,long`; spec documents `Latitude,Longitude,Label`. |
| N7 | Cosmetic | data | US 10 | HOLD | Uploaded-coordinates list omits **District** (shows Custom/Point + Block only). |
| N8 | Minor | functional | US 15 | ASK-PO | Auto-trigger "Save / Don't Save" popup on context change (spec 794–812) **not observed** — unverified whether missing or gated on a stricter dirty-state precondition. |
| N9 | Minor | data/doc | US 15 | ASK-PO | Saved-item **tag taxonomy** drift — app emits "Single District" (outside spec's Multi-* set); older items carry **no tag**. |
| M5 | Major | functional/backend | US 16 | **DEPRIORITISED** | Resilience Profile with a **composite metric** fires `POST /api/api/parquet/trend` + `/scenario-comparison` → **HTTP 500** (×2). UI degrades to "No data available" (no crash), but it's the **same 500 + doubled `/api/api/` family as B1**. Parked — may not be a real bug (composites have no time series); revisit after B1 fix. |
| N10 | Minor | a11y | US 16 | HOLD | Profile panel has **`nested-interactive`** controls (axe *serious*) — new vs prior charters; interactive elements nested inside a clickable accordion header. |
| N11 | Minor | functional | US 16 | ASK-PO | Trend chart offers **"Show model members" + "Max models to draw" slider** but **no "Show percentile band (p05–p95)"** control (spec 872). |
| N12 | Minor | data | US 16 | ASK-PO | Risk Summary shows **"Position in State"**, spec 861 says **"Position in India"** (scope/label drift; same family as the US 13 tooltip "Rank in state"). |
| M6 | Major | data/functional | US 17 | **SENT** | Portfolio **count banner reads "You have added 1 district"** while Manage Portfolio holds **2** (Warangal + Karimnagar) — the total is wrong (and not pluralised). Spec 912–914. |
| N13 | Minor | data | US 17 | ASK-PO | Multi-site comparison **Table omits "Position in State"** (spec 980 lists it first). |
| N14 | Minor | a11y | US 17 | HOLD | Comparison table / heatmap scroll container is **not keyboard-focusable** (axe `scrollable-region-focusable`, *serious*, new). |
| N15 | Minor | functional | US 17 | ASK-PO | No **"Refine your filters"** section inside the panel/modal (spec 897/1035). Top *Select Resilience Filters* panel is separate. |
| N16 | Minor | functional | US 17 | ASK-PO | No **auto-metrics note** ("N metrics from 1 domain(s)…", spec 962–966). |
| N17 | Cosmetic | functional | US 17 | ASK-PO | No **"Advanced Metric / Manually refine" checkbox** (spec 968–972); a *Select Metrics* multi-select is used instead. |
| N18 | Cosmetic | functional | US 17 | ASK-PO | **Scenario as checkboxes** (SSP2-4.5 / SSP5-8.5), not the spec's *Single / Compare Scenario* modes (spec 951–961). Functionally equivalent. |
| N19 | Minor | data | US 07 | ASK-PO | User Profile **omits the "State" field** (spec 280 & 289 list State among profile fields). Only Country (India, locked) shown. |
| N20 | Cosmetic | functional | US 07 | ASK-PO | Profile save button labelled **"Update"** (spec 302 says **"Save"**). |
| N21 | Minor | functional | US 08 | ASK-PO | Feedback popup **auto-triggers on a timer mid-session** (unprompted). Spec 08 defines only header-manual + logout-auto triggers, not a timed nudge. |
| N22 | Minor | data/doc | US 01 | ASK-PO | Logged-in header shows no **Donate button** or **Resustainability logo** (spec 46–49). May exist only on the pre-auth landing — confirm (pre-auth surface unverifiable while logged in). |
| N23 | Minor | security | US 10 | HOLD | **Formula/CSV injection (CWE-1236) stored verbatim.** `=cmd\|…`, `@SUM(…)`, `+…`, `-…` in `custom_name` are accepted + shown verbatim (CSV & XLSX) in portfolio labels **and** the auto-derived saved-analysis name. **Currently latent:** both plausible download sinks are clean — Compare-Portfolio report exports **district names only**, and saved-analysis Actions = Rename/Delete (no export). No live Excel-formula sink found; fix as defense-in-depth. See detail §N23. |
| N24 | Cosmetic | data | US 10 | HOLD | Shapefile upload **silently drops the `.dbf` custom_name** — every point auto-labelled "Point N"; user site names are lost (injection inert on this path). |
| N25 | Minor | data | US 10/17 | ASK-PO | **Out-of-pilot-state uploaded site silently dropped** from Compare Portfolio. A site resolving to Vijayawada Urban (Ntr, Andhra Pradesh) shows in the upload list + portfolio but is **absent from the comparison report** (3 of 4 sites exported), with no warning. |

**US 15 prior-pass headline (superseded by 2026-07-27 rerun update above):** the full **Save → My Analysis list → Reload** loop
**works end-to-end** — save `201`, graceful duplicate-name guard `409`, blank⇒default
label, `/my-analysis` list with search + Rename/Delete, and **reload faithfully
restores** state + district + all filters (verified visually). No new blocker/major.

**US 16 prior-pass headline (superseded by 2026-07-27 rerun update above):** the **single-site Resilience Profile works end-to-end**
for a climate metric (10 steps, 0 failures, 0 errors on the climate-metric path).
Overview (geography / index / scenario / period), **Risk Summary** (Historical
Baseline · Projected Value · Δ-with-indicator · Position), a **Trend Over Time**
line chart (Historical + SSP series, *Show model members* → *Max models* slider),
a **Scenario Comparison** grouped bar chart (Historical/SSP2-4.5/SSP5-8.5 in the
spec's blue/orange/red, *Start y-axis at zero*), panel expand/collapse, and a
**full-screen modal** all render — including a clean **375px mobile** layout. The
one new backend defect (**M5**) is isolated to **composite** metrics.

**US 17 prior-pass headline (superseded by 2026-07-27 rerun update above):** the **multi-site portfolio works end-to-end** (12
steps, 0 failures, 0 errors). *Add to Analysis* → a ≥2-site portfolio; **Manage
Portfolio** lists each site with a working **Remove ⊗** and **Clear Portfolio**;
**Compare Portfolio** (risk domain → *All Metrics (14)* → SSP2-4.5 / Early century)
loads a per-metric **comparison Table** (District/State/Scenario/Period/Index Value/
Absolute Change/Change Percentile/Level of Change, one row per site) and a
**Heatmap** — both via `portfolio-comparison-table` + `portfolio-heatmap`, **HTTP
200, no 500s**. The **full-screen modal** shows the spec's **left/right split**
(Saved Analysis + Manage Portfolio | Compare Portfolio), and the panel is clean at
**375px** — which **closes the open US 15 mobile caveat** for this panel family.
The one real bug (**M6**) is a wrong portfolio count; the rest are spec-label drifts.

**US 12 prior-pass headline (superseded by 2026-07-27 rerun update above):** the **Map View ↔ Ranking Table toggle works to spec**
(5 steps, 0 failed) — Map View default, **mutual exclusivity** (switching hides the
other view), and **geography + filters preserved** across the switch. The only issue
is that the Ranking Table has no data to show — the existing **B1** blocker (ranking
endpoint → 500). Note: in this build the failing call is **`POST /api/api/parquet/ranking`**
(B1 was originally logged as `GET`); same endpoint, doubled path, and 500.

**Chrome stories (US 06/07/08) — all passing (read-only, session-safe).**
- **US 06 (Header & Dropdown Nav):** logo + title + "Welcome, [Name]" + Share
  Feedback; dropdown = **User Profile · My Analysis · Logout**; routes to `/profile`
  and `/my-analysis` work. (Logout not clicked — session-safe.) No findings.
- **US 07 (User Profile):** `/profile` shows Name · Email (**locked**) · Organization ·
  Designation · Purpose of Use · Thematic Activity · Country (India, locked) · Reset
  Password (Send OTP). Two drifts: **State field missing (N19)** and **"Update" vs
  "Save" (N20)**. Edit/Save/Reset not triggered.
- **US 08 (Feedback):** Share Feedback opens a popup with 5 experience radios +
  "Tell us more" + star rating + Submit + close — matches spec. **Submit never
  clicked** (emails admin). Timed mid-session auto-popup noted (**N21**); logout
  auto-trigger variant not verifiable without logout.

**Blocked / partial (see `blocked-and-partial-stories.md`):** US 01 (pre-auth landing —
unverifiable while logged in; **N22**), US 02–04 (need a test-email inbox), US 05
(first-visit guide — already onboarded, won't re-trigger; absence is spec-consistent).

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

## M6 — Portfolio count banner is wrong (says 1, holds 2)  ★ US 17

- **What:** After adding two districts to the portfolio (Warangal, then Karimnagar),
  the green banner at the top of **My Analysis** reads **"You have added 1 district
  in your portfolio."** — but **Manage Portfolio** correctly lists **both** sites,
  each with a working Remove ⊗. Spec 912–914 expects the true total
  (*"You have added 2 districts to your portfolio"*).
- **Two problems in one string:** (a) the **count is wrong** (1 vs 2); (b) even for
  one site it is not pluralised against the count. The banner appears to reflect the
  *last add action*, not the portfolio total.
- **Impact / severity:** the portfolio itself is correct (2 sites compared in the
  Table + Heatmap), so this is a **display defect**, not data loss — but it directly
  contradicts a headline acceptance-criterion number the user reads first. Rated
  **Major** as a user-facing correctness bug; downgrade to Minor if treated as cosmetic.
- **Reproduce:**
  1. Log in; State = Telangana, District = Warangal → **Add to Analysis**.
  2. Add a 2nd district (Karimnagar) → **Add to Analysis**.
  3. Expand **My Analysis**: banner says "1 district"; Manage Portfolio lists 2.
- **Evidence:** `runs/2026-07-08T15-32-52-396Z_us17-analysis-profile/` — `results.json`
  step S3 (`count banner says "1 district" but Manage Portfolio lists 2`), `s3-two-sites.png`,
  `s4-manage.png`.
- **Verify fix:** banner shows "2 districts" with correct pluralisation; re-run `us17` S3.

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

## M4 — Contradictory "could not be resolved" toast on a *successful* coordinate lookup  ★ US 10  — ✅ RESOLVED (re-verified 2026-07-13)

> **Status: RESOLVED / no longer reproducible (re-verified 2026-07-13).** Re-tested against
> the live `dev.resilience.org.in` build. Across **15 fresh cold-load attempts** (every context
> is a brand-new "first Show-on-Map"), the error toast was **never visible** in any screenshot,
> and a `MutationObserver` armed on `<body>` before the click — which cannot miss a node
> insertion regardless of timing — recorded the toast node being inserted **0/6 times**.
> Every resolve returned `POST /api/api/geo/reverse-geocode` **200** with the correct inline
> "This location is …" text and a plotted pin (**15/15**).
>
> **Correction to the original finding:** the "reproduced 3/3" claim came from `repro-m4.mjs`,
> which decides "toast fired" by regex-scraping `document.body.innerText` — it never inspects
> pixels or DOM node insertions. That heuristic produced false positives (matches at ~57–61ms
> with **no** visible toast in the +760ms screenshot and **no** corresponding DOM node). The
> authoritative `MutationObserver` re-test finds no toast. M4 is considered fixed.
>
> Residual note: all false-positive matches occurred on the very first activity of a fresh
> login session; a first-resolve-after-fresh-token cold path could not be re-triggered and is
> not considered a live defect.
>
> _Original finding preserved below for the record._

- **What:** In the Coordinate Panel → *Add Coordinates*, entering a valid, in-coverage
  point (Lat `17.8766`, Long `79.2792`) and clicking **Show on Map** resolves the
  location **correctly** — the panel shows *"This location is GHANPUR(STATION),
  TELANGANA"* and a pin is plotted — **yet a red error toast "Location could not be
  resolved for these coordinates." fires at the same moment.** Success and failure are
  reported simultaneously.
- **Reproducibility:** Confirmed on **5 independent runs** (2 original + **3/3 on a
  dedicated video-repro run**, `qa/harness/repro-m4.mjs` → `runs/repro-m4/`). The
  trigger is the **first "Show on Map" of a fresh page load**; it does **not** fire in
  a warmed session (each Playwright run is a brand-new context = first-ever click),
  which is why manual retesting in one session appeared not to reproduce it.
- **Root cause (corrected):** the resolve API `POST /api/api/geo/reverse-geocode`
  returns **HTTP 200** — the backend succeeds and supplies the inline "This location
  is GHANPUR(STATION), TELANGANA". The error toast appears **~57–61ms after the click**,
  faster than the network round-trip, so it is fired **optimistically by the frontend,
  independent of the (successful) response**. This is a **frontend logic bug**, not a
  cold-load/network race and not a backend error.
- **Impact:** Actively misleading. Users are told their action failed when it
  succeeded; many will re-enter, abandon, or distrust the plotted result. This is the
  primary "add a single site" path (US 10) and the first thing a user does.
- **Secondary a11y note:** the toast is a **class-less `<div>` appended to `<body>`
  with no `role="alert"`/`aria-live`** — screen-reader users get *neither* the success
  detail nor the (wrong) error announced.
- **Reproduce (must be a cold load):**
  1. **Hard-reload** the page (Ctrl/Cmd+Shift+R) — the toast only fires on the
     **first** Show-on-Map action; a warmed session will not reproduce it.
  2. Open **Coordinate Panel** → **Add Coordinates**.
  3. Enter Lat `17.8766`, Long `79.2792` (any valid in-coverage point).
  4. Click **Show on Map** → inline "This location is GHANPUR(STATION), TELANGANA"
     appears **and** a red "Location could not be resolved…" toast pops top-right.
  - Automated: `node qa/harness/repro-m4.mjs 3` (fresh context per attempt) — 3/3.
- **Evidence:** `runs/repro-m4/attempt-{1,2,3}.webm` (screen recordings),
  `attempt-1.png` (both states together), `repro-log.json` (toast at ~60ms +
  reverse-geocode **200**); original `runs/2026-07-08T09-22-12-815Z_us10-coordinates/s2-show-on-map.png`,
  `results.json` step **S2**.
- **Verify fix:** on a successful resolve, no error toast fires; on a genuine failure
  (uncovered/ocean point), the error toast fires and the inline detail is not shown.
  Give the toast `role="alert"` / `aria-live="assertive"`.

---

## M7 — No value or geographic validation on coordinate upload  ★ US 10

- **What:** The Upload Coordinates flow accepts and processes clearly-invalid data
  with no value-level validation. Verified **accepted** (resolved to blocks, plotted,
  addable to analysis) — not rejected, not flagged:
  - **Out-of-range:** lat `999`, long `-500`, `1e9`.
  - **Non-numeric:** `abc`, `#$%` in a coordinate cell.
  - **Empty:** blank lat/long cells.
  - **Out-of-India:** London, mid-Pacific, Null Island `(0,0)`.
  - **Unbounded name:** 5000-character `custom_name`.
  - **Malformed CSV:** unclosed quote / ragged columns partially ingested as garbage rows.
- **Format-independent:** identical on **CSV** and **XLSX**; the **shapefile (.zip)**
  path also accepts out-of-India geometry — fixture `z07` moves the sample's 3 points
  to London/Pacific/Null Island and they upload as "Point 1..3" and plot.
- **What *is* rejected (client-side, correctly):** wrong structure (missing `.shp`,
  `.dbf`-only), unsupported type (`.txt`, binary-as-`.csv`), empty/header-only files,
  and >1 MB (fixture `f01`). So structural gating exists; **value/bounds gating does not.**
- **Root-cause signal:** every upload returns **HTTP 200** → validation is entirely
  client-side and simply omits value/geographic checks.
- **Impact:** garbage or foreign coordinates silently resolve to arbitrary blocks;
  users can build, save, and compare analyses on meaningless points with no warning.
- **Reproduce:** Coordinate Panel → Upload Coordinates → Upload each fixture in
  `wrongly_accepted_upload_fixtures.zip` (v01–v08 CSV, x01/x02/x03/x05 XLSX, z07 shapefile).
- **Evidence:** `runs/…_us10-adversarial-upload/adversarial-results.json` (CSV matrix),
  `runs/…_us10-adversarial-formats/adversarial-results.json` (XLSX + shapefile incl. z07).
- **Verify fix:** out-of-range / non-numeric / empty / out-of-India rows are rejected
  or flagged with **visible text** before resolve/plot.

---

## N23 — Formula/CSV injection stored verbatim (CWE-1236), currently latent  ★ US 10 security

- **What:** `custom_name` values beginning with `=`, `+`, `-`, `@` (classic
  spreadsheet formula-injection triggers) are accepted and stored **without
  sanitisation** on CSV and XLSX. Fixture `v05`: `=cmd|' /C calc'!A1`,
  `@SUM(1+9)*cmd|'/C calc'!A0`, `+1+1`, `-2+3`.
- **Where the payload persists (verbatim):**
  - *My Uploaded Coordinates* list + map/portfolio labels.
  - *Manage Portfolio* rows (e.g. `=cmd|' /C calc'!A1, Nampally, Hyderabad`).
  - **Saved-analysis names** — the Save Analysis modal **auto-defaults the name to
    the first site's injected value** (`=cmd|' /C calc'!A1 - Nampally - Hyderabad -
    Multi Coordinate`), persisted server-side and displayed in the Saved Analysis list.
- **Severity = LATENT (no live download sink found).** Two investigations closed the
  two plausible "detonators" that would write the payload into a downloaded file:
  - **A — Save Analysis / My Analysis:** the per-analysis **Actions (⋮) menu offers
    Rename / Delete only** — no export / share / download of a saved analysis.
  - **B — Add to Analysis → Compare Portfolio → Download Reports:** the produced
    `portfolio-comparison-table-*.xlsx` stores **resolved district names only**
    (sheet cells `<v>Ibrahimpatnam</v>`, `<v>Nampally</v>`, `<v>Serilingampally</v>`) —
    the user `custom_name` is **discarded**, so the payload never enters the workbook.
  So no current path executes the formula in a downloaded spreadsheet.
- **Why still fix it:** it is a genuine *stored* injection — the unsanitised value is
  persisted and rendered verbatim in several places, and any future export that
  includes site or analysis names (or an admin / all-users export) would make it live.
  Standard mitigation: prefix a leading `= + - @` (and tab/CR) with `'` on **any** cell
  export, and/or reject/flag formula-leading names on input.
- **Reproduce:** upload `v05_formula_injection.csv`; observe verbatim names in the
  coordinate list, portfolio, and the Save Analysis default name. Then
  `node qa/harness/portfolio-detonator.mjs` (report xlsx → district names only) and
  `node qa/harness/saved-analysis-probe.mjs` (Actions = Rename/Delete; name default = payload).
- **Evidence:** `runs/portfolio-detonator/` (report xlsx + payload scan "none in
  archive"; sheet cells = district names), `runs/saved-analysis-probe/`
  (`A1-save-dialog.png` default name = payload; `A4-actions-menu.png` = Rename/Delete only).
- **Reassuring (no XSS):** a `<script>` name is HTML-escaped in the DOM; no JS dialog fires (v07).

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
- **N24 (Cosmetic, data, US 10):** On **shapefile (.zip)** upload the `.dbf`
  `custom_name` attribute is **silently ignored** — every feature is auto-labelled
  *"Point N"*, so user-supplied site names are lost on this path (fixtures `z02`, `z05`).
  (Side effect: `.dbf` formula-injection is inert here — the name never surfaces.)
  Consistent with the CSV/XLSX "district omitted" list gap (**N7**). Evidence:
  `runs/…_us10-adversarial-formats/adversarial-results.json` (z02/z05 → "Point N").
- **N25 (Minor, data, US 10/17):** An uploaded site that resolves **outside the pilot
  state** is **silently dropped** from the Compare Portfolio report. `v05` row 3
  (`+1+1`, resolving to *Vijayawada Urban, Ntr, Andhra Pradesh*) appears in *My Uploaded
  Coordinates* and *Manage Portfolio* but is **absent from the comparison workbook**
  (only 3 of the 4 sites exported), with no warning to the user. *ASK-PO — should
  out-of-state sites be rejected on upload, or flagged, rather than dropped downstream?*
  Evidence: `runs/portfolio-detonator/` report xlsx (sheet2 lists Ibrahimpatnam /
  Nampally / Serilingampally only; Vijayawada absent).

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
  **→ Answered by US 17:** the left/right split **is** present in the multi-site
  modal (Saved Analysis + Manage Portfolio left, Compare Portfolio right).
- **N13 (US 17):** Should the multi-site comparison Table include **Position in
  State** (spec 980)? Currently absent (has State/District/Scenario/Period + values).
- **N15 (US 17):** Is a **"Refine your filters"** section meant to live inside the
  My Analysis panel/modal left column (spec 897/1035), or does the top *Select
  Resilience Filters* panel satisfy it?
- **N16/N17 (US 17):** Is the **auto-metrics note** ("N metrics from 1 domain") and
  the **Advanced-Metric "manually refine" checkbox** (spec 962–972) in scope? Today
  metrics are chosen via a *Select Metrics* multi-select with no auto-note.
- **N18 (US 17):** Is scenario meant to be a **Single/Compare mode** toggle (spec
  951–961) or are the current SSP2-4.5 / SSP5-8.5 **checkboxes** acceptable?
- **N19 (US 07):** Should the User Profile include a **State** field (spec 280/289)?
  It is absent today (only Country = India, locked).
- **N20 (US 07):** Profile save button reads **"Update"** vs spec's **"Save"** —
  which label is canonical?
- **N21 (US 08):** Is the **timed mid-session feedback auto-popup** intended? Spec 08
  defines only a header-manual trigger and a logout auto-trigger.
- **N22 (US 01):** Are the **Donate button** and **Resustainability logo** (spec 46–49)
  meant to appear only on the **pre-auth landing**, or also in the logged-in header?

---

## Coverage — all 17 user stories accounted for

**Functionally covered (12) - historical pre-2026-07-27 summary; see rerun update above for current verdicts:**
- US 09 (Geography), US 10 (Coordinates), US 11 (Filters), US 12 (View Mode —
  passing), US 13 (Map), US 14 (Ranking — **B1 blocked**), US 15 (My Analysis
  Save/reload — passing), US 16 (Resilience Profile / single-site — passing;
  **M5**), US 17 (My Analysis Profile / multi-site — passing; **M6**),
  US 06 (Header/Dropdown — passing), US 07 (Profile — passing; **N19/N20**),
  US 08 (Feedback header flow — passing; **N21**).
- **US 15 mobile caveat — CLOSED** by US 17 S12 (375px, no overflow).

**Blocked / partial (5) — see `blocked-and-partial-stories.md`:**
- US 01 (landing) — **pre-auth surface unverifiable while logged in** (**N22**).
- US 02 / US 03 / US 04 (Sign In+2FA / Sign Up / Reset Password) — **need a
  test-email inbox** (+ a disposable account for US 03). Out of autonomous scope.
- US 05 (First-Time Visitor Guide) — **already onboarded**; the guide won't
  re-trigger (spec-consistent). Needs a fresh user to verify the walkthrough.

**To close the remaining 5:** provision a disposable email inbox + a fresh test
account, and one logged-out capture for the pre-auth landing (US 01).
