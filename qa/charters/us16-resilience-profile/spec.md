# US 16: Resilience Profile (Single-Site Analysis)

Source: `_source_user_stories_v1.3.txt` lines 839–904
Scope: functional · data · visual · a11y

## Preconditions
- Logged-in session active (reused via saved storageState).
- Visitor is on the dashboard; the left Selection Panel is visible.
- A single site is selectable (State + District/Block/Coordinate) with the core
  Resilience filters (Risk Domain, Metric, Scenario, Period, Statistic).

## Discovered structure (recon `2026-07-08T10-2x..10-4x`, rounds 1–9)
- **Panel:** top-**right** **"Resilience Profile"** panel. Header has two controls:
  - a chevron toggle — **"Expand resilience profile panel"** / **"Collapse
    resilience profile panel"** — that opens/closes the panel *body*;
  - **"Open Resilience Profile in full screen"** (⛶) — opens a full-screen modal
    (`role="dialog"`, ~1354×792) with close button
    **"Close expanded Resilience Profile view"**.
- **On load (no location):** panel body reads **"No location(s) selected"**.
  (Spec 848 wording is "select a location or coordinate to view Insights" →
  wording SPEC-DRIFT, not FAIL.)
- **Profile Overview** (once State+District+all filters set): heading
  **"<District> — District Climate Profile"** + lines **Index:** `<metric>`,
  **Scenario:** `<ssp>`, **Period:** `<period>`. If Statistic is left unset the
  Overview shows a placeholder ("Select a state and all filters to view profile.")
  even though the charts below can already render — set all six filters for the
  full Overview.
- **Three collapsible accordions** in the body:
  1. **Risk Summary** — header is `div[role="button"]` (matches
     `getByRole('button', {name:/Risk Summary/})`). Cards:
     **HISTORICAL BASELINE**, **PROJECTED VALUE**, a Δ badge (e.g. green
     **↑ +0.42**), **POSITION IN STATE**.
     - *Climate metric* (e.g. Annual Mean Temperature): all four populate —
       `HISTORICAL BASELINE 28.27 °C / PROJECTED VALUE 28.69 °C / +0.42 / POSITION IN STATE 7`.
     - *Composite metric* (Heat Risk Composite (score)): **omits Historical
       Baseline**, Δ shows **+0.00**, shows `PROJECTED VALUE 52.17 score /
       POSITION IN STATE 18`.
  2. **Trend Over Time (State Average)** — header is a `div.cursor-pointer`
     wrapping an `<h4>` (NOT role=button → click the h4 / the cursor-pointer
     ancestor, not `getByRole('button')`). Renders an SVG **line chart**:
     Historical (blue) + SSP245 (orange) series, dashed historical/projection
     divider ~2020, X-axis **Year** (1960–2100), Y-axis the metric+unit. Control
     **"Show model members"** checkbox → on check, a **"Max models to draw"**
     slider (default 5) appears (spec 871 ✓).
  3. **Scenario Comparison (Period-Mean)** — same header pattern. Renders an SVG
     **grouped bar chart**: **Historical (blue) / SSP2-4.5 (orange) / SSP5-8.5
     (red)** — matches spec colours — grouped by period (1990–2010, 2020–2040,
     …). A **"Start y-axis at zero"** control/note is present ("Note: y-axis is
     auto-zoomed … Enable 'Start y-axis at zero' …").
- **Data availability:** for a **composite** metric the two time-series charts
  show graceful **"No data available"** / **"No data available for the selected
  filters."** (composites have no temporal series). This satisfies spec 862–863's
  "Insufficient data / N/A" requirement but confirm it is intended for composites.
- **Metric enumeration** (Heat Risk domain): Heat Risk Composite (score), Annual
  Mean Temperature (TM Mean), Annual Maximum Temperature (TXx), Hot Days, Extreme
  Heat Days, Tropical Nights, … — the composite is first; the rest are projected
  climate variables that carry full trend/scenario series.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| 1 | Fresh dashboard, expand the profile panel with **no location** | Panel body reads a "no location / select a location" message (app: "No location(s) selected") — no crash, no NaN |
| 2 | Build analysis: State=Telangana, District=Warangal, **Annual Mean Temperature** + Scenario + Period + Statistic | Map renders; profile panel becomes populatable |
| 3 | Expand the profile panel; read the **Overview** | Heading "Warangal — District Climate Profile"; Index = Annual Mean Temperature; Scenario = ssp245; Period = 2020-2040 |
| 4 | Open **Risk Summary** | Cards show **Historical Baseline** (°C), **Projected Value** (°C), **Δ vs baseline** with up/down indicator, **Position in …** (app: "Position in State"). No NaN/blank |
| 5 | Open **Trend Over Time (State Average)** | SVG line chart with **Historical + SSP** series, Year X-axis, metric Y-axis; **"Show model members"** checkbox present |
| 6 | Toggle **Show model members** on | A **"Max models to draw"** slider appears (spec 871). Record whether a **percentile band (p05–p95)** control also appears (spec 872) |
| 7 | Open **Scenario Comparison (Period-Mean)** | SVG grouped bar chart: **Historical (blue) / SSP2-4.5 (orange) / SSP5-8.5 (red)**, grouped by period; **"Start y-axis at zero"** control/note present |
| 8 | Click **Open Resilience Profile in full screen** (⛶) | A full-screen modal (`role=dialog`) opens showing the same profile; a close control ("Close expanded Resilience Profile view") is present |
| 9 | In the modal, look for the **left/right split** | Spec 897–899: Left = Saved Analysis / Manage Portfolio / Refine your filters; Right = Compare Portfolio. Record presence (likely gated on a non-empty portfolio = US 17) |
| 10 | Close the modal | Returns to the dashboard; profile panel intact |
| 11 | Switch Metric to **Heat Risk Composite (score)**, re-open Trend + Scenario Comparison | Both charts show graceful **"No data available"**; Risk Summary omits Historical Baseline and Δ shows +0.00. Confirm intended for composites |

## Cross-cutting checks
- No `console.error` / `pageerror` / `requestfailed` / `httperror` (≥400) beyond
  known-benign MapLibre/WebGL/pmtiles noise (harness-classified).
- No `NaN` / blank where a value, axis label, or series label is expected.
- Charts update dynamically when filters change (spec post-condition 904).
- Layout intact desktop / tablet / mobile; capture the profile panel at 375px.
- No serious/critical axe violations on the profile panel or the full-screen modal.

## Known caveats (flag as SPEC-DRIFT / PASS-WITH-NOTE, not FAIL)
- **"Position in State"** vs spec 861 **"Position in India"** — wording/scope
  drift, consistent with the US 13 map-tooltip note. Verify with PO.
- **Percentile band (p05–p95)** control (spec 872) not observed — only
  "Show model members" + "Max models to draw" slider. Flag as a finding.
- **Full-screen modal** shows the single-site profile only; the left/right
  Saved-Analysis / Portfolio / Compare-Portfolio split (spec 897–899) was not
  present for a single site with an empty portfolio — likely a US 17 (multi-site)
  surface. PASS-WITH-NOTE.
- **Composite metric** → no baseline, Δ +0.00, "No data available" charts. Meets
  the "Insufficient data / N/A" clause but confirm the +0.00 Δ is not misleading.
- Empty-state wording differs from spec ("No location(s) selected" vs "select a
  location or coordinate to view Insights").
