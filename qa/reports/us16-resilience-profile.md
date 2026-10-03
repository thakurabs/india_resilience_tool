# QA Report — US 16: Resilience Profile (Single-Site Analysis)  ✅ PASSING (1 backend defect on composites)

**Run:** `qa/runs/2026-07-08T10-49-09-733Z_us16-resilience-profile/`
**Result:** **PASS** — the single-site Resilience Profile works end-to-end for a
climate metric (10 steps, 0 failures; **0 real error events on the climate-metric
path**). One new **Major backend** defect (M5) isolated to **composite** metrics,
plus three Minor drifts (N10/N11/N12) and informational notes. Corroborates
existing a11y findings M2/M3/N1/N2.

## What works (verified — climate metric: Annual Mean Temperature)
- **Empty state:** with no location the panel reads **"No location(s) selected"**
  (spec 848 wording drift only). `s1-empty.png`
- **Profile Overview:** "Warangal — District Climate Profile" + **Index / Scenario /
  Period** all populated. `s3-overview.png`
- **Risk Summary:** **Historical Baseline 28.27 °C** · **Projected Value 28.69 °C** ·
  **Δ ↑ +0.42** (up/down indicator) · **Position in State 7**. No NaN/blank. `s4-risk-summary.png`
- **Trend Over Time (State Average):** SVG line chart — **Historical + SSP245**
  series, ~2020 historical/projection divider, **Year** X-axis, metric Y-axis;
  **"Show model members"** → **"Max models to draw" slider** (default 5) appears
  (spec 871 ✓). `s5-trend.png`, `s6-model-members.png`
- **Scenario Comparison (Period-Mean):** SVG grouped bars —
  **Historical (blue) / SSP2-4.5 (orange) / SSP5-8.5 (red)** (spec colours ✓),
  grouped by period; **"Start y-axis at zero"** control/note present. `s7-scenario-comparison.png`
- **Panel interaction:** expand/collapse + **full-screen modal** (`role=dialog`,
  ~1354×792) with "Close expanded Resilience Profile view"; same profile in the modal
  (spec 896). `s8-fullscreen.png`
- **Responsive:** full profile panel renders cleanly at **375px**. `us16-responsive__mobile.png`
- **Data hygiene:** 0 NaN/blank in the DOM scan.

## Findings
- **M5 (Major, functional/backend, SEND):** with a **composite** metric (*Heat Risk
  Composite (score)*), opening Trend + Scenario Comparison fires
  `POST /api/api/parquet/trend` and `POST /api/api/parquet/scenario-comparison`,
  **both HTTP 500**. UI degrades gracefully to "No data available" (no crash). This
  is the **same 500 + doubled `/api/api/` family as the B1 ranking blocker** and is
  **specific to composite metrics** — the same two endpoints return **200** for
  climate variables (0 errors S1–S9). Either the frontend should not request
  trend/scenario for composites, or the backend should return empty/404 not 500.
  Evidence: `results.json` (2× `httperror` 500), `s10-composite-nodata.png`.
- **N10 (Minor, a11y, HOLD):** axe **`nested-interactive`** (*serious*, new vs prior
  charters) — interactive controls nested inside a clickable accordion header. `us16__axe.json`
- **N11 (Minor, functional, ASK-PO):** the Trend chart has "Show model members" +
  "Max models to draw" slider but **no "Show percentile band (p05–p95)"** control (spec 872).
- **N12 (Minor, data, ASK-PO):** Risk Summary shows **"Position in State"**; spec 861
  says **"Position in India"** (same drift family as the US 13 tooltip "Rank in state").

## Informational (not defects)
- **Composite Risk Summary** omits the Historical Baseline card and shows **Δ +0.00**
  (composites have no historical value) — confirm +0.00 is not read as "no change".
- **Full-screen modal (single-site)** shows the profile only; the spec 897–899
  **left/right split** (Saved Analysis / Manage Portfolio / Refine filters | Compare
  Portfolio) is **absent for a single site with an empty portfolio** — appears to be
  the **US 17 (multi-site)** surface; will verify there.
- **Recharts warning** *"width(-1)/height(-1) of chart …"* ×2 — a chart mounts into a
  zero-size container (collapsed accordion / modal transition). Cosmetic.
- **Empty-state wording** "No location(s) selected" vs spec 848 phrasing.

## Method notes
- Charter: `qa/charters/us16-resilience-profile/{spec.md,scenario.mjs}`.
- Single-site analysis built with **all six** filters (incl. Statistic) so both the
  Risk Summary and the trend/scenario charts populate; a **climate metric** is used
  for the pass because composite scores have no time series (→ M5).
- Accordion selectors: **Risk Summary** header is `div[role=button]`; **Trend** and
  **Scenario Comparison** headers are `div.cursor-pointer` wrapping an `<h4>` (click
  the h4's nearest `cursor:pointer` ancestor — not `getByRole('button')`).
