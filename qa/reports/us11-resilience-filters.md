# QA Report — US 11: Resilience Filters

**Run:** `qa/runs/2026-07-08T06-49-11-689Z_us11-filters/`
**Result:** **PASS** (functional) · resolves US 09 Q1 · a11y issues (see SUMMARY)

## Verdict
All 6 filter groups present with cascade gating working correctly: downstream
dropdowns stay disabled until their parent is chosen (Metric needs Risk Domain;
Scenario/Statistic need Metric; Period needs Scenario). Selecting Risk Domain →
Metric → Scenario → Period applies filters and the map updates with data.

## Resolved
- **US 09 Q1**: with a district selected, "Add to Analysis" is **disabled until a
  resilience filter is applied**, then **enables**. Intended behaviour, not a defect.

## Spec-drifts (not bugs)
- 10 Risk Domains vs spec's 8; Statistic ("Mean") + Map Mode auto-default while
  Scenario + Period are manual; Map legend is numeric (not categorical);
  Map Mode shows "Absolute value" (greyed).

## Observation
- Filter selection + map data load is asynchronous (a few seconds) with no clear
  loading indicator. Non-blocking, but confirm a spinner for slow links.

## a11y (verified via axe)
- critical `image-alt` (1), serious `color-contrast` (multiple incl. the red error
  text), `link-in-text-block` (MapLibre attribution). Consolidated in SUMMARY (M2/M3/N1).
