# Manual Smoke Test — IRT (5 minutes)

Use this checklist after each deletion/move batch. It is intentionally quick and UI-focused.

> Prereq: you need boundary files + processed data configured via `IRT_DATA_DIR` / `IRT_PROCESSED_ROOT`.

## A) Launch

```bash
streamlit run main.py
```

Pass:
- Page loads without exceptions.
- Title renders.

## B) District (ADM2) — Map → Rankings → Details

1. Ensure admin level is **District**
2. In the map-top ribbon:
   - Select a **Risk domain**
   - Select a **Metric**
   - Select **Scenario** (SSP2-4.5 or SSP5-8.5)
   - Select **Period**
   - Select **Statistic** (Mean or Median)
   - Select **Map mode** (Absolute and Change-from-baseline)
3. Confirm the choropleth renders and hover tooltips work.
4. Click a district; confirm the right-side details panel populates.
5. Scroll the right-side panel; confirm the map/ribbon stay visible (right panel scrolls internally).
6. Collapse the right panel to the rail; confirm the left workspace expands and the expand control is visible.
7. Expand the right panel again; confirm the details panel returns.
8. Click the “Open rankings table” control (if present); confirm it switches views.
9. Switch to **Rankings table** view; confirm table renders and sorting modes work.

## C) District portfolio — add + compare + export

1. Switch analysis focus to **Multi-district portfolio**
2. Add 2+ districts using:
   - Map click + “Add to portfolio”
   - Rankings add/remove buttons
   - Add by Location (single coordinate)
3. Confirm portfolio summary badge updates and unit list shows items.
4. Optional UX check: collapse/expand the right panel; confirm portfolio mode still works after expanding.
5. In portfolio compare:
   - Select 1–2 bundles
   - Confirm comparison table builds
   - Download CSV for the displayed table
6. Trigger a case-study export (PDF and ZIP) and confirm downloaded files open.

## D) Block (ADM3) — Map + Rankings + Details + portfolio

1. Switch admin level to **Block**
2. Repeat a minimal pass:
   - Select bundle/metric/scenario/period/stat/mode
   - Confirm block choropleth renders
   - Click a block; details panel updates (and does not crash)
   - Add 1–2 blocks to portfolio and confirm compare table builds

## E) Record result

For each batch, record:
- PASS/FAIL
- First exception stack trace (if FAIL)
- Which step (A–D) failed
