# QA Report — US 13: Map View & Interaction

**Run:** `qa/runs/2026-07-08T06-55-54-673Z_us13-map/`
**Result:** **PASS** with one data spec-drift.

## Verdict
Map analysis works well. With State + District + filters:
- Selected region (Adilabad) boundary highlighted and **filled by value**.
- **Legend** renders: numeric color scale (23.96–77.13) + metric/scenario/period/
  statistic caption.
- **Click tooltip** shows District (Adilabad), State (Telangana), Composite Score
  (54.06), Rank in state (16), and an "Add to Analysis" CTA.
- Zoom in / out / reset controls present and responsive.
- No real error events.

## Finding — tooltip omits spec fields (Minor, data)
The spec lists the tooltip fields as District, State, Block, **Value, Baseline
(1990–2010), Δ vs baseline (Level of Change)**, Position in state, Add to Analysis.
The app tooltip shows District, State, Composite Score (≈ Value), Rank in state
(≈ Position), Add to Analysis — but **omits Baseline (1990–2010) and Δ vs baseline
/ Level of Change**. Confirm whether these were intentionally dropped or are
missing data. Evidence: `s3-tooltip.png`.

## Caveat
Map is WebGL/canvas; hover/click tooltips captured as screenshots. `pmtiles` and
carto basemap `ERR_ABORTED` are benign streaming noise (harness-classified).
