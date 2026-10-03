# QA Report — US 09: Geography Selection

**App:** dev.resilience.org.in · **Charter:** `qa/charters/us09-geography-selection/`
**Run:** `qa/runs/2026-07-08T06-28-20-354Z_us09-geography/`
**Pipeline:** harness (Playwright) → reviewer (Haiku) → triage (Opus, verified)
**Result:** core functionality **PASS** · 0 crashes · 0 real error events (15 benign, filtered)

## Verdict

The Administrative Panel (app's name for "Geography Selection") works correctly
across the flow: State selection loads Districts (34 for Telangana), District/Block
view-zone switching, single- and multi-site selection, Reset, sidebar collapse/expand
with **selection retained**, and Map↔Ranking view switch with geography preserved.
All 9 driven steps passed. No functional blockers found.

Findings below are accessibility issues and one product question — none block use.

## Findings

| # | Severity | Area | Status | Summary | Evidence |
|---|----------|------|--------|---------|----------|
| F1 | Major | a11y | CONFIRMED | Sidebar collapse/expand toggle has **no accessible name** — when collapsed, the only re-open control is an unlabeled `<svg>` (no `aria-label`/`title`/text). Keyboard/screen-reader users cannot identify or reliably operate it. | manual recon; `s12a-collapsed.png` |
| F2 | Major | a11y | CONFIRMED | axe **critical `image-alt`**: 1 icon/image missing alt text (`.hover:text-[#1a9ab8] > .w-6.h-6`). | `us09__axe.json` |
| F3 | Minor | a11y | CONFIRMED | axe **serious `color-contrast`**: 4 elements below WCAG contrast (incl. `.dashboard-sidebar__reset-btn`, 14px text). | `us09__axe.json` |
| F4 | Minor | a11y | CONFIRMED | axe **moderate**: no `<main>` landmark (`landmark-one-main`) and 9 regions not contained in landmarks (`region`). | `us09__axe.json` |
| Q1 | — | functional | OPEN QUESTION | "Add to Analysis" stays **disabled with one district selected** and no resilience filter. Spec says it greys out when "no location selected **&** no filters" — ambiguous. Need product confirmation: is a resilience filter *required* to enable it (intended), or should a location alone enable it (defect)? | `s9-add-state.png`; `results.json:steps[3]` |

## Not bugs (checked and dismissed)

- 15 "benign" events per run = MapLibre WebGL perf warnings + `net::ERR_ABORTED`
  on `indian-state.pmtiles` range requests. The state polygons render correctly;
  these are normal streaming/renderer noise (the harness auto-classifies them).
- "Administrative Panel" vs spec's "Geography Selection" = intended rename, not a defect.

## Next verification (to close Q1)

Recon the "Select Resilience Filters" panel (US 11), apply a filter with one
district selected, and re-check whether "Add to Analysis" enables — that resolves
whether Q1 is intended behaviour or a defect.
