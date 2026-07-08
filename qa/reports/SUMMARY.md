# QA Summary — India Resilience Tool (vendor UAT)

**App:** dev.resilience.org.in · **Basis:** Resilience Actions User Stories v1.3
**Method:** Playwright harness → Haiku evidence review → Opus triage (all findings
verified against raw evidence). Session-authenticated (saved 2FA session).
**Charters run:** US 09 (Geography), US 11 (Filters), US 13 (Map), US 14 (Ranking).

## Headline

The **Map** analysis path works end-to-end (geography → filters → colored map +
legend + click tooltip with data). The **Ranking Table** path is **broken by a
server 500**. Remaining issues are a recurring set of accessibility defects.

## Findings (severity-ranked)

| # | Sev | Area | Story | Finding | Evidence |
|---|-----|------|-------|---------|----------|
| B1 | **Blocker** | functional | US 12/14 | Ranking Table cannot load: `GET /api/api/parquet/ranking` → **HTTP 500**; UI shows "We couldn't load the ranking data". Reproduced with state-only and single-district. Map works with identical filters. Note the **doubled `/api/api/`** path segment. | `us14-ranking` run: `results.json` (rankingResponses 500), `s1-ranking.png` |
| M1 | Major | a11y | US 09 | Sidebar collapse/expand toggle has **no accessible name** (unlabeled `<svg>`) — AT users cannot reopen the panel. | `us09` `s12a-collapsed.png`; recon |
| M2 | Major | a11y | US 09/11 (systemic) | axe **critical `image-alt`**: icon/image missing alt text — recurs on multiple screens. | `us09__axe.json`, `us11__axe.json` |
| M3 | Major | a11y | US 09/11 (systemic) | axe **serious `color-contrast`**: multiple elements below WCAG (Reset button, placeholder text, **red error-banner text `#E75252`**). | `us09__axe.json`, `us11__axe.json` |
| N1 | Minor | a11y | US 11 | axe `link-in-text-block`: MapLibre attribution link not distinguishable without color (third-party control). | `us11__axe.json` |
| N2 | Minor | a11y | US 09 | No `<main>` landmark; 9 blocks not in landmarks (`landmark-one-main`, `region`). | `us09__axe.json` |
| N3 | Minor | data | US 13 | Map click-tooltip **omits spec fields**: "Baseline (1990–2010)" and "Δ vs baseline / Level of Change" are not shown (shows District, State, Composite Score, Rank in state). | `us13` `s3-tooltip.png` |
| N4 | Cosmetic | data | US 14 | Internal slugs leak in ranking caption ("composite_heat_risk • ssp245"). | `us14` `s1-ranking.png` |

## Resolved question

- **US 09 Q1** — "Add to Analysis" stays disabled until a **resilience filter** is
  applied (with a location). Confirmed intended, not a defect (US 11 S7).

## Observations (not defects)

- Filter application + data load is **asynchronous (several seconds)** with no
  obvious loading indicator — confirm a spinner exists for slow connections.

## Spec-drifts (intended refinements; informational, not bugs)

- "Geography Selection" panel is named **"Administrative Panel"**.
- **10 Risk Domains** offered vs spec's 8 (adds "Drought Risk (Advanced)", "Population Exposure").
- Scenario + Period are manual; **Statistic ("Mean") + Map Mode auto-default**.
- Map legend is a **continuous numeric scale**, not categorical "Very Low..Extreme".
- Map Mode shows "Absolute value" (greyed) vs spec's Predicted/Historical/Change modes.

## What passed cleanly

- US 09 Geography: state→district load, single/multi-site, Block switch, Reset,
  sidebar collapse/expand **with selection retained**, Map↔Ranking preserves geography.
- US 11 Filters: cascade gating correct, all filters apply, map updates.
- US 13 Map: region highlight + value fill + legend + click tooltip + zoom controls.

## Coverage & next

- Done: US 09, 11, 13, 14. Q1 resolved.
- Not yet covered: US 01 (landing), 05–08 (nav/profile/feedback), 10 (coordinates),
  15–17 (save/reload, profiles). Auth flows US 02–04 need an email inbox (out of
  autonomous scope).
- Re-run `us14` after the ranking-500 fix to unblock full table verification
  (columns, ranking order, row selection, color coding).
