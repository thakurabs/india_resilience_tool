# US 17: My Analysis Profile (Multi-Site Analysis)

Source: _source_user_stories_v1.3.txt lines 906–1048
Scope: functional | data | visual | a11y (all)

## Preconditions
- Logged in (saved 2FA session).
- Able to add ≥2 locations to a portfolio via **Add to Analysis**.
- Resilience filters + data available for the selected locations.

## Discovered surface (recon 2026-07-08)
The multi-site surface lives in the top-right **"My Analysis"** panel (toggle:
`Expand My Analysis panel`) and its full-screen modal (`Expand`/⛶ →
`Close expanded My Analysis view`). The panel/modal contains three accordions:
**Saved Analysis**, **Manage Portfolio**, **Compare Portfolio**. In the modal these
split **left** (Saved Analysis + Manage Portfolio) / **right** (Compare Portfolio).
Compare Portfolio exposes: *Select Risk Domain to compare* → *Select Metrics* →
*Scenario* (SSP2-4.5 / SSP5-8.5 checkboxes) → *Period* (Early/Mid/End century
checkboxes) → **Table / Visualizations / Download Reports** tabs.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Open dashboard, expand My Analysis with an empty portfolio | Panel shows an empty/'add locations' state (no crash) |
| S2 | Build a district analysis (Telangana → Warangal + all filters), click **Add to Analysis** | Toast "added to your portfolio"; Warangal enters Manage Portfolio |
| S3 | Select a 2nd district (Karimnagar), click **Add to Analysis** | Portfolio now lists **2** districts; count banner reads "You have added **2** districts" (spec 912–914) |
| S4 | Inspect **Manage Portfolio** | Both locations listed by District name (spec 936–938), each with a remove **⊗**; **Clear Portfolio** present |
| S5 | Remove one location via **⊗**, confirm list shrinks | Removed location disappears; count updates |
| S6 | Inspect **Saved Analysis** | Saved items list with per-row 3-dot (Rename/Delete) — spec 915–920 |
| S7 | **Compare Portfolio** → Select Risk Domain to compare | Risk-domain dropdown; on select, metrics auto-included w/ a "N metrics from 1 domain" note (spec 962–966) |
| S8 | Select Metrics + Scenario + Period, view **Table** | Comparison table with fields: Position in State, State, District, Index Value, Absolute Change, Change Percentile, Level of Change (spec 979–988). API returns data (not 500) |
| S9 | Switch to **Visualizations** | Heatmap (portfolio comparison) + scenario-comparison chart + Level-of-Change legend (spec 989–993) |
| S10 | **Download** control present | A download affordance for the heatmap/report exists (spec 994–996) — presence only, do not trigger a file |
| S11 | Open full-screen modal | Modal opens with **left/right split**: Saved Analysis + Manage Portfolio (left), Compare Portfolio (right) — spec 1032–1039 |
| S12 | Capture `/my-analysis` (or panel) at 375px | Mobile layout renders without breakage (closes the open US 15 mobile caveat) |

## Known caveats / expected drift (verify, don't auto-FAIL)
- Spec 897/1035 lists **"Refine your filters"** as a left-column section; recon did
  not see it inside the panel (the top **Select Resilience Filters** panel is
  separate). Treat as SPEC-DRIFT pending PO confirmation.
- Spec describes **Single Scenario / Compare Scenario** as distinct *modes*; the app
  implements scenario as **multi-select checkboxes** (SSP2-4.5 / SSP5-8.5). DRIFT.
- Spec 968–972 **"Advanced Metric — Manually refine metric selection" checkbox**:
  the app uses a **Select Metrics** multi-select instead. DRIFT — verify.
- Spec 994 **"Download Heatmap"**; app label observed as **"Download Reports"**. DRIFT.
- Portfolio **count** banner observed as "You have added **1** district" while
  Manage Portfolio held **2** — verify whether the count is genuinely wrong.
- SPEC-DRIFT-not-FAIL rule (per README): observed-vs-stale-spec → SPEC-DRIFT.
