# US 17 — My Analysis Profile (Multi-Site Analysis)

**Verdict: PASSING feature.** The multi-site portfolio works end-to-end — build a
≥2-site portfolio via *Add to Analysis*, manage it (list / remove ⊗ / Clear),
compare it (risk domain → metrics → scenario → period) into a **Table** and a
**Heatmap**, and open it in a full-screen modal with the spec's left/right split.
Backed by `portfolio-comparison-table` and `portfolio-heatmap` endpoints, both
**HTTP 200** — no 500s in this flow. Findings are one count bug + a cluster of
spec-label drifts + one new a11y item; none block the feature.

Authoritative run: `qa/runs/2026-07-08T15-32-52-396Z_us17-analysis-profile/`
(12 steps, 0 failed, 0 real error events).

## Verified matching spec (no defect)
- **Add to Analysis** builds a portfolio; both districts shade on the map and enter
  **Manage Portfolio** (Warangal, Karimnagar). (S2–S3)
- **Manage Portfolio**: each row lists the District name with a *Show on map* button
  and a **Remove from portfolio** ⊗ that genuinely removes the site; **Clear
  Portfolio** present. (S4, S9b — `["Warangal","Karimnagar"] → ["Warangal"]`)
- **Saved Analysis**: list of saved analyses, each with a per-row 3-dot
  (Rename/Delete) menu. (S6)
- **Compare Portfolio**: *Select Risk Domain to compare* (Heat Risk) → *Select
  Metrics* (multi-select incl. "All Metrics (14)") → *Scenario* (SSP2-4.5 / SSP5-8.5)
  → *Period* (Early/Mid/End century). (S7–S8)
- **Table** loads a per-metric comparison with **District Name · State Name ·
  Scenario · Period · Index Value · Absolute Change · Change Percentile · Level of
  Change**, one row per site (both Warangal + Karimnagar). `portfolio-comparison-table`
  → 200. (S8)
- **Visualizations** render a *Portfolio Comparison* heatmap; `portfolio-heatmap` →
  200. Per-metric coverage gaps show a benign inline *"Data is not available for
  &lt;metric&gt;"* note (informational, not an error). (S9)
- **Download** controls present: *Download Reports* **and** *Download heatmap*. (S10)
- **Full-screen modal** opens with the spec left/right split: **Saved Analysis +
  Manage Portfolio (left, x≈53)** / **Compare Portfolio (right, x≈435)**. (S11)
- **Mobile 375px**: the expanded My Analysis panel renders with no horizontal
  overflow — **closes the open US 15 mobile caveat** for this panel family. (S12)

## Findings
| ID | Sev | Area | Status | Summary |
|----|-----|------|--------|---------|
| M6 | Minor→Major | data/functional | FILE | Portfolio count banner reads **"You have added 1 district"** while Manage Portfolio holds **2** (Warangal + Karimnagar). Count is wrong (and not pluralised). Spec 912–914 wants the true total. |
| N13 | Minor | data | SPEC-DRIFT | Comparison **Table omits "Position in State"** (spec 980 lists it first). Columns are District/State/Scenario/Period/Index Value/Absolute Change/Change Percentile/Level of Change. |
| N14 | Minor | a11y | FILE | **scrollable-region-focusable** (axe *serious*, new): the comparison table / heatmap scroll container is not keyboard-focusable. |
| N15 | Minor | functional | ASK-PO | No **"Refine your filters"** section inside the panel/modal (spec 897/1035 lists it in the left column). The top *Select Resilience Filters* panel is separate. |
| N16 | Minor | functional | ASK-PO | No **auto-metrics note** ("N metrics from 1 domain(s)…", spec 962–966). Metrics start empty and are picked manually. |
| N17 | Cosmetic | functional | ASK-PO | No **"Advanced Metric / Manually refine metric selection" checkbox** (spec 968–972); a *Select Metrics* multi-select is used instead. |
| N18 | Cosmetic | functional | ASK-PO | **Scenario implemented as checkboxes** (SSP2-4.5 / SSP5-8.5), not the spec's *Single Scenario / Compare Scenario* modes (spec 951–961). Functionally equivalent. |

## Corroborates existing a11y findings (not new)
The US 17 axe scan repeats **color-contrast** (serious, 2), **image-alt** (critical,
header brand), **landmark-one-main / region** (moderate), and **nested-interactive**
(serious) — already filed as M3 / M2 / N2 / N10. Strengthens, does not add.

## Side observation (US 08 evidence)
An **auto-triggered feedback survey** ("HELP US IMPROVE YOUR EXPERIENCE") popped up
mid-session and its `data-modal-root` overlay intercepted pointer events. Captured
for **US 08** (auto-popup feedback form); the scenario now dismisses it (Escape /
explicit ×, never Submit) before each step.
