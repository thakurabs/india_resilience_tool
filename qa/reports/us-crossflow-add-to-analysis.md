# US-CROSSFLOW — "Add to Analysis" cross-flow reliability + duplicate detection

**Target:** live `dev.resilience.org.in` (saved post-2FA session)
**Driver:** `qa/harness/add-to-analysis-crossflow.mjs` (us17 stack)
**Latest run:** `qa/runs/2026-07-09T12-04-42-228Z_us-crossflow-add-to-analysis/` — **0 steps failed**, Phase 0 gate PASS
**Charter:** `qa/charters/us-crossflow-add-to-analysis/spec.md`

> Empirical-only: every selector/label/outcome was established against the live app (Phase 0 recon)
> or prior live QA. Repo source was not used as a benchmark.

---

## Headline

| # | Finding | Severity |
|---|---------|----------|
| 1 | **Same coordinate uploaded with a *different name* creates two portfolio entries** — coordinate dedup is defeated by a differing name (C4). Directly violates the confirmed contract. | **Major** |
| 2 | **A portfolio cannot be built by mixing flows** — switching Administrative↔Coordinate mode discards prior adds; a new upload replaces prior coordinate points (B4). | **Major / ASK-PO** |
| 3 | Admin same-district duplicate is correctly **rejected + flagged** "already in your portfolio." (C1). | Pass |
| 4 | Upload of two *identical* rows (same coord + same name) is **silently deduped** to one — works, but no feedback (C3). | Minor |
| 5 | Count banner labels **all** portfolio items as "district(s)", including uploaded coordinate sites (A2/A3). | Minor (spec-drift) |
| 6 | Map-click floating box does not render under headless WebGL → A4 / C2 / C6 map-click path **unverified**. | Test-env limitation |
| 7 | **US 09 Q1 resolved — intended.** Add to Analysis is correctly disabled until resilience filters are selected. | Closed |

---

## Phase 0 — recon (gated the run; all passed)

- **P0.1 Clear proven** — one admin add → count 1 → **Clear → count 0** (banner gone). Clear is a real reset. (The Clear *confirmation* dialog is a `data-modal-root` backdrop; the harness confirms it explicitly.)
- **P0.2 Duplicate flag** — an add-twice probe grew the count with **no** flag on that path, but the real flag string was later captured in C1: **`already in your portfolio.`**
- **P0.3 Map-click** — canvas click never raised the true floating box (only the panel's Add control was found; no Baseline/Position/Value). Headless WebGL limitation — see finding 6.
- **P0.4 Compare endpoint** — `portfolio-comparison-table` fires **200**.

## Phase A — each flow adds one site (isolation)

| ID | Flow | Result |
|----|------|--------|
| A1 | Administrative district | **count = 1** ✓ |
| A2 | Coordinate — manual add | **count = 1** ✓ (staging requires **"Add Coordinate"**, not just "Show on Map") |
| A3 | Coordinate — upload (`app_sample.csv`, 3 rows) | **count = 3** ✓ |
| A4 | Map click | count = 1, but **floating box not raised** (headless WebGL) — the panel Add fired, so map-click itself is **unverified** |

## Phase B — build one portfolio across flows

**B4 — FAILS to accumulate.** Running-count trail after each add:

```
after-admin = 0   after-manual = 1   after-upload = 3   after-mapclick = 1     (final 1, expected ≈ 6)
```

The portfolio never grows past the most-recent add-context: switching Administrative↔Coordinate
discards prior adds, and a new upload replaces prior coordinate points (manual 1 → upload 3, not 4).
Each switch shows a **"Switch to Coordinates? … your current geography selection will be cleared"**
confirmation. **If mixing flows into one portfolio is intended, this is a Major defect. If single-mode-
at-a-time is the intended design, the wording ("selection") undersells that adds are lost → ASK-PO.**

## Phase C — duplicate detection

Grid: (a) second row created; (b) no second row + "already added" flag; (c) no second row, no message.

### Valid cases (no mode switch between seed and dup)

| ID | Case | Observed | Verdict |
|----|------|----------|---------|
| **C1** | Same district twice, Administrative | before **2 → after 2**; flag **"already in your portfolio."** | **Pass** — rejected + flagged (grid b) |
| **C3** | Same coordinate, **same name**, one upload | **1 row** (two identical rows collapsed) | **Minor** — deduped silently, no feedback (grid c) |
| **C4** | Same coordinate, **different name**, one upload | **2 rows** | **Major** — different name defeats coordinate dedup; violates the contract (grid a) |

> C1's baseline reads 2 (the district multi-selector `Select District(s)` retained Karimnagar from B4,
> so the seed added Karimnagar + Warangal); the duplicate step then re-added Warangal → **flagged, no
> increment**. The dedup conclusion holds regardless of the inflated baseline.

### Confounded cases (a mode switch sits between seed and dup) — **INCONCLUSIVE**

C2 (admin + map-click), C5 (admin + manual coordinate), C6 (upload + map-click) all read **1 → 1**, but
because of finding 2 the seed is **cleared by the mode switch** before the duplicate is added — so
"1 → 1" is "seed lost, dup added", **not** dedup. These cannot be judged until cross-mode persistence
is fixed/clarified. (C2/C6 are additionally blocked by the headless map-click limitation.)

---

## What this answers for the user

- **"Do the flows dedupe?"** — *Within a single flow*: Administrative **yes, with feedback** (C1);
  upload dedupes only when the **name also matches** (C3 vs C4). **Different name on the same
  coordinate is NOT deduped (C4) — the Major finding.**
- **"Can I mix flows into one portfolio?"** — **Not currently** (B4): mode switches clear prior adds.
  Cross-flow duplicate semantics (point-in-district) can't even be evaluated until this is resolved.
- **US 09 Q1** — closed as **working-as-intended**: Add stays disabled until filters are chosen
  (the harness had to cascade filters before Add enabled in every flow).

## Recommended next steps

1. **Confirm intent of B4** with the PO (single-mode vs cross-flow portfolio). This gates re-testing C2/C5/C6.
2. File the **C4** coordinate-dedup gap as Major (repro: upload `fixtures/dup_diff_name.csv`).
3. Re-run map-click (A4/C2/C6) in a **headed / GPU** browser to lift the test-env limitation.
4. Do **not** forward to the vendor until findings 1 & 2 are reviewed here.

## Reproduce

```bash
node qa/harness/capture-session.mjs           # refresh 2FA session (~24h)
node qa/harness/add-to-analysis-crossflow.mjs  # Phase 0 gates; evidence → qa/runs/us-crossflow-*/
```
