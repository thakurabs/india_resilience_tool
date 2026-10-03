# Charter US-CROSSFLOW — "Add to Analysis" cross-flow reliability + duplicate detection

**Target:** live `dev.resilience.org.in` (saved post-2FA session).
**Driver:** `qa/harness/add-to-analysis-crossflow.mjs` — us17 stack (`withSession` + `dismissFeedback` + `safe`).
**Evidence:** `qa/runs/us-crossflow-add-to-analysis/`.

## Why this charter exists

Per-flow smoke tests exist (US 09 admin, US 10 coordinates, US 13 map, US 17 portfolio)
but **no test exercises the three add-flows together**, and **duplicate handling has never
been probed**. Three entry points add a site to the portfolio:

1. **Administrative Panel** — State → District (→ Block) → *Add to Analysis*.
2. **Coordinate Panel** — *Add Coordinates* (manual lat/long) or *Upload Coordinates* (CSV).
3. **Map click** — click a district on the canvas → floating box → *Add to Analysis*.

## Confirmed requirements (from the PO / user)

- **Enable precondition is intended, not a defect.** *Add to Analysis* is correctly disabled
  until resilience filters are selected. This closes **US 09 Q1** as "working as intended".
- **Duplicate-identity contract.** Adding the **same location twice — same name OR a different
  name — must be flagged**. A silent duplicate entry is a **Major** defect (not ASK-PO).

## Empirical-only (repo source is NOT a benchmark)

The in-repo product source is stale relative to the deployed target on exactly the surfaces
this charter touches (button wording, upload flow, comparison rendering). Every selector,
label, endpoint, and outcome is established by **recon against the live app (Phase 0)** or by
prior *live* QA observation — never inferred from repo source. **Dedup semantics are an open
empirical question:** we assert nothing about *how* the app dedupes (by name / coordinate /
resolved unit); we drive the PO's contract and classify what the live app does.

## Phase 0 — mandatory recon (gates everything downstream)

Runs first; results recorded to `results.json` under `run.crossflow.recon`. If any P0 step
fails, Phases A/B/C are **skipped** (their results would be void) and the run reports the P0
failure.

| ID | Proves | Method |
|----|--------|--------|
| P0.1 | **Clear is a real reset** | Admin-add Warangal → roster count 1 → `clearPortfolio()` → count 0 **and** count-banner cleared **and** compare surface reset. us17 only text-tests the *label* (`scenario.mjs:163`) — Clear is unproven, not reused. |
| P0.2 | **Duplicate flag string** | During an add-twice probe, capture the exact "already added / duplicate" toast text so Phase C's flag assertion is a hard string match, not a guess. |
| P0.3 | **Map-click selectors** | us13 canvas approach (`us13-map-view/scenario.mjs:46-56`): click `canvas` at proportional coords, capture the floating-box container + its *Add to Analysis* selector; confirm it enables only after core filters. Fallback: lat/long centering a known unit if WebGL hit-testing is flaky. |
| P0.4 | **Compare endpoint fires** | Trigger one comparison; confirm the literal `portfolio-comparison-table` request appears (the reusable collector matches a broader `/parquet|portfolio|compare|analysis/` regex) so a "→200" check is real. |

## Phase A — each flow adds one site (reset → empty first)

| ID | Flow | Expected |
|----|------|----------|
| A1 | Administrative district | button enables after `applyCoreFilters` (4-step); count 1; label = district name |
| A2 | Coordinate — manual add | Show on Map → 5-step cascade (incl. Statistic) → count 1; label = custom name / "Point N" |
| A3 | Coordinate — **upload** (live-only; P0 must confirm the control exists, else OBSERVE) | fixture `us10-coordinates-panel/fixtures/app_sample.csv` (3 rows, accepted `id,custom_name,lat,long`; row 3 blank name) → **count === 3**; labels = `["Site A","Site B", <non-empty default>]` |
| A4 | **Map click** | recon selectors (P0.3); box anchors; *Add to Analysis* enables; count 1 |

`applyCoreFilters`/`pickFirst` rely on "first exact-Select trigger in DOM order" — fragile;
**re-assert button-enabled state after each cascade**, don't trust it.

## Phase B — build one portfolio across flows (reset first; no duplicate inputs)

- B1 Admin district + coordinate upload
- B2 Admin district + map click
- B3 Coordinate manual + map click
- B4 all four sequentially → single portfolio; roster count correct; every site present in
  Manage Portfolio **and** Compare Table — assert via `portfolio-comparison-table` → 200 **if
  P0.4 confirmed it fires**, else fall back to asserting each site's row in the rendered DOM.

## Phase C — duplicate detection (reset + assert-empty before EACH case)

**C-strict** — exact same-type duplicate ⇒ hard **Major** if a second row appears:
- C1 same district twice via Administrative
- C3 identical coordinates uploaded twice, same name (`fixtures/dup_same_name.csv`)
- C4 identical coordinates, **different name** (`fixtures/dup_diff_name.csv`; name must not defeat dedup)

**C-semantic** — point-vs-district containment; assert-then-classify (NOT auto-Major):
- C2 same district via Administrative **and** via map click landing in that district
- C5 manually-added coordinate inside an already-added district
- C6 same coordinate via upload and via map click

### Outcome grid (per case; flag string from P0.2)

| Observed | Verdict (C-strict) | Verdict (C-semantic) |
|----------|--------------------|-----------------------|
| (a) second row created | **Major** | ASK-PO |
| (b) no second row **and** the P0.2 "already added" flag | **Pass** | Pass (distinct-unit) |
| (c) no second row, **no** message | **Minor** (dedup works, feedback missing) | Pass |

## Standard test data

- Districts: **Warangal**, **Karimnagar** (Telangana) — as US 17.
- Clean multi-row upload: `us10-coordinates-panel/fixtures/app_sample.csv` (accepted schema).
  Do **not** use `good.csv` — it uses the rejected `Latitude,Longitude,Label` header.
- Duplicate uploads: `fixtures/dup_same_name.csv` / `fixtures/dup_diff_name.csv`, accepted
  schema, both rows on the Warangal coordinate `17.8766,79.2792`.
- Map-click point: proportional canvas click landing inside Warangal (us13 `0.55/0.45` start).

## Out of scope

- Block-level adds (note as follow-up).
- Sending anything to the vendor from this run until findings are reviewed.
