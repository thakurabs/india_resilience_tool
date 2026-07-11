# Map interactivity gating + order-dependent portfolio (US-MAP-INTERACTIVITY)

**App:** `dev.resilience.org.in` · **Probe:** `qa/harness/add-to-analysis-map-interactivity.mjs`
**Run of record:** `qa/runs/2026-07-11T06-21-13-788Z_us-map-interactivity/`
**Gates (all passed):** `stateNormalized`, `interactionCalibrated` (mode=hover),
`mapPopoverScopedAdd`. Verdicts below are emitted only because all three gates passed;
evidence is reproducible across runs.

Two user-reported claims were established behaviorally by driving the live app (the vendor
frontend source is not in this repo). Both reproduce.

---

## Finding 1 — Dropdown district selection GATES map interactivity to the selected district (Major)

**Claim 1: CONFIRMED (interactivity-only gating, reversible; no view change).**

State-wide (Telangana, no district chosen) the whole choropleth is live: a dense sweep found
**34 live hover points across 21 distinct districts**. After choosing **Nirmal** in the
`Select District(s)` dropdown, a re-sweep of the *same screen locations* found:

| classification | points |
|---|---|
| still live, inside Nirmal (`sameDistrict`) | 22 |
| went **inert** (nothing surfaces) | 14 |
| different district / block | 0 |
| viewport/transform shift | 0 |

- `outsideSelectedLive = 0` — **zero** points outside Nirmal remain interactive.
- `viewDelta = false` — the map did **not** zoom/pan/drill-down; it stayed put and simply
  stopped responding outside Nirmal. This is **interactivity gating**, not a view change.
- `blockMode = false` — no block sub-level was engaged.
- **Reversible:** clearing the district restored whole-state interactivity (live-point overlap
  with the baseline ≥ 0.8).

Evidence: `p2a-baseline-sweep.png`, `p2b-selected-sweep.png`, `p2d-reversibility.png`,
`results.json → probe.claim1`.

**Why this is a defect:** the district dropdown is a *filter/highlight* control. Overloading it
to also **disable the map as an input surface** removes the user's ability to interact with any
other district while a selection is active. It is the root cause of Finding 2.

---

## Finding 2 — Portfolio is order-dependent / non-commutative (Major)

**Claim 2: CONFIRMED at full coverage — final portfolio count depends on add order.**

A portfolio is a **set union** of chosen units; set union is order-independent, so a final
count that depends on the order of additions is a logic defect. Two flows, identical target set
(3 map districts + 3 dropdown districts):

| order | steps (running count) | final |
|---|---|---|
| **Map-first** | map Nirmal→1, Nizamabad→2, Kamareddy→3, dropdown Medak→4, Ranga Reddy→5, Mahabubnagar→6 | **6** |
| **Dropdown-first** | dropdown Medak→1, Ranga Reddy→2, Mahabubnagar→3, then map Nirmal / Nizamabad / Kamareddy each `no-map-popover` (inert) | **3** |

In the dropdown-first flow all three map targets were **inert** (the map produced no
click-popover, so no Add CTA) — a direct consequence of Finding 1's gating. The user's
portfolio is silently capped at 3 instead of 6, with no error or explanation.

Evidence: `p3-p1-map-first.png`, `p3-p2-dropdown-first.png`, `results.json → probe.claim2`
(`p1.trail`, `p2.trail`, `p2.mapTargetClasses`).

---

## Finding 3 — Console noise: MapLibre style error + double-`/api/` 500s (Minor, observed)

Not part of the claims, but recorded by the harness during every run:

- **152×** `Error: layers.coordinate-boundary-outline.paint.line-width: "zoom" expression may
  only be used as input to a top-level "step" or "interpolate" expression` — an invalid MapLibre
  style spec (a `zoom` expression used illegally in `line-width`) that fires on every map
  re-render once a coordinate boundary layer is active.
- `GET /api/api/parquet/trend` and `/api/api/parquet/scenario-comparison` → **HTTP 500**. Note
  the doubled `/api/api/` path segment (consistent with previously reported ranking 500s).

---

## Deliverable B — logically correct behavior (recommendation)

The probe establishes the mechanism as a **hard interactivity gate / lock-out** (reversible, no
view change) — *not* a legitimate reversible drill-down and *not* a mode-switch state wipe.
Accordingly:

- **Decouple the map's `pickable` state from the dropdown selection.** All three selection
  surfaces — map click, district dropdown, coordinate panel — should be **additive peers**; none
  should disable another.
- The district dropdown should **filter / highlight** the chosen district(s), not switch off
  hover/click for the rest of the state. A user must be able to map-add district X while
  districts Y/Z are selected in the dropdown.
- Because the portfolio is a **set union**, the final set must be **identical regardless of add
  order**. Map-first and dropdown-first must both yield 6.
- If some form of focus/drill-down is intended, it must be (a) clearly a *view* change, (b)
  trivially reversible, and (c) must **never silently block or destroy** the ability to add other
  units. Silent capping (6→3 with no message) is unacceptable in any case.

---

## Reproduce

```bash
node qa/harness/capture-session.mjs                             # refresh 2FA session (~24h)
QA_SOFTWARE_GL=1 node qa/harness/add-to-analysis-map-interactivity.mjs
```
`QA_SOFTWARE_GL=1` is required so deck.gl WebGL hit-testing works headless (SwiftShader). The
probe BLOCKs rather than guessing if it cannot normalize geography, calibrate the interaction, or
prove a scoped map-add; each gate reports honestly. See `qa/README.md → Map-interactivity probe`.
