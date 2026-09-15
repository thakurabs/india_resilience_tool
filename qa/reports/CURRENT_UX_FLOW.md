# Current UX Flow — `dev.resilience.org.in` (as built, 2026-09-14)

> **Purpose.** A description, not a judgement, of the workflow the vendor has actually
> implemented — captured live so that the Overview / Detailed Analysis integration can be
> specified against what exists rather than against what we remember. It is the input to the
> vendor instruction set, not the instruction set itself.
>
> **Captured:** 2026-09-14 · **Branch:** `add_flood_depth` · **HEAD:** `6cd83a0`
> **Target:** `https://dev.resilience.org.in` (2FA-gated; frontend not in this repo)
> **Prior description of record:** `qa/reports/QA_RERUN_HANDOFF.md` (2026-07-27) — superseded
> for flow description; still authoritative for the defect baseline.

---

## 0. How this was produced

Six read-only Playwright sweeps drove the live app and recorded, at every state reached: the
full visible text, an interactive-DOM map, an outline (headings / landmarks / tabs / tables),
a screenshot, and **every `/api/` request and response the state fired** with payloads. Nothing
was saved, uploaded, downloaded or submitted; the only writes the app performed on its own were
`POST /audit/event` telemetry and a `PUT /users/profile` when a display preference was toggled.

| Harness | Covers | Evidence run |
|---|---|---|
| `qa/harness/ux-flow-recon-a.mjs` | shell, geography cascade, **complete filter taxonomy** | `runs/2026-09-14T16-35-40-826Z_ux-flow-A` |
| `qa/harness/ux-flow-recon-b.mjs` | committed analysis, map, ranking, profile | `runs/2026-09-14T16-48-46-345Z_ux-flow-B` |
| `qa/harness/ux-flow-recon-b2.mjs` | portfolio, My Analysis, Compare Portfolio | `runs/2026-09-14T17-01-55-626Z_ux-flow-B2` |
| `qa/harness/ux-flow-recon-c.mjs` | block level, historical path, chrome, Reset | `runs/2026-09-14T16-57-41-354Z_ux-flow-C` |
| `qa/harness/ux-flow-recon-c2.mjs` + coord probe | Coordinate Analysis path | `runs/2026-09-14T17-03-04-997Z_ux-coord-probe` |
| `qa/harness/ux-flow-recon-d.mjs` + ruler probe | API capability + **normalization** probes | `runs/2026-09-14T17-04-22-773Z_ux-flow-D-api`, `…T17-05-35-979Z_ux-ruler-probe` |

New shared helper: `qa/harness/lib/recon.mjs` (capture, option enumeration, API recorder).

**Harness defect fixed in passing.** `withSession` discarded the browser context at the end of
every run. The app **rotates refresh tokens**, so the first run that triggered
`POST /auth/refresh` invalidated the refresh token saved on disk and issued the replacement into
a context that was then thrown away — every run silently burned the saved session. This is what
the July note recorded as "the session expires in ~24h". `lib/session.mjs` now persists the
rotated cookies back, guarded so a bounced run cannot overwrite a good session.

---

## 1. The shape of the product today

One screen. No routes, no navigation levels, no breadcrumb. Everything happens in a fixed
three-zone layout over a full-bleed MapLibre canvas:

```text
┌───────────────────────────────────────────────────────────────────────────┐
│ header: India Resilience Tool          Share Feedback | Welcome, <user>   │
├──────────────────┬────────────────────────────────────────────────────────┤
│ SPATIAL PANEL    │  ┌ Select Resilience Filters ─────┐  ┌ Resilience     │
│ (left rail)      │  │ Risk Domain │ Metric           │  │ Profile |      │
│                  │  │ Scenario    │ Period           │  │ My Analysis    │
│ Administrative   │  │ Statistic   │ Map Mode         │  │ (right rail,   │
│   Analysis       │  └────────────────────────────────┘  │  collapsed)    │
│ Coordinate       │                                                        │
│   Analysis       │              [ map canvas ]                            │
│ Select your      │                                                        │
│   views          │        ┌ legend: <metric> + 6 ticks ┐                  │
└──────────────────┴────────────────────────────────────────────────────────┘
```

Three independently collapsible panels — left (geography), top-centre (filters), right
(profile/portfolio) — floating over one persistent map. The map is never replaced; the Ranking
Table renders as a panel **over** it.

---

## 2. The entry contract: the first screen is empty of information

The landing state contains no result. Its entire text is the chrome plus
`Choose a geography to begin analysis` and `Enter precise coordinates manually to begin
analysis.` The map shows an unpainted basemap of India. No default bundle, no default scenario,
no default period, no score, no ranking.

To reach *any* number the user must complete **two independent cascades in the correct order**:

```text
Geography                          Filters (gated, strictly sequential)
  Select State           (36)        Risk Domain      → enables Metric
  → District | Block     (radio)     Metric           → enables Scenario
  → Select District(s)   (multi)     Scenario         → enables Period
                                     Period           → enables Statistic
                                     Statistic        → enables Map Mode
```

Until a Risk Domain is chosen the other five controls read `Select a domain first`,
`Select a metric first`, `Select a scenario first`, `Select a period first`,
`Select a statistic first` and are disabled. Six selections minimum, plus two geography
selections, before the first score appears. `Add to Analysis` and `Save Analysis` stay disabled
until the cascade is complete.

Two display preferences sit under `Select your views`: `Enable hover highlight & tooltip`
(on by default) and `Show quick guide on login` (off; toggling it issues `PUT /users/profile`,
so it is account state, not local state).

---

## 3. Geography model

- **State** — 36 State/UTs plus a `Select State` placeholder. Sourced from
  `POST /geo/india-location-hierarchy` (`{"type":"state-list","count":36}`), with the full
  State → District → Block tree available from `GET /geo/all-geography` (each node carries a
  numeric `id`).
- **Administrative level** — a `District` / `Block` radio pair. Not a drill-down: it re-scopes
  the picker beneath it. Selecting `Block` swaps `Select District(s)` for `Select Block(s)`.
- **District(s) / Block(s)** — a **multi-select** with an `All Districts` option. Telangana
  exposes 33 districts + `All Districts`.
- **Search Geography** — a free-text box above the State picker (new since July).
- **Selection is a filter, not a navigation event.** There is no India view, no State view, no
  District view; there is one map whose camera flies to whatever is selected.

A district is selectable three ways — the dropdown, the search box, or a map click — and the
three are **not equivalent**: the July probe established that picking from the dropdown makes
every point outside the selection inert on the map (`qa/reports/us-map-interactivity.md`,
Claims 1 & 2, both CONFIRMED). That gating was not re-tested in this pass.

---

## 4. Complete filter taxonomy (as enumerated live)

### 4.1 Risk Domains — six, and no sector bundles

| Risk Domain | Metrics | Composite metric id | Scenarios | Periods |
|---|---:|---|---|---|
| Heat Risk | 15 | `composite_heat_risk` | BAU, Pessimistic | Early / Mid / End century |
| Drought Risk | 7 | `composite_drought_risk` | BAU, Pessimistic | Early / Mid / End century |
| Extreme Rainfall \| Flash Flood Risk | 7 | `composite_flood_extreme_rainfall_risk` | BAU, Pessimistic | Early / Mid / End century |
| Riverine Flood | 4 | `composite_flood_jrc_depth` | **Snapshot** | **Current** |
| Heat Stress | 17 | `composite_heat_stress` | BAU, Pessimistic | Early / Mid / End century |
| Cold Risk | 12 | `composite_cold_risk` | BAU, Pessimistic | Early / Mid / End century |

62 metrics in total; the first entry of each domain is its `… Composite (score)`.

**The eight sector-wise bundles do not exist in the deployed product** — no Agricultural,
Health, Industrial, Investment/Financial, Infrastructure, Asset (Thermal), Asset (Hydro) or
Life & Livelihood Loss Risk. The target workflow's screening surface is 13 bundles; the app
ships 5 of them plus Riverine Flood, which the target workflow explicitly *excludes* from that
surface for having a different temporal contract.

### 4.2 Scenario and period vocabulary

| UI label | Internal id | Notes |
|---|---|---|
| `Historical` | — | constituent metrics **only**; never offered on a composite |
| `Business as usual` | `ssp245` | |
| `Pessimistic` | `ssp585` | |
| `Snapshot` | `snapshot` | Riverine Flood only |
| `Historical baseline` | — | auto-selected, control disabled, when Scenario = Historical |
| `Early century` | `2020-2040` | |
| `Mid century` | `2040-2060` | |
| `End century` | `2060-2080` | |
| `Current` | `Current` | Riverine Flood only |

`Statistic`: `Mean` only for composites; `Mean` and `Median` for constituent metrics.
`Map Mode`: auto-fills to `Absolute value` and is **disabled** on the composite path.

The cascade's legal combinations come from
`GET /parquet/metric-options?metric=<id>&valueCol=<id>&level=<district|block>`, which returns an
explicit `combos` array of `{scenario, period, statistic}` — a real manifest, correctly used.

### 4.3 Control help

Every control has an `ⓘ` button, but all six expose the same generic accessible name
`More information`; the tooltip body did not render to the DOM under automation, so the copy
itself is unverified.

---

## 5. Result surfaces

### 5.1 Map view (default)

Paints the **selected administrative level within the selected State** — districts, or blocks
when Block is chosen. The camera zooms to the selection; neighbouring districts in the same
State remain painted, everything outside the State is unpainted.

The legend is a continuous green→yellow→red strip titled with the metric and carrying **six
numeric ticks fitted to the current selection's data extent** — for Telangana / Heat Risk
Composite / BAU / Mid century it read `21.99 · 32.87 · 43.75 · 54.63 · 65.51 · 76.39`. It is not
a fixed `0–100` domain, and it moves whenever the selection moves.

**Hover** gives a tooltip (`pointer-events-none`). **Click** opens an info surface carrying:

```text
District  Warangal        State  Telangana
Composite Score  51.69    Rank in state  18
[ Add to Analysis ]
```

Map data comes from `POST /parquet/composite-map-data`; the click surface additionally calls
`POST /parquet/map-tooltip`.

### 5.2 Ranking Table

A radio in `Select your views` swaps the map for a table panel. **This works now** — the July
blocker (`/parquet/ranking` → HTTP 500) is resolved; the endpoint returned 200 three times in
this pass.

```text
Position in State (1) │ District Name │ State Name │ Composite Score │ Add to Portfolio
       18             │   Warangal    │  Telangana │      51.69      │
```

Beneath it sits a five-interval band key derived from the fitted legend domain
(`21.99–32.87`, `32.87–43.75`, …).

Two things about this table matter for the integration:

- **It lists only the selected districts, not the State's cohort.** With one district selected
  it has one row. The header reads `Position in State (1)` while the row's rank is `18` — the
  parenthetical is the row count, not the rank denominator, so the table asserts "rank 18 of 1".
- Ranking is therefore not a discovery surface. You cannot use it to find the worst districts;
  you must already know which districts you want before it will show you their ranks.

### 5.3 Resilience Profile (right rail)

For a single selected district:

```text
Warangal · District climate profile
Heat Risk Composite   [Business as usual] [2040–2060] [Mean]

Risk summary
  PROJECTED HEAT RISK COMPOSITE
  51.7 score                       2040–2060 under Business as usual
  Position within Telangana        18

▸ Trend over time          (collapsed)
▸ Scenario comparison      (collapsed)
```

Opens in the rail or in a full-screen modal. The two collapsed accordions are driven by
`POST /parquet/trend` and `POST /parquet/scenario-comparison`, **both of which still return HTTP
500** on the composite path — the July M5 finding is unchanged.

The headline is closer to an answer card than anything else in the product, but it carries no
band, no rank denominator (`18`, not `18 of 33`), no drivers, and no interpretation boundary.

### 5.4 Portfolio, My Analysis, Compare

`Add to Analysis` puts the selected location into **My Portfolio**. Adding is confirmed by a
top-right toast (`A district is added to your portfolio.`); re-adding is correctly rejected
(`1 duplicate location was skipped.`). Toasts render over the right-rail header and intercept
clicks there for their lifetime.

`My Analysis` holds two lists: **Saved Analysis** (persisted named analyses, each with a
Rename/Delete action menu; `GET /saved-analyses`) and **My Portfolio** (the working set, with
`Clear Portfolio` and `Compare Portfolio`).

**Compare Portfolio** is the most developed analytical surface in the product:

```text
Select Risk Domain to compare   [Heat Risk]
Select Metrics                  [multi-select checkbox list]
Scenario   ☐ Historical ☐ Business as usual ☐ Pessimistic ☐ Snapshot
Period     ☐ Historical baseline ☐ Early ☐ Mid ☐ End century ☐ Current
[ Table ] [ Visualizations ] [ Download Reports ]

District Name │ State Name │ Scenario │ Period │ Composite Score
Warangal      │ Telangana  │ BAU      │ Mid    │ 51.70
Karimnagar    │ Telangana  │ BAU      │ Mid    │ 66.50
```

Note the shape: **place, scenario and period all vary simultaneously** as independent
multi-selects over one flat table. Backed by `POST /parquet/composite-domain-table`.

### 5.5 Coordinate Analysis

An alternate location path, peer to Administrative Analysis, with its own accordion and a
confirm dialog on entry (`Switch to Coordinate Analysis? … Your current geography selection will
be cleared, and analysis will use block-level…`; buttons `Cancel` / `Use Coordinate Analysis`).

Two tabs:

- **Add Coordinates** — `Latitude` / `Longitude` / `Custom Name` text inputs (placeholders
  `17.8766`, `79.2792`, `Site 1`), then `Show on Map` → `Clear` → `Add Coordinate`. Resolution
  goes through `POST /geo/reverse-geocode` and reports `This location is WARANGAL, TELANGANA`.
- **Upload Coordinates** — one file input accepting `.csv,.xlsx,.zip`, a supported-format list,
  and a `View file upload guidelines` link. Not exercised in this pass; the adversarial findings
  in `qa/reports/UPLOAD_VALIDATION_HANDOFF.md` (M7, N23–N25) were not re-verified.

### 5.6 Global chrome

`Share Feedback` modal, an account menu under `Welcome, <name>`, a `Reset` that clears geography
and filters back to the empty landing state, and per-panel reset buttons.

---

## 6. The data contract behind the screens

| Endpoint | Method | Drives |
|---|---|---|
| `/api/api/geo/india-location-hierarchy` | POST | State list |
| `/api/api/geo/all-geography` | GET | Full State→District→Block tree with ids |
| `/api/api/geo/reverse-geocode` | POST | Coordinate → admin unit |
| `/api/api/parquet/metric-options` | GET | Legal scenario × period × statistic combos |
| `/api/api/parquet/composite-map-data` | POST | Map fill + `rank_in_state` |
| `/api/api/parquet/map-tooltip` | POST | Map click surface |
| `/api/api/parquet/ranking` | POST | Ranking Table — **200, fixed** |
| `/api/api/parquet/composite-domain-table` | POST | Compare Portfolio table |
| `/api/api/parquet/trend` | POST | Profile trend — **500, open** |
| `/api/api/parquet/scenario-comparison` | POST | Profile scenario chart — **500, open** |
| `/api/api/saved-analyses` | GET | Saved Analysis list |
| `/api/api/users/profile` | GET/PUT | Account + display preferences |
| `/api/api/audit/event` | POST | Telemetry on navigation and actions |
| `/api/api/auth/refresh` | POST | Rotating-token refresh |

The map request and response:

```jsonc
POST /api/api/parquet/composite-map-data
{ "riskDomain":"composite_heat_risk", "state":"Telangana",
  "level":"district", "scenario":"ssp245", "period":"2020-2040" }

{ "success":true, "count":33, "data":[
  { "district":"Mancherial", "state":"Telangana",
    "district_key":"telangana|mancherial", "value":77.13,
    "baseline":null, "delta_vs_baseline":null, "rank_in_state":1 }, … ] }
```

Three properties of that contract shape everything downstream:

1. **`state` is mandatory.** Omitting it → `400`. `state:null` → `400`. `state:"All"` → `500`.
   `level:"state"` → `400`. There is **no national payload and no State-level aggregate** — not
   as an unused option, but as a hard constraint of the deployed API.
2. The join key is `district_key` = `"<state>|<district>"`, a **lowercased name pair**, not a
   stable administrative identifier.
3. `baseline` and `delta_vs_baseline` exist in the schema but are `null` on the composite path.

The API enforces CSRF (the `XSRF-TOKEN` cookie must be echoed as a header) — probes without it
get `403`. The doubled `/api/api/` segment is present on **every** endpoint including the ones
that work, so it is the deployed base path, not a fault: the July hypothesis that it caused the
ranking 500 is disproved, since ranking now returns 200 over the same doubled path.

One privacy observation: `GET /users/profile` returns `refreshTokenHash`, `deviceFingerprint`,
`tokenVersion` and `refreshTokenExpiresAt` to the browser. None are needed by the UI.

---

## 7. What the score actually is — the finding that governs the integration

`composite-map-data` was called for six State/UTs across all six scenario × period slices
(`runs/2026-09-14T17-05-35-979Z_ux-ruler-probe/_ruler_probe.json`). Heat Risk Composite,
district level, min–max of returned values:

| State/UT | n | ssp245 early | ssp245 mid | ssp245 end | ssp585 early | ssp585 mid | ssp585 end |
|---|---:|---|---|---|---|---|---|
| Goa | 2 | 15.83–84.17 | **0–100** | **0–100** | 36.67–63.33 | **0–100** | **0–100** |
| Ladakh | 2 | 20–80 | 28.33–71.67 | 28.33–71.67 | 28.33–71.67 | 28.33–71.67 | 28.33–71.67 |
| Kerala | 14 | 6.28–78.86 | 4.79–79.41 | 4.12–79.00 | 7.09–78.86 | 6.20–81.05 | 12.10–82.24 |
| Telangana | 33 | 23.96–77.13 | 21.99–76.39 | 23.71–75.44 | 20.41–77.10 | 20.61–72.94 | 15.44–73.84 |
| Rajasthan | 49 | 20.62–85.32 | 16.24–82.33 | 13.86–82.94 | 21.85–82.22 | 15.91–85.08 | 12.47–84.95 |
| Delhi | 11 | 30.29–88.75 | 26.40–76.46 | 32.65–82.85 | 29.75–77.32 | 33.17–65.50 | 36.51–67.72 |

Four things follow, and they are not matters of interpretation:

1. **The ruler is fitted per State, per slice.** Goa, with two districts, returns exactly
   `0` and `100` in four of six slices — the min–max fingerprint. Every State has its own moving
   range.
2. **Scores are not comparable between States.** Kerala tops out at 79–82 while Telangana tops
   out at 73–77. On any absolute heat scale that ordering is wrong. It is only coherent as
   "position within your own State's cohort".
3. **Scores carry no scenario or warming signal.** Telangana's maximum under SSP5-8.5 runs
   77.10 → 72.94 → 73.84 from early to end century; Delhi's runs 77.32 → 65.50 → 67.72. The
   number goes *down* as the world gets hotter, because relative position within a State is
   roughly conserved while the physical values all rise together. Ladakh returns the **identical
   pair `28.33 / 71.67` in five of six slices** — its score is completely insensitive to both
   scenario and period.
4. **Therefore the Scenario and Period selectors do not currently change the answer** in the way
   their labels promise. They change which slice is normalized, not where the place sits on a
   stable scale.

This is exactly the defect the frozen-CDF programme was built to fix, now observed in the
deployed product. **Every score visible in the app today is a within-State relative position on
a ruler that moves when the State, scenario or period moves.** The Overview cannot be layered
on top of it: a national map painted from these values would be meaningless, because the values
have no common origin.

---

## 8. Gap analysis against the target workflow

Against `recommended_target_workflow.md` (rev. 2026-09-09) and
`docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html`.

### 8.1 Present and reusable

| Target requirement | Status in the build |
|---|---|
| Bundle / Scenario / Period as persistent selectors | Present (as Risk Domain / Scenario / Period) |
| Artifact-driven legal combinations | Present and correct — `metric-options.combos` |
| Map click → unit identity + score + rank | Present |
| Ranking table with Add-to-portfolio | Present |
| Portfolio / comparison tray concept | Present, and more capable than expected |
| Coordinate entry as an alternate path | Present, correctly subordinate |
| Saved analyses, reload, rename, delete | Present |
| Block-level painting | Present |
| Scenario/period label vocabulary | Present but different wording |

### 8.2 Absent

| Target requirement | Gap |
|---|---|
| **National/India view** | No such view, and the API cannot serve one (§6) |
| **Frozen `0–100` national ruler** | Contradicted by the build (§7) |
| **One fixed colourbar, domain never rescales** | Legend refits to every selection |
| State/UT area-weighted mean headline | No State-level statistic at all |
| Ten-bin score distribution histogram | Absent |
| Breadcrumb `India > State/UT` | Absent — no navigation levels to name |
| Answer card (score + band + scoped rank + drivers + boundary) | Partial: score and bare rank only |
| Five interpretive bands (Very Low…Extreme) | Absent — five numeric intervals off a moving domain |
| Metric Drivers / Top Rule Signals | Absent |
| Context and Evidence / Context layers | Absent |
| `Explore in Detailed Analysis` transition | Absent — there is only one level |
| Compare: one axis at a time (places **xor** futures) | Contradicted — all axes vary at once |
| Ranking as discovery (full cohort, top-10, View all) | Contradicted — lists only what you picked |
| Default-populated first screen | Contradicted — empty until 8 selections |
| Eight sector bundles | Absent |
| Stable administrative-ID joins | Absent — name-pair keys |
| `ruler_id` / `colour_scale_id` / roster provenance | Absent |

### 8.3 The structural mismatch, stated plainly

The deployed product is a **metric explorer**: pick a place, pick a measurement, look at it.
The target workflow is a **screening tool**: open on an answer, find where the problem is,
narrow down, then explore. These are not two settings of one design — they invert the order of
the user's work, and the vendor's data contract is built for the first.

Three things must be true before any Overview can exist on this platform, and none of them is
UI work:

1. **Scores must come from one frozen national ruler** (§7). Until then the Overview's central
   claim — that the same colour means the same thing everywhere — is false.
2. **The API must serve a national district payload** (§6). Today `state` is mandatory.
3. **A State/UT-level artifact must exist** — area-weighted mean, `n_valid`, national rank.
   Today there is no State statistic anywhere in the product.

Only after those does the visible work — India view, histogram, breadcrumb, answer card, bands,
drivers, context layers, Detailed Analysis transition — become implementable.

---

## 9. What changed since the July QA pass

| July finding | Status now |
|---|---|
| **B1 Blocker** — `/parquet/ranking` HTTP 500 | **FIXED** — 200, table renders |
| **M5** — `/parquet/trend` + `/scenario-comparison` 500 | **STILL OPEN** — both 500 |
| Doubled `/api/api/` blamed as root cause | **Disproved** — universal base path; ranking works over it |
| "Session expires ~24h" | **Misdiagnosis** — single-use refresh-token rotation (fixed in harness) |
| C1 dedup + feedback | **Holds** — `1 duplicate location was skipped.` |
| Map dropdown-gating Claims 1 & 2 | **Not re-tested** this pass |
| M7 / N23–N25 upload findings | **Not re-tested** this pass |
| a11y M1/M2/M3 | **Not re-assessed** this pass |

New since July: `Spatial Panel` naming, `Search Geography`, `All Districts`, the quick-guide
preference, `Save Analysis` in the geography panel, and a `Visualizations` tab in Compare
Portfolio (was Table + Heatmap).

---

## 10. Open questions before the vendor instruction set is written

1. **Who owns the ruler?** Do we ship the frozen-CDF artifact (`cdf_v1` + `colour_scale.json`,
   already built) and require the vendor to render it as a lookup, or do we ask them to compute
   national normalization themselves? The former is the standing decision; it needs restating to
   them explicitly.
2. **Sector bundles.** Five of thirteen eligible bundles exist. Is the Overview specified for
   13 and shipped with 6, or re-scoped to what exists?
3. **Riverine Flood.** It is in the domain list; the target workflow excludes it from the
   screening surface. Does it stay as a non-scenario product outside the Overview?
4. **Ranking semantics.** Does the ranking table become a cohort ranking (target) or stay a
   selection readout (current)? This changes what "Position in State (N)" means.
5. **Compare Portfolio.** Constrain the existing surface to one-axis-at-a-time, or leave it as
   an advanced feature and build the tray separately?
6. **Sequencing.** Data contract first, or a UI shell against the current per-State scores?
   §7 argues the shell would show numbers that cannot be defended.

---

## 11. Change ledger

| Change ID | File(s) | Summary | Status |
|---|---|---|---|
| CHG-0470 | `qa/harness/lib/recon.mjs` | Recon capture/enumeration/API-recorder helpers | `APPLIED (uncommitted)` |
| CHG-0471 | `qa/harness/lib/session.mjs` | Persist rotated auth cookies; guard against overwriting a good session | `APPLIED (uncommitted)` |
| CHG-0472 | `qa/harness/ux-flow-recon-{a,b,b2,c,c2,d}.mjs` | Six read-only UX-flow recon harnesses | `APPLIED (uncommitted)` |
| CHG-0473 | `qa/reports/CURRENT_UX_FLOW.md` | This document | `APPLIED (uncommitted)` |
| CHG-0474 | `qa/harness/ux-discover.mjs` | Selector-discovery sweep (July selectors were stale) | `APPLIED (uncommitted)` |
