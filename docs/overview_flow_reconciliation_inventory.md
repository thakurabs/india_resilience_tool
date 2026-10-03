# Overview flow — candidate inventory for reconciliation

> **Purpose.** The working instrument for Stage 0: reconciling
> [`recommended_target_workflow.md`](../recommended_target_workflow.md) (the stated design) with
> [`docs/diagnostics/heat_risk_pilot/overview_flow_prototype.html`](diagnostics/heat_risk_pilot/overview_flow_prototype.html)
> (the executable design) into **one finalised flow**. The output of that review becomes the sole
> "target" input to the vendor spec.
>
> This is **scope triage, not a merge.** The workflow doc is brainstorming output; we are not
> obliged to adopt all of it. `OUT` is a first-class verdict.
>
> **Compiled:** 2026-09-15 · prototype read at `3bcce3d` · doc revision 2026-09-09

## The job statement (agreed)

> A user who must decide **where to act** opens the tool, sees the national picture for a default
> hazard, identifies which places are worst, drills into one, understands why, and either compares
> a shortlist or hands off to detailed analysis.

Every verdict below is judged against that sentence. An item that does not help a user do that job
does not earn a place merely because it is interesting or already written down.

## How to read this

| Tag | Meaning |
|---|---|
| `BOTH` | In the doc **and** built in the prototype. Presumed `IN`; confirm and move on. |
| `PROTO` | Built in the prototype, absent or unstated in the doc. Presumed `IN` (deliberate, often decided by measurement) — but needs a ruling so the doc stops contradicting it. |
| `DOC` | Proposed in the doc, **not built**. Presumed `OUT` until it earns its place. This is where most of the real decisions are. |
| `CONFLICT` | The two sources actively disagree. Must be ruled on; cannot be deferred. |

Verdicts: **IN · OUT · LATER · IN-BUT-CHANGED**. `Rec` = my recommendation; the `Verdict` column is
yours and is left blank.

**Separated out — not triaged here.** §"Expected denominator and boundary-vintage contract",
§"Current case-study validation baseline", §9 artifact field lists and §10 acceptance tests are
**engineering contract, not workflow**. They belong in the data/API spec and should not be argued
as features. Listed in Appendix A so nothing is lost, not reviewed in batches.

---

## Batch A — Shell, selectors, defaults

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| SHL-01 | Three primary selectors only: Bundle, Scenario, Period | BOTH | Built, in header | §2 | IN | |
| SHL-02 | 13 bundles, grouped Thematic / Sector-wise | BOTH | Selector lists all 13; only Heat Risk scored | §1 | IN | |
| SHL-03 | Public defaults Heat Risk / SSP5-8.5 / 2040-2060 | BOTH | Built (`default_bundle` etc.) | §1 | IN | |
| SHL-04 | Defaults visibly labelled as defaults, not user choices | BOTH | `defaults-note` line under header | §1 | IN | |
| SHL-05 | Scenario labels "Middle-of-the-road / Fossil-fuelled development" | DOC | Prototype uses raw `ssp245`/`ssp585` ids | §1 | IN-BUT-CHANGED — decide final wording vs vendor's "Business as usual / Pessimistic" | |
| SHL-06 | Period labels Early / Mid / End century | BOTH | Built | §1 | IN | |
| SHL-07 | Persistent method note explaining scale, grammar, hazard-only boundary | BOTH | Collapsed `<details>` "Method note" | §1 | IN | |
| SHL-08 | Riverine Flood / Water Risk excluded from the screening surface | DOC | n/a (single-bundle pilot) | §1 | IN — but vendor ships Riverine today; needs an explicit disposition | |
| SHL-09 | Name "Overview" / "Detailed Analysis" (not Glance / Deep Dive) | BOTH | Title says "Overview" | Working direction | IN | |

## Batch B — National view: map encoding

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| NAT-01 | National map paints **districts** from composite scores | BOTH | Built | §1 | IN | |
| NAT-02 | State/UT polygons **never** filled | BOTH | Built; map-sub says so explicitly | §1 | IN | |
| NAT-03 | Fixed `0–100` domain, never rescaled by selection | BOTH | Built; colourbar foot states it | §1 | IN | |
| NAT-04 | One continuous colourbar, level-neutral title | BOTH | `cbar-title` = "Heat Risk score" | §1 | IN | |
| NAT-05 | `WhiteBlueGreenYellowRed`, 101 stops from fraction 0.045 | BOTH | `ramp` array in payload | §1 | IN | |
| NAT-06 | Boundary grammar: thick = previous level, thin = painted level | BOTH | Built | §1 | IN | |
| NAT-07 | Range bracket on the colourbar + numeric readout | BOTH | `cbar-bracket`, `cbar-range` | §1 | IN | |
| NAT-08 | `Local contrast` opt-in rescale, labelled not-comparable | BOTH | `lc-toggle` built | §1 | IN | |
| NAT-09 | `#d5d8dc` no-data treatment + own legend swatch | BOTH | Built | §1 | IN | |
| NAT-10 | Five interpretive bands Very Low…Extreme | BOTH | `BANDS`, `bandPill`, band ticks | §1 | IN | |
| NAT-11 | Perceptual interpolation (OKLCH/CIELAB) on resample | DOC | Fixed ramp, no resampling | §1 | LATER — implementation detail for the design system | |
| NAT-12 | Quality-state pattern/outline distinct from score colour | DOC | Not built | §1 | LATER — no quality flags exist in the pilot data | |

## Batch C — National view: interaction

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| NIN-01 | Hover highlights the **whole State**, tooltip is State-level only | BOTH | Built exactly | §1 | IN | |
| NIN-02 | District names/scores must NOT appear in the national tooltip | BOTH | Enforced | §1 | IN | |
| NIN-03 | Tooltip carries mean, band, rank of N, n_valid | BOTH | Built | §1 | IN | |
| NIN-04 | Click a State → select, zoom, load blocks, repaint from blocks | BOTH | Built (`selectState`) | §1 | IN | |
| NIN-05 | Painted-units figure line ("784 districts painted · median…") | BOTH | `painted` element | §1 | IN | |
| NIN-06 | Tooltip states *why* a unit is not selectable | PROTO | "Not selectable — only Telangana is scored…" | absent | IN — the pattern generalises to any no-data unit | |
| NIN-07 | Progressive geometry: blocks load only on State selection | BOTH | n/a (all embedded) | §1 | IN — vendor-side requirement | |

## Batch D — The District-view conflict ⚠

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| DIS-01 | **Is District a navigation level or an inspection state?** | **CONFLICT** | Prototype has a full third view: `S.view === "district"`, `openDistrict()`, "District headline", map re-paints at district extent | Doc **withdraws** it (§1 revision note; lines 576–581): district selection is an inspection state inside the State view | prototype | **IN — District IS a navigation level (prototype)** |
| DIS-02 | Third breadcrumb level `India › State › District` | **CONFLICT** | Built (`renderCrumbs`) | Doc: breadcrumb stops at `India › State/UT` | follows DIS-01 | **IN — three levels** |
| DIS-03 | District headline card (score, band, rank, block range) | BOTH | Built as its own view's headline | Doc has it as an inspection **panel** | IN | **IN — as the District view's own headline** |
| DIS-04 | District view repaints the same blocks at a smaller extent | PROTO | Built; map-sub says "same ruler, same colourbar, at this district's extent" | Doc calls this a "redundant re-render" and is why it withdrew the level | follows DIS-01 | **IN — the doc's withdrawal is reversed** |
| DIS-05 | `District fill` toggle in the State view | DOC | **Not built** | §1 line 411 | OUT | **OUT** |

**Ruling on Batch D (2026-09-15).** The prototype's three-level model is adopted; the doc's
2026-09-09 withdrawal of the District view and the third breadcrumb level is **reversed**. The doc
called the district map "a redundant re-render"; the built version is not redundant because it
re-frames the extent, hosts its own headline, and is the only place a block becomes selectable.

Consequences to carry: **INS-04** (district/block selection precedence) must be restated for a
three-level model rather than the doc's inspection-state model.

**CLOSED 2026-09-16 (CHG-0489).** The contradiction in `recommended_target_workflow.md` is
resolved rather than merely marked: the doc now carries the three-level model in its own section 0
and section 1, its 2026-09-09 revision header is annotated as reversed, and the reversal is
recorded with its reason at the site. The same change reversed the block-ranking prohibition
(blocks rank within their own district and nowhere wider) and renamed the top band to `Very High`.
Note that this inventory's own vocabulary predates the naming settled in that doc's section 11;
where the two differ, section 11 governs.

## Batch E — Inspection panels

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| INS-01 | Block inspection panel: name, parents, score, band, drivers | BOTH | Built | §1 | IN | |
| INS-02 | Blocks are painted but **never ranked** at any scope | BOTH | Panel says so in the Rank row | §1 | IN | |
| INS-03 | Block selection adds no breadcrumb level (stated in the crumb bar) | BOTH | Built, with an explicit note | §1 | IN | |
| INS-04 | District/block selection precedence rules (select district clears block; block outside reselects parent) | DOC | Partially — block only selectable inside the district view | §1 lines 601–606 | IN-BUT-CHANGED — restate once DIS-01 is settled | |
| INS-05 | Coverage/quality status row in the panel | BOTH | Hard-coded "Complete — all 9 headline metrics valid" | §1 | IN — needs a real field | |
| INS-06 | District selectable from map, histogram, or ranking row | BOTH | Ranking row + map click built | §1 | IN | |

## Batch F — Distribution histogram

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| HIS-01 | Histogram bins the units the view **ranks**, not paints | BOTH | Built | §1 | IN | |
| HIS-02 | Ten fixed bins of width 10 over `0–100`, empty bins drawn | BOTH | Built | §1 | IN | |
| HIS-03 | Titled by the units it counts | BOTH | `hist-title` / `hist-sub` | §1 | IN | |
| HIS-04 | Band ticks at 20/40/60/80 under the axis | BOTH | `bandticks` row | §1 | IN | |
| HIS-05 | Hover a bin → emphasise those units, mute the rest | DOC | **Click only** — no hover emphasis | §1 line 523 | OUT | **OUT** |
| HIS-06 | Click a bin → pin, filter the ranking, never recompute ranks | BOTH | Built (`S.pinned`, `filterbar`) | §1 | IN | |
| HIS-07 | `Clear filter` action | BOTH | Built | §1 | IN | |
| HIS-08 | Pinned bin clears on bundle/scenario/period/view change | BOTH | Built | §1 | IN | |
| HIS-09 | Five-band bar chart **withdrawn** | BOTH | Not built | §1 | IN (i.e. stays out) | |

## Batch G — Ranking

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| RNK-01 | National = rank State/UTs by area-weighted mean district score | BOTH | Built | §1 | IN | |
| RNK-02 | State view = rank valid districts within that State/UT | BOTH | Built | §1 | IN | |
| RNK-03 | Top-10 shortlist + `View all N` | BOTH | Built (`S.showAll`) | §1 | IN | |
| RNK-04 | Competition ranks, ties marked | BOTH | Built (`=` suffix on ties) | §1 | IN | |
| RNK-05 | Ranking row is a navigation control (click opens the unit) | BOTH | Built | §1 | IN | |
| RNK-06 | Columns: rank, name, score, band, compare | PROTO | Built | doc unspecified | IN | |
| RNK-07 | Extra decimal precision on visually-tied rows | DOC | `displayScores()` — **built** | §1 | IN | |
| RNK-08 | `Rank N (tied) of M valid units` phrasing | DOC | Not built (uses `=`) | §1 | OUT | **OUT** |
| RNK-09 | Ranking as a collapsed `View rankings` section | DOC | Permanently visible card | §4 line 711 | OUT | **OUT** |
| RNK-10 | `Where are the hotspots?` as a separate section | DOC | Not built | §4 line 709 | OUT | **OUT** |

## Batch H — Answer card / headline

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| ANS-01 | State/UT headline: name, band pill, big score | BOTH | Built | §1, §3 | IN | |
| ANS-02 | Headline meta: rank of N, n_valid districts, blocks painted | BOTH | Built | §1 | IN | |
| ANS-03 | Scope sentence: "area-weighted average of its districts' national scores — not a percentile among States" | BOTH | Built verbatim | §1 | IN | |
| ANS-04 | Hazard-only interpretation boundary on the card | BOTH | Built | §3 | IN | |
| ANS-05 | Up to three drivers on the card | BOTH | `driverList()` | §3, §7 | IN | |
| ANS-06 | Prose answer sentence ("Warangal has a score of 72, ranking 4 of 33…") | DOC | Structured card, **no prose sentence** | §3 line 689 | OUT | **OUT** |
| ANS-07 | No numeric driver signal values shown in Overview | BOTH | Built | §7 | IN | |
| ANS-08 | `Metric Drivers` (thematic) vs `Top Rule Signals` (sectoral) | DOC | Only thematic exists in the pilot | §7 | LATER — needs the sector bundles | |
| ANS-09 | "Driver information is not available for this geography." empty state | DOC | Not built | §7 | IN — cheap, prevents a blank region | |

## Batch I — Compare

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| CMP-01 | Tray absent until the user puts something in it | BOTH | Built | §4 | IN | |
| CMP-02 | Places mode / Futures mode, one axis at a time | BOTH | Built (`S.cmp.mode`, `enterFutures`/`leaveFutures`) | §4 | IN | |
| CMP-03 | Max 4 members | BOTH | Built | §4 | IN | |
| CMP-04 | Add control wherever a unit is named (headline, panels, ranking rows) | BOTH | `cmpControl()` in all four places | §4 | IN | |
| CMP-05 | Members retained across a bundle change, figures replaced | BOTH | Built | §4 | IN | |
| CMP-06 | Rank as a scoped string in places mode, never a sortable column | BOTH | `cmpRankText()` | §4 | IN | |
| CMP-07 | Futures mode may show rank movement; flags differing `n_valid` | BOTH | Built | §4 | IN | |
| CMP-08 | Differences in scale points, labelled as percentile position | BOTH | Built | §4 | IN | |
| CMP-09 | Mixed admin levels permitted and labelled | BOTH | Built | §4 | IN | |
| CMP-10 | Shared vs unique drivers visually distinguished | BOTH | Built | §4 | IN | |
| CMP-11 | Tray members outlined on the map in a distinct hue | DOC | Not built | §4 line 814 | LATER — panel is authoritative; map emphasis is a convenience | |
| CMP-12 | **Four known prototype defects** (n_valid means two things; col-1 self-compare; futures/header desync; State outlines vanish on drill) — CHG-0417/0418 never applied | PROTO | Present as defects | n/a | Fix before the spec cites the prototype | |

## Batch J — Context and Evidence

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| CTX-01 | Collapsed by default, from the State view onward | BOTH | Built (`S.ctxOpen`) | §5 | IN | |
| CTX-02 | Scope-aware (State → district → block) | BOTH | `contextScope()` | §5 | IN | |
| CTX-03 | Exposure summary (population, density, cropland, built-up) | BOTH | `ctxExposureHtml` | §5 | IN | |
| CTX-04 | Hydrological context | BOTH | `ctxHydroHtml`, partial | §5 | IN-BUT-CHANGED — no State-scope basin share by design | |
| CTX-05 | Context layers as an independent map overlay selector | BOTH | `ctx-layer`: None / Population density / Cropland share / Surface water | §5 | IN | |
| CTX-06 | Contextual-only disclaimer + provenance line | BOTH | Built | §5 | IN | |
| CTX-07 | Unavailable overlays omitted, never shown disabled | BOTH | Built; basin/river omitted with a note | §5 | IN | |
| CTX-08 | Basin / sub-basin / river-network overlays | DOC | Not built | §5 | LATER — depends on artifacts we have not built | |
| CTX-09 | Rural-facility counts and rates | DOC | Not built | §5 | OUT | **OUT** |
| CTX-10 | State/UT aggregation rules (sum, recompute shares, not average) | BOTH | Built as specified | §5 | IN | |
| CTX-11 | Coordinate context derived from the containing block | DOC | Not built | §5 | LATER — follows the coordinate path decision (MSC-03) | |

## Batch K — Detailed Analysis transition and return

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| DA-01 | One prominent `Explore in Detailed Analysis` action | BOTH | Stub destination; action + carried state are real | §6 | IN | |
| DA-02 | Transition preserves geography, unit, bundle, scenario, period, driver | BOTH | Built (`S.da` carries it, incl. selected driver) | §6 | IN | |
| DA-03 | Hover / bin filter / tooltip NOT carried across | BOTH | Built | §6 | IN | |
| DA-04 | Canonical route registries; no inferring destinations from labels | DOC | Stub only | §6 | IN — vendor-side requirement | |
| DA-05 | Opens on the composite metric, never `Metric = All` | DOC | Stub only | §6 | IN | |
| DA-06 | Driver click routes to its metric in Detailed Analysis | BOTH | Built (`wireDrivers`) | §7 | IN | |
| DA-07 | Valid-but-unroutable driver stays normally styled | DOC | Not built | §7 | LATER — needs the sector rule signals | |
| DA-08 | `Back to Overview` restores the full Overview context | DOC | One mention, not built | §8 | IN | |
| DA-09 | Detailed Analysis `Refine analysis` collapsed advanced controls | DOC | Out of prototype scope | §7 | IN — but this is the *vendor's existing app*, so mostly KEEP | |

## Batch L — Exports, data states, accessibility, misc

| ID | Item | Tag | Prototype behaviour | Doc position | Rec | Verdict |
|---|---|---|---|---|---|---|
| MSC-01 | `Download answer` / answer pack / copyable answer text | DOC | Not built | §4, §"Overview exports" | LATER — spec'd now, built later (already agreed) | |
| MSC-02 | Export respects active bin filter, retains original ranks | DOC | Not built | §4 | LATER — with MSC-01 | |
| MSC-03 | Coordinate path as `Analyse a custom location` | DOC | Not built | §2 line 667 | IN-BUT-CHANGED — vendor already has it; decide Overview vs Detailed Analysis placement | |
| MSC-04 | `No valid data` state for roster-valid, score-missing units | BOTH | Built | §1, §2 | IN | |
| MSC-05 | Loading state: never show stale values under new labels | DOC | n/a (static) | §2 | IN — vendor-side requirement | |
| MSC-06 | Version-mismatch → generic unavailable state | DOC | n/a | §2 | IN — vendor-side requirement | |
| MSC-07 | Geography fallback chain Block → District → State → India | DOC | Not built | §2 | LATER | |
| MSC-08 | Keyboard operability + non-map alternatives for map actions | DOC | **Dropped** — no `tabindex`, no `keydown` | §1, §10 | IN-BUT-CHANGED — ranking rows already give a non-map path; state the minimum | |
| MSC-09 | Colour never the sole carrier of score/band/selection/quality | BOTH | Band pills + numeric scores throughout | §1 | IN | |
| MSC-10 | Tiny geographies selectable off-map | DOC | Ranking row serves this | §1 | IN — via RNK-05 | |
| MSC-11 | Browser Back excluded from the analytical state model | DOC | n/a | §2, §8 | IN | |

---

## Appendix A — Separated as engineering contract (not triaged as flow)

Moved out of the flow review; these belong in the data/API spec.

- **Roster & denominator contract** — 784 districts / 7,137 blocks, `admin_roster_version`,
  `n_expected` from the roster not from score rows, stable administrative IDs as join keys.
- **Provenance fields** — `ruler_id`, `colour_scale_id`, `data_snapshot_hash`, boundary hashes,
  build identity, manifest checksum.
- **Coverage gate** — `coverage_fraction` as a build gate (<90% fails the build), not a runtime
  suppression path.
- **Artifact field lists** — the national State/UT artifact schema (§9) and the per-unit district
  and block schemas, including the deliberate *omission* of raw values and normalization
  parameters so downstream renormalization is unconstructible.
- **Display precision** — 1 dp for scores, integers for counts/ranks, full precision stored.
- **§10 acceptance-test contract** — to be extended with the live canaries from
  `qa/reports/CURRENT_UX_FLOW.md` §7 (Ladakh uniform fill; Goa no longer 0/100; same score = same
  colour across two States; scores rise early→end under SSP5-8.5).
- **Case-study validation baseline** — must be rebuilt against the frozen ruler before it is cited.

## Appendix B — Counts

| Tag | Items |
|---|---|
| `BOTH` | 58 |
| `DOC` (proposed, unbuilt) | 30 |
| `PROTO` (built, unstated) | 5 |
| `CONFLICT` | 2 |
| **Total** | **95** |

Recommended distribution: **IN 62 · IN-BUT-CHANGED 6 · LATER 12 · OUT 7 · to-decide 2 (DIS-01/02)**.

## Appendix C — Ruling log

| Date | Batch | Ruling |
|---|---|---|
| 2026-09-15 | D | District is a **navigation level** (prototype behaviour). Doc's withdrawal reversed. `District fill` toggle stays OUT. |
| 2026-09-15 | — | Engineering contract (Appendix A) separated from the flow review — approved. |
| 2026-09-15 | cuts | Seven `DOC`-only items cut: ANS-06 prose answer sentence · RNK-09 ranking-as-collapsed · RNK-10 `Where are the hotspots?` · HIS-05 histogram hover-emphasis · DIS-05 `District fill` · RNK-08 tied-rank phrasing · CTX-09 rural-facility context. |
