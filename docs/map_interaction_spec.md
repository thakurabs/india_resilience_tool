# Map Interaction Spec — Add-to-Analysis selection model

**Status:** Target design (vendor-buildable, probe-verifiable)
**Applies to:** `dev.resilience.org.in` map / Add-to-Analysis flow (vendor frontend; source not in this repo)
**Origin:** Remediation of `qa/reports/us-map-interactivity.md` (Claim 1 gating, Claim 2 non-commutative portfolio)
**Verified against:** `qa/harness/add-to-analysis-map-interactivity.mjs`

---

## 1. Problem this spec fixes

The current app collapses three distinct concepts into the district dropdown and, as a side
effect, lets one of them switch off another input surface:

| Concept | What it is | Persistent? | Order-independent? |
|---|---|---|---|
| **A. Portfolio membership** | the *set* of units being analyzed | Yes | Yes (set union) |
| **B. Camera / view** | zoom, pan, what is on screen | No — ephemeral | N/A |
| **C. Focus / highlight** | visual emphasis, what the reading panel shows | No — ephemeral | N/A |

Today the dropdown does **A + C**, and the way it does C **disables the map as an input surface
for every other unit**. That single overload is the root of both reported defects:

- **Claim 1** — choosing a district in the dropdown gates map interactivity to only that
  district (reversible, no view change): confirmed, `outsideSelectedLive = 0`.
- **Claim 2** — the resulting portfolio is order-dependent (map-first = 6, dropdown-first = 3),
  with a silent cap and no error.

This spec separates A, B, and C and makes all selection surfaces equal peers.

---

## 2. Concepts (system state)

- **Marked set `M`** — units currently checked/highlighted but **not yet committed**. The
  "focus" layer.
- **Committed portfolio `P`** — units locked in via **Add to analysis**.
- **Active level `L`** — `district` or `block`; exactly one at a time.
- **Camera `C`** — pure view state; never affects `M` or `P`.
- **Analysis route** — **derived** from `|committed set|`, not a mode the user toggles.

---

## 3. Invariant principles

1. **Peers, not gates.** Map click, district dropdown, and coordinate entry are three input
   methods to the *same* marked set. **None may disable another.**
2. **Selection is additive and order-independent.** Any mark is a set union; any unmark is a set
   difference. The final set is identical regardless of path or order.
3. **View ≠ selection.** Moving the camera never adds, removes, or de-activates a unit. Marking a
   unit never *forces* a level change (it may move the camera; see §6).
4. **Every change is visible and reversible.** Each mark/unmark shows a map highlight **and** a
   removable chip. **No silent caps or silent drops — ever.**
5. **Hover and click are consistent everywhere.** Hover = inspect (tooltip preview); click =
   toggle membership. These semantics do not change based on dropdown state.

---

## 4. Core flow — two steps, three peer surfaces

### Step 1 — Mark (reversible, no commitment)
A unit enters `M` via **any** of three equal surfaces:

- **Map click** on the choropleth, or
- **Dropdown checkbox**, or
- **Coordinate entry**.

Marking a unit:

- toggles its checkbox **on both surfaces in sync** (check in dropdown ⇒ highlighted on map, and
  vice-versa),
- **auto-zooms the camera to fit the current marked set** (see §6),
- **never changes the pickability of any other unit** — the map stays fully live everywhere at
  level `L`.

Unmarking (uncheck in dropdown, or click a marked unit on the map) removes it from `M` and its
highlight — synced across surfaces.

### Step 2 — Commit
**Add to analysis** moves `M` → `P` and **routes by count**:

| Committed count | Route |
|---|---|
| **1 unit** (district *or* block) | **Resilience Profile** — deep single-unit analysis |
| **2+ units** | **Compare Portfolio (My Analysis)** — portfolio build |

The route is a *consequence* of how many units were added — the user never toggles a mode.

---

## 5. Granularity rule — one level at a time

- `P` and `M` are **homogeneous**: all districts *or* all blocks — never mixed.
- Seeing/selecting blocks is an **explicit level switch** (`district → block`), distinct from
  auto-zoom.
- If a level switch would discard a non-empty `M`/`P`, **warn explicitly** before clearing:
  *"Switching to block level clears your current district selection. Continue?"* — never a silent
  wipe.
- **Zoom ≠ drill.** Framing a district keeps `L = district` and all other districts clickable at
  the map edges.

---

## 6. Camera rule — auto-zoom to selection

"Selection" = the **current marked set `M`**, so auto-zoom is defined as **fit-to-`M`**:

- 1 marked unit → zoom/frame that unit.
- N marked units → zoom to the **bounding box of all N**.

This keeps the camera sane while building a multi-district portfolio (it widens to keep
everything framed instead of hopping to each latest click). A **Reset view / Fit all**
affordance is always present. **Auto-zoom is camera-only — it does not change `L`.**

---

## 7. Surface-sync & feedback rules

- Map highlight ⇄ dropdown checkbox ⇄ portfolio chip list are **one shared state**, always
  consistent.
- Every marked unit shows a **removable chip**; every add/remove is visibly reflected on the map.
- **No silent failures.** If an action cannot apply (e.g., marking a block while `L = district`),
  show a message — never drop it quietly.

---

## 8. Acceptance criteria (vendor-buildable, probe-verifiable)

1. Marking district X on the map leaves districts Y, Z fully hoverable/clickable regardless of
   dropdown checkboxes. *(Claim-1 probe → `outsideSelectedLive` stays > 0.)*
2. The same 6 targets marked map-first vs dropdown-first ⇒ identical `P` of 6. *(Claim-2 probe →
   both = 6.)*
3. Committing 1 unit lands on Resilience Profile; committing ≥ 2 lands on My Analysis.
4. Auto-zoom fits `M`'s bounding box and never changes `L`; Reset view restores.
5. Attempting a level switch with non-empty `M`/`P` shows the warning and only clears on confirm.
6. Any un-appliable mark shows a message; nothing is dropped silently.

---

## 9. Resolved micro-decisions (defaults; revisit if needed)

- **Auto-zoom target = fit the whole marked set** (not the single latest click), to keep
  multi-select sane.
- **Level switch = warn-then-clear** (homogeneity enforced at the moment of switch).

---

## 10. Traceability

- Findings: `qa/reports/us-map-interactivity.md`
- Probe / regression harness: `qa/harness/add-to-analysis-map-interactivity.mjs`
- Run of record: `qa/runs/2026-07-11T06-21-13-788Z_us-map-interactivity/`
