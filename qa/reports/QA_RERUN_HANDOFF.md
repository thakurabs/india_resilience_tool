# QA RE-RUN HANDOFF — vendor UAT app (dev.resilience.org.in)

> **Purpose.** The vendor has shipped a large batch of new work. This document is a
> self-contained brief so a fresh agent can re-run the full QA pass and diff against
> our prior findings **without re-deriving context**. Read this top-to-bottom, then
> `qa/README.md`, then `qa/reports/VENDOR_REPORT.md` (the authoritative running log).
>
> **Authored:** 2026-07-27 · **Branch:** `add_flood_depth` · **HEAD:** `7fac379`
> **Prior QA HEAD (last qa/ commit):** `8a9c3e0` ("mark M4 resolved").

---

## 0. TL;DR — do this first

1. **The saved 2FA session is STALE** (`qa/.auth/storageState.json`, dated 2026-07-13
   → ~14 days old; expires ~24h). It **will bounce to login**. Re-capture before anything:
   ```bash
   node qa/harness/capture-session.mjs      # opens a real window; log in + 2FA by hand
   node qa/harness/explore.mjs / dashboard-root   # confirm it reaches the dashboard
   ```
   Playwright + chromium are already installed (`node_modules/.bin/playwright` present).
   If chromium complains about OS libs: `sudo node_modules/.bin/playwright install-deps chromium` (one-time).

2. **This is a RE-run against a changed build.** Do not assume prior findings still hold.
   For every finding in §5, the job is: *reproduce → confirm still-open / newly-fixed / regressed*.
   The vendor may also have added features → watch for **new** defects, not just old ones.

3. **The app is a separate third-party reimplementation of IRT** — its frontend is NOT in
   this repo. We only have the black-box harness in `qa/`. Nothing here touches the Streamlit tool.

---

## 1. What this QA is

- **Target:** `https://dev.resilience.org.in` (override via `IRT_QA_URL`). A full vendor
  reimplementation of the India Resilience Tool. 2FA-gated.
- **Basis:** *Resilience Actions User Stories v1.3* — `qa/charters/_source_user_stories_v1.3.txt` (US 01–17).
- **Method (three layers):**
  1. **Harness** (deterministic, no LLM): Playwright scenarios drive each story and
     auto-capture evidence — console errors, failed/HTTP-error requests, multi-viewport
     screenshots, interactive-DOM map, NaN/blank scan, axe-core violations. See
     `qa/harness/lib/evidence.mjs` + `runner.mjs`.
  2. **Reviewer** (cheap model / Haiku subagent): reads one charter's `spec.md` + its
     evidence, returns pass/fail/blocked + observations. Judges artifacts; never free-drives.
  3. **Triage** (Opus): dedupe, drop false positives, assign severity, write `qa/reports/`.
- **Every finding in the report is verified against raw evidence** (axe JSON / network
  results / screenshots) — nothing is model-inferred. Keep that bar on the re-run.

---

## 2. Layout & authoritative files

```
qa/
  README.md                         ← run instructions + env vars (read after this)
  charters/                         ← one folder per US: spec.md (steps+expected) + scenario.mjs
    _source_user_stories_v1.3.txt   ← the spec, verbatim
    us06..us17 + us-crossflow-add-to-analysis
  harness/
    capture-session.mjs             ← (re)capture 2FA session  ★ run first
    explore.mjs                     ← recon / smoke a route
    lib/{flows.mjs, evidence.mjs, runner.mjs, session.mjs}
    add-to-analysis-crossflow.mjs           ← cross-flow + dedup probe
    add-to-analysis-map-interactivity.mjs   ← map dropdown-gating + commutativity probe
    adversarial-upload.mjs / adversarial-formats-upload.mjs  ← US10 garbage-in
    portfolio-detonator.mjs / saved-analysis-probe.mjs       ← injection detonators
    repro-m4.mjs / repro-m4-*                ← M4 toast repro (now superseded; see §5 M4)
  runs/                             ← per-run evidence (GITIGNORED; ~90 dirs already)
  reports/
    VENDOR_REPORT.md                ← ★ AUTHORITATIVE running log (B1/M1–M7/N1–N25)
    SUMMARY.md                      ← early snapshot (US 09/11/13/14 only) — superseded
    us06..us17.md                   ← per-charter triaged reports
    blocked-and-partial-stories.md  ← US 01 / 02–04 / 05 coverage gaps
    UPLOAD_VALIDATION_HANDOFF.md    ← deep US10 upload handoff
    QA_RERUN_HANDOFF.md             ← THIS FILE
  .auth/storageState.json           ← saved login (GITIGNORED, credentials-equivalent)
```

**Read order:** this file → `qa/README.md` → `VENDOR_REPORT.md` → the per-charter report
for whatever you're re-running.

---

## 3. How to run

```bash
# Refresh the session (mandatory — see §0)
node qa/harness/capture-session.mjs

# Run a standard charter
node qa/charters/usNN-<slug>/scenario.mjs

# Special probes (standalone harnesses):
node qa/harness/add-to-analysis-crossflow.mjs
QA_SOFTWARE_GL=1 node qa/harness/add-to-analysis-map-interactivity.mjs   # needs software GL
node qa/harness/adversarial-upload.mjs            # US10 CSV garbage-in
node qa/harness/adversarial-formats-upload.mjs    # US10 XLSX + shapefile
node qa/harness/portfolio-detonator.mjs           # injection sink B (Compare-Portfolio export)
node qa/harness/saved-analysis-probe.mjs          # injection sink A (Save Analysis)
```

**Environment variables:**
| Var | Purpose |
|-----|---------|
| `IRT_QA_URL` | override target (default `https://dev.resilience.org.in`) |
| `QA_SOFTWARE_GL=1` | launch chromium with SwiftShader (`--use-gl=angle --use-angle=swiftshader …`). **Required** for map probes (deck.gl WebGL hit-testing headless). If calibration surfaces nothing even with this, deck.gl needs a real GL context → fall back to headed / `xvfb-run`. |

Evidence lands in `qa/runs/<ISO-ts>_<charter>/`: `results.json`, `*__axe.json`,
`*__dom.json`, screenshots `s*.png`. Machine `automated-summary.md` from probes is NOT a
vendor report — the triaged report is authored by hand.

---

## 4. Auth scope (what the saved session can / can't reach)

- **Covered by saved session:** dashboard stories **US 09–17**, post-login visitor
  stories **US 05–08**, public landing **US 01** (but pre-auth-only elements are
  unverifiable while logged in — see N22).
- **NOT covered (need a real email inbox + disposable account, semi-manual, out of the
  autonomous path):** **US 02** (sign-in/2FA), **US 03** (signup + email verify),
  **US 04** (password reset). Also **US 05** first-visit guide won't re-trigger on an
  already-onboarded account.
- **To close the remaining 5:** provision a disposable email inbox + fresh test account,
  and one logged-out capture for the pre-auth landing (US 01). See `blocked-and-partial-stories.md`.

---

## 5. PRIOR FINDINGS BASELINE — re-verify each against the new build

Legend for the "Re-run action" column: **RE-VERIFY** = confirm still open/fixed;
**REGRESSION-WATCH** = was resolved, make sure it stays fixed; **ASK-PO** = intent
question, unchanged unless spec/behaviour moved.

**Send-status as of last pass — reported to vendor: B1, M3, M6, N5.** Everything else was
HOLD / ASK-PO / DEPRIORITISED and never sent.

### Blocker / Major

| # | Sev | Story | Prior status | Re-run action |
|---|-----|-------|--------------|---------------|
| **B1** | Blocker | US 12/14 | **SENT.** Ranking Table can't load — `/api/api/parquet/ranking` → **HTTP 500**; **doubled `/api/api/`** path. Reproduced state-only + single-district; map works with identical filters. In the later build the call was **`POST`** (originally logged `GET`). | **RE-VERIFY FIRST — highest priority.** Run `us14` + `us12`. If ranking now 200s, verify columns/order/row-select/color coding (previously unreachable). Also re-check the doubled `/api/api/` segment across ALL endpoints — likely the shared root cause of B1+M5. |
| **M3** | Major (a11y) | US 09/11 | **SENT.** Text < WCAG AA contrast; worst = error-red `#E75252` (~3.67:1 on white). Also placeholder text + Reset button. | RE-VERIFY via axe `color-contrast` on us09/us11 runs. Check `#E75252` darkened. |
| **M1** | Major (a11y) | US 09 | HOLD. Collapsed-panel reopen affordance / toggle labelling for AT users (original "no accessible name" was too strong — `aria="Hide sidebar"` exists; issue is the *collapsed→reopen* control). | RE-VERIFY collapsed-state reopen control has a label + keyboard reachability. |
| **M2** | Major (a11y) | US 09/11 | HOLD. Header brand link+icon has **no accessible name** (critical `image-alt`); `<img class="w-6 h-6">` no `alt`, anchor no text/`aria-label`. Present on every screen. | RE-VERIFY axe `image-alt` critical still present. |
| **M4** | Major | US 10 | **RESOLVED (re-verified 2026-07-13).** "Location could not be resolved" toast on a *successful* resolve. No longer reproducible: 15/15 cold loads clean, MutationObserver saw 0/6 node insertions. Original "3/3" was a false positive from an `innerText` scrape in `repro-m4.mjs`. | **REGRESSION-WATCH.** Do a couple of cold-load Show-on-Map resolves; confirm no error toast + resolve 200. Don't trust `repro-m4.mjs`'s innerText heuristic — use the DOM-node/MutationObserver method. |
| **M5** | Major (backend) | US 16 | **DEPRIORITISED.** Composite-metric Resilience Profile fires `POST /api/api/parquet/trend` + `/scenario-comparison` → **HTTP 500 ×2**. UI degrades to "No data available" (no crash). Same doubled-`/api/api/` family as B1. Climate metrics hit same endpoints → 200. | RE-VERIFY alongside B1 — likely one shared root cause. Composite w/ no time series should return 200-empty or 404, not 500. |
| **M6** | Major (data) | US 17 | **SENT.** Portfolio count banner reads "You have added 1 district" while Manage Portfolio holds 2 (Warangal+Karimnagar). Reflects last-add, not total; also not pluralised. | RE-VERIFY via `us17` S3. |
| **M7** | Major (functional/data) | US 10 | HOLD. **No value/geographic validation on coordinate upload** — out-of-range, non-numeric, empty, out-of-India, 5000-char name all accepted (CSV==XLSX==shapefile). Only client-side structural gating exists. | RE-VERIFY via `adversarial-upload.mjs` + `adversarial-formats-upload.mjs`. High-value: this is a real data-integrity gap. |

### Cross-flow findings (uncommitted-era, from crossflow + map-interactivity probes)

| # | Sev | Prior status | Re-run action |
|---|-----|--------------|---------------|
| **C4** | Major | Same coordinate uploaded with a *different name* → **2 portfolio rows** (dedup defeated by name). Repro: `dup_diff_name.csv`. | RE-VERIFY via `add-to-analysis-crossflow.mjs`. |
| **B4** | Major / ASK-PO | Portfolio does **not accumulate across modes** — switching Administrative↔Coordinate discards prior adds (final 1 vs expected 6). Unclear if single-mode is intended. **Gates C2/C5/C6 semantic retest.** | ASK-PO whether single-mode intended; if the vendor changed this, C2/C5/C6 become testable. |
| **C1** | PASS | Admin same-district re-add rejected + "already in your portfolio." flag. | REGRESSION-WATCH. |
| **Map Claim 1** | CONFIRMED | Dropdown district pick gates the map (points outside selection go inert), interactivity-only, reversible, no view change. | RE-VERIFY via map-interactivity probe (`QA_SOFTWARE_GL=1`). Vendor's "Deliverable B" fix = decouple map `pickable` from dropdown state → additive peers, order-independent portfolio. Check if implemented. |
| **Map Claim 2** | CONFIRMED | Non-commutative: map-first=6 vs dropdown-first=3 (root cause = Claim 1 gating). | RE-VERIFY; should be fixed if Deliverable B landed. |

### Minor / Cosmetic / ASK-PO (full detail in VENDOR_REPORT.md)

- **N1** a11y US11 — MapLibre attribution link colour-only (third-party). HOLD.
- **N2** a11y US09 — no `<main>` landmark; 9 blocks outside landmarks.
- **N3** data US13 — map tooltip omits "Baseline (1990–2010)" + "Δ / Level of Change". ASK-PO.
- **N4** cosmetic US14 — internal slugs leak in ranking caption.
- **N5** functional/a11y US10 — **SENT.** Invalid coord fields go red-border only; no error text, no `aria-invalid` (colour-only). RE-VERIFY.
- **N6** doc US10 — sample schema `id,custom_name,lat,long` vs spec's `Latitude,Longitude,Label`. ASK-PO.
- **N7** cosmetic US10 — uploaded-coords list omits District.
- **N8** functional US15 — spec's auto "Save/Don't Save" popup on context change not observed. ASK-PO.
- **N9** doc US15 — saved-item tag taxonomy drift ("Single District" not in spec set; older items untagged). ASK-PO.
- **N10** a11y US16 — `nested-interactive` (serious) in profile accordion header.
- **N11** functional US16 — no "Show percentile band (p05–p95)" control (spec 872). ASK-PO.
- **N12** data US16 — "Position in State" vs spec 861 "Position in India". ASK-PO.
- **N13** data US17 — comparison Table omits "Position in State" (spec 980). ASK-PO.
- **N14** a11y US17 — comparison table/heatmap scroll container not keyboard-focusable (serious).
- **N15/N16/N17/N18** functional US17 — missing "Refine your filters" section / auto-metrics note / Advanced-Metric checkbox / Scenario Single-Compare modes (spec 897–972). All ASK-PO (functionally equivalent alternatives exist).
- **N19** data US07 — Profile omits "State" field (spec 280/289). ASK-PO.
- **N20** cosmetic US07 — save button "Update" vs spec "Save". ASK-PO.
- **N21** functional US08 — timed mid-session feedback auto-popup (unprompted; not in spec). ASK-PO.
- **N22** doc US01 — logged-in header shows no Donate button / Resustainability logo (spec 46–49); may be pre-auth only (unverifiable while logged in).
- **N23** security US10 — **formula/CSV injection (CWE-1236) stored verbatim** in labels + saved-analysis name. **LATENT** — both download sinks proven clean (Compare-Portfolio export = district names only; Save Analysis actions = Rename/Delete, no export). No XSS. Fix as defense-in-depth. **RE-CHECK any NEW export path** the vendor may have added — that would make it live.
- **N24** cosmetic US10 — shapefile silently drops `.dbf` custom_name → "Point N".
- **N25** data US10/17 — out-of-pilot-state uploaded site silently dropped from Compare Portfolio (3 of 4 exported).

### What PASSED cleanly last pass (regression-watch, don't re-file unless broken)

- **US 09** Geography: state→district load, single/multi-site, Block switch, Reset, sidebar collapse/expand with selection retained, Map↔Ranking preserves geography.
- **US 11** Filters: cascade gating, all filters apply, map updates.
- **US 12** View Mode: Map↔Ranking mutual exclusivity + geography/filters preserved (only issue = B1 ranking data).
- **US 13** Map: region highlight + value fill + legend + click tooltip + zoom.
- **US 15** My Analysis: full Save→list→Reload loop (201 save, 409 dup guard, blank⇒default, search, Rename/Delete, faithful reload). Doubled `/api/api/` present but all 2xx here.
- **US 16** single-site Resilience Profile: Overview / Risk Summary / Trend / Scenario Comparison charts, full-screen modal, 375px mobile — all clean on **climate** metrics (M5 is composite-only).
- **US 17** multi-site portfolio: Add→portfolio, Manage (Remove/Clear), Compare (Table + Heatmap, 200s), left/right modal split, 375px clean (closes US15 mobile caveat).
- **US 06/07/08** header nav / profile / feedback — passing (read-only, session-safe; Submit/Logout deliberately not clicked).

---

## 6. HARNESS GOTCHAS (hard-won — read before editing scenarios)

1. **`applyCoreFilters` (`flows.mjs`) is NON-idempotent.** Resilience filters persist per
   session — apply ONCE. Re-running 30s-hangs (no unset "Select" trigger). Never call it
   in a normalization step. Gate via `ensureAddReady` (quick-wait Add-enabled; cascade
   only if unset triggers exist).
2. **Count portfolio via the banner** "You have added N …" (authoritative), not a roster
   scrape. Do NOT clone us17's `managePortfolioNames` (it uses a Telangana whitelist Set).
3. **Mode switch (admin↔coord) raises a `data-modal-root` "Switch to…?" confirm** that
   intercepts every later click — must click its proceed button. It ALSO clears the
   geography selection → must **re-select State** after, or the District picker stays disabled.
4. **The auto "HELP US IMPROVE" survey** and the Clear/Switch confirms are BOTH
   `data-modal-root` `fixed inset-0 bg-black/35` backdrops — discriminate by content:
   remove backdrops WITHOUT action keywords (confirm/cancel/clear/portfolio), keep the
   rest. Installed as in-page interval + MutationObserver via `addInitScript` (no eval — CSP).
5. **Manual coordinate flow:** fill lat/lon/name → "Show on Map" → **"Add Coordinate"
   (singular)** to STAGE the point (NOT "Use Coordinates"). Only then does Add-to-Analysis
   enable after filters.
6. **Map-click floating box does NOT render under headless WebGL** without `QA_SOFTWARE_GL=1`.
   Even then deck.gl may need a real GL context → headed / `xvfb-run` fallback.
7. **Map info surface fields** are `District / State / Composite Score / Rank in state`
   (NOT Baseline/Position/Value). Hover tooltip is `pointer-events-none` (don't filter it
   out); the CLICK tooltip carries the "Add to Analysis" CTA.
8. **Telangana fills only ~26% of the canvas** → coarse-sweep then focus on the live bbox
   (`deriveLiveBBox`) or live-point counts undercount.
9. **`repro-m4.mjs` is unsound** — it decides "toast fired" by regex-scraping
   `document.body.innerText`, producing false positives. Use a MutationObserver on `<body>`
   armed before the click for any toast-presence question.
10. **Pilot state = Telangana.** Most fixtures/whitelists assume it. Out-of-state sites
    (e.g. Vijayawada/AP) get silently dropped downstream (N25).

---

## 7. RECOMMENDED RE-RUN ORDER

1. **Session:** `capture-session.mjs` → `explore.mjs` smoke. (§0)
2. **B1 + M5 (the 500 family):** run `us14`, `us12`, `us16` (composite path). If the
   doubled-`/api/api/` root cause was fixed, this unblocks the most. Verify ranking table
   internals that were previously unreachable.
3. **SENT findings** (M3, M6, N5) — confirm the vendor's fixes actually landed.
4. **Held Majors:** M1, M2 (a11y via axe), M7 + C4 + B4 (upload/cross-flow probes).
5. **Map interactivity** (`QA_SOFTWARE_GL=1`) — check if "Deliverable B" (decouple map
   pickable from dropdown) landed → Claims 1 & 2 should resolve.
6. **Full charter sweep** US 06–17 for regressions + NEW defects the batch introduced.
7. **Security re-check:** N23 — scan for any NEW export path that would make injection live.
8. **ASK-PO batch:** compile the intent questions (N3/N6/N8/N9/N11–N22) for the PO if not
   yet answered.
9. **Coverage gaps:** if an email inbox + fresh account are provisioned, close US 01–05.
10. **Triage → update `VENDOR_REPORT.md`** with per-finding verdicts (fixed / still-open /
    regressed / new). Keep the "verified against raw evidence" bar. Decide send-batch with the user.

---

## 8. STATE / LEDGER NOTES

- **`qa/` working tree is CLEAN** — everything is committed (last qa commit `8a9c3e0`).
  The dirty files in `git status` are unrelated (docs / scratch), not QA.
- **Reports NOT yet sent to vendor:** only B1, M3, M6, N5 were sent. All other findings are
  HOLD/ASK-PO/DEPRIORITISED pending a send decision from the user.
- **`runs/` is gitignored** (~90 evidence dirs already present from prior passes; safe to
  accumulate more). `.auth/` is gitignored (credentials-equivalent).
- **Per CLAUDE.md approval gate:** re-running scenarios drives a live external app and
  captures evidence — treat a full re-run as needing `APPROVED: RUN TESTS` from the user
  before executing. This handoff is read-only preparation.

---

## 9. OPEN QUESTIONS TO RESOLVE WITH THE USER BEFORE / DURING RE-RUN

1. **Is there a changelog / list of what the vendor changed?** Targeting known-changed
   areas first is far cheaper than a blind full sweep.
2. **Send decision:** which of the held findings do we now send, and do we re-send fixed
   ones as "verified fixed"?
3. **Email inbox + fresh account** for US 02–05 — provision now, or keep out of scope?
4. **PO answers** to the N-series intent questions — got any back since last pass?
