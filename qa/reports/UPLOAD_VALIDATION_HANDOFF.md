# PERFECT HANDOFF POINT — US 10 Upload-Coordinates validation testing

_Last updated: 2026-07-09 (branch `add_flood_depth`). **All 5 approved steps COMPLETE.**_

## Objective / task context
Adversarial ("garbage in") testing of the **Upload Coordinates** sub-flow of the
Coordinate Panel on live `https://dev.resilience.org.in`, across **CSV, XLSX, and
shapefile-ZIP** — answering "is upload validation working?" plus confirming whether a
**formula-injection** payload can reach a **downloaded Excel report** (the live
"detonator"), and folding findings into the vendor ledger + a vendor-friendly package.

## Working Snapshot
- `GIT: add_flood_depth@2efb4db` (a concurrent unrelated commit "Restore math overlays
  in docs lightbox" landed mid-session; base was `2d3f774`).
- Working tree: **dirty**. Tests: none (live UI/QA — CLAUDE.md §3 UI tier; verification
  = live Playwright runs + xlsx cell inspection).

## STATUS: DONE
All five approved steps are complete and applied (uncommitted):
1. ✅ Vendor zip + README extended to xlsx/shapefile + cross-format matrix (CHG-0221).
2. ✅ Out-of-India shapefile built (`z07`) and run → **ACCEPTED**.
3. ✅ Findings folded into `qa/reports/VENDOR_REPORT.md` — **M7, N23, N24, N25** (CHG-0220).
4. ✅ Investigation A (Save Analysis → My Analysis export sink) — **no sink**.
5. ✅ Investigation B (Compare Portfolio → Download Reports Excel) — **payload absent**.

## FINAL FINDINGS (verified)

### M7 — No value/geographic validation on upload (Major, all formats)
Out-of-range, non-numeric, empty, out-of-India, 5000-char names, malformed CSV all
**accepted/resolved/plotted** — identical on CSV, XLSX, and shapefile (`z07` out-of-India
accepted). Every upload = HTTP 200; only structural/type/size gating exists (client-side).

### N23 — Formula injection (CWE-1236) stored verbatim, **LATENT** (Minor/security)
`=cmd…`/`@SUM…`/`+`/`-` in `custom_name` stored unsanitised (CSV+XLSX) in the coord list,
Manage Portfolio, **and the auto-defaulted saved-analysis name**
(`=cmd|' /C calc'!A1 - Nampally - Hyderabad - Multi Coordinate`, persisted server-side).
**Both detonators proven clean:**
- **A:** saved-analysis per-item Actions = **Rename / Delete only** — no export/share/download.
- **B:** `portfolio-comparison-table-*.xlsx` stores **resolved district names only**
  (sheet cells `<v>Nampally</v>`, `<v>Ibrahimpatnam</v>`, `<v>Serilingampally</v>`) — the
  user `custom_name` is discarded → payload never enters the workbook.
No live Excel-formula sink. Fix as defense-in-depth (guard leading `= + - @` on import
and prefix `'` on any cell export). **No XSS** (`<script>` name HTML-escaped).

### N24 — Shapefile silently drops `.dbf` custom_name → "Point N" (Cosmetic)
### N25 — Out-of-pilot-state site (Vijayawada/AP) silently dropped from Compare Portfolio (Minor)
3 of 4 sites exported; the AP site vanishes with no warning.

### Reassuring (no defect)
No XSS; 1 MB size limit enforced; empty/header-only/binary-as-csv, `.shp`-missing zip and
`.dbf`-only zip all correctly rejected; documented-schema headers rejected (existing N6).

## The detonator flow (hard-won — reuse via `portfolio-detonator.mjs`)
upload v05 → open **Select Resilience Filters** → cascade **Risk Domain → Metric →
Scenario → Period → Statistic** (each `button[aria-label="X"]` + first `[role=option]`) —
this resolves the points and **ENABLES Add to Analysis** → click it (toast "N districts
added", My Analysis panel auto-opens) → **Open My Analysis in full screen** modal
(`[data-modal-root="true"]`) → Compare Portfolio: pick a metric (renders as
`<label>+<input type=checkbox>`; the option panel **stays open and overlays the tab row** →
switch to the **Download Reports** tab via `dispatchEvent('click')`, not a normal click) →
tick Scenario + Period → **Download Table / Download All** buttons emit the xlsx + heatmap.

## Files (what will / won't commit)
**Staged (commit as-is):** `qa/charters/.../adversarial_formats/_gen_formats.py`,
`qa/harness/adversarial-formats-upload.mjs`, `qa/harness/portfolio-detonator.mjs`,
`qa/harness/saved-analysis-probe.mjs`, `qa/reports/VENDOR_REPORT.md`.

**Untracked, addable normally (xlsx/csv):** `qa/harness/adversarial-upload.mjs`;
`.../fixtures/adversarial/` (v01–v08, f01–f07); `.../adversarial_formats/x01–x05.xlsx`.

**⚠ GITIGNORED — `.gitignore:72 *.zip`:** the vendor deliverable
`wrongly_accepted_upload_fixtures.zip` (repo root, 15 files) **and every shapefile fixture
`z01–z07*.zip`** are ignored. They exist on disk but need **`git add -f`** to be tracked,
or they stay local-only. `f01_over1mb.csv` (1.6 MB) is a large file — decide before committing.

**Evidence (gitignored `qa/runs/`, ephemeral — copy out before any clean):**
`portfolio-detonator/` (report xlsx + "payload absent" scan), `saved-analysis-probe/`
(`A1-save-dialog.png` default-name=payload, `A4-actions-menu.png` Rename/Delete),
`…_us10-adversarial-formats/` (xlsx+shapefile matrix incl. z07).

## Resume gate (auth)
`qa/.auth/storageState.json` 2FA session expires ~24h → first thing next session:
`node qa/harness/capture-session.mjs`, then verify (Coordinate Panel button present, URL ≠ /login).

## Open decisions (for the user)
1. **Send decision** for the new items — M7/N23/N24/N25 are currently `HOLD`/`ASK-PO`;
   pick which to queue `SEND` vs `ASK-PO` to the vendor.
2. **Commit scope** — commit the 5 staged files? `git add -f` the shapefile fixtures +
   vendor zip (else they're local-only)? Track or gitignore `f01_over1mb.csv`?
3. Whether to send the vendor package (`wrongly_accepted_upload_fixtures.zip`) now.
