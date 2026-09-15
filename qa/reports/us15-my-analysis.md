# QA Report — US 15: My Analysis (Save List & Reload)  ✅ PASSING

**Run:** `qa/runs/2026-07-08T09-53-13-915Z_us15-my-analysis/`
**Result:** **PASS** — full Save → List → Reload loop works end-to-end
(12 steps, 0 failures). Two Minor spec/doc drifts (N8, N9) + one informational
position drift; corroborates existing a11y findings M2/M3/N1.

## What works (verified)
- **Save gating:** *Save Analysis* disabled with no location/filters; enabled after
  an analysis (State + District + core filters) is built.
- **Save modal:** "Save Analysis" title, "Analysis Name" input (placeholder
  "My Analysis"), Cancel + Save Analysis. Blank name ⇒ default label.
- **Save API:** unique name → `POST /api/api/saved-analyses` **201** + toast
  *"Analysis saved successfully."*; duplicate name → **409** with correct red toast
  *"An analysis with this name already exists"* (graceful, no crash); blank/default
  when default exists → 409 (confirms blank ⇒ default label).
- **Listing:** *Welcome > My Analysis* → route **`/my-analysis`** (breadcrumb, heading,
  Search Analysis box). Rows: label + date (+ tag when set) + **⋮ Actions** menu.
- **Row actions:** ⋮ → **Rename** / **Delete** (matches US 17 §922–924).
- **Reload:** clicking a saved row restores State=Telangana, District=Warangal,
  filters (Heat Risk Composite / SSP2-4.5 / 2020-2040 / Mean); `composite-map-data`
  → 200; map legend reflects the config. **Verified visually** (`s9-reloaded.png`).
- **Search:** *Search Analysis* filters the list (`GET …?search=…`).

## Findings
- **N8 (Minor, functional, ASK-PO):** auto-trigger "Save / Don't Save" popup on
  context change (spec §794–812) **not observed** when changing State with a freshly
  built (unsaved) analysis. Unverifiable from evidence whether missing or gated on a
  stricter dirty-state precondition (e.g. requires *Add to Analysis* first). Manual
  Save is unaffected. Evidence: `results.json` S12, `s12-context-change.png`.
- **N9 (Minor, data/doc, ASK-PO):** saved-item **tag** drift — app emits "Single
  District" (outside spec's Multi-* set); older items untagged. Evidence:
  `s7-my-analysis-route.png`, POST body `name`.
- **INFO (spec-drift):** the saved list is reached top-**right** via *Welcome > My
  Analysis* (`/my-analysis`), not the top-left dropdown spec line 815 describes.
  Feature works fully; only the entry point differs.

## Notes
- The doubled **`/api/api/`** path (seen on the B1 ranking 500) is also present on
  `saved-analyses` and `composite-map-data`, but every call here returns 2xx — so the
  doubled segment alone isn't fatal; B1 is a ranking-endpoint-specific failure.
- The scenario intentionally provokes two `409`s (S5 duplicate, S6 default) — the only
  "error events" in the run; both are expected duplicate-name guards, not defects.
- **Coverage caveat:** responsive (375px) screenshots captured the dashboard, not
  `/my-analysis` — mobile layout of the saved list is unverified (re-check with US 16/17).
- Independent evidence review (read-only, over `spec.md` + run artifacts) corroborated
  S1–S11 as genuine PASS, confirmed the S9 reload visually, and flagged the S12
  ambiguity + the mobile-coverage gap captured above.

## Test data created (dev server)
Saving is persistent server-side; recon + scenario created saved analyses named
`QA Reload …` and `QA US15 …` under this account. Expected QA test data — can be
removed via the ⋮ → Delete action if cleanup is desired.
