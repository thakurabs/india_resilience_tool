# US 15: My Analysis (Save List & Reload Analysis)

Source: `_source_user_stories_v1.3.txt` lines 772–837
Scope: functional · data · a11y

## Preconditions
- Logged-in session active (reused via saved storageState).
- Visitor is on the dashboard; the left Selection Panel is visible.
- An analysis can be built: State + District (view zone) + core Resilience
  filters (Risk Domain, Metric, Scenario, Period).

## Discovered structure (recon `2026-07-08T09-3x..09-44` rounds 1–5)
- **Save control:** `Save Analysis` button in the Administrative Panel. Disabled
  until a location + filters exist; enabled after an analysis is built.
- **Save modal:** title **"Save Analysis"**, label **"Analysis Name"**, one text
  input (placeholder `My Analysis`), buttons **Cancel** / **Save Analysis**, an X
  close. Blank name = default label ("My Analysis"). No separate "custom vs
  default" toggle — the single blank-or-filled input covers both spec options.
- **Save API:** `POST /api/api/saved-analyses` → **201** on success (toast
  *"Analysis saved successfully."*); **409** on duplicate name (toast *"An
  analysis with this name already exists"*). Note the doubled `/api/api/` path
  (same quirk as the ranking-500 blocker).
- **Server-side name decoration:** the stored name appends ` - <Tag> - <Date>`,
  e.g. `QA Reload 09:39:11 - Single District - 08 Jul, 2026`. A single district
  gets tag **"Single District"** — *not* in the spec's enumerated tag set
  (Multi-District / Multi-Block / Multi-Coordinates).
- **Listing:** NOT a top-left nav dropdown (spec 815). It is reached via the
  top-**right** **"Welcome, <name>"** header button → dropdown
  (**User Profile / My Analysis / Logout**) → **My Analysis** → route
  **`/my-analysis`**. Position drift vs spec → SPEC-DRIFT, not FAIL.
- **List route `/my-analysis`:** breadcrumb "Dashboard / My Analysis", heading
  "My Analysis", a **"Search Analysis"** input, and saved-item rows. Each row:
  `<label> - [tag] - <date>` + a 3-dot menu (aria-label `Open menu`). GET is
  paginated: `GET /api/api/saved-analyses?page=1&limit=5`.
- **Reload:** clicking a saved row restores the dashboard to `/` and should
  re-apply state + district + filters. (Recon reload was inconclusive — an
  over-broad click dropped a stray map pin; the scenario clicks the label text
  precisely.)

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| 1 | On a clean dashboard, inspect **Save Analysis** | Disabled with no location + no filters |
| 2 | Build analysis: State=Telangana, District=Warangal, apply core filters | Map renders; **Save Analysis** becomes enabled |
| 3 | Click **Save Analysis** | Modal opens: title "Save Analysis", "Analysis Name" input (placeholder "My Analysis"), Cancel + Save Analysis buttons |
| 4 | Enter a **unique** custom name, click **Save Analysis** | Toast **"Analysis saved successfully."**; `POST /api/api/saved-analyses` → **201**; stored name decorated with ` - <Tag> - <Date>` |
| 5 | Re-open Save, enter the **same** name, click Save | Graceful duplicate guard: toast **"An analysis with this name already exists"**; POST → **409**; no crash |
| 6 | Save modal: leave name **blank**, Save | Saves with default label "My Analysis" (or 409 if a default already exists) — blank ⇒ default, per spec |
| 7 | Open **Welcome > My Analysis** | Dropdown shows User Profile / My Analysis / Logout; clicking My Analysis routes to `/my-analysis` with heading + Search box |
| 8 | Inspect the saved-item list | Newest item present; each row shows label + date (+ tag when set); 3-dot menu present. Record whether tag matches spec set |
| 9 | **Reload:** click the just-saved row | Dashboard restores: State=Telangana, District=Warangal, filters (Heat Risk Composite / SSP2-4.5 / 2020-2040 / Mean); map legend reflects them; no error |
| 10 | Open a row's **3-dot** menu | Rename / Delete options present (per US 17 §922–924) |
| 11 | Type in **Search Analysis** | List filters to matching saved items |
| 12 | **Auto-trigger save:** with an *unsaved* built analysis, change State | Per spec 794–812, a "Save / Don't Save" popup should appear. Capture whether it does (may be gated) |

## Cross-cutting checks
- No `console.error` / `pageerror` / `requestfailed` / `httperror` (≥400) except
  the deliberately-provoked **409** in step 5 (expected, benign).
- Watch for `500` on `POST /api/api/parquet/composite-map-data` during reload
  (seen once in recon) — flag if reproduced on a clean reload.
- No `NaN` / blank where a saved label, tag, or date is expected.
- Layout intact at desktop / tablet / mobile; list usable at 375px.
- No serious/critical axe violations on the list route or save modal.
- Validation/toast copy should match the spec strings; wording drift → SPEC-DRIFT.

## Known caveats (flag as SPEC-DRIFT / PASS-WITH-NOTE, not FAIL)
- Listing lives top-**right** under "Welcome", not a top-left dropdown (spec 815).
- Tag taxonomy: app emits "Single District"; spec enumerates only Multi-* tags.
  Older saved items carry no tag at all.
- The auto-trigger save popup (step 12) may be gated on genuine unsaved state;
  absence is not automatically a defect — record as PASS-WITH-NOTE.
- Recon created persisted saved analyses on the dev server (`QA Reload …`,
  `QA US15 …`); this is expected QA test data, not app-generated.
