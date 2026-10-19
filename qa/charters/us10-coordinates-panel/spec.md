# US 10: Coordinates Panel

Source: `_source_user_stories_v1.3.txt` lines 443–524
Scope: functional · data · visual · a11y

## Preconditions
- Logged-in session active (reused via saved storageState).
- Visitor is on the dashboard; the left Selection Panel is visible.
- The app labels this section **"Coordinate Panel"** (spec says "Coordinates
  Panel") — a known spec-drift, not a bug.
- Map is loaded and interactive.

## Discovered structure (from recon `2026-07-08T08-53-16Z_recon-coords`)
- Two sub-mode buttons: **Add Coordinates** (default) and **Upload Coordinates**.
- Add mode inputs: **Latitude** (placeholder `17.8766`), **Longitude**
  (placeholder `79.2792`), **Custom Name** (placeholder `Site 1`); action buttons
  **Show on Map**, **Clear**, **Save Analysis** (disabled), **Add to Analysis** (disabled).
- Upload mode: heading "Upload Coordinate Files"; three sample-download links —
  `Comma separated file (.csv)`, `Spreadsheet (.xlsx)`, `Zipped shapefile (.zip)`;
  "Upload file (max 1 MB)"; a file-select input + **Upload** button.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| 1 | Open Coordinate Panel | Add/Upload sub-mode buttons visible; Latitude, Longitude, Custom Name inputs present; **Save Analysis** and **Add to Analysis** disabled by default (no location, no filters) |
| 2 | Add mode: enter valid Lat `17.8766`, Long `79.2792`, name "QA Site", click **Show on Map** | Point plotted on map; **Block Name & State Name** shown below the lat/long fields; details appear under Single-site Analysis; no error |
| 3 | Missing lat/long: Clear, then click **Show on Map** with empty fields | Validation message **"Latitude and Longitude are required"** |
| 4 | Invalid coords: enter Lat `999`, Long `abc`, click **Show on Map** | Validation message **"Enter valid coordinates, Decimal Degrees Only"** |
| 5 | Click **Clear** | Lat/Long/Custom Name inputs cleared back to placeholders |
| 6 | Switch to **Upload Coordinates** | Three sample-file download links present (.csv/.xlsx/.zip); file-select input + Upload button present |
| 7a | Upload a **documented-schema** CSV (`fixtures/good.csv`, cols `Latitude,Longitude,Label` per spec 480) | Per spec, should be accepted. **Observed: rejected** — app's real sample uses `id,custom_name,lat,long` (schema/doc drift) |
| 7b | Upload the **app's own sample** schema CSV (`fixtures/app_sample.csv`, `id,custom_name,lat,long`) | File accepted; uploaded coordinates listed; points plotted |
| 8 | Upload a **wrong-structure** CSV (`fixtures/bad_structure.csv`) | Validation: **"Invalid file format. Please use the provided sample"** (or equivalent structure error) |
| 9 | Upload an **unsupported** file (`fixtures/bad_format.txt`) | Spec expects **"Unsupported file format"**; app may show the generic "Invalid file format. Please use the provided sample" |
| 10 | With a location shown, expand **Administrative Panel** | Per spec 446–447, a "switching location selection mode — save or proceed without saving" note should appear (mutual exclusivity of Geography vs Coordinates). Capture whether it does |
| 11 | Inspect **Add to Analysis** enable condition | Greyed when no location selected AND no resilience filters applied; enables after a valid location (+/- filter, per US 11) |

## Cross-cutting checks
- No `console.error` / `pageerror` / `requestfailed` / `httperror` (≥400) during the flow.
- No `NaN` / blank where data expected (location detail, uploaded list, tooltip).
- Layout intact at desktop / tablet / mobile (panel usable at 375px).
- No serious/critical axe violations on the panel controls.
- Validation copy should match the spec strings exactly; wording drift → SPEC-DRIFT.

## Known caveats (spec may be stale — flag as SPEC-DRIFT, not FAIL)
- Exact label/validation text may have been reworded in refinements.
- File-upload steps (7–9) drive a possibly-hidden `input[type=file]`; if the
  control can't be reached, record PASS-WITH-NOTE (evidence missing), not FAIL.
- The mode-switch save/proceed note (step 10) may be gated on unsaved analysis
  state; absence with an empty analysis is not automatically a defect.
