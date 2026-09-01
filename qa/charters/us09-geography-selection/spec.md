# US 09: Geography Selection

Source: `_source_user_stories_v1.3.txt` lines 368–441
Scope: functional · data · visual · a11y

## Preconditions
- Logged-in session active (reused via saved storageState).
- Visitor is on the dashboard with the Geography Selection panel visible on the left.
- Lists of States, Districts, and Blocks are available.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| 1 | Load dashboard | Geography Selection panel visible on the left; State dropdown present; dependent fields (District/Block, View Mode) disabled until a State is chosen |
| 2 | Open State dropdown, select a State | District/Block data loads for that State; dependent fields enable |
| 3 | Choose View Zone = District | District list appears, populated for the selected State; checkbox-based multi-select |
| 4 | Select exactly one District | System treats as **Single-Site Analysis** |
| 5 | Select multiple / all Districts | System auto-applies **Multi-Site Analysis** |
| 6 | Switch View Zone = Block | Analysis focus + geographic selections reset; only ONE district selectable from dropdown; Block list populates for that district |
| 7 | Select one vs. multiple Blocks | One → Single-Site; multiple/all → Multi-Site |
| 8 | Inspect "Add to Analysis" with nothing selected | Button greyed out / non-clickable when no location selected AND no filters applied |
| 9 | Select a location, click "Add to Analysis" | Location added to My Analysis; Multi-Site Analysis triggered |
| 10 | Toggle "Enable hover highlight & tooltip" (checked by default) | ON → hover on map highlights region + shows tooltip; OFF → no highlight, no tooltip |
| 11 | Click Reset | All selections cleared; defaults restored (India map) |
| 12 | Collapse then expand the left panel via arrow icon | Panel hides/shows; **no loss of selections**; last state retained during session |
| 13 | Change State after selecting districts/blocks | District + Block selections reset |
| 14 | Switch Map ↔ Ranking Table view | Selected geography and filters preserved |

## Cross-cutting checks
- No `console.error` / `pageerror` / failed requests / HTTP ≥400 during the flow.
- No `NaN` / blank values where data is expected (dropdowns, counts, tooltips).
- Layout intact at desktop / tablet / mobile (panel must remain usable on 375px).
- No serious/critical axe violations on the panel controls.
- System-failure copy, if triggered: "Something went wrong, please try again".

## Known caveats (spec may be stale — flag as SPEC-DRIFT, not FAIL)
- Exact label text ("Add to Analysis", "Enable hover highlight & tooltip") may
  have been reworded in refinements.
- The save/proceed note when switching Geography ↔ Coordinates modes (US 10) may
  affect step behaviour.
