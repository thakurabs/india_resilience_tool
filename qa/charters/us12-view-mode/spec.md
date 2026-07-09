# US 12: View Mode (Radio Buttons)

Source: _source_user_stories_v1.3.txt lines 598–620
Scope: functional | data

## Preconditions
- Logged in; data available for a selected geography + filters.
- "Select your views" section visible on the left Selection Panel.

## Discovered surface
Left panel → **"Select your views"** with two radios: **Map View** (default) and
**Ranking Table**. Only one active at a time. (Note: the Ranking Table path is
covered by the **B1 blocker** — `GET /api/api/parquet/ranking` → 500.)

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Build geography + filters (Telangana → Warangal, all filters) | Map View active by default; map visible |
| S2 | Confirm "Select your views" section + both options | Section present; Map View selected, Ranking Table selectable |
| S3 | Select **Ranking Table** | Table view shown, **map hidden** (mutual exclusivity). Ranking data loads OR the known B1 500 surfaces ("couldn't load ranking data") |
| S4 | Select **Map View** again | Map shown, table hidden |
| S5 | Verify geography + filters preserved across the switch | State/District + Risk Domain/Metric/Scenario/Period unchanged after toggling (spec 616) |
| S6 | (If applicable) no-data messaging | If a view has no data, "No data available for the selected filters" is shown (spec 611) |

## Known caveats
- **B1 blocker**: the Ranking Table view fails to load (HTTP 500, doubled
  `/api/api/`). US 12's *switching* mechanic can still be verified independently of
  the ranking data payload — record the 500 as the B1 family, not a new blocker.
- SPEC-DRIFT-not-FAIL rule applies.
