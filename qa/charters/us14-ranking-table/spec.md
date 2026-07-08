# US 14: Ranking Table View & Interaction

Source: `_source_user_stories_v1.3.txt` lines 696–770
Scope: functional · data

## Preconditions
- Logged-in session; State selected; resilience filters applied.
- "Ranking Table" view selected.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Apply geography (State) + full filters, switch to "Ranking Table" | View switches; "District Ranking Table" heading + filter caption shown |
| S2 | Observe table | Table renders with columns: Position in State, State, District, (Block in block mode), Index Value, Absolute Change, Change Percentile, Level of Change, Add-to-Analysis checkbox |
| S3 | Verify ranking | Rows ranked by percentile (highest → Rank 1); no district selected ⇒ all districts ranked |
| S4 | Row color coding | Rows color-coded by Level of Change matching the legend |
| S5 | Legend | Legend shows categorical levels with ranges: Very Low (0–20%) … Extreme (80–100%) |
| S6 | Select row checkbox(es) → "Add to Analysis" | Selected locations added to My Analysis; multi-site triggered |

## Current status — BLOCKED (confirmed defect)
As of this run, the Ranking Table **cannot load data**: the endpoint
`GET /api/api/parquet/ranking` returns **HTTP 500** and the UI shows
"We couldn't load the ranking data for the selected filters. Please try again."
Reproduced with both state-only (all districts) and single-district selections,
while the Map view renders the same filters correctly. S2–S6 cannot be verified
until this is fixed. Note the suspicious doubled `/api/api/` path segment.

## Cross-cutting checks
- No `NaN`/blank in table cells (once loading).
- "No data available for the selected filters" only when genuinely empty (distinct
  from the current 500 error).
