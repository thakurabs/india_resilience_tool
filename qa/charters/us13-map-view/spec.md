# US 13: Map View & Interaction

Source: `_source_user_stories_v1.3.txt` lines 621–694
Scope: functional · visual

## Preconditions
- Logged-in session; Map View selected (default); geography + filters available.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Select State + District + full filters | Map updates; selected region boundary highlighted; region filled by value (data loaded ⇒ "Add to Analysis" enables) |
| S2 | Observe legend | Legend displayed: numeric color scale + metric/scenario/period/statistic caption |
| S3 | Click the selected region on the map | Tooltip appears with District / State / Value / Baseline (1990–2010) / Δ vs baseline / Position in state / "Add to Analysis" CTA (observational — canvas-driven) |
| S4 | Zoom controls | Zoom in / out / reset controls present and clickable; map responds |

## Cross-cutting checks
- No real (non-benign) error events while interacting.
- Region fill + legend consistent with the selected metric.

## Known caveats
- Map is WebGL/canvas (MapLibre); hover/click tooltips are canvas-driven and
  captured as screenshots rather than asserted on DOM text where not exposed.
- `net::ERR_ABORTED` on `*.pmtiles` and carto basemap tiles is benign streaming
  noise (harness-classified).
