# US 12 — View Mode (Radio Buttons)

**Verdict: PASSING mechanic; blocked data on the Ranking side (B1).** The Map View ↔
Ranking Table toggle works exactly to spec — mutual exclusivity, default Map View,
and geography + filters preserved across the switch. The only failure is that the
Ranking Table has **no data to show** because of the **B1 blocker** (ranking
endpoint → HTTP 500), which is already filed.

Authoritative run: `qa/runs/2026-07-08T15-39-34-550Z_us12-view-mode/`
(5 steps, 0 failed; 2 real error events = the ranking 500 + its console mirror).

## Verified matching spec (no defect)
- **"Select your views"** section on the left panel with **Map View** + **Ranking
  Table** options. (S2)
- **Map View is the default**; the map canvas renders. (S1)
- **Mutual exclusivity**: selecting **Ranking Table** hides the map; selecting **Map
  View** hides the table. (S3–S4, `mapHidden=true` / `mapVisible=true`)
- **State + filters preserved across the switch** (spec 616): after toggling to
  Ranking and back, Telangana · Warangal · Heat Risk · Heat Risk Composite · SSP2-4.5 ·
  Early century · Mean all remained set (visible in `s3-ranking.png`). (S5)

## Findings
No **new** US 12 defects. The Ranking Table view surfaces the existing **B1**
blocker.

| ID | Sev | Area | Status | Summary |
|----|-----|------|--------|---------|
| B1 | Blocker | functional | CONFIRMED (existing) | Selecting **Ranking Table** shows *"We couldn't load the ranking data for the selected filters. Please try again."* — `POST /api/api/parquet/ranking` → **HTTP 500** (doubled `/api/api/`). |

**Note on B1 method:** in this build the failing ranking call is a **POST**
(`POST /api/api/parquet/ranking` → 500), whereas the original B1 write-up cites a
`GET`. Same endpoint + same doubled-path + same 500 — worth updating the B1 method
when the fix is verified.

**Message nuance:** the ranking-view error is a **load-failure** message ("We
couldn't load the ranking data…"), which is appropriate for a 500 — distinct from
spec 611's **"No data available for the selected filters"** (which is for a genuine
empty result, not a server error). No action needed unless the PO wants one unified
string.
