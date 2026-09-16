# QA Report — US 14: Ranking Table View  ⛔ BLOCKER

**Run:** `qa/runs/2026-07-08T06-54-45-194Z_us14-ranking/`
**Result:** **BLOCKED** — ranking data endpoint returns HTTP 500.

## Blocker: ranking data fails to load (HTTP 500)

The Ranking Table view switches in correctly (heading "District Ranking Table",
filter caption present), but no data loads:

- Endpoint `GET https://dev.resilience.org.in/api/api/parquet/ranking` → **500**
- UI: "We couldn't load the ranking data for the selected filters. Please try again."
- 0 rows rendered.

### Reproduction
1. Log in; select State = Telangana (District view).
2. Open Resilience Filters; select Risk Domain (Heat Risk) → Metric → Scenario → Period.
3. Switch "Select your views" to **Ranking Table**.
4. Observe the error banner; the ranking request returns 500.

Reproduced in **both** modes: state-only (all districts) and single-district
(Adilabad). The **Map view renders the same filters correctly**, so the data
selection is valid — the failure is specific to the ranking endpoint.

### Notes for the vendor
- The request URL contains a **doubled path segment `/api/api/parquet/ranking`** —
  likely a base-URL/route construction bug; worth checking first.
- Until fixed, US 12 (view mode) and US 14 (ranking table, row selection, ranking
  order, color coding, legend) cannot be verified. The `us14` scenario guards this
  and will pass once the endpoint returns data.

## Cosmetic
- Ranking caption exposes internal slugs ("composite_heat_risk • ssp245 • 2020-2040 • Mean")
  rather than friendly labels.
