# US 11: Resilience Filters

Source: `_source_user_stories_v1.3.txt` lines 526–596
Scope: functional · data · a11y

## Preconditions
- Logged-in session; on dashboard.
- Resilience Filters panel available (top "Select Resilience Filters").
- Filters may be applied before or after geography.

## Filter groups (each with a "?" info tooltip)
Risk Domain · Metric · Scenario · Period · Statistic · Map Mode

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S0 | Select State (Telangana) + one District (Adilabad) in Administrative Panel | Geography set (setup for Q1) |
| S1 | Open "Select Resilience Filters" | Panel expands showing all 6 groups; Metric/Scenario/Period/Statistic show cascade placeholders ("Select a domain first" etc.) and are **disabled** until their parent is chosen; Map Mode is independent |
| S2 | Hover Risk Domain "?" icon | Tooltip with help text appears (captured) |
| S3 | Select Risk Domain (Heat Risk) | Metric dropdown **enables** and updates dynamically for the domain |
| S4 | Select Metric (first option) | Scenario + Statistic **enable** |
| S5 | Select Scenario, then Period | Scenario + Period require **manual** selection; Period un-gates after Scenario. (Statistic defaults to "Mean", Map Mode to "Absolute value" — those two auto-fill.) |
| S6 | Observe map after full filter set | Map renders data — region filled by value + **numeric color-scale legend**. Reliable signal: "Add to Analysis" enables. (Categorical Very Low→Extreme is the US 14 ranking concept, not the map legend.) |
| S7 (Q1) | With district + all filters applied, inspect "Add to Analysis" | Resolves US 09 Q1: is the button now **enabled**? (Enabled ⇒ US 09 Q1 was "filter required", intended. Still disabled ⇒ possible defect.) |

## Cross-cutting checks
- No real (non-benign) error events during filter application.
- No `NaN`/blank in dropdown options or legend numbers.
- Cascade correctness: downstream filters never selectable before their parent.

## Known caveats (SPEC-DRIFT, not FAIL)
- App offers **10 Risk Domains** (adds "Drought Risk (Advanced)", "Population
  Exposure") vs the spec's 8 — a refinement.
- Spec lists Statistic as independent, but the app gates it behind Metric.
- Statistic defaults to "Mean" and Map Mode to "Absolute value" after Metric;
  Scenario + Period still need manual selection.
- Map legend is a **continuous numeric scale** (e.g. 23.96–77.13) with a metric
  caption, not the categorical "Very Low..Extreme" of the spec.
- Map Mode showed "Absolute value" (greyed) rather than the spec's Predicted
  Value / Historical baseline / Change-from-baseline — verify whether Map Mode is
  metric-dependent or disabled here.
- Filter application + map data load are **asynchronous (several seconds)** with
  no obvious loading indicator — worth confirming a spinner exists for slow links.

## Resolved
- **US 09 Q1**: "Add to Analysis" is **enabled** once a district + filters are
  applied → a resilience filter IS required to enable it. Intended, not a defect.
