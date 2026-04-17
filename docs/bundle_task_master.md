# Bundle Task Master

This task sheet tracks the work needed to move the landing/glance bundle scores
from equal-weight normalized averages to the approved custom-weighted bundle
methodology.

## Status Snapshot

Completed in code:
- `Task 1` Freeze the new Glance View scope
- `Task 2` Create a canonical bundle-weight mapping layer
- `Task 3` Implement workbook-derived weights for `Heat Risk`
- `Task 4` Implement missing metrics for `Heat Stress`
- `Task 5` Finish `Drought Risk`
- `Task 6` Finish `Flood & Extreme Rainfall Risk` with current available metrics
- `Task 7` Implement missing metrics for `Cold Risk`
- `Task 8` Finish `Agriculture & Growing Conditions`
- `Task 9` Extend the scoring engine for weighted averages
- `Task 10` Update landing runtime to use weighted bundle specs
- `Task 11` Tighten method transparency in the UI
- `Task 12` Add focused tests for the weighted-bundle path

Still active:
- `Task 13` Keep deferred backlog items current
- `Task 14` Final manual validation across Glance and Deep Dive

Explicitly deferred:
- `Flood Depth Index` remains on the to-do list and is not part of the current flood-bundle completion pass.

## Task List

1. Freeze the new glance-view scope.
- Keep in Glance View:
  - `Heat Risk`
  - `Heat Stress`
  - `Drought Risk`
  - `Flood & Extreme Rainfall Risk`
  - `Cold Risk`
  - `Agriculture & Growing Conditions`
- Remove from Glance View:
  - `Rainfall Totals & Typical Wetness`
  - `Temperature Variability`
- Keep those two available only in Deep Dive.

2. Create a canonical bundle-weight mapping layer in the repo.
- Do not rely on parsing the Excel workbook directly at render time.
- Extract the workbook into a repo-side canonical config/helper that stores:
  - landing bundle name
  - metric slug
  - metric weight
  - source note
  - any approved substitution note
- This becomes the runtime source of truth for weighted scoring.

3. Implement workbook-derived weights for `Heat Risk`.
- Update the tropical nights metric from `TN > 20°C` to `TN > 25°C`.
- Audit where that metric slug/data column is defined today.
- Either:
  - replace the existing metric definition if it is only used for this context, or
  - add a new `TN > 25°C` metric slug and swap it into `Heat Risk`.
- Rebuild the `Heat Risk` weight table by splitting each workbook group weight equally across the metrics in that group.
- Ensure the final per-metric weights sum to `1.0`.

4. Implement missing metrics for `Heat Stress`.
- Compare current `Heat Stress` bundle metrics to workbook-required metrics.
- Define each missing metric formally:
  - name
  - formula
  - units
  - scenario/period/stat compatibility
  - directionality
  - data source path
- Add the missing metrics to the registry/config/master-loading path.
- Compute and expose them at district level.
- Update the `Heat Stress` bundle membership to include the full approved metric set.
- Then split workbook group weights equally across all metrics in each group and normalize to a final per-metric weight table.

5. Finish `Drought Risk`.
- Status: completed in code.
- The visible drought bundle now uses weighted SPI drought-event counts for 3-month, 6-month, and 12-month drought episodes.
- Remaining work is data rebuild / validation, not bundle-definition work.

6. Finish `Flood & Extreme Rainfall Risk` with the current available metrics.
- Status: completed in code for the current available flood metric set.
- The flood bundle now uses custom weights across the active six-metric bundle.
- `Flood Depth Index` remains explicitly deferred as a follow-up revision.

7. Implement missing metrics for `Cold Risk`.
- Status: completed in code.
- The bundle has been expanded to the approved weighted metric set and wired into the Glance scoring path.
- Remaining work for Cold Risk is operational validation and any residual hydro-readiness cleanup, not bundle-definition work.

8. Finish `Agriculture & Growing Conditions`.
- Status: completed in code.
- The agriculture bundle now uses the approved nine-metric mixed-stress bundle and custom weights.
- Remaining work is data rebuild / validation, not bundle-definition work.

9. Extend the scoring engine for weighted averages.
- Update `BundleMetricSpec` to carry `weight`.
- Change `compute_bundle_score_frame()` from simple mean to weighted mean of normalized metric scores.
- Use only available metrics in the row.
- Renormalize weights across available metrics for that row so missing values do not depress scores incorrectly.
- Preserve `NaN` when a row has no valid metrics.

10. Update landing runtime to use weighted bundle specs.
- Change `_bundle_metric_specs()` to pull weights from the canonical mapping layer.
- Preserve existing scenario-period full-coverage validation.
- Preserve Deep Dive handoff behavior.
- Preserve driver-table output, but consider showing weights in driver debug/method notes if helpful.

11. Tighten method transparency in the UI.
- Update the Glance View method note so it stays accurate as additional weighted bundles go live.
- The note should say:
  - weighted average of normalized hazard metrics
  - weights come from approved bundle definitions
  - only full-coverage scenario-periods are shown
  - hazard-only, not resilience
- Keep wording concise and non-misleading.

12. Add focused tests.
- Weight splitting:
  - group weight split equally across metrics in group
  - per-bundle weights sum to `1.0`
- Weighted scoring:
  - weighted average uses normalized values, not raw values
  - row-level missing metrics renormalize remaining weights
  - all-missing rows return `NaN`
- State aggregation:
  - remains simple mean of district bundle scores unless explicitly changed later
- Bundle config:
  - `Heat Risk` uses `TN > 25°C`, not `TN > 20°C`
  - hidden Glance bundles are no longer offered
- Landing runtime:
  - weighted bundles still produce valid district/state scores and Deep Dive handoff

13. Add backlog items in priority order after this exercise.
- `1.` Add `Flood Depth Index` and then revise `Flood & Extreme Rainfall Risk` again against the final flood metric set.
- `2.` Capture any remaining hydro-readiness / bundle-rebuild operational cleanup discovered during validation.

14. Run manual validation.
- Glance launches without `Rainfall Totals & Typical Wetness` and `Temperature Variability`.
- `Heat Risk` reflects the updated tropical nights threshold.
- `Heat Stress` and `Cold Risk` render with expanded metric sets and weighted scores.
- `Drought Risk`, `Flood & Extreme Rainfall Risk`, and `Agriculture & Growing Conditions` render with their final approved bundle definitions after implementation.
- Deep Dive remains intact for all remaining Glance bundles.

## Recommended Implementation Order

1. Run rebuilds and validation for `Drought Risk`, `Flood & Extreme Rainfall Risk`, and `Agriculture & Growing Conditions`.
2. Resolve any remaining readiness or coverage issues discovered during those rebuilds.
3. Keep `Flood Depth Index` on the deferred to-do list.
