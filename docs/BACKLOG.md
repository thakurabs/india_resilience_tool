# IRT Backlog

## Purpose

This file is the durable backlog for deferred or shelved work in the India Resilience Tool.

Use it for:
- work we know we want to do later
- follow-up tasks that should not be lost between sessions
- larger initiatives that are not the current execution priority

Do not use it for:
- session handoff details
- change-by-change implementation history
- generated-data observations with no reusable action item

Session handoffs stay in chat and, when explicitly confirmed by the user, in `docs/HANDOFF.md`.

## How to Use This File

- Keep entries short and action-oriented.
- Prefer one durable backlog item over many tiny notes.
- Move items between `Now`, `Next`, `Later`, and `Icebox` instead of duplicating them.
- Update the `Done when` line when scope becomes clearer.

Entry fields:
- `ID`
- `Title`
- `Area`
- `Why deferred`
- `Dependency / trigger`
- `Done when`

## Now

### BL-0001 — Close remaining river topology QA issues
- `Area`: river, topology
- `Why deferred`: the river foundation is in place, but a small set of unresolved assignment and self-loop cases still needs closure before the river layer can be treated as fully stable.
- `Dependency / trigger`: continue after the latest `build_river_topology` outputs are regenerated and the debug artifacts are available.
- `Done when`: unresolved river hydro assignments are explained or fixed, remaining self-loops are inspected, and `river_topology_qa.csv` contains only accepted residual issues.

### BL-0002 — Complete structured visual validation of river overlays and topology artifacts
- `Area`: river, QA
- `Why deferred`: the dashboard now shows hydro river overlays and river summaries, but representative basin/sub-basin validation still needs to be completed systematically.
- `Dependency / trigger`: requires current `river_basin_name_reconciliation.csv`, `river_subbasin_diagnostics.csv`, `river_reaches.parquet`, and the missing-assignment debug artifacts.
- `Done when`: a representative set of major basins, sub-basins, unresolved cases, and debug layers has been manually reviewed and accepted.

### BL-0003 — Decide whether sub-basin river matching needs a permanent reconciliation artifact
- `Area`: river, data-contract
- `Why deferred`: basin-level reconciliation is permanent, but sub-basin matching is still diagnostics-driven and may or may not need to graduate to a canonical mapping file.
- `Dependency / trigger`: review the current `river_subbasin_diagnostics.csv` results after visual validation.
- `Done when`: the team explicitly decides either to keep diagnostics-only matching or to introduce `river_subbasin_name_reconciliation.csv`.

## Next

### BL-0004 — Build the weighted admin ↔ hydro translation engine
- `Area`: crosswalk, analytics
- `Why deferred`: current crosswalks are intentionally read-optimized and explanatory, not analytical transfer engines.
- `Dependency / trigger`: start after current river QA closure and once the desired weighting/aggregation semantics are agreed.
- `Done when`: the platform can translate values across admin and hydro geographies with explicit weighting rules and provenance.

### BL-0005 — Add hydro portfolio workflows
- `Area`: hydro, UI
- `Why deferred`: portfolio support currently exists only for district and block flows.
- `Dependency / trigger`: start after hydro single-unit flows and the polygon crosswalk bridge are considered stable enough to widen the interaction model.
- `Done when`: basin and sub-basin portfolio selection, comparison, and portfolio-side summaries work reliably in the dashboard.

### BL-0006 — Build the river-network/reach translation layer
- `Area`: river, crosswalk
- `Why deferred`: the current river work is hydro-facing and topology-ready, but not yet connected to admin/hydro crosswalk semantics.
- `Dependency / trigger`: start after river topology QA closure and once the desired river/admin translation semantics are defined.
- `Done when`: the platform can relate reaches to admin and hydro polygons in a reusable, audited way.

## Proposal Bundles Backlog

### BL-0013 — Recalibrate threshold-heavy proposal bundles to avoid spatial saturation
- `Area`: proposal bundles, methodology
- `Why deferred`: several sector-wise proposal bundles currently collapse to one or two composite values across Telangana district and block units, especially in later future periods, because too many rules are binary `0/100` thresholds that saturate statewide.
- `Dependency / trigger`: start after the team confirms whether sector-wise bundles should remain threshold-led, move to continuous severity scoring, or adopt mixed threshold-plus-continuous rules.
- `Done when`: `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Infrastructure Risk`, and `Life & Livelihood Loss Risk` all retain meaningful spatial differentiation for representative district and block slices without silently changing intended semantics.

### BL-0014 — Expose proposal bundle constituent metrics and rule diagnostics in Deep Dive
- `Area`: proposal bundles, UI, transparency
- `Why deferred`: Deep Dive currently exposes only the persisted composite slug for sector-wise domains, so users cannot inspect the constituent metrics, rule logic, or rule-level scores that produced the bundle outcome.
- `Dependency / trigger`: start after the preferred UX is chosen for sector-wise drill-down, such as exposing constituent metrics in the ribbon, adding a dedicated `Bundle drivers` panel in Deep Dive, or supporting both patterns together.
- `Done when`: Deep Dive allows users to inspect the constituent metrics and rule diagnostics for each sector-wise bundle, including rule labels, source metric names, raw values, thresholds or baseline logic, and per-rule scores for the selected geography.

### BL-0015 — Add proposal-bundle saturation diagnostics in runtime and QA workflows
- `Area`: proposal bundles, QA, diagnostics, UI
- `Why deferred`: the current runtime can render flat proposal-bundle maps without clearly telling operators or users that the bundle has collapsed to very low variance because of saturated rule outcomes.
- `Dependency / trigger`: start after the team agrees on what counts as unacceptable bundle flatness, such as fully constant slices or slices with two-or-fewer distinct composite values.
- `Done when`: the dashboard and supporting QA workflows can flag low-variance proposal-bundle outputs, distinguish methodological saturation from data or rendering bugs, and surface an operator-friendly warning or report.

### BL-0016 — Make proposal-bundle baseline semantics explicit and auditable
- `Area`: proposal bundles, methodology, data-contract
- `Why deferred`: change-vs-baseline proposal rules currently rely on fallback baseline-column discovery, which works operationally but makes the intended historical comparison period less explicit than it should be.
- `Dependency / trigger`: start after the desired historical baseline window is agreed for proposal-bundle change rules and reconciled against the currently persisted source-master periods.
- `Done when`: proposal-bundle config explicitly declares the intended baseline semantics, source masters are aligned or transformed to that contract, and change-rule behavior is documented and test-covered.

### BL-0017 — Add regression tests for proposal-bundle rule diversity and Deep Dive driver exposure
- `Area`: proposal bundles, tests
- `Why deferred`: the current test suite validates builder mechanics and output presence, but it does not yet protect against bundles becoming uniformly flat across all units or against Deep Dive hiding constituent metrics and driver context unintentionally.
- `Dependency / trigger`: start after the methodology and UI direction are settled for threshold-heavy bundles and sector-wise driver inspection.
- `Done when`: tests cover representative proposal-bundle outputs for district and block levels, detect unintended saturation patterns, validate baseline-rule behavior, and verify the intended Deep Dive exposure of constituent metrics and bundle-driver diagnostics.

### BL-0018 — Define partial-coverage policy for sector-wise proposal bundles
- `Area`: proposal bundles, UI, data availability
- `Why deferred`: sector-wise proposal bundles can appear valid in the dashboard even when only a subset of states currently has persisted proposal-bundle masters, which can make nationwide views look broken or misleading.
- `Dependency / trigger`: start after the team decides whether partial state coverage should remain visible, be hidden, or be shown only with explicit coverage messaging.
- `Done when`: the dashboard has a clear and tested policy for sector-wise proposal bundles with incomplete state coverage, including visibility rules and user-facing messaging for `State=All` views.

## Later

### BL-0007 — Migrate processed-data storage to build/published/archive Parquet serving
- `Area`: storage, architecture
- `Why deferred`: this is a large repo-wide migration and the immediate focus remains river QA/topology closure and hydro-facing runtime hardening.
- `Dependency / trigger`: begin after current river v1 closure, when runtime loader changes and publish/prune workflow changes can be tackled systematically.
- `Done when`: processed serving data uses the planned `build / published / archive` structure, runtime prefers Parquet with CSV fallback during transition, GeoParquet reference geometry is in place, and legacy hot-path CSV forests are pruned only after parity validation.

### BL-0011 — Reframe exposure rankings so they do not present hazard-style risk classes
- `Area`: exposure, UI, semantics
- `Why deferred`: exposure layers currently inherit the generic ranking and `risk_class` presentation used for hazard metrics, which is mechanically correct but semantically awkward for non-hazard layers such as population.
- `Dependency / trigger`: revisit after the first exposure-layer tranche is stable and the desired exposure-side summary language is agreed across details, map tooltips, and rankings.
- `Done when`: exposure pillars no longer present hazard-style `risk_class` labels by default, and the UI uses clearly named relative-exposure language or suppresses those labels entirely where appropriate.

### BL-0008 — Add upstream/downstream routing behavior to the river experience
- `Area`: river, topology, UI
- `Why deferred`: topology artifacts exist offline, but no routed or direction-aware product behavior has been added yet.
- `Dependency / trigger`: requires stable reach/node/adjacency artifacts and a clear contract for directionality and routed queries.
- `Done when`: the product can surface upstream/downstream relationships in a user-facing way without ambiguous or misleading routing behavior.

### BL-0009 — Add admin-side river overlays
- `Area`: river, admin-ui
- `Why deferred`: the current river overlay is intentionally hydro-only.
- `Dependency / trigger`: start after the hydro-side river experience is accepted and the desired admin-side narrative is clear.
- `Done when`: district/block views can optionally show river context without confusing the current admin analysis workflow.

### BL-0010 — Add river-based metric computation
- `Area`: river, analytics
- `Why deferred`: the current river work is limited to cleaning, topology-ready artifacts, overlays, and hydro-side summary context.
- `Dependency / trigger`: requires a settled reach-level analytical contract and clear methodology for river-native metrics.
- `Done when`: river reaches can participate in metric computation and serving contracts in a scientifically explicit way.

### BL-0012 — Converge from `processed` plus `processed_optimised` to one final runtime-serving contract
- `Area`: storage, architecture, deployment
- `Why deferred`: the current two-step flow is the safest migration path while the compact runtime contract is still being validated, but it adds duplication, rebuild drift risk, and operational complexity if kept forever.
- `Dependency / trigger`: revisit after `processed_optimised` is validated across climate, Aqueduct, population, groundwater, hydro, time-series, and case-study flows, and once the retained field/artifact contract is considered stable.
- `Done when`: the team explicitly decides whether to keep a permanent build-vs-runtime split or move to one canonical dashboard-serving processed directory, with a documented migration plan and clear separation for raw/build/QA artifacts.

## Icebox

- No items recorded yet.
