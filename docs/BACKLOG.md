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

## Later

### BL-0007 — Migrate processed-data storage to build/published/archive Parquet serving
- `Area`: storage, architecture
- `Why deferred`: this is a large repo-wide migration and the immediate focus remains river QA/topology closure and hydro-facing runtime hardening.
- `Dependency / trigger`: begin after current river v1 closure, when runtime loader changes and publish/prune workflow changes can be tackled systematically.
- `Done when`: processed serving data uses the planned `build / published / archive` structure, runtime prefers Parquet with CSV fallback during transition, GeoParquet reference geometry is in place, and legacy hot-path CSV forests are pruned only after parity validation.

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

## Icebox

- No items recorded yet.
