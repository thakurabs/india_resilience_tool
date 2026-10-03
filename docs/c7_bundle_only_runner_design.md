# C7 — Dedicated bundle-only optimized runner: design spike (W4)

Phase-2 / backlog **investigation spike only**. No production code, no path
changes this phase. This note records why the shared runner's optimized scope
cannot be narrowed, what a separate bundle-only runner would have to look like,
and a go/no-go recommendation.

Author: applied by Claude Code under user direction.
Branch at creation: `add_flood_depth`.

---

## 1. The original C7 idea, and why it is unsafe in the shared runner

C7 asked: when refreshing a single dashboard bundle, can we shrink the optimized
build/audit scope from the ~68 **source-metric** roots down to the ~13
**composite** roots, to save build + audit time?

**No — not in `refresh_dashboard_climate_bundles.ps1`.** The runtime reads the
source-metric optimized roots directly, so narrowing the shared runner's scope to
composites would starve the live dashboard:

- `india_resilience_tool/app/ribbon.py`
  - `is_optimized_metric_root(processed_root)` + `optimized_master_sources_from_metric_root(...)`
    at lines 191–192, 306, 601, 614 — the ribbon resolves admin master *sources*
    out of the per-metric optimized roots.
- `india_resilience_tool/app/case_study_runtime.py:74`
  - `resolve_processed_optimised_root(str(slug), data_dir=data_dir)` — case-study
    flow resolves a per-**slug** optimized root, where `slug` is a source metric.

Because both consumers read source-metric optimized roots (not just composites),
the shared runner **must** keep publishing those roots. That is exactly why W1's
union optimized+audit pass spans `source_metrics + composites`, and why
`--prune-scope` operates over that union. Narrowing it is a correctness break, not
an optimization.

(Re-verified at spike time: the four `optimized_master_sources_from_metric_root`
call sites and the `resolve_processed_optimised_root` call site above all still
exist on `add_flood_depth`.)

---

## 2. What a *separate* bundle-only runner would require

A dedicated artifact runner could legitimately publish a **composite-only**
optimized contract, but only if it writes to a **distinct output root the
ribbon/case-study code never reads**. Sketch of the contract:

1. **Distinct output root.** e.g.
   `IRT_PROCESSED_OPTIMISED_ROOT/_bundle_only/<bundle>/...` — a namespace the
   current `is_optimized_metric_root` / `resolve_processed_optimised_root`
   resolvers do **not** discover. It must not alias or prune the shared
   per-metric roots (no shared `--prune-scope` target).
2. **Composite-level only.** Build/audit only the bundle's composite slug(s); do
   not attempt to reconstruct the source-metric roots the live runtime expects.
3. **Its own parity-audit scope.** A separate audit that asserts composite-level
   parity for the bundle-only artifacts, independent of the
   `--require-block-yearly-models` source-metric audit the shared runner runs.
4. **Explicit consumer opt-in.** A bundle landing/preview flow would have to be
   told to read the bundle-only root instead of the shared runtime — a new
   resolver branch, gated by an explicit flag/env, never auto-discovered (so it
   can't accidentally shadow the production runtime).

---

## 3. Open risks

- **Two sources of truth.** A bundle-only root duplicates composite artifacts that
  also exist in the shared runtime; they can drift. Needs a freshness/version
  stamp and a clear "which root wins" rule for any consumer.
- **Audit surface doubles.** A second parity contract is another thing to keep
  green; the win (skipping ~55 source-metric roots) must outweigh maintaining it.
- **Limited reuse.** Because the live dashboard still needs the source-metric
  roots, a bundle-only runner only helps a *separate*, narrower consumer (e.g. a
  fast bundle preview/export) — it cannot replace the shared refresh.
- **Path-contract creep.** Every new optimized root namespace is effectively
  public API for downstream tooling; adding one is a long-lived commitment.

---

## 4. Go / no-go recommendation

**No-go for now.** The shared runner cannot be narrowed (Section 1), and a
separate bundle-only runner (Section 2) only pays off if there is a real consumer
that needs composite-only artifacts faster than the shared refresh can produce
them. Today there is no such consumer: the ribbon and case-study flows both want
the source-metric roots. W1's per-bundle loop already delivers the
bundle-scoped *UX* win (per-bundle progress + fail isolation) without a second
artifact contract.

**Revisit if/when** a bundle-only preview/export surface is actually built and its
latency is dominated by the optimized build+audit of source-metric roots it does
not need. At that point, implement Section 2's distinct-root contract as its own
CHG with its own audit — do **not** retrofit it into the shared runner.
