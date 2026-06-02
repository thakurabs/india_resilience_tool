# C6 — Worker-count & yearly-retention benchmark protocol (W3)

Phase-2 / backlog measurement protocol for
`tools/runs/refresh_dashboard_climate_bundles.ps1`. This is a **measurement
protocol, not a logic change**. No default is changed here; any change to the
`-Workers` default or `-YearlyCleanupPolicy` default is a follow-up gated on the
numbers recorded below and on explicit user sign-off.

Author: applied by Claude Code under user direction.
Branch at creation: `add_flood_depth`.

---

## Why

Phase 1 shipped two knobs on assumptions rather than measurement:

- `-Workers` is **opt-in** (default 36 when supplied; otherwise each downstream
  CLI picks its own machine-aware default — compute uses `default_workers_75pct()`,
  `tools/pipeline/compute_indices_cli_common.py:21`). The *right* worker count was
  never measured on this hardware.
- `-YearlyCleanupPolicy` defaults to `preserve`. Whether
  `delete_after_ensemble` is a safe, faster default — without breaking the strict
  block audit contract (`--require-block-yearly-models`) — was never measured.

"Measure, don't assume." Run the protocol, record the table, then decide.

---

## Ground rules

- All benchmark runs that execute the pipeline (not `-PlanOnly`) are
  **expensive** and require explicit user approval before running.
- `--overwrite` / `--prune-scope` are reached through the shared runner; per
  `tools/CLAUDE.md` they need confirmation. Use a **single representative bundle**
  on a throwaway/again-rebuildable state to bound cost.
- Record wall-clock from the runner's own per-step logs under
  `<ReportRoot>/logs/dashboard_climate_refresh/<state>/<runToken>/`.
- Keep the machine otherwise idle; record CPU core count and RAM.

Fixed scope for every run below (edit once, keep constant):

```
STATE   = Telangana
BUNDLE  = "Heat Risk"        # one thematic bundle, deterministic source set
LEVEL   = block              # block is the heavier, audit-gated level
```

---

## Part A — worker count (compute stage)

Goal: wall-clock + peak memory vs `--workers` at 25/50/75/100% of cores.

For each `W` in {25%, 50%, 75%, 100%} of physical cores, force a real recompute
of one bundle and time it:

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 `
  -State Telangana -Level block -Bundle "Heat Risk" `
  -Overwrite -Workers <W> -SkipOptimized -SkipAudit
```

`-SkipOptimized -SkipAudit` isolates the compute+master cost. `-Overwrite` forces
real work (no skip-existing no-ops). Read the compute step elapsed from the step
log (`timestamp_start` / `timestamp_end`). Capture peak memory with an OS monitor
(Windows: Task Manager peak working set, or `Get-Counter`).

| Workers (% cores) | Workers (n) | Compute wall-clock | Peak RAM | Notes |
|-------------------|-------------|--------------------|----------|-------|
| 25%               |             |                    |          |       |
| 50%               |             |                    |          |       |
| 75% (CLI default) |             |                    |          |       |
| 100%              |             |                    |          |       |

**Recommendation (fill after measuring):** _____. Change the `-Workers` default
only with user sign-off; the compute CLI default (`default_workers_75pct()`)
already encodes a sane machine-aware value, so prefer leaving the runner opt-in
unless the data shows a clear win.

---

## Part B — yearly retention (end-to-end, review #6)

Goal: compare `preserve` vs `delete_after_ensemble` across the **whole runner
contract**, not just compute. A policy that speeds compute but fails the strict
block audit is a regression, not a win.

Run each policy end-to-end at block level for the same bundle:

```powershell
# Policy 1: preserve (current default)
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 `
  -State Telangana -Level block -Bundle "Heat Risk" `
  -Overwrite -YearlyCleanupPolicy preserve

# Policy 2: delete_after_ensemble
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 `
  -State Telangana -Level block -Bundle "Heat Risk" `
  -Overwrite -YearlyCleanupPolicy delete_after_ensemble
```

Then run an **incremental re-run** (no `-Overwrite`) of each to measure the
skip-existing rerun cost after cleanup.

Measure four things per policy:

| Policy                  | (a) compute wall-clock | (b) optimized build time | (c) strict block audit (`--require-block-yearly-models`) pass/fail | (d) incremental rerun time | on-disk size |
|-------------------------|------------------------|--------------------------|--------------------------------------------------------------------|----------------------------|--------------|
| preserve                |                        |                          |                                                                    |                            |              |
| delete_after_ensemble   |                        |                          |                                                                    |                            |              |

The audit (c) is read straight from the runner's audit step exit status / the
parity report it writes (`parity_report_*_block_dashboard_climate*.json`).

**Decision rule:** `delete_after_ensemble` is only viable as a default if column
(c) is **pass** for the block level. If it fails the strict block audit, keep
`preserve` regardless of any compute/disk win.

**Recommendation (fill after measuring):** _____.

---

## Output of this workstream

- This table, filled in, committed here.
- A one-paragraph recommendation for each knob.
- No default change in the script unless the numbers justify it **and** the user
  signs off — that is a separate CHG.
