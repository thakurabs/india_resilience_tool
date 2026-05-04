# CLAUDE.md — Tools (ops / diagnostics / data prep)

Applies to: `tools/`

Subdirectories: `tools/pipeline/`, `tools/geodata/`, `tools/optimized/`, `tools/runs/`, `tools/data_acquisition/`, `tools/data_prep/`, `tools/diagnostics/`, `tools/legacy/`

Goal: keep non-runtime scripts organized, documented, and safe to run.

---

## Non-negotiables

- `tools/` is **not runtime**: code under `india_resilience_tool/` must never import from `tools/`.
- `tools/legacy/` is **reference-only**: never import from it, never modify it.

---

## Script conventions

- Prefer scripts runnable via `python -m tools.<package>.<module> --help`.
- Use `argparse`; support `IRT_DATA_DIR` / `IRT_PROCESSED_ROOT` via environment variables or explicit flags — no hard-coded paths.
- Keep side effects explicit: document write paths, overwrites, and deletes.
- Add `--dry-run` for any destructive operation.

---

## Destructive flags — always confirm before running

The following flags require explicit user confirmation before Claude Code proposes or runs them:

| Flag | Risk |
|------|------|
| `--full-rebuild` | Wipes entire `processed_optimised/` bundle |
| `--overwrite` on pipeline tools | Overwrites existing processed outputs |
| `--prune-scope` | Deletes stale files inside selected metric/level roots |

Never suggest running these without a `--dry-run` first unless the user has explicitly acknowledged the risk.

---

## Documentation requirement

Every script must be documented in `tools/README.md`:
- Purpose
- Command
- Inputs and outputs

If adding a new script, update `tools/README.md` and `MANIFEST.md` as part of the same change.

---

## Canonical pipeline entry points (quick reference)

```bash
# Full dashboard prep
python -m tools.runs.prepare_dashboard --help

# Climate indices (multiprocess)
python -m tools.pipeline.compute_indices_multiprocess --help

# Build composite metrics
python -m tools.pipeline.build_composite_metrics --help

# Build proposal bundles
python -m tools.pipeline.build_proposal_bundles --help

# Build master CSVs
python -m tools.pipeline.build_master_metrics

# Build optimized runtime bundle
python -m tools.optimized.build_processed_optimised --help

# Audit parity
python -m tools.optimized.audit_processed_optimised_parity --help

# Aqueduct admin masters
python -m tools.geodata.build_aqueduct_admin_masters --help

# Population exposure masters
python -m tools.geodata.build_population_admin_masters --help

# Groundwater district masters
python -m tools.geodata.build_groundwater_district_masters --help
```

---

## Validation

For any tools refactor:
- Run the module with `--help` and verify help text is correct
- Run one minimal no-op or `--dry-run` path if available
- Run: `python -m pytest -q`
