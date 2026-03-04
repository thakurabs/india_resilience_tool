# Dead Code Candidate Report — IRT (evidence-based)

This report lists **candidates** for deletion/move/refactor, with evidence rooted in:
- primary dashboard entrypoint (`india_resilience_tool/app/main.py`)
- tool scripts (to be relocated under `tools/`)
- tests (`tests/` and `india_resilience_tool/compute/tests/`)

Rule: only mark **delete** when there is evidence of non-reachability from roots.

## Legend
- **Risk**: low / med / high
- **Action**: delete / move-to-tools / keep / refactor

## Candidates (file-level)

| Candidate | Evidence (how we proved reachability/non-reachability) | Risk | Action |
|---|---|---:|---|
| `dashboard_unfactored.py` | Only referenced in docs previously; no imports from `india_resilience_tool/` or tests. | low | delete (done) |
| `dashboard_unfactored_impl.py` | Only referenced in docs/comments previously; not imported/executed by runtime chain. | low | delete (done) |
| Root pipeline/ops scripts (historical) | These are *functionality to retain*, but they should not be in runtime root. They are now relocated under `tools/` (and the master builder implementation lives in `india_resilience_tool/compute/master_builder.py`). | med | move-to-tools (done) |
| `india_resilience_tool/app/runtime_impl.py` | No longer part of the runtime chain (logic moved into `app/runtime.py` + `app/map_pipeline.py`). | low | delete (done) |

## Candidates (symbol-level)

Symbol-level dead code is only marked when:
- it has **no references** (`rg`), and
- deleting it would not remove a documented public contract or a test-protected behavior.

Symbol-level candidates will be added incrementally as refactors progress and reachability roots stabilize.
