# CLAUDE.md — India Resilience Tool (IRT)

Climate-oriented risk/resilience assessment tool: Python-heavy, Streamlit UI, geospatial + climate analytics.

Source-of-truth docs (always read first):
- `README.md` — human-facing setup, run, test
- `MANIFEST.md` — AI-facing repo map: modules, entry points, file structure, data contracts

---

## 0. Approval gate (non-negotiable)

**Do only tasks explicitly approved by the user.**

Allowed without approval (read-only):
- Reading files, `ls`, `find`, `grep`, `cat`
- `git status`, `git diff`, `git log -1`, `git rev-parse --short HEAD`
- Proposing patches, explaining code, writing test plans

Requires explicit approval before acting:
- Modifying any file
- Running expensive or long pipelines
- Adding or upgrading dependencies
- Creating commits, branches, tags, or pushing

When approval is unclear: produce a patch + plan, **do not apply changes**.

Preferred approval signals from the user:
- `APPROVED: APPLY`
- `APPROVED: RUN TESTS`

---

## 1. Establish current state first

Before any code edits:
1. Read `README.md` and `MANIFEST.md`.
2. Determine the working snapshot:

```bash
git status --short --branch
git rev-parse --short HEAD
git diff --name-only
```

Include a **Status Header** in every technical response:

```
Working Snapshot: GIT:<branch>@<sha>
Working Tree: clean | dirty (<changed files>)
Assumptions: <only if needed>
```

---

## 2. Patch and edit standards

For any modified function, class, or module:
- Provide complete, runnable code — no placeholders
- Preserve existing structure; avoid drive-by refactors
- Keep diffs minimal and task-scoped
- Include required imports; remove unused imports introduced by the change
- Use type hints for non-trivial functions and public APIs
- Add docstrings for public functions and classes
- Handle I/O robustly with clear exceptions or warnings

When not applying changes directly, output as:
- **Unified diff**, or
- **Targeted Replace Blocks** with clear anchors

---

## 3. Tests and validation

Test suite lives in `tests/`. Run with:

```bash
python -m pytest -q
```

For non-trivial logic changes, always include:
- A short manual test plan
- A suggested pytest test (path + assertion)
- Edge cases: empty inputs, all-NaN, single-point, extremes

If tests cannot be run (environment or data missing), state what blocked you and provide a manual validation checklist.

After any accepted/applied code, contract, or workflow change, explicitly check:
- Does `README.md` need updating?
- Does `MANIFEST.md` need updating?

Either update them or state clearly why no update is needed.

---

## 4. Climate / geospatial / risk guardrails

- Assume NaNs and masked values are common everywhere
- Be explicit about units, baselines, and aggregation windows
- Never change methodology silently (ranking, thresholds, baselines, aggregation)
- Any methodology-impacting change must be called out and tested

Nested rules by subdirectory (apply the closest applicable):
- `india_resilience_tool/analysis/CLAUDE.md`
- `india_resilience_tool/app/CLAUDE.md`
- `india_resilience_tool/compute/CLAUDE.md`
- `india_resilience_tool/config/CLAUDE.md`
- `india_resilience_tool/data/CLAUDE.md`
- `india_resilience_tool/viz/CLAUDE.md`
- `tests/CLAUDE.md`
- `tools/CLAUDE.md`
- `notebooks/CLAUDE.md`

---

## 5. Change IDs and in-chat ledger (CHG-xxxx)

Every proposed change gets a Change ID: `CHG-0001`, `CHG-0002`, ...

Maintain an **in-chat ledger** in every code-change response:

| Change ID | File(s) | Summary | Status |
|-----------|---------|---------|--------|
| CHG-0001 | `path/to/file.py` | One-line description | `SUGGESTED` |

Statuses: `SUGGESTED` / `APPLIED (user-confirmed)` / `REJECTED` / `SUPERSEDED`

---

## 6. Persistent HANDOFF ledger: docs/HANDOFF.md

**Do not update `docs/HANDOFF.md`** unless the user explicitly confirms:
- `Applied CHG-xxxx`
- `Applied: CHG-0007, CHG-0008; Rejected: CHG-0006`

Keep all updates in-chat only until that confirmation.

---

## 7. Persistent BACKLOG ledger: docs/BACKLOG.md

Use `docs/BACKLOG.md` for:
- Long-lived deferred work
- Shelved follow-ups to pick up later
- Larger initiatives not in current execution priority

Do not use it for: session handoff details, applied-change history, or transient debugging observations without durable follow-up value.

Update only when the user explicitly asks to capture or revise deferred work.

---

## 8. Git handoff after applied work

After accepted/applied changes, provide:

```bash
git add <file1> <file2> ...
git commit -m "short description of what changed"
```

Rules:
- `git add` must be on **one physical line**, copy-pasteable directly into the terminal
- Do not use shell continuations or line wrapping
- This is guidance — do not run commits automatically
- Skip only if the user explicitly says they don't want git help yet

---

## 9. Perfect handoff point (mandatory on session end)

When the user signals session end (`wrap up`, `pause`, `handoff`, `end`, `stop`, `/quit`, `/new`), produce a copy-paste-ready section:

**PERFECT HANDOFF POINT**
- Objective / task context
- Working Snapshot + working tree status
- CHG ledger excerpt (relevant items)
- Files touched (if any)
- Tests run (commands + results) or why not
- Exact next steps (ordered)
- Open questions / risks

Produce this in-chat even if only patches were suggested. `docs/HANDOFF.md` is updated only after `Applied CHG-xxxx`.

---

## 10. Key entry points (quick reference)

```bash
# Launch dashboard
streamlit run main.py

# Run tests
python -m pytest -q

# Full dashboard prep
python -m tools.runs.prepare_dashboard --help

# Compute climate indices
python -m tools.pipeline.compute_indices_multiprocess --help

# Build optimized runtime bundle
python -m tools.optimized.build_processed_optimised

# Audit parity
python -m tools.optimized.audit_processed_optimised_parity
```

## 11. Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in UI |
| `IRT_DATA_DIR` | resolved in `paths.py` | Base directory for boundaries and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Optional processed-root override |
| `IRT_PROCESSED_OPTIMISED_ROOT` | `IRT_DATA_DIR/processed_optimised` | Optional optimized runtime-bundle override |
| `IRT_DEBUG` | `0` | Enable debug/perf output |

## 12. Do not touch (without explicit discussion)

- `tools/legacy/` — kept for reference only; never import or modify
- `environment.yml` — conda environment; dependency changes need explicit approval
- `docs/HANDOFF.md` and `docs/BACKLOG.md` — update only on explicit user instruction
- Any `--full-rebuild` or `--overwrite` pipeline flags — always confirm before running
- Processed output directories under `IRT_DATA_DIR` — pipeline writes; do not manually edit
