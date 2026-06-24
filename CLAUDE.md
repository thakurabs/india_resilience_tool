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

**Scale testing effort to risk level — do not add tests reflexively.**

| Change type | Test requirement |
|-------------|-----------------|
| Scientific compute (SPI, indices, ranking, aggregation) | Always add or update a pytest test |
| Data contract (identifiers, column naming, CRS) | Always add or update a pytest test |
| Known regression risk (past bugs) | Always add a regression guard |
| Config change (new slug, bundle weight) | Run existing tests; add a test only if new validation logic is introduced |
| UI / viz change | Manual check is sufficient; no pytest required |
| Tools / pipeline script | `--dry-run` check is sufficient; no pytest required |
| Refactor with no logic change | Run existing tests; add nothing unless coverage was previously absent |

If tests cannot be run (environment or data missing), state what blocked you and provide a focused manual checklist — only for the risk tier that warrants it.

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

### Knowledge graph (graphify)

A persistent code/doc knowledge graph lives in `graphify-out/` (git-ignored, local-only).

- **Query (read-only — run freely):** ask a natural-language question about the
  codebase, or run `graphify query "<question>"` / `graphify path "A" "B"` /
  `graphify explain "<node>"`. These reuse `graphify-out/graph.json`; no rebuild.
- **Rebuild (writes files + spends tokens — treat as `APPROVED: APPLY`):**
  `/graphify . --update` (incremental) re-extracts only changed files.
  Full `/graphify .` rebuilds from scratch.
- The graph persists across sessions; nothing needs to run at startup.

## 11. Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in UI |
| `IRT_DATA_DIR` | resolved in `paths.py` | Base directory for boundaries and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Optional processed-root override |
| `IRT_PROCESSED_OPTIMISED_ROOT` | `IRT_DATA_DIR/processed_optimised` | Optional optimized runtime-bundle override |
| `IRT_DEBUG` | `0` | Enable debug/perf output |
| `IRT_ROSTER_GATE` | `warn` | Canonical-roster gate at master publish in `build_processed_optimised`: `warn` (drop stale/renamed admin rows + log) / `strict` (skip stale master + fail the run after publishing the clean bundles) / `off`. Intended end-state is `strict` once the boundary-migration roster audit is clean. |

## 12. Do not touch (without explicit discussion)

- `tools/legacy/` — kept for reference only; never import or modify
- `environment.yml` — conda environment; dependency changes need explicit approval
- `docs/HANDOFF.md` and `docs/BACKLOG.md` — update only on explicit user instruction
- Any `--full-rebuild` or `--overwrite` pipeline flags — always confirm before running
- Processed output directories under `IRT_DATA_DIR` — pipeline writes; do not manually edit

---

## 13. Geospatial environment hygiene (PROJ / GDAL stack)

This repo's geo stack (`pyproj`, `rasterio`, `fiona`, `shapely`, `geopandas`) is fragile on Windows because each package can bundle its own PROJ/GDAL native libraries. Mixing conda-forge and pip installs causes DLL/`proj.db` version mismatches that surface as cryptic runtime errors.

### Rules

- **Install geo packages via conda-forge only.** Never `pip install` `pyproj`, `rasterio`, `fiona`, `shapely`, `geopandas`, `cartopy`, or `gdal` into this env.
- **Never set `PROJ_LIB` or `PROJ_DATA`** as user/system environment variables. Modern pyproj resolves its own data dir; setting these globally pins all envs to one location and breaks version-skewed envs.
- If a user reports geo errors, **diagnose before reinstalling**. Reinstall-first wastes time when the real cause is leftover files or env vars.

### Known-bad symptoms

- `pyproj.exceptions.CRSError: Invalid projection: EPSG:4326: (Internal Proj Error: proj_create: no database context specified)`
- `UserWarning: pyproj unable to set PROJ database path`
- `rasterio` / `fiona` import errors mentioning `proj.db`, `PROJ`, or missing DLLs
- GDAL "Cannot find proj.db" at runtime

### Diagnostic block (read-only; run first)

```bash
python -c "import pyproj, os; print('pyproj:', pyproj.__version__, 'PROJ:', pyproj.proj_version_str); print('data_dir:', pyproj.datadir.get_data_dir()); print('proj.db exists:', os.path.exists(os.path.join(pyproj.datadir.get_data_dir(),'proj.db'))); print('PROJ_LIB=', os.environ.get('PROJ_LIB')); print('PROJ_DATA=', os.environ.get('PROJ_DATA')); from pyproj import CRS; print(CRS('EPSG:4326'))"
conda list | grep -iE "^(proj|pyproj|proj-data|rasterio|fiona|shapely|geopandas|gdal) "
pip list --format=columns | grep -iE "^(pyproj|rasterio|fiona|shapely|geopandas|gdal) "
```

### Triage decision tree

1. **`pyproj.datadir.get_data_dir()` points inside `site-packages/pyproj/proj_dir/...`** → leftover from a previous `pip install pyproj`. `pip uninstall` does not always remove this directory. Delete it, then re-test:
   - Windows: `Remove-Item -Recurse -Force "<env>/Lib/site-packages/pyproj/proj_dir"`
   - Linux/macOS: `rm -rf "<env>/lib/python*/site-packages/pyproj/proj_dir"`
   Conda-forge `pyproj` does NOT ship a `proj_dir` directory; if it exists, it is stale.

2. **`PROJ_LIB` or `PROJ_DATA` is set** → check Windows env vars, `~/.bashrc`, `~/.zshrc`, conda `etc/conda/activate.d/*.sh|*.bat`, and the PowerShell `$PROFILE`. Unset and re-test.

3. **Same package appears in both `conda list` and `pip list`** → pip has shadowed conda. Resolve:
   ```bash
   pip uninstall -y <pkg>
   conda install -n <env> -c conda-forge --force-reinstall <pkg>
   # then re-check for leftover bundled data dirs (proj_dir, gdal_data, ...) under site-packages
   ```

4. **Version skew between `proj` and `pyproj`** → force-reinstall both from conda-forge together:
   ```bash
   conda install -n <env> -c conda-forge --force-reinstall pyproj proj proj-data
   ```

### When suggesting fixes to users

- Always run the diagnostic block first; do not propose reinstalls without seeing its output.
- Treat `--force-reinstall` as the minimum for conda repair (plain `conda install` no-ops if metadata still claims the package is present after a pip uninstall).
- After any reinstall, verify by deleting leftover `proj_dir` / `gdal_data` directories under `site-packages` if present, and re-running `CRS('EPSG:4326')`.

### Do not

- Do not `pip install pyproj` (or other geo packages) into this env, even as a "quick fix".
- Do not advise users to `export PROJ_LIB=...` as a workaround — it masks the real problem and breaks other envs on the same machine.
- Do not edit `environment.yml` to pin around a broken install; fix the install first.
