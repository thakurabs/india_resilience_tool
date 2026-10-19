# IRT Clean State-Wise Regeneration — Runbook + Executor Brief

**Single source of truth** for the clean regen. It is both the plan (rationale, verified facts) and
the linear execution script (numbered steps, exact commands, STOP gates). Follow the steps top to
bottom.

- **This pass: TELANGANA ONLY.** Maharashtra is gated — see [Next pass](#next-pass--maharashtra).
- **Groundwater is SKIPPED this pass** (deferred until the CGWB↔LGD name reconciliation lands).
- **Environment: WSL bash** driving the Windows conda env via `powershell.exe` (see
  [How commands run](#how-commands-run-in-wsl)).
- Supersedes the shelved surgical repair (`docs/plans/proud-gliding-book.md` / memory
  `telangana-p3-execution-progress`). Direction memory: `project-clean-regen-direction`.

---

## What we are doing & why (scope decisions)

Move the existing `processed/` + `processed_optimised/` trees aside and **regenerate cleanly, one
state at a time, from intact raw data**, using the `tools/runs/` orchestrators.

- ❌ **RETIRE hydro MODE** — no basin/sub-basin map/rankings/detail views. Drop
  `compute_indices_multiprocess --level basin|sub_basin`, hydro masters/composites/yearly.
- ❌ **DROP Aqueduct entirely.** ✅ *Verified safe:* no kept thematic/sector bundle references any
  `aq_*`/`aqueduct`/basin/hydro input (only prose in descriptions) — so the drop breaks nothing.
- ✅ **KEEP hydrology CONTEXT** — admin↔basin/sub-basin crosswalks, river overlays, basins/subbasins
  geometry. Shown within admin views, not as a mode.
- **Recompute (not reuse) all states uniformly.** `processed_bak` holds reusable Telangana compute,
  but no other state does; running Telangana on a reused path while others recompute would split
  methodology. `_bak` is a rollback archive only.

---

## GOLDEN RULES (executor — never break)

1. **Do not improvise.** Run only the commands here, verbatim. Do not invent flags, change paths, add
   `--overwrite`/`--full-rebuild`, or "fix" anything yourself.
2. **STOP at every 🛑 gate.** Post your output and wait for the human to reply with the exact token
   shown (e.g. `APPROVED: APPLY`). No token = no action.
3. **One step at a time.** Run a step, paste full output, state PASS/FAIL vs the Success check, then
   continue (unless the next thing is a 🛑).
4. **On ANY failure, deviation, or surprise → STOP immediately.** Paste the full error + last command.
   Do not retry, do not run a different command, do not continue.
5. **Never delete anything.** The Step 1 "move" is a rename. Never `rm`, never `--overwrite`, never
   `del`, unless this file literally shows that command (it does not).
6. **Maharashtra is forbidden this pass.** Never pass any state other than `Telangana`.
7. **Long runs (hours):** let them finish. Do not kill them. If a step seems hung for a very long
   time with no output, STOP and report — do not kill it yourself.

---

## How commands run in WSL

WSL's own `python3` has **no geo stack** and cannot run this pipeline. The pipeline lives on the
**Windows** conda env, reached from bash via `powershell.exe`. So:

- **Filesystem-only steps** (Step 1 rename, disk checks, file listings) run as **plain bash** on
  `/mnt/d/...`.
- **Every Python step and every `.ps1`** runs through `powershell.exe` with a `Set-Location` to the
  repo and the Windows python. The full ready-to-paste command is written out in each step — just
  copy the whole bash line; do not assemble it yourself. Keep each as a **single physical line**
  (the nested quoting breaks if reflowed).

Constants baked into the commands below:
- Windows python: `C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe`
- Repo (Windows): `D:\projects\india_resilience_tool` · Data root: `D:\projects\irt_data`
- Repo (WSL): `/mnt/d/projects/india_resilience_tool` · Data root: `/mnt/d/projects/irt_data`

**If the Windows python path does not exist on this machine → 🛑 STOP at Step -1 and report.** Do not
search for another python.

---

## Run order (phase map)

Top-to-bottom. The one-time pan-India prerequisites run first, then the per-state loop. Exposure (E)
is a prerequisite listed before C/D because it must exist before the state's final audit — it does
**not** feed climate.

| Step | Phase | What | Cadence |
|---|---|---|---|
| -1, 0 | — | Bridge + pre-flight checks | once |
| 1 | **0** | Backup-and-clear `processed/` + `processed_optimised/` → `_bak` | once |
| 2 | **A** | Boundaries + ADM1 sanity (already built this session) | once |
| 3 | **B** | Hydrology CONTEXT (4 crosswalks + river chain) | once, pan-India |
| 4 | **E** | Exposure (population / rural / built-up / LULC) — **gw skipped** | once, pan-India |
| 5 | **C** | Climate thematic + sector bundles | per state |
| 5b | **B′** | Hydrology context summary (`admin_hydro_summary.parquet`) | once, after first C |
| 6 | **D** | Riverine flood bundle (JRC) | per state |
| 7 | **F** | Final gates | per state |

---

## STEP -1 — Bridge sanity (read-only, safe)

```bash
powershell.exe -NoProfile -Command "& 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' --version"
powershell.exe -NoProfile -Command "& 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -c \"import geopandas, rasterio, pyproj; print('geo OK')\""
```
**Success check:** first prints a Python version; second prints `geo OK`. If either fails → 🛑 STOP
(the Windows env is wrong/missing — not your call to fix).

---

## STEP 0 — Pre-flight (read-only, safe)

**0.1 Confirm nothing holds the trees open.** No Streamlit, notebook, Python REPL, or editor with
files under `D:\projects\irt_data\processed*` open (Windows-side — ask the human to close if unsure).
Report what you can determine. *Why: a Windows handle blocks/half-completes the rename.*

**0.2 Disk space:**
```bash
df -h /mnt/d | tail -1
```
**Success check:** confirm free space is present. (`/mnt/d` has ample headroom — multiple TB free —
so this is a trivial confirmation, not a constraint. Do **not** run `du` on the `processed` trees:
over the WSL→Windows `drvfs` boundary a recursive stat of the hundreds-of-thousands of small files
takes many minutes and tells you nothing actionable here.) Report the `df` line.

🛑 **GATE 0 — POST -1, 0.1, 0.2 output and WAIT for `APPROVED: APPLY`** (authorizes the Step 1 rename).

---

## STEP 1 — Backup-and-clear (rename only; reversible) — ✅ COMPLETED this session

> **✅ ALREADY DONE — VERIFY ONLY, DO NOT run `mv`.** Both output trees were already renamed to
> `*_bak` this session (`processed/` → `processed_bak/`, `processed_optimised/` →
> `processed_optimised_bak/`). Running the `mv` again would fail (`No such file or directory`,
> targets already exist) and trip GOLDEN RULE 4. Run the verify check below, confirm the expected
> state, and proceed straight to STEP 2.

> **Scope (for the record): moved ONLY the two output trees.** It did NOT touch the LGD boundaries
> (`blocks/districts/states_4326.geojson`, `LGD_Blocks/`), raw climate (`r1i1p1f1*/`, `era5/`,
> `imd/`), `basins.geojson`/`subbasins.geojson`, JRC rasters, or exposure rasters — the regen reads
> those as inputs. Renames were instant and reversible (not copies/deletes).

**Verify-only (do NOT run `mv`):**
```bash
ls -d /mnt/d/projects/irt_data/processed_bak /mnt/d/projects/irt_data/processed_optimised_bak
ls -d /mnt/d/projects/irt_data/processed /mnt/d/projects/irt_data/processed_optimised 2>&1
```
Expect: the `_bak` dirs exist; `processed`/`processed_optimised` report "No such file or directory."
If instead `processed/` or `processed_optimised/` still exist (STEP 1 not yet done on this machine) →
🛑 STOP and report; do not improvise the rename (a Windows handle may block it — see session notes).

> **Rollback (if a later run goes wrong)** — two-step so the new tree doesn't collide with `_bak`:
> ```bash
> mv /mnt/d/projects/irt_data/processed_optimised     /mnt/d/projects/irt_data/processed_optimised_failed
> mv /mnt/d/projects/irt_data/processed_optimised_bak /mnt/d/projects/irt_data/processed_optimised
> # (same pattern for processed/ ↔ processed_bak)
> ```
> Keep both `_bak` archives until Telangana AND Maharashtra validate.

---

## STEP 2 — Phase A sanity (read-only; boundaries already built)

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.verify_admin_join_consistency"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.verify_states_geojson"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.verify_districts_blocks_geojson"
```
**Success check:** `verify_admin_join_consistency` ends with `NAMING: OK` and `GEOMETRY: OK` (all
units IoU ≥ 0.999); the other two report no errors.
**On any FAIL → 🛑 STOP, paste output.** Do NOT rebuild boundaries (not your call — the LGD set is the
single source of truth, built via `build_admin_boundaries_from_lgd`, already verified this session).

**Boundary tripwire (cheap insurance).** Record the block-boundary file size now; nothing in this
runbook rebuilds boundaries, so it must stay identical through the whole run. If it ever changes, a
boundary-rebuild command was run by mistake → 🛑 STOP.
```bash
stat -c '%s  %n' /mnt/d/projects/irt_data/blocks_4326.geojson
```
Expected admin shape (sanity): **36 states, 783 districts** pan-India; **Telangana = 33 districts /
588 blocks**.

---

## STEP 3 — Phase B: Hydrology context (pan-India, once)

🛑 **GATE B — POST that Step 2 passed and WAIT for `APPROVED: RUN`.**

> **Why before Phase C:** Phase C's `-IncludeContext` publishes the optimized context bundle from
> exactly these 8 outputs (`*_basin_crosswalk.csv` ×2, `*_subbasin_crosswalk.csv` ×2,
> `river_reaches.parquet`, `river_network_display.geojson`, `river_basin_name_reconciliation.csv`,
> `river_subbasin_diagnostics.csv`). All of B must exist before the first Phase C.

**3.1 Crosswalks (4):**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_district_basin_crosswalk"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_district_subbasin_crosswalk"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_block_basin_crosswalk"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_block_subbasin_crosswalk"
```
**3.2 River chain (run in THIS order — each consumes the prior):**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.clean_river_network"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_river_basin_reconciliation"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_river_subbasin_diagnostics"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.geodata.build_river_topology"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.pipeline.enrich_river_network_districts"
```
**Success check:** each exits without error and prints row counts. Unmatched-unit warnings are OK to
report but are not failures (they surface as missing overlays, not hard errors).
**On any non-zero exit / traceback → 🛑 STOP, paste output.**

---

## STEP 4 — Phase E: Exposure (pan-India, once) — GROUNDWATER SKIPPED

> Verified pan-India: these subcommands have **no `--state` flag** — one run publishes every state's
> exposure shards. **Do NOT run `groundwater` this pass.** Run each `--dry-run` first; if a dry-run
> prints anything alarming (errors, unexpected deletes) → 🛑 STOP before the real run.

```bash
# population-exposure
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard population-exposure --dry-run"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard population-exposure"
# rural-facilities
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard rural-facilities --dry-run"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard rural-facilities"
# built-up-area
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard built-up-area --dry-run"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard built-up-area"
# lulc
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard lulc --dry-run"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.runs.prepare_dashboard lulc"
```
**Success check:** population/rural/built-up/lulc each complete and report rows published.
**Any error → 🛑 STOP, paste output.**

🛑 **GATE E — POST the Step 4 summary and WAIT for `APPROVED: RUN` before Phase C.**

---

## STEP 5 — Phase C: Climate bundles (Telangana) — LONG (hours)

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & '.\tools\runs\refresh_dashboard_climate_bundles.ps1' -State Telangana -Level all -IncludeGeometry -IncludeContext"
```
- Runs for **hours** (block per-cell SPI gamma fit is the long pole). Do not interrupt.
- `-IncludeGeometry -IncludeContext` are intentional and **required** on a from-empty tree (both
  default off; omitting them leaves geometry shards / context bundle unwritten). Do not remove them.
- Do **not** add `-Overwrite` (redundant on an empty tree; it would only force redundant compute
  churn — it does NOT touch boundaries on this orchestrator).
- *If RAM thrashes:* `-Workers <n>` (default 36) is a tunable; the human may lower it. Don't change it
  yourself.

**Success check:** the run ends with a per-bundle PASS/FAIL table (5 thematic + 8 sector; Riverine is
Phase D). Report the full table.
- All bundles PASS → continue.
- One or more FAIL → do NOT retry. 🛑 STOP, paste the table + the failing bundle's log path
  (fail-isolation is by design; triage is the human's call).
- Run errors out with no table → 🛑 STOP, paste output.

---

## STEP 5b — Hydrology context summary (once, after first Phase C)

> **Why this is its own step:** the Hydrological Context card reads
> `processed_optimised/context/admin_hydro_summary.parquet` at runtime, and **nothing else builds
> it** — `prepare_dashboard` / the orchestrators do NOT call `build_admin_hydro_summary` (verified).
> It consumes the `context/{district,block}_{basin,subbasin}.parquet` files that Phase C's
> `-IncludeContext` just published, so it must run **after** Step 5. It is pan-India → run once
> (Maharashtra does not need a repeat). *(The Exposure Snapshot card's `admin_exposure_summary.parquet`
> is handled automatically — `prepare_dashboard` builds it inside each Phase E subcommand — so there
> is no separate exposure-summary step.)*

```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.pipeline.build_admin_hydro_summary --data-dir D:\projects\irt_data"
```
**Success check:** prints `wrote …admin_hydro_summary.parquet` (district + block rows). If it warns
"No rows produced — ensure district_basin.parquet exists under context/", the Phase C context publish
did not land → 🛑 STOP (do not re-run Phase C yourself; report it).

---

## STEP 6 — Phase D: Riverine flood (Telangana)

**First verify the JRC raster path (read-only, bash):**
```bash
ls -d "/mnt/d/projects/irt_data/Floodlayers_JRC-20260413T042625Z-3-001/Floodlayers_JRC"
ls "/mnt/d/projects/irt_data/Floodlayers_JRC-20260413T042625Z-3-001/Floodlayers_JRC" | grep -iE 'RP.*depth.*\.tif' | wc -l
```
**Success check:** the directory lists and the `.tif` count is > 0. **If missing or 0 → 🛑 STOP**
(do not guess another path — the rasters live one level **down**, in the nested `Floodlayers_JRC`).

**Then run:**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & '.\tools\runs\refresh_dashboard_riverine_flood_bundle.ps1' -State Telangana -JrcDir 'D:\projects\irt_data\Floodlayers_JRC-20260413T042625Z-3-001\Floodlayers_JRC'"
```
**Success check:** the audit step shows the 4 riverine metrics — `composite_flood_jrc_depth`,
`jrc_flood_depth_index_rp100`, `jrc_flood_extent_rp100`, `jrc_flood_depth_rp100` — published for
Telangana with a clean admin join.
**On any FAIL / traceback → 🛑 STOP, paste output.** Do not add `-Overwrite`.

---

## STEP 7 — Phase F: Final gates (read-only) — report, do not judge

Run each, paste full output. You are **reporting**, not deciding. (Groundwater parity intentionally
omitted this pass.)

**7.1 Parity audit, STRICT, validated set (one long single-line command — copy exactly):**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.optimized.audit_processed_optimised_parity --state Telangana --level all --strict --metric composite_heat_risk --metric composite_drought_risk --metric composite_flood_extreme_rainfall_risk --metric composite_heat_stress --metric composite_cold_risk --metric composite_flood_jrc_depth --metric jrc_flood_depth_index_rp100 --metric jrc_flood_extent_rp100 --metric jrc_flood_depth_rp100 --metric composite_agricultural_risk --metric composite_health_risk --metric composite_industrial_risk --metric composite_investment_financial_risk --metric composite_infrastructure_risk --metric composite_asset_risk_thermal_power --metric composite_asset_risk_hydropower --metric composite_life_livelihood_loss_risk"
```
Report exit code and whether it printed any `error`-severity issues.

**7.2 Roster completeness (both levels):**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.roster_audit --state Telangana --level district"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.roster_audit --state Telangana --level block"
```
Report exit code and any "missing"/"stale" counts.

**7.3 Admin-join geometry (pan-India):**
```bash
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'D:\projects\india_resilience_tool'; & 'C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe' -X utf8 -m tools.diagnostics.verify_admin_join_consistency"
```

**7.4 Compute-failure sidecar sweep (Windows-native — scoped to the new output trees):**
```bash
powershell.exe -NoProfile -Command "Get-ChildItem -Path 'D:\projects\irt_data\processed','D:\projects\irt_data\processed_optimised','D:\projects\india_resilience_tool\tools\runs' -Recurse -File -Include '*_compute_failures.json','*_partial_run.json' -ErrorAction SilentlyContinue | ForEach-Object { '{0}  {1} bytes' -f \$_.FullName, \$_.Length }"
```
> **Do NOT** use a WSL `find /mnt/d/projects/irt_data …` here: over the `drvfs` boundary it
> recursively stats the entire data root — the huge raw `r1i1p1f1/` tree **and** both
> `processed_bak/` + `processed_optimised_bak/` archives — and stalls for many minutes (the same
> trap as `du`). The scoped Windows-native scan above hits only the freshly built output trees +
> run logs, runs natively on D: (no drvfs penalty), and excludes the stale `_bak` sidecars.

Report every file found and its size. Any non-empty file = report it prominently (it is the GAP-7
"map renders but trend blanks" class).

🛑 **GATE F — POST a consolidated report of 7.1–7.4 and STOP.** Do not launch the dashboard, do not
declare "done," do not start Maharashtra.

> **Human-only finalization (not the executor's job):**
> - Dashboard click-path: `streamlit run main.py` → for Telangana confirm all districts/blocks render
>   (esp. the 6 LGD-renamed: Jagitial, Jayashankar Bhupalapally, Kumuram Bheem Asifabad, Suryapet,
>   Wanaparthy, Sangareddy), bundles render at full roster, riverine joins, context overlays show, no
>   hydro mode selectable, and **trend/time-series views populate** (not just the choropleth).
> - **Definition of done (Telangana):** 7.1 strict clean · 7.2 zero roster violations · 7.3 IoU ≥
>   0.999 · 7.4 no unexplained sidecars · click-path passes. Then sign off for Maharashtra.
> - **Open items to settle empirically at this point:** (a) how `--strict` scores a published-row
>   shortfall vs. a schema mismatch (decides whether the gw exclusion is strictly needed); (b) whether
>   the gate should also cover shared-global admin artifacts
>   (`audit_processed_optimised_parity --include-shared-global`).

---

## Next pass — Maharashtra

After Telangana is validated and **explicitly signed off** (`project-maharashtra-gated`):
Steps 1–4 (Phases 0/A/B/E) are already done pan-India — **only re-run the per-state loop**: Step 5
(C), Step 6 (D), Step 7 (F) with `Telangana` → `Maharashtra` in every command. Re-add groundwater
once its reconciliation lands. Watch for Maharashtra district-name aliases (CHG-0066) in the roster
audit.

---

## Explicitly DROPPED (do not run)

- `compute_indices_multiprocess --level basin|sub_basin` + all hydro masters/composites/yearly (hydro
  **mode** retired).
- All Aqueduct tools: `prepare_aqueduct_baseline`, `build_aqueduct_*_crosswalk`,
  `build_aqueduct_*_masters`, `validate_aqueduct_workflow`.
- `prepare_dashboard dashboard-package` (bundles aqueduct) — use the per-bundle drivers only.
- `tools/geodata/build_blocks_geojson` (superseded by `build_admin_boundaries_from_lgd`).
- Raw acquisition/prep (ERA5/NEX downloads, `data_acquisition`, raw `data_prep`) — raw is on disk.

---

## Reference notes (verified facts behind the steps)

- **`processed/_internal/` is a shared compute cache** (metric→model→grid cell), plus
  `spatial_weights/` and `source_inventory/`. The Step 1 rename sweeps it; Phase C repopulates the
  cells in each state's bbox and regenerates spatial weights. Cells are geographic → no cross-state
  contamination → per-state recompute is safe.
- **`build_spatial_weights` is an optional pre-warm, not a prereq.** The gridfirst compute
  auto-generates the cache on miss (`read_…` → compute → `write_…`); the sidecar is keyed by
  `boundary_file_hash`, so old pre-LGD weights auto-invalidate.
- **Sector composites are built entirely from climate metric slugs** — zero jrc/population/exposure
  deps → Phase C has no back-dependency on D or E (verified).
- **`-Overwrite` on the two `.ps1` drivers does NOT clobber boundaries** — neither references
  `build_blocks_geojson` (verified). That clobber risk is exclusive to `prepare_dashboard`, which
  this runbook avoids.
- `composite_agriculture_growing_conditions` is retired (not in the dashboard bundle set).

---

## Report format (use at each 🛑)

```
STEP: <n / name>
COMMAND(S): <what you ran>
RESULT: PASS | FAIL | NEEDS REVIEW
KEY OUTPUT: <exit codes, pass/fail tables, error lines, counts>
NOTES: <warnings, anything unexpected>
WAITING FOR: <the exact token, e.g. APPROVED: RUN>
```

**If anything is unclear, do not guess. 🛑 STOP and ask.** A wrong command on these trees costs hours.
