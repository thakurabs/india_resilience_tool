# CarbonPlan shade WBGT — operator runbook (CHG-0700..0703)

Every command runs from the repository root. `PY` is the app's Windows interpreter,
`C:\Users\22015611\AppData\Local\miniconda3\envs\irt\python.exe`; `CP` is the pinned
CarbonPlan environment, `/tmp/irt-carbonplan-py310/bin/python` (WSL). Environment
variables do not cross from WSL to Windows, so staged builds run through the `.bat`
wrappers in `scratch/wbgt_shade_carbonplan_release_ops/`, each wrapped in `setlocal`.

Steps 1–8 never write production. Steps 9, 11 and 13 do, and each needs its own
explicit go-ahead.

| # | Step | Command | Writes | Gate artifact |
|---|---|---|---|---|
| 1 | Stage 18 models from the signed Route B QDM cache | `CP -B -m tools.pipeline.build_carbonplan_shade_release --stage scratch/wbgt_shade_carbonplan_release --run` | `scratch/` | per-model `status.json` (PASS, hashed outputs) |
| 2 | Holdout scoring (2000–2010, out-of-sample) | `PY -m tools.diagnostics.wbgt_shade_cp_score_run --bundle D:\projects\irt_data\processed_optimised --evidence scratch/wbgt_shade_carbonplan_score --run` | `scratch/` | `gate.json`; verdict ruled on in [HOLDOUT_RULING.md](HOLDOUT_RULING.md) |
| 3 | Sun-adjustment end-to-end control (ACCESS-CM2) | `CP -B -m tools.diagnostics.wbgt_shade_cp_sun_control --evidence scratch/wbgt_shade_carbonplan_sun_control --run` | `scratch/` | `control.json` PASS |
| 4 | Staged processed tree | `PY -m tools.pipeline.publish_carbonplan_shade --run --processed-root C:\irt_shade_s1` | `C:\irt_shade_s1` | `carbonplan_shade_release_manifest.json` |
| 5 | Staged optimised bundle (geometry **and context** copied from production first; without context the step-6 audit reports 23 context/geometry errors) | `build_shade_stage.bat C:\irt_shade_s1 C:\irt_shade_bundle` then `state_values_shade_stage.bat C:\irt_shade_s1 C:\irt_shade_bundle --strict` | `C:\irt_shade_bundle` | build log |
| 6 | Staged strict parity audit, four shade slugs | `PY -m tools.optimized.audit_processed_optimised_parity --metric <each shade slug> --strict --report-path <evidence>` with `IRT_PROCESSED_ROOT=C:\irt_shade_s1`, `IRT_PROCESSED_OPTIMISED_ROOT=C:\irt_shade_bundle` | evidence only | `issue_count: 0` |
| 7 | Full test suite against the recorded baseline | `PY -m pytest -q -p no:cacheprovider` with no `IRT_*` overrides | — | zero failures outside [tests_baseline.json](tests_baseline.json) |
| 8 | Staged dashboard check | copy production `colour_scale.json`; run `junction_metrics.bat` (the Heat Stress domain is offered only when its composite is in the bundle); `verify_shade_streamlit.bat C:\irt_shade_bundle C:\irt_shade_s1 --expect-identity carbonplan-shade-gridfirst-qdm-v1 --label staged`; then **`unjunction_metrics.bat` before step 10** | — | shade metrics load with CarbonPlan identity; blank units explained |
| 9 | **Install the legacy identity sidecar** (production write: one file) | `CP -m tools.pipeline.build_shade_identity_sidecar --processed /mnt/d/projects/irt_data/processed --bundle /mnt/d/projects/irt_data/processed_optimised --backup-dir scratch/wbgt_shade_carbonplan_release_ops/identity_backup --run` | `processed_optimised/shade_method_identity.json` | sidecar + backup hashes |
| 10 | Prepare cutover (copies staged dirs onto `D:`, times the manifest rescan, refuses > 30 min) | `PY -m tools.pipeline.cutover_carbonplan_shade prepare --release-id <id> --staged-bundle C:\irt_shade_bundle --run` | `D:\projects\irt_data\_shade_cutover\<id>\` | `prepare.json` PASS |
| 11 | **Stop the dashboard, swap, restart** (production write) | `PY -m tools.pipeline.cutover_carbonplan_shade swap --release-id <id> --run` | four optimised dirs, four processed dirs moved aside, manifest | `journal.json` PUBLISHED; rollback window opens |
| 12 | Scoped cutover audit, then whole-bundle audit outside downtime | step 6 against production with `IRT_PROCESSED_ROOT=C:\irt_shade_s1`; full audit later | evidence; `parity_report.json` only on the full audit | `issue_count: 0` |
| 13 | Rollback if needed (any time before closing) | `PY -m tools.pipeline.cutover_carbonplan_shade rollback --release-id <id> --run` | reverses the journal, restores shared files | `journal.json` ROLLED_BACK, legacy hashes re-verified |
| 14 | Close the rollback window (≥ 7 days **and** sign-off) | `PY -m tools.pipeline.cutover_carbonplan_shade close --release-id <id> --signed-off-by "<name>" --run` | journal only | `rollback_window_closed: true` |
| 15 | After closing: narrow readers to CarbonPlan, retire the sidecar and backups, lift the routine-build shade exclusion, regenerate the technical-note HTML | separate reviewed change | — | — |

## Rules that hold until step 14

- **A junction writes through to production.** While step 8's junctions exist, run
  no build of any kind against `C:\irt_shade_bundle`. Remove them with
  `unjunction_metrics.bat` (`rmdir` on a junction deletes only the link), never
  with `rm -rf` from WSL.

- Routine `build_processed_optimised` and `build_state_values` runs skip the shade
  slugs. Rebuilding them would change the bytes the legacy sidecar pins, and a
  rollback would then restore files the dashboard refuses to show.
- `--processed-mode aside` (default) moves the legacy processed shade trees into
  the backup and leaves no processed shade tree in production, as with Route B.
  The CarbonPlan processed tree stays at `C:\irt_shade_s1`, which is what the
  audits compare against. `--processed-mode replace` also copies the ~3.5 million
  staged processed files onto `D:` during `prepare`; at the measured 36 small
  files/s that copy takes more than a day, though none of it falls inside downtime.
- The swap's first rename fails if the dashboard still holds a shade file open; the
  journal then shows nothing moved, or `rollback` reverses what did.
