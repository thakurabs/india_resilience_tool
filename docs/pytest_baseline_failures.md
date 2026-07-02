# Pytest Baseline Failures

This file records the **current known failing tests** so dead-code purge / lean-down
batches can enforce the rule: **do not introduce new failures** (even while the suite
is not yet green). If a listed failure starts passing, note it — a known defect may
have been resolved.

Command:
- `python -m pytest -q`

Interpreter note (this environment): the WSL `python` has no deps; the suite is run
with the Windows conda `irt` env
(`/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe`).

Baseline (14 failures) — last refreshed 2026-07-02 at `add_flood_depth@1e2d8e1`
(hydro lean-down Phase 5 docs + G11 river-pair). Full run: **14 failed, 1232 passed**.
All 14 are pre-existing environment / untracked-WIP / repo-text failures, unrelated to
the hydro lean-down work (verified: identical set at the pre-hydro baseline). The prior
`test_prepare_dashboard_runner` ×5 failures were resolved in Phase 3 (CHG-0172 folded in
the `processed-optimised-state-values` step fixes), dropping the count from 19 to 14.

## Environment: missing boundary data (8)

`test_optimized_bundle` builds exercise the optimized bundle against canonical boundary
GeoJSONs that are not present in this environment; pyogrio raises
`districts_4326.geojson: No such file or directory`.

- `tests/test_optimized_bundle.py::test_build_processed_optimised_writes_proposal_bundle_admin_masters`
- `tests/test_optimized_bundle.py::test_build_processed_optimised_overwrite_preserves_prior_level_outputs_and_rebuilds_manifest_inventory`
- `tests/test_optimized_bundle.py::test_build_processed_optimised_prune_scope_removes_selected_owned_roots_only`
- `tests/test_optimized_bundle.py::test_build_processed_optimised_state_scope_preserves_other_states_and_shared_globals`
- `tests/test_optimized_bundle.py::test_audit_processed_optimised_state_scope_does_not_write_global_report_by_default`
- `tests/test_optimized_bundle.py::test_audit_processed_optimised_state_scope_writes_only_explicit_scoped_report`
- `tests/test_optimized_bundle.py::test_list_optimized_yearly_metrics_reports_state_block_metrics`
- `tests/test_optimized_bundle.py::test_strict_audit_requires_block_yearly_models_when_ensemble_exists`

## Untracked WIP test (2)

`tests/test_admin_boundaries_block_placeholder.py` is an untracked work-in-progress test
that targets a not-yet-implemented block-name placeholder path (`KeyError:
'synthesized_block_names'`).

- `tests/test_admin_boundaries_block_placeholder.py::test_blank_block_name_is_recovered_via_placeholder`
- `tests/test_admin_boundaries_block_placeholder.py::test_blank_district_name_is_still_dropped`

## Repo-text / docs (1)

`test_no_emojis` scans repo text for emoji/disallowed non-ASCII glyphs. It flags
`docs/technical_guidance_note_review.md` (tracked, U+2705 checkmarks) plus untracked
working docs (`docs/perf_phase2_brief.md`, `docs/REGEN_RUNBOOK.md`). Not triggered by
`README.md` / `MANIFEST.md`.

- `tests/test_no_emojis.py::test_no_emoji_characters_in_repo_text`

## Other pre-existing (3)

Standing pre-existing failures unrelated to the lean-down; carried from before the
hydro work began.

- `tests/test_color_range_controls.py::test_single_unit_scale_value_preserves_floor_1`
- `tests/test_compute_indices_task_planning.py::test_metric_role_varnames_uses_level_aware_percentile_and_drought_baselines`
- `tests/test_heat_risk_gridfirst.py::test_gridfirst_threshold_day_metrics_cover_txge30_and_tropical_nights_gt25_without_baseline`
