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
| `india_resilience_tool/app/dashboard.py` | Legacy wrapper around `run_app`; removed to enforce a single runtime chain. Evidence: only referenced by `app/main.py` + a smoke test; both updated. | low | delete (done) |
| `india_resilience_tool/app/orchestrator.py` | Legacy shim re-exporting `run_app`; removed to reduce runtime surface area. Evidence: only referenced by `app/dashboard.py` + a smoke test; both updated. | low | delete (done) |
| `india_resilience_tool/data/boundary_loader.py` | Evidence: `rg` shows no imports from runtime (`india_resilience_tool/`), tools (`tools/`), or tests (`tests/`). | low | delete (done) |
| `india_resilience_tool/analysis/case_study.py` | Evidence: `rg` shows no imports from runtime/tools/tests; functionality lives in `app/case_study_runtime.py` + `viz/exports.py`. | low | delete (done) |

## Candidates (symbol-level)

Symbol-level dead code is only marked when:
- it has **no references** (`rg`), and
- deleting it would not remove a documented public contract or a test-protected behavior.

### B3–B4 (symbol-level deletions applied)

Evidence standard:
- `rg` shows **no references** across `india_resilience_tool/`, `tools/`, `tests/`, and root entrypoints.
- Not part of the documented functionality contract (dashboard flows + tools).

| Candidate (file:symbol) | Evidence | Risk | Action |
|---|---|---:|---|
| `india_resilience_tool/app/portfolio_ui.py:render_comparison_table` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/sidebar.py:render_block_selector` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/sidebar.py:render_portfolio_quick_stats` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/sidebar.py:render_portfolio_mode_hint` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/sidebar.py:get_portfolio_summary` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/state.py:get_selected_unit` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/state.py:set_selected_unit` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/state.py:get_master_csv_key` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/state.py:get_portfolio_unit_key` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/views/map_view.py:create_portfolio_style_function` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/app/views/map_view.py:render_district_add_to_portfolio` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/metrics.py:rank_series_within_group` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/metrics.py:percentile_series_within_group` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/metrics.py:safe_apply_numeric` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/portfolio.py:PortfolioState` | `rg` shows only the definition/docstring; no instantiations. | low | delete (done) |
| `india_resilience_tool/analysis/portfolio.py:get_portfolio_district_keys` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/portfolio.py:get_portfolio_block_keys` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/timeseries.py:load_district_yearly_models` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/timeseries.py:load_block_yearly_models` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/analysis/timeseries.py:load_unit_yearly` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/adm3_loader.py:get_districts_with_blocks` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/adm3_loader.py:build_adm2_from_adm3` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/adm3_loader.py:build_adm1_from_adm3` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/adm3_loader.py:get_block_count_by_district` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/adm3_loader.py:get_block_count_by_state` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/merge.py:get_master_unit_column` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/merge.py:filter_merged_by_state` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/merge.py:filter_merged_by_district` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/merge.py:get_units_from_merged` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/data/merge.py:get_districts_from_merged` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/utils/processed_io.py:resolve_existing_table_path` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/utils/processed_io.py:write_parquet_dataset` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/utils/processed_io.py:write_parquet_file` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/viz/colors.py:build_vertical_gradient_legend_block_html` | `rg` shows only the definition; no imports/call sites. | low | delete (done) |
| `india_resilience_tool/viz/formatting.py:FormatSpec` | `rg` shows only the class definition; no imports/call sites. | low | delete (done) |
