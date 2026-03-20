# Pytest Baseline Failures (Pre-Stabilization)

This file records the **current known failing tests** so dead-code purge batches
can enforce the rule: **do not introduce new failures** (even while the suite
is not yet green).

Command:
- `python -m pytest -q`

Baseline (29 failures):

- `india_resilience_tool/compute/tests/test_spi_adapter.py::TestComputeSPIForUnit::test_spi_for_unit_ssp_with_historical_calibration`
- `india_resilience_tool/compute/tests/test_spi_adapter.py::TestComputeSPIRowsClimateIndices::test_rows_format`
- `india_resilience_tool/compute/tests/test_spi_adapter.py::TestComputeSPIRowsClimateIndices::test_rows_block_level`
- `india_resilience_tool/compute/tests/test_spi_adapter.py::TestEdgeCases::test_handles_nan_in_data`
- `tests/test_app_details_panel.py::test_details_panel_exports_subrenderers`
- `tests/test_app_portfolio_ui.py::test_portfolio_ui_exports_subrenderers`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_count_days_above_threshold_all_above`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_count_days_above_threshold_step`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_count_days_ge_threshold_all_at`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_count_days_ge_threshold_step`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_count_days_below_threshold_all_below`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_tropical_nights_constant_warm`
- `tests/test_compute_indices_synthetic.py::TestTierBThresholdCounts::test_frost_days_constant_freezing`
- `tests/test_compute_indices_synthetic.py::TestTierFMultiVariable::test_dtr_varying_offset`
- `tests/test_compute_indices_synthetic.py::TestTierNCommentedOutMetrics::test_summer_days_su25_exists`
- `tests/test_compute_indices_synthetic.py::TestTierNCommentedOutMetrics::test_icing_days_exists`
- `tests/test_compute_indices_synthetic.py::TestTierOMultiYear::test_dtr_averaged_across_years`
- `tests/test_compute_indices_synthetic.py::TestTierQReferenceValidation::test_etccdi_fd_frost_days_reference`
- `tests/test_compute_indices_synthetic.py::TestTierQReferenceValidation::test_etccdi_su_summer_days_reference`
- `tests/test_compute_indices_synthetic.py::TestTierQReferenceValidation::test_etccdi_tr_tropical_nights_reference`
- `tests/test_compute_indices_synthetic.py::TestTierQReferenceValidation::test_percentile_symmetry`
- `tests/test_compute_indices_synthetic_comprehensive.py::TestTierBThresholdCounts::test_count_days_above_threshold_all_above`
- `tests/test_compute_indices_synthetic_comprehensive.py::TestTierBThresholdCounts::test_count_days_ge_threshold_boundary_inclusive`
- `tests/test_compute_indices_synthetic_comprehensive.py::TestBundleMetricSmokeCoverage::test_every_bundle_metric_is_exercised`
- `tests/test_metrics_registry.py::test_duplicate_detection_is_stable`
- `tests/test_metrics_registry.py::test_validate_registry_against_pipeline_reports_duplicates_but_no_mismatch`
- `tests/test_portfolio_tier1_guards.py::test_portfolio_ui_visualizations_are_lazy_opt_in`
- `tests/test_viz_charts.py::test_portfolio_heatmap_percentile_uses_risk_class_colorbar`
- `tests/test_viz_charts.py::test_create_trend_figure_for_index_smoke`

