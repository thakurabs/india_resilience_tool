from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout

from tools.runs.prepare_dashboard import (
    BundleRuntimeScope,
    DEFAULT_VALIDATION_TESTS,
    PlannedCommand,
    build_aqueduct_plan,
    build_blocks_geojson_plan,
    build_climate_hazards_plan,
    build_command_plan,
    build_dashboard_package_plan,
    build_groundwater_plan,
    build_population_plan,
    build_cli,
    execute_plan,
)


def test_aqueduct_bundle_builds_expected_default_steps() -> None:
    args = argparse.Namespace(
        overwrite=True,
        plan_only=False,
        dry_run=False,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
        prepare_baseline=False,
        source_gdb=None,
        baseline_csv=None,
        metric_slug=None,
        skip_validation=False,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["aq_water_stress"],
        pending_metrics=["aq_water_stress"],
        has_global_issues=False,
    )
    plan = build_aqueduct_plan(args, runtime_scope=scope)
    labels = [step.label for step in plan]
    assert labels == [
        "blocks-geojson",
        "aqueduct-admin-crosswalk",
        "aqueduct-block-crosswalk",
        "aqueduct-hydro-crosswalk",
        "aqueduct-admin-masters",
        "aqueduct-hydro-masters",
        "aqueduct-validate",
        "processed-optimised-build",
        "processed-optimised-audit",
    ]
    assert "--overwrite" in plan[-2].argv
    assert "--metric" in plan[-2].argv
    assert "--skip-audit" in plan[-2].argv


def test_aqueduct_bundle_requires_inputs_when_prepare_baseline_enabled() -> None:
    args = argparse.Namespace(
        overwrite=True,
        prepare_baseline=True,
        source_gdb=None,
        baseline_csv=None,
        metric_slug=None,
        skip_validation=False,
    )
    try:
        build_aqueduct_plan(args, include_runtime=False)
    except SystemExit as exc:
        assert "requires both --source-gdb and --baseline-csv" in str(exc)
    else:
        raise AssertionError("Expected baseline-prep validation failure")


def test_climate_hazards_bundle_expands_admin_levels_and_adds_runtime_steps() -> None:
    args = argparse.Namespace(
        level="admin",
        state=["Telangana", "Karnataka"],
        metrics=["tas_annual_mean"],
        models=None,
        scenarios=None,
        workers=4,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=False,
        skip_masters=False,
        overwrite=False,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["tas_annual_mean"],
        pending_metrics=["tas_annual_mean"],
        has_global_issues=False,
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    labels = [step.label for step in plan]
    assert labels == [
        "climate-compute:district:Telangana",
        "climate-compute:district:Karnataka",
        "climate-compute:block:Telangana",
        "climate-compute:block:Karnataka",
        "climate-masters:district",
        "climate-masters:block",
        "processed-optimised-build",
        "processed-optimised-audit",
    ]
    assert any("--state" in step.argv for step in plan[:4])
    assert "--skip-existing" in plan[4].argv
    assert plan[5].argv[-1] == "--quiet"


def test_climate_hazards_audit_only_only_runs_audit() -> None:
    args = argparse.Namespace(
        level="all",
        state=None,
        metrics=["tas_annual_mean"],
        models=None,
        scenarios=None,
        workers=None,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=False,
        skip_masters=False,
        overwrite=False,
        audit_only=True,
        skip_optimised=False,
        skip_audit=False,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["tas_annual_mean"],
        pending_metrics=["tas_annual_mean"],
        has_global_issues=False,
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == ["processed-optimised-audit"]


def test_climate_hazards_skip_optimised_removes_only_build_stage() -> None:
    args = argparse.Namespace(
        level="hydro",
        state=None,
        metrics=["tas_annual_mean"],
        models=None,
        scenarios=None,
        workers=None,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=False,
        skip_masters=False,
        overwrite=False,
        audit_only=False,
        skip_optimised=True,
        skip_audit=False,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["tas_annual_mean"],
        pending_metrics=["tas_annual_mean"],
        has_global_issues=False,
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    assert [step.label for step in plan][-1] == "processed-optimised-audit"
    assert "processed-optimised-build" not in [step.label for step in plan]


def test_dashboard_package_combines_bundle_stages_and_single_runtime_refresh(monkeypatch) -> None:
    parser = build_cli()
    args = parser.parse_args(
        [
            "dashboard-package",
            "--level",
            "hydro",
            "--overwrite",
            "--include-pytest",
        ]
    )
    scope_map = {
        "climate-hazards": BundleRuntimeScope(
            selected_metrics=["tas_annual_mean"],
            pending_metrics=["tas_annual_mean"],
            has_global_issues=False,
        ),
        "aqueduct": BundleRuntimeScope(
            selected_metrics=["aq_water_stress"],
            pending_metrics=["aq_water_stress"],
            has_global_issues=False,
        ),
        "population-exposure": BundleRuntimeScope(
            selected_metrics=["population_total"],
            pending_metrics=["population_total"],
            has_global_issues=False,
        ),
        "groundwater": BundleRuntimeScope(
            selected_metrics=["gw_stage_extraction_pct"],
            pending_metrics=["gw_stage_extraction_pct"],
            has_global_issues=False,
        ),
    }
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_runtime_scope",
        lambda bundle, *_args, **_kwargs: scope_map[bundle],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_bundle_metrics",
        lambda bundle, _args: ["tas_annual_mean", "aq_water_stress", "population_total", "gw_stage_extraction_pct"]
        if bundle == "dashboard-package"
        else [],
    )
    plan = build_dashboard_package_plan(args)
    labels = [step.label for step in plan]
    assert labels[0:5] == [
        "blocks-geojson",
        "climate-compute:basin",
        "climate-compute:sub_basin",
        "climate-masters:basin",
        "climate-masters:sub_basin",
    ]
    assert labels.count("processed-optimised-build") == 1
    assert labels.count("processed-optimised-audit") == 1
    assert "population-admin-masters" in labels
    assert "groundwater-district-masters" in labels
    assert labels[-1] == "pytest-validation"


def test_population_bundle_builds_expected_steps() -> None:
    args = argparse.Namespace(
        overwrite=True,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
        population_raster=None,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["population_total", "population_density"],
        pending_metrics=["population_total", "population_density"],
        has_global_issues=False,
    )
    plan = build_population_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == [
        "blocks-geojson",
        "population-admin-masters",
        "processed-optimised-build",
        "processed-optimised-audit",
    ]


def test_groundwater_bundle_builds_expected_steps() -> None:
    args = argparse.Namespace(
        overwrite=True,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
        groundwater_workbook=None,
        groundwater_alias_csv=None,
    )
    scope = BundleRuntimeScope(
        selected_metrics=["gw_stage_extraction_pct"],
        pending_metrics=["gw_stage_extraction_pct"],
        has_global_issues=False,
    )
    plan = build_groundwater_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == [
        "groundwater-district-masters",
        "processed-optimised-build",
        "processed-optimised-audit",
    ]


def test_blocks_geojson_step_builds_expected_command() -> None:
    args = argparse.Namespace(overwrite=True)
    plan = build_blocks_geojson_plan(args)
    assert [step.label for step in plan] == ["blocks-geojson"]
    assert plan[0].argv[1:] == ["-m", "tools.geodata.build_blocks_geojson", "--overwrite"]


def test_execute_plan_dry_run_prints_commands_without_running() -> None:
    plan = [PlannedCommand(label="one", argv=["python", "-m", "example"])]
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = execute_plan(plan, dry_run=True, plan_only=False)
    text = buf.getvalue()
    assert rc == 0
    assert "DRY-RUN one" in text
    assert "python -m example" in text


def test_execute_plan_plan_only_uses_plan_prefix() -> None:
    plan = [PlannedCommand(label="one", argv=["python", "-m", "example"])]
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = execute_plan(plan, dry_run=False, plan_only=True)
    text = buf.getvalue()
    assert rc == 0
    assert "PLAN one" in text


def test_validate_bundle_with_pytest_uses_default_targets() -> None:
    parser = build_cli()
    args = parser.parse_args(["validate", "--overwrite", "--include-pytest"])
    plan = build_command_plan(args)
    assert plan[-1].label == "pytest-validation"
    for test_path in DEFAULT_VALIDATION_TESTS:
        assert test_path in plan[-1].argv
