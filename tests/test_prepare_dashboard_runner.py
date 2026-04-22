from __future__ import annotations

import argparse
import io
import subprocess
from contextlib import redirect_stdout

import pytest

from tools.runs.prepare_dashboard import (
    _resolve_bundle_metrics,
    BundleRuntimeScope,
    ClimateLevelReadiness,
    ClimateRuntimeScope,
    DEFAULT_VALIDATION_TESTS,
    PlannedCommand,
    build_aqueduct_plan,
    build_blocks_geojson_plan,
    build_climate_hazards_plan,
    build_command_plan,
    build_dashboard_package_plan,
    build_groundwater_plan,
    build_jrc_flood_depth_plan,
    build_population_plan,
    build_cli,
    execute_plan,
    main,
)


def _climate_scope(*, levels: list[str], pending_by_level: dict[str, list[str]]) -> ClimateRuntimeScope:
    by_level: dict[str, ClimateLevelReadiness] = {}
    for level in levels:
        pending = tuple(pending_by_level.get(level, []))
        selected = tuple(pending_by_level.get(level, []))
        by_level[level] = ClimateLevelReadiness(
            level=level,
            selected_metrics=selected,
            runnable_metrics=selected,
            compute_pending_metrics=pending,
            masters_pending_metrics=pending,
            optimized_pending_metrics=pending,
            complete_metrics=tuple(),
            unrunnable_metrics=tuple(),
            unrunnable_reasons_by_metric={},
        )
    return ClimateRuntimeScope(levels=tuple(levels), by_level=by_level, global_issues=())


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
    assert "--prune-scope" not in plan[-2].argv
    assert "--full-rebuild" not in plan[-2].argv
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
    scope = _climate_scope(
        levels=["district", "block"],
        pending_by_level={
            "district": ["tas_annual_mean"],
            "block": ["tas_annual_mean"],
        },
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
        "composite-masters:district",
        "composite-masters:block",
        "processed-optimised-build:district+block",
        "processed-optimised-audit",
    ]
    assert any("--state" in step.argv for step in plan[:4])
    assert "--skip-existing" in plan[0].argv
    assert "--skip-existing" in plan[4].argv
    assert plan[6].argv[-1] == "--quiet"
    assert plan[7].argv[-1] == "--quiet"
    assert plan[8].argv.count("--level") == 2


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
    scope = _climate_scope(
        levels=["district", "block", "basin", "sub_basin"],
        pending_by_level={"district": ["tas_annual_mean"]},
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == ["processed-optimised-audit"]
    assert plan[0].argv.count("--level") == 4


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
    scope = _climate_scope(
        levels=["basin", "sub_basin"],
        pending_by_level={
            "basin": ["tas_annual_mean"],
            "sub_basin": ["tas_annual_mean"],
        },
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    assert [step.label for step in plan][-1] == "processed-optimised-audit"
    assert "processed-optimised-build" not in [step.label for step in plan]


def test_climate_hazards_skip_masters_skips_composite_stage() -> None:
    args = argparse.Namespace(
        level="admin",
        state=["Telangana"],
        metrics=["tas_annual_mean"],
        models=None,
        scenarios=None,
        workers=None,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=False,
        skip_masters=True,
        overwrite=False,
        audit_only=False,
        skip_optimised=True,
        skip_audit=False,
    )
    scope = _climate_scope(
        levels=["district", "block"],
        pending_by_level={"district": ["tas_annual_mean"], "block": ["tas_annual_mean"]},
    )

    plan = build_climate_hazards_plan(args, runtime_scope=scope)

    assert not any(step.label.startswith("composite-masters:") for step in plan)


def test_climate_hazards_overwrite_passes_flag_to_compute() -> None:
    args = argparse.Namespace(
        level="basin",
        state=None,
        metrics=["tas_annual_mean"],
        models=None,
        scenarios=None,
        workers=None,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=False,
        skip_masters=True,
        overwrite=True,
        audit_only=False,
        skip_optimised=True,
        skip_audit=False,
    )
    scope = _climate_scope(levels=["basin"], pending_by_level={"basin": ["tas_annual_mean"]})

    plan = build_climate_hazards_plan(args, runtime_scope=scope)

    assert plan[0].label == "climate-compute:basin"
    assert "--overwrite" in plan[0].argv
    assert "--skip-existing" not in plan[0].argv


def test_climate_hazards_overwrite_uses_single_optimised_rebuild_for_full_selected_scope() -> None:
    args = argparse.Namespace(
        level="all",
        state=None,
        metrics=["metric_a", "metric_b"],
        models=None,
        scenarios=None,
        workers=None,
        verbose=False,
        spi_legacy=False,
        spi_distribution=None,
        skip_compute=True,
        skip_masters=True,
        overwrite=True,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
    )
    scope = ClimateRuntimeScope(
        levels=("district", "block", "basin", "sub_basin"),
        by_level={
            "district": ClimateLevelReadiness(
                level="district",
                selected_metrics=("metric_a", "metric_b"),
                runnable_metrics=("metric_a", "metric_b"),
                compute_pending_metrics=("metric_a",),
                masters_pending_metrics=("metric_a",),
                optimized_pending_metrics=("metric_a",),
                complete_metrics=("metric_b",),
                unrunnable_metrics=(),
                unrunnable_reasons_by_metric={},
            ),
            "block": ClimateLevelReadiness(
                level="block",
                selected_metrics=("metric_a", "metric_b"),
                runnable_metrics=("metric_a", "metric_b"),
                compute_pending_metrics=("metric_b",),
                masters_pending_metrics=("metric_b",),
                optimized_pending_metrics=("metric_b",),
                complete_metrics=("metric_a",),
                unrunnable_metrics=(),
                unrunnable_reasons_by_metric={},
            ),
            "basin": ClimateLevelReadiness(
                level="basin",
                selected_metrics=("metric_a", "metric_b"),
                runnable_metrics=("metric_a", "metric_b"),
                compute_pending_metrics=(),
                masters_pending_metrics=(),
                optimized_pending_metrics=(),
                complete_metrics=("metric_a", "metric_b"),
                unrunnable_metrics=(),
                unrunnable_reasons_by_metric={},
            ),
            "sub_basin": ClimateLevelReadiness(
                level="sub_basin",
                selected_metrics=("metric_a", "metric_b"),
                runnable_metrics=("metric_a", "metric_b"),
                compute_pending_metrics=("metric_b",),
                masters_pending_metrics=("metric_b",),
                optimized_pending_metrics=("metric_b",),
                complete_metrics=("metric_a",),
                unrunnable_metrics=(),
                unrunnable_reasons_by_metric={},
            ),
        },
        global_issues=(),
    )

    plan = build_climate_hazards_plan(args, runtime_scope=scope)

    assert [step.label for step in plan] == [
        "processed-optimised-build:district+block+basin+sub_basin",
        "processed-optimised-audit",
    ]
    assert plan[0].argv.count("--level") == 4
    assert "--overwrite" in plan[0].argv
    assert "--prune-scope" not in plan[0].argv
    assert "--full-rebuild" not in plan[0].argv
    assert plan[0].argv.count("--metric") == 8
    assert "composite_heat_risk" in plan[0].argv
    assert "metric_a" in plan[0].argv
    assert "metric_b" in plan[0].argv


def test_climate_hazards_plans_only_sub_basin_when_basin_is_complete() -> None:
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
        skip_optimised=False,
        skip_audit=False,
    )
    scope = _climate_scope(
        levels=["basin", "sub_basin"],
        pending_by_level={"sub_basin": ["tas_annual_mean"]},
    )
    scope.by_level["basin"] = ClimateLevelReadiness(
        level="basin",
        selected_metrics=("tas_annual_mean",),
        runnable_metrics=("tas_annual_mean",),
        compute_pending_metrics=(),
        masters_pending_metrics=(),
        optimized_pending_metrics=(),
        complete_metrics=("tas_annual_mean",),
        unrunnable_metrics=(),
        unrunnable_reasons_by_metric={},
    )
    plan = build_climate_hazards_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == [
        "climate-compute:sub_basin",
        "climate-masters:sub_basin",
        "processed-optimised-build:sub_basin",
        "processed-optimised-audit",
    ]
    assert "--level" in plan[2].argv
    assert "sub_basin" in plan[2].argv


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
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: _climate_scope(
            levels=["basin", "sub_basin"],
            pending_by_level={
                "basin": ["tas_annual_mean"],
                "sub_basin": ["tas_annual_mean"],
            },
        ),
    )
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


def test_dashboard_package_with_jrc_requires_prefixed_inputs_unless_audit_only() -> None:
    parser = build_cli()
    args = parser.parse_args(["dashboard-package", "--include-jrc-flood-depth"])
    with pytest.raises(SystemExit, match="requires --jrc-source-dir and --jrc-assume-units m"):
        build_dashboard_package_plan(args)


def test_dashboard_package_audit_only_allows_missing_jrc_inputs(monkeypatch) -> None:
    parser = build_cli()
    args = parser.parse_args(["dashboard-package", "--include-jrc-flood-depth", "--audit-only"])
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: _climate_scope(levels=["district"], pending_by_level={"district": ["tas_annual_mean"]}),
    )
    scope_map = {
        "aqueduct": BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False),
        "population-exposure": BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False),
        "groundwater": BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False),
        "jrc-flood-depth": BundleRuntimeScope(
            selected_metrics=["jrc_flood_depth_rp10"],
            pending_metrics=["jrc_flood_depth_rp10"],
            has_global_issues=False,
        ),
    }
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_runtime_scope",
        lambda bundle, *_args, **_kwargs: scope_map[bundle],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_bundle_metrics",
        lambda bundle, _args: ["tas_annual_mean", "jrc_flood_depth_rp10"] if bundle == "dashboard-package" else [],
    )
    plan = build_dashboard_package_plan(args)
    assert [step.label for step in plan] == ["processed-optimised-audit"]


def test_dashboard_package_with_jrc_merges_scope_and_keeps_single_runtime_refresh(monkeypatch) -> None:
    parser = build_cli()
    args = parser.parse_args(
        [
            "dashboard-package",
            "--level",
            "admin",
            "--overwrite",
            "--include-jrc-flood-depth",
            "--jrc-source-dir",
            "/tmp/jrc",
            "--jrc-assume-units",
            "m",
        ]
    )
    scope_map = {
        "aqueduct": BundleRuntimeScope(selected_metrics=["aq_water_stress"], pending_metrics=["aq_water_stress"], has_global_issues=False),
        "population-exposure": BundleRuntimeScope(selected_metrics=["population_total"], pending_metrics=["population_total"], has_global_issues=False),
        "groundwater": BundleRuntimeScope(selected_metrics=["gw_stage_extraction_pct"], pending_metrics=["gw_stage_extraction_pct"], has_global_issues=False),
        "jrc-flood-depth": BundleRuntimeScope(
            selected_metrics=["jrc_flood_depth_rp10", "jrc_flood_depth_rp50"],
            pending_metrics=["jrc_flood_depth_rp10", "jrc_flood_depth_rp50"],
            has_global_issues=False,
        ),
    }
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: _climate_scope(
            levels=["district", "block"],
            pending_by_level={"district": ["tas_annual_mean"], "block": ["tas_annual_mean"]},
        ),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_runtime_scope",
        lambda bundle, *_args, **_kwargs: scope_map[bundle],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_bundle_metrics",
        lambda bundle, _args: [
            "tas_annual_mean",
            "aq_water_stress",
            "population_total",
            "gw_stage_extraction_pct",
            "jrc_flood_depth_rp10",
            "jrc_flood_depth_rp50",
        ]
        if bundle == "dashboard-package"
        else [],
    )
    plan = build_dashboard_package_plan(args)
    labels = [step.label for step in plan]
    assert labels.count("blocks-geojson") == 1
    assert "jrc-flood-depth-admin-masters" in labels
    assert labels.count("processed-optimised-build") == 1
    assert labels.count("processed-optimised-audit") == 1
    assert plan[labels.index("jrc-flood-depth-admin-masters")].argv.count("--dry-run") == 0


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


def test_jrc_bundle_builds_expected_steps_and_never_forwards_builder_dry_run() -> None:
    args = argparse.Namespace(
        overwrite=True,
        audit_only=False,
        skip_optimised=False,
        skip_audit=False,
        source_dir="/tmp/jrc",
        assume_units="m",
        districts_path=None,
        blocks_path=None,
        qa_dir=None,
        dry_run=True,
        plan_only=False,
    )
    scope = BundleRuntimeScope(
        selected_metrics=[
            "jrc_flood_extent_rp100",
            "jrc_flood_depth_rp10",
            "jrc_flood_depth_rp50",
            "jrc_flood_depth_rp100",
            "jrc_flood_depth_rp500",
        ],
        pending_metrics=[
            "jrc_flood_extent_rp100",
            "jrc_flood_depth_rp10",
            "jrc_flood_depth_rp50",
            "jrc_flood_depth_rp100",
            "jrc_flood_depth_rp500",
        ],
        has_global_issues=False,
    )
    plan = build_jrc_flood_depth_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == [
        "blocks-geojson",
        "jrc-flood-depth-admin-masters",
        "processed-optimised-build",
        "processed-optimised-audit",
    ]
    assert "--source-dir" in plan[1].argv
    assert "--assume-units" in plan[1].argv
    assert "--overwrite" in plan[1].argv
    assert plan[1].argv.count("--dry-run") == 0
    assert "--overwrite" not in plan[2].argv


def test_jrc_bundle_metric_resolution_includes_derived_index_slug() -> None:
    args = argparse.Namespace(include_jrc_flood_depth=False, level="admin", metric_slug=None)

    metrics = _resolve_bundle_metrics("jrc-flood-depth", args)

    assert metrics == [
        "jrc_flood_depth_index_rp100",
        "jrc_flood_extent_rp100",
        "jrc_flood_depth_rp10",
        "jrc_flood_depth_rp50",
        "jrc_flood_depth_rp100",
        "jrc_flood_depth_rp500",
    ]


def test_dashboard_package_jrc_scope_resolution_includes_derived_index_slug() -> None:
    args = argparse.Namespace(include_jrc_flood_depth=True, level="admin", metric_slug=None)

    metrics = _resolve_bundle_metrics("dashboard-package", args)

    assert "jrc_flood_depth_index_rp100" in metrics
    assert "jrc_flood_extent_rp100" in metrics


def test_jrc_bundle_requires_source_flags_for_plan_only() -> None:
    parser = build_cli()
    args = parser.parse_args(["jrc-flood-depth", "--plan-only"])
    with pytest.raises(SystemExit, match="requires --source-dir and --assume-units m"):
        build_command_plan(args)


def test_jrc_bundle_audit_only_does_not_require_source_flags() -> None:
    parser = build_cli()
    args = parser.parse_args(["jrc-flood-depth", "--audit-only"])
    scope = BundleRuntimeScope(
        selected_metrics=["jrc_flood_depth_rp10"],
        pending_metrics=["jrc_flood_depth_rp10"],
        has_global_issues=False,
    )
    plan = build_jrc_flood_depth_plan(args, runtime_scope=scope)
    assert [step.label for step in plan] == ["processed-optimised-audit"]


def test_jrc_step_passthrough_includes_builder_inputs_but_not_builder_dry_run() -> None:
    parser = build_cli()
    args = parser.parse_args(
        [
            "jrc-flood-depth-admin-masters",
            "--source-dir",
            "/tmp/jrc",
            "--assume-units",
            "m",
            "--districts-path",
            "/tmp/districts.geojson",
            "--blocks-path",
            "/tmp/blocks.geojson",
            "--qa-dir",
            "/tmp/qa",
            "--overwrite",
            "--dry-run",
        ]
    )
    plan = build_command_plan(args)
    assert [step.label for step in plan] == ["jrc-flood-depth-admin-masters"]
    assert "--source-dir" in plan[0].argv
    assert "--districts-path" in plan[0].argv
    assert "--blocks-path" in plan[0].argv
    assert "--qa-dir" in plan[0].argv
    assert "--overwrite" in plan[0].argv
    assert plan[0].argv.count("--dry-run") == 0


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


def test_execute_plan_returns_nonzero_and_prints_failed_step_summary(monkeypatch) -> None:
    plan = [PlannedCommand(label="climate-compute:basin", argv=["python", "-m", "example"])]

    def _raise_called_process_error(*_args, **_kwargs):
        raise subprocess.CalledProcessError(returncode=7, cmd=["python", "-m", "example"])

    monkeypatch.setattr("tools.runs.prepare_dashboard.subprocess.run", _raise_called_process_error)

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = execute_plan(plan, dry_run=False, plan_only=False)
    text = buf.getvalue()

    assert rc == 7
    assert "STEP FAILED [1/1] climate-compute:basin (exit=7)" in text


def test_main_returns_nonzero_when_climate_readiness_remains_incomplete(monkeypatch) -> None:
    scopes = [
        _climate_scope(levels=["basin", "sub_basin"], pending_by_level={"sub_basin": ["tas_annual_mean"]}),
        _climate_scope(levels=["basin", "sub_basin"], pending_by_level={"sub_basin": ["tas_annual_mean"]}),
    ]

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: scopes.pop(0),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 0,
    )

    rc = main(["climate-hazards", "--level", "hydro"])

    assert rc == 1


def test_main_skips_post_run_readiness_when_execute_plan_fails(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resolve_calls = 0

    def _resolve_scope(*_args, **_kwargs):
        nonlocal resolve_calls
        resolve_calls += 1
        if resolve_calls > 1:
            raise AssertionError("post-run readiness should be skipped after a failed step")
        return _climate_scope(levels=["basin", "sub_basin"], pending_by_level={"sub_basin": ["tas_annual_mean"]})

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        _resolve_scope,
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 2,
    )

    rc = main(["climate-hazards", "--level", "hydro"])

    captured = capsys.readouterr().out
    assert rc == 2
    assert resolve_calls == 1
    assert "POST-RUN CLIMATE READINESS" not in captured


def test_main_returns_zero_when_climate_readiness_becomes_complete(monkeypatch) -> None:
    scopes = [
        _climate_scope(levels=["basin", "sub_basin"], pending_by_level={"sub_basin": ["tas_annual_mean"]}),
        ClimateRuntimeScope(
            levels=("basin", "sub_basin"),
            by_level={
                "basin": ClimateLevelReadiness(
                    level="basin",
                    selected_metrics=("tas_annual_mean",),
                    runnable_metrics=("tas_annual_mean",),
                    compute_pending_metrics=(),
                    masters_pending_metrics=(),
                    optimized_pending_metrics=(),
                    complete_metrics=("tas_annual_mean",),
                    unrunnable_metrics=(),
                    unrunnable_reasons_by_metric={},
                ),
                "sub_basin": ClimateLevelReadiness(
                    level="sub_basin",
                    selected_metrics=("tas_annual_mean",),
                    runnable_metrics=("tas_annual_mean",),
                    compute_pending_metrics=(),
                    masters_pending_metrics=(),
                    optimized_pending_metrics=(),
                    complete_metrics=("tas_annual_mean",),
                    unrunnable_metrics=(),
                    unrunnable_reasons_by_metric={},
                ),
            },
            global_issues=(),
        ),
    ]

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: scopes.pop(0),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 0,
    )

    rc = main(["climate-hazards", "--level", "hydro"])

    assert rc == 0


def test_main_returns_zero_when_only_skipped_stage_pending_remains(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    scopes = [
        _climate_scope(levels=["basin"], pending_by_level={"basin": ["tas_annual_mean"]}),
        ClimateRuntimeScope(
            levels=("basin",),
            by_level={
                "basin": ClimateLevelReadiness(
                    level="basin",
                    selected_metrics=("tas_annual_mean",),
                    runnable_metrics=("tas_annual_mean",),
                    compute_pending_metrics=(),
                    masters_pending_metrics=("tas_annual_mean",),
                    optimized_pending_metrics=("tas_annual_mean",),
                    complete_metrics=(),
                    unrunnable_metrics=(),
                    unrunnable_reasons_by_metric={},
                )
            },
            global_issues=(),
        ),
    ]

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: scopes.pop(0),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 0,
    )

    rc = main(["climate-hazards", "--level", "basin", "--skip-masters", "--skip-optimised"])

    captured = capsys.readouterr().out
    assert rc == 0
    assert "POST-RUN CLIMATE READINESS" in captured
    assert "informational basin: masters_pending=1 (--skip-masters)" in captured
    assert "informational basin: optimized_pending=1 (--skip-optimised)" in captured


def test_main_keeps_compute_pending_blocking_even_when_later_stages_are_skipped(monkeypatch) -> None:
    scopes = [
        _climate_scope(levels=["basin"], pending_by_level={"basin": ["tas_annual_mean"]}),
        ClimateRuntimeScope(
            levels=("basin",),
            by_level={
                "basin": ClimateLevelReadiness(
                    level="basin",
                    selected_metrics=("tas_annual_mean",),
                    runnable_metrics=("tas_annual_mean",),
                    compute_pending_metrics=("tas_annual_mean",),
                    masters_pending_metrics=("tas_annual_mean",),
                    optimized_pending_metrics=("tas_annual_mean",),
                    complete_metrics=(),
                    unrunnable_metrics=(),
                    unrunnable_reasons_by_metric={},
                )
            },
            global_issues=(),
        ),
    ]

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: scopes.pop(0),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 0,
    )

    rc = main(["climate-hazards", "--level", "basin", "--skip-masters", "--skip-optimised"])

    assert rc == 1


def test_main_prints_post_run_failure_diagnostics(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    scopes = [
        _climate_scope(levels=["basin"], pending_by_level={"basin": ["tas_annual_mean"]}),
        ClimateRuntimeScope(
            levels=("basin",),
            by_level={
                "basin": ClimateLevelReadiness(
                    level="basin",
                    selected_metrics=("tas_annual_mean",),
                    runnable_metrics=("tas_annual_mean",),
                    compute_pending_metrics=("tas_annual_mean",),
                    masters_pending_metrics=("tas_annual_mean",),
                    optimized_pending_metrics=("tas_annual_mean",),
                    complete_metrics=(),
                    unrunnable_metrics=(),
                    unrunnable_reasons_by_metric={},
                )
            },
            global_issues=(),
        ),
    ]

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_runtime_scope",
        lambda *_args, **_kwargs: scopes.pop(0),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.build_climate_hazards_plan",
        lambda *args, **kwargs: [PlannedCommand(label="noop", argv=["python", "-m", "noop"])],
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard.execute_plan",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._collect_climate_failure_diagnostics",
        lambda *_args, **_kwargs: ("basin/tas_annual_mean/hydro: compute marker invalid for ACCESS-CM2/historical: compute_marker_output_count_mismatch",),
    )

    rc = main(["climate-hazards", "--level", "basin"])

    captured = capsys.readouterr().out
    assert rc == 1
    assert "POST-RUN CLIMATE FAILURE DETAILS" in captured
    assert "compute_marker_output_count_mismatch" in captured




def test_climate_runtime_scope_passes_filter_scope_to_ensemble_marker_validation(monkeypatch) -> None:
    parser = build_cli()
    args = parser.parse_args([
        "climate-hazards",
        "--level",
        "hydro",
        "--metrics",
        "tas_annual_mean",
        "--models",
        "ACCESS-CM2",
        "--scenarios",
        "historical",
    ])

    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_admin_states",
        lambda _state: ("Telangana",),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._resolve_climate_metrics_for_level",
        lambda _args, level: (["tas_annual_mean"], []),
    )
    monkeypatch.setattr(
        "tools.runs.prepare_dashboard._legacy_master_ready",
        lambda **_kwargs: True,
    )

    monkeypatch.setattr(
        "tools.optimized.build_processed_optimised.audit_processed_optimised_parity",
        lambda **_kwargs: {"issues": []},
    )

    class _Task:
        slug = "tas_annual_mean"

    class _Plan:
        tasks = [_Task()]
        skipped_reasons_by_metric = {}

    monkeypatch.setattr(
        "tools.pipeline.compute_indices_multiprocess.build_processing_task_plan",
        lambda **_kwargs: _Plan(),
    )
    monkeypatch.setattr(
        "tools.pipeline.compute_indices_multiprocess.task_completion_marker_valid",
        lambda _task: True,
    )

    calls: list[dict[str, object]] = []

    def _fake_ensemble_completion_marker_valid(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(
        "tools.pipeline.compute_indices_multiprocess.ensemble_completion_marker_valid",
        _fake_ensemble_completion_marker_valid,
    )

    from tools.runs.prepare_dashboard import _resolve_climate_runtime_scope

    scope = _resolve_climate_runtime_scope(args, levels=["basin"])

    readiness = scope.by_level["basin"]
    assert readiness.compute_pending_metrics == ()
    assert readiness.complete_metrics == ("tas_annual_mean",)
    assert len(calls) == 1
    assert calls[0]["allowed_models"] == ["ACCESS-CM2"]
    assert calls[0]["allowed_scenarios"] == ["historical"]


def test_validate_bundle_with_pytest_uses_default_targets() -> None:
    parser = build_cli()
    args = parser.parse_args(["validate", "--overwrite", "--include-pytest"])
    plan = build_command_plan(args)
    assert plan[-1].label == "pytest-validation"
    for test_path in DEFAULT_VALIDATION_TESTS:
        assert test_path in plan[-1].argv
