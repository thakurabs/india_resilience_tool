from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout

from tools.runs.prepare_dashboard import (
    DEFAULT_VALIDATION_TESTS,
    PlannedCommand,
    build_aqueduct_plan,
    build_blocks_geojson_plan,
    build_climate_hazards_plan,
    build_command_plan,
    build_population_plan,
    build_cli,
    execute_plan,
)


def test_aqueduct_bundle_builds_expected_default_steps() -> None:
    args = argparse.Namespace(
        overwrite=True,
        prepare_baseline=False,
        source_gdb=None,
        baseline_csv=None,
        metric_slug=None,
        skip_validation=False,
    )
    plan = build_aqueduct_plan(args)
    labels = [step.label for step in plan]
    assert labels == [
        "blocks-geojson",
        "aqueduct-admin-crosswalk",
        "aqueduct-block-crosswalk",
        "aqueduct-hydro-crosswalk",
        "aqueduct-admin-masters",
        "aqueduct-hydro-masters",
        "aqueduct-validate",
    ]
    assert all("--overwrite" in step.argv for step in plan)


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
        build_aqueduct_plan(args)
    except SystemExit as exc:
        assert "requires both --source-gdb and --baseline-csv" in str(exc)
    else:
        raise AssertionError("Expected baseline-prep validation failure")


def test_climate_hazards_bundle_expands_admin_levels_and_states() -> None:
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
        skip_masters=False,
    )
    plan = build_climate_hazards_plan(args)
    labels = [step.label for step in plan]
    assert labels == [
        "climate-compute:district:Telangana",
        "climate-compute:district:Karnataka",
        "climate-compute:block:Telangana",
        "climate-compute:block:Karnataka",
        "climate-masters:district",
        "climate-masters:block",
    ]
    assert any("--state" in step.argv for step in plan[:4])
    assert plan[-1].argv[-1] == "--quiet"


def test_dashboard_package_combines_climate_aqueduct_and_pytest() -> None:
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
    plan = build_command_plan(args)
    labels = [step.label for step in plan]
    assert labels[0:5] == [
        "blocks-geojson",
        "climate-compute:basin",
        "climate-compute:sub_basin",
        "climate-masters:basin",
        "climate-masters:sub_basin",
    ]
    assert "population-admin-masters" in labels
    assert labels[-1] == "pytest-validation"


def test_population_bundle_builds_expected_step() -> None:
    args = argparse.Namespace(overwrite=True, population_raster=None)
    plan = build_population_plan(args)
    assert [step.label for step in plan] == ["blocks-geojson", "population-admin-masters"]
    assert plan[1].argv[1:] == ["-m", "tools.geodata.build_population_admin_masters", "--overwrite"]


def test_blocks_geojson_step_builds_expected_command() -> None:
    args = argparse.Namespace(overwrite=True)
    plan = build_blocks_geojson_plan(args)
    assert [step.label for step in plan] == ["blocks-geojson"]
    assert plan[0].argv[1:] == ["-m", "tools.geodata.build_blocks_geojson", "--overwrite"]


def test_execute_plan_dry_run_prints_commands_without_running() -> None:
    plan = [PlannedCommand(label="one", argv=["python", "-m", "example"])]
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = execute_plan(plan, dry_run=True)
    text = buf.getvalue()
    assert rc == 0
    assert "DRY-RUN one" in text
    assert "python -m example" in text


def test_validate_bundle_with_pytest_uses_default_targets() -> None:
    parser = build_cli()
    args = parser.parse_args(["validate", "--overwrite", "--include-pytest"])
    plan = build_command_plan(args)
    assert plan[-1].label == "pytest-validation"
    for test_path in DEFAULT_VALIDATION_TESTS:
        assert test_path in plan[-1].argv
