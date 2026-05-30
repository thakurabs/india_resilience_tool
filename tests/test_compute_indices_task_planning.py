from __future__ import annotations

import logging
from pathlib import Path

from india_resilience_tool.data.source_inventory import (
    InventoryYearRecord,
    SourceInventoryShard,
)
from tools.pipeline import compute_indices_multiprocess as CMP


def _record(
    path: Path,
    *,
    year: int,
    engine: str | None = "netcdf4",
    valid: bool = True,
    reason: str | None = None,
) -> InventoryYearRecord:
    return InventoryYearRecord(
        year=year,
        path=path,
        size=10,
        mtime_ns=year,
        engine=engine if valid else None,
        open_status=valid,
        validation_reason=reason or ("ok" if valid else "open_failed:ValueError"),
        var_present=valid,
    )


def _shard(
    *,
    scenario: str,
    varname: str,
    model: str,
    records: list[InventoryYearRecord],
) -> SourceInventoryShard:
    return SourceInventoryShard(
        schema_version=1,
        scenario=scenario,
        varname=varname,
        model=model,
        records=tuple(records),
        source_signature=f"{scenario}:{varname}:{model}:{len(records)}",
    )


def _configure_planner(
    monkeypatch,
    tmp_path: Path,
    *,
    metrics: list[dict],
    scenarios: dict[str, dict] | None = None,
    models: list[str] | None = None,
    path_map: dict[tuple[str, str, str], Path] | None = None,
) -> None:
    monkeypatch.setattr(CMP, "BASE_OUTPUT_ROOT", tmp_path / "processed")
    monkeypatch.setattr(CMP, "METRICS", metrics)
    monkeypatch.setattr(
        CMP,
        "SCENARIOS",
        scenarios
        or {
            "historical": {"subdir": "historical/tas", "periods": {}},
            "ssp245": {"subdir": "ssp245/tas", "periods": {}},
            "ssp585": {"subdir": "ssp585/tas", "periods": {}},
        },
    )
    monkeypatch.setattr(CMP, "_get_models", lambda: list(models or ["ModelA"]))
    monkeypatch.setattr(CMP, "_inventory_path_engines", {})

    def _fake_var_data_dir(_data_root: Path, scenario_subdir: str, varname: str, model: str) -> Path:
        key = (scenario_subdir.split("/")[0], varname, model)
        if path_map is not None and key in path_map:
            return path_map[key]
        return tmp_path / key[0] / varname / model

    monkeypatch.setattr(CMP, "var_data_dir", _fake_var_data_dir)


def test_build_processing_task_plan_reuses_source_inventory_per_unique_key(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    metrics = [
        {"slug": "tas_metric_a", "var": "tas"},
        {"slug": "tas_metric_b", "var": "tas"},
    ]
    _configure_planner(monkeypatch, tmp_path, metrics=metrics, scenarios={"historical": {"subdir": "historical/tas", "periods": {}}})

    data_dir = tmp_path / "historical" / "tas" / "ModelA"
    data_dir.mkdir(parents=True, exist_ok=True)
    year_path = data_dir / "2030.nc"
    year_path.touch()

    load_calls: list[tuple[str, str, str]] = []
    shard = _shard(
        scenario="historical",
        varname="tas",
        model="ModelA",
        records=[_record(year_path, year=2030)],
    )

    def _fake_loader(_cache_root: Path, *, data_dir: Path, scenario: str, varname: str, model: str, allow_write: bool, engines=()) -> SourceInventoryShard:
        load_calls.append((scenario, varname, model))
        assert allow_write is True
        assert data_dir == year_path.parent
        return shard

    monkeypatch.setattr(CMP, "load_or_refresh_inventory_shard", _fake_loader)

    with caplog.at_level(logging.INFO):
        plan = CMP.build_processing_task_plan(
            metrics_filter=["tas_metric_a", "tas_metric_b"],
            models_filter=["ModelA"],
            scenarios_filter=["historical"],
            level="district",
            state="Telangana",
        )

    assert [task.slug for task in plan.tasks] == ["tas_metric_a", "tas_metric_b"]
    assert load_calls == [("historical", "tas", "ModelA")]
    assert any("unique_source_availability_keys=1" in message for message in caplog.messages)
    assert any("tasks_built=2" in message for message in caplog.messages)
    assert CMP._inventory_path_engines[str(year_path.resolve())] == "netcdf4"


def test_build_processing_task_plan_missing_directory_is_no_available_years(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metrics = [{"slug": "tas_metric", "var": "tas"}]
    missing_dir = tmp_path / "missing" / "tas" / "ModelA"
    _configure_planner(
        monkeypatch,
        tmp_path,
        metrics=metrics,
        scenarios={"historical": {"subdir": "historical/tas", "periods": {}}},
        path_map={("historical", "tas", "ModelA"): missing_dir},
    )

    plan = CMP.build_processing_task_plan(
        metrics_filter=["tas_metric"],
        models_filter=["ModelA"],
        scenarios_filter=["historical"],
        level="district",
        state="Telangana",
    )

    assert plan.tasks == ()
    assert plan.skipped_reasons_by_metric["tas_metric"] == ("no_available_years",)
    assert plan.skipped_counts_by_reason["no_available_years"] == 1


def test_build_processing_task_plan_invalid_only_sources_are_flagged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metrics = [{"slug": "tas_metric", "var": "tas"}]
    _configure_planner(monkeypatch, tmp_path, metrics=metrics, scenarios={"historical": {"subdir": "historical/tas", "periods": {}}})

    data_dir = tmp_path / "historical" / "tas" / "ModelA"
    data_dir.mkdir(parents=True, exist_ok=True)
    bad_path = data_dir / "2030.nc"
    bad_path.touch()

    monkeypatch.setattr(
        CMP,
        "load_or_refresh_inventory_shard",
        lambda *_args, **_kwargs: _shard(
            scenario="historical",
            varname="tas",
            model="ModelA",
            records=[_record(bad_path, year=2030, valid=False)],
        ),
    )

    plan = CMP.build_processing_task_plan(
        metrics_filter=["tas_metric"],
        models_filter=["ModelA"],
        scenarios_filter=["historical"],
        level="district",
        state="Telangana",
    )

    assert plan.tasks == ()
    assert plan.skipped_reasons_by_metric["tas_metric"] == ("invalid_source_files",)
    assert plan.skipped_counts_by_reason["invalid_source_files"] == 1


def test_build_processing_task_plan_multi_var_without_overlap_is_no_common_years(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metrics = [{"slug": "combo_metric", "vars": ["tas", "pr"], "var": "tas"}]
    _configure_planner(monkeypatch, tmp_path, metrics=metrics, scenarios={"historical": {"subdir": "historical/tas", "periods": {}}})

    tas_dir = tmp_path / "historical" / "tas" / "ModelA"
    pr_dir = tmp_path / "historical" / "pr" / "ModelA"
    tas_dir.mkdir(parents=True, exist_ok=True)
    pr_dir.mkdir(parents=True, exist_ok=True)
    tas_path = tas_dir / "2030.nc"
    pr_path = pr_dir / "2031.nc"
    tas_path.touch()
    pr_path.touch()

    def _fake_loader(_cache_root: Path, *, data_dir: Path, scenario: str, varname: str, model: str, allow_write: bool, engines=()) -> SourceInventoryShard:
        if varname == "tas":
            return _shard(
                scenario=scenario,
                varname=varname,
                model=model,
                records=[_record(tas_path, year=2030)],
            )
        return _shard(
            scenario=scenario,
            varname=varname,
            model=model,
            records=[_record(pr_path, year=2031)],
        )

    monkeypatch.setattr(CMP, "load_or_refresh_inventory_shard", _fake_loader)

    plan = CMP.build_processing_task_plan(
        metrics_filter=["combo_metric"],
        models_filter=["ModelA"],
        scenarios_filter=["historical"],
        level="district",
        state="Telangana",
    )

    assert plan.tasks == ()
    assert plan.skipped_reasons_by_metric["combo_metric"] == ("no_common_years",)
    assert plan.skipped_counts_by_reason["no_common_years"] == 1


def test_build_processing_task_plan_caches_signatures_across_shared_historical_roles(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metrics = [
        {
            "slug": "pr_percentile",
            "var": "pr",
            "compute": "percentile_precipitation_total",
        },
        {
            "slug": "pr_djf_mean",
            "var": "pr",
            "compute": "seasonal_mean",
            "params": {"months": [12, 1, 2]},
        },
    ]
    _configure_planner(monkeypatch, tmp_path, metrics=metrics)

    scenario_paths = {}
    for scenario_name in ("historical", "ssp245", "ssp585"):
        data_dir = tmp_path / scenario_name / "pr" / "ModelA"
        data_dir.mkdir(parents=True, exist_ok=True)
        year_path = data_dir / "2030.nc"
        year_path.touch()
        scenario_paths[scenario_name] = year_path

    def _fake_loader(_cache_root: Path, *, data_dir: Path, scenario: str, varname: str, model: str, allow_write: bool, engines=()) -> SourceInventoryShard:
        return _shard(
            scenario=scenario,
            varname=varname,
            model=model,
            records=[_record(scenario_paths[scenario], year=2030)],
        )

    signature_calls: list[tuple[str, tuple[str, ...]]] = []

    def _fake_inventory_signature_for_role(*, role_name: str, shards: dict[str, SourceInventoryShard]) -> str:
        signature_calls.append((role_name, tuple(sorted(shards))))
        return f"{role_name}:{','.join(sorted(shards))}"

    monkeypatch.setattr(CMP, "load_or_refresh_inventory_shard", _fake_loader)
    monkeypatch.setattr(CMP, "_inventory_signature_for_role", _fake_inventory_signature_for_role)

    plan = CMP.build_processing_task_plan(
        metrics_filter=["pr_percentile", "pr_djf_mean"],
        models_filter=["ModelA"],
        scenarios_filter=["ssp245", "ssp585"],
        level="district",
        state="Telangana",
    )

    assert len(plan.tasks) == 4
    assert signature_calls == [
        ("eval", ("pr",)),
        ("baseline", ("pr",)),
        ("historical_prev_dec", ("pr",)),
        ("eval", ("pr",)),
    ]
    percentile_task = next(task for task in plan.tasks if task.slug == "pr_percentile" and task.scenario == "ssp245")
    djf_task = next(task for task in plan.tasks if task.slug == "pr_djf_mean" and task.scenario == "ssp245")
    assert percentile_task.source_signatures["baseline"] == "baseline:pr"
    assert djf_task.source_signatures["historical_prev_dec"] == "historical_prev_dec:pr"
    assert percentile_task.source_signatures["baseline"] != djf_task.source_signatures["historical_prev_dec"]


def test_build_processing_task_plan_populates_inventory_engines_from_cached_shards(
    tmp_path: Path,
    monkeypatch,
) -> None:
    metrics = [{"slug": "tas_metric", "var": "tas"}]
    _configure_planner(monkeypatch, tmp_path, metrics=metrics, scenarios={"historical": {"subdir": "historical/tas", "periods": {}}})

    data_dir = tmp_path / "historical" / "tas" / "ModelA"
    data_dir.mkdir(parents=True, exist_ok=True)
    year_path = data_dir / "2030.nc"
    year_path.touch()

    monkeypatch.setattr(
        CMP,
        "load_or_refresh_inventory_shard",
        lambda *_args, **_kwargs: _shard(
            scenario="historical",
            varname="tas",
            model="ModelA",
            records=[_record(year_path, year=2030, engine="h5netcdf")],
        ),
    )

    CMP.build_processing_task_plan(
        metrics_filter=["tas_metric"],
        models_filter=["ModelA"],
        scenarios_filter=["historical"],
        level="district",
        state="Telangana",
    )

    assert CMP._inventory_path_engines[str(year_path.resolve())] == "h5netcdf"
