from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.pipeline.publish_processed_outputs import publish_metric_root


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_publish_metric_root_writes_published_outputs(tmp_path: Path) -> None:
    metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    build_root = metric_root / "build"
    _write_parquet(build_root / "Telangana" / "master_metrics_by_district.parquet", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))
    _write_parquet(
        build_root / "Telangana" / "state_yearly_ensemble_stats_district.parquet",
        pd.DataFrame({"scenario": ["ssp585"], "year": [2030], "ensemble_mean": [1.5]}),
    )
    _write_parquet(
        build_root / "Telangana" / "districts" / "ensembles" / "yearly" / "scenario=ssp585" / "part.parquet",
        pd.DataFrame({"district": ["Alpha"], "scenario": ["ssp585"], "year": [2030], "mean": [1.5]}),
    )

    stats = publish_metric_root(metric_root, timestamp="20260316T000000Z", dry_run=False)

    assert stats["copied_files"] >= 3
    assert (metric_root / "published" / "Telangana" / "master_metrics_by_district.parquet").exists()
    assert (metric_root / "published" / "Telangana" / "districts" / "ensembles" / "yearly").exists()
    assert not (metric_root / "published" / "Telangana" / "master_metrics_by_district.csv").exists()


def test_publish_metric_root_archives_replaced_targets(tmp_path: Path) -> None:
    metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    _write_parquet(metric_root / "build" / "Telangana" / "master_metrics_by_district.parquet", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))
    published_master = metric_root / "published" / "Telangana" / "master_metrics_by_district.parquet"
    _write_parquet(published_master, pd.DataFrame({"district": ["Old"], "value": [0.5]}))

    publish_metric_root(metric_root, timestamp="20260316T010000Z", dry_run=False)

    archived_master = metric_root / "archive" / "20260316T010000Z" / "Telangana" / "master_metrics_by_district.parquet"
    assert archived_master.exists()
    df = pd.read_parquet(archived_master)
    assert df.loc[0, "district"] == "Old"


def test_publish_metric_root_dry_run_does_not_write(tmp_path: Path) -> None:
    metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    _write_parquet(metric_root / "build" / "Telangana" / "master_metrics_by_district.parquet", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))

    publish_metric_root(metric_root, timestamp="20260316T020000Z", dry_run=True)

    assert not (metric_root / "published").exists()
