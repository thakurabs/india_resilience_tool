from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.pipeline.publish_processed_outputs import publish_metric_root


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def test_publish_metric_root_writes_published_outputs(tmp_path: Path) -> None:
    source_metric_root = tmp_path / "processed" / "tas_annual_mean"
    dest_metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    _write_csv(source_metric_root / "Telangana" / "master_metrics_by_district.csv", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))
    _write_csv(
        source_metric_root / "Telangana" / "state_yearly_ensemble_stats_district.csv",
        pd.DataFrame({"scenario": ["ssp585"], "year": [2030], "ensemble_mean": [1.5]}),
    )
    _write_csv(
        source_metric_root / "Telangana" / "districts" / "ensembles" / "Alpha" / "ssp585" / "Alpha_yearly_ensemble.csv",
        pd.DataFrame({"year": [2030], "mean": [1.5]}),
    )

    stats = publish_metric_root(source_metric_root, dest_metric_root, timestamp="20260316T000000Z", dry_run=False)

    assert stats["csv_mirrors"] >= 3
    assert (dest_metric_root / "published" / "Telangana" / "master_metrics_by_district.csv").exists()
    assert (dest_metric_root / "published" / "Telangana" / "master_metrics_by_district.parquet").exists()
    assert (dest_metric_root / "published" / "Telangana" / "districts" / "ensembles" / "yearly").exists()
    assert not (source_metric_root / "published").exists()


def test_publish_metric_root_archives_replaced_targets(tmp_path: Path) -> None:
    source_metric_root = tmp_path / "processed" / "tas_annual_mean"
    dest_metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    _write_csv(source_metric_root / "Telangana" / "master_metrics_by_district.csv", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))
    published_master = dest_metric_root / "published" / "Telangana" / "master_metrics_by_district.csv"
    _write_csv(published_master, pd.DataFrame({"district": ["Old"], "value": [0.5]}))

    publish_metric_root(source_metric_root, dest_metric_root, timestamp="20260316T010000Z", dry_run=False)

    archived_master = dest_metric_root / "archive" / "20260316T010000Z" / "Telangana" / "master_metrics_by_district.csv"
    assert archived_master.exists()
    df = pd.read_csv(archived_master)
    assert df.loc[0, "district"] == "Old"


def test_publish_metric_root_dry_run_does_not_write(tmp_path: Path) -> None:
    source_metric_root = tmp_path / "processed" / "tas_annual_mean"
    dest_metric_root = tmp_path / "processed_parquet" / "tas_annual_mean"
    _write_csv(source_metric_root / "Telangana" / "master_metrics_by_district.csv", pd.DataFrame({"district": ["Alpha"], "value": [1.0]}))

    publish_metric_root(source_metric_root, dest_metric_root, timestamp="20260316T020000Z", dry_run=True)

    assert not (dest_metric_root / "published").exists()
