"""Unsigned production identity must bind every optimised shade file."""
from __future__ import annotations

import json

import pandas as pd
import pytest

from india_resilience_tool.data.wbgt_contract import (
    CARBONPLAN_SHADE_SIGNATURE, LEGACY_SHADE_IDENTITY, SHADE_METHOD_SIGNATURE,
    SHADE_SLUGS, shade_artifact_identity,
)
from tools.pipeline import build_shade_identity_sidecar as identity


def test_identity_record_hashes_all_masters_and_yearly_files(tmp_path, monkeypatch):
    monkeypatch.setattr(identity, "verify_source_code", lambda repo: {"sha256": "source"})
    monkeypatch.setattr(identity, "source_marker_evidence", lambda processed: {"sample": "verified"})
    bundle = tmp_path / "processed_optimised"
    for slug in SHADE_SLUGS:
        for part in ("masters", "yearly_models", "yearly_ensemble"):
            path = bundle / "metrics" / slug / part / "admin" / "district" / "state=Kerala.parquet"
            path.parent.mkdir(parents=True)
            path.write_bytes(f"{slug}/{part}".encode())
    processed = tmp_path / "processed"
    for slug in SHADE_SLUGS:
        path = processed / slug / "Kerala" / "master_metrics_by_district.csv"
        path.parent.mkdir(parents=True)
        path.write_text("state,district\nKerala,Wayanad\n", encoding="utf-8")
    record = identity.build_record(tmp_path, processed, bundle)
    assert record["optimized_file_count"] == 12
    assert record["processed_master_file_count"] == 4
    assert all(any(f"metrics/{slug}/masters/" in name for name in record["optimized_files"])
               for slug in SHADE_SLUGS)
    assert all(any(f"metrics/{slug}/yearly_models/" in name for name in record["optimized_files"])
               for slug in SHADE_SLUGS)

    target = identity.install(record, bundle, tmp_path / "backup")
    backup = tmp_path / "backup" / identity.SIDECAR
    assert identity.sha256(target) == identity.sha256(backup)
    assert json.loads(target.read_text())["method"] == identity.METHOD
    first = next(iter(record["optimized_files"]))
    assert shade_artifact_identity(bundle / first) == LEGACY_SHADE_IDENTITY
    first_master = next(iter(record["processed_master_files"]))
    assert shade_artifact_identity(processed / first_master) == LEGACY_SHADE_IDENTITY
    (bundle / first).write_bytes(b"changed")
    assert shade_artifact_identity(bundle / first) is None
    with pytest.raises(ValueError, match="Existing identity sidecar"):
        identity.install(record, bundle, tmp_path / "another_backup")


def test_hash_tree_rejects_missing_yearly_models(tmp_path):
    slug = next(iter(SHADE_SLUGS))
    path = tmp_path / "metrics" / slug / "masters" / "admin" / "district" / "one.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"master")
    with pytest.raises(ValueError, match="Incomplete optimized shade inventory"):
        identity.hash_tree(tmp_path, slug)


def test_signed_peak_and_carbonplan_artifacts_are_distinct(tmp_path):
    peak = tmp_path / "peak.csv"
    carbon = tmp_path / "carbon.csv"
    mixed = tmp_path / "mixed.csv"
    pd.DataFrame({"shade_method_signature": [SHADE_METHOD_SIGNATURE]}).to_csv(peak, index=False)
    pd.DataFrame({"shade_method_signature": [CARBONPLAN_SHADE_SIGNATURE]}).to_csv(carbon, index=False)
    pd.DataFrame({"shade_method_signature": [SHADE_METHOD_SIGNATURE,
                                              CARBONPLAN_SHADE_SIGNATURE]}).to_csv(mixed, index=False)
    assert shade_artifact_identity(peak) == SHADE_METHOD_SIGNATURE
    assert shade_artifact_identity(carbon) == CARBONPLAN_SHADE_SIGNATURE
    assert shade_artifact_identity(mixed) is None
