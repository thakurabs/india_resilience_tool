"""Unit tests for nex_india_subset_download_s3_v2.

No live S3. The S3 paginator is mocked; xr.open_dataset is monkeypatched
where we need to exercise the NetCDF lifecycle.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

from tools.data_acquisition import nex_india_subset_download_s3_v2 as v2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_s3_with_pages(pages):
    """Return a MagicMock that mimics boto3 client + paginator returning `pages`."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    s3.get_paginator.return_value = paginator
    return s3


def _key(model, exp, member, var, year, grid="gn"):
    return (
        f"NEX-GDDP-CMIP6/{model}/{exp}/{member}/{var}/"
        f"{var}_day_{model}_{exp}_{member}_{grid}_{year}.nc"
    )


# ---------------------------------------------------------------------------
# 1. Scope listing parses years
# ---------------------------------------------------------------------------


def test_list_year_keys_for_scope_parses_years():
    pages = [
        {
            "Contents": [
                {"Key": _key("MODEL", "historical", "r1i1p1f1", "pr", 2000)},
                {"Key": _key("MODEL", "historical", "r1i1p1f1", "pr", 2001)},
                {"Key": "NEX-GDDP-CMIP6/MODEL/historical/r1i1p1f1/pr/unrelated.txt"},
            ]
        }
    ]
    s3 = _mock_s3_with_pages(pages)
    year_to_key, duplicates = v2.list_year_keys_for_scope(
        s3, "MODEL", "historical", "r1i1p1f1", "pr"
    )
    assert set(year_to_key.keys()) == {2000, 2001}
    assert year_to_key[2000].endswith("_2000.nc")
    assert year_to_key[2001].endswith("_2001.nc")
    assert duplicates == []


# ---------------------------------------------------------------------------
# 2. Duplicate-year detection
# ---------------------------------------------------------------------------


def test_list_year_keys_for_scope_flags_duplicate_years():
    pages = [
        {
            "Contents": [
                {"Key": _key("MODEL", "historical", "r1i1p1f1", "pr", 2000, grid="gn")},
                {"Key": _key("MODEL", "historical", "r1i1p1f1", "pr", 2000, grid="gr1")},
                {"Key": _key("MODEL", "historical", "r1i1p1f1", "pr", 2001)},
            ]
        }
    ]
    s3 = _mock_s3_with_pages(pages)
    year_to_key, duplicates = v2.list_year_keys_for_scope(
        s3, "MODEL", "historical", "r1i1p1f1", "pr"
    )
    assert 2000 not in year_to_key
    assert 2001 in year_to_key
    assert len(duplicates) == 1
    dup_year, dup_keys = duplicates[0]
    assert dup_year == 2000
    assert len(dup_keys) == 2


# ---------------------------------------------------------------------------
# 3. Manifest: year intersection + skip existing + missing
# ---------------------------------------------------------------------------


def test_build_manifest_skips_existing_intersects_years_and_flags_missing(tmp_path):
    # Scope listing returns 2000 & 2001 only.
    def scope_lister(s3, model, exp, member, variable):
        return ({2000: _key(model, exp, member, variable, 2000),
                 2001: _key(model, exp, member, variable, 2001)}, [])

    # Seed an existing file at 2000.nc.
    out_path_2000 = v2._out_path_for(
        tmp_path, "r1i1p1f1_panIndia", "historical", "pr", "MODEL", 2000
    )
    out_path_2000.parent.mkdir(parents=True, exist_ok=True)
    out_path_2000.write_bytes(b"\x00" * 64)

    tasks, decisions, summary, scope_failures = v2.build_manifest(
        s3=MagicMock(),
        out_dir=tmp_path,
        member="r1i1p1f1",
        member_dir="r1i1p1f1_panIndia",
        variables=["pr"],
        experiments=["historical"],
        models=["MODEL"],
        user_years={2000, 2001, 2002},  # 2002 not in S3 listing → missing
        skip_existing=True,
        verify=False,
        delete_bad=False,
        scope_lister=scope_lister,
    )

    assert summary.skipped_existing == 1
    assert summary.missing_on_s3 == 1
    assert len(tasks) == 1
    assert tasks[0].year == 2001
    assert scope_failures == []
    statuses = {d.year: d.local_status for d in decisions}
    assert statuses[2000] == v2.LocalStatus.SKIP_GOOD
    assert statuses[2001] == v2.LocalStatus.ENQUEUE_FRESH


# ---------------------------------------------------------------------------
# 4. --no-skip-existing forces enqueue
# ---------------------------------------------------------------------------


def test_no_skip_existing_forces_replacement(tmp_path):
    def scope_lister(s3, model, exp, member, variable):
        return ({2000: _key(model, exp, member, variable, 2000)}, [])

    out_path = v2._out_path_for(
        tmp_path, "r1i1p1f1_panIndia", "historical", "pr", "MODEL", 2000
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(b"\x00" * 64)

    tasks, decisions, summary, _ = v2.build_manifest(
        s3=MagicMock(),
        out_dir=tmp_path,
        member="r1i1p1f1",
        member_dir="r1i1p1f1_panIndia",
        variables=["pr"],
        experiments=["historical"],
        models=["MODEL"],
        user_years={2000},
        skip_existing=False,
        verify=False,
        delete_bad=False,
        scope_lister=scope_lister,
    )

    assert summary.queued == 1
    assert summary.skipped_existing == 0
    assert decisions[0].local_status == v2.LocalStatus.ENQUEUE_FORCE
    assert tasks[0].year == 2000


# ---------------------------------------------------------------------------
# 5. Verify quarantines vs deletes corrupt files
# ---------------------------------------------------------------------------


def test_handle_existing_quarantines_corrupt_and_deletes_when_flagged(tmp_path):
    target = tmp_path / "2000.nc"
    target.write_bytes(b"this is not a netcdf file")

    # Case A: verify=True, delete_bad=False → quarantine to .bad
    status, quarantined = v2.handle_existing(
        target, skip_existing=True, verify=True, delete_bad=False
    )
    assert quarantined is True
    assert status == v2.LocalStatus.ENQUEUE_AFTER_QUARANTINE
    bad = target.with_suffix(target.suffix + ".bad")
    assert bad.exists()
    assert not target.exists()

    # Restore for Case B
    bad.rename(target)

    # Case B: verify=True, delete_bad=True → original gone, no .bad
    status, quarantined = v2.handle_existing(
        target, skip_existing=True, verify=True, delete_bad=True
    )
    assert quarantined is True
    assert status == v2.LocalStatus.ENQUEUE_AFTER_QUARANTINE
    assert not target.exists()
    assert not target.with_suffix(target.suffix + ".bad").exists()


# ---------------------------------------------------------------------------
# 6. download_one: atomic write + lazy-dataset safety
# ---------------------------------------------------------------------------


def _make_synthetic_dataset():
    """Tiny xr.Dataset with ascending lat/lon and a single data var."""
    lat = np.linspace(10.0, 20.0, 5)
    lon = np.linspace(70.0, 80.0, 6)
    data = np.zeros((5, 6), dtype="float32")
    return xr.Dataset(
        {"pr": (("lat", "lon"), data)},
        coords={"lat": lat, "lon": lon},
    )


class _ClosingDataset:
    """Wrapper that tracks closure and panics if `.sel` is called post-close."""

    def __init__(self, ds: xr.Dataset):
        self._ds = ds
        self._closed = False
        # Expose coords/data_vars for detect_lat_lon_vars
        self.coords = ds.coords
        self.data_vars = ds.data_vars

    def __getitem__(self, key):
        if self._closed:
            raise RuntimeError("dataset accessed after close")
        return self._ds[key]

    def sel(self, indexers):
        if self._closed:
            raise RuntimeError("sel after close")
        return self._ds.sel(indexers)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._closed = True


def test_download_one_atomic_write_and_lazy_dataset_safety(
    tmp_path, monkeypatch
):
    target = tmp_path / "r1i1p1f1_panIndia" / "historical" / "pr" / "MODEL" / "2000.nc"
    task = v2.DownloadTask(
        s3_key="NEX-GDDP-CMIP6/MODEL/historical/r1i1p1f1/pr/x_2000.nc",
        out_path=target,
        variable="pr",
        model="MODEL",
        experiment="historical",
        member="r1i1p1f1",
        year=2000,
    )

    synthetic = _ClosingDataset(_make_synthetic_dataset())

    def fake_open_dataset(path, **kwargs):
        return synthetic

    # Fake S3 client whose download_fileobj writes a dummy byte to the temp file.
    class FakeS3:
        def download_fileobj(self, bucket, key, fh):
            fh.write(b"\x00" * 128)

    monkeypatch.setattr(v2.xr, "open_dataset", fake_open_dataset)
    monkeypatch.setattr(v2, "get_s3_client", lambda: FakeS3())

    result = v2.download_one(task, bbox=(10.0, 20.0, 70.0, 80.0), open_mode="download-first")

    assert result.status == v2.TaskStatus.DOWNLOADED, (
        f"Expected DOWNLOADED, got {result.status} :: "
        f"{result.error_type} :: {result.error_message}"
    )
    assert target.exists()
    # No stray .tmp file in the directory.
    leftovers = [p for p in target.parent.iterdir() if ".tmp" in p.name]
    assert leftovers == [], f"stray temp files: {leftovers}"
    # Source dataset was closed during the run.
    assert synthetic._closed is True


# ---------------------------------------------------------------------------
# 7. Exit code 2 path: quarantined + missing on S3
# ---------------------------------------------------------------------------


def test_exit_code_2_when_quarantined_and_missing_on_s3(tmp_path):
    """A corrupt local file is quarantined but S3 has no replacement key."""

    def scope_lister(s3, model, exp, member, variable):
        # Empty S3 listing → year 2000 will be missing.
        return ({}, [])

    out_path = v2._out_path_for(
        tmp_path, "r1i1p1f1_panIndia", "historical", "pr", "MODEL", 2000
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(b"this is not a netcdf")

    tasks, decisions, summary, _ = v2.build_manifest(
        s3=MagicMock(),
        out_dir=tmp_path,
        member="r1i1p1f1",
        member_dir="r1i1p1f1_panIndia",
        variables=["pr"],
        experiments=["historical"],
        models=["MODEL"],
        user_years={2000},
        skip_existing=True,
        verify=True,
        delete_bad=False,
        scope_lister=scope_lister,
    )

    assert summary.quarantined == 1
    assert summary.quarantined_unreplaceable == 1
    assert summary.failed == 0
    assert tasks == []
    assert v2.compute_exit_code(summary) == 2


# ---------------------------------------------------------------------------
# Bonus: ordered slice for descending lat
# ---------------------------------------------------------------------------


def test_ordered_slice_descending_lat():
    asc = np.array([10.0, 15.0, 20.0])
    desc = np.array([20.0, 15.0, 10.0])
    assert v2._ordered_slice(asc, 12.0, 18.0) == slice(12.0, 18.0)
    assert v2._ordered_slice(desc, 12.0, 18.0) == slice(18.0, 12.0)
