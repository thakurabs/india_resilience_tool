from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import xarray as xr

from tools.pipeline import build_spatial_weights as BSW


def _write_sample_nc(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    xr.Dataset(coords={"lat": [10.0, 11.0], "lon": [77.0, 78.0]}).to_netcdf(path)


def test_build_spatial_weights_data_dir_dry_run_uses_effective_inputs(tmp_path: Path, capsys) -> None:
    data_dir = tmp_path / "irt_data"
    _write_sample_nc(data_dir / "historical" / "tasmax" / "ModelA" / "2030.nc")
    (data_dir / "districts_4326.geojson").write_text("{}", encoding="utf-8")

    exit_code = BSW.main(["--data-dir", str(data_dir), "--level", "district", "--dry-run"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"boundary={(data_dir / 'districts_4326.geojson').resolve()}" in captured.out
    assert f"sample_nc={(data_dir / 'historical' / 'tasmax' / 'ModelA' / '2030.nc').resolve()}" in captured.out


def test_build_spatial_weights_valid_existing_cache_skips(tmp_path: Path, monkeypatch, capsys) -> None:
    data_dir = tmp_path / "irt_data"
    sample_nc = data_dir / "historical" / "tasmax" / "ModelA" / "2030.nc"
    _write_sample_nc(sample_nc)
    boundary = data_dir / "districts_4326.geojson"
    boundary.write_text("{}", encoding="utf-8")

    grid = BSW.dataset_grid_spec(BSW.normalize_lat_lon(xr.open_dataset(sample_nc)))
    output = data_dir / "processed" / "_internal" / "spatial_weights" / f"district__{grid.grid_id}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    BSW.write_spatial_weights_cache(
        pd.DataFrame({"unit_key": ["A"], "cell_index": [0], "lat_index": [0], "lon_index": [0], "area_m2": [1.0]}),
        output_path=output,
        grid=grid,
        level="district",
        boundary_path=boundary,
        analysis_crs=BSW.DEFAULT_ANALYSIS_CRS,
    )

    exit_code = BSW.main(["--data-dir", str(data_dir), "--level", "district"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "cache_status=valid" in captured.out


def test_build_spatial_weights_stale_existing_cache_requires_overwrite(tmp_path: Path, monkeypatch, capsys) -> None:
    data_dir = tmp_path / "irt_data"
    sample_nc = data_dir / "historical" / "tasmax" / "ModelA" / "2030.nc"
    _write_sample_nc(sample_nc)
    boundary = data_dir / "districts_4326.geojson"
    boundary.write_text("{}", encoding="utf-8")

    grid = BSW.dataset_grid_spec(BSW.normalize_lat_lon(xr.open_dataset(sample_nc)))
    output = data_dir / "processed" / "_internal" / "spatial_weights" / f"district__{grid.grid_id}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"stale")

    exit_code = BSW.main(["--data-dir", str(data_dir), "--level", "district"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "rerun_with=--overwrite" in captured.err
