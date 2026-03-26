from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import Polygon

from india_resilience_tool.compute.spi_adapter import Distribution, SPIResult
import india_resilience_tool.compute.spi_adapter as spi_adapter
from tools.pipeline import compute_indices_multiprocess as CMP


def _daily_series() -> xr.DataArray:
    time = xr.date_range("2030-01-01", periods=3, freq="D", use_cftime=True)
    return xr.DataArray([300.0, 301.0, 302.0], coords={"time": time}, dims=["time"])


def _monthly_series() -> xr.DataArray:
    time = pd.date_range("2030-01-01", periods=12, freq="MS")
    return xr.DataArray(np.linspace(-1.0, 1.0, 12), coords={"time": time}, dims=["time"])


def test_add_unit_fields_from_key_populates_sub_basin_fields() -> None:
    row: dict[str, object] = {}
    CMP._add_unit_fields_from_key(row, "Godavari Basin||Pranhita", "sub_basin")
    assert row == {"basin": "Godavari Basin", "sub_basin": "Pranhita"}


def test_tx90p_helper_emits_basin_fields_for_hydro(monkeypatch) -> None:
    monkeypatch.setattr(
        CMP,
        "_collect_daily_mean_by_unit",
        lambda *args, **kwargs: {"Godavari Basin": _daily_series()},
    )
    monkeypatch.setattr(
        CMP,
        "_compute_tx90p_etccdi_yearly",
        lambda **kwargs: {2030: 12.5},
    )

    rows = CMP._compute_tx90p_rows_for_metric(
        metric={"slug": "tx90p_hot_days_pct", "var": "tasmax", "value_col": "tx90p"},
        model="ACCESS-CM2",
        scenario="historical",
        scenario_conf={},
        year_to_paths={2030: {"tasmax": Path("ignored.nc")}},
        masks={"Godavari Basin": xr.DataArray([True])},
        level="basin",
    )

    assert rows == [
        {
            "year": 2030,
            "value": 12.5,
            "tx90p": 12.5,
            "source_file": "",
            "basin": "Godavari Basin",
        }
    ]


def test_validate_output_unit_fields_raises_clear_error_for_invalid_hydro_rows() -> None:
    df = pd.DataFrame({"year": [2030], "value": [1.0], "basin": [np.nan]})

    with pytest.raises(ValueError) as excinfo:
        CMP._validate_output_unit_fields(
            df,
            level="basin",
            slug="spi3_drought_index",
            model="ACCESS-CM2",
            scenario="historical",
            stage_label="yearly outputs",
        )

    message = str(excinfo.value)
    assert "Invalid hydro identity values" in message
    assert "spi3_drought_index" in message
    assert "ACCESS-CM2" in message
    assert "historical" in message


def test_load_boundaries_rejects_blank_hydro_identity(monkeypatch) -> None:
    gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": [""], "hydro_level": ["basin"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    monkeypatch.setattr(CMP.gpd, "read_file", lambda path: gdf)

    with pytest.raises(ValueError, match="Hydro boundary inputs contain blank identity values"):
        CMP.load_boundaries(Path("ignored.geojson"), level="basin")


def test_monthly_spi_csv_path_supports_sub_basins() -> None:
    out = spi_adapter._monthly_spi_csv_path(
        metric_root_path=Path("/tmp/metric"),
        state_name="hydro",
        level_folder="sub_basins",
        level="sub_basin",
        unit_key="Godavari Basin||Pranhita",
        model="ACCESS-CM2",
        scenario="ssp245",
    )

    assert out == Path(
        "/tmp/metric/hydro/sub_basins/Godavari_Basin/Pranhita/ACCESS-CM2/ssp245/Pranhita_monthly.csv"
    )


def test_compute_spi_rows_climate_indices_emits_sub_basin_fields(monkeypatch) -> None:
    result = SPIResult(
        monthly_spi=_monthly_series(),
        annual_spi=xr.DataArray([0.0], coords={"year": [2030]}, dims=["year"]),
        scale_months=3,
        distribution=Distribution.GAMMA,
        calibration_years=(1981, 2010),
        data_years=(2030, 2030),
        valid_months=12,
    )
    monkeypatch.setattr(spi_adapter, "CLIMATE_INDICES_AVAILABLE", True)
    monkeypatch.setattr(spi_adapter, "compute_spi_for_unit", lambda **kwargs: result)

    rows = spi_adapter.compute_spi_rows_climate_indices(
        metric={
            "slug": "spi3_drought_index",
            "var": "pr",
            "value_col": "spi3_index",
            "params": {"distribution": "gamma", "min_months_per_year": 1},
        },
        model="ACCESS-CM2",
        scenario="historical",
        scenario_conf={"periods": {}},
        scen_monthly_by_unit={"Godavari Basin||Pranhita": _monthly_series()},
        calib_monthly_by_unit={"Godavari Basin||Pranhita": _monthly_series()},
        masks={"Godavari Basin||Pranhita": xr.DataArray([True])},
        level="sub_basin",
        baseline_years=(1981, 2010),
        scale_months=3,
        year_to_paths={},
    )

    assert rows
    assert rows[0]["basin"] == "Godavari Basin"
    assert rows[0]["sub_basin"] == "Pranhita"
    assert "district" not in rows[0]
