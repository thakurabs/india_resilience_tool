"""Data-integrity tests for the NEX-GDDP-CMIP6 v2.0 rsds/sfcWind NCSS acquirer.

Everything here is synthetic: NetCDF fixtures are written on the fly and HTTP
and catalog responses are faked. No NASA network access, and nothing here is a
WBGT scientific validation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import cftime
import numpy as np
import pytest
import xarray as xr

from tools.data_acquisition import nex_wbgt_ncss as acq


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

_CFTIME_CLASS = {
    "standard": cftime.DatetimeGregorian,
    "proleptic_gregorian": cftime.DatetimeProlepticGregorian,
    "365_day": cftime.DatetimeNoLeap,
    "360_day": cftime.Datetime360Day,
}

_MONTH_DAYS = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
               7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def year_dates(year: int, calendar: str) -> list:
    """Every day of one year in the given CMIP6 calendar, stamped at 12:00."""
    cls = _CFTIME_CLASS[calendar]
    if calendar == "360_day":
        return [cls(year, m, d, 12) for m in range(1, 13) for d in range(1, 31)]
    leap = calendar != "365_day" and (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    out = []
    for month in range(1, 13):
        days = _MONTH_DAYS[month] + (1 if month == 2 and leap else 0)
        out.extend(cls(year, month, day, 12) for day in range(1, days + 1))
    return out


def write_nc(path: Path, *, variable: str = "rsds", year: int = 1990,
             calendar: str = "365_day", nlat: int = 129, nlon: int = 121,
             units: str | None = "W m-2", version: str | None = "2.0",
             member: str | None = "r1i1p1f1", dates=None,
             values: np.ndarray | None = None, fill: float = 1.0e20,
             mask_fraction: float = 0.3, lat_offset: float = 0.0) -> Path:
    """Write a synthetic NCSS-shaped response file."""
    dates = list(dates) if dates is not None else year_dates(year, calendar)
    lat = 6.125 + lat_offset + 0.25 * np.arange(nlat)
    lon = 68.125 + 0.25 * np.arange(nlon)
    if values is None:
        rng = np.random.default_rng(0)
        values = rng.uniform(40.0, 400.0, size=(len(dates), nlat, nlon)).astype("float32")
        if mask_fraction > 0:
            masked = rng.random(values.shape) < mask_fraction
            values = np.where(masked, np.float32(np.nan), values)
    ds = xr.Dataset(
        {variable: (("time", "lat", "lon"), np.asarray(values, dtype="float32"),
                    {} if units is None else {"units": units})},
        coords={"time": dates, "lat": lat, "lon": lon},
    )
    if version is not None:
        ds.attrs["version"] = version
    if member is not None:
        ds.attrs["variant_label"] = member
    ds.time.encoding.update({"calendar": calendar,
                             "units": f"days since {year}-01-01 00:00:00"})
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path, engine="h5netcdf", format="NETCDF4",
                 encoding={variable: {"dtype": "float32", "_FillValue": np.float32(fill),
                                      "zlib": True, "complevel": 1}})
    ds.close()
    return path


def write_local_reference(root: Path, *, model: str, experiment: str, year: int,
                          calendar: str, member: str = "r1i1p1f1",
                          variable: str = "tasmax", dates=None) -> Path:
    """Write the local tasmax year file that supplies the expected timestamps."""
    path = root / member / experiment / variable / model / f"{year}.nc"
    return write_nc(path, variable=variable, year=year, calendar=calendar,
                    nlat=2, nlon=2, units="K", version="1.0", member=member,
                    dates=dates, mask_fraction=0.0)


class FakeResponse:
    def __init__(self, body: bytes = b"", *, status: int = 200,
                 headers: dict[str, str] | None = None, text: str = "") -> None:
        self.body = body
        self.status_code = status
        self.headers = dict(headers or {})
        self._text = text
        self.closed = False

    @property
    def text(self) -> str:
        return self._text or self.body[:400].decode("latin-1")

    def iter_content(self, chunk_size: int = 1 << 20):
        for start in range(0, len(self.body), max(chunk_size, 1)):
            yield self.body[start:start + chunk_size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self) -> None:
        self.closed = True


class FakeClient:
    """Stands in for ``HttpClient``: serves canned payloads and catalogs."""

    def __init__(self, *, payload: FakeResponse | list[FakeResponse] | None = None,
                 catalogs: dict[str, str] | None = None) -> None:
        self._payloads = payload if isinstance(payload, list) else [payload]
        self.catalogs = dict(catalogs or {})
        self.calls: list[tuple[str, dict]] = []
        self.catalog_calls: list[str] = []

    def get(self, url: str, *, params=None, stream: bool = False) -> FakeResponse:
        self.calls.append((url, dict(params or {})))
        response = self._payloads[min(len(self.calls) - 1, len(self._payloads) - 1)]
        if response is None:
            raise AssertionError("FakeClient had no payload to serve")
        return response

    def get_text(self, url: str) -> str:
        self.catalog_calls.append(url)
        if url not in self.catalogs:
            raise acq.PermanentTaskError(f"catalog not found: {url}")
        return self.catalogs[url]


def nc_response(path: Path, *, advertise: int | None = None,
                headers: dict[str, str] | None = None) -> FakeResponse:
    body = Path(path).read_bytes()
    base = {"Content-Type": "application/x-netcdf",
            "Content-Length": str(len(body) if advertise is None else advertise)}
    base.update(headers or {})
    return FakeResponse(body, headers=base)


def make_task(tmp_path: Path, *, variable: str = "rsds", model: str = "GFDL-ESM4",
              experiment: str = "historical", year: int = 1990,
              calendar: str = "365_day", member: str = "r1i1p1f1",
              grid_label: str = "gr1", reference_dates=None) -> acq.PlanTask:
    data_root = tmp_path / "irt_data"
    out_root = tmp_path / "acq"
    reference = write_local_reference(data_root, model=model, experiment=experiment,
                                     year=year, calendar=calendar, member=member,
                                     dates=reference_dates)
    contract = acq.read_time_contract(reference)
    name = (f"{variable}_day_{model}_{experiment}_{member}_{grid_label}_{year}_v2.0.nc")
    url_path = f"AMES/NEX/GDDP-CMIP6/{model}/{experiment}/{member}/{variable}/{name}"
    params = acq.build_request_parameters(variable, "all", contract["start"],
                                          contract["end"], "netcdf4")
    return acq.PlanTask(
        task_id=acq.make_task_id(model, experiment, member, variable, year),
        model=model, experiment=experiment, member=member, variable=variable,
        year=year, grid_label=grid_label, source_version="2.0",
        source_dataset_path=url_path,
        catalog_url=acq.catalog_url(model, experiment, member, variable),
        ncss_url=acq.ncss_url(url_path),
        request_parameters=json.dumps(params, sort_keys=True),
        source_calendar=contract["calendar"],
        expected_time_count=contract["count"],
        expected_time_start=contract["start"],
        expected_time_end=contract["end"],
        expected_time_sha256=contract["sha256"],
        expected_time_reference=contract["reference"],
        output_path=str(acq.output_path_for(out_root, member, experiment,
                                            variable, model, year)),
    )


def catalog_xml(names: list[str], *, model: str, experiment: str,
                member: str, variable: str) -> str:
    prefix = f"AMES/NEX/GDDP-CMIP6/{model}/{experiment}/{member}/{variable}"
    entries = "".join(
        f'<dataset name="{name}" ID="{prefix}/{name}" urlPath="{prefix}/{name}" />'
        for name in names
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<catalog xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0">'
        f'<dataset name="{prefix}/">{entries}</dataset></catalog>'
    )


# ---------------------------------------------------------------------------
# Filename parsing and remote selection
# ---------------------------------------------------------------------------


def test_parses_the_v2_suffixed_thredds_filename():
    parsed = acq.parse_source_filename(
        "rsds_day_GFDL-ESM4_historical_r1i1p1f1_gr1_1990_v2.0.nc"
    )
    assert parsed == {
        "variable": "rsds", "frequency": "day", "model": "GFDL-ESM4",
        "experiment": "historical", "member": "r1i1p1f1", "grid_label": "gr1",
        "year": 1990, "version": "2.0",
    }


def test_parses_the_unsuffixed_s3_style_filename():
    parsed = acq.parse_source_filename(
        "sfcWind_day_CanESM5_ssp585_r1i1p1f1_gn_2050.nc"
    )
    assert parsed is not None
    assert parsed["version"] is None
    assert parsed["year"] == 2050


@pytest.mark.parametrize("grid_label", ["gn", "gr", "gr1"])
def test_parses_every_published_grid_label(grid_label):
    parsed = acq.parse_source_filename(
        f"rsds_day_EC-Earth3_ssp245_r1i1p1f1_{grid_label}_2030_v2.0.nc"
    )
    assert parsed is not None and parsed["grid_label"] == grid_label


def test_rejects_a_name_that_is_not_a_daily_source_file():
    assert acq.parse_source_filename("catalog.xml") is None
    assert acq.parse_source_filename("rsds_mon_CanESM5_ssp585_r1i1p1f1_gn_2050.nc") is None


def _candidates(*names: str) -> list[dict]:
    out = []
    for name in names:
        parsed = acq.parse_source_filename(name)
        assert parsed is not None, name
        parsed["url_path"] = f"path/{name}"
        parsed["name"] = name
        out.append(parsed)
    return out


def test_selects_v2_when_three_versions_coexist():
    candidate, reason = acq.select_remote_candidate(
        _candidates(
            "rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v1.0.nc",
            "rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v1.1.nc",
            "rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v2.0.nc",
        ),
        member="r1i1p1f1",
    )
    assert reason == "ok"
    assert candidate["version"] == "2.0"


def test_refuses_to_substitute_another_version():
    candidate, reason = acq.select_remote_candidate(
        _candidates("rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v1.1.nc"),
        member="r1i1p1f1",
    )
    assert candidate is None and "no v2.0" in reason


def test_refuses_to_substitute_another_member():
    candidate, reason = acq.select_remote_candidate(
        _candidates("rsds_day_CanESM5_historical_r2i1p1f1_gn_1990_v2.0.nc"),
        member="r1i1p1f1",
    )
    assert candidate is None and "member mismatch" in reason


def test_duplicate_compatible_candidates_are_ambiguous_not_first_wins():
    candidate, reason = acq.select_remote_candidate(
        _candidates(
            "rsds_day_KIOST-ESM_historical_r1i1p1f1_gn_1990_v2.0.nc",
            "rsds_day_KIOST-ESM_historical_r1i1p1f1_gr1_1990_v2.0.nc",
        ),
        member="r1i1p1f1",
    )
    assert candidate is None
    assert "2 compatible" in reason and "documented resolution" in reason


def test_missing_remote_year_is_reported_not_silently_dropped():
    candidate, reason = acq.select_remote_candidate([], member="r1i1p1f1")
    assert candidate is None and reason == "no remote dataset"


# ---------------------------------------------------------------------------
# Coverage and plan
# ---------------------------------------------------------------------------


def test_experiment_windows_are_exactly_21_61_61():
    assert acq.years_for("historical", acq.EXPERIMENT_YEARS) == list(range(1990, 2011))
    assert len(acq.years_for("historical", acq.EXPERIMENT_YEARS)) == 21
    assert len(acq.years_for("ssp245", acq.EXPERIMENT_YEARS)) == 61
    assert len(acq.years_for("ssp585", acq.EXPERIMENT_YEARS)) == 61


def _plan_fixture(tmp_path: Path, *, model="GFDL-ESM4", variable="rsds",
                  catalog_years=range(1950, 2015), omit=(), extra_names=(),
                  member="r1i1p1f1"):
    data_root = tmp_path / "irt_data"
    for var in ("tas", "tasmax", "hurs"):
        (data_root / member / "historical" / var / model).mkdir(parents=True, exist_ok=True)
    for year in range(1990, 2011):
        write_local_reference(data_root, model=model, experiment="historical",
                              year=year, calendar="365_day", member=member)
    names = [
        f"{variable}_day_{model}_historical_{member}_gr1_{year}_v2.0.nc"
        for year in catalog_years if year not in set(omit)
    ] + list(extra_names)
    catalogs = {
        acq.catalog_url(model, "historical", member, variable):
            catalog_xml(names, model=model, experiment="historical",
                        member=member, variable=variable)
    }
    return data_root, FakeClient(catalogs=catalogs)


def test_plan_covers_only_the_production_window(tmp_path):
    data_root, client = _plan_fixture(tmp_path)
    payload = acq.build_plan(
        client=client, data_root=data_root, out_root=tmp_path / "acq",
        models=["GFDL-ESM4"], experiments=["historical"], variables=["rsds"],
        member="r1i1p1f1", experiment_years=acq.EXPERIMENT_YEARS,
        time_selector="all", accept="netcdf4",
    )
    tasks = acq.plan_tasks(payload)
    assert len(tasks) == 21
    assert sorted(t.year for t in tasks) == list(range(1990, 2011))
    assert all(t.status == acq.TaskStatus.PLANNED.value for t in tasks)
    assert all(t.grid_label == "gr1" and t.source_version == "2.0" for t in tasks)


def test_plan_records_a_remote_gap_as_a_task_not_an_omission(tmp_path):
    data_root, client = _plan_fixture(tmp_path, omit=(2005,))
    payload = acq.build_plan(
        client=client, data_root=data_root, out_root=tmp_path / "acq",
        models=["GFDL-ESM4"], experiments=["historical"], variables=["rsds"],
        member="r1i1p1f1", experiment_years=acq.EXPERIMENT_YEARS,
        time_selector="all", accept="netcdf4",
    )
    tasks = {t.year: t for t in acq.plan_tasks(payload)}
    assert len(tasks) == 21
    assert tasks[2005].status == acq.TaskStatus.MISSING_REMOTE.value
    assert tasks[2005].ncss_url is None
    assert tasks[2004].status == acq.TaskStatus.PLANNED.value


def test_plan_marks_duplicate_remote_candidates_ambiguous(tmp_path):
    data_root, client = _plan_fixture(
        tmp_path, omit=(2001,),
        extra_names=("rsds_day_GFDL-ESM4_historical_r1i1p1f1_gn_2001_v2.0.nc",
                     "rsds_day_GFDL-ESM4_historical_r1i1p1f1_gr_2001_v2.0.nc"),
    )
    payload = acq.build_plan(
        client=client, data_root=data_root, out_root=tmp_path / "acq",
        models=["GFDL-ESM4"], experiments=["historical"], variables=["rsds"],
        member="r1i1p1f1", experiment_years=acq.EXPERIMENT_YEARS,
        time_selector="all", accept="netcdf4",
    )
    tasks = {t.year: t for t in acq.plan_tasks(payload)}
    assert tasks[2001].status == acq.TaskStatus.AMBIGUOUS_REMOTE.value


def test_plan_captures_the_local_time_contract(tmp_path):
    data_root, client = _plan_fixture(tmp_path)
    payload = acq.build_plan(
        client=client, data_root=data_root, out_root=tmp_path / "acq",
        models=["GFDL-ESM4"], experiments=["historical"], variables=["rsds"],
        member="r1i1p1f1", experiment_years=acq.EXPERIMENT_YEARS,
        time_selector="all", accept="netcdf4",
    )
    task = next(t for t in acq.plan_tasks(payload) if t.year == 2000)
    assert task.source_calendar == "365_day"
    assert task.expected_time_count == 365  # noleap: no 29 February
    assert (task.expected_time_start, task.expected_time_end) == ("2000-01-01", "2000-12-31")
    assert task.expected_time_sha256


def test_plan_dry_run_writes_nothing(tmp_path, monkeypatch):
    data_root, client = _plan_fixture(tmp_path)
    out_root = tmp_path / "acq"
    monkeypatch.setattr(acq, "HttpClient", lambda **kwargs: client)
    code = acq.main([
        "--out-root", str(out_root), "--dry-run", "plan",
        "--data-root", str(data_root), "--models", "GFDL-ESM4",
        "--experiments", "historical", "--variables", "rsds",
    ])
    assert code == 0
    assert not out_root.exists()


def test_plan_refuses_to_replace_a_different_scope(tmp_path, monkeypatch):
    data_root, client = _plan_fixture(tmp_path)
    out_root = tmp_path / "acq"
    monkeypatch.setattr(acq, "HttpClient", lambda **kwargs: client)
    argv = ["--out-root", str(out_root), "plan", "--data-root", str(data_root),
            "--models", "GFDL-ESM4", "--experiments", "historical", "--variables", "rsds"]
    assert acq.main(argv) == 0
    assert acq.plan_path(out_root).exists()
    assert acq.main(argv[:-1] + ["rsds,sfcWind"]) == 2  # scope changed, no --reconcile


# ---------------------------------------------------------------------------
# Response validation
# ---------------------------------------------------------------------------


def test_publishes_a_trimmed_verified_output(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    client = FakeClient(payload=nc_response(source))
    result = acq.download_task(task, client, overall_deadline=600)

    published = Path(task.output_path)
    assert published.exists()
    assert result["output_sha256"] == acq.sha256_file(published)
    with xr.open_dataset(published, engine="h5netcdf") as ds:
        assert ds["rsds"].dims == ("time", "lat", "lon")
        assert ds["rsds"].shape == (365, 128, 120)
        np.testing.assert_allclose(ds["lat"].values, acq.TARGET_LAT, atol=1e-6)
        np.testing.assert_allclose(ds["lon"].values, acq.TARGET_LON, atol=1e-6)
        assert ds["rsds"].encoding["zlib"] is True
        assert ds["rsds"].encoding["complevel"] == acq.OUTPUT_COMPLEVEL
        assert ds["rsds"].encoding["chunksizes"] == acq.OUTPUT_CHUNKSIZES
        assert float(ds["rsds"].encoding["_FillValue"]) == pytest.approx(1e20)
        assert ds.attrs["irt_acquisition_source_version"] == "2.0"
        assert ds.attrs["version"] == "2.0"  # source attribute not overwritten


def test_trimming_drops_the_extra_ncss_boundary_cells(tmp_path):
    source = write_nc(tmp_path / "r.nc", nlat=129, nlon=121)
    with xr.open_dataset(source, engine="h5netcdf") as ds:
        assert ds.sizes["lat"] == 129 and ds.sizes["lon"] == 121
        trimmed = acq.trim_to_target_grid(ds)
        assert trimmed.sizes["lat"] == 128 and trimmed.sizes["lon"] == 120
        np.testing.assert_allclose(trimmed["lat"].values, acq.TARGET_LAT, atol=1e-6)


def test_a_shifted_grid_is_rejected_rather_than_regridded(tmp_path):
    source = write_nc(tmp_path / "r.nc", lat_offset=0.1)
    with xr.open_dataset(source, engine="h5netcdf") as ds:
        with pytest.raises(acq.PermanentTaskError, match="matched 0 response cells"):
            acq.trim_to_target_grid(ds)


@pytest.mark.parametrize(
    "calendar,year,expected_days",
    [("365_day", 2000, 365), ("360_day", 2000, 360),
     ("standard", 2000, 366), ("proleptic_gregorian", 1999, 365)],
)
def test_every_cmip6_calendar_round_trips(tmp_path, calendar, year, expected_days):
    task = make_task(tmp_path, year=year, calendar=calendar)
    assert task.expected_time_count == expected_days
    source = write_nc(tmp_path / "response.nc", year=year, calendar=calendar)
    client = FakeClient(payload=nc_response(source))
    result = acq.download_task(task, client, overall_deadline=600)
    assert result["time_count"] == expected_days
    with xr.open_dataset(Path(task.output_path), engine="h5netcdf") as ds:
        assert ds.sizes["time"] == expected_days
        assert acq.date_strings(ds)[0] == f"{year}-01-01"


def test_a_short_year_is_a_flagged_source_issue(tmp_path):
    task = make_task(tmp_path, year=1990)
    short = year_dates(1990, "365_day")[1:]  # 1 January absent, as IITM-ESM is
    source = write_nc(tmp_path / "response.nc", year=1990, dates=short)
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="time coverage differs"):
        acq.download_task(task, client, overall_deadline=600)
    assert not Path(task.output_path).exists()


def test_duplicate_dates_are_rejected(tmp_path):
    dates = year_dates(1990, "365_day")
    task = make_task(tmp_path, reference_dates=dates)
    duplicated = dates[:-1] + [dates[-2]]
    source = write_nc(tmp_path / "response.nc", dates=duplicated)
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="duplicate timestamps"):
        acq.download_task(task, client, overall_deadline=600)


def test_the_wrong_variable_is_rejected(tmp_path):
    task = make_task(tmp_path, variable="rsds")
    source = write_nc(tmp_path / "response.nc", variable="sfcWind", units="m s-1")
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="absent"):
        acq.download_task(task, client, overall_deadline=600)


def test_the_wrong_dataset_version_is_rejected(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc", version="1.1")
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="source version"):
        acq.download_task(task, client, overall_deadline=600)


def test_the_wrong_member_is_rejected(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc", member="r4i1p1f1")
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="ensemble member"):
        acq.download_task(task, client, overall_deadline=600)


def test_unrecognised_units_are_flagged_not_converted(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc", units="MJ m-2 d-1")
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="unrecognised units"):
        acq.download_task(task, client, overall_deadline=600)


def test_infinities_are_rejected(tmp_path):
    task = make_task(tmp_path)
    values = np.full((365, 129, 121), 100.0, dtype="float32")
    values[3, 4, 5] = np.inf
    source = write_nc(tmp_path / "response.nc", values=values, mask_fraction=0.0)
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="infinities"):
        acq.download_task(task, client, overall_deadline=600)


def test_an_entirely_missing_payload_is_rejected(tmp_path):
    task = make_task(tmp_path)
    values = np.full((365, 129, 121), np.nan, dtype="float32")
    source = write_nc(tmp_path / "response.nc", values=values, mask_fraction=0.0)
    client = FakeClient(payload=nc_response(source))
    with pytest.raises(acq.PermanentTaskError, match="entirely missing"):
        acq.download_task(task, client, overall_deadline=600)


def test_both_fill_value_and_missing_value_are_preserved(tmp_path):
    """The published NCSS response carries both; neither may be dropped."""
    task = make_task(tmp_path)
    source = tmp_path / "response.nc"
    write_nc(source, mask_fraction=0.2)
    # Re-write with the source's own `missing_value` alongside `_FillValue`,
    # as NASA's NCSS responses do.
    with xr.open_dataset(source, engine="h5netcdf") as ds:
        loaded = ds.load()
    loaded.to_netcdf(source, engine="h5netcdf", format="NETCDF4", mode="w",
                     encoding={"rsds": {"dtype": "float32",
                                        "_FillValue": np.float32(1e20),
                                        "missing_value": np.float32(1e20)}})
    with xr.open_dataset(source, engine="h5netcdf") as ds:
        assert ds["rsds"].encoding["missing_value"] == np.float32(1e20)

    acq.download_task(task, FakeClient(payload=nc_response(source)), overall_deadline=600)
    with xr.open_dataset(Path(task.output_path), engine="h5netcdf") as ds:
        assert float(ds["rsds"].encoding["_FillValue"]) == pytest.approx(1e20)
        assert float(ds["rsds"].encoding["missing_value"]) == pytest.approx(1e20)


def test_partial_missingness_is_preserved_not_filled(tmp_path):
    task = make_task(tmp_path)
    values = np.full((365, 129, 121), 120.0, dtype="float32")
    values[:, :10, :] = np.nan  # an ocean-like masked band
    source = write_nc(tmp_path / "response.nc", values=values, mask_fraction=0.0)
    client = FakeClient(payload=nc_response(source))
    result = acq.download_task(task, client, overall_deadline=600)
    assert result["missing_count"] == 365 * 10 * 120
    assert result["valid_count"] == 365 * 118 * 120
    with xr.open_dataset(Path(task.output_path), engine="h5netcdf") as ds:
        published = ds["rsds"].values
        assert np.isnan(published[:, :10, :]).all()
        assert np.allclose(published[:, 10:, :], 120.0)


# ---------------------------------------------------------------------------
# Transport failures
# ---------------------------------------------------------------------------


def test_a_truncated_http_200_is_a_retryable_failure(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    body = source.read_bytes()
    response = FakeResponse(body[: len(body) // 2],
                            headers={"Content-Type": "application/x-netcdf",
                                     "Content-Length": str(len(body))})
    with pytest.raises(acq.RetryableTaskError, match="truncated body"):
        acq.download_task(task, FakeClient(payload=response), overall_deadline=600)
    assert not Path(task.output_path).exists()


def test_an_html_error_document_with_http_200_is_rejected(tmp_path):
    task = make_task(tmp_path)
    response = FakeResponse(b"<html><body>NCSS error</body></html>",
                            headers={"Content-Type": "text/html;charset=utf-8",
                                     "Content-Length": "36"})
    with pytest.raises(acq.PermanentTaskError, match="error document"):
        acq.download_task(task, FakeClient(payload=response), overall_deadline=600)


def test_a_non_netcdf_body_with_a_netcdf_content_type_is_rejected(tmp_path):
    task = make_task(tmp_path)
    body = b"<?xml version='1.0'?><error>no such dataset</error>"
    response = FakeResponse(body, headers={"Content-Type": "application/x-netcdf",
                                           "Content-Length": str(len(body))})
    with pytest.raises(acq.PermanentTaskError, match="not NetCDF"):
        acq.download_task(task, FakeClient(payload=response), overall_deadline=600)


def test_a_file_whose_header_opens_but_whose_data_read_fails(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc", mask_fraction=0.0)
    body = bytearray(source.read_bytes())
    del body[int(len(body) * 0.55):]  # keep the superblock, lose the chunks
    response = FakeResponse(bytes(body),
                            headers={"Content-Type": "application/x-netcdf",
                                     "Content-Length": str(len(body))})
    with pytest.raises(Exception):
        acq.download_task(task, FakeClient(payload=response), overall_deadline=600)
    assert not Path(task.output_path).exists()


@pytest.mark.parametrize("status,exc", [(404, acq.PermanentTaskError),
                                        (403, acq.PermanentTaskError),
                                        (429, acq.RetryableTaskError),
                                        (503, acq.RetryableTaskError),
                                        (500, acq.RetryableTaskError)])
def test_http_status_is_classified(tmp_path, status, exc):
    task = make_task(tmp_path)
    response = FakeResponse(b"", status=status, headers={"Retry-After": "7"})
    with pytest.raises(exc):
        acq.download_task(task, FakeClient(payload=response), overall_deadline=600)


def test_retry_after_is_honoured_over_the_backoff_curve():
    assert acq.backoff_seconds(1, retry_after=12.0) == 12.0
    assert acq.backoff_seconds(9, retry_after=10_000.0) == acq.BACKOFF_CAP_SECONDS
    assert 0 < acq.backoff_seconds(1) <= acq.BACKOFF_BASE_SECONDS
    assert acq.backoff_seconds(8) <= acq.BACKOFF_CAP_SECONDS


def test_a_failed_task_leaves_no_valid_looking_final_file(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc", version="1.0")
    with pytest.raises(acq.PermanentTaskError):
        acq.download_task(task, FakeClient(payload=nc_response(source)),
                          overall_deadline=600)
    destination = Path(task.output_path)
    assert not destination.exists()
    leftovers = list((destination.parent / ".tmp").glob("*")) if (destination.parent / ".tmp").exists() else []
    assert leftovers == []


# ---------------------------------------------------------------------------
# Resume
# ---------------------------------------------------------------------------


def test_only_a_matching_verified_output_is_skipped(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    result = acq.download_task(task, FakeClient(payload=nc_response(source)),
                               overall_deadline=600)
    task.status = acq.TaskStatus.VERIFIED.value
    task.output_sha256 = result["output_sha256"]
    task.output_bytes = result["output_bytes"]
    assert acq.existing_output_state(task) == (True, "verified")

    task.status = acq.TaskStatus.RETRYABLE_FAILURE.value
    ok, reason = acq.existing_output_state(task)
    assert ok is False and "status=" in reason


def test_a_same_size_but_altered_file_invalidates_checksum_resume(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    result = acq.download_task(task, FakeClient(payload=nc_response(source)),
                               overall_deadline=600)
    task.status = acq.TaskStatus.VERIFIED.value
    task.output_sha256 = result["output_sha256"]
    task.output_bytes = result["output_bytes"]

    published = Path(task.output_path)
    size_before = published.stat().st_size
    with published.open("r+b") as handle:
        handle.seek(size_before - 3)
        original = handle.read(1)
        handle.seek(size_before - 3)
        handle.write(bytes([original[0] ^ 0xFF]))
    assert published.stat().st_size == size_before
    ok, reason = acq.existing_output_state(task)
    assert ok is False and reason == "checksum differs from manifest"


def test_a_missing_output_is_not_skipped(tmp_path):
    task = make_task(tmp_path)
    task.status = acq.TaskStatus.VERIFIED.value
    task.output_sha256 = "0" * 64
    assert acq.existing_output_state(task) == (False, "absent")


def test_a_changed_local_time_reference_blocks_the_download(tmp_path):
    task = make_task(tmp_path)
    task.expected_time_sha256 = "f" * 64
    with pytest.raises(acq.PermanentTaskError, match="local time reference changed"):
        acq.download_task(task, FakeClient(payload=None), overall_deadline=600)


# ---------------------------------------------------------------------------
# Completion accounting
# ---------------------------------------------------------------------------


def test_incomplete_acquisition_reports_a_nonzero_exit_code(tmp_path):
    verified = make_task(tmp_path, year=1990)
    verified.status = acq.TaskStatus.VERIFIED.value
    pending = make_task(tmp_path, year=1991)
    assert acq.compute_exit_code([verified], [verified]) == 0
    assert acq.compute_exit_code([verified, pending], [verified, pending]) == 3


def test_download_is_not_the_same_as_verified():
    assert acq.TaskStatus.DOWNLOADING.value != acq.TaskStatus.VERIFIED.value
    assert acq.TaskStatus.DOWNLOADING.value not in acq.TERMINAL_STATUSES
    assert acq.TaskStatus.VERIFIED.value in acq.TERMINAL_STATUSES


def test_known_downstream_issues_are_recorded_separately():
    joined = " ".join(acq.KNOWN_DOWNSTREAM_ISSUES)
    assert "version 1.0" in joined
    assert "KIOST-ESM/2058.nc" in joined
    assert "IITM-ESM" in joined


def test_workers_are_capped_at_the_published_ceiling():
    assert acq.MAX_WORKERS_CEILING == 16
    assert acq.DEFAULT_BULK_WORKERS <= acq.MAX_WORKERS_CEILING


def test_request_parameters_cover_the_full_india_bbox():
    params = acq.build_request_parameters("rsds", "all", None, None, "netcdf4")
    assert params["south"] == "6" and params["north"] == "38"
    assert params["west"] == "68" and params["east"] == "98"
    assert params["horizStride"] == "1" and params["timeStride"] == "1"
    assert params["time"] == "all"


def test_explicit_time_selector_uses_source_derived_dates_only():
    params = acq.build_request_parameters("rsds", "explicit", "2050-01-01", "2050-12-30",
                                          "netcdf4")
    assert params["time_start"].startswith("2050-01-01")
    assert params["time_end"].startswith("2050-12-30")  # a 360-day source's real last day
    assert "time" not in params
    with pytest.raises(acq.PermanentTaskError):
        acq.build_request_parameters("rsds", "explicit", None, None, "netcdf4")


def test_ncss_url_is_built_from_the_catalog_url_path():
    url = acq.ncss_url(
        "AMES/NEX/GDDP-CMIP6/GFDL-ESM4/historical/r1i1p1f1/rsds/"
        "rsds_day_GFDL-ESM4_historical_r1i1p1f1_gr1_1990_v2.0.nc"
    )
    assert url == (
        "https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/GFDL-ESM4/"
        "historical/r1i1p1f1/rsds/rsds_day_GFDL-ESM4_historical_r1i1p1f1_gr1_1990_v2.0.nc"
    )


# ---------------------------------------------------------------------------
# download / verify plumbing
# ---------------------------------------------------------------------------


def _write_plan(tmp_path: Path, tasks: list[acq.PlanTask]) -> Path:
    out_root = tmp_path / "acq"
    payload = {
        "schema_version": acq.SCHEMA_VERSION, "tool": acq.TOOL_NAME,
        "created_at": "1970-01-01T00:00:00Z", "source_version": "2.0",
        "member": "r1i1p1f1", "models": ["GFDL-ESM4"], "experiments": ["historical"],
        "variables": ["rsds"], "experiment_years": {"historical": [1990, 2010]},
        "time_selector": "all", "accept": "netcdf4",
        "data_root": str(tmp_path / "irt_data"), "out_root": str(out_root),
        "scope_signature": "test", "roster_notes": [], "local_identity": [],
        "tasks": [__import__("dataclasses").asdict(t) for t in tasks],
    }
    acq.write_json_atomic(acq.plan_path(out_root), payload)
    return out_root


def test_download_dry_run_writes_nothing(tmp_path):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    before = sorted(p.name for p in acq.acquisition_dir(out_root).iterdir())
    code = acq.main(["--out-root", str(out_root), "--dry-run", "download",
                     "--workers", "2"])
    assert code == 0
    assert sorted(p.name for p in acq.acquisition_dir(out_root).iterdir()) == before
    assert not Path(task.output_path).exists()
    assert not acq.manifest_path(out_root).exists()


def test_download_refuses_without_a_saved_plan(tmp_path):
    assert acq.main(["--out-root", str(tmp_path / "nowhere"), "download"]) == 2


def test_download_does_not_regenerate_the_plan(tmp_path):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    stamp = acq.plan_path(out_root).stat().st_mtime_ns
    acq.main(["--out-root", str(out_root), "--dry-run", "download"])
    assert acq.plan_path(out_root).stat().st_mtime_ns == stamp


def test_verify_reports_an_absent_output_and_exits_nonzero(tmp_path):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    code = acq.main(["--out-root", str(out_root), "verify"])
    assert code == 3
    rows = acq.verification_path(out_root).read_text(encoding="utf-8")
    assert "output absent" in rows
    summary = json.loads(acq.summary_path(out_root).read_text(encoding="utf-8"))
    assert summary["expected_tasks"] == 1
    assert summary["verified_now"] == 0


def test_verify_passes_a_published_output(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    result = acq.download_task(task, FakeClient(payload=nc_response(source)),
                               overall_deadline=600)
    task.status = acq.TaskStatus.VERIFIED.value
    task.output_sha256 = result["output_sha256"]
    task.output_bytes = result["output_bytes"]
    out_root = _write_plan(tmp_path, [task])
    assert acq.main(["--out-root", str(out_root), "verify"]) == 0
    row = acq.verification_path(out_root).read_text(encoding="utf-8")
    assert ",ok," in row or row.strip().endswith("ok")


def test_verify_catches_a_tampered_output(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    result = acq.download_task(task, FakeClient(payload=nc_response(source)),
                               overall_deadline=600)
    task.status = acq.TaskStatus.VERIFIED.value
    task.output_sha256 = result["output_sha256"]
    task.output_bytes = result["output_bytes"]
    published = Path(task.output_path)
    with published.open("r+b") as handle:
        handle.seek(published.stat().st_size - 3)
        byte = handle.read(1)
        handle.seek(published.stat().st_size - 3)
        handle.write(bytes([byte[0] ^ 0xFF]))
    out_root = _write_plan(tmp_path, [task])
    assert acq.main(["--out-root", str(out_root), "verify"]) == 3
    assert "checksum differs" in acq.verification_path(out_root).read_text(encoding="utf-8")


def test_download_publishes_and_records_manifest_state(tmp_path, monkeypatch):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    source = write_nc(tmp_path / "response.nc")
    client = FakeClient(payload=nc_response(source))
    monkeypatch.setattr(acq, "HttpClient", lambda **kwargs: client)
    code = acq.main(["--out-root", str(out_root), "download", "--workers", "1"])
    assert code == 0
    assert Path(task.output_path).exists()
    manifest = acq.manifest_path(out_root).read_text(encoding="utf-8")
    assert acq.TaskStatus.VERIFIED.value in manifest
    events = acq.events_path(out_root).read_text(encoding="utf-8").strip().splitlines()
    assert json.loads(events[-1])["event"] == "verified"
    # A second run skips the already-verified output rather than refetching.
    calls_before = len(client.calls)
    assert acq.main(["--out-root", str(out_root), "download", "--workers", "1"]) == 0
    assert len(client.calls) == calls_before


def test_manifest_has_every_required_column():
    required = {
        "task_id", "model", "experiment", "member", "variable", "year", "grid_label",
        "source_version", "source_dataset_path", "catalog_url", "ncss_url",
        "request_parameters", "source_calendar", "expected_time_count",
        "expected_time_start", "expected_time_end", "output_path", "status",
        "attempt_count", "last_error_type", "last_error_message", "downloaded_bytes",
        "elapsed_seconds", "output_bytes", "output_sha256", "verified_at",
    }
    assert required.issubset(set(acq.MANIFEST_FIELDS))


def test_catalog_parsing_reads_every_dataset_entry():
    xml = catalog_xml(
        ["rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v2.0.nc",
         "rsds_day_CanESM5_historical_r1i1p1f1_gn_1991_v2.0.nc"],
        model="CanESM5", experiment="historical", member="r1i1p1f1", variable="rsds",
    )
    entries = acq.parse_catalog_datasets(xml)
    names = [e["name"] for e in entries]
    assert "rsds_day_CanESM5_historical_r1i1p1f1_gn_1990_v2.0.nc" in names
    assert all(e["url_path"].startswith("AMES/NEX/GDDP-CMIP6/") for e in entries)


# ---------------------------------------------------------------------------
# Adaptive concurrency governor (CHG-0556)
# ---------------------------------------------------------------------------


def test_governor_starts_at_the_requested_concurrency():
    governor = acq.ConcurrencyGovernor(16)
    assert governor.target == 16


def test_governor_halves_in_flight_on_a_distressed_window():
    governor = acq.ConcurrencyGovernor(16, window=8, distress_ratio=0.25,
                                       recover_after=100)
    messages = []
    for index in range(8):
        _, change = governor.record(failed=index < 2)
        if change:
            messages.append(change)
    assert governor.target == 8
    assert messages and "reduced in-flight 16 -> 8" in messages[0]


def test_governor_never_drops_below_the_floor():
    governor = acq.ConcurrencyGovernor(16, window=4, distress_ratio=0.25,
                                       recover_after=1000, floor=2)
    for _ in range(200):
        governor.record(failed=True)
    assert governor.target == 2


def test_governor_never_rises_above_what_the_operator_requested():
    governor = acq.ConcurrencyGovernor(4, window=4, distress_ratio=0.25,
                                       recover_after=2)
    for _ in range(500):
        governor.record(failed=False)
    assert governor.target == 4


def test_governor_recovers_after_a_clean_streak():
    governor = acq.ConcurrencyGovernor(8, window=4, distress_ratio=0.25,
                                       recover_after=3, floor=2)
    for _ in range(4):
        governor.record(failed=True)
    reduced = governor.target
    assert reduced < 8
    for _ in range(3):
        governor.record(failed=False)
    assert governor.target == reduced + 1


def test_the_run_summary_reports_the_governor_state(tmp_path, monkeypatch):
    """Concurrency reductions must be visible after the fact, not just logged."""
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    source = write_nc(tmp_path / "response.nc")
    client = FakeClient(payload=nc_response(source))
    monkeypatch.setattr(acq, "HttpClient", lambda **kwargs: client)
    monkeypatch.setattr(acq, "GOVERNOR_FLOOR", 1)
    assert acq.main(["--out-root", str(out_root), "download", "--workers", "1"]) == 0
    summary = json.loads(acq.summary_path(out_root).read_text(encoding="utf-8"))
    assert summary["requested_workers"] == 1
    assert summary["concurrency_reductions"] == 0


# ---------------------------------------------------------------------------
# Streaming deadline enforcement (CHG-0557)
# ---------------------------------------------------------------------------


class SlowBody(FakeResponse):
    """Counts how many chunks the downloader actually pulled."""

    def __init__(self, body: bytes, **kwargs) -> None:
        super().__init__(body, **kwargs)
        self.chunks_yielded = 0

    def iter_content(self, chunk_size: int = 1 << 20):
        for start in range(0, len(self.body), 1024):
            self.chunks_yielded += 1
            yield self.body[start:start + 1024]


def test_the_overall_deadline_aborts_mid_stream(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    body = source.read_bytes()
    assert len(body) > 4096, "fixture must span several chunks for this test"
    response = SlowBody(body, headers={"Content-Type": "application/x-netcdf",
                                       "Content-Length": str(len(body))})
    client = FakeClient(payload=response)
    with pytest.raises(acq.RetryableTaskError) as excinfo:
        acq.download_task(task, client, overall_deadline=0.0)
    assert "deadline" in str(excinfo.value)
    # The defect being guarded: the deadline used to be checked only after the
    # whole body had been read, so it could never abort a slow transfer.
    assert response.chunks_yielded == 1
    assert not Path(task.output_path).exists()


def test_a_prompt_transfer_is_unaffected_by_the_deadline(tmp_path):
    task = make_task(tmp_path)
    source = write_nc(tmp_path / "response.nc")
    client = FakeClient(payload=nc_response(source))
    result = acq.download_task(task, client, overall_deadline=3600.0)
    assert Path(task.output_path).exists()
    assert result["output_sha256"]


# ---------------------------------------------------------------------------
# Empty-scope guard (CHG-0558)
# ---------------------------------------------------------------------------


def test_an_empty_download_scope_is_a_scope_error_not_success(tmp_path):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    assert acq.main(["--out-root", str(out_root), "download",
                     "--models", "NOT-A-MODEL"]) == 2


def test_an_empty_verify_scope_does_not_destroy_the_verification_record(
        tmp_path, monkeypatch):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    source = write_nc(tmp_path / "response.nc")
    monkeypatch.setattr(acq, "HttpClient",
                        lambda **kwargs: FakeClient(payload=nc_response(source)))
    assert acq.main(["--out-root", str(out_root), "download", "--workers", "1"]) == 0
    assert acq.main(["--out-root", str(out_root), "verify"]) == 0
    before = acq.verification_path(out_root).read_bytes()
    assert b"ok" in before

    assert acq.main(["--out-root", str(out_root), "verify",
                     "--models", "NOT-A-MODEL", "--years", "1999"]) == 2
    assert acq.verification_path(out_root).read_bytes() == before


# ---------------------------------------------------------------------------
# Transport hardening (CHG-0559)
# ---------------------------------------------------------------------------


def test_transport_requests_an_unencoded_body():
    """Content-Length must stay comparable to the bytes received."""
    client = acq.HttpClient(connect_timeout=1.0, read_timeout=1.0, max_attempts=1)
    assert client.session.headers["Accept-Encoding"] == "identity"


# ---------------------------------------------------------------------------
# verify re-checks identity and units (CHG-0560)
# ---------------------------------------------------------------------------


def _published(tmp_path: Path, **kwargs) -> acq.PlanTask:
    task = make_task(tmp_path, **kwargs)
    source = write_nc(tmp_path / "response.nc",
                      variable=task.variable,
                      year=task.year,
                      calendar=task.source_calendar,
                      units=acq.EXPECTED_UNITS[task.variable][0])
    acq.download_task(task, FakeClient(payload=nc_response(source)),
                      overall_deadline=3600.0)
    return task


def test_verify_accepts_a_sound_output(tmp_path):
    task = _published(tmp_path)
    row = acq.verify_output(task)
    assert row["verified"] is True and row["reason"] == "ok"


def test_verify_rejects_a_wrong_dataset_version_without_a_manifest_checksum(tmp_path):
    task = _published(tmp_path)
    task.output_sha256 = None  # the weak path: nothing to compare the bytes to
    task.source_version = "1.0"
    row = acq.verify_output(task)
    assert row["verified"] is False
    assert "version" in row["reason"]


def test_verify_rejects_a_wrong_ensemble_member(tmp_path):
    task = _published(tmp_path)
    task.output_sha256 = None
    task.member = "r4i1p1f1"
    row = acq.verify_output(task)
    assert row["verified"] is False
    assert "member" in row["reason"]


def test_verify_flags_unrecognised_units_on_a_published_output(tmp_path):
    """verify used to record units without judging them."""
    import h5py

    task = _published(tmp_path, variable="sfcWind")
    assert acq.verify_output(task)["verified"] is True
    with h5py.File(task.output_path, "r+") as handle:
        handle[task.variable].attrs["units"] = np.bytes_(b"knots")
    task.output_sha256 = None  # the weak path: no bytes to compare against
    row = acq.verify_output(task)
    assert row["verified"] is False
    assert "units" in row["reason"] and row["units"] == "knots"


# ---------------------------------------------------------------------------
# Worker ceiling opt-in (CHG-0561)
# ---------------------------------------------------------------------------


def _download_with(tmp_path: Path, monkeypatch, extra: list[str]) -> dict:
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    source = write_nc(tmp_path / "response.nc")
    monkeypatch.setattr(acq, "HttpClient",
                        lambda **kwargs: FakeClient(payload=nc_response(source)))
    assert acq.main(["--out-root", str(out_root), "download"] + extra) == 0
    return json.loads(acq.summary_path(out_root).read_text(encoding="utf-8"))


def test_workers_are_capped_at_the_default_ceiling(tmp_path, monkeypatch):
    summary = _download_with(tmp_path, monkeypatch, ["--workers", "24"])
    assert summary["requested_workers"] == acq.MAX_WORKERS_CEILING


def test_the_ceiling_is_raised_only_by_the_explicit_opt_in(tmp_path, monkeypatch):
    summary = _download_with(tmp_path, monkeypatch,
                             ["--workers", "24", "--allow-workers-above-ceiling"])
    assert summary["requested_workers"] == 24


def test_even_the_opt_in_is_bounded_by_the_hard_limit(tmp_path, monkeypatch):
    summary = _download_with(tmp_path, monkeypatch,
                             ["--workers", "999", "--allow-workers-above-ceiling"])
    assert summary["requested_workers"] == acq.MAX_WORKERS_HARD_LIMIT


def test_a_nonpositive_worker_count_is_a_scope_error(tmp_path):
    task = make_task(tmp_path)
    out_root = _write_plan(tmp_path, [task])
    assert acq.main(["--out-root", str(out_root), "download", "--workers", "0"]) == 2


def test_the_ceiling_opt_in_is_explicit_and_still_bounded():
    assert acq.MAX_WORKERS_CEILING == 16
    assert acq.MAX_WORKERS_HARD_LIMIT == 32
    assert acq.MAX_WORKERS_HARD_LIMIT >= acq.MAX_WORKERS_CEILING
