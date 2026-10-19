"""Offline acquisition contracts: bounded HTTP, spatial identity, and missing data."""
from datetime import date
import io
import json
import struct

import numpy as np
import pytest

from tools.data_acquisition import uhe_daily as uhe


def header_fixture(**overrides):
    """Build only TIFF metadata, with no global raster allocation."""
    tags = {256: (3, (7200,)), 257: (3, (2600,)), 258: (3, (32,)),
            259: (3, (1,)), 262: (3, (1,)), 277: (3, (1,)),
            278: (3, (1,)), 284: (3, (1,)), 339: (3, (3,)),
            273: (4, tuple(65536 + i * 28800 for i in range(2600))),
            279: (3, (28800,) * 2600), 33550: (12, (0.05, 0.05, 0.0)),
            33922: (12, (0., 0., 0., -180., 70., 0.)),
            34735: (3, (1, 1, 0, 3, 1024, 0, 1, 2, 1025, 0, 1, 1, 2048, 0, 1, 4326))}
    tags.update({int(k): v for k, v in overrides.items()})
    result = bytearray(65536)
    result[:8] = b"II" + struct.pack("<HI", 42, 8)
    struct.pack_into("<H", result, 8, len(tags))
    cursor = 10 + 12 * len(tags) + 4
    for index, (tag, (kind, values)) in enumerate(sorted(tags.items())):
        fmt = {3: "H", 4: "I", 12: "d"}[kind]
        raw = struct.pack("<" + str(len(values)) + fmt, *values)
        pos = 10 + index * 12
        struct.pack_into("<HHI", result, pos, tag, kind, len(values))
        if len(raw) <= 4:
            result[pos + 8:pos + 8 + len(raw)] = raw
        else:
            struct.pack_into("<I", result, pos + 8, cursor)
            result[cursor:cursor + len(raw)] = raw
            cursor += len(raw)
    return bytes(result)


class Response:
    def __init__(self, body=b"abcd", status=206, headers=None):
        self.status_code = status
        self.headers = {"Content-Range": "bytes 0-3/100", "ETag": '"v1"', "Content-Length": "4"}
        self.headers.update(headers or {})
        self.raw = io.BytesIO(body)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Session:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def test_native_grid_window_and_leap_day_plan():
    assert uhe.window_indices(uhe.BOUNDS) == (640, 1280, 4960, 5560)
    lat, lon = uhe.coordinates(uhe.BOUNDS)
    assert len(lat) == 640 and len(lon) == 600
    assert lat[0] == pytest.approx(37.975)
    assert lon[-1] == pytest.approx(97.975)


@pytest.mark.parametrize("bounds", [(68, 6, 68, 38), (68, 6, 98, 70.1),
                                         (68.001, 6, 98, 38), (float("nan"), 6, 98, 38)])
def test_reject_invalid_bounds(bounds):
    with pytest.raises(ValueError):
        uhe.window_indices(bounds)


def test_layout_and_row_extent():
    layout = uhe.parse_header(header_fixture(), 65536 + 2600 * 28800)
    assert layout["offsets"][640] == 65536 + 640 * 28800
    assert layout["nodata"] is None


def test_source_float32_pixel_scale_preserved():
    scale = float(np.float32(0.05))
    header = header_fixture(**{"33550": (12, (scale, scale, 0.0))})
    layout = uhe.parse_header(header, 65536 + 2600 * 28800)
    assert layout["pixel_scale"] == scale
    lat, lon = uhe.coordinates(uhe.BOUNDS, layout["pixel_scale"])
    assert lat[0] == 70 - 640.5 * scale
    assert lon[0] == -180 + 4960.5 * scale


@pytest.mark.parametrize("changes", [{"259": (3, (5,))}, {"278": (3, (2,))},
                                        {"33550": (12, (0.25, 0.25, 0.0))},
                                        {"34735": (3, (1, 1, 0, 1, 2048, 0, 1, 3857))}])
def test_layout_changes_fail_closed(changes):
    with pytest.raises(ValueError):
        uhe.parse_header(header_fixture(**changes), 65536 + 2600 * 28800)


@pytest.mark.parametrize("response", [Response(status=200), Response(body=b"ab"),
    Response(headers={"Content-Range": "bytes 1-4/100"}),
    Response(headers={"Content-Encoding": "gzip"}), Response(status=429)])
def test_range_rejects_full_wrong_encoded_or_truncated_response(response):
    client = uhe.RangeClient(100, session=Session(response), sleep=lambda _: None)
    with pytest.raises(ValueError):
        client.read("test", 0, 4)


def test_range_budget_and_source_mutation():
    session = Session(Response(), Response(headers={"ETag": '"v2"'}))
    client = uhe.RangeClient(8, session=session, sleep=lambda _: None)
    assert client.read("test", 0, 4) == b"abcd"
    with pytest.raises(ValueError, match="changed"):
        client.read("test", 0, 4)
    assert session.calls[1][1]["headers"]["If-Range"] == '"v1"'
    with pytest.raises(ValueError, match="budget"):
        client.read("test", 0, 4)
    assert len(session.calls) == 2


@pytest.mark.parametrize("values", [np.array([]), np.full((2, 2), np.nan), np.array([-9999.]), np.array([101.])])
def test_qc_empty_missing_and_extremes(values):
    with pytest.raises(ValueError):
        uhe.quality(values)


def test_qc_single_point_and_partial_nan():
    assert uhe.quality(np.array([np.nan, 31.])) == {
        "valid_cells": 1, "missing_cells": 1, "valid_fraction": 0.5,
        "min_c": 31., "max_c": 31., "mean_c": 31.}


def test_acquire_crop_roundtrip_resume_and_tamper(tmp_path):
    bounds = (68., 37.95, 68.1, 38.)
    row = np.arange(7200, dtype="<f4") / 1000
    row[4960] = np.nan
    header = header_fixture()
    total = 65536 + 2600 * 28800
    start = 65536 + 640 * 28800
    session = Session(
        Response(header, headers={"Content-Range": f"bytes 0-65535/{total}", "Content-Length": "65536"}),
        Response(row.tobytes(), headers={"Content-Range": f"bytes {start}-{start + 28799}/{total}", "Content-Length": "28800"}))
    client = uhe.RangeClient(100000, session=session, sleep=lambda _: None)
    day = date(1985, 1, 15)
    values, metadata = uhe.acquire_day(client, day, bounds)
    assert values.shape == (1, 2)
    assert np.isnan(values[0, 0]) and values[0, 1] == pytest.approx(4.961)
    path = tmp_path / "1985" / "wbgtmax.1985-01-15.npz"
    uhe.save_day(path, values, metadata)
    assert uhe.verify_day(path, day, bounds) == metadata
    with pytest.raises(FileExistsError):
        uhe.save_day(path, values, metadata)
    with pytest.raises(ValueError, match="contract"):
        uhe.verify_day(path, date(1985, 1, 16), bounds)
    lat, lon = uhe.coordinates(bounds)
    values[0, 1] = 30
    np.savez_compressed(path, wbgtmax_c=values, latitude=lat, longitude=lon,
                        metadata=np.array(json.dumps(metadata)))
    with pytest.raises(ValueError, match="checksum"):
        uhe.verify_day(path, day, bounds)


def test_dry_run_no_network_no_write(tmp_path, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("Dry run attempted network access")

    monkeypatch.setattr(uhe, "RangeClient", forbidden)
    output = tmp_path / "not-created"
    assert uhe.main(["--dry-run", "--output-dir", str(output)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["dates"] == 10957
    assert plan["estimated_transfer_gb_no_retries"] == pytest.approx(202.677501952)
    assert not output.exists()


def test_full_download_refused_before_network_or_write(tmp_path, monkeypatch):
    monkeypatch.setattr(uhe, "RangeClient", lambda *a, **k: pytest.fail("Budget preflight bypassed"))
    with pytest.raises(SystemExit) as exc:
        uhe.main(["--download", "--output-dir", str(tmp_path / "missing")])
    assert exc.value.code == 1
    assert not (tmp_path / "missing").exists()


def test_verify_missing_date_fails_without_writing(tmp_path):
    with pytest.raises(SystemExit) as exc:
        uhe.main(["--verify", "--start", "2000-02-29", "--end", "2000-02-29", "--output-dir", str(tmp_path)])
    assert exc.value.code == 1
    assert list(tmp_path.iterdir()) == []
