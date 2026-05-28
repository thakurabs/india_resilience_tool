from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from tools.pipeline import compute_indices_bootstrap as bootstrap
from tools.pipeline import compute_indices_cli_common as cli_common


def test_bootstrap_emits_banner_before_runtime(monkeypatch, capsys) -> None:
    calls: list[list[str] | None] = []
    fake_runtime = SimpleNamespace(main=lambda argv=None: calls.append(list(argv) if argv else None) or 0)
    monkeypatch.setitem(sys.modules, "tools.pipeline.compute_indices_multiprocess", fake_runtime)

    exit_code = bootstrap.main(["--level", "district", "--state", "Karnataka", "--workers", "4"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "IRT Climate Index Pipeline bootstrap" in captured.err
    assert "level=district" in captured.err
    assert calls == [["--level", "district", "--state", "Karnataka", "--workers", "4"]]


def test_bootstrap_list_metrics_stays_lightweight(monkeypatch, capsys) -> None:
    sentinel = object()
    monkeypatch.setitem(sys.modules, "tools.pipeline.compute_indices_multiprocess", sentinel)

    exit_code = bootstrap.main(["--list-metrics"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Available metrics:" in captured.out
    assert captured.err == ""


def test_discover_models_lightweight_uses_data_root(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "irt_data"
    model_dir = data_dir / "r1i1p1f1" / "historical" / "tas" / "ModelA"
    model_dir.mkdir(parents=True)
    monkeypatch.setattr(
        cli_common,
        "get_paths_config",
        lambda: SimpleNamespace(data_dir=data_dir, data_root=data_dir / "r1i1p1f1"),
    )

    models = cli_common.discover_models_lightweight()

    assert models == ["ModelA"]
