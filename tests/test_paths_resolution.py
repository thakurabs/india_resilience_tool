"""
Unit tests for IRT path semantics (processed-root resolution).

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_resolve_processed_root_default_no_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_DATA_DIR", raising=False)

    from india_resilience_tool.config.paths import resolve_processed_root

    slug = "days_gt_32C"
    out = resolve_processed_root(slug, data_dir=tmp_path, mode="single")
    assert out == (tmp_path / "processed" / slug).resolve()


def test_resolve_processed_root_env_single_mode_no_append(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from india_resilience_tool.config.paths import resolve_processed_root

    slug = "days_gt_32C"
    env_root = tmp_path / "processed_base"
    monkeypatch.setenv("IRT_PROCESSED_ROOT", str(env_root))

    out = resolve_processed_root(slug, data_dir=tmp_path, mode="single")
    assert out == env_root.resolve()


def test_resolve_processed_root_env_portfolio_mode_appends_when_needed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from india_resilience_tool.config.paths import resolve_processed_root

    slug = "days_gt_32C"
    env_root = tmp_path / "processed_base"
    monkeypatch.setenv("IRT_PROCESSED_ROOT", str(env_root))

    out = resolve_processed_root(slug, data_dir=tmp_path, mode="portfolio")
    assert out == (env_root / slug).resolve()


def test_resolve_processed_root_env_portfolio_mode_uses_slug_dir_when_already_pointing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from india_resilience_tool.config.paths import resolve_processed_root

    slug = "days_gt_32C"
    env_root = tmp_path / slug
    monkeypatch.setenv("IRT_PROCESSED_ROOT", str(env_root))

    out = resolve_processed_root(slug, data_dir=tmp_path, mode="portfolio")
    assert out == env_root.resolve()
