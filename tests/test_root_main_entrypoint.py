"""
Sanity check: repo-root Streamlit entrypoint exists.

This test intentionally does not import Streamlit or execute the app.
"""

from __future__ import annotations

from pathlib import Path


def test_root_main_py_exists_and_mentions_app_entrypoint() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    main_py = repo_root / "main.py"
    assert main_py.exists(), "Expected repo-root main.py to exist"

    text = main_py.read_text(encoding="utf-8")
    assert "india_resilience_tool.app.main" in text
    assert "__name__" in text and "__main__" in text

