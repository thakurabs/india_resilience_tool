"""
Smoke import test for the IRT package.

This test ensures the package is importable without requiring installation
(editable install) by temporarily adding the repo root to sys.path.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import sys
from pathlib import Path


def test_import_india_resilience_tool() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

    import india_resilience_tool  # noqa: F401

    assert hasattr(india_resilience_tool, "__version__")
