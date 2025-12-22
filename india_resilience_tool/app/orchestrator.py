"""
Dashboard orchestrator (Step 22).

This module becomes the canonical runnable entrypoint for the dashboard logic.
It executes the legacy implementation file located inside the package on every
Streamlit rerun, avoiding import caching.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _legacy_impl_path() -> Path:
    """
    Path to the legacy dashboard implementation file.

    After Step 22 it lives at:
      india_resilience_tool/app/legacy_dashboard_impl.py
    """
    return Path(__file__).resolve().parent / "legacy_dashboard_impl.py"


def _exec_file_as_module(path: Path, *, module_key: str) -> ModuleType:
    """
    Execute a Python file as a fresh module object.

    We avoid normal import caching by deleting sys.modules[module_key] each time.
    We do NOT force __name__="__main__" because importlib validates module names
    against the spec name.

    Returns:
        The executed module object.
    """
    if module_key in sys.modules:
        del sys.modules[module_key]

    spec = importlib.util.spec_from_file_location(module_key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)
    return module


def run_app() -> None:
    """
    Run the dashboard application.

    This is called by the root-level shim dashboard_unfactored_impl.py,
    which is itself executed each rerun by app/dashboard.py (Step 21).
    """
    impl = _legacy_impl_path()
    if not impl.exists():
        raise FileNotFoundError(f"Missing legacy dashboard implementation file: {impl}")

    _exec_file_as_module(impl, module_key="_irt_legacy_dashboard_impl")
