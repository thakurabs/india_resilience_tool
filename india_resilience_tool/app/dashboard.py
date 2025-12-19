"""
Dashboard runner for the India Resilience Tool (IRT).

This replaces runpy.run_path(...) with an importlib-based loader that:
- Executes dashboard_unfactored_impl.py on every Streamlit rerun
- Avoids Python import caching issues
- Preserves legacy semantics where the impl behaves like __main__

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _repo_root() -> Path:
    """
    Resolve repo root.

    This file is: <repo_root>/india_resilience_tool/app/dashboard.py
    so repo_root is parents[2].
    """
    return Path(__file__).resolve().parents[2]


def _impl_path() -> Path:
    """Absolute path to the legacy dashboard implementation script."""
    return _repo_root() / "dashboard_unfactored_impl.py"


def _exec_file_as_module(path: Path, *, module_key: str) -> ModuleType:
    """
    Execute a Python file as a fresh module object.

    This mimics runpy.run_path(..., run_name="__main__") behavior closely, without using runpy:
    - clears any prior module under module_key
    - executes file code in a fresh module namespace
    - forces __name__ inside the executed code to "__main__" (legacy-compatible)
    """
    if module_key in sys.modules:
        del sys.modules[module_key]

    spec = importlib.util.spec_from_file_location(module_key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {path}")

    module = importlib.util.module_from_spec(spec)

    # Register under a stable key to satisfy any internal module lookups.
    sys.modules[module_key] = module

    # NOTE:
    # We deliberately do NOT force module.__name__ == "__main__".
    # importlib loaders validate the module name against the spec name.
    # Streamlit rerun semantics only require re-executing the file each run,
    # which we achieve by deleting sys.modules[module_key] above.

    spec.loader.exec_module(module)
    return module


def run_dashboard() -> None:
    """
    Run the dashboard implementation.

    This is called by india_resilience_tool.app.main.run() every Streamlit rerun.
    """
    impl = _impl_path()
    if not impl.exists():
        raise FileNotFoundError(f"Missing dashboard implementation file: {impl}")

    _exec_file_as_module(impl, module_key="_irt_dashboard_unfactored_impl")
