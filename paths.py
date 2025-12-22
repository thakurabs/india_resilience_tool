"""
Compatibility shim for legacy imports.

The dashboard currently imports:
    from paths import DATA_DIR

This module preserves those public names while delegating the actual semantics
to `india_resilience_tool.config.paths`.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path

from india_resilience_tool.config.paths import (
    BASE_OUTPUT_ROOT,
    DATA_DIR,
    DATA_ROOT,
    DISTRICTS_PATH,
    PROJECTS_ROOT,
    REPO_ROOT,
    debug_enabled_default,
    find_repo_root,
    get_paths_config,
    pilot_state_default,
    resolve_processed_root,
)

__all__ = [
    "Path",
    "find_repo_root",
    "get_paths_config",
    "resolve_processed_root",
    "pilot_state_default",
    "debug_enabled_default",
    "REPO_ROOT",
    "PROJECTS_ROOT",
    "DATA_DIR",
    "DATA_ROOT",
    "DISTRICTS_PATH",
    "BASE_OUTPUT_ROOT",
]
