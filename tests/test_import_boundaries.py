"""
Import-boundary enforcement tests for IRT.

These tests protect the modular boundary contract:
- analysis/data/viz/compute/config/utils must remain Streamlit-free
- runtime package code must not import tools/
"""

from __future__ import annotations

import ast
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_PKG_ROOT = _REPO_ROOT / "india_resilience_tool"


def _py_files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return [p for p in path.rglob("*.py") if p.is_file()]


def _imports_in_file(path: Path) -> list[tuple[str, str]]:
    """
    Return a list of (kind, module) imports from a file.

    kind:
      - "import": `import X`
      - "from":   `from X import ...`
    module:
      - the imported module name (best-effort)
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[tuple[str, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(("import", alias.name))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            imports.append(("from", mod))

    return imports


def test_streamlit_free_layers_do_not_import_streamlit() -> None:
    """
    Enforce that non-UI layers remain Streamlit-free.
    """
    dirs = [
        _PKG_ROOT / "analysis",
        _PKG_ROOT / "data",
        _PKG_ROOT / "viz",
        _PKG_ROOT / "compute",
        _PKG_ROOT / "config",
        _PKG_ROOT / "utils",
    ]

    offenders: list[str] = []
    for d in dirs:
        for f in _py_files_under(d):
            for kind, mod in _imports_in_file(f):
                m = str(mod or "")
                if m == "streamlit" or m.startswith("streamlit."):
                    offenders.append(f"{f.relative_to(_REPO_ROOT)}: {kind} {m}")

    assert not offenders, "Streamlit import(s) found in Streamlit-free layers:\n- " + "\n- ".join(offenders)


def test_package_does_not_import_tools() -> None:
    """
    Enforce that runtime package modules do not import tools/.
    """
    offenders: list[str] = []
    for f in _py_files_under(_PKG_ROOT):
        for kind, mod in _imports_in_file(f):
            m = str(mod or "").strip()
            if m == "tools" or m.startswith("tools."):
                offenders.append(f"{f.relative_to(_REPO_ROOT)}: {kind} {m}")

    assert not offenders, "tools import(s) found under india_resilience_tool/:\n- " + "\n- ".join(offenders)
