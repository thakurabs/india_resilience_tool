"""Streamlit view for the committed Technical Guidance Note HTML asset."""

from __future__ import annotations

import re
from importlib import resources
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

ASSET_PACKAGE = "india_resilience_tool.app.assets"
ASSET_NAME = "read_the_docs.html"
_HTML_TAG_RE = re.compile(r"<\s*html\b([^>]*)>", flags=re.IGNORECASE)
_DATA_THEME_RE = re.compile(r"""\sdata-theme\s*=\s*(['"])(.*?)\1""", flags=re.IGNORECASE)
_DOC_ROOT_RE = re.compile(r"<\s*div\b([^>]*\bdata-irt-doc-root\b[^>]*)>", flags=re.IGNORECASE)


def _resolve_asset() -> Path | None:
    """Return the packaged Read the Docs HTML asset path when present."""
    try:
        asset = resources.files(ASSET_PACKAGE).joinpath(ASSET_NAME)
    except (ModuleNotFoundError, FileNotFoundError):
        return None
    if not asset.is_file():
        return None
    with resources.as_file(asset) as path:
        return Path(path)


def _normalize_theme(theme: object, *, default: str = "light") -> str:
    value = str(theme or "").strip().lower()
    if value in {"light", "dark"}:
        return value
    fallback = str(default or "").strip().lower()
    return fallback if fallback in {"light", "dark"} else "light"


def _stamp_tag_theme(match: re.Match[str], theme: str, tag_name: str) -> str:
    attrs = match.group(1)
    attrs = _DATA_THEME_RE.sub("", attrs)
    return f"<{tag_name}{attrs} data-theme=\"{theme}\">"


def _stamp_theme(html: str, theme: str) -> str:
    """Stamp exactly one light/dark theme marker onto the document root."""
    normalized = _normalize_theme(theme)
    stamped, count = _HTML_TAG_RE.subn(
        lambda match: _stamp_tag_theme(match, normalized, "html"),
        html,
        count=1,
    )
    if count:
        return stamped
    stamped, count = _DOC_ROOT_RE.subn(
        lambda match: _stamp_tag_theme(match, normalized, "div"),
        html,
        count=1,
    )
    if count:
        return stamped
    return f'<div data-theme="{normalized}" data-irt-doc-root>{html}</div>'


def _current_theme() -> str:
    context_theme = getattr(getattr(st, "context", None), "theme", None)
    context_type = getattr(context_theme, "type", None)
    if str(context_type or "").strip().lower() in {"light", "dark"}:
        return str(context_type).strip().lower()
    return _normalize_theme(st.get_option("theme.base"))


def render_read_the_docs() -> None:
    """Render the committed Technical Guidance Note HTML in an iframe."""
    asset = _resolve_asset()
    if asset is None:
        st.warning(
            "Read the Docs asset is missing. Run "
            "`python -m tools.docs.build_technical_note_html` from the repo root."
        )
        return
    html_doc = asset.read_text(encoding="utf-8")
    components.html(_stamp_theme(html_doc, _current_theme()), height=900, scrolling=True)

