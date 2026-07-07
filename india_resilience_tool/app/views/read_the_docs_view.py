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
DOCS_COMPONENT_HEIGHT = 900
DOCS_RESIZER_MARKER_KEY = "irt-read-the-docs"
DOCS_RESIZER_LISTENER_KEY = "__irtReadTheDocsResizeHandler"


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


def _build_docs_resizer_marker_html(marker_key: str = DOCS_RESIZER_MARKER_KEY) -> str:
    """Return a hidden parent-DOM marker placed immediately before the docs iframe."""
    return f'<div class="irt-read-the-docs-marker" data-docs-key="{marker_key}" style="display:none"></div>'


def _build_docs_resizer_html(
    *,
    marker_key: str = DOCS_RESIZER_MARKER_KEY,
    listener_key: str = DOCS_RESIZER_LISTENER_KEY,
    bottom_margin: int = 16,
    min_height: int = 420,
    fallback_height: int = DOCS_COMPONENT_HEIGHT,
) -> str:
    """Return parent-DOM JavaScript that resizes the docs iframe to available viewport height."""
    return f"""
    <script>
    (function() {{
      const selfFrame = window.frameElement;
      const parentWindow = window.parent;
      if (!selfFrame || !parentWindow) {{
        return;
      }}

      const markerKey = {marker_key!r};
      const listenerKey = {listener_key!r};
      const bottomMargin = {int(bottom_margin)};
      const minHeight = {int(min_height)};
      const fallbackHeight = {int(fallback_height)};

      function coerceHeight(value) {{
        const rounded = Math.round(Number(value));
        if (!Number.isFinite(rounded)) {{
          return fallbackHeight;
        }}
        return Math.max(minHeight, rounded);
      }}

      function updateWrapperHeight(iframe, height) {{
        let node = iframe;
        for (let depth = 0; node && depth < 4; depth += 1) {{
          if (node.style) {{
            node.style.height = `${{height}}px`;
            node.style.minHeight = `${{height}}px`;
          }}
          node = node.parentElement;
        }}
      }}

      function updateInnerDocument(iframe, height) {{
        try {{
          const childDocument = iframe.contentDocument;
          const childWindow = iframe.contentWindow;
          if (!childDocument) {{
            return;
          }}
          if (childDocument.documentElement) {{
            childDocument.documentElement.style.height = `${{height}}px`;
          }}
          if (childDocument.body) {{
            childDocument.body.style.height = `${{height}}px`;
          }}
          if (childWindow && typeof childWindow.dispatchEvent === "function") {{
            childWindow.dispatchEvent(new Event("resize"));
          }}
        }} catch (error) {{
          /* Same-origin access is best-effort; the outer iframe height is sufficient. */
        }}
      }}

      function chooseTargetIframe(block, marker, resizerFrame) {{
        const markerTop = marker.getBoundingClientRect().top;
        const resizerTop = resizerFrame.getBoundingClientRect().top;
        return Array.from(block.querySelectorAll("iframe"))
          .filter(function(iframe) {{
            if (iframe === resizerFrame) {{
              return false;
            }}
            const top = iframe.getBoundingClientRect().top;
            return top >= markerTop && top <= resizerTop;
          }})
          .sort(function(left, right) {{
            const leftDistance = Math.abs(left.getBoundingClientRect().top - markerTop);
            const rightDistance = Math.abs(right.getBoundingClientRect().top - markerTop);
            return leftDistance - rightDistance;
          }})[0] || null;
      }}

      function resizeDocsIframe() {{
        const hostBlock = selfFrame.closest('[data-testid="stVerticalBlock"]');
        if (!hostBlock) {{
          return;
        }}
        const marker = hostBlock.querySelector(`.irt-read-the-docs-marker[data-docs-key="${{markerKey}}"]`);
        if (!marker) {{
          return;
        }}
        const target = chooseTargetIframe(hostBlock, marker, selfFrame);
        if (!target) {{
          return;
        }}
        const available = parentWindow.innerHeight - target.getBoundingClientRect().top - bottomMargin;
        const height = coerceHeight(available);
        target.style.height = `${{height}}px`;
        target.height = String(height);
        updateWrapperHeight(target, height);
        updateInnerDocument(target, height);
      }}

      function scheduleResize() {{
        if (parentWindow && typeof parentWindow.requestAnimationFrame === "function") {{
          parentWindow.requestAnimationFrame(resizeDocsIframe);
          return;
        }}
        window.setTimeout(resizeDocsIframe, 0);
      }}

      scheduleResize();
      window.setTimeout(scheduleResize, 50);
      window.setTimeout(scheduleResize, 250);
      window.setTimeout(scheduleResize, 1000);

      if (parentWindow && typeof parentWindow.addEventListener === "function") {{
        const previous = parentWindow[listenerKey];
        if (previous) {{
          parentWindow.removeEventListener("resize", previous);
        }}
        parentWindow[listenerKey] = scheduleResize;
        parentWindow.addEventListener("resize", scheduleResize);
      }}
    }})();
    </script>
    """


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
    st.markdown(_build_docs_resizer_marker_html(), unsafe_allow_html=True)
    components.html(_stamp_theme(html_doc, _current_theme()), height=DOCS_COMPONENT_HEIGHT, scrolling=True)
    components.html(_build_docs_resizer_html(), height=0, width=0)
