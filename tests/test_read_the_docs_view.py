"""Tests for the Read the Docs Streamlit view helpers."""

from __future__ import annotations

from india_resilience_tool.app.views.read_the_docs_view import (
    DOCS_RESIZER_LISTENER_KEY,
    DOCS_RESIZER_MARKER_KEY,
    _build_docs_resizer_html,
    _build_docs_resizer_marker_html,
    _stamp_theme,
)


def test_stamp_theme_replaces_existing_html_theme_idempotently() -> None:
    html = '<html lang="en" data-theme="light"><body>Docs</body></html>'

    stamped = _stamp_theme(html, "dark")
    stamped_twice = _stamp_theme(stamped, "dark")

    assert stamped == '<html lang="en" data-theme="dark"><body>Docs</body></html>'
    assert stamped_twice == stamped
    assert stamped.count("data-theme") == 1


def test_stamp_theme_handles_uppercase_html_and_invalid_theme() -> None:
    html = '<HTML lang="en"><body>Docs</body></HTML>'

    assert _stamp_theme(html, "not-a-theme").startswith('<html lang="en" data-theme="light">')


def test_stamp_theme_handles_generated_wrapper_without_html_root() -> None:
    html = '<div class="doc-shell" data-irt-doc-root><main>Docs</main></div>'

    stamped = _stamp_theme(html, "dark")

    assert stamped.startswith('<div class="doc-shell" data-irt-doc-root data-theme="dark">')
    assert stamped.count("data-theme") == 1


def test_stamp_theme_wraps_fragment_when_no_known_root() -> None:
    stamped = _stamp_theme("<main>Docs</main>", "dark")

    assert stamped == '<div data-theme="dark" data-irt-doc-root><main>Docs</main></div>'


def test_docs_resizer_marker_uses_stable_key() -> None:
    marker = _build_docs_resizer_marker_html()

    assert "irt-read-the-docs-marker" in marker
    assert f'data-docs-key="{DOCS_RESIZER_MARKER_KEY}"' in marker


def test_docs_resizer_contract_strings_are_stable() -> None:
    html = _build_docs_resizer_html()

    assert DOCS_RESIZER_MARKER_KEY in html
    assert DOCS_RESIZER_LISTENER_KEY in html
    assert "parentWindow.removeEventListener(\"resize\", previous);" in html
    assert "parentWindow[listenerKey] = scheduleResize;" in html
    assert "target.style.height = `${height}px`;" in html
    assert "target.height = String(height);" in html
    assert "node.style.height = `${height}px`;" in html
    assert "iframe.contentDocument" in html
    assert 'childWindow.dispatchEvent(new Event("resize"));' in html
    assert "chooseTargetIframe(hostBlock, marker, selfFrame)" in html
