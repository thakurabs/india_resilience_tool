"""Tests for the Read the Docs Streamlit view helpers."""

from __future__ import annotations

from pathlib import Path

from india_resilience_tool.app.views import read_the_docs_view as docs_view
from india_resilience_tool.app.views.read_the_docs_view import (
    DOCS_DEFAULT_FONT_SIZE_LABEL,
    DOCS_FONT_SIZE_OPTIONS,
    DOCS_RESIZER_LISTENER_KEY,
    DOCS_RESIZER_MARKER_KEY,
    _build_docs_resizer_html,
    _build_docs_resizer_marker_html,
    _font_size_value,
    _stamp_document_preferences,
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


def test_font_size_normalization_accepts_only_option_labels() -> None:
    assert _font_size_value("Large") == DOCS_FONT_SIZE_OPTIONS["Large"]
    assert _font_size_value("17px") == DOCS_FONT_SIZE_OPTIONS[DOCS_DEFAULT_FONT_SIZE_LABEL]
    assert _font_size_value("not-a-size") == DOCS_FONT_SIZE_OPTIONS[DOCS_DEFAULT_FONT_SIZE_LABEL]


def test_stamp_document_preferences_replaces_existing_html_preferences_idempotently() -> None:
    html = '<html lang="en" data-theme="light"><body>Docs</body></html>'

    stamped = _stamp_document_preferences(html, "dark", "Large")
    stamped_twice = _stamp_document_preferences(stamped, "dark", "Large")

    assert stamped == (
        '<html lang="en" data-theme="dark" style="--irt-doc-font-size:17px"><body>Docs</body></html>'
    )
    assert stamped_twice == stamped
    assert stamped.count("data-theme") == 1
    assert stamped.count("--irt-doc-font-size") == 1


def test_stamp_document_preferences_preserves_unrelated_style_and_replaces_stale_font_size() -> None:
    html = '<html lang="en" style="color:red; --irt-doc-font-size:99px" data-theme="light"><body>Docs</body></html>'

    stamped = _stamp_document_preferences(html, "dark", "Small")

    assert stamped.startswith('<html lang="en" data-theme="dark" style="color:red; --irt-doc-font-size:14px">')
    assert stamped.count("data-theme") == 1
    assert stamped.count("--irt-doc-font-size") == 1
    assert "99px" not in stamped


def test_stamp_document_preferences_handles_generated_wrapper_without_html_root() -> None:
    html = '<div class="doc-shell" style="color:red" data-irt-doc-root><main>Docs</main></div>'

    stamped = _stamp_document_preferences(html, "dark", "Large")

    assert stamped.startswith(
        '<div class="doc-shell" data-irt-doc-root data-theme="dark" '
        'style="color:red; --irt-doc-font-size:17px">'
    )
    assert stamped.count("data-theme") == 1
    assert stamped.count("--irt-doc-font-size") == 1


def test_stamp_document_preferences_wraps_fragment_when_no_known_root() -> None:
    stamped = _stamp_document_preferences("<main>Docs</main>", "dark", "17px")

    assert stamped == (
        '<div data-theme="dark" style="--irt-doc-font-size:15px" '
        "data-irt-doc-root><main>Docs</main></div>"
    )


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


def test_render_read_the_docs_keeps_resizer_marker_immediately_before_iframe(monkeypatch, tmp_path: Path) -> None:
    asset = tmp_path / "read_the_docs.html"
    asset.write_text('<html lang="en"><body>Docs</body></html>', encoding="utf-8")
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(docs_view, "_resolve_asset", lambda: asset)
    monkeypatch.setattr(docs_view, "_current_theme", lambda: "light")
    monkeypatch.setattr(
        docs_view.st,
        "selectbox",
        lambda *args, **kwargs: calls.append(("selectbox", str(kwargs.get("key")))) or "Large",
    )
    monkeypatch.setattr(
        docs_view.st,
        "markdown",
        lambda body, **kwargs: calls.append(("markdown", body)),
    )
    monkeypatch.setattr(
        docs_view.components,
        "html",
        lambda body, **kwargs: calls.append(("html", body)),
    )

    docs_view.render_read_the_docs()

    assert [kind for kind, _body in calls] == ["selectbox", "markdown", "html", "html"]
    assert DOCS_RESIZER_MARKER_KEY in calls[1][1]
    assert "--irt-doc-font-size:17px" in calls[2][1]
    assert DOCS_RESIZER_LISTENER_KEY in calls[3][1]
