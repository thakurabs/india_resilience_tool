"""Tests for the offline Read the Docs HTML generator."""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pytest

from tools.docs import build_technical_note_html as builder


@lru_cache(maxsize=1)
def _generated_html() -> tuple[str, dict[str, object]]:
    return builder.build_html()


def test_figure_manifest_resolves_exact_approved_set() -> None:
    markdown = Path("docs/technical_guidance_note.md").read_text(encoding="utf-8")

    assets = builder.validate_figure_manifest(markdown)

    assert [asset.filename for asset in assets] == list(builder.APPROVED_FIGURES)


def test_figure_resolution_rejects_path_traversal() -> None:
    with pytest.raises(ValueError, match="not in APPROVED_FIGURES"):
        builder.resolve_figure_asset("../technical_note/FIG-V1V2V3_taylor.png")


def test_generated_html_invariants() -> None:
    html_doc, size_info = _generated_html()

    assert "[FIGURE:" not in html_doc
    assert "[FIGURES TO INSERT]" not in html_doc
    assert html_doc.count("<figure") == 18
    assert "http://" not in html_doc
    assert "https://" not in html_doc
    assert not builder.URL_RE.search(html_doc)
    assert 'id="katex-css"' in html_doc
    assert 'id="katex-js"' in html_doc
    assert len(html_doc.encode("utf-8")) < builder.MAX_HTML_BYTES
    assert size_info["html_bytes"] == len(html_doc.encode("utf-8"))


def test_generated_html_has_no_duplicate_heading_ids() -> None:
    html_doc, _size_info = _generated_html()
    ids = re.findall(r"<h[1-6] id=\"([^\"]+)\"", html_doc)

    assert ids
    assert len(ids) == len(set(ids))


def test_build_is_deterministic_without_timestamp() -> None:
    first, _first_info = _generated_html()
    second, _second_info = builder.build_html()

    assert first == second
    assert "built_at_utc" not in first
    assert "git_sha" not in first


def test_committed_asset_is_under_size_ceiling_if_present() -> None:
    asset = Path("india_resilience_tool/app/assets/read_the_docs.html")
    if not asset.exists():
        pytest.skip("Read the Docs asset has not been generated")

    assert asset.stat().st_size < builder.MAX_HTML_BYTES


def test_generated_html_intercepts_hash_links_without_css_selectors() -> None:
    html_doc, _size_info = _generated_html()

    assert 'closest ? event.target.closest(\'a[href^="#"]\')' in html_doc
    assert "event.preventDefault();" in html_doc
    assert "document.getElementById(href.slice(1))" in html_doc
    assert "querySelector(a.getAttribute('href'))" not in html_doc
    assert 'querySelector(a.getAttribute("href"))' not in html_doc
    assert 'scroller.scrollTo({ top: Math.max(0, targetTop), behavior: "smooth" });' in html_doc


def test_generated_html_uses_full_bleed_content_width() -> None:
    html_doc, _size_info = _generated_html()

    assert ".content{max-width:none;margin:0;padding:34px 40px 96px}" in html_doc
    assert ".content{max-width:980px" not in html_doc


def test_generated_html_uses_delegated_figure_zoom_and_live_headings() -> None:
    html_doc, _size_info = _generated_html()

    assert 'content.addEventListener("click", function(event)' in html_doc
    assert 'event.target.closest(".figure-zoom img")' in html_doc
    assert "document.querySelectorAll('.figure-zoom img').forEach" not in html_doc
    assert 'document.querySelectorAll(".figure-zoom img").forEach' not in html_doc
    assert "function collectHeadings()" in html_doc
    assert "const headings = collectHeadings();" in html_doc


def test_committed_asset_matches_generated_html_if_present() -> None:
    asset = Path("india_resilience_tool/app/assets/read_the_docs.html")
    if not asset.exists():
        pytest.skip("Read the Docs asset has not been generated")

    html_doc, _size_info = _generated_html()

    assert asset.read_text(encoding="utf-8") == html_doc
