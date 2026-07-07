"""Tests for the offline Read the Docs HTML generator."""

from __future__ import annotations

import re
import shutil
import subprocess
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


def test_generated_html_uses_section_pages_and_collapsible_toc() -> None:
    html_doc, _size_info = _generated_html()

    assert 'data-section-id="' in html_doc
    assert '<section class="doc-section active" data-section-id="1-introduction-and-framing">' in html_doc
    assert ".doc-section{display:none}" in html_doc
    assert ".content.searching .doc-section{display:block}" in html_doc
    assert 'class="toc-group"' in html_doc
    assert 'class="toc-toggle' in html_doc
    assert 'class="toc-sub"' in html_doc
    assert "function showSection" in html_doc
    assert 'content.classList.add("searching")' in html_doc
    assert 'content.classList.remove("searching")' in html_doc


def test_generated_html_omits_dashboard_cover_sections() -> None:
    html_doc, _size_info = _generated_html()

    assert 'href="#india-resilience-tool-technical-guidance-note"' not in html_doc
    assert 'href="#climate-risk-methodology-data-metrics-and-bundle-construction"' not in html_doc
    assert 'data-section-id="india-resilience-tool-technical-guidance-note"' not in html_doc
    assert 'data-section-id="climate-risk-methodology-data-metrics-and-bundle-construction"' not in html_doc
    assert 'href="#1-introduction-and-framing"' in html_doc


def test_generated_html_emits_valid_search_escape_regex() -> None:
    html_doc, _size_info = _generated_html()

    assert r"/[.*+?^${}()|[\]\\]/g" in html_doc
    assert r"/[.*+?^${}()|[\]\]/g" not in html_doc
    assert r'"\\$&"' in html_doc
    assert r'"\$&"' not in html_doc


def test_generated_app_script_parses_when_node_is_available(tmp_path: Path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not available")

    html_doc, _size_info = _generated_html()
    script_match = re.search(r"<script>(?P<script>\s*\(function\(\).*?)</script>\s*</body>", html_doc, re.S)
    assert script_match is not None
    script_path = tmp_path / "read_the_docs_app.js"
    script_path.write_text(script_match.group("script"), encoding="utf-8")

    result = subprocess.run([node, "--check", str(script_path)], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_committed_asset_matches_generated_html_if_present() -> None:
    asset = Path("india_resilience_tool/app/assets/read_the_docs.html")
    if not asset.exists():
        pytest.skip("Read the Docs asset has not been generated")

    html_doc, _size_info = _generated_html()

    assert asset.read_text(encoding="utf-8") == html_doc
