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


def test_generated_html_has_no_duplicate_ids() -> None:
    html_doc, _size_info = _generated_html()
    ids = re.findall(r"(?<![-\w])id=\"([^\"]+)\"", html_doc)

    assert ids
    assert len(ids) == len(set(ids))


def test_reference_entries_have_deterministic_ids() -> None:
    markdown = Path("docs/technical_guidance_note.md").read_text(encoding="utf-8")
    display_markdown = builder._strip_dashboard_cover(markdown)
    entries = builder.parse_reference_entries(display_markdown)
    ids_by_source = {entry.source_text: entry.element_id for entry in entries}

    assert ids_by_source[
        next(text for text in ids_by_source if text.startswith("Thrasher, B."))
    ] == "ref-thrasher-2022"
    assert ids_by_source[
        next(text for text in ids_by_source if text.startswith("Department of Science and Technology"))
    ] == "ref-dst-2021"
    assert ids_by_source[
        next(text for text in ids_by_source if text.startswith("Reserve Bank of India"))
    ] == "ref-rbi-2023"
    assert ids_by_source[
        next(text for text in ids_by_source if text.startswith("Government of India (2008)"))
    ] == "ref-government-of-india-2008"
    assert ids_by_source[
        next(text for text in ids_by_source if text.startswith("Government of India, Ministry of Finance"))
    ] == "ref-government-of-india-2018"


def test_citations_link_to_reference_entries() -> None:
    html_doc, _size_info = _generated_html()

    expected_links = [
        '<a class="citation-ref" href="#ref-dst-2021">DST 2021</a>',
        '<a class="citation-ref" href="#ref-rbi-2023">RBI 2023</a>',
        '<a class="citation-ref" href="#ref-dubash-2014">Dubash &amp; Jogesh 2014</a>;',
        '<a class="citation-ref" href="#ref-baugh-2024">Baugh et al. (2024)</a>',
        '<a class="citation-ref" href="#ref-stull-2011">Stull (2011)</a>',
    ]

    for link in expected_links:
        assert link in html_doc


def test_citation_links_preserve_visible_text_and_trailing_punctuation() -> None:
    html_doc, _size_info = _generated_html()

    assert (
        'path (<a class="citation-ref" href="#ref-dubash-2014">'
        "Dubash &amp; Jogesh 2014</a>; "
        '<a class="citation-ref" href="#ref-singh-2017">Singh et al. 2017</a>).'
    ) in html_doc


def test_inline_citations_support_and_alias_and_skip_code_spans() -> None:
    markdown = """## Body

`Dubash and Jogesh 2014` and Dubash and Jogesh 2014.

## References

Dubash, N. K., and Jogesh, A. (2014). *From Margins to Mainstream?*
"""

    html_doc, _headings = builder.render_markdown(markdown, {})

    assert "<code>Dubash and Jogesh 2014</code>" in html_doc
    assert '<a class="citation-ref" href="#ref-dubash-2014">Dubash and Jogesh 2014</a>.' in html_doc


def test_false_positive_year_mentions_remain_plain_text() -> None:
    html_doc, _size_info = _generated_html()

    for phrase in ("Kerala 2018", "Mumbai 2005", "Economic Survey 2017"):
        assert phrase in html_doc
        assert not re.search(rf'<a class="citation-ref"[^>]*>[^<]*{re.escape(phrase)}', html_doc)


def test_reference_hrefs_resolve_to_existing_reference_ids() -> None:
    html_doc, _size_info = _generated_html()
    ids = set(re.findall(r'\bid="(ref-[^"]+)"', html_doc))
    hrefs = re.findall(r'href="#(ref-[^"]+)"', html_doc)

    assert hrefs
    assert set(hrefs) <= ids


def test_reference_entries_are_targets_without_external_links() -> None:
    html_doc, _size_info = _generated_html()
    reference_entries = re.findall(r'<p id="ref-[^"]+" class="reference-entry" tabindex="-1">.*?</p>', html_doc)

    assert reference_entries
    assert any("https&#58;//doi.org/10.1038/s41597-022-01393-4" in entry for entry in reference_entries)
    assert all("<a " not in entry for entry in reference_entries)
    assert not any('href="https' in entry for entry in reference_entries)


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


def test_generated_html_focuses_reference_targets_from_existing_hash_handler() -> None:
    html_doc, _size_info = _generated_html()

    assert "function focusReferenceTarget(target)" in html_doc
    assert 'target.classList.contains("reference-entry")' in html_doc
    assert "target.focus({preventScroll:true});" in html_doc
    assert 'target.classList.add("is-focused");' in html_doc


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
