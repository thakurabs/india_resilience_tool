"""Tests for the offline Read the Docs HTML generator."""

from __future__ import annotations

import base64
import html as html_lib
import re
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from tools.docs import build_technical_note_html as builder


BANNED_VISIBLE_CHROME = (
    "FIG-",
    "Source:",
    "Scope caveat",
    "Important limitation",
    "Figure note",
    "Method note",
    "District B callout",
)


@lru_cache(maxsize=1)
def _generated_html() -> tuple[str, dict[str, object]]:
    return builder.build_html()


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _visible_svg_text(root: ET.Element) -> list[str]:
    return [
        "".join(node.itertext())
        for node in root.iter()
        if _local_name(node.tag) == "text"
    ]


def _embedded_svg_roots(html_doc: str) -> list[ET.Element]:
    roots: list[ET.Element] = []
    for payload in re.findall(r'src="data:image/svg\+xml;base64,([^"]+)"', html_doc):
        roots.append(ET.fromstring(base64.b64decode(payload)))
    return roots


def _math_label_count(root: ET.Element) -> int:
    return sum(
        1
        for node in root.iter()
        if _local_name(node.tag) == "g"
        and "math-label" in node.attrib.get("class", "").split()
        and node.attrib.get("data-math-tex")
    )


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
    assert html_doc.count("<figure") == 16
    assert "<figcaption" not in html_doc
    assert "http://" not in html_doc
    assert "https://" not in html_doc
    assert not builder.URL_RE.search(html_doc)
    assert 'id="katex-css"' in html_doc
    assert 'id="katex-js"' in html_doc
    assert len(html_doc.encode("utf-8")) < builder.MAX_HTML_BYTES
    assert size_info["html_bytes"] == len(html_doc.encode("utf-8"))

    figures = re.findall(r'<figure class="doc-figure"[^>]*>.*?</figure>', html_doc, re.S)
    assert len(figures) == 16
    for figure_html in figures:
        img_match = re.search(r"<img\b[^>]*\balt=\"([^\"]*)\"", figure_html)
        assert img_match is not None
        assert html_lib.unescape(img_match.group(1)).strip()

    assert len(_embedded_svg_roots(html_doc)) == 14
    for root in _embedded_svg_roots(html_doc):
        visible_text_nodes = [node for node in root.iter() if _local_name(node.tag) == "text"]
        assert not any(node.attrib.get("class") in {"title", "subtitle", "note"} for node in visible_text_nodes)
        view_box = [float(part) for part in root.attrib["viewBox"].split()]
        assert view_box[3] < 760
        assert float(root.attrib["height"]) == view_box[3]
        visible_text = "\n".join(_visible_svg_text(root))
        for banned in BANNED_VISIBLE_CHROME:
            assert banned not in visible_text

    assert ".doc-figure{margin:24px 0;padding:8px;" in html_doc


def test_svg_math_overlays_match_embedded_svg_metadata_counts() -> None:
    html_doc, _size_info = _generated_html()
    figures = re.findall(r'<figure class="doc-figure"[^>]*>.*?</figure>', html_doc, re.S)

    for figure_html in figures:
        svg_payload = re.search(r'src="data:image/svg\+xml;base64,([^"]+)"', figure_html)
        if svg_payload is None:
            assert "figure-math-overlay" not in figure_html
            continue
        root = ET.fromstring(base64.b64decode(svg_payload.group(1)))
        metadata_count = _math_label_count(root)
        overlay_count = figure_html.count('class="figure-math-overlay"')
        assert overlay_count == metadata_count
        if metadata_count:
            assert 'class="figure-media"' in figure_html
            assert 'class="figure-math-layer"' in figure_html


def test_svg_math_extraction_subtracts_nonzero_viewbox_origin_and_hides_only_math_groups() -> None:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="400" viewBox="0 100 1200 400">
<text x="10" y="120">ordinary text</text>
<g class="math-label" data-math-tex="x^2" data-math-x="600" data-math-y="300" data-math-anchor="middle" data-math-size="20" data-math-display="0">
<text x="600" y="300">x^2</text>
</g>
</svg>"""

    hidden_svg, overlays, view_box = builder._extract_svg_math(svg)
    root = ET.fromstring(hidden_svg)
    ordinary = next(node for node in root.iter() if _local_name(node.tag) == "text" and "".join(node.itertext()) == "ordinary text")
    math_group = next(node for node in root.iter() if _local_name(node.tag) == "g")

    assert view_box == (0.0, 100.0, 1200.0, 400.0)
    assert len(overlays) == 1
    assert overlays[0].left_pct == pytest.approx(50.0)
    assert overlays[0].top_pct == pytest.approx(50.0)
    assert ordinary.attrib.get("style") != "display:none"
    assert "display:none" in math_group.attrib["style"]
    assert math_group.attrib["aria-hidden"] == "true"


def test_generated_html_keeps_base64_svg_embedding_and_hides_math_fallbacks_only() -> None:
    html_doc, _size_info = _generated_html()

    for figure_html in re.findall(r'<figure class="doc-figure"[^>]*>.*?</figure>', html_doc, re.S):
        assert "<svg xmlns=" not in figure_html
    assert 'src="data:image/svg+xml;base64,' in html_doc
    assert 'data-tex="' in html_doc
    for root in _embedded_svg_roots(html_doc):
        for node in root.iter():
            if _local_name(node.tag) == "g" and "math-label" in node.attrib.get("class", "").split():
                assert "display:none" in node.attrib.get("style", "")
            if _local_name(node.tag) == "text" and "".join(node.itertext()).strip() == "ordinary text":
                assert "display:none" not in node.attrib.get("style", "")


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


def test_generated_html_uses_readable_content_width() -> None:
    html_doc, _size_info = _generated_html()

    assert (
        ".doc-shell{display:grid;grid-template-columns:minmax(220px,290px) minmax(0,1fr);"
        "height:100vh;overflow:hidden;max-width:calc(1210px + clamp(24px,4vw,64px));"
        "margin:0 auto}"
    ) in html_doc
    assert (
        ".content{width:calc(100% - clamp(24px,4vw,64px));max-width:920px;"
        "margin:0 auto 0 clamp(24px,4vw,64px);padding:34px 40px 96px}"
    ) in html_doc
    assert ".doc-shell{display:block;overflow:auto;max-width:none;margin:0}" in html_doc
    assert ".content{width:100%;margin:0;padding:22px 18px 88px}" in html_doc
    assert "html{font-size:var(--irt-doc-font-size,15px)}" in html_doc
    assert ".content{max-width:none" not in html_doc


def test_generated_html_omits_visible_build_provenance_footer() -> None:
    html_doc, _size_info = _generated_html()

    assert "Build provenance:" not in html_doc
    assert 'class="provenance"' not in html_doc


def test_generated_html_uses_delegated_figure_zoom_and_live_headings() -> None:
    html_doc, _size_info = _generated_html()

    assert 'content.addEventListener("click", function(event)' in html_doc
    assert 'event.target.closest(".figure-zoom")' in html_doc
    assert 'zoom.querySelector("img")' in html_doc
    assert "document.querySelectorAll('.figure-zoom img').forEach" not in html_doc
    assert 'document.querySelectorAll(".figure-zoom img").forEach' not in html_doc
    assert "function collectHeadings()" in html_doc
    assert "const headings = collectHeadings();" in html_doc


def test_generated_html_has_accessible_lightbox_close_controls() -> None:
    html_doc, _size_info = _generated_html()

    assert 'role="dialog"' in html_doc
    assert 'aria-modal="true"' in html_doc
    assert 'aria-label="Enlarged figure"' in html_doc
    assert 'class="lightbox-close"' in html_doc
    assert 'aria-label="Close enlarged figure"' in html_doc
    assert "function closeLightbox()" in html_doc
    assert 'document.querySelector(".lightbox-close")' in html_doc
    assert 'lightboxClose.addEventListener("click", closeLightbox)' in html_doc
    assert 'lightboxImage.removeAttribute("src")' in html_doc
    assert 'lightboxImage.alt = "";' in html_doc
    assert "event.target === lightbox" in html_doc
    assert 'event.key === "Escape"' in html_doc
    assert 'lightbox.classList.contains("open")' in html_doc


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


def test_generated_html_renders_svg_math_overlays_explicitly() -> None:
    html_doc, _size_info = _generated_html()

    assert ".figure-media{position:relative;display:block;width:100%}" in html_doc
    assert ".figure-math-layer{position:absolute;inset:0;pointer-events:none}" in html_doc
    assert ".figure-math-overlay .katex-display{margin:0}" in html_doc
    assert 'aria-hidden="true" data-tex=' in html_doc
    assert "function renderFigureMath(root)" in html_doc
    assert "katex.render(overlay.dataset.tex || \"\", overlay" in html_doc
    assert "dataSize * renderedWidth / viewBoxWidth" in html_doc
    assert 'window.addEventListener("resize"' in html_doc


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


def test_vendored_katex_renders_representative_svg_overlay_tex_when_node_is_available(tmp_path: Path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not available")

    script = tmp_path / "katex_smoke.js"
    script.write_text(
        "const katex = require(" + repr(str(builder.KATEX_ROOT / "katex.min.js")) + ");\n"
        r"const tex = String.raw`\bar{v}_i = \frac{\sum_j a_{ij}v_j}{\sum_j a_{ij}}`;" + "\n"
        "const html = katex.renderToString(tex, {throwOnError:false});\n"
        "if (!html.includes('katex')) process.exit(1);\n",
        encoding="utf-8",
    )

    result = subprocess.run([node, str(script)], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_committed_asset_matches_generated_html_if_present() -> None:
    asset = Path("india_resilience_tool/app/assets/read_the_docs.html")
    if not asset.exists():
        pytest.skip("Read the Docs asset has not been generated")

    html_doc, _size_info = _generated_html()

    assert asset.read_text(encoding="utf-8") == html_doc
