"""Build the Technical Guidance Note as a self-contained dashboard HTML asset."""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import mimetypes
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = REPO_ROOT / "docs" / "technical_guidance_note.md"
DEFAULT_OUTPUT = REPO_ROOT / "india_resilience_tool" / "app" / "assets" / "read_the_docs.html"
TECHNICAL_GUIDANCE_FIGURE_ROOT = REPO_ROOT / "docs" / "figures" / "technical_guidance_note"
TECHNICAL_NOTE_FIGURE_ROOT = REPO_ROOT / "docs" / "figures" / "technical_note"
KATEX_ROOT = Path(__file__).resolve().parent / "vendor" / "katex"
GENERATOR_VERSION = "read-the-docs-html-v1"
MAX_HTML_BYTES = 5 * 1024 * 1024
FIGURE_TOKEN_RE = re.compile(r"\[FIGURE:\s*([^|\]]+?)\s*\|\s*([^\]]+?)\s*\]")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
FENCE_RE = re.compile(r"^```")
URL_RE = re.compile(r"url\((['\"]?)(?!data:)([^)'\"\s]+)\1\)")
_SECTION_HEAD_RE = re.compile(r'^<h([12]) id="([^"]+)"')
_DASHBOARD_FIRST_SECTION = "## 1. Introduction and Framing"

APPROVED_FIGURES: tuple[str, ...] = (
    "fig_02_hazard_exposure_vulnerability_scope.svg",
    "fig_05_temporal_coverage_analysis_windows.svg",
    "fig_06_bcsd_schematic.svg",
    "fig_08_district_block_resolution_zoom.svg",
    "FIG-V1V2V3_taylor.png",
    "FIG-V4a_nrmse_era5.png",
    "FIG-V4b_nrmse_imd.png",
    "FIG-V5_rx1day_adilabad.png",
    "fig_01_pipeline_flow.svg",
    "fig_09_admin_first_vs_grid_first.svg",
    "fig_10_fractional_area_overlap_weights.svg",
    "fig_11_temporal_aggregation_ensemble_chain.svg",
    "fig_12_doy_percentile_threshold_curve.svg",
    "fig_14_spi_derivation.svg",
    "fig_15_jrc_rp100_severity_lookup_matrix.svg",
    "fig_18_three_lens_blended_rule.svg",
    "fig_19_impact_band_ramp.svg",
    "fig_20_district_a_b_lens_example.svg",
)


@dataclass(frozen=True)
class Heading:
    level: int
    text: str
    element_id: str


@dataclass(frozen=True)
class FigureAsset:
    filename: str
    path: Path
    mime_type: str
    byte_size: int


@dataclass(frozen=True)
class ReferenceEntry:
    element_id: str
    year: str
    aliases: tuple[str, ...]
    source_text: str


@dataclass(frozen=True)
class CitationIndex:
    entries: tuple[ReferenceEntry, ...]
    targets: dict[tuple[str, str], ReferenceEntry]
    alias_pattern: re.Pattern[str] | None


def parse_figure_tokens(markdown: str) -> list[tuple[str, str]]:
    """Return figure filename/caption pairs from explicit callout tokens."""
    return [(match.group(1).strip(), match.group(2).strip()) for match in FIGURE_TOKEN_RE.finditer(markdown)]


def resolve_figure_asset(filename: str) -> FigureAsset:
    """Resolve an approved figure filename, rejecting traversal and unknown files."""
    if filename not in APPROVED_FIGURES:
        raise ValueError(f"Figure {filename!r} is not in APPROVED_FIGURES")
    if Path(filename).name != filename:
        raise ValueError(f"Figure filename must be a basename, got {filename!r}")
    roots = (TECHNICAL_GUIDANCE_FIGURE_ROOT, TECHNICAL_NOTE_FIGURE_ROOT)
    for root in roots:
        candidate = (root / filename).resolve()
        try:
            inside_root = candidate.is_relative_to(root.resolve())
        except AttributeError:  # pragma: no cover - Python < 3.9 compatibility.
            inside_root = os.path.commonpath([str(candidate), str(root.resolve())]) == str(root.resolve())
        if inside_root and candidate.exists():
            mime = "image/svg+xml" if candidate.suffix.lower() == ".svg" else "image/png"
            return FigureAsset(filename=filename, path=candidate, mime_type=mime, byte_size=candidate.stat().st_size)
    raise FileNotFoundError(f"Approved figure {filename!r} not found in figure roots")


def validate_figure_manifest(markdown: str) -> list[FigureAsset]:
    """Validate that the note references exactly the approved 18-figure manifest."""
    tokens = parse_figure_tokens(markdown)
    referenced = [filename for filename, _caption in tokens]
    unresolved = sorted(set(referenced) - set(APPROVED_FIGURES))
    unused = [filename for filename in APPROVED_FIGURES if filename not in referenced]
    if unresolved:
        raise ValueError(f"Unapproved figure token(s): {', '.join(unresolved)}")
    if unused:
        raise ValueError(f"Approved figure(s) missing callouts: {', '.join(unused)}")
    if len(referenced) != len(APPROVED_FIGURES):
        raise ValueError(f"Expected {len(APPROVED_FIGURES)} figure callouts, found {len(referenced)}")
    return [resolve_figure_asset(filename) for filename in referenced]


def _slugify(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[^\w\s-]", "", text.lower(), flags=re.UNICODE)
    return re.sub(r"[-\s_]+", "-", text).strip("-") or "section"


def _dedupe_preserve_order(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        normalized = re.sub(r"\s+", " ", value).strip(" ,;")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return tuple(unique)


def _citation_alias_key(alias: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(alias)).strip().casefold()


def _reference_year(text: str) -> re.Match[str] | None:
    return re.search(r"\((?P<year>\d{4})\)", text)


def _reference_surnames(pre_year: str) -> list[str]:
    candidates = re.findall(r"(?:(?:^|,\s+|and\s+))([A-Z][A-Za-z'’.-]+),\s+[A-Z]", pre_year)
    return candidates


def _reference_primary_alias_and_aliases(text: str, year_match: re.Match[str]) -> tuple[str, tuple[str, ...]]:
    pre_year = text[: year_match.start()].strip()
    acronym_match = re.search(r"^(?P<full>.+?)\s+\((?P<acronym>[A-Z][A-Z0-9&.-]{1,12})\)\s*$", pre_year)
    if acronym_match:
        acronym = acronym_match.group("acronym").strip()
        full = acronym_match.group("full").strip(" ,.;")
        return acronym, _dedupe_preserve_order((acronym, full))

    first_comma = pre_year.split(",", 1)[0].strip(" ,.;")
    surnames = _reference_surnames(pre_year)
    aliases: list[str] = []
    if len(surnames) == 1:
        aliases.append(surnames[0])
    elif len(surnames) == 2:
        aliases.extend((f"{surnames[0]} & {surnames[1]}", f"{surnames[0]} and {surnames[1]}", surnames[0]))
    elif len(surnames) > 2:
        aliases.extend((f"{surnames[0]} et al.", surnames[0]))
    else:
        aliases.append(first_comma or pre_year.strip(" ,.;"))

    primary = first_comma if first_comma else (aliases[0] if aliases else pre_year.strip(" ,.;"))
    if first_comma and first_comma not in aliases:
        aliases.append(first_comma)
    return primary, _dedupe_preserve_order(aliases)


def parse_reference_entries(markdown: str, *, reserved_ids: Iterable[str] = ()) -> list[ReferenceEntry]:
    """Parse bibliography paragraphs under the References heading into link targets."""
    lines = markdown.splitlines()
    references_start: int | None = None
    for idx, line in enumerate(lines):
        heading = HEADING_RE.match(line)
        if heading and _slugify(heading.group(2).strip()) == "references":
            references_start = idx + 1
            break
    if references_start is None:
        return []

    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines[references_start:]:
        if HEADING_RE.match(line):
            break
        if not line.strip():
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(line.strip())
    if current:
        paragraphs.append(" ".join(current))

    used_ids = set(reserved_ids)
    id_counts: dict[str, int] = {}
    entries: list[ReferenceEntry] = []
    for paragraph in paragraphs:
        year_match = _reference_year(paragraph)
        if not year_match:
            continue
        primary_alias, aliases = _reference_primary_alias_and_aliases(paragraph, year_match)
        base_id = f"ref-{_slugify(primary_alias)}-{year_match.group('year')}"
        count = id_counts.get(base_id, 0) + 1
        candidate = base_id if count == 1 else f"{base_id}-{count}"
        while candidate in used_ids:
            count += 1
            candidate = f"{base_id}-{count}"
        id_counts[base_id] = count
        used_ids.add(candidate)
        entries.append(
            ReferenceEntry(
                element_id=candidate,
                year=year_match.group("year"),
                aliases=aliases,
                source_text=paragraph,
            )
        )
    return entries


def build_citation_index(entries: Sequence[ReferenceEntry]) -> CitationIndex:
    """Build an unambiguous alias/year lookup and escaped longest-first alias regex."""
    alias_targets: dict[tuple[str, str], list[ReferenceEntry]] = {}
    for entry in entries:
        for alias in entry.aliases:
            alias_targets.setdefault((_citation_alias_key(alias), entry.year), []).append(entry)

    targets: dict[tuple[str, str], ReferenceEntry] = {}
    for key, matches in alias_targets.items():
        element_ids = {entry.element_id for entry in matches}
        if len(element_ids) == 1:
            targets[key] = matches[0]

    aliases = sorted({html.escape(alias) for entry in entries for alias in entry.aliases}, key=len, reverse=True)
    if not aliases:
        return CitationIndex(entries=tuple(entries), targets=targets, alias_pattern=None)
    pattern = re.compile(
        r"(?<![A-Za-z0-9])"
        r"(?P<citation>(?P<alias>" + "|".join(re.escape(alias) for alias in aliases) + r")"
        r"(?P<gap>\s{1,4})"
        r"(?P<year>\(\d{4}\)|\d{4}))"
        r"(?![\d-])"
    )
    return CitationIndex(entries=tuple(entries), targets=targets, alias_pattern=pattern)


def _strip_dashboard_cover(markdown: str) -> str:
    """Remove the static note cover so the dashboard starts at the first real section."""
    lines = markdown.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() == _DASHBOARD_FIRST_SECTION:
            return "\n".join(lines[idx:])
    return markdown


def heading_id_map(markdown: str) -> tuple[dict[str, str], list[Heading]]:
    """Build deterministic heading IDs and fail on ambiguous duplicate text slugs."""
    slug_counts: dict[str, int] = {}
    seen_ids: set[str] = set()
    mapping: dict[str, str] = {}
    headings: list[Heading] = []
    for line in markdown.splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        level = len(match.group(1))
        text = match.group(2).strip()
        slug = _slugify(text)
        count = slug_counts.get(slug, 0) + 1
        slug_counts[slug] = count
        element_id = slug if count == 1 else f"{slug}-{count}"
        if element_id in seen_ids:
            raise ValueError(f"Duplicate heading id generated: {element_id}")
        seen_ids.add(element_id)
        mapping[line] = element_id
        headings.append(Heading(level=level, text=text, element_id=element_id))
    return mapping, headings


def _link_citations(escaped_text: str, citation_index: CitationIndex | None) -> str:
    if citation_index is None or citation_index.alias_pattern is None:
        return escaped_text

    def replace(match: re.Match[str]) -> str:
        year = match.group("year").strip("()")
        target = citation_index.targets.get((_citation_alias_key(match.group("alias")), year))
        if target is None:
            return match.group("citation")
        return (
            f'<a class="citation-ref" href="#{html.escape(target.element_id)}">'
            f'{match.group("citation")}</a>'
        )

    return citation_index.alias_pattern.sub(replace, escaped_text)


def _inline_markup(text: str, citation_index: CitationIndex | None = None) -> str:
    parts: list[str] = []
    for segment in re.split(r"(`[^`]+`)", text):
        if not segment:
            continue
        if segment.startswith("`") and segment.endswith("`"):
            code = html.escape(segment[1:-1])
            code = code.replace("https://", "https&#58;//").replace("http://", "http&#58;//")
            parts.append(f"<code>{code}</code>")
            continue
        escaped = html.escape(segment)
        escaped = escaped.replace("https://", "https&#58;//").replace("http://", "http&#58;//")
        escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"\*([^*]+)\*", r"<em>\1</em>", escaped)
        parts.append(_link_citations(escaped, citation_index))
    return "".join(parts)


def _table_html(lines: Sequence[str], citation_index: CitationIndex | None = None) -> str:
    rows = [[cell.strip() for cell in line.strip().strip("|").split("|")] for line in lines]
    if len(rows) < 2:
        return ""
    header = rows[0]
    body = rows[2:]
    head_html = "".join(f"<th>{_inline_markup(cell, citation_index)}</th>" for cell in header)
    body_html = "\n".join(
        "<tr>" + "".join(f"<td>{_inline_markup(cell, citation_index)}</td>" for cell in row) + "</tr>" for row in body
    )
    return f"<div class=\"table-wrap\"><table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table></div>"


def _figure_html(filename: str, caption: str, asset: FigureAsset) -> str:
    payload = base64.b64encode(asset.path.read_bytes()).decode("ascii")
    src = f"data:{asset.mime_type};base64,{payload}"
    return (
        f'<figure class="doc-figure" id="figure-{html.escape(_slugify(filename))}">'
        f'<button type="button" class="figure-zoom" aria-label="Zoom figure">'
        f'<img src="{src}" alt="{html.escape(caption)}" loading="lazy"></button>'
        f"<figcaption>{html.escape(caption)}</figcaption></figure>"
    )


def _wrap_sections(blocks: list[str]) -> str:
    sections: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for block in blocks:
        match = _SECTION_HEAD_RE.match(block)
        if match:
            current = {"id": match.group(2), "blocks": []}
            sections.append(current)
        if current is None:
            current = {"id": "__preamble__", "blocks": []}
            sections.append(current)
        current_blocks = current["blocks"]
        if not isinstance(current_blocks, list):  # pragma: no cover - defensive type guard.
            raise TypeError("Section blocks must be a list")
        current_blocks.append(block)

    parts: list[str] = []
    for idx, section in enumerate(sections):
        section_id = str(section["id"])
        section_blocks = section["blocks"]
        if not isinstance(section_blocks, list):  # pragma: no cover - defensive type guard.
            raise TypeError("Section blocks must be a list")
        section_class = "doc-section active" if idx == 0 else "doc-section"
        parts.append(
            f'<section class="{section_class}" data-section-id="{html.escape(section_id)}">'
            + "\n".join(str(block) for block in section_blocks)
            + "</section>"
        )
    return "\n".join(parts)


def render_markdown(markdown: str, figure_assets: dict[str, FigureAsset]) -> tuple[str, list[Heading]]:
    """Render the note Markdown to HTML with explicit figure token expansion."""
    _validate_math_tokenization_if_available(markdown)
    id_by_heading_line, headings = heading_id_map(markdown)
    reference_entries = parse_reference_entries(markdown, reserved_ids=(heading.element_id for heading in headings))
    citation_index = build_citation_index(reference_entries)
    reference_by_text = {entry.source_text: entry for entry in reference_entries}
    all_ids = [heading.element_id for heading in headings] + [entry.element_id for entry in reference_entries]
    if len(all_ids) != len(set(all_ids)):
        raise ValueError("Duplicate HTML id generated")
    lines = markdown.splitlines()
    rendered: list[str] = []
    paragraph: list[str] = []
    list_items: list[str] = []
    blockquote: list[str] = []
    in_references = False
    i = 0

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            text = " ".join(paragraph)
            reference_entry = reference_by_text.get(text) if in_references else None
            if reference_entry is not None:
                rendered.append(
                    f'<p id="{html.escape(reference_entry.element_id)}" '
                    f'class="reference-entry" tabindex="-1">{_inline_markup(text)}</p>'
                )
            else:
                rendered.append(f"<p>{_inline_markup(text, citation_index)}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            rendered.append("<ul>" + "".join(f"<li>{item}</li>" for item in list_items) + "</ul>")
            list_items = []

    def flush_blockquote() -> None:
        nonlocal blockquote
        if blockquote:
            rendered.append("<blockquote>" + "".join(f"<p>{line}</p>" for line in blockquote) + "</blockquote>")
            blockquote = []

    while i < len(lines):
        line = lines[i]
        if FENCE_RE.match(line):
            flush_paragraph()
            flush_list()
            flush_blockquote()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not FENCE_RE.match(lines[i]):
                code_lines.append(lines[i])
                i += 1
            rendered.append(f"<pre><code>{html.escape(chr(10).join(code_lines))}</code></pre>")
            i += 1
            continue
        if not line.strip():
            flush_paragraph()
            flush_list()
            flush_blockquote()
            i += 1
            continue
        heading = HEADING_RE.match(line)
        if heading:
            flush_paragraph()
            flush_list()
            flush_blockquote()
            level = len(heading.group(1))
            text = heading.group(2).strip()
            element_id = id_by_heading_line[line]
            if level <= 2:
                in_references = _slugify(text) == "references"
            rendered.append(f'<h{level} id="{element_id}">{_inline_markup(text)}</h{level}>')
            i += 1
            continue
        fig = FIGURE_TOKEN_RE.search(line.strip())
        if fig:
            flush_paragraph()
            flush_list()
            flush_blockquote()
            filename = fig.group(1).strip()
            caption = fig.group(2).strip()
            rendered.append(_figure_html(filename, caption, figure_assets[filename]))
            i += 1
            continue
        if line.lstrip().startswith(">"):
            flush_paragraph()
            flush_list()
            blockquote.append(_inline_markup(line.lstrip()[1:].strip(), citation_index))
            i += 1
            continue
        if line.startswith("|") and i + 1 < len(lines) and lines[i + 1].startswith("|"):
            flush_paragraph()
            flush_list()
            flush_blockquote()
            table_lines = [line]
            i += 1
            while i < len(lines) and lines[i].startswith("|"):
                table_lines.append(lines[i])
                i += 1
            rendered.append(_table_html(table_lines, citation_index))
            continue
        list_match = re.match(r"^\s*[-*]\s+(.+)$", line)
        if list_match:
            flush_paragraph()
            flush_blockquote()
            list_items.append(_inline_markup(list_match.group(1), citation_index))
            i += 1
            continue
        flush_list()
        flush_blockquote()
        paragraph.append(line.strip())
        i += 1

    flush_paragraph()
    flush_list()
    flush_blockquote()
    return _wrap_sections(rendered), headings


def _validate_math_tokenization_if_available(markdown: str) -> None:
    """Use markdown-it-py/dollarmath when installed to catch malformed math blocks."""
    try:
        from markdown_it import MarkdownIt
        from mdit_py_plugins.dollarmath import dollarmath_plugin
    except ModuleNotFoundError:
        return

    parser = MarkdownIt("commonmark").enable("table").use(dollarmath_plugin)
    parser.parse(markdown)


def _font_mime(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".woff2":
        return "font/woff2"
    if suffix == ".woff":
        return "font/woff"
    if suffix == ".ttf":
        return "font/ttf"
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def inline_katex_css(css: str, root: Path) -> str:
    """Rewrite KaTeX font URLs to embedded data URIs."""
    def replace(match: re.Match[str]) -> str:
        rel = match.group(2)
        font_path = (root / rel).resolve()
        try:
            inside_root = font_path.is_relative_to(root.resolve())
        except AttributeError:  # pragma: no cover
            inside_root = os.path.commonpath([str(font_path), str(root.resolve())]) == str(root.resolve())
        if not inside_root or not font_path.exists():
            raise FileNotFoundError(f"KaTeX CSS references missing font: {rel}")
        data = base64.b64encode(font_path.read_bytes()).decode("ascii")
        return f"url(data:{_font_mime(font_path)};base64,{data})"

    return URL_RE.sub(replace, css)


def load_katex_bundle() -> tuple[str, str, str, dict[str, int]]:
    """Load and inline the vendored KaTeX CSS/JS bundle."""
    required = {
        "katex_css": KATEX_ROOT / "katex.min.css",
        "katex_js": KATEX_ROOT / "katex.min.js",
        "auto_render_js": KATEX_ROOT / "auto-render.min.js",
    }
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing vendored KaTeX file(s): " + ", ".join(missing))
    raw_css = required["katex_css"].read_text(encoding="utf-8")
    css = inline_katex_css(raw_css, KATEX_ROOT)
    js = required["katex_js"].read_text(encoding="utf-8")
    js = js.replace("http://www.w3.org/2000/svg", 'http:"+"://www.w3.org/2000/svg')
    js = js.replace("http://www.w3.org/1998/Math/MathML", 'http:"+"://www.w3.org/1998/Math/MathML')
    auto_render = required["auto_render_js"].read_text(encoding="utf-8")
    sizes = {
        "katex_css_raw": len(raw_css.encode("utf-8")),
        "katex_css_inlined": len(css.encode("utf-8")),
        "katex_js": required["katex_js"].stat().st_size,
        "auto_render_js": required["auto_render_js"].stat().st_size,
        "katex_fonts": sum(path.stat().st_size for path in (KATEX_ROOT / "fonts").glob("*")),
    }
    return css, js, auto_render, sizes


def _toc_html(headings: Sequence[Heading]) -> str:
    groups: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for heading in headings:
        if heading.level in {1, 2}:
            current = {"heading": heading, "children": []}
            groups.append(current)
            continue
        if heading.level == 3 and current is not None:
            children = current["children"]
            if not isinstance(children, list):  # pragma: no cover - defensive type guard.
                raise TypeError("TOC children must be a list")
            children.append(heading)

    items: list[str] = []
    for group in groups:
        heading = group["heading"]
        children = group["children"]
        if not isinstance(heading, Heading):  # pragma: no cover - defensive type guard.
            raise TypeError("TOC heading must be a Heading")
        if not isinstance(children, list):  # pragma: no cover - defensive type guard.
            raise TypeError("TOC children must be a list")
        toggle_class = "toc-toggle" if children else "toc-toggle is-empty"
        disabled = "" if children else " disabled"
        sub_html = ""
        if children:
            child_links = "\n".join(
                f'<a class="toc-level-{child.level}" href="#{child.element_id}">{html.escape(child.text)}</a>'
                for child in children
                if isinstance(child, Heading)
            )
            sub_html = f'\n<div class="toc-sub">{child_links}</div>'
        items.append(
            '<div class="toc-group">'
            '<div class="toc-group-head">'
            f'<button class="{toggle_class}" type="button" aria-expanded="false" '
            f'aria-label="Toggle section"{disabled}></button>'
            f'<a class="toc-level-{heading.level}" href="#{heading.element_id}">{html.escape(heading.text)}</a>'
            "</div>"
            f"{sub_html}</div>"
        )
    return "\n".join(items)


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None


def _asset_manifest_hash(assets: Sequence[FigureAsset]) -> str:
    digest = hashlib.sha256()
    for asset in assets:
        digest.update(asset.filename.encode("utf-8"))
        digest.update(hashlib.sha256(asset.path.read_bytes()).hexdigest().encode("ascii"))
    return digest.hexdigest()


def build_html(source: Path = DEFAULT_SOURCE, *, include_build_timestamp: bool = False) -> tuple[str, dict[str, object]]:
    """Build the complete self-contained HTML string and provenance metadata."""
    markdown = source.read_text(encoding="utf-8")
    figure_assets = validate_figure_manifest(markdown)
    asset_by_name = {asset.filename: asset for asset in figure_assets}
    display_markdown = _strip_dashboard_cover(markdown)
    body_html, headings = render_markdown(display_markdown, asset_by_name)
    css, katex_js, auto_render_js, katex_sizes = load_katex_bundle()
    source_hash = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
    manifest_hash = _asset_manifest_hash(figure_assets)
    provenance: dict[str, object] = {
        "generator": GENERATOR_VERSION,
        "source_sha256": source_hash,
        "figure_manifest_sha256": manifest_hash,
        "figure_count": len(figure_assets),
    }
    if include_build_timestamp:
        from datetime import datetime, timezone

        provenance["git_sha"] = _git_sha()
        provenance["built_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    html_doc = HTML_TEMPLATE.format(
        katex_css=css,
        app_css=APP_CSS,
        toc_html=_toc_html(headings),
        body_html=body_html,
        provenance=html.escape(json.dumps(provenance, sort_keys=True)),
        katex_js=katex_js,
        auto_render_js=auto_render_js,
        app_js=APP_JS,
    )
    _assert_self_contained(html_doc)
    if len(html_doc.encode("utf-8")) > MAX_HTML_BYTES:
        raise ValueError(f"HTML output exceeds {MAX_HTML_BYTES} bytes")
    size_info: dict[str, object] = {
        "html_bytes": len(html_doc.encode("utf-8")),
        "figures": {asset.filename: asset.byte_size for asset in figure_assets},
        **katex_sizes,
    }
    return html_doc, size_info


def _assert_self_contained(html_doc: str) -> None:
    if "[FIGURE:" in html_doc or "[FIGURES TO INSERT]" in html_doc:
        raise ValueError("Generated HTML still contains unresolved figure placeholder text")
    if re.search(r"https?://", html_doc):
        raise ValueError("Generated HTML contains external http(s) reference")
    if URL_RE.search(html_doc):
        raise ValueError("Generated HTML contains relative CSS url(...) reference")


def write_html(html_doc: str, output: Path) -> None:
    """Write generated HTML, creating the parent directory if needed."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_doc, encoding="utf-8", newline="\n")


def _print_size_info(size_info: dict[str, object]) -> None:
    print(f"HTML output: {size_info['html_bytes']:,} bytes")
    print(f"KaTeX CSS raw: {size_info['katex_css_raw']:,} bytes")
    print(f"KaTeX CSS inlined: {size_info['katex_css_inlined']:,} bytes")
    print(f"KaTeX JS: {size_info['katex_js']:,} bytes")
    print(f"KaTeX auto-render JS: {size_info['auto_render_js']:,} bytes")
    print(f"KaTeX fonts: {size_info['katex_fonts']:,} bytes")
    print("Figures:")
    for filename, byte_size in dict(size_info["figures"]).items():
        print(f"  {filename}: {byte_size:,} bytes")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Source Markdown note.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Generated HTML output.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print size information without writing.")
    parser.add_argument(
        "--include-build-timestamp",
        action="store_true",
        help="Include a wall-clock timestamp in provenance; off by default for deterministic output.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    html_doc, size_info = build_html(args.source, include_build_timestamp=args.include_build_timestamp)
    _print_size_info(size_info)
    if args.dry_run:
        print("Dry run: no file written.")
        return 0
    write_html(html_doc, args.output)
    print(f"Wrote {args.output}")
    return 0


APP_CSS = """
:root{color-scheme:light dark;--bg:#f7f8f5;--panel:#ffffff;--text:#17211c;--muted:#66736c;--line:#dce3dd;--accent:#0f766e;--accent-2:#bf6b21;--mark:#fff1a8;--shadow:0 18px 48px rgba(24,35,31,.12)}
:root[data-theme="dark"]{--bg:#101412;--panel:#171d1a;--text:#edf3ef;--muted:#a8b4ad;--line:#34413a;--accent:#55c2b5;--accent-2:#f4a261;--mark:#55480d;--shadow:0 18px 48px rgba(0,0,0,.28)}
@media(prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#101412;--panel:#171d1a;--text:#edf3ef;--muted:#a8b4ad;--line:#34413a;--accent:#55c2b5;--accent-2:#f4a261;--mark:#55480d;--shadow:0 18px 48px rgba(0,0,0,.28)}}
*{box-sizing:border-box}html{font-size:var(--irt-doc-font-size,15px)}html,body{margin:0;height:100%;scroll-behavior:smooth}body{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:1rem;line-height:1.62;background:var(--bg);color:var(--text)}
a{color:var(--accent)}.doc-shell{display:grid;grid-template-columns:minmax(220px,290px) minmax(0,1fr);height:100vh;overflow:hidden}.toc-panel{border-right:1px solid var(--line);background:color-mix(in srgb,var(--panel) 92%,var(--bg));padding:18px 14px;overflow:auto}.toc-title{font-weight:700;margin:0 0 12px}.toc-search{width:100%;height:38px;border:1px solid var(--line);border-radius:6px;background:var(--panel);color:var(--text);padding:0 10px;margin-bottom:14px}.toc-links{display:flex;flex-direction:column;gap:2px}.toc-group-head{display:flex;align-items:center;gap:4px}.toc-group-head a{flex:1}.toc-links a{color:var(--muted);text-decoration:none;border-radius:6px;padding:7px 8px}.toc-links a.active,.toc-links a:hover{background:color-mix(in srgb,var(--accent) 14%,transparent);color:var(--text)}.toc-level-1,.toc-level-2{font-weight:600}.toc-level-3{padding-left:18px!important;font-size:.867rem}.toc-toggle{width:20px;height:24px;border:0;background:transparent;color:var(--muted);cursor:pointer;font-size:.8rem;line-height:1;padding:0;display:inline-flex;align-items:center;justify-content:center}.toc-toggle::before{content:"\\25B8";display:block;transition:transform .16s ease}.toc-group.open .toc-toggle::before{transform:rotate(90deg)}.toc-toggle.is-empty{visibility:hidden;cursor:default}.toc-sub{display:none;flex-direction:column;gap:2px;margin-left:24px}.toc-group.open .toc-sub{display:flex}.content-scroll{height:100vh;overflow:auto}.content{width:calc(100% - clamp(24px,4vw,64px));max-width:920px;margin:0 auto 0 clamp(24px,4vw,64px);padding:34px 40px 96px}.doc-section{display:none}.doc-section.active{display:block}.content.searching .doc-section{display:block}.provenance{color:var(--muted);font-size:.8rem;border-top:1px solid var(--line);margin-top:42px;padding-top:14px}h1,h2,h3,h4{line-height:1.22;margin:1.7em 0 .55em}h1{font-size:2.267rem;margin-top:0}h2{font-size:1.667rem;border-top:1px solid var(--line);padding-top:28px}h3{font-size:1.267rem}h4{font-size:1.067rem}p{margin:.8em 0}.citation-ref{font-weight:600;text-decoration-thickness:.08em;text-underline-offset:.16em}.reference-entry{scroll-margin-top:22px;border-radius:6px;padding:3px 5px;margin-left:-5px}.reference-entry.is-focused{background:color-mix(in srgb,var(--accent) 16%,transparent);outline:2px solid color-mix(in srgb,var(--accent) 48%,transparent);outline-offset:2px;transition:background .2s ease,outline-color .2s ease}blockquote{border-left:4px solid var(--accent);margin:18px 0;padding:8px 16px;background:color-mix(in srgb,var(--accent) 9%,transparent);border-radius:0 6px 6px 0}code{background:color-mix(in srgb,var(--line) 55%,transparent);border-radius:4px;padding:.12em .28em}pre{overflow:auto;border:1px solid var(--line);background:var(--panel);border-radius:6px;padding:14px}.table-wrap{overflow:auto;margin:16px 0;border:1px solid var(--line);border-radius:6px;background:var(--panel)}table{width:100%;border-collapse:collapse;min-width:580px}th,td{border-bottom:1px solid var(--line);padding:9px 11px;text-align:left;vertical-align:top}th{background:color-mix(in srgb,var(--line) 42%,transparent)}tr:last-child td{border-bottom:0}.doc-figure{margin:24px 0;padding:14px;border:1px solid var(--line);border-radius:8px;background:var(--panel);box-shadow:var(--shadow)}.figure-zoom{display:block;width:100%;padding:0;border:0;background:transparent;cursor:zoom-in}.doc-figure img{display:block;width:100%;height:auto;max-height:620px;object-fit:contain}.doc-figure figcaption{color:var(--muted);font-size:.867rem;margin-top:10px}.back-top{position:fixed;right:18px;bottom:18px;border:1px solid var(--line);background:var(--panel);color:var(--text);border-radius:999px;width:42px;height:42px;box-shadow:var(--shadow);cursor:pointer}.lightbox{position:fixed;inset:0;background:rgba(0,0,0,.78);display:none;align-items:center;justify-content:center;padding:24px;z-index:10}.lightbox.open{display:flex}.lightbox img{max-width:96vw;max-height:90vh;background:#fff;border-radius:8px}.search-hit{background:var(--mark);border-radius:3px}@media(max-width:760px){.doc-shell{display:block;overflow:auto}.toc-panel{position:sticky;top:0;z-index:4;border-right:0;border-bottom:1px solid var(--line);max-height:42vh}.content-scroll{height:auto;overflow:visible}.content{width:100%;margin:0;padding:22px 18px 88px}h1{font-size:1.8rem}h2{font-size:1.467rem}.toc-links{display:flex;flex-direction:column}.toc-sub{margin-left:20px}}
"""

APP_JS = r"""
(function() {
  document.addEventListener("DOMContentLoaded", function() {
    const scroller = document.querySelector(".content-scroll");
    const content = document.querySelector(".content");
    const links = Array.from(document.querySelectorAll('.toc-links a[href^="#"]'));
    let activeSectionId = null;
    let referenceFocusTimer = null;

    function renderMath(root) {
      if (root && window.renderMathInElement) {
        renderMathInElement(root, window.IRT_DOC_MATH_OPTIONS);
      }
    }

    function getAnchorTarget(link) {
      const href = link ? link.getAttribute("href") : "";
      if (!href || href.charAt(0) !== "#" || href.length < 2) {
        return null;
      }
      return document.getElementById(href.slice(1));
    }

    function collectHeadings() {
      return links.map(getAnchorTarget).filter(Boolean);
    }

    function getSections() {
      return content ? Array.from(content.querySelectorAll(".doc-section")) : [];
    }

    function getActiveSection() {
      const sections = getSections();
      return sections.find(function(section) {
        return section.classList.contains("active");
      }) || sections[0] || null;
    }

    function getSectionForTarget(target) {
      return target && target.closest ? target.closest(".doc-section") : null;
    }

    function getTopLinkForSection(sectionId) {
      return links.find(function(link) {
        return link.classList.contains("toc-level-1") || link.classList.contains("toc-level-2")
          ? link.getAttribute("href") === "#" + sectionId
          : false;
      }) || null;
    }

    function setOpenGroup(group) {
      document.querySelectorAll(".toc-group").forEach(function(candidate) {
        const isOpen = candidate === group;
        candidate.classList.toggle("open", isOpen);
        const toggle = candidate.querySelector(".toc-toggle");
        if (toggle && !toggle.classList.contains("is-empty")) {
          toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        }
      });
    }

    function setActiveLink(targetId, explicitLink) {
      const activeSection = getActiveSection();
      const sectionId = activeSection ? activeSection.dataset.sectionId : activeSectionId;
      links.forEach(function(link) {
        const href = link.getAttribute("href");
        const isTopLink = sectionId && href === "#" + sectionId;
        const isCurrentLink = targetId && href === "#" + targetId;
        const isActive = explicitLink
          ? link === explicitLink || isTopLink || isCurrentLink
          : Boolean(isTopLink || isCurrentLink);
        link.classList.toggle("active", Boolean(isActive));
      });
    }

    function scrollTargetIntoScroller(target) {
      if (!scroller || !target) {
        return;
      }
      const targetTop = target.getBoundingClientRect().top - scroller.getBoundingClientRect().top + scroller.scrollTop;
      scroller.scrollTo({ top: Math.max(0, targetTop), behavior: "smooth" });
    }

    function clearReferenceFocus() {
      document.querySelectorAll(".reference-entry.is-focused").forEach(function(entry) {
        entry.classList.remove("is-focused");
      });
      if (referenceFocusTimer) {
        window.clearTimeout(referenceFocusTimer);
        referenceFocusTimer = null;
      }
    }

    function focusReferenceTarget(target) {
      if (!target || !target.classList || !target.classList.contains("reference-entry")) {
        return;
      }
      if (typeof target.focus === "function") {
        target.focus({preventScroll:true});
      }
      target.classList.add("is-focused");
      referenceFocusTimer = window.setTimeout(function() {
        target.classList.remove("is-focused");
        referenceFocusTimer = null;
      }, 4200);
    }

    function showSection(id, options) {
      const opts = options || {};
      const sections = getSections();
      if (!sections.length) {
        return;
      }
      clearReferenceFocus();
      const requested = sections.find(function(section) {
        return section.dataset.sectionId === id;
      }) || sections[0];
      activeSectionId = requested.dataset.sectionId;
      sections.forEach(function(section) {
        section.classList.toggle("active", section === requested);
      });

      const topLink = getTopLinkForSection(activeSectionId);
      setOpenGroup(topLink ? topLink.closest(".toc-group") : null);
      const scrollTarget = opts.scrollToId ? document.getElementById(opts.scrollToId) : null;
      const activeTargetId = scrollTarget ? scrollTarget.id : activeSectionId;
      setActiveLink(activeTargetId, opts.explicitLink || null);

      if (!scroller || opts.resetScroll === false) {
        return;
      }
      if (scrollTarget) {
        scrollTargetIntoScroller(scrollTarget);
        focusReferenceTarget(scrollTarget);
      } else {
        scroller.scrollTo({ top: 0, behavior: opts.smooth === false ? "auto" : "smooth" });
      }
    }

    function onScroll() {
      if (!scroller) {
        return;
      }
      const headings = collectHeadings();
      const activeSection = getActiveSection();
      if (activeSection) {
        activeSectionId = activeSection.dataset.sectionId;
      }
      const visibleHeadings = activeSection
        ? headings.filter(function(heading) {
            return activeSection.contains(heading);
          })
        : headings;
      let current = visibleHeadings[0] || null;
      const scrollerTop = scroller.getBoundingClientRect().top;
      for (const heading of visibleHeadings) {
        if (heading.getBoundingClientRect().top - scrollerTop < 150) {
          current = heading;
        }
      }
      setActiveLink(current ? current.id : null, null);
    }

    document.addEventListener("click", function(event) {
      const link = event.target && event.target.closest ? event.target.closest('a[href^="#"]') : null;
      if (!link) {
        return;
      }
      event.preventDefault();
      const target = getAnchorTarget(link);
      if (!target) {
        return;
      }
      const section = getSectionForTarget(target);
      if (!section) {
        scrollTargetIntoScroller(target);
        setActiveLink(target.id, link);
        return;
      }
      const scrollToId = link.classList.contains("toc-level-3") || !link.closest(".toc-links")
        ? target.id
        : null;
      showSection(section.dataset.sectionId, {
        scrollToId: scrollToId,
        explicitLink: link,
      });
    });

    document.addEventListener("click", function(event) {
      const toggle = event.target && event.target.closest ? event.target.closest(".toc-toggle:not(.is-empty)") : null;
      if (!toggle) {
        return;
      }
      const group = toggle.closest(".toc-group");
      const isOpen = group ? group.classList.contains("open") : false;
      setOpenGroup(isOpen ? null : group);
    });

    if (scroller) {
      scroller.addEventListener("scroll", onScroll, { passive: true });
      const initialSection = getActiveSection();
      showSection(initialSection ? initialSection.dataset.sectionId : null, { resetScroll: false, smooth: false });
      onScroll();
    }

    const backTop = document.querySelector(".back-top");
    if (backTop && scroller) {
      backTop.addEventListener("click", function() {
        scroller.scrollTo({ top: 0, behavior: "smooth" });
      });
    }

    const lightbox = document.querySelector(".lightbox");
    const lightboxImage = lightbox ? lightbox.querySelector("img") : null;
    if (content && lightbox && lightboxImage) {
      content.addEventListener("click", function(event) {
        const image = event.target && event.target.closest ? event.target.closest(".figure-zoom img") : null;
        if (!image) {
          return;
        }
        lightboxImage.src = image.src;
        lightboxImage.alt = image.alt;
        lightbox.classList.add("open");
      });
      lightbox.addEventListener("click", function() {
        lightbox.classList.remove("open");
      });
    }

    const input = document.querySelector(".toc-search");
    if (input && content) {
      const original = content.innerHTML;
      input.addEventListener("input", function() {
        const query = input.value.trim();
        const sectionBeforeSearch = activeSectionId;
        content.innerHTML = original;
        if (sectionBeforeSearch) {
          activeSectionId = sectionBeforeSearch;
        }
        if (!query) {
          content.classList.remove("searching");
          showSection(activeSectionId, { resetScroll: false, smooth: false });
          renderMath(content);
          onScroll();
          return;
        }

        content.classList.add("searching");
        showSection(activeSectionId, { resetScroll: false, smooth: false });
        const needle = query.toLowerCase();
        const walker = document.createTreeWalker(content, NodeFilter.SHOW_TEXT, {
          acceptNode: function(node) {
            return node.nodeValue.toLowerCase().includes(needle)
              ? NodeFilter.FILTER_ACCEPT
              : NodeFilter.FILTER_SKIP;
          },
        });
        const nodes = [];
        while (walker.nextNode()) {
          nodes.push(walker.currentNode);
        }
        const escapedQuery = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const regex = new RegExp(escapedQuery, "ig");
        nodes.forEach(function(node) {
          const span = document.createElement("span");
          span.innerHTML = node.nodeValue.replace(regex, function(match) {
            return '<mark class="search-hit">' + match + "</mark>";
          });
          if (node.parentNode) {
            node.parentNode.replaceChild(span, node);
          }
        });
        const first = content.querySelector(".search-hit");
        renderMath(content);
        if (first) {
          scrollTargetIntoScroller(first);
        }
      });
    }
  });
})();
"""

HTML_TEMPLATE = """<!doctype html>
<html lang="en" data-theme="light">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>India Resilience Tool - Technical Guidance Note</title>
<style id="katex-css">{katex_css}</style>
<style>{app_css}</style>
</head>
<body>
<div class="doc-shell" data-irt-doc-root>
<aside class="toc-panel" aria-label="Table of contents">
<p class="toc-title">Read the Docs</p>
<input class="toc-search" type="search" placeholder="Search note" aria-label="Search note">
<nav class="toc-links">{toc_html}</nav>
</aside>
<main class="content-scroll">
<article class="content">{body_html}<p class="provenance">Build provenance: {provenance}</p></article>
</main>
</div>
<button class="back-top" type="button" aria-label="Back to top">↑</button>
<div class="lightbox" role="dialog" aria-modal="true"><img alt=""></div>
<script id="katex-js">{katex_js}</script>
<script id="katex-auto-render-js">{auto_render_js}</script>
<script>
window.IRT_DOC_MATH_OPTIONS={{delimiters:[{{left:"$$",right:"$$",display:true}},{{left:"$",right:"$",display:false}}],throwOnError:false}};
document.addEventListener("DOMContentLoaded",function(){{renderMathInElement(document.body,window.IRT_DOC_MATH_OPTIONS);}});
</script>
<script>{app_js}</script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())
