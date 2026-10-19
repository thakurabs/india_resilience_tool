"""Render one repo Markdown document as a standalone, styled HTML page (CHG-0376).

Read-only. Written so a long governance document can be published for reading
without maintaining a second hand-authored copy: the Markdown file stays the
source of truth and this renders it.

Deliberately supports only the subset of Markdown the repo's docs use — ATX
headings, fenced code, pipe tables, blockquotes, ordered and bullet lists,
horizontal rules, and inline emphasis/code/links. Anything else passes through
as a paragraph, which is visible rather than silently wrong.

Usage
-----
    python -m tools.diagnostics.render_doc_page docs/composite_scale_decisions.md \
        --out docs/composite_scale_decisions.html
"""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path
from typing import Optional, Sequence

_INLINE_CODE = re.compile(r"`([^`]+)`")
_BOLD = re.compile(r"\*\*([^*]+)\*\*")
_ITALIC = re.compile(r"(?<![\*\w])\*([^*\n]+)\*(?!\*)")
_LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def inline(text: str) -> str:
    """Escape, then re-introduce the inline constructs.

    Code spans are lifted out to placeholders before emphasis and links run, so
    `a**b**c` stays literal inside the span instead of picking up markup — the
    repo's docs are full of identifiers with underscores and asterisks.
    """
    out = html.escape(text)

    spans: list[str] = []

    def _stash(match: re.Match[str]) -> str:
        spans.append(match.group(1))
        return f"\x00{len(spans) - 1}\x00"

    out = _INLINE_CODE.sub(_stash, out)
    out = _BOLD.sub(lambda m: f"<strong>{m.group(1)}</strong>", out)
    out = _ITALIC.sub(lambda m: f"<em>{m.group(1)}</em>", out)
    out = _LINK.sub(lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', out)
    return re.sub(r"\x00(\d+)\x00", lambda m: f"<code>{spans[int(m.group(1))]}</code>", out)


def _table(rows: list[str]) -> str:
    cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
    head, body = cells[0], cells[2:]  # cells[1] is the alignment rule
    aligns = []
    for spec in cells[1]:
        aligns.append("right" if spec.endswith(":") and not spec.startswith(":")
                      else "center" if spec.startswith(":") and spec.endswith(":")
                      else "left")
    out = ['<div class="tablewrap"><table><thead><tr>']
    out += [f'<th style="text-align:{a}">{inline(c)}</th>' for c, a in zip(head, aligns)]
    out.append("</tr></thead><tbody>")
    for row in body:
        out.append("<tr>")
        out += [f'<td style="text-align:{a}">{inline(c)}</td>'
                for c, a in zip(row, aligns + ["left"] * len(row))]
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


def convert(markdown: str) -> tuple[str, list[tuple[int, str, str]]]:
    """Return (html_body, table_of_contents) for a Markdown document."""
    lines = markdown.splitlines()
    out: list[str] = []
    toc: list[tuple[int, str, str]] = []
    i = 0
    list_stack: list[str] = []

    def close_lists() -> None:
        while list_stack:
            out.append(f"</{list_stack.pop()}>")

    while i < len(lines):
        line = lines[i]

        if line.startswith("```"):
            close_lists()
            i += 1
            body: list[str] = []
            while i < len(lines) and not lines[i].startswith("```"):
                body.append(html.escape(lines[i]))
                i += 1
            out.append("<pre><code>" + "\n".join(body) + "</code></pre>")
            i += 1
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading:
            close_lists()
            level = len(heading.group(1))
            text = heading.group(2).strip()
            anchor = _slug(text)
            if level <= 3:
                toc.append((level, anchor, text))
            out.append(f'<h{level} id="{anchor}">{inline(text)}</h{level}>')
            i += 1
            continue

        if line.strip() in {"---", "***", "___"}:
            close_lists()
            out.append("<hr>")
            i += 1
            continue

        if line.lstrip().startswith("|") and i + 1 < len(lines) and re.match(
            r"^\s*\|[\s:|-]+\|\s*$", lines[i + 1]
        ):
            close_lists()
            rows = []
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                rows.append(lines[i])
                i += 1
            out.append(_table(rows))
            continue

        if line.startswith(">"):
            close_lists()
            quote = []
            while i < len(lines) and lines[i].startswith(">"):
                quote.append(lines[i].lstrip("> ").rstrip())
                i += 1
            out.append("<blockquote>" + inline(" ".join(q for q in quote if q)) + "</blockquote>")
            continue

        bullet = re.match(r"^(\s*)([-*])\s+(.*)$", line)
        ordered = re.match(r"^(\s*)(\d+)\.\s+(.*)$", line)
        if bullet or ordered:
            match = bullet or ordered
            tag = "ul" if bullet else "ol"
            if not list_stack:
                list_stack.append(tag)
                out.append(f"<{tag}>")
            elif list_stack[-1] != tag:
                out.append(f"</{list_stack.pop()}>")
                list_stack.append(tag)
                out.append(f"<{tag}>")
            out.append(f"<li>{inline(match.group(3))}</li>")
            i += 1
            continue

        if not line.strip():
            close_lists()
            i += 1
            continue

        close_lists()
        para = [line]
        i += 1
        while i < len(lines) and lines[i].strip() and not re.match(
            r"^(#{1,6}\s|```|>|\s*[-*]\s|\s*\d+\.\s|\|)", lines[i]
        ) and lines[i].strip() not in {"---", "***", "___"}:
            para.append(lines[i])
            i += 1
        out.append("<p>" + inline(" ".join(s.strip() for s in para)) + "</p>")

    close_lists()
    return "\n".join(out), toc


PAGE = """<title>{title}</title>
<style>
  :root {{
    --paper:#fbfcfd; --panel:#eef2f6; --ink:#101922; --muted:#5a6b7b;
    --rule:#ccd6e0; --accent:#12506e; --quote:#12506e;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
      --rule:#26303b; --accent:#6fb2d6; --quote:#6fb2d6;
    }}
  }}
  :root[data-theme="dark"] {{
    --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
    --rule:#26303b; --accent:#6fb2d6; --quote:#6fb2d6;
  }}
  html {{ --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
          --mono: ui-monospace, "Cascadia Mono", "SF Mono", Consolas, "Liberation Mono", monospace;
          scroll-behavior:smooth; }}
  @media (prefers-reduced-motion:reduce) {{ html {{ scroll-behavior:auto; }} }}
  body {{ background:var(--paper); color:var(--ink); font:16px/1.65 var(--sans); margin:0; }}
  .shell {{ max-width:1180px; margin:0 auto; padding:30px clamp(18px,4vw,40px) 80px;
    display:grid; grid-template-columns:minmax(0,1fr); gap:34px; }}
  @media (min-width:1000px) {{ .shell {{ grid-template-columns:236px minmax(0,1fr); }} }}

  nav {{ font-size:13px; align-self:start; }}
  @media (min-width:1000px) {{ nav {{ position:sticky; top:24px; max-height:88vh; overflow:auto; }} }}
  nav .navtitle {{ font:600 10.5px/1 var(--mono); letter-spacing:.13em; text-transform:uppercase;
    color:var(--muted); margin-bottom:12px; }}
  nav a {{ display:block; color:var(--muted); text-decoration:none; padding:3px 0 3px 10px;
    border-left:2px solid var(--rule); }}
  nav a:hover {{ color:var(--ink); border-left-color:var(--accent); }}
  nav a.lvl3 {{ padding-left:22px; font-size:12.5px; }}
  nav a:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}

  main {{ min-width:0; }}
  h1 {{ font-size:30px; font-weight:640; letter-spacing:-.025em; margin:0 0 22px;
    text-wrap:balance; line-height:1.2; }}
  h2 {{ font-size:22px; font-weight:620; letter-spacing:-.015em; margin:46px 0 14px;
    padding-top:20px; border-top:1px solid var(--rule); text-wrap:balance; }}
  h3 {{ font-size:17px; font-weight:620; margin:32px 0 10px; }}
  h4 {{ font-size:15px; font-weight:620; margin:24px 0 8px; color:var(--muted); }}
  p {{ margin:0 0 14px; max-width:74ch; }}
  li {{ max-width:72ch; margin-bottom:5px; }}
  ul, ol {{ margin:0 0 14px; padding-left:22px; }}
  code {{ font:.875em var(--mono); background:var(--panel); padding:1px 5px; border-radius:3px; }}
  pre {{ background:var(--panel); border:1px solid var(--rule); border-radius:6px;
    padding:14px 16px; overflow-x:auto; margin:0 0 18px; }}
  pre code {{ background:none; padding:0; font-size:12.5px; line-height:1.6; }}
  blockquote {{ margin:0 0 18px; padding:2px 0 2px 18px; border-left:3px solid var(--quote);
    color:var(--ink); max-width:74ch; }}
  hr {{ border:0; border-top:1px solid var(--rule); margin:40px 0; }}
  a {{ color:var(--accent); }}
  .tablewrap {{ overflow-x:auto; margin:0 0 20px; }}
  table {{ border-collapse:collapse; font-size:14px; min-width:100%; }}
  th {{ font:600 10.5px/1.4 var(--mono); letter-spacing:.09em; text-transform:uppercase;
    color:var(--muted); padding:8px 14px 8px 0; border-bottom:1px solid var(--rule);
    white-space:nowrap; }}
  td {{ padding:7px 14px 7px 0; border-bottom:1px solid var(--rule); vertical-align:top; }}
  td[style*="right"] {{ font-variant-numeric:tabular-nums; font-family:var(--mono);
    font-size:13px; }}
  tbody tr:last-child td {{ border-bottom:0; }}
</style>

<div class="shell">
  <nav aria-label="Contents">
    <div class="navtitle">Contents</div>
    {toc}
  </nav>
  <main>{body}</main>
</div>
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source", type=Path, help="Markdown file to render")
    parser.add_argument("--out", type=Path, default=None,
                        help="HTML file to write (default: source with .html)")
    parser.add_argument("--title", default=None,
                        help="page title (default: the document's first H1)")
    args = parser.parse_args(argv)

    text = args.source.read_text(encoding="utf-8")
    body, toc_entries = convert(text)

    first_h1 = next((t for lvl, _, t in toc_entries if lvl == 1), args.source.stem)
    links = "".join(
        f'<a class="lvl{lvl}" href="#{anchor}">{html.escape(text)}</a>'
        for lvl, anchor, text in toc_entries if lvl in (2, 3)
    )

    out = args.out or args.source.with_suffix(".html")
    out.write_text(
        PAGE.format(title=html.escape(args.title or first_h1), toc=links, body=body),
        encoding="utf-8",
    )
    print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB, "
          f"{len(toc_entries)} headings)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
