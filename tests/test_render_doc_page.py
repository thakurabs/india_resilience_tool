"""Guards for the Markdown renderer (CHG-0376).

The renderer supports a deliberate subset of Markdown. Its dangerous failure is
silence: an unsupported construct that comes out as a paragraph of raw syntax
rather than as an error. These tests pin the constructs the repo's docs use.
"""

from __future__ import annotations

from tools.diagnostics.render_doc_page import convert, inline


def test_inline_code_wins_over_emphasis_inside_it():
    """`a**b**c` is a code span, not code containing bold."""
    assert inline("`a**b**c`") == "<code>a**b**c</code>"


def test_inline_escapes_before_it_marks_up():
    out = inline("a < b and **bold** and [x](y)")

    assert "&lt;" in out
    assert "<strong>bold</strong>" in out
    assert '<a href="y">x</a>' in out


def test_pipe_table_becomes_a_table_with_alignment():
    body, _ = convert("| a | b |\n|---|---:|\n| 1 | 2 |\n")

    assert "<table>" in body
    assert '<th style="text-align:right">b</th>' in body
    assert '<td style="text-align:right">2</td>' in body
    assert "<p>|" not in body


def test_fenced_code_is_not_reinterpreted():
    body, _ = convert("```text\n| not | a | table |\n# not a heading\n```\n")

    assert "<table>" not in body
    assert "<h1" not in body
    assert "| not | a | table |" in body


def test_headings_to_level_three_reach_the_table_of_contents():
    body, toc = convert("# One\n\n## Two\n\n### Three\n\n#### Four\n")

    assert [(lvl, text) for lvl, _, text in toc] == [
        (1, "One"), (2, "Two"), (3, "Three")
    ]
    assert '<h4 id="four">Four</h4>' in body   # rendered, just not indexed


def test_lists_close_before_the_next_block():
    body, _ = convert("- one\n- two\n\nAfter.\n")

    assert body.count("<ul>") == 1 and body.count("</ul>") == 1
    assert body.index("</ul>") < body.index("<p>After.</p>")
