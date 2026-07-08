"""Generate technical-guidance-note figures as editable SVG.

The figures produced here correspond to selected figure specifications in
``docs/figure_gen_instructions.md``. They are intentionally self-contained SVG
schematics/plots so they can be reviewed and edited without a plotting runtime.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from typing import Iterable, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape


OUT_DIR = Path(__file__).resolve().parent


COLORS = {
    "ink": "#1f2933",
    "muted": "#52606d",
    "grid": "#d9e2ec",
    "panel": "#f8fafc",
    "source": "#2f80b7",
    "process": "#2b8a7e",
    "hazard": "#c77700",
    "output": "#b8324f",
    "grey": "#8b95a1",
    "light_blue": "#dff0fb",
    "light_teal": "#d9f2ee",
    "light_amber": "#fff2cc",
    "light_rose": "#fde2e7",
}


@dataclass(frozen=True)
class Svg:
    width: int = 1200
    height: int = 760

    def wrap(self, body: Iterable[str], title: str, desc: str) -> str:
        lines = [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" height="{self.height}" viewBox="0 0 {self.width} {self.height}" role="img" aria-labelledby="title desc">',
            f'<title id="title">{escape(title)}</title>',
            f'<desc id="desc">{escape(desc)}</desc>',
            "<style>",
            "text { font-family: Arial, Helvetica, sans-serif; fill: #1f2933; }",
            ".title { font-size: 26px; font-weight: 700; }",
            ".subtitle { font-size: 15px; fill: #52606d; }",
            ".label { font-size: 15px; font-weight: 700; }",
            ".small { font-size: 12px; fill: #52606d; }",
            ".tiny { font-size: 10.5px; fill: #52606d; }",
            ".note { font-size: 12px; fill: #3e4c59; font-style: italic; }",
            ".axis { stroke: #52606d; stroke-width: 1.2; }",
            ".gridline { stroke: #d9e2ec; stroke-width: 1; }",
            ".arrow { stroke: #52606d; stroke-width: 2.0; fill: none; marker-end: url(#arrow); }",
            "</style>",
            "<defs>",
            '<marker id="arrow" viewBox="0 0 12 12" refX="10" refY="6" markerWidth="12" markerHeight="12" markerUnits="userSpaceOnUse" orient="auto-start-reverse">',
            '<path d="M 1 1 L 11 6 L 1 11 z" fill="#52606d"/>',
            "</marker>",
            "</defs>",
            f'<rect width="{self.width}" height="{self.height}" fill="white"/>',
            *body,
            "</svg>",
        ]
        return "\n".join(lines) + "\n"


FONT_PX = {"title": 26.0, "subtitle": 15.0, "label": 15.0, "small": 12.0, "tiny": 10.5, "note": 12.0}


def approx_px(content: str, font_px: float, bold: bool = False) -> float:
    return len(content) * font_px * (0.60 if bold else 0.53)


def wrap_words(content: str, max_px: float, font_px: float, bold: bool = False) -> list[str]:
    words = content.split()
    if not words:
        return [content]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if approx_px(candidate, font_px, bold) <= max_px:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    for line_value in lines:
        if approx_px(line_value, font_px, bold) > max_px:
            raise AssertionError(f"Cannot fit text in box: {line_value!r}")
    return lines


def _attrs(attrs: dict[str, str | float] | None = None) -> str:
    if not attrs:
        return ""
    return "".join(f' {key}="{escape(str(value))}"' for key, value in attrs.items())


def text(
    x: float,
    y: float,
    content: str,
    cls: str = "small",
    anchor: str = "start",
    attrs: dict[str, str | float] | None = None,
) -> str:
    return f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}" text-anchor="{anchor}"{_attrs(attrs)}>{escape(content)}</text>'


def formula_quantile_mapping(x: float, y: float, anchor: str = "middle") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" class="label" text-anchor="{anchor}">'
        "x&#8242; = F"
        '<tspan baseline-shift="sub" font-size="11px">obs</tspan>'
        '<tspan baseline-shift="super" font-size="11px">-1</tspan>'
        "(F"
        '<tspan baseline-shift="sub" font-size="11px">mod</tspan>'
        "(x))"
        "</text>"
    )


def formula_spi_transform(x: float, y: float, anchor: str = "middle") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" class="label" text-anchor="{anchor}">'
        "SPI = &#934;"
        '<tspan baseline-shift="super" font-size="11px">-1</tspan>'
        "(H(x))"
        "</text>"
    )


def formula_area_weighted_average(x: float, y: float) -> str:
    left_x = x - 170
    frac_x = x - 78
    frac_w = 250
    return (
        "<g>"
        f'<text x="{left_x:.1f}" y="{y + 5:.1f}" class="label" text-anchor="start">'
        "v&#772;"
        '<tspan baseline-shift="sub" font-size="11px">i</tspan>'
        " ="
        "</text>"
        f'<line x1="{frac_x:.1f}" y1="{y - 8:.1f}" x2="{frac_x + frac_w:.1f}" y2="{y - 8:.1f}" stroke="{COLORS["ink"]}" stroke-width="1.3"/>'
        f'<text x="{frac_x + frac_w / 2:.1f}" y="{y - 18:.1f}" class="label" text-anchor="middle">'
        "&#931;"
        '<tspan baseline-shift="sub" font-size="11px">j</tspan>'
        " a"
        '<tspan baseline-shift="sub" font-size="11px">ij</tspan>'
        " v"
        '<tspan baseline-shift="sub" font-size="11px">j</tspan>'
        "</text>"
        f'<text x="{frac_x + frac_w / 2:.1f}" y="{y + 20:.1f}" class="label" text-anchor="middle">'
        "&#931;"
        '<tspan baseline-shift="sub" font-size="11px">j</tspan>'
        " a"
        '<tspan baseline-shift="sub" font-size="11px">ij</tspan>'
        "</text>"
        "</g>"
    )


def multiline(
    x: float,
    y: float,
    lines: Iterable[str],
    cls: str = "small",
    line_h: float = 17,
    anchor: str = "start",
) -> list[str]:
    out: list[str] = []
    for i, line in enumerate(lines):
        out.append(text(x, y + i * line_h, line, cls=cls, anchor=anchor))
    return out


def rect(
    x: float,
    y: float,
    w: float,
    h: float,
    fill: str,
    stroke: str,
    stroke_width: float = 1.8,
    radius: int = 0,
    cls: str | None = None,
) -> str:
    class_attr = f' class="{cls}"' if cls else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{stroke_width}"{class_attr}/>'
    )


def box(
    x: float,
    y: float,
    w: float,
    h: float,
    fill: str,
    stroke: str,
    label: str,
    sublines: Iterable[str] = (),
    radius: int = 6,
) -> list[str]:
    label_lines = wrap_words(label, w - 24, FONT_PX["label"], bold=True)
    subline_values: list[str] = []
    for subline in sublines:
        subline_values.extend(wrap_words(subline, w - 20, FONT_PX["small"]))
    line_h = 17
    total_h = len(label_lines) * line_h + (6 if subline_values else 0) + len(subline_values) * line_h
    if total_h + 22 > h:
        h = total_h + 22
    start_y = y + (h - total_h) / 2 + 12
    out = [rect(x, y, w, h, fill, stroke, radius=radius, cls="flow-box")]
    box_meta = {"data-box": f"{x},{y},{w},{h}"}
    for i, label_line in enumerate(label_lines):
        out.append(text(x + w / 2, start_y + i * line_h, label_line, cls="label", anchor="middle", attrs=box_meta))
    sub_start = start_y + len(label_lines) * line_h + (6 if subline_values else 0)
    for i, subline in enumerate(subline_values):
        out.append(text(x + w / 2, sub_start + i * line_h, subline, cls="small", anchor="middle", attrs=box_meta))
    return out


def arrow(x1: float, y1: float, x2: float, y2: float) -> str:
    return f'<path class="arrow" d="M {x1:.1f} {y1:.1f} L {x2:.1f} {y2:.1f}"/>'


def elbow_arrow(points: list[tuple[float, float]]) -> str:
    if len(points) < 2:
        raise ValueError("elbow_arrow requires at least two points")
    d = " ".join(("M" if i == 0 else "L") + f" {x:.1f} {y:.1f}" for i, (x, y) in enumerate(points))
    return f'<path class="arrow" d="{d}"/>'


def polyline(points: list[tuple[float, float]], color: str, width: float = 2.5, dash: str | None = None) -> str:
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round"{dash_attr}/>'


def line(x1: float, y1: float, x2: float, y2: float, color: str, width: float = 1.3, dash: str | None = None) -> str:
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{width}"{dash_attr}/>'


def _shoelace_area(poly: list[tuple[float, float]]) -> float:
    """Unsigned area of a simple polygon via the shoelace formula."""
    if len(poly) < 3:
        return 0.0
    s = 0.0
    for i in range(len(poly)):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % len(poly)]
        s += x1 * y2 - x2 * y1
    return abs(s) / 2.0


def _poly_centroid(poly: list[tuple[float, float]]) -> tuple[float, float]:
    """Area centroid of a simple polygon; vertex mean fallback if degenerate."""
    a = cx = cy = 0.0
    n = len(poly)
    for i in range(n):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % n]
        cross = x1 * y2 - x2 * y1
        a += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    if abs(a) < 1e-9:
        return sum(p[0] for p in poly) / n, sum(p[1] for p in poly) / n
    return cx / (3 * a), cy / (3 * a)


def _clip_poly_to_rect(
    poly: list[tuple[float, float]], xmin: float, ymin: float, xmax: float, ymax: float
) -> list[tuple[float, float]]:
    """Sutherland-Hodgman clip of a polygon against an axis-aligned rectangle.

    Returns the clipped vertex ring (empty if the polygon misses the rectangle).
    Consecutive duplicate vertices are removed for a clean SVG polygon. SVG y grows
    downward; area is orientation-independent so this does not affect the weights.
    """
    def _clip_edge(pts, keep, cross):
        out: list[tuple[float, float]] = []
        for i in range(len(pts)):
            cur, prev = pts[i], pts[i - 1]
            cur_in, prev_in = keep(cur), keep(prev)
            if cur_in:
                if not prev_in:
                    out.append(cross(prev, cur))
                out.append(cur)
            elif prev_in:
                out.append(cross(prev, cur))
        return out

    def _cross_x(p, q, bound):
        (px, py), (qx, qy) = p, q
        t = (bound - px) / (qx - px)
        return (bound, py + t * (qy - py))

    def _cross_y(p, q, bound):
        (px, py), (qx, qy) = p, q
        t = (bound - py) / (qy - py)
        return (px + t * (qx - px), bound)

    pts = list(poly)
    pts = _clip_edge(pts, lambda p: p[0] >= xmin, lambda a, b: _cross_x(a, b, xmin))
    pts = _clip_edge(pts, lambda p: p[0] <= xmax, lambda a, b: _cross_x(a, b, xmax))
    pts = _clip_edge(pts, lambda p: p[1] >= ymin, lambda a, b: _cross_y(a, b, ymin))
    pts = _clip_edge(pts, lambda p: p[1] <= ymax, lambda a, b: _cross_y(a, b, ymax))
    cleaned: list[tuple[float, float]] = []
    for p in pts:
        if not cleaned or abs(p[0] - cleaned[-1][0]) > 1e-6 or abs(p[1] - cleaned[-1][1]) > 1e-6:
            cleaned.append(p)
    if (
        len(cleaned) > 1
        and abs(cleaned[0][0] - cleaned[-1][0]) < 1e-6
        and abs(cleaned[0][1] - cleaned[-1][1]) < 1e-6
    ):
        cleaned.pop()
    return cleaned


def figure_01() -> str:
    body: list[str] = []
    stages = [
        ("Climate and flood inputs", COLORS["light_blue"], COLORS["source"], ["NASA-NEX GDDP-CMIP6", "CEMS-GloFAS/JRC RP-100"]),
        ("Source preparation", "#edf7ff", COLORS["source"], ["BCSD applied by NASA", "India clip + unit conversion"]),
        ("Grid-first index compute", COLORS["light_teal"], COLORS["process"], ["Annual per-cell indices", "Static flood metrics separately"]),
        ("Spatial + temporal aggregation", "#e8f4f2", COLORS["process"], ["0.25 deg grid -> polygons", "daily -> annual ->", "period -> ensemble"]),
        ("Composite output", COLORS["light_rose"], COLORS["output"], ["0-100 higher = worse", "District and block views"]),
    ]
    x0, y, w, h, gap = 36, 150, 178, 140, 38
    stage_xs = [x0 + i * (w + gap) for i in range(6)]
    stage_defs = stages[:4] + [None] + [stages[4]]
    for i, stage in enumerate(stage_defs):
        if stage is None:
            continue
        label, fill, stroke, subs = stage
        x = stage_xs[i]
        body.extend(box(x, y, w, h, fill, stroke, label, subs))
    for i in range(3):
        body.append(arrow(stage_xs[i] + w + 2, y + h / 2, stage_xs[i + 1] - 2, y + h / 2))
    bx = stage_xs[4]
    body.append(text(bx + w / 2, y - 14, "Bundle construction", "label", "middle"))
    branch_h = 64
    body.extend(box(bx, y, w, branch_h, "#fff8e6", COLORS["hazard"], "Thematic", ["co-normalized metrics"], radius=5))
    body.extend(box(bx, y + h - branch_h, w, branch_h, "#fff8e6", COLORS["hazard"], "Sectoral", ["abs/chg/impact lenses"], radius=5))
    body.append(elbow_arrow([(stage_xs[3] + w + 2, y + h / 2), (bx - 20, y + h / 2), (bx - 20, y + branch_h / 2), (bx - 2, y + branch_h / 2)]))
    body.append(elbow_arrow([(stage_xs[3] + w + 2, y + h / 2), (bx - 20, y + h / 2), (bx - 20, y + h - branch_h / 2), (bx - 2, y + h - branch_h / 2)]))
    out_x = stage_xs[5]
    body.append(arrow(bx + w + 2, y + branch_h / 2, out_x - 2, y + branch_h / 2))
    body.append(arrow(bx + w + 2, y + h - branch_h / 2, out_x - 2, y + h - branch_h / 2))
    body.extend(
        box(
            36,
            330,
            1258,
            64,
            "#f4f6f8",
            COLORS["grey"],
            "Hazard-pressure scope",
            ["Composite scores are hazard-pressure indices; exposure and vulnerability are not inputs."],
            radius=5,
        )
    )
    body.append(text(60, 492, "Section path", "label"))
    strip = [("2", "Data"), ("3", "Downscaling"), ("4", "Grid-first"), ("5", "Metrics"), ("6/7", "Bundles"), ("8", "Output")]
    sx, sy = 165, 470
    for i, (sec, lab) in enumerate(strip):
        x = sx + i * 160
        body.append(f'<circle cx="{x}" cy="{sy}" r="21" fill="#ffffff" stroke="#8b95a1" stroke-width="1.5"/>')
        body.append(text(x, sy + 5, sec, "label", "middle"))
        body.append(text(x, sy + 44, lab, "small", "middle"))
        if i < len(strip) - 1:
            body.append(arrow(x + 27, sy, x + 131, sy))
    return Svg(width=1320).wrap(body, "FIG-01. End-to-end pipeline flow", "IRT pipeline schematic.")


def figure_02() -> str:
    body = [
        '<defs><pattern id="scope-hatch" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="8" stroke="#8b95a1" stroke-width="1.2" opacity="0.45"/></pattern></defs>',
    ]
    body.append(text(600, 128, "Climate risk = f(Hazard, Exposure, Vulnerability)", "label", "middle"))
    cards = [
        (
            70,
            "Hazard",
            "IRT output: climate hazard-pressure",
            ["heat", "drought", "extreme rainfall", "riverine flood"],
            COLORS["light_rose"],
            COLORS["output"],
        ),
        (
            445,
            "Exposure",
            "Out of scope: people, assets, systems exposed",
            ["population", "assets", "crops", "facilities"],
            "#f4f6f8",
            COLORS["grey"],
        ),
        (
            820,
            "Vulnerability",
            "Out of scope: sensitivity and adaptive capacity",
            ["income", "age", "infrastructure condition", "coping capacity"],
            "#f4f6f8",
            COLORS["grey"],
        ),
    ]
    for x, title, scope, examples, fill, stroke in cards:
        body.extend(box(x, 190, 310, 130, fill, stroke, title, [scope], radius=6))
        if title != "Hazard":
            body.append(f'<rect x="{x}" y="190" width="310" height="130" rx="6" fill="url(#scope-hatch)" stroke="none"/>')
            body.append(rect(x + 135, 198, 166, 28, "white", stroke, stroke_width=1.0, radius=4))
            body.append(text(x + 218, 217, "NOT PRODUCED BY IRT", "tiny", "middle"))
        body.append(line(x + 155, 320, x + 155, 342, stroke, 1.0, "3 4"))
        body.append(rect(x, 342, 310, 208, "white", stroke, stroke_width=1.0, radius=5))
        body.append(text(x + 155, 370, "Examples", "label", "middle"))
        for i, example in enumerate(examples):
            py = 406 + i * 38
            body.append(f'<circle cx="{x + 62}" cy="{py - 4}" r="7" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
            body.append(text(x + 86, py, example, "small"))
    body.append(line(380, 255, 445, 255, COLORS["grey"], 2.0, "6 5"))
    body.append(line(755, 255, 820, 255, COLORS["grey"], 2.0, "6 5"))
    return Svg().wrap(body, "FIG-02. Hazard, exposure, and vulnerability scope", "IRT hazard-pressure scope schematic.")


def draw_grid(x: float, y: float, cols: int, rows: int, cell: float, fill: str, stroke: str, heavy: int = 0) -> list[str]:
    out = [f'<rect x="{x}" y="{y}" width="{cols * cell}" height="{rows * cell}" fill="{fill}" stroke="{stroke}" stroke-width="1.4"/>']
    for c in range(1, cols):
        out.append(line(x + c * cell, y, x + c * cell, y + rows * cell, stroke, 0.8 if c != heavy else 2))
    for r in range(1, rows):
        out.append(line(x, y + r * cell, x + cols * cell, y + r * cell, stroke, 0.8 if r != heavy else 2))
    return out


def cdf_points(x: float, y: float, w: float, h: float, shift: float) -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    for i in range(80):
        t = i / 79
        xx = x + t * w
        yy = y + h - h / (1 + 2.71828 ** (-10 * (t - shift)))
        pts.append((xx, yy))
    return pts


def figure_05() -> str:
    body: list[str] = []
    x0, x1 = 210, 1085
    start, end = 1950, 2100

    def xpos(year: int) -> float:
        return x0 + (year - start) / (end - start) * (x1 - x0)

    axis_y = 620
    for year in [1950, 1990, 2020, 2040, 2060, 2080, 2100]:
        xx = xpos(year)
        body.append(line(xx, 132, xx, axis_y, COLORS["grid"], 0.7))
    body.append(line(x0, axis_y, x1, axis_y, COLORS["muted"], 1.4))
    for year in [1950, 1990, 2020, 2040, 2060, 2080, 2100]:
        xx = xpos(year)
        body.append(line(xx, axis_y - 10, xx, axis_y + 10, COLORS["muted"], 1.0))
        body.append(text(xx, axis_y + 30, str(year), "tiny", "middle"))
    for year in [2010, 2014, 2015]:
        xx = xpos(year)
        body.append(line(xx, axis_y - 7, xx, axis_y + 7, COLORS["grey"], 0.9))

    rows = [
        (150, "Raw climate runs"),
        (295, "Baseline"),
        (420, "Future windows"),
        (535, "Static snapshot"),
    ]
    for y, label_value in rows:
        body.append(text(62, y + 30, label_value, "label"))
        body.append(line(x0, y + 45, x1, y + 45, COLORS["grid"], 0.6))

    hist_y = 150
    body.append(rect(xpos(1950), hist_y, xpos(2014) - xpos(1950), 50, "#e7f2fb", COLORS["source"], stroke_width=1.5, radius=5))
    body.append(text((xpos(1950) + xpos(2014)) / 2, hist_y + 31, "Historical raw span: 1950-2014", "small", "middle"))
    ssp_y = 150
    body.append(rect(xpos(2015), ssp_y, xpos(2100) - xpos(2015), 50, "#e4f7f4", COLORS["process"], stroke_width=1.5, radius=5))
    body.append(text((xpos(2015) + xpos(2100)) / 2, ssp_y + 24, "SSP2-4.5 and SSP5-8.5 raw span", "small", "middle"))
    body.append(text((xpos(2015) + xpos(2100)) / 2, ssp_y + 41, "2015-2100", "tiny", "middle"))
    split_x = (xpos(2014) + xpos(2015)) / 2
    body.append(line(split_x, hist_y - 8, split_x, hist_y + 58, COLORS["grey"], 1.0, "4 4"))
    body.append(text(split_x, hist_y + 78, "2014/2015 handoff", "tiny", "middle"))

    base_y = 330
    body.append(rect(xpos(1990), base_y, xpos(2010) - xpos(1990), 36, "#fff8e6", COLORS["hazard"], stroke_width=1.5, radius=4))
    body.append(line(xpos(1990), base_y - 18, xpos(2010), base_y - 18, COLORS["hazard"], 1.4))
    body.append(line(xpos(1990), base_y - 18, xpos(1990), base_y, COLORS["hazard"], 1.4))
    body.append(line(xpos(2010), base_y - 18, xpos(2010), base_y, COLORS["hazard"], 1.4))
    body.append(text((xpos(1990) + xpos(2010)) / 2, base_y - 28, "Baseline/reference", "small", "middle"))
    body.append(text((xpos(1990) + xpos(2010)) / 2, base_y + 23, "1990-2010", "tiny", "middle"))

    windows = [(2020, 2040), (2040, 2060), (2060, 2080)]
    win_y = 435
    for i, (a, b) in enumerate(windows):
        fill = ["#e5f4f2", "#ccebe6", "#b6dfd8"][i]
        body.append(rect(xpos(a), win_y, xpos(b) - xpos(a), 44, fill, COLORS["process"], stroke_width=1.5, radius=4))
        body.append(text((xpos(a) + xpos(b)) / 2, win_y + 27, f"{a}-{b}", "tiny", "middle"))
        if i > 0:
            body.append(line(xpos(a), win_y - 10, xpos(a), win_y + 54, COLORS["process"], 0.9, "3 4"))
    body.append(text((xpos(2020) + xpos(2080)) / 2, win_y + 68, "Inclusive 21-year means; endpoints are shared.", "small", "middle"))

    snap_x = xpos(2025)
    snap_y = 555
    body.append(line(snap_x, snap_y - 38, snap_x, snap_y + 30, COLORS["grey"], 1.8, "5 4"))
    body.append(f'<circle cx="{snap_x}" cy="{snap_y - 38}" r="9" fill="{COLORS["grey"]}" stroke="{COLORS["ink"]}" stroke-width="1.0"/>')
    body.append(text(snap_x + 24, snap_y - 34, "Current / Snapshot inputs", "small"))
    body.append(text(snap_x + 24, snap_y - 14, "Riverine flood and static exposure layers", "tiny"))
    body.append(text(snap_x + 24, snap_y + 6, "Not modeled as an SSP period", "tiny"))
    return Svg().wrap(body, "FIG-05. Temporal coverage and analysis windows", "Timeline of raw spans and analysis windows.")


def figure_06() -> str:
    body: list[str] = []
    body.extend(draw_grid(78, 168, 3, 3, 64, "#e7f2fb", COLORS["source"], heavy=1))
    body.append(text(174, 138, "Coarse GCM grid", "label", "middle"))
    body.extend(multiline(174, 386, ["Raw monthly value x", "Coarse-resolution dynamics"], "small", anchor="middle"))
    body.append(arrow(280, 264, 372, 264))
    px, py, pw, ph = 380, 150, 310, 230
    body.append(f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" rx="6" fill="#fffdf7" stroke="{COLORS["hazard"]}" stroke-width="1.6"/>')
    body.append(text(px + pw / 2, py - 14, "Monthly CDF quantile mapping", "label", "middle"))
    body.append(line(px + 42, py + 185, px + 270, py + 185, COLORS["muted"]))
    body.append(line(px + 42, py + 185, px + 42, py + 30, COLORS["muted"]))
    body.append(polyline(cdf_points(px + 42, py + 30, 228, 155, 0.58), COLORS["source"], 2.5))
    body.append(polyline(cdf_points(px + 42, py + 30, 228, 155, 0.47), COLORS["output"], 2.5))
    body.append(text(px + 65, py + 50, "F_mod", "small"))
    body.append(text(px + 218, py + 70, "F_obs", "small"))
    body.append(text(px + 156, py + 212, "monthly value", "tiny", "middle"))
    body.append(text(px + 22, py + 112, "CDF", "tiny", "middle"))
    body.append(formula_quantile_mapping(px + pw / 2, py + 252, "middle"))
    body.append(arrow(704, 264, 794, 264))
    body.extend(draw_grid(812, 126, 8, 8, 28, "#e4f7f4", COLORS["process"], heavy=4))
    body.append(text(924, 98, "0.25 deg target grid", "label", "middle"))
    body.extend(multiline(924, 386, ["Bilinear interpolation /", "anomaly disaggregation"], "small", anchor="middle"))
    return Svg().wrap(body, "FIG-06. BCSD schematic", "BCSD two-step schematic.")


def figure_08() -> str:
    body: list[str] = []
    # District panel.
    body.append(f'<rect x="70" y="125" width="485" height="455" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    body.append(text(312, 108, "District-scale view", "label", "middle"))
    for gx in range(90, 535, 72):
        body.append(line(gx, 135, gx, 560, COLORS["grid"], 1))
    for gy in range(150, 560, 72):
        body.append(line(80, gy, 545, gy, COLORS["grid"], 1))
    district = "135,182 215,145 335,160 453,226 503,335 452,482 310,535 182,492 103,378 94,262"
    body.append(f'<polygon points="{district}" fill="#dff0fb" fill-opacity="0.58" stroke="{COLORS["source"]}" stroke-width="3"/>')
    block_polys = [
        ("150,210 220,172 285,210 250,295 170,318 112,270", "#fef3c7"),
        ("220,172 333,176 425,235 354,307 285,210", "#d9f2ee"),
        ("354,307 425,235 488,335 430,410 352,372", "#fde2e7"),
        ("170,318 250,295 352,372 302,470 196,462 118,375", "#e7f0ff"),
        ("302,470 352,372 430,410 438,462 328,515 196,462", "#ecfdf5"),
    ]
    for pts, fill in block_polys:
        body.append(f'<polygon points="{pts}" fill="{fill}" fill-opacity="0.65" stroke="#52606d" stroke-width="1.4"/>')
    body.append(f'<polygon points="{district}" fill="none" stroke="{COLORS["source"]}" stroke-width="3"/>')
    body.append(text(168, 607, "District intersects many cells (typical: 4-20)", "small"))
    # Block zoom.
    body.append(f'<rect x="650" y="125" width="485" height="455" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    body.append(text(892, 108, "Block-scale zoom", "label", "middle"))
    for gx in range(670, 1135, 118):
        body.append(line(gx, 135, gx, 560, COLORS["grid"], 1.6))
    for gy in range(150, 560, 118):
        body.append(line(660, gy, 1125, gy, COLORS["grid"], 1.6))
    # Section 8.2 case: several sub-cell blocks fall inside one 0.25 deg cell and
    # take its single value; a block filling the next cell can carry a different value.
    body.append(
        '<rect x="788" y="268" width="118" height="118" fill="#fff7ed" '
        f'stroke="{COLORS["output"]}" stroke-width="2.5"/>'
    )
    shared_value = "62"
    same_fill = "#f6b26b"
    shared_blocks = [
        ("788,268 840,268 855,325 830,386 788,386", (812, 340)),
        ("840,268 906,268 906,318 870,340 855,325", (874, 300)),
        ("855,325 870,340 906,318 906,386 830,386", (872, 368)),
    ]
    for pts, (lx, ly) in shared_blocks:
        body.append(
            f'<polygon points="{pts}" fill="{same_fill}" fill-opacity="0.85" '
            'stroke="#52606d" stroke-width="1.4"/>'
        )
        body.append(text(lx, ly, shared_value, "label", "middle"))
    body.append(
        '<polygon points="906,268 1024,268 1024,386 906,386" fill="#9fc5e8" '
        'fill-opacity="0.85" stroke="#52606d" stroke-width="1.4"/>'
    )
    body.append(text(965, 332, "48", "label", "middle"))
    body.append(text(660, 600, "Number in each block = its composite score (0-100).", "tiny"))
    body.append(text(660, 620, "Three blocks inside one cell all take that cell's value (62); the", "small"))
    body.append(text(660, 638, "adjacent cell can differ (48). Block scores cannot resolve", "small"))
    body.append(text(660, 654, "contrast finer than the ~25 km cell.", "small"))
    return Svg().wrap(body, "FIG-08. District/block resolution zoom", "Illustrative resolution contrast.")


def figure_09() -> str:
    body: list[str] = []
    days = [1, 2, 3, 4, 5]
    city = [36, 37, 38, 37, 36]
    valley = [28, 29, 30, 29, 28]
    avg = [(a + b) / 2 for a, b in zip(city, valley)]
    x0, y0, w, h = 90, 170, 395, 265
    for panel_x, title, series in [(x0, "Admin-first", [("District average", avg, COLORS["grey"])]), (690, "Grid-first", [("City cell", city, COLORS["output"]), ("Valley cell", valley, COLORS["source"])])]:
        body.append(f'<rect x="{panel_x}" y="{y0}" width="{w}" height="{h}" rx="6" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
        body.append(text(panel_x + w / 2, y0 - 18, title, "label", "middle"))
        body.append(line(panel_x + 58, y0 + h - 42, panel_x + w - 28, y0 + h - 42, COLORS["muted"]))
        body.append(line(panel_x + 58, y0 + 28, panel_x + 58, y0 + h - 42, COLORS["muted"]))
        for temp in [28, 30, 32, 34, 36, 38]:
            yy = y0 + h - 42 - (temp - 27) / 12 * (h - 76)
            body.append(line(panel_x + 58, yy, panel_x + w - 28, yy, COLORS["grid"], 0.9))
            body.append(text(panel_x + 48, yy + 4, str(temp), "tiny", "end"))
        yy35 = y0 + h - 42 - (35 - 27) / 12 * (h - 76)
        body.append(line(panel_x + 58, yy35, panel_x + w - 28, yy35, COLORS["hazard"], 2, "6 5"))
        body.append(text(panel_x + w - 125, yy35 - 8, "35 deg C threshold", "tiny"))
        for name, vals, color in series:
            pts = []
            for i, v in enumerate(vals):
                xx = panel_x + 78 + i * 66
                yy = y0 + h - 42 - (v - 27) / 12 * (h - 76)
                pts.append((xx, yy))
                body.append(f'<circle cx="{xx}" cy="{yy}" r="4.5" fill="{color}"/>')
            body.append(polyline(pts, color, 2.8))
            body.append(text(panel_x + 80, y0 + h + 18 + 18 * len(series), name, "small"))
        for i, day in enumerate(days):
            body.append(text(panel_x + 78 + i * 66, y0 + h - 20, f"D{day}", "tiny", "middle"))
    body.extend(box(115, 500, 345, 86, "#fff8e6", COLORS["hazard"], "Average first", ["32-34 deg C all days", "Hot days >35 deg C: 0"]))
    body.extend(box(720, 500, 345, 86, COLORS["light_rose"], COLORS["output"], "Threshold first", ["City: 5 hot days; valley: 0", "Area-weighted mean: 2.5 hot days"]))
    body.append(arrow(485, 545, 690, 545))
    return Svg().wrap(body, "FIG-09. Admin-first vs grid-first", "Worked example of nonlinear threshold bias.")


def figure_10() -> str:
    body: list[str] = []
    gx, gy, cell = 88, 140, 96
    values = [22, 35, 48, 31, 55, 64, 46, 70, 82, 39, 52, 61]
    value_colors = ["#edf7fb", "#d7eef7", "#bde1f1", "#92cde7", "#5ab3d6"]
    # 1) valued grid cells (background)
    for r in range(3):
        for c in range(4):
            x = gx + c * cell
            y = gy + r * cell
            value = values[r * 4 + c]
            shade = value_colors[min(max((value - 20) // 14, 0), len(value_colors) - 1)]
            body.append(rect(x, y, cell, cell, shade, COLORS["grid"], stroke_width=1.1))
            body.append(text(x + cell - 10, y + 20, f"v{r * 4 + c + 1}={value}", "tiny", "end"))

    # 2) district polygon, clipped to every cell -> one true sliver per overlapped cell
    district = [
        (230, 150), (340, 150), (376, 236), (430, 288),
        (376, 332), (356, 408), (240, 412), (184, 332), (184, 236),
    ]
    cell_area = float(cell * cell)
    slivers: list[tuple[int, int, float, float, list[tuple[float, float]], tuple[float, float]]] = []
    for r in range(3):
        for c in range(4):
            idx = r * 4 + c
            xmin = gx + c * cell
            ymin = gy + r * cell
            piece = _clip_poly_to_rect(district, xmin, ymin, xmin + cell, ymin + cell)
            frac = _shoelace_area(piece) / cell_area
            if frac <= 0.02:
                continue
            slivers.append((idx + 1, values[idx], frac, values[idx] * frac, piece, _poly_centroid(piece)))

    # Shade each sliver green by coverage fraction (darker = larger weight); grid-coloured
    # stroke so the grid lines visibly cut the district into cell-pieces.
    for _cn, _v, frac, _co, points, _cen in slivers:
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        opacity = 0.16 + 0.5 * frac
        body.append(f'<polygon points="{pts}" fill="{COLORS["process"]}" fill-opacity="{opacity:.2f}" stroke="{COLORS["grid"]}" stroke-width="1.1"/>')

    # District outline on top so the boundary reads as a single polygon.
    dpts = " ".join(f"{x:.1f},{y:.1f}" for x, y in district)
    body.append(f'<polygon points="{dpts}" fill="none" stroke="{COLORS["output"]}" stroke-width="3"/>')

    # Weight label centred in each sliver (full cells show 1.00 in bold).
    for _cn, _v, frac, _co, _points, (cenx, ceny) in slivers:
        body.append(text(cenx, ceny + 4, f"{frac:.2f}", "label" if frac > 0.995 else "small", "middle"))

    body.append(text(300, 470, "Grid lines cut the district into one sliver per cell it covers.", "small", "middle"))
    body.append(text(300, 491, "Sliver area = intersection a_ij; number in sliver = a_ij as a fraction of the cell.", "tiny", "middle"))

    # Formula + equal-area note.
    body.append(text(705, 156, "Weighted average", "label", "middle"))
    body.append(formula_area_weighted_average(705, 194))
    body.append(arrow(492, 300, 590, 210))
    body.extend(
        box(
            600,
            232,
            410,
            72,
            "#fff8e6",
            COLORS["hazard"],
            "Equal-area rule",
            ["Cells are equal-area after EPSG:6933 reprojection, so area fraction is proportional to a_ij."],
            radius=5,
        )
    )

    # 3) worked table derived from the SAME computation
    tx, ty = 618, 336
    widths = [78, 74, 104, 96]
    x = tx
    for w, header in zip(widths, ["cell", "value", "area frac.", "contrib."]):
        body.append(rect(x, ty, w, 30, "#f4f6f8", COLORS["grey"], stroke_width=1.0))
        body.append(text(x + w / 2, ty + 20, header, "tiny", "middle"))
        x += w
    sum_frac = sum(s[2] for s in slivers)
    sum_contrib = sum(s[3] for s in slivers)
    data_rows = [(f"j{cn}", str(v), f"{fr:.2f}", f"{co:.1f}") for cn, v, fr, co, _p, _c in slivers]
    data_rows.append(("v\u0304_i", "", f"{sum_frac:.2f}", f"{sum_contrib / sum_frac:.1f}"))
    for row_i, row in enumerate(data_rows):
        x = tx
        y = ty + 30 + row_i * 30
        last = row_i == len(data_rows) - 1
        for w, value in zip(widths, row):
            body.append(rect(x, y, w, 30, "#f8fafc" if last else "white", COLORS["grid"], stroke_width=0.8))
            body.append(text(x + w / 2, y + 20, value, "tiny", "middle"))
            x += w
    return Svg().wrap(body, "FIG-10. Fractional-area overlap weights", "Schematic of polygon-cell area weights.")


def figure_11() -> str:
    body: list[str] = []
    body.extend(box(70, 165, 220, 104, COLORS["light_blue"], COLORS["source"], "Daily model fields", ["tasmax, pr, etc.", "one GCM member"]))
    body.append(arrow(292, 217, 350, 217))
    body.extend(box(350, 155, 230, 124, COLORS["light_teal"], COLORS["process"], "Daily \u2192 annual index", ["hot days, Rx1day,", "SPI spell counts"]))
    body.append(arrow(582, 217, 640, 217))
    body.extend(box(640, 155, 230, 124, "#fff8e6", COLORS["hazard"], "Annual \u2192 period mean", ["example: 2040-2060", "21 annual index fields"]))
    body.append(arrow(872, 217, 930, 217))
    body.extend(box(930, 155, 210, 124, COLORS["light_rose"], COLORS["output"], "Per-model period mean", ["one value per model", "per admin unit"]))
    body.append(line(350, 132, 870, 132, COLORS["process"], 1.5))
    body.append(line(350, 132, 350, 145, COLORS["process"], 1.5))
    body.append(line(870, 132, 870, 145, COLORS["process"], 1.5))
    body.append(text(610, 122, "1. time-average within each model", "small", "middle"))

    fan_x0, fan_y0 = 110, 390
    body.append(text(600, 340, "24 model period means collapse to ensemble statistics", "label", "middle"))
    body.append(line(132, fan_y0 + 82, 448, fan_y0 + 82, COLORS["grid"], 1.1))
    body.append(line(132, fan_y0 + 46, 132, fan_y0 + 118, COLORS["grid"], 1.1))
    body.append(line(448, fan_y0 + 46, 448, fan_y0 + 118, COLORS["grid"], 1.1))
    body.append(text(290, fan_y0 + 140, "24 model period means", "small", "middle"))
    member_values = [17, 25, 31, 35, 39, 42, 44, 46, 49, 51, 53, 55, 57, 59, 61, 62, 64, 66, 68, 70, 72, 75, 79, 84]
    for i in range(24):
        x = 150 + i * 12
        y = fan_y0 + 118 - member_values[i] / 100 * 72
        body.append(line(x, fan_y0 + 82, x, y, COLORS["grey"], 0.9))
        body.append(f'<circle cx="{x}" cy="{y:.1f}" r="4.2" fill="{COLORS["grey"]}" fill-opacity="0.78"/>')
    body.append(arrow(466, fan_y0 + 82, 850, fan_y0 + 82))
    body.append(line(890, fan_y0 + 82, 1055, fan_y0 + 82, COLORS["output"], 4.0))
    body.append(text(975, fan_y0 + 70, "ensemble mean", "label", "middle"))
    spread = [("std. dev.", fan_y0 + 110), ("median", fan_y0 + 128), ("p5 / p95", fan_y0 + 146)]
    for label_value, y in spread:
        body.append(line(890, y, 1055, y, COLORS["grey"], 1.4, "5 4"))
        body.append(text(1070, y + 4, label_value, "tiny"))
    body.append(line(850, fan_y0 + 34, 1058, fan_y0 + 34, COLORS["output"], 1.5))
    body.append(line(850, fan_y0 + 34, 850, fan_y0 + 48, COLORS["output"], 1.5))
    body.append(line(1058, fan_y0 + 34, 1058, fan_y0 + 48, COLORS["output"], 1.5))
    body.append(text(954, fan_y0 + 24, "2. ensemble-average across models", "small", "middle"))
    body.extend(
        box(
            210,
            620,
            780,
            66,
            "#f4f6f8",
            COLORS["grey"],
            "Composite contract",
            ["Composite scores use the ensemble mean; spread statistics are retained for diagnostics."],
            radius=5,
        )
    )
    return Svg().wrap(body, "FIG-11. Temporal aggregation and ensemble chain", "Daily to annual to period to ensemble schematic.")


def figure_12() -> str:
    body: list[str] = []
    x0, y0, w, h = 95, 125, 990, 455
    body.append(f'<rect x="{x0}" y="{y0}" width="{w}" height="{h}" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    for m in range(0, 13):
        xx = x0 + m / 12 * w
        body.append(line(xx, y0 + 20, xx, y0 + h - 45, COLORS["grid"], 0.8))
    for temp in range(18, 45, 4):
        yy = y0 + h - 45 - (temp - 16) / 30 * (h - 80)
        body.append(line(x0 + 55, yy, x0 + w - 28, yy, COLORS["grid"], 0.8))
        body.append(text(x0 + 45, yy + 4, str(temp), "tiny", "end"))
    body.append(line(x0 + 55, y0 + h - 45, x0 + w - 28, y0 + h - 45, COLORS["muted"]))
    body.append(line(x0 + 55, y0 + 20, x0 + 55, y0 + h - 45, COLORS["muted"]))
    pts: list[tuple[float, float]] = []
    for d in range(1, 366, 3):
        seasonal = 29 + 9 * __import__("math").sin((d - 78) / 365 * 2 * __import__("math").pi) + 1.2 * __import__("math").sin((d - 20) / 365 * 4 * __import__("math").pi)
        xx = x0 + 55 + (d - 1) / 364 * (w - 83)
        yy = y0 + h - 45 - (seasonal - 16) / 30 * (h - 80)
        pts.append((xx, yy))
    body.append(polyline(pts, COLORS["output"], 3))
    day121 = 121
    x121 = x0 + 55 + (day121 - 1) / 364 * (w - 83)
    for offset in [-2, 2]:
        xx = x0 + 55 + (day121 + offset - 1) / 364 * (w - 83)
        body.append(line(xx, y0 + 20, xx, y0 + h - 45, COLORS["hazard"], 1.2, "5 4"))
    body.append(f'<rect x="{x121 - 5}" y="{y0 + 20}" width="10" height="{h - 65}" fill="#f59e0b" fill-opacity="0.12"/>')
    body.append(text(x121, y0 + h - 16, "1 May (DOY 121)", "small", "middle"))
    body.append(text(x121 + 64, y0 + 42, "+/-2-day pooling window", "small"))
    eval_days = [(118, 40.4), (121, 41.8), (124, 42.5), (160, 38.9), (250, 32.0)]
    for d, val in eval_days:
        xx = x0 + 55 + (d - 1) / 364 * (w - 83)
        yy = y0 + h - 45 - (val - 16) / 30 * (h - 80)
        body.append(f'<circle cx="{xx}" cy="{yy}" r="6" fill="{COLORS["hazard"]}" stroke="white" stroke-width="1.5"/>')
    body.append(text(x0 + w - 16, y0 + 52, "tau_d: baseline 90th percentile", "label", "end"))
    body.append(text(x0 + w - 16, y0 + 76, "orange points: evaluation-year exceedances", "small", "end"))
    body.append(
        text(
            x0 + 18,
            y0 + 20 + (h - 65) / 2,
            "tasmax (deg C)",
            "small",
            "middle",
            attrs={"transform": f"rotate(-90 {x0 + 18} {y0 + 20 + (h - 65) / 2})"},
        )
    )
    body.append(text(x0 + w / 2, y0 + h - 2, "day of year", "small", "middle"))
    return Svg().wrap(body, "FIG-12. DOY percentile threshold curve", "Illustrative seasonal percentile threshold.")


def gamma_pdf(x: float, alpha: float = 2.4, beta: float = 38.0) -> float:
    import math

    return (x ** (alpha - 1) * math.exp(-x / beta)) / (math.gamma(alpha) * beta**alpha)


def gamma_cdf_approx(x: float, alpha: float = 2.4, beta: float = 38.0) -> float:
    # Numerical integration is sufficient for this deterministic illustrative SVG.
    if x <= 0:
        return 0.0
    steps = 140
    dx = x / steps
    return min(sum(gamma_pdf((i + 0.5) * dx, alpha, beta) * dx for i in range(steps)), 0.999)


def figure_14() -> str:
    body: list[str] = []
    panels = [(60, 135, "1. Accumulate monthly precipitation"), (440, 135, "2. Gamma fit and mixed CDF"), (820, 135, "3. Normal-quantile transform")]
    for x, y, title in panels:
        body.append(f'<rect x="{x}" y="{y}" width="320" height="410" rx="6" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
        body.append(text(x + 160, y + 30, title, "label", "middle"))
    # Panel 1 bars.
    vals = [22, 18, 30, 55, 115, 210, 255, 240, 170, 86, 42, 26]
    months = list("JFMAMJJASOND")
    maxv = 280
    for i, v in enumerate(vals):
        bx = 90 + i * 22
        bh = v / maxv * 230
        body.append(f'<rect x="{bx}" y="{485 - bh}" width="15" height="{bh}" fill="{COLORS["source"]}"/>')
        body.append(text(bx + 7.5, 502, months[i], "tiny", "middle"))
    body.append(line(85, 485, 345, 485, COLORS["muted"]))
    body.append(line(85, 250, 85, 485, COLORS["muted"]))
    body.append(text(210, 522, "calendar months -> SPI-3/6/12 accumulations", "small", "middle"))
    body.append(text(93, 238, "mm", "small"))
    # Panel 2 PDF and CDF.
    x0, ybase = 480, 485
    body.append(line(x0, ybase, x0 + 245, ybase, COLORS["muted"]))
    body.append(line(x0, 250, x0, ybase, COLORS["muted"]))
    pdf_pts = []
    cdf_pts = []
    for i in range(1, 120):
        xxv = i / 119 * 260
        px = x0 + xxv / 260 * 245
        pdf_y = ybase - gamma_pdf(xxv) * 9000
        cdf = 0.08 + 0.92 * gamma_cdf_approx(xxv)
        cdf_y = ybase - cdf * 215
        pdf_pts.append((px, pdf_y))
        cdf_pts.append((px, cdf_y))
    body.append(polyline(pdf_pts, COLORS["hazard"], 2.5))
    body.append(polyline(cdf_pts, COLORS["output"], 2.5))
    body.append(rect(518, 182, 178, 54, "white", COLORS["grey"], stroke_width=1.2, radius=4))
    body.append(line(532, 202, 558, 202, COLORS["hazard"], 2.5))
    body.append(text(565, 206, "g(x): Gamma density", "tiny"))
    body.append(line(532, 224, 558, 224, COLORS["output"], 2.5))
    body.append(text(565, 228, "H(x): mixed CDF", "tiny"))
    body.append(text(x0 + 125, 507, "q = zero-month probability", "small", "middle"))
    # Panel 3 normal transform.
    x0, ybase = 860, 390
    body.append(line(x0, ybase, x0 + 230, ybase, COLORS["muted"]))
    body.append(line(x0 + 115, 235, x0 + 115, 500, COLORS["muted"]))
    nd = NormalDist()
    norm_pts = []
    for i in range(2, 99):
        p = i / 100
        z = nd.inv_cdf(p)
        px = x0 + p * 230
        py = ybase - z * 45
        norm_pts.append((px, py))
    body.append(polyline(norm_pts, COLORS["process"], 2.5))
    y_spi = ybase + 45
    body.append(line(x0, y_spi, x0 + 230, y_spi, COLORS["output"], 2, "6 4"))
    body.append(f'<rect x="{x0}" y="{y_spi}" width="{0.1587 * 230}" height="{500 - y_spi}" fill="{COLORS["light_rose"]}" fill-opacity="0.9"/>')
    body.append(text(x0 + 42, y_spi + 22, "SPI < -1", "small"))
    body.append(text(x0 + 116, 530, "H(x)", "small", "middle"))
    body.append(formula_spi_transform(x0 + 120, 224, "middle"))
    body.append(arrow(380, 340, 438, 340))
    body.append(arrow(760, 340, 818, 340))
    return Svg().wrap(body, "FIG-14. SPI derivation", "Three-panel illustrative SPI transformation.")


def figure_15() -> str:
    body: list[str] = []
    matrix = [
        [1, 2, 2, 3, 4],
        [2, 2, 3, 4, 4],
        [2, 3, 4, 4, 5],
        [3, 4, 4, 5, 5],
        [4, 5, 5, 5, 5],
    ]
    depth_labels = ["<=0.2 m", "<=0.5 m", "<=1.0 m", "<=2.5 m", ">2.5 m"]
    extent_labels = ["<=1%", "<=5%", "<=15%", "<=25%", ">25%"]
    severity_colors = {
        1: "#dff0fb",
        2: "#b7e3d8",
        3: "#fff2cc",
        4: "#f7c7a6",
        5: "#e89aa8",
    }
    severity_names = {
        1: "Very low",
        2: "Low",
        3: "Moderate",
        4: "High",
        5: "Extreme",
    }

    x0, y0 = 300, 150
    cell_w, cell_h = 118, 82
    grid_w, grid_h = cell_w * 5, cell_h * 5
    body.append(rect(x0 - 160, y0 - 70, grid_w + 300, grid_h + 190, COLORS["panel"], COLORS["grid"], stroke_width=1.2, radius=6))
    body.append(text(x0 + grid_w / 2, y0 - 42, "Depth class", "label", "middle"))
    body.append(
        text(
            x0 - 118,
            y0 + grid_h / 2,
            "Extent class",
            "label",
            "middle",
            attrs={"transform": f"rotate(-90 {x0 - 118} {y0 + grid_h / 2})"},
        )
    )

    for col, label in enumerate(depth_labels):
        x = x0 + col * cell_w + cell_w / 2
        body.append(text(x, y0 - 16, label, "small", "middle"))
    for row, label in enumerate(extent_labels):
        y = y0 + row * cell_h + cell_h / 2
        body.append(text(x0 - 18, y + 4, label, "small", "end"))

    for row, values in enumerate(matrix):
        for col, severity in enumerate(values):
            x = x0 + col * cell_w
            y = y0 + row * cell_h
            body.append(rect(x, y, cell_w, cell_h, severity_colors[severity], "white", stroke_width=2.0))
            body.append(text(x + cell_w / 2, y + 38, str(severity), "label", "middle"))
            body.append(text(x + cell_w / 2, y + 58, severity_names[severity], "tiny", "middle"))

    body.append(rect(x0, y0, grid_w, grid_h, "none", COLORS["muted"], stroke_width=1.6))
    body.append(text(x0 + grid_w / 2, y0 + grid_h + 38, "Rows = extent class; columns = depth class.", "small", "middle"))

    legend_x, legend_y = 185, 620
    body.append(text(legend_x, legend_y - 18, "Severity class", "label"))
    for severity in range(1, 6):
        x = legend_x + (severity - 1) * 170
        body.append(rect(x, legend_y, 34, 24, severity_colors[severity], COLORS["grid"], stroke_width=1.0, radius=3))
        body.append(text(x + 47, legend_y + 17, f"{severity}: {severity_names[severity]}", "small"))

    return Svg().wrap(body, "FIG-15. JRC RP-100 severity lookup matrix", "Heatmap of fixed JRC depth and extent severity classes.")


def figure_18() -> str:
    body: list[str] = []
    body.extend(box(50, 256, 210, 120, COLORS["light_blue"], COLORS["source"], "Inputs", ["future metric v", "1990-2010 baseline", "impact band [a,b]"]))
    lanes = [
        (325, 138, "S_abs", "p10-p90 cohort position", COLORS["source"], "#e7f2fb"),
        (325, 268, "S_chg", "change vs 1990-2010, then p10-p90", COLORS["process"], "#e4f7f4"),
        (325, 398, "S_imp", "clip((v-a)/(b-a)) x 100", COLORS["hazard"], "#fff2cc"),
    ]
    body.append(line(260, 316, 300, 316, COLORS["muted"], 2.0))
    body.append(line(300, 186, 300, 446, COLORS["muted"], 2.0))
    for x, y, title, sub, stroke, fill in lanes:
        lens_center_y = y + 48
        body.append(arrow(300, lens_center_y, x, lens_center_y))
        body.extend(box(x, y, 250, 96, fill, stroke, title, [sub], radius=5))
        body.append(arrow(x + 250, lens_center_y, 610, lens_center_y))
    body.append(line(610, 186, 610, 446, COLORS["muted"], 2.0))
    body.append(arrow(610, 316, 640, 316))
    body.extend(
        box(
            640,
            250,
            225,
            132,
            COLORS["light_rose"],
            COLORS["output"],
            "One rule score S_r",
            ["lens-weighted mean", "available lenses only", "renormalized weights"],
        )
    )
    body.append(arrow(865, 316, 895, 316))
    body.append(line(895, 186, 895, 446, COLORS["grey"], 1.6, "6 5"))
    body.append(text(895, 126, "repeat per declared rule", "small", "middle"))
    repeated_rules = [
        (930, 150, "Rule 1 score", "S_1, W_r1"),
        (930, 280, "Rule 2 score", "S_2, W_r2"),
        (930, 410, "Rule 3 score", "S_3, W_r3"),
    ]
    for x, y, label, subline in repeated_rules:
        rule_center_y = y + 36
        body.append(arrow(895, rule_center_y, x, rule_center_y))
        body.extend(box(x, y, 150, 72, "#fff8e6", COLORS["hazard"], label, [subline], radius=5))
        body.append(arrow(x + 150, rule_center_y, 1105, rule_center_y))
    body.append(line(1105, 186, 1105, 446, COLORS["muted"], 2.0))
    body.append(arrow(1105, 316, 1130, 316))
    body.append(f'<circle cx="1160" cy="316" r="30" fill="{COLORS["light_rose"]}" stroke="{COLORS["output"]}" stroke-width="2"/>')
    body.append(text(1160, 310, "Bundle", "label", "middle"))
    body.append(text(1160, 330, "composite", "small", "middle"))
    body.append(rect(60, 104, 230, 54, "#f4f6f8", COLORS["grey"], stroke_width=1.2, radius=4))
    body.append(text(72, 126, "Lens weights combine scores", "tiny"))
    body.append(text(72, 145, "inside a rule; rule weights form bundle.", "tiny"))
    return Svg().wrap(body, "FIG-18. Three-lens blended rule schematic", "Sectoral lens and rule aggregation schematic.")


def figure_19() -> str:
    body: list[str] = []
    x0, y0, w, h = 150, 145, 860, 410
    body.append(f'<rect x="{x0}" y="{y0}" width="{w}" height="{h}" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    body.append(f'<rect x="{x0 + 60}" y="{y0 + 35}" width="{(w - 100) / 3}" height="{h - 90}" fill="#eef3f8" opacity="0.55"/>')
    body.append(f'<rect x="{x0 + 60 + (40 - 35) / 15 * (w - 100)}" y="{y0 + 35}" width="{(45 - 40) / 15 * (w - 100)}" height="{h - 90}" fill="#fff2cc" opacity="0.48"/>')
    body.append(f'<rect x="{x0 + 60 + (45 - 35) / 15 * (w - 100)}" y="{y0 + 35}" width="{(50 - 45) / 15 * (w - 100)}" height="{h - 90}" fill="#fde2e7" opacity="0.42"/>')
    body.append(line(x0 + 60, y0 + h - 55, x0 + w - 40, y0 + h - 55, COLORS["muted"]))
    body.append(line(x0 + 60, y0 + 35, x0 + 60, y0 + h - 55, COLORS["muted"]))
    for s in [0, 25, 50, 75, 100]:
        yy = y0 + h - 55 - s / 100 * (h - 100)
        body.append(line(x0 + 60, yy, x0 + w - 40, yy, COLORS["grid"], 0.8))
        body.append(text(x0 + 50, yy + 4, str(s), "tiny", "end"))
    def xp(v: float) -> float:
        return x0 + 60 + (v - 35) / 15 * (w - 100)

    def yp(score: float) -> float:
        return y0 + h - 55 - score / 100 * (h - 100)

    a, b = 40, 45
    pts = [(xp(35), yp(0)), (xp(a), yp(0)), (xp(b), yp(100)), (xp(50), yp(100))]
    body.append(polyline(pts, COLORS["output"], 4.0))
    body.append(line(xp(a), yp(0), xp(a), yp(100), COLORS["hazard"], 1.8, "6 5"))
    body.append(line(xp(b), yp(0), xp(b), yp(100), COLORS["hazard"], 1.8, "6 5"))
    body.append(text(xp(a), y0 + h - 26, "onset a = 40 \u00b0C", "small", "middle"))
    body.append(text(xp(b), y0 + h - 26, "saturation b = 45 \u00b0C", "small", "middle"))
    body.append(text(x0 + w / 2, y0 + h + 22, "raw metric value v: TXx (\u00b0C)", "small", "middle"))
    body.append(text(x0 + 18, y0 + h / 2, "impact score S_imp", "small", "middle", attrs={"transform": f"rotate(-90 {x0 + 18} {y0 + h / 2})"}))
    body.extend(
        box(
            700,
            200,
            330,
            88,
            "#fff8e6",
            COLORS["hazard"],
            "Formula",
            ["S_imp = clip((v - a) / (b - a), 0, 1)", "x 100"],
            radius=5,
        )
    )
    body.append(text(520, 305, "linear ramp", "label", "middle"))
    body.append(text(268, 512, "score = 0", "small", "middle"))
    body.append(text(920, 162, "score = 100", "small", "middle"))
    return Svg().wrap(body, "FIG-19. Impact-band ramp", "Impact lens ramp schematic.")


def figure_20() -> str:
    body: list[str] = []
    lens = {"A - already hot": [90, 20, 100], "B - fast-warming": [20, 100, 40]}
    final = {"A - already hot": [76, 90], "B - fast-warming": [47, 20]}
    labels = ["S_abs", "S_chg", "S_imp"]
    colors = [COLORS["source"], COLORS["process"], COLORS["hazard"]]
    # Panel A.
    body.append(f'<rect x="70" y="130" width="520" height="425" rx="6" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    body.append(text(330, 112, "Panel A: lens scores", "label", "middle"))
    body.append(line(125, 500, 555, 500, COLORS["muted"]))
    body.append(line(125, 210, 125, 500, COLORS["muted"]))
    for s in [0, 25, 50, 75, 100]:
        yy = 500 - s / 100 * 290
        body.append(line(125, yy, 555, yy, COLORS["grid"], 0.8))
        body.append(text(115, yy + 4, str(s), "tiny", "end"))
    x_positions = [180, 330, 480]
    for group_i, (district, vals) in enumerate(lens.items()):
        dx = 0 if group_i == 0 else 42
        for i, value in enumerate(vals):
            x = x_positions[i] + dx
            bh = value / 100 * 290
            body.append(f'<rect x="{x}" y="{500 - bh}" width="30" height="{bh}" fill="{colors[i]}"/>')
            body.append(text(x + 15, 500 - bh - 8, str(value), "tiny", "middle"))
    for i, lab in enumerate(labels):
        body.append(text(x_positions[i] + 36, 528, lab, "small", "middle"))
    body.append(text(185, 580, "A: TXx 45.5 deg C, anomaly +1.5 deg C", "small"))
    body.append(text(185, 604, "B: TXx 42.0 deg C, anomaly +3.5 deg C", "small"))
    # Panel B.
    body.append(f'<rect x="670" y="130" width="460" height="425" rx="6" fill="{COLORS["panel"]}" stroke="{COLORS["grid"]}"/>')
    body.append(text(900, 112, "Panel B: blended vs pure-absolute", "label", "middle"))
    body.append(line(725, 500, 1085, 500, COLORS["muted"]))
    body.append(line(725, 210, 725, 500, COLORS["muted"]))
    for s in [0, 25, 50, 75, 100]:
        yy = 500 - s / 100 * 290
        body.append(line(725, yy, 1085, yy, COLORS["grid"], 0.8))
        body.append(text(715, yy + 4, str(s), "tiny", "end"))
    groups = [(805, "A - already hot"), (970, "B - fast-warming")]
    for gx, district in groups:
        vals = final[district]
        for i, value in enumerate(vals):
            x = gx + i * 42
            bh = value / 100 * 290
            fill = COLORS["output"] if i == 0 else COLORS["grey"]
            body.append(f'<rect x="{x}" y="{500 - bh}" width="34" height="{bh}" fill="{fill}"/>')
            body.append(text(x + 17, 500 - bh - 8, str(value), "tiny", "middle"))
        body.append(text(gx + 38, 528, "A" if district.startswith("A") else "B", "small", "middle"))
    body.append(text(855, 580, "Blended = 0.40*S_abs + 0.25*S_chg + 0.35*S_imp", "small", "middle"))
    body.append(text(855, 605, "Impact band: TXx 40-45 deg C; cohort q10/q90: 41-46 deg C and +1.0/+3.5 deg C", "small", "middle"))
    return Svg().wrap(body, "FIG-20. District A vs B lens worked example", "Worked example bar chart.")


FIGURES = {
    "fig_01_pipeline_flow.svg": figure_01,
    "fig_02_hazard_exposure_vulnerability_scope.svg": figure_02,
    "fig_05_temporal_coverage_analysis_windows.svg": figure_05,
    "fig_06_bcsd_schematic.svg": figure_06,
    "fig_08_district_block_resolution_zoom.svg": figure_08,
    "fig_09_admin_first_vs_grid_first.svg": figure_09,
    "fig_10_fractional_area_overlap_weights.svg": figure_10,
    "fig_11_temporal_aggregation_ensemble_chain.svg": figure_11,
    "fig_12_doy_percentile_threshold_curve.svg": figure_12,
    "fig_14_spi_derivation.svg": figure_14,
    "fig_15_jrc_rp100_severity_lookup_matrix.svg": figure_15,
    "fig_18_three_lens_blended_rule.svg": figure_18,
    "fig_19_impact_band_ramp.svg": figure_19,
    "fig_20_district_a_b_lens_example.svg": figure_20,
}


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _float_attr(node: ET.Element, name: str, default: float = 0.0) -> float:
    value = node.attrib.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _canvas_size(root: ET.Element) -> tuple[float, float]:
    return _float_attr(root, "width", 1200.0), _float_attr(root, "height", 760.0)


def _rects(root: ET.Element, cls: str) -> list[tuple[float, float, float, float]]:
    found: list[tuple[float, float, float, float]] = []
    for node in root.iter():
        if _local_name(node.tag) != "rect":
            continue
        if node.attrib.get("class") != cls:
            continue
        x = _float_attr(node, "x")
        y = _float_attr(node, "y")
        w = _float_attr(node, "width")
        h = _float_attr(node, "height")
        found.append((x, y, w, h))
    return found


def _overlap(a: tuple[float, float, float, float], b: tuple[float, float, float, float]) -> tuple[float, float]:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    return max(0.0, min(ax + aw, bx + bw) - max(ax, bx)), max(0.0, min(ay + ah, by + bh) - max(ay, by))


def validate(svg: str, filename: str) -> None:
    root = ET.fromstring(svg)
    width, height = _canvas_size(root)

    for node in root.iter():
        tag = _local_name(node.tag)
        if tag == "rect":
            x = _float_attr(node, "x")
            y = _float_attr(node, "y")
            w = _float_attr(node, "width")
            h = _float_attr(node, "height")
            if x < -0.1 or y < -0.1 or x + w > width + 0.1 or y + h > height + 0.1:
                raise AssertionError(f"{filename}: rect outside canvas at {(x, y, w, h)}")
        elif tag == "circle":
            cx = _float_attr(node, "cx")
            cy = _float_attr(node, "cy")
            r = _float_attr(node, "r")
            if cx - r < -0.1 or cy - r < -0.1 or cx + r > width + 0.1 or cy + r > height + 0.1:
                raise AssertionError(f"{filename}: circle outside canvas at {(cx, cy, r)}")
        elif tag == "line":
            x1 = _float_attr(node, "x1")
            y1 = _float_attr(node, "y1")
            x2 = _float_attr(node, "x2")
            y2 = _float_attr(node, "y2")
            if min(x1, x2) < -0.1 or min(y1, y2) < -0.1 or max(x1, x2) > width + 0.1 or max(y1, y2) > height + 0.1:
                raise AssertionError(f"{filename}: line outside canvas")
        elif tag == "text":
            content = "".join(node.itertext())
            x = _float_attr(node, "x")
            y = _float_attr(node, "y")
            cls = node.attrib.get("class", "small")
            font_px = FONT_PX.get(cls, FONT_PX["small"])
            bold = cls in {"title", "label"}
            text_width = approx_px(content, font_px, bold=bold)
            anchor = node.attrib.get("text-anchor", "start")
            left = x - text_width / 2 if anchor == "middle" else x - text_width if anchor == "end" else x
            right = left + text_width
            if left < -1.0 or right > width + 1.0 or y < -1.0 or y > height + 1.0:
                raise AssertionError(f"{filename}: text outside canvas: {content!r}")
            box_meta = node.attrib.get("data-box")
            if box_meta:
                bx, by, bw, bh = [float(part) for part in box_meta.split(",")]
                if left < bx + 8 or right > bx + bw - 8:
                    raise AssertionError(f"{filename}: text exceeds box: {content!r}")
                if y < by + 8 or y > by + bh - 6:
                    raise AssertionError(f"{filename}: text vertical extent exceeds box: {content!r}")

    boxes = _rects(root, "flow-box")
    for i, box_a in enumerate(boxes):
        for box_b in boxes[i + 1 :]:
            ox, oy = _overlap(box_a, box_b)
            if ox > 3 and oy > 3:
                raise AssertionError(f"{filename}: flow boxes overlap by {(ox, oy)}")


def export_png(svg_path: Path) -> None:
    try:
        import cairosvg  # type: ignore[import-not-found]
    except ImportError:
        print(f"PNG export skipped for {svg_path.name}: install cairosvg or run `rsvg-convert -w 2400 {svg_path.name} -o {svg_path.with_suffix('.png').name}`")
        return
    cairosvg.svg2png(url=str(svg_path), write_to=str(svg_path.with_suffix(".png")), output_width=2400)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--png", action="store_true", help="Also export PNG previews next to the generated SVGs.")
    args = parser.parse_args(argv)
    for filename, builder in FIGURES.items():
        path = OUT_DIR / filename
        svg = builder()
        validate(svg, filename)
        path.write_text(svg, encoding="utf-8")
        if args.png:
            export_png(path)
        try:
            display_path = path.relative_to(Path.cwd())
        except ValueError:
            display_path = path
        print(display_path)


if __name__ == "__main__":
    main()
