"""Generate essential technical-guidance-note figures as editable SVG.

The figures produced here correspond to FIG-01, FIG-06, FIG-08, FIG-09,
FIG-12, FIG-14, FIG-18, and FIG-20 in ``docs/figure_gen_instructions.md``.
They are intentionally self-contained SVG schematics/plots so they can be
reviewed and edited without a plotting runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from typing import Iterable
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
            f"<title>{escape(title)}</title>",
            f"<desc>{escape(desc)}</desc>",
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


def figure_01() -> str:
    body: list[str] = [
        text(60, 48, "FIG-01. End-to-End IRT Pipeline Flow", "title"),
        text(60, 74, "From source inputs to grid-first metrics, bundles, and 0-100 higher-is-worse hazard-pressure outputs.", "subtitle"),
    ]
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
    body.append(text(60, 710, "Source: author-created schematic from technical guidance note sections 1-8.", "note"))
    return Svg(width=1320).wrap(body, "FIG-01. End-to-end pipeline flow", "IRT pipeline schematic.")


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


def figure_06() -> str:
    body = [
        text(60, 48, "FIG-06. BCSD Schematic", "title"),
        text(60, 74, "Bias correction maps monthly model distributions to observations; spatial disaggregation places corrected fields on the 0.25 deg grid.", "subtitle"),
    ]
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
    body.extend(
        box(
            96,
            515,
            1008,
            82,
            "#f4f6f8",
            COLORS["grey"],
            "Important limitation",
            ["NASA applies BCSD before IRT ingestion; BCSD preserves GCM trends/variability but does not fix large-scale monsoon dynamics."],
            radius=5,
        )
    )
    body.append(text(60, 710, "Source: author-created schematic; no real data used.", "note"))
    return Svg().wrap(body, "FIG-06. BCSD schematic", "BCSD two-step schematic.")


def figure_08() -> str:
    body = [
        text(60, 48, "FIG-08. District/Block Resolution Zoom with 0.25 Degree Cells", "title"),
        text(60, 74, "Illustrative geometry: districts sample many cells; blocks sharing a grid cell share a score.", "subtitle"),
    ]
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
    body.append(text(600, 350, "zoom", "small", "middle"))
    body.append(arrow(535, 350, 642, 350))
    body.extend(
        box(
            158,
            672,
            884,
            48,
            "#f4f6f8",
            COLORS["grey"],
            "Figure note",
            ["0.25 deg grid cell (~25 km scale; physical size varies by latitude). Illustrative geometry, not a boundary-derived map."],
            radius=5,
        )
    )
    return Svg().wrap(body, "FIG-08. District/block resolution zoom", "Illustrative resolution contrast.")


def figure_09() -> str:
    body = [
        text(60, 48, "FIG-09. Admin-First vs Grid-First Worked Example", "title"),
        text(60, 74, "Averaging before thresholding can erase a nonlinear extreme; grid-first computation preserves it.", "subtitle"),
    ]
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
    body.append(text(60, 710, "Source: author-created schematic using the exact conceptual example in section 4.1; values are illustrative.", "note"))
    return Svg().wrap(body, "FIG-09. Admin-first vs grid-first", "Worked example of nonlinear threshold bias.")


def figure_12() -> str:
    body = [
        text(60, 48, "FIG-12. DOY Percentile Threshold Curve", "title"),
        text(60, 74, "A day-of-year threshold tau_d is calibrated from a +/-2-day baseline window and applied unchanged to evaluation years.", "subtitle"),
    ]
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
    body.append(text(x0 + 10, y0 + 28, "tasmax (deg C)", "small"))
    body.append(text(x0 + w / 2, y0 + h - 2, "day of year", "small", "middle"))
    body.append(text(60, 710, "Source: synthetic illustrative curve. Baseline concept: 1990-2010, Feb 29 excluded, strict exceedance x_t > tau_d.", "note"))
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
    body = [
        text(60, 48, "FIG-14. SPI Derivation", "title"),
        text(60, 74, "Monthly precipitation accumulation is fitted with a Gamma distribution, mixed with zero probability, and transformed to standard-normal SPI.", "subtitle"),
    ]
    panels = [(60, 135, "1. Accumulate monthly precipitation"), (440, 135, "2. Mixed Gamma CDF H(x)"), (820, 135, "3. Normal-quantile transform")]
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
    body.append(line(548, 286, 588, 294, COLORS["hazard"], 1.3))
    body.append(text(515, 282, "G(x): Gamma CDF", "small"))
    body.append(line(637, 318, 676, 302, COLORS["output"], 1.3))
    body.append(text(604, 334, "H(x) = q + (1-q)G(x)", "small"))
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
    body.extend(
        box(
            155,
            620,
            890,
            70,
            "#f4f6f8",
            COLORS["grey"],
            "Method note",
            ["Illustrative synthetic distribution. IRT fits Gamma parameters over 1990-2010 and applies them unchanged to SSP periods."],
            radius=5,
        )
    )
    return Svg().wrap(body, "FIG-14. SPI derivation", "Three-panel illustrative SPI transformation.")


def figure_18() -> str:
    body = [
        text(60, 48, "FIG-18. Three-Lens Blended Rule Schematic", "title"),
        text(60, 74, "Sectoral rules blend relative absolute pressure, change from baseline, and fixed impact-band position before bundle aggregation.", "subtitle"),
    ]
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
    body.extend(
        box(
            160,
            610,
            880,
            70,
            "#f4f6f8",
            COLORS["grey"],
            "Two weight layers",
            ["Lens weights combine S_abs/S_chg/S_imp inside a rule; rule weights combine finite rule scores into the sector bundle."],
            radius=5,
        )
    )
    return Svg().wrap(body, "FIG-18. Three-lens blended rule schematic", "Sectoral lens and rule aggregation schematic.")


def figure_20() -> str:
    body = [
        text(60, 48, "FIG-20. District A vs B Lens Worked Example", "title"),
        text(60, 74, "Health Risk TXx rule, SSP5-8.5, 2060-2080: the blended score surfaces fast-warming District B.", "subtitle"),
    ]
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
    body.extend(box(695, 625, 470, 66, "#fff8e6", COLORS["hazard"], "District B callout", ["Fast-warming and newly above onset; blended score rises from 20 to 47."], radius=5))
    body.append(text(60, 710, "Source: author-created from the exact worked example table in section 7.3; A/B are illustrative districts.", "note"))
    return Svg().wrap(body, "FIG-20. District A vs B lens worked example", "Worked example bar chart.")


FIGURES = {
    "fig_01_pipeline_flow.svg": figure_01,
    "fig_06_bcsd_schematic.svg": figure_06,
    "fig_08_district_block_resolution_zoom.svg": figure_08,
    "fig_09_admin_first_vs_grid_first.svg": figure_09,
    "fig_12_doy_percentile_threshold_curve.svg": figure_12,
    "fig_14_spi_derivation.svg": figure_14,
    "fig_18_three_lens_blended_rule.svg": figure_18,
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


def main() -> None:
    for filename, builder in FIGURES.items():
        path = OUT_DIR / filename
        svg = builder()
        validate(svg, filename)
        path.write_text(svg, encoding="utf-8")
        export_png(path)
        print(path.relative_to(Path.cwd()))


if __name__ == "__main__":
    main()
