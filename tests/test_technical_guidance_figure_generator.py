"""Tests for the generated Technical Guidance Note SVG figures."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from xml.etree import ElementTree as ET

import pytest


GENERATOR_PATH = Path("docs/figures/technical_guidance_note/generate_essential_figures.py")
BANNED_VISIBLE_CHROME = (
    "FIG-",
    "Source:",
    "Scope caveat",
    "Important limitation",
    "Figure note",
    "Method note",
    "District B callout",
)


def _load_generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("technical_guidance_figure_generator", GENERATOR_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Could not load {GENERATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _visible_text_nodes(root: ET.Element) -> list[ET.Element]:
    return [node for node in root.iter() if _local_name(node.tag) == "text"]


def _math_label_groups(root: ET.Element) -> list[ET.Element]:
    return [
        node
        for node in root.iter()
        if _local_name(node.tag) == "g"
        and "math-label" in node.attrib.get("class", "").split()
        and node.attrib.get("data-math-tex")
    ]


def _view_box(root: ET.Element) -> tuple[float, float, float, float]:
    return tuple(float(part) for part in root.attrib["viewBox"].split())  # type: ignore[return-value]


def _background_rect(root: ET.Element) -> ET.Element:
    backgrounds = [
        node
        for node in root.iter()
        if _local_name(node.tag) == "rect" and node.attrib.get("data-bg") == "canvas"
    ]
    assert len(backgrounds) == 1
    return backgrounds[0]


def test_generated_svgs_keep_accessible_metadata_without_visible_chrome() -> None:
    generator = _load_generator()

    for filename, builder in generator.FIGURES.items():
        svg = builder()
        generator.validate(svg, filename)
        root = ET.fromstring(svg)

        assert root.attrib["aria-labelledby"] == "title desc"
        titles = [node for node in root if _local_name(node.tag) == "title"]
        descs = [node for node in root if _local_name(node.tag) == "desc"]
        assert len(titles) == 1
        assert len(descs) == 1
        assert titles[0].attrib.get("id") == "title"
        assert descs[0].attrib.get("id") == "desc"
        assert "".join(titles[0].itertext()).strip()
        assert "".join(descs[0].itertext()).strip()

        visible_text = _visible_text_nodes(root)
        assert not any(node.attrib.get("class") in {"title", "subtitle", "note"} for node in visible_text)
        text_content = "\n".join("".join(node.itertext()) for node in visible_text)
        for banned in BANNED_VISIBLE_CHROME:
            assert banned not in text_content


def test_generated_svgs_are_vertically_cropped_to_visible_content() -> None:
    generator = _load_generator()
    formerly_whitespace_heavy = {
        "fig_02_hazard_exposure_vulnerability_scope.svg",
        "fig_06_bcsd_schematic.svg",
        "fig_18_three_lens_blended_rule.svg",
        "fig_20_district_a_b_lens_example.svg",
    }

    assert len(generator.FIGURES) == 14
    for filename, builder in generator.FIGURES.items():
        root = ET.fromstring(builder())
        _min_x, min_y, _width, height = _view_box(root)
        content_bounds = generator._visible_bounds(root)
        assert content_bounds is not None
        assert float(root.attrib["height"]) == height
        assert height < 760
        if filename in formerly_whitespace_heavy:
            assert min_y > 0

        background = _background_rect(root)
        assert float(background.attrib["y"]) == min_y
        assert float(background.attrib["height"]) == height

        if min_y > 0:
            assert abs(content_bounds.top - min_y - generator.CROP_PADDING_PX) <= 1.0
        bottom_padding = min_y + height - content_bounds.bottom
        assert 0.0 <= bottom_padding <= generator.CROP_PADDING_PX + 1.0


def test_visible_bounds_ignore_generator_background_and_reject_unknown_paths() -> None:
    generator = _load_generator()
    svg = generator.Svg(width=160, height=120).wrap(
        [
            '<rect x="0" y="0" width="160" height="120" fill="white" data-bg="canvas"/>',
            '<rect x="20" y="45" width="50" height="20" fill="white" stroke="#52606d"/>',
        ],
        "Bounds test",
        "Background exclusion test.",
    )
    root = ET.fromstring(svg)
    bounds = generator._visible_bounds(root)

    assert bounds is not None
    assert 40 <= bounds.top <= 45
    assert bounds.bottom <= 70

    with pytest.raises(ValueError, match="Unsupported SVG path command"):
        generator.Svg(width=160, height=120).wrap(
            ['<path d="M 10 10 C 20 20 30 20 40 10" stroke="#52606d" fill="none"/>'],
            "Bad path",
            "Unsupported path test.",
        )


def test_math_labels_emit_svg_metadata_and_visible_fallbacks() -> None:
    generator = _load_generator()
    expected_tex = {
        r"\mathrm{Climate\ risk}=f(\mathrm{Hazard},\mathrm{Exposure},\mathrm{Vulnerability})",
        r"x' = F_{\mathrm{obs}}^{-1}(F_{\mathrm{mod}}(x))",
        r"F_{\mathrm{obs}}",
        r"F_{\mathrm{mod}}",
        r"\bar{v}_i = \frac{\sum_j a_{ij}v_j}{\sum_j a_{ij}}",
        r"\tau_d:\ \mathrm{baseline\ 90th\ percentile}",
        r"\mathrm{SPI} = \Phi^{-1}(H(x))",
        r"S_{\mathrm{abs}}",
        r"S_{\mathrm{chg}}",
        r"S_{\mathrm{imp}}",
        r"\mathrm{Blended}=0.40S_{\mathrm{abs}}+0.25S_{\mathrm{chg}}+0.35S_{\mathrm{imp}}",
        r"\mathrm{Impact\ band:}\ \mathrm{TXx}\ 40{-}45^\circ\mathrm{C};\ \mathrm{cohort}\ q_{10}/q_{90}:41{-}46^\circ\mathrm{C}\ \mathrm{and}\ +1.0/+3.5^\circ\mathrm{C}",
    }
    found_tex: set[str] = set()

    for filename, builder in generator.FIGURES.items():
        root = ET.fromstring(builder())
        for group in _math_label_groups(root):
            tex = group.attrib["data-math-tex"]
            found_tex.add(tex)
            assert tex.strip()
            assert group.attrib["data-math-anchor"] in {"start", "middle", "end"}
            assert group.attrib["data-math-display"] in {"0", "1"}
            assert float(group.attrib["data-math-x"]) >= 0
            assert float(group.attrib["data-math-size"]) > 0
            assert "display:none" not in group.attrib.get("style", "")
            assert group.attrib.get("aria-hidden") is None
            fallback = "".join(group.itertext()).strip()
            assert fallback, filename

    assert expected_tex <= found_tex


def test_generator_cli_exports_png_only_when_requested(monkeypatch, tmp_path: Path) -> None:
    generator = _load_generator()
    exports: list[Path] = []

    def tiny_svg() -> str:
        return generator.Svg(width=120, height=100).wrap(
            [generator.text(10, 50, "core label", "small")],
            "Tiny figure",
            "Tiny test figure.",
        )

    monkeypatch.setattr(generator, "FIGURES", {"tiny.svg": tiny_svg})
    monkeypatch.setattr(generator, "OUT_DIR", tmp_path)
    monkeypatch.setattr(generator, "export_png", lambda path: exports.append(path))

    generator.main([])

    assert (tmp_path / "tiny.svg").exists()
    assert exports == []

    generator.main(["--png"])

    assert exports == [tmp_path / "tiny.svg"]
