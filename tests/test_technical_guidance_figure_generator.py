"""Tests for the generated Technical Guidance Note SVG figures."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from xml.etree import ElementTree as ET


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
