#!/usr/bin/env python3
"""Build the canonical block ↔ sub-basin crosswalk CSV for IRT."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from india_resilience_tool.data.crosswalks import ensure_block_subbasin_crosswalk
from paths import BLOCKS_PATH, BLOCK_SUBBASIN_CROSSWALK_PATH, SUBBASINS_PATH
from tools.geodata.build_district_subbasin_crosswalk import (
    build_block_subbasin_crosswalk,
    load_block_boundaries,
    load_hydro_boundaries,
    _run_build,
)


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Build the canonical block-subbasin crosswalk CSV for IRT."
    )
    parser.add_argument("--blocks", type=str, default=str(BLOCKS_PATH))
    parser.add_argument("--subbasins", type=str, default=str(SUBBASINS_PATH))
    parser.add_argument("--out", type=str, default=str(BLOCK_SUBBASIN_CROSSWALK_PATH))
    parser.add_argument("--area-epsg", type=int, default=6933)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    return _run_build(
        title="BLOCK ↔ SUB-BASIN CROSSWALK",
        admin_path=Path(args.blocks).expanduser().resolve(),
        hydro_path=Path(args.subbasins).expanduser().resolve(),
        out_path=Path(args.out).expanduser().resolve(),
        area_epsg=int(args.area_epsg),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        load_admin_fn=load_block_boundaries,
        load_hydro_fn=lambda path: load_hydro_boundaries(path, level="sub_basin"),
        build_fn=build_block_subbasin_crosswalk,
        ensure_fn=ensure_block_subbasin_crosswalk,
        admin_level="block",
        hydro_level="sub_basin",
    )


if __name__ == "__main__":
    raise SystemExit(main())
