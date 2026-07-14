#!/usr/bin/env python3
"""Prepare a versioned JRC RP-100 source manifest scaffold.

This CHG-0231 scaffold is intentionally network-free: it selects expected
India-intersecting RP-100 tiles from a local official tile-extents file or an
explicit tile filename list, records deterministic source inventory metadata,
and writes planned ``source_inventory.json`` / ``source_manifest.json`` files.
Actual HTTP download, per-tile raster validation, VRT creation, and coverage
mask export are CHG-0232 responsibilities.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import geopandas as gpd
from shapely.geometry import box
from shapely.ops import unary_union


DEFAULT_DATASET_VERSION = "2.1.2"
DEFAULT_BASE_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/"
NATIVE_PIXEL_DEGREES = 1.0 / 1200.0
SOURCE_MANIFEST_SCHEMA_VERSION = 1
SOURCE_INVENTORY_SCHEMA_VERSION = 1
_TILE_TOKEN_RE = re.compile(r"(?P<lat_hemi>[NS])(?P<lat>\d{1,2})(?P<lon_hemi>[EW])(?P<lon>\d{1,3})", re.IGNORECASE)


@dataclass(frozen=True)
class TileFootprint:
    """One candidate JRC tile footprint in WGS84 coordinates."""

    tile_id: str
    filename: str
    bounds: tuple[float, float, float, float]
    geometry_wkt: str
    source: str

    @property
    def url_path(self) -> str:
        return f"RP100/{self.filename}"


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_base_url(base_url: str) -> str:
    stripped = str(base_url or "").strip()
    if not stripped:
        raise ValueError("--base-url must not be empty.")
    return stripped.rstrip("/") + "/"


def _url_join(base_url: str, relative_path: str) -> str:
    return _canonical_base_url(base_url) + relative_path.lstrip("/")


def _tile_token(raw: object) -> str:
    text = str(raw or "").strip()
    match = _TILE_TOKEN_RE.search(text)
    if not match:
        raise ValueError(f"Could not derive JRC 10-degree tile token from {text!r}.")
    lat_hemi = match.group("lat_hemi").upper()
    lon_hemi = match.group("lon_hemi").upper()
    lat = int(match.group("lat"))
    lon = int(match.group("lon"))
    return f"{lat_hemi}{lat:02d}{lon_hemi}{lon:03d}"


def _signed_from_hemi(hemi: str, value: int) -> int:
    return -value if hemi.upper() in {"S", "W"} else value


def fallback_footprint_from_filename(filename: str) -> TileFootprint:
    """Derive a nominal 10-degree footprint from a JRC tile filename."""
    tile_id = _tile_token(filename)
    match = _TILE_TOKEN_RE.search(tile_id)
    if match is None:
        raise ValueError(f"Could not parse tile token: {tile_id}")
    south = _signed_from_hemi(match.group("lat_hemi"), int(match.group("lat")))
    west = _signed_from_hemi(match.group("lon_hemi"), int(match.group("lon")))
    north = south + 10
    east = west + 10
    geom = box(west, south, east, north)
    return TileFootprint(
        tile_id=tile_id,
        filename=Path(str(filename)).name,
        bounds=(float(west), float(south), float(east), float(north)),
        geometry_wkt=geom.wkt,
        source="filename_fallback",
    )


def _feature_filename(row: object) -> str:
    for attr in ("filename", "file_name", "name", "tile", "tile_name", "path", "url", "href"):
        value = getattr(row, attr, None)
        if value is not None and str(value).strip():
            return Path(str(value).strip()).name
    raise ValueError("Tile extents feature is missing a filename/name/path property.")


def load_tile_extents(path: Path) -> list[TileFootprint]:
    """Load official tile extents from a local GeoJSON/vector file."""
    gdf = gpd.read_file(path)
    if gdf.empty:
        raise ValueError(f"Tile extents file contains no features: {path}")
    if gdf.crs is None:
        raise ValueError(f"Tile extents file must declare a CRS: {path}")
    gdf = gdf.to_crs("EPSG:4326")
    footprints: list[TileFootprint] = []
    for row in gdf.itertuples(index=False):
        filename = _feature_filename(row)
        tile_id = _tile_token(filename)
        geom = row.geometry
        if geom is None or geom.is_empty:
            raise ValueError(f"Tile {tile_id} has empty geometry in {path}")
        minx, miny, maxx, maxy = geom.bounds
        footprints.append(
            TileFootprint(
                tile_id=tile_id,
                filename=filename,
                bounds=(float(minx), float(miny), float(maxx), float(maxy)),
                geometry_wkt=geom.wkt,
                source="tile_extents",
            )
        )
    return _dedupe_footprints(footprints)


def load_fallback_filenames(values: Iterable[str]) -> list[TileFootprint]:
    """Build fallback tile footprints from an explicit iterable of filenames."""
    filenames = [str(value).strip() for value in values if str(value).strip()]
    if not filenames:
        raise ValueError("At least one --tile-filename or --tile-list-path entry is required for fallback mode.")
    return _dedupe_footprints([fallback_footprint_from_filename(name) for name in filenames])


def _dedupe_footprints(footprints: list[TileFootprint]) -> list[TileFootprint]:
    seen: dict[str, TileFootprint] = {}
    duplicates: set[str] = set()
    for tile in footprints:
        if tile.tile_id in seen:
            duplicates.add(tile.tile_id)
        seen[tile.tile_id] = tile
    if duplicates:
        raise ValueError(f"Duplicate JRC tile IDs in candidate set: {sorted(duplicates)}")
    return sorted(seen.values(), key=lambda tile: tile.tile_id)


def load_boundary_union(boundary_path: Path):
    """Load and union the canonical admin boundary in EPSG:4326."""
    gdf = gpd.read_file(boundary_path)
    if gdf.empty:
        raise ValueError(f"Boundary file contains no features: {boundary_path}")
    if gdf.crs is None:
        raise ValueError(f"Boundary file must declare a CRS: {boundary_path}")
    gdf = gdf.to_crs("EPSG:4326")
    union = unary_union([geom for geom in gdf.geometry if geom is not None and not geom.is_empty])
    if union.is_empty:
        raise ValueError(f"Boundary file has no non-empty geometries: {boundary_path}")
    return union


def select_intersecting_tiles(
    *,
    boundary_path: Path,
    tile_footprints: list[TileFootprint],
    selection_buffer_degrees: float = NATIVE_PIXEL_DEGREES,
) -> list[TileFootprint]:
    """Select tiles intersecting the buffered canonical boundary union."""
    boundary = load_boundary_union(boundary_path).buffer(selection_buffer_degrees)
    selected = [
        tile
        for tile in tile_footprints
        if box(*tile.bounds).intersects(boundary)
    ]
    return sorted(selected, key=lambda tile: tile.tile_id)


def _tile_record(tile: TileFootprint, *, base_url: str) -> dict[str, object]:
    west, south, east, north = tile.bounds
    return {
        "tile_id": tile.tile_id,
        "filename": tile.filename,
        "url": _url_join(base_url, tile.url_path),
        "bounds": [west, south, east, north],
        "footprint_source": tile.source,
    }


def build_inventory(
    *,
    dataset_version: str,
    base_url: str,
    boundary_path: Path,
    tile_footprints: list[TileFootprint],
    selected_tiles: list[TileFootprint],
    tile_extents_path: Optional[Path],
    selection_buffer_degrees: float = NATIVE_PIXEL_DEGREES,
) -> dict[str, object]:
    """Build the deterministic source inventory JSON payload."""
    boundary_sha256 = _sha256_file(boundary_path)
    expected_tile_ids = [tile.tile_id for tile in selected_tiles]
    source_mode = "tile_extents" if tile_extents_path is not None else "filename_fallback"
    return {
        "schema_version": SOURCE_INVENTORY_SCHEMA_VERSION,
        "dataset_version": dataset_version,
        "base_url": _canonical_base_url(base_url),
        "official_urls": {
            "root": _canonical_base_url(base_url),
            "readme": _url_join(base_url, "README.txt"),
            "changelog": _url_join(base_url, "CHANGELOG.txt"),
            "tile_extents": _url_join(base_url, "tile_extents.geojson"),
            "rp100_directory": _url_join(base_url, "RP100/"),
        },
        "source_mode": source_mode,
        "tile_extents_path": str(tile_extents_path) if tile_extents_path else "",
        "fallback_footprint_validation_required": source_mode == "filename_fallback",
        "boundary_path": str(boundary_path),
        "boundary_sha256": boundary_sha256,
        "selection_buffer_degrees": selection_buffer_degrees,
        "selection_method": "intersects_boundary_union_buffered_by_one_native_pixel",
        "candidate_tile_count": len(tile_footprints),
        "expected_tile_count": len(selected_tiles),
        "expected_tile_ids": expected_tile_ids,
        "selected_tiles": [_tile_record(tile, base_url=base_url) for tile in selected_tiles],
    }


def build_source_manifest(
    *,
    inventory: dict[str, object],
    acquisition_timestamp_utc: str,
) -> dict[str, object]:
    """Build a planned source_manifest payload consumed by later CHG-0232 work."""
    expected_tile_ids = list(inventory["expected_tile_ids"])
    return {
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "acquisition_status": "planned",
        "download_implemented": False,
        "dataset_version": inventory["dataset_version"],
        "base_url": inventory["base_url"],
        "official_urls": inventory["official_urls"],
        "rp100_depth_vrt": "RP100_depth.vrt",
        "rp100_tile_coverage_vrt": "RP100_tile_coverage.vrt",
        "source_inventory": "source_inventory.json",
        "expected_tile_ids": expected_tile_ids,
        "acquired_tile_ids": [],
        "validated_tile_ids": [],
        "rejected_tile_ids": [],
        "tiles": [],
        "boundary_path": inventory["boundary_path"],
        "boundary_sha256": inventory["boundary_sha256"],
        "selection_buffer_degrees": inventory["selection_buffer_degrees"],
        "selection_method": inventory["selection_method"],
        "upstream_checksum_available": False,
        "integrity_basis": [
            "https_content_length",
            "local_sha256",
            "full_blockwise_raster_read",
            "header_and_grid_contract",
        ],
        "residual_integrity_caveat": (
            "CHG-0231 scaffold only. No upstream checksum or local raster validation "
            "has been performed; CHG-0232 must replace this planned manifest after download."
        ),
        "acquisition_timestamp_utc": acquisition_timestamp_utc,
        "tool": "tools.data_acquisition.prepare_jrc_rp100_source",
    }


def _read_tile_list(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare a planned, versioned JRC RP-100 source inventory/manifest. "
            "This CHG-0231 scaffold performs tile selection only; it does not download rasters."
        )
    )
    parser.add_argument("--dataset-version", default=DEFAULT_DATASET_VERSION)
    parser.add_argument("--boundary-path", required=True, help="Canonical district/admin boundary used for India tile selection.")
    parser.add_argument("--output-dir", required=True, help="Versioned source directory for planned inventory/manifest outputs.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Official JRC flood-hazard root URL.")
    parser.add_argument("--tile-extents-path", default=None, help="Local official tile_extents.geojson/vector file.")
    parser.add_argument("--tile-filename", action="append", default=[], help="Fallback tile filename; may be repeated.")
    parser.add_argument("--tile-list-path", default=None, help="Text file containing fallback tile filenames, one per line.")
    parser.add_argument("--workers", type=int, default=1, help="Reserved for CHG-0232 downloads; accepted for CLI compatibility.")
    parser.add_argument("--resume", action="store_true", help="Reserved for CHG-0232 downloads; accepted for CLI compatibility.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing source_inventory/source_manifest files.")
    parser.add_argument("--dry-run", action="store_true", help="Select tiles and print a summary without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    if int(args.workers) < 1:
        raise SystemExit("--workers must be >= 1.")

    boundary_path = Path(args.boundary_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    tile_extents_path = Path(args.tile_extents_path).expanduser().resolve() if args.tile_extents_path else None

    if tile_extents_path is not None:
        tile_footprints = load_tile_extents(tile_extents_path)
    else:
        filenames = list(args.tile_filename or [])
        if args.tile_list_path:
            filenames.extend(_read_tile_list(Path(args.tile_list_path).expanduser().resolve()))
        tile_footprints = load_fallback_filenames(filenames)

    selected_tiles = select_intersecting_tiles(
        boundary_path=boundary_path,
        tile_footprints=tile_footprints,
    )
    if not selected_tiles:
        raise SystemExit("No JRC RP-100 tiles intersect the buffered boundary.")

    inventory = build_inventory(
        dataset_version=str(args.dataset_version),
        base_url=str(args.base_url),
        boundary_path=boundary_path,
        tile_footprints=tile_footprints,
        selected_tiles=selected_tiles,
        tile_extents_path=tile_extents_path,
    )
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    manifest = build_source_manifest(inventory=inventory, acquisition_timestamp_utc=now)

    inventory_path = output_dir / "source_inventory.json"
    manifest_path = output_dir / "source_manifest.json"
    print("JRC RP-100 SOURCE PREP")
    print(f"dataset_version: {inventory['dataset_version']}")
    print(f"boundary_path: {boundary_path}")
    print(f"source_mode: {inventory['source_mode']}")
    print(f"candidate_tile_count: {inventory['candidate_tile_count']}")
    print(f"expected_tile_count: {inventory['expected_tile_count']}")
    print(f"expected_tile_ids: {', '.join(inventory['expected_tile_ids'])}")
    if args.dry_run:
        print("dry_run: True")
        return 0

    existing = [path for path in (inventory_path, manifest_path) if path.exists()]
    if existing and not args.overwrite:
        raise FileExistsError(
            "Refusing to overwrite existing JRC source prep outputs without --overwrite: "
            + ", ".join(str(path) for path in existing)
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"source_inventory: {inventory_path}")
    print(f"source_manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
