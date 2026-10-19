#!/usr/bin/env python3
"""Prepare and finalize a versioned JRC RP-100 source manifest.

The default path is network-free: it selects expected India-intersecting RP-100
tiles from a local official tile-extents file or an explicit tile filename
list, records deterministic source inventory metadata, and writes planned
``source_inventory.json`` / ``source_manifest.json`` files.

With ``--finalize``, the tool validates already-downloaded official RP-100 TIFFs,
builds aligned depth and tile-coverage VRTs, and replaces the planned manifest
with a validated manifest suitable for strict downstream builders.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
import xml.etree.ElementTree as ET

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import box
from shapely.ops import unary_union


DEFAULT_DATASET_VERSION = "2.1.2"
DEFAULT_BASE_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/"
NATIVE_PIXEL_DEGREES = 1.0 / 1200.0
SOURCE_MANIFEST_SCHEMA_VERSION = 1
SOURCE_INVENTORY_SCHEMA_VERSION = 1
DEPTH_VRT_NAME = "RP100_depth.vrt"
COVERAGE_VRT_NAME = "RP100_tile_coverage.vrt"
COVERAGE_TILE_DIR_NAME = "RP100_tile_coverage"
DEPTH_NODATA = -9999.0
RASTER_GRID_TOLERANCE_DEGREES = 1e-7
RASTER_RESOLUTION_TOLERANCE_DEGREES = 1e-11
_TILE_TOKEN_RE = re.compile(r"(?P<lat_hemi>[NS])(?P<lat>\d{1,2})_?(?P<lon_hemi>[EW])(?P<lon>\d{1,3})", re.IGNORECASE)


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


@dataclass(frozen=True)
class ValidatedTile:
    """Validated local raster metadata for one selected RP-100 tile."""

    tile_id: str
    filename: str
    relative_path: str
    coverage_relative_path: str
    sha256: str
    size_bytes: int
    width: int
    height: int
    bounds: tuple[float, float, float, float]
    transform: tuple[float, float, float, float, float, float]
    crs: str
    resolution: tuple[float, float]
    nodata: float
    dtype: str
    block_count: int
    min_value: float | None
    max_value: float | None


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


def _official_tile_filename(tile_name: str, tile_id_value: object) -> str:
    """Return the official RP-100 depth filename for a tile-extents feature."""
    try:
        numeric_id = int(tile_id_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Tile extents feature has invalid id for tile {tile_name!r}: {tile_id_value!r}") from exc
    name = str(tile_name or "").strip()
    if not name:
        raise ValueError("Tile extents feature has an empty tile name.")
    return f"ID{numeric_id}_{name}_RP100_depth.tif"


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
    filename = getattr(row, "filename", None)
    if filename is not None and str(filename).strip():
        return Path(str(filename).strip()).name

    name = getattr(row, "name", None)
    tile_id_value = getattr(row, "id", None)
    if name is not None and str(name).strip() and tile_id_value is not None:
        return _official_tile_filename(str(name).strip(), tile_id_value)

    for attr in ("file_name", "tile", "tile_name", "path", "url", "href"):
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


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON file is not valid: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _selected_tiles_from_inventory(inventory: dict[str, object]) -> list[dict[str, object]]:
    raw_tiles = inventory.get("selected_tiles")
    if not isinstance(raw_tiles, list) or not raw_tiles:
        raise ValueError("source_inventory.json must contain a non-empty selected_tiles list.")
    tiles: list[dict[str, object]] = []
    for raw_tile in raw_tiles:
        if not isinstance(raw_tile, dict):
            raise ValueError("Every selected_tiles entry must be a JSON object.")
        for field in ("tile_id", "filename", "bounds"):
            if field not in raw_tile:
                raise ValueError(f"selected_tiles entry is missing required field {field!r}.")
        bounds = raw_tile["bounds"]
        if not isinstance(bounds, list) or len(bounds) != 4:
            raise ValueError(f"Tile {raw_tile.get('tile_id')!r} has invalid bounds.")
        tiles.append(raw_tile)
    return tiles


def _expected_tile_ids(inventory: dict[str, object]) -> list[str]:
    raw_ids = inventory.get("expected_tile_ids")
    if not isinstance(raw_ids, list) or not raw_ids:
        raise ValueError("source_inventory.json must contain a non-empty expected_tile_ids list.")
    return [str(tile_id) for tile_id in raw_ids]


def _relative_to(base_dir: Path, path: Path) -> str:
    return path.resolve().relative_to(base_dir.resolve()).as_posix()


def _assert_close_tuple(
    observed: Iterable[float],
    expected: Iterable[float],
    *,
    label: str,
    tolerance: float = RASTER_GRID_TOLERANCE_DEGREES,
) -> None:
    obs = tuple(float(value) for value in observed)
    exp = tuple(float(value) for value in expected)
    if len(obs) != len(exp) or any(abs(left - right) > tolerance for left, right in zip(obs, exp)):
        raise ValueError(f"{label} mismatch: observed={obs!r}, expected={exp!r}.")


def _blockwise_value_range(src: rasterio.io.DatasetReader) -> tuple[int, float | None, float | None]:
    """Force a full blockwise read and return finite min/max values."""
    block_count = 0
    min_value: float | None = None
    max_value: float | None = None
    for _, window in src.block_windows(1):
        block_count += 1
        data = src.read(1, window=window, masked=False)
        finite = data[np.isfinite(data)]
        if finite.size:
            current_min = float(finite.min())
            current_max = float(finite.max())
            min_value = current_min if min_value is None else min(min_value, current_min)
            max_value = current_max if max_value is None else max(max_value, current_max)
    return block_count, min_value, max_value


def _validate_depth_raster(path: Path, tile: dict[str, object], *, output_dir: Path) -> ValidatedTile:
    tile_id = str(tile["tile_id"])
    expected_bounds = tuple(float(value) for value in tile["bounds"])
    if not path.exists():
        raise FileNotFoundError(f"Expected JRC RP-100 tile is missing for {tile_id}: {path}")
    with rasterio.open(path) as src:
        if src.count != 1:
            raise ValueError(f"JRC RP-100 tile must be single-band: {path}")
        if src.crs is None:
            raise ValueError(f"JRC RP-100 tile must declare CRS: {path}")
        crs = src.crs.to_string()
        if crs != "EPSG:4326":
            raise ValueError(f"JRC RP-100 tile {tile_id} must be EPSG:4326, got {crs}.")
        if (
            abs(float(src.res[0]) - NATIVE_PIXEL_DEGREES) > RASTER_RESOLUTION_TOLERANCE_DEGREES
            or abs(float(src.res[1]) - NATIVE_PIXEL_DEGREES) > RASTER_RESOLUTION_TOLERANCE_DEGREES
        ):
            raise ValueError(
                f"JRC RP-100 tile {tile_id} must be 3 arc-second resolution "
                f"({NATIVE_PIXEL_DEGREES} degrees), got {src.res}."
            )
        if src.nodata is None or not np.isclose(float(src.nodata), DEPTH_NODATA):
            raise ValueError(f"JRC RP-100 tile {tile_id} nodata must be {DEPTH_NODATA:g}, got {src.nodata!r}.")
        observed_bounds = (float(src.bounds.left), float(src.bounds.bottom), float(src.bounds.right), float(src.bounds.top))
        _assert_close_tuple(observed_bounds, expected_bounds, label=f"Tile {tile_id} raster bounds")
        block_count, min_value, max_value = _blockwise_value_range(src)
        transform = (src.transform.c, src.transform.a, src.transform.b, src.transform.f, src.transform.d, src.transform.e)
        return ValidatedTile(
            tile_id=tile_id,
            filename=path.name,
            relative_path=_relative_to(output_dir, path),
            coverage_relative_path="",
            sha256=_sha256_file(path),
            size_bytes=path.stat().st_size,
            width=int(src.width),
            height=int(src.height),
            bounds=observed_bounds,
            transform=tuple(float(value) for value in transform),
            crs=crs,
            resolution=(float(src.res[0]), float(src.res[1])),
            nodata=float(src.nodata),
            dtype=str(src.dtypes[0]),
            block_count=block_count,
            min_value=min_value,
            max_value=max_value,
        )


def _write_coverage_tile(depth_path: Path, coverage_path: Path, *, overwrite: bool) -> None:
    if coverage_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing coverage tile without --overwrite: {coverage_path}")
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(depth_path) as src:
        profile = src.profile.copy()
        tile_large_enough = src.width >= 16 and src.height >= 16
        profile.update(
            driver="GTiff",
            dtype="uint8",
            nodata=0,
            compress="DEFLATE",
            predictor=2,
            tiled=tile_large_enough,
            count=1,
        )
        with rasterio.open(coverage_path, "w", **profile) as dst:
            for _, window in dst.block_windows(1):
                shape = (int(window.height), int(window.width))
                dst.write(np.ones(shape, dtype=np.uint8), 1, window=window)


def _with_coverage(tile: ValidatedTile, coverage_path: Path, *, output_dir: Path) -> ValidatedTile:
    return ValidatedTile(
        tile_id=tile.tile_id,
        filename=tile.filename,
        relative_path=tile.relative_path,
        coverage_relative_path=_relative_to(output_dir, coverage_path),
        sha256=tile.sha256,
        size_bytes=tile.size_bytes,
        width=tile.width,
        height=tile.height,
        bounds=tile.bounds,
        transform=tile.transform,
        crs=tile.crs,
        resolution=tile.resolution,
        nodata=tile.nodata,
        dtype=tile.dtype,
        block_count=tile.block_count,
        min_value=tile.min_value,
        max_value=tile.max_value,
    )


def _vrt_grid(tiles: list[ValidatedTile]) -> tuple[float, float, int, int]:
    west = min(tile.bounds[0] for tile in tiles)
    north = max(tile.bounds[3] for tile in tiles)
    width = 0
    height = 0
    for tile in tiles:
        x_off = int(round((tile.bounds[0] - west) / NATIVE_PIXEL_DEGREES))
        y_off = int(round((north - tile.bounds[3]) / NATIVE_PIXEL_DEGREES))
        if x_off < 0 or y_off < 0:
            raise ValueError(f"Validated tile {tile.tile_id} has a negative VRT offset.")
        width = max(width, x_off + tile.width)
        height = max(height, y_off + tile.height)
    if width <= 0 or height <= 0:
        raise ValueError("Validated tile mosaic has invalid dimensions.")
    return west, north, width, height


def _add_vrt_source(
    band: ET.Element,
    *,
    vrt_path: Path,
    tile: ValidatedTile,
    source_relative_path: str,
    data_type: str,
    add_nodata: bool,
    west: float,
    north: float,
) -> None:
    source = ET.SubElement(band, "ComplexSource")
    source_filename = ET.SubElement(source, "SourceFilename", relativeToVRT="1")
    source_filename.text = Path(source_relative_path).as_posix()
    ET.SubElement(source, "SourceBand").text = "1"
    ET.SubElement(
        source,
        "SourceProperties",
        RasterXSize=str(tile.width),
        RasterYSize=str(tile.height),
        DataType=data_type,
        BlockXSize=str(min(tile.width, 512)),
        BlockYSize=str(min(tile.height, 512)),
    )
    ET.SubElement(source, "SrcRect", xOff="0", yOff="0", xSize=str(tile.width), ySize=str(tile.height))
    x_off = int(round((tile.bounds[0] - west) / NATIVE_PIXEL_DEGREES))
    y_off = int(round((north - tile.bounds[3]) / NATIVE_PIXEL_DEGREES))
    ET.SubElement(source, "DstRect", xOff=str(x_off), yOff=str(y_off), xSize=str(tile.width), ySize=str(tile.height))
    if add_nodata:
        ET.SubElement(source, "NODATA").text = f"{DEPTH_NODATA:g}"
    if not (vrt_path.parent / source_relative_path).exists():
        raise FileNotFoundError(f"VRT source path does not exist: {source_relative_path}")


def _write_vrt(
    *,
    vrt_path: Path,
    tiles: list[ValidatedTile],
    source_attr: str,
    data_type: str,
    nodata: float,
    add_source_nodata: bool,
    overwrite: bool,
) -> None:
    if vrt_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing VRT without --overwrite: {vrt_path}")
    west, north, width, height = _vrt_grid(tiles)
    root = ET.Element("VRTDataset", rasterXSize=str(width), rasterYSize=str(height))
    ET.SubElement(root, "SRS", dataAxisToSRSAxisMapping="2,1").text = "EPSG:4326"
    ET.SubElement(root, "GeoTransform").text = (
        f"{west:.15g}, {NATIVE_PIXEL_DEGREES:.15g}, 0, {north:.15g}, 0, {-NATIVE_PIXEL_DEGREES:.15g}"
    )
    band = ET.SubElement(root, "VRTRasterBand", dataType=data_type, band="1")
    ET.SubElement(band, "NoDataValue").text = f"{nodata:g}"
    for tile in tiles:
        source_relative_path = str(getattr(tile, source_attr))
        _add_vrt_source(
            band,
            vrt_path=vrt_path,
            tile=tile,
            source_relative_path=source_relative_path,
            data_type=data_type,
            add_nodata=add_source_nodata,
            west=west,
            north=north,
        )
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    vrt_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(vrt_path, encoding="UTF-8", xml_declaration=True)


def _validate_vrt_pair(depth_vrt: Path, coverage_vrt: Path) -> None:
    with rasterio.open(depth_vrt) as depth, rasterio.open(coverage_vrt) as coverage:
        depth_grid = (depth.crs.to_string(), depth.transform, depth.shape, tuple(depth.bounds), depth.res)
        coverage_grid = (coverage.crs.to_string(), coverage.transform, coverage.shape, tuple(coverage.bounds), coverage.res)
        if depth_grid != coverage_grid:
            raise ValueError("Final RP-100 depth and coverage VRTs do not share the same grid.")
        if depth.count != 1 or coverage.count != 1:
            raise ValueError("Final RP-100 VRTs must be single-band.")
        if depth.nodata is None or not np.isclose(float(depth.nodata), DEPTH_NODATA):
            raise ValueError(f"Final RP-100 depth VRT nodata must be {DEPTH_NODATA:g}.")
        if coverage.nodata is None or not np.isclose(float(coverage.nodata), 0.0):
            raise ValueError("Final RP-100 coverage VRT nodata must be 0.")


def build_validated_source_manifest(
    *,
    inventory: dict[str, object],
    output_dir: Path,
    validated_tiles: list[ValidatedTile],
    acquisition_timestamp_utc: str,
    finalization_timestamp_utc: str,
) -> dict[str, object]:
    """Build the validated strict source manifest consumed by RP-100 builders."""
    expected_tile_ids = _expected_tile_ids(inventory)
    validated_tile_ids = [tile.tile_id for tile in validated_tiles]
    return {
        "schema_version": SOURCE_MANIFEST_SCHEMA_VERSION,
        "acquisition_status": "validated",
        "download_implemented": False,
        "dataset_version": inventory["dataset_version"],
        "base_url": inventory["base_url"],
        "official_urls": inventory["official_urls"],
        "rp100_depth_vrt": DEPTH_VRT_NAME,
        "rp100_tile_coverage_vrt": COVERAGE_VRT_NAME,
        "source_inventory": "source_inventory.json",
        "expected_tile_ids": expected_tile_ids,
        "acquired_tile_ids": validated_tile_ids,
        "validated_tile_ids": validated_tile_ids,
        "rejected_tile_ids": [],
        "tiles": [
            {
                "tile_id": tile.tile_id,
                "filename": tile.filename,
                "path": tile.relative_path,
                "coverage_path": tile.coverage_relative_path,
                "sha256": tile.sha256,
                "size_bytes": tile.size_bytes,
                "bounds": list(tile.bounds),
                "width": tile.width,
                "height": tile.height,
                "crs": tile.crs,
                "resolution_degrees": list(tile.resolution),
                "nodata": tile.nodata,
                "dtype": tile.dtype,
                "blockwise_read_blocks": tile.block_count,
                "min_value": tile.min_value,
                "max_value": tile.max_value,
            }
            for tile in validated_tiles
        ],
        "boundary_path": inventory["boundary_path"],
        "boundary_sha256": inventory["boundary_sha256"],
        "selection_buffer_degrees": inventory["selection_buffer_degrees"],
        "selection_method": inventory["selection_method"],
        "upstream_checksum_available": False,
        "integrity_basis": [
            "filename_coverage_against_source_inventory",
            "local_sha256",
            "full_blockwise_raster_read",
            "header_and_grid_contract",
            "raster_bounds_match_official_tile_extents",
            "explicit_0_1_tile_coverage_vrt",
        ],
        "residual_integrity_caveat": (
            "No upstream checksum was available from the official source at finalization time; "
            "local integrity is pinned by SHA-256, full raster reads, and header/grid validation."
        ),
        "acquisition_timestamp_utc": acquisition_timestamp_utc,
        "finalization_timestamp_utc": finalization_timestamp_utc,
        "source_dir": str(output_dir),
        "tool": "tools.data_acquisition.prepare_jrc_rp100_source",
    }


def finalize_downloaded_source(
    *,
    inventory_path: Path,
    output_dir: Path,
    overwrite: bool,
    acquisition_timestamp_utc: str,
    finalization_timestamp_utc: str,
) -> dict[str, object]:
    """Validate downloaded RP-100 TIFFs, write VRTs, and return a final manifest."""
    inventory = _read_json(inventory_path)
    selected_tiles = _selected_tiles_from_inventory(inventory)
    expected_ids = _expected_tile_ids(inventory)
    selected_ids = [str(tile["tile_id"]) for tile in selected_tiles]
    if selected_ids != expected_ids:
        raise ValueError(
            "source_inventory.json selected_tiles order/coverage does not match expected_tile_ids: "
            f"selected={selected_ids!r}, expected={expected_ids!r}."
        )

    validated_tiles: list[ValidatedTile] = []
    for tile in selected_tiles:
        depth_path = output_dir / "RP100" / str(tile["filename"])
        validated = _validate_depth_raster(depth_path, tile, output_dir=output_dir)
        coverage_path = output_dir / COVERAGE_TILE_DIR_NAME / f"{validated.tile_id}_coverage.tif"
        _write_coverage_tile(depth_path, coverage_path, overwrite=overwrite)
        validated_tiles.append(_with_coverage(validated, coverage_path, output_dir=output_dir))

    depth_vrt = output_dir / DEPTH_VRT_NAME
    coverage_vrt = output_dir / COVERAGE_VRT_NAME
    _write_vrt(
        vrt_path=depth_vrt,
        tiles=validated_tiles,
        source_attr="relative_path",
        data_type="Float32",
        nodata=DEPTH_NODATA,
        add_source_nodata=True,
        overwrite=overwrite,
    )
    _write_vrt(
        vrt_path=coverage_vrt,
        tiles=validated_tiles,
        source_attr="coverage_relative_path",
        data_type="Byte",
        nodata=0.0,
        add_source_nodata=False,
        overwrite=overwrite,
    )
    _validate_vrt_pair(depth_vrt, coverage_vrt)
    return build_validated_source_manifest(
        inventory=inventory,
        output_dir=output_dir,
        validated_tiles=validated_tiles,
        acquisition_timestamp_utc=acquisition_timestamp_utc,
        finalization_timestamp_utc=finalization_timestamp_utc,
    )


def _read_tile_list(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare or finalize a versioned JRC RP-100 source inventory/manifest. "
            "Default mode performs tile selection only; --finalize validates already-downloaded rasters "
            "and builds strict depth/coverage VRTs."
        )
    )
    parser.add_argument("--dataset-version", default=DEFAULT_DATASET_VERSION)
    parser.add_argument("--boundary-path", default=None, help="Canonical district/admin boundary used for India tile selection.")
    parser.add_argument("--output-dir", required=True, help="Versioned source directory for planned inventory/manifest outputs.")
    parser.add_argument("--inventory-path", default=None, help="Existing source_inventory.json to finalize; defaults to <output-dir>/source_inventory.json.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Official JRC flood-hazard root URL.")
    parser.add_argument("--tile-extents-path", default=None, help="Local official tile_extents.geojson/vector file.")
    parser.add_argument("--tile-filename", action="append", default=[], help="Fallback tile filename; may be repeated.")
    parser.add_argument("--tile-list-path", default=None, help="Text file containing fallback tile filenames, one per line.")
    parser.add_argument("--workers", type=int, default=1, help="Reserved for CHG-0232 downloads; accepted for CLI compatibility.")
    parser.add_argument("--resume", action="store_true", help="Reserved for CHG-0232 downloads; accepted for CLI compatibility.")
    parser.add_argument("--finalize", action="store_true", help="Validate downloaded RP-100 tiles and replace the planned manifest with a final manifest.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing source_inventory/source_manifest files.")
    parser.add_argument("--dry-run", action="store_true", help="Select tiles and print a summary without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    if int(args.workers) < 1:
        raise SystemExit("--workers must be >= 1.")

    output_dir = Path(args.output_dir).expanduser().resolve()
    input_inventory_path = (
        Path(args.inventory_path).expanduser().resolve()
        if args.inventory_path
        else output_dir / "source_inventory.json"
    )
    output_inventory_path = output_dir / "source_inventory.json"
    boundary_path = Path(args.boundary_path).expanduser().resolve() if args.boundary_path else None
    tile_extents_path = Path(args.tile_extents_path).expanduser().resolve() if args.tile_extents_path else None

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    inventory: dict[str, object]
    manifest: dict[str, object]
    reused_inventory = False

    if args.finalize and input_inventory_path.exists() and boundary_path is None and tile_extents_path is None and not args.tile_filename and not args.tile_list_path:
        inventory = _read_json(input_inventory_path)
        reused_inventory = True
    else:
        if boundary_path is None:
            raise SystemExit("--boundary-path is required when creating a new source inventory.")
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

    manifest_path = output_dir / "source_manifest.json"
    print("JRC RP-100 SOURCE PREP")
    print(f"dataset_version: {inventory['dataset_version']}")
    print(f"boundary_path: {inventory.get('boundary_path', boundary_path or '')}")
    print(f"source_mode: {inventory['source_mode']}")
    print(f"candidate_tile_count: {inventory['candidate_tile_count']}")
    print(f"expected_tile_count: {inventory['expected_tile_count']}")
    print(f"expected_tile_ids: {', '.join(inventory['expected_tile_ids'])}")
    print(f"finalize: {bool(args.finalize)}")
    if reused_inventory:
        print(f"source_inventory_reused: {input_inventory_path}")
    if args.dry_run:
        print("dry_run: True")
        return 0

    existing_candidates = (manifest_path,) if reused_inventory else (output_inventory_path, manifest_path)
    if reused_inventory and input_inventory_path != output_inventory_path and output_inventory_path.exists():
        existing_candidates = (output_inventory_path, manifest_path)
    existing = [path for path in existing_candidates if path.exists()]
    if existing and not args.overwrite:
        raise FileExistsError(
            "Refusing to overwrite existing JRC source prep outputs without --overwrite: "
            + ", ".join(str(path) for path in existing)
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    if not reused_inventory or input_inventory_path != output_inventory_path:
        output_inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
    if args.finalize:
        acquisition_timestamp = str(inventory.get("acquisition_timestamp_utc") or now)
        manifest = finalize_downloaded_source(
            inventory_path=output_inventory_path,
            output_dir=output_dir,
            overwrite=bool(args.overwrite),
            acquisition_timestamp_utc=acquisition_timestamp,
            finalization_timestamp_utc=now,
        )
    else:
        manifest = build_source_manifest(inventory=inventory, acquisition_timestamp_utc=now)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(f"source_inventory: {output_inventory_path}")
    print(f"source_manifest: {manifest_path}")
    if args.finalize:
        print(f"rp100_depth_vrt: {output_dir / DEPTH_VRT_NAME}")
        print(f"rp100_tile_coverage_vrt: {output_dir / COVERAGE_VRT_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
