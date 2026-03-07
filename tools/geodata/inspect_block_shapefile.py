#!/usr/bin/env python3
"""
Inspect block shapefile and convert to GeoJSON.

This script will:
1. Load your block shapefile
2. Print column names and sample data (for Claude to understand structure)
3. Convert to GeoJSON with optional simplification (to reduce file size)
4. Print summary statistics

Usage:
    python inspect_block_shapefile.py /path/to/your/blocks.shp

Author: Generated for IRT block-level support
"""

import sys
from pathlib import Path

try:
    import geopandas as gpd
    import pandas as pd
except ImportError:
    print("ERROR: Please install geopandas: pip install geopandas")
    sys.exit(1)


def inspect_and_convert(shp_path: str, simplify_tolerance: float = 0.001):
    """
    Inspect shapefile and convert to GeoJSON.
    
    Args:
        shp_path: Path to .shp file
        simplify_tolerance: Simplification tolerance in degrees (0.001 ≈ 100m)
                           Set to 0 to disable simplification
    """
    shp_path = Path(shp_path)
    
    if not shp_path.exists():
        print(f"ERROR: File not found: {shp_path}")
        sys.exit(1)
    
    print("=" * 70)
    print("BLOCK SHAPEFILE INSPECTION REPORT")
    print("=" * 70)
    print(f"\nLoading: {shp_path}")
    
    # Load shapefile
    gdf = gpd.read_file(shp_path)
    
    # Basic info
    print(f"\n{'─' * 70}")
    print("1. BASIC INFO")
    print(f"{'─' * 70}")
    print(f"   Total features (blocks): {len(gdf):,}")
    print(f"   CRS: {gdf.crs}")
    print(f"   Geometry type(s): {gdf.geometry.geom_type.unique().tolist()}")
    print(f"   Bounds: {gdf.total_bounds}")  # [minx, miny, maxx, maxy]
    
    # Column info
    print(f"\n{'─' * 70}")
    print("2. COLUMNS (copy this section for Claude)")
    print(f"{'─' * 70}")
    print(f"\n   Total columns: {len(gdf.columns)}")
    print(f"\n   Column names and types:")
    for col in gdf.columns:
        if col != 'geometry':
            dtype = gdf[col].dtype
            n_unique = gdf[col].nunique()
            print(f"      - {col}: {dtype} ({n_unique:,} unique values)")
    
    # Sample data (first 3 rows, excluding geometry)
    print(f"\n{'─' * 70}")
    print("3. SAMPLE DATA (first 3 rows)")
    print(f"{'─' * 70}")
    sample_cols = [c for c in gdf.columns if c != 'geometry']
    print(gdf[sample_cols].head(3).to_string())
    
    # Look for likely hierarchy columns
    print(f"\n{'─' * 70}")
    print("4. LIKELY HIERARCHY COLUMNS (for Claude)")
    print(f"{'─' * 70}")
    
    # Common patterns for block/tehsil/mandal names
    block_patterns = ['block', 'tehsil', 'mandal', 'taluk', 'taluka', 'subdistrict', 'sub_district', 'adm3']
    district_patterns = ['district', 'dist', 'adm2']
    state_patterns = ['state', 'st_nm', 'state_ut', 'adm1']
    
    def find_matching_cols(patterns):
        matches = []
        for col in gdf.columns:
            col_lower = col.lower()
            for pat in patterns:
                if pat in col_lower:
                    matches.append(col)
                    break
        return matches
    
    block_cols = find_matching_cols(block_patterns)
    district_cols = find_matching_cols(district_patterns)
    state_cols = find_matching_cols(state_patterns)
    
    print(f"\n   Likely BLOCK name columns: {block_cols or 'NOT FOUND - check manually'}")
    print(f"   Likely DISTRICT columns: {district_cols or 'NOT FOUND - check manually'}")
    print(f"   Likely STATE columns: {state_cols or 'NOT FOUND - check manually'}")
    
    # Show unique values for likely columns
    print(f"\n{'─' * 70}")
    print("5. UNIQUE VALUES IN KEY COLUMNS")
    print(f"{'─' * 70}")
    
    for col_list, label in [(state_cols, "STATE"), (district_cols, "DISTRICT")]:
        for col in col_list[:1]:  # Just first match
            unique_vals = gdf[col].dropna().unique()
            n = len(unique_vals)
            print(f"\n   {label} column '{col}' has {n} unique values:")
            if n <= 20:
                for v in sorted(unique_vals):
                    print(f"      - {v}")
            else:
                print(f"      (showing first 10)")
                for v in sorted(unique_vals)[:10]:
                    print(f"      - {v}")
                print(f"      ... and {n - 10} more")
    
    # Blocks per district (if district column found)
    if district_cols:
        dist_col = district_cols[0]
        blocks_per_dist = gdf.groupby(dist_col).size().describe()
        print(f"\n   Blocks per district (using '{dist_col}'):")
        print(f"      Min: {blocks_per_dist['min']:.0f}")
        print(f"      Max: {blocks_per_dist['max']:.0f}")
        print(f"      Mean: {blocks_per_dist['mean']:.1f}")
    
    # Convert to EPSG:4326 if needed
    print(f"\n{'─' * 70}")
    print("6. CONVERSION TO GEOJSON")
    print(f"{'─' * 70}")
    
    if gdf.crs is None:
        print("   WARNING: No CRS defined. Assuming EPSG:4326")
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        print(f"   Converting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")
    else:
        print("   Already in EPSG:4326")
    
    # Simplify if requested
    if simplify_tolerance > 0:
        print(f"   Simplifying geometries (tolerance={simplify_tolerance} degrees)...")
        gdf['geometry'] = gdf['geometry'].simplify(simplify_tolerance, preserve_topology=True)
    
    # Save GeoJSON
    output_path = shp_path.parent / "blocks_4326.geojson"
    print(f"\n   Saving to: {output_path}")
    gdf.to_file(output_path, driver="GeoJSON")
    
    # File size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"   Output size: {size_mb:.1f} MB")
    
    if size_mb > 50:
        print(f"\n   TIP: File is still large. You can reduce size by:")
        print(f"        - Increasing simplify_tolerance (current: {simplify_tolerance})")
        print(f"        - Filtering to specific states")
        print(f"\n   Example for more aggressive simplification:")
        print(f"        python {Path(__file__).name} {shp_path} --simplify 0.005")
    
    # Summary for Claude
    print(f"\n{'=' * 70}")
    print("COPY THE SECTION BELOW AND PASTE IT FOR CLAUDE:")
    print("=" * 70)
    print(f"""
BLOCK SHAPEFILE SUMMARY:
- Total blocks: {len(gdf):,}
- Columns: {[c for c in gdf.columns if c != 'geometry']}

HIERARCHY COLUMNS:
- Block name column: {block_cols[0] if block_cols else 'UNKNOWN - please specify'}
- District column: {district_cols[0] if district_cols else 'UNKNOWN - please specify'}
- State column: {state_cols[0] if state_cols else 'UNKNOWN - please specify'}

SAMPLE ROW:
{gdf[[c for c in gdf.columns if c != 'geometry']].iloc[0].to_dict()}

UNIQUE STATES: {sorted(gdf[state_cols[0]].unique().tolist()) if state_cols else 'UNKNOWN'}
""")
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_block_shapefile.py /path/to/blocks.shp [--simplify TOLERANCE]")
        print("\nExample:")
        print("  python inspect_block_shapefile.py ~/Downloads/blocks.shp")
        print("  python inspect_block_shapefile.py ~/Downloads/blocks.shp --simplify 0.005")
        sys.exit(1)
    
    shp_path = sys.argv[1]
    
    # Parse optional simplify argument
    simplify_tol = 0.001  # Default: ~100m
    if "--simplify" in sys.argv:
        idx = sys.argv.index("--simplify")
        if idx + 1 < len(sys.argv):
            try:
                simplify_tol = float(sys.argv[idx + 1])
            except ValueError:
                print(f"ERROR: Invalid simplify value: {sys.argv[idx + 1]}")
                sys.exit(1)
    
    inspect_and_convert(shp_path, simplify_tolerance=simplify_tol)