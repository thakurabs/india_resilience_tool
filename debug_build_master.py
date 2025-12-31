#!/usr/bin/env python3
"""
Debug script to verify the clean districts/blocks folder structure.

Usage:
    python debug_folder_structure.py D:/projects/irt_data/processed/tas_gt32 Telangana

Author: Debug helper for IRT
"""

import sys
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print("Usage: python debug_folder_structure.py <output_root> <state>")
        print("Example: python debug_folder_structure.py D:/projects/irt_data/processed/tas_gt32 Telangana")
        sys.exit(1)
    
    output_root = Path(sys.argv[1])
    state = sys.argv[2]
    
    print("=" * 60)
    print("DEBUG: Clean Folder Structure Verification")
    print("=" * 60)
    print(f"Output root: {output_root}")
    print(f"State: {state}")
    print()
    
    state_root = output_root / state
    
    if not state_root.exists():
        print(f"ERROR: State root does not exist: {state_root}")
        sys.exit(1)
    
    # Check for districts/ folder
    districts_path = state_root / "districts"
    blocks_path = state_root / "blocks"
    
    print("[1] Checking folder structure...")
    print(f"  districts/ folder: {'EXISTS' if districts_path.exists() else 'NOT FOUND'}")
    print(f"  blocks/ folder: {'EXISTS' if blocks_path.exists() else 'NOT FOUND'}")
    print()
    
    # Check districts folder
    if districts_path.exists():
        print("[2] District-level data (districts/):")
        district_dirs = [p for p in districts_path.iterdir() if p.is_dir() and p.name != "ensembles"]
        print(f"  Districts found: {len(district_dirs)}")
        
        # Check structure of first district
        if district_dirs:
            sample = district_dirs[0]
            print(f"  Sample district: {sample.name}")
            model_dirs = [p for p in sample.iterdir() if p.is_dir()]
            print(f"  Models in sample: {[m.name for m in model_dirs[:3]]}...")
            
            # Count files
            yearly_files = list(districts_path.glob("*/*/*_yearly.csv"))
            periods_files = list(districts_path.glob("*/*/*_periods.csv"))
            print(f"  Total _yearly.csv files: {len(yearly_files)}")
            print(f"  Total _periods.csv files: {len(periods_files)}")
        
        # Check ensembles
        ensembles_path = districts_path / "ensembles"
        if ensembles_path.exists():
            ensemble_files = list(ensembles_path.glob("*/*/*.csv"))
            print(f"  Ensemble files: {len(ensemble_files)}")
        print()
    
    # Check blocks folder
    if blocks_path.exists():
        print("[3] Block-level data (blocks/):")
        district_dirs = [p for p in blocks_path.iterdir() if p.is_dir() and p.name != "ensembles"]
        print(f"  Districts found: {len(district_dirs)}")
        
        # Count blocks
        total_blocks = 0
        for ddir in district_dirs:
            block_dirs = [p for p in ddir.iterdir() if p.is_dir()]
            total_blocks += len(block_dirs)
        print(f"  Total blocks: {total_blocks}")
        
        # Check structure
        if district_dirs:
            sample_district = district_dirs[0]
            sample_blocks = [p for p in sample_district.iterdir() if p.is_dir()]
            if sample_blocks:
                sample_block = sample_blocks[0]
                print(f"  Sample path: blocks/{sample_district.name}/{sample_block.name}/")
                model_dirs = [p for p in sample_block.iterdir() if p.is_dir()]
                print(f"  Models in sample block: {[m.name for m in model_dirs[:3]]}...")
        
        # Count files
        yearly_files = list(blocks_path.glob("*/*/*/*_yearly.csv"))
        periods_files = list(blocks_path.glob("*/*/*/*_periods.csv"))
        print(f"  Total _yearly.csv files: {len(yearly_files)}")
        print(f"  Total _periods.csv files: {len(periods_files)}")
        
        # Check ensembles
        ensembles_path = blocks_path / "ensembles"
        if ensembles_path.exists():
            ensemble_files = list(ensembles_path.glob("*/*/*.csv"))
            print(f"  Ensemble files: {len(ensemble_files)}")
        print()
    
    # Check for OLD mixed structure (problem indicator)
    print("[4] Checking for OLD mixed structure (should be empty)...")
    old_style_models = ["ACCESS-CM2", "ACCESS-ESM1-5", "BCC-CSM2-MR", "CanESM5", "CMCC-CM2-SR5"]
    found_old = []
    for item in state_root.iterdir():
        if item.is_dir() and item.name in old_style_models:
            found_old.append(item.name)
    
    if found_old:
        print(f"  WARNING: Found old-style model directories at state level!")
        print(f"  These should be cleaned up: {found_old[:5]}...")
        print(f"  Run: rm -rf {state_root}/ACCESS-CM2 {state_root}/ACCESS-ESM1-5 ...")
    else:
        print(f"  OK - No old-style model directories found at state level")
    print()
    
    # Check master CSVs
    print("[5] Master CSV files...")
    district_master = state_root / "master_metrics_by_district.csv"
    block_master = state_root / "master_metrics_by_block.csv"
    print(f"  master_metrics_by_district.csv: {'EXISTS' if district_master.exists() else 'NOT FOUND'}")
    print(f"  master_metrics_by_block.csv: {'EXISTS' if block_master.exists() else 'NOT FOUND'}")
    
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    
    if districts_path.exists() and blocks_path.exists():
        print("✓ Clean folder structure detected!")
        print(f"  - districts/ contains district-level data")
        print(f"  - blocks/ contains block-level data")
    elif blocks_path.exists():
        print("✓ Block-level data exists in clean structure")
        print("  - Run compute for --level district if needed")
    elif districts_path.exists():
        print("✓ District-level data exists in clean structure")
        print("  - Run compute for --level block if needed")
    else:
        print("✗ Clean folder structure not found")
        print("  - Run compute_indices_multiprocess.py with new version")
    
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
