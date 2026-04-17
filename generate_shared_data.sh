#!/usr/bin/env bash

set -euo pipefail

SRC_DATA="/mnt/d/projects/irt_data"
SRC_REPO="/mnt/d/projects/india_resilience_tool"
DEST="/mnt/d/projects/shared_data"
STATE="TELANGANA"
SLUG="tas_gt32"

if [ -e "$DEST" ]; then
  echo "Destination already exists: $DEST"
  echo "Rename or remove it first, then rerun."
  exit 1
fi

mkdir -p \
  "$DEST/processed/$SLUG/$STATE" \
  "$DEST/repo_docs"

cp -v "$SRC_DATA/districts_4326.geojson" "$DEST/"
cp -v "$SRC_DATA/blocks_4326.geojson" "$DEST/"

cp -v "$SRC_REPO/environment.yml" "$DEST/repo_docs/"
cp -v "$SRC_REPO/README.md" "$DEST/repo_docs/"
cp -v "$SRC_REPO/MANIFEST.md" "$DEST/repo_docs/"

cp -v "$SRC_DATA/processed/$SLUG/$STATE/master_metrics_by_district.csv" "$DEST/processed/$SLUG/$STATE/"
cp -v "$SRC_DATA/processed/$SLUG/$STATE/master_metrics_by_block.csv" "$DEST/processed/$SLUG/$STATE/"

find "$SRC_DATA/processed/$SLUG/$STATE" -maxdepth 1 -type f \
  \( -name 'state_*_district.csv' -o -name 'state_*_block.csv' \) \
  -exec cp -v {} "$DEST/processed/$SLUG/$STATE/" \;

find "$SRC_DATA/processed/$SLUG/$STATE" -maxdepth 1 -type f \
  \( -name 'state_ensemble_stats.csv' -o -name 'state_model_averages.csv' -o -name 'state_yearly_ensemble_stats.csv' -o -name 'state_yearly_model_averages.csv' \) \
  -exec cp -v {} "$DEST/processed/$SLUG/$STATE/" \;

cp -avr "$SRC_DATA/processed/$SLUG/$STATE/districts" "$DEST/processed/$SLUG/$STATE/"
cp -avr "$SRC_DATA/processed/$SLUG/$STATE/blocks" "$DEST/processed/$SLUG/$STATE/"

cat > "$DEST/README_FOR_VENDOR.txt" <<'EOF'
Sample data bundle for India Resilience Tool

Included sample:
- Metric slug: tas_gt32
- State: TELANGANA

Expected runtime setup:
1. Set IRT_DATA_DIR to this shared_data folder
2. Set IRT_PILOT_STATE=TELANGANA
3. Run: streamlit run main.py

Notes:
- This bundle includes only one metric slug, so other metrics in the ribbon will not work.
- If level-specific state summary CSVs are missing, the app may rebuild them from the included processed district/block files on first run.
EOF

echo
echo "Shared bundle created at: $DEST"
