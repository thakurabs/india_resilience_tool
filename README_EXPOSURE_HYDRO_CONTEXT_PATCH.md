# Exposure Snapshot + Hydrological Context patch bundle

This bundle adds the lightweight data loaders and UI helper modules for the new admin-mode context cards, plus a conservative patch script that wires them into the existing dashboard files.

## Apply

Unzip this folder at the repository root, then run:

```powershell
python tools/patches/apply_exposure_hydro_context_patch.py
```

The patcher creates `.bak_exposure_hydro` backups before editing existing files.

## What it adds

- `india_resilience_tool/data/exposure_summary.py`
- `india_resilience_tool/data/hydro_summary.py`
- `india_resilience_tool/app/summary_cache.py`
- `india_resilience_tool/app/views/context_cards.py`
- `india_resilience_tool/app/hydro_boundary_overlay.py`
- targeted patcher for:
  - `app/state.py`
  - `app/details_runtime.py`
  - `app/runtime.py`
  - `app/left_panel_runtime.py`
  - `app/views/map_view.py`

## Expected runtime context files

Resolved through `optimized_context_path(..., data_dir=data_dir)`:

- `processed_optimised/context/admin_exposure_summary.parquet`
- `processed_optimised/context/admin_hydro_summary.parquet`
- optional: `processed_optimised/context/admin_hydro_overlaps.parquet`

Admin keys use pipe separators, for example:

- district: `alias(state)|alias(district)`
- block: `alias(state)|alias(district)|alias(block)`

## Verify

```powershell
python -m pytest tests/test_exposure_summary.py tests/test_hydro_summary.py -v
python -m pytest -q
```
