"""Apply targeted Exposure Snapshot + Hydrological Context wiring edits.

Run from the repository root after unzipping this patch bundle:

    python tools/patches/apply_exposure_hydro_context_patch.py

The script is intentionally conservative: it only inserts small, identifiable
blocks and writes ``.bak_exposure_hydro`` backups before changing existing files.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKUP_SUFFIX = ".bak_exposure_hydro"


def _path(rel: str) -> Path:
    return ROOT / rel


def _read(rel: str) -> str:
    return _path(rel).read_text(encoding="utf-8")


def _write(rel: str, text: str) -> None:
    p = _path(rel)
    backup = p.with_name(p.name + BACKUP_SUFFIX)
    if not backup.exists() and p.exists():
        backup.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
    p.write_text(text, encoding="utf-8")


def _ensure_contains(rel: str, needle: str, label: str) -> None:
    if needle not in _read(rel):
        raise RuntimeError(f"Could not find {label} in {rel}")


def patch_state() -> None:
    rel = "india_resilience_tool/app/state.py"
    text = _read(rel)
    if '"active_hydro_boundary_overlay"' in text:
        print(f"{rel}: active_hydro_boundary_overlay already present")
        return
    needle = '    "right_panel_collapsed": False,\n'
    _ensure_contains(rel, needle, "right_panel_collapsed default")
    text = text.replace(
        needle,
        needle + '    "active_hydro_boundary_overlay": None,\n',
        1,
    )
    _write(rel, text)
    print(f"{rel}: added active_hydro_boundary_overlay default")


def patch_left_panel_runtime() -> None:
    rel = "india_resilience_tool/app/left_panel_runtime.py"
    text = _read(rel)
    if "map_key_suffix: str = \"\"" not in text:
        needle = "    map_height: int,\n    perf_section: Optional[Callable[[str], Any]],\n"
        _ensure_contains(rel, needle, "render_left_panel map params")
        text = text.replace(
            needle,
            "    map_height: int,\n    map_key_suffix: str = \"\",\n    perf_section: Optional[Callable[[str], Any]] = None,\n",
            1,
        )
    if "map_key_suffix=map_key_suffix" not in text:
        needle = "                        level=level,\n                    )\n"
        _ensure_contains(rel, needle, "render_map_view call closing")
        text = text.replace(
            needle,
            "                        level=level,\n                        map_key_suffix=map_key_suffix,\n                    )\n",
            1,
        )
    _write(rel, text)
    print(f"{rel}: threaded map_key_suffix")


def patch_map_view() -> None:
    rel = "india_resilience_tool/app/views/map_view.py"
    text = _read(rel)
    if "map_key_suffix: str = \"\"" not in text:
        needle = "    perf_section: Optional[Callable[[str], Any]] = None,\n) -> Tuple[Mapping[str, Any], Optional[str], Optional[str]]:\n"
        _ensure_contains(rel, needle, "render_map_view signature ending")
        text = text.replace(
            needle,
            "    perf_section: Optional[Callable[[str], Any]] = None,\n    map_key_suffix: str = \"\",\n) -> Tuple[Mapping[str, Any], Optional[str], Optional[str]]:\n",
            1,
        )
    if "if map_key_suffix:" not in text:
        needle = (
            "        map_key = (\n"
            "            f\"map_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_\"\n"
            "            f\"{selected_state}_{selected_district}_{selected_block}_\"\n"
            "            f\"{selected_basin}_{selected_subbasin}_{str(level).strip().lower()}\"\n"
            "        )\n"
        )
        _ensure_contains(rel, needle, "map_key multi-line assignment")
        text = text.replace(
            needle,
            needle + "        if map_key_suffix:\n            map_key = f\"{map_key}_{map_key_suffix}\"\n",
            1,
        )
    _write(rel, text)
    print(f"{rel}: added map_key_suffix to map key")


def patch_runtime() -> None:
    rel = "india_resilience_tool/app/runtime.py"
    text = _read(rel)
    if "add_hydro_boundary_overlay_to_map" not in text:
        needle = "    from india_resilience_tool.app.left_panel_runtime import render_left_panel\n"
        _ensure_contains(rel, needle, "left_panel import anchor")
        insert = '''    # Optional compact Hydrological Context boundary overlay.\n    _active_hydro = st.session_state.get("active_hydro_boundary_overlay")\n    _hydro_key_suffix = ""\n    if isinstance(_active_hydro, dict):\n        try:\n            from india_resilience_tool.app.hydro_boundary_overlay import (\n                add_hydro_boundary_overlay_to_map,\n            )\n\n            add_hydro_boundary_overlay_to_map(\n                m=artifacts.folium_map,\n                active_overlay=_active_hydro,\n                data_dir=DATA_DIR,\n            )\n        except Exception:\n            pass\n\n        try:\n            import hashlib\n            import json\n\n            _hydro_key_suffix = hashlib.md5(\n                json.dumps(_active_hydro, sort_keys=True).encode("utf-8")\n            ).hexdigest()[:8]\n        except Exception:\n            _hydro_key_suffix = ""\n\n'''
        text = text.replace(needle, insert + needle, 1)
    if "map_key_suffix=_hydro_key_suffix" not in text:
        needle = "        map_height=MAP_HEIGHT,\n        perf_section=perf_section,\n"
        _ensure_contains(rel, needle, "render_left_panel map_height args")
        text = text.replace(
            needle,
            "        map_height=MAP_HEIGHT,\n        map_key_suffix=_hydro_key_suffix,\n        perf_section=perf_section,\n",
            1,
        )
    _write(rel, text)
    print(f"{rel}: wired hydro boundary overlay into map runtime")


def patch_details_runtime() -> None:
    rel = "india_resilience_tool/app/details_runtime.py"
    text = _read(rel)
    if "render_admin_context_cards" in text and "admin_exposure_summary.parquet" in text:
        print(f"{rel}: context-card loading already present")
        return
    needle = "    render_details_panel(\n"
    _ensure_contains(rel, needle, "render_details_panel call")
    insert = '''    # Optional admin-only Exposure Snapshot + compact Hydrological Context cards.\n    # These summaries are lightweight runtime artifacts under processed_optimised/context/.\n    exposure_summary_row = None\n    hydro_summary_row = None\n    context_admin_key = None\n    context_spatial_family = str(st.session_state.get("spatial_family", "admin") or "admin").strip().lower()\n    context_level = str(admin_level or "district").strip().lower()\n\n    if context_spatial_family == "admin" and context_level in {"district", "block"}:\n        try:\n            from india_resilience_tool.app.summary_cache import (\n                load_exposure_summary_cached,\n                load_hydro_summary_cached,\n            )\n            from india_resilience_tool.app.views.context_cards import render_admin_context_cards\n            from india_resilience_tool.data.exposure_summary import slice_exposure_for_admin_key\n            from india_resilience_tool.data.hydro_summary import slice_hydro_for_admin_key\n            from india_resilience_tool.data.optimized_bundle import optimized_context_path\n\n            state_key = alias_fn(str(row.get("state_name") or state_to_show or ""))\n            district_key = alias_fn(str(district_name or row.get("district_name") or ""))\n            if state_key and district_key:\n                context_admin_key = f"{state_key}|{district_key}"\n                if context_level == "block":\n                    block_key = alias_fn(str(row.get("block_name") or block_for_fs or selected_block or ""))\n                    if block_key and block_key.lower() != "all":\n                        context_admin_key = f"{context_admin_key}|{block_key}"\n\n            active_boundary = st.session_state.get("active_hydro_boundary_overlay")\n            if (\n                isinstance(active_boundary, dict)\n                and context_admin_key\n                and active_boundary.get("admin_key") != context_admin_key\n            ):\n                st.session_state["active_hydro_boundary_overlay"] = None\n\n            if context_admin_key:\n                exp_path = optimized_context_path("admin_exposure_summary.parquet", data_dir=data_dir)\n                if exp_path.exists():\n                    exp_df = load_exposure_summary_cached(\n                        str(exp_path),\n                        float(exp_path.stat().st_mtime),\n                    )\n                    exposure_summary_row = slice_exposure_for_admin_key(\n                        exp_df,\n                        admin_key=context_admin_key,\n                        admin_level=context_level,\n                    )\n\n                hyd_path = optimized_context_path("admin_hydro_summary.parquet", data_dir=data_dir)\n                if hyd_path.exists():\n                    hyd_df = load_hydro_summary_cached(\n                        str(hyd_path),\n                        float(hyd_path.stat().st_mtime),\n                    )\n                    hydro_summary_row = slice_hydro_for_admin_key(\n                        hyd_df,\n                        admin_key=context_admin_key,\n                        admin_level=context_level,\n                    )\n\n            render_admin_context_cards(\n                exposure_summary_row=exposure_summary_row,\n                hydro_summary_row=hydro_summary_row,\n                level=context_level,\n                admin_key=context_admin_key,\n                spatial_family=context_spatial_family,\n            )\n        except Exception:\n            # Optional context cards must never break the core risk profile.\n            pass\n\n'''
    text = text.replace(needle, insert + needle, 1)
    _write(rel, text)
    print(f"{rel}: inserted summary loading + compact context-card rendering")


def main() -> None:
    required = [
        "india_resilience_tool/app/state.py",
        "india_resilience_tool/app/runtime.py",
        "india_resilience_tool/app/details_runtime.py",
        "india_resilience_tool/app/left_panel_runtime.py",
        "india_resilience_tool/app/views/map_view.py",
    ]
    missing = [rel for rel in required if not _path(rel).exists()]
    if missing:
        raise SystemExit("Missing required files; run from repo root. Missing: " + ", ".join(missing))

    patch_state()
    patch_left_panel_runtime()
    patch_map_view()
    patch_runtime()
    patch_details_runtime()
    print("\nDone. Backups use suffix:", BACKUP_SUFFIX)


if __name__ == "__main__":
    main()
