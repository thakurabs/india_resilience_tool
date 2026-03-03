"""AST-driven tests for state profile missing-file helper in legacy dashboard."""

from __future__ import annotations

import ast
from pathlib import Path



def _load_state_profile_files_missing_fn():
    src_path = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "orchestrator_impl.py"
    )
    source = src_path.read_text(encoding="utf-8")
    module_ast = ast.parse(source, filename=str(src_path))

    target = None
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name == "state_profile_files_missing":
            target = node
            break

    assert target is not None, "state_profile_files_missing must exist in orchestrator_impl.py"

    isolated_mod = ast.Module(body=[target], type_ignores=[])
    code = compile(isolated_mod, filename=str(src_path), mode="exec")
    namespace: dict[str, object] = {"Path": Path}
    exec(code, namespace)
    return namespace["state_profile_files_missing"]


def test_state_profile_files_missing_true_when_required_files_absent(tmp_path: Path) -> None:
    fn = _load_state_profile_files_missing_fn()
    assert fn(tmp_path, "Telangana", "district") is True


def test_state_profile_files_missing_false_when_required_files_present(tmp_path: Path) -> None:
    fn = _load_state_profile_files_missing_fn()
    state_dir = tmp_path / "Telangana"
    state_dir.mkdir(parents=True)
    (state_dir / "state_yearly_ensemble_stats_block.csv").write_text("scenario,year,ensemble_mean\nssp245,2030,1.0\n")
    (state_dir / "state_ensemble_stats_block.csv").write_text("scenario,period,ensemble_mean\nssp245,2020-2040,1.0\n")

    assert fn(tmp_path, "Telangana", "block") is False
