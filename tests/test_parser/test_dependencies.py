"""`dependencies:` top-level Blueprint block (Phase 88).

Covers `aqueduct/dependencies.py` (PEP 508-lite parsing + the
importlib.metadata preflight), the `BlueprintSchema.dependencies` parse-time
validator, flow-through onto the parsed `Blueprint` / compiled `Manifest`,
and the compile-time preflight in `aqueduct/compiler/compiler.py` that
raises `DependencyError` for an unsatisfied requirement.

NOT engine-scoped, no capability leaf — see the module owner's design note
in `aqueduct/executor/capability_leaves.py` (top-level Blueprint fields are
outside `_SCHEMA_BLOCKS`) and `tests/test_capabilities/` for the closure
test this must not perturb.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.dependencies import (
    Requirement,
    check_requirements,
    parse_requirement,
    requirement_status,
)
from aqueduct.errors import CompileError, DependencyError, ParseError
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit


def _write_bp(tmp_path: pathlib.Path, dependencies: list[str]) -> pathlib.Path:
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(
        yaml.dump(
            {
                "aqueduct": "1.0",
                "id": "t.bp",
                "name": "T",
                "dependencies": dependencies,
                "modules": [
                    {
                        "id": "in",
                        "type": "Ingress",
                        "label": "In",
                        "config": {"format": "parquet", "path": "p"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    return bp_path


# ── parse_requirement ────────────────────────────────────────────────────────


def test_parse_bare_name():
    req = parse_requirement("pandas")
    assert req == Requirement(raw="pandas", name="pandas", extras=(), specifier="")


def test_parse_name_with_specifier():
    req = parse_requirement("holidays>=0.40")
    assert req.name == "holidays"
    assert req.specifier == ">=0.40"
    assert req.raw == "holidays>=0.40"


def test_parse_name_with_extras():
    req = parse_requirement("holidays[extra1,extra2]")
    assert req.name == "holidays"
    assert req.extras == ("extra1", "extra2")
    assert req.specifier == ""


def test_parse_name_with_extras_and_multi_clause_specifier():
    req = parse_requirement("holidays[x]>=1.2,<2")
    assert req.name == "holidays"
    assert req.extras == ("x",)
    assert req.specifier == ">=1.2,<2"


def test_parse_normalizes_name_for_lookup_but_keeps_raw_verbatim():
    req = parse_requirement("My_Cool.Package>=1.0")
    assert req.name == "my-cool-package"
    assert req.raw == "My_Cool.Package>=1.0"


def test_parse_malformed_string_rejected():
    with pytest.raises(ValueError, match="malformed"):
        parse_requirement("not a valid requirement!!!")


def test_parse_empty_string_rejected():
    with pytest.raises(ValueError):
        parse_requirement("")


def test_parse_env_marker_rejected():
    with pytest.raises(ValueError, match="environment markers"):
        parse_requirement('pandas>=1.0; python_version < "3.12"')


# ── requirement_status / check_requirements ─────────────────────────────────


def test_requirement_status_satisfied_for_installed_package():
    # pytest is guaranteed present (it's running this test).
    req = parse_requirement("pytest")
    status, version = requirement_status(req)
    assert status == "satisfied"
    assert version is not None


def test_requirement_status_missing_for_fake_package():
    req = parse_requirement("this-package-does-not-exist-aqueduct-phase88")
    status, version = requirement_status(req)
    assert status == "missing"
    assert version is None


def test_requirement_status_version_conflict():
    import pytest as _pytest

    req = parse_requirement(f"pytest>={_pytest.__version__}.1")
    status, version = requirement_status(req)
    assert status == "version_conflict"
    assert version == _pytest.__version__


def test_requirement_status_unknown_version_passes_silently(monkeypatch):
    import aqueduct.dependencies as deps_mod

    def _fake_version(name):
        return "1.2.3.post1"

    monkeypatch.setattr(deps_mod._importlib_metadata, "version", _fake_version)
    req = parse_requirement("somepkg>=1.0")
    status, version = requirement_status(req)
    assert status == "unknown_version"
    assert version == "1.2.3.post1"

    # check_requirements must treat this as clean (no problem line).
    problems = check_requirements(["somepkg>=1.0"])
    assert problems == []


def test_check_requirements_clean_for_installed_package():
    assert check_requirements(["pytest"]) == []


def test_check_requirements_reports_missing():
    problems = check_requirements(["this-package-does-not-exist-aqueduct-phase88"])
    assert len(problems) == 1
    assert "this-package-does-not-exist-aqueduct-phase88" in problems[0]
    assert "not installed" in problems[0]


# ── schema / parse-time validation ──────────────────────────────────────────


def test_malformed_dependency_rejected_at_parse_time(tmp_path):
    bp_path = _write_bp(tmp_path, ["not a valid requirement!!!"])
    with pytest.raises(ParseError):
        parse(str(bp_path))


def test_env_marker_dependency_rejected_at_parse_time(tmp_path):
    bp_path = _write_bp(tmp_path, ['pandas>=1.0; python_version < "3.12"'])
    with pytest.raises(ParseError):
        parse(str(bp_path))


# ── flow-through to Blueprint / Manifest ────────────────────────────────────


def test_dependencies_flow_through_to_blueprint(tmp_path):
    bp_path = _write_bp(tmp_path, ["pytest", "pydantic>=1.0"])
    bp = parse(str(bp_path))
    assert bp.dependencies == ("pytest", "pydantic>=1.0")


def test_dependencies_default_empty(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(
        yaml.dump(
            {
                "aqueduct": "1.0",
                "id": "t.bp",
                "name": "T",
                "modules": [
                    {
                        "id": "in",
                        "type": "Ingress",
                        "label": "In",
                        "config": {"format": "parquet", "path": "p"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    bp = parse(str(bp_path))
    assert bp.dependencies == ()


def test_dependencies_flow_through_to_manifest(tmp_path):
    bp_path = _write_bp(tmp_path, ["pytest", "pydantic"])
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    assert manifest.dependencies == ("pytest", "pydantic")
    assert manifest.to_dict()["dependencies"] == ["pytest", "pydantic"]


# ── compile-time preflight ───────────────────────────────────────────────────


def test_preflight_passes_for_installed_packages(tmp_path):
    bp_path = _write_bp(tmp_path, ["pytest", "pydantic"])
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    assert manifest.blueprint_id == "t.bp"


def test_preflight_raises_dependency_error_for_missing_package(tmp_path):
    bp_path = _write_bp(tmp_path, ["this-package-does-not-exist-aqueduct-phase88>=1.0"])
    bp = parse(str(bp_path))
    with pytest.raises(DependencyError) as exc_info:
        compiler_compile(bp, blueprint_path=bp_path)
    assert isinstance(exc_info.value, CompileError)
    msg = str(exc_info.value)
    assert "this-package-does-not-exist-aqueduct-phase88>=1.0" in msg
    assert "not installed" in msg
    assert "pip install 'this-package-does-not-exist-aqueduct-phase88>=1.0'" in msg
    assert exc_info.value.problems


def test_preflight_raises_dependency_error_for_version_conflict(tmp_path):
    import pytest as _pytest

    bad_specifier = f"pytest>={_pytest.__version__}.1"
    bp_path = _write_bp(tmp_path, [bad_specifier])
    bp = parse(str(bp_path))
    with pytest.raises(DependencyError) as exc_info:
        compiler_compile(bp, blueprint_path=bp_path)
    msg = str(exc_info.value)
    assert bad_specifier in msg
    assert f"installed: {_pytest.__version__}" in msg
    assert f"pip install '{bad_specifier}'" in msg


def test_preflight_only_names_failing_requirements_in_pip_command(tmp_path):
    """A satisfied requirement must never show up in the printed pip
    command alongside the actually-failing one."""
    bp_path = _write_bp(tmp_path, ["pytest", "this-package-does-not-exist-aqueduct-phase88"])
    bp = parse(str(bp_path))
    with pytest.raises(DependencyError) as exc_info:
        compiler_compile(bp, blueprint_path=bp_path)
    msg = str(exc_info.value)
    assert "this-package-does-not-exist-aqueduct-phase88" in msg
    pip_line = [line for line in msg.splitlines() if line.strip().startswith("pip install")][0]
    assert "pytest" not in pip_line
