"""CLI tests for `aqueduct dev capabilities` — the SHIPPED capability tooling.

The tool used to live in `scripts/capabilities.py`, which is not in the wheel:
a third-party engine author who `pip install`ed aqueduct had no way to generate
the 261-row capability table their engine cannot register without, and the only
alternatives (hand-write it, or copy Spark's) either do not scale or hand the new
engine 261 `supported` rows — a silent claim to implement the whole grammar, the
exact blindness the capability framework exists to prevent. So these commands
ship; these tests pin that they do.
"""

from __future__ import annotations

import pytest
import yaml
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli
from aqueduct.errors import CapabilityDeclarationError
from aqueduct.executor.capabilities import Support, load_declaration
from aqueduct.executor.capability_tooling import governed_leaves

pytestmark = pytest.mark.unit


def _rows(path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["leaves"]


def _verdict(row) -> str:
    return row if isinstance(row, str) else row["support"]


def test_dev_group_and_capabilities_commands_are_registered():
    assert "dev" in cli.commands
    caps = cli.commands["dev"].commands["capabilities"]
    assert set(caps.commands) == {"check", "sync", "scaffold", "docs"}


def test_scaffold_emits_every_leaf_all_undeclared(tmp_path):
    """The whole point of a generated scaffold: complete (cannot go stale like a
    checked-in template) and entirely `undeclared` (cannot smuggle in a default
    verdict, which is what copying Spark's table would do)."""
    out = tmp_path / "capabilities.yml"
    result = CliRunner().invoke(
        cli, ["dev", "capabilities", "scaffold", "--engine", "toyengine", "--out", str(out)]
    )
    assert result.exit_code == 0, result.output

    rows = _rows(out)
    assert set(rows) == set(governed_leaves())
    assert {_verdict(v) for v in rows.values()} == {Support.UNDECLARED.value}

    header = out.read_text(encoding="utf-8")
    assert "NOT A FILE TO COPY" in header
    assert "aqueduct dev capabilities sync" in header


def test_scaffolded_engine_cannot_register_until_verdicts_are_filled_in(tmp_path):
    """End-to-end proof of the forcing function: the file the CLI just wrote is
    REFUSED by the loader, and the refusal names the leaves and the real fix."""
    out = tmp_path / "capabilities.yml"
    assert (
        CliRunner()
        .invoke(
            cli, ["dev", "capabilities", "scaffold", "--engine", "toyengine", "--out", str(out)]
        )
        .exit_code
        == 0
    )

    with pytest.raises(CapabilityDeclarationError) as exc:
        load_declaration(out, governed_leaves())
    assert "UNDECLARED" in str(exc.value)
    assert "aqueduct dev capabilities sync" in str(exc.value)
    assert "Reinstall" not in str(exc.value)  # wrong advice for this state
    assert len(exc.value.leaves) == len(governed_leaves())

    # …and once every row is a real verdict, the same file loads.
    text = out.read_text(encoding="utf-8").replace(": undeclared", ": unsupported")
    out.write_text(text, encoding="utf-8")
    caps = load_declaration(out, governed_leaves())
    assert caps.engine == "toyengine"
    assert all(c.support is Support.UNSUPPORTED for c in caps.table.values())


def test_scaffold_refuses_to_overwrite_without_force(tmp_path):
    out = tmp_path / "capabilities.yml"
    args = ["dev", "capabilities", "scaffold", "--engine", "toyengine", "--out", str(out)]
    runner = CliRunner()
    assert runner.invoke(cli, args).exit_code == 0

    again = runner.invoke(cli, args)
    assert again.exit_code == exit_codes.CONFIG_ERROR
    assert "refusing to overwrite" in again.output

    forced = runner.invoke(cli, [*args, "--force"])
    assert forced.exit_code == 0


def test_check_passes_for_the_shipped_spark_declaration():
    result = CliRunner().invoke(cli, ["dev", "capabilities", "check"])
    assert result.exit_code == 0, result.output
    assert "all declared" in result.output


def test_check_fails_and_names_the_gap(tmp_path, monkeypatch):
    """A new grammar/config leaf with no verdict must fail `check` loudly, naming
    the leaf — this is the CI gate."""
    import aqueduct.executor.capability_tooling as tooling

    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\nleaves:\n  feature.a: supported\n  feature.ghost: supported\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(tooling, "discover_declarations", lambda extra=None: [decl])
    monkeypatch.setattr(tooling, "governed_leaves", lambda: frozenset({"feature.a", "feature.new"}))

    result = CliRunner().invoke(cli, ["dev", "capabilities", "check"])
    assert result.exit_code == exit_codes.CONFIG_ERROR
    assert "MISSING     feature.new" in result.output
    assert "ORPHANED    feature.ghost" in result.output
    assert "aqueduct dev capabilities sync" in result.output


def test_sync_appends_undeclared_and_never_invents_a_verdict(tmp_path, monkeypatch):
    import aqueduct.executor.capability_tooling as tooling

    decl = tmp_path / "capabilities.yml"
    decl.write_text("engine: toy\nleaves:\n  feature.a: supported\n", encoding="utf-8")
    monkeypatch.setattr(tooling, "discover_declarations", lambda extra=None: [decl])
    monkeypatch.setattr(tooling, "governed_leaves", lambda: frozenset({"feature.a", "feature.new"}))

    result = CliRunner().invoke(cli, ["dev", "capabilities", "sync"])
    assert result.exit_code == 0, result.output

    rows = _rows(decl)
    assert rows["feature.a"] == "supported"  # untouched
    assert rows["feature.new"] == "undeclared"  # parked, NOT swept into supported
    assert "feature.new" in result.output

    # Still red until a human decides — sync does not make the build pass.
    with pytest.raises(CapabilityDeclarationError, match="UNDECLARED|undeclared"):
        load_declaration(decl, frozenset({"feature.a", "feature.new"}))


def test_sync_reports_orphans_but_never_deletes_them(tmp_path, monkeypatch):
    import aqueduct.executor.capability_tooling as tooling

    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\nleaves:\n  feature.a: supported\n  feature.ghost: supported\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(tooling, "discover_declarations", lambda extra=None: [decl])
    monkeypatch.setattr(tooling, "governed_leaves", lambda: frozenset({"feature.a"}))

    result = CliRunner().invoke(cli, ["dev", "capabilities", "sync"])
    assert result.exit_code == 0
    assert "orphaned" in result.output
    assert "feature.ghost" in _rows(decl)  # a rename gets reviewed, not silently dropped


def test_docs_regenerates_the_matrix_between_markers(tmp_path, monkeypatch):
    import aqueduct.executor.capability_tooling as tooling

    decl = tmp_path / "capabilities.yml"
    decl.write_text(
        "engine: toy\n"
        "leaves:\n"
        "  feature.a: supported\n"
        "  feature.b:\n"
        "    support: unsupported\n"
        '    hint: "use feature.a"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(tooling, "discover_declarations", lambda extra=None: [decl])

    doc = tmp_path / "compatibility.md"
    doc.write_text(
        "# Compat\n\n<!-- ENGINE_MATRIX_START -->\nstale\n<!-- ENGINE_MATRIX_END -->\ntail\n",
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["dev", "capabilities", "docs", "--out", str(doc)])
    assert result.exit_code == 0, result.output

    text = doc.read_text(encoding="utf-8")
    assert "stale" not in text
    assert "| `toy` | `feature.b` | unsupported |" in text
    assert "use feature.a" in text
    assert text.endswith("tail\n")  # only the marked region is rewritten
