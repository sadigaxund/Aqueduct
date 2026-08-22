"""Golden-output tests for the Phase 85 Wave 1 render funnel.

These are the owner's redline artifacts: each expected-text block below is
meant to be read (and objected to) directly in the diff, so screens are kept
inline rather than farmed out to a ``tests/fixtures/golden/`` tree.

All screens come from ``gallery/snippets/47_agent_cascade`` — copied into
``tmp_path`` (never mutating the repo's gallery directory) and run against
``duckdb`` via ``-s deployment.engine=duckdb`` so nothing here needs Spark or
a reachable LLM. ``ANTHROPIC_API_KEY`` is explicitly unset and no tier-1
Ollama server is expected on ``localhost:11434``, so the self-healing cascade
always ends at SCREEN 5's "unreachable" case — deterministic on any machine.

Determinism knobs, pinned via ``monkeypatch.setenv`` in every test:
    COLUMNS=80          — pins wrap/table width
    AQ_FORCE_TTY=0 or 1 — pins the piped vs. TTY rendering branch
    --run-id            — pins the run id (never a random UUID)

``_normalize()`` is the one place non-deterministic text (durations, byte
sizes, absolute paths, the doctor command's environment-dependent rows) gets
scrubbed before comparison.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.cli.render.width import strip_ansi

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_SNIPPET_SRC = _REPO / "gallery" / "snippets" / "47_agent_cascade"
_PY = sys.executable

_DURATION_RE = re.compile(r"\[\d+(?:\.\d+)?(?:ms|s)\]")
_SIZE_RE = re.compile(r"\d+(?:\.\d+)?\s*[KMGT]i?B")
_JAVA_HOME_RE = re.compile(r"JAVA_HOME=\S+")
_JAVA_VER_RE = re.compile(r"Java \d+")


def _normalize(text: str, *, tmp_root: Path | None = None) -> str:
    """Scrub non-deterministic substrings so golden comparisons are stable.

    Strips ANSI first (via the shared ``strip_ansi`` primitive — never a
    second hand-rolled regex), then replaces: durations (``[123ms]``,
    ``[0.2s]``), byte sizes (``295.6 GiB``), ``JAVA_HOME=...``, the Java
    version number, and — when ``tmp_root`` is given — every absolute path
    under it (so a doctor/patch-list capture doesn't embed the test's
    ``tmp_path``).
    """
    text = strip_ansi(text)
    text = _DURATION_RE.sub("[<DUR>]", text)
    text = _SIZE_RE.sub("<SIZE>", text)
    text = _JAVA_HOME_RE.sub("JAVA_HOME=<PATH>", text)
    text = _JAVA_VER_RE.sub("Java <N>", text)
    if tmp_root is not None:
        text = text.replace(str(tmp_root), "<TMP>")
    return text


def _prepare_cascade_snippet(tmp_path: Path) -> Path:
    """Copy the 47_agent_cascade snippet into ``tmp_path`` and populate its
    input data there — never touching the repo's gallery directory."""
    dest = tmp_path / "cascade"
    shutil.copytree(_SNIPPET_SRC, dest)
    subprocess.run([_PY, "populate_data.py"], cwd=dest, check=True, capture_output=True)
    return dest


def _invoke(args: list[str]):
    return CliRunner().invoke(cli, args)


# ---------------------------------------------------------------------------
# SCREEN 1 — clean run
# ---------------------------------------------------------------------------


class TestCleanRun:
    """`aqueduct run blueprint.yml -s deployment.engine=duckdb` — healed
    blueprint, framed screen, all-✓ module tree."""

    _EXPECTED_STDOUT = (
        "─" * 80
        + "\n"
        + "▶ cascade_demo  ·  3 modules  ·  run golden-clean  ·  duckdb local[*]\n"
        + "─" * 80
        + "\n"
        + "\n"
        + "  ✓ raw_orders\n"
        + "  ✓ enrich\n"
        + "  ✓ output\n"
        + "─" * 80
        + "\n"
        + "✓ blueprint complete"
    )

    def test_clean_run_piped(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        dest = _prepare_cascade_snippet(tmp_path)
        monkeypatch.chdir(dest)

        res = _invoke(
            ["run", "blueprint.yml", "-s", "deployment.engine=duckdb", "--run-id", "golden-clean"]
        )

        assert res.exit_code == 0, res.output
        stdout = _normalize(res.stdout).rstrip("\n")
        assert stdout == self._EXPECTED_STDOUT

    def test_clean_run_tty(self, monkeypatch, tmp_path) -> None:
        """Same screen, forced TTY branch — the framed run screen does not
        change shape between piped and TTY at default verbosity (the
        divergence this ruling cares about is wrap/truncate behaviour on
        LONGER lines, exercised by the patch-list table tests below)."""
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        dest = _prepare_cascade_snippet(tmp_path)
        monkeypatch.chdir(dest)

        res = _invoke(
            [
                "run",
                "blueprint.yml",
                "-s",
                "deployment.engine=duckdb",
                "--run-id",
                "golden-clean-tty",
            ]
        )

        assert res.exit_code == 0, res.output
        stdout = _normalize(res.stdout).rstrip("\n")
        expected = self._EXPECTED_STDOUT.replace("golden-clean", "golden-clean-tty")
        assert stdout == expected


# ---------------------------------------------------------------------------
# SCREEN 5 — failure + heal-unreachable (the stream-split regression test)
# ---------------------------------------------------------------------------


class TestFailureHealUnreachable:
    """`blueprint_bugged.yml` with no LLM reachable anywhere.

    This is the regression pin: the framed run screen (header, per-module
    lines, closing divider, final verdict) must land ENTIRELY on stdout, and
    the whole self-healing ceremony block must land ENTIRELY on stderr. If
    the heal block's ``echo()`` call sites ever drift back to ``err=False``
    (or the run screen leaks a heal line), this test goes red.
    """

    _EXPECTED_STDOUT = (
        "─" * 80
        + "\n"
        + "▶ cascade_demo  ·  3 modules  ·  run golden-bug  ·  duckdb local[*]\n"
        + "─" * 80
        + "\n"
        + "\n"
        + "  ✓ raw_orders\n"
        + '  ✗ enrich  — [enrich] SQL execution failed: Binder Error: Referenced column "total" not found in FROM clause!\n'
        + "─" * 80
        + "\n"
        + "✗ blueprint failed  run_id=golden-bug  failed_module=enrich"
    )

    @pytest.fixture()
    def _bugged_result(self, monkeypatch, tmp_path):
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        dest = _prepare_cascade_snippet(tmp_path)
        monkeypatch.chdir(dest)
        return _invoke(
            [
                "run",
                "blueprint_bugged.yml",
                "-s",
                "deployment.engine=duckdb",
                "--run-id",
                "golden-bug",
            ]
        )

    def test_stdout_carries_only_the_framed_screen(self, _bugged_result) -> None:
        res = _bugged_result
        stdout = _normalize(res.stdout).rstrip("\n")
        assert stdout == self._EXPECTED_STDOUT
        # The regression this test exists to catch: none of the heal
        # ceremony's own text may leak onto stdout.
        assert "self-healing" not in stdout
        assert "◆" not in stdout
        assert "tier" not in stdout

    def test_stderr_carries_the_whole_heal_block(self, _bugged_result) -> None:
        res = _bugged_result
        stderr = _normalize(res.stderr)
        assert "⚠ enrich failed → agent self-healing" in stderr
        assert "◆ cascade · 2 tier(s) · qwen2.5-coder:7b → claude-sonnet-4-6" in stderr
        assert "tier 1 · qwen2.5-coder:7b" in stderr
        assert "tier 2 · claude-sonnet-4-6" in stderr
        assert "└─ ✗ all tiers unreachable" in stderr
        assert "↑ no patch to stage" in stderr
        # And the inverse of the stdout assertion: none of the framed run
        # screen's own text may leak onto stderr.
        assert "blueprint failed" not in stderr
        assert "▶ cascade_demo" not in stderr

    def test_exit_code_is_data_or_runtime(self, _bugged_result) -> None:
        from aqueduct import exit_codes

        assert _bugged_result.exit_code == exit_codes.DATA_OR_RUNTIME


# ---------------------------------------------------------------------------
# SCREEN — doctor (default collapsed tail vs. -v expanded rows)
# ---------------------------------------------------------------------------


class TestDoctor:
    """``aqueduct doctor blueprint.yml`` — default vs. ``-v``.

    Byte-exact assertions are used only for the header/footer and the
    section-header ordering (deterministic given fixed input). Individual
    check rows (Java version/path, free disk space, per-row millisecond
    timings) are environment-dependent, so those are asserted STRUCTURALLY
    — the row's leading icon + label + that it exists at all — after running
    text through ``_normalize()`` to strip the volatile parts. ``--skip-spark``
    is used so the golden test never depends on a live Spark/JVM check
    succeeding on the CI machine; JOB 2's captures use the un-skipped
    default separately so the owner also sees the real Spark row.
    """

    @pytest.fixture()
    def _dest(self, tmp_path):
        return _prepare_cascade_snippet(tmp_path)

    def _run_doctor(self, dest, monkeypatch, *, verbose: bool = False):
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        # Some other test module in this suite sets ANTHROPIC_API_KEY via
        # bare `os.environ[...] = ...` (not monkeypatch), which leaks across
        # tests sharing this process — pin it unset so the cascade-tier-2
        # row is deterministic regardless of run order.
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.chdir(dest)
        args = ["doctor", "blueprint.yml", "--skip-spark"]
        if verbose:
            args.append("-v")
        return _invoke(args)

    def test_default_header_and_footer(self, _dest, monkeypatch) -> None:
        res = self._run_doctor(_dest, monkeypatch)
        assert res.exit_code == 0, res.output
        stdout = _normalize(res.stdout, tmp_root=_dest)
        lines = stdout.splitlines()
        assert lines[0] == "─" * 80
        assert lines[1] == "▶ doctor  ·  blueprint.yml  ·  21 checks"
        assert lines[2] == "─" * 80
        assert stdout.rstrip().splitlines()[-2] == "─" * 80
        assert stdout.rstrip().splitlines()[-1] == "✓ all checks passed"

    def test_default_sections_present_and_rows_shaped(self, _dest, monkeypatch) -> None:
        res = self._run_doctor(_dest, monkeypatch)
        stdout = _normalize(res.stdout, tmp_root=_dest)
        for section in ("Config", "Stores", "Blueprint sources", "Agent / LLM", "Secrets"):
            assert f"\n  {section}\n" in stdout, f"missing section header {section!r}"
        # Structural row shape: two-space section indent, four-space row
        # indent, an icon leader.
        assert "    ✓ config" in stdout
        assert "    ✓ cascade-tier-1" in stdout
        assert "    ⚠ cascade-tier-2" in stdout
        # Default view collapses the long tail of ok/skip rows behind a
        # single "· more" line — the whole point of the verbosity tier.
        assert re.search(r"^  · more\s", stdout, re.MULTILINE)

    def test_verbose_expands_the_collapsed_rows(self, _dest, monkeypatch) -> None:
        res = self._run_doctor(_dest, monkeypatch, verbose=True)
        assert res.exit_code == 0, res.output
        stdout = _normalize(res.stdout, tmp_root=_dest)
        # -v is the visible effect of the verbosity tier: no collapsed tail,
        # and rows the default view hides (skipped / not-applicable) appear
        # individually instead.
        assert "· more" not in stdout
        assert "cluster-stores" in stdout
        assert "hooks" in stdout
        assert "remote-target" in stdout
        # Same header/footer shape as default.
        lines = stdout.splitlines()
        assert lines[1] == "▶ doctor  ·  blueprint.yml  ·  21 checks"
        assert stdout.rstrip().splitlines()[-1] == "✓ all checks passed"

    def test_verbose_has_strictly_more_lines_than_default(self, _dest, monkeypatch) -> None:
        default_lines = self._run_doctor(_dest, monkeypatch).stdout.splitlines()
        verbose_lines = self._run_doctor(_dest, monkeypatch, verbose=True).stdout.splitlines()
        assert len(verbose_lines) > len(default_lines)


# ---------------------------------------------------------------------------
# SCREEN 7 — patch list table (TTY fit + piped full text)
# ---------------------------------------------------------------------------


def _write_patch_store(project: Path) -> Path:
    """Small local patch store — mirrors ``test_cli_patch_extra.py``'s
    ``setup`` fixture (reused pattern, not reinvented)."""
    project.mkdir(parents=True, exist_ok=True)
    bp_path = project / "blueprint.yml"
    bp_path.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test\n"
        "modules:\n"
        "  - id: src\n"
        "    type: Ingress\n"
        "    label: Source\n"
        "    config: {path: data.csv}\n"
        "edges: []\n"
    )
    patches_dir = project / "patches"
    for sub in ("pending", "applied", "rejected"):
        (patches_dir / sub).mkdir(parents=True)
    long_rationale = (
        'replace unknown column "total" with "total_amt" in enrich SQL '
        "before the join step runs, otherwise downstream aggregation breaks"
    )
    (patches_dir / "pending" / "a3f2c1.json").write_text(
        json.dumps({"patch_id": "a3f2c1", "rationale": long_rationale})
    )
    (patches_dir / "pending" / "99d0e4.json").write_text(
        json.dumps({"patch_id": "99d0e4", "rationale": "short one"})
    )
    return bp_path


class TestPatchListTable:
    _LONG_RATIONALE = (
        'replace unknown column "total" with "total_amt" in enrich SQL '
        "before the join step runs, otherwise downstream aggregation breaks"
    )

    def test_tty_rendering_fits_80_columns_and_ellipsises_rationale(
        self, monkeypatch, tmp_path
    ) -> None:
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        bp_path = _write_patch_store(tmp_path / "project")

        res = _invoke(["patch", "list", "--blueprint", str(bp_path)])

        assert res.exit_code == 0, res.output
        for line in res.stdout.splitlines():
            assert len(strip_ansi(line)) <= 80, repr(line)
        stripped = strip_ansi(res.stdout)
        assert self._LONG_RATIONALE not in stripped  # flex column truncated
        assert "…" in stripped

    def test_piped_rendering_has_full_rationale_and_no_ansi(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setenv("COLUMNS", "80")
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        bp_path = _write_patch_store(tmp_path / "project")

        res = _invoke(["patch", "list", "--blueprint", str(bp_path)])

        assert res.exit_code == 0, res.output
        assert "\x1b" not in res.stdout
        assert self._LONG_RATIONALE in res.stdout
        assert "…" not in res.stdout
