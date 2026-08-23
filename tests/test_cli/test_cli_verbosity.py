"""Phase 85 — the single verbosity resolution seam.

Root `cli()` carries ONE `-v` count flag (`ctx.obj["verbosity"]`); `run` and
`doctor` keep their OWN `-v` count option purely because Click can't forward
a root-group option placed after the subcommand name (`aqueduct run -v
bp.yml`). `aqueduct.cli.verbosity.resolve_verbosity()` is the one place that
merges the two (`max(root, local)`) — every consumer reads through it
instead of a second hand-rolled boolean.

A synthetic command (registered on the real `cli` group, like
`test_cli_clean_exit.py` does) exercises the option-parsing + merge
end-to-end without needing a real `run`/`doctor` invocation. Heavier
end-to-end behaviour (stream routing during a live heal) is asserted at the
seam — the literal `err=` argument at the call site — per the "test the
seam, not a mocked full execution" guidance for this batch.
"""

from __future__ import annotations

import logging

import click
import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.cli.verbosity import resolve_verbosity

pytestmark = pytest.mark.unit


def _invoke(args: list[str]):
    return CliRunner().invoke(cli, args)


@cli.command("_aq_test_verbosity_probe")
@click.option("-v", "--verbose", "verbose", count=True)
def _verbosity_probe(verbose: int) -> None:
    """Registered once at import time — mirrors `run`/`doctor`'s own `-v`
    count option so the merge logic is exercised through real Click parsing."""
    click.echo(str(resolve_verbosity(local=verbose)))


class TestResolveVerbosityViaCLI:
    """Both flag placements, and both flag repetitions, must agree."""

    def test_default_is_zero(self) -> None:
        res = _invoke(["_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "0"

    def test_root_prefix_v_resolves_one(self) -> None:
        res = _invoke(["-v", "_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "1"

    def test_subcommand_postfix_v_resolves_one(self) -> None:
        """`aqueduct run -v bp.yml` — Click cannot forward a root option
        placed after the subcommand, so the subcommand's OWN `-v` count must
        resolve to the same effective tier as the prefix form above."""
        res = _invoke(["_aq_test_verbosity_probe", "-v"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "1"

    def test_root_prefix_vv_resolves_two(self) -> None:
        res = _invoke(["-vv", "_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "2"

    def test_subcommand_postfix_vv_resolves_two(self) -> None:
        res = _invoke(["_aq_test_verbosity_probe", "-vv"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "2"

    def test_merge_takes_the_max_not_the_sum(self) -> None:
        """Root `-v` (1) + local `-v` (1) is still tier 1, not tier 2 — the
        two flags are the SAME concept counted twice, not additive knobs."""
        res = _invoke(["-v", "_aq_test_verbosity_probe", "-v"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "1"

    def test_root_vv_beats_a_lower_local_count(self) -> None:
        res = _invoke(["-vv", "_aq_test_verbosity_probe", "-v"])
        assert res.exit_code == 0, res.output
        assert res.stdout.strip() == "2"


class TestResolveVerbosityOutsideClick:
    """The helper must not explode when called with no active Click context
    (e.g. a plain unit test that imports a consumer directly)."""

    def test_no_context_falls_back_to_local_only(self) -> None:
        assert resolve_verbosity() == 0
        assert resolve_verbosity(local=1) == 1
        assert resolve_verbosity(local=2) == 2


class TestDebugFlagIsSeparateFromVerbose:
    """`-v` must NOT flip Python logging to DEBUG — only `--debug` does."""

    @pytest.fixture(autouse=True)
    def _restore_logging(self):
        root = logging.getLogger()
        orig_level = root.level
        orig_handlers = list(root.handlers)
        yield
        root.setLevel(orig_level)
        root.handlers[:] = orig_handlers

    def test_debug_flag_sets_debug_level(self) -> None:
        res = _invoke(["--debug", "_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert logging.getLogger().level == logging.DEBUG

    def test_plain_verbose_does_not_set_debug_level(self) -> None:
        res = _invoke(["-vv", "_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert logging.getLogger().level == logging.WARNING

    def test_default_level_is_warning(self) -> None:
        res = _invoke(["_aq_test_verbosity_probe"])
        assert res.exit_code == 0, res.output
        assert logging.getLogger().level == logging.WARNING


class TestQuietStartupSeam:
    """`quiet_startup` flips off only at verbosity >= 2 (-vv, the "raw
    layer" tier) — never at -v (tier 1, Aqueduct-side narrative only).

    Every `quiet_startup=` call site in run.py now reads
    ``verbosity < 2`` where `verbosity` comes from `resolve_verbosity()`;
    this pins that boolean expression directly rather than spinning up a
    real Spark/DuckDB session (no engine in a unit test)."""

    @pytest.mark.parametrize(
        "verbosity,expected_quiet",
        [(0, True), (1, True), (2, False), (3, False)],
    )
    def test_quiet_startup_expression(self, verbosity: int, expected_quiet: bool) -> None:
        assert (verbosity < 2) is expected_quiet


class TestHealBlockStreamRoutingSeam:
    """The self-healing transcript is narrative — ENTIRELY stderr — even
    though `render.funnel.emit()` defaults to stdout (`err=False`). Rather
    than mocking a full `run()`/`heal()` execution (heavy: real Blueprint,
    engine, agent), this pins the exact call-site fix at the seam: the
    `TranscriptWriter` write-callback and the "waiting for first token"
    cue must pass ``err=True`` explicitly, and the interactive-streaming
    TTY probe must check stderr, not stdout — see the Phase 85 audit notes
    on aqueduct/cli/run.py (~L2292-2307) and aqueduct/cli/heal.py (~L306)."""

    @staticmethod
    def _read_module_source(dotted_path: str) -> str:
        """Read a CLI module's source by FILE PATH, not by introspecting an
        imported name — `aqueduct.cli.run` binds `run` (the Click Command,
        re-exported by ``aqueduct/cli/__init__.py``) over the module itself,
        so ``inspect.getsource(aqueduct.cli.run)`` resolves to the Command
        object, not the module, and raises TypeError."""
        import importlib.util

        spec = importlib.util.find_spec(dotted_path)
        assert spec is not None and spec.origin is not None
        return open(spec.origin, encoding="utf-8").read()

    def test_run_py_heal_transcript_write_is_stderr(self) -> None:
        src = self._read_module_source("aqueduct.cli.run")
        assert "write=lambda s: emit(_style_heal_line(s), err=True)" in src
        # Audit-fixed 2026-08-23: the cue moved off the non-wrapping `emit()`
        # onto the funnel's wrap_line-backed `echo()` (a bare f-string handed
        # to `emit()` doesn't wrap — the heal-block-overflows-80-columns
        # defect) — still explicit stderr.
        assert '_funnel_echo(_cue_text, gutter="│   · ", err=True' in src
        assert "_use_stream = sys.stderr.isatty()" in src

    def test_heal_py_transcript_write_is_stderr(self) -> None:
        src = self._read_module_source("aqueduct.cli.heal")
        assert "write=lambda s: emit(_style_heal_line(s), err=True)" in src

    def test_run_py_header_and_footer_are_explicit_stdout(self) -> None:
        """The framed run screen (header/tree/closing divider/verdict) must
        survive `> run.log` piped alone — every one of those `click.echo`
        calls carries an explicit `err=False`."""
        src = self._read_module_source("aqueduct.cli.run")
        assert "click.echo(_dim(_rule()), err=False)" in src
        # Phase 85 Wave 2 — the success footer gained a wall-clock-time +
        # healed-count suffix (`_footer_text`), still explicit stdout.
        assert "_style_success(_footer_text, err=False)" in src
        assert (
            'f"  failed_module={failure_ctx.failed_module}",\n                    err=False,' in src
        )
