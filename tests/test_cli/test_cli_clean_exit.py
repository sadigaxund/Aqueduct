"""The CLI's last-resort exception net must not swallow click's clean-exit signal.

`_AqueductCLIGroup.invoke` wraps the whole dispatch in a broad `except Exception`
so nothing escapes as a bare traceback (see the class docstring). The bug this
file pins: `click.exceptions.Exit` — the object click raises to request a
SUCCESSFUL exit, e.g. after `--help` printed — derives from `RuntimeError`, NOT
from `SystemExit`, so it fell into that broad clause. Every subcommand `--help`
and the bare `aqueduct` banner therefore reported

    exit=2   stderr: ✗ unexpected error: 0

where the `0` is `str(Exit(0))`: the exit code the command ASKED for, rendered
as if it were an error message.

`aqueduct --help` / `--version` were unaffected because click resolves the ROOT
group's eager params before `Group.invoke` is entered — which is why the two
smoke checks everyone runs looked clean while every subcommand was broken.

The positive control at the bottom is load-bearing: without it, deleting the net
entirely would make every other test in this file pass.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

# Subcommands whose --help must exit cleanly. Deliberately spans several
# modules (benchmark.py, run.py, dev.py, patch.py, doctor lives in __init__)
# so a regression in any one command module is visible here.
_HELP_SUBCOMMANDS = ["benchmark", "run", "dev", "patch", "doctor", "heal"]


def _invoke(args: list[str]):
    return CliRunner().invoke(cli, args)


@pytest.mark.parametrize("sub", _HELP_SUBCOMMANDS)
def test_subcommand_help_exits_zero_without_error_text(sub: str) -> None:
    res = _invoke([sub, "--help"])
    assert res.exit_code == 0, f"`aqueduct {sub} --help` exited {res.exit_code}"
    assert "unexpected error" not in res.output
    assert "Usage:" in res.output


def test_root_help_exits_zero() -> None:
    res = _invoke(["--help"])
    assert res.exit_code == 0
    assert "unexpected error" not in res.output


def test_bare_aqueduct_prints_banner_and_exits_zero() -> None:
    """Bare `aqueduct` is the one command that calls `ctx.exit()` itself."""
    res = _invoke([])
    assert res.exit_code == 0, f"bare `aqueduct` exited {res.exit_code}"
    assert "unexpected error" not in res.output
    assert "Usage:" in res.output


def test_ctx_exit_with_a_nonzero_code_is_not_rewritten() -> None:
    """The general class of bug: a command asking for exit code N via
    ``ctx.exit(N)`` must get N, not DATA_OR_RUNTIME.

    No shipped command does this today (every other command uses
    ``sys.exit``), so this guards the seam rather than a current call site —
    the net rewrote ANY ``Exit``, so the moment someone writes
    ``ctx.exit(HEAL_PENDING)`` it would silently become 2.
    """

    @cli.command("_aq_test_ctx_exit")
    @click.pass_context
    def _ctx_exit_cmd(ctx: click.Context) -> None:
        ctx.exit(exit_codes.HEAL_PENDING)

    try:
        res = _invoke(["_aq_test_ctx_exit"])
    finally:
        cli.commands.pop("_aq_test_ctx_exit", None)

    assert res.exit_code == exit_codes.HEAL_PENDING
    assert "unexpected error" not in res.output


def test_net_still_catches_a_genuine_unexpected_exception() -> None:
    """POSITIVE CONTROL — the net must keep doing its job.

    Deleting `_AqueductCLIGroup.invoke` outright would make every other test
    in this file pass; this one goes red.
    """

    @cli.command("_aq_test_boom")
    def _boom_cmd() -> None:
        raise RuntimeError("boom-from-a-subcommand")

    try:
        res = _invoke(["_aq_test_boom"])
    finally:
        cli.commands.pop("_aq_test_boom", None)

    assert res.exit_code == exit_codes.DATA_OR_RUNTIME
    assert "unexpected error: boom-from-a-subcommand" in res.output
