#!/usr/bin/env python3
"""Thin wrapper over the SHIPPED capability tooling — `aqueduct dev capabilities`.

The implementation lives in the installed package (`aqueduct/executor/
capability_tooling.py`, surfaced as `aqueduct dev capabilities …`), not here.
`scripts/` is not in the wheel, so a third-party engine author who `pip install`s
aqueduct never sees this file — and generating a capability declaration is the
very first thing they need to do. There is exactly ONE implementation; this
script only forwards to it, so old muscle memory and old links keep working.

    python scripts/capabilities.py check      ==  aqueduct dev capabilities check
    python scripts/capabilities.py sync       ==  aqueduct dev capabilities sync
    python scripts/capabilities.py docs       ==  aqueduct dev capabilities docs
    python scripts/capabilities.py scaffold --engine duckdb
                                              ==  aqueduct dev capabilities scaffold --engine duckdb
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from aqueduct.cli import cli  # noqa: E402

if __name__ == "__main__":
    cli(args=["dev", "capabilities", *sys.argv[1:]], prog_name="aqueduct dev capabilities")
