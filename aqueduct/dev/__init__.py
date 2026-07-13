"""Dev-tooling package — extension-seam scaffolding (`aqueduct dev scaffold`).

Ships in the wheel on purpose: an extension author (custom probe signal, custom
Assert rule, UDF, Python DataSource, secrets resolver, or a whole engine) has a
`pip install`, not a checkout of this repository. Anything they need to start
must therefore be a command, not a file in `scripts/`.

Rendering lives in `aqueduct/cli/dev.py`; the generators here return values.
"""

from __future__ import annotations

from aqueduct.dev.scaffolds import KINDS, Scaffold, render, write

__all__ = ["KINDS", "Scaffold", "render", "write"]
