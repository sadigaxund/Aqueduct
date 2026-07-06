"""User-code module loading — the ONE way to import Blueprint-referenced code.

Any feature that imports **user-authored** code from a Blueprint/config
reference (a dotted ``fn:``, a ``module:`` + ``entry:`` pointer, a
``module.Class`` path) must resolve it through :func:`load_module` /
:func:`load_callable` with the manifest's ``base_dir`` — never a bare
``importlib.import_module``. Bare imports only search ``sys.path``, and the
``aqueduct`` console script never has the blueprint's directory there — a
sibling ``.py`` next to the blueprint is invisible. This exact bug shipped
independently 5 times (secrets resolver, custom Assert, custom Probe pointer,
python UDF, custom DataSource) before being fixed once here.

When ``base_dir`` is given and ``<base_dir>/<module/path>.py`` exists, the
module is loaded directly from that file via
``importlib.util.spec_from_file_location`` and is never registered in
``sys.modules`` — this makes it immune to colliding with an unrelated module
of the same name already loaded elsewhere in the process (e.g. a resolver
package named ``secrets`` colliding with the stdlib ``secrets`` module). The
module is re-loaded on every call (no caching) — a nice side effect is that
editing the file takes effect on the next call with no restart.

Caveat: the file-path load only covers a single flat file. If the loaded
module itself does a relative (``from . import x``) or sibling-package
import, that inner import still goes through the normal ``sys.path``-based
import system and is not covered by this collision-proofing — keep custom
callables self-contained in one file.

When ``base_dir`` is not set, or no file exists at the derived path (e.g. the
path names an installed package, or a module already reachable via
``sys.path``), falls back to the normal ``importlib.import_module`` —
``base_dir`` is passed unconditionally by callers (config/blueprint
directory), so this must stay a fallback, not a hard requirement, for
callables that aren't a file sitting next to the blueprint.

Pure stdlib (``importlib`` + ``pathlib``) — no pyspark, no domain imports.
``executor/probe_plugins.py`` (pyspark-free by charter) is a consumer.
"""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any


def load_module(module_path: str, base_dir: str | None = None) -> ModuleType:
    """Load ``module_path``, preferring a file under ``base_dir`` (see module docstring).

    ``base_dir=""`` is treated the same as ``None``: the Manifest field
    defaults to ``""``, and here the empty string genuinely means "not set" —
    so the falsy check is the correct semantic, not a falsy-trap.
    """
    file_path = Path(base_dir) / (module_path.replace(".", "/") + ".py") if base_dir else None
    if file_path is not None and file_path.is_file():
        spec = importlib.util.spec_from_file_location(module_path, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"could not load spec for {file_path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return importlib.import_module(module_path)


def load_callable(dotted_path: str, base_dir: str | None = None) -> Callable[..., Any]:
    """Resolve ``module.callable`` (or ``pkg.module.callable``) to the callable.

    ``rsplit('.', 1)`` then :func:`load_module` + ``getattr``. Raises
    ``ValueError`` when the path has no dot, ``ImportError`` when the module
    can't be loaded, ``AttributeError`` when the attribute is missing —
    callers wrap these into their own domain error types.
    """
    module_path, fn_name = dotted_path.rsplit(".", 1)
    mod = load_module(module_path, base_dir)
    return getattr(mod, fn_name)
