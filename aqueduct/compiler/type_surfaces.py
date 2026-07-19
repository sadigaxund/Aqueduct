"""Shared walker for the three inventoried type-string surfaces (Phase 80).

A Blueprint carries type spellings in exactly three places: Channel
``op: cast`` columns, Ingress ``schema_hint`` fields, and UDF
``return_type``. Two independent compile-time passes need to find every one
of those spellings:

  - ``compiler.compiler.compile()`` step 8h — VALIDATES each spelling parses
    (``aqueduct.typehub.parse_type``), a hard ``CompileError`` on garbage.
  - ``aqueduct.compiler.capability_check.leaves_for_module`` /
    ``type_leaves_for_manifest`` (Phase 80 work package 2) — maps each
    spelling's PARSED hub type to the ``type.*`` capability leaves it uses,
    for the engine capability gate.

Both need the same "where do the type strings live in this module's config /
this udf_registry entry" extraction — the dict-vs-list ``columns:`` shape,
the dict-vs-list ``schema_hint:`` shape. Duplicating that shape-parsing in
both call sites is exactly the drift risk this framework exists to prevent
(a new schema_hint shape variant fixed in one call site and silently missed
in the other). This module is the single source of truth for "where are the
type strings", shared by both; each caller still makes its OWN
``typehub.parse_type`` call for its own purpose (validation vs. leaf
derivation) — extraction is shared, interpretation is not.

Returns ``(where, spelling)`` pairs: ``where`` is a human-readable label
naming the surface (module + field), matching the error-attribution strings
step 8h already produced before this module existed.
"""

from __future__ import annotations

from typing import Any

from aqueduct.parser.models import ModuleType


def module_type_spellings(module: Any) -> list[tuple[str, str]]:
    """Every ``(where, spelling)`` pair from ONE module's cast columns /
    schema_hint fields. Empty for any other module type or op.
    """
    out: list[tuple[str, str]] = []
    cfg = module.config if isinstance(module.config, dict) else {}
    mtype = str(getattr(module, "type", ""))

    if mtype == ModuleType.Channel and cfg.get("op") == "cast":
        columns = cfg.get("columns")
        if isinstance(columns, dict):
            for col_name, col_type in columns.items():
                if col_type is not None:
                    out.append((f"Channel {module.id!r} op=cast column {col_name!r}", str(col_type)))
        elif isinstance(columns, list):
            for item in columns:
                if not isinstance(item, dict):
                    continue
                col_name = item.get("column") or item.get("name")
                col_type = item.get("type")
                if col_type is not None:
                    out.append((f"Channel {module.id!r} op=cast column {col_name!r}", str(col_type)))
    elif mtype == ModuleType.Ingress:
        hint_raw = cfg.get("schema_hint")
        hint_fields: list = []
        if isinstance(hint_raw, dict):
            if "columns" in hint_raw:
                cols = hint_raw.get("columns")
                if isinstance(cols, list):
                    hint_fields = cols
            else:
                hint_fields = [{"name": k, "type": v} for k, v in hint_raw.items()]
        elif isinstance(hint_raw, list):
            hint_fields = hint_raw
        for field_hint in hint_fields:
            if not isinstance(field_hint, dict):
                continue
            ftype = field_hint.get("type")
            if ftype is not None:
                out.append(
                    (f"Ingress {module.id!r} schema_hint field {field_hint.get('name')!r}", str(ftype))
                )
    return out


def udf_return_type_spellings(udf_registry: Any) -> list[tuple[str, str]]:
    """Every ``(where, spelling)`` pair from the manifest's ``udf_registry``
    ``return_type`` entries — manifest-scoped, like
    ``capability_check.feature_leaves_for_manifest``: a UDF is declared once
    and referenced from SQL across any number of Channels, not owned by one
    module.
    """
    out: list[tuple[str, str]] = []
    for udf_entry in udf_registry or ():
        if not isinstance(udf_entry, dict):
            continue
        rt = udf_entry.get("return_type")
        if rt is not None:
            out.append((f"UDF {udf_entry.get('id', '?')!r} return_type", str(rt)))
    return out


__all__ = ["module_type_spellings", "udf_return_type_spellings"]
