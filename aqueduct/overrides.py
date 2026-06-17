"""``-s/--set PATH=VALUE`` config overrides — a single, repeatable flag that
replaces the scattered one-off override flags (``--master``, ``--provider``,
``--base-url``, ``--timeout``, …).

Semantics:

  - **In-memory only.** An override mutates the loaded config / blueprint for
    *this invocation*. Nothing is ever written back to ``aqueduct.yml`` or the
    Blueprint file — same lifetime as ``--ctx``.
  - **Top precedence.** ``--set`` is applied as the final overlay, on top of the
    already-merged result, so a path wins regardless of which file originally
    set it: ``--set > blueprint agent: > aqueduct.yml > built-in defaults``.
  - **One flat namespace.** A dotted path addresses whichever schema owns the
    field. ``agent.*`` is split between the blueprint (`approval_mode`,
    `max_patches`, guardrails, …) and the engine config (`budget`, `retry`,
    connection defaults); each path is routed to the schema that declares it.

Value grammar:

  - ``path=value``   → ``value`` is coerced: ``true``/``false`` → bool,
    ``null``/``none`` → None, then int, then float, else the literal string.
  - ``path:=value``  → ``value`` is parsed as JSON (objects, arrays, typed
    scalars) — for structured values that the scalar coercion can't express.
"""

from __future__ import annotations

import json
import types
import typing
from aqueduct.errors import AqueductError
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any

from pydantic import BaseModel


class OverrideError(AqueductError):
    """Raised for a malformed ``--set`` item or a path no schema accepts."""


@dataclass(frozen=True)
class Override:
    path: tuple[str, ...]   # dotted path split into segments
    value: Any              # already coerced (scalar) or JSON-parsed
    raw: str                # original "path=value" item, for error messages


# ── parsing + coercion ────────────────────────────────────────────────────────

def _coerce_scalar(token: str) -> Any:
    """Coerce a bare ``--set`` value: bool/null first, then int, then float, else str."""
    low = token.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    # int before float so "5" stays an int. Reject things like "1_0" / "0x1"
    # that int() would otherwise accept but a user almost never means.
    if token.lstrip("-").isdigit():
        try:
            return int(token)
        except ValueError:  # pragma: no cover
            pass
    try:
        return float(token)
    except ValueError:
        return token


def parse_set_items(items: typing.Iterable[str]) -> list[Override]:
    """Parse ``--set`` CLI items into coerced :class:`Override` records."""
    out: list[Override] = []
    for item in items:
        eq = item.find("=")
        if eq <= 0:
            raise OverrideError(
                f"--set must be PATH=VALUE (or PATH:=JSON), got: {item!r}"
            )
        if item[eq - 1] == ":":           # PATH:=JSON
            path_str, raw_val, is_json = item[: eq - 1], item[eq + 1:], True
        else:                              # PATH=scalar
            path_str, raw_val, is_json = item[:eq], item[eq + 1:], False
        segments = tuple(s for s in path_str.split("."))
        if not path_str or any(not s for s in segments):
            raise OverrideError(f"--set path is empty or malformed: {item!r}")
        if is_json:
            try:
                value = json.loads(raw_val)
            except json.JSONDecodeError as exc:
                raise OverrideError(f"--set {path_str}:= invalid JSON: {exc}") from exc
        else:
            value = _coerce_scalar(raw_val)
        out.append(Override(path=segments, value=value, raw=item))
    return out


# ── schema introspection (routing + suggestions) ───────────────────────────────

_FREEFORM = object()   # sentinel: a dict[str, Any] node accepts arbitrary keys


def _unwrap(annotation: Any) -> Any:
    """Resolve a field annotation to a BaseModel subclass, ``_FREEFORM`` (open
    dict / Any), or None (a leaf scalar type)."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, getattr(types, "UnionType", ())):
        # Optional[X] / X | None — recurse into the non-None members.
        for arg in typing.get_args(annotation):
            if arg is type(None):
                continue
            got = _unwrap(arg)
            if got is not None:
                return got
        return None
    if origin in (list, tuple, set):
        args = typing.get_args(annotation)
        return _unwrap(args[0]) if args else None
    if origin is dict:
        return _FREEFORM
    if annotation is Any:
        return _FREEFORM
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    return None


def _field_names(model_cls: type[BaseModel]) -> set[str]:
    """All field names + alias choices a model accepts as input."""
    names: set[str] = set()
    for fname, fld in model_cls.model_fields.items():
        names.add(fname)
        alias = getattr(fld, "validation_alias", None)
        if isinstance(alias, str):
            names.add(alias)
        else:  # AliasChoices
            for choice in getattr(alias, "choices", []) or []:
                if isinstance(choice, str):
                    names.add(choice)
    return names


def model_accepts_path(model_cls: type[BaseModel], path: tuple[str, ...]) -> bool:
    """True if ``model_cls``'s schema declares the full dotted ``path``."""
    cur: Any = model_cls
    for seg in path:
        if cur is _FREEFORM:
            return True                       # inside an open dict — anything goes
        if not (isinstance(cur, type) and issubclass(cur, BaseModel)):
            return False                      # path goes deeper than a leaf scalar
        if seg not in _field_names(cur):
            return False
        cur = _unwrap(cur.model_fields[seg].annotation) if seg in cur.model_fields else _FREEFORM
    return True


def suggest_for_path(model_classes: typing.Sequence[type[BaseModel]], path: tuple[str, ...]) -> str | None:
    """Suggest the nearest valid sibling for the deepest resolvable segment.

    Walks each candidate root as far as the path resolves, then fuzzy-matches the
    first unknown segment against the field names available at that depth.
    """
    best_depth = -1
    bad_seg: str | None = None
    candidates: set[str] = set()
    for root in model_classes:
        cur: Any = root
        for depth, seg in enumerate(path):
            if not (isinstance(cur, type) and issubclass(cur, BaseModel)):
                break
            names = _field_names(cur)
            if seg not in names:
                # Keep the deepest failure; union sibling names across roots
                # that fail at the same depth on the same segment (e.g. the
                # split engine-config vs blueprint `agent.*` namespace).
                if depth > best_depth:
                    best_depth, bad_seg, candidates = depth, seg, set(names)
                elif depth == best_depth and seg == bad_seg:
                    candidates |= names
                break
            cur = _unwrap(cur.model_fields[seg].annotation) if seg in cur.model_fields else _FREEFORM
    if best_depth < 0 or bad_seg is None:
        return None
    depth, bad, candidate_list = best_depth, bad_seg, sorted(candidates)
    matches = get_close_matches(bad, candidate_list, n=3, cutoff=0.5)
    candidates = candidate_list  # for the fall-through sample below
    prefix = ".".join(path[:depth])
    where = f" under {prefix!r}" if prefix else ""
    if matches:
        return f"unknown segment {bad!r}{where} — did you mean: {', '.join(matches)}?"
    sample = ", ".join(candidates[:8])
    return f"unknown segment {bad!r}{where} — valid keys: {sample}{' …' if len(candidates) > 8 else ''}"


# ── nesting + merge + apply ─────────────────────────────────────────────────────

def to_nested(overrides: typing.Iterable[Override]) -> dict[str, Any]:
    """Fold a list of overrides into one nested dict."""
    root: dict[str, Any] = {}
    for ov in overrides:
        node = root
        for seg in ov.path[:-1]:
            nxt = node.get(seg)
            if not isinstance(nxt, dict):
                nxt = {}
                node[seg] = nxt
            node = nxt
        node[ov.path[-1]] = ov.value
    return root


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` into a copy of ``base`` (overlay wins)."""
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def apply_to_model(model_instance: BaseModel, nested: dict[str, Any]) -> BaseModel:
    """Return a new validated model with ``nested`` overlaid (top precedence)."""
    if not nested:
        return model_instance
    data = model_instance.model_dump(mode="python")
    merged = deep_merge(data, nested)
    try:
        return type(model_instance).model_validate(merged)
    except Exception as exc:  # pydantic ValidationError
        raise OverrideError(f"--set produced an invalid config: {exc}") from exc


def route_overrides(
    items: typing.Iterable[str], *, allow_blueprint: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Parse ``--set`` items and split them into ``(config_nested, blueprint_nested)``.

    A path is routed to the Blueprint when ``allow_blueprint`` and the
    ``BlueprintSchema`` declares it — so a field shared with the engine config
    (e.g. ``agent.timeout``) lands on the blueprint, which already wins the
    merge. Otherwise it is routed to the engine ``AqueductConfig`` if that
    declares it. A path no schema accepts raises :class:`OverrideError` with a
    nearest-sibling suggestion.
    """
    from aqueduct.config import AqueductConfig
    from aqueduct.parser.schema import BlueprintSchema

    config_ov: list[Override] = []
    blueprint_ov: list[Override] = []
    for ov in parse_set_items(items):
        if allow_blueprint and model_accepts_path(BlueprintSchema, ov.path):
            blueprint_ov.append(ov)
        elif model_accepts_path(AqueductConfig, ov.path):
            config_ov.append(ov)
        else:
            roots = [AqueductConfig, BlueprintSchema] if allow_blueprint else [AqueductConfig]
            hint = suggest_for_path(roots, ov.path)
            raise OverrideError(
                f"--set {ov.raw!r}: no config field at path "
                f"{'.'.join(ov.path)!r}" + (f" — {hint}" if hint else "")
            )
    return to_nested(config_ov), to_nested(blueprint_ov)
