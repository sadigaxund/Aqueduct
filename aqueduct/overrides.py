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
    Engine/session config (``engine.<name>.*``) is resolved by a separate
    merge that layers the Blueprint's own ``engine.<name>:`` block over the
    ``aqueduct.yml`` one, so the overlay alone would leave ``--set`` BELOW a
    healed Blueprint value there. ``apply_to_model`` therefore also pins the
    engine-config subset as an explicit third layer above the Blueprint —
    same ``--set`` wins everywhere rule, one extra layer to express it. See
    ``aqueduct.executor.session_config.resolve_session_engine_config``.
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

import dataclasses
import json
import types
import typing
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any

from pydantic import BaseModel, ValidationError

from aqueduct.errors import AqueductError


class OverrideError(AqueductError):
    """Raised for a malformed ``--set`` item or a path no schema accepts."""


@dataclass(frozen=True)
class Override:
    path: tuple[str, ...]  # dotted path split into segments
    value: Any  # already coerced (scalar) or JSON-parsed
    raw: str  # original "path=value" item, for error messages


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
    # float() accepts the same underscore grouping ("1_000" -> 1000.0) plus
    # "inf"/"nan" spellings — none of which a `--set` user means as a number.
    # Reject those before the float() call so they fall through to the
    # literal-string branch, same intent as the int-form guard above.
    if "_" in token or low in (
        "inf",
        "-inf",
        "+inf",
        "infinity",
        "-infinity",
        "+infinity",
        "nan",
        "-nan",
        "+nan",
    ):
        return token
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
            raise OverrideError(f"--set must be PATH=VALUE (or PATH:=JSON), got: {item!r}")
        if item[eq - 1] == ":":  # PATH:=JSON
            path_str, raw_val, is_json = item[: eq - 1], item[eq + 1 :], True
        else:  # PATH=scalar
            path_str, raw_val, is_json = item[:eq], item[eq + 1 :], False
        segments = tuple(s for s in path_str.split("."))
        if not path_str or any(not s for s in segments):
            # Report the PATH only, never `item` — it embeds the (possibly
            # secret) value, which is unregistered at this point and would
            # bypass the click.echo redaction wrapper (that only scrubs
            # values registered via @aq.secret()).
            raise OverrideError(f"--set path is empty or malformed: {path_str!r}")
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

_FREEFORM = object()  # sentinel: a dict[str, Any] node accepts arbitrary keys


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
            return True  # inside an open dict — anything goes
        if not (isinstance(cur, type) and issubclass(cur, BaseModel)):
            return False  # path goes deeper than a leaf scalar
        if seg not in _field_names(cur):
            return False
        cur = _unwrap(cur.model_fields[seg].annotation) if seg in cur.model_fields else _FREEFORM
    return True


def _dict_value_is_model(annotation: Any) -> bool:
    """True when *annotation* is a ``dict[...]`` whose VALUE type is a
    pydantic model (``stores.depots: dict[str, DepotMountConfig]``) rather
    than a scalar/``Any`` bag (``engine.spark.conf: dict[str, Any]``)."""
    if typing.get_origin(annotation) is not dict:
        return False
    args = typing.get_args(annotation)
    return bool(args) and _unwrap(args[-1]) not in (None, _FREEFORM)


def freeform_key_boundary(model_cls: type[BaseModel], path: tuple[str, ...]) -> int | None:
    """Index in *path* at which the remaining segments address ONE key
    inside a free-form scalar-valued dict field, or None when *path* never
    enters one.

    ``engine.spark.conf`` is ``dict[str, Any]``, and Spark's own key names
    are themselves dotted (``spark.sql.shuffle.partitions``). Splitting the
    whole ``--set`` path on every dot therefore built a DEEP nested dict
    (``conf["spark"]["sql"]["shuffle"]["partitions"]``) instead of the one
    flat key the user meant, and because the field is ``dict[str, Any]``
    pydantic accepted it without complaint — the session was then configured
    with a key literally named ``spark`` whose value was a dict. A silent
    wrong answer, so it is fixed here rather than documented.

    A dict whose values are MODELS (``stores.depots: dict[str,
    DepotMountConfig]``) is deliberately NOT a boundary: there the segments
    after the dict really are a key followed by that model's own fields, and
    collapsing them would break paths that work today.
    """
    cur: Any = model_cls
    for idx, seg in enumerate(path):
        if not (isinstance(cur, type) and issubclass(cur, BaseModel)):
            return None
        fld = cur.model_fields.get(seg)
        if fld is None:
            return None
        annotation = fld.annotation
        if _dict_value_is_model(annotation):
            return None
        nxt = _unwrap(annotation)
        if nxt is _FREEFORM:
            return idx + 1
        if nxt is None:
            return None
        cur = nxt
    return None


def collapse_freeform_tail(model_cls: type[BaseModel], path: tuple[str, ...]) -> tuple[str, ...]:
    """*path* with everything past a free-form-dict boundary rejoined into a
    single dotted key. A path with no such boundary is returned unchanged."""
    boundary = freeform_key_boundary(model_cls, path)
    if boundary is None or len(path) - boundary < 2:
        return path
    return path[:boundary] + (".".join(path[boundary:]),)


def suggest_for_path(
    model_classes: typing.Sequence[type[BaseModel]], path: tuple[str, ...]
) -> str | None:
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
            cur = (
                _unwrap(cur.model_fields[seg].annotation) if seg in cur.model_fields else _FREEFORM
            )
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
    """Return a new validated model with ``nested`` overlaid (top precedence).

    For an ``AqueductConfig`` the overlay alone does NOT deliver "top
    precedence", and used not to: engine config is resolved as a merge of
    the ``aqueduct.yml`` layer and the Blueprint's own ``engine.<name>:``
    block, with the Blueprint winning, so a ``--set`` written into the
    ``aqueduct.yml`` layer lost to any value a heal had written into the
    Blueprint. The returned config therefore also carries the engine-config
    subset of *nested* as an explicit third layer ABOVE the Blueprint (see
    ``AqueductConfig.cli_engine_overrides`` and
    ``aqueduct.executor.session_config.resolve_session_engine_config``).

    Pinning happens here rather than at each CLI call site on purpose: this
    is the one function every config ``--set`` already flows through
    (``run``, ``heal``, both ``benchmark`` paths), so no call site can
    forget it and silently drop the user's flag.
    """
    if not nested:
        return model_instance
    data = model_instance.model_dump(mode="python")
    merged = deep_merge(data, nested)
    try:
        out = type(model_instance).model_validate(merged)
    except ValidationError as exc:
        raise OverrideError(f"--set produced an invalid config: {exc}") from exc
    return _pin_cli_engine_layers(out, nested)


def _pin_cli_engine_layers(model_instance: BaseModel, nested: dict[str, Any]) -> BaseModel:
    """Record *nested*'s engine-config keys on an ``AqueductConfig`` as the
    top session-config layer. Any other model is returned untouched — the
    Blueprint has no session-config merge to sit on top of."""
    from aqueduct.config import AqueductConfig

    if not isinstance(model_instance, AqueductConfig):
        return model_instance
    from aqueduct.executor.session_config import cli_engine_config_layers

    return model_instance.with_cli_engine_overrides(
        cli_engine_config_layers(model_instance, nested)
    )


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
            blueprint_ov.append(
                dataclasses.replace(ov, path=collapse_freeform_tail(BlueprintSchema, ov.path))
            )
        elif model_accepts_path(AqueductConfig, ov.path):
            config_ov.append(
                dataclasses.replace(ov, path=collapse_freeform_tail(AqueductConfig, ov.path))
            )
        else:
            roots = [AqueductConfig, BlueprintSchema] if allow_blueprint else [AqueductConfig]
            hint = suggest_for_path(roots, ov.path)
            # Report the PATH only, never `ov.raw`/the value — a typo'd key
            # (e.g. `--set agent.api_ke=<secret>`) would otherwise echo the
            # value unredacted: it is unregistered at this point (redaction
            # only scrubs values registered via @aq.secret() on successful
            # resolution) so the click.echo redaction wrapper can't catch it.
            raise OverrideError(
                f"--set: no config field at path "
                f"{'.'.join(ov.path)!r}" + (f" — {hint}" if hint else "")
            )
    return to_nested(config_ov), to_nested(blueprint_ov)
