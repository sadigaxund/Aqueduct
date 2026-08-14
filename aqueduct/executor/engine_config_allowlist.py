"""Per-engine healing config-key allowlist — pure data + loader, zero pyspark.

**Why this exists.** The LLM healing loop can emit a ``set_engine_config``
patch op that writes into the Blueprint's ``engine.<name>`` block
(``engine.spark.conf.<vendor.key>``, ``engine.duckdb.memory_limit``, …).
That block is Blueprint-level session/tuning config, NOT Blueprint grammar —
``aqueduct/executor/capability_leaves.py``'s ``_SCHEMA_BLOCKS`` walker never
included ``SparkEngineBlockSchema``/``DuckDBEngineBlockSchema``, so
``engine.spark.conf`` has no capability leaf at all. Nothing in the existing
capability framework constrains which key a patch may touch. This module is
the missing constraint: each engine ships an explicit allowlist of the
key/value shapes ``set_engine_config`` may write, loaded from
``engine_config_allowlist.yml`` next to that engine's ``capabilities.yml``.

**Scope, deliberately narrow (see the module's originating task).** This
file builds the DATA + the LOADER/VALIDATOR + DISCOVERY + a PRESENCE guard.
It does **not** wire an enforcement gate against a real patch — nothing here
is called from ``aqueduct/patch/``. That is a separate, later task. Until
that gate exists, a malformed shipped allowlist is caught by
``tests/test_executor/test_engine_config_allowlist.py`` loading every
SHIPPED file through ``load_allowlist()`` (i.e. the "build" that must never
go red is CI, not a live heal) — never by a user hitting it mid-heal, because
nothing consumes this file at heal time yet.

**Two shapes, one rule — not hardcoded per engine.** ``engine.<name>:``
blocks come in two shapes (``aqueduct/parser/schema.py``), and the same
structural question ``aqueduct.parser.parser._resolve_engine_block_raw`` and
``aqueduct.patch.operations.apply_set_engine_config`` already ask — does the
engine's own ``EngineBlockSchema`` field declare a ``conf`` sub-field? —
decides how this module validates and matches entries too:

  - **Free-form bag** (today: only Spark's ``SparkEngineBlockSchema``) —
    thousands of possible vendor keys, so an entry's ``pattern`` is a GLOB
    (``fnmatch`` semantics; a plain string with no ``*``/``?`` is just the
    exact-match special case). A glob here is an ALLOWLIST, not a denylist:
    fails closed, so a key nobody listed is refused until a human adds a
    row. This is authorization, not classification — it does not contradict
    AGENTS.md's "classify by what you exclude" rule, which governs
    capability-leaf CLASSIFICATION, not this closed-world grant list.
  - **Typed fields** (today: only DuckDB's ``DuckDBEngineBlockSchema``) — the
    block declares named fields directly (``memory_limit``, ``threads``), so
    an entry's ``pattern`` must be an EXACT field name (a glob is rejected —
    there is nothing for it to usefully match against a closed field set),
    and its declared ``type`` is cross-checked against that pydantic field's
    own annotation at load time.

**Entry shape.** Each entry carries ``pattern`` + ``type`` + an optional
``range`` (2-element ``[min, max]``, inclusive, for ``int``/``float``/
``size``) or ``enum`` (non-empty list, for any type). ``range``/``enum`` are
mutually exclusive. Legal ``type`` values: ``int``, ``float``, ``bool``,
``str``, and ``size`` (a JVM/DuckDB-style byte-size string such as ``"4g"``
or ``"512MB"`` — a TYPE, not a delta-bound; range bounds for a ``size``
entry are themselves size strings, compared in bytes). Deliberately no
richer constraint language (no "no more than 4x the current value") until a
real incident asks for one — see the originating task's explicit steer
against delta-bounds.

**``range``/``enum`` are a MECHANISM, not a mandate.** A bound core cannot
derive (the memory/partition-size ceiling a heal should respect depends on
the cluster it runs on, which this process never observes) is a bound core
must not state, so the two shipped files carry no invented magnitude
``range``. They still carry ``spark.serializer``'s ``enum`` — a closed set
of legal Spark classes is a shape fact, not a guessed number. The loader
keeps validating ``range``/``enum`` in full for a future operator-narrowing
feature to use; an empty set of ranges today is a data fact about this
release, not a capability that was removed.

**Deny layer — core-only, immutable.** Alongside ``entries:``, a file may
carry a ``deny:`` list: named key/family patterns (glob or exact, same
``fnmatch`` semantics as an allow entry) that no allow entry — present or a
future one — may cover, each with a mandatory ``reason`` that renders into
the eventual patch-rejection message. This is the enforced replacement for
what used to be only YAML comments describing "excluded families" — a
comment cannot stop a future row from allowlisting ``spark.master``; a deny
row can. Two kinds:

  - **Absolute ban** (no ``deny_values``): the pattern is refused
    regardless of value. ``load_allowlist()`` itself refuses to load a file
    where any allow entry's pattern is reachable through one of these —
    derived via ``_glob_canary`` (see below) — so a shipped file that lets
    an allow row re-admit a denied family fails the same CI load test every
    other malformed file fails, not a live heal.
  - **Scoped value-ban** (``deny_values`` set): the key stays allowed —
    there is normally a matching allow entry for the same pattern — but a
    fixed set of sentinel values on it are refused (``spark.driver.
    maxResultSize``'s ``0`` = "unlimited", excluded so raising the cap
    stays possible while removing the guard does not). A scoped row is
    exempt from the overlap check above by design: coexisting with an
    allow entry on the same key is exactly its job, not a bug.

Deny patterns are exempt from the typed-field-engine "pattern must name an
existing declared field" check allow entries get (see DuckDB's
``database_path``/``extension_repository``/``s3_*`` rows, none of which
exist as fields on ``DuckDBEngineBlockSchema`` today) — a deny row's job
can be guarding a field that does not exist YET, not describing one that
does.

``_glob_canary(pattern)`` derives one concrete representative key from a
pattern: the pattern unchanged when it has no ``*``/``?``, or the pattern
with every ``*``/``?`` replaced by a single fixed placeholder segment when
it does. This is deliberately not exhaustive — it proves an allow entry
broad enough to swallow the WHOLE family the deny pattern denotes, not that
no differently-shaped allow entry could reach some other member of that
family through a route the placeholder didn't happen to hit. In practice
every allow-side glob in these two shipped files is narrow (a single fixed
suffix/prefix), so the one-canary check is exact here; a future allow entry
with a more elaborate glob would need a human to notice the same way any
allow/deny design review would.

**Immutability, encoded, not just documented.** The deny layer is
core-only. ``load_allowlist()`` is the ONLY function that ever populates
``EngineConfigAllowlist.deny_entries``, always by reading the ONE file this
engine ships next to its ``capabilities.yml`` — there is no parameter
anywhere in this module to merge, append to, override, or shrink a loaded
allowlist's deny entries, and ``EngineConfigAllowlist``/``DenyEntry`` are
frozen dataclasses with no post-construction mutation path.
``validate_against_deny()`` — the contract a future operator-extension
config surface (a prospective ``danger.`` flag or similar) MUST validate a
candidate entry through before it can be considered, pinned now even though
no such extension surface exists yet — only ever reads deny rows off the
``EngineConfigAllowlist`` object it is handed, so an extension cannot
construct a narrower "private" allowlist of its own and pass that in to
dodge a core deny; the only way to obtain a real one is
``load_allowlist()`` against the shipped core file, and this file ships in
the wheel where no user-supplied ``aqueduct.yml`` path can reach it.

**Presence guard.** Every engine ``discover_allowlist_paths()`` can see via
its ``aqueduct.engines`` entry point must ship this file — an ABSENT file is
not the same decision as an EXPLICIT EMPTY one (``entries: []``). An absent
file already fails closed (nothing allowed) exactly like an empty one would,
but only the explicit empty file makes "this engine heals no config" a
decision a human made, visible in the diff, rather than a gap nobody
noticed — the same reasoning that killed the capability framework's
default-verdict sweep (see ``aqueduct/executor/capabilities.py``'s module
docstring). ``check_presence()`` raises ``EngineConfigAllowlistError`` naming
every registered engine with no file.

This module must stay importable without ``pyspark`` (or any other engine's
runtime dependency) installed.
"""

from __future__ import annotations

import fnmatch
import importlib.metadata
import importlib.util
import re
import typing
from dataclasses import dataclass
from pathlib import Path
from types import UnionType
from typing import Any

import yaml

from aqueduct.errors import EngineConfigAllowlistError
from aqueduct.executor.capabilities import AQ_ENGINES_ENTRYPOINT_GROUP
from aqueduct.parser.schema import EngineBlockSchema

DECLARATION_FILENAME = "engine_config_allowlist.yml"

_LEGAL_TYPES = ("int", "float", "bool", "str", "size")

# The Python type each declared `type:` value is checked against — both a
# typed-field entry's cross-check against the pydantic annotation, and a
# range/enum entry's own bound/member shape check. `size` values are strings
# (byte-size literals), so it maps to `str` here; `_size_to_bytes` is the
# separate numeric interpretation used only for range comparison/ordering.
_PY_TYPE_OF: dict[str, type] = {
    "int": int,
    "float": float,
    "bool": bool,
    "str": str,
    "size": str,
}

# JVM/DuckDB-style byte-size literal: a number followed by an optional
# k/m/g/t unit (case-insensitive) and an optional trailing 'b'. Covers both
# Spark's own JVM-config style ("512m", "4g") and DuckDB's ("4GB", "512MB").
_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kKmMgGtT]?)[bB]?\s*$")
_SIZE_MULTIPLIER = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}


def _size_to_bytes(value: str) -> float:
    """Parse a size literal (``"4g"``, ``"512MB"``, ``"1024"``) to bytes.

    Raises:
        ValueError: ``value`` is not a well-formed size literal.
    """
    m = _SIZE_RE.match(value)
    if not m:
        raise ValueError(f"not a valid size literal: {value!r}")
    return float(m.group(1)) * _SIZE_MULTIPLIER[m.group(2).lower()]


def _unwrap_optional(annotation: Any) -> Any:
    """Strip an ``X | None`` (or ``Optional[X]``) wrapper down to ``X``.

    Returns ``None`` (meaning "cannot cross-check") for anything else that
    isn't a plain 2-armed nullable union — this function is only ever asked
    to unwrap the resource-knob fields on ``DuckDBEngineBlockSchema``
    (``str | None`` / ``int | None`` today), never an arbitrary annotation.
    """
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
        return None
    return annotation


@dataclass(frozen=True)
class RangeConstraint:
    """Inclusive ``[min, max]`` bound. For ``type: size`` entries, ``min``/
    ``max`` are size literals (strings); compare via ``_size_to_bytes``."""

    minimum: Any
    maximum: Any


@dataclass(frozen=True)
class EnumConstraint:
    """A closed set of legal values."""

    values: tuple[Any, ...]


@dataclass(frozen=True)
class AllowlistEntry:
    """One allowed key (or key family, for a free-form bag) + its value shape."""

    pattern: str
    value_type: str  # one of _LEGAL_TYPES
    constraint: RangeConstraint | EnumConstraint | None = None

    def matches(self, key: str) -> bool:
        """Whether ``key`` is covered by this entry's pattern.

        ``fnmatch`` semantics — an exact string with no ``*``/``?`` matches
        only itself, so this is a safe superset of plain equality for a
        typed-field entry (whose pattern is always exact-match by
        construction, enforced at load time) and a real glob for a
        free-form-bag entry.
        """
        return fnmatch.fnmatchcase(key, self.pattern)


@dataclass(frozen=True)
class DenyEntry:
    """One CORE, immutable deny rule — never a bound (that's a ``range``'s
    job), only a name/family plus optional discrete forbidden values, with
    a mandatory ``reason`` that renders into the eventual patch-rejection
    message. See the module docstring's "Deny layer" section."""

    pattern: str
    reason: str
    deny_values: tuple[Any, ...] | None = None

    def matches(self, key: str) -> bool:
        """``fnmatch`` semantics — identical rule to ``AllowlistEntry.matches``."""
        return fnmatch.fnmatchcase(key, self.pattern)

    def forbids(self, value: Any = None) -> bool:
        """Whether this entry forbids ``value`` for a key it already
        ``matches()``. No ``deny_values`` means an absolute ban (any value,
        including no value supplied); ``deny_values`` set means only those
        specific sentinels are forbidden — the key otherwise stays allowed."""
        if self.deny_values is None:
            return True
        return value in self.deny_values


def _glob_canary(pattern: str) -> str:
    """Derive one concrete, representative key from a (possibly glob)
    pattern — see the module docstring's "Deny layer" section for what this
    can and cannot prove."""
    if "*" not in pattern and "?" not in pattern:
        return pattern
    return pattern.replace("*", "x").replace("?", "x")


@dataclass(frozen=True)
class EngineConfigAllowlist:
    """One engine's full ``set_engine_config`` allowlist."""

    engine: str
    is_free_form: bool
    entries: tuple[AllowlistEntry, ...] = ()
    deny_entries: tuple[DenyEntry, ...] = ()


def _fail(message: str, *, engine: str, path: Path, keys: list[str] | None = None) -> None:
    raise EngineConfigAllowlistError(message, engine=engine, path=str(path), keys=keys or [])


def load_allowlist(path: Path | str, engine: str) -> EngineConfigAllowlist:
    """Load + HARD-VALIDATE one engine's ``engine_config_allowlist.yml``.

    A malformed or missing file is always ``EngineConfigAllowlistError`` (an
    ``AqueductError``; an engine author is a user of this seam, never a bare
    builtin). See the module docstring for the full validation contract;
    summarized:

      - the file must exist (the presence guard — see ``check_presence``)
      - it must parse to a mapping with ``engine:`` (matching ``engine``)
        and ``entries:`` (a list)
      - each entry: a non-empty ``pattern`` (no duplicates), a legal
        ``type``, and at most one of ``range``/``enum``, shape-checked
        against ``type``
      - shape decided by ``EngineBlockSchema``'s ``conf`` field (see module
        docstring): a typed-field engine's entry ``pattern`` must be an
        EXACT, EXISTING field name whose declared ``type`` matches that
        field's own pydantic annotation; a free-form-bag engine's pattern
        may be a glob
    """
    p = Path(path)
    if not p.is_file():
        _fail(
            f"engine {engine!r} ships no {DECLARATION_FILENAME} at {p}. Every "
            "registered engine must ship this file, even when it heals no "
            "engine config — write an explicit empty one "
            f'("engine: {engine}\\nentries: []\\n") so that is a decision '
            "someone made, not a gap nobody noticed. An absent file already "
            "fails closed (nothing allowed); this only makes that fact visible.",
            engine=engine,
            path=p,
        )

    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise EngineConfigAllowlistError(
            f"engine config allowlist {p} could not be read/parsed: "
            f"{type(exc).__name__}: {exc}",
            engine=engine,
            path=str(p),
        ) from exc

    if not isinstance(raw, dict) or "engine" not in raw or "entries" not in raw:
        _fail(
            f"engine config allowlist {p} must be a mapping with 'engine:' and "
            "'entries:' keys (entries: [] for an explicit empty allowlist).",
            engine=engine,
            path=p,
        )

    declared_engine = raw["engine"]
    if declared_engine != engine:
        _fail(
            f"engine config allowlist {p} declares engine {declared_engine!r}, "
            f"expected {engine!r} (this file must live beside that engine's "
            "own capabilities.yml).",
            engine=engine,
            path=p,
        )

    rows = raw["entries"]
    if not isinstance(rows, list):
        _fail(
            f"engine config allowlist {p}: 'entries:' must be a list of "
            "{pattern, type, [range|enum]} mappings.",
            engine=engine,
            path=p,
        )

    engine_fields = EngineBlockSchema.model_fields
    if engine not in engine_fields:
        _fail(
            f"engine config allowlist {p}: engine {engine!r} has no "
            "engine.<name> Blueprint block (EngineBlockSchema in "
            "aqueduct/parser/schema.py) to validate entries against — "
            "add one there first.",
            engine=engine,
            path=p,
        )
    block_cls = engine_fields[engine].annotation
    block_fields = getattr(block_cls, "model_fields", {})
    is_free_form = "conf" in block_fields

    seen: set[str] = set()
    entries: list[AllowlistEntry] = []
    for row in rows:
        if not isinstance(row, dict) or "pattern" not in row or "type" not in row:
            _fail(
                f"engine config allowlist {p}: every entry needs a 'pattern' "
                f"and a 'type'; got {row!r}.",
                engine=engine,
                path=p,
            )
        pattern = row["pattern"]
        if not isinstance(pattern, str) or not pattern.strip():
            _fail(
                f"engine config allowlist {p}: entry 'pattern' must be a "
                f"non-empty string; got {pattern!r}.",
                engine=engine,
                path=p,
            )
        if pattern in seen:
            _fail(
                f"engine config allowlist {p}: duplicate entry for pattern " f"{pattern!r}.",
                engine=engine,
                path=p,
                keys=[pattern],
            )
        seen.add(pattern)

        type_raw = row["type"]
        if type_raw not in _LEGAL_TYPES:
            _fail(
                f"engine config allowlist {p}: entry {pattern!r} has invalid "
                f"type {type_raw!r}. Legal types: {list(_LEGAL_TYPES)}.",
                engine=engine,
                path=p,
                keys=[pattern],
            )
        value_type = type_raw

        range_raw = row.get("range")
        enum_raw = row.get("enum")
        if range_raw is not None and enum_raw is not None:
            _fail(
                f"engine config allowlist {p}: entry {pattern!r} declares both "
                "'range' and 'enum' — at most one constraint per entry.",
                engine=engine,
                path=p,
                keys=[pattern],
            )

        constraint: RangeConstraint | EnumConstraint | None = None
        if range_raw is not None:
            if value_type not in ("int", "float", "size"):
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r} has a "
                    f"'range' but type {value_type!r} does not support one "
                    "(only int/float/size do).",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )
            if not isinstance(range_raw, list) or len(range_raw) != 2:
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r}'s 'range' "
                    f"must be a 2-element [min, max] list; got {range_raw!r}.",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )
            lo, hi = range_raw
            if value_type == "size":
                try:
                    lo_bytes, hi_bytes = _size_to_bytes(str(lo)), _size_to_bytes(str(hi))
                except ValueError as exc:
                    _fail(
                        f"engine config allowlist {p}: entry {pattern!r}'s "
                        f"'range' is not a valid [min, max] size pair: {exc}",
                        engine=engine,
                        path=p,
                        keys=[pattern],
                    )
                if lo_bytes > hi_bytes:
                    _fail(
                        f"engine config allowlist {p}: entry {pattern!r}'s "
                        f"range min ({lo!r}) exceeds its max ({hi!r}).",
                        engine=engine,
                        path=p,
                        keys=[pattern],
                    )
            else:
                pytype = _PY_TYPE_OF[value_type]
                # bool is an int subclass in Python; explicitly excluded above
                # (range never applies to `bool`), so this is safe.
                if not isinstance(lo, pytype) or not isinstance(hi, pytype):
                    _fail(
                        f"engine config allowlist {p}: entry {pattern!r}'s "
                        f"range bounds must both be {pytype.__name__}; got "
                        f"{range_raw!r}.",
                        engine=engine,
                        path=p,
                        keys=[pattern],
                    )
                if lo > hi:
                    _fail(
                        f"engine config allowlist {p}: entry {pattern!r}'s "
                        f"range min ({lo!r}) exceeds its max ({hi!r}).",
                        engine=engine,
                        path=p,
                        keys=[pattern],
                    )
            constraint = RangeConstraint(minimum=lo, maximum=hi)
        elif enum_raw is not None:
            if not isinstance(enum_raw, list) or not enum_raw:
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r}'s 'enum' "
                    f"must be a non-empty list; got {enum_raw!r}.",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )
            pytype = _PY_TYPE_OF[value_type]
            for v in enum_raw:
                if not isinstance(v, pytype):
                    _fail(
                        f"engine config allowlist {p}: entry {pattern!r}'s "
                        f"'enum' member {v!r} is not a {pytype.__name__}.",
                        engine=engine,
                        path=p,
                        keys=[pattern],
                    )
            constraint = EnumConstraint(values=tuple(enum_raw))

        if is_free_form:
            # Glob prefix into the free-form `conf` bag — nothing further to
            # structurally validate; the bag accepts any string key.
            pass
        else:
            if "*" in pattern or "?" in pattern:
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r} is a "
                    f"glob, but engine {engine!r}'s Blueprint block "
                    "(EngineBlockSchema) declares typed fields, not a "
                    "free-form conf bag — a pattern must name one declared "
                    f"field exactly. Available: {sorted(block_fields)}.",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )
            if pattern not in block_fields:
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r} is not a "
                    f"declared field of engine.{engine}'s Blueprint block. "
                    f"Available: {sorted(block_fields)}.",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )
            field_annotation = block_fields[pattern].annotation
            expected_py_type = _unwrap_optional(field_annotation)
            declared_py_type = _PY_TYPE_OF[value_type]
            if expected_py_type is not None and expected_py_type is not declared_py_type:
                _fail(
                    f"engine config allowlist {p}: entry {pattern!r} declares "
                    f"type {value_type!r} ({declared_py_type.__name__}) but "
                    f"engine.{engine}.{pattern} is typed "
                    f"{field_annotation!r} ({expected_py_type.__name__}) in "
                    "EngineBlockSchema.",
                    engine=engine,
                    path=p,
                    keys=[pattern],
                )

        entries.append(
            AllowlistEntry(pattern=pattern, value_type=value_type, constraint=constraint)
        )

    # ── deny layer — core-only; see module docstring "Deny layer" section ──
    deny_raw = raw.get("deny", [])
    if not isinstance(deny_raw, list):
        _fail(
            f"engine config allowlist {p}: 'deny:' must be a list of "
            "{pattern, reason, [deny_values]} mappings.",
            engine=engine,
            path=p,
        )

    deny_entries: list[DenyEntry] = []
    for row in deny_raw:
        if not isinstance(row, dict) or "pattern" not in row or "reason" not in row:
            _fail(
                f"engine config allowlist {p}: every deny entry needs a "
                f"'pattern' and a 'reason'; got {row!r}.",
                engine=engine,
                path=p,
            )
        d_pattern = row["pattern"]
        if not isinstance(d_pattern, str) or not d_pattern.strip():
            _fail(
                f"engine config allowlist {p}: deny entry 'pattern' must be "
                f"a non-empty string; got {d_pattern!r}.",
                engine=engine,
                path=p,
            )
        d_reason = row["reason"]
        if not isinstance(d_reason, str) or not d_reason.strip():
            _fail(
                f"engine config allowlist {p}: deny entry {d_pattern!r} "
                f"'reason' must be a non-empty string; got {d_reason!r}.",
                engine=engine,
                path=p,
                keys=[d_pattern],
            )
        dv_raw = row.get("deny_values")
        deny_values: tuple[Any, ...] | None = None
        if dv_raw is not None:
            if not isinstance(dv_raw, list) or not dv_raw:
                _fail(
                    f"engine config allowlist {p}: deny entry {d_pattern!r}'s "
                    f"'deny_values' must be a non-empty list; got {dv_raw!r}.",
                    engine=engine,
                    path=p,
                    keys=[d_pattern],
                )
            deny_values = tuple(dv_raw)
        deny_entries.append(DenyEntry(pattern=d_pattern, reason=d_reason, deny_values=deny_values))

    # Load-time overlap guard (TEST 1 in the originating task): an absolute
    # ban must never be reachable through an allow entry. A scoped
    # value-ban (`deny_values` set) is exempt — coexisting with an allow
    # entry on the same key is its whole purpose, not a defect.
    for d in deny_entries:
        if d.deny_values is not None:
            continue
        canary = _glob_canary(d.pattern)
        offending = [e.pattern for e in entries if e.matches(canary)]
        if offending:
            _fail(
                f"engine config allowlist {p}: deny pattern {d.pattern!r} "
                f"({d.reason}) is reachable through allow "
                f"entr{'y' if len(offending) == 1 else 'ies'} {offending!r} "
                f"via canary key {canary!r} — a core deny must never be "
                "reachable through an allow entry.",
                engine=engine,
                path=p,
                keys=[d.pattern, *offending],
            )

    return EngineConfigAllowlist(
        engine=engine,
        is_free_form=is_free_form,
        entries=tuple(entries),
        deny_entries=tuple(deny_entries),
    )


def validate_against_deny(
    allowlist: EngineConfigAllowlist, pattern: str, value: Any = None
) -> None:
    """Raise ``EngineConfigAllowlistError`` if ``pattern`` (optionally with
    ``value``) is covered by ``allowlist``'s CORE deny entries.

    This is the validation contract a future operator-extension config
    surface must call before admitting a candidate entry — pinned now, at
    the same time the deny data ships, even though no such extension
    surface exists yet (see the module docstring's "Immutability" section
    for why this function cannot be handed a narrowed deny set to dodge).
    """
    for entry in allowlist.deny_entries:
        if entry.matches(pattern) and entry.forbids(value):
            raise EngineConfigAllowlistError(
                f"{pattern!r} is denied for engine {allowlist.engine!r}: {entry.reason}",
                engine=allowlist.engine,
                keys=[pattern],
            )


def evaluate_set_engine_config(
    allowlist: EngineConfigAllowlist, key: str, value: Any
) -> str | None:
    """Evaluate one candidate ``set_engine_config`` write against
    *allowlist*. Pure: returns ``None`` when ``key``/``value`` is permitted,
    or a human-readable rejection reason naming which layer refused
    otherwise. Raises nothing — this function is the enforcement LOGIC the
    module docstring's "Why this exists" section calls out as not yet
    wired; the exception TYPE a rejection surfaces as is the caller's call
    (Gate 1, ``aqueduct/patch/apply.py::_check_guardrails``, wraps a
    non-``None`` return in its own ``PatchError`` — a policy violation is a
    patch problem, never this module's ``EngineConfigAllowlistError``, which
    stays reserved for a malformed/missing SHIPPED file, already raised by
    ``load_allowlist`` long before a caller reaches this function).

    Evaluation order (deliberate, for error quality — the first violation
    found is the one reported):
      1. Deny — key match, then (for a scoped ban) a ``deny_values`` match.
         The returned reason always carries the deny entry's own ``reason``.
      2. Allow membership — a key matching no allow entry is refused
         (fail closed: the allowlist is a closed-world grant list, not a
         denylist — see the module docstring).
      3. Type.
      4. Constraint — the matched entry's ``range`` or ``enum``, if it
         declares one (most shipped entries declare neither today — see
         the module docstring's "range/enum are a MECHANISM, not a
         mandate" — but a future entry that DOES declare one must not be
         silently unenforced, so both are checked here even though today's
         shipped files never exercise this branch).
    """
    for d in allowlist.deny_entries:
        if d.matches(key) and d.forbids(value):
            return f"{key!r} is denied for engine {allowlist.engine!r}: {d.reason}"

    entry = next((e for e in allowlist.entries if e.matches(key)), None)
    if entry is None:
        return f"{key!r} is not on engine {allowlist.engine!r}'s set_engine_config " "allowlist"

    if entry.value_type == "size":
        if not isinstance(value, str):
            return (
                f"{key!r}={value!r} must be a size string (e.g. '4g', '512MB') "
                f"for engine {allowlist.engine!r}, got {type(value).__name__}"
            )
        try:
            _size_to_bytes(value)
        except ValueError:
            return (
                f"{key!r}={value!r} is not a valid size literal for engine "
                f"{allowlist.engine!r} (expected e.g. '4g', '512MB')"
            )
    else:
        expected_py_type = _PY_TYPE_OF[entry.value_type]
        # bool is an int subclass in Python — a stray bool must not silently
        # satisfy `type: int` (mirrors the same guard in load_allowlist's
        # own range-bound check).
        type_ok = isinstance(value, expected_py_type) and not (
            entry.value_type == "int" and isinstance(value, bool)
        )
        if not type_ok:
            return (
                f"{key!r}={value!r} has the wrong type for engine "
                f"{allowlist.engine!r}: expected {entry.value_type}, got "
                f"{type(value).__name__}"
            )

    if isinstance(entry.constraint, EnumConstraint):
        if value not in entry.constraint.values:
            return (
                f"{key!r}={value!r} is not one of engine {allowlist.engine!r}'s "
                f"allowed values for {key!r}: {list(entry.constraint.values)}"
            )
    elif isinstance(entry.constraint, RangeConstraint):
        if entry.value_type == "size":
            try:
                v_bytes = _size_to_bytes(value)
                lo_bytes = _size_to_bytes(str(entry.constraint.minimum))
                hi_bytes = _size_to_bytes(str(entry.constraint.maximum))
            except ValueError:
                v_bytes = lo_bytes = hi_bytes = None
            in_range = v_bytes is not None and lo_bytes <= v_bytes <= hi_bytes
        else:
            in_range = entry.constraint.minimum <= value <= entry.constraint.maximum
        if not in_range:
            return (
                f"{key!r}={value!r} is outside engine {allowlist.engine!r}'s "
                f"allowed range [{entry.constraint.minimum!r}, "
                f"{entry.constraint.maximum!r}] for {key!r}"
            )

    return None


# ── discovery — mirrors capability_tooling.discover_declarations() ──────────


def _engine_dir_of_entry_point(ep: importlib.metadata.EntryPoint) -> Path | None:
    """Locate an engine entry point's package directory WITHOUT importing it.

    Same rationale and same mechanism as
    ``aqueduct.executor.capability_tooling._engine_dir_of_entry_point``
    (duplicated rather than imported — this module stays a self-contained,
    minimal, pyspark-free leaf, the same precedent ``channel_ops.py``/
    ``path_keys.py`` set for hoisting a tiny engine-agnostic helper rather
    than reaching into a CLI-tooling-oriented module): importing the engine
    module is exactly what discovery must NOT do, since an engine whose
    capability declaration is incomplete raises at import time. ``find_spec``
    imports only the parent package, never the engine module itself.
    """
    target = ep.value.split(":")[0]
    try:
        spec = importlib.util.find_spec(target)
    except (ImportError, AttributeError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin).parent


def discover_registered_engines() -> dict[str, Path]:
    """Every ``aqueduct.engines`` entry point name -> its package directory.

    Resolved via ``find_spec`` only — no engine module is imported. This is
    the set the presence guard checks against; it deliberately does NOT
    fall back to a directory glob the way
    ``capability_tooling.discover_declarations()`` does for a
    not-yet-registered scaffold, because "every REGISTERED engine ships a
    file" is exactly the guarantee this function backs.
    """
    result: dict[str, Path] = {}
    for ep in importlib.metadata.entry_points(group=AQ_ENGINES_ENTRYPOINT_GROUP):
        d = _engine_dir_of_entry_point(ep)
        if d is not None:
            result[ep.name] = d
    return result


def discover_allowlist_paths() -> dict[str, Path | None]:
    """Registered engine name -> its ``engine_config_allowlist.yml`` path,
    or ``None`` if that engine ships no such file (a build gap)."""
    paths: dict[str, Path | None] = {}
    for name, engine_dir in discover_registered_engines().items():
        candidate = engine_dir / DECLARATION_FILENAME
        paths[name] = candidate if candidate.is_file() else None
    return paths


def check_presence() -> None:
    """Raise ``EngineConfigAllowlistError`` naming every registered engine
    that ships no ``engine_config_allowlist.yml``.

    An absent file already fails closed (``load_allowlist`` itself refuses
    to proceed without one), but a human must make that an explicit,
    reviewable decision — see the module docstring's presence-guard section.
    """
    missing = sorted(name for name, path in discover_allowlist_paths().items() if path is None)
    if missing:
        raise EngineConfigAllowlistError(
            f"engine(s) {missing} ship no {DECLARATION_FILENAME}. Every "
            "registered engine must have one, even if empty "
            '("engine: <name>\\nentries: []\\n") to state explicitly that '
            "it heals no engine config — an absent file already fails "
            "closed, but only an explicit empty one makes that a decision "
            "someone made rather than a gap nobody noticed.",
            keys=missing,
        )


__all__ = [
    "DECLARATION_FILENAME",
    "AllowlistEntry",
    "DenyEntry",
    "EngineConfigAllowlist",
    "RangeConstraint",
    "EnumConstraint",
    "load_allowlist",
    "validate_against_deny",
    "evaluate_set_engine_config",
    "discover_registered_engines",
    "discover_allowlist_paths",
    "check_presence",
]
