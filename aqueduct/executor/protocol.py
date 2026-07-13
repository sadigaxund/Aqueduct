"""``ExecutorProtocol`` — the engine-agnostic execution contract (Phase 78 Step 2).

Every execution engine (``spark`` today, ``duckdb`` planned) registers one
``ExecutorProtocol`` instance describing how core talks to it. This module is
pure declaration — no engine runtime dependency (``pyspark``, ``duckdb``, ...)
is imported here, directly or transitively, so it stays importable in any
install, including a base install with no engine extras.

**Why a frozen dataclass, not ``typing.Protocol``/ABC.** The rest of the
codebase's "pluggable contract" precedents are frozen dataclasses carried
through a registry — ``aqueduct/executor/capabilities.py``'s
``EngineCapabilities``/``CAPABILITY_REGISTRY`` and ``aqueduct/tools/registry.py``'s
``Tool``/``REGISTRY`` — not ``typing.Protocol`` structural typing or an ABC
hierarchy. An ``ExecutorProtocol`` is data (a bundle of callables + a string),
not behavior to subclass, so the same shape is used here: a value an engine
constructs once at import time and hands to ``register_protocol()``.

**What "the contract" is, derived from the CURRENT Spark ``execute()``**
(``aqueduct/executor/spark/executor.py::execute``), not invented from first
principles:

  - ``execute``: takes a compiled ``Manifest`` plus engine-specific session/
    config kwargs and returns an ``ExecutionResult`` (frozen,
    ``aqueduct/executor/models.py``). Raises an engine-specific error for
    setup failures (Spark raises ``ExecuteError``); per-module failures are
    caught internally and reported as ``ModuleResult(status="error", ...)``,
    never raised. ``ExecutorProtocol`` does not narrow this further — it is
    intentionally ``Callable[..., ExecutionResult]`` because the real Spark
    signature carries a long list of engine-specific optional kwargs
    (``checkpoint_root``, ``parallel``, ``use_observe``, ``sampling``, ...)
    that a structurally different engine (e.g. DuckDB, no distributed
    checkpoint/observe story) has no reason to share verbatim. The uniform
    part of the contract is "Manifest in, ExecutionResult out"; kwargs are a
    per-engine extension point, same as today.
  - ``extract_error``: an engine exception (or ``None``) -> a
    ``dict[str, Any] | None`` of ``FailureContext`` fields (``error_class``,
    ``root_exception``, ``sql_state``, ``suggested_columns``, ``object_name``).
    Spark's implementation is the existing
    ``aqueduct.surveyor.error_extraction._extract_structured_error`` — this
    is a refactor (naming the existing function as the engine's contribution
    through the seam), not new behavior. Without an engine-provided
    extractor, ``FailureContext.error_class``/``root_exception`` silently stay
    ``None`` for every failure on that engine and the healing LLM loses the
    structured root-cause block — the exact failure mode TODOs.md calls out
    ("heal quality dies on [engine] errors"). ``ExecutorProtocol`` makes this
    structural: an engine without an extractor cannot register (see
    ``__post_init__`` below).
  - ``prompt_rules``: a ``PromptRules`` pack (below) — the engine-specific
    system-prompt content the healing LLM sees: persona line, the note
    describing the engine's structured root-cause block, the engine-flavored
    rule bullets (Spark's temp-view / AnalysisException / schema-inference
    notes), and the engine's slice of the defer-to-human section (its
    infrastructure failure modes, its UDF languages). The healing system
    prompt is COMPOSED at build time from an engine-independent scaffold that
    stays in the agent layer (``aqueduct/agent/prompts.py`` — PatchSpec
    schema, op-selection table, provenance rules, output contract, the generic
    defer categories) plus the target engine's pack, pulled through this
    registry. Spark's pack lives in ``aqueduct/executor/spark/prompt_rules.py``;
    the agent imports no engine specifics and the executor imports nothing
    from the agent layer. NB the scaffold is not just the template constant:
    parts of the prompt (the defer section) are assembled at RUNTIME, so the
    anti-bleed guard greps the COMPOSED prompt for a non-Spark engine, never
    the source constants.

**Registration seam.** Mirrors ``aqueduct/executor/capabilities.py``:
importing an engine's ``aqueduct.engines`` entry-point module
(``aqueduct/executor/spark/engine.py`` for ``spark``) constructs its
``ExecutorProtocol`` and calls ``register_protocol()`` as an import side
effect. ``get_protocol()`` calls ``load_engines()`` first (same idempotent,
cached-per-process resolution ``get_capabilities()`` already uses) so the
registry is populated on the real compile/run path with zero explicit
per-engine imports anywhere in core.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aqueduct.errors import EnginePluginError, UnknownEngineError

if TYPE_CHECKING:
    from aqueduct.executor.models import ExecutionResult

# engine exception (or None) -> FailureContext field dict (or None).
ErrorExtractor = Callable[[BaseException | None], "dict[str, Any] | None"]


@dataclass(frozen=True)
class DeferRules:
    """The engine's contribution to the "when to defer to a human" section.

    The defer section (shown only when ``allow_defer`` is on) tells the model
    which failures NO PatchSpec op can fix. Most of that list is engine-
    independent (upstream schema changes needing human judgment, checkpoint
    corruption, object-store consistency, cluster config), but the concrete
    examples an engine can actually produce are not: a Hive metastore lock is
    not a thing a single-node engine hits, and "Python/Scala UDF code" names
    the wrong languages for an engine whose UDFs are Python-only. Left
    generic, those strings would tell a DuckDB heal to defer over Hive
    metastore locks — the exact engine bleed this seam exists to stop.

    Attributes:
        infra_examples: Engine-specific infrastructure failure modes, rendered
            INTO the generic "Infrastructure failures" bullet's example list
            (e.g. Spark: ``"Hive metastore locks"``). Comma-free phrasing of a
            single example, or a comma-joined list.
        udf_languages: The UDF languages this engine's registry accepts,
            rendered into the generic "UDF body bugs" bullet (e.g. Spark:
            ``"Python/Scala"``). PatchSpec cannot modify UDF bodies on ANY
            engine — that part is generic; only the language names are not.
        extra_bullets: Whole additional defer bullets that exist only for this
            engine, appended after the generic list. Verbatim markdown lines,
            each ending in a newline. Optional (``""`` when the engine has
            none) — unlike the two above, "this engine has no extra defer
            category" is a legitimate, complete answer.
    """

    infra_examples: str
    udf_languages: str
    extra_bullets: str = ""

    def __post_init__(self) -> None:
        for field_name in ("infra_examples", "udf_languages"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise EnginePluginError(
                    f"DeferRules.{field_name} is required and must be a non-empty string — "
                    "leaving it blank falls back to another engine's examples, which is how a "
                    "heal on one engine ends up being told to defer over another engine's "
                    "infrastructure."
                )
        if not isinstance(self.extra_bullets, str):
            raise EnginePluginError('DeferRules.extra_bullets must be a string ("" if none)')


@dataclass(frozen=True)
class PromptRules:
    """One engine's healing prompt-rules pack.

    The healing system prompt is composed at build time as
    ``generic scaffold + this engine's pack`` (see
    ``aqueduct/agent/prompts.py::_build_system_prompt``). The generic scaffold
    — the PatchSpec grammar/schema, the op-selection table, the provenance
    rules, the output contract, the coaching/history sections — is
    engine-independent and stays in the agent layer. Everything that names an
    engine, an engine's exception vocabulary, or engine-flavored advice lives
    here, supplied BY the engine.

    The fields map exactly onto the places engine-flavored text appears in the
    composed prompt (they are slots in the scaffold, not free prose):

      - ``persona``: the system prompt's opening line ("You are an expert
        <engine> blueprint repair agent for the Aqueduct blueprint engine.").
      - ``root_cause_note``: the parenthetical in the failure-report bullet
        describing what the engine's structured root-cause block contains
        (it is the prose counterpart of ``ExecutorProtocol.extract_error``'s
        output, so the two stay honest about each other).
      - ``rules``: the engine-specific bullets inside the prompt's "Other
        rules" section — engine error idioms, engine-flavored advice, engine
        API/config references.
      - ``defer``: a ``DeferRules`` (above) — the engine's slice of the
        "when to defer to a human" section, which is assembled at RUNTIME
        (only when ``allow_defer`` is on) rather than living in the template
        constant. That runtime assembly is precisely where the first pass at
        this split missed three Spark-flavored strings, so the guard test
        greps the COMPOSED prompt for a non-Spark engine, not the template
        constant.

    Text slots are rendered verbatim (they are ``.format()`` *arguments*, not
    part of a format string), so braces in them are literal.

    Everything except ``DeferRules.extra_bullets`` is required and non-empty: a
    healing agent with no engine context produces generic, lower-quality
    patches with no signal that anything is missing, which is the exact "heal
    quality silently dies on a new engine's errors" failure this seam exists to
    prevent.
    """

    persona: str
    root_cause_note: str
    rules: str
    defer: DeferRules

    def __post_init__(self) -> None:
        for field_name in ("persona", "root_cause_note", "rules"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise EnginePluginError(
                    f"PromptRules.{field_name} is required and must be a non-empty string — "
                    "an engine cannot register a prompt-rules pack without it, or the healing "
                    "agent loses engine context with no signal that anything is missing."
                )
        if not isinstance(self.defer, DeferRules):
            raise EnginePluginError(
                "PromptRules.defer is required and must be a DeferRules — without it the "
                "'when to defer to a human' section falls back to another engine's "
                "infrastructure and UDF vocabulary."
            )


@dataclass(frozen=True)
class ExecutorProtocol:
    """One engine's execution contract.

    Attributes:
        engine: Engine name (matches the ``aqueduct.engines`` entry-point
            name and ``deployment.engine`` values, e.g. ``"spark"``).
        execute: ``(manifest, ...) -> ExecutionResult`` — the engine's
            ``execute()`` function. Must defer any engine runtime import
            (e.g. ``pyspark``) to call time so constructing the protocol
            object itself never requires the engine's dependency installed.
        extract_error: Engine exception -> ``FailureContext`` field dict (see
            module docstring). Required — an engine cannot register without
            one (``__post_init__`` enforces this); a healing loop that can
            never populate ``error_class``/``root_exception`` for an engine
            is a silent quality regression, not an acceptable default.
        prompt_rules: The engine's ``PromptRules`` pack (see above). Required
            for the same reason.

    A registration failure raises ``EnginePluginError`` (an ``AqueductError``),
    never a bare builtin: an engine-plugin author hitting one of these guards
    is a user of this seam, and the error-taxonomy rule applies to every
    user-reachable path.
    """

    engine: str
    execute: Callable[..., ExecutionResult]
    extract_error: ErrorExtractor
    prompt_rules: PromptRules

    def __post_init__(self) -> None:
        if not self.engine:
            raise EnginePluginError("ExecutorProtocol.engine must be a non-empty string")
        if self.execute is None:
            raise EnginePluginError(f"engine {self.engine!r}: ExecutorProtocol.execute is required")
        if self.extract_error is None:
            raise EnginePluginError(
                f"engine {self.engine!r}: ExecutorProtocol.extract_error is required — "
                "an engine cannot register without an error extractor (engine exception -> "
                "FailureContext fields), or FailureContext.error_class/root_exception stay "
                "silently None for every failure and the healing LLM loses the structured "
                "root-cause block."
            )
        if not isinstance(self.prompt_rules, PromptRules):
            raise EnginePluginError(
                f"engine {self.engine!r}: ExecutorProtocol.prompt_rules is required and must "
                "be a PromptRules pack (the engine-specific healing system-prompt persona / "
                "root-cause note / rules), or the healing agent gets no engine context at all."
            )


PROTOCOL_REGISTRY: dict[str, ExecutorProtocol] = {}


def register_protocol(protocol: ExecutorProtocol) -> None:
    """Register (or replace) an engine's ``ExecutorProtocol``."""
    PROTOCOL_REGISTRY[protocol.engine] = protocol


def get_protocol(engine: str) -> ExecutorProtocol:
    """Look up a registered engine's ``ExecutorProtocol``.

    Calls ``aqueduct.executor.capabilities.load_engines()`` first (imported
    lazily to avoid a module-level cycle), so this resolves correctly with
    zero explicit per-engine imports anywhere on the call path — same
    fail-closed posture as ``capabilities.get_capabilities()``.

    Raises:
        UnknownEngineError: no engine registered under that name. Mirrors
            ``get_capabilities()``'s message shape (empty-registry vs
            unknown-name diagnosis) so callers get the same actionable text
            regardless of which registry they hit.
        EnginePluginError: an engine plugin's entry point failed to import.
    """
    from aqueduct.executor.capabilities import load_engines

    load_engines()
    if engine not in PROTOCOL_REGISTRY:
        registered = sorted(PROTOCOL_REGISTRY)
        if not registered:
            from aqueduct.executor.capabilities import NO_ENGINES_HINT

            raise UnknownEngineError(
                f"cannot resolve executor for engine {engine!r}: {NO_ENGINES_HINT}",
                engine=engine,
                engines=registered,
            )
        raise UnknownEngineError(
            f"unknown engine {engine!r} — no executor registered. "
            f"Registered engines: {registered}",
            engine=engine,
            engines=registered,
        )
    return PROTOCOL_REGISTRY[engine]


__all__ = [
    "DeferRules",
    "ErrorExtractor",
    "ExecutorProtocol",
    "PromptRules",
    "PROTOCOL_REGISTRY",
    "register_protocol",
    "get_protocol",
]
