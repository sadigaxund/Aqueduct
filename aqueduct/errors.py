"""Base exception for all Aqueduct-internal errors.

Every Aqueduct-raised exception inherits from ``AqueductError`` so callers
can catch ``except AqueductError:`` to handle any expected engine failure
without swallowing ``KeyboardInterrupt``, ``SystemExit``, or foreign-library
errors.  Individual exception classes carry layer-specific semantics (parser,
compiler, executor, etc.) and SHOULD be caught by type when the caller needs
to distinguish one failure mode from another.
"""


class AqueductError(Exception):
    """Root of the Aqueduct exception hierarchy."""


class ParseError(AqueductError):
    """Raised for any Blueprint parse, validation, or resolution failure."""


class CompileError(AqueductError):
    """Raised for any compilation failure."""


class TypeSpellingError(ParseError):
    """Raised when a Blueprint type string cannot be parsed by the hub type
    vocabulary (``aqueduct/typehub.py``, Phase 80).

    A ``ParseError`` subclass: a type spelling is a small self-contained
    grammar nested inside a Blueprint value (``schema_hint``, ``cast``
    columns, UDF ``return_type``), so a spelling that vocabulary cannot
    parse is semantically a parse failure even when the surrounding
    validation happens to run during ``compile()`` — every ``except
    ParseError:`` caller keeps working unchanged. Distinct from a bare
    ``ValueError`` so callers that need to react specifically (e.g. suggest
    the ``<engine>:<spelling>`` native-namespace escape hatch) can catch it
    by type instead of parsing the message.
    """


class UnknownEngineError(CompileError):
    """Raised when ``deployment.engine`` names an engine with no registered
    capability declaration (Phase 78).

    A ``CompileError`` subclass, so the compile-time capability gate's existing
    ``except CompileError`` callers keep working unchanged, but it is a distinct
    type so callers that must tell "this engine is not registered" apart from
    "this blueprint failed to compile" (``aqueduct/doctor/checks_io.py``) can do
    so by TYPE, never by matching on the message text.

    ``engines`` carries the registered-engine names at raise time. An EMPTY list
    means no engine registered at all — the package's ``aqueduct.engines`` entry
    points are not visible to ``importlib.metadata`` (typically a stale editable
    install), which is a different diagnosis from a misspelled engine name.
    """

    def __init__(self, message: str, *, engine: str = "", engines: list[str] | None = None) -> None:
        super().__init__(message)
        self.engine = engine
        self.engines = list(engines or [])

    @property
    def no_engines_registered(self) -> bool:
        """True when the registry is empty (stale install), not just a bad name."""
        return not self.engines


class EnginePluginError(AqueductError):
    """Raised when an ``aqueduct.engines`` entry point fails to LOAD (Phase 78).

    A broken or half-installed third-party engine plugin must surface as a clean
    Aqueduct error naming the entry point and its underlying cause, never as a
    bare ``ImportError`` escaping out of ``aqueduct.yml`` loading. This is an
    INSTALL-time problem — the package is broken or half-present — so the message
    ends with reinstall/uninstall advice.

    It is deliberately NOT the error for "the engine's capability declaration is
    incomplete or invalid" — that is ``CapabilityDeclarationError`` below. Those
    are different states with different fixes, and reinstalling never fixes the
    second one. Callers distinguish them by TYPE, never by message substring.
    """


class CapabilityDeclarationError(AqueductError):
    """Raised when an engine's capability declaration (``capabilities.yml``) is
    incomplete or invalid (Phase 78).

    A DEV-time build failure, not an install problem: a leaf has no row, a row is
    still parked on the ``undeclared`` sentinel, a row names a leaf that does not
    exist, a verdict is illegal, or a ``requires`` specifier is malformed. The
    usual trigger is a first-party developer adding a schema/config key — every
    registered engine now owes that new leaf a verdict. Reinstalling the package
    fixes nothing; running ``aqueduct dev capabilities sync`` (or ``scaffold``
    for a brand-new engine) and declaring a verdict per engine does.

    ``leaves`` carries the offending leaf ids, so a caller can report them
    without re-parsing the message.
    """

    def __init__(
        self,
        message: str,
        *,
        engine: str = "",
        path: str = "",
        leaves: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.engine = engine
        self.path = path
        self.leaves = list(leaves or [])


class CapabilityScopeError(AqueductError):
    """Raised when a ``config.*`` leaf's engine-scoping is undecided (Q4 step 2).

    A SIBLING of ``CapabilityDeclarationError`` — both are direct
    ``AqueductError`` subclasses, deliberately NOT one derived from the other
    — so ``except CapabilityDeclarationError:`` (e.g. ``aqueduct/doctor/
    checks_io.py``) cannot silently swallow this and re-conflate the two.
    They are different states with three DIFFERENT fixes:

      - ``EnginePluginError``: the ``aqueduct.engines`` entry point failed to
        IMPORT. Fix: reinstall.
      - ``CapabilityDeclarationError``: a governed leaf has no row / an
        ``undeclared`` row / an orphaned row / an illegal verdict. Fix: run
        ``aqueduct dev capabilities sync`` and declare a verdict.
      - ``CapabilityScopeError`` (this one): a ``config.*`` leaf has NO
        ``Field(..., json_schema_extra={"engine_scoped": ...})`` tag at all in
        ``aqueduct/config.py`` — there is no untagged state and no "untagged
        means core" fallback; every leaf must carry an EXPLICIT ``True`` or
        ``False``. A leaf living under an ``engine.<name>.*`` block is
        additionally restricted to ``True`` — tagging it ``False`` (or
        leaving it untagged) is a contradiction, since there is no valid
        "core" reading for a field namespaced to exactly one engine. The
        walker (``aqueduct/executor/config_leaves.py``) raises this at
        REGISTRATION TIME (import of each engine's ``capabilities.py``, which
        calls ``all_config_leaves(engine=...)``) — never CI-only. Fix: tag
        the field ``engine_scoped: True`` if it dispatches through an engine
        (an engine module reads it, or it reaches ``ExecutorProtocol``), or
        ``engine_scoped: False`` if it only ever executes in core code paths
        — the latter is illegal for a field under ``engine.<name>.*``.

    Raised by ``aqueduct.executor.config_leaves``.
    """


class EngineConfigAllowlistError(AqueductError):
    """Raised when an engine's healing config-key allowlist
    (``engine_config_allowlist.yml``) is missing, incomplete, or invalid.

    The Blueprint-level ``engine.<name>`` block (``EngineBlockSchema`` in
    ``aqueduct/parser/schema.py``) is NOT governed by the capability
    framework — the leaf walker never included the engine-block schemas, so
    ``spark.sql.shuffle.partitions`` and friends carry no capability leaf at
    all. This allowlist is a SEPARATE governance file naming exactly which
    ``set_engine_config`` keys/values the healing agent may propose, and it
    is deliberately its own error type rather than reusing one of the three
    capability-declaration types above:

      - Different CONTENT: a key/type/range triple for one engine's config
        surface, not a verdict for a Blueprint-grammar or ``aqueduct.yml``
        leaf — ``capability_leaves.py``/``config_leaves.py`` never derive
        this checklist, so there is no walker for a closure test to compare
        against.
      - Different FIX: editing the allowlist entry directly. There is no
        ``aqueduct dev capabilities sync``/``scaffold`` equivalent for this
        file (v1 ships no tooling for it), so pointing at that advice would
        be actively wrong.
      - Different CONSUMER: a future patch-apply gate (not yet wired — see
        the module docstring), not ``aqueduct/compiler/capability_check.py``.

    A direct ``AqueductError`` subclass, deliberately NOT derived from
    ``CapabilityDeclarationError``/``CapabilityScopeError``/``EnginePluginError``
    (or vice versa) — same reasoning as those three staying siblings: a
    handler written for one must not silently swallow this and re-conflate
    two states with different fixes.

    ``keys`` carries the offending pattern/field name(s) (or the list of
    engines with a missing file, for the presence guard), so a caller can
    report them without re-parsing the message.
    """

    def __init__(
        self,
        message: str,
        *,
        engine: str = "",
        path: str = "",
        keys: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.engine = engine
        self.path = path
        self.keys = list(keys or [])


class ConfigError(AqueductError):
    """Raised when aqueduct.yml cannot be loaded or fails validation."""


class ScenarioError(AqueductError):
    """Raised when a ``.aqscenario.yml`` benchmark file is malformed.

    A scenario file is user-authored (a benchmark suite is something a user
    writes and ships), so a bad one is a user-reachable failure and must not
    surface as a bare ``ValueError`` — see AGENTS.md's "User-reachable errors
    raise an ``AqueductError`` subclass". ``aqueduct/doctor/checks_io.py``
    ``check_aqscenario`` branches on this TYPE to tell "this file's shape is
    wrong" (fix the YAML) from "loading it blew up some other way".

    Covers the whole load-time contract: a missing/unsupported
    ``aqueduct_scenario:`` version, a missing required key, an UNKNOWN key at
    any level of the file, an assertion nobody implements, and a ``domains:``
    member outside the closed vocabulary. The unknown-key cases are the load-
    bearing ones: a permissive reader that drops a typo'd key silently grades
    a scenario against an expectation its author never wrote.
    """


class DependencyError(CompileError):
    """Raised when a Blueprint's ``dependencies:`` block (Phase 88) names a
    PEP 508 requirement not satisfied by the installed environment.

    A ``CompileError`` subclass — this is a compile-time PREFLIGHT (see
    ``aqueduct/dependencies.py::check_requirements`` and its call site in
    ``aqueduct/compiler/compiler.py``), so every existing ``except
    CompileError:`` caller keeps working unchanged, but it is a distinct
    type so a caller that needs to tell "missing/mismatched third-party
    package" apart from "this Blueprint failed to compile for some other
    reason" (e.g. a doctor check) can do so by TYPE.

    Unlike the capability-leaf framework, ``dependencies:`` is NOT
    engine-scoped and carries no allowlist — Aqueduct never installs
    anything, so the fix is always the same shape: run the printed ``pip
    install`` command yourself. ``problems`` carries the human-readable
    per-requirement lines (see ``check_requirements``) so a caller can
    report them without re-parsing the message.
    """

    def __init__(self, message: str, *, problems: list[str] | None = None) -> None:
        super().__init__(message)
        self.problems = list(problems or [])


class WatermarkIntentPendingError(AqueductError):
    """Raised at run start when a leftover `__intent__:<key>` depot row shows a
    prior run's append Egress started (or finished) but its downstream
    `format: depot` watermark Egress never cleared the intent row.

    Classified `CONFIG_ERROR` at the CLI (`aqueduct/cli/run_setup.py`) — like
    a `ParseError`, this blocks the run before any Spark/DuckDB work starts,
    since resolving the incremental read range against a stale/ambiguous
    watermark risks a duplicate append. ``key``/``run_id``/``started_at``
    carry the intent row's fields so a caller can act on them without
    re-parsing the message; the message itself names both remediations
    (de-duplicate then clear, or just clear once satisfied nothing landed).
    """

    def __init__(
        self, message: str, *, key: str = "", run_id: str = "", started_at: str = ""
    ) -> None:
        super().__init__(message)
        self.key = key
        self.run_id = run_id
        self.started_at = started_at


class ExecuteError(AqueductError):
    """Raised for unrecoverable execution-setup failures — the Executor
    layer's root error, mirroring ``ParseError`` (Parser) and
    ``CompileError`` (Compiler) above: config errors surfaced at execute()
    time, an unsupported module type, a cycle detected in the execution
    graph, a missing ``--from``/``--to`` module, etc. Per-module failures are
    NOT raised this way — each engine's ``execute()`` catches those
    internally and reports them as ``ModuleResult(status="error", ...)``
    (``aqueduct/executor/models.py``); this type is only for failures that
    abort the run before/between modules.

    EVERY registered engine raises this SAME type — never a private
    per-engine subclass — so a cross-engine caller (the CLI, the healing
    loop, ``aqueduct.executor.orchestrator``) can catch one type regardless
    of which engine actually ran. Before this fix, Spark's and DuckDB's
    executors each defined their OWN class named ``ExecuteError`` (identical
    name, ``AqueductError`` subclass, but two distinct types that never
    compared equal) — ``aqueduct/cli/run.py`` imported Spark's via
    ``aqueduct.executor``'s lazy ``__getattr__`` and caught only that one, so
    a DuckDB setup failure of the exact same shape passed through uncaught,
    and merely importing the name for the ``except`` clause pulled in
    ``pyspark`` on every engine's run. Both engine modules
    (``aqueduct/executor/spark/executor.py``,
    ``aqueduct/executor/duckdb_/executor.py``) now import this class rather
    than defining their own, and ``aqueduct/executor/__init__.py`` resolves
    the name directly from here (pyspark-free) instead of via the Spark
    executor.
    """
