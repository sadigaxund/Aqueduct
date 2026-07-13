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
    """Raised when an ``aqueduct.engines`` entry point fails to load (Phase 78).

    A broken or half-installed third-party engine plugin must surface as a clean
    Aqueduct error naming the entry point and its underlying cause, never as a
    bare ``ImportError`` escaping out of ``aqueduct.yml`` loading.
    """


class ConfigError(AqueductError):
    """Raised when aqueduct.yml cannot be loaded or fails validation."""
