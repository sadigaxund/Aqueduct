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


class ConfigError(AqueductError):
    """Raised when aqueduct.yml cannot be loaded or fails validation."""
