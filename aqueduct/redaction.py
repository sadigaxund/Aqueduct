"""Secret redaction registry — scrubs resolved @aq.secret() values from outputs.

Defense-in-depth, not the primary defense. Industry-standard substring redaction
with two guards against false positives:

  1. Length + entropy gate at registration time. Short or low-entropy values
     (e.g. ``password: "abc"``) are NOT registered — substring redaction would
     produce too many false positives. A weak-secret warning is emitted so the
     user knows the value is not being scrubbed.
  2. Token-boundary regex at redaction time. ``hunter2`` registered as a secret
     will NOT redact occurrences inside ``hunter2_module`` — the match requires
     non-word characters (or string boundaries) on both sides.

The primary defense remains "do not echo secrets at all". Redaction is the
backstop for tracebacks, error messages, FailureContext payloads, and webhook
bodies that may incidentally embed a resolved value.

Sinks that call ``redact()``:
  - CLI console emit paths (errors, status output)
  - observability.db writes (failure_contexts.context_json, runs.error_message)
  - patch sidecar files (patches/{pending,applied}/*.json)
  - webhook payloads (surveyor/webhook.py)
  - LLM agent request body (agent/__init__.py before httpx.post)

The registry is process-global by design: a secret resolved during config load
must be scrubbable everywhere downstream without threading the registry through
every call site.
"""

from __future__ import annotations

import math
import re
import warnings
from collections.abc import Iterable
from typing import Any

# ── Configuration ─────────────────────────────────────────────────────────────

# Minimum length for a value to be registered. Below this, substring redaction
# is too likely to false-positive on common identifiers, and the user is warned
# instead.
_MIN_SECRET_LENGTH = 8

# Minimum Shannon entropy (bits per character) for a value to be registered.
# A value below this threshold is too low-entropy to safely redact globally.
# Typical thresholds: random hex ~3.9, base64 ~5.5, English text ~3.5. Tuned
# so dictionary words and repeated patterns fall under.
_MIN_SECRET_ENTROPY = 2.5

# The placeholder substituted for each redacted occurrence.
REDACTED_PLACEHOLDER = "[REDACTED]"


# ── Internal state ────────────────────────────────────────────────────────────

# Distinct registered secret values. A set, not a list — duplicates do not
# change redaction behaviour and we want O(1) membership checks during tests.
_REGISTRY: set[str] = set()

# Compiled regex covering every registered secret with token boundaries. Rebuilt
# when the registry changes; None means "rebuild on next redact()".
_COMBINED_RE: re.Pattern[str] | None = None


# ── Public API ────────────────────────────────────────────────────────────────

def register(value: str, key_hint: str | None = None) -> bool:
    """Register a resolved secret value for global redaction.

    Args:
        value:    The resolved secret value (the actual sensitive string).
        key_hint: The secret's key/name, used only in warning text.

    Returns:
        True if the value was registered, False if rejected by the length /
        entropy gate. A rejected value triggers a single ``AQ-WARN
        [secret-weak-redact]`` so the operator can choose a stronger secret.
    """
    global _COMBINED_RE
    if not value:
        return False
    if len(value) < _MIN_SECRET_LENGTH or _shannon_entropy(value) < _MIN_SECRET_ENTROPY:
        _emit_weak_warning(key_hint, value)
        return False
    if value in _REGISTRY:
        return True
    _REGISTRY.add(value)
    _COMBINED_RE = None
    return True


def redact(text: Any) -> Any:
    """Replace every registered secret occurrence with the placeholder.

    Non-string inputs are walked recursively for dicts, lists, and tuples so
    that callers can hand in JSON-shaped data without flattening it first.
    Other types (int, bool, None, custom objects) are returned untouched —
    a secret value is by construction a string, so non-strings cannot match.
    """
    if not _REGISTRY:
        return text
    if isinstance(text, str):
        return _redact_string(text)
    if isinstance(text, dict):
        return {k: redact(v) for k, v in text.items()}
    if isinstance(text, list):
        return [redact(item) for item in text]
    if isinstance(text, tuple):
        return tuple(redact(item) for item in text)
    return text


def is_registered(value: str) -> bool:
    """Test helper: did this exact value pass the entropy/length gate?"""
    return value in _REGISTRY


def clear() -> None:
    """Test helper: reset the registry. Not for production use."""
    global _COMBINED_RE
    _REGISTRY.clear()
    _COMBINED_RE = None


def registered_values() -> Iterable[str]:
    """Test helper: read-only snapshot of currently registered values."""
    return tuple(_REGISTRY)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _redact_string(text: str) -> str:
    global _COMBINED_RE
    if _COMBINED_RE is None:
        _COMBINED_RE = _build_regex()
    if _COMBINED_RE is None:
        return text
    return _COMBINED_RE.sub(REDACTED_PLACEHOLDER, text)


def _build_regex() -> re.Pattern[str] | None:
    if not _REGISTRY:
        return None
    # Longest first so a secret that is a prefix of another doesn't shadow it.
    sorted_values = sorted(_REGISTRY, key=len, reverse=True)
    alternation = "|".join(re.escape(v) for v in sorted_values)
    # Token boundary on both sides — (?<![\w]) and (?![\w]) — so a secret value
    # equal to "hunter2" does not match inside "hunter2_module". Word characters
    # are [A-Za-z0-9_]; everything else (whitespace, quotes, colons, slashes,
    # punctuation, EOS/BOS) acts as a boundary.
    return re.compile(rf"(?<!\w)(?:{alternation})(?!\w)")


def _shannon_entropy(value: str) -> float:
    """Bits-per-character Shannon entropy. 0 for empty string."""
    if not value:
        return 0.0
    n = len(value)
    counts: dict[str, int] = {}
    for ch in value:
        counts[ch] = counts.get(ch, 0) + 1
    return -sum((c / n) * math.log2(c / n) for c in counts.values())


def _emit_weak_warning(key_hint: str | None, value: str) -> None:
    name = f"@aq.secret({key_hint!r})" if key_hint else "an @aq.secret() value"
    warnings.warn(
        f"AQ-WARN [secret-weak-redact] {name} resolved to a short or low-entropy "
        f"value (len={len(value)}, entropy={_shannon_entropy(value):.2f} bits/char); "
        "redaction will NOT scrub it from logs / FailureContext / patches because "
        "substring removal of common identifiers would produce too many false "
        "positives. Use a longer, higher-entropy secret to enable global redaction.",
        stacklevel=3,
    )
