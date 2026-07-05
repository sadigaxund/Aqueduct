"""Agent constants — external contracts and defaults shared across the LLM subsystem.

Import this module directly at each call-site::

    from aqueduct.agent.constants import DEFAULT_MAX_TOKENS, DEFAULT_LLM_TIMEOUT

It must stay **leaf-level** (no intra-package imports) so layers that use it
do not create a circular dependency.
"""

DEFAULT_MAX_TOKENS = 4096
"""Default LLM ``max_tokens`` value used when no caller supplies one."""

DEFAULT_LLM_TIMEOUT = 120.0
"""Default HTTP timeout (seconds) for every LLM provider call."""

ANTHROPIC_API_VERSION = "2023-06-01"
"""The ``anthropic-version`` header value sent with every Anthropic Messages API request."""

DEFAULT_LLM_MODEL = "claude-sonnet-4-6"
"""Default model id set on ``agent.model`` in ``aqueduct.yml`` and as a
fallback when no blueprint override is present."""
