"""Infrastructure utilities shared across layers (no domain logic).

`http` holds the single source of truth for outbound-HTTP retry policy, backoff,
and fire-and-forget delivery so the webhook, OpenLineage, LLM-provider, and CI
callback paths don't each reimplement (and drift on) the same mechanics.
"""
