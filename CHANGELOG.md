# Changelog

## 0.1.0 — 2026-04-20

Initial release.

- Declarative YAML Blueprint pipelines for Apache Spark
- Modules: Ingress, Egress, Channel, Junction, Funnel, Probe, Regulator, Arcade
- LLM self-healing loop (Anthropic, Ollama, OpenAI-compatible)
- PatchSpec grammar: structured, auditable LLM patches
- Depot KV store for cross-run state
- Spillway error-row routing
- RetryPolicy with exponential/linear/fixed backoff + deadline
- Column-level lineage writer (sqlglot + DuckDB)
- CLI: validate / compile / run / patch apply / patch reject
