# Aqueduct: Blueprint & Engine Reference

**Version 2.81: Reference Document**

*Self-healing LLM-integrated data pipelines*
*Declarative · Observable · Autonomous · Self-healing*

Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor

> **Document map:** This document covers the Blueprint format, engine architecture, module types, and self-healing agent. Companion docs cover specific areas:
> - **[CLI Reference](cli_reference.md)**: All commands and flags
> - **[Spark Guide](spark_guide.md)**: Compiler warnings, performance, tuning
> - **[Observability Guide](observability_guide.md)**: Store schemas and diagnostic query cookbook
> - **[Production Guide](production_guide.md)**: Cluster deployment, security, Delta operations

This page is the index. Each topic page below owns its sections in full; nothing here duplicates their prose.

- **[Overview](specs/01-overview.md)** (§1 Introduction, §2 Naming glossary, §3 System architecture): purpose, design principles, naming conventions, and the high-level component architecture.
- **[Blueprint Format](specs/02-blueprint.md)** (§4 Blueprint format): file structure, module schema, ports, and every module type's specification.
- **[Context Registry](specs/03-resolution.md)** (§5 Context Registry): the three-tier resolution model, runtime functions, and the UDF registry.
- **[Execution](specs/04-execution.md)** (§6 Observability, Probes & Flow Report): the observability and probe design constraints.
- **[Lineage](specs/05-lineage.md)** (§7 Lineage): structural lineage and the runtime flow report.
- **[Self-Healing](specs/06-healing.md)** (§8 Self-healing & LLM agent loop): the healing flow, approval and sandbox modes, patch grammar, and drift detection.
- **[Stores and Engine Ops](specs/07-stores-and-ops.md)** (§9 Type system, §10 Deployment & engine integration): the hub type system, engine configuration, path resolution, and the capability framework.
- **[Polyglot Engine Boundaries](specs/08-polyglot.md)** (§11 Engine scope & boundaries, plus the synthetic Handoff module and its runtime execution): what Aqueduct is and is not, scheduling, and cross-engine handoff.
