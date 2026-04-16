# 🏛️ Aqueduct

**Intelligent, self-healing Spark pipelines. Declarative. Observable. Autonomous.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)

Aqueduct is a control plane for Apache Spark. You write pipelines as YAML *Blueprints*. Aqueduct validates, compiles, and executes them—monitoring every step. When something breaks, Aqueduct can **autonomously patch the pipeline** using an LLM agent, applying structured, auditable fixes.

> **Vibe-Transform-Load (VTL) for Apache Spark.**

---

## 🧠 What Makes Aqueduct Different?

- **Declarative YAML Blueprints** – No DAG wiring in code. Version‑control your entire pipeline.
- **LLM‑First Observability** – Every failure ships with a complete `FailureContext` JSON, ready for an agent (or you) to diagnose without digging through logs.
- **Patch Grammar, Not Codegen** – The LLM operates inside a structured `PatchSpec` schema. Patches are auditable, reversible, and never hallucinate invalid YAML.
- **Zero‑Cost Observability** – Probes use Spark’s internal metrics (`SparkListener`) to collect row counts and skew data without adding extra Spark actions.
- **Passive‑by‑Default Gates** – Regulators (data quality gates) compile away entirely unless you wire them up. No overhead for unused features.

---

## 🏛️ Open‑Core Model

**Aqueduct Core is Apache 2.0 licensed and always will be.**  
You can run it locally, in CI, or on a production Spark cluster for free—forever.

A commercial frontend, **Aqueduct Platform**, adds:
- Centralized dashboards for all your pipelines
- Team collaboration and RBAC
- Managed Depot (persistent KV store)
- Audit logs of every LLM patch

The Core engine emits a documented webhook event stream so you can integrate it with *any* frontend—ours, yours, or a third‑party.

**This repository contains the full engine. No telemetry. No proprietary code.**

---

## 📦 Installation

```bash
pip install aqueduct