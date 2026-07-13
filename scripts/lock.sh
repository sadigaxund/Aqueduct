#!/usr/bin/env bash
# Regenerate the CI/dev environment lock (requirements/ci-py311.txt).
#
# TWO TIERS, and both matter:
#
#   1. LIBRARY DEPENDENCIES STAY AS RANGES in pyproject.toml. aqueduct-core is a
#      published library: pinning exact versions there would make it uninstallable
#      alongside a user's own stack. Never pin pyproject.
#   2. THE CI/DEV ENVIRONMENT IS LOCKED. The file this script writes is a pip
#      CONSTRAINTS file (`pip install -c` / $PIP_CONSTRAINT): it does not add or
#      remove anything from an install, it only fixes the version of whatever the
#      ranges resolve to. So the reproducible lanes install the same tree today and
#      in six months, while pyproject keeps its ranges.
#
# The canary lane (version-matrix.yml's `snippets` job) deliberately does NOT use
# this file. Its job is to resolve fresh and go red first when upstream drifts —
# that is the early-warning signal. Constraining it would delete the feature.
#
# Usage:  bash scripts/lock.sh          # rewrite the lock from pyproject.toml
#         git diff requirements/        # review what moved, then commit
#
# Requires uv (https://docs.astral.sh/uv/): `pipx install uv` or
# `curl -LsSf https://astral.sh/uv/install.sh | sh`. uv is used only to RESOLVE;
# the output is a plain pip constraints file and CI needs no uv.
set -euo pipefail

cd "$(dirname "$0")/.."

# ── 1. requirements/ci-py311.txt ──────────────────────────────────────────────
# Python 3.11 = the supported floor and the interpreter every single-version lane
# runs on: every test-suite.yml job, plus version-matrix.yml's unit-tests job.
# Extras = the union of what those lanes install; a constraint for a package a
# given lane does not install is simply inert, so one file serves all of them.
uv pip compile pyproject.toml \
  --python-version 3.11 \
  --extra dev \
  --extra spark \
  --extra llm \
  --extra redis \
  --extra postgres \
  --extra mcp \
  --extra tui \
  --extra dashboard \
  --output-file requirements/ci-py311.txt

# ── 2. requirements/compat-py3XX.txt ──────────────────────────────────────────
# The compat matrix's promise lanes (version-matrix.yml) run three interpreters,
# so they need one constraints file each. They deliberately OMIT the `spark` extra:
# pyspark / delta-spark are the axes the matrix itself pins per combo (the Legacy
# combo installs pyspark 3.5.8 explicitly), and a constraint pinning pyspark would
# collide with that. Everything else those lanes install is locked.
for PY in 3.11 3.12 3.13; do
  uv pip compile pyproject.toml \
    --python-version "$PY" \
    --extra dev \
    --extra postgres \
    --extra redis \
    --output-file "requirements/compat-py${PY}.txt"
done

# ── 3. Prepend the contract header ────────────────────────────────────────────
# Written by the script, not by hand, so a regeneration cannot silently drop it.
for f in requirements/ci-py311.txt requirements/compat-py3.11.txt \
         requirements/compat-py3.12.txt requirements/compat-py3.13.txt; do
  tmp=$(mktemp)
  cat > "$tmp" <<'HEADER'
# CI / dev ENVIRONMENT LOCK — a pip CONSTRAINTS file, not a dependency list.
#
# Regenerate with `bash scripts/lock.sh`, then review `git diff requirements/`.
#
# Two tiers, both load-bearing:
#   * pyproject.toml keeps RANGES. aqueduct-core is a published library; exact pins
#     there would make it uninstallable next to a user's own stack. Do not pin it.
#   * This file pins the ENVIRONMENT the reproducible CI lanes install. Applied via
#     $PIP_CONSTRAINT, it adds nothing and removes nothing — it only fixes the
#     version of whatever the ranges resolve to, so a lane installs the same tree
#     today and in six months regardless of what PyPI published this morning.
#
# The CANARY lane (version-matrix.yml's `snippets` job) deliberately does NOT set
# $PIP_CONSTRAINT: it resolves fresh so it goes red first when upstream drifts, and
# that early warning is the feature. Do not constrain it.
HEADER
  cat "$f" >> "$tmp"
  mv "$tmp" "$f"
done

echo "✓ requirements/ regenerated — review \`git diff requirements/\` before committing."
