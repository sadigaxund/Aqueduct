# Contributing

## Setup

```bash
pip install -e ".[dev]"
```

## Dependency lock (CI and dev environments)

Two tiers, doing different jobs:

- `pyproject.toml` keeps version **ranges**. Aqueduct is a published library, so exact pins there would make it uninstallable next to your own stack. Do not pin it.
- `requirements/*.txt` are committed pip **constraints** files that pin the CI/dev environment. A constraints file adds nothing to an install: it fixes the version of whatever the ranges resolve to, so a lane installs the same tree today and in six months. CI applies them through `PIP_CONSTRAINT`, and you can do the same locally:

```bash
PIP_CONSTRAINT=requirements/ci-py311.txt pip install -e ".[dev,spark]"
```

Regenerate after changing a dependency range in `pyproject.toml`:

```bash
bash scripts/lock.sh          # needs uv: pipx install uv
git diff requirements/        # review what moved, then commit
```

One lane is deliberately not locked: the `snippets` canary in `.github/workflows/version-matrix.yml` resolves fresh on every run, so it goes red first when an upstream release breaks something. Setting `PIP_CONSTRAINT` there would remove the only lane watching upstream. See `docs/compatibility.md`.

## Requirements

- Python 3.11+
- Java 17 (required for Apache Spark)
  - Ensure `JAVA_HOME` points to a Java 17 installation.
  - Verify with `java -version`.

## Tests

```bash
pytest tests/                 # full suite
pytest -m unit                # fast, pure tests only (no Spark/network)
pytest --collect-only -m todo # the backlog: planned-but-unwritten tests
pytest -rsx                   # show skipped (incl. todo) + xfail with reasons
```

### Three layers

Every test carries one layer marker:

- **`unit`**: fast and pure, without Spark, network, or external services.
- **`integration`**: blueprint/feature level. `gallery/snippets` parse + compile, `*.aqtest.yml` run on real `local[1]` Spark, `*.aqscenario.yml` heal with a mocked agent.
- **`e2e`**: full `gallery/showcase` pipelines.

Capability gates (`spark`, `agent`, `airflow`, `slow`) skip when the dependency is absent.

### Test backlog (no manifest)

There is no hand-maintained test ledger; the suite is the source of truth.

- **Need a test you haven't written?** Add a `@pytest.mark.todo("input → expected output/error")` stub to **`tests/test_backlog.py`** (the single landing file), with an `intended:` line for where it should ultimately live. It auto-skips and shows up in `pytest --collect-only -m todo`. When you implement it, move it to that path and drop the marker.
- **Found a bug?** Write the test that *should* pass and mark it `@pytest.mark.xfail(strict=True, reason="bug: …")`. `xfail_strict` is on, so the build fails the instant the bug is fixed, forcing you to drop the marker. No status is ever flipped by hand.
- **Honesty guard:** `tests/test_meta_quality.py` fails the build if any test asserts nothing (and isn't a `todo`/`xfail` stub). Every test must check an outcome.

(The old `TEST_MANIFEST.md` is frozen in `docs/archive/` for history only.)

CI runs jobs per feature area (parser, compiler, executor, agent, etc.).
Only jobs whose files changed fire on branches, so don't worry if unrelated
jobs skip your PR.  See AGENTS.md for the full job table.

## Code style

```bash
ruff check . && black .
```

## Design docs

See `docs/specs.md` and `.dev/JOURNAL.md` for architecture decisions.
