# Contributing

## Setup

```bash
pip install -e ".[dev]"
```

## Requirements

- Python 3.11+
- Java 17 (required for Apache Spark)
  - Ensure `JAVA_HOME` points to a Java 17 installation.
  - Verify with `java -version`.

## Tests

```bash
pytest tests/
```

CI runs jobs per feature area (parser, compiler, executor, agent, etc.).
Only jobs whose files changed fire on branches — don't worry if unrelated
jobs skip your PR.  See AGENTS.md for the full job table.

## Code style

```bash
ruff check . && black .
```

## Design docs

See `docs/specs.md` and `.dev/JOURNAL.md` for architecture decisions.
