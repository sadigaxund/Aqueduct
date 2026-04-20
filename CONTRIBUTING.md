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

## Code style

```bash
ruff check . && black .
```

## Design docs

See `docs/specs.md` and `.dev/JOURNAL.md` for architecture decisions.
