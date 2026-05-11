# Issue: aqueduct init crashes when git is not installed

**Module/Function**: `aqueduct.cli.init`

**Error**:
```
FileNotFoundError: [Errno 2] No such file or directory: 'git'
```

**Context**:
When `aqueduct init` is run in an environment where the `git` binary is not installed, it attempts to execute `subprocess.run(["git", "rev-parse", ...])` without wrapping it in a try-except block for `FileNotFoundError`. As a result, the scaffolding process crashes instead of gracefully skipping the git steps and emitting a warning, failing the requirement specified in the test manifest.
