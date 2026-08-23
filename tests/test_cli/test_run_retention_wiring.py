"""Phase 85 W9 follow-ups — `aqueduct run` must actually thread
`observability.retention:` (aqueduct.yml) through to both:

  * `ProbeSampling.sample_rows_keep_last_n` (built in
    `aqueduct.cli.run._load_engine_config`)
  * `Surveyor(..., retention=...)` (built in `aqueduct.cli.run._setup_surveyor`)

Without this wiring both would silently fall back to their own class
defaults regardless of what the user configured — a silent no-op AGENTS.md
forbids. These are seam tests: they exercise a real (but minimal, failing —
fast) `aqueduct run` end-to-end and spy on the two constructors, rather than
asserting on prune behaviour itself (already covered by
`test_report_prune_costs.py` / `tests/test_surveyor/test_retention.py`).
"""

from __future__ import annotations

from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = __import__("pytest").mark.duckdb

# Ingress pointed at a file that doesn't exist — fails fast, but only AFTER
# Phase 1 (_load_engine_config, builds ProbeSampling) and Phase 3
# (_setup_surveyor, builds Surveyor) have already run, so both constructors
# are exercised without needing a real successful pipeline.
_BP = """\
aqueduct: '1.0'
id: retention_wiring_bp
name: Retention Wiring BP
agent:
  approval: disabled
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: csv, path: /nonexistent/does-not-exist.csv}
edges: []
"""

_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb

stores:
  observability:
    backend: duckdb
    path: "{obs}"
  depots:
    default:
      backend: duckdb
      path: "{dep}"

observability:
  retention:
    sample_rows_keep_last_n: 7
    run_records_days: 5
    heal_attempts_days: 11
"""


def test_probe_sampling_and_surveyor_retention_wiring(tmp_path, monkeypatch):
    import aqueduct.executor.spark.probe as probe_module
    import aqueduct.surveyor.surveyor as surveyor_module

    captured: dict = {}

    _real_probe_sampling = probe_module.ProbeSampling

    def _spy_probe_sampling(**kwargs):
        captured["probe_kwargs"] = kwargs
        return _real_probe_sampling(**kwargs)

    _real_surveyor = surveyor_module.Surveyor

    class _SpySurveyor(_real_surveyor):
        def __init__(self, *args, **kwargs):
            captured["surveyor_retention"] = kwargs.get("retention")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(probe_module, "ProbeSampling", _spy_probe_sampling)
    monkeypatch.setattr(surveyor_module, "Surveyor", _SpySurveyor)

    bp = tmp_path / "bp.yml"
    bp.write_text(_BP, encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        _CFG.format(obs=str(tmp_path / "obs"), dep=str(tmp_path / "dep.duckdb")),
        encoding="utf-8",
    )

    runner = CliRunner()
    runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert "probe_kwargs" in captured, "ProbeSampling was never constructed — seam not reached"
    assert captured["probe_kwargs"]["sample_rows_keep_last_n"] == 7

    retention = captured.get("surveyor_retention")
    assert retention is not None, "Surveyor(retention=...) was not passed at all"
    assert retention.sample_rows_keep_last_n == 7
    assert retention.run_records_days == 5
    assert retention.heal_attempts_days == 11
