"""Gate 1's effective-engine-config check (`aqueduct/patch/config_delta.py`).

The property under test throughout: a `set_engine_config` write that cannot
change what the engine sees must be REFUSED, and a patch that writes no
engine config at all must report `not_applicable` — never `pass`.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from aqueduct.config import (
    AqueductConfig,
    DuckDBEngineConfig,
    EngineConfig,
    SparkEngineConfig,
)
from aqueduct.patch.apply import PatchError, _check_guardrails, apply_patch_file
from aqueduct.patch.config_delta import (
    engine_config_write_targets,
    run_engine_config_delta_gate,
)
from aqueduct.patch.grammar import PatchSpec

pytestmark = pytest.mark.unit


def _spec(*ops, patch_id="p1"):
    return PatchSpec.model_validate(
        {
            "patch_id": patch_id,
            "run_id": "r1",
            "rationale": "test",
            "operations": list(ops),
        }
    )


def _set_spark(key, value):
    return {"op": "set_engine_config", "engine": "spark", "key": key, "value": value}


def _bp(engine_block=None, modules=None):
    bp: dict = {
        "aqueduct": "1.0",
        "id": "bp",
        "name": "bp",
        "modules": modules if modules is not None else [],
    }
    if engine_block is not None:
        bp["engine"] = engine_block
    return bp


def _cfg(spark_conf=None, duckdb=None):
    return AqueductConfig(
        engine=EngineConfig(
            spark=SparkEngineConfig(conf=dict(spark_conf or {})),
            duckdb=duckdb or DuckDBEngineConfig(),
        )
    )


# ── (b) empty effective delta → refuse ────────────────────────────────────────


def test_write_matching_the_aqueduct_yml_value_is_refused():
    """The value already resolves from `aqueduct.yml` — the write is inert."""
    cfg = _cfg({"spark.sql.shuffle.partitions": "400"})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    with pytest.raises(PatchError) as exc:
        run_engine_config_delta_gate(cfg=cfg, blueprint_before=_bp(), patch_spec=spec)
    msg = str(exc.value)
    assert "has no effect" in msg
    assert "engine 'spark' key 'spark.sql.shuffle.partitions' already resolves to '400'" in msg


def test_write_matching_the_existing_blueprint_value_is_refused():
    """The Blueprint already carries the value, so `bp_after == bp_before`.

    A gate that derived applicability from a before/after diff of the
    `engine:` block would see no change and call this "no config surface".
    The write-target probe sees the write regardless.
    """
    bp = _bp({"spark": {"conf": {"spark.sql.shuffle.partitions": "400"}}})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    with pytest.raises(PatchError, match="has no effect"):
        run_engine_config_delta_gate(cfg=_cfg(), blueprint_before=bp, patch_spec=spec)


def test_respelling_a_value_without_changing_it_is_refused():
    """`400` and `"400"` are the same setting to a session; the YAML spelling
    is not the effective config."""
    cfg = _cfg({"spark.sql.shuffle.partitions": 400})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    with pytest.raises(PatchError, match="has no effect"):
        run_engine_config_delta_gate(cfg=cfg, blueprint_before=_bp(), patch_spec=spec)


def test_typed_engine_field_no_op_is_refused():
    """Same rule for a typed-field engine block (DuckDB), not just Spark's bag."""
    cfg = _cfg(duckdb=DuckDBEngineConfig(threads=4))
    spec = _spec({"op": "set_engine_config", "engine": "duckdb", "key": "threads", "value": 4})
    with pytest.raises(PatchError, match="has no effect"):
        run_engine_config_delta_gate(cfg=cfg, blueprint_before=_bp(), patch_spec=spec)


def test_refusal_is_a_patch_error_not_an_allowlist_error():
    """Type, not message: a patch that cannot achieve anything is a PATCH
    problem (fix = a different patch), the same class as an allowlist
    violation — never `EngineConfigAllowlistError`, which means the shipped
    data is broken."""
    from aqueduct.errors import EngineConfigAllowlistError

    cfg = _cfg({"spark.sql.shuffle.partitions": "400"})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    with pytest.raises(PatchError) as exc:
        run_engine_config_delta_gate(cfg=cfg, blueprint_before=_bp(), patch_spec=spec)
    assert not isinstance(exc.value, EngineConfigAllowlistError)


# ── (a) a real change passes and records the delta ────────────────────────────


def test_real_change_passes_and_records_before_and_after():
    cfg = _cfg({"spark.sql.shuffle.partitions": "200"})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    res = run_engine_config_delta_gate(cfg=cfg, blueprint_before=_bp(), patch_spec=spec)
    assert res.status == "pass"
    assert res.delta["spark"]["spark.sql.shuffle.partitions"] == {
        "before": "200",
        "after": "400",
    }
    assert res.write_targets["spark"] == ("spark.sql.shuffle.partitions",)


def test_blueprint_layer_wins_over_aqueduct_yml():
    """Precedence, asserted rather than assumed: the Blueprint's own
    `engine.<name>` entry is layered ON TOP of `aqueduct.yml`'s."""
    cfg = _cfg({"spark.sql.shuffle.partitions": "200"})
    bp = _bp({"spark": {"conf": {"spark.sql.shuffle.partitions": "999"}}})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    res = run_engine_config_delta_gate(cfg=cfg, blueprint_before=bp, patch_spec=spec)
    # `before` is the Blueprint's 999, NOT aqueduct.yml's 200 — proving which
    # layer wins when both set the key.
    assert res.delta["spark"]["spark.sql.shuffle.partitions"]["before"] == "999"


# ── (4) a patch with no config ops must NOT report `pass` ─────────────────────


def test_pipeline_only_patch_reports_not_applicable_never_pass():
    bp = _bp(
        modules=[{"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT 1"}}]
    )
    spec = _spec(
        {
            "op": "set_module_config_key",
            "module_id": "ch1",
            "key": "query",
            "value": "SELECT 2",
        }
    )
    res = run_engine_config_delta_gate(cfg=_cfg(), blueprint_before=bp, patch_spec=spec)
    assert res.status == "not_applicable"
    assert res.status != "pass"
    assert res.detail
    assert res.delta == {}


def test_defer_to_human_patch_reports_not_applicable():
    spec = _spec({"op": "defer_to_human", "diagnosis": "needs a human"})
    res = run_engine_config_delta_gate(cfg=_cfg(), blueprint_before=_bp(), patch_spec=spec)
    assert res.status == "not_applicable"


# ── (3) applicability is derived, not an op-name include-list ─────────────────


def test_write_targets_are_independent_of_whether_the_value_changed():
    """The write set comes from re-applying onto a stripped `engine:` block, so
    it reports the key even when the patch writes what is already there."""
    bp = _bp({"spark": {"conf": {"spark.sql.shuffle.partitions": "400"}}})
    spec = _spec(_set_spark("spark.sql.shuffle.partitions", "400"))
    assert engine_config_write_targets(bp, spec) == {"spark": ("spark.sql.shuffle.partitions",)}


def test_write_targets_empty_for_a_patch_that_writes_no_engine_config():
    bp = _bp(
        modules=[{"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT 1"}}]
    )
    spec = _spec(
        {"op": "set_module_config_key", "module_id": "ch1", "key": "query", "value": "SELECT 2"}
    )
    assert engine_config_write_targets(bp, spec) == {}


def test_config_op_is_still_evaluated_when_a_sibling_module_op_cannot_apply():
    """`aqueduct heal` runs Gate 1 against a Blueprint stub with no modules.

    The module op cannot apply there; the config op still must be judged, or
    the heal loop silently stops checking config writes on that path.
    """
    stub = {"agent": {"guardrails": {}}}
    spec = _spec(
        {"op": "set_module_config_key", "module_id": "nope", "key": "query", "value": "SELECT 2"},
        _set_spark("spark.sql.shuffle.partitions", "400"),
    )
    assert engine_config_write_targets(stub, spec) == {"spark": ("spark.sql.shuffle.partitions",)}
    cfg = _cfg({"spark.sql.shuffle.partitions": "400"})
    with pytest.raises(PatchError, match="has no effect"):
        run_engine_config_delta_gate(cfg=cfg, blueprint_before=stub, patch_spec=spec)


# ── (2) the check cannot be skipped on any apply path ─────────────────────────


def test_every_gate1_call_site_supplies_a_config():
    """Structural: `cfg` has no default on `_check_guardrails`, so a call site
    that forgets it is a TypeError, not a silently weakened gate. This scan
    fails the build if a NEW Gate 1 call site is added without one."""
    root = Path(__file__).resolve().parents[2] / "aqueduct"
    sites = 0
    for py in root.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            if name not in ("_check_guardrails", "_apply_check_guardrails"):
                continue
            sites += 1
            assert any(kw.arg == "cfg" for kw in node.keywords), (
                f"{py}:{node.lineno} calls Gate 1 without cfg= — the "
                "effective-engine-config check would be answered against the "
                "wrong config layer"
            )
    # The five apply paths: patch apply/import (apply.py), aqueduct run's
    # in-loop callback + its pre-staging check, aqueduct heal's callback,
    # patch preview, and benchmark scenario replay.
    assert sites >= 6, f"expected every Gate 1 call site to be scanned, found {sites}"


def test_check_guardrails_requires_cfg_keyword():
    import inspect

    sig = inspect.signature(_check_guardrails)
    assert sig.parameters["cfg"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["cfg"].default is inspect.Parameter.empty


# ── (a) the delta lands in `healed_by:` on the real apply path ────────────────


_BP_YAML = """\
aqueduct: "1.0"
id: delta-bp
name: delta bp
engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: "200"
modules:
  - id: ing
    type: Ingress
    label: source
    config:
      format: parquet
      path: data/in.parquet
  - id: out
    type: Egress
    label: sink
    config:
      format: parquet
      path: data/out.parquet
      mode: overwrite
edges:
  - from: ing
    to: out
"""


def test_apply_patch_file_records_the_effective_delta_in_healed_by(tmp_path):
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(_BP_YAML, encoding="utf-8")
    patch_path = tmp_path / "patch.json"
    patch_path.write_text(
        json.dumps(
            {
                "patch_id": "cfgdelta",
                "run_id": "run-1",
                "rationale": "raise shuffle partitions",
                "operations": [_set_spark("spark.sql.shuffle.partitions", 800)],
                "_aq_meta": {"engine": "spark", "run_id": "run-1"},
            }
        ),
        encoding="utf-8",
    )

    apply_patch_file(
        blueprint_path=bp_path,
        patch_path=patch_path,
        patches_dir=tmp_path / "patches",
        cfg=_cfg(),
    )

    import yaml

    patched = yaml.safe_load(bp_path.read_text(encoding="utf-8"))
    record = patched["healed_by"][0]
    assert record["engine_config_delta"] == {
        "spark": {"spark.sql.shuffle.partitions": {"before": "200", "after": 800}}
    }

    # The stamped Blueprint must still parse — the new field is a real schema
    # member, not extra baggage `extra="forbid"` rejects on the next load.
    from aqueduct.parser.parser import parse

    parsed = parse(str(bp_path))
    assert (
        parsed.healed_by[0].engine_config_delta["spark"]["spark.sql.shuffle.partitions"]["after"]
        == 800
    )


def test_apply_patch_file_omits_the_field_for_a_pipeline_only_patch(tmp_path):
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(_BP_YAML, encoding="utf-8")
    patch_path = tmp_path / "patch.json"
    patch_path.write_text(
        json.dumps(
            {
                "patch_id": "labelonly",
                "run_id": "run-1",
                "rationale": "relabel",
                "operations": [
                    {"op": "replace_module_label", "module_id": "ing", "label": "source"}
                ],
                "_aq_meta": {"engine": "spark", "run_id": "run-1"},
            }
        ),
        encoding="utf-8",
    )

    apply_patch_file(
        blueprint_path=bp_path,
        patch_path=patch_path,
        patches_dir=tmp_path / "patches",
        cfg=_cfg(),
    )

    import yaml

    patched = yaml.safe_load(bp_path.read_text(encoding="utf-8"))
    assert "engine_config_delta" not in patched["healed_by"][0]


def test_apply_patch_file_refuses_an_inert_config_write(tmp_path):
    """End-to-end on the `patch apply` path: the Blueprint is left untouched."""
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(_BP_YAML, encoding="utf-8")
    original = bp_path.read_text(encoding="utf-8")
    patch_path = tmp_path / "patch.json"
    patch_path.write_text(
        json.dumps(
            {
                "patch_id": "inert",
                "run_id": "run-1",
                "rationale": "no-op",
                "operations": [_set_spark("spark.sql.shuffle.partitions", 200)],
                "_aq_meta": {"engine": "spark", "run_id": "run-1"},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PatchError, match="has no effect"):
        apply_patch_file(
            blueprint_path=bp_path,
            patch_path=patch_path,
            patches_dir=tmp_path / "patches",
            cfg=_cfg(),
        )
    assert bp_path.read_text(encoding="utf-8") == original
